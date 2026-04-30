use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use crossterm::{
    event::{
        DisableMouseCapture, Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use protocol::{read_frame, write_frame, Message};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame, Terminal,
};
use std::{
    collections::HashSet,
    io::Stdout,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpStream, sync::mpsc};
use unicode_width::UnicodeWidthStr;

#[derive(Parser, Debug)]
#[command(author, version, about, bin_name = "multi-chat-client")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:9000")]
    addr: String,
    #[arg(short, long)]
    name: String,
}

#[derive(Debug, Clone)]
enum Entry {
    Chat {
        from: String,
        body: String,
        ts: u64,
        own: bool,
        integrity_ok: bool,
    },
    Join {
        who: String,
        ts: u64,
    },
    Leave {
        who: String,
        ts: u64,
    },
    System {
        body: String,
        ts: u64,
    },
    Error {
        body: String,
        ts: u64,
    },
}

struct App {
    addr: String,
    name: String,
    entries: Vec<Entry>,
    input: String,
    cursor: usize, // byte offset within `input`
    scroll_back: u16,
    connected: bool,
    peers: HashSet<String>,
    seq: u64,
}

impl App {
    fn new(addr: String, name: String) -> Self {
        let mut me = HashSet::new();
        me.insert(name.clone());
        Self {
            addr,
            name,
            entries: Vec::new(),
            input: String::new(),
            cursor: 0,
            scroll_back: 0,
            connected: false,
            peers: me,
            seq: 0,
        }
    }

    fn push_system(&mut self, body: impl Into<String>) {
        self.entries.push(Entry::System { body: body.into(), ts: now_ms() });
    }
    fn push_error(&mut self, body: impl Into<String>) {
        self.entries.push(Entry::Error { body: body.into(), ts: now_ms() });
    }

    fn handle_incoming(&mut self, msg: Message) {
        match msg {
            Message::Chat { from, body, ts, hash, .. } => {
                // The server in older revisions echoes a sender's own
                // messages back to them. Drop the duplicate — we already
                // rendered it locally on send.
                if from == self.name {
                    return;
                }
                let ok = Message::calculate_body_hash(&body) == hash;
                self.peers.insert(from.clone());
                self.entries.push(Entry::Chat { from, body, ts, own: false, integrity_ok: ok });
            }
            Message::Join { client_id } => {
                if client_id == self.name {
                    return;
                }
                self.peers.insert(client_id.clone());
                self.entries.push(Entry::Join { who: client_id, ts: now_ms() });
            }
            Message::Leave { client_id } => {
                if client_id == self.name {
                    return;
                }
                self.peers.remove(&client_id);
                self.entries.push(Entry::Leave { who: client_id, ts: now_ms() });
            }
            Message::Sys { body } => self.push_system(body),
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut terminal = setup_terminal()?;
    // Restore terminal even on panic.
    let _guard = TerminalGuard;

    let result = run_app(&mut terminal, args).await;

    teardown_terminal(&mut terminal).ok();

    result
}

struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
    }
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
    Ok(Terminal::new(CrosstermBackend::new(stdout))?)
}

fn teardown_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;
    Ok(())
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    args: Args,
) -> Result<()> {
    let mut app = App::new(args.addr.clone(), args.name.clone());
    app.push_system(format!("Connecting to {} as {} ...", args.addr, args.name));

    let stream = TcpStream::connect(&args.addr)
        .await
        .with_context(|| format!("failed to connect to {}", args.addr))?;
    let (reader, mut writer) = stream.into_split();

    write_frame(&mut writer, &Message::Join { client_id: args.name.clone() })
        .await
        .context("failed to send Join")?;
    app.connected = true;
    app.push_system("Connected. Type a message and press Enter.");

    let (net_tx, mut net_rx) = mpsc::unbounded_channel::<NetEvent>();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();

    // Reader: socket -> UI channel
    let net_tx_reader = net_tx.clone();
    let reader_task = tokio::spawn(async move {
        let mut reader = reader;
        loop {
            match read_frame(&mut reader).await {
                Ok(msg) => {
                    if net_tx_reader.send(NetEvent::Incoming(msg)).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = net_tx_reader.send(NetEvent::Disconnected(e.to_string()));
                    break;
                }
            }
        }
    });

    // Writer: UI channel -> socket
    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write_frame(&mut writer, &msg).await.is_err() {
                break;
            }
        }
    });

    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(200));

    terminal.draw(|f| ui(f, &app))?;

    'main_loop: loop {
        tokio::select! {
            biased;
            Some(net_evt) = net_rx.recv() => {
                match net_evt {
                    NetEvent::Incoming(msg) => app.handle_incoming(msg),
                    NetEvent::Disconnected(reason) => {
                        app.connected = false;
                        app.push_error(format!("disconnected: {reason}"));
                    }
                }
            }
            Some(Ok(evt)) = events.next() => {
                if let Event::Key(key) = evt {
                    if key.kind != KeyEventKind::Press { continue; }
                    if handle_key(key, &mut app, &out_tx) {
                        let _ = out_tx.send(Message::Leave { client_id: app.name.clone() });
                        break 'main_loop;
                    }
                }
            }
            _ = tick.tick() => {}
        }

        terminal.draw(|f| ui(f, &app))?;
    }

    drop(out_tx);
    let _ = tokio::time::timeout(Duration::from_millis(500), writer_task).await;
    reader_task.abort();
    Ok(())
}

enum NetEvent {
    Incoming(Message),
    Disconnected(String),
}

/// Returns `true` when the user wants to quit.
fn handle_key(
    key: KeyEvent,
    app: &mut App,
    out_tx: &mpsc::UnboundedSender<Message>,
) -> bool {
    match key.code {
        KeyCode::Esc => return true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) && app.input.is_empty() => {
            return true;
        }
        KeyCode::Char(c) => {
            app.input.insert(app.cursor, c);
            app.cursor += c.len_utf8();
        }
        KeyCode::Backspace => {
            if app.cursor > 0 {
                let prev = prev_char_boundary(&app.input, app.cursor);
                app.input.replace_range(prev..app.cursor, "");
                app.cursor = prev;
            }
        }
        KeyCode::Delete => {
            if app.cursor < app.input.len() {
                let next = next_char_boundary(&app.input, app.cursor);
                app.input.replace_range(app.cursor..next, "");
            }
        }
        KeyCode::Left => {
            if app.cursor > 0 {
                app.cursor = prev_char_boundary(&app.input, app.cursor);
            }
        }
        KeyCode::Right => {
            if app.cursor < app.input.len() {
                app.cursor = next_char_boundary(&app.input, app.cursor);
            }
        }
        KeyCode::Home => app.cursor = 0,
        KeyCode::End => app.cursor = app.input.len(),
        KeyCode::PageUp => app.scroll_back = app.scroll_back.saturating_add(5),
        KeyCode::PageDown => app.scroll_back = app.scroll_back.saturating_sub(5),
        KeyCode::Enter => {
            let text = std::mem::take(&mut app.input);
            app.cursor = 0;
            let text = text.trim().to_string();
            if text.is_empty() || !app.connected {
                return false;
            }
            app.seq += 1;
            let ts = now_ms();
            let hash = Message::calculate_body_hash(&text);
            let msg = Message::Chat {
                msg_id: format!("{}-{}", app.name, app.seq),
                from: app.name.clone(),
                ts,
                hash,
                body: text.clone(),
            };
            // Show our own message locally — the server no longer echoes
            // it back to us once PR #1 is merged. If echo is still active,
            // the duplicate from the server is filtered out by `from == self.name`
            // below on incoming, so we never double-print.
            app.entries.push(Entry::Chat {
                from: app.name.clone(),
                body: text,
                ts,
                own: true,
                integrity_ok: true,
            });
            app.scroll_back = 0;
            let _ = out_tx.send(msg);
        }
        _ => {}
    }
    false
}

fn prev_char_boundary(s: &str, i: usize) -> usize {
    let mut idx = i.saturating_sub(1);
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}
fn next_char_boundary(s: &str, i: usize) -> usize {
    let mut idx = i + 1;
    while idx < s.len() && !s.is_char_boundary(idx) {
        idx += 1;
    }
    idx.min(s.len())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn format_ts(ms: u64) -> String {
    use chrono::TimeZone;
    let secs = (ms / 1000) as i64;
    let dt = Local.timestamp_opt(secs, 0).single();
    match dt {
        Some(d) => d.format("%H:%M:%S").to_string(),
        None => "--:--:--".into(),
    }
}

fn color_for_name(name: &str) -> Color {
    use std::hash::{Hash, Hasher};
    let palette = [
        Color::Cyan,
        Color::LightCyan,
        Color::Magenta,
        Color::LightMagenta,
        Color::Yellow,
        Color::LightYellow,
        Color::Blue,
        Color::LightBlue,
        Color::LightGreen,
        Color::Indexed(208), // orange
        Color::Indexed(213), // pink
        Color::Indexed(141), // violet
    ];
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    palette[(h.finish() as usize) % palette.len()]
}

fn ui(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),    // message log
            Constraint::Length(1), // status bar
            Constraint::Length(3), // input box
        ])
        .split(f.area());

    render_messages(f, chunks[0], app);
    render_status(f, chunks[1], app);
    render_input(f, chunks[2], app);
}

fn render_messages(f: &mut Frame, area: Rect, app: &App) {
    let title = Line::from(vec![
        Span::styled(" 💬 ", Style::default().fg(Color::Cyan)),
        Span::styled(
            "Multi-Chat",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("  ({} online) ", app.peers.len()),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(title);

    let lines: Vec<Line> = app
        .entries
        .iter()
        .flat_map(entry_to_lines)
        .collect();

    let viewport = area.height.saturating_sub(2);
    let total = lines.len() as u16;
    let max_scroll = total.saturating_sub(viewport);
    let scroll = max_scroll.saturating_sub(app.scroll_back);

    let p = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    f.render_widget(p, area);
}

fn entry_to_lines(e: &Entry) -> Vec<Line<'static>> {
    let dim = Style::default().fg(Color::DarkGray);
    match e {
        Entry::Chat { from, body, ts, own, integrity_ok } => {
            let color = if *own { Color::Green } else { color_for_name(from) };
            let arrow = if *own { "▶" } else { " " };
            let mut spans = vec![
                Span::styled(format!("{} ", arrow), Style::default().fg(color)),
                Span::styled(format!("[{}] ", format_ts(*ts)), dim),
                Span::styled(
                    from.clone(),
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::styled("  ", dim),
            ];
            if *integrity_ok {
                spans.push(Span::raw(body.clone()));
            } else {
                spans.push(Span::styled(
                    body.clone(),
                    Style::default()
                        .fg(Color::Red)
                        .add_modifier(Modifier::CROSSED_OUT),
                ));
                spans.push(Span::styled(
                    "  ⚠ integrity failed",
                    Style::default().fg(Color::Red),
                ));
            }
            vec![Line::from(spans)]
        }
        Entry::Join { who, ts } => vec![Line::from(vec![
            Span::styled("  ", dim),
            Span::styled(format!("[{}] ", format_ts(*ts)), dim),
            Span::styled(
                format!("→ {who} joined the chat"),
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::ITALIC),
            ),
        ])],
        Entry::Leave { who, ts } => vec![Line::from(vec![
            Span::styled("  ", dim),
            Span::styled(format!("[{}] ", format_ts(*ts)), dim),
            Span::styled(
                format!("← {who} left the chat"),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::ITALIC),
            ),
        ])],
        Entry::System { body, ts } => vec![Line::from(vec![
            Span::styled("  ", dim),
            Span::styled(format!("[{}] ", format_ts(*ts)), dim),
            Span::styled(
                format!("• {body}"),
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::ITALIC),
            ),
        ])],
        Entry::Error { body, ts } => vec![Line::from(vec![
            Span::styled("  ", dim),
            Span::styled(format!("[{}] ", format_ts(*ts)), dim),
            Span::styled(
                format!("✗ {body}"),
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
        ])],
    }
}

fn render_status(f: &mut Frame, area: Rect, app: &App) {
    let dim = Style::default().fg(Color::DarkGray);
    let (dot_color, label) = if app.connected {
        (Color::Green, "connected")
    } else {
        (Color::Red, "disconnected")
    };
    let scroll_hint = if app.scroll_back > 0 {
        format!(" │ ↑ scrolled back {} (PgDn to follow)", app.scroll_back)
    } else {
        String::new()
    };
    let line = Line::from(vec![
        Span::raw(" "),
        Span::styled("●", Style::default().fg(dot_color)),
        Span::raw(" "),
        Span::styled(label.to_string(), Style::default().fg(dot_color)),
        Span::styled(format!(" │ {} ", app.addr), dim),
        Span::styled("│ as ", dim),
        Span::styled(
            app.name.clone(),
            Style::default()
                .fg(color_for_name(&app.name))
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" │ {} online", app.peers.len()),
            dim,
        ),
        Span::styled(scroll_hint, Style::default().fg(Color::Yellow)),
        Span::styled(
            "  · PgUp/PgDn scroll · Esc/Ctrl-C quit",
            Style::default().fg(Color::DarkGray).add_modifier(Modifier::DIM),
        ),
    ]);
    f.render_widget(Paragraph::new(line), area);
}

fn render_input(f: &mut Frame, area: Rect, app: &App) {
    let title = Line::from(vec![
        Span::raw(" "),
        Span::styled(
            app.name.clone(),
            Style::default()
                .fg(color_for_name(&app.name))
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
    ]);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if app.connected {
            Color::DarkGray
        } else {
            Color::Red
        }))
        .title(title);

    let prompt = Span::styled("› ", Style::default().fg(Color::Cyan));
    let line = Line::from(vec![prompt, Span::raw(app.input.clone())]);
    let p = Paragraph::new(line).block(block);
    f.render_widget(p, area);

    // Place the cursor inside the input area, accounting for the prompt
    // and the display width of the text up to the cursor.
    let prompt_width = 2u16; // "› "
    let typed_width = app.input[..app.cursor].width() as u16;
    let cursor_x = area.x + 1 + prompt_width + typed_width;
    let cursor_y = area.y + 1;
    f.set_cursor_position((cursor_x, cursor_y));
}
