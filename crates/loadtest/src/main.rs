use anyhow::{Result, Context};
use clap::{Parser, ValueEnum};
use protocol::{Message, read_frame, write_frame};
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::time::{interval, Duration, MissedTickBehavior};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tracing::{info, error};
use tracing_subscriber;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Md,
}

#[derive(Parser, Debug)]
#[command(author, version, about, bin_name = "multi-chat-loadtest")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:9000")]
    addr: String,
    /// Number of concurrent clients. Must be >= 2 (loss rate is undefined for a single client).
    #[arg(short, long, default_value_t = 500, value_parser = clap::value_parser!(u32).range(2..))]
    clients: u32,
    /// Messages per second per client. Must be >= 1.
    #[arg(short, long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    rate: u64,
    /// Test duration in seconds.
    #[arg(short, long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    duration: u64,
    /// Output format for the final report.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    output: OutputFormat,
    /// Optional scenario label (e.g. "S2", "baseline-S2") embedded in machine-readable output.
    #[arg(long, default_value = "")]
    label: String,
}

struct ClientStats {
    sent: u64,
    received: u64,
    errors: u64,
    latencies: Vec<u128>,
}

#[derive(Serialize)]
struct ReportSummary {
    label: String,
    clients: u32,
    rate: u64,
    duration: u64,
    total_sent: u64,
    total_received: u64,
    expected_received: u64,
    integrity_errors: u64,
    loss_rate_pct: f64,
    latency_p50_ms: u128,
    latency_p95_ms: u128,
    latency_p99_ms: u128,
    sample_count: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Send tracing logs to stderr so callers can capture the JSON/Markdown
    // summary on stdout via `> file.json` without polluting it with log lines.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    let addr = args.addr.clone();
    let clients_count = args.clients as usize;
    let rate = args.rate;
    let duration = args.duration;

    info!("Starting load test with {} clients, rate {} msg/s, duration {}s", clients_count, rate, duration);

    let stats = Arc::new(Mutex::new(HashMap::<usize, ClientStats>::new()));
    let mut client_handles = Vec::new();

    for i in 0..clients_count {
        let addr = addr.clone();
        let stats = stats.clone();
        let client_id = format!("load-client-{}", i);

        let handle = tokio::spawn(async move {
            if let Err(e) = run_client(i, &addr, &client_id, rate, duration, &stats).await {
                error!("Client {} failed: {:?}", i, e);
            }
        });
        client_handles.push(handle);
    }

    // Wait for all clients to finish
    for handle in client_handles {
        let _ = handle.await;
    }

    // Report results
    report_results(&stats, &args);

    Ok(())
}

async fn run_client(
    id: usize,
    addr: &str,
    client_id: &str,
    rate: u64,
    duration: u64,
    stats: &Arc<Mutex<HashMap<usize, ClientStats>>>,
) -> Result<()> {
    let mut stream = TcpStream::connect(addr).await.context("Connection failed")?;

    // 1. Join
    let join_msg = Message::Join { client_id: client_id.to_string() };
    write_frame(&mut stream, &join_msg).await?;

    let (reader, writer) = stream.into_split();
    let mut reader = reader;
    let mut writer = writer;

    let stats_local = Arc::new(Mutex::new(ClientStats {
        sent: 0,
        received: 0,
        errors: 0,
        latencies: Vec::new(),
    }));

    // Receiver task
    let stats_recv = stats_local.clone();
    let rx_task = tokio::spawn(async move {
        loop {
            match read_frame(&mut reader).await {
                Ok(msg) => {
                    if let Message::Chat { body, hash, ts, .. } = &msg {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                        let latency = now.saturating_sub(*ts as u128);

                        let computed_hash = protocol::Message::calculate_body_hash(body);
                        let mut s = stats_recv.lock().unwrap();
                        if computed_hash == *hash {
                            s.received += 1;
                            s.latencies.push(latency);
                        } else {
                            s.errors += 1;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Sender loop. Use a tick interval rather than `sleep(interval)` so that
    // per-message work doesn't accumulate into rate drift.
    let start = tokio::time::Instant::now();
    let total = Duration::from_secs(duration);
    let mut ticker = interval(Duration::from_millis(1000 / rate));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut seq = 0u64;
    while start.elapsed() < total {
        ticker.tick().await;
        if start.elapsed() >= total {
            break;
        }

        seq += 1;
        let msg_id = format!("{}-{}", client_id, seq);
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let body = format!("Load test message {} from {}", seq, client_id);
        let hash = protocol::Message::calculate_body_hash(&body);

        let chat_msg = Message::Chat {
            msg_id,
            from: client_id.to_string(),
            ts,
            hash,
            body,
        };

        if write_frame(&mut writer, &chat_msg).await.is_err() {
            break;
        }

        stats_local.lock().unwrap().sent += 1;
    }

    // Notify the server we're leaving so it can broadcast the Leave event,
    // then give the receiver a brief grace period to drain in-flight frames
    // before tearing the task down.
    let _ = write_frame(&mut writer, &Message::Leave { client_id: client_id.to_string() }).await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    rx_task.abort();

    // Final stats merge
    let local = stats_local.lock().unwrap();
    let mut global_stats = stats.lock().unwrap();
    global_stats.insert(id, ClientStats {
        sent: local.sent,
        received: local.received,
        errors: local.errors,
        latencies: local.latencies.clone(),
    });

    Ok(())
}

fn build_summary(stats_map: &Arc<Mutex<HashMap<usize, ClientStats>>>, args: &Args) -> ReportSummary {
    let stats = stats_map.lock().unwrap();
    let mut total_sent = 0u64;
    let mut total_received = 0u64;
    let mut total_errors = 0u64;
    let mut all_latencies = Vec::new();

    for s in stats.values() {
        total_sent += s.sent;
        total_received += s.received;
        total_errors += s.errors;
        all_latencies.extend(s.latencies.clone());
    }

    all_latencies.sort_unstable();

    let p50 = all_latencies.get(all_latencies.len() / 2).cloned().unwrap_or(0);
    let p95 = all_latencies.get((all_latencies.len() as f64 * 0.95) as usize).cloned().unwrap_or(0);
    let p99 = all_latencies.get((all_latencies.len() as f64 * 0.99) as usize).cloned().unwrap_or(0);

    let expected_received = if args.clients >= 2 {
        total_sent * (args.clients as u64 - 1)
    } else {
        0
    };
    let loss_rate_pct = if expected_received > 0 {
        (1.0 - (total_received as f64 / expected_received as f64)) * 100.0
    } else {
        0.0
    };

    ReportSummary {
        label: args.label.clone(),
        clients: args.clients,
        rate: args.rate,
        duration: args.duration,
        total_sent,
        total_received,
        expected_received,
        integrity_errors: total_errors,
        loss_rate_pct,
        latency_p50_ms: p50,
        latency_p95_ms: p95,
        latency_p99_ms: p99,
        sample_count: all_latencies.len(),
    }
}

fn report_results(stats_map: &Arc<Mutex<HashMap<usize, ClientStats>>>, args: &Args) {
    let summary = build_summary(stats_map, args);
    match args.output {
        OutputFormat::Text => print_text(&summary),
        OutputFormat::Json => print_json(&summary),
        OutputFormat::Md => print_markdown(&summary),
    }
}

fn print_text(s: &ReportSummary) {
    println!("\n--- Load Test Results ---");
    if !s.label.is_empty() {
        println!("Label: {}", s.label);
    }
    println!("Total Clients: {}", s.clients);
    println!("Total Messages Sent: {}", s.total_sent);
    println!("Total Messages Received: {}", s.total_received);
    println!("Total Integrity Errors: {}", s.integrity_errors);

    if s.sample_count > 0 {
        println!(
            "Latency (ms): P50: {}, P95: {}, P99: {}",
            s.latency_p50_ms, s.latency_p95_ms, s.latency_p99_ms
        );
    } else {
        println!("Latency: N/A");
    }

    if s.expected_received > 0 {
        println!(
            "Loss Rate: {:.2}% (received {} / expected {})",
            s.loss_rate_pct, s.total_received, s.expected_received
        );
    } else {
        println!("Loss Rate: N/A");
    }
    println!("Throughput: Check server logs (AtomicU64 sampling)");
    println!("------------------------\n");
}

fn print_json(s: &ReportSummary) {
    // stdout-only JSON so callers can capture with `> file.json`.
    match serde_json::to_string_pretty(s) {
        Ok(json) => println!("{}", json),
        Err(e) => eprintln!("failed to serialize summary: {}", e),
    }
}

fn print_markdown(s: &ReportSummary) {
    let label = if s.label.is_empty() { "-" } else { &s.label };
    println!("| Label | Clients | Rate (msg/s) | Duration (s) | Sent | Received | Loss (%) | P50 (ms) | P95 (ms) | P99 (ms) | Errors |");
    println!("|-------|---------|--------------|--------------|------|----------|----------|----------|----------|----------|--------|");
    println!(
        "| {} | {} | {} | {} | {} | {} | {:.2} | {} | {} | {} | {} |",
        label,
        s.clients,
        s.rate,
        s.duration,
        s.total_sent,
        s.total_received,
        s.loss_rate_pct,
        s.latency_p50_ms,
        s.latency_p95_ms,
        s.latency_p99_ms,
        s.integrity_errors,
    );
}
