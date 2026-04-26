use anyhow::{Result, Context};
use clap::{Parser};
use protocol::{Message, read_frame, write_frame};
use tokio::net::TcpStream;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use tracing_subscriber;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(author, version, about, bin_name = "multi-chat-client")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:9000")]
    addr: String,
    #[arg(short, long)]
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let addr = args.addr;
    let name = args.name;

    info!("Connecting to server at {} as {}", addr, name);
    let mut stream = TcpStream::connect(&addr).await.context("Failed to connect to server")?;

    // 1. Send Join message
    let join_msg = Message::Join { client_id: name.clone() };
    write_frame(&mut stream, &join_msg).await.context("Failed to send join message")?;
    info!("Joined chat as {}", name);

    let (reader, writer) = stream.into_split();
    let mut reader = reader;
    let mut writer = writer;

    // Sequence tracking for verification
    let mut peer_sequences: HashMap<String, u64> = HashMap::new();

    // Task for receiving messages
    let rx_task = tokio::spawn(async move {
        loop {
            match read_frame(&mut reader).await {
                Ok(msg) => {
                    match &msg {
                        Message::Chat { from, body, hash, msg_id, .. } => {
                            // Verify Integrity (Hash)
                            let computed_hash = Message::calculate_body_hash(body);
                            if computed_hash != *hash {
                                error!("Integrity check failed for message {} from {}: computed {}, got {}", msg_id, from, computed_hash, hash);
                            } else {
                                println!("\n[{}] {}: {}", msg_id, from, body);
                                print!("> "); // Reprint prompt
                            }
                        }
                        Message::Join { client_id } => {
                            println!("\n*** {} joined the chat ***", client_id);
                            print!("> ");
                        }
                        Message::Leave { client_id } => {
                            println!("\n*** {} left the chat ***", client_id);
                            print!("> ");
                        }
                        Message::Sys { body } => {
                            println!("\n[SYSTEM] {}", body);
                            print!("> ");
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    error!("Error reading from server: {:?}", e);
                    break;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    // Task for sending messages
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut line = String::new();
    let mut seq = 0u64;

    println!("Welcome to Multi-Chat! Type your message and press Enter.");
    println!("(Use Ctrl+C to exit)");
    print!("> ");
    std::io::Write::flush(&mut std::io::stdout()).unwrap();

    loop {
        line.clear();
        match stdin.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {
                let text = line.trim();
                if text.is_empty() {
                    continue;
                }

                seq += 1;
                let msg_id = format!("{}-{}", name, seq);
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                let hash = Message::calculate_body_hash(text);

                let chat_msg = Message::Chat {
                    msg_id,
                    from: name.clone(),
                    ts,
                    hash,
                    body: text.to_string(),
                };

                if let Err(e) = write_frame(&mut writer, &chat_msg).await {
                    error!("Failed to send message: {:?}", e);
                    break;
                }

                line.clear();
            }
            Err(e) => {
                error!("Stdin read error: {:?}", e);
                break;
            }
        }
    }

    // Send Leave message upon exit
    let leave_msg = Message::Leave { client_id: name.clone() };
    let _ = write_frame(&mut writer, &leave_msg).await;

    rx_task.abort();
    Ok(())
}
