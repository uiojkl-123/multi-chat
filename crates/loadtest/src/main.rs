use anyhow::{Result, Context};
use clap::{Parser};
use protocol::{Message, read_frame, write_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tracing::{info, error, warn};
use tracing_subscriber;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(author, version, about, bin_name = "multi-chat-loadtest")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:9000")]
    addr: String,
    #[arg(short, long, default_value = "500")]
    clients: usize,
    #[arg(short, long, default_value = "1")]
    rate: u64, // messages per second per client
    #[arg(short, long, default_value = "60")]
    duration: u64, // seconds
}

struct ClientStats {
    sent: u64,
    received: u64,
    lost: u64,
    errors: u64,
    latencies: Vec<u128>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let addr = args.addr.clone();
    let clients_count = args.clients;
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
    report_results(&stats, clients_count);

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
        lost: 0,
        errors: 0,
        latencies: Vec::new(),
    }));

    // Receiver task
    let stats_recv = stats_local.clone();
    let rx_task = tokio::spawn(async move {
        loop {
            match read_frame(&mut reader).await {
                Ok(msg) => {
                    match &msg {
                        Message::Chat { body, hash, ts, .. } => {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                            let latency = now.saturating_sub(*ts as u128);

                            let computed_hash = protocol::Message::calculate_body_hash(body);
                            if computed_hash == *hash {
                                let mut s = stats_recv.lock().unwrap();
                                s.received += 1;
                                s.latencies.push(latency);
                            } else {
                                let mut s = stats_recv.lock().unwrap();
                                s.errors += 1;
                            }
                        }
                        _ => {}
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Sender loop
    let start_time = SystemTime::now();
    let mut seq = 0u64;
    let interval = Duration::from_millis(1000 / rate);

    while start_time.elapsed().unwrap().as_secs() < duration {
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

        if let Err(_) = write_frame(&mut writer, &chat_msg).await {
            break;
        }

        {
            let mut s = stats_local.lock().unwrap();
            s.sent += 1;
        }

        sleep(interval).await;
    }

    // Final stats merge
    let mut global_stats = stats.lock().unwrap();
    let local = stats_local.lock().unwrap();
    global_stats.insert(id, ClientStats {
        sent: local.sent,
        received: local.received,
        lost: local.lost,
        errors: local.errors,
        latencies: local.latencies.clone(),
    });

    rx_task.abort();
    Ok(())
}

fn report_results(stats_map: &Arc<Mutex<HashMap<usize, ClientStats>>>, total_clients: usize) {
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

    println!("\n--- Load Test Results ---");
    println!("Total Clients: {}", total_clients);
    println!("Total Messages Sent: {}", total_sent);
    println!("Total Messages Received: {}", total_received);
    println!("Total Integrity Errors: {}", total_errors);

    if !all_latencies.is_empty() {
        println!("Latency (ms): P50: {}, P95: {}, P99: {}", p50, p95, p99);
    } else {
        println!("Latency: N/A");
    }

    if total_sent > 0 {
        // Loss Rate = (Total Sent - Avg Received per Client) / Total Sent
        let avg_received = total_received as f64 / (total_clients as f64 - 1.0);
        let loss_rate = (1.0 - (avg_received / total_sent as f64)) * 100.0;
        println!("Loss Rate: {:.2}%", loss_rate);
    } else {
        println!("Loss Rate: N/A");
    }
    println!("Throughput: Check server logs (AtomicU64 sampling)");
    println!("------------------------\n");
}
