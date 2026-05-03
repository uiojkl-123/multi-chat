use anyhow::{Result, Context};
use clap::{Parser};
use protocol::{Message, read_frame, write_frame};
use tokio::net::TcpStream;
use tokio::time::{interval, Duration, MissedTickBehavior};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tracing::{info, error};
use tracing_subscriber;
use std::time::{SystemTime, UNIX_EPOCH};

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
}

struct ClientStats {
    sent: u64,
    received: u64,
    errors: u64,
    latencies: Vec<u128>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
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

    // Each sent message is expected to reach (total_clients - 1) peers,
    // since the sender doesn't receive its own message back.
    if total_sent > 0 && total_clients >= 2 {
        let expected_received = total_sent * (total_clients as u64 - 1);
        let loss_rate = (1.0 - (total_received as f64 / expected_received as f64)) * 100.0;
        println!("Loss Rate: {:.2}% (received {} / expected {})", loss_rate, total_received, expected_received);
    } else {
        println!("Loss Rate: N/A");
    }
    println!("Throughput: Check server logs (AtomicU64 sampling)");
    println!("------------------------\n");
}
