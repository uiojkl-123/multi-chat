use anyhow::{Result, Context};
use clap::{Parser};
use protocol::{Message, read_frame};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tracing::{info, error, warn};
use tracing_subscriber;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use bytes::Bytes;

#[derive(Parser, Debug)]
#[command(author, version, about, bin_name = "multi-chat-server")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:9000")]
    addr: String,
}

// Pre-serialized frame paired with the originating connection id, so the
// writer task can both skip echoing back to the sender and avoid re-encoding.
type BroadcastFrame = (u64, Arc<Bytes>);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let addr = args.addr;

    info!("Starting Multi-Chat Server on {}", addr);

    // Broadcast channel buffer. Sized for the worst-case fan-out under §5-2/S3
    // (500 clients × 10 msg/s): smaller capacities forced Lagged disconnects
    // and pushed loss rate near 40% in baseline measurements.
    let (tx, _rx) = broadcast::channel::<BroadcastFrame>(8192);
    let tx = Arc::new(tx);

    // Per-connection identifier used to filter the sender out of the broadcast.
    let next_conn_id = Arc::new(AtomicU64::new(0));

    // Throughput monitoring: Atomic counter and sampling task
    let msg_counter = Arc::new(AtomicU64::new(0));
    let counter_clone = msg_counter.clone();
    tokio::spawn(async move {
        let mut last_count = 0u64;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let current_count = counter_clone.load(Ordering::Relaxed);
            let throughput = current_count.saturating_sub(last_count);
            info!("Current Throughput: {} msg/s", throughput);
            last_count = current_count;
        }
    });

    let listener = TcpListener::bind(&addr).await.context("Failed to bind TCP listener")?;
    info!("Listening on {}", addr);

    loop {
        let (socket, peer_addr) = listener.accept().await.context("Failed to accept connection")?;
        // Disable Nagle: per-message latency matters more than coalescing for chat traffic.
        if let Err(e) = socket.set_nodelay(true) {
            warn!("set_nodelay failed for {}: {:?}", peer_addr, e);
        }
        let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed);
        info!("New connection from {} (conn_id={})", peer_addr, conn_id);

        let tx = tx.clone();
        let msg_counter = msg_counter.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, conn_id, tx, msg_counter).await {
                error!("Error handling connection from {}: {:?}", peer_addr, e);
            }
        });
    }
}

fn encode_frame(msg: &Message) -> Result<Arc<Bytes>> {
    let payload = serde_json::to_vec(msg)?;
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(Arc::new(Bytes::from(frame)))
}

async fn handle_connection(
    socket: TcpStream,
    conn_id: u64,
    tx: Arc<broadcast::Sender<BroadcastFrame>>,
    msg_counter: Arc<AtomicU64>,
) -> Result<()> {
    let (reader, writer) = socket.into_split();

    // --- Writer Task: Broadcast Channel -> Socket ---
    let mut rx = tx.subscribe();
    let writer_handle = tokio::spawn(async move {
        let mut writer = writer;
        loop {
            match rx.recv().await {
                Ok((sender_conn_id, bytes)) => {
                    // Skip messages that originated from this same connection so
                    // each client only receives messages from its peers. This
                    // matches the verification policy in README 4-2.
                    if sender_conn_id == conn_id {
                        continue;
                    }
                    // Write the pre-serialized frame: [4 bytes length][JSON payload]
                    if let Err(e) = writer.write_all(&bytes).await {
                        warn!("Failed to write frame to client: {:?}", e);
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Client lagged by {} messages. Disconnecting to maintain server health.", n);
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    // --- Reader Task: Socket -> Broadcast Channel ---
    let mut reader = reader;
    let mut client_id = None;

    loop {
        match read_frame(&mut reader).await {
            Ok(msg) => {
                match &msg {
                    Message::Join { client_id: id } => {
                        info!("Client {} joined the chat", id);
                        client_id = Some(id.clone());
                        let frame = encode_frame(&msg)?;
                        let _ = tx.send((conn_id, frame));
                        msg_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Message::Leave { client_id: id } => {
                        info!("Client {} left the chat", id);
                        let frame = encode_frame(&msg)?;
                        let _ = tx.send((conn_id, frame));
                        msg_counter.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    _ => {
                        if client_id.is_none() {
                            warn!("Received message from unregistered client. Dropping.");
                            continue;
                        }
                        let frame = encode_frame(&msg)?;
                        let _ = tx.send((conn_id, frame));
                        msg_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(e) => {
                warn!("Connection closed or error reading frame: {:?}", e);
                break;
            }
        }
    }

    if let Some(id) = client_id {
        info!("Client {} disconnected unexpectedly", id);
        let leave_msg = Message::Leave { client_id: id };
        let frame = encode_frame(&leave_msg)?;
        let _ = tx.send((conn_id, frame));
    }

    writer_handle.abort();

    Ok(())
}
