use anyhow::{Result, Context};
use clap::{Parser};
use protocol::{Message, read_frame, write_frame};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{info, error, warn};
use tracing_subscriber;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, bin_name = "multi-chat-server")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:9000")]
    addr: String,
}

// Broadcast payload pairs each message with the connection id of the sender,
// so the writer task can skip echoing the message back to its origin.
type BroadcastMessage = (u64, Message);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let addr = args.addr;

    info!("Starting Multi-Chat Server on {}", addr);

    // Broadcast channel: Buffer size 1024 to handle spikes.
    let (tx, _rx) = broadcast::channel::<BroadcastMessage>(1024);
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

async fn handle_connection(
    socket: TcpStream,
    conn_id: u64,
    tx: Arc<broadcast::Sender<BroadcastMessage>>,
    msg_counter: Arc<AtomicU64>,
) -> Result<()> {
    let (reader, writer) = socket.into_split();

    // --- Writer Task: Broadcast Channel -> Socket ---
    let mut rx = tx.subscribe();
    let writer_handle = tokio::spawn(async move {
        let mut writer = writer;
        loop {
            match rx.recv().await {
                Ok((sender_conn_id, msg)) => {
                    // Skip messages that originated from this same connection so
                    // each client only receives messages from its peers. This
                    // matches the verification policy in README 4-2.
                    if sender_conn_id == conn_id {
                        continue;
                    }
                    if let Err(e) = write_frame(&mut writer, &msg).await {
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
                        // Broadcast the join event to all other clients
                        let _ = tx.send((conn_id, msg));
                        msg_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Message::Leave { client_id: id } => {
                        info!("Client {} left the chat", id);
                        let _ = tx.send((conn_id, msg));
                        msg_counter.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    _ => {
                        // For Chat, Ack, Sys messages, simply broadcast them
                        if client_id.is_none() {
                            warn!("Received message from unregistered client. Dropping.");
                            continue;
                        }
                        let _ = tx.send((conn_id, msg));
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

    // Cleanup: if the client disconnected without a Leave message
    if let Some(id) = client_id {
        info!("Client {} disconnected unexpectedly", id);
        let _ = tx.send((conn_id, Message::Leave { client_id: id }));
    }

    // Stop the writer task by aborting it
    writer_handle.abort();

    Ok(())
}
