use serde::{Deserialize, Serialize};
use bytes::{BytesMut, Buf, BufMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io;
use anyhow::{Result, anyhow};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum Message {
    Join {
        client_id: String,
    },
    Leave {
        client_id: String,
    },
    Chat {
        msg_id: String,
        from: String,
        ts: u64,
        hash: String,
        body: String,
    },
    Ack {
        msg_id: String,
    },
    Sys {
        body: String,
    },
}

impl Message {
    /// Calculate BLAKE3 hash of the message body for integrity verification.
    pub fn calculate_body_hash(body: &str) -> String {
        let hash = blake3::hash(body.as_bytes());
        hash.to_hex().to_string()
    }
}

/// Helper for length-prefixed framing: [4 bytes length (BE)][JSON payload]
pub async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Message> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 10 * 1024 * 1024 { // 10MB limit to prevent DoS
        return Err(anyhow!("Frame too large: {} bytes", len));
    }

    let mut body_buf = vec![0u8; len];
    reader.read_exact(&mut body_buf).await?;

    let msg: Message = serde_json::from_slice(&body_buf)?;
    Ok(msg)
}

pub async fn write_frame<W: AsyncWriteExt + Unpin>(writer: &mut W, msg: &Message) -> Result<()> {
    let payload = serde_json::to_vec(msg)?;
    let len = payload.len() as u32;

    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;

    Ok(())
}
