use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    if len > 10 * 1024 * 1024 {
        // 10MB limit to prevent DoS
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn body_hash_matches_blake3_hex() {
        let body = "hello multi-chat";
        let expected = blake3::hash(body.as_bytes()).to_hex().to_string();

        assert_eq!(Message::calculate_body_hash(body), expected);
    }

    #[tokio::test]
    async fn frame_round_trips_chat_message() {
        let message = Message::Chat {
            msg_id: "client-1-1".to_string(),
            from: "client-1".to_string(),
            ts: 1_737_270_000_123,
            hash: Message::calculate_body_hash("hello"),
            body: "hello".to_string(),
        };
        let (mut writer, mut reader) = tokio::io::duplex(1024);

        write_frame(&mut writer, &message).await.unwrap();
        drop(writer);

        let decoded = read_frame(&mut reader).await.unwrap();
        assert_eq!(decoded, message);
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected_before_payload_read() {
        let too_large = 10 * 1024 * 1024 + 1u32;
        let (mut writer, mut reader) = tokio::io::duplex(4);

        writer.write_all(&too_large.to_be_bytes()).await.unwrap();
        drop(writer);

        let err = read_frame(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("Frame too large"));
    }
}
