// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{BepError, Result};
use crate::proto::bep::{Header, Hello, MessageCompression};

/// Maximum message size: 500 MiB (matches Syncthing's limit).
const MAX_MESSAGE_SIZE: u32 = 500 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Standard BEP Message Framing
// ---------------------------------------------------------------------------

/// A framed BEP message: header + body, already decompressed.
pub struct RawMessage {
    pub header: Header,
    pub body: Bytes,
}

/// Read a framed BEP message (header + body) from the stream.
/// Handles LZ4 decompression if the header indicates it.
pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<RawMessage> {
    // Read header (2-byte length prefix)
    let header_buf = read_header_prefixed(reader).await?;
    let header = Header::decode(header_buf.chunk())
        .map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;

    // Read message body (4-byte length prefix)
    let body_buf = read_body_prefixed(reader).await?;

    // Decompress if needed
    let body = if header.compression() == MessageCompression::Lz4 {
        decompress_lz4(&body_buf)?
    } else {
        body_buf.freeze()
    };

    Ok(RawMessage { header, body })
}

/// Write a framed BEP message (header + body) to the stream.
/// Compresses the body with LZ4 if `compress` is true.
pub async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: &Header,
    body: &[u8],
    compress: bool,
) -> Result<()> {
    // Encode header
    let mut header_to_write = *header;
    if compress {
        header_to_write.compression = MessageCompression::Lz4 as i32;
    }
    let header_bytes = header_to_write.encode_to_vec();
    write_header_prefixed(writer, &header_bytes).await?;

    // Encode body (optionally compressed)
    if compress {
        let compressed = compress_lz4(body);
        write_body_prefixed(writer, &compressed).await?;
    } else {
        write_body_prefixed(writer, body).await?;
    }

    writer
        .flush()
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    Ok(())
}

async fn read_header_prefixed<R: AsyncRead + Unpin>(reader: &mut R) -> Result<BytesMut> {
    let len = reader
        .read_u16()
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))? as u32;
    if len > MAX_MESSAGE_SIZE {
        return Err(BepError::PeerBadMessage(format!(
            "message too large: {len} bytes (max {MAX_MESSAGE_SIZE})"
        )));
    }
    let mut buf = BytesMut::zeroed(len as usize);
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    Ok(buf)
}

async fn read_body_prefixed<R: AsyncRead + Unpin>(reader: &mut R) -> Result<BytesMut> {
    let len = reader
        .read_u32()
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    if len > MAX_MESSAGE_SIZE {
        return Err(BepError::PeerBadMessage(format!(
            "message too large: {len} bytes (max {MAX_MESSAGE_SIZE})"
        )));
    }
    let mut buf = BytesMut::zeroed(len as usize);
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    Ok(buf)
}

async fn write_header_prefixed<W: AsyncWrite + Unpin>(writer: &mut W, data: &[u8]) -> Result<()> {
    writer
        .write_u16(
            u16::try_from(data.len())
                .map_err(|_| BepError::Internal("header data too large for u16".into()))?,
        )
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    writer
        .write_all(data)
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    Ok(())
}

async fn write_body_prefixed<W: AsyncWrite + Unpin>(writer: &mut W, data: &[u8]) -> Result<()> {
    writer
        .write_u32(
            u32::try_from(data.len())
                .map_err(|_| BepError::Internal("body data too large for u32".into()))?,
        )
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    writer
        .write_all(data)
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    Ok(())
}

fn decompress_lz4(data: &[u8]) -> Result<Bytes> {
    if data.len() < 4 {
        return Err(BepError::PeerBadMessage(
            "LZ4 data too short for length prefix".into(),
        ));
    }
    let mut cursor = data;
    let uncompressed_size = cursor.get_u32() as usize;
    if uncompressed_size > MAX_MESSAGE_SIZE as usize {
        return Err(BepError::PeerBadMessage(format!(
            "claimed uncompressed size {uncompressed_size} exceeds maximum {MAX_MESSAGE_SIZE}"
        )));
    }
    let compressed = &data[4..];
    let decompressed = lz4_flex::decompress(compressed, uncompressed_size)
        .map_err(|e| BepError::PeerBadMessage(e.to_string()))?;
    if decompressed.len() != uncompressed_size {
        return Err(BepError::PeerBadMessage(format!(
            "decompressed size {} does not match claimed {}",
            decompressed.len(),
            uncompressed_size
        )));
    }
    Ok(Bytes::from(decompressed))
}

fn compress_lz4(data: &[u8]) -> Vec<u8> {
    let compressed = lz4_flex::compress(data);
    let mut out = Vec::with_capacity(4 + compressed.len());
    out.put_u32(u32::try_from(data.len()).expect("data too large for u32 prefix"));
    out.extend_from_slice(&compressed);
    out
}

// ---------------------------------------------------------------------------
// Hello Message Framing
// ---------------------------------------------------------------------------

/// BEP Hello magic number (0x2EA7D90B).
const HELLO_MAGIC: u32 = 0x2EA7_D90B;

/// Maximum Hello message size (32 KiB — way more than needed, but safe).
const MAX_HELLO_SIZE: u16 = 32 * 1024;

/// Our client name.
pub const CLIENT_NAME: &str = "bepository";

/// Our client version.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Send a Hello message.
///
/// Format: [4 bytes magic][2 bytes length][Hello protobuf]
pub async fn send_hello<W: AsyncWrite + Unpin>(writer: &mut W, device_name: &str) -> Result<()> {
    let timestamp_duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| BepError::Internal(format!("failed to get system time: {e}")))?;
    let timestamp = timestamp_duration
        .as_secs()
        .try_into()
        .map_err(|e| BepError::Internal(format!("timestamp exceeds i64::MAX: {e}")))?;

    let hello = Hello {
        device_name: device_name.to_string(),
        client_name: CLIENT_NAME.to_string(),
        client_version: CLIENT_VERSION.to_string(),
        num_connections: 1,
        timestamp,
    };

    let encoded = hello.encode_to_vec();
    let mut buf = Vec::with_capacity(4 + 2 + encoded.len());
    buf.put_u32(HELLO_MAGIC);
    buf.put_u16(u16::try_from(encoded.len()).expect("hello message too large"));
    buf.extend_from_slice(&encoded);

    writer
        .write_all(&buf)
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    Ok(())
}

/// Receive and validate a Hello message.
///
/// Returns the peer's Hello on success.
pub async fn recv_hello<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Hello> {
    let magic = reader
        .read_u32()
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    if magic != HELLO_MAGIC {
        return Err(BepError::PeerBadHello(format!(
            "expected magic 0x{HELLO_MAGIC:08X}, got 0x{magic:08X}"
        )));
    }

    let len = reader
        .read_u16()
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;
    if len > MAX_HELLO_SIZE {
        return Err(BepError::PeerBadHello(format!(
            "hello message too large: {len} bytes"
        )));
    }

    let mut buf = vec![0u8; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| BepError::NetworkError(e.to_string()))?;

    let hello = Hello::decode(&buf[..])
        .map_err(|e| BepError::PeerBadMessage(format!("protobuf decode error: {e}")))?;

    if hello.client_name.is_empty() {
        return Err(BepError::PeerBadHello("peer sent empty client_name".into()));
    }

    if hello.device_name.is_empty() {
        return Err(BepError::PeerBadHello("peer sent empty device_name".into()));
    }

    Ok(hello)
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    #[test]
    fn lz4_round_trip() {
        let original = b"hello world, this is a test of LZ4 compression in BEP";
        let compressed = compress_lz4(original);
        let decompressed = decompress_lz4(&compressed).unwrap();
        assert_eq!(&decompressed[..], &original[..]);
    }

    #[tokio::test]
    async fn message_round_trip() {
        use crate::proto::bep::MessageType;

        let header = Header {
            r#type: MessageType::Ping as i32,
            compression: MessageCompression::None as i32,
        };
        let body = b"test body";

        // Write
        let mut buf = Vec::new();
        write_message(&mut buf, &header, body, false).await.unwrap();

        // Read
        let mut cursor = &buf[..];
        let msg = read_message(&mut cursor).await.unwrap();

        assert_eq!(msg.header.r#type, MessageType::Ping as i32);
        assert_eq!(&msg.body[..], b"test body");
    }

    #[test]
    fn lz4_empty() {
        let compressed = compress_lz4(b"");
        let decompressed = decompress_lz4(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[tokio::test]
    async fn message_round_trip_compressed() {
        use crate::proto::bep::MessageType;

        let header = Header {
            r#type: MessageType::Index as i32,
            compression: MessageCompression::None as i32,
        };
        let body = b"this body will be compressed with lz4";

        let mut buf = Vec::new();
        write_message(&mut buf, &header, body, true).await.unwrap();

        let mut cursor = &buf[..];
        let msg = read_message(&mut cursor).await.unwrap();

        assert_eq!(msg.header.r#type, MessageType::Index as i32);
        assert_eq!(&msg.body[..], &body[..]);
    }

    #[tokio::test]
    async fn hello_round_trip() {
        let mut buf = Vec::new();
        send_hello(&mut buf, "test-device").await.unwrap();

        let mut cursor = &buf[..];
        let hello = recv_hello(&mut cursor).await.unwrap();

        assert_eq!(hello.device_name, "test-device");
        assert_eq!(hello.client_name, CLIENT_NAME);
        assert_eq!(hello.client_version, CLIENT_VERSION);
    }

    #[tokio::test]
    async fn bad_magic() {
        let buf = [0x00, 0x00, 0x00, 0x01, 0x00, 0x00];
        let mut cursor = &buf[..];
        let err = recv_hello(&mut cursor).await.unwrap_err();
        assert!(matches!(err, BepError::PeerBadHello(_)));
    }

    #[tokio::test]
    async fn empty_client_name_rejected() {
        let hello = Hello {
            device_name: "test".into(),
            client_name: "".into(),
            client_version: "1.0".into(),
            ..Default::default()
        };
        let encoded = hello.encode_to_vec();
        let mut buf = Vec::new();
        buf.put_u32(HELLO_MAGIC);
        buf.put_u16(encoded.len() as u16);
        buf.extend_from_slice(&encoded);

        let mut cursor = &buf[..];
        let err = recv_hello(&mut cursor).await.unwrap_err();
        assert!(matches!(err, BepError::PeerBadHello(_)));
    }

    #[tokio::test]
    async fn empty_device_name_rejected() {
        let hello = Hello {
            device_name: "".into(),
            client_name: "syncthing".into(),
            client_version: "1.0".into(),
            ..Default::default()
        };
        let encoded = hello.encode_to_vec();
        let mut buf = Vec::new();
        buf.put_u32(HELLO_MAGIC);
        buf.put_u16(encoded.len() as u16);
        buf.extend_from_slice(&encoded);

        let mut cursor = &buf[..];
        let err = recv_hello(&mut cursor).await.unwrap_err();
        assert!(matches!(err, BepError::PeerBadHello(_)));
    }

    #[test]
    fn test_decompress_lz4_mismatched_size() {
        let original = b"hello world world world world world";
        let compressed = lz4_flex::compress(original);

        // Correct case
        let mut data = Vec::new();
        data.put_u32(original.len() as u32);
        data.extend_from_slice(&compressed);
        let decompressed = decompress_lz4(&data).unwrap();
        assert_eq!(decompressed, original[..]);

        // Claimed size too small
        let mut data_too_small = Vec::new();
        data_too_small.put_u32((original.len() - 1) as u32);
        data_too_small.extend_from_slice(&compressed);
        let result = decompress_lz4(&data_too_small);
        assert!(
            result.is_err(),
            "Should fail when claimed size is too small"
        );

        // Claimed size too large
        let mut data_too_large = Vec::new();
        data_too_large.put_u32((original.len() + 1) as u32);
        data_too_large.extend_from_slice(&compressed);
        let result = decompress_lz4(&data_too_large);
        assert!(
            result.is_err(),
            "Should fail when claimed size is too large"
        );
    }

    #[test]
    fn test_max_message_size_enforced() {
        let mut data = Vec::new();
        data.put_u32(MAX_MESSAGE_SIZE + 1);
        data.extend_from_slice(&[0, 0, 0, 0]);
        let result = decompress_lz4(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }
}
