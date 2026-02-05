use crate::LWP_HEADER_SIZE;
use crate::protocol::{ControlCommand, IngestHeader, LwpFlags, LwpHeader};
use bytes::Bytes;
use lnc_core::{Result, crc32c};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Ingest,
    Ack,
    Backpressure,
    Keepalive,
    Control(ControlCommand),
    Unknown,
}

pub struct Frame {
    pub header: LwpHeader,
    pub payload: Option<Bytes>,
    pub frame_type: FrameType,
}

impl Frame {
    pub fn new_ingest(batch_id: u64, timestamp_ns: u64, record_count: u32, payload: Bytes) -> Self {
        Self::new_ingest_with_topic(batch_id, timestamp_ns, record_count, payload, 0)
    }

    pub fn new_ingest_with_topic(
        batch_id: u64,
        timestamp_ns: u64,
        record_count: u32,
        payload: Bytes,
        topic_id: u32,
    ) -> Self {
        let payload_crc = crc32c(&payload);
        let ingest = IngestHeader::with_topic(
            batch_id,
            timestamp_ns,
            record_count,
            payload.len() as u32,
            payload_crc,
            topic_id,
        );
        let header = LwpHeader::new(LwpFlags::BatchMode as u8, ingest);

        Self {
            header,
            payload: Some(payload),
            frame_type: FrameType::Ingest,
        }
    }

    pub fn new_ack(batch_id: u64) -> Self {
        let ingest = IngestHeader {
            batch_id,
            ..Default::default()
        };
        let header = LwpHeader::new(LwpFlags::Ack as u8, ingest);

        Self {
            header,
            payload: None,
            frame_type: FrameType::Ack,
        }
    }

    pub fn new_keepalive() -> Self {
        let header = LwpHeader::new(LwpFlags::Keepalive as u8, IngestHeader::default());

        Self {
            header,
            payload: None,
            frame_type: FrameType::Keepalive,
        }
    }

    pub fn new_backpressure() -> Self {
        let header = LwpHeader::new(LwpFlags::Backpressure as u8, IngestHeader::default());

        Self {
            header,
            payload: None,
            frame_type: FrameType::Backpressure,
        }
    }

    pub fn new_control(command: ControlCommand, payload: Option<Bytes>) -> Self {
        let (payload_len, payload_crc) = match &payload {
            Some(p) => (p.len() as u32, crc32c(p)),
            None => (0, 0),
        };

        let ingest = IngestHeader {
            payload_length: payload_len,
            payload_crc,
            batch_id: command as u64,
            ..Default::default()
        };

        let header = LwpHeader::new(LwpFlags::Control as u8, ingest);

        Self {
            header,
            payload,
            frame_type: FrameType::Control(command),
        }
    }

    pub fn new_create_topic(topic_name: &str) -> Self {
        Self::new_control(
            ControlCommand::CreateTopic,
            Some(Bytes::copy_from_slice(topic_name.as_bytes())),
        )
    }

    pub fn new_list_topics() -> Self {
        Self::new_control(ControlCommand::ListTopics, None)
    }

    pub fn new_get_topic(topic_id: u32) -> Self {
        Self::new_control(
            ControlCommand::GetTopic,
            Some(Bytes::copy_from_slice(&topic_id.to_le_bytes())),
        )
    }

    pub fn new_delete_topic(topic_id: u32) -> Self {
        Self::new_control(
            ControlCommand::DeleteTopic,
            Some(Bytes::copy_from_slice(&topic_id.to_le_bytes())),
        )
    }

    /// Create a set retention policy request frame
    /// Payload format: topic_id(4) + max_age_secs(8) + max_bytes(8)
    pub fn new_set_retention(topic_id: u32, max_age_secs: u64, max_bytes: u64) -> Self {
        let mut payload = Vec::with_capacity(20);
        payload.extend_from_slice(&topic_id.to_le_bytes());
        payload.extend_from_slice(&max_age_secs.to_le_bytes());
        payload.extend_from_slice(&max_bytes.to_le_bytes());
        Self::new_control(ControlCommand::SetRetention, Some(Bytes::from(payload)))
    }

    /// Create a topic with retention configuration
    /// Payload format: name_len(2) + name(var) + max_age_secs(8) + max_bytes(8)
    pub fn new_create_topic_with_retention(
        topic_name: &str,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Self {
        let name_bytes = topic_name.as_bytes();
        let mut payload = Vec::with_capacity(2 + name_bytes.len() + 16);
        payload.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(name_bytes);
        payload.extend_from_slice(&max_age_secs.to_le_bytes());
        payload.extend_from_slice(&max_bytes.to_le_bytes());
        Self::new_control(
            ControlCommand::CreateTopicWithRetention,
            Some(Bytes::from(payload)),
        )
    }

    /// Create a fetch request frame
    /// Payload format: topic_id(4) + start_offset(8) + max_bytes(4)
    pub fn new_fetch(topic_id: u32, start_offset: u64, max_bytes: u32) -> Self {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&topic_id.to_le_bytes());
        payload.extend_from_slice(&start_offset.to_le_bytes());
        payload.extend_from_slice(&max_bytes.to_le_bytes());
        Self::new_control(ControlCommand::Fetch, Some(Bytes::from(payload)))
    }

    pub fn new_fetch_response(payload: Bytes) -> Self {
        Self::new_control(ControlCommand::FetchResponse, Some(payload))
    }

    /// Create a get cluster status request frame
    pub fn new_get_cluster_status() -> Self {
        Self::new_control(ControlCommand::GetClusterStatus, None)
    }

    /// Create a cluster status response frame
    pub fn new_cluster_status_response(payload: Bytes) -> Self {
        Self::new_control(ControlCommand::ClusterStatusResponse, Some(payload))
    }

    /// Create an authenticate request frame
    /// Payload format: token (UTF-8 string bytes)
    pub fn new_authenticate(token: &str) -> Self {
        Self::new_control(
            ControlCommand::Authenticate,
            Some(Bytes::copy_from_slice(token.as_bytes())),
        )
    }

    /// Create an authenticate response frame
    /// Payload format: success(1) + optional message
    pub fn new_authenticate_response(success: bool, message: Option<&str>) -> Self {
        let mut payload = Vec::with_capacity(1 + message.map_or(0, |m| m.len()));
        payload.push(if success { 1 } else { 0 });
        if let Some(msg) = message {
            payload.extend_from_slice(msg.as_bytes());
        }
        Self::new_control(
            ControlCommand::AuthenticateResponse,
            Some(Bytes::from(payload)),
        )
    }

    /// Create a subscribe request frame
    /// Payload format: topic_id(4) + start_offset(8) + max_batch_bytes(4) + consumer_id(8)
    pub fn new_subscribe(
        topic_id: u32,
        start_offset: u64,
        max_batch_bytes: u32,
        consumer_id: u64,
    ) -> Self {
        let mut payload = Vec::with_capacity(24);
        payload.extend_from_slice(&topic_id.to_le_bytes());
        payload.extend_from_slice(&start_offset.to_le_bytes());
        payload.extend_from_slice(&max_batch_bytes.to_le_bytes());
        payload.extend_from_slice(&consumer_id.to_le_bytes());
        Self::new_control(ControlCommand::Subscribe, Some(Bytes::from(payload)))
    }

    /// Create an unsubscribe request frame
    /// Payload format: topic_id(4) + consumer_id(8)
    pub fn new_unsubscribe(topic_id: u32, consumer_id: u64) -> Self {
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&topic_id.to_le_bytes());
        payload.extend_from_slice(&consumer_id.to_le_bytes());
        Self::new_control(ControlCommand::Unsubscribe, Some(Bytes::from(payload)))
    }

    /// Create a commit offset request frame
    /// Payload format: topic_id(4) + consumer_id(8) + offset(8)
    pub fn new_commit_offset(topic_id: u32, consumer_id: u64, offset: u64) -> Self {
        let mut payload = Vec::with_capacity(20);
        payload.extend_from_slice(&topic_id.to_le_bytes());
        payload.extend_from_slice(&consumer_id.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());
        Self::new_control(ControlCommand::CommitOffset, Some(Bytes::from(payload)))
    }

    /// Create a subscribe acknowledgment frame
    /// Payload format: consumer_id(8) + start_offset(8)
    pub fn new_subscribe_ack(consumer_id: u64, start_offset: u64) -> Self {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&consumer_id.to_le_bytes());
        payload.extend_from_slice(&start_offset.to_le_bytes());
        Self::new_control(ControlCommand::SubscribeAck, Some(Bytes::from(payload)))
    }

    /// Create a commit offset acknowledgment frame
    /// Payload format: consumer_id(8) + committed_offset(8)
    pub fn new_commit_ack(consumer_id: u64, committed_offset: u64) -> Self {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&consumer_id.to_le_bytes());
        payload.extend_from_slice(&committed_offset.to_le_bytes());
        Self::new_control(ControlCommand::CommitAck, Some(Bytes::from(payload)))
    }

    pub fn new_topic_response(payload: Bytes) -> Self {
        Self::new_control(ControlCommand::TopicResponse, Some(payload))
    }

    pub fn new_error_response(message: &str) -> Self {
        Self::new_control(
            ControlCommand::ErrorResponse,
            Some(Bytes::copy_from_slice(message.as_bytes())),
        )
    }

    #[inline]
    #[must_use]
    pub fn batch_id(&self) -> u64 {
        self.header.ingest_header.batch_id
    }

    #[inline]
    #[must_use]
    pub fn payload_length(&self) -> u32 {
        self.header.ingest_header.payload_length
    }

    #[inline]
    #[must_use]
    pub fn record_count(&self) -> u32 {
        self.header.ingest_header.record_count
    }

    #[inline]
    #[must_use]
    pub fn topic_id(&self) -> u32 {
        self.header.ingest_header.topic_id
    }
}

pub fn parse_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>> {
    if buf.len() < LWP_HEADER_SIZE {
        return Ok(None);
    }

    let header = LwpHeader::parse(buf)?;

    let frame_type = determine_frame_type(&header);
    let payload_len = header.ingest_header.payload_length as usize;
    let total_len = LWP_HEADER_SIZE + payload_len;

    if buf.len() < total_len {
        return Ok(None);
    }

    let payload = if payload_len > 0 {
        let payload_bytes = &buf[LWP_HEADER_SIZE..total_len];
        header.ingest_header.validate_payload(payload_bytes)?;
        Some(Bytes::copy_from_slice(payload_bytes))
    } else {
        None
    };

    let frame = Frame {
        header,
        payload,
        frame_type,
    };

    Ok(Some((frame, total_len)))
}

pub fn encode_frame(frame: &Frame) -> Vec<u8> {
    let header_bytes = frame.header.encode();
    let payload_len = frame.payload.as_ref().map_or(0, |p| p.len());

    let mut buf = Vec::with_capacity(LWP_HEADER_SIZE + payload_len);
    buf.extend_from_slice(&header_bytes);

    if let Some(ref payload) = frame.payload {
        buf.extend_from_slice(payload);
    }

    buf
}

fn determine_frame_type(header: &LwpHeader) -> FrameType {
    if header.has_flag(LwpFlags::Control) {
        let command = ControlCommand::from(header.ingest_header.batch_id as u8);
        FrameType::Control(command)
    } else if header.has_flag(LwpFlags::Keepalive) {
        FrameType::Keepalive
    } else if header.has_flag(LwpFlags::Ack) {
        FrameType::Ack
    } else if header.has_flag(LwpFlags::Backpressure) {
        FrameType::Backpressure
    } else if header.has_flag(LwpFlags::BatchMode) || header.ingest_header.payload_length > 0 {
        FrameType::Ingest
    } else {
        FrameType::Unknown
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_frame_roundtrip() {
        let payload = Bytes::from_static(b"test payload data");
        let frame = Frame::new_ingest(12345, 1_000_000_000, 1, payload.clone());

        let encoded = encode_frame(&frame);
        let (parsed, len) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(len, encoded.len());
        assert_eq!(parsed.frame_type, FrameType::Ingest);
        assert_eq!(parsed.batch_id(), 12345);
        assert_eq!(parsed.payload.unwrap(), payload);
    }

    #[test]
    fn test_ack_frame_roundtrip() {
        let frame = Frame::new_ack(54321);

        let encoded = encode_frame(&frame);
        let (parsed, _) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(parsed.frame_type, FrameType::Ack);
        assert_eq!(parsed.batch_id(), 54321);
        assert!(parsed.payload.is_none());
    }

    #[test]
    fn test_keepalive_frame() {
        let frame = Frame::new_keepalive();

        let encoded = encode_frame(&frame);
        let (parsed, _) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(parsed.frame_type, FrameType::Keepalive);
    }

    #[test]
    fn test_partial_frame() {
        let payload = Bytes::from_static(b"test");
        let frame = Frame::new_ingest(1, 0, 1, payload);
        let encoded = encode_frame(&frame);

        let result = parse_frame(&encoded[..LWP_HEADER_SIZE - 1]).unwrap();
        assert!(result.is_none());

        let result = parse_frame(&encoded[..LWP_HEADER_SIZE + 2]).unwrap();
        assert!(result.is_none());
    }

    // =========================================================================
    // Streaming Control Frame Tests
    // =========================================================================

    #[test]
    fn test_subscribe_frame_roundtrip() {
        let topic_id = 42u32;
        let start_offset = 1000u64;
        let max_batch_bytes = 65536u32;
        let consumer_id = 0xDEADBEEF12345678u64;

        let frame = Frame::new_subscribe(topic_id, start_offset, max_batch_bytes, consumer_id);

        let encoded = encode_frame(&frame);
        let (parsed, len) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(len, encoded.len());
        assert_eq!(
            parsed.frame_type,
            FrameType::Control(ControlCommand::Subscribe)
        );

        // Verify payload contents
        let payload = parsed.payload.unwrap();
        assert_eq!(payload.len(), 24);

        let parsed_topic_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let parsed_offset = u64::from_le_bytes([
            payload[4],
            payload[5],
            payload[6],
            payload[7],
            payload[8],
            payload[9],
            payload[10],
            payload[11],
        ]);
        let parsed_max_bytes =
            u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);
        let parsed_consumer_id = u64::from_le_bytes([
            payload[16],
            payload[17],
            payload[18],
            payload[19],
            payload[20],
            payload[21],
            payload[22],
            payload[23],
        ]);

        assert_eq!(parsed_topic_id, topic_id);
        assert_eq!(parsed_offset, start_offset);
        assert_eq!(parsed_max_bytes, max_batch_bytes);
        assert_eq!(parsed_consumer_id, consumer_id);
    }

    #[test]
    fn test_unsubscribe_frame_roundtrip() {
        let topic_id = 7u32;
        let consumer_id = 0xCAFEBABE00000001u64;

        let frame = Frame::new_unsubscribe(topic_id, consumer_id);

        let encoded = encode_frame(&frame);
        let (parsed, len) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(len, encoded.len());
        assert_eq!(
            parsed.frame_type,
            FrameType::Control(ControlCommand::Unsubscribe)
        );

        // Verify payload contents
        let payload = parsed.payload.unwrap();
        assert_eq!(payload.len(), 12);

        let parsed_topic_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let parsed_consumer_id = u64::from_le_bytes([
            payload[4],
            payload[5],
            payload[6],
            payload[7],
            payload[8],
            payload[9],
            payload[10],
            payload[11],
        ]);

        assert_eq!(parsed_topic_id, topic_id);
        assert_eq!(parsed_consumer_id, consumer_id);
    }

    #[test]
    fn test_commit_offset_frame_roundtrip() {
        let topic_id = 99u32;
        let consumer_id = 0x1234567890ABCDEFu64;
        let offset = 50000u64;

        let frame = Frame::new_commit_offset(topic_id, consumer_id, offset);

        let encoded = encode_frame(&frame);
        let (parsed, len) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(len, encoded.len());
        assert_eq!(
            parsed.frame_type,
            FrameType::Control(ControlCommand::CommitOffset)
        );

        // Verify payload contents
        let payload = parsed.payload.unwrap();
        assert_eq!(payload.len(), 20);

        let parsed_topic_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let parsed_consumer_id = u64::from_le_bytes([
            payload[4],
            payload[5],
            payload[6],
            payload[7],
            payload[8],
            payload[9],
            payload[10],
            payload[11],
        ]);
        let parsed_offset = u64::from_le_bytes([
            payload[12],
            payload[13],
            payload[14],
            payload[15],
            payload[16],
            payload[17],
            payload[18],
            payload[19],
        ]);

        assert_eq!(parsed_topic_id, topic_id);
        assert_eq!(parsed_consumer_id, consumer_id);
        assert_eq!(parsed_offset, offset);
    }

    #[test]
    fn test_subscribe_ack_frame_roundtrip() {
        let consumer_id = 0xABCDEF0123456789u64;
        let start_offset = 12345u64;

        let frame = Frame::new_subscribe_ack(consumer_id, start_offset);

        let encoded = encode_frame(&frame);
        let (parsed, len) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(len, encoded.len());
        assert_eq!(
            parsed.frame_type,
            FrameType::Control(ControlCommand::SubscribeAck)
        );

        // Verify payload contents
        let payload = parsed.payload.unwrap();
        assert_eq!(payload.len(), 16);

        let parsed_consumer_id = u64::from_le_bytes([
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ]);
        let parsed_offset = u64::from_le_bytes([
            payload[8],
            payload[9],
            payload[10],
            payload[11],
            payload[12],
            payload[13],
            payload[14],
            payload[15],
        ]);

        assert_eq!(parsed_consumer_id, consumer_id);
        assert_eq!(parsed_offset, start_offset);
    }

    #[test]
    fn test_commit_ack_frame_roundtrip() {
        let consumer_id = 0xFEDCBA9876543210u64;
        let committed_offset = 99999u64;

        let frame = Frame::new_commit_ack(consumer_id, committed_offset);

        let encoded = encode_frame(&frame);
        let (parsed, len) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(len, encoded.len());
        assert_eq!(
            parsed.frame_type,
            FrameType::Control(ControlCommand::CommitAck)
        );

        // Verify payload contents
        let payload = parsed.payload.unwrap();
        assert_eq!(payload.len(), 16);

        let parsed_consumer_id = u64::from_le_bytes([
            payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
            payload[7],
        ]);
        let parsed_offset = u64::from_le_bytes([
            payload[8],
            payload[9],
            payload[10],
            payload[11],
            payload[12],
            payload[13],
            payload[14],
            payload[15],
        ]);

        assert_eq!(parsed_consumer_id, consumer_id);
        assert_eq!(parsed_offset, committed_offset);
    }

    #[test]
    fn test_fetch_frame_roundtrip() {
        let topic_id = 5u32;
        let start_offset = 1024u64;
        let max_bytes = 32768u32;

        let frame = Frame::new_fetch(topic_id, start_offset, max_bytes);

        let encoded = encode_frame(&frame);
        let (parsed, len) = parse_frame(&encoded).unwrap().unwrap();

        assert_eq!(len, encoded.len());
        assert_eq!(parsed.frame_type, FrameType::Control(ControlCommand::Fetch));

        // Verify payload contents
        let payload = parsed.payload.unwrap();
        assert_eq!(payload.len(), 16);

        let parsed_topic_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let parsed_offset = u64::from_le_bytes([
            payload[4],
            payload[5],
            payload[6],
            payload[7],
            payload[8],
            payload[9],
            payload[10],
            payload[11],
        ]);
        let parsed_max_bytes =
            u32::from_le_bytes([payload[12], payload[13], payload[14], payload[15]]);

        assert_eq!(parsed_topic_id, topic_id);
        assert_eq!(parsed_offset, start_offset);
        assert_eq!(parsed_max_bytes, max_bytes);
    }
}
