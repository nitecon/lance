use lnc_core::{LANCE_MAGIC, LanceError, Result, crc32c};

pub const PROTOCOL_VERSION: u8 = 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LwpFlags {
    None = 0x00,
    Compressed = 0x01,
    Encrypted = 0x02,
    BatchMode = 0x04,
    Ack = 0x08,
    Backpressure = 0x10,
    Keepalive = 0x20,
    Control = 0x40,
}

impl From<u8> for LwpFlags {
    fn from(value: u8) -> Self {
        match value {
            0x01 => Self::Compressed,
            0x02 => Self::Encrypted,
            0x04 => Self::BatchMode,
            0x08 => Self::Ack,
            0x10 => Self::Backpressure,
            0x20 => Self::Keepalive,
            0x40 => Self::Control,
            _ => Self::None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCommand {
    // Topic management (0x01-0x0F)
    CreateTopic = 0x01,
    DeleteTopic = 0x02,
    ListTopics = 0x03,
    GetTopic = 0x04,
    /// Set retention policy for a topic
    SetRetention = 0x05,
    /// Create topic with retention configuration
    CreateTopicWithRetention = 0x06,

    // Cluster management (0x07-0x0F)
    /// Get cluster status and health information
    GetClusterStatus = 0x07,
    /// Authenticate client with token
    Authenticate = 0x08,
    /// Authentication response (success/failure)
    AuthenticateResponse = 0x09,

    // Data fetch - request/response (0x10-0x1F)
    Fetch = 0x10,
    FetchResponse = 0x11,

    // Streaming control (0x20-0x2F)
    /// Subscribe to a topic for streaming - server will push data
    Subscribe = 0x20,
    /// Unsubscribe from a topic - stop streaming
    Unsubscribe = 0x21,
    /// Commit consumer offset for checkpointing
    CommitOffset = 0x22,
    /// Server acknowledgment of subscription
    SubscribeAck = 0x23,
    /// Server acknowledgment of offset commit
    CommitAck = 0x24,

    // Responses (0x80+)
    TopicResponse = 0x80,
    /// Cluster status response
    ClusterStatusResponse = 0x81,
    ErrorResponse = 0xFF,
}

impl From<u8> for ControlCommand {
    fn from(value: u8) -> Self {
        match value {
            0x01 => Self::CreateTopic,
            0x02 => Self::DeleteTopic,
            0x03 => Self::ListTopics,
            0x04 => Self::GetTopic,
            0x05 => Self::SetRetention,
            0x06 => Self::CreateTopicWithRetention,
            0x07 => Self::GetClusterStatus,
            0x08 => Self::Authenticate,
            0x09 => Self::AuthenticateResponse,
            0x10 => Self::Fetch,
            0x11 => Self::FetchResponse,
            0x20 => Self::Subscribe,
            0x21 => Self::Unsubscribe,
            0x22 => Self::CommitOffset,
            0x23 => Self::SubscribeAck,
            0x24 => Self::CommitAck,
            0x80 => Self::TopicResponse,
            0x81 => Self::ClusterStatusResponse,
            0xFF => Self::ErrorResponse,
            _ => Self::ErrorResponse,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LwpHeader {
    pub magic: [u8; 4],
    pub version: u8,
    pub flags: u8,
    pub reserved: [u8; 2],
    pub header_crc: u32,
    pub ingest_header: IngestHeader,
}

impl LwpHeader {
    pub const SIZE: usize = 44;

    pub fn new(flags: u8, ingest_header: IngestHeader) -> Self {
        let mut header = Self {
            magic: LANCE_MAGIC,
            version: PROTOCOL_VERSION,
            flags,
            reserved: [0; 2],
            header_crc: 0,
            ingest_header,
        };

        let mut crc_buf = [0u8; 8];
        crc_buf[0..4].copy_from_slice(&header.magic);
        crc_buf[4] = header.version;
        crc_buf[5] = header.flags;
        crc_buf[6..8].copy_from_slice(&header.reserved);
        header.header_crc = crc32c(&crc_buf);

        header
    }

    pub fn parse(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::SIZE {
            return Err(LanceError::Protocol(
                "Buffer too small for LWP header".into(),
            ));
        }

        if buf[0..4] != LANCE_MAGIC {
            return Err(LanceError::InvalidMagic);
        }

        let version = buf[4];
        if version != PROTOCOL_VERSION {
            return Err(LanceError::Protocol(format!(
                "Unsupported protocol version: {}",
                version
            )));
        }

        let stored_crc = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let computed_crc = crc32c(&buf[0..8]);

        if stored_crc != computed_crc {
            return Err(LanceError::CrcMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[0..4]);

        let ingest_header = IngestHeader::parse(&buf[12..Self::SIZE])?;

        Ok(Self {
            magic,
            version: buf[4],
            flags: buf[5],
            reserved: [buf[6], buf[7]],
            header_crc: stored_crc,
            ingest_header,
        })
    }

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version;
        buf[5] = self.flags;
        buf[6..8].copy_from_slice(&self.reserved);
        buf[8..12].copy_from_slice(&self.header_crc.to_le_bytes());
        self.ingest_header.encode_into(&mut buf[12..Self::SIZE]);
        buf
    }

    #[inline]
    pub fn has_flag(&self, flag: LwpFlags) -> bool {
        self.flags & (flag as u8) != 0
    }

    #[inline]
    pub fn is_keepalive(&self) -> bool {
        self.has_flag(LwpFlags::Keepalive)
    }

    #[inline]
    pub fn is_batch_mode(&self) -> bool {
        self.has_flag(LwpFlags::BatchMode)
    }

    #[inline]
    pub fn is_compressed(&self) -> bool {
        self.has_flag(LwpFlags::Compressed)
    }

    #[inline]
    pub fn is_control(&self) -> bool {
        self.has_flag(LwpFlags::Control)
    }

    /// Create a keepalive header
    pub fn keepalive() -> Self {
        Self::new(LwpFlags::Keepalive as u8, IngestHeader::default())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IngestHeader {
    pub batch_id: u64,
    pub timestamp_ns: u64,
    pub record_count: u32,
    pub payload_length: u32,
    pub payload_crc: u32,
    pub topic_id: u32,
}

impl IngestHeader {
    pub const SIZE: usize = 32;

    pub fn new(
        batch_id: u64,
        timestamp_ns: u64,
        record_count: u32,
        payload_length: u32,
        payload_crc: u32,
    ) -> Self {
        Self {
            batch_id,
            timestamp_ns,
            record_count,
            payload_length,
            payload_crc,
            topic_id: 0,
        }
    }

    pub fn with_topic(
        batch_id: u64,
        timestamp_ns: u64,
        record_count: u32,
        payload_length: u32,
        payload_crc: u32,
        topic_id: u32,
    ) -> Self {
        Self {
            batch_id,
            timestamp_ns,
            record_count,
            payload_length,
            payload_crc,
            topic_id,
        }
    }

    pub fn parse(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::SIZE {
            return Err(LanceError::Protocol(
                "Buffer too small for IngestHeader".into(),
            ));
        }

        Ok(Self {
            batch_id: u64::from_le_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]),
            timestamp_ns: u64::from_le_bytes([
                buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
            ]),
            record_count: u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]),
            payload_length: u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]),
            payload_crc: u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]),
            topic_id: u32::from_le_bytes([buf[28], buf[29], buf[30], buf[31]]),
        })
    }

    pub fn encode_into(&self, buf: &mut [u8]) {
        buf[0..8].copy_from_slice(&self.batch_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        buf[16..20].copy_from_slice(&self.record_count.to_le_bytes());
        buf[20..24].copy_from_slice(&self.payload_length.to_le_bytes());
        buf[24..28].copy_from_slice(&self.payload_crc.to_le_bytes());
        buf[28..32].copy_from_slice(&self.topic_id.to_le_bytes());
    }

    pub fn validate_payload(&self, payload: &[u8]) -> Result<()> {
        if payload.len() != self.payload_length as usize {
            return Err(LanceError::Protocol(format!(
                "Payload length mismatch: expected {}, got {}",
                self.payload_length,
                payload.len()
            )));
        }

        let actual_crc = crc32c(payload);
        if actual_crc != self.payload_crc {
            lnc_metrics::increment_crc_failures();
            return Err(LanceError::CrcMismatch {
                expected: self.payload_crc,
                actual: actual_crc,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_lwp_header_roundtrip() {
        let ingest = IngestHeader::new(12345, 1_000_000_000, 100, 4096, 0xDEADBEEF);
        let header = LwpHeader::new(LwpFlags::BatchMode as u8, ingest);

        let encoded = header.encode();
        let parsed = LwpHeader::parse(&encoded).unwrap();

        assert_eq!(parsed.magic, LANCE_MAGIC);
        assert_eq!(parsed.version, PROTOCOL_VERSION);
        assert_eq!(parsed.ingest_header.batch_id, 12345);
        assert_eq!(parsed.ingest_header.record_count, 100);
    }

    #[test]
    fn test_invalid_magic() {
        let mut buf = [0u8; LwpHeader::SIZE];
        buf[0..4].copy_from_slice(b"JUNK");

        let result = LwpHeader::parse(&buf);
        assert!(matches!(result, Err(LanceError::InvalidMagic)));
    }

    #[test]
    fn test_keepalive() {
        let header = LwpHeader::keepalive();
        assert!(header.is_keepalive());
        assert!(!header.is_batch_mode());
    }

    #[test]
    fn test_payload_validation() {
        let payload = b"test payload data";
        let crc = crc32c(payload);

        let ingest = IngestHeader::new(1, 0, 1, payload.len() as u32, crc);
        assert!(ingest.validate_payload(payload).is_ok());

        let wrong_crc_ingest = IngestHeader::new(1, 0, 1, payload.len() as u32, 0xBADBAD);
        assert!(wrong_crc_ingest.validate_payload(payload).is_err());
    }

    // =========================================================================
    // ControlCommand Parsing Tests
    // =========================================================================

    #[test]
    fn test_control_command_topic_management() {
        assert_eq!(ControlCommand::from(0x01), ControlCommand::CreateTopic);
        assert_eq!(ControlCommand::from(0x02), ControlCommand::DeleteTopic);
        assert_eq!(ControlCommand::from(0x03), ControlCommand::ListTopics);
        assert_eq!(ControlCommand::from(0x04), ControlCommand::GetTopic);
    }

    #[test]
    fn test_control_command_fetch() {
        assert_eq!(ControlCommand::from(0x10), ControlCommand::Fetch);
        assert_eq!(ControlCommand::from(0x11), ControlCommand::FetchResponse);
    }

    #[test]
    fn test_control_command_streaming() {
        assert_eq!(ControlCommand::from(0x20), ControlCommand::Subscribe);
        assert_eq!(ControlCommand::from(0x21), ControlCommand::Unsubscribe);
        assert_eq!(ControlCommand::from(0x22), ControlCommand::CommitOffset);
        assert_eq!(ControlCommand::from(0x23), ControlCommand::SubscribeAck);
        assert_eq!(ControlCommand::from(0x24), ControlCommand::CommitAck);
    }

    #[test]
    fn test_control_command_responses() {
        assert_eq!(ControlCommand::from(0x80), ControlCommand::TopicResponse);
        assert_eq!(ControlCommand::from(0xFF), ControlCommand::ErrorResponse);
    }

    #[test]
    fn test_control_command_authentication() {
        assert_eq!(ControlCommand::from(0x08), ControlCommand::Authenticate);
        assert_eq!(
            ControlCommand::from(0x09),
            ControlCommand::AuthenticateResponse
        );
        assert_eq!(ControlCommand::Authenticate as u8, 0x08);
        assert_eq!(ControlCommand::AuthenticateResponse as u8, 0x09);
    }

    #[test]
    fn test_control_command_unknown_defaults_to_error() {
        // Unknown command codes should default to ErrorResponse
        assert_eq!(ControlCommand::from(0x00), ControlCommand::ErrorResponse);
        assert_eq!(ControlCommand::from(0x99), ControlCommand::ErrorResponse);
        assert_eq!(ControlCommand::from(0xFE), ControlCommand::ErrorResponse);
    }

    #[test]
    fn test_control_command_roundtrip() {
        // Verify command codes match enum values
        assert_eq!(ControlCommand::Subscribe as u8, 0x20);
        assert_eq!(ControlCommand::Unsubscribe as u8, 0x21);
        assert_eq!(ControlCommand::CommitOffset as u8, 0x22);
        assert_eq!(ControlCommand::SubscribeAck as u8, 0x23);
        assert_eq!(ControlCommand::CommitAck as u8, 0x24);
    }
}
