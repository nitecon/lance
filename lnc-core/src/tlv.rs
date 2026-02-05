//! TLV (Type-Length-Value) encoding for LANCE records
//!
//! Based on Architecture Section 2: TLV Record Format
//!
//! Standard TLV Header (5 bytes):
//! - Type: 1 byte (0x00-0xFE for standard types, 0xFF for extended)
//! - Length: 4 bytes (little-endian, payload length)
//!
//! Extended TLV Header (7 bytes, when type == 0xFF):
//! - Type marker: 1 byte (0xFF)
//! - Extended type: 2 bytes (little-endian)
//! - Length: 4 bytes (little-endian)

use crate::{EXTENDED_TLV_HEADER_SIZE, EXTENDED_TYPE_MARKER, LanceError, Result, TLV_HEADER_SIZE};

/// Standard record types (0x00-0xFE)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Raw payload data
    Data = 0x00,
    /// JSON-encoded payload
    Json = 0x01,
    /// MessagePack-encoded payload
    MsgPack = 0x02,
    /// Protobuf-encoded payload
    Protobuf = 0x03,
    /// Avro-encoded payload
    Avro = 0x04,
    /// Control message (internal)
    Control = 0xFE,
    /// Extended type marker (use `extended_type` field)
    Extended = 0xFF,
}

impl TryFrom<u8> for RecordType {
    type Error = LanceError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(Self::Json),
            0x02 => Ok(Self::MsgPack),
            0x03 => Ok(Self::Protobuf),
            0x04 => Ok(Self::Avro),
            0xFE => Ok(Self::Control),
            0xFF => Ok(Self::Extended),
            // 0x00 and unknown types are treated as raw data
            _ => Ok(Self::Data),
        }
    }
}

/// Parsed TLV header
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Record type (standard or extended marker)
    pub record_type: RecordType,
    /// Extended type (only valid if `record_type` == Extended)
    pub extended_type: u16,
    /// Payload length in bytes
    pub length: u32,
    /// Total header size (5 for standard, 7 for extended)
    pub header_size: usize,
}

impl Header {
    /// Create a standard header
    #[must_use]
    pub const fn new(record_type: RecordType, length: u32) -> Self {
        Self {
            record_type,
            extended_type: 0,
            length,
            header_size: TLV_HEADER_SIZE,
        }
    }

    /// Create an extended header
    #[must_use]
    pub const fn new_extended(extended_type: u16, length: u32) -> Self {
        Self {
            record_type: RecordType::Extended,
            extended_type,
            length,
            header_size: EXTENDED_TLV_HEADER_SIZE,
        }
    }

    /// Total record size (header + payload)
    #[must_use]
    pub const fn total_size(&self) -> usize {
        self.header_size + self.length as usize
    }

    /// Encode header to bytes
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        if self.record_type == RecordType::Extended {
            let mut buf = Vec::with_capacity(EXTENDED_TLV_HEADER_SIZE);
            buf.push(EXTENDED_TYPE_MARKER);
            buf.extend_from_slice(&self.extended_type.to_le_bytes());
            buf.extend_from_slice(&self.length.to_le_bytes());
            buf
        } else {
            let mut buf = Vec::with_capacity(TLV_HEADER_SIZE);
            buf.push(self.record_type as u8);
            buf.extend_from_slice(&self.length.to_le_bytes());
            buf
        }
    }
}

/// Parse a TLV header from a byte slice.
///
/// Returns the parsed header or an error if the buffer is too small
/// or contains invalid data.
///
/// # Errors
/// Returns an error if the buffer is too small for the header.
pub fn parse_header(data: &[u8]) -> Result<Header> {
    if data.len() < TLV_HEADER_SIZE {
        return Err(LanceError::BufferTooSmall {
            required: TLV_HEADER_SIZE,
            available: data.len(),
        });
    }

    let type_byte = data[0];

    if type_byte == EXTENDED_TYPE_MARKER {
        // Extended header
        if data.len() < EXTENDED_TLV_HEADER_SIZE {
            return Err(LanceError::BufferTooSmall {
                required: EXTENDED_TLV_HEADER_SIZE,
                available: data.len(),
            });
        }

        let extended_type = u16::from_le_bytes([data[1], data[2]]);
        let length = u32::from_le_bytes([data[3], data[4], data[5], data[6]]);

        Ok(Header {
            record_type: RecordType::Extended,
            extended_type,
            length,
            header_size: EXTENDED_TLV_HEADER_SIZE,
        })
    } else {
        // Standard header
        let record_type = RecordType::try_from(type_byte)?;
        let length = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);

        Ok(Header {
            record_type,
            extended_type: 0,
            length,
            header_size: TLV_HEADER_SIZE,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_header_roundtrip() {
        let header = Header::new(RecordType::Json, 1024);
        let encoded = header.encode();
        assert_eq!(encoded.len(), TLV_HEADER_SIZE);

        let parsed = parse_header(&encoded).unwrap();
        assert_eq!(parsed.record_type, RecordType::Json);
        assert_eq!(parsed.length, 1024);
        assert_eq!(parsed.header_size, TLV_HEADER_SIZE);
    }

    #[test]
    fn test_extended_header_roundtrip() {
        let header = Header::new_extended(0x1234, 65536);
        let encoded = header.encode();
        assert_eq!(encoded.len(), EXTENDED_TLV_HEADER_SIZE);

        let parsed = parse_header(&encoded).unwrap();
        assert_eq!(parsed.record_type, RecordType::Extended);
        assert_eq!(parsed.extended_type, 0x1234);
        assert_eq!(parsed.length, 65536);
        assert_eq!(parsed.header_size, EXTENDED_TLV_HEADER_SIZE);
    }

    #[test]
    fn test_buffer_too_small() {
        let data = [0x00, 0x01, 0x02]; // Only 3 bytes
        let result = parse_header(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_total_size() {
        let header = Header::new(RecordType::Data, 100);
        assert_eq!(header.total_size(), TLV_HEADER_SIZE + 100);

        let ext_header = Header::new_extended(0x01, 200);
        assert_eq!(ext_header.total_size(), EXTENDED_TLV_HEADER_SIZE + 200);
    }
}
