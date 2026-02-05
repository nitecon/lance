//! TLV Record Parsing
//!
//! Provides parsing utilities for TLV (Type-Length-Value) encoded records
//! from LANCE stream data.
//!
//! # Record Format
//!
//! Each record in a LANCE stream follows the TLV format:
//!
//! ```text
//! +--------+--------+------------------+
//! | Type   | Length | Value (payload)  |
//! | 1 byte | 4 bytes| variable         |
//! +--------+--------+------------------+
//! ```
//!
//! - **Type**: Record type identifier (1 byte)
//! - **Length**: Payload length in bytes (4 bytes, big-endian)
//! - **Value**: Record payload (variable length)
//!
//! # Example
//!
//! ```rust,no_run
//! use lnc_client::{RecordIterator, Record, RecordType};
//! use bytes::Bytes;
//!
//! fn process_records(data: Bytes) {
//!     for result in RecordIterator::new(data) {
//!         match result {
//!             Ok(record) => {
//!                 println!("Type: {:?}, Length: {}", record.record_type, record.payload.len());
//!             }
//!             Err(e) => {
//!                 eprintln!("Parse error: {}", e);
//!                 break;
//!             }
//!         }
//!     }
//! }
//! ```

use bytes::Bytes;
use std::fmt;

use crate::error::{ClientError, Result};

/// TLV header size in bytes (1 byte type + 4 bytes length)
pub const TLV_HEADER_SIZE: usize = 5;

/// Record type identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RecordType {
    /// Standard data record
    Data = 0x01,
    /// Tombstone/deletion marker
    Tombstone = 0x02,
    /// Transaction boundary marker
    Transaction = 0x03,
    /// Checkpoint/snapshot marker
    Checkpoint = 0x04,
    /// Schema change record
    Schema = 0x05,
    /// Compression marker (following records are compressed)
    Compressed = 0x10,
    /// User-defined record type (application-specific)
    UserDefined = 0x80,
    /// Unknown/reserved type
    Unknown = 0xFF,
}

impl From<u8> for RecordType {
    fn from(byte: u8) -> Self {
        match byte {
            0x01 => RecordType::Data,
            0x02 => RecordType::Tombstone,
            0x03 => RecordType::Transaction,
            0x04 => RecordType::Checkpoint,
            0x05 => RecordType::Schema,
            0x10 => RecordType::Compressed,
            0x80..=0xFE => RecordType::UserDefined,
            _ => RecordType::Unknown,
        }
    }
}

impl From<RecordType> for u8 {
    fn from(rt: RecordType) -> u8 {
        rt as u8
    }
}

/// A parsed TLV record
#[derive(Debug, Clone)]
pub struct Record {
    /// Record type
    pub record_type: RecordType,
    /// Raw type byte (for user-defined types)
    pub type_byte: u8,
    /// Record payload (zero-copy reference to original data)
    pub payload: Bytes,
    /// Offset in the original data where this record started
    pub offset: usize,
}

impl Record {
    /// Create a new record
    pub fn new(record_type: RecordType, type_byte: u8, payload: Bytes, offset: usize) -> Self {
        Self {
            record_type,
            type_byte,
            payload,
            offset,
        }
    }

    /// Check if this is a data record
    pub fn is_data(&self) -> bool {
        self.record_type == RecordType::Data
    }

    /// Check if this is a tombstone record
    pub fn is_tombstone(&self) -> bool {
        self.record_type == RecordType::Tombstone
    }

    /// Get the total size of this record (header + payload)
    pub fn total_size(&self) -> usize {
        TLV_HEADER_SIZE + self.payload.len()
    }

    /// Get payload as UTF-8 string (if valid)
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.payload).ok()
    }

    /// Get payload as bytes slice
    pub fn as_bytes(&self) -> &[u8] {
        &self.payload
    }
}

/// Error type for record parsing
#[derive(Debug, Clone)]
pub enum RecordParseError {
    /// Not enough data for TLV header
    InsufficientHeader { needed: usize, available: usize },
    /// Not enough data for payload
    InsufficientPayload { needed: usize, available: usize },
    /// Invalid record type
    InvalidType(u8),
    /// Payload length exceeds maximum
    PayloadTooLarge { length: u32, max: u32 },
}

impl fmt::Display for RecordParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordParseError::InsufficientHeader { needed, available } => {
                write!(f, "Insufficient data for header: need {} bytes, have {}", needed, available)
            }
            RecordParseError::InsufficientPayload { needed, available } => {
                write!(f, "Insufficient data for payload: need {} bytes, have {}", needed, available)
            }
            RecordParseError::InvalidType(t) => {
                write!(f, "Invalid record type: 0x{:02X}", t)
            }
            RecordParseError::PayloadTooLarge { length, max } => {
                write!(f, "Payload too large: {} bytes (max: {})", length, max)
            }
        }
    }
}

impl std::error::Error for RecordParseError {}

impl From<RecordParseError> for ClientError {
    fn from(e: RecordParseError) -> Self {
        ClientError::ProtocolError(e.to_string())
    }
}

/// Configuration for record parsing
#[derive(Debug, Clone)]
pub struct RecordParserConfig {
    /// Maximum allowed payload size (default: 16MB)
    pub max_payload_size: u32,
    /// Skip unknown record types instead of erroring
    pub skip_unknown: bool,
}

impl Default for RecordParserConfig {
    fn default() -> Self {
        Self {
            max_payload_size: 16 * 1024 * 1024, // 16MB
            skip_unknown: true,
        }
    }
}

/// Iterator over TLV records in a byte buffer
pub struct RecordIterator {
    data: Bytes,
    offset: usize,
    config: RecordParserConfig,
}

impl RecordIterator {
    /// Create a new record iterator with default config
    pub fn new(data: Bytes) -> Self {
        Self {
            data,
            offset: 0,
            config: RecordParserConfig::default(),
        }
    }

    /// Create a new record iterator with custom config
    pub fn with_config(data: Bytes, config: RecordParserConfig) -> Self {
        Self {
            data,
            offset: 0,
            config,
        }
    }

    /// Get current offset in the data
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get remaining bytes
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    /// Parse a single record at the current position
    fn parse_record(&mut self) -> std::result::Result<Option<Record>, RecordParseError> {
        let remaining = self.remaining();
        
        // Check if we have enough for header
        if remaining == 0 {
            return Ok(None);
        }
        
        if remaining < TLV_HEADER_SIZE {
            return Err(RecordParseError::InsufficientHeader {
                needed: TLV_HEADER_SIZE,
                available: remaining,
            });
        }

        let start_offset = self.offset;
        
        // Parse type byte
        let type_byte = self.data[self.offset];
        let record_type = RecordType::from(type_byte);
        
        // Parse length (4 bytes little-endian per Architecture §2.2)
        let length = u32::from_le_bytes([
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
            self.data[self.offset + 4],
        ]);

        // Validate length
        if length > self.config.max_payload_size {
            return Err(RecordParseError::PayloadTooLarge {
                length,
                max: self.config.max_payload_size,
            });
        }

        let payload_len = length as usize;
        
        // Check if we have enough for payload
        if remaining < TLV_HEADER_SIZE + payload_len {
            return Err(RecordParseError::InsufficientPayload {
                needed: payload_len,
                available: remaining - TLV_HEADER_SIZE,
            });
        }

        // Extract payload (zero-copy slice)
        let payload_start = self.offset + TLV_HEADER_SIZE;
        let payload_end = payload_start + payload_len;
        let payload = self.data.slice(payload_start..payload_end);

        // Advance offset
        self.offset = payload_end;

        Ok(Some(Record::new(record_type, type_byte, payload, start_offset)))
    }
}

impl Iterator for RecordIterator {
    type Item = std::result::Result<Record, RecordParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.parse_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Parse all records from a byte buffer
pub fn parse_records(data: Bytes) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    for result in RecordIterator::new(data) {
        records.push(result?);
    }
    Ok(records)
}

/// Parse a single record from a byte buffer
pub fn parse_record(data: &[u8]) -> std::result::Result<(Record, usize), RecordParseError> {
    if data.len() < TLV_HEADER_SIZE {
        return Err(RecordParseError::InsufficientHeader {
            needed: TLV_HEADER_SIZE,
            available: data.len(),
        });
    }

    let type_byte = data[0];
    let record_type = RecordType::from(type_byte);
    
    let length = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
    let payload_len = length as usize;

    if data.len() < TLV_HEADER_SIZE + payload_len {
        return Err(RecordParseError::InsufficientPayload {
            needed: payload_len,
            available: data.len() - TLV_HEADER_SIZE,
        });
    }

    let payload = Bytes::copy_from_slice(&data[TLV_HEADER_SIZE..TLV_HEADER_SIZE + payload_len]);
    let total_size = TLV_HEADER_SIZE + payload_len;

    Ok((Record::new(record_type, type_byte, payload, 0), total_size))
}

/// Encode a record to TLV format
pub fn encode_record(record_type: RecordType, payload: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(TLV_HEADER_SIZE + payload.len());
    
    // Type byte
    buf.push(record_type as u8);
    
    // Length (4 bytes little-endian per Architecture §2.2)
    let length = payload.len() as u32;
    buf.extend_from_slice(&length.to_le_bytes());
    
    // Payload
    buf.extend_from_slice(payload);
    
    Bytes::from(buf)
}

/// Encode a record with custom type byte
pub fn encode_record_with_type(type_byte: u8, payload: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(TLV_HEADER_SIZE + payload.len());
    
    buf.push(type_byte);
    let length = payload.len() as u32;
    buf.extend_from_slice(&length.to_le_bytes());
    buf.extend_from_slice(payload);
    
    Bytes::from(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_type_from_byte() {
        assert_eq!(RecordType::from(0x01), RecordType::Data);
        assert_eq!(RecordType::from(0x02), RecordType::Tombstone);
        assert_eq!(RecordType::from(0x80), RecordType::UserDefined);
        assert_eq!(RecordType::from(0x00), RecordType::Unknown);
    }

    #[test]
    fn test_encode_decode_record() {
        let payload = b"hello world";
        let encoded = encode_record(RecordType::Data, payload);
        
        assert_eq!(encoded.len(), TLV_HEADER_SIZE + payload.len());
        assert_eq!(encoded[0], 0x01); // Data type
        
        let (record, size) = parse_record(&encoded).unwrap();
        assert_eq!(size, encoded.len());
        assert_eq!(record.record_type, RecordType::Data);
        assert_eq!(record.payload.as_ref(), payload);
    }

    #[test]
    fn test_record_iterator() {
        // Create multiple records
        let mut data = Vec::new();
        data.extend_from_slice(&encode_record(RecordType::Data, b"record1"));
        data.extend_from_slice(&encode_record(RecordType::Data, b"record2"));
        data.extend_from_slice(&encode_record(RecordType::Tombstone, b""));
        
        let records: Vec<_> = RecordIterator::new(Bytes::from(data))
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].as_str(), Some("record1"));
        assert_eq!(records[1].as_str(), Some("record2"));
        assert!(records[2].is_tombstone());
    }

    #[test]
    fn test_insufficient_header() {
        let data = Bytes::from(vec![0x01, 0x00]); // Only 2 bytes
        let mut iter = RecordIterator::new(data);
        
        let result = iter.next();
        assert!(matches!(result, Some(Err(RecordParseError::InsufficientHeader { .. }))));
    }

    #[test]
    fn test_insufficient_payload() {
        // Header says 100 bytes but only 5 provided
        // Length is little-endian: 100 = 0x64 0x00 0x00 0x00
        let mut data = vec![0x01, 0x64, 0x00, 0x00, 0x00]; // type + length (100 LE)
        data.extend_from_slice(b"short"); // Only 5 bytes
        
        let mut iter = RecordIterator::new(Bytes::from(data));
        let result = iter.next();
        assert!(matches!(result, Some(Err(RecordParseError::InsufficientPayload { .. }))));
    }

    #[test]
    fn test_empty_record() {
        let encoded = encode_record(RecordType::Tombstone, b"");
        let (record, _) = parse_record(&encoded).unwrap();
        
        assert!(record.is_tombstone());
        assert!(record.payload.is_empty());
    }

    #[test]
    fn test_record_offset_tracking() {
        let mut data = Vec::new();
        let rec1 = encode_record(RecordType::Data, b"first");
        let rec2 = encode_record(RecordType::Data, b"second");
        data.extend_from_slice(&rec1);
        data.extend_from_slice(&rec2);
        
        let records: Vec<_> = RecordIterator::new(Bytes::from(data))
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, rec1.len());
    }
}
