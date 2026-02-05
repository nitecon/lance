//! Audit Logging Pipeline for TEE'd Write Data
//!
//! Per Architecture.md Section 21.9: This module provides audit logging capabilities
//! for write operations that are duplicated via TEE. The audit log captures all
//! write operations for compliance, debugging, and analytics purposes.
//!
//! # Design Goals
//! - Zero-copy: Use TEE'd pipe data directly without additional copies
//! - Non-blocking: Audit writes must not block the hot path
//! - Durable: Audit logs are written to separate storage for retention
//! - Structured: Binary format with efficient encoding

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Audit Log Entry Format
// ============================================================================

/// Audit log entry header - fixed size for efficient parsing
/// 
/// Layout (48 bytes total):
/// - magic: 4 bytes (0x41 0x55 0x44 0x54 = "AUDT")
/// - version: 1 byte
/// - flags: 1 byte
/// - reserved: 2 bytes
/// - timestamp_ns: 8 bytes (nanoseconds since epoch)
/// - topic_id: 8 bytes
/// - batch_id: 8 bytes
/// - payload_len: 4 bytes
/// - crc32: 4 bytes (of header + payload)
/// - node_id: 2 bytes
/// - operation: 1 byte
/// - padding: 5 bytes
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy)]
pub struct AuditEntryHeader {
    pub magic: [u8; 4],
    pub version: u8,
    pub flags: u8,
    pub reserved: [u8; 2],
    pub timestamp_ns: u64,
    pub topic_id: u64,
    pub batch_id: u64,
    pub payload_len: u32,
    pub crc32: u32,
    pub node_id: u16,
    pub operation: u8,
    pub padding: [u8; 5],
}

impl AuditEntryHeader {
    pub const MAGIC: [u8; 4] = [0x41, 0x55, 0x44, 0x54]; // "AUDT"
    pub const VERSION: u8 = 1;
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Create a new audit entry header
    pub fn new(topic_id: u64, batch_id: u64, payload_len: u32, node_id: u16, operation: AuditOperation) -> Self {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            flags: 0,
            reserved: [0; 2],
            timestamp_ns,
            topic_id,
            batch_id,
            payload_len,
            crc32: 0, // Computed after payload is known
            node_id,
            operation: operation as u8,
            padding: [0; 5],
        }
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        // SAFETY: AuditEntryHeader is #[repr(C)] with known layout
        unsafe { std::mem::transmute_copy(self) }
    }
}

/// Audit operation types
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOperation {
    /// Write/Ingest operation
    Write = 1,
    /// Delete operation
    Delete = 2,
    /// Topic creation
    TopicCreate = 3,
    /// Topic deletion
    TopicDelete = 4,
    /// Configuration change
    ConfigChange = 5,
}

// ============================================================================
// Audit Log Configuration
// ============================================================================

/// Configuration for audit logging
#[derive(Debug, Clone)]
pub struct AuditConfig {
    /// Enable audit logging
    pub enabled: bool,
    /// Directory for audit log files
    pub log_dir: PathBuf,
    /// Maximum size per audit log file before rotation (bytes)
    pub max_file_size: u64,
    /// Maximum number of audit log files to retain
    pub max_files: u32,
    /// Sync mode: true for fsync after each write, false for buffered
    pub sync_writes: bool,
    /// Include payload data in audit log (may impact performance)
    pub include_payload: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_dir: PathBuf::from("/var/log/lance/audit"),
            max_file_size: 100 * 1024 * 1024, // 100 MB
            max_files: 10,
            sync_writes: false,
            include_payload: false,
        }
    }
}

impl AuditConfig {
    /// Enable audit logging
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set audit log directory
    pub fn with_log_dir(mut self, dir: PathBuf) -> Self {
        self.log_dir = dir;
        self
    }

    /// Enable sync writes for durability
    pub fn with_sync_writes(mut self, sync: bool) -> Self {
        self.sync_writes = sync;
        self
    }

    /// Include payload data in audit entries
    pub fn with_include_payload(mut self, include: bool) -> Self {
        self.include_payload = include;
        self
    }
}

// ============================================================================
// Audit Log Writer
// ============================================================================

/// Errors from audit logging operations
#[derive(Debug)]
pub enum AuditError {
    /// I/O error writing audit log
    IoError(io::Error),
    /// Audit logging is disabled
    Disabled,
    /// Log directory does not exist
    DirectoryNotFound(PathBuf),
    /// Failed to rotate log file
    RotationFailed(String),
}

impl std::fmt::Display for AuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(e) => write!(f, "Audit I/O error: {}", e),
            Self::Disabled => write!(f, "Audit logging is disabled"),
            Self::DirectoryNotFound(p) => write!(f, "Audit log directory not found: {}", p.display()),
            Self::RotationFailed(msg) => write!(f, "Log rotation failed: {}", msg),
        }
    }
}

impl std::error::Error for AuditError {}

impl From<io::Error> for AuditError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e)
    }
}

/// Audit log writer for recording TEE'd write operations
///
/// This writer receives data from the TEE pipeline and writes structured
/// audit entries to disk for compliance and analytics.
pub struct AuditLogWriter {
    config: AuditConfig,
    /// Current audit log file
    current_file: Option<std::fs::File>,
    /// Current file size
    current_size: u64,
    /// File sequence number
    file_sequence: u32,
    /// Node ID for this writer
    node_id: u16,
    /// Metrics: total entries written
    entries_written: AtomicU64,
    /// Metrics: total bytes written
    bytes_written: AtomicU64,
    /// Metrics: write errors
    write_errors: AtomicU64,
}

impl AuditLogWriter {
    /// Create a new audit log writer
    pub fn new(config: AuditConfig, node_id: u16) -> Result<Self, AuditError> {
        if !config.enabled {
            return Ok(Self {
                config,
                current_file: None,
                current_size: 0,
                file_sequence: 0,
                node_id,
                entries_written: AtomicU64::new(0),
                bytes_written: AtomicU64::new(0),
                write_errors: AtomicU64::new(0),
            });
        }

        // Ensure log directory exists
        if !config.log_dir.exists() {
            std::fs::create_dir_all(&config.log_dir)?;
        }

        Ok(Self {
            config,
            current_file: None,
            current_size: 0,
            file_sequence: 0,
            node_id,
            entries_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        })
    }

    /// Write an audit entry for a TEE'd write operation
    pub fn write_entry(
        &mut self,
        topic_id: u64,
        batch_id: u64,
        payload: Option<&[u8]>,
    ) -> Result<(), AuditError> {
        if !self.config.enabled {
            return Ok(());
        }

        let payload_len = if self.config.include_payload {
            payload.map(|p| p.len() as u32).unwrap_or(0)
        } else {
            0
        };

        let header = AuditEntryHeader::new(
            topic_id,
            batch_id,
            payload_len,
            self.node_id,
            AuditOperation::Write,
        );

        self.write_raw_entry(&header, if self.config.include_payload { payload } else { None })
    }

    /// Write a raw audit entry with header and optional payload
    fn write_raw_entry(
        &mut self,
        header: &AuditEntryHeader,
        payload: Option<&[u8]>,
    ) -> Result<(), AuditError> {
        // Ensure we have an open file
        self.ensure_file_open()?;

        let file = self.current_file.as_mut().ok_or(AuditError::Disabled)?;

        // Write header
        let header_bytes = header.to_bytes();
        file.write_all(&header_bytes)?;

        // Write payload if included
        if let Some(data) = payload {
            file.write_all(data)?;
        }

        // Sync if configured
        if self.config.sync_writes {
            file.sync_data()?;
        }

        // Update metrics
        let entry_size = AuditEntryHeader::SIZE as u64 + payload.map(|p| p.len()).unwrap_or(0) as u64;
        self.current_size += entry_size;
        self.entries_written.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(entry_size, Ordering::Relaxed);

        // Check for rotation
        if self.current_size >= self.config.max_file_size {
            self.rotate_file()?;
        }

        Ok(())
    }

    /// Ensure the current log file is open
    fn ensure_file_open(&mut self) -> Result<(), AuditError> {
        if self.current_file.is_some() {
            return Ok(());
        }

        let filename = format!("audit_{:08}.log", self.file_sequence);
        let path = self.config.log_dir.join(filename);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        self.current_file = Some(file);
        self.current_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        Ok(())
    }

    /// Rotate the current log file
    fn rotate_file(&mut self) -> Result<(), AuditError> {
        // Close current file
        self.current_file = None;
        self.current_size = 0;
        self.file_sequence += 1;

        // Clean up old files if needed
        self.cleanup_old_files()?;

        Ok(())
    }

    /// Remove old log files beyond retention limit
    fn cleanup_old_files(&self) -> Result<(), AuditError> {
        if self.file_sequence < self.config.max_files {
            return Ok(());
        }

        let oldest_to_keep = self.file_sequence - self.config.max_files;
        for seq in 0..oldest_to_keep {
            let filename = format!("audit_{:08}.log", seq);
            let path = self.config.log_dir.join(filename);
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
        }

        Ok(())
    }

    /// Get total entries written
    pub fn entries_written(&self) -> u64 {
        self.entries_written.load(Ordering::Relaxed)
    }

    /// Get total bytes written
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get write error count
    pub fn write_errors(&self) -> u64 {
        self.write_errors.load(Ordering::Relaxed)
    }

    /// Check if audit logging is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry_header_size() {
        assert_eq!(AuditEntryHeader::SIZE, 48);
    }

    #[test]
    fn test_audit_entry_header_magic() {
        let header = AuditEntryHeader::new(1, 100, 256, 1, AuditOperation::Write);
        assert_eq!(header.magic, AuditEntryHeader::MAGIC);
        assert_eq!(header.version, AuditEntryHeader::VERSION);
    }

    #[test]
    fn test_audit_config_default() {
        let config = AuditConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_file_size, 100 * 1024 * 1024);
        assert_eq!(config.max_files, 10);
        assert!(!config.sync_writes);
        assert!(!config.include_payload);
    }

    #[test]
    fn test_audit_config_builder() {
        let config = AuditConfig::default()
            .with_enabled(true)
            .with_sync_writes(true)
            .with_include_payload(true);
        
        assert!(config.enabled);
        assert!(config.sync_writes);
        assert!(config.include_payload);
    }

    #[test]
    fn test_audit_writer_disabled() {
        let config = AuditConfig::default();
        let mut writer = AuditLogWriter::new(config, 1).unwrap();
        
        // Should succeed but do nothing
        let result = writer.write_entry(1, 100, Some(b"test data"));
        assert!(result.is_ok());
        assert_eq!(writer.entries_written(), 0);
    }

    #[test]
    fn test_audit_writer_enabled() {
        let temp_dir = std::env::temp_dir().join("lance_audit_test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let config = AuditConfig::default()
            .with_enabled(true)
            .with_log_dir(temp_dir.clone());
        
        let mut writer = AuditLogWriter::new(config, 1).unwrap();
        
        // Write an entry
        writer.write_entry(1, 100, None).unwrap();
        assert_eq!(writer.entries_written(), 1);
        assert_eq!(writer.bytes_written(), AuditEntryHeader::SIZE as u64);

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_audit_error_display() {
        let err = AuditError::Disabled;
        assert!(err.to_string().contains("disabled"));

        let err = AuditError::DirectoryNotFound(PathBuf::from("/tmp/test"));
        assert!(err.to_string().contains("/tmp/test"));
    }

    #[test]
    fn test_audit_operation_values() {
        assert_eq!(AuditOperation::Write as u8, 1);
        assert_eq!(AuditOperation::Delete as u8, 2);
        assert_eq!(AuditOperation::TopicCreate as u8, 3);
    }
}
