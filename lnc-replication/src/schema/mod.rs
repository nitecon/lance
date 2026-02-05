//! FlatBuffers schema definitions for LANCE replication protocol.
//!
//! The actual `.fbs` schema file is located at `schema/replication.fbs`.
//! This module provides documentation and constants related to the schema.

/// Schema version for compatibility checking.
pub const SCHEMA_VERSION: u32 = 1;

/// Maximum size for a single replication message (16 MiB).
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Maximum number of log entries in a single AppendEntries request.
pub const MAX_ENTRIES_PER_REQUEST: usize = 1024;

/// Maximum snapshot chunk size (1 MiB).
pub const MAX_SNAPSHOT_CHUNK_SIZE: usize = 1024 * 1024;
