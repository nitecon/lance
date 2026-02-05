//! Client-side offset persistence for LANCE consumers
//!
//! Provides traits and implementations for storing consumer offsets locally,
//! enabling stateless broker architecture per the Client-Managed Offset Strategy.
//!
//! # Design Philosophy
//!
//! LANCE treats the broker as a stateless data pipe. Offset tracking is the
//! client's responsibility, enabling:
//! - Exactly-once semantics (store offset with business data atomically)
//! - Stateless horizontal scaling
//! - Reduced broker metadata overhead
//!
//! # Example
//!
//! ```ignore
//! use lnc_client::offset::{OffsetStore, LockFileOffsetStore};
//! use std::path::Path;
//!
//! let store = LockFileOffsetStore::open(
//!     Path::new("/var/lib/lance/offsets"),
//!     "my-consumer",
//! )?;
//!
//! // Load stored offset for a topic
//! let offset = store.load(topic_id, consumer_id).unwrap_or(0);
//!
//! // After processing, save the new offset
//! store.save(topic_id, consumer_id, new_offset)?;
//! ```

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::error::{ClientError, Result};

/// Trait for client-side offset persistence
///
/// Implementations store consumer offsets locally, allowing consumers to resume
/// from their last position after restarts without server-side state.
pub trait OffsetStore: Send + Sync {
    /// Load the stored offset for a topic and consumer
    ///
    /// Returns `None` if no offset has been stored (first run).
    fn load(&self, topic_id: u32, consumer_id: u64) -> Result<Option<u64>>;

    /// Save the current offset for a topic and consumer
    ///
    /// This should be called after successfully processing records.
    fn save(&self, topic_id: u32, consumer_id: u64, offset: u64) -> Result<()>;

    /// Delete stored offset for a topic and consumer
    ///
    /// Use when resetting consumer position or cleaning up.
    fn delete(&self, topic_id: u32, consumer_id: u64) -> Result<()>;

    /// List all stored offsets
    ///
    /// Returns a map of (topic_id, consumer_id) -> offset
    fn list_all(&self) -> Result<HashMap<(u32, u64), u64>>;
}

/// In-memory offset store for testing and ephemeral consumers
///
/// Offsets are lost when the process exits. Use for testing or when
/// offset persistence is handled externally (e.g., in a database transaction).
#[derive(Debug, Default, Clone)]
pub struct MemoryOffsetStore {
    offsets: Arc<RwLock<HashMap<(u32, u64), u64>>>,
}

impl MemoryOffsetStore {
    /// Create a new in-memory offset store
    pub fn new() -> Self {
        Self {
            offsets: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl OffsetStore for MemoryOffsetStore {
    fn load(&self, topic_id: u32, consumer_id: u64) -> Result<Option<u64>> {
        let offsets = self.offsets.read().map_err(|e| {
            ClientError::IoError(std::io::Error::other(
                format!("Lock poisoned: {}", e),
            ))
        })?;
        Ok(offsets.get(&(topic_id, consumer_id)).copied())
    }

    fn save(&self, topic_id: u32, consumer_id: u64, offset: u64) -> Result<()> {
        let mut offsets = self.offsets.write().map_err(|e| {
            ClientError::IoError(std::io::Error::other(
                format!("Lock poisoned: {}", e),
            ))
        })?;
        offsets.insert((topic_id, consumer_id), offset);
        Ok(())
    }

    fn delete(&self, topic_id: u32, consumer_id: u64) -> Result<()> {
        let mut offsets = self.offsets.write().map_err(|e| {
            ClientError::IoError(std::io::Error::other(
                format!("Lock poisoned: {}", e),
            ))
        })?;
        offsets.remove(&(topic_id, consumer_id));
        Ok(())
    }

    fn list_all(&self) -> Result<HashMap<(u32, u64), u64>> {
        let offsets = self.offsets.read().map_err(|e| {
            ClientError::IoError(std::io::Error::other(
                format!("Lock poisoned: {}", e),
            ))
        })?;
        Ok(offsets.clone())
    }
}

/// File-based offset store with lock file protection
///
/// Stores offsets in individual files under a base directory:
/// ```text
/// {base_dir}/{consumer_name}/
/// ├── topic-{topic_id}-consumer-{consumer_id}.offset
/// └── ...
/// ```
///
/// Uses file locking (flock on Unix, LockFile on Windows) to ensure
/// safe concurrent access from multiple processes.
#[derive(Debug)]
pub struct LockFileOffsetStore {
    base_dir: PathBuf,
    consumer_name: String,
    cache: RwLock<HashMap<(u32, u64), u64>>,
}

impl LockFileOffsetStore {
    /// Open or create an offset store at the given directory
    ///
    /// # Arguments
    /// * `base_dir` - Base directory for offset files
    /// * `consumer_name` - Name for this consumer instance (subdirectory name)
    ///
    /// # Example
    /// ```ignore
    /// let store = LockFileOffsetStore::open(
    ///     Path::new("/var/lib/lance/offsets"),
    ///     "my-service",
    /// )?;
    /// ```
    pub fn open(base_dir: &Path, consumer_name: &str) -> Result<Self> {
        let dir = base_dir.join(consumer_name);
        fs::create_dir_all(&dir).map_err(ClientError::IoError)?;

        let store = Self {
            base_dir: dir,
            consumer_name: consumer_name.to_string(),
            cache: RwLock::new(HashMap::new()),
        };

        // Pre-load existing offsets into cache
        store.load_all_into_cache()?;

        Ok(store)
    }

    /// Get the file path for a specific topic/consumer offset
    fn offset_file_path(&self, topic_id: u32, consumer_id: u64) -> PathBuf {
        self.base_dir
            .join(format!("topic-{}-consumer-{}.offset", topic_id, consumer_id))
    }

    /// Get the lock file path for a specific topic/consumer
    fn lock_file_path(&self, topic_id: u32, consumer_id: u64) -> PathBuf {
        self.base_dir
            .join(format!("topic-{}-consumer-{}.lock", topic_id, consumer_id))
    }

    /// Load all existing offsets into the cache
    fn load_all_into_cache(&self) -> Result<()> {
        let mut cache = self.cache.write().map_err(|e| {
            ClientError::IoError(std::io::Error::other(
                format!("Lock poisoned: {}", e),
            ))
        })?;

        let entries = match fs::read_dir(&self.base_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(ClientError::IoError(e)),
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".offset") {
                    // Parse topic-{id}-consumer-{id}.offset
                    if let Some((topic_id, consumer_id)) = Self::parse_offset_filename(name) {
                        if let Ok(offset) = Self::read_offset_file(&path) {
                            cache.insert((topic_id, consumer_id), offset);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Parse offset filename to extract topic_id and consumer_id
    fn parse_offset_filename(name: &str) -> Option<(u32, u64)> {
        // Format: topic-{topic_id}-consumer-{consumer_id}.offset
        let name = name.strip_suffix(".offset")?;
        let parts: Vec<&str> = name.split('-').collect();
        if parts.len() >= 4 && parts[0] == "topic" && parts[2] == "consumer" {
            let topic_id: u32 = parts[1].parse().ok()?;
            let consumer_id: u64 = parts[3].parse().ok()?;
            Some((topic_id, consumer_id))
        } else {
            None
        }
    }

    /// Read offset from a file
    fn read_offset_file(path: &Path) -> Result<u64> {
        let mut file = File::open(path).map_err(ClientError::IoError)?;
        let mut content = String::new();
        file.read_to_string(&mut content)
            .map_err(ClientError::IoError)?;
        content
            .trim()
            .parse()
            .map_err(|e| ClientError::IoError(std::io::Error::other(e)))
    }

    /// Write offset to a file atomically
    fn write_offset_file(&self, topic_id: u32, consumer_id: u64, offset: u64) -> Result<()> {
        let path = self.offset_file_path(topic_id, consumer_id);
        let lock_path = self.lock_file_path(topic_id, consumer_id);
        let temp_path = path.with_extension("offset.tmp");

        // Acquire lock file
        let lock_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&lock_path)
            .map_err(ClientError::IoError)?;

        // Platform-specific file locking
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = lock_file.as_raw_fd();
            // SAFETY: fd is valid file descriptor, flock with LOCK_EX is safe
            unsafe {
                if libc::flock(fd, libc::LOCK_EX) != 0 {
                    return Err(ClientError::IoError(std::io::Error::last_os_error()));
                }
            }
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Foundation::HANDLE;
            use windows_sys::Win32::Storage::FileSystem::{
                LockFileEx, LOCKFILE_EXCLUSIVE_LOCK,
            };
            let handle = lock_file.as_raw_handle() as HANDLE;
            // SAFETY: handle is valid from as_raw_handle, OVERLAPPED is zeroed correctly
            unsafe {
                let mut overlapped = std::mem::zeroed::<windows_sys::Win32::System::IO::OVERLAPPED>();
                if LockFileEx(handle, LOCKFILE_EXCLUSIVE_LOCK, 0, u32::MAX, u32::MAX, &mut overlapped) == 0 {
                    return Err(ClientError::IoError(std::io::Error::last_os_error()));
                }
            }
        }

        // Write to temp file
        let mut temp_file = File::create(&temp_path).map_err(ClientError::IoError)?;
        writeln!(temp_file, "{}", offset).map_err(ClientError::IoError)?;
        temp_file.sync_all().map_err(ClientError::IoError)?;
        drop(temp_file);

        // Atomic rename
        fs::rename(&temp_path, &path).map_err(ClientError::IoError)?;

        // Lock is released when lock_file is dropped
        drop(lock_file);

        Ok(())
    }

    /// Get the consumer name
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Get the base directory
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

impl OffsetStore for LockFileOffsetStore {
    fn load(&self, topic_id: u32, consumer_id: u64) -> Result<Option<u64>> {
        // Check cache first
        {
            let cache = self.cache.read().map_err(|e| {
                ClientError::IoError(std::io::Error::other(
                    format!("Lock poisoned: {}", e),
                ))
            })?;
            if let Some(&offset) = cache.get(&(topic_id, consumer_id)) {
                return Ok(Some(offset));
            }
        }

        // Try to read from file
        let path = self.offset_file_path(topic_id, consumer_id);
        if !path.exists() {
            return Ok(None);
        }

        let offset = Self::read_offset_file(&path)?;

        // Update cache
        {
            let mut cache = self.cache.write().map_err(|e| {
                ClientError::IoError(std::io::Error::other(
                    format!("Lock poisoned: {}", e),
                ))
            })?;
            cache.insert((topic_id, consumer_id), offset);
        }

        Ok(Some(offset))
    }

    fn save(&self, topic_id: u32, consumer_id: u64, offset: u64) -> Result<()> {
        // Write to file
        self.write_offset_file(topic_id, consumer_id, offset)?;

        // Update cache
        {
            let mut cache = self.cache.write().map_err(|e| {
                ClientError::IoError(std::io::Error::other(
                    format!("Lock poisoned: {}", e),
                ))
            })?;
            cache.insert((topic_id, consumer_id), offset);
        }

        Ok(())
    }

    fn delete(&self, topic_id: u32, consumer_id: u64) -> Result<()> {
        let path = self.offset_file_path(topic_id, consumer_id);
        let lock_path = self.lock_file_path(topic_id, consumer_id);

        // Remove from cache
        {
            let mut cache = self.cache.write().map_err(|e| {
                ClientError::IoError(std::io::Error::other(
                    format!("Lock poisoned: {}", e),
                ))
            })?;
            cache.remove(&(topic_id, consumer_id));
        }

        // Delete files (ignore if not found)
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&lock_path);

        Ok(())
    }

    fn list_all(&self) -> Result<HashMap<(u32, u64), u64>> {
        let cache = self.cache.read().map_err(|e| {
            ClientError::IoError(std::io::Error::other(
                format!("Lock poisoned: {}", e),
            ))
        })?;
        Ok(cache.clone())
    }
}

/// Information about a committed offset
#[derive(Debug, Clone)]
pub struct CommitInfo {
    /// Topic ID
    pub topic_id: u32,
    /// Consumer ID
    pub consumer_id: u64,
    /// Committed offset
    pub offset: u64,
    /// Previous offset (if known)
    pub previous_offset: Option<u64>,
    /// Timestamp of commit
    pub timestamp: std::time::SystemTime,
}

/// Trait for post-commit hooks
///
/// Implement this trait to receive notifications after offsets are committed.
/// Use cases include: external offset persistence, metrics, audit logging.
pub trait PostCommitHook: Send + Sync {
    /// Called after an offset is successfully committed
    ///
    /// This method should not block for extended periods. For expensive
    /// operations, consider spawning a task or using a channel.
    fn on_commit(&self, info: &CommitInfo) -> Result<()>;
}

/// Offset store wrapper that invokes post-commit hooks
///
/// Wraps an inner `OffsetStore` and calls registered hooks after each commit.
pub struct HookedOffsetStore<S: OffsetStore> {
    inner: S,
    hooks: Vec<Arc<dyn PostCommitHook>>,
    previous_offsets: RwLock<HashMap<(u32, u64), u64>>,
}

impl<S: OffsetStore> HookedOffsetStore<S> {
    /// Create a new hooked offset store
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            hooks: Vec::new(),
            previous_offsets: RwLock::new(HashMap::new()),
        }
    }

    /// Add a post-commit hook
    pub fn add_hook(mut self, hook: Arc<dyn PostCommitHook>) -> Self {
        self.hooks.push(hook);
        self
    }

    /// Add multiple post-commit hooks
    pub fn with_hooks(mut self, hooks: Vec<Arc<dyn PostCommitHook>>) -> Self {
        self.hooks.extend(hooks);
        self
    }

    /// Get the inner store
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Invoke all registered hooks
    fn invoke_hooks(&self, info: &CommitInfo) -> Result<()> {
        for hook in &self.hooks {
            hook.on_commit(info)?;
        }
        Ok(())
    }
}

impl<S: OffsetStore> OffsetStore for HookedOffsetStore<S> {
    fn load(&self, topic_id: u32, consumer_id: u64) -> Result<Option<u64>> {
        let offset = self.inner.load(topic_id, consumer_id)?;
        
        // Track for previous_offset in commits
        if let Some(off) = offset {
            if let Ok(mut prev) = self.previous_offsets.write() {
                prev.insert((topic_id, consumer_id), off);
            }
        }
        
        Ok(offset)
    }

    fn save(&self, topic_id: u32, consumer_id: u64, offset: u64) -> Result<()> {
        // Get previous offset
        let previous_offset = self.previous_offsets.read()
            .ok()
            .and_then(|prev| prev.get(&(topic_id, consumer_id)).copied());

        // Save to inner store
        self.inner.save(topic_id, consumer_id, offset)?;

        // Update previous offset tracking
        if let Ok(mut prev) = self.previous_offsets.write() {
            prev.insert((topic_id, consumer_id), offset);
        }

        // Invoke hooks
        let info = CommitInfo {
            topic_id,
            consumer_id,
            offset,
            previous_offset,
            timestamp: std::time::SystemTime::now(),
        };
        self.invoke_hooks(&info)?;

        Ok(())
    }

    fn delete(&self, topic_id: u32, consumer_id: u64) -> Result<()> {
        // Remove from previous tracking
        if let Ok(mut prev) = self.previous_offsets.write() {
            prev.remove(&(topic_id, consumer_id));
        }
        
        self.inner.delete(topic_id, consumer_id)
    }

    fn list_all(&self) -> Result<HashMap<(u32, u64), u64>> {
        self.inner.list_all()
    }
}

/// A simple logging hook for debugging
#[derive(Debug, Default)]
pub struct LoggingCommitHook;

impl PostCommitHook for LoggingCommitHook {
    fn on_commit(&self, info: &CommitInfo) -> Result<()> {
        tracing::debug!(
            topic_id = info.topic_id,
            consumer_id = info.consumer_id,
            offset = info.offset,
            previous = ?info.previous_offset,
            "Offset committed"
        );
        Ok(())
    }
}

/// A hook that collects commit events for testing
#[derive(Debug, Default)]
pub struct CollectingCommitHook {
    commits: RwLock<Vec<CommitInfo>>,
}

impl CollectingCommitHook {
    /// Create a new collecting hook
    pub fn new() -> Self {
        Self::default()
    }

    /// Get all collected commits
    pub fn commits(&self) -> Vec<CommitInfo> {
        self.commits.read()
            .map(|c| c.clone())
            .unwrap_or_default()
    }

    /// Clear collected commits
    pub fn clear(&self) {
        if let Ok(mut commits) = self.commits.write() {
            commits.clear();
        }
    }
}

impl PostCommitHook for CollectingCommitHook {
    fn on_commit(&self, info: &CommitInfo) -> Result<()> {
        if let Ok(mut commits) = self.commits.write() {
            commits.push(info.clone());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_memory_offset_store() {
        let store = MemoryOffsetStore::new();

        // Initially empty
        assert!(store.load(1, 100).unwrap().is_none());

        // Save and load
        store.save(1, 100, 42).unwrap();
        assert_eq!(store.load(1, 100).unwrap(), Some(42));

        // Update
        store.save(1, 100, 100).unwrap();
        assert_eq!(store.load(1, 100).unwrap(), Some(100));

        // Different topic/consumer
        store.save(2, 200, 999).unwrap();
        assert_eq!(store.load(2, 200).unwrap(), Some(999));
        assert_eq!(store.load(1, 100).unwrap(), Some(100));

        // List all
        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get(&(1, 100)), Some(&100));
        assert_eq!(all.get(&(2, 200)), Some(&999));

        // Delete
        store.delete(1, 100).unwrap();
        assert!(store.load(1, 100).unwrap().is_none());
        assert_eq!(store.load(2, 200).unwrap(), Some(999));
    }

    #[test]
    fn test_lock_file_offset_store() {
        let temp_dir = TempDir::new().unwrap();
        let store = LockFileOffsetStore::open(temp_dir.path(), "test-consumer").unwrap();

        // Initially empty
        assert!(store.load(1, 100).unwrap().is_none());

        // Save and load
        store.save(1, 100, 12345).unwrap();
        assert_eq!(store.load(1, 100).unwrap(), Some(12345));

        // Verify file exists
        let file_path = store.offset_file_path(1, 100);
        assert!(file_path.exists());

        // Read file content
        let content = fs::read_to_string(&file_path).unwrap();
        assert_eq!(content.trim(), "12345");

        // Update
        store.save(1, 100, 67890).unwrap();
        assert_eq!(store.load(1, 100).unwrap(), Some(67890));

        // Delete
        store.delete(1, 100).unwrap();
        assert!(store.load(1, 100).unwrap().is_none());
        assert!(!file_path.exists());
    }

    #[test]
    fn test_lock_file_offset_store_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Create store and save offset
        {
            let store = LockFileOffsetStore::open(temp_dir.path(), "persist-test").unwrap();
            store.save(5, 500, 99999).unwrap();
        }

        // Reopen and verify offset is still there
        {
            let store = LockFileOffsetStore::open(temp_dir.path(), "persist-test").unwrap();
            assert_eq!(store.load(5, 500).unwrap(), Some(99999));
        }
    }

    #[test]
    fn test_parse_offset_filename() {
        assert_eq!(
            LockFileOffsetStore::parse_offset_filename("topic-1-consumer-100.offset"),
            Some((1, 100))
        );
        assert_eq!(
            LockFileOffsetStore::parse_offset_filename("topic-999-consumer-12345.offset"),
            Some((999, 12345))
        );
        assert_eq!(
            LockFileOffsetStore::parse_offset_filename("invalid.offset"),
            None
        );
        assert_eq!(
            LockFileOffsetStore::parse_offset_filename("topic-abc-consumer-100.offset"),
            None
        );
    }

    #[test]
    fn test_hooked_offset_store() {
        let inner = MemoryOffsetStore::new();
        let hook = Arc::new(CollectingCommitHook::new());
        let store = HookedOffsetStore::new(inner).add_hook(hook.clone());

        // Save triggers hook
        store.save(1, 100, 42).unwrap();
        
        let commits = hook.commits();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].topic_id, 1);
        assert_eq!(commits[0].consumer_id, 100);
        assert_eq!(commits[0].offset, 42);
        assert!(commits[0].previous_offset.is_none());

        // Second save has previous offset
        store.save(1, 100, 100).unwrap();
        
        let commits = hook.commits();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[1].offset, 100);
        assert_eq!(commits[1].previous_offset, Some(42));

        // Different topic/consumer
        store.save(2, 200, 500).unwrap();
        assert_eq!(hook.commits().len(), 3);

        // Load returns correct values
        assert_eq!(store.load(1, 100).unwrap(), Some(100));
        assert_eq!(store.load(2, 200).unwrap(), Some(500));

        // Clear and verify
        hook.clear();
        assert!(hook.commits().is_empty());
    }

    #[test]
    fn test_collecting_commit_hook() {
        let hook = CollectingCommitHook::new();
        
        let info = CommitInfo {
            topic_id: 1,
            consumer_id: 100,
            offset: 42,
            previous_offset: None,
            timestamp: std::time::SystemTime::now(),
        };
        
        hook.on_commit(&info).unwrap();
        
        let commits = hook.commits();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].topic_id, 1);
        
        hook.clear();
        assert!(hook.commits().is_empty());
    }
}
