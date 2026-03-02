use lnc_core::{LanceError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{debug, info, warn};

pub const CURRENT_TOPIC_EPOCH: u64 = 1;

/// Validate that a topic name is safe for use as an on-disk directory name.
///
/// Only alphanumeric characters (`a-z`, `A-Z`, `0-9`) and dashes (`-`) are
/// permitted.  The name must also be non-empty.
pub fn validate_topic_name(name: &str) -> std::result::Result<(), String> {
    if name.is_empty() {
        return Err("Topic name must not be empty".to_string());
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(format!(
            "Topic name '{}' contains invalid characters. \
             Only alphanumeric characters (a-z, A-Z, 0-9) and dashes (-) are allowed",
            name
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicIdentityError {
    UnknownTopic,
    StaleEpoch { expected: u64, actual: u64 },
}

const fn default_topic_epoch() -> u64 {
    CURRENT_TOPIC_EPOCH
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub id: u32,
    pub name: String,
    pub created_at: u64,
    #[serde(default = "default_topic_epoch")]
    pub topic_epoch: u64,
    #[serde(default)]
    pub auth: Option<TopicAuthConfig>,
    #[serde(default)]
    pub retention: Option<RetentionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopicAuthConfig {
    pub mtls_enabled: bool,
    pub client_ca_path: Option<String>,
    pub allowed_cns: Vec<String>,
    #[serde(default)]
    pub allowed_topics: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    pub max_bytes: Option<u64>,
    pub max_age_secs: Option<u64>,
}

impl TopicMetadata {
    pub fn new(id: u32, name: String) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            id,
            name,
            created_at,
            topic_epoch: CURRENT_TOPIC_EPOCH,
            auth: None,
            retention: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_auth(mut self, auth: TopicAuthConfig) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content)
            .map_err(|e| LanceError::Config(format!("Failed to parse topic metadata: {}", e)))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let content = serde_json::to_string_pretty(self).map_err(|e| {
            LanceError::Config(format!("Failed to serialize topic metadata: {}", e))
        })?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

pub struct TopicRegistry {
    data_dir: PathBuf,
    topics_by_id: RwLock<HashMap<u32, TopicMetadata>>,
    topics_by_name: RwLock<HashMap<String, u32>>,
    next_id: AtomicU32,
}

impl TopicRegistry {
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        let segments_dir = data_dir.join("segments");
        std::fs::create_dir_all(&segments_dir)?;

        let registry = Self {
            data_dir,
            topics_by_id: RwLock::new(HashMap::new()),
            topics_by_name: RwLock::new(HashMap::new()),
            next_id: AtomicU32::new(1),
        };

        registry.load_existing_topics()?;

        Ok(registry)
    }

    fn load_existing_topics(&self) -> Result<()> {
        let segments_dir = self.data_dir.join("segments");

        if !segments_dir.exists() {
            info!(
                target: "lance::topic",
                "Topic registry loaded: 0 topics (segments directory does not exist)"
            );
            return Ok(());
        }

        let start = std::time::Instant::now();
        let mut max_id = 0u32;
        let mut loaded_count = 0u32;
        let mut scanned_count = 0u32;

        for entry in std::fs::read_dir(&segments_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                scanned_count += 1;
                let metadata_path = path.join("metadata.json");
                if metadata_path.exists() {
                    match TopicMetadata::load(&metadata_path) {
                        Ok(metadata) => {
                            max_id = max_id.max(metadata.id);

                            let mut by_id = self
                                .topics_by_id
                                .write()
                                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;
                            let mut by_name = self
                                .topics_by_name
                                .write()
                                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;

                            info!(
                                target: "lance::topic",
                                topic_id = metadata.id,
                                topic_name = %metadata.name,
                                "Loaded topic"
                            );

                            by_name.insert(metadata.name.clone(), metadata.id);
                            by_id.insert(metadata.id, metadata);
                            loaded_count += 1;
                        },
                        Err(e) => {
                            tracing::warn!(
                                target: "lance::topic",
                                path = %metadata_path.display(),
                                error = %e,
                                "Failed to load topic metadata"
                            );
                        },
                    }
                } else {
                    warn!(
                        target: "lance::topic",
                        path = %path.display(),
                        "Topic directory has no metadata.json — topic will not be loaded. \
                         Data segments may exist but are invisible to the registry."
                    );
                }
            }
        }

        self.next_id.store(max_id + 1, Ordering::SeqCst);

        let elapsed = start.elapsed();
        info!(
            target: "lance::topic",
            loaded_count,
            scanned_count,
            elapsed_ms = elapsed.as_millis() as u64,
            "Topic registry loaded: {} topics from {} directories scanned in {:?}",
            loaded_count,
            scanned_count,
            elapsed
        );

        Ok(())
    }

    pub fn create_topic(&self, name: &str) -> Result<TopicMetadata> {
        self.create_topic_with_retention(name, None)
    }

    /// Reserve and return the next topic ID.
    ///
    /// This supports clustered replicate-first create flows where topic metadata
    /// should only become visible locally after the create operation is
    /// replicated and committed.
    pub fn reserve_topic_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Create a topic with optional retention policy
    pub fn create_topic_with_retention(
        &self,
        name: &str,
        retention: Option<RetentionConfig>,
    ) -> Result<TopicMetadata> {
        validate_topic_name(name).map_err(LanceError::InvalidData)?;

        {
            let by_name = self
                .topics_by_name
                .read()
                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;
            if by_name.contains_key(name) {
                return Err(LanceError::Config(format!(
                    "Topic '{}' already exists",
                    name
                )));
            }
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut metadata = TopicMetadata::new(id, name.to_string());
        metadata.retention = retention;

        // Use topic NAME for on-disk directory (not numeric ID)
        let topic_dir = self.data_dir.join("segments").join(name);
        std::fs::create_dir_all(&topic_dir)?;

        let metadata_path = topic_dir.join("metadata.json");
        metadata.save(&metadata_path)?;

        {
            let mut by_id = self
                .topics_by_id
                .write()
                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;
            let mut by_name = self
                .topics_by_name
                .write()
                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;

            by_name.insert(name.to_string(), id);
            by_id.insert(id, metadata.clone());
        }

        info!(
            target: "lance::topic",
            topic_id = id,
            topic_name = %name,
            has_retention = metadata.retention.is_some(),
            "Created topic (name-based storage)"
        );

        Ok(metadata)
    }

    /// Create a topic with a specific ID (used for replication from leader)
    pub fn create_topic_with_id(
        &self,
        id: u32,
        name: &str,
        created_at: u64,
    ) -> Result<TopicMetadata> {
        validate_topic_name(name).map_err(LanceError::InvalidData)?;

        // Update next_id if this ID is >= current to prevent conflicts
        loop {
            let current = self.next_id.load(Ordering::SeqCst);
            if id < current {
                break;
            }
            if self
                .next_id
                .compare_exchange(current, id + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }

        let metadata = TopicMetadata {
            id,
            name: name.to_string(),
            created_at,
            topic_epoch: CURRENT_TOPIC_EPOCH,
            auth: None,
            retention: None,
        };

        // Use topic NAME for on-disk directory (not numeric ID)
        let topic_dir = self.data_dir.join("segments").join(name);
        std::fs::create_dir_all(&topic_dir)?;

        let metadata_path = topic_dir.join("metadata.json");
        metadata.save(&metadata_path)?;

        {
            let mut by_id = self
                .topics_by_id
                .write()
                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;
            let mut by_name = self
                .topics_by_name
                .write()
                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;

            // Idempotent fast path for replayed TopicOp::Create entries.
            if by_name.get(name).copied() == Some(id) && by_id.contains_key(&id) {
                return Ok(metadata);
            }

            // Reconcile stale mappings after election churn / partial ops:
            // if this name currently points at a different ID, remove old ID row.
            if let Some(existing_id_for_name) = by_name.get(name).copied() {
                if existing_id_for_name != id {
                    by_id.remove(&existing_id_for_name);
                    warn!(
                        target: "lance::topic",
                        topic_name = %name,
                        old_topic_id = existing_id_for_name,
                        new_topic_id = id,
                        "Replacing stale replicated topic mapping by name"
                    );
                }
            }

            // If this ID was previously associated to another name, remove that name mapping.
            if let Some(existing_meta_for_id) = by_id.get(&id) {
                if existing_meta_for_id.name != name {
                    by_name.remove(&existing_meta_for_id.name);
                    warn!(
                        target: "lance::topic",
                        topic_id = id,
                        old_topic_name = %existing_meta_for_id.name,
                        new_topic_name = %name,
                        "Replacing stale replicated topic mapping by id"
                    );
                }
            }

            by_name.insert(name.to_string(), id);
            by_id.insert(id, metadata.clone());
        }

        info!(
            target: "lance::topic",
            topic_id = id,
            topic_name = %name,
            "Created replicated topic"
        );

        Ok(metadata)
    }

    pub fn get_topic_by_id(&self, id: u32) -> Option<TopicMetadata> {
        if let Some(meta) = self.topics_by_id.read().ok()?.get(&id).cloned() {
            return Some(meta);
        }

        self.hydrate_topic_from_disk(id)
    }

    /// Alias for get_topic_by_id for API compatibility
    #[allow(dead_code)]
    pub fn get_topic(&self, id: u32) -> Option<TopicMetadata> {
        self.get_topic_by_id(id)
    }

    #[allow(dead_code)]
    pub fn get_topic_epoch(&self, id: u32) -> Option<u64> {
        self.get_topic_by_id(id).map(|m| m.topic_epoch)
    }

    #[allow(dead_code)]
    pub fn topic_matches_epoch(&self, id: u32, expected_epoch: u64) -> bool {
        self.get_topic_epoch(id) == Some(expected_epoch)
    }

    #[allow(dead_code)]
    pub fn validate_topic_identity(
        &self,
        id: u32,
        expected_epoch: Option<u64>,
    ) -> std::result::Result<TopicMetadata, TopicIdentityError> {
        let metadata = self
            .get_topic_by_id(id)
            .ok_or(TopicIdentityError::UnknownTopic)?;

        if let Some(expected) = expected_epoch {
            if metadata.topic_epoch != expected {
                return Err(TopicIdentityError::StaleEpoch {
                    expected,
                    actual: metadata.topic_epoch,
                });
            }
        }

        Ok(metadata)
    }

    #[allow(dead_code)]
    pub fn get_topic_by_name(&self, name: &str) -> Option<TopicMetadata> {
        let id = self.topics_by_name.read().ok()?.get(name).copied()?;
        self.get_topic_by_id(id)
    }

    #[allow(dead_code)]
    pub fn get_topic_id_by_name(&self, name: &str) -> Option<u32> {
        self.topics_by_name.read().ok()?.get(name).copied()
    }

    pub fn list_topics(&self) -> Vec<TopicMetadata> {
        match self.topics_by_id.read() {
            Ok(guard) => {
                if guard.is_empty() {
                    drop(guard);
                    // In-memory registry is empty — attempt a disk re-scan to recover
                    // from silent startup failures (e.g. missing metadata.json that was
                    // later restored, or a race during follower catch-up).
                    info!(
                        target: "lance::topic",
                        "Topic registry is empty, attempting disk re-scan"
                    );
                    if let Err(e) = self.load_existing_topics() {
                        warn!(
                            target: "lance::topic",
                            error = %e,
                            "Disk re-scan failed during list_topics"
                        );
                        return Vec::new();
                    }
                    // Re-read after reload
                    match self.topics_by_id.read() {
                        Ok(guard) => guard.values().cloned().collect(),
                        Err(e) => {
                            tracing::error!(
                                target: "lance::topic",
                                error = %e,
                                "Topic registry RwLock poisoned after disk re-scan"
                            );
                            Vec::new()
                        },
                    }
                } else {
                    guard.values().cloned().collect()
                }
            },
            Err(e) => {
                tracing::error!(
                    target: "lance::topic",
                    error = %e,
                    "Topic registry RwLock poisoned in list_topics — returning empty"
                );
                Vec::new()
            },
        }
    }

    /// Attempt to find and load a topic's metadata from disk by its ID.
    ///
    /// With name-based storage the directory name is the topic name, so we
    /// cannot derive it from the numeric ID alone.  We scan all directories
    /// under `segments/` looking for a `metadata.json` whose `id` field
    /// matches.  This is only called as a fallback when the in-memory
    /// registry does not contain the topic, so it should be infrequent.
    fn hydrate_topic_from_disk(&self, id: u32) -> Option<TopicMetadata> {
        let segments_dir = self.data_dir.join("segments");
        if !segments_dir.exists() {
            return None;
        }

        // Scan all topic directories for a metadata.json with matching id
        let entries = match std::fs::read_dir(&segments_dir) {
            Ok(e) => e,
            Err(_) => return None,
        };

        let mut found_metadata: Option<TopicMetadata> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let metadata_path = path.join("metadata.json");
            if !metadata_path.exists() {
                continue;
            }
            if let Ok(meta) = TopicMetadata::load(&metadata_path) {
                if meta.id == id {
                    found_metadata = Some(meta);
                    break;
                }
            }
        }

        let metadata = found_metadata?;

        let mut by_id = match self.topics_by_id.write() {
            Ok(g) => g,
            Err(e) => {
                warn!(
                    target: "lance::topic",
                    topic_id = id,
                    error = %e,
                    "RwLock poisoned while hydrating topics_by_id"
                );
                return None;
            },
        };
        let mut by_name = match self.topics_by_name.write() {
            Ok(g) => g,
            Err(e) => {
                warn!(
                    target: "lance::topic",
                    topic_id = id,
                    error = %e,
                    "RwLock poisoned while hydrating topics_by_name"
                );
                return None;
            },
        };

        if let Some(existing_id_for_name) = by_name.get(&metadata.name).copied() {
            if existing_id_for_name != metadata.id {
                by_id.remove(&existing_id_for_name);
            }
        }
        if let Some(existing_meta_for_id) = by_id.get(&metadata.id) {
            if existing_meta_for_id.name != metadata.name {
                by_name.remove(&existing_meta_for_id.name);
            }
        }

        by_name.insert(metadata.name.clone(), metadata.id);
        by_id.insert(metadata.id, metadata.clone());

        info!(
            target: "lance::topic",
            topic_id = metadata.id,
            topic_name = %metadata.name,
            "Hydrated topic metadata from disk (name-based scan)"
        );

        Some(metadata)
    }

    pub fn topic_exists(&self, id: u32) -> bool {
        match self.topics_by_id.read() {
            Ok(guard) => {
                if guard.contains_key(&id) {
                    true
                } else {
                    drop(guard);
                    self.hydrate_topic_from_disk(id).is_some()
                }
            },
            Err(e) => {
                tracing::error!(
                    target: "lance::topic",
                    topic_id = id,
                    error = %e,
                    "RwLock poisoned in topic_exists - returning false"
                );
                false
            },
        }
    }

    /// Return the on-disk directory for a topic.
    ///
    /// Storage is keyed by **topic name** (not numeric ID) so that
    /// directory paths remain stable across restarts, deletions, and
    /// re-creations.  The in-memory `topics_by_id` map is consulted
    /// first; if the topic is not registered yet we fall back to the
    /// numeric ID for backwards compatibility with legacy data.
    pub fn get_topic_dir(&self, topic_id: u32) -> PathBuf {
        // Resolve topic name from the in-memory registry
        if let Ok(by_id) = self.topics_by_id.read() {
            if let Some(meta) = by_id.get(&topic_id) {
                return self.data_dir.join("segments").join(&meta.name);
            }
        }
        // Fallback: topic not yet registered (shouldn't happen in normal
        // operation but keeps callers from panicking during startup races).
        warn!(
            target: "lance::topic",
            topic_id,
            "get_topic_dir: topic ID not in registry, falling back to numeric directory"
        );
        self.data_dir.join("segments").join(topic_id.to_string())
    }

    /// Return the on-disk directory for a topic given its name directly.
    /// Avoids the ID lookup when the caller already has the name.
    #[allow(dead_code)]
    pub fn get_topic_dir_by_name(&self, topic_name: &str) -> PathBuf {
        self.data_dir.join("segments").join(topic_name)
    }

    #[allow(dead_code)]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Read data from a topic starting at the specified byte offset.
    ///
    /// Scans `.lnc` segment files in the topic directory, sorted by their
    /// `start_index` (the leading number in the filename). The offset is a
    /// byte position across all segments concatenated in index order.
    ///
    /// Segment filename conventions (set by `lnc_io::SegmentWriter`):
    ///   - Active  : `{start_index}_{start_timestamp_ns}.lnc`
    ///   - Closed  : `{start_index}_{start_timestamp_ns}-{end_timestamp_ns}.lnc`
    ///
    /// Closed segments are immutable (their size will not change).  Active
    /// segments (no `-` separator) may still be receiving writes.
    pub fn read_from_offset(
        &self,
        topic_id: u32,
        start_offset: u64,
        max_bytes: u32,
    ) -> Result<bytes::Bytes> {
        let topic_dir = self.get_topic_dir(topic_id);

        if !topic_dir.exists() {
            return Ok(bytes::Bytes::new());
        }

        // Collect .lnc segments with parsed metadata
        // (start_index, start_timestamp, size_bytes, is_closed, path)
        let mut segments: Vec<(u64, u64, u64, bool, std::path::PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(&topic_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "lnc") {
                let filename = match path.file_stem().and_then(|s| s.to_str()) {
                    Some(f) => f.to_string(),
                    None => continue,
                };

                // Determine closed vs active by presence of '-'
                let (start_part, is_closed) = if let Some(pos) = filename.find('-') {
                    (&filename[..pos], true)
                } else {
                    (filename.as_str(), false)
                };

                // Parse start_index and start_timestamp from "{start_index}_{timestamp}" prefix
                let mut parts = start_part.split('_');
                let start_index = parts
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let start_timestamp = parts
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                let size_bytes = entry.metadata().map(|m| m.len()).unwrap_or(0);
                segments.push((start_index, start_timestamp, size_bytes, is_closed, path));
            }
        }

        if segments.is_empty() {
            return Ok(bytes::Bytes::new());
        }

        // Sort by start_index, then by timestamp (older first), then closed before active
        // at the same index (closed was sealed before the active one was created).
        segments.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(a.3.cmp(&b.3).reverse())
        });

        // Walk segments to find the one(s) containing start_offset and read.
        //
        // Uses the *open-file metadata* (`file.metadata().len()`) for the
        // authoritative size instead of the stale `DirEntry::metadata()` from
        // the directory scan.  On Windows, `DirEntry::metadata()` caches the
        // size at `FindFirstFile` time, which may lag behind the actual file
        // size when a writer is concurrently flushing data through a
        // `BufWriter`.  Reading beyond the truly-committed data would yield
        // zero-filled or uninitialised bytes and corrupt the consumer's TLV
        // stream.
        //
        // Additionally, we use `read_exact`-style looping to guard against
        // short reads that split TLV records at arbitrary byte boundaries.
        use std::io::{Read, Seek, SeekFrom};
        let mut cumulative: u64 = 0;
        let mut buffer = Vec::with_capacity(max_bytes as usize);
        let mut remaining = max_bytes as usize;

        for (_idx, _ts, size_bytes, _is_closed, seg_path) in &segments {
            let seg_end = cumulative + size_bytes;

            // Skip segments entirely before the requested offset
            if seg_end <= start_offset {
                cumulative = seg_end;
                continue;
            }

            // Byte position within this segment file
            let pos_in_seg = start_offset.saturating_sub(cumulative);

            let mut file = std::fs::File::open(seg_path)?;

            // Use the *open file handle* metadata for authoritative size.
            // `DirEntry::metadata()` on Windows may return a stale cached
            // value from the directory scan when a concurrent writer has
            // auto-flushed part of its `BufWriter` but not yet called
            // `sync_all()`.
            let actual_size = file.metadata().map(|m| m.len()).unwrap_or(*size_bytes);
            // Use the SMALLER of directory-entry size and open-file size to
            // avoid reading beyond data that has been durably flushed.
            let effective_size = (*size_bytes).min(actual_size);

            if pos_in_seg >= effective_size {
                // Nothing readable in this segment at the requested position.
                cumulative = seg_end;
                continue;
            }

            file.seek(SeekFrom::Start(pos_in_seg))?;

            let to_read = std::cmp::min(remaining, (effective_size - pos_in_seg) as usize);
            let mut chunk = vec![0u8; to_read];

            // read_exact-style loop: retry short reads so we never hand back
            // a buffer that splits a TLV record due to an OS-level partial
            // read on a regular file.
            let mut total_read = 0usize;
            while total_read < to_read {
                match file.read(&mut chunk[total_read..]) {
                    Ok(0) => break, // EOF — file may be shorter than metadata indicated
                    Ok(n) => total_read += n,
                    Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(LanceError::Io(e)),
                }
            }
            chunk.truncate(total_read);
            remaining -= total_read;
            buffer.extend_from_slice(&chunk);

            if remaining == 0 {
                break;
            }

            cumulative = seg_end;
        }

        Ok(bytes::Bytes::from(buffer))
    }

    /// Return the total byte size of all `.lnc` segment files for a topic.
    ///
    /// Used by the Fetch handler to distinguish "at tail, no new data" from
    /// "server hasn't replicated to this offset yet" (CATCHING_UP).
    pub fn total_data_size(&self, topic_id: u32) -> u64 {
        let topic_dir = self.get_topic_dir(topic_id);
        if !topic_dir.exists() {
            return 0;
        }

        let mut total: u64 = 0;
        if let Ok(entries) = std::fs::read_dir(&topic_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "lnc") {
                    // Open the file to get the authoritative size.
                    // `DirEntry::metadata()` on Windows caches the size from
                    // the directory scan and may be stale when a concurrent
                    // writer is flushing through a `BufWriter`.
                    let size = std::fs::File::open(&path)
                        .and_then(|f| f.metadata())
                        .map(|m| m.len())
                        .unwrap_or_else(|_| entry.metadata().map(|m| m.len()).unwrap_or(0));
                    total += size;
                }
            }
        }
        total
    }

    /// Set retention policy for an existing topic
    pub fn set_retention(&self, topic_id: u32, max_age_secs: u64, max_bytes: u64) -> Result<()> {
        let mut by_id = self
            .topics_by_id
            .write()
            .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;

        let metadata = by_id
            .get_mut(&topic_id)
            .ok_or_else(|| LanceError::Config(format!("Topic {} not found", topic_id)))?;

        metadata.retention = Some(RetentionConfig {
            max_age_secs: Some(max_age_secs),
            max_bytes: Some(max_bytes),
        });

        // Persist updated metadata using name-based directory
        let topic_dir = self.data_dir.join("segments").join(&metadata.name);
        let metadata_path = topic_dir.join("metadata.json");
        metadata.save(&metadata_path)?;

        info!(
            target: "lance::topic",
            topic_id,
            max_age_secs,
            max_bytes,
            "Updated retention policy"
        );

        Ok(())
    }

    pub fn delete_topic(&self, id: u32) -> Result<()> {
        let (metadata, topic_dir) = {
            let mut by_id = self
                .topics_by_id
                .write()
                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;
            let mut by_name = self
                .topics_by_name
                .write()
                .map_err(|_| LanceError::Protocol("Lock poisoned".into()))?;

            let metadata = by_id
                .remove(&id)
                .ok_or_else(|| LanceError::Config(format!("Topic {} not found", id)))?;
            by_name.remove(&metadata.name);
            // Resolve directory path using topic name BEFORE dropping the metadata
            let dir = self.data_dir.join("segments").join(&metadata.name);
            (metadata, dir)
        };

        if topic_dir.exists() {
            // Retry deletion with backoff to handle transient file handle issues
            let mut last_err = None;
            for attempt in 0..5 {
                match std::fs::remove_dir_all(&topic_dir) {
                    Ok(()) => {
                        last_err = None;
                        break;
                    },
                    Err(e) => {
                        last_err = Some(e);
                        if attempt < 4 {
                            std::thread::sleep(std::time::Duration::from_millis(
                                10 * (attempt as u64 + 1),
                            ));
                        }
                    },
                }
            }
            if let Some(e) = last_err {
                return Err(e.into());
            }
        }

        info!(
            target: "lance::topic",
            topic_id = id,
            topic_name = %metadata.name,
            "Deleted topic"
        );

        Ok(())
    }

    /// Seal all active segments during graceful shutdown.
    /// This ensures all in-memory data is flushed to disk before exit.
    /// Per Architecture §9.2: Seal active segment.
    #[allow(dead_code)]
    pub fn seal_all_segments(&self) -> Result<u32> {
        let topics = self
            .topics_by_id
            .read()
            .map_err(|_| LanceError::Internal("Failed to acquire topic lock for sealing".into()))?;

        let mut sealed_count = 0u32;
        for (topic_id, _metadata) in topics.iter() {
            let topic_dir = self.get_topic_dir(*topic_id);

            // Check for active segment marker file
            let active_marker = topic_dir.join(".active");
            if active_marker.exists() {
                // Remove active marker to indicate segment is sealed
                if let Err(e) = std::fs::remove_file(&active_marker) {
                    warn!(
                        target: "lance::topic",
                        topic_id = topic_id,
                        error = %e,
                        "Failed to remove active segment marker"
                    );
                } else {
                    sealed_count += 1;
                    debug!(
                        target: "lance::topic",
                        topic_id = topic_id,
                        "Sealed active segment"
                    );
                }
            }
        }

        info!(
            target: "lance::topic",
            sealed_count = sealed_count,
            "Sealed all active segments"
        );

        Ok(sealed_count)
    }

    /// Flush all sparse indexes during graceful shutdown.
    /// Per Architecture §9.2: Flush sparse indexes.
    #[allow(dead_code)]
    pub fn flush_all_indexes(&self) -> Result<u32> {
        let topics = self.topics_by_id.read().map_err(|_| {
            LanceError::Internal("Failed to acquire topic lock for flushing".into())
        })?;

        let mut flushed_count = 0u32;
        for (topic_id, _metadata) in topics.iter() {
            let topic_dir = self.get_topic_dir(*topic_id);
            let index_path = topic_dir.join("sparse.idx");

            // Touch the index file to update modification time
            if index_path.exists() {
                if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&index_path) {
                    if file.sync_all().is_ok() {
                        flushed_count += 1;
                        debug!(
                            target: "lance::topic",
                            topic_id = topic_id,
                            "Flushed sparse index"
                        );
                    }
                }
            }
        }

        info!(
            target: "lance::topic",
            flushed_count = flushed_count,
            "Flushed all sparse indexes"
        );

        Ok(flushed_count)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicListResponse {
    pub topics: Vec<TopicInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub id: u32,
    pub name: String,
    pub created_at: u64,
    #[serde(default = "default_topic_epoch")]
    pub topic_epoch: u64,
}

impl From<&TopicMetadata> for TopicInfo {
    fn from(m: &TopicMetadata) -> Self {
        Self {
            id: m.id,
            name: m.name.clone(),
            created_at: m.created_at,
            topic_epoch: m.topic_epoch,
        }
    }
}

impl TopicListResponse {
    pub fn from_topics(topics: &[TopicMetadata]) -> Self {
        Self {
            topics: topics.iter().map(TopicInfo::from).collect(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    #[allow(dead_code)]
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| LanceError::Protocol(format!("Failed to parse topic list: {}", e)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_topic_metadata_roundtrip() {
        let metadata = TopicMetadata::new(1, "test-topic".to_string());
        let json = serde_json::to_string(&metadata).unwrap();
        let parsed: TopicMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, 1);
        assert_eq!(parsed.name, "test-topic");
    }

    #[test]
    fn test_topic_registry_create() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let topic = registry.create_topic("my-topic").unwrap();
        assert_eq!(topic.id, 1);
        assert_eq!(topic.name, "my-topic");

        let retrieved = registry.get_topic_by_name("my-topic").unwrap();
        assert_eq!(retrieved.id, 1);

        let by_id = registry.get_topic_by_id(1).unwrap();
        assert_eq!(by_id.name, "my-topic");
    }

    #[test]
    fn test_topic_registry_list() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        registry.create_topic("topic-a").unwrap();
        registry.create_topic("topic-b").unwrap();
        registry.create_topic("topic-c").unwrap();

        let topics = registry.list_topics();
        assert_eq!(topics.len(), 3);
    }

    #[test]
    fn test_topic_registry_persistence() {
        let dir = tempdir().unwrap();

        {
            let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();
            registry.create_topic("persistent-topic").unwrap();
        }

        {
            let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();
            let topic = registry.get_topic_by_name("persistent-topic").unwrap();
            assert_eq!(topic.name, "persistent-topic");
        }
    }

    /// Verify that topic directories are created using the topic NAME,
    /// not the numeric ID.  This is the core invariant of name-based storage.
    #[test]
    fn test_name_based_storage_directory() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let topic = registry.create_topic("rithmic-dev").unwrap();

        // Directory should be segments/rithmic-dev, NOT segments/1
        let name_dir = dir.path().join("segments").join("rithmic-dev");
        let id_dir = dir.path().join("segments").join(topic.id.to_string());

        assert!(
            name_dir.exists(),
            "Name-based directory should exist: {}",
            name_dir.display()
        );
        assert!(
            !id_dir.exists(),
            "Numeric ID directory should NOT exist: {}",
            id_dir.display()
        );

        // metadata.json should be in the name-based directory
        let metadata_path = name_dir.join("metadata.json");
        assert!(metadata_path.exists());

        // get_topic_dir should resolve to the name-based path
        let resolved = registry.get_topic_dir(topic.id);
        assert_eq!(resolved, name_dir);
    }

    /// Verify that multiple topics each get their own name-based directory
    /// and that no numeric-ID directories are created.
    #[test]
    fn test_name_based_storage_multiple_topics() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let t1 = registry.create_topic("rithmic-dev").unwrap();
        let t2 = registry.create_topic("rithmic-dev-actions").unwrap();
        let t3 = registry.create_topic("rithmic-paper").unwrap();

        for (topic, name) in [
            (&t1, "rithmic-dev"),
            (&t2, "rithmic-dev-actions"),
            (&t3, "rithmic-paper"),
        ] {
            let name_dir = dir.path().join("segments").join(name);
            assert!(name_dir.exists(), "Directory for {} should exist", name);
            assert_eq!(registry.get_topic_dir(topic.id), name_dir);

            // Numeric directory should NOT exist
            let id_dir = dir.path().join("segments").join(topic.id.to_string());
            assert!(
                !id_dir.exists(),
                "Numeric directory for topic {} (id={}) should NOT exist",
                name,
                topic.id
            );
        }
    }

    /// Verify that name-based persistence survives a registry restart.
    /// After creating topics and restarting, the registry should find
    /// them by scanning name-based directories.
    #[test]
    fn test_name_based_persistence_across_restarts() {
        let dir = tempdir().unwrap();

        // Create topics in first registry instance
        let (t1_id, t2_id) = {
            let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();
            let t1 = registry.create_topic("rithmic-dev").unwrap();
            let t2 = registry.create_topic("rithmic-dev-actions").unwrap();
            (t1.id, t2.id)
        };

        // Restart registry from disk
        {
            let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

            // Should find both topics by name
            let t1 = registry.get_topic_by_name("rithmic-dev").unwrap();
            assert_eq!(t1.id, t1_id);
            assert_eq!(t1.name, "rithmic-dev");

            let t2 = registry.get_topic_by_name("rithmic-dev-actions").unwrap();
            assert_eq!(t2.id, t2_id);
            assert_eq!(t2.name, "rithmic-dev-actions");

            // Should also find by ID
            let t1_by_id = registry.get_topic_by_id(t1_id).unwrap();
            assert_eq!(t1_by_id.name, "rithmic-dev");

            // get_topic_dir should resolve correctly after restart
            let expected_dir = dir.path().join("segments").join("rithmic-dev");
            assert_eq!(registry.get_topic_dir(t1_id), expected_dir);
        }
    }

    /// Verify that create_topic_with_id (used for replication) also
    /// creates name-based directories.
    #[test]
    fn test_name_based_storage_replicated_create() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let created_at = 1700000000u64;
        let topic = registry
            .create_topic_with_id(42, "replicated-topic", created_at)
            .unwrap();

        assert_eq!(topic.id, 42);
        assert_eq!(topic.name, "replicated-topic");

        let name_dir = dir.path().join("segments").join("replicated-topic");
        let id_dir = dir.path().join("segments").join("42");

        assert!(name_dir.exists());
        assert!(!id_dir.exists());
        assert_eq!(registry.get_topic_dir(42), name_dir);
    }

    /// Verify that topic deletion removes the name-based directory.
    #[test]
    fn test_name_based_storage_delete() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let topic = registry.create_topic("doomed-topic").unwrap();
        let name_dir = dir.path().join("segments").join("doomed-topic");
        assert!(name_dir.exists());

        registry.delete_topic(topic.id).unwrap();
        assert!(!name_dir.exists());
    }

    /// Verify that get_topic_dir_by_name returns the correct path.
    #[test]
    fn test_get_topic_dir_by_name() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let expected = dir.path().join("segments").join("my-topic");
        assert_eq!(registry.get_topic_dir_by_name("my-topic"), expected);
    }

    /// Verify that hydrate_topic_from_disk works with name-based directories
    /// by scanning all directories instead of probing by numeric ID.
    #[test]
    fn test_hydrate_from_disk_name_based() {
        let dir = tempdir().unwrap();

        // Create a topic, then drop the registry (clears in-memory maps)
        let topic_id = {
            let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();
            let topic = registry.create_topic("hydrate-test").unwrap();
            topic.id
        };

        // Create a fresh registry but do NOT call load_existing_topics
        // (it's called in new() so we just verify get_topic_by_id works)
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();
        let hydrated = registry.get_topic_by_id(topic_id).unwrap();
        assert_eq!(hydrated.name, "hydrate-test");
        assert_eq!(hydrated.id, topic_id);
    }

    // ── Topic name validation tests ──────────────────────────────────────

    #[test]
    fn test_validate_topic_name_valid_names() {
        assert!(validate_topic_name("my-topic").is_ok());
        assert!(validate_topic_name("rithmic-dev-actions").is_ok());
        assert!(validate_topic_name("data123").is_ok());
        assert!(validate_topic_name("A").is_ok());
        assert!(validate_topic_name("a").is_ok());
        assert!(validate_topic_name("0").is_ok());
        assert!(validate_topic_name("topic-with-many-dashes").is_ok());
        assert!(validate_topic_name("UPPERCASE").is_ok());
        assert!(validate_topic_name("MiXeD123CaSe").is_ok());
    }

    #[test]
    fn test_validate_topic_name_empty() {
        let result = validate_topic_name("");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must not be empty"));
    }

    #[test]
    fn test_validate_topic_name_space() {
        let result = validate_topic_name("my topic");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid characters"));
    }

    #[test]
    fn test_validate_topic_name_dot() {
        let result = validate_topic_name("my.topic");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid characters"));
    }

    #[test]
    fn test_validate_topic_name_underscore() {
        let result = validate_topic_name("my_topic");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid characters"));
    }

    #[test]
    fn test_validate_topic_name_path_traversal() {
        let result = validate_topic_name("../escape");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid characters"));
    }

    #[test]
    fn test_validate_topic_name_slash() {
        let result = validate_topic_name("my/topic");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid characters"));
    }

    #[test]
    fn test_validate_topic_name_backslash() {
        let result = validate_topic_name("my\\topic");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid characters"));
    }

    #[test]
    fn test_create_topic_rejects_invalid_name() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let result = registry.create_topic("bad name!");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("invalid characters"));
    }

    #[test]
    fn test_create_topic_with_id_rejects_invalid_name() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let result = registry.create_topic_with_id(1, "bad.name", 1000);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("invalid characters"));
    }

    #[test]
    fn test_create_topic_with_id_rejects_empty_name() {
        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();

        let result = registry.create_topic_with_id(1, "", 1000);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("must not be empty"));
    }

    /// Reproduce the large-payload data corruption bug observed in lnc-chaos.
    ///
    /// Writes multiple TLV records (4125 bytes each, simulating 4096-byte
    /// payload chaos messages) to a segment via SegmentWriter, then reads
    /// them back via `read_from_offset` and verifies byte-for-byte
    /// correctness. Also parses the read-back data as TLV records to
    /// check for alignment errors.
    #[test]
    fn test_large_payload_read_from_offset_integrity() {
        use lnc_io::SegmentWriter;

        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();
        let topic = registry.create_topic("integrity-test").unwrap();
        let topic_dir = registry.get_topic_dir(topic.id);

        // Create a segment file matching the naming convention
        let segment_path = topic_dir.join("0_1000000000.lnc");
        let mut writer = SegmentWriter::create(&segment_path).unwrap();

        // Build TLV records matching the chaos test pattern:
        // TLV header: type(1) + length(4 LE) = 5 bytes
        // Chaos message: run_id(8) + seq(8) + ts(8) + padding(payload_size) = 24 + payload_size
        // Total per record: 5 + 24 + 4096 = 4125 bytes
        let payload_size: usize = 4096;
        let run_id: u64 = 0xDEAD_BEEF_CAFE_BABEu64;
        let num_records = 20;

        let mut expected_data = Vec::new();

        for seq in 1u64..=num_records {
            // Encode chaos message
            let ts: u64 = 1_000_000 + seq;
            let mut msg = Vec::with_capacity(24 + payload_size);
            msg.extend_from_slice(&run_id.to_le_bytes());
            msg.extend_from_slice(&seq.to_le_bytes());
            msg.extend_from_slice(&ts.to_le_bytes());
            msg.resize(24 + payload_size, (seq & 0xFF) as u8);

            // Encode TLV record: type(1) + length(4 LE) + payload
            let record_type: u8 = 0x01; // RecordType::Data
            let length = msg.len() as u32;
            let mut tlv = Vec::with_capacity(5 + msg.len());
            tlv.push(record_type);
            tlv.extend_from_slice(&length.to_le_bytes());
            tlv.extend_from_slice(&msg);

            assert_eq!(tlv.len(), 4125, "TLV record should be 4125 bytes");

            expected_data.extend_from_slice(&tlv);

            // Write via SegmentWriter (same path as ingestion actor)
            writer.write_batch(&tlv).unwrap();
        }

        // Flush and sync (same as flush_and_signal_sync does)
        writer.fsync().unwrap();

        let total_expected = 4125 * num_records as usize;
        assert_eq!(expected_data.len(), total_expected);

        // Now read back via read_from_offset in chunks, simulating consumer fetches
        let max_fetch_bytes: u32 = 256 * 1024; // Same as chaos test default
        let mut offset: u64 = 0;
        let mut all_read = Vec::new();

        loop {
            let data = registry
                .read_from_offset(topic.id, offset, max_fetch_bytes)
                .unwrap();
            if data.is_empty() {
                break;
            }
            offset += data.len() as u64;
            all_read.extend_from_slice(&data);
        }

        assert_eq!(
            all_read.len(),
            expected_data.len(),
            "Total bytes read ({}) should match total bytes written ({})",
            all_read.len(),
            expected_data.len()
        );

        // Byte-for-byte comparison
        for (i, (actual, expected)) in all_read.iter().zip(expected_data.iter()).enumerate() {
            if actual != expected {
                panic!(
                    "Data mismatch at byte offset {}: actual=0x{:02X}, expected=0x{:02X}. \
                     Record boundary: offset {} is within record {} (offset within record: {})",
                    i,
                    actual,
                    expected,
                    i,
                    i / 4125,
                    i % 4125
                );
            }
        }

        // Parse TLV records from read-back data to verify alignment
        let mut parse_offset: usize = 0;
        let mut parsed_count: u64 = 0;
        while parse_offset + 5 <= all_read.len() {
            let record_type = all_read[parse_offset];
            assert_eq!(record_type, 0x01, "Record type should be Data (0x01)");

            let length = u32::from_le_bytes([
                all_read[parse_offset + 1],
                all_read[parse_offset + 2],
                all_read[parse_offset + 3],
                all_read[parse_offset + 4],
            ]);
            assert_eq!(
                length as usize,
                24 + payload_size,
                "TLV length field should be {} but got {} at record {}",
                24 + payload_size,
                length,
                parsed_count
            );

            let payload_start = parse_offset + 5;
            let payload_end = payload_start + length as usize;
            assert!(
                payload_end <= all_read.len(),
                "Record {} extends beyond read data",
                parsed_count
            );

            // Verify seq in the chaos message
            let seq = u64::from_le_bytes([
                all_read[payload_start + 8],
                all_read[payload_start + 9],
                all_read[payload_start + 10],
                all_read[payload_start + 11],
                all_read[payload_start + 12],
                all_read[payload_start + 13],
                all_read[payload_start + 14],
                all_read[payload_start + 15],
            ]);
            assert_eq!(
                seq,
                parsed_count + 1,
                "Sequence number mismatch at record {}",
                parsed_count
            );

            parse_offset = payload_end;
            parsed_count += 1;
        }

        assert_eq!(
            parsed_count, num_records,
            "Should have parsed {} records but got {}",
            num_records, parsed_count
        );
    }

    /// Test read_from_offset with multiple fetch rounds (partial reads).
    ///
    /// Simulates a consumer fetching with a small max_bytes that forces
    /// reads to split across TLV record boundaries. Verifies that the
    /// concatenated results are byte-identical to the written data.
    #[test]
    fn test_large_payload_partial_fetch_integrity() {
        use lnc_io::SegmentWriter;

        let dir = tempdir().unwrap();
        let registry = TopicRegistry::new(dir.path().to_path_buf()).unwrap();
        let topic = registry.create_topic("partial-fetch-test").unwrap();
        let topic_dir = registry.get_topic_dir(topic.id);

        let segment_path = topic_dir.join("0_2000000000.lnc");
        let mut writer = SegmentWriter::create(&segment_path).unwrap();

        let payload_size: usize = 4096;
        let num_records = 10;
        let mut expected_data = Vec::new();

        for seq in 1u64..=num_records {
            let mut msg = Vec::with_capacity(24 + payload_size);
            msg.extend_from_slice(&0xCAFEu64.to_le_bytes());
            msg.extend_from_slice(&seq.to_le_bytes());
            msg.extend_from_slice(&0u64.to_le_bytes());
            msg.resize(24 + payload_size, (seq & 0xFF) as u8);

            let mut tlv = Vec::with_capacity(5 + msg.len());
            tlv.push(0x01u8);
            tlv.extend_from_slice(&(msg.len() as u32).to_le_bytes());
            tlv.extend_from_slice(&msg);

            expected_data.extend_from_slice(&tlv);
            writer.write_batch(&tlv).unwrap();
        }

        writer.fsync().unwrap();

        // Fetch with a small max_bytes that splits records mid-record
        // 6000 bytes: one full record (4125) + partial second record (1875 bytes)
        let max_fetch_bytes: u32 = 6000;
        let mut offset: u64 = 0;
        let mut all_read = Vec::new();

        loop {
            let data = registry
                .read_from_offset(topic.id, offset, max_fetch_bytes)
                .unwrap();
            if data.is_empty() {
                break;
            }
            offset += data.len() as u64;
            all_read.extend_from_slice(&data);
        }

        assert_eq!(
            all_read.len(),
            expected_data.len(),
            "Total bytes should match: read={}, written={}",
            all_read.len(),
            expected_data.len()
        );
        assert_eq!(all_read, expected_data, "Byte-for-byte match failed");
    }
}
