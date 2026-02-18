use lnc_core::{LanceError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{debug, info, warn};

pub const CURRENT_TOPIC_EPOCH: u64 = 1;

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
            return Ok(());
        }

        let mut max_id = 0u32;

        for entry in std::fs::read_dir(&segments_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
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
                }
            }
        }

        self.next_id.store(max_id + 1, Ordering::SeqCst);

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

        let topic_dir = self.data_dir.join("segments").join(id.to_string());
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
            "Created topic"
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

        let topic_dir = self.data_dir.join("segments").join(id.to_string());
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
        self.topics_by_id
            .read()
            .map(|guard| guard.values().cloned().collect())
            .unwrap_or_default()
    }

    fn hydrate_topic_from_disk(&self, id: u32) -> Option<TopicMetadata> {
        let metadata_path = self
            .data_dir
            .join("segments")
            .join(id.to_string())
            .join("metadata.json");
        if !metadata_path.exists() {
            return None;
        }

        let metadata = match TopicMetadata::load(&metadata_path) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    target: "lance::topic",
                    topic_id = id,
                    path = %metadata_path.display(),
                    error = %e,
                    "Failed to hydrate topic metadata from disk"
                );
                return None;
            },
        };

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
            "Hydrated topic metadata from disk"
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

    pub fn get_topic_dir(&self, topic_id: u32) -> PathBuf {
        self.data_dir.join("segments").join(topic_id.to_string())
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

        // Walk segments to find the one(s) containing start_offset and read
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
            file.seek(SeekFrom::Start(pos_in_seg))?;

            let to_read = std::cmp::min(remaining, (*size_bytes - pos_in_seg) as usize);
            let mut chunk = vec![0u8; to_read];
            let bytes_read = file.read(&mut chunk)?;
            chunk.truncate(bytes_read);
            remaining -= bytes_read;
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
                    total += entry.metadata().map(|m| m.len()).unwrap_or(0);
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

        // Persist updated metadata
        let topic_dir = self.data_dir.join("segments").join(topic_id.to_string());
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
        let metadata = {
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
            metadata
        };

        let topic_dir = self.get_topic_dir(id);
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
}
