//! Tiered storage for hot/cold data management
//!
//! Provides automatic movement of old segments from hot (fast) storage
//! to cold (cheap) storage based on age or access patterns.

use lnc_core::{LanceError, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tracing::{debug, info, warn};

/// Storage tier types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    /// Hot tier - fast storage for recent/frequently accessed data
    Hot,
    /// Warm tier - intermediate storage (optional)
    Warm,
    /// Cold tier - cheap storage for old/rarely accessed data
    Cold,
    /// Archive tier - long-term retention (optional)
    Archive,
}

impl StorageTier {
    /// Get tier name for logging
    pub fn name(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
            StorageTier::Archive => "archive",
        }
    }
}

/// Configuration for a storage tier
#[derive(Debug, Clone)]
pub struct TierConfig {
    /// Tier type
    pub tier: StorageTier,
    /// Base path for this tier's storage
    pub path: PathBuf,
    /// Maximum age before data moves to next tier (None = no limit)
    pub max_age: Option<Duration>,
    /// Maximum size before data moves to next tier (None = no limit)
    pub max_size: Option<u64>,
    /// Whether this tier is enabled
    pub enabled: bool,
}

impl TierConfig {
    /// Create a new tier configuration
    pub fn new(tier: StorageTier, path: PathBuf) -> Self {
        Self {
            tier,
            path,
            max_age: None,
            max_size: None,
            enabled: true,
        }
    }

    /// Set maximum age for this tier
    pub fn with_max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }

    /// Set maximum size for this tier
    pub fn with_max_size(mut self, size: u64) -> Self {
        self.max_size = Some(size);
        self
    }
}

/// Configuration for tiered storage
#[derive(Debug, Clone)]
pub struct TieredStorageConfig {
    /// Hot tier configuration (required)
    pub hot: TierConfig,
    /// Warm tier configuration (optional)
    pub warm: Option<TierConfig>,
    /// Cold tier configuration (optional)
    pub cold: Option<TierConfig>,
    /// Archive tier configuration (optional)
    pub archive: Option<TierConfig>,
    /// Whether to delete from source after successful move
    pub delete_after_move: bool,
    /// Whether tiering is enabled
    pub enabled: bool,
}

impl TieredStorageConfig {
    /// Create a basic hot-only configuration
    pub fn hot_only(hot_path: PathBuf) -> Self {
        Self {
            hot: TierConfig::new(StorageTier::Hot, hot_path),
            warm: None,
            cold: None,
            archive: None,
            delete_after_move: true,
            enabled: true,
        }
    }

    /// Create a hot/cold configuration
    pub fn hot_cold(hot_path: PathBuf, cold_path: PathBuf, hot_max_age: Duration) -> Self {
        Self {
            hot: TierConfig::new(StorageTier::Hot, hot_path).with_max_age(hot_max_age),
            warm: None,
            cold: Some(TierConfig::new(StorageTier::Cold, cold_path)),
            archive: None,
            delete_after_move: true,
            enabled: true,
        }
    }
}

/// Metadata about a segment for tiering decisions
#[derive(Debug, Clone)]
pub struct TierableSegment {
    /// Path to the segment
    pub path: PathBuf,
    /// Current tier
    pub current_tier: StorageTier,
    /// Size in bytes
    pub size: u64,
    /// Last modified time
    pub modified_at: SystemTime,
    /// Last accessed time (if available)
    pub accessed_at: Option<SystemTime>,
    /// Age since last modification
    pub age: Duration,
}

/// Result of a tier movement operation
#[derive(Debug)]
pub struct TierMoveResult {
    /// Source path
    pub source: PathBuf,
    /// Destination path
    pub destination: PathBuf,
    /// Source tier
    pub from_tier: StorageTier,
    /// Destination tier
    pub to_tier: StorageTier,
    /// Bytes moved
    pub bytes_moved: u64,
}

/// Tiered storage manager
pub struct TieredStorageManager {
    config: TieredStorageConfig,
}

impl TieredStorageManager {
    /// Create a new tiered storage manager
    pub fn new(config: TieredStorageConfig) -> Result<Self> {
        // Ensure tier directories exist
        fs::create_dir_all(&config.hot.path)?;

        if let Some(ref warm) = config.warm {
            if warm.enabled {
                fs::create_dir_all(&warm.path)?;
            }
        }

        if let Some(ref cold) = config.cold {
            if cold.enabled {
                fs::create_dir_all(&cold.path)?;
            }
        }

        if let Some(ref archive) = config.archive {
            if archive.enabled {
                fs::create_dir_all(&archive.path)?;
            }
        }

        Ok(Self { config })
    }

    /// Get the path for a specific tier
    pub fn tier_path(&self, tier: StorageTier) -> Option<&Path> {
        match tier {
            StorageTier::Hot => Some(&self.config.hot.path),
            StorageTier::Warm => self.config.warm.as_ref().map(|t| t.path.as_path()),
            StorageTier::Cold => self.config.cold.as_ref().map(|t| t.path.as_path()),
            StorageTier::Archive => self.config.archive.as_ref().map(|t| t.path.as_path()),
        }
    }

    /// Scan a tier for segments
    pub fn scan_tier(&self, tier: StorageTier) -> Result<Vec<TierableSegment>> {
        let tier_path = match self.tier_path(tier) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        let now = SystemTime::now();
        let mut segments = Vec::new();

        for entry in fs::read_dir(tier_path)? {
            let entry = entry?;
            let path = entry.path();

            // Only consider .lnc files
            if path.extension().and_then(|e| e.to_str()) != Some("lnc") {
                continue;
            }

            let metadata = entry.metadata()?;
            let modified_at = metadata.modified().unwrap_or(now);
            let age = now.duration_since(modified_at).unwrap_or(Duration::ZERO);

            segments.push(TierableSegment {
                path,
                current_tier: tier,
                size: metadata.len(),
                modified_at,
                accessed_at: metadata.accessed().ok(),
                age,
            });
        }

        // Sort by age (oldest first)
        segments.sort_by(|a, b| b.age.cmp(&a.age));

        Ok(segments)
    }

    /// Determine the next tier for a segment
    pub fn next_tier(&self, current: StorageTier) -> Option<StorageTier> {
        match current {
            StorageTier::Hot => {
                if self.config.warm.as_ref().is_some_and(|t| t.enabled) {
                    Some(StorageTier::Warm)
                } else if self.config.cold.as_ref().is_some_and(|t| t.enabled) {
                    Some(StorageTier::Cold)
                } else {
                    None
                }
            },
            StorageTier::Warm => {
                if self.config.cold.as_ref().is_some_and(|t| t.enabled) {
                    Some(StorageTier::Cold)
                } else if self.config.archive.as_ref().is_some_and(|t| t.enabled) {
                    Some(StorageTier::Archive)
                } else {
                    None
                }
            },
            StorageTier::Cold => {
                if self.config.archive.as_ref().is_some_and(|t| t.enabled) {
                    Some(StorageTier::Archive)
                } else {
                    None
                }
            },
            StorageTier::Archive => None,
        }
    }

    /// Check if a segment should be moved to the next tier
    pub fn should_move(&self, segment: &TierableSegment) -> bool {
        let tier_config = match segment.current_tier {
            StorageTier::Hot => &self.config.hot,
            StorageTier::Warm => match &self.config.warm {
                Some(c) => c,
                None => return false,
            },
            StorageTier::Cold => match &self.config.cold {
                Some(c) => c,
                None => return false,
            },
            StorageTier::Archive => return false,
        };

        // Check age threshold
        if let Some(max_age) = tier_config.max_age {
            if segment.age > max_age {
                return true;
            }
        }

        false
    }

    /// Find segments that need to be moved
    pub fn find_segments_to_move(&self) -> Result<Vec<TierableSegment>> {
        let mut to_move = Vec::new();

        // Check hot tier
        for segment in self.scan_tier(StorageTier::Hot)? {
            if self.should_move(&segment) && self.next_tier(StorageTier::Hot).is_some() {
                to_move.push(segment);
            }
        }

        // Check warm tier
        if self.config.warm.is_some() {
            for segment in self.scan_tier(StorageTier::Warm)? {
                if self.should_move(&segment) && self.next_tier(StorageTier::Warm).is_some() {
                    to_move.push(segment);
                }
            }
        }

        // Check cold tier
        if self.config.cold.is_some() {
            for segment in self.scan_tier(StorageTier::Cold)? {
                if self.should_move(&segment) && self.next_tier(StorageTier::Cold).is_some() {
                    to_move.push(segment);
                }
            }
        }

        Ok(to_move)
    }

    /// Move a segment to the next tier
    pub fn move_segment(&self, segment: &TierableSegment) -> Result<TierMoveResult> {
        let next_tier = self
            .next_tier(segment.current_tier)
            .ok_or_else(|| LanceError::InvalidData("No next tier available".into()))?;

        let dest_path = self.tier_path(next_tier).ok_or_else(|| {
            LanceError::InvalidData("Destination tier path not configured".into())
        })?;

        let file_name = segment
            .path
            .file_name()
            .ok_or_else(|| LanceError::InvalidData("Invalid segment path".into()))?;

        let destination = dest_path.join(file_name);

        debug!(
            target: "lance::tiering",
            source = %segment.path.display(),
            destination = %destination.display(),
            from_tier = segment.current_tier.name(),
            to_tier = next_tier.name(),
            "Moving segment"
        );

        // Copy file to destination
        fs::copy(&segment.path, &destination)?;

        // Delete source if configured
        if self.config.delete_after_move {
            fs::remove_file(&segment.path)?;
        }

        info!(
            target: "lance::tiering",
            from = segment.current_tier.name(),
            to = next_tier.name(),
            size = segment.size,
            "Segment moved to next tier"
        );

        Ok(TierMoveResult {
            source: segment.path.clone(),
            destination,
            from_tier: segment.current_tier,
            to_tier: next_tier,
            bytes_moved: segment.size,
        })
    }

    /// Run automatic tiering for all segments that need to move
    pub fn run_tiering(&self) -> Result<Vec<TierMoveResult>> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }

        let segments_to_move = self.find_segments_to_move()?;
        let mut results = Vec::new();

        for segment in &segments_to_move {
            match self.move_segment(segment) {
                Ok(result) => results.push(result),
                Err(e) => {
                    warn!(
                        target: "lance::tiering",
                        path = %segment.path.display(),
                        error = %e,
                        "Failed to move segment"
                    );
                },
            }
        }

        if !results.is_empty() {
            info!(
                target: "lance::tiering",
                moved_count = results.len(),
                total_bytes = results.iter().map(|r| r.bytes_moved).sum::<u64>(),
                "Tiering cycle completed"
            );
        }

        Ok(results)
    }

    /// Get total size of a tier
    pub fn tier_size(&self, tier: StorageTier) -> Result<u64> {
        let segments = self.scan_tier(tier)?;
        Ok(segments.iter().map(|s| s.size).sum())
    }

    /// Get statistics for all tiers
    pub fn get_stats(&self) -> Result<TierStats> {
        Ok(TierStats {
            hot_size: self.tier_size(StorageTier::Hot)?,
            hot_count: self.scan_tier(StorageTier::Hot)?.len(),
            warm_size: self
                .config
                .warm
                .as_ref()
                .map(|_| self.tier_size(StorageTier::Warm))
                .transpose()?,
            warm_count: self
                .config
                .warm
                .as_ref()
                .map(|_| self.scan_tier(StorageTier::Warm).map(|s| s.len()))
                .transpose()?,
            cold_size: self
                .config
                .cold
                .as_ref()
                .map(|_| self.tier_size(StorageTier::Cold))
                .transpose()?,
            cold_count: self
                .config
                .cold
                .as_ref()
                .map(|_| self.scan_tier(StorageTier::Cold).map(|s| s.len()))
                .transpose()?,
            archive_size: self
                .config
                .archive
                .as_ref()
                .map(|_| self.tier_size(StorageTier::Archive))
                .transpose()?,
            archive_count: self
                .config
                .archive
                .as_ref()
                .map(|_| self.scan_tier(StorageTier::Archive).map(|s| s.len()))
                .transpose()?,
        })
    }
}

/// Statistics for all storage tiers
#[derive(Debug, Clone)]
pub struct TierStats {
    /// Total size of hot tier
    pub hot_size: u64,
    /// Number of segments in hot tier
    pub hot_count: usize,
    /// Total size of warm tier (if enabled)
    pub warm_size: Option<u64>,
    /// Number of segments in warm tier (if enabled)
    pub warm_count: Option<usize>,
    /// Total size of cold tier (if enabled)
    pub cold_size: Option<u64>,
    /// Number of segments in cold tier (if enabled)
    pub cold_count: Option<usize>,
    /// Total size of archive tier (if enabled)
    pub archive_size: Option<u64>,
    /// Number of segments in archive tier (if enabled)
    pub archive_count: Option<usize>,
}

impl TierStats {
    /// Get total size across all tiers
    pub fn total_size(&self) -> u64 {
        self.hot_size
            + self.warm_size.unwrap_or(0)
            + self.cold_size.unwrap_or(0)
            + self.archive_size.unwrap_or(0)
    }

    /// Get total segment count across all tiers
    pub fn total_count(&self) -> usize {
        self.hot_count
            + self.warm_count.unwrap_or(0)
            + self.cold_count.unwrap_or(0)
            + self.archive_count.unwrap_or(0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_storage_tier_name() {
        assert_eq!(StorageTier::Hot.name(), "hot");
        assert_eq!(StorageTier::Cold.name(), "cold");
    }

    #[test]
    fn test_tier_config_builder() {
        let config = TierConfig::new(StorageTier::Hot, PathBuf::from("/data/hot"))
            .with_max_age(Duration::from_secs(3600))
            .with_max_size(1024 * 1024 * 1024);

        assert_eq!(config.tier, StorageTier::Hot);
        assert_eq!(config.max_age, Some(Duration::from_secs(3600)));
        assert_eq!(config.max_size, Some(1024 * 1024 * 1024));
    }

    #[test]
    fn test_tiered_storage_config_hot_only() {
        let config = TieredStorageConfig::hot_only(PathBuf::from("/data/hot"));
        assert!(config.cold.is_none());
        assert!(config.warm.is_none());
    }

    #[test]
    fn test_tiered_storage_config_hot_cold() {
        let config = TieredStorageConfig::hot_cold(
            PathBuf::from("/data/hot"),
            PathBuf::from("/data/cold"),
            Duration::from_secs(86400),
        );

        assert!(config.cold.is_some());
        assert_eq!(config.hot.max_age, Some(Duration::from_secs(86400)));
    }

    #[test]
    fn test_tiered_storage_manager_creation() {
        let dir = tempdir().unwrap();
        let hot_path = dir.path().join("hot");
        let cold_path = dir.path().join("cold");

        let config = TieredStorageConfig::hot_cold(
            hot_path.clone(),
            cold_path.clone(),
            Duration::from_secs(3600),
        );

        let manager = TieredStorageManager::new(config).unwrap();

        assert!(hot_path.exists());
        assert!(cold_path.exists());
        assert_eq!(
            manager.tier_path(StorageTier::Hot),
            Some(hot_path.as_path())
        );
    }

    #[test]
    fn test_next_tier() {
        let dir = tempdir().unwrap();
        let config = TieredStorageConfig::hot_cold(
            dir.path().join("hot"),
            dir.path().join("cold"),
            Duration::from_secs(3600),
        );

        let manager = TieredStorageManager::new(config).unwrap();

        assert_eq!(manager.next_tier(StorageTier::Hot), Some(StorageTier::Cold));
        assert_eq!(manager.next_tier(StorageTier::Cold), None);
    }

    #[test]
    fn test_tier_stats_total() {
        let stats = TierStats {
            hot_size: 1000,
            hot_count: 5,
            warm_size: Some(2000),
            warm_count: Some(10),
            cold_size: Some(5000),
            cold_count: Some(20),
            archive_size: None,
            archive_count: None,
        };

        assert_eq!(stats.total_size(), 8000);
        assert_eq!(stats.total_count(), 35);
    }
}
