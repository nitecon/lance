#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicationMode {
    /// L1: Single node, no replication
    #[default]
    L1,
    /// L2: Async replication to followers
    L2,
    /// L3: Sync replication with quorum
    L3,
}

impl ReplicationMode {
    #[inline]
    #[must_use]
    pub const fn requires_quorum(&self) -> bool {
        matches!(self, Self::L2 | Self::L3)
    }

    #[inline]
    #[must_use]
    pub const fn is_synchronous(&self) -> bool {
        matches!(self, Self::L3)
    }

    #[inline]
    #[must_use]
    pub const fn is_replicated(&self) -> bool {
        !matches!(self, Self::L1)
    }

    #[inline]
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::L1 => "L1 (Log-Based)",
            Self::L2 => "L2 (Async Replication)",
            Self::L3 => "L3 (Sync Quorum)",
        }
    }
}

impl std::fmt::Display for ReplicationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl std::str::FromStr for ReplicationMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "l1" | "log" | "log-based" | "standalone" => Ok(Self::L1),
            "l2" | "async" | "async-replication" => Ok(Self::L2),
            "l3" | "sync" | "quorum" | "sync-quorum" => Ok(Self::L3),
            _ => Err(format!("Unknown replication mode: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_mode_default() {
        let mode = ReplicationMode::default();
        assert_eq!(mode, ReplicationMode::L1);
    }

    #[test]
    fn test_replication_mode_requires_quorum() {
        assert!(!ReplicationMode::L1.requires_quorum());
        assert!(ReplicationMode::L2.requires_quorum());
    }

    #[test]
    fn test_replication_mode_is_synchronous() {
        assert!(!ReplicationMode::L1.is_synchronous());
        assert!(!ReplicationMode::L2.is_synchronous());
        assert!(ReplicationMode::L3.is_synchronous());
    }

    #[test]
    fn test_replication_mode_name() {
        assert_eq!(ReplicationMode::L1.name(), "L1 (Log-Based)");
        assert_eq!(ReplicationMode::L2.name(), "L2 (Async Replication)");
        assert_eq!(ReplicationMode::L3.name(), "L3 (Sync Quorum)");
    }

    #[test]
    fn test_replication_mode_display() {
        assert_eq!(format!("{}", ReplicationMode::L1), "L1 (Log-Based)");
        assert_eq!(format!("{}", ReplicationMode::L2), "L2 (Async Replication)");
        assert_eq!(format!("{}", ReplicationMode::L3), "L3 (Sync Quorum)");
    }

    #[test]
    fn test_replication_mode_from_str() {
        // L1 variants
        assert_eq!(
            "l1".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L1
        );
        assert_eq!(
            "L1".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L1
        );
        assert_eq!(
            "log".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L1
        );
        assert_eq!(
            "log-based".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L1
        );
        assert_eq!(
            "standalone".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L1
        );

        // L2 variants
        assert_eq!(
            "l2".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L2
        );
        assert_eq!(
            "L2".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L2
        );
        assert_eq!(
            "async".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L2
        );
        assert_eq!(
            "async-replication".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L2
        );

        // L3 variants
        assert_eq!(
            "l3".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L3
        );
        assert_eq!(
            "L3".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L3
        );
        assert_eq!(
            "sync".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L3
        );
        assert_eq!(
            "quorum".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L3
        );
        assert_eq!(
            "sync-quorum".parse::<ReplicationMode>().unwrap(),
            ReplicationMode::L3
        );
    }

    #[test]
    fn test_replication_mode_from_str_invalid() {
        let result = "invalid".parse::<ReplicationMode>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown replication mode"));
    }

    #[test]
    fn test_replication_mode_equality() {
        assert_eq!(ReplicationMode::L1, ReplicationMode::L1);
        assert_eq!(ReplicationMode::L2, ReplicationMode::L2);
        assert_ne!(ReplicationMode::L1, ReplicationMode::L2);
    }

    #[test]
    fn test_replication_mode_clone() {
        let original = ReplicationMode::L2;
        let cloned = original;
        assert_eq!(original, cloned);
    }
}
