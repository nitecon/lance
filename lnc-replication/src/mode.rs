#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicationMode {
    /// L1: Single node, no replication (standalone)
    #[default]
    L1,
    /// L3: Filesystem-consistent quorum replication
    L3,
}

impl ReplicationMode {
    #[inline]
    #[must_use]
    pub const fn requires_quorum(&self) -> bool {
        matches!(self, Self::L3)
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
            Self::L1 => "L1 (Standalone)",
            Self::L3 => "L3 (Quorum)",
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
            "l3" | "sync" | "quorum" | "sync-quorum" => Ok(Self::L3),
            _ => Err(format!(
                "Unknown replication mode: '{}'. Valid modes: l1, l3",
                s
            )),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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
        assert!(ReplicationMode::L3.requires_quorum());
    }

    #[test]
    fn test_replication_mode_is_synchronous() {
        assert!(!ReplicationMode::L1.is_synchronous());
        assert!(ReplicationMode::L3.is_synchronous());
    }

    #[test]
    fn test_replication_mode_name() {
        assert_eq!(ReplicationMode::L1.name(), "L1 (Standalone)");
        assert_eq!(ReplicationMode::L3.name(), "L3 (Quorum)");
    }

    #[test]
    fn test_replication_mode_display() {
        assert_eq!(format!("{}", ReplicationMode::L1), "L1 (Standalone)");
        assert_eq!(format!("{}", ReplicationMode::L3), "L3 (Quorum)");
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
        assert_eq!(ReplicationMode::L3, ReplicationMode::L3);
        assert_ne!(ReplicationMode::L1, ReplicationMode::L3);
    }

    #[test]
    fn test_replication_mode_clone() {
        let original = ReplicationMode::L3;
        let cloned = original;
        assert_eq!(original, cloned);
    }
}
