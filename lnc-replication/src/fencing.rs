//! Fencing token implementation for preventing stale leader writes.
//!
//! Fencing tokens are monotonically increasing identifiers that encode
//! (term, node_id) to ensure uniqueness. They are used to revoke write
//! authority from deposed leaders.

/// Fencing token for preventing stale leaders from writing.
///
/// The token encodes (term, node_id) to ensure uniqueness and monotonicity.
/// High 48 bits = term, low 16 bits = node_id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FencingToken(u64);

impl FencingToken {
    /// Create a new fencing token from term and node ID.
    #[inline]
    #[must_use]
    pub const fn new(term: u64, node_id: u16) -> Self {
        // High 48 bits = term, low 16 bits = node_id
        Self((term << 16) | (node_id as u64))
    }

    /// Extract the term from the token.
    #[inline]
    #[must_use]
    pub const fn term(&self) -> u64 {
        self.0 >> 16
    }

    /// Extract the node ID from the token.
    #[inline]
    #[must_use]
    pub const fn node_id(&self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    /// Get the raw u64 value.
    #[inline]
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for FencingToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fence(term={}, node={})", self.term(), self.node_id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fencing_token_creation() {
        let token = FencingToken::new(5, 3);
        assert_eq!(token.term(), 5);
        assert_eq!(token.node_id(), 3);
    }

    #[test]
    fn test_fencing_token_ordering() {
        let t1 = FencingToken::new(1, 1);
        let t2 = FencingToken::new(1, 2);
        let t3 = FencingToken::new(2, 1);

        assert!(t1 < t2); // Same term, higher node_id
        assert!(t2 < t3); // Higher term wins
        assert!(t1 < t3);
    }

    #[test]
    fn test_fencing_token_display() {
        let token = FencingToken::new(10, 5);
        assert_eq!(format!("{}", token), "Fence(term=10, node=5)");
    }
}
