//! Sort key for total ordering of records across a distributed cluster.
//!
//! The `SortKey` provides a 128-bit composite key that ensures deterministic
//! ordering even when multiple nodes produce events at the same logical time.
//!
//! # Structure
//!
//! - **HLC timestamp (64 bits)**: Hybrid Logical Clock for causal ordering
//! - **Node ID (16 bits)**: Tie-breaker for concurrent events (lower wins)
//! - **Sequence (48 bits)**: Per-node monotonic counter for total ordering

use crate::hlc::HlcTimestamp;
use zerocopy::{FromBytes, IntoBytes};

/// 128-bit composite sort key for total ordering across distributed nodes.
///
/// Records are ordered by:
/// 1. HLC timestamp (causal ordering)
/// 2. Node ID (tie-breaker, lower wins)
/// 3. Sequence number (per-node ordering)
#[repr(C)]
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, IntoBytes, FromBytes,
)]
pub struct SortKey {
    /// HLC timestamp (64 bits) - provides causal ordering across nodes.
    pub hlc: HlcTimestamp,
    /// Packed `node_id` (16 bits) + sequence (48 bits).
    /// Layout: [`node_id`: 16 bits][sequence: 48 bits]
    pub node_seq: u64,
}

impl SortKey {
    /// Bits allocated to the sequence counter.
    const SEQUENCE_BITS: u32 = 48;

    /// Mask for extracting the sequence counter.
    const SEQUENCE_MASK: u64 = (1 << Self::SEQUENCE_BITS) - 1;

    /// Create a new sort key from components.
    ///
    /// # Arguments
    ///
    /// * `hlc` - Hybrid Logical Clock timestamp
    /// * `node_id` - Node identifier (lower wins in tie-breaks)
    /// * `sequence` - Per-node monotonic sequence (only lower 48 bits used)
    #[inline]
    #[must_use]
    pub const fn new(hlc: HlcTimestamp, node_id: u16, sequence: u64) -> Self {
        let node_seq = ((node_id as u64) << Self::SEQUENCE_BITS) | (sequence & Self::SEQUENCE_MASK);
        Self { hlc, node_seq }
    }

    /// Create a sort key from raw timestamp (legacy compatibility).
    ///
    /// Converts nanosecond timestamp to HLC milliseconds with zero logical counter.
    #[inline]
    #[must_use]
    pub const fn from_timestamp_ns(timestamp_ns: u64, sequence: u32) -> Self {
        let physical_ms = timestamp_ns / 1_000_000;
        let hlc = HlcTimestamp::new(physical_ms, 0);
        Self::new(hlc, 0, sequence as u64)
    }

    /// Create a zero (minimum) sort key.
    #[inline]
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            hlc: HlcTimestamp::zero(),
            node_seq: 0,
        }
    }

    /// Create a maximum sort key.
    #[inline]
    #[must_use]
    pub const fn max() -> Self {
        Self {
            hlc: HlcTimestamp::max(),
            node_seq: u64::MAX,
        }
    }

    /// Get the HLC timestamp component.
    #[inline]
    #[must_use]
    pub const fn hlc(&self) -> HlcTimestamp {
        self.hlc
    }

    /// Get the node ID component.
    #[inline]
    #[must_use]
    pub const fn node_id(&self) -> u16 {
        (self.node_seq >> Self::SEQUENCE_BITS) as u16
    }

    /// Get the sequence number component.
    #[inline]
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.node_seq & Self::SEQUENCE_MASK
    }

    /// Get approximate timestamp in nanoseconds (for compatibility).
    ///
    /// Note: This loses precision since HLC uses milliseconds internally.
    #[inline]
    #[must_use]
    pub const fn timestamp_ns(&self) -> u64 {
        self.hlc.to_nanos_approx()
    }

    /// Convert to a 128-bit integer for atomic operations or serialization.
    #[inline]
    #[must_use]
    pub const fn to_u128(&self) -> u128 {
        ((self.hlc.as_u64() as u128) << 64) | (self.node_seq as u128)
    }

    /// Create from a 128-bit integer.
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn from_u128(value: u128) -> Self {
        let hlc = HlcTimestamp::from_raw((value >> 64) as u64);
        let node_seq = value as u64;
        Self { hlc, node_seq }
    }

    /// Get the size of a sort key in bytes.
    #[inline]
    #[must_use]
    pub const fn size() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl std::fmt::Display for SortKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SortKey({}, node={}, seq={})",
            self.hlc,
            self.node_id(),
            self.sequence()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_key_ordering() {
        let hlc1 = HlcTimestamp::new(100, 0);
        let hlc2 = HlcTimestamp::new(100, 1);
        let hlc3 = HlcTimestamp::new(200, 0);

        let k1 = SortKey::new(hlc1, 1, 0);
        let k2 = SortKey::new(hlc2, 1, 0);
        let k3 = SortKey::new(hlc3, 1, 0);

        assert!(k1 < k2, "Higher logical counter should be greater");
        assert!(k2 < k3, "Higher physical time should be greater");
        assert!(k1 < k3);
    }

    #[test]
    fn test_sort_key_node_id_tiebreak() {
        let hlc = HlcTimestamp::new(100, 0);

        let k1 = SortKey::new(hlc, 1, 0);
        let k2 = SortKey::new(hlc, 2, 0);

        assert!(k1 < k2, "Lower node_id should win tie-break");
    }

    #[test]
    fn test_sort_key_sequence_ordering() {
        let hlc = HlcTimestamp::new(100, 0);

        let k1 = SortKey::new(hlc, 1, 0);
        let k2 = SortKey::new(hlc, 1, 1);
        let k3 = SortKey::new(hlc, 1, 100);

        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn test_sort_key_component_extraction() {
        let hlc = HlcTimestamp::new(12345, 42);
        let key = SortKey::new(hlc, 7, 999);

        assert_eq!(key.hlc().physical_ms(), 12345);
        assert_eq!(key.hlc().logical(), 42);
        assert_eq!(key.node_id(), 7);
        assert_eq!(key.sequence(), 999);
    }

    #[test]
    fn test_sort_key_u128_roundtrip() {
        let hlc = HlcTimestamp::new(12345, 42);
        let original = SortKey::new(hlc, 7, 999);

        let as_u128 = original.to_u128();
        let restored = SortKey::from_u128(as_u128);

        assert_eq!(original, restored);
    }

    #[test]
    fn test_sort_key_size() {
        assert_eq!(SortKey::size(), 16);
    }

    #[test]
    fn test_sort_key_legacy_compatibility() {
        let key = SortKey::from_timestamp_ns(1_000_000_000, 42);

        // 1 second in nanoseconds = 1000 milliseconds
        assert_eq!(key.hlc().physical_ms(), 1000);
        assert_eq!(key.sequence(), 42);
    }
}
