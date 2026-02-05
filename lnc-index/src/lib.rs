#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

pub mod builder;
pub mod secondary;
pub mod sparse;

pub use builder::IndexBuilder;
pub use secondary::{
    JitterQueryResult, JitterWindowConfig, SecondaryIndex, SecondaryIndexWriter, TimestampEntry,
};
pub use sparse::{IndexEntry, SparseIndex, SparseIndexWriter};

pub const INDEX_FILE_EXTENSION: &str = "idx";
pub const SECONDARY_INDEX_EXTENSION: &str = "sidx";
