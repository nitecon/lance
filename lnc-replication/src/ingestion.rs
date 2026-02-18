//! Zero-copy ingestion layer using AsyncIoBackend.
//!
//! This module implements the handoff from network (Bytes) to disk (LoanableBatch)
//! with a single unavoidable memcpy, then zero-copy kernel submission.

use bytes::Bytes;
use lnc_core::{BatchPool, Result};
use lnc_io::{AsyncIoBackend, IoPriority};
use std::sync::Arc;

/// Ingestion handler that writes data using AsyncIoBackend.
///
/// # Flow
/// 1. Acquire `LoanableBatch` from pool (pre-allocated, NUMA-aligned)
/// 2. Copy `Bytes` → `LoanableBatch` (single memcpy, unavoidable)
/// 3. Submit to `AsyncIoBackend` with priority (zero-copy to kernel)
/// 4. Await completion
/// 5. Recycle batch to pool
pub struct IngestionHandler {
    batch_pool: BatchPool,
    io_backend: Arc<dyn AsyncIoBackend>,
}

impl IngestionHandler {
    /// Create a new ingestion handler.
    ///
    /// # Arguments
    /// * `batch_pool` - Pre-allocated pool of aligned batches
    /// * `io_backend` - Async I/O backend (typically AsyncIoUringBackend)
    pub fn new(batch_pool: BatchPool, io_backend: Arc<dyn AsyncIoBackend>) -> Self {
        Self {
            batch_pool,
            io_backend,
        }
    }

    /// Write data to disk using zero-copy I/O.
    ///
    /// # Arguments
    /// * `data` - Network payload (Arc-based, no copy on clone)
    /// * `offset` - File offset to write to
    /// * `priority` - I/O priority class
    ///
    /// # Returns
    /// Number of bytes written
    ///
    /// # Errors
    /// Returns error if pool exhausted, I/O submission fails, or write fails
    pub async fn write(&self, data: Bytes, offset: u64, priority: IoPriority) -> Result<usize> {
        // 1. Acquire batch from pool (lock-free, pre-allocated)
        let mut batch = self.batch_pool.acquire()?;

        // 2. Single memcpy: Bytes → LoanableBatch
        // This is unavoidable because network layer uses Arc<[u8]> (Bytes)
        // and kernel needs page-aligned, pinned memory (LoanableBatch)
        batch.append(&data)?;

        // 3. Zero-copy handoff to kernel with priority
        let write_future = self.io_backend.submit_write(batch, offset, priority)?;

        let (returned_batch, result) = write_future.await;

        // 4. Check I/O result
        let bytes_written = result?;

        // 5. Recycle batch to pool
        self.batch_pool.release(returned_batch);

        Ok(bytes_written)
    }

    /// Get the number of available batches in the pool.
    #[inline]
    pub fn available_batches(&self) -> usize {
        self.batch_pool.available()
    }

    /// Get the batch capacity.
    #[inline]
    pub fn batch_capacity(&self) -> usize {
        self.batch_pool.batch_capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lnc_core::{DEFAULT_BATCH_SIZE, LoanableBatch};

    #[tokio::test]
    async fn test_ingestion_handler_creation() {
        let pool = BatchPool::with_default_size(4).unwrap();
        let backend: Arc<dyn AsyncIoBackend> = Arc::new(MockBackend);
        let handler = IngestionHandler::new(pool, backend);

        assert_eq!(handler.available_batches(), 4);
        assert_eq!(handler.batch_capacity(), DEFAULT_BATCH_SIZE);
    }

    // Mock backend for testing
    struct MockBackend;

    impl AsyncIoBackend for MockBackend {
        fn submit_write(
            &self,
            batch: LoanableBatch,
            _offset: u64,
            _priority: IoPriority,
        ) -> Result<lnc_io::WriteFuture> {
            let len = batch.len();
            Ok(lnc_io::WriteFuture::ready((batch, Ok(len))))
        }
    }
}
