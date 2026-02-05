use crate::{DEFAULT_BATCH_SIZE, LanceError, Result, SortKey};
use bytes::Bytes;
use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

#[repr(C, align(8))]
pub struct IngestionBatch {
    pub batch_id: u64,
    pub sort_key: SortKey,
    pub payload: Bytes,
    pub record_count: u32,
    pub crc: u32,
}

impl IngestionBatch {
    #[inline]
    #[must_use]
    pub fn new(
        batch_id: u64,
        sort_key: SortKey,
        payload: Bytes,
        record_count: u32,
        crc: u32,
    ) -> Self {
        Self {
            batch_id,
            sort_key,
            payload,
            record_count,
            crc,
        }
    }

    #[inline]
    #[must_use]
    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }
}

#[repr(C, align(4096))]
pub struct LoanableBatch {
    pub batch_id: u64,
    data: Box<[u8]>,
    len: usize,
    capacity: usize,
}

impl LoanableBatch {
    /// Create a new loanable batch with the given capacity.
    #[must_use]
    pub fn new(batch_id: u64, capacity: usize) -> Self {
        let data = vec![0u8; capacity].into_boxed_slice();
        Self {
            batch_id,
            data,
            len: 0,
            capacity,
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.len = 0;
    }

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data[..self.len]
    }

    #[inline]
    #[must_use]
    pub fn remaining_capacity(&self) -> usize {
        self.capacity - self.len
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    /// Append data to the batch.
    ///
    /// # Errors
    /// Returns an error if there is insufficient capacity.
    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > self.remaining_capacity() {
            return Err(LanceError::BufferPoolExhausted);
        }
        self.data[self.len..self.len + data.len()].copy_from_slice(data);
        self.len += data.len();
        Ok(())
    }

    #[inline]
    pub fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.capacity);
        self.len = len.min(self.capacity);
    }
}

pub struct BatchPool {
    free_list: Arc<ArrayQueue<LoanableBatch>>,
    batch_capacity: usize,
}

impl BatchPool {
    /// Create a new batch pool.
    ///
    /// # Errors
    /// Returns an error if pool creation fails.
    pub fn new(pool_size: usize, batch_capacity: usize) -> Result<Self> {
        let free_list = Arc::new(ArrayQueue::new(pool_size));

        for i in 0..pool_size {
            let batch = LoanableBatch::new(i as u64, batch_capacity);
            if free_list.push(batch).is_err() {
                return Err(LanceError::BufferPoolExhausted);
            }
        }

        Ok(Self {
            free_list,
            batch_capacity,
        })
    }

    /// Create a new batch pool with default batch size.
    ///
    /// # Errors
    /// Returns an error if pool creation fails.
    pub fn with_default_size(pool_size: usize) -> Result<Self> {
        Self::new(pool_size, DEFAULT_BATCH_SIZE)
    }

    /// Acquire a batch from the pool.
    ///
    /// # Errors
    /// Returns an error if the pool is exhausted.
    #[inline]
    pub fn acquire(&self) -> Result<LoanableBatch> {
        self.free_list.pop().ok_or(LanceError::BufferPoolExhausted)
    }

    #[inline]
    pub fn release(&self, mut batch: LoanableBatch) {
        batch.reset();
        let _ = self.free_list.push(batch);
    }

    #[inline]
    #[must_use]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    #[inline]
    #[must_use]
    pub fn batch_capacity(&self) -> usize {
        self.batch_capacity
    }
}

impl Clone for BatchPool {
    fn clone(&self) -> Self {
        Self {
            free_list: Arc::clone(&self.free_list),
            batch_capacity: self.batch_capacity,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_pool_acquire_release() {
        let pool = BatchPool::new(4, 1024).unwrap();
        assert_eq!(pool.available(), 4);

        let batch1 = pool.acquire().unwrap();
        assert_eq!(pool.available(), 3);

        let batch2 = pool.acquire().unwrap();
        assert_eq!(pool.available(), 2);

        pool.release(batch1);
        assert_eq!(pool.available(), 3);

        pool.release(batch2);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_batch_pool_exhaustion() {
        let pool = BatchPool::new(2, 1024).unwrap();

        let _b1 = pool.acquire().unwrap();
        let _b2 = pool.acquire().unwrap();

        let result = pool.acquire();
        assert!(result.is_err());
    }

    #[test]
    fn test_loanable_batch_append() {
        let mut batch = LoanableBatch::new(0, 1024);
        assert!(batch.is_empty());

        batch.append(b"hello").unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(batch.as_slice(), b"hello");

        batch.append(b" world").unwrap();
        assert_eq!(batch.len(), 11);
        assert_eq!(batch.as_slice(), b"hello world");
    }

    #[test]
    fn test_loanable_batch_reset() {
        let mut batch = LoanableBatch::new(0, 1024);
        batch.append(b"test data").unwrap();
        assert!(!batch.is_empty());

        batch.reset();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }
}
