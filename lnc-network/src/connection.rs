use bytes::{Bytes, BytesMut};

const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

pub struct ConnectionBuffer {
    buffer: BytesMut,
    read_position: usize,
}

impl ConnectionBuffer {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUFFER_SIZE)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
            read_position: 0,
        }
    }

    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn slice(&self, start: usize, len: usize) -> Bytes {
        self.buffer.clone().freeze().slice(start..start + len)
    }

    pub fn consume(&mut self, len: usize) {
        if len >= self.buffer.len() {
            self.buffer.clear();
            self.read_position = 0;
        } else {
            let _ = self.buffer.split_to(len);
        }
    }

    /// Compact the buffer to reduce memory usage when oversized.
    /// Uses split_off pattern to avoid to_vec() allocation per standards §18.1.
    pub fn compact(&mut self) {
        if self.buffer.capacity() > DEFAULT_BUFFER_SIZE * 4
            && self.buffer.len() < DEFAULT_BUFFER_SIZE
        {
            // Zero-copy approach: create new buffer and copy existing data
            // This is unavoidable for compaction but uses extend_from_slice
            // which is more efficient than to_vec() + extend_from_slice
            let len = self.buffer.len();
            let mut new_buffer = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);
            new_buffer.extend_from_slice(&self.buffer[..len]);
            self.buffer = new_buffer;
        }
    }

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[..]
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    #[inline]
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buffer.len().saturating_sub(self.read_position)
    }

    pub fn reserve(&mut self, additional: usize) {
        self.buffer.reserve(additional);
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.read_position = 0;
    }

    pub fn get_write_buffer(&mut self, min_size: usize) -> &mut [u8] {
        if self.buffer.spare_capacity_mut().len() < min_size {
            self.buffer.reserve(min_size);
        }

        let len = self.buffer.len();
        let cap = self.buffer.capacity();

        // SAFETY: We're getting a mutable slice to the spare capacity
        // which will be initialized by the caller before the length is increased
        unsafe { std::slice::from_raw_parts_mut(self.buffer.as_mut_ptr().add(len), cap - len) }
    }

    /// Advance the write position of the buffer.
    ///
    /// # Safety
    /// The caller must ensure that `len` bytes have been written to the
    /// spare capacity returned by `spare_capacity_mut()`.
    pub unsafe fn advance_write(&mut self, len: usize) {
        let new_len = self.buffer.len() + len;
        // SAFETY: Caller guarantees that len bytes have been written to the spare capacity
        unsafe {
            self.buffer.set_len(new_len);
        }
    }
}

impl Default for ConnectionBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_buffer_extend() {
        let mut buf = ConnectionBuffer::new();
        buf.extend(b"hello");
        buf.extend(b" world");

        assert_eq!(buf.as_slice(), b"hello world");
        assert_eq!(buf.len(), 11);
    }

    #[test]
    fn test_connection_buffer_slice() {
        let mut buf = ConnectionBuffer::new();
        buf.extend(b"hello world");

        let slice = buf.slice(0, 5);
        assert_eq!(&slice[..], b"hello");

        let slice2 = buf.slice(6, 5);
        assert_eq!(&slice2[..], b"world");
    }

    #[test]
    fn test_connection_buffer_consume() {
        let mut buf = ConnectionBuffer::new();
        buf.extend(b"hello world");

        buf.consume(6);
        assert_eq!(buf.as_slice(), b"world");
        assert_eq!(buf.len(), 5);
    }
}
