use crate::{LanceError, Result};
use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;

const PAGE_SIZE: usize = 4096;

#[repr(C, align(4096))]
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    capacity: usize,
}

impl AlignedBuffer {
    /// Create a new aligned buffer with the given capacity.
    ///
    /// # Errors
    /// Returns an error if memory allocation fails.
    pub fn new(capacity: usize) -> Result<Self> {
        let aligned_capacity = (capacity + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
        let layout = Layout::from_size_align(aligned_capacity, PAGE_SIZE)
            .map_err(|_| LanceError::NumaAllocFailed(0))?;

        // SAFETY: Layout is valid (checked above), alloc returns valid ptr or null
        let ptr = unsafe {
            let raw = alloc(layout);
            if raw.is_null() {
                return Err(LanceError::NumaAllocFailed(0));
            }
            // SAFETY: We just verified raw is not null
            NonNull::new_unchecked(raw)
        };

        Ok(Self {
            ptr,
            len: 0,
            capacity: aligned_capacity,
        })
    }

    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid, len <= capacity, and memory is initialized
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid, len <= capacity, and we have exclusive access
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
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
    pub fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.capacity);
        self.len = len.min(self.capacity);
    }

    /// Write data at the specified offset.
    ///
    /// # Errors
    /// Returns an error if write exceeds buffer capacity.
    pub fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        if offset + data.len() > self.capacity {
            return Err(LanceError::IndexOutOfBounds(format!(
                "write at offset {} with len {} exceeds capacity {}",
                offset,
                data.len(),
                self.capacity
            )));
        }

        // SAFETY: Bounds checked above, ptr is valid, regions don't overlap
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr().add(offset), data.len());
        }

        if offset + data.len() > self.len {
            self.len = offset + data.len();
        }

        Ok(())
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Lock the buffer in memory using mlock to prevent page faults.
    /// This ensures the buffer pages are resident in RAM and won't be swapped.
    ///
    /// # Errors
    /// Returns an error if mlock fails (e.g., insufficient privileges or limits).
    #[cfg(target_os = "linux")]
    pub fn mlock(&self) -> Result<()> {
        // SAFETY: ptr is valid and capacity is the correct size
        let result =
            unsafe { libc::mlock(self.ptr.as_ptr() as *const libc::c_void, self.capacity) };
        if result != 0 {
            return Err(LanceError::MlockFailed(
                std::io::Error::last_os_error().to_string(),
            ));
        }
        Ok(())
    }

    /// Lock the buffer in memory (no-op on non-Linux platforms).
    ///
    /// # Errors
    ///
    /// Returns `Ok(())` on non-Linux platforms (no-op).
    #[cfg(not(target_os = "linux"))]
    pub fn mlock(&self) -> Result<()> {
        Ok(())
    }

    /// Prefault all pages in the buffer by touching each page.
    /// This ensures pages are allocated and mapped before use on the hot path.
    pub fn prefault(&mut self) {
        let page_count = self.capacity.div_ceil(PAGE_SIZE);
        // SAFETY: We touch each page within our allocated capacity
        unsafe {
            let ptr = self.ptr.as_ptr();
            for i in 0..page_count {
                // Write a zero byte to each page to fault it in
                std::ptr::write_volatile(ptr.add(i * PAGE_SIZE), 0);
            }
        }
    }

    /// Lock and prefault the buffer for zero-latency access on hot path.
    ///
    /// # Errors
    /// Returns an error if mlock fails.
    pub fn lock_and_prefault(&mut self) -> Result<()> {
        self.prefault();
        self.mlock()
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: Layout was valid during allocation, so it must be valid here
        if let Ok(layout) = Layout::from_size_align(self.capacity, PAGE_SIZE) {
            unsafe {
                dealloc(self.ptr.as_ptr(), layout);
            }
        }
    }
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

pub struct NumaAlignedBuffer {
    inner: AlignedBuffer,
    numa_node: usize,
}

impl NumaAlignedBuffer {
    /// Create a new NUMA-aligned buffer.
    ///
    /// # Errors
    /// Returns an error if NUMA allocation fails.
    #[cfg(target_os = "linux")]
    pub fn new(capacity: usize, numa_node: usize) -> Result<Self> {
        let aligned_capacity = (capacity + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);

        // SAFETY: mmap with MAP_ANONYMOUS creates new private mapping,
        // mbind binds pages to NUMA node, munmap cleans up on failure
        let ptr = unsafe {
            let raw = libc::mmap(
                std::ptr::null_mut(),
                aligned_capacity,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            );

            if raw == libc::MAP_FAILED {
                return Err(LanceError::NumaAllocFailed(numa_node));
            }

            let nodemask: u64 = 1u64 << numa_node;
            let result = libc::syscall(
                libc::SYS_mbind,
                raw,
                aligned_capacity,
                1i32,
                std::ptr::addr_of!(nodemask),
                64usize,
                0i32,
            );

            if result != 0 {
                libc::munmap(raw, aligned_capacity);
                return Err(LanceError::NumaAllocFailed(numa_node));
            }

            NonNull::new(raw.cast::<u8>()).ok_or(LanceError::NumaAllocFailed(numa_node))?
        };

        Ok(Self {
            inner: AlignedBuffer {
                ptr,
                len: 0,
                capacity: aligned_capacity,
            },
            numa_node,
        })
    }

    /// Create a new NUMA-aligned buffer.
    ///
    /// # Errors
    /// Returns an error if NUMA allocation fails.
    #[cfg(not(target_os = "linux"))]
    pub fn new(capacity: usize, numa_node: usize) -> Result<Self> {
        Ok(Self {
            inner: AlignedBuffer::new(capacity)?,
            numa_node,
        })
    }

    #[inline]
    #[must_use]
    pub fn numa_node(&self) -> usize {
        self.numa_node
    }

    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.inner.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.inner.as_mut_ptr()
    }

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.inner.as_mut_slice()
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub fn set_len(&mut self, len: usize) {
        self.inner.set_len(len);
    }

    /// Write data at the specified offset.
    ///
    /// # Errors
    /// Returns an error if write exceeds buffer capacity.
    pub fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        self.inner.write(offset, data)
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Lock the buffer in memory using mlock to prevent page faults.
    ///
    /// # Errors
    /// Returns an error if mlock fails.
    pub fn mlock(&self) -> Result<()> {
        self.inner.mlock()
    }

    /// Prefault all pages in the buffer by touching each page.
    pub fn prefault(&mut self) {
        self.inner.prefault();
    }

    /// Lock and prefault the buffer for zero-latency access on hot path.
    ///
    /// # Errors
    /// Returns an error if mlock fails.
    pub fn lock_and_prefault(&mut self) -> Result<()> {
        self.inner.lock_and_prefault()
    }
}

#[cfg(target_os = "linux")]
impl Drop for NumaAlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(
                self.inner.ptr.as_ptr().cast::<libc::c_void>(),
                self.inner.capacity,
            );
        }
        self.inner.capacity = 0;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer_creation() {
        let buffer = AlignedBuffer::new(8192).unwrap();
        assert!(buffer.as_ptr() as usize % PAGE_SIZE == 0);
        assert!(buffer.capacity() >= 8192);
    }

    #[test]
    fn test_aligned_buffer_write() {
        let mut buffer = AlignedBuffer::new(4096).unwrap();
        buffer.write(0, b"hello world").unwrap();
        assert_eq!(&buffer.as_slice()[..11], b"hello world");
    }

    #[test]
    fn test_aligned_buffer_bounds() {
        let mut buffer = AlignedBuffer::new(4096).unwrap();
        let result = buffer.write(4090, &[0u8; 100]);
        assert!(result.is_err());
    }

    #[test]
    fn test_aligned_buffer_prefault() {
        let mut buffer = AlignedBuffer::new(8192).unwrap();
        // Prefault should complete without panic
        buffer.prefault();
        // Verify buffer is still usable
        buffer.write(0, b"test data after prefault").unwrap();
        assert_eq!(&buffer.as_slice()[..24], b"test data after prefault");
    }

    #[test]
    fn test_aligned_buffer_lock_and_prefault() {
        let mut buffer = AlignedBuffer::new(4096).unwrap();
        // On non-Linux or without privileges, mlock may fail but prefault should work
        let _ = buffer.lock_and_prefault();
        // Buffer should still be usable regardless
        buffer.write(0, b"locked data").unwrap();
        assert_eq!(&buffer.as_slice()[..11], b"locked data");
    }
}
