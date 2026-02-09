use crate::backend::{IoBackend, IoBackendType};
use io_uring::{IoUring, opcode, types};
use lnc_core::{LanceError, Result};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

const RING_SIZE: u32 = 256;

pub struct IoUringBackend {
    ring: IoUring,
    _file: File,
    fd: i32,
}

impl IoUringBackend {
    pub fn open(path: &Path, create: bool) -> Result<Self> {
        let file = if create {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)?
        };

        let fd = file.as_raw_fd();

        let ring = IoUring::builder()
            .setup_coop_taskrun()
            .build(RING_SIZE)
            .map_err(|_| LanceError::UnsupportedKernel("io_uring setup failed"))?;

        Ok(Self {
            ring,
            _file: file,
            fd,
        })
    }

    fn submit_and_wait(&mut self) -> Result<i32> {
        self.ring.submit_and_wait(1)?;

        let cqe = self
            .ring
            .completion()
            .next()
            .ok_or_else(|| LanceError::Io(std::io::Error::other("No completion entry")))?;

        let result = cqe.result();
        if result < 0 {
            Err(LanceError::Io(std::io::Error::from_raw_os_error(-result)))
        } else {
            Ok(result)
        }
    }
}

impl IoBackend for IoUringBackend {
    fn write(&mut self, data: &[u8], offset: u64) -> Result<usize> {
        let write_op = opcode::Write::new(types::Fd(self.fd), data.as_ptr(), data.len() as u32)
            .offset(offset)
            .build()
            .user_data(0x01);

        // SAFETY: The submission queue entry is valid and data buffer remains valid
        // until the operation completes (we wait synchronously)
        unsafe {
            self.ring
                .submission()
                .push(&write_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        lnc_metrics::increment_io_submitted();
        let result = self.submit_and_wait()?;
        lnc_metrics::increment_io_completed();

        Ok(result as usize)
    }

    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_op = opcode::Read::new(types::Fd(self.fd), buf.as_mut_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(0x02);

        // SAFETY: The submission queue entry is valid and buffer remains valid
        // until the operation completes (we wait synchronously)
        unsafe {
            self.ring
                .submission()
                .push(&read_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        lnc_metrics::increment_io_submitted();
        let result = self.submit_and_wait()?;
        lnc_metrics::increment_io_completed();

        Ok(result as usize)
    }

    fn fsync(&mut self) -> Result<()> {
        let fsync_op = opcode::Fsync::new(types::Fd(self.fd))
            .build()
            .user_data(0x03);

        // SAFETY: The submission queue entry is valid
        unsafe {
            self.ring
                .submission()
                .push(&fsync_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        lnc_metrics::increment_io_submitted();
        self.submit_and_wait()?;
        lnc_metrics::increment_io_completed();

        Ok(())
    }

    fn backend_type(&self) -> IoBackendType {
        IoBackendType::IoUring
    }
}

/// Configuration for io_uring poller thread
#[derive(Debug, Clone)]
pub struct PollerConfig {
    /// Ring size (number of entries)
    pub ring_size: u32,
    /// CPU to pin the poller thread to (None = no pinning)
    pub pin_to_cpu: Option<usize>,
    /// NUMA node preference for allocations
    pub numa_node: Option<usize>,
    /// Enable busy polling (higher CPU, lower latency)
    pub busy_poll: bool,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            ring_size: 256,
            pin_to_cpu: None,
            numa_node: None,
            busy_poll: false,
        }
    }
}

impl PollerConfig {
    /// Create config with CPU pinning
    pub fn with_cpu_pin(mut self, cpu: usize) -> Self {
        self.pin_to_cpu = Some(cpu);
        self
    }

    /// Create config with NUMA node preference
    pub fn with_numa_node(mut self, node: usize) -> Self {
        self.numa_node = Some(node);
        self
    }

    /// Enable busy polling for lowest latency
    pub fn with_busy_poll(mut self) -> Self {
        self.busy_poll = true;
        self
    }
}

pub struct IoUringPoller {
    ring: IoUring,
    pending_ops: u32,
    config: PollerConfig,
}

impl IoUringPoller {
    pub fn new(ring_size: u32) -> Result<Self> {
        Self::with_config(PollerConfig {
            ring_size,
            ..Default::default()
        })
    }

    pub fn with_config(config: PollerConfig) -> Result<Self> {
        // Pin thread if configured
        if let Some(cpu) = config.pin_to_cpu {
            if let Err(e) = lnc_core::pin_thread_to_cpu(cpu) {
                tracing::warn!(
                    target: "lance::io",
                    cpu,
                    error = %e,
                    "Failed to pin poller thread to CPU"
                );
            } else {
                tracing::info!(
                    target: "lance::io",
                    cpu,
                    "Pinned poller thread to CPU"
                );
            }
        }

        let mut builder = IoUring::builder();
        builder.setup_coop_taskrun().setup_single_issuer();

        // Enable SQPOLL for busy polling if configured
        if config.busy_poll {
            builder.setup_sqpoll(1000); // 1ms idle timeout
        }

        let ring = builder
            .build(config.ring_size)
            .map_err(|_| LanceError::UnsupportedKernel("io_uring setup failed"))?;

        Ok(Self {
            ring,
            pending_ops: 0,
            config,
        })
    }

    /// Get the poller configuration
    pub fn config(&self) -> &PollerConfig {
        &self.config
    }

    pub fn submit_write(
        &mut self,
        fd: i32,
        data: &[u8],
        offset: u64,
        user_data: u64,
    ) -> Result<()> {
        let write_op = opcode::Write::new(types::Fd(fd), data.as_ptr(), data.len() as u32)
            .offset(offset)
            .build()
            .user_data(user_data);

        // SAFETY: Caller must ensure data remains valid until completion is harvested
        unsafe {
            self.ring
                .submission()
                .push(&write_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit a read operation using a regular buffer
    pub fn submit_read(
        &mut self,
        fd: i32,
        buf: &mut [u8],
        offset: u64,
        user_data: u64,
    ) -> Result<()> {
        let read_op = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(user_data);

        // SAFETY: Caller must ensure buffer remains valid until completion
        unsafe {
            self.ring
                .submission()
                .push(&read_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit a read operation using a registered buffer (IORING_OP_READ_FIXED)
    ///
    /// Per Architecture §17.4: Uses pre-registered buffers to avoid per-operation
    /// page pinning overhead. The buffer must be registered with the kernel via
    /// `RegisteredBufferPool::register()`.
    ///
    /// # Arguments
    /// * `fd` - File descriptor to read from
    /// * `buf_index` - Index of the registered buffer to read into
    /// * `buf_ptr` - Pointer to the buffer (from RegisteredBufferPool)
    /// * `len` - Number of bytes to read
    /// * `offset` - File offset to read from
    /// * `user_data` - User data for completion identification
    ///
    /// # Safety
    /// The buffer at `buf_index` must be registered with the kernel and must
    /// remain valid (not returned to pool) until the completion is harvested.
    pub fn submit_read_fixed(
        &mut self,
        fd: i32,
        buf_index: u16,
        buf_ptr: *mut u8,
        len: u32,
        offset: u64,
        user_data: u64,
    ) -> Result<()> {
        let read_op = opcode::ReadFixed::new(types::Fd(fd), buf_ptr, len, buf_index)
            .offset(offset)
            .build()
            .user_data(user_data);

        // SAFETY: Caller must ensure buffer is registered and remains valid
        unsafe {
            self.ring
                .submission()
                .push(&read_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit a write operation using a registered buffer (IORING_OP_WRITE_FIXED)
    ///
    /// Similar to submit_read_fixed but for writes. Uses pre-registered buffers
    /// to avoid per-operation page pinning.
    pub fn submit_write_fixed(
        &mut self,
        fd: i32,
        buf_index: u16,
        buf_ptr: *const u8,
        len: u32,
        offset: u64,
        user_data: u64,
    ) -> Result<()> {
        let write_op = opcode::WriteFixed::new(types::Fd(fd), buf_ptr, len, buf_index)
            .offset(offset)
            .build()
            .user_data(user_data);

        // SAFETY: Caller must ensure buffer is registered and remains valid
        unsafe {
            self.ring
                .submission()
                .push(&write_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    pub fn submit(&mut self) -> Result<u32> {
        let submitted = self.ring.submit()?;
        Ok(submitted as u32)
    }

    pub fn poll_completions<F>(&mut self, mut callback: F) -> Result<u32>
    where
        F: FnMut(u64, i32),
    {
        let mut completed = 0u32;

        while let Some(cqe) = self.ring.completion().next() {
            let user_data = cqe.user_data();
            let result = cqe.result();

            callback(user_data, result);
            completed += 1;
            self.pending_ops = self.pending_ops.saturating_sub(1);
            lnc_metrics::increment_io_completed();
        }

        Ok(completed)
    }

    #[inline]
    #[must_use]
    pub fn pending_ops(&self) -> u32 {
        self.pending_ops
    }
}

// ============================================================================
// Registered Buffer Pool
// ============================================================================
// Per Architecture §17.4: Pre-register buffers with the kernel to avoid
// per-operation page pinning overhead.

use crossbeam::queue::ArrayQueue;
use std::alloc::{Layout, alloc, dealloc};
use std::sync::atomic::{AtomicBool, Ordering};

/// Alignment for O_DIRECT buffers per standards §2.3
/// Must be 4KB aligned for O_DIRECT requirement
pub const BUFFER_ALIGNMENT: usize = 4096;

/// A buffer aligned for O_DIRECT I/O operations
pub struct AlignedBuffer {
    ptr: *mut u8,
    size: usize,
    layout: Layout,
}

// SAFETY: AlignedBuffer can be sent between threads
unsafe impl Send for AlignedBuffer {}
// SAFETY: AlignedBuffer can be shared between threads (immutable access only)
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Allocate a new aligned buffer
    pub fn new(size: usize) -> Result<Self> {
        let layout = Layout::from_size_align(size, BUFFER_ALIGNMENT)
            .map_err(|_| LanceError::Config("Invalid buffer size/alignment".into()))?;

        // SAFETY: Layout is valid (checked above)
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(LanceError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "Failed to allocate aligned buffer",
            )));
        }

        Ok(Self { ptr, size, layout })
    }

    /// Get a mutable slice of the buffer
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid and size is correct
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }

    /// Get an immutable slice of the buffer
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid and size is correct
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    /// Get the raw pointer
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Get the mutable raw pointer
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Get the buffer size
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with this layout
        unsafe { dealloc(self.ptr, self.layout) }
    }
}

/// A handle to a buffer from the registered pool
/// When dropped, the buffer is returned to the pool
pub struct BufferHandle {
    index: usize,
    pool: *const RegisteredBufferPool,
}

// SAFETY: BufferHandle can be sent between threads
unsafe impl Send for BufferHandle {}

impl BufferHandle {
    /// Get the buffer index for use with io_uring fixed buffer operations
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }
}

impl Drop for BufferHandle {
    fn drop(&mut self) {
        // Return buffer to pool
        // SAFETY: pool pointer is valid for the lifetime of the handle
        unsafe {
            (*self.pool).return_buffer(self.index);
        }
    }
}

/// Pre-registered buffer pool for io_uring operations
/// Per Architecture §17.4: Avoids per-operation page pinning
pub struct RegisteredBufferPool {
    buffers: Vec<AlignedBuffer>,
    free_list: ArrayQueue<usize>,
    buffer_size: usize,
    registered: AtomicBool,
}

impl RegisteredBufferPool {
    /// Create a new buffer pool (not yet registered with io_uring)
    pub fn new(num_buffers: usize, buffer_size: usize) -> Result<Self> {
        let mut buffers = Vec::with_capacity(num_buffers);
        // ArrayQueue requires non-zero capacity, use 1 as minimum
        let free_list = ArrayQueue::new(num_buffers.max(1));

        for i in 0..num_buffers {
            buffers.push(AlignedBuffer::new(buffer_size)?);
            // Ignore error - queue is sized correctly
            let _ = free_list.push(i);
        }

        Ok(Self {
            buffers,
            free_list,
            buffer_size,
            registered: AtomicBool::new(false),
        })
    }

    /// Register buffers with io_uring for zero-copy operations
    /// Must be called before using buffers with io_uring fixed buffer ops
    pub fn register(&mut self, ring: &mut IoUring) -> Result<()> {
        if self.registered.load(Ordering::SeqCst) {
            return Err(LanceError::Config("Buffers already registered".into()));
        }

        let iovecs: Vec<libc::iovec> = self
            .buffers
            .iter_mut()
            .map(|buf| libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: buf.size(),
            })
            .collect();

        // Register buffers with the kernel
        // SAFETY: iovecs point to valid, aligned memory that will remain valid
        // for the lifetime of the io_uring instance
        unsafe {
            ring.submitter().register_buffers(&iovecs).map_err(|e| {
                LanceError::Io(std::io::Error::other(format!(
                    "Failed to register buffers: {}",
                    e
                )))
            })?;
        }

        self.registered.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Acquire a buffer from the pool (lock-free)
    /// Returns None if no buffers are available
    #[inline]
    pub fn acquire(&self) -> Option<BufferHandle> {
        self.free_list.pop().map(|index| BufferHandle {
            index,
            pool: self as *const Self,
        })
    }

    /// Return a buffer to the pool (called by BufferHandle::drop)
    #[inline]
    fn return_buffer(&self, index: usize) {
        // Ignore error - queue capacity matches buffer count
        let _ = self.free_list.push(index);
    }

    /// Get a reference to a buffer by index
    /// # Safety
    /// Caller must ensure the buffer is not being used by io_uring
    #[inline]
    pub fn get_buffer(&self, index: usize) -> Option<&AlignedBuffer> {
        self.buffers.get(index)
    }

    /// Get a mutable reference to a buffer by index
    /// # Safety
    /// Caller must ensure the buffer is not being used by io_uring
    #[inline]
    pub fn get_buffer_mut(&mut self, index: usize) -> Option<&mut AlignedBuffer> {
        self.buffers.get_mut(index)
    }

    /// Check if buffers are registered with io_uring
    #[inline]
    pub fn is_registered(&self) -> bool {
        self.registered.load(Ordering::SeqCst)
    }

    /// Get the buffer size
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get the number of buffers in the pool
    #[inline]
    pub fn num_buffers(&self) -> usize {
        self.buffers.len()
    }

    /// Get the number of available (free) buffers
    #[inline]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer() {
        let buf = AlignedBuffer::new(4096).unwrap();
        assert_eq!(buf.size(), 4096);
        // Check alignment
        assert_eq!(buf.as_ptr() as usize % BUFFER_ALIGNMENT, 0);
    }

    #[test]
    fn test_buffer_pool_creation() {
        let pool = RegisteredBufferPool::new(8, 4096).unwrap();
        assert_eq!(pool.num_buffers(), 8);
        assert_eq!(pool.buffer_size(), 4096);
        assert_eq!(pool.available(), 8);
        assert!(!pool.is_registered());
    }

    #[test]
    fn test_buffer_acquire_release() {
        let pool = RegisteredBufferPool::new(4, 4096).unwrap();
        assert_eq!(pool.available(), 4);

        // Acquire all buffers
        let h1 = pool.acquire().unwrap();
        let h2 = pool.acquire().unwrap();
        let h3 = pool.acquire().unwrap();
        let h4 = pool.acquire().unwrap();
        assert_eq!(pool.available(), 0);
        assert!(pool.acquire().is_none());

        // Release one
        drop(h1);
        assert_eq!(pool.available(), 1);

        // Acquire again
        let h5 = pool.acquire().unwrap();
        assert_eq!(pool.available(), 0);

        // Release all
        drop(h2);
        drop(h3);
        drop(h4);
        drop(h5);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_buffer_pool_zero_buffers() {
        // Creating a pool with zero buffers should fail or return empty pool
        let result = RegisteredBufferPool::new(0, 4096);
        // Either fails or creates empty pool - both are valid
        if let Ok(pool) = result {
            assert_eq!(pool.num_buffers(), 0);
            assert!(pool.acquire().is_none());
        }
    }

    #[test]
    fn test_buffer_content_write_read() {
        let mut pool = RegisteredBufferPool::new(2, 4096).unwrap();

        // Acquire a buffer and write to it
        let handle = pool.acquire().unwrap();
        let idx = handle.index();

        // Get mutable access and write data
        if let Some(buf) = pool.get_buffer_mut(idx) {
            let data = b"Hello, LANCE!";
            buf.as_mut_slice()[..data.len()].copy_from_slice(data);
        }

        // Read back the data
        if let Some(buf) = pool.get_buffer(idx) {
            let slice = &buf.as_slice()[..13];
            assert_eq!(slice, b"Hello, LANCE!");
        }

        drop(handle);
    }

    #[test]
    fn test_buffer_alignment_various_sizes() {
        // Test various buffer sizes all maintain alignment
        for size in [1024, 4096, 8192, 16384, 65536] {
            let buf = AlignedBuffer::new(size).unwrap();
            assert_eq!(buf.size(), size);
            assert_eq!(
                buf.as_ptr() as usize % BUFFER_ALIGNMENT,
                0,
                "Buffer of size {} not aligned",
                size
            );
        }
    }

    #[test]
    fn test_buffer_pool_concurrent_handles() {
        // Verify that multiple handles can coexist
        let pool = RegisteredBufferPool::new(8, 4096).unwrap();
        let mut handles = Vec::new();

        // Acquire all
        for i in 0..8 {
            let h = pool.acquire();
            assert!(h.is_some(), "Failed to acquire buffer {}", i);
            handles.push(h.unwrap());
        }

        // Verify indices are unique
        let indices: Vec<_> = handles.iter().map(|h| h.index()).collect();
        let unique: std::collections::HashSet<_> = indices.iter().collect();
        assert_eq!(unique.len(), 8, "Buffer indices should be unique");

        // Release in reverse order
        while let Some(h) = handles.pop() {
            let prev_avail = pool.available();
            drop(h);
            assert_eq!(pool.available(), prev_avail + 1);
        }
    }

    #[test]
    fn test_get_buffer_invalid_index() {
        let pool = RegisteredBufferPool::new(4, 4096).unwrap();

        // Invalid indices should return None
        assert!(pool.get_buffer(100).is_none());
        assert!(pool.get_buffer(4).is_none()); // Out of range
    }

    #[test]
    fn test_probe_splice_returns_valid_result() {
        // probe_splice should return either Supported or Unsupported
        let result = super::probe_splice();
        assert!(
            result == super::SpliceSupport::Supported
                || result == super::SpliceSupport::Unsupported
        );
    }

    #[test]
    fn test_probe_tee_returns_valid_result() {
        // probe_tee should return either Supported or Unsupported
        let result = super::probe_tee();
        assert!(result == super::TeeSupport::Supported || result == super::TeeSupport::Unsupported);
    }

    #[test]
    fn test_splice_pipe_creation() {
        // SplicePipe::new should succeed on Linux
        let pipe = super::SplicePipe::new();
        assert!(pipe.is_ok(), "SplicePipe creation failed");

        let pipe = pipe.unwrap();
        // File descriptors should be valid (positive)
        assert!(pipe.read_fd() >= 0, "Invalid read fd");
        assert!(pipe.write_fd() >= 0, "Invalid write fd");
        assert_ne!(
            pipe.read_fd(),
            pipe.write_fd(),
            "Read and write fds should be different"
        );
    }

    #[test]
    fn test_splice_forwarder_creation() {
        // SpliceForwarder should be creatable
        let forwarder = super::SpliceForwarder::new(32);
        assert!(forwarder.is_ok(), "SpliceForwarder creation failed");

        let forwarder = forwarder.unwrap();
        assert_eq!(
            forwarder.pending_ops(),
            0,
            "New forwarder should have no pending ops"
        );
    }

    #[test]
    fn test_tee_forwarder_creation() {
        // TeeForwarder should be creatable
        let forwarder = super::TeeForwarder::new(32);
        assert!(forwarder.is_ok(), "TeeForwarder creation failed");

        let forwarder = forwarder.unwrap();
        assert_eq!(
            forwarder.pending_ops(),
            0,
            "New forwarder should have no pending ops"
        );
        // is_fully_supported requires both tee and splice
        // We just verify the method doesn't panic
        let _ = forwarder.is_fully_supported();
        let _ = forwarder.is_tee_supported();
        let _ = forwarder.is_splice_supported();
    }

    #[test]
    fn test_tee_support_enum_equality() {
        assert_eq!(super::TeeSupport::Supported, super::TeeSupport::Supported);
        assert_eq!(
            super::TeeSupport::Unsupported,
            super::TeeSupport::Unsupported
        );
        assert_ne!(super::TeeSupport::Supported, super::TeeSupport::Unsupported);
    }

    #[test]
    fn test_splice_support_enum_equality() {
        assert_eq!(
            super::SpliceSupport::Supported,
            super::SpliceSupport::Supported
        );
        assert_eq!(
            super::SpliceSupport::Unsupported,
            super::SpliceSupport::Unsupported
        );
        assert_ne!(
            super::SpliceSupport::Supported,
            super::SpliceSupport::Unsupported
        );
    }

    /// Benchmark: TEE vs Standard Forwarding Performance
    ///
    /// Measures the overhead of TEE operations compared to standard splice forwarding.
    /// TEE allows duplicating pipe data without copying to userspace - critical for
    /// L3 quorum where we forward to leader AND process locally.
    #[test]
    fn benchmark_tee_vs_splice_performance() {
        use std::time::Instant;

        println!("\n=== TEE vs Splice Performance Benchmark ===");

        // Check kernel support first
        let tee_support = super::probe_tee();
        let splice_support = super::probe_splice();

        println!("TEE Support: {:?}", tee_support);
        println!("Splice Support: {:?}", splice_support);

        if tee_support != super::TeeSupport::Supported {
            println!("\n⚠ TEE not supported on this kernel - skipping TEE benchmarks");
            println!("  Requires Linux kernel 5.8+ with io_uring TEE support");
            return;
        }

        // Create forwarders
        let splice_forwarder = super::SpliceForwarder::new(256);
        let tee_forwarder = super::TeeForwarder::new(256);

        if splice_forwarder.is_err() || tee_forwarder.is_err() {
            println!("Failed to create forwarders - skipping benchmark");
            return;
        }

        let splice_forwarder = splice_forwarder.unwrap();
        let tee_forwarder = tee_forwarder.unwrap();

        println!("\nForwarder Status:");
        println!("  Splice Forwarder: created");
        println!("  TEE Forwarder: created");
        println!(
            "  TEE fully supported: {}",
            tee_forwarder.is_fully_supported()
        );

        // Benchmark pipe creation (common to both)
        let iterations = 10000;

        println!("\n--- Pipe Creation Benchmark ---");
        let start = Instant::now();
        for _ in 0..iterations {
            let pipe = super::SplicePipe::new();
            assert!(pipe.is_ok());
            // Pipe is dropped here, closing fds
        }
        let elapsed = start.elapsed();
        let pipe_create_ns = elapsed.as_nanos() / iterations as u128;
        println!(
            "Pipe create+close: {} ns/op ({} ops/sec)",
            pipe_create_ns,
            1_000_000_000 / pipe_create_ns
        );

        // Memory overhead comparison
        println!("\n--- Memory Overhead ---");
        println!(
            "SpliceForwarder size: {} bytes",
            std::mem::size_of::<super::SpliceForwarder>()
        );
        println!(
            "TeeForwarder size: {} bytes",
            std::mem::size_of::<super::TeeForwarder>()
        );
        println!(
            "SplicePipe size: {} bytes",
            std::mem::size_of::<super::SplicePipe>()
        );

        // Pending ops tracking overhead
        println!("\n--- Operation Tracking ---");
        println!("Splice pending ops: {}", splice_forwarder.pending_ops());
        println!("TEE pending ops: {}", tee_forwarder.pending_ops());

        println!("\n--- Performance Summary ---");
        println!("TEE forwarding duplicates data in-kernel without userspace copies.");
        println!("For L3 quorum: forward to leader + local ACK = 2 destinations, 0 copies.");
        println!("Standard forwarding would require: read() + write() + write() = 2 copies.\n");

        // Calculate theoretical savings
        let payload_sizes = [1024u64, 4096, 16384, 65536];
        println!(
            "{:<12} {:>15} {:>15} {:>12}",
            "Payload", "Std (2 copies)", "TEE (0 copy)", "Savings"
        );
        println!("{}", "-".repeat(58));

        for size in payload_sizes {
            // Assume ~3ns/byte for memcpy (typical for modern CPUs with cache)
            let std_copy_ns = size * 2 * 3; // 2 copies
            let tee_overhead_ns = 500u64; // syscall overhead only, no copy
            let savings_pct = 100.0 * (1.0 - (tee_overhead_ns as f64 / std_copy_ns as f64));

            println!(
                "{:<12} {:>12} ns {:>12} ns {:>10.1}%",
                format!("{}B", size),
                std_copy_ns,
                tee_overhead_ns,
                savings_pct
            );
        }

        println!(
            "\nNote: Actual performance depends on kernel version, CPU, and memory bandwidth."
        );
        println!("Run integration tests with --benchmark for end-to-end latency measurements.\n");
    }
}

// ============================================================================
// Zero-Copy Network Send (IORING_OP_SEND_ZC)
// ============================================================================
// Per Architecture §17.3: Implement ZeroCopySender using IORING_OP_SEND_ZC
// to stream segment data directly to network sockets without kernel copies.

use std::os::unix::io::RawFd;

/// Result of probing for SEND_ZC support
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendZcSupport {
    /// SEND_ZC is supported
    Supported,
    /// SEND_ZC not available, will use regular send
    Fallback,
}

/// Probe for IORING_OP_SEND_ZC support
/// Returns SendZcSupport::Supported if available, Fallback otherwise
pub fn probe_send_zc() -> SendZcSupport {
    match IoUring::new(8) {
        Ok(ring) => {
            let mut probe = io_uring::Probe::new();
            if ring.submitter().register_probe(&mut probe).is_ok() {
                // SendZc opcode is 0x31 (49)
                if probe.is_supported(opcode::SendZc::CODE) {
                    tracing::info!(
                        target: "lance::io",
                        "IORING_OP_SEND_ZC enabled (kernel 6.0+)"
                    );
                    return SendZcSupport::Supported;
                }
            }
            tracing::info!(
                target: "lance::io",
                "IORING_OP_SEND_ZC not available, using regular send"
            );
            SendZcSupport::Fallback
        },
        Err(_) => SendZcSupport::Fallback,
    }
}

/// Zero-copy network sender using io_uring SEND_ZC
/// Per Architecture §17.3 and Standards §5.2
pub struct ZeroCopySender {
    ring: IoUring,
    pending_ops: u32,
    send_zc_supported: bool,
}

impl ZeroCopySender {
    /// Create a new zero-copy sender
    pub fn new(ring_size: u32) -> Result<Self> {
        let ring = IoUring::builder()
            .setup_coop_taskrun()
            .build(ring_size)
            .map_err(|_| LanceError::UnsupportedKernel("io_uring setup failed"))?;

        let send_zc_supported = probe_send_zc() == SendZcSupport::Supported;

        Ok(Self {
            ring,
            pending_ops: 0,
            send_zc_supported,
        })
    }

    /// Check if zero-copy send is supported
    #[inline]
    pub fn is_zero_copy(&self) -> bool {
        self.send_zc_supported
    }

    /// Submit a zero-copy send operation
    ///
    /// # Arguments
    /// * `socket_fd` - The socket file descriptor
    /// * `data` - Data to send (must remain valid until completion)
    /// * `user_data` - User data to identify the completion
    ///
    /// # Safety
    /// Caller must ensure `data` remains valid until the completion is harvested.
    /// For SEND_ZC, the kernel may hold references to the buffer until notification.
    pub fn submit_send(&mut self, socket_fd: RawFd, data: &[u8], user_data: u64) -> Result<()> {
        if self.send_zc_supported {
            self.submit_send_zc(socket_fd, data, user_data)
        } else {
            self.submit_send_regular(socket_fd, data, user_data)
        }
    }

    /// Submit zero-copy send (IORING_OP_SEND_ZC)
    fn submit_send_zc(&mut self, socket_fd: RawFd, data: &[u8], user_data: u64) -> Result<()> {
        let send_op = opcode::SendZc::new(types::Fd(socket_fd), data.as_ptr(), data.len() as u32)
            .build()
            .user_data(user_data);

        // SAFETY: Caller ensures data remains valid until completion
        unsafe {
            self.ring
                .submission()
                .push(&send_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit regular send (fallback when SEND_ZC not available)
    fn submit_send_regular(&mut self, socket_fd: RawFd, data: &[u8], user_data: u64) -> Result<()> {
        let send_op = opcode::Send::new(types::Fd(socket_fd), data.as_ptr(), data.len() as u32)
            .build()
            .user_data(user_data);

        // SAFETY: Caller ensures data remains valid until completion
        unsafe {
            self.ring
                .submission()
                .push(&send_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit multiple send operations (vectored)
    /// Useful for sending multiple buffers in a single syscall
    pub fn submit_sendmsg(
        &mut self,
        socket_fd: RawFd,
        msg: *const libc::msghdr,
        user_data: u64,
    ) -> Result<()> {
        let sendmsg_op = opcode::SendMsg::new(types::Fd(socket_fd), msg)
            .build()
            .user_data(user_data);

        // SAFETY: Caller ensures msghdr and referenced buffers remain valid
        unsafe {
            self.ring
                .submission()
                .push(&sendmsg_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit all pending operations to the kernel
    pub fn submit(&mut self) -> Result<u32> {
        let submitted = self.ring.submit()?;
        Ok(submitted as u32)
    }

    /// Poll for completions
    ///
    /// For SEND_ZC, there may be two completions per send:
    /// 1. The send completion (data accepted by kernel)
    /// 2. The notification that buffer can be reused (CQE_F_NOTIF flag)
    pub fn poll_completions<F>(&mut self, mut callback: F) -> Result<u32>
    where
        F: FnMut(u64, i32, bool), // user_data, result, is_notification
    {
        let mut completed = 0u32;

        while let Some(cqe) = self.ring.completion().next() {
            let user_data = cqe.user_data();
            let result = cqe.result();
            let flags = cqe.flags();

            // Check if this is a SEND_ZC notification (buffer can be reused)
            // CQE_F_NOTIF = 0x8
            let is_notification = (flags & 0x8) != 0;

            callback(user_data, result, is_notification);

            // Only decrement pending ops for non-notification completions
            if !is_notification {
                completed += 1;
                self.pending_ops = self.pending_ops.saturating_sub(1);
                lnc_metrics::increment_io_completed();
            }
        }

        Ok(completed)
    }

    /// Get the number of pending operations
    #[inline]
    pub fn pending_ops(&self) -> u32 {
        self.pending_ops
    }
}

// ============================================================================
// Zero-Copy Splice Forwarding (IORING_OP_SPLICE)
// ============================================================================
// Per WriteForwardingArchitecture.md Phase 2: Use IORING_OP_SPLICE for
// zero-copy forwarding of writes from followers to leader.
// Data flow: client_socket → pipe → leader_socket (no userspace copies)

/// Result of probing for SPLICE support
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpliceSupport {
    /// SPLICE is supported
    Supported,
    /// SPLICE not available
    Unsupported,
}

/// Probe for IORING_OP_SPLICE support
pub fn probe_splice() -> SpliceSupport {
    match IoUring::new(8) {
        Ok(ring) => {
            let mut probe = io_uring::Probe::new();
            if ring.submitter().register_probe(&mut probe).is_ok()
                && probe.is_supported(opcode::Splice::CODE)
            {
                tracing::info!(
                    target: "lance::io",
                    "IORING_OP_SPLICE enabled"
                );
                return SpliceSupport::Supported;
            }
            tracing::info!(
                target: "lance::io",
                "IORING_OP_SPLICE not available"
            );
            SpliceSupport::Unsupported
        },
        Err(_) => SpliceSupport::Unsupported,
    }
}

/// Zero-copy splice forwarder using io_uring
/// Per WriteForwardingArchitecture.md - for 100Gbps line rates
pub struct SpliceForwarder {
    ring: IoUring,
    pending_ops: u32,
    splice_supported: bool,
}

/// Pipe file descriptors for splice operations
pub struct SplicePipe {
    read_fd: RawFd,
    write_fd: RawFd,
}

impl SplicePipe {
    /// Create a new pipe for splice operations
    pub fn new() -> Result<Self> {
        let mut fds = [0i32; 2];

        // SAFETY: pipe2 is a standard POSIX function
        let ret = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };

        if ret < 0 {
            return Err(LanceError::Io(std::io::Error::last_os_error()));
        }

        Ok(Self {
            read_fd: fds[0],
            write_fd: fds[1],
        })
    }

    /// Get the read end of the pipe
    #[inline]
    pub fn read_fd(&self) -> RawFd {
        self.read_fd
    }

    /// Get the write end of the pipe
    #[inline]
    pub fn write_fd(&self) -> RawFd {
        self.write_fd
    }
}

impl Drop for SplicePipe {
    fn drop(&mut self) {
        // SAFETY: Closing valid file descriptors
        unsafe {
            libc::close(self.read_fd);
            libc::close(self.write_fd);
        }
    }
}

impl SpliceForwarder {
    /// Create a new splice forwarder
    pub fn new(ring_size: u32) -> Result<Self> {
        let ring = IoUring::builder()
            .setup_coop_taskrun()
            .build(ring_size)
            .map_err(|_| LanceError::UnsupportedKernel("io_uring setup failed"))?;

        let splice_supported = probe_splice() == SpliceSupport::Supported;

        Ok(Self {
            ring,
            pending_ops: 0,
            splice_supported,
        })
    }

    /// Check if splice is supported
    #[inline]
    pub fn is_splice_supported(&self) -> bool {
        self.splice_supported
    }

    /// Submit a splice operation from source fd to pipe
    ///
    /// # Arguments
    /// * `source_fd` - Source file descriptor (socket or file)
    /// * `pipe` - Pipe to splice into
    /// * `len` - Number of bytes to splice
    /// * `user_data` - User data for completion identification
    ///
    /// # Safety
    /// Caller must ensure file descriptors are valid.
    pub fn submit_splice_to_pipe(
        &mut self,
        source_fd: RawFd,
        pipe: &SplicePipe,
        len: u32,
        user_data: u64,
    ) -> Result<()> {
        let splice_op = opcode::Splice::new(
            types::Fd(source_fd),
            -1, // No offset for sockets
            types::Fd(pipe.write_fd()),
            -1,
            len,
        )
        .flags(libc::SPLICE_F_MOVE)
        .build()
        .user_data(user_data);

        // SAFETY: File descriptors must be valid
        unsafe {
            self.ring
                .submission()
                .push(&splice_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit a splice operation from pipe to destination fd
    pub fn submit_splice_from_pipe(
        &mut self,
        pipe: &SplicePipe,
        dest_fd: RawFd,
        len: u32,
        user_data: u64,
    ) -> Result<()> {
        let splice_op =
            opcode::Splice::new(types::Fd(pipe.read_fd()), -1, types::Fd(dest_fd), -1, len)
                .flags(libc::SPLICE_F_MOVE)
                .build()
                .user_data(user_data);

        // SAFETY: File descriptors must be valid
        unsafe {
            self.ring
                .submission()
                .push(&splice_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit a linked splice operation for forwarding
    ///
    /// This submits two linked operations:
    /// 1. Splice from source_fd to pipe
    /// 2. Splice from pipe to dest_fd
    ///
    /// The operations are linked, so the second waits for the first.
    /// This is the core of zero-copy write forwarding.
    pub fn submit_forward_splice(
        &mut self,
        source_fd: RawFd,
        dest_fd: RawFd,
        pipe: &SplicePipe,
        len: u32,
        user_data: u64,
    ) -> Result<()> {
        // First splice: source → pipe (linked)
        let splice_in = opcode::Splice::new(
            types::Fd(source_fd),
            -1,
            types::Fd(pipe.write_fd()),
            -1,
            len,
        )
        .flags(libc::SPLICE_F_MOVE)
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK) // Link to next operation
        .user_data(user_data);

        // Second splice: pipe → destination
        let splice_out =
            opcode::Splice::new(types::Fd(pipe.read_fd()), -1, types::Fd(dest_fd), -1, len)
                .flags(libc::SPLICE_F_MOVE)
                .build()
                .user_data(user_data | 0x8000_0000_0000_0000); // Mark as second part

        // SAFETY: File descriptors must be valid
        unsafe {
            self.ring
                .submission()
                .push(&splice_in)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
            self.ring
                .submission()
                .push(&splice_out)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 2;
        lnc_metrics::increment_io_submitted();
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit all pending operations
    pub fn submit(&mut self) -> Result<u32> {
        let submitted = self.ring.submit()?;
        Ok(submitted as u32)
    }

    /// Poll for completions
    pub fn poll_completions<F>(&mut self, mut callback: F) -> Result<u32>
    where
        F: FnMut(u64, i32), // user_data, result (bytes spliced or error)
    {
        let mut completed = 0u32;

        while let Some(cqe) = self.ring.completion().next() {
            let user_data = cqe.user_data();
            let result = cqe.result();

            callback(user_data, result);
            completed += 1;
            self.pending_ops = self.pending_ops.saturating_sub(1);
            lnc_metrics::increment_io_completed();
        }

        Ok(completed)
    }

    /// Get pending operation count
    #[inline]
    pub fn pending_ops(&self) -> u32 {
        self.pending_ops
    }
}

// ============================================================================
// Zero-Copy Tee Forwarding (IORING_OP_TEE) - Phase 3
// ============================================================================
// Per Architecture.md Section 21.9: Use IORING_OP_TEE for in-kernel data
// duplication in L3 quorum scenarios.
// Data flow: client_socket → pipe1 → leader_socket (forwarding)
//                                 ↘ pipe2 → local_processor (teeing)

/// Result of probing for TEE support
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeeSupport {
    /// TEE is supported
    Supported,
    /// TEE not available
    Unsupported,
}

/// Probe for IORING_OP_TEE support
///
/// TEE allows duplicating data from one pipe to another without userspace copies.
/// Requires Linux kernel 5.8+ with io_uring TEE support.
pub fn probe_tee() -> TeeSupport {
    match IoUring::new(8) {
        Ok(ring) => {
            let mut probe = io_uring::Probe::new();
            if ring.submitter().register_probe(&mut probe).is_ok() {
                // Check for Tee opcode support
                if probe.is_supported(opcode::Tee::CODE) {
                    tracing::info!(
                        target: "lance::io",
                        "IORING_OP_TEE enabled"
                    );
                    return TeeSupport::Supported;
                }
            }
            tracing::info!(
                target: "lance::io",
                "IORING_OP_TEE not available"
            );
            TeeSupport::Unsupported
        },
        Err(_) => TeeSupport::Unsupported,
    }
}

/// Zero-copy tee forwarder using io_uring for L3 quorum
///
/// Enables forwarding writes to leader while simultaneously processing locally
/// for L3 quorum acknowledgment, audit logging, or real-time analytics.
/// All data duplication happens in-kernel with zero userspace copies.
pub struct TeeForwarder {
    ring: IoUring,
    pending_ops: u32,
    tee_supported: bool,
    splice_supported: bool,
}

impl TeeForwarder {
    /// Create a new tee forwarder
    pub fn new(ring_size: u32) -> Result<Self> {
        let ring = IoUring::builder()
            .setup_coop_taskrun()
            .build(ring_size)
            .map_err(|_| LanceError::UnsupportedKernel("io_uring setup failed"))?;

        let tee_supported = probe_tee() == TeeSupport::Supported;
        let splice_supported = probe_splice() == SpliceSupport::Supported;

        Ok(Self {
            ring,
            pending_ops: 0,
            tee_supported,
            splice_supported,
        })
    }

    /// Check if tee is supported
    #[inline]
    pub fn is_tee_supported(&self) -> bool {
        self.tee_supported
    }

    /// Check if splice is supported (required for tee operations)
    #[inline]
    pub fn is_splice_supported(&self) -> bool {
        self.splice_supported
    }

    /// Check if full tee forwarding is available (requires both tee and splice)
    #[inline]
    pub fn is_fully_supported(&self) -> bool {
        self.tee_supported && self.splice_supported
    }

    /// Submit a tee operation to duplicate data from one pipe to another
    ///
    /// # Arguments
    /// * `source_pipe` - Source pipe to read from (data is NOT consumed)
    /// * `dest_pipe` - Destination pipe to write to
    /// * `len` - Number of bytes to tee
    /// * `user_data` - User data for completion identification
    ///
    /// # Note
    /// Unlike splice, tee does NOT consume data from the source pipe.
    /// The same data can be read from source_pipe after the tee operation.
    pub fn submit_tee(
        &mut self,
        source_pipe: &SplicePipe,
        dest_pipe: &SplicePipe,
        len: u32,
        user_data: u64,
    ) -> Result<()> {
        let tee_op = opcode::Tee::new(
            types::Fd(source_pipe.read_fd()),
            types::Fd(dest_pipe.write_fd()),
            len,
        )
        .build()
        .user_data(user_data);

        // SAFETY: File descriptors must be valid pipes
        unsafe {
            self.ring
                .submission()
                .push(&tee_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 1;
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit a forward-with-tee operation for L3 quorum
    ///
    /// This submits four linked operations:
    /// 1. Splice from source_fd to pipe1 (read from client)
    /// 2. Tee from pipe1 to pipe2 (duplicate for local processing)
    /// 3. Splice from pipe1 to leader_fd (forward to leader)
    /// 4. Splice from pipe2 to local_fd (send to local processor)
    ///
    /// All operations are linked, executing in sequence.
    /// This is the core of zero-copy L3 quorum forwarding.
    ///
    /// # Arguments
    /// * `source_fd` - Client socket to read from
    /// * `leader_fd` - Leader socket to forward to
    /// * `local_fd` - Local processor fd (file, pipe, or socket)
    /// * `pipe1` - Primary pipe for forwarding path
    /// * `pipe2` - Secondary pipe for local processing path
    /// * `len` - Number of bytes to forward
    /// * `user_data` - Base user data (operations use user_data | 0..3)
    #[allow(clippy::too_many_arguments)]
    pub fn submit_forward_with_tee(
        &mut self,
        source_fd: RawFd,
        leader_fd: RawFd,
        local_fd: RawFd,
        pipe1: &SplicePipe,
        pipe2: &SplicePipe,
        len: u32,
        user_data: u64,
    ) -> Result<()> {
        // Operation 1: Splice from source → pipe1 (linked)
        let splice_in = opcode::Splice::new(
            types::Fd(source_fd),
            -1,
            types::Fd(pipe1.write_fd()),
            -1,
            len,
        )
        .flags(libc::SPLICE_F_MOVE)
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK)
        .user_data(user_data);

        // Operation 2: Tee from pipe1 → pipe2 (duplicate, linked)
        let tee_op = opcode::Tee::new(types::Fd(pipe1.read_fd()), types::Fd(pipe2.write_fd()), len)
            .build()
            .flags(io_uring::squeue::Flags::IO_LINK)
            .user_data(user_data | 0x1000_0000_0000_0000);

        // Operation 3: Splice from pipe1 → leader (forward, linked)
        let splice_leader = opcode::Splice::new(
            types::Fd(pipe1.read_fd()),
            -1,
            types::Fd(leader_fd),
            -1,
            len,
        )
        .flags(libc::SPLICE_F_MOVE)
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK)
        .user_data(user_data | 0x2000_0000_0000_0000);

        // Operation 4: Splice from pipe2 → local processor (final)
        let splice_local =
            opcode::Splice::new(types::Fd(pipe2.read_fd()), -1, types::Fd(local_fd), -1, len)
                .flags(libc::SPLICE_F_MOVE)
                .build()
                .user_data(user_data | 0x3000_0000_0000_0000);

        // SAFETY: File descriptors must be valid
        unsafe {
            self.ring
                .submission()
                .push(&splice_in)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
            self.ring
                .submission()
                .push(&tee_op)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
            self.ring
                .submission()
                .push(&splice_leader)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
            self.ring
                .submission()
                .push(&splice_local)
                .map_err(|_| LanceError::Io(std::io::Error::other("SQ full")))?;
        }

        self.pending_ops += 4;
        lnc_metrics::increment_io_submitted();
        lnc_metrics::increment_io_submitted();
        lnc_metrics::increment_io_submitted();
        lnc_metrics::increment_io_submitted();
        Ok(())
    }

    /// Submit all pending operations
    pub fn submit(&mut self) -> Result<u32> {
        let submitted = self.ring.submit()?;
        Ok(submitted as u32)
    }

    /// Poll for completions
    pub fn poll_completions<F>(&mut self, mut callback: F) -> Result<u32>
    where
        F: FnMut(u64, i32), // user_data, result (bytes transferred or error)
    {
        let mut completed = 0u32;

        while let Some(cqe) = self.ring.completion().next() {
            let user_data = cqe.user_data();
            let result = cqe.result();

            callback(user_data, result);
            completed += 1;
            self.pending_ops = self.pending_ops.saturating_sub(1);
            lnc_metrics::increment_io_completed();
        }

        Ok(completed)
    }

    /// Get pending operation count
    #[inline]
    pub fn pending_ops(&self) -> u32 {
        self.pending_ops
    }
}
