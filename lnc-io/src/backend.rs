use lnc_core::Result;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoBackendType {
    #[default]
    IoUring,
    Pwritev2,
}

pub trait IoBackend: Send {
    fn write(&mut self, data: &[u8], offset: u64) -> Result<usize>;
    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn fsync(&mut self) -> Result<()>;
    fn backend_type(&self) -> IoBackendType;
}

use crate::priority::IoPriority;
use lnc_core::LoanableBatch;
use std::future::Future;
use std::pin::Pin;

/// The result of an async write operation:
/// 1. The original batch (returned for recycling to BatchPool)
/// 2. The result (bytes written or error)
pub type AsyncWriteResult = (LoanableBatch, Result<usize>);

/// A future that resolves when the hardware acknowledges the write.
pub type WriteFuture = Pin<Box<dyn Future<Output = AsyncWriteResult> + Send>>;

/// High-Performance Asynchronous I/O Interface.
///
/// Unlike `IoBackend` (synchronous, copying), this trait enforces:
/// 1. Zero-copy (ownership transfer of aligned buffers)
/// 2. Priority propagation to the kernel
/// 3. Asynchronous completion (compatible with tokio reactor)
///
/// # Memory Safety
/// The implementation MUST keep the `LoanableBatch` alive until the kernel
/// completes the I/O operation. Dropping the batch while I/O is in-flight
/// causes undefined behavior (kernel writes to freed memory).
pub trait AsyncIoBackend: Send + Sync {
    /// Submit a write batch to the kernel.
    ///
    /// # Arguments
    /// * `batch` - The aligned data buffer. Ownership is transferred.
    /// * `offset` - The file offset to write to.
    /// * `priority` - The I/O priority level (propagated to kernel on Linux).
    ///
    /// # Returns
    /// A future that resolves to `(batch, result)` when the kernel completes the I/O.
    /// The batch is returned for recycling to the `BatchPool`.
    ///
    /// # Errors
    /// Returns an error if the submission queue is full or the operation cannot be queued.
    fn submit_write(
        &self,
        batch: LoanableBatch,
        offset: u64,
        priority: IoPriority,
    ) -> Result<WriteFuture>;
}

#[cfg(target_os = "linux")]
pub fn probe_io_uring() -> bool {
    use tracing::info;
    match io_uring::IoUring::new(8) {
        Ok(ring) => {
            let mut probe = io_uring::Probe::new();
            if ring.submitter().register_probe(&mut probe).is_ok() {
                let has_write = probe.is_supported(io_uring::opcode::Write::CODE);
                let has_read = probe.is_supported(io_uring::opcode::Read::CODE);
                let has_fsync = probe.is_supported(io_uring::opcode::Fsync::CODE);

                if has_write && has_read && has_fsync {
                    info!(
                        target: "lance::io",
                        "io_uring enabled (kernel support verified)"
                    );
                    return true;
                }
            }
            warn!(
                target: "lance::io",
                "io_uring available but missing required ops"
            );
            false
        },
        Err(e) => {
            warn!(
                target: "lance::io",
                "io_uring not available: {}. Falling back to pwritev2",
                e
            );
            false
        },
    }
}

#[cfg(not(target_os = "linux"))]
pub fn probe_io_uring() -> bool {
    warn!(
        target: "lance::io",
        "io_uring not available on this platform. Using pwritev2 fallback"
    );
    false
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_io_backend_type_default() {
        let backend_type = IoBackendType::default();
        assert_eq!(backend_type, IoBackendType::IoUring);
    }

    #[test]
    fn test_io_backend_type_equality() {
        assert_eq!(IoBackendType::IoUring, IoBackendType::IoUring);
        assert_eq!(IoBackendType::Pwritev2, IoBackendType::Pwritev2);
        assert_ne!(IoBackendType::IoUring, IoBackendType::Pwritev2);
    }

    #[test]
    fn test_io_backend_type_clone() {
        let original = IoBackendType::Pwritev2;
        let cloned = original;
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_io_backend_type_debug() {
        let uring = IoBackendType::IoUring;
        let pwritev2 = IoBackendType::Pwritev2;

        assert_eq!(format!("{:?}", uring), "IoUring");
        assert_eq!(format!("{:?}", pwritev2), "Pwritev2");
    }

    #[test]
    fn test_probe_io_uring() {
        // This test verifies the probe function runs without panic
        // The result depends on the platform and kernel support
        let _result = probe_io_uring();
        // On non-Linux, this should always return false
        #[cfg(not(target_os = "linux"))]
        assert!(!_result);
    }
}
