use crate::backend::{AsyncIoBackend, IoBackend, IoBackendType, WriteFuture};
use crate::priority::IoPriority;
use lnc_core::{LanceError, LoanableBatch, Result};
use std::fs::File;
use std::path::Path;

#[cfg(all(unix, not(target_os = "linux")))]
use std::os::unix::fs::FileExt; // for write_at

#[cfg(windows)]
use std::os::windows::fs::FileExt; // for seek_write

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

pub struct Pwritev2Backend {
    file: File,
}

impl Pwritev2Backend {
    pub fn open(path: &Path, create: bool) -> Result<Self> {
        let file = if create {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?
        } else {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?
        };

        tracing::info!(
            target: "lance::io",
            path = ?path,
            backend = "Pwritev2Backend",
            "Fallback I/O backend initialized (using spawn_blocking for async operations)"
        );

        Ok(Self { file })
    }

    #[cfg(target_os = "linux")]
    fn pwritev2(&self, data: &[u8], offset: u64) -> Result<usize> {
        use std::io::IoSlice;

        let iov = [IoSlice::new(data)];
        let fd = self.file.as_raw_fd();

        // SAFETY: We're calling pwritev2 with valid file descriptor and buffer
        let result = unsafe {
            libc::pwritev2(
                fd,
                iov.as_ptr() as *const libc::iovec,
                1,
                offset as i64,
                libc::RWF_DSYNC,
            )
        };

        if result < 0 {
            Err(LanceError::Io(std::io::Error::last_os_error()))
        } else {
            Ok(result as usize)
        }
    }

    #[cfg(target_os = "linux")]
    fn preadv2(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::io::IoSliceMut;

        let mut iov = [IoSliceMut::new(buf)];
        let fd = self.file.as_raw_fd();

        // SAFETY: We're calling preadv2 with valid file descriptor and buffer
        let result = unsafe {
            libc::preadv2(
                fd,
                iov.as_mut_ptr() as *mut libc::iovec,
                1,
                offset as i64,
                0,
            )
        };

        if result < 0 {
            Err(LanceError::Io(std::io::Error::last_os_error()))
        } else {
            Ok(result as usize)
        }
    }
}

impl IoBackend for Pwritev2Backend {
    fn write(&mut self, data: &[u8], offset: u64) -> Result<usize> {
        #[cfg(target_os = "linux")]
        {
            self.pwritev2(data, offset)
        }

        // Portable fallback using FileExt for positional writes
        #[cfg(not(target_os = "linux"))]
        {
            #[cfg(unix)]
            self.file.write_at(data, offset)?;

            #[cfg(windows)]
            self.file.seek_write(data, offset)?;

            self.file.sync_data()?;
            Ok(data.len())
        }
    }

    fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        #[cfg(target_os = "linux")]
        {
            self.preadv2(buf, offset)
        }
        #[cfg(not(target_os = "linux"))]
        {
            use std::io::{Read, Seek, SeekFrom};
            self.file.seek(SeekFrom::Start(offset))?;
            let n = self.file.read(buf)?;
            Ok(n)
        }
    }

    fn fsync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    fn backend_type(&self) -> IoBackendType {
        IoBackendType::Pwritev2
    }
}

/// Async implementation for fallback using spawn_blocking.
///
/// This enables the high-performance `IngestionHandler` to run on systems
/// without io_uring support. It offloads blocking disk I/O to Tokio's
/// blocking thread pool to prevent executor thread starvation.
///
/// Performance Impact: Uses spawn_blocking to avoid blocking Tokio worker
/// threads. This is critical for maintaining low latency under load.
impl AsyncIoBackend for Pwritev2Backend {
    fn submit_write(
        &self,
        batch: LoanableBatch,
        offset: u64,
        _priority: IoPriority,
    ) -> Result<WriteFuture> {
        // Clone the file handle (File is just a wrapper around an FD/Handle, cheap to clone)
        let file = self.file.try_clone().map_err(LanceError::Io)?;

        // Return a pinned boxed future that offloads blocking I/O
        Ok(Box::pin(async move {
            // Offload the blocking I/O to Tokio's blocking thread pool
            let result = tokio::task::spawn_blocking(move || {
                // --- BLOCKING CONTEXT START ---
                let io_result = {
                    #[cfg(target_os = "linux")]
                    {
                        use std::io::IoSlice;
                        use std::os::unix::io::AsRawFd;

                        let iov = [IoSlice::new(batch.as_slice())];
                        let fd = file.as_raw_fd();

                        // SAFETY: We're calling pwritev2 with valid file descriptor and buffer
                        let result = unsafe {
                            libc::pwritev2(
                                fd,
                                iov.as_ptr() as *const libc::iovec,
                                1,
                                offset as i64,
                                libc::RWF_DSYNC,
                            )
                        };

                        if result < 0 {
                            Err(LanceError::Io(std::io::Error::last_os_error()))
                        } else {
                            Ok(result as usize)
                        }
                    }

                    #[cfg(not(target_os = "linux"))]
                    {
                        // Portable implementation
                        #[cfg(unix)]
                        let res =
                            std::os::unix::fs::FileExt::write_at(&file, batch.as_slice(), offset)
                                .map_err(LanceError::Io);

                        #[cfg(windows)]
                        let res = std::os::windows::fs::FileExt::seek_write(
                            &file,
                            batch.as_slice(),
                            offset,
                        )
                        .map_err(LanceError::Io);

                        res.and_then(|_| {
                            file.sync_data().map_err(LanceError::Io)?;
                            Ok(batch.len())
                        })
                    }
                };
                // --- BLOCKING CONTEXT END ---

                // Return both batch and result
                (batch, io_result)
            })
            .await;

            match result {
                Ok((batch, io_result)) => {
                    // Return batch with the I/O result
                    (batch, io_result)
                },
                Err(join_err) => {
                    // Task panicked - we've lost the batch, this is a critical error
                    // We can't return the batch, so we have to panic or create a dummy error
                    panic!("Blocking I/O task panicked: {}", join_err)
                },
            }
        }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_pwritev2_backend_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let mut backend = Pwritev2Backend::open(&path, true).unwrap();

        let data = b"hello world";
        let written = backend.write(data, 0).unwrap();
        assert_eq!(written, data.len());

        let mut buf = [0u8; 64];
        let read = backend.read(&mut buf, 0).unwrap();
        assert_eq!(&buf[..read], data);
    }
}
