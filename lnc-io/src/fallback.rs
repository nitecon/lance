use crate::backend::{IoBackend, IoBackendType};
use lnc_core::Result;
use std::fs::File;
use std::path::Path;

#[cfg(not(target_os = "linux"))]
use std::io::{Read, Seek, SeekFrom, Write};

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
use lnc_core::LanceError;

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
        #[cfg(not(target_os = "linux"))]
        {
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(data)?;
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

#[cfg(test)]
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
