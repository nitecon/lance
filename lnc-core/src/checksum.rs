use crc32fast::Hasher;

#[inline]
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32c_empty() {
        assert_eq!(crc32c(&[]), 0);
    }

    #[test]
    fn test_crc32c_data() {
        let data = b"hello world";
        let crc = crc32c(data);
        assert_ne!(crc, 0);
        assert_eq!(crc, crc32c(data));
    }
}
