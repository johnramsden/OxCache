use aligned_vec::{AVec, RuntimeAlign};
use async_trait::async_trait;
use bytes::Bytes;
use rand::{RngCore, SeedableRng};
use rand_pcg::Pcg64;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::time::Duration;
use tokio::time::sleep;

#[async_trait]
pub trait RemoteBackend: Send + Sync {
    async fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Bytes>;

    fn set_blocksize(&mut self, blocksize: usize);
}

pub struct S3Backend {
    _bucket: String,
    _chunk_size: usize,
    block_size: Option<usize>, // For buffer alignment
}

impl S3Backend {
    pub fn new(bucket: String, chunk_size: usize) -> Self {
        Self {
            _bucket: bucket,
            _chunk_size: chunk_size,
            block_size: None,
        }
    }
}

const EMULATED_BUFFER_SEED: u64 = 1;
pub struct EmulatedBackend {
    chunk_size: usize,
    block_size: Option<usize>, // For buffer alignment
    remote_artificial_delay_microsec: Option<usize>,
}

impl EmulatedBackend {
    pub fn new(chunk_size: usize, remote_artificial_delay_microsec: Option<usize>) -> Self {
        Self {
            chunk_size,
            block_size: None,
            remote_artificial_delay_microsec,
        }
    }

    fn gen_buffer_prefix(buf: &mut AVec<u8, RuntimeAlign>, key: &str, offset: usize, size: usize) {
        // Hash using DefaultHasher (64-bit hash)
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let key_hash = hasher.finish(); // 8bytes

        // Build the buffer
        buf.extend_from_slice(&key_hash.to_be_bytes());
        buf.extend_from_slice(&offset.to_be_bytes());
        buf.extend_from_slice(&size.to_be_bytes());
    }

    fn gen_random_buffer(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Bytes> {
        if size > self.chunk_size {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Requested size {} cannot be larger than chunk size {}",
                    size, self.chunk_size
                ),
            ));
        }

        if self.block_size.is_none() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "Block size not set",
            ));
        }

        let capacity = get_aligned_buffer_size(size, self.block_size.unwrap());
        let mut buffer: AVec<u8, RuntimeAlign> =
            AVec::with_capacity(self.block_size.unwrap(), capacity);
        Self::gen_buffer_prefix(&mut buffer, key, offset, size);

        // Add prefix
        let prefix_len = buffer.len();
        if size < prefix_len {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "buffer size {} < (prefix_len {}) too small",
                    size, prefix_len
                ),
            ));
        }

        // Fill the rest of the buffer with random bytes
        let mut rng = Pcg64::seed_from_u64(EMULATED_BUFFER_SEED);
        let remaining = size - prefix_len;
        let mut random_bytes = vec![0u8; remaining];
        rng.fill_bytes(&mut random_bytes);
        buffer.extend_from_slice(&random_bytes);

        // Zero pad up to full capacity
        let current_len = buffer.len();
        if current_len < capacity {
            buffer.resize(capacity, 0);
        }

        // Check size
        assert_eq!(
            buffer.len(),
            capacity,
            "Buffer should be exactly capacity-sized"
        );

        Ok(Bytes::from_owner(buffer))
    }
}

fn get_aligned_buffer_size(buffer_size: usize, block_size: usize) -> usize {
    if buffer_size.rem_euclid(block_size) != 0 {
        buffer_size + (block_size - buffer_size.rem_euclid(block_size))
    } else {
        buffer_size
    }
}

#[async_trait]
impl RemoteBackend for S3Backend {
    async fn get(&self, _key: &str, _offset: usize, _size: usize) -> tokio::io::Result<Bytes> {
        // TODO: Implement with AWS SDK
        Ok(Bytes::from(vec![]))
    }

    fn set_blocksize(&mut self, blocksize: usize) {
        self.block_size = Some(blocksize);
    }
}

#[async_trait]
impl RemoteBackend for EmulatedBackend {
    async fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Bytes> {
        if let Some(remote_artificial_delay_microsec) = self.remote_artificial_delay_microsec {
            sleep(Duration::from_micros(
                remote_artificial_delay_microsec as u64,
            ))
            .await;
        }

        self.gen_random_buffer(key, offset, size)
    }

    fn set_blocksize(&mut self, blocksize: usize) {
        self.block_size = Some(blocksize);
    }
}

pub enum RemoteBackendType {
    Emulated,
    S3,
}

pub fn validated_type(remote_type: &str) -> Result<RemoteBackendType, Box<dyn std::error::Error>> {
    match remote_type {
        "emulated" => Ok(RemoteBackendType::Emulated),
        "s3" => Ok(RemoteBackendType::S3),
        other => Err(format!("Unknown remote type: {}", other).into()),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_gen_random_buffer_correctness() {
        let chunk_size = 128;
        let backend = EmulatedBackend::new(chunk_size, None);

        let key = "mykey";
        let offset = 42;
        let size = 100; // Less than chunk_size

        let buffer = backend.gen_random_buffer(key, offset, size).unwrap();

        // === Validate buffer length ===
        assert_eq!(
            buffer.len(),
            chunk_size,
            "Expected buffer to be chunk_size padded"
        );

        // === Validate prefix ===
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let key_hash = hasher.finish();

        let prefix_len = 8 + 8 + 8; // u64 + usize + usize, assuming 64-bit usize

        let expected_prefix: Vec<u8> = [
            &key_hash.to_be_bytes()[..],
            &offset.to_be_bytes()[..],
            &size.to_be_bytes()[..],
        ]
        .concat();

        assert_eq!(
            &buffer[..prefix_len],
            &expected_prefix[..],
            "Prefix bytes do not match"
        );

        // === Validate random data ===
        let mut rng = rand_pcg::Pcg64::seed_from_u64(EMULATED_BUFFER_SEED);
        let mut expected_random = vec![0u8; size - prefix_len];
        rng.fill_bytes(&mut expected_random);

        assert_eq!(
            &buffer[prefix_len..size],
            &expected_random[..],
            "Random portion of buffer does not match expected"
        );

        // === Validate padding ===
        assert_eq!(
            &buffer[size..],
            vec![0u8; chunk_size - size].as_slice(),
            "Padding bytes after size are not zero"
        );
    }
}
