use crate::cache::Cache;
use crate::server::ServerRemoteConfig;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, Config};
use rand::{RngCore, SeedableRng};
use rand_pcg::Pcg64;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::sync::Arc;

#[async_trait]
pub trait RemoteBackend: Send + Sync {
    async fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Vec<u8>>;
}

pub struct S3Backend {
    bucket: String,
    chunk_size: usize,
}

impl S3Backend {
    pub fn new(bucket: String, chunk_size: usize) -> Self {
        Self { bucket, chunk_size }
    }
}

const EMULATED_BUFFER_SEED: u64 = 1;
pub struct EmulatedBackend {
    chunk_size: usize,
}

impl EmulatedBackend {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    fn gen_buffer_prefix(buf: &mut Vec<u8>, key: &str, offset: usize, size: usize) {
        // Hash using DefaultHasher (64-bit hash)
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let key_hash = hasher.finish(); // 8bytes

        // Build the buffer
        buf.extend_from_slice(&key_hash.to_be_bytes());
        buf.extend_from_slice(&offset.to_be_bytes());
        buf.extend_from_slice(&size.to_be_bytes());
    }

    fn gen_random_buffer(
        &self,
        key: &str,
        offset: usize,
        size: usize,
    ) -> tokio::io::Result<Vec<u8>> {
        if size > self.chunk_size {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Requested size {} cannot be larger than chunk size {}",
                    size, self.chunk_size
                ),
            ));
        }

        let mut buffer_prefix: Vec<u8> = Vec::with_capacity(self.chunk_size);
        Self::gen_buffer_prefix(&mut buffer_prefix, key, offset, size);
        let prefix_len = buffer_prefix.len();
        if size < prefix_len {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "buffer size too small",
            ));
        }

        let mut rng = Pcg64::seed_from_u64(EMULATED_BUFFER_SEED);
        let mut buf = vec![0u8; size - prefix_len];
        rng.fill_bytes(&mut buf);
        buffer_prefix.extend(buf);
        buffer_prefix.resize(
            buffer_prefix.len() + (self.chunk_size - buffer_prefix.len()),
            0,
        );
        assert_eq!(
            buffer_prefix.capacity(),
            self.chunk_size,
            "Expected to have capacity of {}, realloc occurred",
            self.chunk_size
        );
        Ok(buffer_prefix)
    }
}

#[async_trait]
impl RemoteBackend for S3Backend {
    async fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Vec<u8>> {
        // TODO: Implement with AWS SDK
        Ok(vec![])
    }
}

#[async_trait]
impl RemoteBackend for EmulatedBackend {
    async fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Vec<u8>> {
        // TODO: Implement
        // println!("GET {} {} {}", key, offset, size);
        self.gen_random_buffer(key, offset, size)
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
        let backend = EmulatedBackend::new(chunk_size);

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
