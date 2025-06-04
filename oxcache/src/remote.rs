use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use crate::cache::Cache;
use aws_sdk_s3::{Client, Config};
use aws_config::meta::region::RegionProviderChain;
use crate::server::ServerRemoteConfig;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use rand::{RngCore, SeedableRng};
use rand_pcg::Pcg64;

#[async_trait]
pub trait RemoteBackend: Send + Sync {
    async fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Vec<u8>>;
}

pub struct S3Backend {
    bucket: String,
}

impl S3Backend {
    pub fn new(bucket: String) -> Self {
        Self { bucket }
    }
}

const EMULATED_BUFFER_SEED: u64 = 1;
pub struct EmulatedBackend {}

impl EmulatedBackend {
    pub fn new() -> Self {
        Self {}
    }

    fn gen_buffer_prefix(key: &str, offset: usize, size: usize) -> Vec<u8> {
        // Hash using DefaultHasher (64-bit hash)
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let key_hash = hasher.finish(); // 8bytes

        // Build the buffer
        let mut buf = Vec::with_capacity(8 + 8 + 8);
        buf.extend_from_slice(&key_hash.to_be_bytes());
        buf.extend_from_slice(&offset.to_be_bytes());
        buf.extend_from_slice(&size.to_be_bytes());

        buf
    }

    fn gen_random_buffer(key: &str, offset: usize, size: usize) -> tokio::io::Result<Vec<u8>> {
        let mut buffer_prefix = Self::gen_buffer_prefix(key, offset, size);
        let prefix_len = buffer_prefix.len();
        if size <= prefix_len {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "buffer size too small"));
        }
        let mut rng = Pcg64::seed_from_u64(EMULATED_BUFFER_SEED);
        let mut buf = vec![0u8; size-prefix_len];
        rng.fill_bytes(&mut buf);
        buffer_prefix.extend(buf);
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
        println!("GET {} {} {}", key, offset, size);
        Self::gen_random_buffer(key, offset, size)
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