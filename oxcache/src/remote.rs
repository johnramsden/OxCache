use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use crate::cache::Cache;
use aws_sdk_s3::{Client, Config};
use aws_config::meta::region::RegionProviderChain;
use crate::server::ServerRemoteConfig;
use async_trait::async_trait;

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

pub struct EmulatedBackend {}

impl EmulatedBackend {
    pub fn new() -> Self {
        Self {}
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
        Ok(vec![])
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