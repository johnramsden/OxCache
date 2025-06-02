use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use crate::cache::Cache;
use aws_sdk_s3::{Client, Config};
use aws_config::meta::region::RegionProviderChain;
use crate::server::ServerRemoteConfig;

pub trait RemoteBackend {
    fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Vec<u8>>;
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

impl RemoteBackend for S3Backend {
    

    fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Vec<u8>> {
        // TODO: Implement with AWS SDK
        Ok(vec![])
    }
}

impl RemoteBackend for EmulatedBackend {
    fn get(&self, key: &str, offset: usize, size: usize) -> tokio::io::Result<Vec<u8>> {
        // TODO: Implement
        println!("GET {} {} {}", key, offset, size);
        Ok(vec![])
    }
}

pub fn from_config(config: &ServerRemoteConfig) -> Result<Arc<dyn RemoteBackend>, Box<dyn std::error::Error>> {
    match config.remote_type.as_str() {
        "emulated" => Ok(Arc::new(EmulatedBackend::new())),
        "s3" => {
            let bucket = config
                .bucket
                .clone()
                .ok_or("S3 remote requires a bucket")?;
            Ok(Arc::new(S3Backend::new(bucket)))
        }
        other => Err(format!("Unknown remote type: {}", other).into()),
    }
}