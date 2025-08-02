use clap::Parser;
use oxcache;
use oxcache::remote;
use oxcache::server::{RUNTIME, Server, ServerConfig, ServerEvictionConfig, ServerRemoteConfig};
use serde::Deserialize;
use std::fs;
use std::process::exit;

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct CliArgs {
    /// Path to config file
    #[arg(long)]
    pub config: Option<String>,

    #[arg(long)]
    pub socket: Option<String>,

    #[arg(long)]
    pub disk: Option<String>,

    #[arg(long)]
    pub writer_threads: Option<usize>,

    #[arg(long)]
    pub reader_threads: Option<usize>,

    #[arg(long)]
    pub chunk_size: Option<usize>,
    
    #[arg(long)]
    pub max_write_size: Option<usize>,

    #[arg(long)]
    pub remote_type: Option<String>,

    #[arg(long)]
    pub remote_bucket: Option<String>,

    #[arg(long)]
    pub eviction_policy: Option<String>,

    #[arg(long)]
    pub high_water_evict: Option<usize>,

    #[arg(long)]
    pub low_water_evict: Option<usize>,

    #[arg(long)]
    pub block_zone_capacity: Option<usize>,

    #[arg(long)]
    pub eviction_interval: Option<usize>,

    #[arg(long)]
    pub remote_artificial_delay_microsec: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ParsedServerConfig {
    pub socket: Option<String>,
    pub disk: Option<String>,
    pub writer_threads: Option<usize>,
    pub reader_threads: Option<usize>,
    pub chunk_size: Option<usize>,
    pub max_write_size: Option<usize>,
    pub block_zone_capacity: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ParsedRemoteConfig {
    pub remote_type: Option<String>,
    pub bucket: Option<String>,
    pub remote_artificial_delay_microsec: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ParsedEvictionConfig {
    pub eviction_policy: Option<String>,
    pub high_water_evict: Option<usize>,
    pub low_water_evict: Option<usize>,
    pub eviction_interval: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub server: ParsedServerConfig,
    pub remote: ParsedRemoteConfig,
    pub eviction: ParsedEvictionConfig,
}

fn load_config(cli: &CliArgs) -> Result<ServerConfig, Box<dyn std::error::Error>> {
    let config = if let Some(path) = &cli.config {
        let config_str = fs::read_to_string(path)?;
        Some(toml::from_str::<AppConfig>(&config_str)?)
    } else {
        None
    };

    let socket = cli
        .socket
        .clone()
        .or_else(|| config.as_ref()?.server.socket.clone());
    let disk = cli
        .disk
        .clone()
        .or_else(|| config.as_ref()?.server.disk.clone());
    let writer_threads = cli
        .writer_threads
        .or_else(|| config.as_ref()?.server.writer_threads);
    let reader_threads = cli
        .reader_threads
        .or_else(|| config.as_ref()?.server.reader_threads);

    let remote_type = cli
        .remote_type
        .clone()
        .or_else(|| config.as_ref()?.remote.remote_type.clone());
    let remote_bucket = cli
        .remote_bucket
        .clone()
        .or_else(|| config.as_ref()?.remote.bucket.clone());
    let remote_artificial_delay_microsec =
        cli.remote_artificial_delay_microsec.clone().or_else(|| {
            config
                .as_ref()?
                .remote
                .remote_artificial_delay_microsec
                .clone()
        });

    let socket = socket.ok_or("Missing required `socket` (in CLI or file)")?;
    let disk = disk.ok_or("Missing required `disk` (in CLI or file)")?;

    let writer_threads =
        writer_threads.ok_or("Missing required `writer_threads` (in CLI or file)")?;
    if writer_threads == 0 {
        return Err("writer_threads must be greater than 0".into());
    }

    let reader_threads =
        reader_threads.ok_or("Missing required `reader_threads` (in CLI or file)")?;
    if reader_threads == 0 {
        return Err("reader_threads must be greater than 0".into());
    }

    let remote_type = remote_type
        .ok_or("Missing required `remote_type` (in CLI or file)")?
        .to_lowercase();

    if remote_type != "emulated" && remote_type != "s3" {
        return Err("remote_type must be either `emulated` or `S3`".into());
    }

    if remote_type != "emulated" && remote_bucket.is_none() {
        return Err("remote_bucket must be set if remote_type is not `emulated`".into());
    }

    if remote_type != "emulated" && remote_artificial_delay_microsec.is_some() {
        eprintln!(
            "WARNING: remote_artificial_delay_microsec has no effect if remote_type is not `emulated`"
        );
    }

    let eviction_policy = cli
        .eviction_policy
        .clone()
        .or_else(|| config.as_ref()?.eviction.eviction_policy.clone())
        .ok_or("Missing eviction policy")?
        .to_lowercase();
    let high_water_evict = cli
        .high_water_evict
        .clone()
        .or_else(|| config.as_ref()?.eviction.high_water_evict.clone())
        .ok_or("Missing high_water_evict")?;
    let low_water_evict = cli
        .low_water_evict
        .clone()
        .or_else(|| config.as_ref()?.eviction.low_water_evict.clone())
        .ok_or("Missing low_water_evict")?;
    let eviction_interval = cli
        .eviction_interval
        .clone()
        .or_else(|| config.as_ref()?.eviction.eviction_interval.clone())
        .ok_or("Missing eviction_interval")?;

    if low_water_evict < high_water_evict {
        return Err("low_water_evict must be greater than high_water_evict".into());
    }

    let chunk_size = cli
        .chunk_size
        .or_else(|| config.as_ref()?.server.chunk_size);
    let chunk_size = chunk_size.ok_or("Missing chunk size")?;
    let max_write_size = cli
        .max_write_size
        .or_else(|| config.as_ref()?.server.max_write_size);
    let max_write_size = max_write_size.ok_or("Missing max write size")?;

    let block_zone_capacity = cli
        .block_zone_capacity
        .or_else(|| config.as_ref()?.server.block_zone_capacity);
    let block_zone_capacity = block_zone_capacity.ok_or("Missing block_zone_capacity")?;

    // TODO: Add secrets from env vars

    Ok(ServerConfig {
        socket,
        disk,
        writer_threads,
        reader_threads,
        remote: ServerRemoteConfig {
            remote_type,
            bucket: remote_bucket,
            remote_artificial_delay_microsec,
        },
        eviction: ServerEvictionConfig {
            eviction_type: eviction_policy,
            high_water_evict,
            low_water_evict,
            eviction_interval: eviction_interval as u64,
        },
        chunk_size,
        block_zone_capacity,
        max_write_size
    })
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CliArgs::parse();
    let config = load_config(&cli)?;

    // console_subscriber::init(); // -- To use tokio-console

    println!("Config: {:?}", config);
    let remote = remote::validated_type(config.remote.remote_type.as_str());
    let remote = remote.unwrap_or_else(|err| {
        eprintln!("Error: {}", err);
        exit(1);
    });

    let chunk_size = config.chunk_size;
    let remote_artificial_delay_microsec = config.remote.remote_artificial_delay_microsec;

    match remote {
        remote::RemoteBackendType::Emulated => {
            Server::new(
                config,
                remote::EmulatedBackend::new(chunk_size, remote_artificial_delay_microsec),
            )?
            .run()
            .await?;
        }
        remote::RemoteBackendType::S3 => {
            let bucket = config.remote.bucket.clone().unwrap();
            Server::new(config, remote::S3Backend::new(bucket, chunk_size))?
                .run()
                .await?;
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    RUNTIME.block_on(async_main())
}
