use std::{fs, net::{IpAddr, SocketAddr}};

use clap::Parser;
use nvme::types::Byte;
use serde::Deserialize;

use crate::server::{ServerConfig, ServerEvictionConfig, ServerMetricsConfig, ServerRemoteConfig};


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
    pub cache_shards: Option<usize>,

    #[arg(long)]
    pub chunk_size: Option<Byte>,

    #[arg(long)]
    pub max_write_size: Option<Byte>,

    #[arg(long)]
    pub remote_type: Option<String>,

    #[arg(long)]
    pub remote_bucket: Option<String>,

    #[arg(long)]
    pub eviction_policy: Option<String>,

    #[arg(long)]
    pub high_water_evict: Option<u64>,

    #[arg(long)]
    pub low_water_evict: Option<u64>,

    #[arg(long)]
    pub low_water_clean: Option<u64>,

    #[arg(long)]
    pub block_zone_capacity: Option<Byte>,

    #[arg(long)]
    pub eviction_interval_ms: Option<usize>,

    #[arg(long)]
    pub remote_artificial_delay_microsec: Option<usize>,

    #[arg(long)]
    pub metrics_ip_addr: Option<String>,

    #[arg(long)]
    pub metrics_port: Option<u16>,

    #[arg(long)]
    pub max_zones: Option<u64>,

    /// Logging level: error, warn, info, debug, trace. Overrides config if set.
    #[arg(long)]
    pub log_level: Option<String>,

    /// Directory to store metrics log files
    #[arg(long)]
    pub file_metrics_directory: Option<String>,

    /// Enable throughput benchmark mode (measures bytes/sec after first eviction)
    #[arg(long)]
    pub benchmark_mode: bool,

    /// Duration in seconds to run benchmark after first eviction (optional, default: unset)
    #[arg(long)]
    pub benchmark_duration_secs: Option<u64>,

    /// Target bytes to process in benchmark mode before stopping (optional, default: unset)
    #[arg(long)]
    pub benchmark_target_bytes: Option<u64>,

    /// Enable detailed eviction metrics tracking (requires compilation with --features eviction-metrics)
    #[arg(long)]
    pub eviction_metrics: bool,

    /// Eviction metrics log interval in seconds (default: 60)
    #[arg(long)]
    pub eviction_metrics_interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ParsedServerConfig {
    pub socket: Option<String>,
    pub disk: Option<String>,
    pub writer_threads: Option<usize>,
    pub reader_threads: Option<usize>,
    pub cache_shards: Option<usize>,
    pub chunk_size: Option<Byte>,
    pub max_write_size: Option<Byte>,
    pub block_zone_capacity: Option<Byte>,
    pub max_zones: Option<u64>,
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
    pub high_water_evict: Option<u64>,
    pub low_water_evict: Option<u64>,
    pub low_water_clean: Option<u64>,
    pub eviction_interval_ms: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ParsedMetricsConfig {
    pub ip_addr: Option<String>,
    pub port: Option<u16>,
    pub file_metrics_directory: Option<String>,
    pub benchmark_mode: Option<bool>,
    pub benchmark_duration_secs: Option<u64>,
    pub benchmark_target_bytes: Option<u64>,
    pub eviction_metrics: Option<bool>,
    pub eviction_metrics_interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub server: ParsedServerConfig,
    pub remote: ParsedRemoteConfig,
    pub eviction: ParsedEvictionConfig,
    pub metrics: ParsedMetricsConfig,
    pub log_level: Option<String>,
}

pub fn load_config(cli: &CliArgs) -> Result<ServerConfig, Box<dyn std::error::Error>> {
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
    let cache_shards = cli
        .cache_shards
        .or_else(|| config.as_ref()?.server.cache_shards);

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
        // Warn if artificial delay is set for non-emulated remote types
        tracing::warn!(
            "remote_artificial_delay_microsec has no effect if remote_type is not `emulated`"
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

    let low_water_clean = cli
        .low_water_clean
        .clone()
        .or_else(|| config.as_ref()?.eviction.low_water_clean.clone());

    let eviction_interval = cli
        .eviction_interval_ms
        .clone()
        .or_else(|| config.as_ref()?.eviction.eviction_interval_ms.clone())
        .ok_or("Missing eviction_interval")?;

    if low_water_evict < high_water_evict {
        return Err("low_water_evict must be greater than high_water_evict".into());
    }

    // Check clean settings
    if eviction_policy.to_lowercase() == "chunk" {
        let low_water_clean = if low_water_clean.is_none() {
            return Err("low_water_clean must be set".into());
        } else { low_water_clean.unwrap() };

        if low_water_clean >= (low_water_evict - high_water_evict) {
            return Err("low_water_clean must be less than (low_water_evict - high_water_evict)".into());
        }
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

    let max_zones = cli
        .max_zones
        .or_else(|| config.as_ref()?.server.max_zones);

    // Metrics

    let metrics_port = cli
        .metrics_port
        .clone()
        .or_else(|| config.as_ref()?.metrics.port.clone());
    let metrics_ip = cli
        .metrics_ip_addr
        .clone()
        .or_else(|| config.as_ref()?.metrics.ip_addr.clone());

    if metrics_port.is_none() && metrics_ip.is_some() || metrics_port.is_some() && metrics_ip.is_none() {
        return Err("Missing metrics ip or port, both must be set or neither".into());
    }

    let metrics_exporter_addr = if metrics_port.is_some() {
        let ip = match metrics_ip.unwrap().parse::<IpAddr>() {
            Ok(ip) => ip,
            Err(e) => {
                return Err(format!("Invalid metrics ip address {:?}", e).into());
            }
        };
        Some(SocketAddr::new(ip, metrics_port.unwrap()))
    } else {
        None
    };

    // Benchmark mode settings
    let benchmark_mode = cli.benchmark_mode || config.as_ref().and_then(|c| c.metrics.benchmark_mode).unwrap_or(false);
    let benchmark_duration_secs = cli.benchmark_duration_secs
        .or_else(|| config.as_ref().and_then(|c| c.metrics.benchmark_duration_secs));
    let benchmark_target_bytes = cli.benchmark_target_bytes
        .or_else(|| config.as_ref().and_then(|c| c.metrics.benchmark_target_bytes));

    let eviction_metrics = cli.eviction_metrics || config.as_ref().and_then(|c| c.metrics.eviction_metrics).unwrap_or(false);
    let eviction_metrics_interval = cli.eviction_metrics_interval
        .or_else(|| config.as_ref().and_then(|c| c.metrics.eviction_metrics_interval))
        .unwrap_or(60); // Default to 60 seconds

    // TODO: Add secrets from env vars

    Ok(ServerConfig {
        socket,
        disk,
        writer_threads,
        reader_threads,
        cache_shards,
        remote: ServerRemoteConfig {
            remote_type,
            bucket: remote_bucket,
            remote_artificial_delay_microsec,
        },
        eviction: ServerEvictionConfig {
            eviction_type: eviction_policy,
            high_water_evict,
            low_water_evict,
            low_water_clean,
            eviction_interval: eviction_interval as u64,
        },
        chunk_size,
        block_zone_capacity,
        max_write_size,
        max_zones,
        metrics: ServerMetricsConfig {
            metrics_exporter_addr,
            benchmark_mode,
            benchmark_duration_secs,
            benchmark_target_bytes,
            eviction_metrics,
            eviction_metrics_interval,
        }
    })
}
