use clap::Parser;
use nvme::types::Byte;
use oxcache;
use oxcache::remote;
use oxcache::server::{RUNTIME, Server, ServerConfig, ServerEvictionConfig, ServerRemoteConfig, ServerMetricsConfig};
use serde::Deserialize;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::process::exit;
use std::sync::OnceLock;
use tracing::{event, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use tracing_appender::{non_blocking, rolling};

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
    pub high_water_clean: Option<u64>,

    #[arg(long)]
    pub low_water_clean: Option<u64>,

    #[arg(long)]
    pub block_zone_capacity: Option<Byte>,

    #[arg(long)]
    pub eviction_interval: Option<usize>,

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
}

#[derive(Debug, Deserialize)]
pub struct ParsedServerConfig {
    pub socket: Option<String>,
    pub disk: Option<String>,
    pub writer_threads: Option<usize>,
    pub reader_threads: Option<usize>,
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
    pub high_water_clean: Option<u64>,
    pub low_water_clean: Option<u64>,
    pub eviction_interval: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ParsedMetricsConfig {
    pub ip_addr: Option<String>,
    pub port: Option<u16>,
    pub file_metrics_directory: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub server: ParsedServerConfig,
    pub remote: ParsedRemoteConfig,
    pub eviction: ParsedEvictionConfig,
    pub metrics: ParsedMetricsConfig,
    pub log_level: Option<String>,
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

    let high_water_clean = cli
        .high_water_clean
        .clone()
        .or_else(|| config.as_ref()?.eviction.high_water_clean.clone());
    let low_water_clean = cli
        .low_water_clean
        .clone()
        .or_else(|| config.as_ref()?.eviction.low_water_clean.clone());

    let eviction_interval = cli
        .eviction_interval
        .clone()
        .or_else(|| config.as_ref()?.eviction.eviction_interval.clone())
        .ok_or("Missing eviction_interval")?;

    if low_water_evict < high_water_evict {
        return Err("low_water_evict must be greater than high_water_evict".into());
    }

    // Check clean settings
    if eviction_policy.to_lowercase() == "chunk" {
        let low_water_clean = if low_water_clean.is_none() {
            return Err("low_water_clean must be set".into());
        } else { low_water_clean.unwrap() };

        let high_water_clean = if high_water_clean.is_none() {
            return Err("high_water_clean must be set".into());
        } else { high_water_clean.unwrap() };

        if low_water_clean > high_water_clean {
            return Err("low_water_clean must be less than high_water_clean".into());
        }

        if high_water_clean > low_water_evict {
            return Err("high_water_clean must be less than or equal to low_water_evict".into());
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
            high_water_clean,
            low_water_clean,
            eviction_interval: eviction_interval as u64,
        },
        chunk_size,
        block_zone_capacity,
        max_write_size,
        max_zones,
        metrics: ServerMetricsConfig {
            metrics_exporter_addr 
        }
    })
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CliArgs::parse();

    // Determine logging level from CLI or config file before loading server config
    let app_config: Option<AppConfig> = match &cli.config {
        Some(path) => {
            let config_str = fs::read_to_string(path)?;
            Some(toml::from_str::<AppConfig>(&config_str)?)
        }
        None => None,
    };

    let log_level = cli
        .log_level
        .clone()
        .or_else(|| app_config.as_ref().and_then(|c| c.log_level.clone()))
        .unwrap_or_else(|| "info".to_string());
    
    let file_metrics_directory = cli
        .file_metrics_directory
        .clone()
        .or_else(|| app_config.as_ref().and_then(|c| c.metrics.file_metrics_directory.clone()));
    
    init_logging(&log_level, file_metrics_directory.as_deref());

    let config = load_config(&cli)?;

    // console_subscriber::init(); // -- To use tokio-console

    tracing::debug!("Config: {:?}", config);
    let remote = remote::validated_type(config.remote.remote_type.as_str());
    let remote = remote.unwrap_or_else(|err| {
        tracing::error!("Error: {}", err);
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

// Keep the background writer alive for the program lifetime
static METRICS_GUARD: OnceLock<non_blocking::WorkerGuard> = OnceLock::new();

pub fn init_logging(level: &str, metrics_directory: Option<&str>) {
    let directive = match level.to_lowercase().as_str() {
        "error" => "error",
        "warn"  => "warn",
        "info"  => "info",
        "debug" => "debug",
        "trace" => "trace",
        _       => "info",
    };

    if let Some(metrics_dir) = metrics_directory {
        // Regular logs, no metrics
        let stdout_layer = fmt::layer()
            .with_writer(std::io::stdout)
            .with_target(true)
            .with_level(true)
            .compact()
            .with_filter(EnvFilter::new(format!("{},metrics=off", directive)));

        // Metrics in log dir
        let date = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
        let filename = format!("metrics-{}.json", date);
        let metrics_file = rolling::never(metrics_dir, filename);
        let (metrics_nb, guard) = non_blocking(metrics_file);
        let _ = METRICS_GUARD.set(guard);

        // JSON formatting for metrics
        let json_fmt = fmt::format()
            .with_level(false)
            .with_target(false)
            .with_source_location(false)
            .with_thread_ids(false)
            .with_thread_names(false)
            .with_file(false)
            .with_line_number(false)
            .json();

        // Setup metrics
        let metrics_layer = fmt::layer()
            .with_writer(metrics_nb)
            .event_format(json_fmt)
            .with_ansi(false)
            .with_filter(EnvFilter::new("metrics=info"));

        // Register layers
        let _ = tracing_subscriber::registry()
            .with(stdout_layer)
            .with(metrics_layer)
            .try_init();
    } else {
        // Only logs, no metrics (metrics go nowhere)
        let stdout_layer = fmt::layer()
            .with_writer(std::io::stdout)
            .with_target(true)
            .with_level(true)
            .compact()
            .with_filter(EnvFilter::new(format!("{},metrics=off", directive)));

        let _ = tracing_subscriber::registry()
            .with(stdout_layer)
            .try_init();
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    RUNTIME.block_on(async_main())
}
