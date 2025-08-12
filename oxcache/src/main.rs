use clap::Parser;
use log::LevelFilter;
use oxcache;
use oxcache::cli::{load_config, AppConfig, CliArgs};
use oxcache::remote;
use oxcache::server::{RUNTIME, Server};
use std::fs;
use std::process::exit;

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
    init_logging(&log_level);

    let config = load_config(&cli)?;

    // console_subscriber::init(); // -- To use tokio-console

    log::debug!("Config: {:?}", config);
    let remote = remote::validated_type(config.remote.remote_type.as_str());
    let remote = remote.unwrap_or_else(|err| {
        log::error!("Error: {}", err);
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

/// Initialize global logging with a given level string. If the logger is already initialized,
/// subsequent calls will be ignored. Supported levels (case-insensitive) are
/// "error", "warn", "info", "debug" and "trace". Any other value defaults to "info".
fn init_logging(level: &str) {
    // Parse the provided level string into a LevelFilter
    let level_filter = match level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    };
    let mut builder = env_logger::Builder::new();
    builder.filter_level(level_filter);
    // Attempt to initialize the logger. If it has already been initialized
    // (for example, by another library), silently ignore the error.
    let _ = builder.try_init();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    RUNTIME.block_on(async_main())
}
