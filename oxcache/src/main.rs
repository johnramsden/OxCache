use clap::Parser;
use oxcache;
use oxcache::cli::{AppConfig, CliArgs, load_config};
use oxcache::remote;
use oxcache::server::{RUNTIME, Server};
use std::fs;
use std::process::exit;
use std::sync::OnceLock;
use tracing_appender::non_blocking;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use flate2::write::GzEncoder;
use flate2::Compression;

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

    let file_metrics_directory = cli.file_metrics_directory.clone().or_else(|| {
        app_config
            .as_ref()
            .and_then(|c| c.metrics.file_metrics_directory.clone())
    });

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
        "warn" => "warn",
        "info" => "info",
        "debug" => "debug",
        "trace" => "trace",
        _ => "info",
    };

    if let Some(metrics_dir) = metrics_directory {
        // Regular logs, no metrics
        let stdout_layer = fmt::layer()
            .with_writer(std::io::stdout)
            .with_target(true)
            .with_level(true)
            .compact()
            .with_filter(EnvFilter::new(format!("{},metrics=off", directive)));

        // Metrics in log dir - always compressed with gzip
        let date = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
        let filename = format!("{}/metrics-{}.json.gz", metrics_dir, date);
        let file = fs::File::create(&filename)
            .expect("Failed to create metrics file");
        let encoder = GzEncoder::new(file, Compression::default());
        let (metrics_nb, guard) = non_blocking(encoder);
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

        let _ = tracing_subscriber::registry().with(stdout_layer).try_init();
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    RUNTIME.block_on(async_main())
}
