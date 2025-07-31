use clap::Parser;
use oxcache;
use oxcache::cli::{load_config, CliArgs};
use oxcache::remote;
use oxcache::server::{RUNTIME, Server};
use std::process::exit;

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
