use oxcache;
use std::fs;

use clap::Parser;
use oxcache::server::{Server, ServerConfig};
use serde::Deserialize;

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
}

#[derive(Debug, Deserialize)]
pub struct ParsedServerConfig {
    pub socket: Option<String>,
    pub disk: Option<String>,
    pub writer_threads: Option<usize>,
    pub reader_threads: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub server: ParsedServerConfig,
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
        .clone()
        .or_else(|| config.as_ref()?.server.writer_threads.clone());
    let reader_threads = cli
        .reader_threads
        .clone()
        .or_else(|| config.as_ref()?.server.reader_threads.clone());

    let socket = socket.ok_or("Missing required `socket` (in CLI or file)")?;
    let disk = disk.ok_or("Missing required `disk` (in CLI or file)")?;

    let writer_threads =
        writer_threads.ok_or("Missing required `writer_threads` (in CLI or file)")?;

    if writer_threads == 0 {
        return Err("writer_threads must be greater than 0".into());
    }
    let reader_threads = reader_threads.ok_or("Missing required `disk` (in CLI or file)")?;

    if reader_threads == 0 {
        return Err("reader_threads must be greater than 0".into());
    }

    Ok(ServerConfig {
        socket,
        disk,
        writer_threads,
        reader_threads,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CliArgs::parse();
    let config = load_config(&cli)?;

    println!("Config: {:?}", config);

    Server::new(config).run().await?;
    Ok(())
}
