use std::fs;
use oxcache;

use clap::Parser;
use serde::Deserialize;
use oxcache::server::{ServerConfig,Server};

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
}

#[derive(Debug, Deserialize)]
pub struct ParsedServerConfig {
    pub socket: Option<String>,
    pub disk: Option<String>
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

    let socket = cli.socket.clone().or_else(|| config.as_ref()?.server.socket.clone());
    let disk = cli.disk.clone().or_else(|| config.as_ref()?.server.disk.clone());

    let socket = socket.ok_or("Missing required `socket` (in CLI or file)")?;
    let disk = disk.ok_or("Missing required `disk` (in CLI or file)")?;

    Ok(ServerConfig {
        socket,
        disk
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
