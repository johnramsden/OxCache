use clap::Parser;
use oxcache::cli::{load_config, CliArgs};

fn main() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../example.server.toml");

    let path_str = path.to_str().unwrap();
    let cli = CliArgs::parse_from(["oxcache", "--config", path_str]);
    let config = load_config(&cli).unwrap();
    println!("{:?}", config);
}
