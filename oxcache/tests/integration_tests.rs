use clap::Parser;
use oxcache::cli::{load_config, CliArgs};

fn main() {
    // Change based on if we are running in GH actions or not
    let path = match option_env!("RUNNING_IN_GITHUB_ACTIONS") {
        Some(_) => std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../github_runner.server.toml"),
        None => std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../example.server.toml")
    };

    let path_str = path.to_str().unwrap();
    let cli = CliArgs::parse_from(["oxcache", "--config", path_str]);
    let config = load_config(&cli).unwrap();

}

