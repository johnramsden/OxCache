pub mod eval_client;
pub mod mock_device;

use clap::Parser;
use oxcache::{
    cli::{CliArgs, load_config},
    remote,
    server::{RUNTIME, Server},
};

async fn async_main() {
    // Change based on if we are running in GH actions or not
    let path = match option_env!("RUNNING_IN_GITHUB_ACTIONS") {
        Some(_) => {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../github_runner.server.toml")
        }
        None => std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../example.server.toml"),
    };

    let path_str = path.to_str().unwrap();
    let cli = CliArgs::parse_from(["oxcache", "--config", path_str]);
    let config = load_config(&cli).unwrap();

    let chunk_size = config.chunk_size;
    let remote_artificial_delay_microsec = config.remote.remote_artificial_delay_microsec;

    let server = Server::new(
        config,
        remote::EmulatedBackend::new(chunk_size, remote_artificial_delay_microsec),
    );
    let server = match server {
        Ok(server) => server,
        Err(error) => panic!("Failed to spawn server: {}", error),
    };

    server.shutdown.notify_waiters();

    let handle = RUNTIME.spawn(async move { server.run().await });

    match handle.await {
        Ok(server_result) => match server_result {
            Ok(()) => (),
            Err(err) => panic!("{}", err),
        },
        Err(tokio_err) => panic!("{}", tokio_err),
    }
}

fn main() {
    RUNTIME.block_on(async_main());
}
