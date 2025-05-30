use crate::cache::Cache;
use crate::eviction::Evictor;
use crate::readerpool::ReaderPool;
use crate::writerpool::WriterPool;

// use tokio::spawn;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug)]
pub struct ServerConfig {
    pub socket: String,
    pub disk: String,
    pub writer_threads: usize,
    pub reader_threads: usize,
}

pub struct Server {
    cache: Arc<Cache>, // shared across tasks
    config: ServerConfig,
}

impl Server {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            cache: Arc::new(Cache::new()),
            config,
        }
    }

    pub async fn run(&self) -> tokio::io::Result<()> {
        let socket_path = Path::new(&self.config.socket);

        // Clean up old socket if present
        if socket_path.exists() {
            tokio::fs::remove_file(socket_path).await?;
        }

        let listener = UnixListener::bind(socket_path)?;
        println!("Listening on socket: {}", self.config.socket);

        let evictor = Evictor::start();
        let writerpool = WriterPool::start(self.config.writer_threads);
        let readerpool = ReaderPool::start(self.config.reader_threads);

        // Shutdown signal
        let shutdown = Arc::new(Notify::new());
        let shutdown_signal = shutdown.clone();

        // Spawn a task to listen for Ctrl+C
        let shutdown_task = tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl_c");
            println!("Ctrl+C received, shutting down...");
            shutdown_signal.notify_waiters();
            evictor.stop();
            writerpool.stop();
            readerpool.stop();
        });

        // Request handling (green threads)
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    println!("Shutting down accept loop.");
                    break;
                }

                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            println!("Accepted connection: {:?}", addr);

                            tokio::spawn({
                                let cache = Arc::clone(&self.cache);
                                async move {
                                if let Err(e) = handle_connection(stream, cache).await {
                                    eprintln!("Connection error: {}", e);
                                }
                            }});
                        },
                        Err(e) => {
                            eprintln!("Accept failed: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn handle_connection(mut stream: UnixStream, _cache: Arc<Cache>) -> tokio::io::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).await?;

        let recieved = String::from_utf8_lossy(&buf[..n]);

        println!("Received: {}", recieved);

        stream.write_all(b"OK\n").await?;
        if recieved == "exit\n" {
            break;
        }
    }
    Ok(())
}
