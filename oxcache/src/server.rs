use crate::cache::Cache;
use crate::writerpool::WriterPool;
use crate::readerpool::ReaderPool;
use crate::eviction::Evictor;

// use tokio::spawn;
use tokio::net::{UnixListener, UnixStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        
        // TODO: Clean shutdown.
        //   let shutdown_signal = tokio::signal::ctrl_c();
        //   How to handle this? tokio::select!?. What about threads?
        //         evictor.stop();
        //         writerpool.stop();
        //         readerpool.stop();

        // Request handling (green threads)
        loop {
            let (stream, addr) = listener.accept().await?;
            println!("Accepted connection: {:?}", addr);

            let cache = Arc::clone(&self.cache);

            // Spawn green thread for each connection
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, cache).await {
                    eprintln!("Connection error: {}", e);
                }
            });
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
