use bincode::error::DecodeError;
use bytes::Bytes;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use oxcache::request;
use oxcache::request::{GetRequest, Request};
use rand::distr::weighted::Error;
use rand::prelude::IndexedRandom;
use rand::rng;
use std::fmt::format;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio;
use tokio::io::split;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// Path to the unix socket to connect to
    #[arg(long)]
    socket: String,

    #[arg(long)]
    num_clients: usize,
    
    #[arg(long)]
    query_size: usize,
}

const MAX_FRAME_LENGTH: usize = 2 * 1024 * 1024 * 1024; // 2 GB

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nr_queries = 10000;
    let nr_uuids = 100;
    let mut queries: Arc<Mutex<Vec<GetRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let args = Cli::parse();
    let counter = Arc::new(AtomicUsize::new(0));
    let step = nr_queries / 10; // 10%
    let mut rng = rng(); // uses fast, thread-local RNG

    let mut uuids = Vec::with_capacity(nr_uuids);
    for _ in 0..nr_uuids {
        uuids.push(Uuid::new_v4());
    }

    {
        let mut queries = queries.lock().unwrap();
        for _ in 0..nr_queries {
            let uuid = uuids
                .choose(&mut rng)
                .expect("uuids vec should not be empty")
                .clone();

            queries.push(GetRequest {
                key: uuid.to_string(),
                size: args.query_size,
                offset: 0,
            });
        }
    }

    if args.num_clients <= 0 {
        return Err(format!("num_clients={} must be at least 1", args.num_clients).into());
    }

    let mut handles: Vec<JoinHandle<tokio::io::Result<()>>> = Vec::new();

    let start = Instant::now();

    for c in 0..args.num_clients {
        let sock = args.socket.clone();
        let queries = Arc::clone(&queries);
        let counter = Arc::clone(&counter);
        handles.push(tokio::spawn(async move {
            let stream = UnixStream::connect(&sock).await?;
            println!("[t.{}] Client connected to {}", c, sock);

            let (read_half, write_half) = tokio::io::split(stream);

            let codec = LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_LENGTH)
                .new_codec();

            let mut reader = FramedRead::new(read_half, codec.clone());
            let mut writer = FramedWrite::new(write_half, codec);

            loop {
                let query_num = counter.fetch_add(1, Ordering::Relaxed) + 1;

                let q: GetRequest;
                {
                    let mut queries = queries.lock().unwrap();
                    if queries.len() == 0 {
                        println!("[t.{}] Client completed", c);
                        return Ok(());
                    }
                    q = queries.pop().unwrap();
                }

                if query_num % step == 0 {
                    let percent = (query_num as f64 / nr_queries as f64) * 100.0;
                    println!(
                        "[t.{}] Query num: {}/{} ({:.0}%)",
                        c, query_num, nr_queries, percent
                    );
                }

                let encoded =
                    bincode::serde::encode_to_vec(Request::Get(q), bincode::config::standard())
                        .unwrap();
                writer.send(Bytes::from(encoded)).await?;

                // wait for a response after each send

                if let Some(frame) = reader.next().await {
                    let f = frame?;
                    let bytes = f.as_ref();
                    let msg: Result<(request::GetResponse, usize), DecodeError> =
                        bincode::serde::decode_from_slice(bytes, bincode::config::standard());
                    match msg.unwrap().0 {
                        (request::GetResponse::Error(s)) => {
                            return Err(std::io::Error::new(
                                ErrorKind::Other,
                                format!("[t.{}] Received error {} from client", c, s),
                            ));
                        }
                        (request::GetResponse::Response(_)) => {}
                    }
                }
            }
        }));
    }


    for handle in handles {
        let res = handle.await;
        match res {
            Err(join_err) => {
                eprintln!("Task panicked or was cancelled: {join_err}");
            }
            Ok(Err(io_err)) => {
                eprintln!("Task exited with error: {io_err}");
            }
            Ok(Ok(())) => {
                // success
            }
        }
    }

    let duration = start.elapsed(); // Stop the timer

    println!(
        "Executed {} queries across {} clients",
        nr_queries, args.num_clients
    );
    println!("Total run time: {:.2?}", duration);

    Ok(())
}
