use bincode::error::DecodeError;
use bytes::Bytes;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use oxcache::request;
use oxcache::request::{GetRequest, Request};
use rand::prelude::IndexedRandom;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio;
use tokio::io::split;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// Path to the unix socket to connect to
    #[arg(long)]
    socket: String,

    #[arg(long)]
    num_clients: usize,

    #[arg(long)]
    data_file: String,
}

use byteorder::{LittleEndian, ReadBytesExt};
use std::fs::File;
use std::io::{self, BufReader};

fn read_queries(path: &str) -> io::Result<Vec<i32>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut vec = Vec::new();

    while let Ok(value) = reader.read_i32::<LittleEndian>() {
        vec.push(value);
    }

    Ok(vec)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let mut query_inputs = read_queries(&args.data_file)?;
    let nr_queries = query_inputs.len();
    let queries: Arc<Mutex<Vec<GetRequest>>> = Arc::new(Mutex::new(Vec::new()));

    let counter = Arc::new(AtomicUsize::new(0));
    let step = nr_queries / 10; // 10%

    {
        let mut queries = queries.lock().unwrap();
        for _ in 0..nr_queries {
            queries.push(GetRequest {
                key: query_inputs.pop().unwrap().to_string(),
                size: 8192,
                offset: 0,
            });
        }
    }

    if args.num_clients <= 0 {
        return Err(format!("num_clients={} must be at least 1", args.num_clients).into());
    }

    let mut handles: Vec<JoinHandle<io::Result<()>>> = Vec::new();

    let start = Instant::now();

    for c in 0..args.num_clients {
        let sock = args.socket.clone();
        let queries = Arc::clone(&queries);
        let counter = Arc::clone(&counter);
        handles.push(tokio::spawn(async move {
            let stream = UnixStream::connect(&sock).await?;
            println!("[t.{}] Client connected to {}", c, sock);

            let (read_half, write_half) = split(stream);
            let mut reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
            let mut writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());

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
                        request::GetResponse::Error(s) => {
                            return Err(std::io::Error::new(
                                ErrorKind::Other,
                                format!("[t.{}] Received error {} from client", c, s),
                            ));
                        }
                        request::GetResponse::Response(_) => {}
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    let duration = start.elapsed(); // Stop the timer

    println!(
        "Executed {} queries across {} clients",
        nr_queries, args.num_clients
    );
    println!("Total run time: {:.2?}", duration);

    Ok(())
}
