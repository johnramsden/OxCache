use clap::Parser;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::{Duration, sleep};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use futures::sink::SinkExt;

use oxcache::request::{GetRequest, Request};
use bincode;
use bincode::error::DecodeError;
use bytes::Bytes;
use futures::StreamExt;
use oxcache::request;

/// Simple Unix socket client
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// Path to the unix socket to connect to
    #[arg(long)]
    socket: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    let stream = UnixStream::connect(&args.socket).await?;
    println!("Connected to {}", args.socket);

    let (read_half, write_half) = split(stream);
    let mut reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
    let mut writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());
    
    let work = vec![
        ("0", 0, 4096, true), ("0", 0, 4096, true),
        ("Unaligned", 7, 4096, false),
        ("Too large", 7, 8096, false),
    ];
    for req in work {
        let msg = Request::Get(GetRequest {
            key: req.0.to_string(),
            offset: req.1,
            size: req.2,
        });
        
        println!("Sending: {:?}", msg);
        
        let encoded = bincode::serde::encode_to_vec(
            msg,
            bincode::config::standard()
        ).unwrap();
        writer.send(Bytes::from(encoded)).await?;

        // wait for a response after each send

        if let Some(frame) = reader.next().await {
            let f = frame?;
            let bytes = f.as_ref();
            let msg: Result<(request::GetResponse, usize), DecodeError> =
                bincode::serde::decode_from_slice(bytes, bincode::config::standard());
            match msg?.0 {
                (request::GetResponse::Error(s)) => {
                    assert!(!req.3, "Expected success, recieved error {}", s);
                },
                (request::GetResponse::Response(_)) => {
                    assert!(req.3, "Expected error, recieved success");
                }
            }
        }

        sleep(Duration::from_secs(2)).await;
    }

    // let msg = "exit\n";
    // // writer.write_all(msg.as_bytes()).await?;
    // println!("Sent: {}", msg);
    // 
    // // Optional: wait for a response after each send
    // let mut buf = vec![0u8; 1024];
    // let n = stream.read(&mut buf).await?;
    // println!("Received: {}", String::from_utf8_lossy(&buf[..n]));

    Ok(())
}
