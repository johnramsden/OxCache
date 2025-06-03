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
    
    for number in 0..10 {
        let msg = Request::Get(GetRequest {
            key: number.to_string(),
            offset: number,
            size: number
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
            let msg: Result<(Vec<u8>, usize), DecodeError> =
                bincode::serde::decode_from_slice(bytes, bincode::config::standard());
            println!("Received: {:?}", msg);
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
