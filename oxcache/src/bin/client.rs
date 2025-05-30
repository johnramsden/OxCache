use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::{Duration, sleep};

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

    let mut stream = UnixStream::connect(&args.socket).await?;
    println!("Connected to {}", args.socket);

    for number in 0..10 {
        let msg = format!("{}\n", number);
        stream.write_all(msg.as_bytes()).await?;
        println!("Sent: {}", number);

        // Optional: wait for a response after each send
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await?;
        println!("Received: {}", String::from_utf8_lossy(&buf[..n]));

        sleep(Duration::from_secs(2)).await;
    }

    let msg = "exit\n";
    stream.write_all(msg.as_bytes()).await?;
    println!("Sent: {}", msg);

    // Optional: wait for a response after each send
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    println!("Received: {}", String::from_utf8_lossy(&buf[..n]));

    Ok(())
}
