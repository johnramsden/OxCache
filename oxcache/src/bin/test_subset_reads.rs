use bincode::error::DecodeError;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use oxcache::request::{GetRequest, GetResponse, Request};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::time::sleep;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

const MAX_FRAME_LENGTH: usize = 2 * 1024 * 1024 * 1024; // 2 GB

struct TestClient {
    reader: FramedRead<tokio::io::ReadHalf<UnixStream>, LengthDelimitedCodec>,
    writer: FramedWrite<tokio::io::WriteHalf<UnixStream>, LengthDelimitedCodec>,
}

impl TestClient {
    async fn new(socket_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = UnixStream::connect(socket_path).await?;
        let (read_half, write_half) = tokio::io::split(stream);

        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_LENGTH)
            .new_codec();

        let reader = FramedRead::new(read_half, codec.clone());
        let writer = FramedWrite::new(write_half, codec);

        Ok(Self { reader, writer })
    }

    async fn get(
        &mut self,
        key: String,
        offset: u64,
        size: u64,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        let request = Request::Get(GetRequest { key, offset, size });

        let encoded = bincode::serde::encode_to_vec(request, bincode::config::standard())?;
        self.writer.send(Bytes::from(encoded)).await?;

        if let Some(frame) = self.reader.next().await {
            let f = frame?;
            let bytes = f.as_ref();
            let msg: Result<(GetResponse, usize), DecodeError> =
                bincode::serde::decode_from_slice(bytes, bincode::config::standard());

            match msg?.0 {
                GetResponse::Error(e) => Err(e.into()),
                GetResponse::Response(data) => Ok(data),
            }
        } else {
            Err("No response received".into())
        }
    }

    async fn close(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let encoded = bincode::serde::encode_to_vec(Request::Close, bincode::config::standard())?;
        self.writer.send(Bytes::from(encoded)).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the cache server
    let socket_path = "/tmp/oxcache.sock";
    let mut client = TestClient::new(socket_path).await?;

    println!("Testing subset reads with deterministic data...");

    // Test parameters - using 4KB (4096 byte) aligned values for LBA alignment
    let key = "test_subset_key";
    let chunk_size = 65536; // 64KB chunk size
    let lba_size = 4096; // 4KB logical block size
    let test_cases = vec![
        (0, lba_size),                // Read first 4KB (includes header)
        (lba_size, lba_size),         // Read second 4KB block
        (lba_size * 2, lba_size),     // Read third 4KB block
        (lba_size * 4, lba_size * 2), // Read 8KB starting at 16KB offset
        (0, chunk_size),              // Read entire chunk
    ];

    for (offset, size) in test_cases {
        println!("\nTesting read: offset={}, size={}", offset, size);

        // Make the request
        let result = client.get(key.to_string(), offset, size).await;

        match result {
            Ok(data) => {
                println!("Read successful, received {} bytes", data.len());

                // Verify the data is deterministic and correct
                if validate_subset_data(&data, key, offset, size) {
                    println!("Data validation passed");
                } else {
                    println!("ERROR: Data validation failed");
                }
            }
            Err(e) => {
                println!("ERROR: Read failed: {}", e);
            }
        }

        // Small delay between requests
        sleep(Duration::from_millis(100)).await;
    }

    // Test edge cases
    println!("\nTesting edge cases...");

    // Test LBA-aligned edge cases
    let edge_cases = vec![
        (0, lba_size),                     // Single LBA at start
        (chunk_size - lba_size, lba_size), // Single LBA at end
    ];

    for (offset, size) in edge_cases {
        println!("\nTesting edge case: offset={}, size={}", offset, size);

        let result = client.get(key.to_string(), offset, size).await;

        match result {
            Ok(data) => {
                println!("Edge case read successful, received {} bytes", data.len());
                if validate_subset_data(&data, key, offset, size) {
                    println!("Edge case data validation passed");
                } else {
                    println!("ERROR: Edge case data validation failed");
                }
            }
            Err(e) => {
                println!("ERROR: Edge case read failed: {}", e);
            }
        }
    }

    client.close().await?;
    println!("\nTest completed!");

    Ok(())
}

fn validate_subset_data(data: &Bytes, key: &str, offset: u64, size: u64) -> bool {
    println!("=== DEBUG: Validating offset={}, size={} ===", offset, size);
    println!("Actual data length: {}", data.len());
    println!("Expected data length: {}", size);

    if data.len() != size as usize {
        println!(
            "ERROR: Data length mismatch: expected {}, got {}",
            size,
            data.len()
        );
        return false;
    }

    // Generate expected data using the same algorithm as EmulatedBackend
    let expected_data = generate_expected_subset_data(key, offset, size);
    println!("Generated expected data length: {}", expected_data.len());

    // Show more bytes for comparison
    let show_bytes = std::cmp::min(32, std::cmp::min(data.len(), expected_data.len()));
    println!(
        "Expected first {} bytes: {:02x?}",
        show_bytes,
        &expected_data[0..show_bytes]
    );
    println!(
        "Actual first {} bytes:   {:02x?}",
        show_bytes,
        &data[0..show_bytes]
    );

    // Find first mismatch
    for (i, (actual, expected)) in data.iter().zip(expected_data.iter()).enumerate() {
        if actual != expected {
            println!(
                "ERROR: First mismatch at byte {}: expected 0x{:02x}, got 0x{:02x}",
                i, expected, actual
            );
            return false;
        }
    }

    if data.len() != expected_data.len() {
        println!(
            "ERROR: Length mismatch after byte comparison: actual {}, expected {}",
            data.len(),
            expected_data.len()
        );
        return false;
    }

    true
}

fn generate_expected_subset_data(key: &str, offset: u64, size: u64) -> Vec<u8> {
    use rand::{RngCore, SeedableRng};
    use rand_pcg::Pcg64;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    println!(
        "=== DEBUG: Generating expected data for offset={}, size={} ===",
        offset, size
    );

    const EMULATED_BUFFER_SEED: u64 = 1;
    const CHUNK_SIZE: u64 = 65536; // Match our test chunk size

    // Generate the same buffer that EmulatedBackend would create
    // EmulatedBackend is called with (key, 0, CHUNK_SIZE)

    // Create header (24 bytes)
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let key_hash = hasher.finish();
    println!("Key hash: 0x{:016x}", key_hash);

    let mut full_chunk = Vec::new();
    full_chunk.extend_from_slice(&key_hash.to_be_bytes()); // 8 bytes
    full_chunk.extend_from_slice(&0u64.to_be_bytes()); // 8 bytes - offset=0
    full_chunk.extend_from_slice(&CHUNK_SIZE.to_be_bytes()); // 8 bytes - size=chunk_size

    println!("Header (24 bytes): {:02x?}", &full_chunk[0..24]);

    // Fill remaining with random data
    let mut rng = Pcg64::seed_from_u64(EMULATED_BUFFER_SEED);
    let remaining_size = CHUNK_SIZE - 24;
    let mut random_data = vec![0u8; remaining_size as usize];
    rng.fill_bytes(&mut random_data);
    full_chunk.extend_from_slice(&random_data);

    println!("Generated full chunk length: {}", full_chunk.len());
    println!("Full chunk first 32 bytes: {:02x?}", &full_chunk[0..32]);

    // Extract the requested subset from the full chunk
    let start = offset as usize;
    let end = (offset + size) as usize;
    println!("Extracting bytes {}..{}", start, end);

    let result = full_chunk[start..end].to_vec();
    println!("Extracted subset length: {}", result.len());
    println!(
        "Extracted first 32 bytes: {:02x?}",
        &result[0..std::cmp::min(32, result.len())]
    );

    result
}
