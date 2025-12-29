use bincode::error::DecodeError;
use bytes::Bytes;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use oxcache::request;
use oxcache::request::{GetRequest, Request};
use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

const MAX_FRAME_LENGTH: usize = 2 * 1024 * 1024 * 1024; // 2 GB
const ALIGNMENT: u64 = 4096; // 4KB alignment
const REQUEST_TIMEOUT_SECS: u64 = 1000; // 1000 second timeout for requests

#[derive(Parser, Debug)]
#[command(author, version, about = "SPC trace workload generator for OxCache")]
struct Cli {
    /// Path to SPC trace file (CSV format)
    #[arg(long, short = 't')]
    trace_file: String,

    /// Chunk size in bytes for quantization
    #[arg(long, short = 'c')]
    chunk_size: u64,

    /// Run mode: 'dry' for dry-run, 'real' for actual execution
    #[arg(long, short = 'm', default_value = "dry")]
    mode: String,

    /// Path to unix socket (required for real mode)
    #[arg(long, short = 's')]
    socket: Option<String>,

    /// Number of concurrent clients (for real mode)
    #[arg(long, short = 'n', default_value = "1")]
    num_clients: usize,

    /// Maximum number of requests to process (0 = unlimited)
    #[arg(long, default_value = "0")]
    max_requests: usize,

    /// Show verbose output including all warnings
    #[arg(long, short = 'v')]
    verbose: bool,
}

#[derive(Debug, Clone)]
struct TraceEntry {
    device: String,
    lba: u64,
    size: u64,
    rw: String,
    timestamp: String,
}

#[derive(Debug, Clone)]
struct QuantizedRequest {
    original_lba: u64,
    chunk_key: String,
    offset: u64,
    size: u64,
    warnings: Vec<String>,
}

#[derive(Debug, Default)]
struct RequestStats {
    total_trace_entries: usize,
    total_get_requests: usize,
    single_chunk_requests: usize,
    multi_chunk_requests: usize,
    max_chunks_spanned: usize,
    alignment_warnings: usize,
}

fn align_to_4k(value: u64) -> (u64, bool) {
    let aligned = (value + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT; // Rounds up to alignment (unless zero)
    (aligned, value != aligned)
}

fn parse_trace(path: &str) -> Result<Vec<TraceEntry>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    let mut errors = 0;

    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;

        // Skip empty lines and comments
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }

        let fields: Vec<&str> = line.split(',').collect();

        if fields.len() < 5 {
            eprintln!(
                "Line {}: Invalid format (need 5 fields, got {})",
                line_num + 1,
                fields.len()
            );
            errors += 1;
            continue;
        }

        // Parse fields: device, LBA, size, R/W, timestamp
        match (fields[1].parse::<u64>(), fields[2].parse::<u64>()) {
            (Ok(lba), Ok(size)) => {
                if size == 0 {
                    eprintln!("Line {}: Skipping zero-size request", line_num + 1);
                    continue;
                }
                entries.push(TraceEntry {
                    device: fields[0].to_string(),
                    lba,
                    size,
                    rw: fields[3].to_string(),
                    timestamp: fields[4].to_string(),
                });
            }
            _ => {
                eprintln!("Line {}: Failed to parse LBA/size", line_num + 1);
                errors += 1;
            }
        }
    }

    if errors > 0 {
        eprintln!("Warning: {} lines had parse errors", errors);
    }

    if entries.is_empty() {
        return Err("No valid trace entries found".into());
    }

    Ok(entries)
}

fn quantize_request(entry: &TraceEntry, chunk_size: u64) -> Vec<QuantizedRequest> {
    let mut results = Vec::new();
    let mut warnings = Vec::new();

    // Step 1: Align LBA to 4KB boundary
    let (aligned_lba, lba_unaligned) = align_to_4k(entry.lba);
    if lba_unaligned {
        warnings.push(format!(
            "LBA {} rounded up to {} (4KB alignment)",
            entry.lba, aligned_lba
        ));
    }

    // Step 2: Align size to 4KB boundary
    let (aligned_size, size_unaligned) = align_to_4k(entry.size);
    if size_unaligned {
        warnings.push(format!(
            "Size {} rounded up to {} (4KB alignment)",
            entry.size, aligned_size
        ));
    }

    // Step 3: Calculate end address
    let end_lba = aligned_lba + aligned_size;

    // Step 4: Iterate through all chunks this request spans
    let mut current_lba = aligned_lba;

    while current_lba < end_lba {
        // Calculate which chunk this LBA belongs to
        let chunk_num = current_lba / chunk_size;
        let offset_in_chunk = current_lba % chunk_size;

        // Calculate how much data fits in this chunk
        let remaining_in_request = end_lba - current_lba;
        let remaining_in_chunk = chunk_size - offset_in_chunk;
        let request_size = remaining_in_request.min(remaining_in_chunk);

        // Generate key format: "chunk_{chunk_num}"
        let chunk_key = format!("chunk_{}", chunk_num);

        results.push(QuantizedRequest {
            original_lba: aligned_lba,
            chunk_key,
            offset: offset_in_chunk,
            size: request_size,
            warnings: warnings.clone(),
        });

        current_lba += request_size;
    }

    results
}

fn generate_requests(
    trace: Vec<TraceEntry>,
    chunk_size: u64,
) -> (Vec<(TraceEntry, Vec<QuantizedRequest>)>, RequestStats) {
    let mut all_requests = Vec::new();
    let mut stats = RequestStats::default();

    stats.total_trace_entries = trace.len();

    for entry in trace {
        let quantized = quantize_request(&entry, chunk_size);

        // Update statistics
        if quantized.len() == 1 {
            stats.single_chunk_requests += 1;
        } else {
            stats.multi_chunk_requests += 1;
            stats.max_chunks_spanned = stats.max_chunks_spanned.max(quantized.len());
        }

        // Count alignment warnings
        if !quantized.is_empty() && !quantized[0].warnings.is_empty() {
            stats.alignment_warnings += 1;
        }

        stats.total_get_requests += quantized.len();

        all_requests.push((entry, quantized));
    }

    (all_requests, stats)
}

fn print_statistics(stats: &RequestStats) {
    println!("\nStatistics:");
    println!("  Total trace entries: {}", stats.total_trace_entries);
    println!("  Total GET requests: {}", stats.total_get_requests);
    println!(
        "  Single-chunk requests: {} ({:.1}%)",
        stats.single_chunk_requests,
        (stats.single_chunk_requests as f64 / stats.total_trace_entries as f64) * 100.0
    );
    println!(
        "  Multi-chunk requests: {} ({:.1}%)",
        stats.multi_chunk_requests,
        (stats.multi_chunk_requests as f64 / stats.total_trace_entries as f64) * 100.0
    );
    println!("  Max chunks spanned: {}", stats.max_chunks_spanned);
    println!("  Alignment warnings: {}", stats.alignment_warnings);
}

fn run_dry_mode(
    requests: &[(TraceEntry, Vec<QuantizedRequest>)],
    max_requests: usize,
    verbose: bool,
) {
    println!("\n=== Dry Run: Quantized Requests ===\n");
    println!(
        "{:<8} {:<15} {:<3} {:<25} {:<12} {:<12}",
        "Index", "Original LBA", "→", "Key", "Offset", "Size"
    );
    println!("{:-<80}", "");

    let mut total_output = 0;
    let limit = if max_requests == 0 {
        usize::MAX
    } else {
        max_requests
    };

    for (entry, quantized) in requests {
        for qreq in quantized {
            if total_output >= limit {
                println!("\n... (remaining requests not shown)");
                return;
            }

            println!(
                "{:<8} {:<15} {:<3} {:<25} {:<12} {:<12}",
                total_output, entry.lba, "→", qreq.chunk_key, qreq.offset, qreq.size
            );

            if verbose && !qreq.warnings.is_empty() {
                for warning in &qreq.warnings {
                    println!("         WARNING: {}", warning);
                }
            }

            total_output += 1;
        }
    }
}

async fn run_real_mode(
    requests: Vec<(TraceEntry, Vec<QuantizedRequest>)>,
    socket: &str,
    num_clients: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Convert to GetRequests and validate
    let mut get_requests = Vec::new();

    for (_, quantized) in requests {
        for qreq in quantized {
            // Validate request size
            if qreq.size > MAX_FRAME_LENGTH as u64 {
                return Err(
                    format!("Request size {} exceeds MAX_FRAME_LENGTH (2GB)", qreq.size).into(),
                );
            }

            get_requests.push(GetRequest {
                key: qreq.chunk_key,
                offset: qreq.offset,
                size: qreq.size,
            });
        }
    }

    let nr_requests = get_requests.len();
    let requests = Arc::new(Mutex::new(get_requests));
    let counter = Arc::new(AtomicUsize::new(0));
    let step = if nr_requests >= 10 {
        nr_requests / 10
    } else {
        1
    };

    if num_clients == 0 {
        return Err("num_clients must be at least 1".into());
    }

    let mut handles: Vec<JoinHandle<tokio::io::Result<()>>> = Vec::new();
    let start = Instant::now();

    println!();

    for client_id in 0..num_clients {
        let sock = socket.to_string();
        let requests = Arc::clone(&requests);
        let counter = Arc::clone(&counter);

        handles.push(tokio::spawn(async move {
            // Connect to cache server
            let stream = UnixStream::connect(&sock).await?;
            println!("[Client {}] Connected to {}", client_id, sock);

            let (read_half, write_half) = tokio::io::split(stream);

            let codec = LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_LENGTH)
                .new_codec();

            let mut reader = FramedRead::new(read_half, codec.clone());
            let mut writer = FramedWrite::new(write_half, codec);

            loop {
                // Get next request
                let request: GetRequest;
                {
                    let mut reqs = requests.lock().await;
                    if reqs.is_empty() {
                        println!("[Client {}] Completed", client_id);
                        return Ok(());
                    }
                    request = reqs.pop().unwrap();
                }

                let req_num = counter.fetch_add(1, Ordering::Relaxed) + 1;

                // Progress reporting
                if step > 0 && req_num % step == 0 {
                    let percent = (req_num as f64 / nr_requests as f64) * 100.0;
                    println!(
                        "[Client {}] Progress: {}/{} ({:.0}%)",
                        client_id, req_num, nr_requests, percent
                    );
                }

                // Send request with timeout
                let encoded = bincode::serde::encode_to_vec(
                    Request::Get(request),
                    bincode::config::standard(),
                )
                .unwrap();

                match timeout(
                    Duration::from_secs(REQUEST_TIMEOUT_SECS),
                    writer.send(Bytes::from(encoded)),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        // Send successful
                    }
                    Ok(Err(e)) => {
                        return Err(std::io::Error::new(
                            ErrorKind::Other,
                            format!("[Client {}] Send error: {}", client_id, e),
                        ));
                    }
                    Err(_) => {
                        return Err(std::io::Error::new(
                            ErrorKind::TimedOut,
                            format!(
                                "[Client {}] Send timeout after {} seconds",
                                client_id, REQUEST_TIMEOUT_SECS
                            ),
                        ));
                    }
                }

                // Wait for response with timeout
                match timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS), reader.next()).await {
                    Ok(Some(Ok(frame))) => {
                        let bytes = frame.as_ref();
                        let msg: Result<(request::GetResponse, usize), DecodeError> =
                            bincode::serde::decode_from_slice(bytes, bincode::config::standard());

                        match msg.unwrap().0 {
                            request::GetResponse::Error(e) => {
                                return Err(std::io::Error::new(
                                    ErrorKind::Other,
                                    format!("[Client {}] Error: {}", client_id, e),
                                ));
                            }
                            request::GetResponse::Response(_) => {
                                // Success - continue
                            }
                        }
                    }
                    Ok(Some(Err(e))) => {
                        return Err(std::io::Error::new(
                            ErrorKind::Other,
                            format!("[Client {}] Frame read error: {}", client_id, e),
                        ));
                    }
                    Ok(None) => {
                        // Server closed connection gracefully - this is expected behavior
                        // when server reaches benchmark target and shuts down
                        let completed_reqs = counter.load(Ordering::Relaxed);
                        println!(
                            "[Client {}] Server closed connection gracefully after {} requests",
                            client_id, completed_reqs
                        );
                        println!(
                            "[Client {}] This is expected when server reaches benchmark target",
                            client_id
                        );
                        return Ok(());
                    }
                    Err(_) => {
                        return Err(std::io::Error::new(
                            ErrorKind::TimedOut,
                            format!(
                                "[Client {}] Receive timeout after {} seconds",
                                client_id, REQUEST_TIMEOUT_SECS
                            ),
                        ));
                    }
                }
            }
        }));
    }

    // Wait for all clients to complete
    let mut had_errors = false;
    for handle in handles {
        match handle.await {
            Err(join_err) => {
                eprintln!("Task panicked or was cancelled: {}", join_err);
                had_errors = true;
            }
            Ok(Err(io_err)) => {
                eprintln!("Task exited with error: {}", io_err);
                had_errors = true;
            }
            Ok(Ok(())) => {
                // Success
            }
        }
    }

    let duration = start.elapsed();
    let completed_requests = counter.load(Ordering::Relaxed);

    println!("\n=== Execution Complete ===");
    if completed_requests < nr_requests {
        println!(
            "NOTE: Server shutdown before all requests completed (expected for byte-target benchmarks)"
        );
        println!(
            "Completed {} of {} requests ({:.1}%)",
            completed_requests,
            nr_requests,
            (completed_requests as f64 / nr_requests as f64) * 100.0
        );
    } else {
        println!("Completed all {} requests", nr_requests);
    }
    println!("Clients: {}", num_clients);
    println!("Total time: {:.2?}", duration);
    println!(
        "Throughput: {:.2} req/sec",
        completed_requests as f64 / duration.as_secs_f64()
    );

    if had_errors {
        println!("One or more clients exited with errors");
        Ok(())
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    // Validate chunk size
    if args.chunk_size == 0 || args.chunk_size % ALIGNMENT != 0 {
        return Err("chunk_size must be non-zero and 4KB-aligned".into());
    }

    // Validate arguments based on mode
    if args.mode == "real" && args.socket.is_none() {
        return Err("--socket required for real mode".into());
    }

    // Parse trace file
    println!("Parsing trace file: {}", args.trace_file);
    let trace_entries = parse_trace(&args.trace_file)?;
    println!("Loaded {} trace entries", trace_entries.len());

    // Generate quantized requests
    println!("Quantizing with chunk_size={}", args.chunk_size);
    let (requests, stats) = generate_requests(trace_entries, args.chunk_size);

    // Print statistics
    print_statistics(&stats);

    // Execute based on mode
    match args.mode.as_str() {
        "dry" => {
            run_dry_mode(&requests, args.max_requests, args.verbose);
        }
        "real" => {
            let socket = args.socket.unwrap();
            run_real_mode(requests, &socket, args.num_clients).await?;
        }
        _ => {
            return Err(format!("Invalid mode: '{}' (must be 'dry' or 'real')", args.mode).into());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_to_4k() {
        assert_eq!(align_to_4k(0), (0, false));
        assert_eq!(align_to_4k(4096), (4096, false));
        assert_eq!(align_to_4k(100), (4096, true));
        assert_eq!(align_to_4k(8192), (8192, false));
        assert_eq!(align_to_4k(8193), (12288, true));
    }

    #[test]
    fn test_quantize_single_chunk() {
        let entry = TraceEntry {
            device: "dev0".to_string(),
            lba: 0,
            size: 4096,
            rw: "R".to_string(),
            timestamp: "0".to_string(),
        };

        let result = quantize_request(&entry, 65536);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].offset, 0);
        assert_eq!(result[0].size, 4096);
        assert_eq!(result[0].chunk_key, "chunk_0");
    }

    #[test]
    fn test_quantize_spanning_chunks() {
        let entry = TraceEntry {
            device: "dev0".to_string(),
            lba: 4096,
            size: 20480,
            rw: "R".to_string(),
            timestamp: "0".to_string(),
        };

        let result = quantize_request(&entry, 8192);
        assert_eq!(result.len(), 3);

        // First chunk: offset 4096, size 4096
        assert_eq!(result[0].chunk_key, "chunk_0");
        assert_eq!(result[0].offset, 4096);
        assert_eq!(result[0].size, 4096);

        // Second chunk: offset 0, size 8192
        assert_eq!(result[1].chunk_key, "chunk_1");
        assert_eq!(result[1].offset, 0);
        assert_eq!(result[1].size, 8192);

        // Third chunk: offset 0, size 8192
        assert_eq!(result[2].chunk_key, "chunk_2");
        assert_eq!(result[2].offset, 0);
        assert_eq!(result[2].size, 8192);
    }

    #[test]
    fn test_quantize_unaligned() {
        let entry = TraceEntry {
            device: "dev0".to_string(),
            lba: 100,
            size: 100,
            rw: "R".to_string(),
            timestamp: "0".to_string(),
        };

        let result = quantize_request(&entry, 8192);
        assert_eq!(result.len(), 1);
        assert!(!result[0].warnings.is_empty());
        // LBA 100 rounds up to 4096, size 100 rounds up to 4096
        assert_eq!(result[0].offset, 4096);
        assert_eq!(result[0].size, 4096);
    }

    #[test]
    fn test_quantize_chunk_boundary() {
        let entry = TraceEntry {
            device: "dev0".to_string(),
            lba: 8192,
            size: 16384,
            rw: "R".to_string(),
            timestamp: "0".to_string(),
        };

        let result = quantize_request(&entry, 8192);
        assert_eq!(result.len(), 2);

        // First chunk at boundary
        assert_eq!(result[0].chunk_key, "chunk_1");
        assert_eq!(result[0].offset, 0);
        assert_eq!(result[0].size, 8192);

        // Second chunk
        assert_eq!(result[1].chunk_key, "chunk_2");
        assert_eq!(result[1].offset, 0);
        assert_eq!(result[1].size, 8192);
    }
}
