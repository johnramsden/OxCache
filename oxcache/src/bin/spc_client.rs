use bincode::error::DecodeError;
use bytes::Bytes;
use clap::Parser;
use csv::ReaderBuilder;
use futures::{SinkExt, StreamExt};
use intervaltree::{Element, IntervalTree};
use oxcache::request;
use oxcache::request::{GetRequest, Request};
use std::io::ErrorKind;
use std::ops::Range;
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
    spc_file_location: String,

    // Number of bytes in a sector
    #[arg(long)]
    sector_size: u64,

    #[arg(long)]
    use_timestamps: bool,
}

const MAX_FRAME_LENGTH: usize = 2 * 1024 * 1024 * 1024; // 2 GB

#[derive(Debug, Clone)]
pub struct SPCRecord {
    pub address: u64,
    pub operation: SPCOperation,
    pub timestamp: f64,
    pub bytes_written: u64,
}

impl SPCRecord {
    pub fn sectors(&self, sector_size: u64) -> u64 {
        self.bytes_written / sector_size
    }

    pub fn end_address(&self, sector_size: u64) -> u64 {
        self.address + self.bytes_written / sector_size
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SPCOperation {
    Read,
    Write,
}

#[derive(Debug, Clone)]
pub struct SPCTrace {
    pub records: Vec<SPCRecord>,
    pub total_records: usize,
}

#[derive(Debug, Clone)]
pub struct UniqueObject {
    pub address: u64,
    pub extent_sectors: u64,
    pub hash: Uuid,
}

impl UniqueObject {
    pub fn new(rec: SPCRecord, sector_size: u64) -> Self {
        Self {
            address: rec.address,
            extent_sectors: rec.bytes_written / sector_size,
            hash: Uuid::new_v4(),
        }
    }

    pub fn end_address(&self) -> u64 {
        self.address + self.extent_sectors
    }
}

impl From<UniqueObject> for Element<u64, UniqueObject> {
    fn from(object: UniqueObject) -> Self {
        Self {
            range: Range {
                start: object.address,
                end: object.address + object.extent_sectors,
            },
            value: object,
        }
    }
}

impl SPCTrace {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            total_records: 0,
        }
    }

    pub fn add_record(&mut self, record: SPCRecord) {
        self.records.push(record);
    }

    pub fn generate_requests(&self, sector_size: u64) -> Vec<GetRequest> {
        let mut objects = self.records.clone();
        println!("Sorting objects");
        objects.sort_by(|a, b| a.address.cmp(&b.address));

        println!("Finding unique objects");
        let objects = objects.into_iter().fold(
            Vec::new(),
            |mut objects: Vec<UniqueObject>, rec| match objects.last_mut() {
                Some(prev) => {
                    if rec.bytes_written % sector_size != 0 {
                        panic!("Byte extent is not a multiple of sector size")
                    }
                    if rec.address < prev.end_address() {
                        prev.extent_sectors = std::cmp::max(
                            rec.end_address(sector_size) - prev.address,
                            prev.extent_sectors,
                        )
                    } else {
                        objects.push(UniqueObject::new(rec, sector_size))
                    }
                    objects
                }
                None => {
                    objects.push(UniqueObject::new(rec, sector_size));
                    objects
                }
            },
        );
        println!("Number of unique objects: {}", objects.len());

        println!("Building interval tree");
        let obj_query = IntervalTree::from_iter(objects.into_iter());

        println!("Generating requests");
        self.records.iter().map(|rec|{
            let unique_object = obj_query.query_point(rec.address).collect::<Vec<_>>();
            let object = match unique_object.len() {
                1 => &unique_object[0].value,
                _ => {
                    panic!("Overlapping intervals or invalid request: {} unique objects: {:#?}. Address is {}", unique_object.len(), unique_object, rec.address)
                }
            };

            GetRequest {
                key: object.hash.to_string(),
                offset: (rec.address - object.address) * sector_size,
                size: rec.bytes_written,
            }
        }).collect()
    }
}

pub fn parse_spc_trace(path: &String) -> std::io::Result<SPCTrace> {
    println!("Reading spc trace from file {}", path);
    let mut trace = SPCTrace::new();

    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true) // Optional description field
        .trim(csv::Trim::All) // Trim whitespace
        .from_path(path)?;

    println!("Parsing");
    for result in rdr.records() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                panic!("Couldn't parse CSV record: {}", e)
            }
        };

        if let (Ok(_), Ok(address), Ok(bytes_written), Ok(op_str), Ok(timestamp)) = (
            record[0].parse::<u32>(),
            record[1].parse::<u64>(),
            record[2].parse::<u64>(),
            record[3].parse::<String>(),
            record[4].parse::<f64>(),
        ) {
            let operation = match op_str.as_str() {
                "R" => SPCOperation::Read,
                "W" => SPCOperation::Write,
                _ => panic!("Unknown operation '{}'", op_str),
            };
            trace.add_record(SPCRecord {
                address,
                operation,
                timestamp: timestamp,
                bytes_written: bytes_written,
            });
        } else {
            panic!("Bad record")
        }
    }
    println!("Total traces: {}", trace.records.len());

    Ok(trace)
}

// Generate readable file with request details
#[allow(dead_code)]
pub fn write_to_file(
    requests: Vec<GetRequest>,
    sector_size: u64,
    file_location: &String,
) -> Result<(), Box<dyn std::error::Error>> {
    let readable_file = "requests_readable.txt";
    let mut readable_content = String::new();
    readable_content.push_str("# SPC Requests Analysis\n");
    readable_content.push_str(&format!("Total requests: {}\n", requests.len()));
    readable_content.push_str(&format!("Sector size: {} bytes\n", sector_size));
    readable_content.push_str(&format!("SPC trace file: {}\n", file_location));
    readable_content.push_str("--------------------------------------------------\n\n");

    for (i, request) in requests.iter().enumerate() {
        readable_content.push_str(&format!("Request #{}\n", i + 1));
        readable_content.push_str(&format!("  Key: {}\n", request.key));
        readable_content.push_str(&format!("  Offset: {} bytes\n", request.offset));
        readable_content.push_str(&format!("  Size: {} bytes\n", request.size));
        readable_content.push_str(&format!("  Sectors: {}\n", request.size / sector_size));
        readable_content.push_str("\n");
    }

    std::fs::write(readable_file, readable_content).map_err(|e| {
        format!(
            "Failed to write readable requests to {}: {}",
            readable_file, e
        )
    })?;

    println!("Generated readable requests file: {}", readable_file);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let trace = parse_spc_trace(&args.spc_file_location)?;
    let requests = trace.generate_requests(args.sector_size);

    let nr_queries = requests.len();
    let queries: Arc<Mutex<Vec<GetRequest>>> = Arc::new(Mutex::new(requests));

    let counter = Arc::new(AtomicUsize::new(0));
    let step = nr_queries / 10; // 10%

    if args.num_clients <= 0 {
        return Err(format!("num_clients={} must be at least 1", args.num_clients).into());
    }

    let mut handles: Vec<JoinHandle<std::io::Result<()>>> = Vec::new();

    let start = Instant::now();

    for c in 0..args.num_clients {
        let sock = args.socket.clone();
        let queries = Arc::clone(&queries);
        let counter = Arc::clone(&counter);
        handles.push(tokio::spawn(async move {
            let stream = UnixStream::connect(&sock).await?;
            println!("[t.{}] Client connected to {}", c, sock);

            let (read_half, write_half) = split(stream);

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
        let _ = handle.await?;
    }

    let duration = start.elapsed(); // Stop the timer

    println!(
        "Executed {} queries across {} clients",
        nr_queries, args.num_clients
    );
    println!("Total run time: {:.2?}", duration);

    Ok(())
}
