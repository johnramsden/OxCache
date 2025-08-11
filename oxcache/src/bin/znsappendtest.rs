use clap::Parser;
use nvme::types::PerformOn;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[arg(long)]
    device: String,

    #[arg(long, default_value_t = 14)]
    concurrency: usize,
}

/// Tests throughput for multi-threaded ZNS append

fn main() -> std::io::Result<()> {
    let args = Cli::parse();
    let _concurrency = args.concurrency.min(14); // clamp to 14

    let nvme_config = Arc::new(match nvme::info::nvme_get_info(args.device.as_str()) {
        Ok(config) => config,
        Err(err) => return Err(err.try_into().unwrap()),
    });

    let zc = Arc::new(match nvme::info::zns_get_info(&nvme_config) {
        Ok(config) => config,
        Err(err) => return Err(std::io::Error::new(ErrorKind::Other, err)),
    });

    nvme::ops::reset_zone(&nvme_config, &zc, PerformOn::AllZones).unwrap();

    let write_total = 1077 * 1024 * 1024;
    let write_sz = 256 * 1024;
    let data: Arc<Vec<u8>> = Arc::new(vec![0; write_total]);

    let zones_written = 100;
    let zone_counter = Arc::new(AtomicUsize::new(0));
    let concurrency = args.concurrency.min(14);
    let mut handles = vec![];

    let start = Instant::now();

    for thread_id in 0..concurrency {
        let nvme_config = nvme_config.clone();
        let zc = zc.clone();
        let data = Arc::clone(&data);
        let zone_counter = Arc::clone(&zone_counter);

        let handle = thread::spawn(move || {
            loop {
                let zone = zone_counter.fetch_add(1, Ordering::SeqCst) as u64;
                if zone >= zones_written {
                    break;
                }

                let mut byte_ind = 0;
                while byte_ind < data.len() {
                    let end = (byte_ind + write_sz).min(data.len());
                    match nvme::ops::zns_append(&nvme_config, &zc, zone, &data[byte_ind..end]) {
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!(
                                "Thread {}: Failed at zone {} offset {}: {}",
                                thread_id, zone, byte_ind, err
                            );
                            return Err(std::io::Error::new(ErrorKind::Other, err));
                        }
                    }
                    byte_ind += write_sz;
                }
                println!("Thread {}: Completed zone {}", thread_id, zone);
            }
            Ok::<_, std::io::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        if let Err(e) = handle.join().expect("Thread panicked") {
            return Err(e);
        }
    }
    let duration = start.elapsed();

    println!("Elapsed: {:.3} seconds", duration.as_secs_f64());

    let gib: f64 = (write_total as f64 * zones_written as f64) / (1024.0 * 1024.0 * 1024.0);
    println!(
        "Wrote {:.3} GiB ({:.3} GiB/s)",
        gib,
        gib / duration.as_secs_f64()
    );

    Ok(())
}
