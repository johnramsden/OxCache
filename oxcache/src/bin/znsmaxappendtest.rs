use std::io::ErrorKind;
use clap::Parser;
use nvme::types::PerformOn;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[arg(long)]
    device: String,
}

/// Tests size where appends begin to fail

fn main() -> std::io::Result<()> {
    let args = Cli::parse();
    let nvme_config = match nvme::info::nvme_get_info(args.device.as_str()) {
        Ok(config) => config,
        Err(err) => return Err(err.try_into().unwrap()),
    };

    let zc = match nvme::info::zns_get_info(&nvme_config) {
        Ok(config) => {
            config
        }
        Err(err) => return Err(std::io::Error::new(ErrorKind::Other, err)),
    };
    
    let stop = 1024*1024*1024;
    
    let mut start = 64*1024;

    loop {
        nvme::ops::reset_zone(&nvme_config, &zc, PerformOn::AllZones).unwrap();
        let data: Vec<u8> = vec![0; start];
        match nvme::ops::zns_append(&nvme_config, &zc, 0, data.as_slice()) {
            Ok(_res) => {
                println!("Wrote {} bytes", start);
            }
            Err(err) => {
                println!("Failed to write {} bytes", start);
                return Err(std::io::Error::new(ErrorKind::Other, err));
            }
        }
        start *= 2;
        if start >= stop {
            break;
        }
    }

    Ok(())
}
