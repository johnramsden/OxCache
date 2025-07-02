use nvme::info::zns_get_info;
use nvme::ops::{reset_zone, zns_append, zns_open, zns_read};
use nvme::types::ZNSConfig;
use std::path::PathBuf;
use std::{fs::File, io::Read};
use toml::{self, Table};

#[test]
fn init() {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("tests/test-config.toml");
    let mut file = File::open(&d).expect(format!("Failed to open config file: {:?}", &d).as_str());
    let mut buffer = String::new();
    file.read_to_string(&mut buffer)
        .expect("Failed to read test config file");

    let table = buffer.parse::<Table>().unwrap();
    let zns_device = table["zns"].as_str().unwrap();

    let config = zns_get_info(zns_device).expect("Couldn't open device");

    test_config(&config);

    test_append(&config);
    test_append_multiple_zones(&config);
    test_append_offset(&config);
    test_reset_zone(&config);
}

fn test_config(config: &ZNSConfig) {
    println!("{:?}", config);
}

fn test_append(config: &ZNSConfig) {
    reset_zone(config, nvme::types::PerformOn::AllZones).expect("Failed to reset zones");

    let string = "hello world. this is a test\n".as_bytes();
    let mut buf = vec![0_u8; config.block_size as usize];
    buf[..string.len()].clone_from_slice(&string);

    zns_append(config, 0, &mut buf).expect("Failed to append");
    zns_append(config, 0, &mut buf).expect("Failed to append");
    zns_append(config, 0, &mut buf).expect("Failed to append");

    let mut read_buf = vec![0_u8; config.block_size as usize];

    zns_read(config, 0, 0, &mut read_buf).expect("Failed to read");

    assert_eq!(buf, read_buf);
}

fn test_append_multiple_zones(config: &ZNSConfig) {
    reset_zone(config, nvme::types::PerformOn::AllZones).expect("Failed to reset zones");

    let string = "hello world. this is a test\n".as_bytes();
    let mut buf = vec![0_u8; config.block_size as usize];
    buf[..string.len()].clone_from_slice(&string);

    zns_append(config, 0, &mut buf).expect("Failed to append");
    zns_append(config, 1, &mut buf).expect("Failed to append");
    zns_append(config, 2, &mut buf).expect("Failed to append");

    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 0, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 1, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 2, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);
}

fn test_append_offset(config: &ZNSConfig) {
    reset_zone(config, nvme::types::PerformOn::AllZones).expect("Failed to reset zones");

    let string = "hello world. this is a test\n".as_bytes();
    let mut buf = vec![0_u8; config.block_size as usize];
    buf[..string.len()].clone_from_slice(&string);

    assert_eq!(zns_append(config, 0, &mut buf).expect("Failed to append"), 0);
    assert_eq!(zns_append(config, 0, &mut buf).expect("Failed to append"), 1);
    assert_eq!(zns_append(config, 0, &mut buf).expect("Failed to append"), 2);

    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 0, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 0, 1, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 0, 2, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);
}

fn test_reset_zone(config: &ZNSConfig) {
    reset_zone(config, nvme::types::PerformOn::AllZones).expect("Failed to reset all zones");

    let string = "hello world. this is a test\n".as_bytes();
    let mut buf = vec![0_u8; config.block_size as usize];
    buf[..string.len()].clone_from_slice(&string);

    zns_append(config, 0, &mut buf).expect("Failed to append");
    zns_append(config, 1, &mut buf).expect("Failed to append");
    zns_append(config, 2, &mut buf).expect("Failed to append");

    reset_zone(config, nvme::types::PerformOn::Zone(0)).expect("Failed to reset zone 1");    
    reset_zone(config, nvme::types::PerformOn::Zone(2)).expect("Failed to reset zone 2");    

    let empty_buf = vec![0_u8; config.block_size as usize];
    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 0, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(empty_buf, read_buf);

    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 1, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; config.block_size as usize];
    zns_read(config, 2, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(empty_buf, read_buf);
}
