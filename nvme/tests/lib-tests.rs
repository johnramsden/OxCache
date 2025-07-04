use nvme::info::{nvme_get_info, zns_get_info};
use nvme::ops::{open_device, reset_zone, zns_append, zns_read};
use nvme::types::{NVMeConfig, ZNSConfig};
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

    let nvme_config = nvme_get_info(zns_device).expect("Couldn't open NVMe device");
    let config = zns_get_info(&nvme_config).expect("Couldn't open ZNS device");

    test_config(&nvme_config, &config);
    test_append(&nvme_config, &config);
    test_append_multiple_zones(&nvme_config, &config);
    test_append_offset(&nvme_config, &config);
    test_reset_zone(&nvme_config, &config);
}

fn test_config(nvme_config: &NVMeConfig, config: &ZNSConfig) {
    println!("{:?}", nvme_config);
    println!("{:?}", config);
}

fn test_append(nvme_config: &NVMeConfig, config: &ZNSConfig) {
    reset_zone(nvme_config, config, nvme::types::PerformOn::AllZones)
        .expect("Failed to reset zones");

    let string = "hello world. this is a test\n".as_bytes();
    let mut buf = vec![0_u8; nvme_config.logical_block_size as usize];
    buf[..string.len()].clone_from_slice(&string);

    zns_append(nvme_config, config, 0, &mut buf).expect("Failed to append");
    zns_append(nvme_config, config, 0, &mut buf).expect("Failed to append");
    zns_append(nvme_config, config, 0, &mut buf).expect("Failed to append");

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];

    zns_read(nvme_config, config, 0, 0, &mut read_buf).expect("Failed to read");

    assert_eq!(buf, read_buf);
}

fn test_append_multiple_zones(nvme_config: &NVMeConfig, config: &ZNSConfig) {
    reset_zone(nvme_config, config, nvme::types::PerformOn::AllZones)
        .expect("Failed to reset zones");

    let string = "hello world. this is a test\n".as_bytes();
    let mut buf = vec![0_u8; nvme_config.logical_block_size as usize];
    buf[..string.len()].clone_from_slice(&string);

    zns_append(nvme_config, config, 0, &mut buf).expect("Failed to append");
    zns_append(nvme_config, config, 1, &mut buf).expect("Failed to append");
    zns_append(nvme_config, config, 2, &mut buf).expect("Failed to append");

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 0, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 1, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 2, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);
}

fn test_append_offset(nvme_config: &NVMeConfig, config: &ZNSConfig) {
    reset_zone(nvme_config, config, nvme::types::PerformOn::AllZones)
        .expect("Failed to reset zones");

    let string = "hello world. this is a test\n".as_bytes();
    let mut buf = vec![0_u8; nvme_config.logical_block_size as usize];
    buf[..string.len()].clone_from_slice(&string);

    assert_eq!(
        zns_append(nvme_config, config, 0, &mut buf).expect("Failed to append"),
        0
    );
    assert_eq!(
        zns_append(nvme_config, config, 0, &mut buf).expect("Failed to append"),
        1
    );
    assert_eq!(
        zns_append(nvme_config, config, 0, &mut buf).expect("Failed to append"),
        2
    );

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 0, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 0, 1, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 0, 2, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);
}

fn test_reset_zone(nvme_config: &NVMeConfig, config: &ZNSConfig) {
    reset_zone(nvme_config, config, nvme::types::PerformOn::AllZones)
        .expect("Failed to reset all zones");

    let string = "hello world. this is a test\n".as_bytes();
    let mut buf = vec![0_u8; nvme_config.logical_block_size as usize];
    buf[..string.len()].clone_from_slice(&string);

    zns_append(nvme_config, config, 0, &mut buf).expect("Failed to append");
    zns_append(nvme_config, config, 1, &mut buf).expect("Failed to append");
    zns_append(nvme_config, config, 2, &mut buf).expect("Failed to append");

    reset_zone(nvme_config, config, nvme::types::PerformOn::Zone(0))
        .expect("Failed to reset zone 1");
    reset_zone(nvme_config, config, nvme::types::PerformOn::Zone(2))
        .expect("Failed to reset zone 2");

    let empty_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 0, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(empty_buf, read_buf);

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 1, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(buf, read_buf);

    let mut read_buf = vec![0_u8; nvme_config.logical_block_size as usize];
    zns_read(nvme_config, config, 2, 0, &mut read_buf).expect("Failed to read");
    assert_eq!(empty_buf, read_buf);
}
