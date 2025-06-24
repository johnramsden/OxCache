use errno::errno;
use libnvme_sys::bindings::nvme_cmd_dword_fields_NVME_DEVICE_SELF_TEST_CDW10_STC_MASK;
use nvme::{
    self, ZoneState, get_error_string, get_nsid, open_zone, report_zones, report_zones_all,
    reset_zone, zns_append, zns_open,
};
use serde::Deserialize;
use serial_test::serial;
use std::ffi::{CStr, CString};
use std::io::copy;
use std::path::PathBuf;
use std::{fs::File, io::Read, os::fd::RawFd};
use toml::{self, Value};

#[derive(Debug)]
struct Config {
    device_name: String,
    file_desc: RawFd,
    nsid: u32,
}

#[derive(Debug, Deserialize)]
struct ReadConfig {
    zns: String,
}

fn init() -> ReadConfig {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("tests/test-config.toml");
    let mut file = File::open(&d).expect(format!("Failed to open config file: {:?}", &d).as_str());
    let mut buffer = String::new();
    file.read_to_string(&mut buffer)
        .expect("Failed to read test config file");
    toml::from_str(buffer.as_str()).expect("Couldn't deserialize")
}

fn init_zns() -> Config {
    let cfg = init();
    Config {
        device_name: cfg.zns.clone(),
        file_desc: zns_open(&cfg.zns).expect("Failed to open nvme device"),
        nsid: get_nsid(cfg.zns.as_str()).expect("Couldn't parse namespace ID"),
    }
}

#[test]
#[serial]
fn test_zns_open() {
    let config = init();
    if zns_open(&config.zns).is_err() {
        let error_number = errno();
        assert!(false, "Failed to open. Errno is {error_number}");
    }
}

#[test]
#[serial]
fn test_get_nsid() {
    let device_name = "nvme9n1";
    assert_eq!(get_nsid(device_name), Ok(1));

    let device_name2 = "nvme0n10";
    assert_eq!(get_nsid(device_name2), Ok(10));
}

#[test]
#[serial]
fn test_report_all_zones() {
    let config = init_zns();

    reset_zone(config.file_desc, 0, 0, 0, config.nsid, true).expect("Failed to reset zones");

    match report_zones_all(config.file_desc, config.nsid) {
        Ok((nz, descriptors)) => {
            assert_eq!(nz, 256);
            for (i, descriptor) in descriptors.iter().enumerate() {
                assert_eq!(descriptor.zone_state, ZoneState::Empty, "Failed at {}", i);
            }
        }
        Err(nvme_status_field) => {
            assert!(false, "{}", get_error_string(nvme_status_field))
        }
    }

    // For some reason, this does NOT explicitly open the zone... Need to figure out why

    open_zone(config.file_desc, 0, 0, 3000, config.nsid, true).expect("Failed to open zones");

    // match report_zones_all(config.file_desc, config.nsid) {
    //     Ok((nz, descriptors)) => {
    //         assert_eq!(nz, 256);
    //         for (i, descriptor) in descriptors.iter().enumerate() {
    //             assert_eq!(descriptor.zone_state, ZoneState::ExplicitlyOpened, "Failed at {}", i);
    //         }

    //     },
    //     Err(nvme_status_field) => {
    //         assert!(false, "{}", get_error_string(nvme_status_field))
    //     },
    // }
}

#[test]
#[serial]
fn test_zns_reset() {
    let config = init_zns();
    reset_zone(config.file_desc, 0, 0, 0, config.nsid, true).expect("Failed to reset zones");
    match report_zones_all(config.file_desc, config.nsid) {
        Ok((nz, descriptors)) => {
            assert_eq!(nz, 256);
            for (i, descriptor) in descriptors.iter().enumerate() {
                assert_eq!(descriptor.zone_state, ZoneState::Empty, "Failed at {}", i);
            }
        }
        Err(nvme_status_field) => {
            assert!(false, "{}", get_error_string(nvme_status_field))
        }
    }
}

#[test]
#[serial]
fn test_zns_append() {
    let config = init_zns();
    reset_zone(config.file_desc, 0, 0, 0, config.nsid, true).unwrap();
    let text = "hello world";
    let ctext = CString::new(text).unwrap();
    let mut buffer = ctext.as_bytes().to_vec();

    match zns_append(config.file_desc, 0, &mut buffer, 0, config.nsid, 4096) {
        Ok(_) => {
            assert!(true);
        }
        Err(err) => assert!(false, "{}", get_error_string(err)),
    }
}
