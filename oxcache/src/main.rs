use libnvme_sys;
use nvme;

fn main() {
	let fd = unsafe{libnvme_sys::nvme_open("/dev/nvme1n1".as_ptr())};
	let fd2 = unsafe{nvme::nvme_open("/dev/nvme1n1".as_ptr())};
	println!("fd: {}", fd);
	println!("fd2: {}", fd2);
}
