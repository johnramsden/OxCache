use nvme;

fn main() {
	let fd = unsafe{nvme::nvme_open("/dev/nvme1n1".as_ptr())};
	println!("fd: {}", fd);
}
