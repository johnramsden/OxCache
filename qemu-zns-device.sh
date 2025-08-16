#!/bin/sh

print_usage() {
    echo "Usage: $0 --sector-size <sector size (B)> --zone-size <zone size (MB)> \\"
    echo "          --zone-capacity <zone capacity (MB)> --disk-size <disk size (GB)> \\"
    echo "          --max-open <num> --max-active <num> --zasl <num> --img <dir> --cores <num> \\"
    echo "          --host-dir <dir>"
    echo
    echo "  --sector-size     Sector size in bytes (default: 4096)"
    echo "  --zone-size       Size of each zone in megabytes (default: 128)"
    echo "  --zone-capacity   Usable capacity per zone in megabytes. 0 for zone size (default: 0)"
    echo "  --disk-size       Total disk size in gigabytes (default: 32)"
    echo "  --max-open        Maximum number of concurrently open zones (default: 14)"
    echo "  --max-active      Maximum number of concurrently active zones (default: 14)"
    echo "  --zasl            Zone append size limit (ZASL), (2 * zasl * sector-size) bytes is the max append size (default: 5)"
    echo "  --img             Location of QEMU image (default: downloads Ubuntu 24.04)"
    echo "  --cores           How many cores to run the VM with (default: nproc)"
    echo "  --host-dir        Directory where the repository lives on the host"
    echo "  -h, --help        Show this help message and exit"
}

sudo -v

if [[ "$EUID" -eq 0 ]]; then
    echo "This script should not be run as root"
    exit 1
fi

# Default values
sector_size=4096
zone_size=128
zone_capacity=0
disk_size=1 # Gigabyte
max_open=3
max_active=3
zasl=5
img=""
hostdir=""
cores=$(nproc)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sector-size) sector_size="$2"; shift 2 ;;
        --zone-size) zone_size="$2"; shift 2 ;;
        --zone-capacity) zone_capacity="$2"; shift 2 ;;
        --disk-size) disk_size="$2"; shift 2 ;;
        --max-open) max_open="$2"; shift 2 ;;
        --max-active) max_active="$2"; shift 2 ;;
        --zasl) zasl="$2"; shift 2 ;;
        --img) img="$2"; shift 2 ;;
        --cores) cores="$2"; shift 2 ;;
        --host-dir) hostdir="$2"; shift 2 ;;
        -h|--help) print_usage; exit 0 ;;
        *) echo "Unknown option: $1"; print_usage; exit 1 ;;
    esac
done

if [ -z "$hostdir" ]; then
    print_usage
    exit 1
fi

# Get the Ubuntu guest VM image, and resize
if [ -z "$img" ]; then
    img=/tmp/ubuntu.qcow2
    wget -O "$img" https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img
fi

sudo qemu-img resize "$img" 32G

# Make the raw disk images
# znsimg=/var/lib/qemu/images/zns.raw
znsimg=/mnt/zns.raw
sudo mkdir -p "$(dirname ${znsimg})"
sudo touch ${znsimg}
sudo truncate -s "${disk_size}"G ${znsimg}

# ssdimg=/var/lib/qemu/images/ssd.raw
ssdimg=/mnt/ssd.raw
sudo mkdir -p "$(dirname ${ssdimg})"
sudo touch ${ssdimg}
sudo truncate -s "${disk_size}"G ${ssdimg}

# Make cloud-init image
cat > user-data <<EOF
#cloud-config
password: ubuntu
chpasswd: { expire: False }
ssh_pwauth: True
EOF

cloud-localds /tmp/cloud-init.iso user-data

# Run the QEMU vm
sudo qemu-system-x86_64 \
     -enable-kvm \
     -m 12G \
     -smp "$cores" \
     -cpu host \
     -drive file="$img",format=qcow2,if=virtio \
     -drive file=/tmp/cloud-init.iso,format=raw,if=virtio \
     -net user,hostfwd=tcp::2222-:22 \
     -net nic \
     -device nvme,id=nvme0,serial=deadbeef,zoned.zasl="${zasl}" \
     -drive file="${znsimg}",id=nvmezns0,format=raw,if=none \
     -device nvme-ns,drive=nvmezns0,bus=nvme0,nsid=1,logical_block_size="${sector_size}",\
physical_block_size="${sector_size}",zoned=true,zoned.zone_size="${zone_size}"M,zoned.zone_capacity="${zone_capacity}"M,\
zoned.max_open="${max_open}",zoned.max_active="${max_active}",\
uuid=5e40ec5f-eeb6-4317-bc5e-c919796a5f79 \
     -device nvme,id=nvme1,serial=deadbaaf \
     -drive file="${ssdimg}",id=nvmessd1,format=raw,if=none \
     -device nvme-ns,drive=nvmessd1,bus=nvme1,nsid=1,logical_block_size="${sector_size}",\
physical_block_size="${sector_size}",zoned=false,uuid=5e40ec5f-eeb6-4317-bc5e-c919796a5f7a \
     -nographic \
     -virtfs local,path="$hostdir",mount_tag=hostshare,security_model=mapped-xattr,id=share0 &

echo "PID of qemu program: "$!

# Adjust if needed
sleep 18

ssh-keygen -R "[127.0.0.1]:2222"

# shellcheck disable=SC2016
sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 'sudo apt update && sudo apt install -y \
            linux-modules-extra-$(uname -r) \
            build-essential \
            git \
	    nvme-cli \
	    ninja-build \
	    meson \
	    libclang-dev'

sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 'nvme list'

sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 << 'EOF'
curl https://sh.rustup.rs -sSf | sh -s -- -y
. "$HOME/.cargo/env"
rustup component add rust-analyzer
sudo ln -s ~/.cargo/bin/* /usr/bin/
mkdir /home/ubuntu/OxCache
mount -t 9p -o trans=virtio hostshare /home/ubuntu/OxCache
# git clone --recurse-submodules https://github.com/johnramsden/OxCache.git
cd OxCache
cargo clean
cargo build
EOF
