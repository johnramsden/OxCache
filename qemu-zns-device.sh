#!/bin/sh

# Get the Ubuntu guest VM image, and resize
wget -v -O /tmp/ubuntu.qcow2 https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img -o /dev/null
qemu-img resize /tmp/ubuntu.qcow2 12G

# Make the raw disk image
znsimg=/var/lib/qemu/images/zns.raw
mkdir -p "$(dirname ${znsimg})"
touch ${znsimg}
truncate -s 32G ${znsimg}

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
     -smp 2 \
     -cpu host \
     -drive file=/tmp/ubuntu.qcow2,format=qcow2,if=virtio \
     -drive file=/tmp/cloud-init.iso,format=raw,if=virtio \
     -net user,hostfwd=tcp::2222-:22 \
     -net nic \
     -device nvme,id=nvme0,serial=deadbeef,zoned.zasl=5 \
     -drive file=${znsimg},id=nvmezns0,format=raw,if=none \
     -device nvme-ns,drive=nvmezns0,bus=nvme0,nsid=1,logical_block_size=4096,\
physical_block_size=4096,zoned=true,zoned.zone_size=64M,zoned.zone_capacity=62M,\
zoned.max_open=16,zoned.max_active=32,\
uuid=5e40ec5f-eeb6-4317-bc5e-c919796a5f79 \
     -nographic &

echo "PID of qemu program: "$!

sleep 60

# shellcheck disable=SC2016
sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 'sudo apt update && sudo apt install -y \
            linux-modules-extra-$(uname -r) \
            build-essential \
            git'

sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 << 'EOF'
curl https://sh.rustup.rs -sSf | sh -s -- -y
. "$HOME/.cargo/env"
git clone --recurse-submodules git@github.com:johnramsden/OxCache.git
cd OxCache
cargo build
EOF
