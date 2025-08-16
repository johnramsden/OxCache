#!/bin/bash

kill_oxcache() {
    sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 << 'EOF'
sudo kill -9 $(pidof oxcache)
sudo kill -9 $(pidof simpleevaluationclient)
EOF

}

trap kill_oxcache INT


rsync -avP \
      Cargo.lock \
      Cargo.toml \
      example.server.toml \
      qemu.zns.toml \
      'libnvme-sys' \
      'nvme' \
      'oxcache' \
      ubuntu@127.0.0.1:/home/ubuntu/OxCacheLocal/

sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 << 'EOF'
cd /home/ubuntu/OxCacheLocal/
cargo build

RUST_BACKTRACE=1 cargo run --bin oxcache -- --config ./qemu.zns.toml &
sleep 3
RUST_BACKTRACE=1 cargo run --bin simpleevaluationclient -- --socket /tmp/oxcache.sock --num-clients 32 --query-size 67108864
EOF

