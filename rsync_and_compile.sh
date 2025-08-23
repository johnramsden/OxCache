#!/bin/bash

kill_oxcache() {
    sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 << 'EOF'
kill -9 $(pidof oxcache)
kill -9 $(pidof simpleevaluationclient)
EOF

}

trap kill_oxcache INT

rsync -avP \
      --delete \
      Cargo.lock \
      Cargo.toml \
      example.server.toml \
      'libnvme-sys' \
      'nvme' \
      'oxcache' \
      ubuntu@127.0.0.1:/home/ubuntu/OxCacheLocal/

sshpass -p "ubuntu" ssh -o StrictHostKeyChecking=no -p 2222 ubuntu@127.0.0.1 << 'EOF'
cd /home/ubuntu/OxCacheLocal/
cargo build

RUST_BACKTRACE=1 cargo test

# cargo run --bin oxcache -- --config ./example.server.toml &
# sleep 3
# cargo run --bin simpleevaluationclient -- --socket /tmp/oxcache.sock --num-clients 10 --query-size 8192
EOF

