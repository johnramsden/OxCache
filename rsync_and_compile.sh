rsync -avP \
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
cargo run --bin oxcache -- --config example.server.toml &
SERVER_PID=$!
sleep 5
cargo run --bin client -- --socket /tmp/oxcache.sock

kill -9 $SERVER_PID
kill -9 $CLIENT_PID

trap 'finish' INT
EOF

