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
EOF

