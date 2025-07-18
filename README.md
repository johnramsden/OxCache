# OxCache

Disk based cache with ZNS and block-interface backends.

## Dependencies

Rust

## Running Server

WARNING, DESTRUCTIVE TO DISK USED, WIPES DATA

See options via:

```
oxcache --help
```

Set config via CLI or toml file. See [example.server.toml](example.server.toml)

EG:

```
cargo build --package oxcache --bin oxcache
./target/debug/oxcache --config ./cortes.server.zns.toml
```

## Running Clients

The sample clients currently exist:

* Simple single threaded test - [client.rs](oxcache/src/bin/client.rs)
* Multithreaded test accepting a file input - [evaluationclient.rs](oxcache/src/bin/evaluationclient.rs)
* Simple mltithreaded test - [simpleevaluationclient.rs](oxcache/src/bin/simpleevaluationclient.rs)

To run:

```
CLIENT -- ARGS
```

EG:

```
cargo build --package oxcache --bin simpleevaluationclient
./target/debug/simpleevaluationclient --help
./target/debug/simpleevaluationclient --socket /tmp/oxcache.sock --num-clients 32
```