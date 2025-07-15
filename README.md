# OxCache

Disk based cache with ZNS and block-interface backends.

## Dependencies

Rust

## Running Server

WARNING, DESTRUCTIVE TO DISK USED, WIPES DATA

See options via:

```
cargo run --package oxcache --bin oxcache -- --help
```

Set config via CLI or toml file. See [example.server.toml](example.server.toml)

EG:

```
cargo run --package oxcache --bin oxcache -- -- --config ../example.server.toml
```

## Running Clients

The sample clients currently exist:

* Simple single threaded test - [client.rs](oxcache/src/bin/client.rs)
* Multithreaded test accepting a file input - [evaluationclient.rs](oxcache/src/bin/evaluationclient.rs)
* Simple mltithreaded test - [simpleevaluationclient.rs](oxcache/src/bin/simpleevaluationclient.rs)

To run:

```
cargo run --package oxcache --bin CLIENT -- ARGS
```

EG:

```
cargo run --package oxcache \
    --bin simpleevaluationclient -- \
    --socket /tmp/oxcache.sock \
    --num-clients 32
```