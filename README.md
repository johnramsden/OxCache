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

Set config via CLI or toml file. See [example.server.toml](example.server.toml) for a basic configuration or [example.benchmark.toml](example.benchmark.toml) for a benchmark configuration

### Server Parameters

All parameters can be set via command line arguments or in a TOML configuration file. Command line arguments override config file values.

#### Core Parameters (Required)

- `--socket` - Path to Unix socket for client connections (e.g., `/tmp/oxcache.sock`)
- `--disk` - Device path for cache storage (e.g., `/dev/nvme0n1`). **WARNING: All data on this device will be wiped**
- `--writer-threads` - Number of writer threads (must be > 0)
- `--reader-threads` - Number of reader threads (must be > 0)
- `--chunk-size` - Size of cache chunks in bytes (e.g., `65536` for 64KB)
- `--max-write-size` - Maximum write size in bytes (e.g., `262144` for 256KB)
- `--block-zone-capacity` - Zone capacity for block devices in bytes

#### Remote Backend Parameters (Required)

- `--remote-type` - Remote storage type: `emulated`
- `--remote-artificial-delay-microsec` - Artificial delay in microseconds for emulated remote (only applies to `emulated` type)

#### Eviction Policy Parameters (Required)

- `--eviction-policy` - Eviction policy: `promotional` or `chunk`
- `--high-water-evict` - Number of zones remaining from end that triggers eviction
- `--low-water-evict` - Target number of free zones after eviction (must be > `high_water_evict`)
- `--low-water-clean` - Clean watermark for chunk eviction policy (required for `chunk` policy, must be < `low_water_evict - high_water_evict`)
- `--eviction-interval-ms` - Eviction check interval in milliseconds

#### Optional Parameters

- `--max-zones` - Maximum number of zones to use on the device
- `--config` - Path to TOML configuration file
- `--log-level` - Logging level: `error`, `warn`, `info`, `debug`, `trace`

#### Metrics Parameters (Optional)

- `--metrics-ip-addr` - IP address for Prometheus metrics exporter (must be set with `--metrics-port`)
- `--metrics-port` - Port for Prometheus metrics exporter (must be set with `--metrics-ip-addr`)
- `--file-metrics-directory` - Directory to store metrics log files
- `--benchmark-mode` - Enable throughput benchmark mode (measures bytes/sec after first eviction)
- `--benchmark-duration-secs` - Duration in seconds to run benchmark after first eviction
- `--benchmark-target-bytes` - Target bytes to process in benchmark mode before stopping
- `--eviction-metrics` - Enable detailed eviction metrics tracking (requires compilation with `--features eviction-metrics`)
- `--eviction-metrics-interval` - Eviction metrics log interval in seconds (default: 60)

### Example

```
cargo build --package oxcache --bin oxcache
./target/debug/oxcache --config ./example.server.toml
```

Or with CLI arguments:

```
./target/debug/oxcache \
  --socket /tmp/oxcache.sock \
  --disk /dev/nvme0n1 \
  --writer-threads 14 \
  --reader-threads 14 \
  --chunk-size 65536 \
  --max-write-size 262144 \
  --block-zone-capacity 1129316352 \
  --remote-type emulated \
  --remote-artificial-delay-microsec 40632 \
  --eviction-policy promotional \
  --high-water-evict 5 \
  --low-water-evict 7 \
  --eviction-interval-ms 1000
```

## Running Clients

The following test and benchmark clients are available:

### Test Clients

#### client

Simple single-threaded test client with hardcoded test cases - [client.rs](oxcache/src/bin/client.rs)

**Parameters:**
- `--socket` - Path to the Unix socket to connect to

**Example:**
```bash
cargo build --package oxcache --bin client
./target/debug/client --socket /tmp/oxcache.sock
```

#### simpleevaluationclient

Simple multithreaded test client with random UUID keys - [simpleevaluationclient.rs](oxcache/src/bin/simpleevaluationclient.rs)

**Parameters:**
- `--socket` - Path to the Unix socket to connect to
- `--num-clients` - Number of concurrent client threads
- `--query-size` - Size of each query in bytes

**Example:**
```bash
cargo build --package oxcache --bin simpleevaluationclient
./target/debug/simpleevaluationclient --socket /tmp/oxcache.sock --num-clients 32 --query-size 4096
```

#### evaluationclient

Multithreaded test client accepting a binary file with query inputs - [evaluationclient.rs](oxcache/src/bin/evaluationclient.rs)

**Parameters:**
- `--socket` - Path to the Unix socket to connect to
- `--num-clients` - Number of concurrent client threads
- `--data-file` - Path to binary file containing i32 query keys (little-endian)
- `--query-size` - Size of each query in bytes

**Example:**
```bash
cargo build --package oxcache --bin evaluationclient
./target/debug/evaluationclient --socket /tmp/oxcache.sock --num-clients 32 --data-file ./queries.bin --query-size 4096
```

### Workload Clients

#### spclient

SPC (Storage Performance Council) trace workload generator - [spclient.rs](oxcache/src/bin/spclient.rs)

**Parameters:**
- `--trace-file` / `-t` - Path to SPC trace file (CSV format)
- `--chunk-size` / `-c` - Chunk size in bytes for quantization
- `--mode` / `-m` - Run mode: `dry` for dry-run analysis, `real` for actual execution (default: `dry`)
- `--socket` / `-s` - Path to Unix socket (required for `real` mode)
- `--num-clients` / `-n` - Number of concurrent clients for real mode (default: 1)
- `--max-requests` - Maximum number of requests to process (0 = unlimited, default: 0)
- `--verbose` / `-v` - Show verbose output including all warnings

**Example (dry run):**
```bash
cargo build --package oxcache --bin spclient
./target/debug/spclient --trace-file ./trace.csv --chunk-size 65536 --mode dry
```

**Example (real execution):**
```bash
./target/debug/spclient --trace-file ./trace.csv --chunk-size 65536 --mode real --socket /tmp/oxcache.sock --num-clients 16
```

### Other Test Utilities

- **test_subset_reads** - Test client for subset read operations - [test_subset_reads.rs](oxcache/src/bin/test_subset_reads.rs)
- **znsappendtest** - ZNS device append throughput test - [znsappendtest.rs](oxcache/src/bin/znsappendtest.rs)
- **znsmaxappendtest** - ZNS device maximum append test - [znsmaxappendtest.rs](oxcache/src/bin/znsmaxappendtest.rs)
