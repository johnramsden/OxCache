# SPC Trace Workload Generator (spclient)

A workload generator for OxCache that reads SPC (Storage Performance Council) trace files and quantizes them into chunk-based cache requests.

## Overview

`spclient` reads industry-standard SPC I/O trace files (CSV format) and converts logical block address (LBA) based requests into chunk-based cache GET requests. It supports both dry-run mode for analysis and real execution mode for actual cache workload testing.

## Features

- **SPC Trace Parsing**: Reads CSV format traces with fields: device, LBA, size, R/W, timestamp
- **Quantization Algorithm**: Converts LBA-based requests into chunk-based cache requests
- **4KB Alignment**: Automatically rounds up LBA and size to 4KB boundaries
- **Multi-Chunk Splitting**: Splits requests spanning multiple chunks into separate GetRequests
- **Dry Run Mode**: Display quantized requests without executing them
- **Real Execution Mode**: Send requests to cache server with multi-client support
- **Comprehensive Statistics**: Single/multi-chunk breakdown, alignment warnings, max chunks spanned

## Building

```bash
cargo build --bin spclient --release
```

## Usage

### Basic Syntax

```bash
spclient --trace-file <TRACE_FILE> --chunk-size <CHUNK_SIZE> [OPTIONS]
```

### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--trace-file` | `-t` | Path to SPC trace file (CSV format) | Required |
| `--chunk-size` | `-c` | Chunk size in bytes (must be 4KB-aligned) | Required |
| `--mode` | `-m` | Run mode: 'dry' or 'real' | `dry` |
| `--socket` | `-s` | Path to unix socket (required for real mode) | None |
| `--num-clients` | `-n` | Number of concurrent clients | `1` |
| `--max-requests` | | Limit number of requests to process (0 = unlimited) | `0` |
| `--verbose` | `-v` | Show detailed output including all warnings | Off |

### Examples

#### Dry Run (Analysis Mode)

```bash
# Basic dry run with 64KB chunks
cargo run --bin spclient --release -- \
  -t logs/spc_fmt.log \
  -c 65536 \
  -m dry

# Dry run with verbose warnings and limited output
cargo run --bin spclient --release -- \
  -t logs/spc_fmt.log \
  -c 65536 \
  -m dry \
  --max-requests 100 \
  -v

# Test with smaller 8KB chunks
cargo run --bin spclient --release -- \
  -t logs/spc_fmt.log \
  -c 8192 \
  -m dry \
  --max-requests 50
```

#### Real Execution Mode

```bash
# Single client execution
cargo run --bin spclient --release -- \
  -t logs/spc_fmt.log \
  -c 65536 \
  -m real \
  -s /tmp/oxcache.sock

# Multi-client execution (4 concurrent clients)
cargo run --bin spclient --release -- \
  -t logs/spc_fmt.log \
  -c 65536 \
  -m real \
  -s /tmp/oxcache.sock \
  -n 4

# Limited workload with 2 clients
cargo run --bin spclient --release -- \
  -t logs/spc_fmt.log \
  -c 65536 \
  -m real \
  -s /tmp/oxcache.sock \
  -n 2 \
  --max-requests 10000
```

## SPC Trace Format

The tool expects CSV files with 5 comma-separated fields per line:

```
device,lba,size,operation,timestamp
```

**Example:**
```
0,0,4096,R,0.948266
0,1889629706,28672,R,0.953770
0,45788,28672,W,555.164077
```

**Fields:**
- **Field 1 (device)**: Device identifier (ignored by spclient)
- **Field 2 (lba)**: Logical Block Address - **REQUIRED**
- **Field 3 (size)**: Transfer size in bytes - **REQUIRED**
- **Field 4 (operation)**: R (Read) or W (Write) - treated identically as GET requests
- **Field 5 (timestamp)**: Timestamp in seconds (ignored, no timing delays)

## Quantization Algorithm

### Overview

The quantization process converts LBA-based I/O requests into chunk-based cache requests:

```
SPC Trace Entry (LBA, Size) → One or More GetRequests (Key, Offset, Size)
```

### Algorithm Steps

1. **Align LBA to 4KB boundary** (round up):
   ```
   aligned_lba = ((lba + 4095) / 4096) * 4096
   ```

2. **Align Size to 4KB boundary** (round up):
   ```
   aligned_size = ((size + 4095) / 4096) * 4096
   ```

3. **Calculate end address**:
   ```
   end_lba = aligned_lba + aligned_size
   ```

4. **Iterate through chunks**:
   - For each chunk from `aligned_lba` to `end_lba`:
     - Calculate chunk number: `chunk_num = current_lba / chunk_size`
     - Calculate offset within chunk: `offset = current_lba % chunk_size`
     - Calculate bytes to read: `min(remaining_bytes, chunk_size - offset)`
     - Generate GetRequest with key `"chunk_{chunk_num}"`, offset, and size

### Example: Multi-Chunk Request

**Input:**
- LBA: 4096
- Size: 20480 bytes
- Chunk Size: 8192 bytes

**Step 1-2:** Already 4KB-aligned
**Step 3:** End = 4096 + 20480 = 24576

**Step 4:** Generate requests:

| Chunk | LBA Range | Offset | Size | Key |
|-------|-----------|--------|------|-----|
| 0 | 4096-8192 | 4096 | 4096 | `chunk_0` |
| 1 | 8192-16384 | 0 | 8192 | `chunk_1` |
| 2 | 16384-24576 | 0 | 8192 | `chunk_2` |

**Result:** 3 GetRequests spanning 3 chunks

### Example: Unaligned Request

**Input:**
- LBA: 100
- Size: 100 bytes
- Chunk Size: 8192 bytes

**Step 1:** LBA 100 → 4096 (round up, **WARNING**)
**Step 2:** Size 100 → 4096 (round up, **WARNING**)
**Step 3:** End = 4096 + 4096 = 8192

**Step 4:** Generate request:

| Chunk | Offset | Size | Key | Warnings |
|-------|--------|------|-----|----------|
| 0 | 4096 | 4096 | `chunk_0` | LBA rounded up, Size rounded up |

**Result:** 1 GetRequest with 2 alignment warnings

## Output Examples

### Dry Run Output

```
Parsing trace file: logs/spc_fmt.log
Loaded 17714275 trace entries
Quantizing with chunk_size=65536

Statistics:
  Total trace entries: 17714275
  Total GET requests: 21537655
  Single-chunk requests: 13892393 (78.4%)
  Multi-chunk requests: 3821882 (21.6%)
  Max chunks spanned: 398
  Alignment warnings: 17709996

=== Dry Run: Quantized Requests ===

Index    Original LBA    →   Key                       Offset       Size
--------------------------------------------------------------------------------
0        0               →   chunk_0                   0            4096
1        1889629706      →   chunk_28833               32768        28672
2        1889629706      →   chunk_28833               32768        28672
3        1889629697      →   chunk_28833               32768        4096
...
```

### Dry Run with Verbose Warnings

```bash
cargo run --bin spclient --release -- -t logs/spc_fmt.log -c 65536 -m dry --max-requests 5 -v
```

```
Index    Original LBA    →   Key                       Offset       Size
--------------------------------------------------------------------------------
0        0               →   chunk_0                   0            4096
1        1889629706      →   chunk_28833               32768        28672
         WARNING: LBA 1889629706 rounded up to 1889632256 (4KB alignment)
2        1889629706      →   chunk_28833               32768        28672
         WARNING: LBA 1889629706 rounded up to 1889632256 (4KB alignment)
...
```

### Real Execution Output

```
Parsing trace file: logs/spc_fmt.log
Loaded 17714275 trace entries
Quantizing with chunk_size=65536

Statistics:
  Total trace entries: 17714275
  Total GET requests: 21537655

[Client 0] Connected to /tmp/oxcache.sock
[Client 1] Connected to /tmp/oxcache.sock
[Client 2] Connected to /tmp/oxcache.sock
[Client 3] Connected to /tmp/oxcache.sock
[Client 1] Progress: 2153765/21537655 (10%)
[Client 0] Progress: 4307530/21537655 (20%)
[Client 2] Progress: 6461295/21537655 (30%)
...
[Client 3] Completed
[Client 1] Completed
[Client 0] Completed
[Client 2] Completed

=== Execution Complete ===
Executed 21537655 requests
Clients: 4
Total time: 45.23s
Throughput: 476123.45 req/sec
```

## Chunk Size Guidelines

### Recommended Chunk Sizes

All chunk sizes must be 4KB-aligned (multiples of 4096).

| Chunk Size | Use Case | Expected Behavior |
|------------|----------|-------------------|
| 4KB (4096) | Minimal chunking | Most requests fit in single chunk, many chunks total |
| 8KB (8192) | Small chunks | Good for testing, moderate multi-chunk requests |
| 64KB (65536) | Medium chunks | Balanced single/multi-chunk ratio |
| 256KB (262144) | Large chunks | Fewer total chunks, most requests in single chunk |
| 1MB (1048576) | Very large chunks | Minimal chunk count, rare multi-chunk requests |

### Impact on Statistics

**Smaller chunk sizes:**
- More total GET requests
- Higher percentage of multi-chunk requests
- Higher max chunks spanned
- More cache keys generated

**Larger chunk sizes:**
- Fewer total GET requests
- Higher percentage of single-chunk requests
- Lower max chunks spanned
- Fewer cache keys generated

**Example comparison** (same trace file):

| Chunk Size | Total Requests | Single-Chunk % | Multi-Chunk % | Max Chunks Spanned |
|------------|----------------|----------------|---------------|--------------------|
| 8KB | 48,325,083 | 42.4% | 57.6% | 3,184 |
| 64KB | 21,537,655 | 78.4% | 21.6% | 398 |

## Request Validation

### Pre-execution Checks

1. **Chunk size validation**: Must be non-zero and 4KB-aligned
2. **Socket validation** (real mode): Socket path must be provided
3. **Request size validation**: No request can exceed 2GB (MAX_FRAME_LENGTH)

### Runtime Validation

Each GetRequest is validated before sending:
- `offset + size <= chunk_size` (no cross-boundary reads)
- `size > 0` (no zero-size requests)
- `size <= 2GB` (frame length limit)

## Error Handling

### Common Errors

**Invalid chunk size:**
```
Error: chunk_size must be non-zero and 4KB-aligned
```
Solution: Use a multiple of 4096 (e.g., 4096, 8192, 65536, 262144)

**Missing socket in real mode:**
```
Error: --socket required for real mode
```
Solution: Provide socket path with `-s /path/to/socket`

**Trace file not found:**
```
Error: No such file or directory (os error 2)
```
Solution: Check trace file path is correct

**No valid trace entries:**
```
Error: No valid trace entries found
```
Solution: Check trace file format, ensure it has valid LBA/size fields

### Warnings

**Alignment warnings:**
```
WARNING: LBA 1889629706 rounded up to 1889632256 (4KB alignment)
WARNING: Size 100 rounded up to 4096 (4KB alignment)
```
These are informational and do not prevent execution.

**Oversized request warning (dry mode):**
```
WARNING: Request size 2147483648 exceeds MAX_FRAME_LENGTH (2GB)
```
This becomes an error in real mode.

## Performance Considerations

### Memory Usage

- **Trace loading**: ~100 bytes per entry → 10M entries ≈ 1GB RAM
- **Request generation**: ~50 bytes per GetRequest
- **Large traces**: May require significant RAM (loaded entirely into memory)

### Execution Performance

**Single client:**
- Throughput limited by serial request/response pattern
- Typical: 1,000-5,000 req/sec depending on cache server performance

**Multi-client:**
- Near-linear scaling up to server bottleneck
- Typical: 4 clients → 3,000-20,000 req/sec
- Recommended: 2-8 clients for most workloads

### Recommendations

1. **Start with dry run** to understand workload characteristics
2. **Use --max-requests** for initial testing before full trace
3. **Tune num-clients** based on server performance
4. **Monitor server** metrics during execution

## Technical Details

### Implementation

- **Language**: Rust with Tokio async runtime
- **Serialization**: Bincode for request encoding
- **Transport**: Unix domain sockets with length-delimited framing
- **Concurrency**: Multi-client support with shared request queue
- **Frame limit**: 2GB maximum request/response size

### Code Structure

```
spclient.rs (~580 lines)
├── Data structures: Cli, TraceEntry, QuantizedRequest, RequestStats
├── Parsing: parse_trace() - CSV parsing
├── Quantization: quantize_request() - Core algorithm
├── Generation: generate_requests() - Batch processing
├── Dry mode: run_dry_mode() - Display requests
├── Real mode: run_real_mode() - Execute via cache client
└── Tests: Unit tests for core functions
```

### Dependencies

All dependencies already in OxCache Cargo.toml:
- `clap` - CLI parsing
- `tokio` - Async runtime
- `tokio-util` - LengthDelimitedCodec
- `bincode` - Serialization
- `bytes` - Byte buffers
- `futures` - Stream/Sink traits

## Testing

### Run Unit Tests

```bash
cargo test --bin spclient
```

Tests cover:
- 4KB alignment logic
- Single-chunk quantization
- Multi-chunk spanning
- Unaligned input handling
- Chunk boundary cases

### Integration Testing

1. **Create small test trace:**
   ```bash
   cat > test_trace.csv << EOF
   0,0,4096,R,0.0
   0,8192,16384,R,1.0
   0,100,100,W,2.0
   EOF
   ```

2. **Run dry mode:**
   ```bash
   cargo run --bin spclient --release -- -t test_trace.csv -c 8192 -m dry -v
   ```

3. **Expected output:**
   - 3 trace entries
   - 4 total GetRequests (entry 2 spans 2 chunks)
   - 1 alignment warning (entry 3)

## Troubleshooting

### Client connection errors

**Error:** Connection refused
```
Error: Connection refused (os error 111)
```
**Solution:** Ensure cache server is running and socket path is correct

### Slow execution

**Symptom:** Very low throughput in real mode

**Solutions:**
- Increase `--num-clients` (try 2, 4, 8)
- Check server is not CPU/IO bound
- Verify network/socket performance

### Out of memory

**Symptom:** Process killed during parsing

**Solutions:**
- Use `--max-requests` to limit workload size
- Process trace in batches (split trace file)
- Increase available system memory

## See Also

- **simpleevaluationclient.rs** - Reference implementation for multi-client pattern
- **evaluationclient.rs** - Alternative client with binary trace format
- **OxCache documentation** - Cache server setup and configuration

## License

Part of the OxCache project.
