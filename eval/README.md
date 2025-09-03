# OxCache Evaluation Data Processing

This directory contains tools for processing and analyzing OxCache evaluation data.

## Files

- `split_data.py` - Script to split JSON metric data files by metric name
- `plot.py` - Data visualization script
- `parse_pidstat.py` - Script to parse pidstat output files and generate statistics
- `generate_pidstat_latex.sh` - Shell script to generate LaTeX tables for all pidstat files

## Data Structure

The evaluation data is stored in directories like `BLOCK-PROMO` and `ZONED-PROMO`, containing:
- JSON files with timestamped metrics
- Client and server run files

### JSON Data Format

Each JSON file contains lines with timestamped metrics in the format:
```json
{"timestamp":"2025-08-19T20:58:54.461540Z","fields":{"name":"usage_percentage","value":0.0}}
```

Common metric types include:
- `usage_percentage`
- `requests`
- `hitratio`
- `miss`
- `bytes_total`
- `device_write_latency_ms`
- `disk_write_latency_ms`
- `get_miss_latency_ms`
- `get_total_latency_ms`
- `written_bytes_total`

## Usage

### Splitting Data by Metric Type

To split JSON data files by metric name:

```bash
python split_data.py <target_directory>
```

Example:
```bash
python split_data.py data/BLOCK-PROMO
python split_data.py data/ZONED-PROMO
```

This will:
1. Create a `split_output` directory in the target location
2. For each JSON file, create a subdirectory named after the original file
3. Within each subdirectory, create separate `.json` files for each metric type
4. Each metric file contains only the lines for that specific metric

**Output structure:**
```
data/BLOCK-PROMO/split_output/
├── original_filename/
│   ├── usage_percentage.json
│   ├── requests.json
│   ├── hitratio.json
│   └── ...
```

### Performance Note

The JSON files can be very large (2-3GB each). The script processes files efficiently by:
- Writing data directly to output files (no intermediate storage)
- Providing progress updates every 100,000 lines
- Using streaming processing to handle large files

Processing time may take several minutes per file depending on file size and system performance.

## Data Visualization

After splitting the data, you can generate various plots using the provided visualization scripts. All scripts support both single-directory and multi-directory comparison modes.

### Single Directory Plotting

Plot data from one experiment directory:

```bash
python plot_throughput.py data/BLOCK-PROMO/split_output --bucket-seconds 60
python plot_latency.py data/BLOCK-PROMO/split_output
python plot_latency_smoothed.py data/BLOCK-PROMO/split_output --window-seconds 30
python plot_hitratio.py data/BLOCK-PROMO/split_output
```

### Multi-Directory Comparison

Compare data across different experiment types with custom labels:

```bash
# Compare BLOCK vs ZNS interface performance
python plot_throughput.py data/BLOCK-PROMO/split_output data/ZONED-PROMO/split_output --labels Block-interface ZNS --bucket-seconds 60
python plot_latency.py data/BLOCK-PROMO/split_output data/ZONED-PROMO/split_output --labels Block-interface ZNS
python plot_latency_smoothed.py data/BLOCK-PROMO/split_output data/ZONED-PROMO/split_output --labels Block-interface ZNS --window-seconds 30
python plot_hitratio.py data/BLOCK-PROMO/split_output data/ZONED-PROMO/split_output --labels Block-interface ZNS
```

### Script Options

**Common options for all scripts:**
- `--output-dir`: Output directory for plots (default: plots/)
- `--labels`: Custom labels for each directory (must match number of directories)

**Throughput plotting (`plot_throughput.py`):**
- `--bucket-seconds`: Time bucket size for throughput calculation (default: 60s)
- `--metrics`: Specific metrics to plot (default: bytes_total, written_bytes_total)

**Raw latency plotting (`plot_latency.py`):**
- `--metrics`: Latency metrics to plot (default: all latency metrics)

**Smoothed latency plotting (`plot_latency_smoothed.py`):**
- `--window-seconds`: Smoothing window size in seconds (default: 30s)
- `--metrics`: Latency metrics to plot

### File Matching and Normalization

When comparing multiple directories, the scripts automatically match similar files by normalizing device names (e.g., `nvme1n1` and `nvme0n2` are treated as equivalent). This allows direct comparison between experiments that only differ in device identifiers.

### Output Behavior

**Single directory mode:**
- Creates individual plots for each data file
- Time axis starts at 0 minutes for each experiment

**Multi-directory comparison mode:**
- Creates comparison plots overlaying similar experiments from different directories
- Each dataset uses a different color and includes legend
- Files are matched by normalized names (ignoring device differences)

**Output naming convention:**
- Throughput: `{normalized_filename}_{metric_name}_throughput.png`
- Raw latency: `{normalized_filename}_{metric_name}_raw.png`
- Smoothed latency: `{normalized_filename}_{metric_name}_smoothed_{window_size}s.png`
- Hit ratio: `{normalized_filename}_hitratio.png`

All plotting scripts:
- Generate high-resolution PNG files (300 DPI)
- Use time in minutes starting from 0
- Include appropriate legends when comparing multiple datasets
- Handle large datasets efficiently

## Automated Plot Generation

For convenience, use the automated shell script to generate all plots:

```bash
./generate_all_plots.sh
```

This script will:
1. **Check and split data** if needed for both BLOCK-PROMO and ZONED-PROMO
2. **Generate individual plots** for each dataset separately
3. **Generate comparison plots** overlaying Block vs ZNS performance
4. **Organize outputs** into separate directories:
   - `plots/block_individual/` - Individual BLOCK-PROMO plots  
   - `plots/zoned_individual/` - Individual ZONED-PROMO plots
   - `plots/comparison/` - Side-by-side comparison plots

**Configuration:**
- Throughput bucket size: 60 seconds
- Latency smoothing window: 60 seconds
- Labels: "Block-interface" vs "ZNS"
- Metrics: All throughput, latency, and hit ratio metrics

**Generated plot types:**
- Throughput plots (bytes_total, written_bytes_total)
- Raw latency plots (device_write_latency_ms, disk_write_latency_ms, get_miss_latency_ms, get_total_latency_ms)
- Smoothed latency plots (same metrics with 60s averaging)
- Hit ratio comparison plots

The script provides progress updates and a summary of generated plots upon completion.

## System Resource Analysis

### Pidstat Output Analysis

The `parse_pidstat.py` script analyzes pidstat output files to extract CPU and memory usage statistics. It supports multiple output formats including LaTeX tables for publication-quality results.

**Basic usage:**
```bash
# Human-readable output
python parse_pidstat.py path/to/pidstat_file.txt

# CSV format
python parse_pidstat.py --csv path/to/pidstat_file.txt

# LaTeX table format
python parse_pidstat.py --latex path/to/pidstat_file.txt
```

**LaTeX Output Features:**
- Parses filename metadata (chunk size, latency, distribution, ratio, zones, disk type)
- Maps device names: nvme1n1 → Block-interface, nvme0n2 → ZNS
- Converts memory values to GiB for better readability
- Generates separate tables for CPU and Memory statistics
- Includes descriptive comments with experiment parameters

**Filename Format Expected:**
```
CHUNKSZ,L=LATENCY,DISTRIBUTION,R=RATIO,NZ+NUM_ZONES,disk_type.pidstat
```

**Example output:**
```latex
% File: 65536,L=40632,ZIPFIAN,R=10,I=19660800,NZ+80,nvme0n2.pidstat
% Chunk Size: 65536, Latency: 40632
% Distribution: Zipfian, Ratio: 10
% Zones: 80, Disk Type: ZNS

\begin{table}[H]
\centering
\caption{CPU Usage Statistics - ZNS (65536)}
\begin{tabular}{|l|r|}
...
\end{tabular}
\end{table}
```

### Automated LaTeX Generation

Use the provided shell script to process all pidstat files at once:

```bash
./generate_pidstat_latex.sh
```

This script processes all `.pidstat` files in the `data/OXCACHE-UTILIZATION` and `data/ZNCACHE-UTILIZATION` directories (if they exist) and outputs LaTeX tables to stdout.
