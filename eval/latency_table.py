#!/usr/bin/env python3
"""
Generate LaTeX table for OxCache WT workload latency comparison.
Compares ZNS vs Block devices for chunk and promotional eviction types.
Data is normalized around ZNS device to show percentage increase for Block.
"""

import argparse
from pathlib import Path
import numpy as np
import data_cache

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Device name mappings
DEVICE_MAPPINGS = {
    "nvme0n2": "ZNS",
    "nvme1n1": "Block"
}

# Eviction types
EVICTION_TYPES = ["promotional", "chunk"]

EVICTION_TYPE_LABELS = {
    "promotional": "Zone LRU",
    "chunk": "Chunk LRU"
}

# Supported latency metrics
LATENCY_METRICS = {
    "get_total": {
        "file": "get_total_latency_ms.json",
        "label": "Get Total",
    },
    "get_hit": {
        "file": "get_hit_latency_ms.json",
        "label": "Get Hit",
    },
    "get_miss": {
        "file": "get_miss_latency_ms.json",
        "label": "Get Miss",
    },
    "device_read": {
        "file": "device_read_latency_ms.json",
        "label": "Device Read",
    },
    "device_write": {
        "file": "device_write_latency_ms.json",
        "label": "Device Write",
    },
    "disk_read": {
        "file": "disk_read_latency_ms.json",
        "label": "Disk Read",
    },
    "disk_write": {
        "file": "disk_write_latency_ms.json",
        "label": "Disk Write",
    },
    "get_response_latency_ms": {
        "file": "get_response_latency_ms.json",
        "label": "Get Response",
    }
}

# ============================================================================
# DATA PARSING FUNCTIONS
# ============================================================================

def parse_directory_name(dirname):
    """
    Parse directory name to extract parameters.

    Format: chunk_size,L=...,NZ=...,eviction_type,device-timestamp

    Returns dict with: chunk_size, eviction_type, device
    """
    parts = dirname.split(",")

    if len(parts) < 5:
        return None

    try:
        chunk_size = int(parts[0])
        eviction_type = parts[3]
        device_part = parts[4].split("-")[0]  # Extract device name before timestamp

        # Map device name to type
        device = None
        for dev_name, dev_type in DEVICE_MAPPINGS.items():
            if device_part.startswith(dev_name):
                device = dev_type
                break

        if device is None:
            return None

        return {
            "chunk_size": chunk_size,
            "eviction_type": eviction_type,
            "device": device,
            "dirname": dirname
        }
    except (ValueError, IndexError):
        return None


def collect_runs(split_output_dir):
    """
    Collect all runs from a split_output directory.

    Returns list of dicts with run parameters and directory path.
    """
    runs = []
    split_path = Path(split_output_dir)

    if not split_path.exists():
        print(f"Warning: Directory does not exist: {split_output_dir}")
        return runs

    for entry in split_path.iterdir():
        if entry.is_dir():
            params = parse_directory_name(entry.name)
            if params:
                params["path"] = entry
                runs.append(params)

    return runs


# ============================================================================
# STATISTICS CALCULATION
# ============================================================================

def calculate_statistics(data):
    """
    Calculate max, median, mean, and P99 for a dataset.

    Args:
        data: NumPy array of values

    Returns:
        dict: {'max': float, 'median': float, 'mean': float, 'p99': float}
              Returns None values if data is empty
    """
    if len(data) == 0:
        return {'max': None, 'median': None, 'mean': None, 'p99': None}

    return {
        'max': np.max(data),
        'median': np.median(data),
        'mean': np.mean(data),
        'p99': np.percentile(data, 99)
    }


def load_metric_stats(run_path, metric_file, filter_minutes=None, sample_size=None):
    """
    Load metric data and calculate statistics.

    Args:
        run_path: Path to run directory
        metric_file: Name of metric file to load
        filter_minutes: Number of minutes to exclude from end of run
        sample_size: If provided, sample every Nth line from JSON files

    Returns:
        dict: Statistics dict or None if file doesn't exist
    """
    data_file = run_path / metric_file
    if not data_file.exists():
        return None

    # Load latency data using data_cache
    ts, vals = data_cache.load_metric_data(
        data_file,
        filter_minutes=filter_minutes,
        use_cache=True,
        sample_size=sample_size
    )

    return calculate_statistics(vals)


# ============================================================================
# TABLE GENERATION
# ============================================================================

def format_stat_value(value, precision=2):
    """Format a statistic value for display."""
    if value is None:
        return "N/A"
    return f"{value:.{precision}f}"


def format_percentage_increase(zns_value, block_value, precision=1):
    """
    Calculate and format percentage increase from ZNS to Block.

    Args:
        zns_value: ZNS baseline value
        block_value: Block value to compare

    Returns:
        str: Formatted percentage increase (e.g., "+15.3\\%") or "N/A"
    """
    if zns_value is None or block_value is None:
        return "N/A"

    if zns_value == 0:
        return "N/A"

    increase = ((block_value - zns_value) / zns_value) * 100

    # Format with sign
    if increase >= 0:
        return f"+{increase:.{precision}f}\\%"
    else:
        return f"{increase:.{precision}f}\\%"


def generate_latex_table(block_runs, zns_runs, metric_key, output_file, filter_minutes=None, sample_size=None):
    """
    Generate LaTeX table comparing ZNS and Block latencies for a single metric.

    The table shows ZNS values as baseline and Block values as percentage increase.
    Compares Chunk LRU with Chunk LRU and Zone LRU with Zone LRU.

    Args:
        block_runs: List of block device runs
        zns_runs: List of ZNS device runs
        metric_key: Single metric key to include (from LATENCY_METRICS)
        output_file: Output file path for LaTeX table
        filter_minutes: Number of minutes to exclude from end of run
        sample_size: If provided, sample every Nth line from JSON files
    """
    print("\nGenerating latency comparison table...")
    if filter_minutes:
        print(f"  Excluding last {filter_minutes} minutes of data...")

    # Get metric configuration
    metric_config = LATENCY_METRICS[metric_key]
    metric_file = metric_config["file"]
    metric_label = metric_config["label"]

    print(f"\nProcessing {metric_label}...")
    data = {}

    for eviction_type in EVICTION_TYPES:
        # Find matching ZNS run
        zns_run = None
        for run in zns_runs:
            if run["eviction_type"] == eviction_type and run["device"] == "ZNS":
                zns_run = run
                break

        # Find matching Block run
        block_run = None
        for run in block_runs:
            if run["eviction_type"] == eviction_type and run["device"] == "Block":
                block_run = run
                break

        # Load statistics
        zns_stats = None
        block_stats = None

        if zns_run:
            print(f"  Loading ZNS {EVICTION_TYPE_LABELS[eviction_type]}...")
            zns_stats = load_metric_stats(zns_run["path"], metric_file, filter_minutes, sample_size)

        if block_run:
            print(f"  Loading Block {EVICTION_TYPE_LABELS[eviction_type]}...")
            block_stats = load_metric_stats(block_run["path"], metric_file, filter_minutes, sample_size)

        data[eviction_type] = {
            'zns': zns_stats,
            'block': block_stats
        }

    # Generate LaTeX table
    print(f"\nGenerating LaTeX table: {output_file}")

    with open(output_file, 'w') as f:
        # Write table header
        f.write("\\begin{table}[htbp]\n")
        f.write("\\centering\n")
        f.write(f"\\caption{{Latency statistics for Zone LRU and Chunk LRU eviction policies. Each entry reports ZNS / block-interface latency, with the percentage indicating the relative change when using the block-interface device. (ms)}}\n")
        f.write("\\label{tab:latency_comparison}\n")
        f.write("\\begin{tabular}{|l|c|c|}\n")
        f.write("\\hline\n")

        # Column headers
        f.write("\\textbf{Statistic} & \\textbf{Zone LRU} & \\textbf{Chunk LRU} \\\\\n")
        f.write("\\hline\n")

        # Statistics to display
        stats_list = [
            ('max', 'Maximum'),
            ('median', 'Median'),
            ('mean', 'Mean'),
            ('p99', 'P99')
        ]

        # Write data rows - for each statistic, show ZNS / Block (percentage)
        for stat_key, stat_label in stats_list:
            f.write(f"{stat_label} & ")

            # For each eviction type
            for i, eviction_type in enumerate(EVICTION_TYPES):
                zns_stats = data[eviction_type]['zns']
                block_stats = data[eviction_type]['block']

                if zns_stats and block_stats:
                    zns_val = zns_stats[stat_key]
                    block_val = block_stats[stat_key]
                    pct = format_percentage_increase(zns_val, block_val)
                    f.write(f"{format_stat_value(zns_val)} / {format_stat_value(block_val)} ({pct})")
                else:
                    f.write("N/A")

                # Add separator or newline
                if i < len(EVICTION_TYPES) - 1:
                    f.write(" & ")
                else:
                    f.write(" \\\\\n")

            f.write("\\hline\n")

        # Close table
        f.write("\\end{tabular}\n")
        f.write("\\end{table}\n")

    print(f"Saved: {output_file}")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate LaTeX table for OxCache WT workload latency comparison."
    )
    parser.add_argument(
        '--block-dir',
        required=True,
        help='Path to block device directory (e.g., .../WT-SPC-SSD-consolidated/)'
    )
    parser.add_argument(
        '--zns-dir',
        required=True,
        help='Path to ZNS device directory (e.g., .../WT-SPC-ZNS-consolidated/)'
    )
    parser.add_argument(
        '--output-file',
        default='latency_comparison_table.tex',
        help='Output file for LaTeX table (default: latency_comparison_table.tex)'
    )
    parser.add_argument(
        '--metric',
        choices=list(LATENCY_METRICS.keys()),
        required=True,
        help='Latency metric to include in table'
    )
    parser.add_argument(
        '--sample',
        type=int,
        default=None,
        help='Sample every Nth line from JSON files for faster testing (e.g., --sample 100)'
    )
    parser.add_argument(
        '--filter-minutes',
        type=int,
        default=None,
        help='Exclude last N minutes of data from each run (use 0 for no filtering)'
    )

    args = parser.parse_args()

    # Construct split_output paths
    block_split = Path(args.block_dir) / "split_output"
    zns_split = Path(args.zns_dir) / "split_output"

    # Verify directories exist
    if not block_split.exists():
        print(f"Error: Block split_output directory not found: {block_split}")
        return 1

    if not zns_split.exists():
        print(f"Error: ZNS split_output directory not found: {zns_split}")
        return 1

    # Collect runs from both directories
    block_runs = collect_runs(block_split)
    zns_runs = collect_runs(zns_split)

    print(f"Found {len(block_runs)} block runs and {len(zns_runs)} ZNS runs")

    # Convert filter_minutes=0 to None for data_cache
    filter_min = args.filter_minutes if args.filter_minutes and args.filter_minutes > 0 else None

    # Generate LaTeX table
    generate_latex_table(block_runs, zns_runs, args.metric, args.output_file, filter_min, args.sample)

    print("\nLatency comparison table generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
