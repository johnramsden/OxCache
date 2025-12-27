#!/usr/bin/env python3
"""
Compute and report latency statistics for OxCache runs.
Reports minimum, maximum, mean, median, and percentiles for each configuration.
"""

import argparse
from pathlib import Path
import numpy as np
import data_cache
import csv
import sys

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Distributions
DISTRIBUTIONS = ["ZIPFIAN", "UNIFORM"]

# Chunk sizes (in bytes)
CHUNK_SIZES = [65536, 268435456, 1129316352]  # 64KiB, 256MiB, 1076MiB

CHUNK_SIZE_LABELS = {
    65536: "64KiB",
    268435456: "256MiB",
    1129316352: "1077MiB"
}

# Ratios
RATIOS = [2, 10]

# Eviction types
EVICTION_TYPES = ["promotional", "chunk"]  # Zone-LRU, Chunk-LRU

EVICTION_TYPE_LABELS = {
    "promotional": "Zone-LRU",
    "chunk": "Chunk-LRU"
}

# Device name mappings
DEVICE_MAPPINGS = {
    "nvme0n2": "ZNS",
    "nvme1n1": "Block"
}

# ============================================================================
# DATA PARSING FUNCTIONS
# ============================================================================

def find_eviction_start_time(run_path):
    """
    Find the timestamp when eviction begins by finding peak usage.

    Args:
        run_path: Path to run directory containing usage_percentage.json

    Returns:
        float: Unix timestamp when usage peaks, or None if no data available
    """
    usage_file = run_path / "usage_percentage.json"

    if not usage_file.exists():
        return None

    try:
        timestamps, usage_values = data_cache.load_metric_data(
            usage_file,
            filter_minutes=None,
            use_cache=True
        )
    except Exception as e:
        return None

    if len(usage_values) == 0:
        return None

    # Find the index where usage reaches its maximum
    max_usage_index = usage_values.argmax()
    eviction_start_time = timestamps[max_usage_index]

    return eviction_start_time


def parse_directory_name(dirname):
    """
    Parse directory name to extract parameters.

    Format: chunk_size,L=...,DISTRIBUTION,R=ratio,I=...,NZ=...,eviction_type,device-timestamp

    Returns dict with: chunk_size, distribution, ratio, eviction_type, device
    """
    parts = dirname.split(",")

    if len(parts) < 8:
        return None

    try:
        chunk_size = int(parts[0])
        distribution = parts[2]
        ratio = int(parts[3].split("=")[1])
        eviction_type = parts[6]
        device_part = parts[7].split("-")[0]

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
            "distribution": distribution,
            "ratio": ratio,
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
        print(f"Warning: Directory does not exist: {split_output_dir}", file=sys.stderr)
        return runs

    for entry in split_path.iterdir():
        if entry.is_dir():
            params = parse_directory_name(entry.name)
            if params:
                params["path"] = entry
                runs.append(params)

    return runs


# ============================================================================
# STATISTICS COMPUTATION
# ============================================================================

def compute_latency_statistics(block_dir, zns_dir, from_eviction_start=False, filter_minutes=5, sample_size=None):
    """
    Compute latency statistics for all configurations.

    Args:
        block_dir: Path to block device split_output directory
        zns_dir: Path to ZNS device split_output directory
        from_eviction_start: If True, only include data from when eviction begins
        filter_minutes: Number of minutes to exclude from end of run
        sample_size: If provided, sample every Nth line from JSON files

    Returns:
        list: List of dicts with statistics for each configuration
    """
    metric_file = "get_total_latency_ms.json"
    scale = 1.0  # Already in ms

    # Collect runs from both directories
    block_runs = collect_runs(block_dir)
    zns_runs = collect_runs(zns_dir)

    print(f"Found {len(block_runs)} block runs and {len(zns_runs)} ZNS runs", file=sys.stderr)

    results = []

    # Iterate through all parameter combinations
    for distribution in DISTRIBUTIONS:
        for ratio in RATIOS:
            for chunk_size in CHUNK_SIZES:
                # Determine which eviction types to include
                if chunk_size == 1129316352:  # 1076MiB only has promotional
                    eviction_types_to_use = ["promotional"]
                else:
                    eviction_types_to_use = EVICTION_TYPES

                for device in ["ZNS", "Block"]:
                    for eviction_type in eviction_types_to_use:
                        # Find matching run
                        runs_to_search = zns_runs if device == "ZNS" else block_runs
                        matching_run = None

                        for run in runs_to_search:
                            if (run["chunk_size"] == chunk_size and
                                run["distribution"] == distribution and
                                run["ratio"] == ratio and
                                run["eviction_type"] == eviction_type and
                                run["device"] == device):
                                matching_run = run
                                break

                        # Load and compute statistics if run found
                        if matching_run:
                            data_file = matching_run["path"] / metric_file
                            if data_file.exists():
                                print(f"Processing: {distribution} {device} {EVICTION_TYPE_LABELS[eviction_type]} "
                                      f"chunk={CHUNK_SIZE_LABELS[chunk_size]} ratio=1:{ratio}", file=sys.stderr)

                                # Find eviction start time if filtering is enabled
                                eviction_start_time = None
                                if from_eviction_start:
                                    eviction_start_time = find_eviction_start_time(matching_run["path"])

                                # Load latency data
                                ts, vals = data_cache.load_metric_data(
                                    data_file,
                                    filter_minutes=filter_minutes,
                                    use_cache=True,
                                    sample_size=sample_size
                                )

                                # Filter from eviction start if enabled
                                if from_eviction_start and eviction_start_time is not None:
                                    mask = ts >= eviction_start_time
                                    ts = ts[mask]
                                    vals = vals[mask]

                                # Scale data
                                vals = vals * scale

                                # Compute statistics
                                if len(vals) > 0:
                                    stats = {
                                        'distribution': distribution,
                                        'device': device,
                                        'eviction_type': EVICTION_TYPE_LABELS[eviction_type],
                                        'chunk_size': CHUNK_SIZE_LABELS[chunk_size],
                                        'ratio': f"1:{ratio}",
                                        'count': len(vals),
                                        'min': np.min(vals),
                                        'max': np.max(vals),
                                        'mean': np.mean(vals),
                                        'median': np.median(vals),
                                        'std': np.std(vals),
                                        'p50': np.percentile(vals, 50),
                                        'p90': np.percentile(vals, 90),
                                        'p95': np.percentile(vals, 95),
                                        'p99': np.percentile(vals, 99),
                                        'p999': np.percentile(vals, 99.9)
                                    }
                                    results.append(stats)
                                else:
                                    print(f"  Warning: No data points after filtering", file=sys.stderr)
                            else:
                                print(f"Warning: Missing {metric_file} for {device} {eviction_type} "
                                      f"chunk={chunk_size} ratio={ratio}", file=sys.stderr)
                        else:
                            print(f"Warning: No run found for {device} {eviction_type} "
                                  f"chunk={chunk_size} dist={distribution} ratio={ratio}", file=sys.stderr)

    return results


# ============================================================================
# OUTPUT FORMATTING
# ============================================================================

def print_statistics_table(results):
    """Print statistics in a formatted text table."""

    # Print header
    header = f"{'Distribution':<12} {'Device':<6} {'Eviction':<12} {'Chunk':<8} {'Ratio':<6} " \
             f"{'Count':>8} {'Min':>8} {'Mean':>8} {'Median':>8} {'Max':>8} " \
             f"{'P90':>8} {'P95':>8} {'P99':>8} {'P99.9':>8}"
    print(header)
    print("=" * len(header))

    # Print data rows
    for stat in results:
        row = f"{stat['distribution']:<12} {stat['device']:<6} {stat['eviction_type']:<12} " \
              f"{stat['chunk_size']:<8} {stat['ratio']:<6} " \
              f"{stat['count']:>8} {stat['min']:>8.2f} {stat['mean']:>8.2f} " \
              f"{stat['median']:>8.2f} {stat['max']:>8.2f} " \
              f"{stat['p90']:>8.2f} {stat['p95']:>8.2f} {stat['p99']:>8.2f} {stat['p999']:>8.2f}"
        print(row)


def write_statistics_csv(results, output_file):
    """Write statistics to a CSV file."""

    fieldnames = ['distribution', 'device', 'eviction_type', 'chunk_size', 'ratio',
                  'count', 'min', 'max', 'mean', 'median', 'std',
                  'p50', 'p90', 'p95', 'p99', 'p999']

    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nStatistics written to: {output_file}", file=sys.stderr)


def print_summary_by_distribution(results):
    """Print summary statistics grouped by distribution."""

    for distribution in DISTRIBUTIONS:
        dist_results = [r for r in results if r['distribution'] == distribution]

        if not dist_results:
            continue

        print(f"\n{'='*80}")
        print(f"Distribution: {distribution}")
        print(f"{'='*80}")

        # Group by chunk size for easier reading
        for chunk_size in ["64KiB", "256MiB", "1077MiB"]:
            chunk_results = [r for r in dist_results if r['chunk_size'] == chunk_size]

            if not chunk_results:
                continue

            print(f"\n  Chunk Size: {chunk_size}")
            print(f"  {'-'*76}")

            for stat in chunk_results:
                print(f"    {stat['device']:<6} {stat['eviction_type']:<12} {stat['ratio']:<6} | "
                      f"Min: {stat['min']:>6.2f}ms  Mean: {stat['mean']:>6.2f}ms  "
                      f"Median: {stat['median']:>6.2f}ms  Max: {stat['max']:>7.2f}ms  "
                      f"P99: {stat['p99']:>6.2f}ms")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Compute and report latency statistics for OxCache runs."
    )
    parser.add_argument(
        '--block-dir',
        required=True,
        help='Path to block device directory (e.g., .../ssd-param-consolidated/)'
    )
    parser.add_argument(
        '--zns-dir',
        required=True,
        help='Path to ZNS device directory (e.g., .../zns-param-logs-consolidated/)'
    )
    parser.add_argument(
        '--output',
        help='Output CSV file for statistics (optional, prints to stdout if not provided)'
    )
    parser.add_argument(
        '--sample',
        type=int,
        default=None,
        help='Sample every Nth line from JSON files for faster testing'
    )
    parser.add_argument(
        '--from-eviction-start',
        action='store_true',
        help='Only compute statistics from when eviction begins (detected by peak usage_percentage)'
    )
    parser.add_argument(
        '--filter-minutes',
        type=int,
        default=5,
        help='Exclude last N minutes of data from each run (default: 5, use 0 for no filtering)'
    )
    parser.add_argument(
        '--format',
        choices=['table', 'summary', 'csv'],
        default='summary',
        help='Output format: table (detailed table), summary (grouped by distribution), or csv (CSV only)'
    )

    args = parser.parse_args()

    # Construct split_output paths
    block_split = Path(args.block_dir) / "split_output"
    zns_split = Path(args.zns_dir) / "split_output"

    # Verify directories exist
    if not block_split.exists():
        print(f"Error: Block split_output directory not found: {block_split}", file=sys.stderr)
        return 1

    if not zns_split.exists():
        print(f"Error: ZNS split_output directory not found: {zns_split}", file=sys.stderr)
        return 1

    # Convert filter_minutes=0 to None for data_cache
    filter_min = args.filter_minutes if args.filter_minutes > 0 else None

    print(f"\nComputing latency statistics...", file=sys.stderr)
    if args.from_eviction_start:
        print(f"  Filtering data from eviction start...", file=sys.stderr)
    if filter_min:
        print(f"  Excluding last {filter_min} minutes of data...", file=sys.stderr)

    # Compute statistics
    results = compute_latency_statistics(
        block_split,
        zns_split,
        from_eviction_start=args.from_eviction_start,
        filter_minutes=filter_min,
        sample_size=args.sample
    )

    if not results:
        print("\nNo statistics computed - no data found", file=sys.stderr)
        return 1

    print(f"\nComputed statistics for {len(results)} configurations", file=sys.stderr)

    # Output results
    if args.output:
        write_statistics_csv(results, args.output)

        # Also print to stdout if not in csv-only mode
        if args.format != 'csv':
            if args.format == 'table':
                print_statistics_table(results)
            else:  # summary
                print_summary_by_distribution(results)
    else:
        # Print to stdout based on format
        if args.format == 'table':
            print_statistics_table(results)
        elif args.format == 'summary':
            print_summary_by_distribution(results)
        elif args.format == 'csv':
            # Write CSV to stdout
            fieldnames = ['distribution', 'device', 'eviction_type', 'chunk_size', 'ratio',
                          'count', 'min', 'max', 'mean', 'median', 'std',
                          'p50', 'p90', 'p95', 'p99', 'p999']
            writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

    print("\n✓ Statistics computation complete!", file=sys.stderr)
    return 0


if __name__ == "__main__":
    exit(main())
