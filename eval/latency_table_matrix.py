#!/usr/bin/env python3
"""
Generate LaTeX table matrix for OxCache latency comparison.
Compares ZNS vs Block devices across multiple chunk sizes and distributions.
Data is normalized around ZNS device to show percentage increase for Block.
"""

import argparse
from pathlib import Path
import numpy as np
import data_cache
import re

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
        "label": "Total end-to-end latency request latency",
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
    "get_response": {
        "file": "get_response_latency_ms.json",
        "label": "Response latency",
    }
}

# ============================================================================
# DATA PARSING FUNCTIONS
# ============================================================================

def find_eviction_start_time(run_path, threshold=0.98):
    """
    Find the timestamp when eviction begins.

    Eviction starts when usage first crosses a high threshold (default 98%).
    This threshold-based approach is more robust and consistent across different
    workload types compared to detecting the first decrease.

    Args:
        run_path: Path to run directory containing usage_percentage.json
        threshold: Usage percentage threshold (0.0-1.0) to detect eviction start (default: 0.98)

    Returns:
        float: Unix timestamp when eviction starts, or None if no data available
    """
    usage_file = run_path / "usage_percentage.json"

    if not usage_file.exists():
        return None

    # Load usage_percentage data
    try:
        timestamps, usage_values = data_cache.load_metric_data(
            usage_file,
            filter_minutes=None,  # Don't filter, we need all data
            use_cache=True
        )
    except Exception as e:
        return None

    if len(usage_values) == 0:
        return None

    # Find first time usage crosses threshold
    above_threshold = np.where(usage_values >= threshold)[0]

    if len(above_threshold) > 0:
        eviction_start_index = above_threshold[0]
        eviction_start_time = timestamps[eviction_start_index]
        return eviction_start_time

    # Threshold never reached, find where we reach maximum instead
    max_val = usage_values.max()
    first_max_idx = np.where(usage_values >= max_val)[0]

    if len(first_max_idx) > 0:
        eviction_start_index = first_max_idx[0]
        eviction_start_time = timestamps[eviction_start_index]
        return eviction_start_time

    return None


def parse_directory_name(dirname):
    """
    Parse directory name to extract parameters.

    Format: chunk_size,L=...,DISTRIBUTION,R=...,I=...,NZ=...,eviction_type,device-timestamp
    Or: chunk_size,distribution,eviction_type,device-timestamp
    Or: chunk_size,L=...,NZ=...,eviction_type,device-timestamp

    Returns dict with: chunk_size, eviction_type, device, distribution (if present), ratio (if present)
    """
    parts = dirname.split(",")

    if len(parts) < 4:
        return None

    try:
        chunk_size = int(parts[0])

        # Determine format by checking parts
        distribution = None
        ratio = None
        eviction_idx = None

        # Format 1: chunk_size,L=...,DISTRIBUTION,R=...,I=...,NZ=...,eviction_type,device-timestamp
        if parts[1].startswith("L=") and len(parts) >= 7 and not parts[2].startswith(("R=", "I=", "NZ=")):
            distribution = parts[2].upper()
            # Extract ratio (R=...) parameter
            for part in parts:
                if part.startswith("R="):
                    ratio = int(part[2:])
                    break
            # Find eviction_type (should be second to last before device)
            eviction_idx = len(parts) - 2
        # Format 2: chunk_size,L=...,NZ=...,eviction_type,device-timestamp (WT workload)
        elif parts[1].startswith("L=") and len(parts) >= 5:
            eviction_idx = len(parts) - 2
        # Format 3: chunk_size,distribution,eviction_type,device-timestamp
        elif not parts[1].startswith("L="):
            distribution = parts[1].upper()
            eviction_idx = 2

        if eviction_idx is None:
            return None

        eviction_type = parts[eviction_idx]
        device_part = parts[eviction_idx + 1].split("-")[0]  # Extract device name before timestamp

        # Map device name to type
        device = None
        for dev_name, dev_type in DEVICE_MAPPINGS.items():
            if device_part.startswith(dev_name):
                device = dev_type
                break

        if device is None:
            return None

        result = {
            "chunk_size": chunk_size,
            "eviction_type": eviction_type,
            "device": device,
            "dirname": dirname
        }

        if distribution:
            result["distribution"] = distribution

        if ratio is not None:
            result["ratio"] = ratio

        return result
    except (ValueError, IndexError):
        return None


def collect_runs(base_dirs):
    """
    Collect all runs from multiple base directories.

    Args:
        base_dirs: List of base directory paths

    Returns list of dicts with run parameters and directory path.
    """
    runs = []

    for base_dir in base_dirs:
        base_path = Path(base_dir)
        if not base_path.exists():
            print(f"Warning: Directory does not exist: {base_dir}")
            continue

        split_path = base_path / "split_output"
        if not split_path.exists():
            print(f"Warning: split_output does not exist in: {base_dir}")
            continue

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


def load_metric_stats(run_path, metric_file, filter_minutes=None, sample_size=None,
                      eviction_start_time=None):
    """
    Load metric data and calculate statistics.

    Args:
        run_path: Path to run directory
        metric_file: Name of metric file to load
        filter_minutes: Number of minutes to exclude from end of run
        sample_size: If provided, sample every Nth line from JSON files
        eviction_start_time: If provided, only include data from this timestamp onward

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

    # Filter from eviction start if requested
    if eviction_start_time is not None and len(ts) > 0:
        mask = ts >= eviction_start_time
        vals = vals[mask]

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


def format_chunk_size(size_bytes):
    """Format chunk size for display."""
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes // (1024 * 1024)}M"
    elif size_bytes >= 1024:
        return f"{size_bytes // 1024}K"
    else:
        return str(size_bytes)


def generate_latex_table(runs, chunk_sizes, distributions, ratios, eviction_type, metric_key,
                         output_file, filter_minutes=None, sample_size=None, from_eviction_start=False):
    """
    Generate LaTeX table comparing ZNS and Block latencies across configurations.

    Args:
        runs: List of all runs
        chunk_sizes: List of chunk sizes to include
        distributions: List of distribution names to include
        ratios: List of ratio values to include (e.g., [2, 10])
        eviction_type: Eviction type to use ("promotional" or "chunk")
        metric_key: Metric key to include (from LATENCY_METRICS)
        output_file: Output file path for LaTeX table
        filter_minutes: Number of minutes to exclude from end of run
        sample_size: If provided, sample every Nth line from JSON files
        from_eviction_start: If True, only include data from when eviction begins (98% usage threshold)
    """
    print("\nGenerating latency comparison matrix table...")
    if filter_minutes:
        print(f"  Excluding last {filter_minutes} minutes of data...")
    if from_eviction_start:
        print(f"  Filtering data from eviction start (98% usage threshold)...")

    # Get metric configuration
    metric_config = LATENCY_METRICS[metric_key]
    metric_file = metric_config["file"]
    metric_label = metric_config["label"]
    eviction_label = EVICTION_TYPE_LABELS[eviction_type]

    print(f"\nProcessing {metric_label} ({eviction_label})...")

    # Normalize distribution names to uppercase for matching
    distributions_normalized = [d.upper() for d in distributions]

    # Collect statistics for all configurations
    # Structure: data[chunk_size][distribution][ratio][device] = stats
    data = {}

    for chunk_size in chunk_sizes:
        data[chunk_size] = {}
        for distribution in distributions_normalized:
            data[chunk_size][distribution] = {}
            for ratio in ratios:
                data[chunk_size][distribution][ratio] = {}

                # Find matching ZNS and Block runs
                for device in ["ZNS", "Block"]:
                    matching_run = None
                    for run in runs:
                        if (run.get("chunk_size") == chunk_size and
                            run.get("distribution") == distribution and
                            run.get("ratio") == ratio and
                            run.get("eviction_type") == eviction_type and
                            run.get("device") == device):
                            matching_run = run
                            break

                    if matching_run:
                        print(f"  Loading {device} - {format_chunk_size(chunk_size)} - {distribution} - R={ratio}...")

                        # Find eviction start time if filtering is enabled
                        eviction_start_time = None
                        if from_eviction_start:
                            eviction_start_time = find_eviction_start_time(matching_run["path"])
                            if eviction_start_time:
                                print(f"    Eviction detected at timestamp {eviction_start_time}")
                            else:
                                print(f"    Warning: Could not detect eviction start, using all data")

                        stats = load_metric_stats(matching_run["path"], metric_file,
                                                filter_minutes, sample_size, eviction_start_time)
                        data[chunk_size][distribution][ratio][device] = stats
                    else:
                        print(f"  Warning: No run found for {device} - {format_chunk_size(chunk_size)} - {distribution} - R={ratio}")
                        data[chunk_size][distribution][ratio][device] = None

    # Generate LaTeX table
    print(f"\nGenerating LaTeX table: {output_file}")

    with open(output_file, 'w') as f:
        # Write table header
        f.write("\\begin{table}[htbp]\n")
        f.write("\\centering\n")
        f.write(f"\\caption{{{metric_config["label"]} statistics for {eviction_label} eviction policy. Each entry reports ZNS / block-interface latency, with the percentage indicating the relative change when using the block-interface device. (ms)}}\n")
        f.write(f"\\label{{tab:latency_comparison_matrix_{metric_key}_{eviction_type}}}\n")

        # Number of columns: Statistic + (distributions × ratios)
        num_data_cols = len(distributions_normalized) * len(ratios)
        num_cols = 1 + num_data_cols
        col_spec = "|l|" + "c|" * num_data_cols

        # Add resizebox to scale table to page width
        f.write("\\resizebox{\\textwidth}{!}{%\n")
        f.write(f"\\begin{{tabular}}{{{col_spec}}}\n")
        f.write("\\hline\n")

        # Column headers - first row (distributions)
        f.write("\\textbf{Chunk / Stat}")
        for dist in distributions_normalized:
            if len(ratios) > 1:
                f.write(f" & \\multicolumn{{{len(ratios)}}}{{c|}}{{\\textbf{{{dist}}}}}")
            else:
                f.write(f" & \\textbf{{{dist}}}")
        f.write(" \\\\\n")

        # Column headers - second row (ratios) if there are multiple ratios
        if len(ratios) > 1:
            f.write("\\cline{2-" + str(num_cols) + "}\n")
            f.write(" ")
            for dist in distributions_normalized:
                for ratio in ratios:
                    f.write(f" & \\textbf{{R={ratio}}}")
            f.write(" \\\\\n")

        f.write("\\hline\n")

        # Statistics to display
        stats_list = [
            ('max', 'Max'),
            ('median', 'Median'),
            ('mean', 'Mean'),
            ('p99', 'P99')
        ]

        # Write data rows - for each chunk size
        for chunk_size in chunk_sizes:
            # Chunk size header
            chunk_label = format_chunk_size(chunk_size)
            f.write(f"\\multicolumn{{{num_cols}}}{{|c|}}{{\\textbf{{{chunk_label}}}}} \\\\\n")
            f.write("\\hline\n")

            # Statistics rows
            for stat_key, stat_label in stats_list:
                f.write(f"{stat_label}")

                for distribution in distributions_normalized:
                    for ratio in ratios:
                        zns_stats = data[chunk_size][distribution][ratio].get("ZNS")
                        block_stats = data[chunk_size][distribution][ratio].get("Block")

                        if zns_stats and block_stats:
                            zns_val = zns_stats[stat_key]
                            block_val = block_stats[stat_key]
                            pct = format_percentage_increase(zns_val, block_val)
                            f.write(f" & {format_stat_value(zns_val)} / {format_stat_value(block_val)} ({pct})")
                        else:
                            f.write(" & N/A")

                f.write(" \\\\\n")

            f.write("\\hline\n")

        # Close table
        f.write("\\end{tabular}\n")
        f.write("}%\n")  # Close resizebox
        f.write("\\end{table}\n")

    print(f"Saved: {output_file}")

    # Calculate and print summary statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)

    all_increases = []
    stat_increases = {'max': [], 'median': [], 'mean': [], 'p99': []}

    for chunk_size in chunk_sizes:
        for distribution in distributions_normalized:
            for ratio in ratios:
                zns_stats = data[chunk_size][distribution][ratio].get("ZNS")
                block_stats = data[chunk_size][distribution][ratio].get("Block")

                if zns_stats and block_stats:
                    for stat_key in ['max', 'median', 'mean', 'p99']:
                        zns_val = zns_stats[stat_key]
                        block_val = block_stats[stat_key]

                        if zns_val is not None and block_val is not None and zns_val > 0:
                            increase = ((block_val - zns_val) / zns_val) * 100
                            all_increases.append(increase)
                            stat_increases[stat_key].append(increase)

    if all_increases:
        print(f"\nOverall latency increase (Block vs ZNS):")
        print(f"  Average increase: {np.mean(all_increases):.1f}%")
        print(f"  Median increase:  {np.median(all_increases):.1f}%")
        print(f"  Min increase:     {np.min(all_increases):.1f}%")
        print(f"  Max increase:     {np.max(all_increases):.1f}%")

        print(f"\nBreakdown by statistic type:")
        for stat_name, increases in stat_increases.items():
            if increases:
                print(f"  {stat_name.upper():8s}: avg={np.mean(increases):6.1f}%, "
                      f"min={np.min(increases):6.1f}%, max={np.max(increases):6.1f}%")

        print(f"\nTotal comparisons: {len(all_increases)}")
        print(f"Configurations: {len(chunk_sizes)} chunk sizes × {len(distributions_normalized)} distributions × {len(ratios)} ratios")
    else:
        print("\nNo valid comparisons found to calculate summary statistics.")

    print("="*80)


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate LaTeX matrix table for OxCache latency comparison across chunk sizes and distributions."
    )
    parser.add_argument(
        '--data-dirs',
        nargs='+',
        required=True,
        help='Paths to data directories (e.g., .../Zipf-consolidated/ .../Normal-consolidated/)'
    )
    parser.add_argument(
        '--chunk-sizes',
        nargs='+',
        type=int,
        required=True,
        help='Chunk sizes to include (in bytes, e.g., 65536 262144 1077936128)'
    )
    parser.add_argument(
        '--distributions',
        nargs='+',
        required=True,
        help='Distribution names to include (e.g., zipf normal)'
    )
    parser.add_argument(
        '--ratios',
        nargs='+',
        type=int,
        required=True,
        help='Ratio values to include (e.g., 2 10)'
    )
    parser.add_argument(
        '--eviction',
        choices=EVICTION_TYPES,
        required=True,
        help='Eviction type to use (promotional=Zone LRU, chunk=Chunk LRU)'
    )
    parser.add_argument(
        '--output-file',
        default='latency_comparison_matrix.tex',
        help='Output file for LaTeX table (default: latency_comparison_matrix.tex)'
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
    parser.add_argument(
        '--from-eviction-start',
        action='store_true',
        help='Only include latency data from when eviction begins (detected by 98%% usage threshold)'
    )

    args = parser.parse_args()

    # Collect runs from all directories
    runs = collect_runs(args.data_dirs)

    print(f"Found {len(runs)} total runs")

    # Convert filter_minutes=0 to None for data_cache
    filter_min = args.filter_minutes if args.filter_minutes and args.filter_minutes > 0 else None

    # Generate LaTeX table
    generate_latex_table(runs, args.chunk_sizes, args.distributions, args.ratios,
                        args.eviction, args.metric, args.output_file, filter_min, args.sample,
                        args.from_eviction_start)

    print("\nLatency comparison matrix table generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
