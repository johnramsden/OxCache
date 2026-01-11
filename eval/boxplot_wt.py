#!/usr/bin/env python3
"""
Generate boxplot graph for OxCache WT workload throughput.
Compares ZNS vs Block devices for chunk and promotional eviction types.
"""

import argparse
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
from matplotlib import rcParams
from matplotlib import ticker
import data_cache

# Increase all font sizes by 16 points from their defaults
rcParams.update({key: rcParams[key] + 16 for key in rcParams if "size" in key and isinstance(rcParams[key], (int, float))})

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Device name mappings
DEVICE_MAPPINGS = {
    "nvme0n2": "ZNS",
    "nvme1n1": "Block"
}

# Eviction types
EVICTION_TYPES = ["promotional", "chunk"]  # Zone-LRU, Chunk-LRU

EVICTION_TYPE_LABELS = {
    "promotional": "Zone-LRU",
    "chunk": "Chunk-LRU"
}

# Device colors (matching distribution_comparison_boxplots.py)
DEVICE_COLORS = {
    "ZNS": "#a65628",
    "Block": "#f781bf"
}

# Eviction type hatching
EVICTION_HATCH = {
    "promotional": "",      # No hatch for Zone-LRU
    "chunk": "///"         # Hash marks for Chunk-LRU
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

    # Extract run info from directory name for logging
    run_name = run_path.name
    print(f"    Analyzing eviction for: {run_name}")

    if not usage_file.exists():
        print(f"    WARNING: No usage_percentage.json found in {run_name}")
        return None

    # Load usage_percentage data
    try:
        timestamps, usage_values = data_cache.load_metric_data(
            usage_file,
            filter_minutes=None,  # Don't filter, we need all data
            use_cache=True
        )
    except Exception as e:
        print(f"    WARNING: Failed to load usage_percentage from {run_name}: {e}")
        return None

    if len(usage_values) == 0:
        print(f"    WARNING: No usage data found in {run_name}")
        return None

    import numpy as np

    # Find first time usage crosses threshold
    above_threshold = np.where(usage_values >= threshold)[0]

    if len(above_threshold) > 0:
        eviction_start_index = above_threshold[0]
        eviction_start_time = timestamps[eviction_start_index]
        print(f"    ✓ Threshold {threshold} reached at index {eviction_start_index}/{len(usage_values)} "
              f"(usage: {usage_values[eviction_start_index]:.6f})")
        return eviction_start_time

    # Threshold never reached, find where we reach maximum instead
    max_val = usage_values.max()
    first_max_idx = np.where(usage_values >= max_val)[0]

    if len(first_max_idx) > 0:
        eviction_start_index = first_max_idx[0]
        eviction_start_time = timestamps[eviction_start_index]
        print(f"    ✓ Threshold not reached, max {max_val:.6f} at index {eviction_start_index}/{len(usage_values)}")
        return eviction_start_time

    print(f"    WARNING: Could not determine eviction start for {run_name}")
    return None


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


def match_runs(block_runs, zns_runs, eviction_type):
    """
    Find matching block and ZNS runs for given eviction type.

    Returns tuple of (block_run, zns_run) or (None, None) if not found.
    """
    block_run = None
    zns_run = None

    for run in block_runs:
        if run["eviction_type"] == eviction_type and run["device"] == "Block":
            block_run = run
            break

    for run in zns_runs:
        if run["eviction_type"] == eviction_type and run["device"] == "ZNS":
            zns_run = run
            break

    return block_run, zns_run


# ============================================================================
# BOXPLOT GENERATION
# ============================================================================

def generate_boxplot(block_dir, zns_dir, output_file, sample_size=None, show_outliers=False, from_eviction_start=False, filter_minutes=None):
    """
    Generate single boxplot comparing ZNS vs Block for throughput across eviction types.

    Args:
        block_dir: Path to block device split_output directory
        zns_dir: Path to ZNS device split_output directory
        output_file: Output filename for the plot
        sample_size: If provided, sample every Nth line from JSON files for faster processing
        show_outliers: If True, display outliers beyond 1.5×IQR from quartiles
        from_eviction_start: If True, only include data from when eviction begins
        filter_minutes: Number of minutes to exclude from end of run
    """
    print(f"\nGenerating WT throughput boxplot...")
    if from_eviction_start:
        print("  Filtering data from eviction start...")
    if filter_minutes:
        print(f"  Excluding last {filter_minutes} minutes of data...")

    metric_file = "client_request_bytes_total.json"
    metric_label = "Throughput (MiB/s)"
    scale = 1 / (2**20)  # Convert bytes to MiB

    # Collect runs from both directories
    block_runs = collect_runs(block_dir)
    zns_runs = collect_runs(zns_dir)

    print(f"Found {len(block_runs)} block runs and {len(zns_runs)} ZNS runs")

    # Create figure with single subplot
    fig, ax = plt.subplots(1, 1, figsize=(5, 5.38))

    # Prepare data: 4 boxes (ZNS-promotional, ZNS-chunk, Block-promotional, Block-chunk)
    current_data = []
    labels = []
    colors = []
    hatches = []

    # For each device and eviction type combination
    for device in ["ZNS", "Block"]:
        for eviction_type in EVICTION_TYPES:
            # Find matching run
            runs_to_search = zns_runs if device == "ZNS" else block_runs
            matching_run = None

            for run in runs_to_search:
                if (run["eviction_type"] == eviction_type and
                    run["device"] == device):
                    matching_run = run
                    break

            # Load data if run found
            if matching_run:
                data_file = matching_run["path"] / metric_file
                if data_file.exists():
                    print(f"  Loading {metric_file} for {device} {EVICTION_TYPE_LABELS[eviction_type]}...")

                    # Find eviction start time if filtering is enabled
                    eviction_start_time = None
                    if from_eviction_start:
                        eviction_start_time = find_eviction_start_time(matching_run["path"])

                    # Load with timestamps and calculate throughput using data_cache
                    ts, vals = data_cache.load_metric_data(
                        data_file,
                        filter_minutes=filter_minutes,
                        use_cache=True,
                        sample_size=sample_size
                    )

                    # Filter from eviction start if enabled
                    if from_eviction_start and eviction_start_time is not None:
                        import numpy as np
                        mask = ts >= eviction_start_time
                        ts = ts[mask]
                        vals = vals[mask]
                        print(f"    Filtered to {len(vals)} points from eviction start")

                    # Calculate throughput in 60-second bins (vectorized)
                    throughput = data_cache.calculate_throughput_bins(ts, vals, bin_seconds=60)
                    # Scale and convert to list for matplotlib
                    current_data.append((throughput * scale).tolist())
                else:
                    print(f"Warning: Missing {metric_file} for {device} {eviction_type}")
                    current_data.append([])
            else:
                print(f"Warning: No run found for {device} {eviction_type}")
                current_data.append([])

            # Add styling info
            labels.append(f"{device}-{EVICTION_TYPE_LABELS[eviction_type]}")
            colors.append(DEVICE_COLORS[device])
            hatches.append(EVICTION_HATCH[eviction_type])

    # Create boxplot (identical to distribution_comparison_boxplots.py)
    if current_data:
        # Adjust box width based on number of boxes so they're consistent across subplots
        # Base width is 0.8 for 4 boxes, scale proportionally for fewer boxes
        num_boxes = len(current_data)
        box_width = 0.8 * (num_boxes / 4) if num_boxes > 0 else 0.8

        bp = ax.boxplot(current_data,
                        showfliers=show_outliers,
                        widths=box_width,
                        medianprops=dict(linewidth=2, color='black'),
                        patch_artist=True)

        # Apply colors and hatches
        for i, (box, color, hatch) in enumerate(zip(bp['boxes'], colors, hatches)):
            box.set_facecolor(color)
            box.set_hatch(hatch)
            box.set_hatch_linewidth(3.0)
            box.set_alpha(0.7)

        # Set x-axis labels (empty for cleaner look)
        ax.set_xticks(range(1, len(labels) + 1))
        ax.set_xticklabels([], rotation=45, fontsize=10)

        # Add label below subplot
        # ax.set_xlabel("64KiB", fontsize=16, weight='bold')

        # Set y-axis label
        ax.set_ylabel(metric_label, fontsize=22, weight='bold')

        # Use scalar formatter without scientific notation
        ax.yaxis.set_major_formatter(ticker.ScalarFormatter(useOffset=False, useMathText=False))

        # Rotate y-axis labels
        for label in ax.get_yticklabels():
            label.set_rotation(45)

        # Set y-axis range (start at 0)
        ax.set_ylim(bottom=0)

    # Add legend at bottom (matching distribution_comparison_boxplots.py)
    legend_patches = [
        mpatches.Patch(facecolor='#a65628', label='ZNS (Zone LRU)', alpha=0.7),
        mpatches.Patch(facecolor='#a65628', hatch='///', label='ZNS (Chunk LRU)', alpha=0.7),
        mpatches.Patch(facecolor='#f781bf', label='Block (Zone LRU)', alpha=0.7),
        mpatches.Patch(facecolor='#f781bf', hatch='///', label='Block (Chunk LRU)', alpha=0.7),
    ]

    fig.legend(
        ncols=4,
        handles=legend_patches,
        bbox_to_anchor=(0.5, 0.08),
        loc='center',
        fontsize="large",
        columnspacing=2.0,
        frameon=False
    )

    # Adjust layout (matching distribution_comparison_boxplots.py proportions)
    plt.tight_layout(pad=0.0)
    plt.subplots_adjust(top=0.851, bottom=0.279)

    # Save figure
    plt.savefig(output_file, bbox_inches='tight', dpi=100)
    print(f"Saved: {output_file}")
    plt.close()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate throughput boxplot comparing ZNS vs Block devices for OxCache WT workload."
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
        '--output-dir',
        default='.',
        help='Output directory for plots (default: current directory)'
    )
    parser.add_argument(
        '--sample',
        type=int,
        default=None,
        help='Sample every Nth line from JSON files for faster testing (e.g., --sample 100)'
    )
    parser.add_argument(
        '--show-outliers',
        action='store_true',
        help='Show outliers in box plots (points beyond 1.5×IQR from quartiles)'
    )
    parser.add_argument(
        '--from-eviction-start',
        action='store_true',
        help='Only plot data from when eviction begins (detected by usage_percentage peak)'
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
    output_dir = Path(args.output_dir)

    # Verify directories exist
    if not block_split.exists():
        print(f"Error: Block split_output directory not found: {block_split}")
        return 1

    if not zns_split.exists():
        print(f"Error: ZNS split_output directory not found: {zns_split}")
        return 1

    # Create output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert filter_minutes=0 to None for data_cache
    filter_min = args.filter_minutes if args.filter_minutes and args.filter_minutes > 0 else None

    # Generate throughput boxplot
    output_file = output_dir / "wt_throughput.png"
    generate_boxplot(block_split, zns_split, output_file, args.sample, args.show_outliers,
                     args.from_eviction_start, filter_min)

    print("\nWT throughput boxplot generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
