#!/usr/bin/env python3
"""
Generate distribution comparison ECDF plots for OxCache latency metrics.
Compares device types (Zoned vs Block) and eviction algorithms (Zone LRU vs Chunk LRU)
organized by distribution (ZIPFIAN vs UNIFORM).
"""

import json
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
from matplotlib import rcParams
from matplotlib.ticker import LogLocator, FuncFormatter, MaxNLocator
import numpy as np
from datetime import datetime, timedelta
import data_cache

# Increase all font sizes by 8 points from their defaults
rcParams.update({key: rcParams[key] + 8 for key in rcParams if "size" in key and isinstance(rcParams[key], (int, float))})

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Distributions
DISTRIBUTIONS = ["ZIPFIAN", "UNIFORM"]

# Chunk sizes (in bytes) - ordered for subplot arrangement
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

# Device colors (same as box plots)
DEVICE_COLORS = {
    "ZNS": "#a65628",
    "Block": "#f781bf"
}

# Line styles for eviction types
EVICTION_LINE_STYLE = {
    "promotional": "-",      # Solid line for Zone-LRU
    "chunk": "--"           # Dashed line for Chunk-LRU
}

# Line width
LINE_WIDTH = 3.5

# Device name mappings
DEVICE_MAPPINGS = {
    "nvme0n2": "ZNS",
    "nvme1n1": "Block"
}

# Supported latency metrics
LATENCY_METRICS = {
    "get_total": {
        "file": "get_total_latency_ms.json",
        "label": "Get Total Latency (ms)",
        "short_label": "Get Total"
    },
    "get_hit": {
        "file": "get_hit_latency_ms.json",
        "label": "Get Hit Latency (ms)",
        "short_label": "Get Hit"
    },
    "get_miss": {
        "file": "get_miss_latency_ms.json",
        "label": "Get Miss Latency (ms)",
        "short_label": "Get Miss"
    },
    "device_read": {
        "file": "device_read_latency_ms.json",
        "label": "Device Read Latency (ms)",
        "short_label": "Device Read"
    },
    "device_write": {
        "file": "device_write_latency_ms.json",
        "label": "Device Write Latency (ms)",
        "short_label": "Device Write"
    },
    "disk_read": {
        "file": "disk_read_latency_ms.json",
        "label": "Disk Read Latency (ms)",
        "short_label": "Disk Read"
    },
    "disk_write": {
        "file": "disk_write_latency_ms.json",
        "label": "Disk Write Latency (ms)",
        "short_label": "Disk Write"
    }
}

# ============================================================================
# DATA PARSING FUNCTIONS
# ============================================================================

def find_eviction_start_time(run_path):
    """
    Find the timestamp when eviction begins.

    Eviction starts at either:
    1. The peak right before the first reduction in usage
    2. OR if usage never reduces, where it first reaches its maximum value

    Args:
        run_path: Path to run directory containing usage_percentage.json

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

    # Look for the first decrease in usage
    for i in range(len(usage_values) - 1):
        if usage_values[i] > usage_values[i + 1]:
            # Found first decrease, eviction starts at this peak
            eviction_start_time = timestamps[i]
            print(f"    ✓ First decrease at index {i}/{len(usage_values)} "
                  f"(usage: {usage_values[i]:.6f} -> {usage_values[i+1]:.6f})")
            return eviction_start_time

    # No decrease found, find where we first reach maximum
    max_val = usage_values.max()
    first_max_idx = np.where(usage_values >= max_val)[0]

    if len(first_max_idx) > 0:
        eviction_start_index = first_max_idx[0]
        eviction_start_time = timestamps[eviction_start_index]
        print(f"    ✓ No decrease found, max reached at index {eviction_start_index}/{len(usage_values)} "
              f"(usage: {usage_values[eviction_start_index]:.6f})")
        return eviction_start_time

    print(f"    WARNING: Could not determine eviction start for {run_name}")
    return None


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
        device_part = parts[7].split("-")[0]  # Extract device name before timestamp

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
# ECDF GENERATION
# ============================================================================

def compute_ecdf(data):
    """
    Compute ECDF coordinates for a dataset.

    Args:
        data: NumPy array or list of values

    Returns:
        tuple: (x_values, y_values) for plotting ECDF
               x_values: sorted data points
               y_values: cumulative probabilities (0 to 1)
    """
    data = np.asarray(data)
    n = len(data)

    if n == 0:
        return np.array([]), np.array([])

    # Sort the data
    x = np.sort(data)

    # Compute cumulative probabilities
    y = np.arange(1, n + 1) / n

    return x, y


def generate_distribution_comparison(block_dir, zns_dir, distribution, output_file, metric_type="get_total", sample_size=None, from_eviction_start=False, filter_minutes=5, log_scale=False):
    """
    Generate distribution comparison ECDF plot for latency.

    Args:
        block_dir: Path to block device split_output directory
        zns_dir: Path to ZNS device split_output directory
        distribution: "ZIPFIAN" or "UNIFORM"
        output_file: Output filename for the plot
        metric_type: Type of latency metric to plot (from LATENCY_METRICS)
        sample_size: If provided, sample every Nth line from JSON files for faster processing
        from_eviction_start: If True, only include data from when eviction begins
        filter_minutes: Number of minutes to exclude from end of run (default: 5)
        log_scale: If True, use logarithmic scale for x-axis
    """
    # Get metric configuration
    if metric_type not in LATENCY_METRICS:
        raise ValueError(f"Unknown metric type: {metric_type}. Choose from: {list(LATENCY_METRICS.keys())}")

    metric_config = LATENCY_METRICS[metric_type]
    metric_file = metric_config["file"]
    metric_label = metric_config["label"]

    print(f"\nGenerating {distribution} {metric_config['short_label']} ECDF plot...")
    if from_eviction_start:
        print("  Filtering data from eviction start...")
    if filter_minutes:
        print(f"  Excluding last {filter_minutes} minutes of data...")
    if log_scale:
        print("  Using logarithmic x-axis scale...")

    scale = 1.0  # Already in ms

    # Collect runs from both directories
    block_runs = collect_runs(block_dir)
    zns_runs = collect_runs(zns_dir)

    print(f"Found {len(block_runs)} block runs and {len(zns_runs)} ZNS runs")

    # Create figure with 6 subplots (1 row x 6 columns)
    # Subplots are 2/5 original height, but all spacing preserved
    num_subplots = len(RATIOS) * len(CHUNK_SIZES)  # 2 ratios * 3 chunk sizes = 6
    fig, axes = plt.subplots(1, num_subplots, figsize=(5 * num_subplots, 5.38))

    # Ensure axes is always a list
    if num_subplots == 1:
        axes = [axes]

    idx = 0

    # Iterate through ratios, then chunk sizes
    for ratio in RATIOS:
        for chunk_size in CHUNK_SIZES:
            # Prepare data for this subplot
            current_ax = axes[idx]

            # Determine which eviction types to include
            if chunk_size == 1129316352:  # 1076MiB only has promotional
                eviction_types_to_use = ["promotional"]
            else:
                eviction_types_to_use = EVICTION_TYPES

            # For each device and eviction type combination
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

                    # Load data if run found
                    if matching_run:
                        data_file = matching_run["path"] / metric_file
                        if data_file.exists():
                            print(f"  Loading {metric_config['short_label']} for {device} {EVICTION_TYPE_LABELS[eviction_type]}...")

                            # Find eviction start time if filtering is enabled
                            eviction_start_time = None
                            if from_eviction_start:
                                eviction_start_time = find_eviction_start_time(matching_run["path"])

                            # Load latency data with filtering in one pass using shared cache
                            # Returns NumPy arrays
                            ts, vals = data_cache.load_metric_data(data_file,
                                                                   filter_minutes=filter_minutes,
                                                                   use_cache=True,
                                                                   sample_size=sample_size)

                            # Filter from eviction start if enabled
                            if from_eviction_start and eviction_start_time is not None:
                                mask = ts >= eviction_start_time
                                ts = ts[mask]
                                vals = vals[mask]
                                print(f"    Filtered to {len(vals)} points from eviction start")

                            # Scale data
                            vals = vals * scale

                            # Compute ECDF
                            x, y = compute_ecdf(vals)

                            # Plot ECDF
                            label = f"{device} {EVICTION_TYPE_LABELS[eviction_type]}"
                            color = DEVICE_COLORS[device]
                            linestyle = EVICTION_LINE_STYLE[eviction_type]

                            current_ax.plot(x, y * 100,  # Convert to percentage
                                          label=label,
                                          color=color,
                                          linestyle=linestyle,
                                          linewidth=LINE_WIDTH,
                                          alpha=0.8)

                            # Add p99 marker
                            if len(x) > 0 and len(y) > 0:
                                # Find p99 value (where y >= 0.99)
                                p99_idx = np.searchsorted(y, 0.99)
                                if p99_idx < len(x):
                                    p99_value = x[p99_idx]
                                    # Draw vertical line at p99 with same style as curve but thinner
                                    current_ax.axvline(x=p99_value, color=color,
                                                      linestyle=linestyle, linewidth=1.5, alpha=0.6)

                        else:
                            print(f"Warning: Missing {metric_file} for {device} {eviction_type} chunk={chunk_size} ratio={ratio}")
                    else:
                        print(f"Warning: No run found for {device} {eviction_type} chunk={chunk_size} dist={distribution} ratio={ratio}")

            # Configure subplot
            current_ax.set_xlabel(CHUNK_SIZE_LABELS[chunk_size], fontsize=16, weight='bold')
            current_ax.set_ylabel('Cumulative Probability (%)', fontsize=18)
            current_ax.set_ylim(0, 100)

            # Configure x-axis scale
            if log_scale:
                current_ax.set_xscale('log')
                # Use exponential notation for tick labels
                current_ax.xaxis.set_major_locator(LogLocator(base=10.0, numticks=10))
                # Don't show minor ticks to keep it clean
                current_ax.xaxis.set_minor_locator(plt.NullLocator())

                # Format tick labels to show exponents (e.g., 10^0, 10^1, etc.)
                def exp_formatter(x, pos):
                    if x == 0:
                        return '0'
                    exponent = int(np.log10(x))
                    # Show 10^n format
                    if x == 10**exponent:
                        if exponent == 0:
                            return '1'
                        elif exponent == 1:
                            return '10'
                        else:
                            return f'$10^{{{exponent}}}$'
                    else:
                        # For intermediate values, show the number
                        return f'{x:.0f}' if x >= 1 else f'{x:.1f}'

                current_ax.xaxis.set_major_formatter(FuncFormatter(exp_formatter))
            else:
                current_ax.set_xlim(left=0)
                # Use MaxNLocator to ensure nice, evenly-spaced tick intervals
                current_ax.xaxis.set_major_locator(MaxNLocator(nbins=6, integer=False, prune=None))

            current_ax.grid(True, alpha=0.3, linestyle='--')

            # Rotate y-axis labels
            for label in current_ax.get_yticklabels():
                label.set_rotation(45)

            idx += 1

    # Adjust layout - subplots at 2/5 height with proportional spacing
    plt.subplots_adjust(wspace=0.15, hspace=0.0)
    plt.tight_layout(pad=0.0)
    plt.subplots_adjust(top=0.851, bottom=0.279, left=0.05)

    # Make sure layout is finalized
    fig.canvas.draw()

    # Compute positions for ratio boxes and labels based on actual subplot bounds
    axes_bboxes = [ax.get_position().bounds for ax in axes]  # (x, y, w, h) per axes

    # Calculate the true center of all subplots
    leftmost = axes_bboxes[0][0]  # x position of first subplot
    rightmost = axes_bboxes[-1][0] + axes_bboxes[-1][2]  # x + width of last subplot
    subplot_center = (leftmost + rightmost) / 2

    # Create figure-level legend at bottom
    legend_lines = [
        Line2D([0], [0], color='#a65628', linestyle='-', linewidth=LINE_WIDTH,
               label='ZNS (Zone LRU)', alpha=0.8),
        Line2D([0], [0], color='#a65628', linestyle='--', linewidth=LINE_WIDTH,
               label='ZNS (Chunk LRU)', alpha=0.8),
        Line2D([0], [0], color='#f781bf', linestyle='-', linewidth=LINE_WIDTH,
               label='Block (Zone LRU)', alpha=0.8),
        Line2D([0], [0], color='#f781bf', linestyle='--', linewidth=LINE_WIDTH,
               label='Block (Chunk LRU)', alpha=0.8),
    ]
    fig.legend(ncols=4, handles=legend_lines, bbox_to_anchor=(subplot_center, 0.02),
               loc='center', fontsize="large", columnspacing=2.0, frameon=False)

    # Add a background box for the x-axis label to make it stand out
    label_y = 0.08
    label_width = 0.12
    label_height = 0.04
    fig.add_artist(
        Rectangle(
            (subplot_center - label_width/2, label_y - label_height/2),
            label_width,
            label_height,
            transform=fig.transFigure,
            facecolor='white',
            edgecolor='black',
            linewidth=1.5,
            zorder=10,
        )
    )

    # Add x-axis label at the bottom, centered over the subplots
    fig.text(subplot_center, label_y, 'Latency (ms)', ha='center', va='center',
             fontsize=18, weight='bold', zorder=11)

    # First 3 subplots -> Ratio 1:2, next 3 -> Ratio 1:10
    group1 = axes_bboxes[0:3]
    group2 = axes_bboxes[3:6]

    # Left/right bounds of each group
    g1_left = group1[0][0]
    g1_right = group1[-1][0] + group1[-1][2]
    g1_width = g1_right - g1_left

    g2_left = group2[0][0]
    g2_right = group2[-1][0] + group2[-1][2]
    g2_width = g2_right - g2_left

    # Vertical placement of the grey boxes in figure coords
    box_y = 0.93
    box_h = 0.06

    # Grey box for Ratio 1:2
    fig.add_artist(
        Rectangle(
            (g1_left, box_y),
            g1_width,
            box_h,
            transform=fig.transFigure,
            facecolor='lightgrey',
            edgecolor='black',
            linewidth=1.5,
            alpha=0.5,
            zorder=1,
        )
    )

    # Grey box for Ratio 1:10
    fig.add_artist(
        Rectangle(
            (g2_left, box_y),
            g2_width,
            box_h,
            transform=fig.transFigure,
            facecolor='lightgrey',
            edgecolor='black',
            linewidth=1.5,
            alpha=0.5,
            zorder=1,
        )
    )

    # Centered text in each box
    fig.text(
        g1_left + g1_width / 2,
        box_y + box_h / 2,
        "Ratio: 1:2",
        ha='center',
        va='center',
        fontsize=20,
        weight='bold',
        zorder=2,
    )
    fig.text(
        g2_left + g2_width / 2,
        box_y + box_h / 2,
        "Ratio: 1:10",
        ha='center',
        va='center',
        fontsize=20,
        weight='bold',
        zorder=2,
    )

    # Save figure
    plt.savefig(output_file, bbox_inches='tight', dpi=100)
    print(f"Saved: {output_file}")
    plt.close()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate distribution comparison ECDF graphs for OxCache latency metrics."
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
        '--from-eviction-start',
        action='store_true',
        help='Only plot data from when eviction begins (detected by peak usage_percentage)'
    )
    parser.add_argument(
        '--filter-minutes',
        type=int,
        default=5,
        help='Exclude last N minutes of data from each run (default: 5, use 0 for no filtering)'
    )
    parser.add_argument(
        '--metric',
        choices=list(LATENCY_METRICS.keys()),
        required=True,
        help='Latency metric to plot (default: get_total)'
    )
    parser.add_argument(
        '--log-scale',
        action='store_true',
        help='Use logarithmic scale for x-axis (latency)'
    )
    parser.add_argument(
        '--distribution',
        choices=['ZIPFIAN', 'UNIFORM', 'both'],
        default='both',
        help='Which distribution(s) to process (default: both)'
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
    filter_min = args.filter_minutes if args.filter_minutes > 0 else None

    # Get metric short label for filename
    metric_short = LATENCY_METRICS[args.metric]['short_label'].lower().replace(' ', '_')

    # Generate ECDF plots for selected distribution(s)
    all_graphs = [
        ("ZIPFIAN", f"zipfian_{metric_short}_ecdf.png"),
        ("UNIFORM", f"uniform_{metric_short}_ecdf.png"),
    ]

    # Filter based on --distribution argument
    if args.distribution == 'both':
        graphs = all_graphs
    else:
        graphs = [g for g in all_graphs if g[0] == args.distribution]

    for distribution, filename in graphs:
        output_file = output_dir / filename
        generate_distribution_comparison(block_split, zns_split, distribution, output_file, args.metric, args.sample, args.from_eviction_start, filter_min, args.log_scale)

    print("\n✓ All distribution comparison ECDF plots generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
