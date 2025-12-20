#!/usr/bin/env python3
"""
Generate distribution comparison boxplots for OxCache performance metrics.
Compares device types (Zoned vs Block) and eviction algorithms (Zone LRU vs Chunk LRU)
organized by distribution (ZIPFIAN vs UNIFORM).
"""

import json
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
from matplotlib.patches import Rectangle
from matplotlib import rcParams
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

# Device colors (using lighter colors from original boxplot_graphs.py)
DEVICE_COLORS = {
    "ZNS": "lightcoral",
    "Block": "lightblue"
}

# Eviction type hatching
EVICTION_HATCH = {
    "promotional": "",      # No hatch for Zone-LRU
    "chunk": "///"         # Hash marks for Chunk-LRU
}

# Device name mappings
DEVICE_MAPPINGS = {
    "nvme0n2": "ZNS",
    "nvme1n1": "Block"
}

# ============================================================================
# DATA PARSING FUNCTIONS
# ============================================================================

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
# BOXPLOT GENERATION
# ============================================================================

def generate_distribution_comparison(block_dir, zns_dir, distribution, metric, output_file, sample_size=None):
    """
    Generate distribution comparison boxplot for a specific distribution and metric.

    Args:
        block_dir: Path to block device split_output directory
        zns_dir: Path to ZNS device split_output directory
        distribution: "ZIPFIAN" or "UNIFORM"
        metric: "latency" or "throughput"
        output_file: Output filename for the plot
        sample_size: If provided, sample every Nth line from JSON files for faster processing
    """
    print(f"\nGenerating {distribution} {metric} comparison boxplot...")

    # Determine metric filename and scale
    if metric == "latency":
        metric_file = "get_total_latency_ms.json"
        metric_label = "Latency (ms)"
        scale = 1.0  # Already in ms
    elif metric == "throughput":
        metric_file = "bytes_total.json"
        metric_label = "Throughput (GiB/s)"
        scale = 1 / (2**30)  # Convert bytes to GiB
    else:
        raise ValueError(f"Unknown metric: {metric}")

    # Collect runs from both directories
    block_runs = collect_runs(block_dir)
    zns_runs = collect_runs(zns_dir)

    print(f"Found {len(block_runs)} block runs and {len(zns_runs)} ZNS runs")

    # Create figure with 6 subplots (1 row x 6 columns)
    num_subplots = len(RATIOS) * len(CHUNK_SIZES)  # 2 ratios * 3 chunk sizes = 6
    fig, axes = plt.subplots(1, num_subplots, figsize=(5 * num_subplots, 10))

    # Ensure axes is always a list
    if num_subplots == 1:
        axes = [axes]

    idx = 0

    # Iterate through ratios, then chunk sizes
    for ratio in RATIOS:
        for chunk_size in CHUNK_SIZES:
            # Prepare data for this subplot
            current_data = []
            labels = []
            colors = []
            hatches = []

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
                            print(f"  Loading {metric_file} for {device} {EVICTION_TYPE_LABELS[eviction_type]}...")
                            if metric == "throughput":
                                # Load with timestamps and filter in one pass using shared cache
                                # Returns NumPy arrays for efficient processing
                                ts, vals = data_cache.load_metric_data(data_file,
                                                                       filter_minutes=5,
                                                                       use_cache=True,
                                                                       sample_size=sample_size)
                                # Calculate throughput in 60-second bins (vectorized)
                                throughput = data_cache.calculate_throughput_bins(ts, vals, bin_seconds=60)
                                # Scale and convert to list for matplotlib
                                current_data.append((throughput * scale).tolist())
                            else:
                                # Load latency data with filtering in one pass using shared cache
                                # Returns NumPy arrays
                                ts, vals = data_cache.load_metric_data(data_file,
                                                                       filter_minutes=5,
                                                                       use_cache=True,
                                                                       sample_size=sample_size)
                                # Scale and convert to list for matplotlib
                                current_data.append((vals * scale).tolist())

                            # Add styling info
                            labels.append(f"{device}-{EVICTION_TYPE_LABELS[eviction_type]}")
                            colors.append(DEVICE_COLORS[device])
                            hatches.append(EVICTION_HATCH[eviction_type])
                        else:
                            print(f"Warning: Missing {metric_file} for {device} {eviction_type} chunk={chunk_size} ratio={ratio}")
                            current_data.append([])
                            labels.append(f"{device}-{EVICTION_TYPE_LABELS[eviction_type]}")
                            colors.append(DEVICE_COLORS[device])
                            hatches.append(EVICTION_HATCH[eviction_type])
                    else:
                        print(f"Warning: No run found for {device} {eviction_type} chunk={chunk_size} dist={distribution} ratio={ratio}")

            # Create boxplot for this subplot
            if current_data:
                bp = axes[idx].boxplot(current_data,
                                      showfliers=False,
                                      widths=0.8,
                                      medianprops=dict(linewidth=2, color='black'),
                                      patch_artist=True)

                # Apply colors and hatches
                for i, (box, color, hatch) in enumerate(zip(bp['boxes'], colors, hatches)):
                    box.set_facecolor(color)
                    box.set_hatch(hatch)
                    box.set_alpha(0.7)

                # Set x-axis labels (empty for cleaner look, or could add device labels)
                axes[idx].set_xticks(range(1, len(labels) + 1))
                axes[idx].set_xticklabels([], rotation=45, fontsize=10)

                # Add chunk size label below subplot
                axes[idx].set_xlabel(CHUNK_SIZE_LABELS[chunk_size], fontsize=16, weight='bold')

                # Rotate y-axis labels
                for label in axes[idx].get_yticklabels():
                    label.set_rotation(45)

            idx += 1

    # Add y-axis label on the far left
    fig.text(0.02, 0.5, metric_label, va='center', rotation='vertical', fontsize=22, weight='bold')

    # Adjust layout (do these BEFORE computing positions)
    plt.subplots_adjust(wspace=0.05, hspace=0.0)
    plt.tight_layout(pad=0.0)
    plt.subplots_adjust(top=0.92, bottom=0.15, left=0.05)

    # Make sure layout is finalized
    fig.canvas.draw()

    # Compute positions for ratio boxes and labels based on actual subplot bounds
    axes_bboxes = [ax.get_position().bounds for ax in axes]  # (x, y, w, h) per axes

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
    box_h = 0.05

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

    # Add legend at bottom
    legend_patches = [
        mpatches.Patch(facecolor='lightcoral', label='ZNS (Zone LRU)', alpha=0.7),
        mpatches.Patch(facecolor='lightcoral', hatch='///', label='ZNS (Chunk LRU)', alpha=0.7),
        mpatches.Patch(facecolor='lightblue', label='Block (Zone LRU)', alpha=0.7),
        mpatches.Patch(facecolor='lightblue', hatch='///', label='Block (Chunk LRU)', alpha=0.7),
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

    # Save figure
    plt.savefig(output_file, bbox_inches='tight', dpi=100)
    print(f"Saved: {output_file}")
    plt.close()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate distribution comparison boxplot graphs for OxCache performance metrics."
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

    # Generate all 4 graphs (2 distributions × 2 metrics)
    graphs = [
        ("ZIPFIAN", "latency", "zipfian_latency.png"),
        # ("ZIPFIAN", "throughput", "zipfian_throughput.png"),
        ("UNIFORM", "latency", "uniform_latency.png"),
        # ("UNIFORM", "throughput", "uniform_throughput.png"),
    ]

    for distribution, metric, filename in graphs:
        output_file = output_dir / filename
        generate_distribution_comparison(block_split, zns_split, distribution, metric, output_file, args.sample)

    print("\n✓ All distribution comparison boxplots generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
