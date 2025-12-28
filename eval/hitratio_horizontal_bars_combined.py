#!/usr/bin/env python3
"""
Generate combined horizontal bar charts comparing final hit ratios across configurations.
Displays both ZIPFIAN and UNIFORM distributions side by side in one figure.
"""

import json
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
from matplotlib import rcParams

# Increase all font sizes by 8 points from their defaults
rcParams.update({key: rcParams[key] + 8 for key in rcParams if "size" in key and isinstance(rcParams[key], (int, float))})

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Chunk sizes to include for each eviction type (in bytes)
CHUNK_SIZES_CONFIG = {
    "promotional": [1129316352, 268435456, 65536],  # Zone LRU: 1077MiB, 256MiB, 64KiB
    "chunk": [268435456, 65536]                     # Chunk LRU: 256MiB, 64KiB
}

# Visual styling for chunk sizes
CHUNK_SIZE_LABELS = {
    1129316352: "1077MiB",
    268435456: "256MiB",
    65536: "64KiB"
}

# Distributions and ratios
DISTRIBUTIONS = ["ZIPFIAN", "UNIFORM"]
RATIOS = [2, 10]

# Device name mappings
DEVICE_MAPPINGS = {
    "nvme0n2": "ZNS",
    "nvme1n1": "Block"
}

# Eviction type labels
EVICTION_TYPE_LABELS = {
    "promotional": "Zone LRU",
    "chunk": "Chunk LRU"
}

# Bar colors for different devices (matching boxplot color scheme)
BAR_COLORS = {
    "Block": "lightblue",
    "ZNS": "lightcoral"
}

# Bar hatching patterns to distinguish eviction types
BAR_HATCHES = {
    "promotional": "",      # Solid for promotional
    "chunk": "\\\\"         # Hatched for chunk (matching boxplot pattern)
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
# HIT RATIO EXTRACTION
# ============================================================================

def get_final_hitratio(run_path):
    """
    Extract the final hit ratio from a run directory by reading only the last line.

    Args:
        run_path: Path object to the run directory

    Returns:
        float: Final hit ratio value (0.0 to 1.0), or None if file not found
    """
    hitratio_file = run_path / "hitratio.json"

    if not hitratio_file.exists():
        return None

    try:
        with open(hitratio_file, 'rb') as f:
            # Seek to end of file
            f.seek(0, 2)
            file_size = f.tell()

            if file_size == 0:
                return None

            # Read backwards to find the last complete line
            # Start from the end, skip any trailing newlines
            pos = file_size - 1

            # Skip trailing whitespace/newlines
            while pos >= 0:
                f.seek(pos)
                char = f.read(1)
                if char not in (b'\n', b'\r', b' ', b'\t'):
                    break
                pos -= 1

            if pos < 0:
                return None

            # Now read backwards to find the start of this line
            while pos >= 0:
                f.seek(pos)
                char = f.read(1)
                if char in (b'\n', b'\r'):
                    break
                pos -= 1

            # Read the last line
            if pos >= 0:
                f.seek(pos + 1)
            else:
                f.seek(0)

            last_line = f.read().decode('utf-8').strip()

            # Parse the JSON
            data = json.loads(last_line)
            if "fields" in data and "value" in data["fields"]:
                return data["fields"]["value"]

    except Exception as e:
        print(f"Warning: Error reading {hitratio_file}: {e}")
        return None

    return None


def collect_hitratio_data(block_runs, zns_runs):
    """
    Collect final hit ratios organized by distribution.

    Args:
        block_runs: List of run dicts from block directory
        zns_runs: List of run dicts from ZNS directory

    Returns:
        dict: Nested structure organizing data by distribution
        {
            "ZIPFIAN": [config_dicts...],
            "UNIFORM": [config_dicts...]
        }

        Each config_dict contains:
        {
            "chunk_size": int,
            "ratio": int,
            "block_promotional": float or None,
            "block_chunk": float or None,
            "zns_promotional": float or None,
            "zns_chunk": float or None,
            "avg_hitratio": float  # For sorting
        }
    """
    data_by_distribution = {dist: [] for dist in DISTRIBUTIONS}

    # Get all possible chunk sizes (union of promotional and chunk configs)
    all_chunk_sizes = set(CHUNK_SIZES_CONFIG["promotional"]) | set(CHUNK_SIZES_CONFIG["chunk"])

    for distribution in DISTRIBUTIONS:
        for chunk_size in all_chunk_sizes:
            for ratio in RATIOS:
                config = {
                    "chunk_size": chunk_size,
                    "ratio": ratio,
                    "block_promotional": None,
                    "block_chunk": None,
                    "zns_promotional": None,
                    "zns_chunk": None,
                }

                # Find matching runs for all 4 combinations
                # Block + promotional
                for run in block_runs:
                    if (run["chunk_size"] == chunk_size and
                        run["distribution"] == distribution and
                        run["ratio"] == ratio and
                        run["eviction_type"] == "promotional"):
                        config["block_promotional"] = get_final_hitratio(run["path"])
                        break

                # Block + chunk
                for run in block_runs:
                    if (run["chunk_size"] == chunk_size and
                        run["distribution"] == distribution and
                        run["ratio"] == ratio and
                        run["eviction_type"] == "chunk"):
                        config["block_chunk"] = get_final_hitratio(run["path"])
                        break

                # ZNS + promotional
                for run in zns_runs:
                    if (run["chunk_size"] == chunk_size and
                        run["distribution"] == distribution and
                        run["ratio"] == ratio and
                        run["eviction_type"] == "promotional"):
                        config["zns_promotional"] = get_final_hitratio(run["path"])
                        break

                # ZNS + chunk
                for run in zns_runs:
                    if (run["chunk_size"] == chunk_size and
                        run["distribution"] == distribution and
                        run["ratio"] == ratio and
                        run["eviction_type"] == "chunk"):
                        config["zns_chunk"] = get_final_hitratio(run["path"])
                        break

                # Calculate average for sorting (only from non-None values)
                values = [v for v in [config["block_promotional"], config["block_chunk"],
                                     config["zns_promotional"], config["zns_chunk"]]
                         if v is not None]

                if values:  # Only include configs with at least one value
                    config["avg_hitratio"] = sum(values) / len(values)
                    data_by_distribution[distribution].append(config)

        # Sort by fixed order: ratio descending, then chunk size ascending
        # This gives: (64KiB, R=10), (256MiB, R=10), (1077MiB, R=10), (64KiB, R=2), (256MiB, R=2), (1077MiB, R=2)
        data_by_distribution[distribution].sort(key=lambda x: (-x["ratio"], x["chunk_size"]))

    return data_by_distribution


# ============================================================================
# VISUALIZATION FUNCTION
# ============================================================================

def create_combined_horizontal_bar_chart(data_by_distribution, output_file):
    """
    Create a combined horizontal bar chart with both distributions side by side.

    Args:
        data_by_distribution: Dict with ZIPFIAN and UNIFORM data
        output_file: Path to save the figure
    """
    # Get max number of configs to ensure both subplots have same y-range
    max_configs = max(len(data_by_distribution["ZIPFIAN"]),
                      len(data_by_distribution["UNIFORM"]))

    if max_configs == 0:
        print("Warning: No data to plot")
        return

    # Calculate figure dimensions - half width, adjusted height to prevent overlap
    fig_height = max(6, max_configs * 0.7)  # Increased from 0.4 to 0.7 for more vertical space
    fig_width = 8  # Half the original width

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(fig_width, fig_height), sharey=False)

    # Bar dimensions and spacing - original thickness for readability
    bar_height = 0.15  # Original thickness
    group_spacing = 1.0  # Original spacing
    bar_spacing = 0.02  # Original spacing

    # Plot for each distribution
    axes = [ax1, ax2]
    for dist_idx, distribution in enumerate(DISTRIBUTIONS):
        ax = axes[dist_idx]
        distribution_data = data_by_distribution[distribution]

        if len(distribution_data) == 0:
            print(f"Warning: No data for {distribution}")
            ax.text(0.5, 0.5, f"No data for {distribution}",
                   ha='center', va='center', transform=ax.transAxes)
            continue

        # Y-axis setup
        y_ticks = []
        y_labels = []

        # Draw bars (reversed for proper display order)
        for i, config in enumerate(reversed(distribution_data)):
            y_base = i * group_spacing

            # Extract values (may be None)
            # Order: ZNS promotional, Block promotional, ZNS chunk, Block chunk
            values_and_labels = [
                (config.get("zns_promotional"), "ZNS", "promotional"),
                (config.get("block_promotional"), "Block", "promotional"),
                (config.get("zns_chunk"), "ZNS", "chunk"),
                (config.get("block_chunk"), "Block", "chunk"),
            ]

            # Draw each bar if value exists
            for j, (value, device, eviction) in enumerate(values_and_labels):
                if value is not None:
                    y_pos = y_base + j * (bar_height + bar_spacing)
                    color = BAR_COLORS[device]
                    hatch = BAR_HATCHES[eviction]

                    ax.barh(y_pos, value * 100, height=bar_height,
                           color=color, hatch=hatch, edgecolor='black', linewidth=0.5, alpha=0.99)

                    # Add percentage label to the right of the bar
                    percentage = value * 100
                    text_x = percentage + 1.5
                    ax.text(text_x, y_pos, f'{percentage:.1f}%',
                           va='center', ha='left', fontsize=7, color='black')

            # Y-tick at center of the 4-bar group
            y_center = y_base + 1.5 * (bar_height + bar_spacing)
            y_ticks.append(y_center)

            # Format label: just chunk size (ratio shown in box on left)
            chunk_label = CHUNK_SIZE_LABELS[config["chunk_size"]]
            y_labels.append(chunk_label)

        # Configure Y-axis
        ax.set_yticks(y_ticks)
        ax.set_yticklabels(y_labels, fontsize=11)

        # Configure X-axis - use fewer ticks to avoid overlap
        ax.set_xlabel("Hit Ratio (%)", fontsize=13)
        ax.set_xlim(0, 105)  # Extended to give room for labels
        ax.set_xticks(range(0, 101, 20))  # Every 20% instead of 10%
        ax.grid(axis='x', alpha=0.3, linestyle='--')

        # Add distribution title
        ax.set_title(distribution, fontsize=16, weight='bold')

        # Invert y-axis so highest hit ratio is at top
        ax.invert_yaxis()

    # Create legend (order: ZNS Zone-LRU, ZNS Chunk-LRU, Block Zone-LRU, Block Chunk-LRU)
    legend_handles = []
    for device in ["ZNS", "Block"]:
        for eviction in ["promotional", "chunk"]:
            color = BAR_COLORS[device]
            hatch = BAR_HATCHES[eviction]
            eviction_label = EVICTION_TYPE_LABELS[eviction]

            patch = mpatches.Patch(
                facecolor=color,
                hatch=hatch,
                edgecolor='black',
                label=f"{device} - {eviction_label}",
                alpha=0.99
            )
            legend_handles.append(patch)

    # Adjust layout with more space at bottom for legend and left for ratio boxes
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.18, wspace=0.45, left=0.22)

    # Make sure layout is finalized before getting positions
    fig.canvas.draw()

    # Add ratio grouping boxes on the left
    # Get the positions of the subplots
    ax1_bbox = ax1.get_position()

    # Calculate y positions for ratio groups
    # Assuming 6 configs total (3 for R=2, 3 for R=10)
    if len(data_by_distribution["ZIPFIAN"]) > 0 or len(data_by_distribution["UNIFORM"]) > 0:
        # We need to figure out where R=2 and R=10 groups are
        # Based on the fixed ordering, first 3 are R=2, next 3 are R=10
        num_r2 = sum(1 for c in data_by_distribution["ZIPFIAN"] if c["ratio"] == 2)
        num_r10 = sum(1 for c in data_by_distribution["ZIPFIAN"] if c["ratio"] == 10)

        if num_r2 > 0 and num_r10 > 0:
            # Calculate the vertical extent of each group based on subplot position
            # The bars are drawn from bottom to top in reversed order
            subplot_height = ax1_bbox.height
            total_configs = num_r2 + num_r10

            # R=10 group is sorted first, but after reverse() and invert_yaxis()
            # R=2 ends up at top of display, R=10 at bottom
            r10_height = (num_r10 / total_configs) * subplot_height
            r2_height = (num_r2 / total_configs) * subplot_height

            # Box dimensions - thinner, just enough for text
            box_width = 0.055
            box_x = 0.04  # Shifted right to avoid overlap with figure edge

            # R=2 box (top portion of plot - after inversion)
            r2_y_start = ax1_bbox.y0 + r10_height
            r2_box = plt.Rectangle(
                (box_x, r2_y_start),
                box_width,
                r2_height,
                transform=fig.transFigure,
                facecolor='lightgrey',
                edgecolor='black',
                linewidth=1.5,
                alpha=0.5,
                zorder=1,
                clip_on=False
            )
            fig.add_artist(r2_box)

            # R=10 box (bottom portion of plot - after inversion)
            r10_y_start = ax1_bbox.y0
            r10_box = plt.Rectangle(
                (box_x, r10_y_start),
                box_width,
                r10_height,
                transform=fig.transFigure,
                facecolor='lightgrey',
                edgecolor='black',
                linewidth=1.5,
                alpha=0.5,
                zorder=1,
                clip_on=False
            )
            fig.add_artist(r10_box)

            # Add text labels (matching distribution graph format)
            fig.text(
                box_x + box_width / 2,
                r2_y_start + r2_height / 2,
                "Ratio: 1:2",
                ha='center',
                va='center',
                fontsize=12,
                weight='bold',
                rotation=90,
                transform=fig.transFigure,
                zorder=2
            )

            fig.text(
                box_x + box_width / 2,
                r10_y_start + r10_height / 2,
                "Ratio: 1:10",
                ha='center',
                va='center',
                fontsize=12,
                weight='bold',
                rotation=90,
                transform=fig.transFigure,
                zorder=2
            )

    # Add legend at bottom center (below both subplots)
    fig.legend(handles=legend_handles, loc='lower center', fontsize=10,
              ncol=4, frameon=True, fancybox=True, shadow=True,
              bbox_to_anchor=(0.5, 0.01), columnspacing=1.0)

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate combined horizontal bar chart comparing final hit ratios across configurations."
    )
    parser.add_argument(
        '--block-dir',
        required=True,
        help='Path to block device directory containing split_output/'
    )
    parser.add_argument(
        '--zns-dir',
        required=True,
        help='Path to ZNS device directory containing split_output/'
    )
    parser.add_argument(
        '--output-dir',
        default='.',
        help='Output directory for plots (default: current directory)'
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

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect runs
    print("Collecting runs from directories...")
    block_runs = collect_runs(block_split)
    zns_runs = collect_runs(zns_split)
    print(f"Found {len(block_runs)} block runs and {len(zns_runs)} ZNS runs")

    # Collect and organize hit ratio data
    print("Extracting final hit ratios...")
    data_by_distribution = collect_hitratio_data(block_runs, zns_runs)

    # Generate combined chart
    output_file = output_dir / "hitratio_bars_combined.png"
    print("\nGenerating combined chart...")
    create_combined_horizontal_bar_chart(data_by_distribution, output_file)

    print("\nChart generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
