#!/usr/bin/env python3
"""
Generate horizontal bar charts comparing final hit ratios across configurations.
Compares Block vs ZNS devices for different chunk sizes, ratios, and eviction types.
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
# DATA PARSING FUNCTIONS (copied from boxplot_graphs.py)
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
# NEW FUNCTIONS FOR HIT RATIO EXTRACTION
# ============================================================================

def get_final_hitratio(run_path):
    """
    Extract the final hit ratio from a run directory.

    Args:
        run_path: Path object to the run directory

    Returns:
        float: Final hit ratio value (0.0 to 1.0), or None if file not found
    """
    hitratio_file = run_path / "hitratio.json"

    if not hitratio_file.exists():
        return None

    last_value = None

    try:
        with open(hitratio_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    if "fields" in data and "value" in data["fields"]:
                        last_value = data["fields"]["value"]
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"Warning: Error reading {hitratio_file}: {e}")
        return None

    return last_value


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

        # Sort each distribution by average hit ratio (descending)
        data_by_distribution[distribution].sort(key=lambda x: x["avg_hitratio"], reverse=True)

    return data_by_distribution


# ============================================================================
# VISUALIZATION FUNCTION
# ============================================================================

def create_horizontal_bar_chart(distribution_data, distribution_name, output_file):
    """
    Create a horizontal bar chart for one distribution.

    Args:
        distribution_data: List of config dicts sorted by hit ratio
        distribution_name: "ZIPFIAN" or "UNIFORM"
        output_file: Path to save the figure
    """
    num_configs = len(distribution_data)

    if num_configs == 0:
        print(f"Warning: No data to plot for {distribution_name}")
        return

    # Calculate figure dimensions
    fig_height = max(8, num_configs * 0.8)
    fig_width = 12

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    # Bar dimensions and spacing
    bar_height = 0.15
    group_spacing = 1.0
    bar_spacing = 0.02

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
                text_x = percentage + 1
                ax.text(text_x, y_pos, f'{percentage:.1f}%',
                       va='center', ha='left', fontsize=10, color='black')

        # Y-tick at center of the 4-bar group
        y_center = y_base + 1.5 * (bar_height + bar_spacing)
        y_ticks.append(y_center)

        # Format label: "256MiB, R=2"
        chunk_label = CHUNK_SIZE_LABELS[config["chunk_size"]]
        ratio_label = f"R={config['ratio']}"
        y_labels.append(f"{chunk_label}, {ratio_label}")

    # Configure Y-axis
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels, fontsize=14)

    # Configure X-axis
    ax.set_xlabel("Hit Ratio (%)", fontsize=16)
    ax.set_xlim(0, 100)
    ax.set_xticks(range(0, 101, 10))
    ax.grid(axis='x', alpha=0.3, linestyle='--')

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

    # Invert y-axis so highest hit ratio is at top
    ax.invert_yaxis()

    # Adjust layout to make room for legend at bottom
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)

    # Add legend at bottom center (below the x-axis label)
    fig.legend(handles=legend_handles, loc='lower center', fontsize=12,
              ncol=4, frameon=True, fancybox=True, shadow=True,
              bbox_to_anchor=(0.5, -0.02))

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate horizontal bar charts comparing final hit ratios across configurations."
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

    # Generate chart for each distribution
    for distribution in DISTRIBUTIONS:
        if distribution not in data_by_distribution:
            print(f"Warning: No data found for {distribution}")
            continue

        configs = data_by_distribution[distribution]
        if not configs:
            print(f"Warning: No configurations found for {distribution}")
            continue

        output_file = output_dir / f"hitratio_bars_{distribution}.png"
        print(f"\nGenerating {distribution} chart...")
        create_horizontal_bar_chart(configs, distribution, output_file)

    print("\nAll charts generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
