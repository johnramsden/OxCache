#!/usr/bin/env python3
"""
Generate horizontal bar chart comparing final hit ratios for WT data.
Compares Block vs ZNS devices for one chunk size and two eviction types.
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

# Visual styling for chunk sizes
CHUNK_SIZE_LABELS = {
    65536: "64KiB"
}

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
    "Block": "#f781bf",
    "ZNS": "#a65628"
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
    Parse directory name to extract parameters for WT data.

    Format: chunk_size,L=...,NZ=...,eviction_type,device-timestamp-run

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
    Collect final hit ratios for both eviction types.

    Args:
        block_runs: List of run dicts from block directory
        zns_runs: List of run dicts from ZNS directory

    Returns:
        dict with structure:
        {
            "block_promotional": float or None,
            "block_chunk": float or None,
            "zns_promotional": float or None,
            "zns_chunk": float or None,
        }
    """
    data = {
        "block_promotional": None,
        "block_chunk": None,
        "zns_promotional": None,
        "zns_chunk": None,
    }

    # Find matching runs
    for run in block_runs:
        if run["eviction_type"] == "promotional":
            data["block_promotional"] = get_final_hitratio(run["path"])
        elif run["eviction_type"] == "chunk":
            data["block_chunk"] = get_final_hitratio(run["path"])

    for run in zns_runs:
        if run["eviction_type"] == "promotional":
            data["zns_promotional"] = get_final_hitratio(run["path"])
        elif run["eviction_type"] == "chunk":
            data["zns_chunk"] = get_final_hitratio(run["path"])

    return data


# ============================================================================
# VISUALIZATION FUNCTION
# ============================================================================

def create_horizontal_bar_chart(data, output_file):
    """
    Create a horizontal bar chart comparing hit ratios.

    Args:
        data: Dict with block_promotional, block_chunk, zns_promotional, zns_chunk
        output_file: Path to save the figure
    """
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 4))

    # Bar dimensions and spacing
    bar_height = 0.2
    y_pos = 0.5  # Single group centered
    bar_spacing = 0.05

    # Order: ZNS promotional, Block promotional, ZNS chunk, Block chunk
    values_and_labels = [
        (data.get("zns_promotional"), "ZNS", "promotional"),
        (data.get("block_promotional"), "Block", "promotional"),
        (data.get("zns_chunk"), "ZNS", "chunk"),
        (data.get("block_chunk"), "Block", "chunk"),
    ]

    # Draw each bar if value exists
    for i, (value, device, eviction) in enumerate(values_and_labels):
        if value is not None:
            y = y_pos + i * (bar_height + bar_spacing)
            color = BAR_COLORS[device]
            hatch = BAR_HATCHES[eviction]

            ax.barh(y, value * 100, height=bar_height,
                   color=color, hatch=hatch, edgecolor='black', linewidth=0.5, alpha=0.99)

            # Add percentage label to the right of the bar
            percentage = value * 100
            text_x = percentage + 1
            ax.text(text_x, y, f'{percentage:.1f}%',
                   va='center', ha='left', fontsize=12, color='black')

    # Configure Y-axis (hide it since we only have one group)
    ax.set_yticks([])
    ax.set_ylim(0, 2)

    # Configure X-axis
    ax.set_xlabel("Hit Ratio (%)", fontsize=16)
    ax.set_xlim(0, 100)
    ax.set_xticks(range(0, 101, 10))
    ax.grid(axis='x', alpha=0.3, linestyle='--')

    # Add title
    ax.set_title("Write-Through Cache Hit Ratio Comparison", fontsize=18, weight='bold', pad=20)

    # Create legend
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

    # Add legend at bottom center
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.25)

    fig.legend(handles=legend_handles, loc='lower center', fontsize=12,
              ncol=4, frameon=True, fancybox=True, shadow=True,
              bbox_to_anchor=(0.5, 0.02))

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate horizontal bar chart comparing final hit ratios for WT data."
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
    data = collect_hitratio_data(block_runs, zns_runs)

    # Print collected data
    print("\nCollected hit ratios:")
    for key, value in data.items():
        if value is not None:
            print(f"  {key}: {value*100:.1f}%")
        else:
            print(f"  {key}: Not found")

    # Generate chart
    output_file = output_dir / "hitratio_bars_wt.png"
    print(f"\nGenerating chart...")
    create_horizontal_bar_chart(data, output_file)

    print("\nChart generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
