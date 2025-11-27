#!/usr/bin/env python3
"""
Generate boxplot graphs for OxCache performance metrics.
Compares ZNS vs Block devices across different configurations.
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
    "promotional": [1129316352, 268435456, 65536],  # Zone LRU: 1076MiB, 256MiB, 64KiB
    "chunk": [268435456, 65536]                     # Chunk LRU: 256MiB, 64KiB
}

# Visual styling for chunk sizes
CHUNK_SIZE_COLORS = {
    1129316352: "lightcoral",    # 1077MiB
    268435456: "lightgreen",     # 256MiB
    65536: "lightblue"           # 64KiB
}

CHUNK_SIZE_LABELS = {
    1129316352: "1077MiB",
    268435456: "256MiB",
    65536: "64KiB"
}

# Distributions and their hatching patterns
DISTRIBUTIONS = ["ZIPFIAN", "UNIFORM"]
DISTRIBUTION_HATCHES = {
    "ZIPFIAN": "O",
    "UNIFORM": "\\"
}

# Ratios to include
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


def load_json_data(filepath, sample_size=None, include_timestamps=False):
    """
    Load JSON Lines data and extract values.

    Args:
        filepath: Path to JSON Lines file
        sample_size: If provided, sample every Nth line for faster testing
        include_timestamps: If True, return (timestamps, values) tuple

    Returns list of values or (timestamps, values) tuple if include_timestamps=True.
    """
    values = []
    timestamps = []
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f):
            # Skip lines if sampling
            if sample_size and line_num % sample_size != 0:
                continue

            try:
                data = json.loads(line.strip())
                if "fields" in data and "value" in data["fields"]:
                    values.append(data["fields"]["value"])
                    if include_timestamps and "timestamp" in data:
                        timestamps.append(data["timestamp"])
            except json.JSONDecodeError:
                continue

    if include_timestamps:
        return timestamps, values
    return values


def calculate_throughput_bins(timestamps, cumulative_bytes, bin_seconds=60):
    """
    Calculate throughput by binning cumulative bytes over time intervals.

    Args:
        timestamps: List of ISO timestamp strings
        cumulative_bytes: List of cumulative byte counts
        bin_seconds: Size of time bins in seconds (default: 60)

    Returns list of throughput values (bytes per second) for each bin.
    """
    from datetime import datetime

    if len(timestamps) == 0 or len(cumulative_bytes) == 0:
        return []

    # Parse timestamps
    parsed_times = []
    for ts in timestamps:
        try:
            # Parse ISO format timestamp
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            parsed_times.append(dt)
        except:
            continue

    if len(parsed_times) == 0:
        return []

    # Get start time and create bins
    start_time = parsed_times[0]
    throughputs = []

    i = 0
    while i < len(parsed_times):
        # Find all data points in this bin
        bin_start_time = start_time.timestamp() + (len(throughputs) * bin_seconds)
        bin_end_time = bin_start_time + bin_seconds

        bin_start_bytes = None
        bin_end_bytes = None

        # Find measurements within this bin
        while i < len(parsed_times) and parsed_times[i].timestamp() < bin_end_time:
            if bin_start_bytes is None:
                bin_start_bytes = cumulative_bytes[i]
            bin_end_bytes = cumulative_bytes[i]
            i += 1

        # Calculate throughput for this bin
        if bin_start_bytes is not None and bin_end_bytes is not None:
            bytes_in_bin = bin_end_bytes - bin_start_bytes
            throughput = bytes_in_bin / bin_seconds
            throughputs.append(throughput)

        # If we didn't find any data in this bin, stop
        if bin_start_bytes is None:
            break

    return throughputs


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


def match_runs(block_runs, zns_runs, chunk_size, distribution, ratio, eviction_type):
    """
    Find matching block and ZNS runs for given parameters.

    Returns tuple of (block_run, zns_run) or (None, None) if not found.
    """
    block_run = None
    zns_run = None

    for run in block_runs:
        if (run["chunk_size"] == chunk_size and
            run["distribution"] == distribution and
            run["ratio"] == ratio and
            run["eviction_type"] == eviction_type and
            run["device"] == "Block"):
            block_run = run
            break

    for run in zns_runs:
        if (run["chunk_size"] == chunk_size and
            run["distribution"] == distribution and
            run["ratio"] == ratio and
            run["eviction_type"] == eviction_type and
            run["device"] == "ZNS"):
            zns_run = run
            break

    return block_run, zns_run


# ============================================================================
# BOXPLOT GENERATION
# ============================================================================

def generate_boxplot(block_dir, zns_dir, eviction_type, metric, output_file, sample_size=None):
    """
    Generate boxplot comparing ZNS vs Block for a specific eviction type and metric.

    Args:
        block_dir: Path to block device split_output directory
        zns_dir: Path to ZNS device split_output directory
        eviction_type: "promotional" or "chunk"
        metric: "latency" or "throughput"
        output_file: Output filename for the plot
        sample_size: If provided, sample every Nth line from JSON files for faster processing
    """
    print(f"\nGenerating {eviction_type} {metric} boxplot...")

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

    # Get chunk sizes for this eviction type
    chunk_sizes = CHUNK_SIZES_CONFIG.get(eviction_type, [])
    if not chunk_sizes:
        print(f"Warning: No chunk sizes configured for {eviction_type}")
        return

    # Collect runs from both directories
    block_runs = collect_runs(block_dir)
    zns_runs = collect_runs(zns_dir)

    print(f"Found {len(block_runs)} block runs and {len(zns_runs)} ZNS runs")

    # Calculate number of subplots
    num_subplots = len(chunk_sizes) * len(DISTRIBUTIONS) * len(RATIOS)

    # Create figure with subplots
    fig, axes = plt.subplots(1, num_subplots, figsize=(5 * num_subplots, 10))

    # Ensure axes is always a list
    if num_subplots == 1:
        axes = [axes]

    idx = 0
    legend_patches = []
    legend_keys_seen = set()  # Track which legend entries we've already added

    # Iterate through configurations
    for chunk_size in chunk_sizes:
        chunk_color = CHUNK_SIZE_COLORS.get(chunk_size, "lightgray")
        chunk_label = CHUNK_SIZE_LABELS.get(chunk_size, f"{chunk_size}")

        for distribution in DISTRIBUTIONS:
            dist_hatch = DISTRIBUTION_HATCHES.get(distribution, "")

            for ratio in RATIOS:
                # Find matching runs
                block_run, zns_run = match_runs(block_runs, zns_runs,
                                                chunk_size, distribution, ratio, eviction_type)

                current_data = []

                # Load ZNS data
                if zns_run:
                    zns_data_file = zns_run["path"] / metric_file
                    if zns_data_file.exists():
                        if metric == "throughput":
                            # Load with timestamps and calculate throughput in 60-second bins
                            zns_timestamps, zns_values = load_json_data(zns_data_file, sample_size, include_timestamps=True)
                            zns_throughput = calculate_throughput_bins(zns_timestamps, zns_values, bin_seconds=60)
                            current_data.append([v * scale for v in zns_throughput])
                        else:
                            zns_values = load_json_data(zns_data_file, sample_size)
                            current_data.append([v * scale for v in zns_values])
                    else:
                        print(f"Warning: Missing {metric_file} for ZNS run: {zns_run['dirname']}")
                        current_data.append([])
                else:
                    print(f"Warning: No ZNS run found for chunk={chunk_size}, dist={distribution}, ratio={ratio}")
                    current_data.append([])

                # Load Block data
                if block_run:
                    block_data_file = block_run["path"] / metric_file
                    if block_data_file.exists():
                        if metric == "throughput":
                            # Load with timestamps and calculate throughput in 60-second bins
                            block_timestamps, block_values = load_json_data(block_data_file, sample_size, include_timestamps=True)
                            block_throughput = calculate_throughput_bins(block_timestamps, block_values, bin_seconds=60)
                            current_data.append([v * scale for v in block_throughput])
                        else:
                            block_values = load_json_data(block_data_file, sample_size)
                            current_data.append([v * scale for v in block_values])
                    else:
                        print(f"Warning: Missing {metric_file} for Block run: {block_run['dirname']}")
                        current_data.append([])
                else:
                    print(f"Warning: No Block run found for chunk={chunk_size}, dist={distribution}, ratio={ratio}")
                    current_data.append([])

                # Create boxplot
                bp = axes[idx].boxplot(current_data,
                                      showfliers=False,
                                      widths=1.0,
                                      medianprops=dict(linewidth=2, color='red'),
                                      patch_artist=True)

                # Set x-axis labels
                axes[idx].set_xticks([1, 2])
                axes[idx].set_xticklabels(["ZNS", "Block"], rotation=45, fontsize=20)

                # Add ratio label above boxes
                ylim = axes[idx].get_ylim()
                axes[idx].text(1.5, ylim[1], f"Ratio: 1:{ratio}",
                             ha='center', va='bottom', fontsize=18, weight='bold')

                # Rotate y-axis labels
                for label in axes[idx].get_yticklabels():
                    label.set_rotation(45)

                # Apply styling to boxes
                if len(bp['boxes']) >= 2:
                    bp['boxes'][0].set_hatch(dist_hatch)
                    bp['boxes'][1].set_hatch(dist_hatch)
                    bp['boxes'][0].set_facecolor(chunk_color)
                    bp['boxes'][1].set_facecolor(chunk_color)

                # Add to legend if not already present
                legend_key = (chunk_size, distribution)
                if legend_key not in legend_keys_seen:
                    legend_keys_seen.add(legend_key)
                    legend_patches.append(mpatches.Patch(
                        facecolor=chunk_color,
                        hatch=dist_hatch,
                        label=f"{distribution.capitalize()} {chunk_label}",
                        alpha=.99
                    ))

                idx += 1

    # Add y-axis label on the far left
    fig.text(0.02, 0.5, metric_label, va='center', rotation='vertical', fontsize=22)

    # Adjust layout
    plt.subplots_adjust(wspace=0.0, hspace=0.0)
    plt.tight_layout(pad=0.0)
    plt.subplots_adjust(top=0.95, bottom=0.22, left=0.05)

    # Add legend
    fig.legend(
        ncols=len(legend_patches),
        handles=legend_patches,
        bbox_to_anchor=(0.5, 0.09),
        loc='center',
        fontsize="x-large",
        columnspacing=2.0,
        frameon=False
    )

    # Save figure
    plt.savefig(output_file, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate boxplot graphs comparing ZNS vs Block devices for OxCache performance metrics."
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

    # Generate all 4 graphs
    graphs = [
        ("chunk", "latency", "chunk_lru_latency.png"),
        ("chunk", "throughput", "chunk_lru_throughput.png"),
        ("promotional", "latency", "zone_lru_latency.png"),
        ("promotional", "throughput", "zone_lru_throughput.png"),
    ]

    for eviction_type, metric, filename in graphs:
        output_file = output_dir / filename
        generate_boxplot(block_split, zns_split, eviction_type, metric, output_file, args.sample)

    print("\n✓ All boxplots generated successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
