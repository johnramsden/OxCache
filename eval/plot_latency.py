#!/usr/bin/env python3
"""
Script to plot latency metrics as-is from split data.
"""

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams
import numpy as np
import data_cache

# Increase all font sizes by 4 points
rcParams.update({key: rcParams[key] + 4 for key in rcParams if "size" in key and isinstance(rcParams[key], (int, float))})

# Device colors
DEVICE_COLORS = {
    "ZNS": "#a65628",
    "Block": "#f781bf"
}


def get_color_for_label(label):
    """Get color based on label content."""
    if "ZNS" in label:
        return DEVICE_COLORS["ZNS"]
    elif "Block" in label or "SSD" in label:
        return DEVICE_COLORS["Block"]
    return None  # Let matplotlib choose


def parse_timestamp(timestamp_str):
    """Parse ISO timestamp string to datetime object."""
    return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))


def extract_chunk_size(directory_name):
    """Extract chunk size from directory name.

    Directory format: {chunk_size},L=...,{distribution},...

    Args:
        directory_name: Directory name string

    Returns:
        str: Chunk size as string (e.g., "268435456")

    Raises:
        ValueError: If chunk size cannot be extracted
    """
    parts = directory_name.split(',')
    if not parts or not parts[0].isdigit():
        raise ValueError(f"Cannot extract chunk size from: {directory_name}")
    return parts[0]


def normalize_filename(filename):
    """Normalize filename by replacing device names and timestamps with a common pattern for matching."""
    # Replace nvme device names with a placeholder for matching
    import re
    normalized = re.sub(r'nvme\d+n\d+', 'nvmeXnX', filename)
    # Replace timestamp pattern (DD_HH:MM:SS) with a placeholder
    normalized = re.sub(r'-\d+_\d+:\d+:\d+-', '-XX_XX:XX:XX-', normalized)
    return normalized


def find_device_fill_time(data_dir):
    """
    Find the timestamp when the device fills completely for the first time.

    Device fill is determined by finding where usage first decreases (indicating
    the device filled and eviction began), or if no decrease, where usage first
    reaches its maximum value.

    Args:
        data_dir: Path to data directory containing usage_percentage.json

    Returns:
        float: Unix timestamp when device fills, or None if no data available
    """
    usage_file = data_dir / "usage_percentage.json"

    if not usage_file.exists():
        return None

    try:
        # Load usage_percentage data
        timestamps_unix, usage_values = data_cache.load_metric_data(
            usage_file,
            filter_minutes=None,  # Don't filter, we need all data
            use_cache=True
        )

        if len(usage_values) == 0:
            return None

        # Look for the first decrease in usage
        for i in range(len(usage_values) - 1):
            if usage_values[i] > usage_values[i + 1]:
                # Found first decrease, device filled at this peak
                fill_time = timestamps_unix[i]
                return fill_time

        # No decrease found, find where we first reach maximum
        max_val = usage_values.max()
        first_max_idx = np.where(usage_values >= max_val)[0]

        if len(first_max_idx) > 0:
            fill_time = timestamps_unix[first_max_idx[0]]
            return fill_time

    except Exception as e:
        print(f"Warning: Failed to determine device fill time: {e}")

    return None


def filter_last_n_minutes(timestamps, values, minutes=5):
    """Filter out the last N minutes of data from a workload.

    Args:
        timestamps: List of datetime objects
        values: List of values corresponding to timestamps
        minutes: Number of minutes to filter from the end (default: 5)

    Returns:
        tuple: (filtered_timestamps, filtered_values)
    """
    from datetime import timedelta

    if not timestamps or not values:
        return timestamps, values

    # Find the last timestamp
    max_time = max(timestamps)
    # Calculate cutoff time (max_time - N minutes)
    cutoff_time = max_time - timedelta(minutes=minutes)

    # Filter data points before cutoff
    filtered_data = [(t, v) for t, v in zip(timestamps, values) if t <= cutoff_time]

    if not filtered_data:
        return [], []

    filtered_timestamps, filtered_values = zip(*filtered_data)
    return list(filtered_timestamps), list(filtered_values)


def collect_chunk_groups(split_data_dirs, labels):
    """Collect and group directories by chunk size and normalized name.

    Args:
        split_data_dirs: List of split data directory paths
        labels: List of labels corresponding to each directory

    Returns:
        dict: {chunk_size: {normalized_name: [(path, label), ...]}}
    """
    chunk_groups = {}

    for split_dir, label in zip(split_data_dirs, labels):
        split_path = Path(split_dir)
        if not split_path.exists():
            print(f"Error: Split data directory {split_path} not found")
            continue

        data_dirs = [d for d in split_path.iterdir() if d.is_dir()]
        for data_dir in data_dirs:
            try:
                # Extract chunk size
                chunk_size = extract_chunk_size(data_dir.name)

                # Normalize filename
                normalized = normalize_filename(data_dir.name)

                # Add to chunk groups
                if chunk_size not in chunk_groups:
                    chunk_groups[chunk_size] = {}

                if normalized not in chunk_groups[chunk_size]:
                    chunk_groups[chunk_size][normalized] = []

                chunk_groups[chunk_size][normalized].append((data_dir, label))

            except ValueError as e:
                print(f"Warning: Skipping directory {data_dir.name}: {e}")
                continue

    return chunk_groups


def calculate_chunk_global_max_latency(normalized_groups, metric_name):
    """Calculate global maximum latency for a chunk size group.

    OPTIMIZED: Loads data once using data_cache module and returns both
    the global min/max AND the loaded data for reuse in plotting.

    Args:
        normalized_groups: Dict of {normalized_name: [(path, label), ...]}
        metric_name: Name of the latency metric

    Returns:
        tuple: (global_min, global_max, loaded_data_dict)
               where loaded_data_dict = {metric_file_path: (timestamps_array, values_array)}
    """
    all_values = []
    loaded_data = {}  # Cache loaded data for reuse in plotting

    for normalized_name, dir_label_pairs in normalized_groups.items():
        for data_dir, label in dir_label_pairs:
            metric_file = data_dir / f"{metric_name}.json"

            if not metric_file.exists():
                continue

            try:
                # Load data using optimized cache module (streaming + NumPy + disk caching)
                # This filters last 5 minutes during load for efficiency
                timestamps_unix, values = data_cache.load_metric_data(
                    metric_file,
                    filter_minutes=5,
                    use_cache=True
                )

                if len(timestamps_unix) > 0 and len(values) > 0:
                    # Store loaded data for reuse in plotting (avoids re-reading file)
                    loaded_data[str(metric_file)] = (timestamps_unix, values)

                    # Collect all values for global min/max calculation
                    all_values.extend(values)

            except Exception as e:
                print(f"Error processing {metric_file}: {e}")
                continue

    if not all_values:
        return None, None, loaded_data

    return float(np.min(all_values)), float(np.max(all_values)), loaded_data


def plot_latency_group(normalized_name, dir_label_pairs, metric_name, output_dir, y_min, y_max, loaded_data, mark_device_fill=False):
    """Create a single latency plot for a normalized group.

    OPTIMIZED: Reuses pre-loaded data instead of re-reading files.

    Args:
        normalized_name: Normalized directory name
        dir_label_pairs: List of (data_dir, label) tuples
        metric_name: Name of metric
        output_dir: Output directory path
        y_min: Minimum y-axis value
        y_max: Maximum y-axis value
        loaded_data: Dict of pre-loaded data {file_path: (timestamps, values)}
        mark_device_fill: If True, mark the point where device fills completely
    """
    plt.figure(figsize=(12, 4))
    has_data = False

    for data_dir, label in dir_label_pairs:
        metric_file = data_dir / f"{metric_name}.json"
        metric_file_str = str(metric_file)

        # Check if data was already loaded
        if metric_file_str not in loaded_data:
            print(f"Warning: {metric_file} not found in loaded data, skipping")
            continue

        print(f"Processing {metric_file}...")

        # Retrieve pre-loaded data (already filtered, as NumPy arrays)
        timestamps_unix, values = loaded_data[metric_file_str]

        if len(timestamps_unix) == 0 or len(values) == 0:
            print(f"No valid data points in {metric_file}")
            continue

        # Convert Unix timestamps to minutes from start
        start_time_unix = timestamps_unix[0]
        time_minutes = (timestamps_unix - start_time_unix) / 60.0

        # Plot the workload line and get its color
        color = get_color_for_label(label)
        line = plt.plot(time_minutes, values, alpha=0.8, linewidth=0.8, label=label, color=color)
        line_color = line[0].get_color()
        has_data = True

        # Mark device fill time if requested (per workload)
        if mark_device_fill:
            fill_time = find_device_fill_time(data_dir)
            if fill_time is not None:
                # Convert fill time to minutes from this workload's start
                fill_time_minutes = (fill_time - start_time_unix) / 60.0
                plt.axvline(x=fill_time_minutes, color=line_color, linestyle='--',
                           linewidth=2, alpha=0.7)

    if has_data:
        plt.xlabel('Time (minutes)')
        plt.ylabel(f'Latency (ms)')
        plt.grid(True, alpha=0.3)

        # Apply chunk-level y-axis limits
        plt.ylim(y_min, y_max)

        if len(dir_label_pairs) > 1:
            plt.legend()

        plt.tight_layout()

        output_file = output_dir / f"{normalized_name}_{metric_name}_raw.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight', pad_inches=0)
        print(f"Saved plot to {output_file}")

    plt.close()


def plot_metric_latency(split_data_dirs, metric_name, output_dir, labels=None, mark_device_fill=False):
    """Plot latency values for a specific metric with chunk-size-based scaling."""
    # Handle single directory case (backward compatibility)
    if isinstance(split_data_dirs, (str, Path)):
        split_data_dirs = [split_data_dirs]

    # Default labels if not provided
    if labels is None:
        labels = [f"Dataset_{i+1}" for i in range(len(split_data_dirs))]
    elif len(labels) != len(split_data_dirs):
        print(f"Error: Number of labels ({len(labels)}) must match number of directories ({len(split_data_dirs)})")
        return

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Collect directories with two-level grouping
    chunk_groups = collect_chunk_groups(split_data_dirs, labels)

    if not chunk_groups:
        print("No data directories found")
        return

    # Process each chunk size group
    for chunk_size, normalized_groups in chunk_groups.items():
        print(f"\nProcessing chunk size: {chunk_size}")

        # Create chunk-size-specific output directory
        chunk_output_dir = output_path / chunk_size
        chunk_output_dir.mkdir(parents=True, exist_ok=True)

        # Calculate global min/max for this chunk size
        # OPTIMIZED: Returns both global min/max AND loaded_data for reuse
        global_min, global_max, loaded_data = calculate_chunk_global_max_latency(
            normalized_groups, metric_name
        )

        if global_min is None or global_max is None:
            print(f"No valid data for chunk size {chunk_size}")
            continue

        # Calculate y-axis limits with 5% padding
        y_padding = (global_max - global_min) * 0.05
        y_min = max(0, global_min - y_padding)  # Don't go below 0 for latency
        y_max = global_max + y_padding

        print(f"  Y-axis range for chunk size {chunk_size}: [{y_min:.2f}, {y_max:.2f}]")

        # Create plots for each normalized group within this chunk size
        # OPTIMIZED: Pass loaded_data to avoid re-reading files
        for normalized_name, dir_label_pairs in normalized_groups.items():
            plot_latency_group(
                normalized_name, dir_label_pairs, metric_name,
                chunk_output_dir, y_min, y_max, loaded_data, mark_device_fill
            )


def main():
    parser = argparse.ArgumentParser(description='Plot raw latency metrics from split data')
    parser.add_argument('split_data_dirs', nargs='+', help='Directory(s) containing split data output')
    parser.add_argument('--output-dir', '-o', default='plots',
                       help='Output directory for plots (default: plots)')
    parser.add_argument('--metrics', '-m', nargs='+',
                       default=['device_write_latency_ms', 'disk_write_latency_ms', 'device_read_latency_ms', 'disk_read_latency_ms',
                               'get_miss_latency_ms', 'get_total_latency_ms'],
                       help='Latency metrics to plot')
    parser.add_argument('--labels', '-l', nargs='+',
                       help='Labels for each directory (must match number of directories)')
    parser.add_argument('--mark-device-fill', action='store_true',
                       help='Mark the point where device fills completely for the first time')

    args = parser.parse_args()

    if len(args.split_data_dirs) == 1:
        print("Plotting raw latency metrics...")
    else:
        print(f"Comparing raw latency metrics across {len(args.split_data_dirs)} datasets...")

    for metric in args.metrics:
        print(f"\nPlotting {metric}...")
        plot_metric_latency(args.split_data_dirs, metric, args.output_dir, args.labels, args.mark_device_fill)

    print(f"\nAll plots saved to {args.output_dir}/")


if __name__ == "__main__":
    main()