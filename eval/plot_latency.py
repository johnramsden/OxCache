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

# Increase all font sizes by 4 points
rcParams.update({key: rcParams[key] + 4 for key in rcParams if "size" in key and isinstance(rcParams[key], (int, float))})


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

    Args:
        normalized_groups: Dict of {normalized_name: [(path, label), ...]}
        metric_name: Name of the latency metric

    Returns:
        tuple: (global_min, global_max) of latency values
    """
    all_values = []

    for normalized_name, dir_label_pairs in normalized_groups.items():
        for data_dir, label in dir_label_pairs:
            metric_file = data_dir / f"{metric_name}.json"

            if not metric_file.exists():
                continue

            # Read latency values
            timestamps = []
            values = []

            try:
                with open(metric_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            data = json.loads(line)
                            timestamp = parse_timestamp(data['timestamp'])
                            value = float(data['fields']['value'])
                            timestamps.append(timestamp)
                            values.append(value)
                        except (json.JSONDecodeError, KeyError, ValueError):
                            continue
            except Exception as e:
                print(f"Error reading {metric_file}: {e}")
                continue

            if timestamps and values:
                # Filter last 5 minutes
                filtered_timestamps, filtered_values = filter_last_n_minutes(timestamps, values, minutes=5)
                if filtered_values:
                    all_values.extend(filtered_values)

    if not all_values:
        return None, None

    return min(all_values), max(all_values)


def plot_latency_group(normalized_name, dir_label_pairs, metric_name, output_dir, y_min, y_max):
    """Create a single latency plot for a normalized group.

    Uses pre-calculated y-axis limits from chunk-level min/max.
    """
    plt.figure(figsize=(12, 4))
    has_data = False

    for data_dir, label in dir_label_pairs:
        metric_file = data_dir / f"{metric_name}.json"

        if not metric_file.exists():
            print(f"Warning: {metric_file} not found, skipping")
            continue

        print(f"Processing {metric_file}...")

        # Read data
        timestamps = []
        values = []

        try:
            with open(metric_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        data = json.loads(line)
                        timestamp = parse_timestamp(data['timestamp'])
                        value = float(data['fields']['value'])
                        timestamps.append(timestamp)
                        values.append(value)
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue
        except Exception as e:
            print(f"Error reading {metric_file}: {e}")
            continue

        if not timestamps or not values:
            print(f"No valid data points in {metric_file}")
            continue

        # Filter last 5 minutes
        filtered_timestamps, filtered_values = filter_last_n_minutes(timestamps, values, minutes=5)

        if not filtered_timestamps or not filtered_values:
            print(f"No data remaining after filtering for {metric_file}")
            continue

        # Convert to minutes
        start_time = filtered_timestamps[0]
        time_minutes = [(t - start_time).total_seconds() / 60 for t in filtered_timestamps]

        plt.plot(time_minutes, filtered_values, alpha=0.8, linewidth=0.8, label=label)
        has_data = True

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


def plot_metric_latency(split_data_dirs, metric_name, output_dir, labels=None):
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
        global_min, global_max = calculate_chunk_global_max_latency(
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
        for normalized_name, dir_label_pairs in normalized_groups.items():
            plot_latency_group(
                normalized_name, dir_label_pairs, metric_name,
                chunk_output_dir, y_min, y_max
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
    
    args = parser.parse_args()
    
    if len(args.split_data_dirs) == 1:
        print("Plotting raw latency metrics...")
    else:
        print(f"Comparing raw latency metrics across {len(args.split_data_dirs)} datasets...")
    
    for metric in args.metrics:
        print(f"\nPlotting {metric}...")
        plot_metric_latency(args.split_data_dirs, metric, args.output_dir, args.labels)
    
    print(f"\nAll plots saved to {args.output_dir}/")


if __name__ == "__main__":
    main()