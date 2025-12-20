#!/usr/bin/env python3
"""
Script to plot latency metrics with time-based averaging/smoothing.
"""

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams
from collections import defaultdict
import numpy as np
import data_cache

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


def smooth_data(timestamps_unix, values, window_seconds):
    """Smooth data by averaging over time windows.

    OPTIMIZED: Uses vectorized NumPy operations for faster processing.

    Args:
        timestamps_unix: NumPy array of Unix timestamps
        values: NumPy array of values
        window_seconds: Size of smoothing window in seconds

    Returns:
        tuple: (smoothed_timestamps, smoothed_values) as NumPy arrays
    """
    if len(timestamps_unix) < 2:
        return timestamps_unix, values

    # Calculate time span
    start_time = timestamps_unix[0]
    end_time = timestamps_unix[-1]
    duration = end_time - start_time

    if duration <= 0:
        return timestamps_unix, values

    # Create bin edges
    num_bins = int(np.ceil(duration / window_seconds))
    bin_edges = np.arange(num_bins + 1) * window_seconds + start_time

    # Assign each timestamp to a bin using searchsorted (vectorized)
    bin_indices = np.searchsorted(bin_edges[1:], timestamps_unix, side='left')

    # Calculate average for each bin using vectorized operations
    smoothed_timestamps = []
    smoothed_values = []

    for bin_idx in range(num_bins):
        # Find all data points in this bin
        mask = (bin_indices == bin_idx)
        values_in_bin = values[mask]

        if len(values_in_bin) == 0:
            continue  # Skip empty bins

        # Calculate average timestamp (center of bin) and average value
        avg_timestamp = start_time + (bin_idx + 0.5) * window_seconds
        avg_value = np.mean(values_in_bin)

        smoothed_timestamps.append(avg_timestamp)
        smoothed_values.append(avg_value)

    return np.array(smoothed_timestamps, dtype=np.float64), np.array(smoothed_values, dtype=np.float64)


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


def calculate_chunk_global_max_latency_smoothed(normalized_groups, metric_name, window_seconds):
    """Calculate global maximum smoothed latency for a chunk size group.

    OPTIMIZED: Loads data once using data_cache module and returns both
    the global min/max AND the loaded+smoothed data for reuse in plotting.

    Args:
        normalized_groups: Dict of {normalized_name: [(path, label), ...]}
        metric_name: Name of the latency metric
        window_seconds: Smoothing window size

    Returns:
        tuple: (global_min, global_max, loaded_data_dict)
               where loaded_data_dict = {metric_file_path: (smoothed_timestamps, smoothed_values)}
    """
    all_values = []
    loaded_data = {}  # Cache loaded+smoothed data for reuse in plotting

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

                if len(timestamps_unix) < 2 or len(values) < 2:
                    continue

                # Apply smoothing using vectorized operations
                smooth_timestamps, smooth_values = smooth_data(
                    timestamps_unix, values, window_seconds
                )

                if len(smooth_values) > 0:
                    # Store loaded+smoothed data for reuse in plotting (avoids re-reading and re-smoothing)
                    loaded_data[str(metric_file)] = (smooth_timestamps, smooth_values)

                    # Collect all smoothed values for global min/max calculation
                    all_values.extend(smooth_values)

            except Exception as e:
                print(f"Error processing {metric_file}: {e}")
                continue

    if not all_values:
        return None, None, loaded_data

    return float(np.min(all_values)), float(np.max(all_values)), loaded_data


def plot_latency_smoothed_group(normalized_name, dir_label_pairs, metric_name,
                                window_seconds, output_dir, y_min, y_max, loaded_data):
    """Create a single smoothed latency plot for a normalized group.

    OPTIMIZED: Reuses pre-loaded and pre-smoothed data instead of re-reading and re-smoothing files.

    Args:
        normalized_name: Normalized directory name
        dir_label_pairs: List of (data_dir, label) tuples
        metric_name: Name of metric
        window_seconds: Smoothing window size
        output_dir: Output directory path
        y_min: Minimum y-axis value
        y_max: Maximum y-axis value
        loaded_data: Dict of pre-loaded+smoothed data {file_path: (timestamps, values)}
    """
    plt.figure(figsize=(12, 4))
    has_data = False

    for data_dir, label in dir_label_pairs:
        metric_file = data_dir / f"{metric_name}.json"
        metric_file_str = str(metric_file)

        # Check if data was already loaded and smoothed
        if metric_file_str not in loaded_data:
            print(f"Warning: {metric_file} not found in loaded data, skipping")
            continue

        print(f"Processing {metric_file}...")

        # Retrieve pre-loaded and pre-smoothed data (as NumPy arrays)
        smooth_timestamps_unix, smooth_values = loaded_data[metric_file_str]

        if len(smooth_timestamps_unix) == 0 or len(smooth_values) == 0:
            print(f"No valid data points in {metric_file}")
            continue

        # Convert Unix timestamps to minutes from start
        start_time_unix = smooth_timestamps_unix[0]
        time_minutes = (smooth_timestamps_unix - start_time_unix) / 60.0

        plt.plot(time_minutes, smooth_values, alpha=0.8, linewidth=1.5, label=label)
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

        output_file = output_dir / f"{normalized_name}_{metric_name}_smoothed_{window_seconds}s.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight', pad_inches=0)
        print(f"Saved plot to {output_file}")

    plt.close()


def plot_metric_latency_smoothed(split_data_dirs, metric_name, window_seconds, output_dir, labels=None):
    """Plot smoothed latency values for a specific metric with chunk-size-based scaling."""
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

        # Calculate global min/max for smoothed data in this chunk size
        # OPTIMIZED: Returns both global min/max AND loaded+smoothed data for reuse
        global_min, global_max, loaded_data = calculate_chunk_global_max_latency_smoothed(
            normalized_groups, metric_name, window_seconds
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
        # OPTIMIZED: Pass loaded_data to avoid re-reading and re-smoothing files
        for normalized_name, dir_label_pairs in normalized_groups.items():
            plot_latency_smoothed_group(
                normalized_name, dir_label_pairs, metric_name,
                window_seconds, chunk_output_dir, y_min, y_max, loaded_data
            )


def main():
    parser = argparse.ArgumentParser(description='Plot smoothed latency metrics from split data')
    parser.add_argument('split_data_dirs', nargs='+', help='Directory(s) containing split data output')
    parser.add_argument('--window-seconds', '-w', type=int, default=30,
                       help='Smoothing window size in seconds (default: 30)')
    parser.add_argument('--output-dir', '-o', default='plots',
                       help='Output directory for plots (default: plots)')
    parser.add_argument('--metrics', '-m', nargs='+', 
                       default=['device_write_latency_ms', 'disk_write_latency_ms', 
                               'get_miss_latency_ms', 'get_hit_latency_ms', 'get_total_latency_ms'],
                       help='Latency metrics to plot')
    parser.add_argument('--labels', '-l', nargs='+',
                       help='Labels for each directory (must match number of directories)')
    
    args = parser.parse_args()
    
    if len(args.split_data_dirs) == 1:
        print(f"Plotting smoothed latency metrics with {args.window_seconds}s windows...")
    else:
        print(f"Comparing smoothed latency metrics across {len(args.split_data_dirs)} datasets with {args.window_seconds}s windows...")
    
    for metric in args.metrics:
        print(f"\nPlotting {metric}...")
        plot_metric_latency_smoothed(args.split_data_dirs, metric, args.window_seconds, args.output_dir, args.labels)
    
    print(f"\nAll plots saved to {args.output_dir}/")


if __name__ == "__main__":
    main()