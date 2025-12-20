#!/usr/bin/env python3
"""
Script to plot throughput metrics from counter-based data (bytes_total, etc.)
Uses a bucket approach to calculate throughput over specified time intervals.
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


def calculate_throughput(data_points, bucket_seconds):
    """Calculate throughput from counter data using time buckets."""
    if len(data_points) < 2:
        return [], []
    
    # Sort by timestamp
    data_points.sort(key=lambda x: x[0])
    
    start_time = data_points[0][0]
    bucket_duration = timedelta(seconds=bucket_seconds)
    
    buckets = defaultdict(list)
    
    # Group data points into time buckets
    for timestamp, value in data_points:
        bucket_index = int((timestamp - start_time).total_seconds() // bucket_seconds)
        buckets[bucket_index].append((timestamp, value))
    
    timestamps = []
    throughputs = []
    
    # Calculate throughput for each bucket
    for bucket_index in sorted(buckets.keys()):
        bucket_data = buckets[bucket_index]
        if len(bucket_data) < 2:
            continue
            
        # Get first and last values in bucket
        bucket_data.sort(key=lambda x: x[0])
        start_ts, start_val = bucket_data[0]
        end_ts, end_val = bucket_data[-1]
        
        # Calculate throughput (bytes per second)
        time_diff = (end_ts - start_ts).total_seconds()
        if time_diff > 0:
            throughput = (end_val - start_val) / time_diff
            # Use middle of bucket as timestamp
            bucket_time = start_time + timedelta(seconds=(bucket_index + 0.5) * bucket_seconds)
            timestamps.append(bucket_time)
            throughputs.append(throughput)
    
    return timestamps, throughputs


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


def determine_throughput_unit(max_throughput, metric_name):
    """Determine appropriate unit for throughput display.

    Args:
        max_throughput: Maximum throughput value
        metric_name: Name of the metric

    Returns:
        tuple: (unit_divisor, ylabel_string)
    """
    if 'bytes' in metric_name.lower():
        if max_throughput >= 1024 * 1024 * 1024:  # >= 1 GiB/s
            return 1024 * 1024 * 1024, "Throughput (GiB/s)"
        elif max_throughput >= 1024 * 1024:  # >= 1 MiB/s
            return 1024 * 1024, "Throughput (MiB/s)"
        elif max_throughput >= 1024:  # >= 1 KiB/s
            return 1024, "Throughput (KiB/s)"
        else:
            return 1, "Throughput (B/s)"
    else:
        return 1, "Throughput (units/s)"


def calculate_chunk_global_max_throughput(normalized_groups, metric_name, bucket_seconds):
    """Calculate global maximum throughput for a chunk size group.

    OPTIMIZED: Loads data once using data_cache module and returns both
    the global max AND the loaded data for reuse in plotting.

    Args:
        normalized_groups: Dict of {normalized_name: [(path, label), ...]}
        metric_name: Name of the metric (e.g., 'bytes_total')
        bucket_seconds: Bucket size for throughput calculation

    Returns:
        tuple: (global_max_throughput, loaded_data_dict)
               where loaded_data_dict = {metric_file_path: (timestamps_array, values_array)}
    """
    all_throughputs = []
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

                if len(timestamps_unix) < 2 or len(values) < 2:
                    continue

                # Store loaded data for reuse in plotting (avoids re-reading file)
                loaded_data[str(metric_file)] = (timestamps_unix, values)

                # Calculate throughput using vectorized NumPy operations
                throughputs = data_cache.calculate_throughput_bins(
                    timestamps_unix,
                    values,
                    bin_seconds=bucket_seconds
                )

                if len(throughputs) > 0:
                    all_throughputs.extend(throughputs)

            except Exception as e:
                print(f"Error processing {metric_file}: {e}")
                continue

    if not all_throughputs:
        return 0, loaded_data

    return max(all_throughputs), loaded_data


def plot_throughput_group(normalized_name, dir_label_pairs, metric_name,
                          bucket_seconds, output_dir, ylabel, unit_divisor, y_max, loaded_data):
    """Create a single throughput plot for a normalized group.

    OPTIMIZED: Reuses pre-loaded data instead of re-reading files.

    Args:
        normalized_name: Normalized directory name
        dir_label_pairs: List of (data_dir, label) tuples
        metric_name: Name of metric
        bucket_seconds: Bucket size for throughput
        output_dir: Output directory path
        ylabel: Y-axis label
        unit_divisor: Divisor for unit scaling
        y_max: Maximum y-axis value
        loaded_data: Dict of pre-loaded data {file_path: (timestamps, values)}
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

        if len(timestamps_unix) < 2:
            print(f"Not enough data points in {metric_file}")
            continue

        # Calculate throughput using vectorized operations
        throughputs = data_cache.calculate_throughput_bins(
            timestamps_unix,
            values,
            bin_seconds=bucket_seconds
        )

        if len(throughputs) > 0:
            # Convert Unix timestamps to minutes from start
            start_time_unix = timestamps_unix[0]

            # For throughput bins, we need to reconstruct time points
            # Each bin represents bucket_seconds interval starting from start_time
            time_minutes = np.arange(len(throughputs)) * bucket_seconds / 60.0

            # Scale throughputs using chunk-level unit
            scaled_throughputs = throughputs / unit_divisor

            plt.plot(time_minutes, scaled_throughputs, label=label)
            has_data = True

    if has_data:
        plt.xlabel('Time (minutes)')
        plt.ylabel(ylabel)

        # Set consistent y-axis limits for all plots in this chunk size
        plt.ylim(0, y_max)

        if len(dir_label_pairs) > 1:
            plt.legend()

        plt.tight_layout()

        # Save with chunk size in path
        output_file = output_dir / f"{normalized_name}_{metric_name}_throughput.png"
        plt.savefig(output_file, bbox_inches='tight', pad_inches=0)
        print(f"Saved plot to {output_file}")

    plt.close()


def plot_metric_throughput(split_data_dirs, metric_name, bucket_seconds, output_dir, labels=None):
    """Plot throughput for a specific metric with chunk-size-based scaling."""
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

        # Calculate global max for this chunk size
        # OPTIMIZED: Returns both global_max AND loaded_data for reuse
        chunk_global_max, loaded_data = calculate_chunk_global_max_throughput(
            normalized_groups, metric_name, bucket_seconds
        )

        if chunk_global_max == 0:
            print(f"No valid data for chunk size {chunk_size}")
            continue

        # Determine unit based on global max
        unit_divisor, ylabel = determine_throughput_unit(chunk_global_max, metric_name)

        # Calculate y_max in the scaled units
        y_max = chunk_global_max / unit_divisor

        print(f"  Y-axis range for chunk size {chunk_size}: [0, {y_max:.2f}] {ylabel.split('(')[1].strip(')')}")

        # Create plots for each normalized group within this chunk size
        # OPTIMIZED: Pass loaded_data to avoid re-reading files
        for normalized_name, dir_label_pairs in normalized_groups.items():
            plot_throughput_group(
                normalized_name, dir_label_pairs, metric_name,
                bucket_seconds, chunk_output_dir, ylabel, unit_divisor, y_max, loaded_data
            )


def main():
    parser = argparse.ArgumentParser(description='Plot throughput metrics from split data')
    parser.add_argument('split_data_dirs', nargs='+', help='Directory(s) containing split data output')
    parser.add_argument('--bucket-seconds', '-b', type=int, default=60,
                       help='Time bucket size in seconds (default: 60)')
    parser.add_argument('--output-dir', '-o', default='plots',
                       help='Output directory for plots (default: plots)')
    parser.add_argument('--metrics', '-m', nargs='+', 
                       default=['bytes_total', 'written_bytes_total', 'read_bytes_total'],
                       help='Metrics to plot (default: bytes_total written_bytes_total)')
    parser.add_argument('--labels', '-l', nargs='+',
                       help='Labels for each directory (must match number of directories)')
    
    args = parser.parse_args()
    
    if len(args.split_data_dirs) == 1:
        print(f"Plotting throughput metrics with {args.bucket_seconds}s buckets...")
    else:
        print(f"Comparing throughput metrics across {len(args.split_data_dirs)} datasets with {args.bucket_seconds}s buckets...")
    
    for metric in args.metrics:
        print(f"\nPlotting {metric}...")
        plot_metric_throughput(args.split_data_dirs, metric, args.bucket_seconds, args.output_dir, args.labels)
    
    print(f"\nAll plots saved to {args.output_dir}/")


if __name__ == "__main__":
    main()