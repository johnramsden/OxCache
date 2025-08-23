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


def normalize_filename(filename):
    """Normalize filename by replacing device names with a common pattern for matching."""
    # Replace nvme device names with a placeholder for matching
    import re
    normalized = re.sub(r'nvme\d+n\d+', 'nvmeXnX', filename)
    return normalized


def plot_metric_latency(split_data_dirs, metric_name, output_dir, labels=None):
    """Plot latency values for a specific metric, comparing across directories if multiple provided."""
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
    
    # Collect all data directories from all split data directories
    all_data_dirs = {}  # normalized_name -> [(path, label), ...]
    
    for split_dir, label in zip(split_data_dirs, labels):
        split_path = Path(split_dir)
        if not split_path.exists():
            print(f"Error: Split data directory {split_path} not found")
            continue
        
        data_dirs = [d for d in split_path.iterdir() if d.is_dir()]
        for data_dir in data_dirs:
            normalized = normalize_filename(data_dir.name)
            if normalized not in all_data_dirs:
                all_data_dirs[normalized] = []
            all_data_dirs[normalized].append((data_dir, label))
    
    if not all_data_dirs:
        print("No data directories found")
        return
    
    colors = ['red', 'blue', 'green', 'orange', 'purple', 'brown']
    
    # Create one plot per normalized filename
    for normalized_name, dir_label_pairs in all_data_dirs.items():
        plt.figure(figsize=(12, 4))
        
        has_data = False
        all_latency_data = []  # Store all latency data to determine common scale
        
        # First pass: collect all data to determine the common scale
        for i, (data_dir, label) in enumerate(dir_label_pairs):
            metric_file = data_dir / f"{metric_name}.json"
            
            if not metric_file.exists():
                print(f"Warning: {metric_file} not found, skipping")
                continue
            
            print(f"Processing {metric_file}...")
            
            # Read data points
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
                        except (json.JSONDecodeError, KeyError, ValueError) as e:
                            continue
            except Exception as e:
                print(f"Error reading {metric_file}: {e}")
                continue
            
            if not timestamps or not values:
                print(f"No valid data points in {metric_file}")
                continue
            
            # Convert timestamps to minutes from start
            start_time = timestamps[0]
            time_minutes = [(t - start_time).total_seconds() / 60 for t in timestamps]
            all_latency_data.append((time_minutes, values, label, i))
        
        if not all_latency_data:
            print(f"No valid latency data for {normalized_name}")
            plt.close()
            continue
        
        # Determine common y-axis range
        all_values = []
        for _, values, _, _ in all_latency_data:
            all_values.extend(values)
        
        if all_values:
            y_min = min(all_values)
            y_max = max(all_values)
            # Add some padding to the y-axis
            y_padding = (y_max - y_min) * 0.05
            y_min = max(0, y_min - y_padding)  # Don't go below 0 for latency
            y_max = y_max + y_padding
        
        # Second pass: plot all data with common scale
        for time_minutes, values, label, i in all_latency_data:
            color = colors[i % len(colors)]
            plt.plot(time_minutes, values, color=color, alpha=0.8, linewidth=0.8, label=label)
            has_data = True
        
        if has_data:
            plt.xlabel('Time (minutes)')
            plt.ylabel(f'Latency (ms)')
            plt.grid(True, alpha=0.3)
            
            # Set common y-axis limits
            if all_values:
                plt.ylim(y_min, y_max)
            
            if len(dir_label_pairs) > 1:
                plt.legend()
            
            plt.tight_layout()
            
            # Save plot
            output_file = output_path / f"{normalized_name}_{metric_name}_raw.png"
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            print(f"Saved plot to {output_file}")
        else:
            print(f"No valid latency data for {normalized_name}")
        
        plt.close()


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