#!/usr/bin/env python3
"""
Script to plot hit ratio metrics over time.
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


def plot_hitratio(split_data_dirs, output_dir, labels=None):
    """Plot hit ratio over time, comparing across directories if multiple provided."""
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
    
    colors = ['orange', 'blue', 'red', 'green', 'purple', 'brown']
    
    # Create one plot per normalized filename
    for normalized_name, dir_label_pairs in all_data_dirs.items():
        plt.figure(figsize=(12, 4))
        
        has_data = False
        
        for i, (data_dir, label) in enumerate(dir_label_pairs):
            metric_file = data_dir / "hitratio.json"
            
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
                            value = float(data['fields']['value']) * 100  # Convert to percentage
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
            
            color = colors[i % len(colors)]
            plt.plot(time_minutes, values, color=color, alpha=0.8, linewidth=1.2, label=label)
            has_data = True
        
        if has_data:
            plt.xlabel('Time (minutes)')
            plt.ylabel('Hit Ratio (%)')
            plt.grid(True, alpha=0.3)
            plt.ylim(0, 100)  # Hit ratio should be between 0 and 100%
            
            if len(dir_label_pairs) > 1:
                plt.legend()
            
            plt.tight_layout()
            
            # Save plot
            output_file = output_path / f"{normalized_name}_hitratio.png"
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            print(f"Saved plot to {output_file}")
        else:
            print(f"No valid hit ratio data for {normalized_name}")
        
        plt.close()


def main():
    parser = argparse.ArgumentParser(description='Plot hit ratio from split data')
    parser.add_argument('split_data_dirs', nargs='+', help='Directory(s) containing split data output')
    parser.add_argument('--output-dir', '-o', default='plots',
                       help='Output directory for plots (default: plots)')
    parser.add_argument('--labels', '-l', nargs='+',
                       help='Labels for each directory (must match number of directories)')
    
    args = parser.parse_args()
    
    if len(args.split_data_dirs) == 1:
        print("Plotting hit ratio...")
    else:
        print(f"Comparing hit ratio across {len(args.split_data_dirs)} datasets...")
    
    plot_hitratio(args.split_data_dirs, args.output_dir, args.labels)
    print(f"\nPlots saved to {args.output_dir}/")


if __name__ == "__main__":
    main()