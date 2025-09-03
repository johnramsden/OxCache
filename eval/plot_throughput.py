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

# Increase all font sizes by 4 points
rcParams.update({key: rcParams[key] + 4 for key in rcParams if "size" in key and isinstance(rcParams[key], (int, float))})

map_files = {
"metrics-2025-08-30-17-52-04": "536870912,L=5413781,UNIFORM,R=10,I=6000,NZ=200,nvme0n2",
"metrics-2025-08-30-18-35-00": "536870912,L=5413781,UNIFORM,R=2,I=6000,NZ=200,nvme0n2",
"metrics-2025-08-30-19-07-57": "536870912,L=5413781,ZIPFIAN,R=10,I=6000,NZ=200,nvme0n2",
"metrics-2025-08-30-19-37-57": "536870912,L=5413781,ZIPFIAN,R=2,I=6000,NZ=200,nvme0n2",
"metrics-2025-08-30-20-06-12": "65536,L=40632,UNIFORM,R=10,I=9830400,NZ=40,nvme0n2",
"metrics-2025-08-30-21-45-11": "65536,L=40632,UNIFORM,R=2,I=9830400,NZ=40,nvme0n2",
"metrics-2025-08-30-22-41-50": "65536,L=40632,ZIPFIAN,R=10,I=9830400,NZ=40,nvme0n2",
"metrics-2025-08-30-23-07-40": "65536,L=40632,ZIPFIAN,R=2,I=9830400,NZ=40,nvme0n2"
}

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


def normalize_filename(filename):
    """Normalize filename by replacing device names with a common pattern for matching."""
    # Replace nvme device names with a placeholder for matching
    import re
    normalized = re.sub(r'nvme\d+n\d+', 'nvmeXnX', filename)
    return normalized


def plot_metric_throughput(split_data_dirs, metric_name, bucket_seconds, output_dir, ax, labels=None):
    """Plot throughput for a specific metric, comparing across directories if multiple provided."""
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
    
    
    # Create one plot per normalized filename
    for normalized_name, dir_label_pairs in all_data_dirs.items():
        experiment_info = extract_experiment_info(map_files[normalized_name])
        
        plt.figure(figsize=(12, 4))
        
        has_data = False
        all_throughputs_data = []  # Store all throughput data to determine common scale
        
        # First pass: collect all data to determine the common scale
        for i, (data_dir, label) in enumerate(dir_label_pairs):
            metric_file = data_dir / f"{metric_name}.json"
            
            if not metric_file.exists():
                print(f"Warning: {metric_file} not found, skipping")
                continue
            
            print(f"Processing {metric_file}...")
            
            # Read data points
            data_points = []
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
                            data_points.append((timestamp, value))
                        except (json.JSONDecodeError, KeyError, ValueError) as e:
                            continue
            except Exception as e:
                print(f"Error reading {metric_file}: {e}")
                continue
            
            if len(data_points) < 2:
                print(f"Not enough data points in {metric_file}")
                continue
            
            # Calculate throughput
            timestamps, throughputs = calculate_throughput(data_points, bucket_seconds)

            if timestamps and throughputs:
                # Convert timestamps to minutes from start
                start_time = timestamps[0]
                time_minutes = [(t - start_time).total_seconds() / 60 for t in timestamps]
                all_throughputs_data.append((time_minutes, throughputs, label, i))
        
        if not all_throughputs_data:
            print(f"No valid throughput data for {normalized_name}")
            plt.close()
            continue
        
        # Determine common scale based on maximum throughput across all datasets
        max_throughput = max(max(throughputs) for _, throughputs, _, _ in all_throughputs_data)
        
        if 'bytes' in metric_name.lower():
            # Determine best common unit based on overall maximum
            if max_throughput >= 1024 * 1024 * 1024:  # >= 1 GiB/s
                unit_divisor = 1024 * 1024 * 1024
                ylabel = "Throughput (GiB/s)"
            elif max_throughput >= 1024 * 1024:  # >= 1 MiB/s
                unit_divisor = 1024 * 1024
                ylabel = "Throughput (MiB/s)"
            elif max_throughput >= 1024:  # >= 1 KiB/s
                unit_divisor = 1024
                ylabel = "Throughput (KiB/s)"
            else:
                unit_divisor = 1
                ylabel = "Throughput (B/s)"
        else:
            unit_divisor = 1
            ylabel = "Throughput (units/s)"
        
        # Second pass: plot all data with common scale
        for time_minutes, throughputs, label, i in all_throughputs_data:
            # Apply common scaling
            scaled_throughputs = [t / unit_divisor for t in throughputs]
            
            plt.plot(time_minutes, scaled_throughputs, alpha=0.8, linewidth=1.2, label=label)
            has_data = True

        
        
        bp = ax.boxplot(current_data,
                        showfliers=False,
                        widths=1,
                        medianprops=dict(linewidth=1, color='red'),
                        patch_artist=True)

        ax.set_xticks([1, 2])
            axes[idx].set_xticklabels(["ZNS", "Block"], rotation=45, fontsize=14)

        # axes[idx].set_xlabel(f"1:{r}")
        ylim = ax.get_ylim()
        ax.text(1.5, ylim[1],  # Centered above both boxes
                       f"Ratio: 1:{r}", ha='center', va='bottom', fontsize=14)
        # axes[idx].set_ylabel("GB/s", rotation=90)
        for label in ax.get_yticklabels():
            label.set_rotation(45)
        # plt.suptitle(title)
        bp['boxes'][0].set_hatch(dh)
        bp['boxes'][1].set_hatch(dh)
        bp['boxes'][0].set_facecolor(cc)
        bp['boxes'][1].set_facecolor(cc)

        return

        #     plt.xlabel('Time (minutes)')
        #     plt.ylabel(ylabel)
        #     plt.grid(True, alpha=0.3)
            
        #     if len(dir_label_pairs) > 1:
        #         plt.legend()
            
        #     plt.tight_layout()
            
        #     # Save plot
        #     output_file = output_path / f"{normalized_name}_{metric_name}_throughput.png"
        #     plt.savefig(output_file, dpi=300, bbox_inches='tight', pad_inches=0)
        #     print(f"Saved plot to {output_file}")
        
        # plt.close()

def extract_experiment_info(filename):
    """Extract experiment parameters from filename."""
    # Example: 536870912,L=5413781,UNIFORM,R=10,I=6000,NZ=200,nvme1n1
    # Maps to: chunk_size, cache_size, distribution, ratio, total_io, zones, device
    
    parts = filename.split(',')
    if len(parts) < 7:
        return None
    
    try:
        chunk_size_bytes = int(parts[0])
        cache_size = int(parts[1].split('=')[1])
        distribution = parts[2]
        ratio = int(parts[3].split('=')[1])
        total_io = int(parts[4].split('=')[1])
        zones = int(parts[5].split('=')[1])
        device = parts[6]
        
        # Convert chunk size to readable format
        if chunk_size_bytes >= 1024*1024:
            chunk_size = f"{chunk_size_bytes // (1024*1024)}M"
        elif chunk_size_bytes >= 1024:
            chunk_size = f"{chunk_size_bytes // 1024}K"
        else:
            chunk_size = f"{chunk_size_bytes}B"
        
        # Determine interface type from device name
        if 'nvme1n1' in device:
            interface = 'Block'
        elif 'nvme0n2' in device:
            interface = 'ZNS'
        else:
            interface = 'Unknown'
        
        # Create readable name
        dist_short = 'UNIF' if distribution == 'UNIFORM' else 'ZIPF'
        experiment_name = f"{interface}-{chunk_size}-{dist_short}-{ratio}"
        
        return {
            'name': experiment_name,
            'interface': interface,
            'chunk_size': chunk_size,
            'chunk_size_bytes': chunk_size_bytes,
            'distribution': distribution,
            'ratio': ratio,
            'device': device
        }
    except (ValueError, IndexError):
        return None


def GenerateGraph(runfile, data, analysis, title, scale, genpdf_name):
# def GenerateGraph():
    font = {'size'   : 12}
    # matplotlib.rc('font', **font)
    distribution = ["ZIPFIAN", "UNIFORM"]
    distrib_hatch = ['oo', '//']
    chunk_size = [536870912,65536]
    chunk_color = ["lightgreen", "lightblue"]
    ratio = [2, 10]
    type = ["ZNS", "SSD"]

    idx = 0
    fig, axes = plt.subplots(1, 8, figsize=(10, 4))
    for c, cc in zip(chunk_size, chunk_color):
        for d, dh in zip(distribution, distrib_hatch):
            for r in ratio:
                current_data = []
                ids = []
                for t in type:
                    ids.append(runfile[(runfile["type"] == t) &
                                       (runfile["ratio"] == r) &
                                       (runfile["chunk_size"] == c) &
                                       (runfile["distribution"] == d)].index[0])
                cur = data[analysis]
                zns = cur[cur["id"] == ids[0]]
                ssd = cur[cur["id"] == ids[1]]
                current_data.append(zns["value"].to_numpy()*scale)
                current_data.append(ssd["value"].to_numpy()*scale)

                bp = axes[idx].boxplot(current_data,
                                  showfliers=False,
                                  widths=1,
                                  medianprops=dict(linewidth=1, color='red'),
                                      patch_artist=True)
                # axes[idx].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
                axes[idx].set_xticks([1, 2])
                axes[idx].set_xticklabels(["ZNS", "Block"], rotation=45, fontsize=14)

                # axes[idx].set_xlabel(f"1:{r}")
                ylim = axes[idx].get_ylim()
                axes[idx].text(1.5, ylim[1],  # Centered above both boxes
                               f"Ratio: 1:{r}", ha='center', va='bottom', fontsize=14)
                # axes[idx].set_ylabel("GB/s", rotation=90)
                for label in axes[idx].get_yticklabels():
                    label.set_rotation(45)
                # plt.suptitle(title)
                bp['boxes'][0].set_hatch(dh)
                bp['boxes'][1].set_hatch(dh)
                bp['boxes'][0].set_facecolor(cc)
                bp['boxes'][1].set_facecolor(cc)
                idx += 1

    plt.subplots_adjust(wspace=0.0, hspace=0.0)
    plt.tight_layout(pad=0.0)
    plt.subplots_adjust(top=0.94)
    # Alpha=.99 is due to a bug with pdf export
    legends = [mpatches.Patch(facecolor='lightgreen', hatch='oo', label='Zipfian 512MiB', alpha=.99),
               mpatches.Patch(facecolor='lightgreen', hatch='//', label='Uniform 512MiB', alpha=.99),
               mpatches.Patch(facecolor='lightblue', hatch='oo', label='Zipfian 64KiB', alpha=.99),
               mpatches.Patch(facecolor='lightblue', hatch='//', label='Uniform 64KiB', alpha=.99)]

    plt.legend(
        ncols=4,
        handles=legends,
        bbox_to_anchor=(1, -0.15),  # x = center, y = push it lower
        # loc='upper center',
        fontsize="large"
    )
    plt.savefig(genpdf_name, bbox_inches='tight')




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
