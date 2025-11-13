#!/usr/bin/env python3
"""
Script to create boxplot visualizations for latency metrics.
Processes latency data directly without throughput calculations.
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

from numpy import extract
import numpy as np

# Increase all font sizes by 8 points
rcParams.update({key: rcParams[key] + 8 for key in rcParams if "size" in key and isinstance(rcParams[key], (int, float))})

map_files = {
"metrics-2025-08-30-17-52-04": "536870912,L=5413781,UNIFORM,R=10,I=6000,NZ=200,nvme0n2",
"metrics-2025-08-30-18-35-00": "536870912,L=5413781,UNIFORM,R=2,I=6000,NZ=200,nvme0n2",
"metrics-2025-08-30-19-07-57": "536870912,L=5413781,ZIPFIAN,R=10,I=6000,NZ=200,nvme0n2",
"metrics-2025-08-30-19-37-57": "536870912,L=5413781,ZIPFIAN,R=2,I=6000,NZ=200,nvme0n2",
"metrics-2025-08-30-20-06-12": "65536,L=40632,UNIFORM,R=10,I=9830400,NZ=40,nvme0n2",
"metrics-2025-08-30-21-45-11": "65536,L=40632,UNIFORM,R=2,I=9830400,NZ=40,nvme0n2",
"metrics-2025-08-30-22-41-50": "65536,L=40632,ZIPFIAN,R=10,I=9830400,NZ=40,nvme0n2",
"metrics-2025-08-30-23-07-40": "65536,L=40632,ZIPFIAN,R=2,I=9830400,NZ=40,nvme0n2",
"metrics-2025-08-31-00-17-38": "536870912,L=5413781,UNIFORM,R=10,I=6000,NZ=200,nvme1n1",
"metrics-2025-08-31-02-24-14": "536870912,L=5413781,UNIFORM,R=2,I=6000,NZ=200,nvme1n1",
"metrics-2025-08-31-04-35-44": "536870912,L=5413781,ZIPFIAN,R=10,I=6000,NZ=200,nvme1n1",
"metrics-2025-08-31-06-21-32": "536870912,L=5413781,ZIPFIAN,R=2,I=6000,NZ=200,nvme1n1",
"metrics-2025-08-31-07-51-04": "65536,L=40632,UNIFORM,R=10,I=9830400,NZ=40,nvme1n1",
"metrics-2025-08-31-10-14-21": "65536,L=40632,UNIFORM,R=2,I=9830400,NZ=40,nvme1n1",
"metrics-2025-08-31-11-55-39": "65536,L=40632,ZIPFIAN,R=10,I=9830400,NZ=40,nvme1n1",
"metrics-2025-08-31-13-07-17": "65536,L=40632,ZIPFIAN,R=2,I=9830400,NZ=40,nvme1n1"
}

def parse_timestamp(timestamp_str):
    """Parse ISO timestamp string to datetime object."""
    return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))


def extract_latency_values(data_points):
    """Extract latency values from data points (no calculation needed)."""
    if len(data_points) < 1:
        return []
    
    # Sort by timestamp and extract just the values
    data_points.sort(key=lambda x: x[0])
    latency_values = [value for _, value in data_points]
    
    return latency_values


def normalize_filename(filename):
    """Normalize filename by replacing device names with a common pattern for matching."""
    # Replace nvme device names with a placeholder for matching
    import re
    normalized = re.sub(r'nvme\d+n\d+', 'nvmeXnX', filename)
    return normalized


def plot_metric_latency(split_data_dirs, metric_name, output_dir, labels=None):
    """Plot latency boxplots for a specific metric, comparing across directories if multiple provided."""
    # Handle single directory case (backward compatibility)
    if isinstance(split_data_dirs, (str, Path)):
        split_data_dirs = [split_data_dirs]
    
    if len(split_data_dirs) != 2:
        raise RuntimeError

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
    
    zoned_path = split_data_dirs[0]
    block_path = split_data_dirs[1]

    print(zoned_path)
    print(block_path)

    data_dirs = [d for d in Path(zoned_path).iterdir() if d.is_dir()] + [d for d in Path(block_path).iterdir() if d.is_dir()]
    for data_dir in data_dirs:
        normalized = normalize_filename(data_dir.name)
        if normalized not in all_data_dirs:
            all_data_dirs[normalized] = []
            if normalized in map_files:
                all_data_dirs[normalized].append((data_dir, extract_experiment_info(map_files[normalized])))

    all_latencies_data = []  # Store all latency data
        
    print(all_data_dirs)

    # First pass: collect all data to determine the common scale
    for i, (metric_data, [(path, info)]) in enumerate(all_data_dirs.items()):
        print(metric_data)
        metric_file = Path(path) / f"{metric_name}.json"

        if not metric_file.exists():
            print(f"Warning: {metric_file} not found, skipping")
            continue

        print(f"Processing {metric_file}...")

        # Read data points - optimized file reading
        data_points = []
        try:
            with open(metric_file, 'r') as f:
                content = f.read()
            
            lines = content.strip().split('\n')
            data_points = [None] * len(lines)  # Pre-allocate list size
            valid_count = 0
            
            for line in lines:
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    timestamp = parse_timestamp(data['timestamp'])
                    value = float(data['fields']['value'])
                    data_points[valid_count] = (timestamp, value)
                    valid_count += 1
                except (json.JSONDecodeError, KeyError, ValueError):
                    continue
            
            # Trim the list to actual valid data
            data_points = data_points[:valid_count]
        except Exception as e:
            print(f"Error reading {metric_file}: {e}")
            continue

        if len(data_points) < 2:
            print(f"Not enough data points in {metric_file}")
            continue

        # Extract latency values (no calculation needed)
        latency_values = extract_latency_values(data_points)

        if not latency_values:
            print(f"No latency values for {metric_file}")
            continue
        
        # Convert to numpy array for efficiency
        latency_array = np.array(latency_values)
        all_latencies_data.append((None, latency_array, i, info))  # No timestamps needed
        
    # For latency, no scaling needed - keep in milliseconds
    ylabel = "Latency (ms)"
    
    # No scaling needed for latency data
    scaled_latencies = [(a, t, c, d) for (a, t, c, d) in all_latencies_data]
    
    # Organize data by experiment configuration (chunk_size, distribution, ratio)
    experiment_configs = {}
    
    for (time, latencies, i, info) in scaled_latencies:
        # Create key from chunk size, distribution, and ratio
        key = (info['chunk_size_bytes'], info['distribution'], info['ratio'])
        if key not in experiment_configs:
            experiment_configs[key] = {'ZNS': [], 'Block': []}
        
        # Add latency data based on interface type
        if info['interface'] == 'ZNS':
            experiment_configs[key]['ZNS'] = latencies.tolist()  # Convert back to list for boxplot
        elif info['interface'] == 'Block':
            experiment_configs[key]['Block'] = latencies.tolist()  # Convert back to list for boxplot

    # Create subplots
    import matplotlib.patches as mpatches
    
    fig, axes = plt.subplots(1, 8, figsize=(20, 6))
    
    # Define colors and hatches - larger hatch patterns
    distribution_hatches = {'ZIPFIAN': 'O', 'UNIFORM': '\\'}
    chunk_colors = {536870912: "lightgreen", 65536: "lightblue"}
    
    idx = 0
    for chunk_size in [536870912, 65536]:
        for distribution in ["ZIPFIAN", "UNIFORM"]:
            for ratio in [2, 10]:
                key = (chunk_size, distribution, ratio)
                if key not in experiment_configs:
                    continue
                
                current_data = []
                if experiment_configs[key]['ZNS']:
                    current_data.append(experiment_configs[key]['ZNS'])
                if experiment_configs[key]['Block']:
                    current_data.append(experiment_configs[key]['Block'])
                
                if not current_data:
                    continue
                
                bp = axes[idx].boxplot(current_data,
                                     showfliers=False,
                                     widths=1.0,
                                     medianprops=dict(linewidth=2, color='red'),
                                     patch_artist=True)
                
                # Set labels and formatting
                axes[idx].set_xticks([1, 2])
                axes[idx].set_xticklabels(["ZNS", "Block"], rotation=45, fontsize=20)
                
                # Add ratio label at top
                ylim = axes[idx].get_ylim()
                axes[idx].text(1.5, ylim[1],  # Centered above both boxes
                               f"Ratio: 1:{ratio}", ha='center', va='bottom', fontsize=18, weight='bold')
                
                # Rotate y-axis labels
                for label in axes[idx].get_yticklabels():
                    label.set_rotation(45)
                
                # Apply colors and hatches
                cc = chunk_colors[chunk_size]
                dh = distribution_hatches[distribution]
                
                for box in bp['boxes']:
                    box.set_facecolor(cc)
                    box.set_hatch(dh)
                
                idx += 1
    
    # Add legend and formatting
    plt.subplots_adjust(wspace=0.0, hspace=0.0)
    plt.tight_layout(pad=0.0)
    plt.subplots_adjust(top=0.94)
    
    # Create legend
    legends = [
        mpatches.Patch(facecolor='lightgreen', hatch='O', label='Zipfian 512MiB', alpha=.99),
        mpatches.Patch(facecolor='lightgreen', hatch='\\', label='Uniform 512MiB', alpha=.99),
        mpatches.Patch(facecolor='lightblue', hatch='O', label='Zipfian 64KiB', alpha=.99),
        mpatches.Patch(facecolor='lightblue', hatch='\\', label='Uniform 64KiB', alpha=.99)
    ]
    
    fig.legend(
        ncols=4,
        handles=legends,
        bbox_to_anchor=(0.5, 0.02),  # Center horizontally at bottom of figure
        loc='center',
        fontsize="x-large",
        columnspacing=2.0,  # More space between columns
        frameon=False  # Remove legend frame
    )
    
    # Set common y-label
    # fig.text(0.04, 0.5, ylabel, va='center', rotation='vertical', fontsize=16)

    plt.savefig(f'latency_boxplots_{metric_name}.png', dpi=300, bbox_inches='tight')
    plt.show()
    
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
    parser = argparse.ArgumentParser(description='Create latency boxplots from split data')
    parser.add_argument('split_data_dirs', nargs='+', help='Directory(s) containing split data output')
    parser.add_argument('--output-dir', '-o', default='plots',
                       help='Output directory for plots (default: plots)')
    parser.add_argument('--metrics', '-m', nargs='+', 
                       default=['get_total_latency_ms', 'device_write_latency_ms', 'device_read_latency_ms'],
                       help='Latency metrics to plot')
    parser.add_argument('--labels', '-l', nargs='+',
                       help='Labels for each directory (must match number of directories)')
    
    args = parser.parse_args()
    
    if len(args.split_data_dirs) == 1:
        print(f"Plotting latency metrics...")
    else:
        print(f"Comparing latency metrics across {len(args.split_data_dirs)} datasets...")
    
    for metric in args.metrics:
        print(f"\nPlotting {metric}...")

        plot_metric_latency(args.split_data_dirs, metric, args.output_dir, args.labels)
    
    print(f"\nAll plots saved to {args.output_dir}/")


if __name__ == "__main__":
    main()
