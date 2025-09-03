#!/usr/bin/env python3
"""
Script to generate performance comparison tables from split cache data.
Creates tables in both Markdown and LaTeX formats similar to the appendix tables.
"""

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime
import numpy as np
from collections import defaultdict
import re
import matplotlib.pyplot as plt
from matplotlib import patches as mpatches


def parse_timestamp(timestamp_str):
    """Parse ISO timestamp string to datetime object."""
    return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))


def calculate_statistics(values):
    """Calculate mean, P99, and other statistics from a list of values."""
    if not values:
        return {}
    
    values = np.array(values)
    return {
        'mean': np.mean(values),
        'p99': np.percentile(values, 99),
        'p95': np.percentile(values, 95),
        'p90': np.percentile(values, 90),
        'median': np.median(values),
        'std': np.std(values),
        'min': np.min(values),
        'max': np.max(values)
    }


def calculate_throughput_stats(timestamps, values, bucket_seconds=60):
    """Calculate throughput statistics using bucket approach."""
    if len(timestamps) < 2:
        return {}
    
    # Sort by timestamp
    data_points = list(zip(timestamps, values))
    data_points.sort(key=lambda x: x[0])
    
    start_time = data_points[0][0]
    throughputs = []
    
    # Group into buckets and calculate throughput
    buckets = defaultdict(list)
    for timestamp, value in data_points:
        bucket_index = int((timestamp - start_time).total_seconds() // bucket_seconds)
        buckets[bucket_index].append((timestamp, value))
    
    for bucket_index in sorted(buckets.keys()):
        bucket_data = buckets[bucket_index]
        if len(bucket_data) < 2:
            continue
        
        bucket_data.sort(key=lambda x: x[0])
        start_ts, start_val = bucket_data[0]
        end_ts, end_val = bucket_data[-1]
        
        time_diff = (end_ts - start_ts).total_seconds()
        if time_diff > 0:
            throughput = (end_val - start_val) / time_diff
            throughputs.append(throughput)
    
    return calculate_statistics(throughputs)


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


def process_metric_file(file_path, is_counter_metric=False):
    """Process a single metric file and return statistics."""
    timestamps = []
    values = []
    
    try:
        with open(file_path, 'r') as f:
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
        print(f"Error reading {file_path}: {e}")
        return {}
    
    if not timestamps or not values:
        return {}
    
    if is_counter_metric:
        # For counter metrics like bytes_total, calculate throughput
        return calculate_throughput_stats(timestamps, values)
    else:
        # For direct metrics like latency, calculate stats directly
        return calculate_statistics(values)


def collect_experiment_data(split_data_dirs, labels):
    """Collect all experiment data from split directories."""
    experiments = defaultdict(dict)  # experiment_name -> {interface -> {metric -> stats}}

    print(split_data_dirs, labels)
    
    for split_dir, label in zip(split_data_dirs, labels):
        split_path = Path(split_dir)
        if not split_path.exists():
            print(f"Skipping {split_path}")
            continue
        
        # Find all experiment directories
        for exp_dir in split_path.iterdir():
            print(f"Checking {exp_dir}")

            if not exp_dir.is_dir():
                print(f"Skipping {exp_dir}")
                continue
            
            exp_info = extract_experiment_info(exp_dir.name)
            if not exp_info:
                print("Skipping")
                continue
            
            exp_name = exp_info['name'].replace(exp_info['interface'], 'TEMPLATE')
            interface = label  # Use provided label instead of extracted interface
            
            if exp_name not in experiments:
                experiments[exp_name] = {}
            
            experiments[exp_name][interface] = {}
            
            # Process different metrics
            metrics_config = {
                'get_latency': ('get_total_latency_ms', False),
                'write_latency': ('device_write_latency_ms', False),
                'read_latency': ('device_read_latency_ms', False),
                'hit_ratio': ('hitratio', False),
                'throughput_bytes': ('bytes_total', True),
                'throughput_written': ('written_bytes_total', True)
            }
            
            for metric_key, (metric_file, is_counter) in metrics_config.items():
                metric_path = exp_dir / f"{metric_file}.json"
                if metric_path.exists():
                    stats = process_metric_file(metric_path, is_counter)
                    if stats:
                        experiments[exp_name][interface][metric_key] = stats
    
    return experiments


def format_value(value, unit="ms", precision=2):
    """Format a numeric value with appropriate units."""
    if value is None or np.isnan(value):
        return "N/A"
    
    if unit == "ms":
        return f"{value:.{precision}f}"
    elif unit == "s":
        return f"{value:.{precision}f}"
    elif unit == "%":
        return f"{value*100:.1f}%"
    elif unit == "MiB/s":
        return f"{value/(1024*1024):.{precision}f}"
    elif unit == "GiB/s":
        return f"{value/(1024*1024*1024):.{precision}f}"
    else:
        return f"{value:.{precision}f}"


def calculate_improvement(zns_value, block_value, metric_key):
    """Calculate percentage improvement of ZNS over Block.
    
    For latency metrics (lower is better): improvement = (block - zns) / block * 100
    For throughput metrics (higher is better): improvement = (zns - block) / block * 100
    For hit_ratio (higher is better): improvement = (zns - block) / block * 100
    """
    if block_value == 0 or zns_value is None or block_value is None:
        return None
    
    # Determine if this is a "higher is better" metric
    higher_is_better = any(keyword in metric_key.lower() for keyword in ['throughput', 'hit_ratio', 'hitratio'])
    
    if higher_is_better:
        # For throughput and hit ratio, higher values are better
        improvement = ((zns_value - block_value) / block_value) * 100
    else:
        # For latency, lower values are better
        improvement = ((block_value - zns_value) / block_value) * 100
    
    return improvement


def generate_markdown_table(experiments, metric_key, metric_name, unit="ms"):
    """Generate a Markdown table for a specific metric."""
    
    table = f"## {metric_name}\n\n"
    table += "| Experiment | Block Mean | Block P99 | ZNS Mean | ZNS P99 | Mean Improvement | P99 Improvement |\n"
    table += "|------------|------------|-----------|----------|---------|------------------|------------------|\n"
    
    for exp_name in sorted(experiments.keys()):
        exp_data = experiments[exp_name]
        
        if 'Block-interface' in exp_data and 'ZNS' in exp_data:
            block_stats = exp_data['Block-interface'].get(metric_key, {})
            zns_stats = exp_data['ZNS'].get(metric_key, {})
            
            block_mean = block_stats.get('mean')
            block_p99 = block_stats.get('p99')
            zns_mean = zns_stats.get('mean')
            zns_p99 = zns_stats.get('p99')
            
            # Calculate improvements
            mean_improvement = calculate_improvement(zns_mean, block_mean, metric_key)
            p99_improvement = calculate_improvement(zns_p99, block_p99, metric_key)
            
            # Format values
            block_mean_str = format_value(block_mean, unit)
            block_p99_str = format_value(block_p99, unit)
            zns_mean_str = format_value(zns_mean, unit)
            zns_p99_str = format_value(zns_p99, unit)
            
            mean_imp_str = f"{mean_improvement:.1f}%" if mean_improvement is not None else "N/A"
            p99_imp_str = f"{p99_improvement:.1f}%" if p99_improvement is not None else "N/A"
            
            # Replace TEMPLATE with actual experiment parameters
            display_name = exp_name.replace('TEMPLATE-', '')
            
            table += f"| {display_name} | {block_mean_str} | {block_p99_str} | {zns_mean_str} | {zns_p99_str} | {mean_imp_str} | {p99_imp_str} |\n"
    
    return table + "\n"


def generate_latex_table(experiments, metric_key, metric_name, unit="ms"):
    """Generate a LaTeX table for a specific metric."""
    
    table = f"\\begin{{table}}[h!]\n"
    table += f"\\centering\n"
    table += f"\\caption{{{metric_name} Comparison}}\n"
    table += f"\\begin{{tabular}}{{|l|c|c|c|c|c|c|}}\n"
    table += f"\\hline\n"
    table += f"Experiment & Block Mean & Block P99 & ZNS Mean & ZNS P99 & Mean Impr. & P99 Impr. \\\\\n"
    table += f"\\hline\n"
    
    for exp_name in sorted(experiments.keys()):
        exp_data = experiments[exp_name]
        
        if 'Block-interface' in exp_data and 'ZNS' in exp_data:
            block_stats = exp_data['Block-interface'].get(metric_key, {})
            zns_stats = exp_data['ZNS'].get(metric_key, {})
            
            block_mean = block_stats.get('mean')
            block_p99 = block_stats.get('p99')
            zns_mean = zns_stats.get('mean')
            zns_p99 = zns_stats.get('p99')
            
            # Calculate improvements
            mean_improvement = calculate_improvement(zns_mean, block_mean, metric_key)
            p99_improvement = calculate_improvement(zns_p99, block_p99, metric_key)
            
            # Format values for LaTeX
            block_mean_str = format_value(block_mean, unit).replace('%', '\\%')
            block_p99_str = format_value(block_p99, unit).replace('%', '\\%')
            zns_mean_str = format_value(zns_mean, unit).replace('%', '\\%')
            zns_p99_str = format_value(zns_p99, unit).replace('%', '\\%')
            
            mean_imp_str = f"{mean_improvement:.1f}\\%" if mean_improvement is not None else "N/A"
            p99_imp_str = f"{p99_improvement:.1f}\\%" if p99_improvement is not None else "N/A"
            
            # Replace TEMPLATE and escape underscores for LaTeX
            display_name = exp_name.replace('TEMPLATE-', '').replace('_', '\\_')
            
            table += f"{display_name} & {block_mean_str} & {block_p99_str} & {zns_mean_str} & {zns_p99_str} & {mean_imp_str} & {p99_imp_str} \\\\\n"
    
    table += f"\\hline\n"
    table += f"\\end{{tabular}}\n"
    table += f"\\end{{table}}\n\n"
    
    return table


# def GenerateGraph(runfile, data, analysis, title, scale, genpdf_name):
def GenerateGraph():
    print(f"Generating {title} from {analysis}, output is {genpdf_name}")
    # font = {'size'   : 12}

    for exp_name in sorted(experiments.keys()):
        exp_data = experiments[exp_name]

    # # matplotlib.rc('font', **font)
    # distribution = ["ZIPFIAN", "UNIFORM"]
    # distrib_hatch = ['oo', '//']
    # chunk_size = [536870912,65536]
    # chunk_color = ["lightgreen", "lightblue"]
    # ratio = [2, 10]
    # type = ["ZNS", "SSD"]

    # idx = 0
    # fig, axes = plt.subplots(1, 8, figsize=(10, 4))
    # for c, cc in zip(chunk_size, chunk_color):
    #     for d, dh in zip(distribution, distrib_hatch):
    #         for r in ratio:
    #             current_data = []
    #             ids = []
    #             for t in type:
    #                 ids.append(runfile[(runfile["type"] == t) &
    #                                    (runfile["ratio"] == r) &
    #                                    (runfile["chunk_size"] == c) &
    #                                    (runfile["distribution"] == d)].index[0])
    #             cur = data[analysis]
    #             zns = cur[cur["id"] == ids[0]]
    #             ssd = cur[cur["id"] == ids[1]]
    #             current_data.append(zns["value"].to_numpy()*scale)
    #             current_data.append(ssd["value"].to_numpy()*scale)

    #             bp = axes[idx].boxplot(current_data,
    #                               showfliers=False,
    #                               widths=1,
    #                               medianprops=dict(linewidth=1, color='red'),
    #                                   patch_artist=True)
    #             # axes[idx].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
    #             axes[idx].set_xticks([1, 2])
    #             axes[idx].set_xticklabels(["ZNS", "Block"], rotation=45, fontsize=14)

    #             # axes[idx].set_xlabel(f"1:{r}")
    #             ylim = axes[idx].get_ylim()
    #             axes[idx].text(1.5, ylim[1],  # Centered above both boxes
    #                            f"Ratio: 1:{r}", ha='center', va='bottom', fontsize=14)
    #             # axes[idx].set_ylabel("GB/s", rotation=90)
    #             for label in axes[idx].get_yticklabels():
    #                 label.set_rotation(45)
    #             # plt.suptitle(title)
    #             bp['boxes'][0].set_hatch(dh)
    #             bp['boxes'][1].set_hatch(dh)
    #             bp['boxes'][0].set_facecolor(cc)
    #             bp['boxes'][1].set_facecolor(cc)
    #             idx += 1

    # plt.subplots_adjust(wspace=0.0, hspace=0.0)
    # plt.tight_layout(pad=0.0)
    # plt.subplots_adjust(top=0.94)
    # # Alpha=.99 is due to a bug with pdf export
    # legends = [mpatches.Patch(facecolor='lightgreen', hatch='oo', label='Zipfian 512MiB', alpha=.99),
    #            mpatches.Patch(facecolor='lightgreen', hatch='//', label='Uniform 512MiB', alpha=.99),
    #            mpatches.Patch(facecolor='lightblue', hatch='oo', label='Zipfian 64KiB', alpha=.99),
    #            mpatches.Patch(facecolor='lightblue', hatch='//', label='Uniform 64KiB', alpha=.99)]

    # plt.legend(
    #     ncols=4,
    #     handles=legends,
    #     bbox_to_anchor=(1, -0.15),  # x = center, y = push it lower
    #     # loc='upper center',
    #     fontsize="large"
    # )
    # plt.savefig(genpdf_name, bbox_inches='tight')


def main():
    parser = argparse.ArgumentParser(description='Generate performance comparison tables from split data')
    parser.add_argument('split_data_dirs', nargs='+', help='Directory(s) containing split data output')
    parser.add_argument('--labels', '-l', nargs='+', default=['Block-interface', 'ZNS'],
                       help='Labels for each directory (default: Block-interface ZNS)')
    parser.add_argument('--output-dir', '-o', default='tables',
                       help='Output directory for tables (default: tables)')
    parser.add_argument('--format', choices=['markdown', 'latex', 'both'], default='both',
                       help='Output format (default: both)')
    
    args = parser.parse_args()
    
    if len(args.labels) != len(args.split_data_dirs):
        print(f"Error: Number of labels ({len(args.labels)}) must match number of directories ({len(args.split_data_dirs)})")
        return 1
    
    print("Collecting experiment data...")
    experiments = collect_experiment_data(args.split_data_dirs, args.labels)
    
    if not experiments:
        print("No experiment data found!")
        return 1
    
    output_path = Path(args.output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Define metrics to generate tables for
    metrics = [
        ('get_latency', 'Get Latency (End-to-End)', 'ms'),
        ('write_latency', 'Disk Write Latency', 'ms'), 
        ('read_latency', 'Disk Read Latency', 'ms'),
        ('hit_ratio', 'Hit Ratio', '%'),
        ('throughput_bytes', 'Throughput (Bytes Total)', 'MiB/s'),
        ('throughput_written', 'Throughput (Written Bytes)', 'MiB/s')
    ]
    
    for metric_key, metric_name, unit in metrics:
        print(f"Generating tables for {metric_name}...")
        
        if args.format in ['markdown', 'both']:
            markdown_content = generate_markdown_table(experiments, metric_key, metric_name, unit)
            markdown_file = output_path / f"{metric_key}_comparison.md"
            with open(markdown_file, 'w') as f:
                f.write(markdown_content)
            print(f"  Created {markdown_file}")
        
        if args.format in ['latex', 'both']:
            latex_content = generate_latex_table(experiments, metric_key, metric_name, unit)
            latex_file = output_path / f"{metric_key}_comparison.tex"
            with open(latex_file, 'w') as f:
                f.write(latex_content)
            print(f"  Created {latex_file}")
    
    # Generate combined files
    if args.format in ['markdown', 'both']:
        combined_md = output_path / "all_comparisons.md"
        with open(combined_md, 'w') as f:
            f.write("# Cache Performance Comparison Tables\n\n")
            f.write(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for metric_key, metric_name, unit in metrics:
                f.write(generate_markdown_table(experiments, metric_key, metric_name, unit))
        
        print(f"Created combined markdown file: {combined_md}")
    
    if args.format in ['latex', 'both']:
        combined_tex = output_path / "all_comparisons.tex"
        with open(combined_tex, 'w') as f:
            f.write("% Cache Performance Comparison Tables\n")
            f.write(f"% Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for metric_key, metric_name, unit in metrics:
                f.write(generate_latex_table(experiments, metric_key, metric_name, unit))
        
        print(f"Created combined LaTeX file: {combined_tex}")
    
    print(f"\nTable generation complete! Files saved to {output_path}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
