#!/usr/bin/env python3
"""
Script to generate latency statistics (mean, median, 95%, 99%) for each run in 
BLOCK-PROMO-2025-08-31 or ZONED-PROMO-2025-08-30 directories.
Based on generate_throughput_stats.py structure.
"""

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict

def generate_map_files(logs_dir, split_output_dir):
    """Dynamically generate mapping from metrics directory names to experiment filenames."""
    map_files = {}
    
    # Find all .client files in logs directory
    client_files = list(logs_dir.glob("*.client"))
    
    # Get the experiment configs from client files
    experiment_configs = []
    for client_file in client_files:
        filename = client_file.stem
        if filename.endswith("-run"):
            filename = filename[:-4]
        
        # Extract experiment part (everything before timestamp)
        parts = filename.rsplit('-', 1)
        if len(parts) >= 2:
            experiment_part = parts[0]
            experiment_configs.append(experiment_part)
    
    # Get metrics directories
    metrics_dirs = [d for d in split_output_dir.iterdir() if d.is_dir()]
    
    # Simple approach: if we have the same number of each, match in sorted order
    if len(experiment_configs) == len(metrics_dirs):
        sorted_configs = sorted(experiment_configs)
        sorted_metrics = sorted([d.name for d in metrics_dirs])
        
        for config, metrics in zip(sorted_configs, sorted_metrics):
            map_files[metrics] = config
    else:
        # Fallback: try to find matching config by looking at timestamps
        for i, config in enumerate(sorted(experiment_configs)):
            if i < len(sorted(metrics_dirs)):
                map_files[sorted([d.name for d in metrics_dirs])[i]] = config
    
    return map_files


def parse_timestamp(timestamp_str):
    """Parse ISO timestamp string to datetime object."""
    return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))


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


def format_latency(latency_ms, unit="ms"):
    """Format latency with appropriate unit."""
    if latency_ms >= 1000:  # >= 1 second
        return f"{latency_ms / 1000:.2f} s"
    elif latency_ms >= 1:  # >= 1 ms
        return f"{latency_ms:.2f} ms"
    else:  # < 1 ms, show in microseconds
        return f"{latency_ms * 1000:.2f} μs"


def compare_interfaces(all_stats):
    """Compare ZNS and Block interface latency performance and calculate percentage differences."""
    # Group stats by interface
    zns_stats = [s for s in all_stats if s['interface'] == 'ZNS']
    block_stats = [s for s in all_stats if s['interface'] == 'Block']
    
    if not zns_stats or not block_stats:
        print("Warning: Need both ZNS and Block data for comparison")
        return None
    
    # Calculate per-configuration comparisons
    comparisons = []
    
    # Group by experiment configuration (chunk_size, distribution, ratio)
    zns_configs = {}
    block_configs = {}
    
    for stats in zns_stats:
        key = (stats['chunk_size'], stats['distribution'], stats['ratio'])
        zns_configs[key] = stats
    
    for stats in block_stats:
        key = (stats['chunk_size'], stats['distribution'], stats['ratio'])
        block_configs[key] = stats
    
    # Find matching configurations and compare
    for config_key in zns_configs:
        if config_key in block_configs:
            zns = zns_configs[config_key]
            block = block_configs[config_key]
            
            # Calculate percentage differences using (block - zns) / block * 100
            # For latency, positive percentage means ZNS is better (lower latency)
            def calc_percentage_diff(zns_val, block_val):
                if block_val == 0:
                    return 0.0
                return ((block_val - zns_val) / block_val) * 100
            
            comparison = {
                'config': f"{config_key[0]}-{config_key[1]}-R{config_key[2]}",
                'chunk_size': config_key[0],
                'distribution': config_key[1],
                'ratio': config_key[2],
                'zns_mean': zns['mean'],
                'block_mean': block['mean'],
                'zns_p95': zns['p95'],
                'block_p95': block['p95'],
                'zns_p99': zns['p99'],
                'block_p99': block['p99'],
                'mean_diff_pct': calc_percentage_diff(zns['mean'], block['mean']),
                'p95_diff_pct': calc_percentage_diff(zns['p95'], block['p95']),
                'p99_diff_pct': calc_percentage_diff(zns['p99'], block['p99'])
            }
            comparisons.append(comparison)
    
    return comparisons


def print_interface_comparison(comparisons):
    """Print detailed interface comparison results for latency."""
    if not comparisons:
        return
    
    print("=" * 100)
    print("INTERFACE COMPARISON: ZNS vs Block (Latency - Lower is Better)")
    print("=" * 100)
    print(f"{'Configuration':<25} {'Mean Diff':<12} {'95th % Diff':<12} {'99th % Diff':<12} {'Notes'}")
    print("-" * 100)
    
    total_mean_diffs = []
    total_p95_diffs = []
    total_p99_diffs = []
    
    for comp in comparisons:
        # Format percentage differences with indicators (for latency, positive is better - lower latency)
        mean_sign = "↓" if comp['mean_diff_pct'] > 0 else "↑" if comp['mean_diff_pct'] < 0 else "="
        p95_sign = "↓" if comp['p95_diff_pct'] > 0 else "↑" if comp['p95_diff_pct'] < 0 else "="
        p99_sign = "↓" if comp['p99_diff_pct'] > 0 else "↑" if comp['p99_diff_pct'] < 0 else "="
        
        mean_str = f"{mean_sign}{abs(comp['mean_diff_pct']):.1f}%"
        p95_str = f"{p95_sign}{abs(comp['p95_diff_pct']):.1f}%"
        p99_str = f"{p99_sign}{abs(comp['p99_diff_pct']):.1f}%"
        
        # Add notes for significant differences
        notes = []
        if abs(comp['mean_diff_pct']) > 50:
            notes.append("Major diff")
        if abs(comp['mean_diff_pct']) > 100:
            notes.append("2x+ diff")
        if comp['mean_diff_pct'] < -50:
            notes.append("ZNS much faster")
        if comp['mean_diff_pct'] > 50:
            notes.append("Block much faster")
        
        print(f"{comp['config']:<25} {mean_str:<12} {p95_str:<12} {p99_str:<12} {', '.join(notes)}")
        
        total_mean_diffs.append(comp['mean_diff_pct'])
        total_p95_diffs.append(comp['p95_diff_pct'])
        total_p99_diffs.append(comp['p99_diff_pct'])
    
    # Calculate overall averages
    avg_mean_diff = np.mean(total_mean_diffs)
    avg_p95_diff = np.mean(total_p95_diffs)
    avg_p99_diff = np.mean(total_p99_diffs)
    
    print("-" * 100)
    print("OVERALL COMPARISON (Lower latency = Better performance):")
    mean_trend = "faster" if avg_mean_diff < 0 else "slower" if avg_mean_diff > 0 else "similar"
    p95_trend = "faster" if avg_p95_diff < 0 else "slower" if avg_p95_diff > 0 else "similar"
    p99_trend = "faster" if avg_p99_diff < 0 else "slower" if avg_p99_diff > 0 else "similar"
    
    print(f"Average Mean Latency:     ZNS is {abs(avg_mean_diff):.1f}% {mean_trend} than Block")
    print(f"Average 95th Percentile:  ZNS is {abs(avg_p95_diff):.1f}% {p95_trend} than Block") 
    print(f"Average 99th Percentile:  ZNS is {abs(avg_p99_diff):.1f}% {p99_trend} than Block")
    print()
    
    # Break down by chunk size
    chunk_512m = [c for c in comparisons if '512M' in c['config']]
    chunk_64k = [c for c in comparisons if '64K' in c['config']]
    
    if chunk_512m:
        chunk_512m_mean_diff = np.mean([c['mean_diff_pct'] for c in chunk_512m])
        chunk_512m_trend = "faster" if chunk_512m_mean_diff < 0 else "slower"
        print(f"512MiB Chunks: ZNS is {abs(chunk_512m_mean_diff):.1f}% {chunk_512m_trend} on average")
    
    if chunk_64k:
        chunk_64k_mean_diff = np.mean([c['mean_diff_pct'] for c in chunk_64k])
        chunk_64k_trend = "faster" if chunk_64k_mean_diff < 0 else "slower"
        print(f"64KiB Chunks:  ZNS is {abs(chunk_64k_mean_diff):.1f}% {chunk_64k_trend} on average")
    
    print()
    
    return {
        'avg_mean_diff': avg_mean_diff,
        'avg_p95_diff': avg_p95_diff,
        'avg_p99_diff': avg_p99_diff,
        'comparisons': comparisons
    }


def export_latex_table(all_stats, output_file="latency_results.tex", metric_name="get_total_latency_ms"):
    """Export results to a LaTeX table with ZNS/Block pairs and percentage improvements."""
    
    # Group stats by interface and configuration
    zns_stats = {(s['chunk_size'], s['distribution'], s['ratio']): s 
                 for s in all_stats if s['interface'] == 'ZNS'}
    block_stats = {(s['chunk_size'], s['distribution'], s['ratio']): s 
                   for s in all_stats if s['interface'] == 'Block'}
    
    # Find matching configurations
    matched_configs = []
    for config_key in sorted(zns_stats.keys()):
        if config_key in block_stats:
            zns = zns_stats[config_key]
            block = block_stats[config_key]
            
            # Calculate percentage improvement ((block - zns) / block * 100)
            # Positive means ZNS is better (lower latency)
            mean_improvement = ((block['mean'] - zns['mean']) / block['mean']) * 100
            p99_improvement = ((block['p99'] - zns['p99']) / block['p99']) * 100
            
            matched_configs.append({
                'zns': zns,
                'block': block,
                'mean_improvement': mean_improvement,
                'p99_improvement': p99_improvement
            })
    
    if not matched_configs:
        print("Warning: No matching ZNS/Block configurations found for LaTeX export")
        return
    
    # Generate LaTeX table
    latex_content = []
    latex_content.append("\\begin{table}[htbp]")
    latex_content.append("\\centering")
    latex_content.append("\\begin{tabular}{|l|l|l|}")
    latex_content.append("\\hline")
    latex_content.append("Name & Mean (ms) & P99 (ms) \\\\")
    latex_content.append("\\hline")
    
    for config in matched_configs:
        zns = config['zns']
        block = config['block']
        mean_imp = config['mean_improvement']
        p99_imp = config['p99_improvement']
        
        # Format experiment names
        zns_name = f"ZNS-{zns['chunk_size']}-{zns['distribution'][:4]}-{zns['ratio']}"
        block_name = f"Block-{block['chunk_size']}-{block['distribution'][:4]}-{block['ratio']}"
        
        # Format values with improvement percentages
        zns_mean_str = f"{zns['mean']:.2f} ({mean_imp:+.2f}\\%)"
        zns_p99_str = f"{zns['p99']:.2f} ({p99_imp:+.2f}\\%)"
        
        block_mean_str = f"{block['mean']:.2f}"
        block_p99_str = f"{block['p99']:.2f}"
        
        # Add ZNS row
        latex_content.append(f"{zns_name} & {zns_mean_str} & {zns_p99_str} \\\\")
        # Add Block row immediately after
        latex_content.append(f"{block_name} & {block_mean_str} & {block_p99_str} \\\\")
    
    latex_content.append("\\hline")
    latex_content.append("\\end{tabular}")
    latex_content.append(f"\\caption{{Latency comparison results for {metric_name}}}")
    latex_content.append(f"\\label{{tab:latency_{metric_name.replace('_', '_')}}}")
    latex_content.append("\\end{table}")
    
    # Write to file
    try:
        with open(output_file, 'w') as f:
            f.write('\n'.join(latex_content))
        print(f"LaTeX table exported to {output_file}")
        return output_file
    except Exception as e:
        print(f"Error writing LaTeX table: {e}")
        return None


def process_single_directory(data_dir, metric_name="get_total_latency_ms"):
    """Process a single data directory and return latency statistics."""
    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"Error: Directory {data_dir} does not exist")
        return []
    
    # Find logs directory
    logs_dir = data_path / "logs"
    if not logs_dir.exists():
        print(f"Error: No logs directory found in {data_dir}")
        return []
    
    # Find split_output directory
    split_output_dir = logs_dir / "split_output"
    if not split_output_dir.exists():
        print(f"Error: No split_output directory found in {logs_dir}")
        return []
    
    print(f"Processing {data_dir}...")
    
    # Generate dynamic mapping from client files
    map_files = generate_map_files(logs_dir, split_output_dir)
    
    if not map_files:
        print(f"Warning: No client files found in {data_dir}")
        return []
    
    # Process each metrics directory
    metrics_dirs = [d for d in split_output_dir.iterdir() if d.is_dir()]
    
    dir_stats = []
    
    for metrics_dir in sorted(metrics_dirs):
        if metrics_dir.name not in map_files:
            print(f"  Warning: No mapping found for {metrics_dir.name}, skipping")
            continue
        
        # Get experiment info
        experiment_info = extract_experiment_info(map_files[metrics_dir.name])
        if not experiment_info:
            print(f"  Warning: Could not parse experiment info for {metrics_dir.name}")
            continue
        
        # Read metric data
        metric_file = metrics_dir / f"{metric_name}.json"
        if not metric_file.exists():
            print(f"  Warning: {metric_file} not found, skipping")
            continue
        
        print(f"  Processing {experiment_info['name']} ({metrics_dir.name})...")
        
        # Load latency values
        latency_values = []
        try:
            with open(metric_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        # For latency metrics, we collect the raw values (not cumulative)
                        value = float(data['fields']['value'])
                        latency_values.append(value)
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue
        except Exception as e:
            print(f"    Error reading {metric_file}: {e}")
            continue
        
        if len(latency_values) < 1:
            print(f"    Warning: No latency data points ({len(latency_values)})")
            continue
        
        # Calculate statistics
        latency_array = np.array(latency_values)
        mean_lat = np.mean(latency_array)
        median_lat = np.median(latency_array)
        std_lat = np.std(latency_array)
        min_lat = np.min(latency_array)
        max_lat = np.max(latency_array)
        percentile_95 = np.percentile(latency_array, 95)
        percentile_99 = np.percentile(latency_array, 99)
        
        # Store results
        stats = {
            'experiment': experiment_info['name'],
            'interface': experiment_info['interface'],
            'chunk_size': experiment_info['chunk_size'],
            'distribution': experiment_info['distribution'],
            'ratio': experiment_info['ratio'],
            'data_points': len(latency_array),
            'mean': mean_lat,
            'median': median_lat,
            'std': std_lat,
            'min': min_lat,
            'max': max_lat,
            'p95': percentile_95,
            'p99': percentile_99,
            'source_dir': data_dir
        }
        dir_stats.append(stats)
        
        # Print statistics
        print(f"    Data points: {len(latency_array)}")
        print(f"    Mean:        {format_latency(mean_lat)}")
        print(f"    Median:      {format_latency(median_lat)}")
        print(f"    Std Dev:     {format_latency(std_lat)}")
        print(f"    Min:         {format_latency(min_lat)}")
        print(f"    Max:         {format_latency(max_lat)}")
        print(f"    95th %ile:   {format_latency(percentile_95)}")
        print(f"    99th %ile:   {format_latency(percentile_99)}")
        print()
    
    return dir_stats


def generate_latency_statistics(data_dirs, metric_name="get_total_latency_ms", compare_interfaces_flag=False, latex_output=None):
    """Generate latency statistics for all runs in the specified data directories."""
    
    if isinstance(data_dirs, str):
        data_dirs = [data_dirs]
    
    print(f"Generating latency statistics for {len(data_dirs)} directory(ies)")
    print(f"Directories: {', '.join(data_dirs)}")
    print(f"Metric: {metric_name}")
    print("=" * 80)
    
    all_stats = []
    
    # Process each directory
    for data_dir in data_dirs:
        dir_stats = process_single_directory(data_dir, metric_name)
        all_stats.extend(dir_stats)
        print()
    
    # Print summary table
    if all_stats:
        print("=" * 80)
        print("SUMMARY TABLE")
        print("=" * 80)
        print(f"{'Experiment':<20} {'Mean':<12} {'95th %':<12} {'99th %':<12}")
        print("-" * 80)
        
        for stats in all_stats:
            mean_str = format_latency(stats['mean']).split()[0]
            p95_str = format_latency(stats['p95']).split()[0]
            p99_str = format_latency(stats['p99']).split()[0]
            print(f"{stats['experiment']:<20} {mean_str:<12} {p95_str:<12} {p99_str:<12}")
        
        print()
        
        # Group by interface
        zns_stats = [s for s in all_stats if s['interface'] == 'ZNS']
        block_stats = [s for s in all_stats if s['interface'] == 'Block']
        
        if zns_stats:
            print("ZNS Interface Summary:")
            zns_means = [s['mean'] for s in zns_stats]
            zns_p95s = [s['p95'] for s in zns_stats]
            zns_p99s = [s['p99'] for s in zns_stats]
            print(f"  Mean latency:     {format_latency(np.mean(zns_means))}")
            print(f"  Mean 95th %ile:   {format_latency(np.mean(zns_p95s))}")
            print(f"  Mean 99th %ile:   {format_latency(np.mean(zns_p99s))}")
            print()
        
        if block_stats:
            print("Block Interface Summary:")
            block_means = [s['mean'] for s in block_stats]
            block_p95s = [s['p95'] for s in block_stats]
            block_p99s = [s['p99'] for s in block_stats]
            print(f"  Mean latency:     {format_latency(np.mean(block_means))}")
            print(f"  Mean 95th %ile:   {format_latency(np.mean(block_p95s))}")
            print(f"  Mean 99th %ile:   {format_latency(np.mean(block_p99s))}")
            print()
        
        # Perform interface comparison if requested
        if compare_interfaces_flag:
            print()
            comparisons = compare_interfaces(all_stats)
            if comparisons:
                comparison_results = print_interface_comparison(comparisons)
        
        # Export LaTeX table if requested
        if latex_output:
            print()
            export_latex_table(all_stats, latex_output, metric_name)


def main():
    parser = argparse.ArgumentParser(description='Generate latency statistics for experiment runs')
    parser.add_argument('data_dirs', nargs='+', help='Data directory(ies) (BLOCK-PROMO-2025-08-31, ZONED-PROMO-2025-08-30, etc.)')
    parser.add_argument('--metric', '-m', default='get_total_latency_ms',
                       help='Metric to analyze (default: get_total_latency_ms)')
    parser.add_argument('--compare', '-c', action='store_true',
                       help='Compare ZNS vs Block interfaces and show percentage differences')
    parser.add_argument('--latex', '-l', type=str, metavar='FILE',
                       help='Export results to LaTeX table file (e.g., --latex results.tex)')
    
    args = parser.parse_args()
    
    generate_latency_statistics(args.data_dirs, args.metric, args.compare, args.latex)


if __name__ == "__main__":
    main()