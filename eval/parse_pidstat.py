#!/usr/bin/env python3
"""
Parse pidstat output to determine distribution of memory usage and CPU usage.
Output as individual LaTeX tables with filename metadata parsing.
"""

import sys
import re
import statistics
from collections import defaultdict
import argparse
import os


def parse_filename_metadata(filename):
    """
    Parse filename to extract metadata components.
    Expected format: CHUNKSZ,L=LATENCY,DISTRIBUTION,R=RATIO,NZ+NUM_ZONES,disk_type
    """
    basename = os.path.basename(filename)
    
    # Remove common extensions
    basename = re.sub(r'\.(txt|log|pidstat)$', '', basename)
    
    metadata = {
        'chunk_size': 'Unknown',
        'latency': 'Unknown', 
        'distribution': 'Unknown',
        'ratio': 'Unknown',
        'num_zones': 'Unknown',
        'disk_type': 'Unknown'
    }
    
    # Split by common delimiters
    parts = re.split(r'[_\-,.]', basename)
    
    # The chunk size is typically the first numeric part
    for i, part in enumerate(parts):
        part = part.strip()
        if not part:
            continue
            
        # Parse latency (L=value)
        if part.startswith('L='):
            metadata['latency'] = part[2:]
        # Parse ratio (R=value)
        elif part.startswith('R='):
            metadata['ratio'] = part[2:]
        # Parse number of zones (NZ+value)
        elif part.startswith('NZ+'):
            metadata['num_zones'] = part[3:]
        # Parse disk type mappings
        elif 'nvme1n1' in part:
            metadata['disk_type'] = 'Block-interface'
        elif 'nvme0n2' in part:
            metadata['disk_type'] = 'ZNS'
        # Chunk size - look for the first pure numeric part (should be chunk size)
        elif (re.match(r'^\d+[KMG]?$', part) and ':' not in part and 
              metadata['chunk_size'] == 'Unknown'):
            metadata['chunk_size'] = part
        # Distribution patterns (only ZIPFIAN and UNIFORM are valid)
        elif part.lower() in ['uniform', 'zipfian']:
            if part.lower() == 'zipfian':
                metadata['distribution'] = 'Zipfian'
            elif part.lower() == 'uniform':
                metadata['distribution'] = 'Uniform'
    
    return metadata


def format_latex_table(cpu_data, memory_data, metadata, filename, args):
    """Format data as LaTeX table with metadata comment."""
    
    # Calculate statistics
    latex_output = []
    
    # Add comment with metadata
    latex_output.append(f"% File: {os.path.basename(filename)}")
    latex_output.append(f"% Chunk Size: {metadata['chunk_size']}, Latency: {metadata['latency']}")
    latex_output.append(f"% Distribution: {metadata['distribution']}, Ratio: {metadata['ratio']}")
    latex_output.append(f"% Zones: {metadata['num_zones']}, Disk Type: {metadata['disk_type']}")
    latex_output.append("")
    
    # CPU Table
    if cpu_data:
        cpu_stats = calculate_stats_dict(cpu_data)
        
        latex_output.append("\\begin{table}[H]")
        latex_output.append("\\centering")
        cpu_caption = f"CPU Usage Statistics - {metadata['disk_type']} (Chunk Size: {metadata['chunk_size']}, Distribution: {metadata['distribution']}, Ratio: {metadata['ratio']})"
        if args.caption_prefix:
            cpu_caption = f"{args.caption_prefix} {cpu_caption}"
        latex_output.append(f"\\caption{{{cpu_caption}}}")
        latex_output.append("\\begin{tabular}{|l|r|}")
        latex_output.append("\\hline")
        latex_output.append("Metric & Value (\\%) \\\\")
        latex_output.append("\\hline")
        latex_output.append(f"Max & {cpu_stats['max']:.2f} \\\\")
        latex_output.append(f"Mean & {cpu_stats['mean']:.2f} \\\\")
        latex_output.append(f"Median & {cpu_stats['median']:.2f} \\\\")
        latex_output.append(f"Std Dev & {cpu_stats['stddev']:.2f} \\\\")
        latex_output.append(f"95th Percentile & {cpu_stats['p95']:.2f} \\\\")
        latex_output.append(f"99th Percentile & {cpu_stats['p99']:.2f} \\\\")
        latex_output.append("\\hline")
        latex_output.append("\\end{tabular}")
        latex_output.append("\\end{table}")
        latex_output.append("")
    
    # Memory Table
    if memory_data:
        rss_data = [m['rss_kb'] for m in memory_data]
        if rss_data:
            # Convert KB to GiB
            rss_gib_data = [rss_kb / (1024 * 1024) for rss_kb in rss_data]
            mem_stats = calculate_stats_dict(rss_gib_data)
            
            latex_output.append("\\begin{table}[H]")
            latex_output.append("\\centering")
            mem_caption = f"Memory Usage Statistics - {metadata['disk_type']} (Chunk Size: {metadata['chunk_size']}, Distribution: {metadata['distribution']}, Ratio: {metadata['ratio']})"
            if args.caption_prefix:
                mem_caption = f"{args.caption_prefix} {mem_caption}"
            latex_output.append(f"\\caption{{{mem_caption}}}")
            latex_output.append("\\begin{tabular}{|l|r|}")
            latex_output.append("\\hline")
            latex_output.append("Metric & Value (GiB) \\\\")
            latex_output.append("\\hline")
            latex_output.append(f"Max & {mem_stats['max']:.3f} \\\\")
            latex_output.append(f"Mean & {mem_stats['mean']:.3f} \\\\")
            latex_output.append(f"Median & {mem_stats['median']:.3f} \\\\")
            latex_output.append(f"Std Dev & {mem_stats['stddev']:.3f} \\\\")
            latex_output.append(f"95th Percentile & {mem_stats['p95']:.3f} \\\\")
            latex_output.append(f"99th Percentile & {mem_stats['p99']:.3f} \\\\")
            latex_output.append("\\hline")
            latex_output.append("\\end{tabular}")
            latex_output.append("\\end{table}")
            latex_output.append("")
    
    return "\n".join(latex_output)


def calculate_stats_dict(data):
    """Calculate statistics and return as dictionary."""
    if not data:
        return {}
    
    stats = {
        'count': len(data),
        'min': min(data),
        'max': max(data),
        'mean': statistics.mean(data),
        'median': statistics.median(data),
        'stddev': statistics.stdev(data) if len(data) > 1 else 0.0
    }
    
    # Percentiles
    sorted_data = sorted(data)
    stats['p95'] = sorted_data[int(len(sorted_data) * 0.95)]
    stats['p99'] = sorted_data[int(len(sorted_data) * 0.99)]
    
    return stats


def parse_pidstat_file(filename):
    """Parse pidstat output file and extract CPU and memory metrics."""
    cpu_data = []
    memory_data = []
    debug_count = 0
    
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('Linux') or line.startswith('Average'):
                continue
            
            # Skip header lines
            if 'Time' in line and 'PID' in line:
                continue
            
            # Parse data lines
            parts = line.split()
            if len(parts) >= 9:
                try:
                    # CPU usage line format: Time UID PID %usr %system %guest %wait %CPU CPU Command (11 columns)
                    if (len(parts) >= 11 and 
                        parts[9].isdigit()):  # CPU column should be integer
                        # Test if %CPU column is numeric
                        float(parts[8])  # This will raise ValueError if not numeric
                        cpu_percent = float(parts[8])  # %CPU is in column 8 (0-indexed)
                        cpu_data.append(cpu_percent)
                    
                    # Memory usage line format: Time UID PID minflt/s majflt/s VSZ RSS %MEM Command (10 columns)
                    elif (len(parts) >= 10 and 
                          parts[6].isdigit() and parts[7].isdigit() and  # VSZ and RSS should be integers
                          not parts[8].isdigit()):  # CPU column should NOT be integer (to distinguish from CPU lines)
                        rss_kb = float(parts[7])  # RSS is in column 7 (0-indexed)
                        mem_percent = float(parts[8])  # %MEM is in column 8 (0-indexed)  
                        memory_data.append({'rss_kb': rss_kb, 'mem_percent': mem_percent})
                        
                except (ValueError, IndexError):
                    continue
    
    return cpu_data, memory_data


def calculate_statistics(data, label):
    """Calculate and print statistics for a dataset."""
    if not data:
        print(f"No {label} data found")
        return
    
    print(f"\n{label} Statistics:")
    print(f"  Count: {len(data)}")
    print(f"  Min: {min(data):.2f}")
    print(f"  Max: {max(data):.2f}")
    print(f"  Mean: {statistics.mean(data):.2f}")
    print(f"  Median: {statistics.median(data):.2f}")
    
    if len(data) > 1:
        print(f"  Std Dev: {statistics.stdev(data):.2f}")
    
    # Percentiles
    sorted_data = sorted(data)
    p95 = sorted_data[int(len(sorted_data) * 0.95)]
    p99 = sorted_data[int(len(sorted_data) * 0.99)]
    print(f"  95th percentile: {p95:.2f}")
    print(f"  99th percentile: {p99:.2f}")


def main():
    parser = argparse.ArgumentParser(description='Parse pidstat output and analyze CPU/memory usage')
    parser.add_argument('pidstat_file', help='Path to pidstat output file')
    parser.add_argument('--csv', action='store_true', help='Output in CSV format')
    parser.add_argument('--latex', action='store_true', help='Output in LaTeX table format')
    parser.add_argument('--caption-prefix', type=str, default='', help='Prefix for table captions')
    
    args = parser.parse_args()
    
    try:
        cpu_data, memory_data = parse_pidstat_file(args.pidstat_file)
        
        if args.latex:
            # LaTeX output
            metadata = parse_filename_metadata(args.pidstat_file)
            latex_output = format_latex_table(cpu_data, memory_data, metadata, args.pidstat_file, args)
            print(latex_output)
        elif args.csv:
            # CSV output
            print("metric,count,min,max,mean,median,stddev,p95,p99")
            
            if cpu_data:
                cpu_stats = {
                    'count': len(cpu_data),
                    'min': min(cpu_data),
                    'max': max(cpu_data),
                    'mean': statistics.mean(cpu_data),
                    'median': statistics.median(cpu_data),
                    'stddev': statistics.stdev(cpu_data) if len(cpu_data) > 1 else 0,
                    'p95': sorted(cpu_data)[int(len(cpu_data) * 0.95)],
                    'p99': sorted(cpu_data)[int(len(cpu_data) * 0.99)]
                }
                print(f"cpu_percent,{cpu_stats['count']},{cpu_stats['min']:.2f},{cpu_stats['max']:.2f},"
                      f"{cpu_stats['mean']:.2f},{cpu_stats['median']:.2f},{cpu_stats['stddev']:.2f},"
                      f"{cpu_stats['p95']:.2f},{cpu_stats['p99']:.2f}")
            
            if memory_data:
                rss_data = [m['rss_kb'] for m in memory_data]
                mem_percent_data = [m['mem_percent'] for m in memory_data if m['mem_percent'] > 0]
                
                if rss_data:
                    rss_stats = {
                        'count': len(rss_data),
                        'min': min(rss_data),
                        'max': max(rss_data),
                        'mean': statistics.mean(rss_data),
                        'median': statistics.median(rss_data),
                        'stddev': statistics.stdev(rss_data) if len(rss_data) > 1 else 0,
                        'p95': sorted(rss_data)[int(len(rss_data) * 0.95)],
                        'p99': sorted(rss_data)[int(len(rss_data) * 0.99)]
                    }
                    print(f"memory_rss_kb,{rss_stats['count']},{rss_stats['min']:.2f},{rss_stats['max']:.2f},"
                          f"{rss_stats['mean']:.2f},{rss_stats['median']:.2f},{rss_stats['stddev']:.2f},"
                          f"{rss_stats['p95']:.2f},{rss_stats['p99']:.2f}")
        else:
            # Human readable output
            print(f"Parsed pidstat file: {args.pidstat_file}")
            
            calculate_statistics(cpu_data, "CPU Usage (%)")
            
            if memory_data:
                rss_data = [m['rss_kb'] for m in memory_data]
                mem_percent_data = [m['mem_percent'] for m in memory_data if m['mem_percent'] > 0]
                
                calculate_statistics(rss_data, "Memory RSS (KB)")
                if mem_percent_data:
                    calculate_statistics(mem_percent_data, "Memory Usage (%)")
    
    except FileNotFoundError:
        print(f"Error: File '{args.pidstat_file}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()