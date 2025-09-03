#!/usr/bin/env python3
"""
Calculate average statistics across multiple pidstat files and output as LaTeX tables.
Groups files by configuration parameters and shows averages for each group.
"""

import sys
import re
import statistics
import os
import argparse
import glob
from collections import defaultdict


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


def parse_pidstat_file(filename):
    """Parse pidstat output file and extract CPU and memory metrics."""
    cpu_data = []
    memory_data = []
    
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


def create_config_key(metadata):
    """Create a configuration key for grouping similar files."""
    return metadata['chunk_size']


def format_average_latex_table(grouped_data, args):
    """Format grouped data as LaTeX tables showing averages."""
    latex_output = []
    
    for chunk_size, files_data in grouped_data.items():
        
        # Group by disk type within this chunk size
        disk_groups = defaultdict(list)
        for file_data in files_data:
            disk_type = file_data['metadata']['disk_type']
            disk_groups[disk_type].append(file_data)
        
        for disk_type, disk_files in disk_groups.items():
            # Calculate averages across all files for this chunk size and disk type
            all_cpu_means = []
            all_memory_means = []
            
            for file_data in disk_files:
                cpu_data, memory_data = file_data['data']
                
                if cpu_data:
                    all_cpu_means.append(statistics.mean(cpu_data))
                
                if memory_data:
                    rss_data = [m['rss_kb'] / (1024 * 1024) for m in memory_data]  # Convert to GiB
                    if rss_data:
                        all_memory_means.append(statistics.mean(rss_data))
            
            # Add comment with configuration
            latex_output.append(f"% Chunk Size: {chunk_size}, Disk Type: {disk_type}")
            latex_output.append(f"% Files analyzed: {len(disk_files)}")
            latex_output.append("")
            
            # Combined CPU and Memory Table
            if all_cpu_means or all_memory_means:
                latex_output.append("\\begin{table}[H]")
                latex_output.append("\\centering")
                caption = f"Average Resource Usage - {disk_type} Chunk Size {chunk_size}"
                if args.caption_prefix:
                    caption = f"{args.caption_prefix} {caption}"
                latex_output.append(f"\\caption{{{caption}}}")
                latex_output.append("\\begin{tabular}{|l|r|}")
                latex_output.append("\\hline")
                latex_output.append("Metric & Value \\\\")
                latex_output.append("\\hline")
                
                if all_cpu_means:
                    cpu_avg_stats = calculate_stats_dict(all_cpu_means)
                    latex_output.append(f"CPU Usage (\\%) & {cpu_avg_stats['mean']:.2f} \\\\")
                
                if all_memory_means:
                    mem_avg_stats = calculate_stats_dict(all_memory_means)
                    latex_output.append(f"Memory Usage (GiB) & {mem_avg_stats['mean']:.3f} \\\\")
                
                latex_output.append("\\hline")
                latex_output.append("\\end{tabular}")
                latex_output.append("\\end{table}")
                latex_output.append("")
    
    return "\n".join(latex_output)


def main():
    parser = argparse.ArgumentParser(description='Calculate average statistics across multiple pidstat files')
    parser.add_argument('files', nargs='+', help='Pidstat files or glob patterns')
    parser.add_argument('--csv', action='store_true', help='Output in CSV format')
    parser.add_argument('--latex', action='store_true', help='Output in LaTeX table format (default)')
    parser.add_argument('--caption-prefix', type=str, default='', help='Prefix for table captions')
    
    args = parser.parse_args()
    
    # Expand glob patterns
    all_files = []
    for file_pattern in args.files:
        if '*' in file_pattern or '?' in file_pattern:
            all_files.extend(glob.glob(file_pattern))
        else:
            all_files.append(file_pattern)
    
    # Group files by configuration
    grouped_data = defaultdict(list)
    
    for filename in all_files:
        if not os.path.exists(filename):
            print(f"Warning: File {filename} does not exist", file=sys.stderr)
            continue
            
        try:
            metadata = parse_filename_metadata(filename)
            cpu_data, memory_data = parse_pidstat_file(filename)
            
            config_key = create_config_key(metadata)
            grouped_data[config_key].append({
                'filename': filename,
                'metadata': metadata,
                'data': (cpu_data, memory_data)
            })
            
        except Exception as e:
            print(f"Error processing {filename}: {e}", file=sys.stderr)
            continue
    
    if not grouped_data:
        print("No valid pidstat files found", file=sys.stderr)
        sys.exit(1)
    
    if args.csv:
        # CSV output
        print("config,chunk_size,metric,avg_mean,avg_median,avg_min,avg_max,avg_stddev,file_count")
        
        for chunk_size, files_data in grouped_data.items():
            config_str = f"chunk_{chunk_size}"
            
            # Calculate CPU averages
            cpu_means = []
            memory_means = []
            
            for file_data in files_data:
                cpu_data, memory_data = file_data['data']
                
                if cpu_data:
                    cpu_means.append(statistics.mean(cpu_data))
                
                if memory_data:
                    rss_data = [m['rss_kb'] / (1024 * 1024) for m in memory_data]
                    if rss_data:
                        memory_means.append(statistics.mean(rss_data))
            
            if cpu_means:
                cpu_stats = calculate_stats_dict(cpu_means)
                print(f"{config_str},{chunk_size},cpu_percent,"
                      f"{cpu_stats['mean']:.2f},{cpu_stats['median']:.2f},{cpu_stats['min']:.2f},"
                      f"{cpu_stats['max']:.2f},{cpu_stats['stddev']:.2f},{len(files_data)}")
            
            if memory_means:
                mem_stats = calculate_stats_dict(memory_means)
                print(f"{config_str},{chunk_size},memory_gib,"
                      f"{mem_stats['mean']:.3f},{mem_stats['median']:.3f},{mem_stats['min']:.3f},"
                      f"{mem_stats['max']:.3f},{mem_stats['stddev']:.3f},{len(files_data)}")
    
    else:
        # LaTeX output (default)
        latex_output = format_average_latex_table(grouped_data, args)
        print(latex_output)


if __name__ == "__main__":
    main()