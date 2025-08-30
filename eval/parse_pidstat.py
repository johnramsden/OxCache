#!/usr/bin/env python3
"""
Parse pidstat output to determine distribution of memory usage and CPU usage.
"""

import sys
import re
import statistics
from collections import defaultdict
import argparse


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
    
    args = parser.parse_args()
    
    try:
        cpu_data, memory_data = parse_pidstat_file(args.pidstat_file)
        
        if args.csv:
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