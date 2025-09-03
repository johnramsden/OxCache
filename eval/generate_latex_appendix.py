#!/usr/bin/env python3

import os
import sys
import re
from pathlib import Path
from collections import defaultdict

def parse_filename(filename):
    """Parse filename to extract parameters"""
    # Remove .png extension
    basename = filename.replace('.png', '')
    
    # Split by commas
    parts = basename.split(',')
    
    if len(parts) < 3:
        return None
    
    # Extract chunk size (convert to readable format)
    chunk_size_bytes = int(parts[0])
    if chunk_size_bytes >= 1024*1024*1024:
        chunk_size = f"{chunk_size_bytes//1024//1024//1024}G"
    elif chunk_size_bytes >= 1024*1024:
        chunk_size = f"{chunk_size_bytes//1024//1024}M"
    elif chunk_size_bytes >= 1024:
        chunk_size = f"{chunk_size_bytes//1024}K"
    else:
        chunk_size = str(chunk_size_bytes)
    
    # Extract distribution
    distribution = parts[2]
    
    # Extract ratio from R=X
    ratio = None
    for part in parts:
        if part.startswith('R='):
            ratio = part[2:]
            break
    
    # Extract metric type from the end
    metric_parts = parts[-1].split('_')
    
    # Determine metric type and category
    if 'hitratio' in filename:
        metric_type = 'hit_ratio'
        metric_category = 'Hit Ratio'
    elif 'get_total_latency' in filename:
        metric_type = 'get_latency'
        metric_category = 'Get Latency'
    elif 'get_hit_latency' in filename:
        metric_type = 'hit_latency'
        metric_category = 'Hit Latency'
    elif 'get_miss_latency' in filename:
        metric_type = 'miss_latency'
        metric_category = 'Miss Latency'
    elif 'disk_read_latency' in filename:
        metric_type = 'read_latency'
        metric_category = 'Read Latency'
    elif 'disk_write_latency' in filename or 'device_write_latency' in filename:
        metric_type = 'write_latency'
        metric_category = 'Write Latency'
    elif 'read_bytes_total_throughput' in filename:
        metric_type = 'read_thpt'
        metric_category = 'Read Throughput'
    elif 'written_bytes_total_throughput' in filename:
        metric_type = 'write_thpt'
        metric_category = 'Write Throughput'
    elif 'bytes_total_throughput' in filename:
        metric_type = 'thpt'
        metric_category = 'Get Throughput'
    else:
        return None
    
    return {
        'chunk_size': chunk_size,
        'distribution': distribution,
        'ratio': ratio,
        'metric_type': metric_type,
        'metric_category': metric_category,
        'filename': filename
    }

def generate_figure_latex(file_info, input_dir, figure_base_path):
    """Generate LaTeX figure block for a single file"""
    chunk_size = file_info['chunk_size']
    distribution = file_info['distribution']
    ratio = file_info['ratio']
    metric_type = file_info['metric_type']
    filename = file_info['filename']
    
    # Create figure path (keep original .png extension)
    figure_path = f"{figure_base_path}/{filename}"
    
    # Generate label
    label = f"fig:{metric_type}-{chunk_size}-{distribution}-{ratio}"
    
    # Generate caption based on metric type
    if metric_type == 'hit_ratio':
        caption = f"Cache Hit Ratio for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
    elif 'latency' in metric_type:
        if metric_type == 'get_latency':
            caption = f"Cache Get latency (ms) for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
        elif metric_type == 'hit_latency':
            caption = f"Cache Hit latency (ms) for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
        elif metric_type == 'miss_latency':
            caption = f"Cache Miss latency (ms) for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
        elif metric_type == 'read_latency':
            caption = f"Disk Read latency (ms) for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
        elif metric_type == 'write_latency':
            caption = f"Disk Write latency (ms) for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
    elif 'thpt' in metric_type:
        if metric_type == 'thpt':
            caption = f"Cache Get Throughput (GiB/s) for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
        elif metric_type == 'read_thpt':
            caption = f"Disk Read Throughput (GiB/s) for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
        elif metric_type == 'write_thpt':
            caption = f"Disk Write Throughput (GiB/s) for {chunk_size} chunk size, {distribution.title()} distribution, and 1:{ratio} ratio."
    
    return f"""\\begin{{figure}}[H]
\\centering
\\includegraphics[width=\\columnwidth]{{{figure_path}}}
\\caption{{{caption}}}
\\label{{{label}}}
\\end{{figure}}"""

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 generate_latex_appendix.py <input_directory>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    if not os.path.isdir(input_dir):
        print(f"Error: {input_dir} is not a valid directory")
        sys.exit(1)
    
    # Get directory name for figure path
    dir_name = os.path.basename(input_dir.rstrip('/'))
    figure_base_path = f"content/figures/{dir_name}"
    
    # Parse all files
    files_by_category = defaultdict(list)
    
    for filename in os.listdir(input_dir):
        if filename.endswith('.png'):
            file_info = parse_filename(filename)
            if file_info:
                files_by_category[file_info['metric_category']].append(file_info)
    
    # Sort files within each category
    for category in files_by_category:
        files_by_category[category].sort(key=lambda x: (x['chunk_size'], x['distribution'], int(x['ratio'])))
    
    # Define section order with labels
    section_order = [
        ('Get Latency', 'appendix:get-latency'),
        ('Hit Latency', 'appendix:hit-latency'), 
        ('Miss Latency', 'appendix:miss-latency'),
        ('Read Latency', 'appendix:read-latency'),
        ('Write Latency', 'appendix:write-latency'),
        ('Get Throughput', 'appendix:get-throughput'),
        ('Read Throughput', 'appendix:read-throughput'),
        ('Write Throughput', 'appendix:write-throughput'),
        ('Hit Ratio', 'appendix:hit-ratio')
    ]
    
    # Generate LaTeX output
    print("% Generated LaTeX appendix")
    print()
    
    for section_name, section_label in section_order:
        if section_name in files_by_category:
            print(f"\\subsection{{{section_name}}}\\label{{{section_label}}}")
            print()
            
            for file_info in files_by_category[section_name]:
                print(generate_figure_latex(file_info, input_dir, figure_base_path))
            
            print()

if __name__ == "__main__":
    main()