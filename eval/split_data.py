#!/usr/bin/env python3
"""
Script to split JSON data files by metric name.
Each JSON file gets its own directory, and within that directory,
data is split into separate files based on the 'name' field.
"""

import json
import os
import sys
from pathlib import Path
from collections import defaultdict


def process_json_file(json_file_path, target_dir):
    """Process a single JSON file and split data by metric name."""
    json_filename = json_file_path.stem
    output_dir = target_dir / json_filename
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Dictionary to store open file handles by metric name
    metric_files = {}
    
    print(f"Processing {json_file_path.name}...")
    
    try:
        with open(json_file_path, 'r') as f:
            line_count = 0
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    data = json.loads(line)
                    metric_name = data.get('fields', {}).get('name')
                    
                    if metric_name:
                        # Open file handle if not already open
                        if metric_name not in metric_files:
                            output_file = output_dir / f"{metric_name}.json"
                            metric_files[metric_name] = open(output_file, 'w')
                        
                        # Write line directly to file
                        metric_files[metric_name].write(line + '\n')
                        line_count += 1
                        
                        # Progress update every 100k lines
                        if line_count % 100000 == 0:
                            print(f"  Processed {line_count:,} lines...")
                    else:
                        print(f"Warning: No metric name found in line {line_num}")
                        
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON on line {line_num}: {e}")
                    continue
                    
    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found")
        return
    except Exception as e:
        print(f"Error reading {json_file_path}: {e}")
        return
    finally:
        # Close all file handles
        for metric_name, file_handle in metric_files.items():
            file_handle.close()
            output_file = output_dir / f"{metric_name}.json"
            print(f"  Created {output_file.relative_to(target_dir)}")
    
    print(f"  Completed processing {line_count:,} total lines")


def main():
    if len(sys.argv) != 2:
        print("Usage: python split_data.py <target_directory>")
        print("Example: python split_data.py data/BLOCK-PROMO")
        sys.exit(1)
    
    target_path = Path(sys.argv[1])
    
    if not target_path.exists():
        print(f"Error: Directory {target_path} does not exist")
        sys.exit(1)
    
    if not target_path.is_dir():
        print(f"Error: {target_path} is not a directory")
        sys.exit(1)
    
    # Find all JSON files in the target directory
    json_files = list(target_path.glob("*.json"))
    
    if not json_files:
        print(f"No JSON files found in {target_path}")
        sys.exit(1)
    
    print(f"Found {len(json_files)} JSON files to process")
    
    # Create output directory
    output_base = target_path / "split_output"
    output_base.mkdir(exist_ok=True)
    
    # Process each JSON file
    for json_file in json_files:
        process_json_file(json_file, output_base)
    
    print(f"\nData splitting complete! Output saved to {output_base}")


if __name__ == "__main__":
    main()