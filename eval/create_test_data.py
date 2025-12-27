#!/usr/bin/env python3
"""
Generate test data structure for testing distribution_comparison_boxplots.py.

Creates small JSON files that simulate the full parameter spread from the real
data directories, but with only 500 lines each for fast testing.
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

# Test data configuration
NUM_LINES = 500
EVICTION_START_LINE = 200  # Line where eviction begins (usage starts decreasing)

# Distributions
DISTRIBUTIONS = ["ZIPFIAN", "UNIFORM"]

# Chunk sizes and their parameters
CHUNK_CONFIGS = [
    {"size": 65536, "L": 40632, "I": 196608000},           # 64KiB
    {"size": 268435456, "L": 3209583, "I": 48000},         # 256MiB
    {"size": 1129316352, "L": 5413781, "I": 12000},        # 1076MiB
]

# Ratios
RATIOS = [2, 10]

# Eviction types
EVICTION_TYPES = {
    65536: ["chunk", "promotional"],
    268435456: ["chunk", "promotional"],
    1129316352: ["promotional"],  # Only promotional for largest chunk size
}

# Devices
DEVICES = {
    "ZNS": "nvme0n2",
    "Block": "nvme1n1"
}


def generate_usage_percentage_data(num_lines, eviction_start):
    """
    Generate usage_percentage data that increases to ~99%, then drops and fluctuates.

    Args:
        num_lines: Total number of lines to generate
        eviction_start: Line number where eviction begins (usage drops)

    Returns:
        list: List of usage percentage values
    """
    usage_data = []

    # Pre-eviction: increase from 0% to 99%
    for i in range(eviction_start):
        progress = i / eviction_start
        # Sigmoid-like curve approaching 99%
        usage = 99.0 * (1 - np.exp(-5 * progress))
        usage_data.append(usage)

    # Eviction start: drop to 95%
    usage_data.append(95.0)

    # Post-eviction: fluctuate around 95-98%
    for i in range(eviction_start + 1, num_lines):
        # Random fluctuation with bias toward 96-97%
        base = 96.5
        variation = np.random.normal(0, 1.0)
        usage = np.clip(base + variation, 93.0, 98.5)
        usage_data.append(usage)

    return usage_data


def generate_latency_data(num_lines, eviction_start):
    """
    Generate latency data (ms) that increases after eviction starts.

    Args:
        num_lines: Total number of lines to generate
        eviction_start: Line number where eviction begins

    Returns:
        list: List of latency values in milliseconds
    """
    latency_data = []

    # Pre-eviction: lower latency (0.5-2 ms)
    for i in range(eviction_start):
        latency = np.random.uniform(0.5, 2.0)
        latency_data.append(latency)

    # Post-eviction: higher latency (2-5 ms)
    for i in range(eviction_start, num_lines):
        latency = np.random.uniform(2.0, 5.0)
        latency_data.append(latency)

    return latency_data


def generate_bytes_total_data(num_lines, eviction_start):
    """
    Generate cumulative bytes_total data.

    Args:
        num_lines: Total number of lines to generate
        eviction_start: Line number where eviction begins

    Returns:
        list: List of cumulative byte counts
    """
    bytes_data = []
    cumulative = 0

    # Pre-eviction: faster growth
    for i in range(eviction_start):
        increment = np.random.randint(1_000_000, 5_000_000)  # 1-5 MB per second
        cumulative += increment
        bytes_data.append(cumulative)

    # Post-eviction: slower growth (eviction overhead)
    for i in range(eviction_start, num_lines):
        increment = np.random.randint(500_000, 2_000_000)  # 0.5-2 MB per second
        cumulative += increment
        bytes_data.append(cumulative)

    return bytes_data


def write_json_file(filepath, metric_name, values, start_time):
    """
    Write a JSON lines file with timestamp and metric data.

    Args:
        filepath: Output file path
        metric_name: Name of the metric
        values: List of values to write
        start_time: Starting timestamp
    """
    with open(filepath, 'w') as f:
        for i, value in enumerate(values):
            timestamp = start_time + timedelta(seconds=i)
            data = {
                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "fields": {
                    "name": metric_name,
                    "value": value
                }
            }
            f.write(json.dumps(data) + '\n')


def create_run_directory(base_path, chunk_config, distribution, ratio, eviction_type, device_name):
    """
    Create a single run directory with test JSON files.

    Args:
        base_path: Base output path
        chunk_config: Chunk configuration dict
        distribution: Distribution type
        ratio: Ratio value
        eviction_type: Eviction type
        device_name: Device name (e.g., nvme0n2)
    """
    # Build directory name matching real data format
    dirname = (f"{chunk_config['size']},L={chunk_config['L']},{distribution},"
               f"R={ratio},I={chunk_config['I']},NZ=904,{eviction_type},"
               f"{device_name}-test-run")

    run_path = base_path / dirname
    run_path.mkdir(parents=True, exist_ok=True)

    # Generate data
    start_time = datetime(2025, 12, 24, 10, 0, 0)
    usage_data = generate_usage_percentage_data(NUM_LINES, EVICTION_START_LINE)
    latency_data = generate_latency_data(NUM_LINES, EVICTION_START_LINE)
    bytes_data = generate_bytes_total_data(NUM_LINES, EVICTION_START_LINE)

    # Write JSON files
    write_json_file(run_path / "usage_percentage.json", "usage_percentage", usage_data, start_time)
    write_json_file(run_path / "get_total_latency_ms.json", "get_total_latency_ms", latency_data, start_time)
    write_json_file(run_path / "bytes_total.json", "bytes_total", bytes_data, start_time)

    print(f"  Created: {dirname}")


def main():
    """Generate complete test data structure."""

    print("Generating test data structure...")
    print(f"Lines per file: {NUM_LINES}")
    print(f"Eviction starts at line: {EVICTION_START_LINE}")
    print()

    base_path = Path("data/logs/test-data-small")

    # Create both device directories
    for device_type, device_name in DEVICES.items():
        print(f"\nCreating {device_type} test data ({device_name})...")

        if device_type == "ZNS":
            device_base = base_path / "zns-test" / "split_output"
        else:
            device_base = base_path / "block-test" / "split_output"

        # Create all parameter combinations
        for chunk_config in CHUNK_CONFIGS:
            eviction_types = EVICTION_TYPES[chunk_config['size']]

            for distribution in DISTRIBUTIONS:
                for ratio in RATIOS:
                    for eviction_type in eviction_types:
                        create_run_directory(
                            device_base,
                            chunk_config,
                            distribution,
                            ratio,
                            eviction_type,
                            device_name
                        )

    print("\n✓ Test data structure created successfully!")
    print(f"\nTest data location: {base_path}")
    print("\nYou can now test with:")
    print("  python distribution_comparison_boxplots.py \\")
    print("    --zns-dir data/logs/test-data-small/zns-test \\")
    print("    --block-dir data/logs/test-data-small/block-test \\")
    print("    --output-dir test-output")
    print("\nOr with eviction filtering:")
    print("  python distribution_comparison_boxplots.py \\")
    print("    --zns-dir data/logs/test-data-small/zns-test \\")
    print("    --block-dir data/logs/test-data-small/block-test \\")
    print("    --output-dir test-output \\")
    print("    --from-eviction-start")


if __name__ == "__main__":
    main()
