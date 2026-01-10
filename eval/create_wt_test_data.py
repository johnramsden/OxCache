#!/usr/bin/env python3
"""
Create test data for WT boxplot demonstration.
"""

import json
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

def create_test_run(base_dir, device, eviction_type, mean_throughput, std_throughput):
    """
    Create a test run with synthetic throughput and usage data.

    Args:
        base_dir: Base directory for test data
        device: Device name (nvme0n2 for ZNS, nvme1n1 for Block)
        eviction_type: "promotional" or "chunk"
        mean_throughput: Mean throughput in GiB/s
        std_throughput: Standard deviation in GiB/s
    """
    # Create directory name matching WT format
    dirname = f"65536,L=40632,NZ=904,{eviction_type},{device}-27_12:00:00-run"
    run_dir = base_dir / "split_output" / dirname
    run_dir.mkdir(parents=True, exist_ok=True)

    # Generate 300 data points (5 hours at 60-second bins)
    num_points = 300
    start_time = datetime(2024, 12, 27, 0, 0, 0)

    # Filling phase is first 60 points (1 hour), then eviction phase
    filling_points = 60

    # Generate cumulative bytes (throughput integrated over time)
    # Each bin represents 60 seconds of throughput
    bytes_per_gib = 2**30
    cumulative_bytes = 0

    # Create client_request_bytes_total.json
    output_file = run_dir / "client_request_bytes_total.json"
    with open(output_file, 'w') as f:
        for i in range(num_points):
            # During filling, throughput is higher; during eviction it stabilizes
            if i < filling_points:
                # Filling phase: higher throughput with upward trend
                throughput_gibs = mean_throughput * 1.5 + (i / filling_points) * 0.5
            else:
                # Eviction phase: stable throughput around mean
                throughput_gibs = np.random.normal(mean_throughput, std_throughput)
                throughput_gibs = max(0.1, throughput_gibs)  # Keep positive

            # Convert to bytes and accumulate
            bytes_in_interval = throughput_gibs * bytes_per_gib * 60  # 60 seconds per bin
            cumulative_bytes += bytes_in_interval

            # Create timestamp
            timestamp = start_time + timedelta(seconds=i*60)
            timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Write JSON line
            data = {
                "measurement": "client_request_bytes_total",
                "tags": {"run_id": dirname},
                "fields": {"value": cumulative_bytes},
                "timestamp": timestamp_str
            }
            f.write(json.dumps(data) + '\n')

    print(f"Created: {output_file}")

    # Create usage_percentage.json
    usage_file = run_dir / "usage_percentage.json"
    with open(usage_file, 'w') as f:
        for i in range(num_points):
            # Usage increases linearly during filling, plateaus at eviction
            if i < filling_points:
                usage = (i / filling_points) * 100.0  # 0 to 100%
            else:
                usage = 100.0  # Full

            # Create timestamp
            timestamp = start_time + timedelta(seconds=i*60)
            timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Write JSON line
            data = {
                "measurement": "usage_percentage",
                "tags": {"run_id": dirname},
                "fields": {"value": usage},
                "timestamp": timestamp_str
            }
            f.write(json.dumps(data) + '\n')

    print(f"Created: {usage_file}")

    # Create get_total_latency_ms.json
    latency_file = run_dir / "get_total_latency_ms.json"
    with open(latency_file, 'w') as f:
        for i in range(num_points * 100):  # More data points for latency
            # During filling, latency is lower; during eviction it's higher
            if i < filling_points * 100:
                # Filling phase: lower latency with less variance
                latency = np.random.lognormal(mean=np.log(5), sigma=0.5)
            else:
                # Eviction phase: higher latency with more variance
                latency = np.random.lognormal(mean=np.log(mean_throughput * 10), sigma=0.8)

            # Create timestamp (simulate higher frequency sampling)
            timestamp = start_time + timedelta(milliseconds=i*600)
            timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            # Write JSON line
            data = {
                "measurement": "get_total_latency_ms",
                "tags": {"run_id": dirname},
                "fields": {"value": latency},
                "timestamp": timestamp_str
            }
            f.write(json.dumps(data) + '\n')

    print(f"Created: {latency_file}")

def main():
    """Create test data for all 4 combinations."""
    base_dir = Path("test_data_wt")

    # Create directories
    ssd_dir = base_dir / "WT-SPC-SSD-consolidated"
    zns_dir = base_dir / "WT-SPC-ZNS-consolidated"

    # Test data with different characteristics to show visual differences
    # ZNS Zone-LRU: Higher throughput, lower variance
    create_test_run(zns_dir, "nvme0n2", "promotional", mean_throughput=2.8, std_throughput=0.3)

    # ZNS Chunk-LRU: Moderate throughput, moderate variance
    create_test_run(zns_dir, "nvme0n2", "chunk", mean_throughput=2.4, std_throughput=0.4)

    # Block Zone-LRU: Moderate throughput, higher variance
    create_test_run(ssd_dir, "nvme1n1", "promotional", mean_throughput=2.5, std_throughput=0.5)

    # Block Chunk-LRU: Lower throughput, moderate variance
    create_test_run(ssd_dir, "nvme1n1", "chunk", mean_throughput=2.0, std_throughput=0.4)

    print("\nTest data created successfully!")
    print(f"Run the boxplot script with:")
    print(f"  ./boxplot_wt.py \\")
    print(f"    --block-dir test_data_wt/WT-SPC-SSD-consolidated \\")
    print(f"    --zns-dir test_data_wt/WT-SPC-ZNS-consolidated \\")
    print(f"    --output-dir test_output")

if __name__ == "__main__":
    main()
