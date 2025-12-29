#!/usr/bin/env python3

import boto3
import time
import statistics
import math

US_PER_SEC = 1_000_000

BUCKET_NAME = 'oxcache'
KEY_NAME = 'random_data_2GiB.bin'
TRANSFER_SIZES = [
    64 * 1024,  # 64 KiB in bytes
    8 * 1024 * 1024,  # 8 MiB in bytes
    256 * 1024 * 1024,  # 256 MiB in bytes
    512 * 1024 * 1024,  # 512 MiB in bytes
    1077 * 1024 * 1024  # 1077 MiB in bytes
]
REPEAT = 100  # Number of times to repeat the transfer for each size

# Create an S3 client.
s3 = boto3.client('s3')

def geometric_mean(nums):
    """Calculate the geometric mean of a list of numbers."""
    # Using logarithms to avoid floating point overflow
    return math.exp(sum(math.log(x) for x in nums) / len(nums))


def measure_latency(chunk_size, repeat=REPEAT):
    """Measure the latency of transferring a chunk of data from S3 repeatedly.

    Args:
        chunk_size (int): The number of bytes to download.
        repeat (int): How many times to perform the download.

    Returns:
        list: A list of elapsed times (in seconds) for each transfer.
    """
    times = []
    print(f"Chunk size: {chunk_size}")
    print("| Iteration | Time (s) |")
    print("|-----------|----------|")
    for i in range(repeat):
        # Define the Range header (download bytes 0 to chunk_size-1)
        byte_range = f'bytes=0-{chunk_size - 1}'
        start = time.perf_counter()
        response = s3.get_object(Bucket=BUCKET_NAME, Key=KEY_NAME, Range=byte_range)
        # Read the data to ensure the transfer completes
        _ = response['Body'].read()
        end = time.perf_counter()
        elapsed = end - start
        times.append(elapsed)
        print(f"| {i + 1} |  {elapsed:.4f} |")
    return times


def main():
    for chunk_size in TRANSFER_SIZES:
        print(f"\nBenchmarking transfer size: {chunk_size} bytes")
        times = measure_latency(chunk_size)

        # Compute statistics
        mean_time = statistics.mean(times)
        geo_mean_time = geometric_mean(times)
        min_time = min(times)
        max_time = max(times)
        stdev_time = statistics.stdev(times) if len(times) > 1 else 0

        # Display the results
        print(f"\nResults for chunk size {chunk_size} bytes:")
        print("| Metric                | Seconds    | Microseconds |")
        print("|-----------------------|------------|--------------|")
        print(f"| Mean latency          | {mean_time:.4f}   | {int(mean_time * US_PER_SEC)}       |")
        print(f"| Geometric mean latency| {geo_mean_time:.4f}   | {int(geo_mean_time * US_PER_SEC)}       |")
        print(f"| Minimum latency       | {min_time:.4f}   | {int(min_time * US_PER_SEC)}       |")
        print(f"| Maximum latency       | {max_time:.4f}   | {int(max_time * US_PER_SEC)}       |")
        print(f"| Standard deviation    | {stdev_time:.4f}   | {int(stdev_time * US_PER_SEC)}       |")


if __name__ == '__main__':
    main()
