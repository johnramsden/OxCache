#!/usr/bin/env python3
"""
Parallel cache generator for OxCache evaluation data.

Pre-generates .npz cache files for JSON metrics in parallel to dramatically
speed up subsequent plotting script runs.
"""

import argparse
import sys
from pathlib import Path
from multiprocessing import Pool, Manager, cpu_count
import time
import data_cache


# All metrics used by generate_all_plots.sh
ALL_METRICS = [
    "bytes_total",
    "written_bytes_total",
    "read_bytes_total",
    "device_write_latency_ms",
    "device_read_latency_ms",
    "disk_write_latency_ms",
    "disk_read_latency_ms",
    "get_miss_latency_ms",
    "get_hit_latency_ms",
    "get_total_latency_ms",
    "hitratio",
    "usage_percentage",
    "client_request_bytes_total",
]


def scan_for_json_files(base_dirs, metrics):
    """
    Scan base directories for JSON files matching specified metrics.

    Args:
        base_dirs: List of base directory paths
        metrics: List of metric names to look for

    Returns:
        List of Path objects for JSON files to process
    """
    json_files = []

    for base_dir in base_dirs:
        base_path = Path(base_dir)

        # Check if split_output exists
        split_output = base_path / "split_output"
        if split_output.exists():
            search_path = split_output
        else:
            search_path = base_path

        # Find all matching JSON files
        for metric in metrics:
            pattern = f"**/{metric}.json"
            found_files = list(search_path.glob(pattern))
            json_files.extend(found_files)

    return json_files


def process_file_worker(args):
    """
    Worker function to process a single JSON file.

    Args:
        args: Tuple of (file_path, file_index, total_files, counter_dict, recent_files)

    Returns:
        Tuple of (success, file_path, message)
    """
    file_path, file_index, total_files, counter_dict, recent_files = args

    # Get a short display name for the file
    # Show parent directory name + filename for context
    display_name = f"{file_path.parent.name}/{file_path.name}"

    try:
        # Check if cache already exists and is valid
        cache_path = data_cache.get_cache_path(file_path)
        if data_cache.is_cache_valid(file_path, cache_path):
            print(f"⊘ Skipping: {display_name} (cache exists)", flush=True)
            with counter_dict['lock']:
                counter_dict['skipped'] += 1
                recent_files.append(('SKIP', display_name, counter_dict['skipped']))
            return (True, file_path, "SKIPPED (cache exists)")

        # Update currently processing
        with counter_dict['lock']:
            recent_files.append(('PROC', display_name, counter_dict['processed']))

        # Print what we're processing (visible immediately)
        print(f"→ Processing: {display_name}", flush=True)

        # Load data (this will create the cache)
        # We don't need timestamps for caching, but load them anyway
        # to ensure cache has both timestamps and values
        timestamps, values = data_cache.load_metric_data(
            file_path,
            filter_minutes=None,  # Don't filter - cache unfiltered data
            use_cache=True,
            sample_size=None
        )

        with counter_dict['lock']:
            counter_dict['processed'] += 1
            recent_files.append(('DONE', display_name, len(values)))

        return (True, file_path, f"CACHED ({len(values)} points)")

    except Exception as e:
        with counter_dict['lock']:
            counter_dict['failed'] += 1
            recent_files.append(('FAIL', display_name, str(e)))
        return (False, file_path, f"FAILED: {str(e)}")


def print_progress(counter_dict, total_files, start_time, recent_files, last_update_time):
    """Print progress information with recent activity."""
    processed = counter_dict['processed']
    skipped = counter_dict['skipped']
    failed = counter_dict['failed']
    completed = processed + skipped + failed

    if completed == 0:
        return last_update_time

    current_time = time.time()

    # Only update every 0.5 seconds to avoid flickering
    if current_time - last_update_time < 0.5 and completed < total_files:
        return last_update_time

    elapsed = current_time - start_time
    rate = completed / elapsed if elapsed > 0 else 0
    eta = (total_files - completed) / rate if rate > 0 else 0

    percent = (completed / total_files) * 100

    # Clear previous lines (up to 8 lines)
    print('\033[2K', end='')  # Clear current line
    for _ in range(7):
        print('\033[1A\033[2K', end='')  # Move up and clear line

    # Print header
    print(f"╔════════════════════════════════════════════════════════════════════════════╗")
    print(f"║ Progress: {completed:4d}/{total_files:4d} ({percent:5.1f}%) │ "
          f"✓ {processed:3d} │ ⊘ {skipped:3d} │ ✗ {failed:3d} │ "
          f"{rate:4.1f} files/s │ ETA: {eta:4.0f}s ║")
    print(f"╠════════════════════════════════════════════════════════════════════════════╣")

    # Show recent activity (last 5 items)
    recent_items = list(recent_files)[-5:] if len(recent_files) > 0 else []

    for i in range(5):
        if i < len(recent_items):
            status, name, info = recent_items[i]
            if status == 'PROC':
                symbol = '⟳'
                color = '\033[93m'  # Yellow
                status_text = f"Processing..."
                line = f"║ {color}{symbol}\033[0m {name:<60s} {status_text:<10s} ║"
            elif status == 'DONE':
                symbol = '✓'
                color = '\033[92m'  # Green
                status_text = f"{info:,} pts"
                line = f"║ {color}{symbol}\033[0m {name:<60s} {status_text:<10s} ║"
            elif status == 'SKIP':
                symbol = '⊘'
                color = '\033[90m'  # Gray
                status_text = "cached"
                line = f"║ {color}{symbol}\033[0m {name:<60s} {status_text:<10s} ║"
            elif status == 'FAIL':
                symbol = '✗'
                color = '\033[91m'  # Red
                status_text = "FAILED"
                line = f"║ {color}{symbol}\033[0m {name:<60s} {status_text:<10s} ║"

            # Truncate if too long
            if len(name) > 60:
                truncated_name = name[:57] + '...'
                line = f"║ {color}{symbol}\033[0m {truncated_name:<60s} {status_text:<10s} ║"

            print(line)
        else:
            print(f"║{' ' * 76}║")

    print(f"╚════════════════════════════════════════════════════════════════════════════╝")

    return current_time


def main():
    parser = argparse.ArgumentParser(
        description="Pre-generate cache files for OxCache evaluation data in parallel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate caches for all metrics used by generate_all_plots.sh
  %(prog)s --dirs data/ZONED data/BLOCK --metrics all --workers 8

  # Generate caches for specific metrics only
  %(prog)s --dirs data/ZONED --metrics bytes_total get_total_latency_ms --workers 4

  # Use all available CPU cores
  %(prog)s --dirs data/ZONED data/BLOCK --metrics all

Available metrics:
  """ + ", ".join(ALL_METRICS)
    )

    parser.add_argument(
        '--dirs',
        nargs='+',
        required=True,
        help='Base directories to scan for data (e.g., data/ZONED data/BLOCK)'
    )

    parser.add_argument(
        '--metrics',
        nargs='+',
        default=['all'],
        help='Metrics to cache (use "all" for all metrics, or specify individual ones)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help=f'Number of parallel workers (default: CPU count = {cpu_count()})'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force regeneration of caches even if they exist'
    )

    args = parser.parse_args()

    # Determine metrics to process
    if 'all' in args.metrics:
        metrics = ALL_METRICS
    else:
        # Validate metrics
        invalid = [m for m in args.metrics if m not in ALL_METRICS]
        if invalid:
            print(f"Error: Unknown metrics: {', '.join(invalid)}")
            print(f"Valid metrics: {', '.join(ALL_METRICS)}")
            return 1
        metrics = args.metrics

    # Determine number of workers
    workers = args.workers if args.workers else cpu_count()

    print(f"=== OxCache Parallel Cache Generator ===")
    print(f"Base directories: {', '.join(args.dirs)}")
    print(f"Metrics: {', '.join(metrics)}")
    print(f"Workers: {workers}")
    print(f"Force regeneration: {args.force}")
    print()

    # Scan for JSON files
    print("Scanning for JSON files...")
    json_files = scan_for_json_files(args.dirs, metrics)

    if not json_files:
        print("No JSON files found matching the specified metrics.")
        return 1

    print(f"Found {len(json_files)} JSON files to process")
    print()

    # Set up progress tracking
    manager = Manager()
    counter_dict = manager.dict({
        'processed': 0,
        'skipped': 0,
        'failed': 0,
        'lock': manager.Lock()
    })
    recent_files = manager.list()

    # Prepare work items
    work_items = [
        (json_file, idx, len(json_files), counter_dict, recent_files)
        for idx, json_file in enumerate(json_files, 1)
    ]

    # Process files in parallel
    start_time = time.time()
    last_update_time = start_time
    print("Processing files in parallel...")
    print()
    # Print initial empty progress box
    for _ in range(8):
        print()

    with Pool(processes=workers) as pool:
        # Process files and show progress
        results = []
        for i, result in enumerate(pool.imap_unordered(process_file_worker, work_items)):
            results.append(result)

            # Print progress
            last_update_time = print_progress(counter_dict, len(json_files), start_time, recent_files, last_update_time)

    # Final progress update
    print_progress(counter_dict, len(json_files), start_time, recent_files, 0)
    print()  # New line after progress
    print()

    # Summary
    elapsed = time.time() - start_time
    print(f"=== Summary ===")
    print(f"Total files: {len(json_files)}")
    print(f"Newly cached: {counter_dict['processed']}")
    print(f"Skipped (cache exists): {counter_dict['skipped']}")
    print(f"Failed: {counter_dict['failed']}")
    print(f"Time elapsed: {elapsed:.1f}s")
    print(f"Average rate: {len(json_files)/elapsed:.1f} files/s")

    # Show failed files if any
    if counter_dict['failed'] > 0:
        print()
        print("Failed files:")
        for success, file_path, message in results:
            if not success:
                print(f"  - {file_path}: {message}")

    print()
    print("✓ Cache generation complete!")
    print()
    print("You can now run generate_all_plots.sh and it will load from cache.")

    return 0 if counter_dict['failed'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
