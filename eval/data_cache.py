#!/usr/bin/env python3
"""
Shared data caching module for OxCache evaluation scripts.

Provides efficient JSON→NumPy conversion with disk caching to avoid
re-parsing large (2GB+) JSON files across multiple plotting scripts.

Key features:
- Stream-based JSON loading (low memory footprint)
- Automatic .npz caching with cache invalidation
- NumPy arrays for fast numerical operations
- Vectorized throughput calculations
"""

import json
import numpy as np
from pathlib import Path
from datetime import datetime
import os


def parse_timestamp_to_unix(iso_string):
    """
    Parse ISO format timestamp to Unix epoch (seconds since 1970).

    Args:
        iso_string: ISO format timestamp string (e.g., "2024-01-01T12:00:00Z")

    Returns:
        float: Unix timestamp in seconds, or None if parsing fails
    """
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return dt.timestamp()
    except:
        return None


def get_cache_path(json_path):
    """
    Get the cache file path for a given JSON file.

    Args:
        json_path: Path to JSON file (str or Path)

    Returns:
        Path: Path to corresponding .npz cache file
    """
    json_path = Path(json_path)
    cache_path = json_path.with_suffix('.npz')
    return cache_path


def is_cache_valid(json_path, cache_path):
    """
    Check if cache file is valid (exists and newer than JSON source).

    Args:
        json_path: Path to source JSON file
        cache_path: Path to cache .npz file

    Returns:
        bool: True if cache is valid and can be used
    """
    json_path = Path(json_path)
    cache_path = Path(cache_path)

    if not cache_path.exists():
        return False

    if not json_path.exists():
        return False

    # Check if cache is newer than source JSON
    json_mtime = json_path.stat().st_mtime
    cache_mtime = cache_path.stat().st_mtime

    return cache_mtime >= json_mtime


def load_metric_data(filepath, filter_minutes=None, use_cache=True, sample_size=None):
    """
    Load metric data from JSON file with optional caching.

    This function:
    1. Checks for valid .npz cache and loads if available
    2. Otherwise, streams JSON line-by-line (low memory)
    3. Parses timestamps to Unix epoch floats
    4. Optionally filters last N minutes
    5. Saves .npz cache for future runs
    6. Returns NumPy arrays for efficient processing

    Args:
        filepath: Path to JSON Lines file (str or Path)
        filter_minutes: If provided, exclude last N minutes of data
        use_cache: If True, use/create .npz cache files
        sample_size: If provided, sample every Nth line for testing

    Returns:
        tuple: (timestamps, values) as NumPy float64 arrays
               timestamps are Unix epoch (seconds since 1970)
    """
    filepath = Path(filepath)

    # Try to load from cache
    if use_cache:
        cache_path = get_cache_path(filepath)
        if is_cache_valid(filepath, cache_path):
            try:
                print(f"  ✓ Loading from cache: {cache_path.name}")
                cache_data = np.load(cache_path)
                timestamps = cache_data['timestamps']
                values = cache_data['values']

                # Apply filtering if requested (cache stores unfiltered data)
                if filter_minutes is not None:
                    max_time = timestamps.max() if len(timestamps) > 0 else 0
                    cutoff_time = max_time - (filter_minutes * 60)
                    mask = timestamps <= cutoff_time
                    timestamps = timestamps[mask]
                    values = values[mask]

                print(f"  ✓ Loaded {len(values)} data points from cache")
                return timestamps, values
            except Exception as e:
                # If cache loading fails, fall back to JSON parsing
                print(f"Warning: Failed to load cache {cache_path}: {e}")
                print("Falling back to JSON parsing...")

    # Load from JSON (streaming, low memory)
    print(f"  → Parsing JSON: {filepath.name}")
    timestamps_list = []
    values_list = []
    cutoff_unix = None

    with open(filepath, 'r') as f:
        # First pass: determine time range if filtering is needed
        if filter_minutes is not None:
            # Find the max timestamp efficiently
            max_unix = None
            for line_num, line in enumerate(f):
                if sample_size and line_num % sample_size != 0:
                    continue
                try:
                    data = json.loads(line.strip())
                    if "timestamp" in data:
                        unix_ts = parse_timestamp_to_unix(data["timestamp"])
                        if unix_ts is not None:
                            if max_unix is None or unix_ts > max_unix:
                                max_unix = unix_ts
                except json.JSONDecodeError:
                    continue

            if max_unix is not None:
                cutoff_unix = max_unix - (filter_minutes * 60)

            # Reset file pointer for main read
            f.seek(0)

        # Main pass: load data with filtering
        for line_num, line in enumerate(f):
            # Skip lines if sampling
            if sample_size and line_num % sample_size != 0:
                continue

            try:
                data = json.loads(line.strip())
                if "fields" in data and "value" in data["fields"]:
                    value = data["fields"]["value"]

                    if "timestamp" in data:
                        unix_ts = parse_timestamp_to_unix(data["timestamp"])
                        if unix_ts is None:
                            continue

                        # Apply filtering if requested (for cache, we store unfiltered)
                        # But if not caching, filter on-the-fly
                        if not use_cache and cutoff_unix is not None and unix_ts > cutoff_unix:
                            continue

                        timestamps_list.append(unix_ts)
                        values_list.append(value)
                    else:
                        # No timestamp available, can't filter
                        timestamps_list.append(0.0)
                        values_list.append(value)

            except json.JSONDecodeError:
                continue

    # Convert to NumPy arrays
    timestamps_array = np.array(timestamps_list, dtype=np.float64)
    values_array = np.array(values_list, dtype=np.float64)

    print(f"  ✓ Parsed {len(values_array)} data points from JSON")

    # Save to cache (unfiltered data for flexibility)
    if use_cache:
        try:
            cache_path = get_cache_path(filepath)
            json_mtime = filepath.stat().st_mtime
            print(f"  → Saving cache: {cache_path.name}")
            np.savez_compressed(
                cache_path,
                timestamps=timestamps_array,
                values=values_array,
                json_mtime=json_mtime
            )
            print(f"  ✓ Cache saved successfully")
        except Exception as e:
            print(f"Warning: Failed to save cache {cache_path}: {e}")

    # Apply filtering if needed and we're using cache
    if use_cache and filter_minutes is not None and len(timestamps_array) > 0:
        max_time = timestamps_array.max()
        cutoff_time = max_time - (filter_minutes * 60)
        mask = timestamps_array <= cutoff_time
        timestamps_array = timestamps_array[mask]
        values_array = values_array[mask]

    return timestamps_array, values_array


def calculate_throughput_bins(timestamps, cumulative_bytes, bin_seconds=60):
    """
    Calculate throughput by binning cumulative bytes over time intervals.

    Uses vectorized NumPy operations for efficiency.

    Args:
        timestamps: NumPy array of Unix timestamps (seconds since epoch)
        cumulative_bytes: NumPy array of cumulative byte counts
        bin_seconds: Size of time bins in seconds (default: 60)

    Returns:
        NumPy array of throughput values (bytes per second) for each bin
    """
    if len(timestamps) == 0 or len(cumulative_bytes) == 0:
        return np.array([])

    # Calculate time span
    start_time = timestamps[0]
    end_time = timestamps[-1]
    duration = end_time - start_time

    if duration <= 0:
        return np.array([])

    # Create bin edges
    num_bins = int(np.ceil(duration / bin_seconds))
    bin_edges = np.arange(num_bins + 1) * bin_seconds + start_time

    # Assign each timestamp to a bin
    bin_indices = np.searchsorted(bin_edges[1:], timestamps, side='left')

    # Calculate throughput for each bin
    throughputs = []

    for bin_idx in range(num_bins):
        # Find all data points in this bin
        mask = (bin_indices == bin_idx)
        points_in_bin = np.where(mask)[0]

        if len(points_in_bin) == 0:
            continue  # Skip empty bins

        # Get first and last byte counts in this bin
        first_idx = points_in_bin[0]
        last_idx = points_in_bin[-1]

        bytes_in_bin = cumulative_bytes[last_idx] - cumulative_bytes[first_idx]
        throughput = bytes_in_bin / bin_seconds
        throughputs.append(throughput)

    return np.array(throughputs, dtype=np.float64)
