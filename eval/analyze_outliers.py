#!/usr/bin/env python3
"""
Analyze outliers in latency data for OxCache runs.
Helps identify extreme values and decide whether they should be filtered.
"""

import argparse
from pathlib import Path
import numpy as np
import data_cache
import sys

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

DISTRIBUTIONS = ["ZIPFIAN", "UNIFORM"]
CHUNK_SIZES = [65536, 268435456, 1129316352]
CHUNK_SIZE_LABELS = {
    65536: "64KiB",
    268435456: "256MiB",
    1129316352: "1077MiB"
}
RATIOS = [2, 10]
EVICTION_TYPES = ["promotional", "chunk"]
EVICTION_TYPE_LABELS = {
    "promotional": "Zone-LRU",
    "chunk": "Chunk-LRU"
}
DEVICE_MAPPINGS = {
    "nvme0n2": "ZNS",
    "nvme1n1": "Block"
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def find_eviction_start_time(run_path):
    """
    Find timestamp when eviction starts.

    Returns the peak before first decrease, or first max if no decrease.
    """
    usage_file = run_path / "usage_percentage.json"
    if not usage_file.exists():
        return None

    try:
        timestamps, usage_values = data_cache.load_metric_data(
            usage_file,
            filter_minutes=None,
            use_cache=True
        )
    except Exception as e:
        return None

    if len(usage_values) == 0:
        return None

    import numpy as np

    # Look for the first decrease in usage
    for i in range(len(usage_values) - 1):
        if usage_values[i] > usage_values[i + 1]:
            return timestamps[i]

    # No decrease found, find where we first reach maximum
    max_val = usage_values.max()
    first_max_idx = np.where(usage_values >= max_val)[0]
    if len(first_max_idx) > 0:
        return timestamps[first_max_idx[0]]

    return None


def parse_directory_name(dirname):
    """Parse directory name to extract parameters."""
    parts = dirname.split(",")
    if len(parts) < 8:
        return None

    try:
        chunk_size = int(parts[0])
        distribution = parts[2]
        ratio = int(parts[3].split("=")[1])
        eviction_type = parts[6]
        device_part = parts[7].split("-")[0]

        device = None
        for dev_name, dev_type in DEVICE_MAPPINGS.items():
            if device_part.startswith(dev_name):
                device = dev_type
                break

        if device is None:
            return None

        return {
            "chunk_size": chunk_size,
            "distribution": distribution,
            "ratio": ratio,
            "eviction_type": eviction_type,
            "device": device,
            "dirname": dirname
        }
    except (ValueError, IndexError):
        return None


def collect_runs(split_output_dir):
    """Collect all runs from a split_output directory."""
    runs = []
    split_path = Path(split_output_dir)

    if not split_path.exists():
        return runs

    for entry in split_path.iterdir():
        if entry.is_dir():
            params = parse_directory_name(entry.name)
            if params:
                params["path"] = entry
                runs.append(params)

    return runs


# ============================================================================
# OUTLIER DETECTION METHODS
# ============================================================================

def detect_outliers_iqr(data, multiplier=1.5):
    """
    Detect outliers using IQR method (same as box plots).

    Args:
        data: NumPy array of values
        multiplier: IQR multiplier (default 1.5, use 3.0 for extreme outliers)

    Returns:
        dict with outlier information
    """
    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 75)
    iqr = q3 - q1

    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    outlier_mask = (data < lower_bound) | (data > upper_bound)
    outliers = data[outlier_mask]

    return {
        'method': f'IQR (×{multiplier})',
        'lower_bound': lower_bound,
        'upper_bound': upper_bound,
        'outlier_count': len(outliers),
        'outlier_percentage': 100 * len(outliers) / len(data),
        'outliers': outliers,
        'outlier_indices': np.where(outlier_mask)[0]
    }


def detect_outliers_zscore(data, threshold=3.0):
    """
    Detect outliers using z-score method.

    Args:
        data: NumPy array of values
        threshold: Z-score threshold (default 3.0 = 99.7% of data)

    Returns:
        dict with outlier information
    """
    mean = np.mean(data)
    std = np.std(data)

    if std == 0:
        return {
            'method': f'Z-score (>{threshold})',
            'outlier_count': 0,
            'outlier_percentage': 0.0,
            'outliers': np.array([]),
            'outlier_indices': np.array([])
        }

    z_scores = np.abs((data - mean) / std)
    outlier_mask = z_scores > threshold
    outliers = data[outlier_mask]

    return {
        'method': f'Z-score (>{threshold})',
        'mean': mean,
        'std': std,
        'outlier_count': len(outliers),
        'outlier_percentage': 100 * len(outliers) / len(data),
        'outliers': outliers,
        'outlier_indices': np.where(outlier_mask)[0]
    }


def detect_outliers_percentile(data, lower_pct=0.1, upper_pct=99.9):
    """
    Detect outliers using percentile thresholds.

    Args:
        data: NumPy array of values
        lower_pct: Lower percentile threshold (default 0.1)
        upper_pct: Upper percentile threshold (default 99.9)

    Returns:
        dict with outlier information
    """
    lower_bound = np.percentile(data, lower_pct)
    upper_bound = np.percentile(data, upper_pct)

    outlier_mask = (data < lower_bound) | (data > upper_bound)
    outliers = data[outlier_mask]

    return {
        'method': f'Percentile ({lower_pct}-{upper_pct})',
        'lower_bound': lower_bound,
        'upper_bound': upper_bound,
        'outlier_count': len(outliers),
        'outlier_percentage': 100 * len(outliers) / len(data),
        'outliers': outliers,
        'outlier_indices': np.where(outlier_mask)[0]
    }


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def analyze_configuration(run_info, from_eviction_start=False, filter_minutes=5, sample_size=None):
    """
    Analyze outliers for a single configuration.

    Returns:
        dict with analysis results or None if no data
    """
    metric_file = "get_total_latency_ms.json"
    data_file = run_info["path"] / metric_file

    if not data_file.exists():
        return None

    # Find eviction start time if filtering
    eviction_start_time = None
    if from_eviction_start:
        eviction_start_time = find_eviction_start_time(run_info["path"])

    # Load latency data
    ts, vals = data_cache.load_metric_data(
        data_file,
        filter_minutes=filter_minutes,
        use_cache=True,
        sample_size=sample_size
    )

    # Filter from eviction start if enabled
    if from_eviction_start and eviction_start_time is not None:
        mask = ts >= eviction_start_time
        ts = ts[mask]
        vals = vals[mask]

    if len(vals) == 0:
        return None

    # Detect outliers using different methods
    iqr_outliers = detect_outliers_iqr(vals, multiplier=1.5)
    iqr_extreme = detect_outliers_iqr(vals, multiplier=3.0)
    zscore_outliers = detect_outliers_zscore(vals, threshold=3.0)
    pct_outliers = detect_outliers_percentile(vals, lower_pct=0.1, upper_pct=99.9)

    # Compute basic statistics
    stats = {
        'count': len(vals),
        'min': np.min(vals),
        'max': np.max(vals),
        'mean': np.mean(vals),
        'median': np.median(vals),
        'std': np.std(vals),
        'p90': np.percentile(vals, 90),
        'p95': np.percentile(vals, 95),
        'p99': np.percentile(vals, 99),
        'p999': np.percentile(vals, 99.9)
    }

    return {
        'config': run_info,
        'stats': stats,
        'iqr_outliers': iqr_outliers,
        'iqr_extreme': iqr_extreme,
        'zscore_outliers': zscore_outliers,
        'pct_outliers': pct_outliers,
        'data': vals
    }


def print_configuration_analysis(analysis, show_values=False):
    """Print detailed analysis for a configuration."""
    config = analysis['config']
    stats = analysis['stats']

    print(f"\n{'='*80}")
    print(f"{config['distribution']} | {config['device']} | "
          f"{EVICTION_TYPE_LABELS[config['eviction_type']]} | "
          f"{CHUNK_SIZE_LABELS[config['chunk_size']]} | Ratio 1:{config['ratio']}")
    print(f"{'='*80}")

    print(f"\nBasic Statistics:")
    print(f"  Total data points: {stats['count']}")
    print(f"  Min: {stats['min']:.3f} ms")
    print(f"  Max: {stats['max']:.3f} ms")
    print(f"  Mean: {stats['mean']:.3f} ms")
    print(f"  Median: {stats['median']:.3f} ms")
    print(f"  Std: {stats['std']:.3f} ms")
    print(f"  P90: {stats['p90']:.3f} ms")
    print(f"  P99: {stats['p99']:.3f} ms")
    print(f"  P99.9: {stats['p999']:.3f} ms")

    print(f"\nOutlier Detection:")

    for method_key in ['iqr_outliers', 'iqr_extreme', 'zscore_outliers', 'pct_outliers']:
        result = analysis[method_key]
        print(f"\n  {result['method']}:")
        print(f"    Count: {result['outlier_count']} ({result['outlier_percentage']:.2f}%)")

        if result['outlier_count'] > 0:
            if 'lower_bound' in result and 'upper_bound' in result:
                print(f"    Bounds: [{result['lower_bound']:.3f}, {result['upper_bound']:.3f}] ms")

            outliers = result['outliers']
            print(f"    Outlier range: [{np.min(outliers):.3f}, {np.max(outliers):.3f}] ms")

            if show_values and len(outliers) <= 50:
                print(f"    Outlier values: {', '.join([f'{v:.3f}' for v in sorted(outliers)])}")
            elif show_values:
                print(f"    First 20 outliers: {', '.join([f'{v:.3f}' for v in sorted(outliers)[:20]])}...")
                print(f"    Last 20 outliers: {', '.join([f'{v:.3f}' for v in sorted(outliers)[-20:]])}")


def print_summary_table(results):
    """Print summary table of outlier counts."""
    print(f"\n{'='*120}")
    print("OUTLIER SUMMARY (IQR ×1.5 method - same as box plot whiskers)")
    print(f"{'='*120}")

    header = f"{'Distribution':<12} {'Device':<6} {'Eviction':<12} {'Chunk':<8} {'Ratio':<6} " \
             f"{'Total':>8} {'Outliers':>10} {'%':>8} {'Min Out':>10} {'Max Out':>10}"
    print(header)
    print("-" * 120)

    for analysis in results:
        config = analysis['config']
        stats = analysis['stats']
        outliers = analysis['iqr_outliers']

        if outliers['outlier_count'] > 0:
            min_out = np.min(outliers['outliers'])
            max_out = np.max(outliers['outliers'])
        else:
            min_out = 0
            max_out = 0

        ratio_str = f"1:{config['ratio']}"
        row = f"{config['distribution']:<12} {config['device']:<6} " \
              f"{EVICTION_TYPE_LABELS[config['eviction_type']]:<12} " \
              f"{CHUNK_SIZE_LABELS[config['chunk_size']]:<8} {ratio_str:<6} " \
              f"{stats['count']:>8} {outliers['outlier_count']:>10} " \
              f"{outliers['outlier_percentage']:>7.2f}% " \
              f"{min_out:>9.2f}ms {max_out:>9.2f}ms"
        print(row)


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Analyze outliers in OxCache latency data."
    )
    parser.add_argument(
        '--block-dir',
        required=True,
        help='Path to block device directory'
    )
    parser.add_argument(
        '--zns-dir',
        required=True,
        help='Path to ZNS device directory'
    )
    parser.add_argument(
        '--distribution',
        choices=['ZIPFIAN', 'UNIFORM', 'all'],
        default='all',
        help='Which distribution to analyze (default: all)'
    )
    parser.add_argument(
        '--chunk-size',
        choices=['64KiB', '256MiB', '1077MiB', 'all'],
        default='all',
        help='Which chunk size to analyze (default: all)'
    )
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed analysis for each configuration'
    )
    parser.add_argument(
        '--show-values',
        action='store_true',
        help='Show actual outlier values (use with --detailed)'
    )
    parser.add_argument(
        '--from-eviction-start',
        action='store_true',
        help='Only analyze data from when eviction begins'
    )
    parser.add_argument(
        '--filter-minutes',
        type=int,
        default=5,
        help='Exclude last N minutes of data (default: 5, use 0 for no filtering)'
    )
    parser.add_argument(
        '--sample',
        type=int,
        default=None,
        help='Sample every Nth line for faster testing'
    )

    args = parser.parse_args()

    # Construct split_output paths
    block_split = Path(args.block_dir) / "split_output"
    zns_split = Path(args.zns_dir) / "split_output"

    # Verify directories exist
    if not block_split.exists():
        print(f"Error: Block split_output directory not found: {block_split}", file=sys.stderr)
        return 1

    if not zns_split.exists():
        print(f"Error: ZNS split_output directory not found: {zns_split}", file=sys.stderr)
        return 1

    # Convert filter_minutes=0 to None
    filter_min = args.filter_minutes if args.filter_minutes > 0 else None

    # Collect runs
    block_runs = collect_runs(block_split)
    zns_runs = collect_runs(zns_split)

    print(f"Found {len(block_runs)} block runs and {len(zns_runs)} ZNS runs", file=sys.stderr)

    # Filter based on command-line arguments
    all_runs = block_runs + zns_runs

    if args.distribution != 'all':
        all_runs = [r for r in all_runs if r['distribution'] == args.distribution]

    if args.chunk_size != 'all':
        # Convert label back to size
        size_map = {v: k for k, v in CHUNK_SIZE_LABELS.items()}
        chunk_size = size_map[args.chunk_size]
        all_runs = [r for r in all_runs if r['chunk_size'] == chunk_size]

    print(f"\nAnalyzing {len(all_runs)} configurations...", file=sys.stderr)

    # Analyze each configuration
    results = []
    for run in all_runs:
        analysis = analyze_configuration(
            run,
            from_eviction_start=args.from_eviction_start,
            filter_minutes=filter_min,
            sample_size=args.sample
        )
        if analysis:
            results.append(analysis)

    if not results:
        print("\nNo data to analyze", file=sys.stderr)
        return 1

    # Print results
    if args.detailed:
        for analysis in results:
            print_configuration_analysis(analysis, show_values=args.show_values)
    else:
        print_summary_table(results)

    print(f"\n{'='*120}")
    print("RECOMMENDATION:")
    print(f"{'='*120}")
    print("• IQR ×1.5: Standard outlier definition (same as box plot whiskers)")
    print("  - Typically flags ~1-5% of data in normal distributions")
    print("  - Safe to remove if: >5% are outliers OR outliers are data collection errors")
    print("\n• IQR ×3.0: Extreme outliers only")
    print("  - More conservative, typically flags <1% of data")
    print("  - Consider removing only these if unsure")
    print("\n• For latency data: Outliers might be legitimate (GC pauses, context switches)")
    print("  - Consider keeping them if <2% and they represent real system behavior")
    print("  - Remove them if they're measurement artifacts or clearly erroneous")

    return 0


if __name__ == "__main__":
    exit(main())
