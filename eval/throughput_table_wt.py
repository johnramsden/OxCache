#!/usr/bin/env python3
"""
Generate LaTeX table comparing WiredTiger throughput:
  ZNS promotional eviction  vs  Block chunk eviction

Uses client_request_bytes_total, binned into 60-second windows,
starting from eviction start (98% usage threshold).
"""

import argparse
from pathlib import Path
import numpy as np
import data_cache

# ============================================================================
# CONFIGURATION
# ============================================================================

DEVICE_MAPPINGS = {
    "nvme0n2": "ZNS",
    "nvme1n1": "Block"
}

METRIC_FILE = "client_request_bytes_total.json"
BIN_SECONDS = 60
MiB = 2 ** 20


# ============================================================================
# DIRECTORY PARSING (same format as boxplot_wt.py / latency_table.py)
# ============================================================================

def parse_directory_name(dirname):
    """
    Format: chunk_size,L=...,NZ=...,eviction_type,device-timestamp
    Returns dict with chunk_size, eviction_type, device, dirname — or None.
    """
    parts = dirname.split(",")
    if len(parts) < 5:
        return None
    try:
        chunk_size = int(parts[0])
        eviction_type = parts[3]
        device_part = parts[4].split("-")[0]

        device = None
        for dev_name, dev_type in DEVICE_MAPPINGS.items():
            if device_part.startswith(dev_name):
                device = dev_type
                break

        if device is None:
            return None

        return {
            "chunk_size": chunk_size,
            "eviction_type": eviction_type,
            "device": device,
            "dirname": dirname,
        }
    except (ValueError, IndexError):
        return None


def collect_runs(split_output_dir):
    """Return list of run dicts from a split_output directory."""
    runs = []
    split_path = Path(split_output_dir)
    if not split_path.exists():
        print(f"Warning: directory does not exist: {split_output_dir}")
        return runs
    for entry in split_path.iterdir():
        if entry.is_dir():
            params = parse_directory_name(entry.name)
            if params:
                params["path"] = entry
                runs.append(params)
    return runs


def find_run(runs, device, eviction_type):
    for run in runs:
        if run["device"] == device and run["eviction_type"] == eviction_type:
            return run
    return None


# ============================================================================
# EVICTION START (identical to boxplot_wt.py)
# ============================================================================

def find_eviction_start_time(run_path, threshold=0.98):
    usage_file = run_path / "usage_percentage.json"
    if not usage_file.exists():
        return None
    try:
        ts, vals = data_cache.load_metric_data(usage_file, filter_minutes=None, use_cache=True)
    except Exception:
        return None
    if len(vals) == 0:
        return None

    above = np.where(vals >= threshold)[0]
    if len(above) > 0:
        return ts[above[0]]

    max_val = vals.max()
    first_max = np.where(vals >= max_val)[0]
    if len(first_max) > 0:
        return ts[first_max[0]]

    return None


# ============================================================================
# THROUGHPUT LOADING
# ============================================================================

def load_throughput(run, filter_minutes=None):
    """
    Load client_request_bytes_total, trim to eviction start, compute per-bin
    throughput in MiB/s.  Returns (throughput_array, eviction_start_ts).
    """
    data_file = run["path"] / METRIC_FILE
    if not data_file.exists():
        print(f"  Warning: {METRIC_FILE} not found in {run['path'].name}")
        return np.array([]), None

    eviction_start = find_eviction_start_time(run["path"])
    if eviction_start is None:
        print(f"  Warning: could not detect eviction start for {run['path'].name}, using all data")

    ts, vals = data_cache.load_metric_data(
        data_file,
        filter_minutes=filter_minutes,
        use_cache=True,
    )

    if eviction_start is not None and len(ts) > 0:
        mask = ts >= eviction_start
        ts = ts[mask]
        vals = vals[mask]
        print(f"  Filtered to {len(vals)} points from eviction start")

    throughput_bps = data_cache.calculate_throughput_bins(ts, vals, bin_seconds=BIN_SECONDS)
    throughput_mibs = throughput_bps / MiB
    return throughput_mibs, eviction_start


# ============================================================================
# STATISTICS
# ============================================================================

def stats(arr):
    if len(arr) == 0:
        return {k: None for k in ("mean", "median", "p99", "p95", "max", "min")}
    return {
        "mean":   float(np.mean(arr)),
        "median": float(np.median(arr)),
        "p99":    float(np.percentile(arr, 99)),
        "p95":    float(np.percentile(arr, 95)),
        "max":    float(np.max(arr)),
        "min":    float(np.min(arr)),
    }


def fmt(val, precision=2):
    return "N/A" if val is None else f"{val:.{precision}f}"


def pct_change(baseline, other, precision=1):
    """Percentage change from baseline to other."""
    if baseline is None or other is None or baseline == 0:
        return "N/A"
    change = (other - baseline) / baseline * 100
    sign = "+" if change >= 0 else ""
    return f"{sign}{change:.{precision}f}\\%"


# ============================================================================
# TABLE GENERATION
# ============================================================================

def generate_table(zns_stats, block_stats, output_file, filter_minutes):
    STAT_ROWS = [
        ("mean",   "Mean"),
        ("median", "Median"),
        ("p95",    "P95"),
        ("p99",    "P99"),
        ("max",    "Max"),
        ("min",    "Min"),
    ]

    filter_note = f", last {filter_minutes} min excluded" if filter_minutes else ""

    with open(output_file, "w") as f:
        f.write("\\begin{table}[htbp]\n")
        f.write("\\centering\n")
        f.write(
            f"\\caption{{WiredTiger client request throughput (MiB/s) from eviction start"
            f"{filter_note}. "
            f"ZNS uses Zone/promotional LRU; Block uses Chunk LRU. "
            f"\\%\\,Change is relative to ZNS baseline.}}\n"
        )
        f.write("\\label{tab:wt_throughput_comparison}\n")
        f.write("\\begin{tabular}{|l|r|r|r|}\n")
        f.write("\\hline\n")
        f.write("\\textbf{Statistic} & \\textbf{ZNS (Zone LRU)} & \\textbf{Block (Chunk LRU)} & \\textbf{\\%\\,Change} \\\\\n")
        f.write("\\hline\n")

        for stat_key, stat_label in STAT_ROWS:
            z = zns_stats[stat_key]
            b = block_stats[stat_key]
            f.write(f"{stat_label} & {fmt(z)} & {fmt(b)} & {pct_change(z, b)} \\\\\n")
            f.write("\\hline\n")

        f.write("\\end{tabular}\n")
        f.write("\\end{table}\n")

    print(f"Saved: {output_file}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="WiredTiger throughput table: ZNS promotional vs Block chunk, from eviction start."
    )
    parser.add_argument("--block-dir", required=True,
                        help="Path to Block WT consolidated directory")
    parser.add_argument("--zns-dir", required=True,
                        help="Path to ZNS WT consolidated directory")
    parser.add_argument("--output-file", default="wt_throughput_table.tex",
                        help="Output LaTeX file (default: wt_throughput_table.tex)")
    parser.add_argument("--filter-minutes", type=int, default=None,
                        help="Exclude last N minutes of data from each run")
    args = parser.parse_args()

    block_split = Path(args.block_dir) / "split_output"
    zns_split   = Path(args.zns_dir)   / "split_output"

    if not block_split.exists():
        print(f"Error: {block_split} does not exist")
        return 1
    if not zns_split.exists():
        print(f"Error: {zns_split} does not exist")
        return 1

    filter_min = args.filter_minutes if args.filter_minutes and args.filter_minutes > 0 else None

    block_runs = collect_runs(block_split)
    zns_runs   = collect_runs(zns_split)
    print(f"Found {len(block_runs)} Block run(s), {len(zns_runs)} ZNS run(s)")

    # Target runs
    zns_run   = find_run(zns_runs,   device="ZNS",   eviction_type="promotional")
    block_run = find_run(block_runs, device="Block",  eviction_type="chunk")

    if zns_run is None:
        print("Error: no ZNS promotional run found")
        return 1
    if block_run is None:
        print("Error: no Block chunk run found")
        return 1

    print(f"\nZNS run:   {zns_run['path'].name}")
    print(f"Block run: {block_run['path'].name}\n")

    print("Loading ZNS throughput...")
    zns_tp, zns_ev = load_throughput(zns_run, filter_minutes=filter_min)
    print(f"  {len(zns_tp)} bins, eviction start: {zns_ev}")

    print("Loading Block throughput...")
    block_tp, block_ev = load_throughput(block_run, filter_minutes=filter_min)
    print(f"  {len(block_tp)} bins, eviction start: {block_ev}")

    zns_s   = stats(zns_tp)
    block_s = stats(block_tp)

    print("\nZNS   throughput stats (MiB/s):", {k: fmt(v) for k, v in zns_s.items()})
    print("Block throughput stats (MiB/s):", {k: fmt(v) for k, v in block_s.items()})

    Path(args.output_file).parent.mkdir(parents=True, exist_ok=True)
    generate_table(zns_s, block_s, args.output_file, filter_min)

    return 0


if __name__ == "__main__":
    exit(main())
