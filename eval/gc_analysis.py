#!/usr/bin/env python3
# ============================================================================
# gc_analysis.py — Host-side WAF / RAF / eviction-onset analysis for OxCache
# ============================================================================
#
# PURPOSE
#   Computes per-run garbage-collection metrics from OxCache's per-event
#   metrics logs, without re-running any experiments:
#
#     * Host-side Write Amplification Factor (WAF)
#         WAF = physical chunk writes / cache admissions
#             = count(disk_write_latency_ms events) / count(written_bytes_total events)
#       Cross-checked against bytes:
#         WAF_bytes = count(disk_write) * chunk_size / final(written_bytes_total)
#       disk_write_latency_ms fires once per LOGICAL chunk write — the timer
#       wraps the entire chunked_append call, so a large chunk splitting into
#       many NVMe commands still emits exactly one event — including cleaning
#       relocations (oxcache/src/device.rs:570-577 Zoned, :1105-1111 Block).
#       written_bytes_total is a cumulative counter incremented once per
#       successful admission (oxcache/src/server.rs:735, sole call site).
#       Hence 1 admission -> 1 event, and every surplus write event is one
#       cleaning relocation. Empirical null test: zero-cleaning runs show
#       EXACT count equality (15,028 == 15,028) despite 256 MiB chunks
#       decomposing into ~100 NVMe commands each. Zone LRU on ZNS should give
#       WAF = 1.0 exactly.
#
#     * Device Read Amplification Factor (RAF), cleaning-driven
#         RAF     = count(disk_read_latency_ms events) / count(read_bytes_total events)
#         RAF_adj = count(disk_read_latency_ms events) /
#                   (count(read_bytes_total events) - count(buffer_hit events))
#       disk_read_latency_ms fires on every physical read including the
#       Chunk-LRU cleaning-phase reads; read_bytes_total increments once per
#       served cache hit (RAM-buffer hits included, hence RAF_adj, which counts
#       physical reads per DISK-served hit and equals exactly 1.0 for Zone LRU).
#
#     * Cleaning writes = physical writes - admissions (Zone LRU: 0)
#
#     * Eviction onset: first timestamp where usage_percentage crosses a
#       threshold (default 0.98, matching latency_table.py's
#       find_eviction_start_time convention; scale is 0.0-1.0), reported as
#       offset from the run's first event.
#
#     * Cleaning rate: cleaning writes / (last physical write - eviction onset),
#       plus a "cleaning start" estimate from the first sustained divergence of
#       the physical-write count from the admission count.
#
# INPUT
#   One or more of:
#     * split_output run directories, e.g.
#         .../PARAM/ZNS-consolidated/split_output/268435456,...,chunk,nvme0n2-..-run/
#       (uses the per-metric files: disk_write_latency_ms.json,
#        written_bytes_total.json, disk_read_latency_ms.json,
#        read_bytes_total.json, usage_percentage.json, buffer_hit.json, ...)
#     * raw combined metrics files (metrics-*.json), streamed in one pass.
#   Chunk size is parsed from the first comma-separated field of the run name
#   (bytes); override with --chunk-size if the name is not in that format.
#
#   Log line schema (both input kinds):
#     {"timestamp":"2026-04-02T21:49:34.242646Z","fields":{"name":"<metric>","value":<num>}}
#   Timestamps are ISO-8601 UTC and compare correctly as strings.
#
# USAGE
#   # One run directory:
#   ./gc_analysis.py data/logs/FINAL/PARAM/ZNS-consolidated/split_output/<run>
#
#   # All Chunk-LRU runs of a device (shell glob), CSV output:
#   ./gc_analysis.py --format csv \
#       data/logs/FINAL/PARAM/ZNS-consolidated/split_output/*chunk*-run
#
#   # Raw combined log (e.g. the local spctest run):
#   ./gc_analysis.py logs/spctest/logs/metrics-2025-12-10-20-32-20.json --chunk-size 268435456
#
#   # Skip the cleaning-rate pass (fastest; counts and finals only):
#   ./gc_analysis.py --skip-cleaning-rate <run> ...
#
# VALIDATION (2026-07-14; full sweep output in eval/gc_analysis_results-2026-07-14.csv)
#   * Zone-LRU/ZNS SPC run (logs/spctest): 8163 physical writes == 8163
#     admissions -> WAF = 1.000; 2,130,504 physical reads == 2,130,504 read
#     events -> RAF = 1.000, matching the paper's zero-host-GC claim.
#   * Chunk-LRU/ZNS 256 MiB grid (FINAL/PARAM, all post-25a45b0, all passed the
#     count-vs-bytes cross-check, zero event loss):
#       uniform 1:10 -> 1.136   Zipfian 1:10 -> 1.433 (REDO-vintage cell)
#       Zipfian 1:2  -> 1.700   uniform 1:2  -> 1.780
#     These four values are cited in the paper's Limitations subsection.
#   * Chunk-LRU/block 256 MiB grid: WAF = 1.000 exactly in all four cells
#     (zero cleaning writes), as expected for device-side GC.
#   * 64 KiB and WiredTiger cells: event loss detected (see CAVEATS) — their
#     count-based WAF/RAF is NOT publication-grade.
#
# CAVEATS
#   * read_bytes_total increments on BOTH disk hits and RAM-buffer hits
#     (server.rs), so RAF is slightly deflated when buffer hits occur; the
#     buffer_hit count is reported alongside for context.
#   * IMPORTANT — pre-fix logs: data recorded before OxCache commit 25a45b0
#     ("Fix incorrect metric recording", 2025-12-08) has written_bytes_total and
#     read_bytes_total SWAPPED (incremented on hit/miss respectively), making
#     WAF/RAF from such logs meaningless (e.g. the eval/data/oddsz set yields a
#     fictitious WAF of ~10.5). Post-fix logs satisfy the invariants
#     count(written_bytes_total) == count(miss) and
#     count(read_bytes_total) == count(hit); the script checks both and prints a
#     loud warning when they fail — do NOT trust WAF/RAF for a run that warns.
#   * The disk_write event of an admission is logged at the device layer while
#     its written_bytes_total increment is logged at the server layer, so the
#     two cumulative counts can transiently diverge by up to the number of
#     in-flight writes without any cleaning taking place. "Cleaning start"
#     therefore requires the divergence to exceed --divergence-margin; the
#     default (128) is 2x the largest writer pool in the published runs
#     (Zone-LRU configs use 64 writer threads, Chunk-LRU configs use 14, plus
#     one dedicated priority writer). It must exceed YOUR writer-pool size to
#     be meaningful.
#   * cleaning_writes = physical_writes - admissions also counts any FAILED
#     admission write (disk_write_latency_ms is emitted before the success
#     check; written_bytes_total only increments on success). Exact for
#     error-free runs.
#   * EVENT LOSS at high event rates (observed 2026-07-14 on the 64 KiB PARAM
#     cells and the WiredTiger runs, tens of millions of events): some log
#     lines are dropped, so event COUNTS undercount while cumulative counter
#     VALUES (written/read_bytes_total) remain exact. Symptoms: count-based vs
#     bytes-based WAF disagreement (warned), physical_writes < admissions /
#     negative cleaning_writes, admissions != misses. Count-derived WAF/RAF is
#     NOT publishable for such runs; the 256 MiB cells (~10-50k events) show
#     zero loss and pass all cross-checks. The admissions!=misses warning also
#     over-fires benignly on variable-request-size trace runs (WiredTiger),
#     where one client request can span multiple chunks.
#   * Latency values in *_latency_ms files are ignored here; only event counts
#     and timestamps are used.
#
# Related: SYSTOR'26 camera-ready tasks T1-T3 (vault: planning/2026-07-14-
# systor26-cortes-tasks.md); fills the %TODO(cortes T1) in evaluation.tex 4.6.
# ============================================================================

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

METRICS_NEEDED = [
    "disk_write_latency_ms", "written_bytes_total",
    "disk_read_latency_ms", "read_bytes_total",
    "usage_percentage", "buffer_hit", "disk_hit", "hit", "miss",
]

TS_PREFIX_LEN = len('{"timestamp":"')


def parse_ts(ts: str) -> float:
    """ISO-8601 'Z' timestamp -> unix epoch seconds."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def line_ts(line: str) -> str:
    """Extract the timestamp substring without full JSON parsing."""
    end = line.index('"', TS_PREFIX_LEN)
    return line[TS_PREFIX_LEN:end]


def line_value(line: str):
    return json.loads(line)["fields"]["value"]


def count_lines(path: str) -> int:
    n = 0
    with open(path, "rb") as f:
        while chunk := f.read(1 << 22):
            n += chunk.count(b"\n")
        f.seek(0, os.SEEK_END)
        if f.tell() > 0:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                n += 1  # unterminated final line
    return n


def first_line(path: str) -> str:
    with open(path) as f:
        return f.readline().strip()


def last_line(path: str) -> str:
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        back = min(size, 1 << 16)
        f.seek(size - back)
        tail = f.read().splitlines()
        for raw in reversed(tail):
            if raw.strip():
                return raw.decode()
    return ""


def chunk_size_from_name(name: str):
    m = re.match(r"^(\d+),", os.path.basename(name.rstrip("/")))
    return int(m.group(1)) if m else None


def usage_onset(path: str, threshold: float):
    """First timestamp where usage_percentage >= threshold."""
    if not os.path.exists(path):
        return None, None
    with open(path) as f:
        for line in f:
            if not line.strip():
                continue
            v = line_value(line)
            if v >= threshold:
                return line_ts(line), v
    return None, None


def cleaning_start(disk_write_path: str, written_path: str, margin: int):
    """First timestamp where cumulative physical writes exceed cumulative
    admissions by more than `margin` (two-pointer merge on sorted streams)."""
    with open(disk_write_path) as fw, open(written_path) as fa:
        w = fw.readline()
        a = fa.readline()
        cum_w = cum_a = 0
        while w or a:
            tw = line_ts(w) if w else "￿"
            ta = line_ts(a) if a else "￿"
            if tw <= ta:
                cum_w += 1
                if cum_w - cum_a > margin:
                    return tw
                w = fw.readline()
            else:
                cum_a += 1
                a = fa.readline()
    return None


def analyze_split_run(run_dir: str, args):
    p = lambda m: os.path.join(run_dir, f"{m}.json")
    r = {"run": os.path.basename(run_dir.rstrip("/"))}
    r["chunk_size"] = args.chunk_size or chunk_size_from_name(run_dir)

    counts = {}
    for m in METRICS_NEEDED:
        counts[m] = count_lines(p(m)) if os.path.exists(p(m)) else None
    r["physical_writes"] = counts["disk_write_latency_ms"]
    r["admissions"] = counts["written_bytes_total"]
    r["physical_reads"] = counts["disk_read_latency_ms"]
    r["read_events"] = counts["read_bytes_total"]
    r["buffer_hits"] = counts["buffer_hit"]
    r["misses"] = counts["miss"]
    r["hits"] = counts["hit"]

    r["admitted_bytes"] = (
        line_value(last_line(p("written_bytes_total"))) if r["admissions"] else None
    )
    r["read_bytes"] = (
        line_value(last_line(p("read_bytes_total"))) if r["read_events"] else None
    )

    finish(r)

    if not args.skip_cleaning_rate and r["physical_writes"]:
        ts_first = line_ts(first_line(p("disk_write_latency_ms")))
        ts_last = line_ts(last_line(p("disk_write_latency_ms")))
        onset_ts, onset_val = usage_onset(p("usage_percentage"), args.threshold)
        r["onset_usage"] = onset_val
        if onset_ts:
            run_start = line_ts(first_line(p("usage_percentage")))
            r["onset_offset_s"] = parse_ts(onset_ts) - parse_ts(run_start)
            window = parse_ts(ts_last) - parse_ts(onset_ts)
            if r.get("cleaning_writes") and window > 0:
                r["cleaning_ops_per_s"] = r["cleaning_writes"] / window
        if r.get("cleaning_writes"):
            cs = cleaning_start(
                p("disk_write_latency_ms"), p("written_bytes_total"), args.divergence_margin
            )
            if cs:
                r["cleaning_start_offset_s"] = parse_ts(cs) - parse_ts(ts_first)
    return r


def analyze_raw_file(path: str, args):
    """Single streaming pass over a combined metrics-*.json file."""
    r = {"run": os.path.basename(path)}
    r["chunk_size"] = args.chunk_size or chunk_size_from_name(path)
    counts = {m: 0 for m in METRICS_NEEDED}
    finals = {}
    cum_w = cum_a = 0
    clean_start_ts = onset_ts = None
    onset_val = first_write_ts = last_write_ts = first_usage_ts = None

    with open(path) as f:
        for line in f:
            if len(line) < TS_PREFIX_LEN + 2:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = obj["fields"]["name"]
            if name not in counts:
                continue
            counts[name] += 1
            ts = obj["timestamp"]
            if name in ("written_bytes_total", "read_bytes_total"):
                finals[name] = obj["fields"]["value"]
            if name == "disk_write_latency_ms":
                cum_w += 1
                first_write_ts = first_write_ts or ts
                last_write_ts = ts
                if clean_start_ts is None and cum_w - cum_a > args.divergence_margin:
                    clean_start_ts = ts
            elif name == "written_bytes_total":
                cum_a += 1
            elif name == "usage_percentage":
                first_usage_ts = first_usage_ts or ts
                if onset_ts is None and obj["fields"]["value"] >= args.threshold:
                    onset_ts, onset_val = ts, obj["fields"]["value"]

    r["physical_writes"] = counts["disk_write_latency_ms"] or None
    r["admissions"] = counts["written_bytes_total"] or None
    r["physical_reads"] = counts["disk_read_latency_ms"] or None
    r["read_events"] = counts["read_bytes_total"] or None
    r["buffer_hits"] = counts["buffer_hit"] or 0
    r["misses"] = counts["miss"] or None
    r["hits"] = counts["hit"] or None
    r["admitted_bytes"] = finals.get("written_bytes_total")
    r["read_bytes"] = finals.get("read_bytes_total")
    finish(r)
    r["onset_usage"] = onset_val
    if onset_ts and first_usage_ts:
        r["onset_offset_s"] = parse_ts(onset_ts) - parse_ts(first_usage_ts)
        if r.get("cleaning_writes") and last_write_ts:
            window = parse_ts(last_write_ts) - parse_ts(onset_ts)
            if window > 0:
                r["cleaning_ops_per_s"] = r["cleaning_writes"] / window
    if clean_start_ts and first_write_ts:
        # measured from the first physical write, matching analyze_split_run
        r["cleaning_start_offset_s"] = parse_ts(clean_start_ts) - parse_ts(first_write_ts)
    return r


def warn(r, msg):
    r["warning"] = f"{r['warning']}; {msg}" if r.get("warning") else msg


def finish(r):
    """Derive WAF/RAF/cleaning from the raw counts, with a bytes cross-check."""
    # Pre-25a45b0 detection: post-fix logs satisfy admissions==misses and
    # read_events==hits; the pre-fix metric swap breaks both (see README).
    adm_, miss_ = r.get("admissions"), r.get("misses")
    if adm_ and miss_ and abs(adm_ - miss_) > max(5, 0.001 * miss_):
        warn(r, f"admissions ({adm_}) != misses ({miss_}): log likely predates "
                "commit 25a45b0 metric-swap fix — WAF/RAF UNRELIABLE")
    re__, hit_ = r.get("read_events"), r.get("hits")
    if re__ and hit_ and abs(re__ - hit_) > max(5, 0.001 * hit_):
        warn(r, f"read_events ({re__}) != hits ({hit_}): log likely predates "
                "commit 25a45b0 metric-swap fix — WAF/RAF UNRELIABLE")
    pw, adm = r.get("physical_writes"), r.get("admissions")
    if pw and adm:
        r["waf"] = pw / adm
        r["cleaning_writes"] = pw - adm
        if r.get("chunk_size") and r.get("admitted_bytes"):
            waf_bytes = pw * r["chunk_size"] / r["admitted_bytes"]
            r["waf_bytes"] = waf_bytes
            if abs(waf_bytes - r["waf"]) / r["waf"] > 0.001:
                warn(r, f"count-based WAF {r['waf']:.4f} != bytes-based {waf_bytes:.4f}; "
                        "check chunk size / partial admissions")
    pr, re_ = r.get("physical_reads"), r.get("read_events")
    if pr and re_:
        r["raf"] = pr / re_
        disk_served = re_ - (r.get("buffer_hits") or 0)
        if disk_served > 0:
            r["raf_adj"] = pr / disk_served


COLUMNS = [
    ("run", "run"), ("waf", "WAF"), ("raf", "RAF"), ("raf_adj", "RAF_adj"),
    ("admissions", "admissions"), ("physical_writes", "phys_writes"),
    ("cleaning_writes", "clean_writes"), ("physical_reads", "phys_reads"),
    ("read_events", "read_events"), ("buffer_hits", "buf_hits"),
    ("onset_usage", "onset_usage"), ("onset_offset_s", "onset_s"),
    ("cleaning_start_offset_s", "clean_start_s"), ("cleaning_ops_per_s", "clean_ops/s"),
]


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.3f}"
    return str(v)


def emit(results, out_format):
    rows = [[fmt(r.get(k)) for k, _ in COLUMNS] for r in results]
    headers = [h for _, h in COLUMNS]
    if out_format == "csv":
        import csv
        w = csv.writer(sys.stdout)
        w.writerow(headers)
        w.writerows(rows)
    elif out_format == "markdown":
        print("| " + " | ".join(headers) + " |")
        print("|" + "|".join("---" for _ in headers) + "|")
        for row in rows:
            print("| " + " | ".join(row) + " |")
    else:
        widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h)
                  for i, h in enumerate(headers)]
        print("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
        for row in rows:
            print("  ".join(c.ljust(w) for c, w in zip(row, widths)))
    for r in results:
        if r.get("warning"):
            print(f"WARNING [{r['run']}]: {r['warning']}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(
        description="Host-side WAF/RAF/eviction-onset analysis from OxCache per-event logs "
                    "(see README header in this file)."
    )
    ap.add_argument("inputs", nargs="+",
                    help="split_output run directories and/or raw metrics-*.json files")
    ap.add_argument("--chunk-size", type=int, default=None,
                    help="chunk size in bytes (default: parsed from the run name)")
    ap.add_argument("--threshold", type=float, default=0.98,
                    help="usage_percentage eviction-onset threshold, 0.0-1.0 (default 0.98)")
    ap.add_argument("--divergence-margin", type=int, default=128,
                    help="write-count divergence needed to declare cleaning start; must exceed "
                         "the writer-pool size (default 128 = 2x the largest published pool: "
                         "Zone-LRU configs use 64 writers, Chunk-LRU configs 14, +1 priority writer)")
    ap.add_argument("--skip-cleaning-rate", action="store_true",
                    help="skip timestamp passes; report counts/WAF/RAF only")
    ap.add_argument("--format", choices=["table", "csv", "markdown"], default="table")
    args = ap.parse_args()

    results = []
    for inp in args.inputs:
        if os.path.isdir(inp):
            results.append(analyze_split_run(inp, args))
        elif os.path.isfile(inp):
            results.append(analyze_raw_file(inp, args))
        else:
            print(f"skipping {inp}: not found", file=sys.stderr)
    emit(results, args.format)


if __name__ == "__main__":
    main()
