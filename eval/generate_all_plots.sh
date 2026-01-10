#!/bin/bash

# OxCache Evaluation Plot Generation Script
# Automates data splitting and plot generation for all metrics

set -e  # Exit on any error

# Display usage information
usage() {
    cat << EOF
Usage: $0 <zoned_dir> <block_dir>

Arguments:
  zoned_dir    Path to the zoned (ZNS) data directory
  block_dir    Path to the block interface data directory

Example:
  $0 data/ZONED-PROMO data/BLOCK-PROMO

EOF
    exit 1
}

# Check if required arguments are provided
if [ $# -ne 2 ]; then
    echo "Error: Missing required arguments"
    echo ""
    usage
fi

ZONED_DIR="$1"
BLOCK_DIR="$2"

# Validate that directories exist
if [ ! -d "$ZONED_DIR" ]; then
    echo "Error: Zoned directory does not exist: $ZONED_DIR"
    exit 1
fi

if [ ! -d "$BLOCK_DIR" ]; then
    echo "Error: Block directory does not exist: $BLOCK_DIR"
    exit 1
fi

echo "=== OxCache Evaluation Plot Generation ==="
echo ""
echo "Configuration:"
echo "  Zoned directory: $ZONED_DIR"
echo "  Block directory: $BLOCK_DIR"
echo ""

# Configuration
BUCKET_SECONDS=60
WINDOW_SECONDS=60
OUTPUT_DIR="plots"
ZONED_SPLIT_DIR="${ZONED_DIR}/split_output"
BLOCK_SPLIT_DIR="${BLOCK_DIR}/split_output"

# Labels for comparison plots
ZONED_LABEL="ZNS"
BLOCK_LABEL="Block-interface"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to check if directory exists and has data
check_split_data() {
    local dir="$1"
    local name="$2"
    
    if [ ! -d "$dir" ] || [ -z "$(ls -A "$dir" 2>/dev/null)" ]; then
        echo "❌ $name split data not found or empty"
        return 1
    else
        echo "✅ $name split data found"
        return 0
    fi
}

# Function to split data if needed
split_data_if_needed() {
    local source_dir="$1"
    local split_dir="$2"
    local name="$3"
    
    if ! check_split_data "$split_dir" "$name"; then
        echo "📊 Splitting $name data..."
        python3 split_data_fast.py "$source_dir"

        if check_split_data "$split_dir" "$name"; then
            echo "✅ $name data splitting completed"
        else
            echo "❌ Failed to split $name data"
            exit 1
        fi
    fi
}

# Step 1: Ensure data is split
echo "Step 1: Checking and splitting data if needed"
echo "============================================="

split_data_if_needed "$ZONED_DIR" "$ZONED_SPLIT_DIR" "ZONED-PROMO"
split_data_if_needed "$BLOCK_DIR" "$BLOCK_SPLIT_DIR" "BLOCK-PROMO"

echo ""

# Step 2: Generate individual plots for each dataset
echo "Step 2: Generating individual plots"
echo "===================================="

echo "📈 Generating BLOCK-PROMO individual plots..."

# Throughput plots
python plot_throughput.py "$BLOCK_SPLIT_DIR" \
   --bucket-seconds $BUCKET_SECONDS \
   --output-dir "${OUTPUT_DIR}/block_individual" \
   --metrics bytes_total written_bytes_total \
   --mark-device-fill
#
# Raw latency plots
python plot_latency.py "$BLOCK_SPLIT_DIR" \
   --output-dir "${OUTPUT_DIR}/block_individual" \
   --metrics get_total_latency_ms device_write_latency_ms disk_write_latency_ms get_miss_latency_ms

# Smoothed latency plots
python plot_latency_smoothed.py "$BLOCK_SPLIT_DIR" \
   --window-seconds $WINDOW_SECONDS \
   --output-dir "${OUTPUT_DIR}/block_individual" \
   --metrics get_total_latency_ms device_write_latency_ms disk_write_latency_ms get_miss_latency_ms \
   --mark-device-fill

# Hit ratio plots
python plot_hitratio.py "$BLOCK_SPLIT_DIR" \
   --output-dir "${OUTPUT_DIR}/block_individual"

echo "✅ BLOCK-PROMO individual plots completed"

echo "📈 Generating ZONED-PROMO individual plots..."

# Throughput plots
python plot_throughput.py "$ZONED_SPLIT_DIR" \
   --bucket-seconds $BUCKET_SECONDS \
   --output-dir "${OUTPUT_DIR}/zoned_individual" \
   --metrics bytes_total  written_bytes_total \
   --mark-device-fill

# Raw latency plots
python plot_latency.py "$ZONED_SPLIT_DIR" \
   --output-dir "${OUTPUT_DIR}/zoned_individual" \
   --metrics get_total_latency_ms device_write_latency_ms disk_write_latency_ms get_miss_latency_ms

# Smoothed latency plots
python plot_latency_smoothed.py "$ZONED_SPLIT_DIR" \
   --window-seconds $WINDOW_SECONDS \
   --output-dir "${OUTPUT_DIR}/zoned_individual" \
   --metrics get_total_latency_ms device_write_latency_ms disk_write_latency_ms get_miss_latency_ms \
   --mark-device-fill

# Hit ratio plots
python plot_hitratio.py "$ZONED_SPLIT_DIR" \
   --output-dir "${OUTPUT_DIR}/zoned_individual"

echo "✅ ZONED-PROMO individual plots completed"

echo ""

# Step 3: Generate comparison plots
echo "Step 3: Generating comparison plots"
echo "===================================="

echo "📊 Generating comparison plots (Block vs ZNS)..."

# Throughput comparison plots
python plot_throughput.py "$ZONED_SPLIT_DIR" "$BLOCK_SPLIT_DIR" \
   --labels "$ZONED_LABEL" "$BLOCK_LABEL" \
   --bucket-seconds $BUCKET_SECONDS \
   --output-dir "${OUTPUT_DIR}/comparison" \
   --metrics bytes_total written_bytes_total read_bytes_total \
   --mark-device-fill

# Smoothed latency comparison plots
python plot_latency_smoothed.py "$ZONED_SPLIT_DIR" "$BLOCK_SPLIT_DIR" \
   --labels "$ZONED_LABEL" "$BLOCK_LABEL" \
   --window-seconds $WINDOW_SECONDS \
   --output-dir "${OUTPUT_DIR}/comparison" \
   --metrics get_total_latency_ms device_write_latency_ms device_read_latency_ms disk_write_latency_ms disk_read_latency_ms get_miss_latency_ms get_hit_latency_ms \
   --mark-device-fill

# Hit ratio comparison plots
python plot_hitratio.py "$ZONED_SPLIT_DIR" "$BLOCK_SPLIT_DIR" \
   --labels "$ZONED_LABEL" "$BLOCK_LABEL" \
   --output-dir "${OUTPUT_DIR}/comparison"

# Distribution comparison boxplots
python distribution_comparison_boxplots.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/boxplot-fill" \
    --common-y-scale

python distribution_comparison_boxplots.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/boxplot-nofill" \
    --common-y-scale \
    --from-eviction-start

# Hit ratio horizontal bar charts
python hitratio_horizontal_bars_combined.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison"

# ecdfs lat - split by distribution to avoid memory issues

# Fill phase (whole run)
echo "📊 Generating ECDF plots for fill phase..."
python distribution_comparison_ecdfs.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/ecdfs-fill" \
    --log-scale \
    --metric disk_write \
    --distribution ZIPFIAN

python distribution_comparison_ecdfs.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/ecdfs-fill" \
    --log-scale \
    --metric disk_write \
    --distribution UNIFORM
#
python distribution_comparison_ecdfs.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/ecdfs-fill" \
    --log-scale \
    --metric disk_read \
    --distribution ZIPFIAN

python distribution_comparison_ecdfs.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/ecdfs-fill" \
    --log-scale \
    --metric disk_read \
    --distribution UNIFORM

# Eviction phase (from eviction start)
echo "📊 Generating ECDF plots for eviction phase..."
python distribution_comparison_ecdfs.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/ecdfs-nofill" \
    --log-scale \
    --metric disk_write \
    --from-eviction-start \
    --distribution ZIPFIAN

python distribution_comparison_ecdfs.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/ecdfs-nofill" \
    --log-scale \
    --metric disk_write \
    --from-eviction-start \
    --distribution UNIFORM

python distribution_comparison_ecdfs.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/ecdfs-nofill" \
    --log-scale \
    --metric disk_read \
    --from-eviction-start \
    --distribution ZIPFIAN

python distribution_comparison_ecdfs.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/ecdfs-nofill" \
    --log-scale \
    --metric disk_read \
    --from-eviction-start \
    --distribution UNIFORM


echo "✅ Comparison plots completed"

echo ""

# Step 4: Generate latency comparison matrix tables
echo "Step 4: Generating latency comparison matrix tables"
echo "===================================================="

echo "📊 Generating latency comparison matrix tables (excluding last 5 minutes)..."
mkdir -p "${OUTPUT_DIR}/tables"

# Chunk sizes to include
CHUNK_SIZES="65536 268435456 1129316352"

# Distributions to include
DISTRIBUTIONS="zipfian uniform"

# Ratios to include
RATIOS="2 10"

# Generate disk_read tables for both eviction types
echo "  - Generating disk_read Zone LRU matrix table..."
python3 latency_table_matrix.py \
    --data-dirs "$ZONED_DIR" "$BLOCK_DIR" \
    --chunk-sizes $CHUNK_SIZES \
    --distributions $DISTRIBUTIONS \
    --ratios $RATIOS \
    --eviction promotional \
    --metric disk_read \
    --output-file "${OUTPUT_DIR}/tables/disk_read_zone_lru_matrix.tex" \
    --filter-minutes 5

echo "  - Generating disk_read Chunk LRU matrix table..."
python3 latency_table_matrix.py \
    --data-dirs "$ZONED_DIR" "$BLOCK_DIR" \
    --chunk-sizes $CHUNK_SIZES \
    --distributions $DISTRIBUTIONS \
    --ratios $RATIOS \
    --eviction chunk \
    --metric disk_read \
    --output-file "${OUTPUT_DIR}/tables/disk_read_chunk_lru_matrix.tex" \
    --filter-minutes 5

# Generate disk_write tables for both eviction types
echo "  - Generating disk_write Zone LRU matrix table..."
python3 latency_table_matrix.py \
    --data-dirs "$ZONED_DIR" "$BLOCK_DIR" \
    --chunk-sizes $CHUNK_SIZES \
    --distributions $DISTRIBUTIONS \
    --ratios $RATIOS \
    --eviction promotional \
    --metric disk_write \
    --output-file "${OUTPUT_DIR}/tables/disk_write_zone_lru_matrix.tex" \
    --filter-minutes 5

echo "  - Generating disk_write Chunk LRU matrix table..."
python3 latency_table_matrix.py \
    --data-dirs "$ZONED_DIR" "$BLOCK_DIR" \
    --chunk-sizes $CHUNK_SIZES \
    --distributions $DISTRIBUTIONS \
    --ratios $RATIOS \
    --eviction chunk \
    --metric disk_write \
    --output-file "${OUTPUT_DIR}/tables/disk_write_chunk_lru_matrix.tex" \
    --filter-minutes 5

echo "✅ Latency comparison matrix tables completed"

echo ""

# Step 5: Summary
echo "Step 5: Generation Summary"
echo "=========================="

# Count generated plots and tables
block_count=$(find "${OUTPUT_DIR}/block_individual" -name "*.png" 2>/dev/null | wc -l)
zoned_count=$(find "${OUTPUT_DIR}/zoned_individual" -name "*.png" 2>/dev/null | wc -l)
comparison_count=$(find "${OUTPUT_DIR}/comparison" -name "*.png" 2>/dev/null | wc -l)
table_count=$(find "${OUTPUT_DIR}/tables" -name "*.tex" 2>/dev/null | wc -l)
total_count=$((block_count + zoned_count + comparison_count))

echo "📊 Plot and Table Generation Complete!"
echo ""
echo "Generated plots:"
echo "  • BLOCK-PROMO individual: $block_count plots"
echo "  • ZONED-PROMO individual: $zoned_count plots"
echo "  • Block vs ZNS comparison: $comparison_count plots"
echo "  • Total plots: $total_count"
echo ""
echo "Generated tables:"
echo "  • Latency comparison matrix tables: $table_count tables"
echo ""
echo "Output directories:"
echo "  • Individual BLOCK plots: ${OUTPUT_DIR}/block_individual/"
echo "  • Individual ZONED plots: ${OUTPUT_DIR}/zoned_individual/"
echo "  • Comparison plots: ${OUTPUT_DIR}/comparison/"
echo "  • LaTeX tables: ${OUTPUT_DIR}/tables/"
echo ""
echo "Configuration used:"
echo "  • Throughput bucket size: ${BUCKET_SECONDS}s"
echo "  • Latency smoothing window: ${WINDOW_SECONDS}s"
echo "  • Block interface label: $BLOCK_LABEL"
echo "  • ZNS interface label: $ZONED_LABEL"
echo ""
echo "🎉 All plots and tables generated successfully!"
