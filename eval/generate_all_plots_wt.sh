#!/bin/bash

# OxCache WT Workload Plot Generation Script
# Automates plot generation for WT workload metrics

set -e  # Exit on any error

# Display usage information
usage() {
    cat << EOF
Usage: $0 <zns_dir> <block_dir>

Arguments:
  zns_dir      Path to the ZNS WT data directory
  block_dir    Path to the Block WT data directory

Example:
  $0 data/logs/WT/WT-SPC-ZNS-consolidated data/logs/WT/WT-SPC-SSD-consolidated

EOF
    exit 1
}

# Check if required arguments are provided
if [ $# -ne 2 ]; then
    echo "Error: Missing required arguments"
    echo ""
    usage
fi

ZNS_DIR="$1"
BLOCK_DIR="$2"

# Validate that directories exist
if [ ! -d "$ZNS_DIR" ]; then
    echo "Error: ZNS directory does not exist: $ZNS_DIR"
    exit 1
fi

if [ ! -d "$BLOCK_DIR" ]; then
    echo "Error: Block directory does not exist: $BLOCK_DIR"
    exit 1
fi

echo "=== OxCache WT Workload Plot Generation ==="
echo ""
echo "Configuration:"
echo "  ZNS directory: $ZNS_DIR"
echo "  Block directory: $BLOCK_DIR"
echo ""

# Configuration
BUCKET_SECONDS=60
WINDOW_SECONDS=60
OUTPUT_DIR="plots_wt"
ZNS_SPLIT_DIR="${ZNS_DIR}/split_output"
BLOCK_SPLIT_DIR="${BLOCK_DIR}/split_output"

# Labels for comparison plots
ZNS_LABEL="ZNS"
BLOCK_LABEL="Block"

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

# Step 1: Check split data exists
echo "Step 1: Checking split data"
echo "============================"

check_split_data "$ZNS_SPLIT_DIR" "ZNS WT"
check_split_data "$BLOCK_SPLIT_DIR" "Block WT"

echo ""

# Step 2: Generate individual plots for each dataset
echo "Step 2: Generating individual plots"
echo "===================================="

echo "📈 Generating Block WT individual plots..."

# Throughput plots
python3 plot_throughput.py "$BLOCK_SPLIT_DIR" \
    --bucket-seconds $BUCKET_SECONDS \
    --output-dir "${OUTPUT_DIR}/block_individual" \
    --metrics client_request_bytes_total \
    --mark-device-fill

# Raw latency plots
python3 plot_latency.py "$BLOCK_SPLIT_DIR" \
    --output-dir "${OUTPUT_DIR}/block_individual" \
    --metrics disk_write_latency_ms disk_read_latency_ms get_response_latency_ms

# Smoothed latency plots
python3 plot_latency_smoothed.py "$BLOCK_SPLIT_DIR" \
    --window-seconds $WINDOW_SECONDS \
    --output-dir "${OUTPUT_DIR}/block_individual" \
    --metrics disk_write_latency_ms disk_read_latency_ms get_response_latency_ms \
    --mark-device-fill

# Hit ratio plots
python3 plot_hitratio.py "$BLOCK_SPLIT_DIR" \
    --output-dir "${OUTPUT_DIR}/block_individual"

echo "✅ Block WT individual plots completed"

echo "📈 Generating ZNS WT individual plots..."

# Throughput plots
python3 plot_throughput.py "$ZNS_SPLIT_DIR" \
    --bucket-seconds $BUCKET_SECONDS \
    --output-dir "${OUTPUT_DIR}/zns_individual" \
    --metrics bytes_total written_bytes_total read_bytes_total client_request_bytes_total \
    --mark-device-fill

# Raw latency plots
python3 plot_latency.py "$ZNS_SPLIT_DIR" \
    --output-dir "${OUTPUT_DIR}/zns_individual" \
    --metrics disk_write_latency_ms disk_read_latency_ms get_response_latency_ms

# Smoothed latency plots
python3 plot_latency_smoothed.py "$ZNS_SPLIT_DIR" \
    --window-seconds $WINDOW_SECONDS \
    --output-dir "${OUTPUT_DIR}/zns_individual" \
    --metrics disk_write_latency_ms disk_read_latency_ms get_response_latency_ms \
    --mark-device-fill

# Hit ratio plots
python3 plot_hitratio.py "$ZNS_SPLIT_DIR" \
    --output-dir "${OUTPUT_DIR}/zns_individual"

echo "✅ ZNS WT individual plots completed"

echo ""

# Step 3: Generate comparison plots
echo "Step 3: Generating comparison plots"
echo "===================================="

echo "📊 Generating comparison plots (Block vs ZNS)..."

# Throughput comparison plots
python3 plot_throughput.py "$ZNS_SPLIT_DIR" "$BLOCK_SPLIT_DIR" \
    --labels "$ZNS_LABEL" "$BLOCK_LABEL" \
    --bucket-seconds $BUCKET_SECONDS \
    --output-dir "${OUTPUT_DIR}/comparison" \
    --metrics written_bytes_total read_bytes_total client_request_bytes_total \
    --mark-device-fill

# Smoothed latency comparison plots
python3 plot_latency.py "$ZNS_SPLIT_DIR" "$BLOCK_SPLIT_DIR" \
    --labels "$ZNS_LABEL" "$BLOCK_LABEL" \
    --output-dir "${OUTPUT_DIR}/comparison" \
    --metrics disk_write_latency_ms disk_read_latency_ms get_response_latency_ms \
    --mark-device-fill
#
# Smoothed latency comparison plots
python3 plot_latency_smoothed.py "$ZNS_SPLIT_DIR" "$BLOCK_SPLIT_DIR" \
    --labels "$ZNS_LABEL" "$BLOCK_LABEL" \
    --window-seconds $WINDOW_SECONDS \
    --output-dir "${OUTPUT_DIR}/comparison" \
    --metrics disk_write_latency_ms disk_read_latency_ms get_response_latency_ms \
    --mark-device-fill

# Hit ratio comparison plots
python3 plot_hitratio.py "$ZNS_SPLIT_DIR" "$BLOCK_SPLIT_DIR" \
    --labels "$ZNS_LABEL" "$BLOCK_LABEL" \
    --output-dir "${OUTPUT_DIR}/comparison"

echo "✅ Comparison plots completed"

echo ""

# Step 4: Generate boxplots
echo "Step 4: Generating boxplots"
echo "============================"

# Throughput boxplots - fill phase (whole run)
echo "📊 Generating WT throughput boxplots (fill phase)..."
./boxplot_wt.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZNS_DIR" \
    --output-dir "${OUTPUT_DIR}/boxplot-fill"

echo "✅ Fill phase boxplots completed"

# Throughput boxplots - eviction phase (from eviction start)
echo "📊 Generating WT throughput boxplots (eviction phase)..."
./boxplot_wt.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZNS_DIR" \
    --output-dir "${OUTPUT_DIR}/boxplot-nofill" \
    --from-eviction-start

echo "✅ Eviction phase boxplots completed"

echo ""

# Step 5: Generate ECDF plots
echo "Step 5: Generating ECDF plots"
echo "=============================="

# ECDF plots for different latency metrics
LATENCY_METRICS=("disk_read" "disk_write" "get_response_latency_ms")

# Fill phase (whole run)
echo "📊 Generating ECDF plots for fill phase..."
for metric in "${LATENCY_METRICS[@]}"; do
    echo "  - Generating ${metric} ECDF (fill phase)..."
    ./ecdf_wt.py \
        --block-dir "$BLOCK_DIR" \
        --zns-dir "$ZNS_DIR" \
        --output-dir "${OUTPUT_DIR}/ecdfs-fill" \
        --log-scale \
        --metric "$metric"
done

echo "✅ Fill phase ECDF plots completed"

# Eviction phase (from eviction start)
echo "📊 Generating ECDF plots for eviction phase..."
for metric in "${LATENCY_METRICS[@]}"; do
    echo "  - Generating ${metric} ECDF (eviction phase)..."
    ./ecdf_wt.py \
        --block-dir "$BLOCK_DIR" \
        --zns-dir "$ZNS_DIR" \
        --output-dir "${OUTPUT_DIR}/ecdfs-nofill" \
        --log-scale \
        --metric "$metric" \
        --from-eviction-start
done

echo "✅ Eviction phase ECDF plots completed"

echo ""

# Step 6: Generate latency comparison tables
echo "Step 6: Generating latency comparison tables"
echo "=============================================="

# Latency metrics for tables
TABLE_METRICS=("disk_read" "disk_write" "get_response_latency_ms")

echo "📊 Generating latency comparison tables (excluding last 5 minutes)..."
mkdir -p "${OUTPUT_DIR}/tables"

for metric in "${TABLE_METRICS[@]}"; do
    echo "  - Generating ${metric} latency table..."
    ./latency_table.py \
        --block-dir "$BLOCK_DIR" \
        --zns-dir "$ZNS_DIR" \
        --output-file "${OUTPUT_DIR}/tables/${metric}_latency_table.tex" \
        --metric "$metric" \
        --filter-minutes 5
done

echo "✅ Latency comparison tables completed"

echo ""

# Step 7: Summary
echo "Step 7: Generation Summary"
echo "=========================="

# Count generated plots and tables
block_count=$(find "${OUTPUT_DIR}/block_individual" -name "*.png" 2>/dev/null | wc -l)
zns_count=$(find "${OUTPUT_DIR}/zns_individual" -name "*.png" 2>/dev/null | wc -l)
comparison_count=$(find "${OUTPUT_DIR}/comparison" -name "*.png" 2>/dev/null | wc -l)
boxplot_fill_count=$(find "${OUTPUT_DIR}/boxplot-fill" -name "*.png" 2>/dev/null | wc -l)
boxplot_nofill_count=$(find "${OUTPUT_DIR}/boxplot-nofill" -name "*.png" 2>/dev/null | wc -l)
ecdf_fill_count=$(find "${OUTPUT_DIR}/ecdfs-fill" -name "*.png" 2>/dev/null | wc -l)
ecdf_nofill_count=$(find "${OUTPUT_DIR}/ecdfs-nofill" -name "*.png" 2>/dev/null | wc -l)
table_count=$(find "${OUTPUT_DIR}/tables" -name "*.tex" 2>/dev/null | wc -l)
total_count=$((block_count + zns_count + comparison_count + boxplot_fill_count + boxplot_nofill_count + ecdf_fill_count + ecdf_nofill_count))

echo "📊 WT Plot and Table Generation Complete!"
echo ""
echo "Generated plots:"
echo "  • Block WT individual: $block_count plots"
echo "  • ZNS WT individual: $zns_count plots"
echo "  • Block vs ZNS comparison: $comparison_count plots"
echo "  • Boxplots (fill phase): $boxplot_fill_count plots"
echo "  • Boxplots (eviction phase): $boxplot_nofill_count plots"
echo "  • ECDFs (fill phase): $ecdf_fill_count plots"
echo "  • ECDFs (eviction phase): $ecdf_nofill_count plots"
echo "  • Total plots: $total_count"
echo ""
echo "Generated tables:"
echo "  • Latency comparison tables: $table_count tables"
echo ""
echo "Output directories:"
echo "  • Individual Block plots: ${OUTPUT_DIR}/block_individual/"
echo "  • Individual ZNS plots: ${OUTPUT_DIR}/zns_individual/"
echo "  • Comparison plots: ${OUTPUT_DIR}/comparison/"
echo "  • Boxplots (fill): ${OUTPUT_DIR}/boxplot-fill/"
echo "  • Boxplots (eviction): ${OUTPUT_DIR}/boxplot-nofill/"
echo "  • ECDFs (fill): ${OUTPUT_DIR}/ecdfs-fill/"
echo "  • ECDFs (eviction): ${OUTPUT_DIR}/ecdfs-nofill/"
echo "  • LaTeX tables: ${OUTPUT_DIR}/tables/"
echo ""
echo "Configuration used:"
echo "  • Throughput bucket size: ${BUCKET_SECONDS}s"
echo "  • Latency smoothing window: ${WINDOW_SECONDS}s"
echo "  • Block interface label: $BLOCK_LABEL"
echo "  • ZNS interface label: $ZNS_LABEL"
echo ""
echo "🎉 All WT plots and tables generated successfully!"
