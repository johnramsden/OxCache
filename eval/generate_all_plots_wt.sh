#!/bin/bash

# OxCache WiredTiger case-study figure/table generation (SYSTOR '26)
#
# Produces every WiredTiger-workload artifact in the published paper:
#   * plots_wt/boxplot-nofill/wt_throughput.png
#       -> Fig. "WiredTiger throughput" boxplot (eviction phase)
#   * plots_wt/comparison/65536/*client_request_bytes_total_throughput.png
#       -> Fig. "WiredTiger throughput timelines" (Chunk LRU / Zone LRU)
#   * plots_wt/tables/get_total_latency_table.tex
#       -> Table 3 (WiredTiger GET latency statistics)
#   * plots_wt/tables/wt_throughput_table.tex
#       -> WiredTiger throughput comparison values

set -e  # Exit on any error

# Display usage information
usage() {
    cat << EOF
Usage: $0 <zns_dir> <block_dir>

Arguments:
  zns_dir      Path to the ZNS WT consolidated data directory
  block_dir    Path to the Block WT consolidated data directory

Example:
  $0 data/logs/FINAL/WTHIGHERHRATIO/ZNS-consolidated data/logs/FINAL/WTHIGHERHRATIO/SSD-consolidated

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

split_data_if_needed "$ZNS_DIR" "$ZNS_SPLIT_DIR" "ZNS WT"
split_data_if_needed "$BLOCK_DIR" "$BLOCK_SPLIT_DIR" "Block WT"

echo ""

# Step 2: Throughput timeline comparison plots
echo "Step 2: Generating throughput timelines"
echo "========================================"

python3 plot_throughput.py "$ZNS_SPLIT_DIR" "$BLOCK_SPLIT_DIR" \
    --labels "$ZNS_LABEL" "$BLOCK_LABEL" \
    --bucket-seconds $BUCKET_SECONDS \
    --output-dir "${OUTPUT_DIR}/comparison" \
    --metrics client_request_bytes_total \
    --mark-device-fill

echo "✅ Throughput timelines completed"
echo ""

# Step 3: Throughput boxplot (eviction phase)
echo "Step 3: Generating throughput boxplot"
echo "======================================"

./boxplot_wt.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZNS_DIR" \
    --output-dir "${OUTPUT_DIR}/boxplot-nofill" \
    --from-eviction-start

echo "✅ Throughput boxplot completed"
echo ""

# Step 4: Latency and throughput tables
echo "Step 4: Generating comparison tables"
echo "====================================="

mkdir -p "${OUTPUT_DIR}/tables"

echo "  - Generating get_total latency table (Table 3)..."
./latency_table.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZNS_DIR" \
    --output-file "${OUTPUT_DIR}/tables/get_total_latency_table.tex" \
    --metric get_total \
    --filter-minutes 5 \
    --from-eviction-start

echo "  - Generating throughput comparison table..."
./throughput_table_wt.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZNS_DIR" \
    --output-file "${OUTPUT_DIR}/tables/wt_throughput_table.tex" \
    --filter-minutes 5

echo "✅ Comparison tables completed"
echo ""

# Step 5: Summary
echo "Step 5: Generation Summary"
echo "=========================="

comparison_count=$(find "${OUTPUT_DIR}/comparison" -name "*.png" 2>/dev/null | wc -l)
boxplot_nofill_count=$(find "${OUTPUT_DIR}/boxplot-nofill" -name "*.png" 2>/dev/null | wc -l)
table_count=$(find "${OUTPUT_DIR}/tables" -name "*.tex" 2>/dev/null | wc -l)
total_count=$((comparison_count + boxplot_nofill_count))

echo "📊 WT Plot and Table Generation Complete!"
echo ""
echo "Generated plots:"
echo "  • Throughput timelines: $comparison_count plots"
echo "  • Boxplots (eviction phase): $boxplot_nofill_count plots"
echo "  • Total plots: $total_count"
echo ""
echo "Generated tables:"
echo "  • Comparison tables: $table_count tables"
echo ""
echo "Output directories:"
echo "  • Timeline plots: ${OUTPUT_DIR}/comparison/"
echo "  • Boxplots (eviction): ${OUTPUT_DIR}/boxplot-nofill/"
echo "  • LaTeX tables: ${OUTPUT_DIR}/tables/"
echo ""
echo "🎉 All WT plots and tables generated successfully!"
