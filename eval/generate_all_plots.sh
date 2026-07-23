#!/bin/bash

# OxCache parameter-sweep figure/table generation (SYSTOR '26)
#
# Produces every parameter-sweep artifact in the published paper:
#   * plots/comparison/boxplot-nofill/{zipfian,uniform}_throughput.png
#       -> Fig. "Cache throughput" boxplots (eviction phase)
#   * plots/comparison/hitratio_bars_combined.png
#       -> Fig. "Hit ratio" horizontal bars
#   * plots/tables/get_total_{zone,chunk}_lru_matrix.tex
#       -> Table 2 (GET P99 latency matrix; the P99 rows of these
#          matrices are the published values)

set -e  # Exit on any error

# Display usage information
usage() {
    cat << EOF
Usage: $0 <zoned_dir> <block_dir>

Arguments:
  zoned_dir    Path to the zoned (ZNS) consolidated data directory
  block_dir    Path to the block-interface consolidated data directory

Example:
  $0 data/logs/FINAL/PARAM/ZNS-consolidated data/logs/FINAL/PARAM/SSD-consolidated

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
OUTPUT_DIR="plots"
ZONED_SPLIT_DIR="${ZONED_DIR}/split_output"
BLOCK_SPLIT_DIR="${BLOCK_DIR}/split_output"

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

split_data_if_needed "$ZONED_DIR" "$ZONED_SPLIT_DIR" "ZNS"
split_data_if_needed "$BLOCK_DIR" "$BLOCK_SPLIT_DIR" "Block"

echo ""

# Step 2: Throughput boxplots (eviction phase)
echo "Step 2: Generating throughput boxplots"
echo "======================================="

python distribution_comparison_boxplots.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison/boxplot-nofill" \
    --common-y-scale \
    --from-eviction-start

echo "✅ Throughput boxplots completed"
echo ""

# Step 3: Hit-ratio horizontal bar chart
echo "Step 3: Generating hit-ratio bars"
echo "=================================="

python hitratio_horizontal_bars_combined.py \
    --block-dir "$BLOCK_DIR" \
    --zns-dir "$ZONED_DIR" \
    --output-dir "${OUTPUT_DIR}/comparison"

echo "✅ Hit-ratio bars completed"
echo ""

# Step 4: GET latency comparison matrix tables (Table 2)
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

echo "  - Generating get_total Chunk LRU matrix table..."
python3 latency_table_matrix.py \
    --data-dirs "$ZONED_DIR" "$BLOCK_DIR" \
    --chunk-sizes $CHUNK_SIZES \
    --distributions $DISTRIBUTIONS \
    --ratios $RATIOS \
    --eviction chunk \
    --metric get_total \
    --output-file "${OUTPUT_DIR}/tables/get_total_chunk_lru_matrix.tex" \
    --from-eviction-start \
    --filter-minutes 5

echo "  - Generating get_total Zone LRU matrix table..."
python3 latency_table_matrix.py \
    --data-dirs "$ZONED_DIR" "$BLOCK_DIR" \
    --chunk-sizes $CHUNK_SIZES \
    --distributions $DISTRIBUTIONS \
    --ratios $RATIOS \
    --eviction promotional \
    --metric get_total \
    --output-file "${OUTPUT_DIR}/tables/get_total_zone_lru_matrix.tex" \
    --from-eviction-start \
    --filter-minutes 5

echo "✅ Latency comparison matrix tables completed"
echo ""

# Step 5: Summary
echo "Step 5: Generation Summary"
echo "=========================="

comparison_count=$(find "${OUTPUT_DIR}/comparison" -name "*.png" 2>/dev/null | wc -l)
table_count=$(find "${OUTPUT_DIR}/tables" -name "*.tex" 2>/dev/null | wc -l)

echo "📊 Plot and Table Generation Complete!"
echo ""
echo "Generated artifacts:"
echo "  • Comparison plots: $comparison_count"
echo "  • LaTeX tables: $table_count"
echo ""
echo "Output directories:"
echo "  • Comparison plots: ${OUTPUT_DIR}/comparison/"
echo "  • LaTeX tables: ${OUTPUT_DIR}/tables/"
echo ""
echo "🎉 All plots and tables generated successfully!"
