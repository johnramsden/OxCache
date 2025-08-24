#!/bin/bash

# OxCache Evaluation Plot Generation Script
# Automates data splitting and plot generation for all metrics

set -e  # Exit on any error

echo "=== OxCache Evaluation Plot Generation ==="
echo ""

# Configuration
BUCKET_SECONDS=60
WINDOW_SECONDS=60
OUTPUT_DIR="plots"
BLOCK_DIR="data/BLOCK-PROMO"
ZONED_DIR="data/ZONED-PROMO"
BLOCK_SPLIT_DIR="${BLOCK_DIR}/split_output"
ZONED_SPLIT_DIR="${ZONED_DIR}/split_output"

# Labels for comparison plots
BLOCK_LABEL="Block-interface"
ZONED_LABEL="ZNS"

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
        python split_data.py "$source_dir"
        
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

split_data_if_needed "$BLOCK_DIR" "$BLOCK_SPLIT_DIR" "BLOCK-PROMO"
split_data_if_needed "$ZONED_DIR" "$ZONED_SPLIT_DIR" "ZONED-PROMO"

echo ""

## Step 2: Generate individual plots for each dataset
#echo "Step 2: Generating individual plots"
#echo "===================================="
#
#echo "📈 Generating BLOCK-PROMO individual plots..."
#
## Throughput plots
#python plot_throughput.py "$BLOCK_SPLIT_DIR" \
#    --bucket-seconds $BUCKET_SECONDS \
#    --output-dir "${OUTPUT_DIR}/block_individual" \
#    --metrics bytes_total written_bytes_total
#
## Raw latency plots
#python plot_latency.py "$BLOCK_SPLIT_DIR" \
#    --output-dir "${OUTPUT_DIR}/block_individual" \
#    --metrics device_write_latency_ms disk_write_latency_ms get_miss_latency_ms get_total_latency_ms
#
## Smoothed latency plots
#python plot_latency_smoothed.py "$BLOCK_SPLIT_DIR" \
#    --window-seconds $WINDOW_SECONDS \
#    --output-dir "${OUTPUT_DIR}/block_individual" \
#    --metrics device_write_latency_ms disk_write_latency_ms get_miss_latency_ms get_total_latency_ms
#
## Hit ratio plots
#python plot_hitratio.py "$BLOCK_SPLIT_DIR" \
#    --output-dir "${OUTPUT_DIR}/block_individual"
#
#echo "✅ BLOCK-PROMO individual plots completed"
#
#echo "📈 Generating ZONED-PROMO individual plots..."
#
## Throughput plots
#python plot_throughput.py "$ZONED_SPLIT_DIR" \
#    --bucket-seconds $BUCKET_SECONDS \
#    --output-dir "${OUTPUT_DIR}/zoned_individual" \
#    --metrics bytes_total written_bytes_total
#
## Raw latency plots
#python plot_latency.py "$ZONED_SPLIT_DIR" \
#    --output-dir "${OUTPUT_DIR}/zoned_individual" \
#    --metrics device_write_latency_ms disk_write_latency_ms get_miss_latency_ms get_total_latency_ms
#
## Smoothed latency plots
#python plot_latency_smoothed.py "$ZONED_SPLIT_DIR" \
#    --window-seconds $WINDOW_SECONDS \
#    --output-dir "${OUTPUT_DIR}/zoned_individual" \
#    --metrics device_write_latency_ms disk_write_latency_ms get_miss_latency_ms get_total_latency_ms
#
## Hit ratio plots
#python plot_hitratio.py "$ZONED_SPLIT_DIR" \
#    --output-dir "${OUTPUT_DIR}/zoned_individual"
#
#echo "✅ ZONED-PROMO individual plots completed"
#
#echo ""

# Step 3: Generate comparison plots
echo "Step 3: Generating comparison plots"
echo "===================================="

echo "📊 Generating comparison plots (Block vs ZNS)..."

# Throughput comparison plots
python plot_throughput.py "$BLOCK_SPLIT_DIR" "$ZONED_SPLIT_DIR" \
    --labels "$BLOCK_LABEL" "$ZONED_LABEL" \
    --bucket-seconds $BUCKET_SECONDS \
    --output-dir "${OUTPUT_DIR}/comparison" \
    --metrics bytes_total written_bytes_total read_bytes_total

# Smoothed latency comparison plots
python plot_latency_smoothed.py "$BLOCK_SPLIT_DIR" "$ZONED_SPLIT_DIR" \
    --labels "$BLOCK_LABEL" "$ZONED_LABEL" \
    --window-seconds $WINDOW_SECONDS \
    --output-dir "${OUTPUT_DIR}/comparison" \
    --metrics device_write_latency_ms device_read_latency_ms disk_write_latency_ms disk_read_latency_ms get_miss_latency_ms get_hit_latency_ms get_total_latency_ms

# Hit ratio comparison plots
python plot_hitratio.py "$BLOCK_SPLIT_DIR" "$ZONED_SPLIT_DIR" \
    --labels "$BLOCK_LABEL" "$ZONED_LABEL" \
    --output-dir "${OUTPUT_DIR}/comparison"

echo "✅ Comparison plots completed"

echo ""

# Step 4: Summary
echo "Step 4: Generation Summary"
echo "=========================="

# Count generated plots
block_count=$(find "${OUTPUT_DIR}/block_individual" -name "*.png" 2>/dev/null | wc -l)
zoned_count=$(find "${OUTPUT_DIR}/zoned_individual" -name "*.png" 2>/dev/null | wc -l)
comparison_count=$(find "${OUTPUT_DIR}/comparison" -name "*.png" 2>/dev/null | wc -l)
total_count=$((block_count + zoned_count + comparison_count))

echo "📊 Plot Generation Complete!"
echo ""
echo "Generated plots:"
echo "  • BLOCK-PROMO individual: $block_count plots"
echo "  • ZONED-PROMO individual: $zoned_count plots"
echo "  • Block vs ZNS comparison: $comparison_count plots"
echo "  • Total: $total_count plots"
echo ""
echo "Plot directories:"
echo "  • Individual BLOCK plots: ${OUTPUT_DIR}/block_individual/"
echo "  • Individual ZONED plots: ${OUTPUT_DIR}/zoned_individual/"
echo "  • Comparison plots: ${OUTPUT_DIR}/comparison/"
echo ""
echo "Configuration used:"
echo "  • Throughput bucket size: ${BUCKET_SECONDS}s"
echo "  • Latency smoothing window: ${WINDOW_SECONDS}s"
echo "  • Block interface label: $BLOCK_LABEL"
echo "  • ZNS interface label: $ZONED_LABEL"
echo ""
echo "🎉 All plots generated successfully!"