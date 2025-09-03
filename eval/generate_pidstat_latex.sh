#!/bin/bash

# Simple script to generate LaTeX from pidstat files
# Processes all .pidstat files in OXCACHE-UTILIZATION and ZNCACHE-UTILIZATION directories

echo "\subsection{Resource Use}"
echo "\label{sec:appendix-resources}"

echo "The following tables show distributions for CPU and RAM usage throughout workloads."

./average_pidstat.py --caption-prefix OxCache data/ZONED-PROMO/*.pidstat
./average_pidstat.py --caption-prefix OxCache data/BLOCK-PROMO/*.pidstat
./average_pidstat.py --caption-prefix ZNCache data/ZNCACHE-UTILIZATION-ZONED/*.pidstat
./average_pidstat.py --caption-prefix ZNCache data/ZNCACHE-UTILIZATION-BLOCK/*.pidstat

for dir in data/ZONED-PROMO; do
    [ -d "$dir" ] && find "$dir" -name "*.pidstat" -exec python3 parse_pidstat.py --latex {} --caption-prefix OxCache \;
done

for dir in data/BLOCK-PROMO; do
    [ -d "$dir" ] && find "$dir" -name "*.pidstat" -exec python3 parse_pidstat.py --latex {} --caption-prefix OxCache \;
done


for dir in data/ZNCACHE-UTILIZATION-ZONED; do
    [ -d "$dir" ] && find "$dir" -name "*.pidstat" -exec python3 parse_pidstat.py --latex {} --caption-prefix ZNCache \;
done

for dir in data/ZNCACHE-UTILIZATION-BLOCK; do
    [ -d "$dir" ] && find "$dir" -name "*.pidstat" -exec python3 parse_pidstat.py --latex {} --caption-prefix ZNCache \;
done