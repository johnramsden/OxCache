#!/bin/bash
# shellcheck disable=SC2154

set -e

# Get the directory in which this script is located:
SCRIPT_DIR="$(cd "$(dirname "$0")" || exit 1; pwd)"

# Cleanup function to kill server if it's running
cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Cleaning up server PID $SERVER_PID"
        kill "$SERVER_PID"
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

# Set trap to cleanup on script exit
trap cleanup EXIT INT TERM

usage() {
    printf "Usage: %s WORKLOAD_DIR NR_THREADS CONFIGFILE DEVICE\n" "$(basename "$0")"
    exit 1
}

# Check that exactly 4 positional arguments remain
if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters $#, should be 3"
    usage
fi

directory="$1"
threads="$2"
configfile="$3"
device="$4"

echo "Workload Directory: $directory"
echo "Threads: $threads"
echo "Logging to: ./logs"
echo "Configfile: $configfile"
echo "Device: $device"

ret=0

# Loop through all files in the directory
for file in "$directory"/*.bin; do
    # Check if it's a file (not a directory)
    if ! [ -f "$file" ]; then
        echo "ERROR: no such file '$file', skipping"
        continue
    fi
    filename=$(basename ${file})
    # Split the filename into an array using comma as the delimiter
    IFS=, read -ra params <<< "${filename%.*}"

    # Iterate over each key-value pair
    for param in "${params[@]}"; do
        # Extract the key and value by removing the longest matching suffix and prefix respectively
        key="${param%%=*}"
        value="${param#*=}"
        # Declare the variable globally in the script
        declare -g "$key"="$value"
    done

    runfile="./logs/$chunk_size,L=$latency,$distr,R=$ratio,I=$iterations,NZ=$n_zones,${device##*/}-$(date '+%d_%H:%M:%S')-run"
    # Now you can access the variables
    {
        echo "Chunk Size: $chunk_size"
        echo "Latency: $latency"
        echo "Distribution Type: $distr"
        echo "Working Set Ratio: $ratio"
        echo "Zone Size: $zone_size"
        echo "Iterations: $iterations"
        echo "Number of Zones: $n_zones"
        echo "Total Chunks: $chunks"
        echo "High Water: $evict_high"
        echo "Low water: $evict_low"
        echo "Eviction $eviction"
        echo "Device $device"
    } | tee "$runfile.client"

    echo >> "$runfile.client"

    # shellcheck disable=SC2024
    echo "Running $runfile"

    if [ "$device" = "/dev/nvme1n1" ]; then
        echo "Pre-conditioning SSD"
        "${SCRIPT_DIR}/precondition-nvme1n1.sh" "${n_zones}"
    fi

    ./target/debug/oxcache --config="$configfile" \
      --max-zones="${n_zones}" \
      --chunk-size="$chunk_size" \
      --eviction-policy="$eviction" \
      --high-water-evict="$evict_high" \
      --low-water-evict="$evict_low" \
      --log-level=info \
      --remote-artificial-delay_microsec="$latency" \
      --disk="$device" &>> "$runfile.server" &
    SERVER_PID=$!

    sleep 5s

    ./target/debug/evaluationclient \
      --data-file="$file" \
      --socket /tmp/oxcache.sock \
      --num-clients="$threads" \
      --query-size="$chunk_size" >> "$runfile.client"
    ret=$?

    kill $SERVER_PID || true
    wait "$SERVER_PID" 2>/dev/null || true
    unset SERVER_PID | true

    if [ $ret -ne 0 ]; then
        echo "Run FAILED!"
        ret=1
    else
        tail -n3 "$runfile.client"
    fi
done

exit $ret
