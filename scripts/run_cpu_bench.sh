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
    if [ -n "$PIDSTAT_PID" ] && kill -0 "$PIDSTAT_PID" 2>/dev/null; then
        echo "Cleaning up pidstat PID $PIDSTAT_PID"
        kill "$PIDSTAT_PID"
        wait "$PIDSTAT_PID" 2>/dev/null || true
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

    runfile="./logs/$chunk_size,L=$latency,$distr,R=$ratio,I=$iterations,NZ=$n_zones,$eviction,${device##*/}-$(date '+%d_%H:%M:%S')-run"
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
        if [ -n "$clean_high" ]; then
            echo "Clean High Water: $clean_high"
        fi
        if [ -n "$clean_low" ]; then
            echo "Clean Low Water: $clean_low"
        fi
    } | tee "$runfile.client"

    echo >> "$runfile.client"

    # shellcheck disable=SC2024
    echo "Running $runfile"

    if [ "$device" = "/dev/nvme1n1" ]; then
        echo "Pre-conditioning SSD"
         "${SCRIPT_DIR}/precondition-nvme1n1.sh" "${n_zones}"
#        sudo blkdiscard -f /dev/nvme1n1
    fi

    clean_args=""
    if [ -n "$clean_high" ]; then
        clean_args="$clean_args --high-water-clean=$clean_high"
    fi
    if [ -n "$clean_low" ]; then
        clean_args="$clean_args --low-water-clean=$clean_low"
    fi

        # --max-zones="${n_zones}" \
    ./target/release/oxcache --config="$configfile" \
        --chunk-size="$chunk_size" \
        --eviction-policy="$eviction" \
        --high-water-evict="$evict_high" \
        --low-water-evict="$evict_low" \
        --log-level=info \
        "$([ "$latency" -ne 0 ] && echo "--remote-artificial-delay-microsec=$latency")" \
        --disk="$device" $clean_args &>> "$runfile.server" &
    SERVER_PID=$!

    sleep 30s

    # Start pidstat to monitor CPU and memory usage
    pidstat -u -r 1 -p "${SERVER_PID}" > "$runfile.pidstat" &
    PIDSTAT_PID=$!

    ./target/release/evaluationclient \
      --data-file="$file" \
      --socket /tmp/oxcache.sock \
      --num-clients="$threads" \
      --query-size="$chunk_size" >> "$runfile.client"
    ret=$?

    # Stop pidstat monitoring
    kill "$PIDSTAT_PID" 2>/dev/null || true
    wait "$PIDSTAT_PID" 2>/dev/null || true

    kill -9 "$(pidof oxcache)" || true
    wait "$SERVER_PID" 2>/dev/null || true
    unset SERVER_PID | true

    if [ $ret -ne 0 ]; then
        echo "Run FAILED!"
        ret=1
    else
        tail -n3 "$runfile.client"
    fi

    sleep 10s

    tar -czf "./logs-compressed/$runfile.tar.gz" ./logs

done

exit $ret
