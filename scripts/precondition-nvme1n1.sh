#!/bin/sh

set -e

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters $#, should be 1"
    echo "Usage: $0 NUM_ZONES"
    exit 1
fi

NUM_ZONES=${1}

sgdisk --zap-all /dev/nvme1n1

fio --name=precondition --filename=/dev/nvme1n1 --direct=1 \
    --rw=randwrite --bs=64k --size=100% --loops=1 \
    --randrepeat=0 --ioengine=libaio \
    --numjobs=1 --group_reporting

MIB=$((NUM_ZONES * 1077 + 1))

echo "Creating partition of size $MIB MiB on /dev/nvme1n1"

sgdisk -n 1:2048B:${MIB}MiB -n 2:0:0 /dev/nvme1n1

