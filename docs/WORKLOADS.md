# Workloads

## General

```shell
sudo apt install maven meson
```

To generate ZNS workloads run:

```shell
cd vendor/workloadgen/core
mvn -Dtest=site.ycsb.generator.TestZipfianGeneratorZNS test
```


To generate block-interface workloads run:

```shell
cd vendor/workloadgen/core
mvn -Dtest=site.ycsb.generator.TestZipfianGeneratorBLOCK test
```

Workloads will be located in `target/workloads`.

THESE SCRIPTS WILL WIPE THE DISK!

To run ZNS workloads, run (replacing $DEVICE and $CONFIGFILE):

```shell
sudo  ./scripts/run_cpu_bench.sh vendor/workloadgen/core/target/workloadszoned $CONFIGFILE $DEVICE
```

To run block-interface workloads, run (replacing $DEVICE and $CONFIGFILE):

```shell
sudo  ./scripts/run_cpu_bench.sh vendor/workloadgen/core/target/workloadszoned $CONFIGFILE $DEVICE
```

Output will be in `./logs/*` files

## Cortes

On cortes, code is in `/data/john/OxCache`.

SSD: `/dev/nvme1n1`
ZNS: `/dev/nvme0n2`

Sanity check:

```
$ lsblk
NAME         MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
sda            8:0    0 894.3G  0 disk
├─sda1         8:1    0   511M  0 part /boot/efi
├─sda2         8:2    0     1M  0 part
├─sda3         8:3    0 893.8G  0 part
│ ├─vg-swap  252:0    0     8G  0 lvm  [SWAP]
│ ├─vg-root1 252:1    0  92.7G  0 lvm  /
│ ├─vg-var1  252:2    0  10.5G  0 lvm  /var
│ ├─vg-root2 252:3    0  92.7G  0 lvm  /altroot
│ ├─vg-var2  252:4    0  10.5G  0 lvm  /altroot/var
│ └─vg-data  252:5    0 669.4G  0 lvm  /data
└─sda4         8:4    0     1M  0 part
nvme0n1      259:0    0     2G  0 disk
nvme0n2      259:1    0   1.8T  0 disk
nvme1n1      259:2    0 894.3G  0 disk
```

```
$ lsblk -z
NAME         ZONED
sda          none
├─sda1       none
├─sda2       none
├─sda3       none
│ ├─vg-swap  none
│ ├─vg-root1 none
│ ├─vg-var1  none
│ ├─vg-root2 none
│ ├─vg-var2  none
│ └─vg-data  none
└─sda4       none
nvme0n1      none
nvme0n2      host-managed
nvme1n1      none
```

## Scheduler

**IMPORTANT**: On Linux older than 6.10 Must use a non re-ordering scheduler such as `mq-deadline`, set accordingly in `/sys/block/$DEVICE/queue/scheduler`

```shell
echo mq-deadline | tee /sys/block/$DEVICE/queue/scheduler
cat /sys/block/$DEVICE/queue/scheduler
```

Run simple:

```shell
sudo ./target/debug/oxcache --config cortes.server.zns.toml
sudo ./target/debug/evaluationclient --socket /tmp/oxcache.sock --num-clients 64 --data-file $FILE
```

## Pre-conditioning

```shell
fio --name=precondition --filename=/dev/nvme1n1 --direct=1 \
    --rw=randwrite --bs=64k --size=100% --loops=2 \
    --randrepeat=0 --ioengine=libaio \
    --numjobs=1 --group_reporting
```
