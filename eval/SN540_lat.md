# Latency Test SN540

```
fio --name=lat_512MiB_reads \
  --filename=/dev/nvme1n1 \
  --rw=read \
  --direct=1 \
  --ioengine=sync \
  --iodepth=1 \
  --numjobs=1 \
  --bs=512MiB \
  --io_size=51200MiB \
  --group_reporting
```

```
lat_512MiB_reads: (g=0): rw=read, bs=(R) 488MiB-488MiB, (W) 488MiB-488MiB, (T) 488MiB-488MiB, ioengine=sync, iodepth=1
fio-3.36
Starting 1 process
Jobs: 1 (f=1): [R(1)][100.0%][r=2933MiB/s][r=6 IOPS][eta 00m:00s]
lat_512MiB_reads: (groupid=0, jobs=1): err= 0: pid=3607581: Sun Mar  1 11:14:35 2026
  read: IOPS=5, BW=2792MiB/s (2928MB/s)(47.7GiB/17487msec)
    clat (msec): min=168, max=355, avg=174.85, stdev=18.42
     lat (msec): min=168, max=355, avg=174.86, stdev=18.42
    clat percentiles (msec):
     |  1.00th=[  169],  5.00th=[  171], 10.00th=[  171], 20.00th=[  171],
     | 30.00th=[  171], 40.00th=[  171], 50.00th=[  174], 60.00th=[  174],
     | 70.00th=[  174], 80.00th=[  176], 90.00th=[  178], 95.00th=[  180],
     | 99.00th=[  184], 99.50th=[  355], 99.90th=[  355], 99.95th=[  355],
     | 99.99th=[  355]
   bw (  MiB/s): min=  976, max= 2929, per=99.77%, avg=2785.90, stdev=425.44, samples=34
   iops        : min=    2, max=    6, avg= 5.68, stdev= 0.88, samples=34
  lat (msec)   : 250=99.00%, 500=1.00%
  cpu          : usr=0.01%, sys=25.37%, ctx=118, majf=0, minf=125015
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=100,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=1

Run status group 0 (all jobs):
   READ: bw=2792MiB/s (2928MB/s), 2792MiB/s-2792MiB/s (2928MB/s-2928MB/s), io=47.7GiB (51.2GB), run=17487-17487msec

Disk stats (read/write):
  nvme1n1: ios=72923/0, sectors=99364544/0, merge=0/0, ticks=5361715/0, in_queue=5361715, util=99.35%
```

Lat:

```
clat (msec): min=168, max=355, avg=174.85, stdev=18.42
lat (msec): min=168, max=355, avg=174.86, stdev=18.42
```
