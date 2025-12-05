#!/bin/sh

# BLOCK
workloads=(
    'chunk_size=1129316352,latency=5413781,evict_high=2,evict_low=3,eviction=promotional,n_zones=904,distr=UNIFORM,ratio=10,zone_size=1129316352,iterations=6000,chunks=904'
    'chunk_size=1129316352,latency=5413781,evict_high=2,evict_low=3,eviction=promotional,n_zones=904,distr=UNIFORM,ratio=2,zone_size=1129316352,iterations=6000,chunks=904'
    'chunk_size=1129316352,latency=5413781,evict_high=2,evict_low=3,eviction=promotional,n_zones=904,distr=ZIPFIAN,ratio=10,zone_size=1129316352,iterations=6000,chunks=904'
    'chunk_size=1129316352,latency=5413781,evict_high=2,evict_low=3,eviction=promotional,n_zones=904,distr=ZIPFIAN,ratio=2,zone_size=1129316352,iterations=6000,chunks=904'
    'chunk_size=268435456,latency=3209583,evict_high=2,evict_low=6,eviction=promotional,n_zones=904,distr=UNIFORM,ratio=10,zone_size=1129316352,iterations=24000,chunks=3616'
    'chunk_size=268435456,latency=3209583,evict_high=2,evict_low=6,eviction=promotional,n_zones=904,distr=UNIFORM,ratio=2,zone_size=1129316352,iterations=24000,chunks=3616'
    'chunk_size=268435456,latency=3209583,evict_high=2,evict_low=6,eviction=promotional,n_zones=904,distr=ZIPFIAN,ratio=10,zone_size=1129316352,iterations=24000,chunks=3616'
    'chunk_size=268435456,latency=3209583,evict_high=2,evict_low=6,eviction=promotional,n_zones=904,distr=ZIPFIAN,ratio=2,zone_size=1129316352,iterations=24000,chunks=3616'
    'chunk_size=268435456,latency=3209583,evict_high=5,evict_low=37,eviction=chunk,n_zones=904,clean_low=0,distr=UNIFORM,ratio=10,zone_size=1129316352,iterations=24000,chunks=3616'
    'chunk_size=268435456,latency=3209583,evict_high=5,evict_low=37,eviction=chunk,n_zones=904,clean_low=0,distr=UNIFORM,ratio=2,zone_size=1129316352,iterations=24000,chunks=3616'
    'chunk_size=268435456,latency=3209583,evict_high=5,evict_low=37,eviction=chunk,n_zones=904,clean_low=0,distr=ZIPFIAN,ratio=10,zone_size=1129316352,iterations=24000,chunks=3616'
    'chunk_size=268435456,latency=3209583,evict_high=5,evict_low=37,eviction=chunk,n_zones=904,clean_low=0,distr=ZIPFIAN,ratio=2,zone_size=1129316352,iterations=24000,chunks=3616'
    'chunk_size=65536,latency=40632,evict_high=17233,evict_low=86161,eviction=chunk,n_zones=904,clean_low=0,distr=UNIFORM,ratio=10,zone_size=1129316352,iterations=98304000,chunks=15577728'
    'chunk_size=65536,latency=40632,evict_high=17233,evict_low=86161,eviction=chunk,n_zones=904,clean_low=0,distr=UNIFORM,ratio=2,zone_size=1129316352,iterations=98304000,chunks=15577728'
    'chunk_size=65536,latency=40632,evict_high=17233,evict_low=86161,eviction=chunk,n_zones=904,clean_low=0,distr=ZIPFIAN,ratio=10,zone_size=1129316352,iterations=98304000,chunks=15577728'
    'chunk_size=65536,latency=40632,evict_high=17233,evict_low=86161,eviction=chunk,n_zones=904,clean_low=0,distr=ZIPFIAN,ratio=2,zone_size=1129316352,iterations=98304000,chunks=15577728'
    'chunk_size=65536,latency=40632,evict_high=5,evict_low=9,eviction=promotional,n_zones=904,distr=UNIFORM,ratio=10,zone_size=1129316352,iterations=98304000,chunks=15577728'
    'chunk_size=65536,latency=40632,evict_high=5,evict_low=9,eviction=promotional,n_zones=904,distr=UNIFORM,ratio=2,zone_size=1129316352,iterations=98304000,chunks=15577728'
    'chunk_size=65536,latency=40632,evict_high=5,evict_low=9,eviction=promotional,n_zones=904,distr=ZIPFIAN,ratio=10,zone_size=1129316352,iterations=98304000,chunks=15577728'
    'chunk_size=65536,latency=40632,evict_high=5,evict_low=9,eviction=promotional,n_zones=904,distr=ZIPFIAN,ratio=2,zone_size=1129316352,iterations=98304000,chunks=15577728'
)

# SSD
# workloads=(

# )
