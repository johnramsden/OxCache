#!/bin/sh

# ZONED
workloads=(
    'chunk_size=1129316352,latency=5413781,evict_high=9,evict_low=13,eviction=promotional,n_zones=904,t=64,zone_size=1129316352'
    'chunk_size=268435456,latency=3209583,evict_high=5,evict_low=37,eviction=chunk,n_zones=904,t=256,clean_low=24,zone_size=1129316352'
    'chunk_size=268435456,latency=3209583,evict_high=9,evict_low=17,eviction=promotional,n_zones=904,t=256,zone_size=1129316352'
    'chunk_size=65536,latency=40632,evict_high=2,evict_low=10,eviction=promotional,n_zones=904,t=1024,zone_size=1129316352'
    'chunk_size=65536,latency=40632,evict_high=68929,evict_low=206785,eviction=chunk,n_zones=904,t=1024,clean_low=68928,zone_size=1129316352'
)
