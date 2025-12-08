#!/bin/sh

# BLOCK
workloads=(
    'chunk_size=1129316352,latency=5413781,evict_high=2,evict_low=3,eviction=promotional,n_zones=904,t=64,zone_size=1129316352'
    'chunk_size=268435456,latency=3209583,evict_high=2,evict_low=6,eviction=promotional,n_zones=904,t=256,zone_size=1129316352'
    'chunk_size=268435456,latency=3209583,evict_high=5,evict_low=37,eviction=chunk,n_zones=904,t=256,clean_low=0,zone_size=1129316352'
    'chunk_size=65536,latency=40632,evict_high=17233,evict_low=86161,eviction=chunk,n_zones=904,t=1024,clean_low=0,zone_size=1129316352'
    'chunk_size=65536,latency=40632,evict_high=5,evict_low=9,eviction=promotional,n_zones=904,t=1024,zone_size=1129316352'
)
