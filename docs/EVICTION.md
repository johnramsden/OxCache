# Eviction

## Chunk

Maintain LRU of chunks, this determines when to invalidate.

### Block Storage

Invalidation/Eviction will:

* Remove from map
* Add chunk back to ZoneList Zone `chunks_available: Vec<Chunk>`

### Zoned

Invalidation will:

* Remove from map
* Mark chunk None in cache `zone_to_entry` reverse map
* Keep an invalid queue (priority-queue), with each zone index and its invalid count

Eviction will:

