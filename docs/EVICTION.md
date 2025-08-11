# Eviction

## Chunk

Maintain LRU of chunks, this determines when to invalidate.

### Block Storage

Invalidations will:

* Update map
* Add chunk back to Zone chunk vector (modify impl to have Zone store ordered Vec of chunks)