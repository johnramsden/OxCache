use crate::cache::bucket::ChunkLocation;
use dashmap::DashMap;
use nvme::types::{Chunk, Zone};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Metrics for a single zone
#[derive(Debug)]
pub struct ZoneBucket {
    pub evict_count: AtomicU64,
    pub evict_chunk_count: AtomicU64,
    pub read_count: AtomicU64,
    pub write_count: AtomicU64,
    pub hot_chunk_count: AtomicU64,
}

impl ZoneBucket {
    fn new() -> Self {
        Self {
            evict_count: AtomicU64::new(0),
            evict_chunk_count: AtomicU64::new(0),
            read_count: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            hot_chunk_count: AtomicU64::new(0),
        }
    }
}

/// Eviction metrics tracking for hypothesis validation
#[derive(Debug)]
pub struct EvictionMetrics {
    pub num_zones: Zone,
    pub zone_buckets: Vec<ZoneBucket>,
    pub total_evictions: AtomicU64,
    pub total_chunks_evicted: AtomicU64,
    pub chunk_access_counts: DashMap<ChunkLocation, AtomicUsize>,
    pub start_time: Instant,
}

impl EvictionMetrics {
    /// Create new metrics tracker with dynamic zone count
    pub fn new(num_zones: Zone) -> Arc<Self> {
        let zone_buckets = (0..num_zones).map(|_| ZoneBucket::new()).collect();

        Arc::new(Self {
            num_zones,
            zone_buckets,
            total_evictions: AtomicU64::new(0),
            total_chunks_evicted: AtomicU64::new(0),
            chunk_access_counts: DashMap::new(),
            start_time: Instant::now(),
        })
    }

    /// Record a zone eviction (promotional policy)
    pub fn record_zone_eviction(&self, zone: Zone, num_chunks: Chunk) {
        if (zone as usize) < self.zone_buckets.len() {
            self.zone_buckets[zone as usize]
                .evict_count
                .fetch_add(1, Ordering::Relaxed);
            self.zone_buckets[zone as usize]
                .evict_chunk_count
                .fetch_add(num_chunks, Ordering::Relaxed);
            self.total_evictions.fetch_add(1, Ordering::Relaxed);
            self.total_chunks_evicted
                .fetch_add(num_chunks, Ordering::Relaxed);
        }
    }

    /// Record chunk evictions (chunk policy)
    pub fn record_chunk_evictions(&self, chunks: &[ChunkLocation]) {
        for chunk in chunks {
            if (chunk.zone as usize) < self.zone_buckets.len() {
                self.zone_buckets[chunk.zone as usize]
                    .evict_chunk_count
                    .fetch_add(1, Ordering::Relaxed);
                self.total_chunks_evicted.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Count zones that had evictions
        let mut zones_evicted = std::collections::HashSet::new();
        for chunk in chunks {
            zones_evicted.insert(chunk.zone);
        }
        self.total_evictions
            .fetch_add(zones_evicted.len() as u64, Ordering::Relaxed);
    }

    /// Record a read operation
    pub fn record_read(&self, chunk: &ChunkLocation) {
        // Update zone bucket
        if (chunk.zone as usize) < self.zone_buckets.len() {
            self.zone_buckets[chunk.zone as usize]
                .read_count
                .fetch_add(1, Ordering::Relaxed);
        }

        // Update per-chunk access count
        self.chunk_access_counts
            .entry(chunk.clone())
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write operation
    pub fn record_write(&self, chunk: &ChunkLocation) {
        // Update zone bucket
        if (chunk.zone as usize) < self.zone_buckets.len() {
            self.zone_buckets[chunk.zone as usize]
                .write_count
                .fetch_add(1, Ordering::Relaxed);
        }

        // Update per-chunk access count
        self.chunk_access_counts
            .entry(chunk.clone())
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Update hot chunk counts (chunks with 10+ accesses)
    pub fn update_hot_chunk_counts(&self) {
        // Reset hot chunk counts
        for bucket in &self.zone_buckets {
            bucket.hot_chunk_count.store(0, Ordering::Relaxed);
        }

        // Count chunks with 10+ accesses per zone
        for entry in self.chunk_access_counts.iter() {
            let chunk_location = entry.key();
            let access_count = entry.value().load(Ordering::Relaxed);

            if access_count >= 10 && (chunk_location.zone as usize) < self.zone_buckets.len() {
                self.zone_buckets[chunk_location.zone as usize]
                    .hot_chunk_count
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Generate a comprehensive report
    pub fn generate_report(&self, policy_name: &str) -> String {
        // Update hot chunk counts before generating report
        self.update_hot_chunk_counts();

        let runtime = self.start_time.elapsed().as_secs_f64();
        let total_evictions = self.total_evictions.load(Ordering::Relaxed);
        let total_chunks_evicted = self.total_chunks_evicted.load(Ordering::Relaxed);
        let total_unique_chunks = self.chunk_access_counts.len();

        // Collect zone metrics
        let mut zone_metrics: Vec<(Zone, u64, u64, u64, u64, u64)> = self
            .zone_buckets
            .iter()
            .enumerate()
            .map(|(zone, bucket)| {
                (
                    zone as Zone,
                    bucket.evict_count.load(Ordering::Relaxed),
                    bucket.evict_chunk_count.load(Ordering::Relaxed),
                    bucket.read_count.load(Ordering::Relaxed),
                    bucket.write_count.load(Ordering::Relaxed),
                    bucket.hot_chunk_count.load(Ordering::Relaxed),
                )
            })
            .collect();

        // Find top 20 hottest zones (by read count)
        let mut hottest_zones = zone_metrics.clone();
        hottest_zones.sort_by(|a, b| b.3.cmp(&a.3)); // Sort by read_count descending
        let top_20_hottest: Vec<_> = hottest_zones.iter().take(20).collect();

        // Find top 20 most evicted zones (by evict_count)
        let mut most_evicted_zones = zone_metrics.clone();
        most_evicted_zones.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by evict_count descending
        let top_20_evicted: Vec<_> = most_evicted_zones.iter().take(20).collect();

        // Divide zones into thirds for summary analysis
        let third = self.num_zones / 3;
        let early_end = third;
        let middle_end = third * 2;

        let mut early_stats = (0u64, 0u64, 0u64);
        let mut middle_stats = (0u64, 0u64, 0u64);
        let mut late_stats = (0u64, 0u64, 0u64);

        for (zone, evict_count, _, read_count, _, hot_chunks) in &zone_metrics {
            if *zone < early_end {
                early_stats.0 += evict_count;
                early_stats.1 += hot_chunks;
                early_stats.2 += read_count;
            } else if *zone < middle_end {
                middle_stats.0 += evict_count;
                middle_stats.1 += hot_chunks;
                middle_stats.2 += read_count;
            } else {
                late_stats.0 += evict_count;
                late_stats.1 += hot_chunks;
                late_stats.2 += read_count;
            }
        }

        // Calculate ratios
        let early_late_eviction_ratio = if late_stats.0 > 0 {
            early_stats.0 as f64 / late_stats.0 as f64
        } else {
            0.0
        };

        let early_late_hot_chunk_ratio = if late_stats.1 > 0 {
            early_stats.1 as f64 / late_stats.1 as f64
        } else {
            0.0
        };

        let early_late_read_ratio = if late_stats.2 > 0 {
            early_stats.2 as f64 / late_stats.2 as f64
        } else {
            0.0
        };

        // Determine verdict
        let hypothesis_supported = early_late_eviction_ratio < 0.5
            && early_late_hot_chunk_ratio > 5.0
            && early_stats.2 as f64 > (early_stats.2 + middle_stats.2 + late_stats.2) as f64 * 0.5;

        let verdict = if hypothesis_supported {
            "Hypothesis SUPPORTED - hot chunks cluster in early zones"
        } else {
            "Hypothesis NOT SUPPORTED - no clear early-zone clustering"
        };

        // Build report
        let mut report = String::new();
        report.push_str(
            "================================================================================\n",
        );
        report.push_str(&format!(
            "EVICTION METRICS REPORT - {} Policy\n",
            policy_name
        ));
        report.push_str(
            "================================================================================\n",
        );
        report.push_str(&format!("Runtime: {:.2}s\n", runtime));
        report.push_str(&format!("Total Zones: {}\n", self.num_zones));
        report.push_str(&format!("Total Evictions: {} zones\n", total_evictions));
        report.push_str(&format!("Total Chunks Evicted: {}\n", total_chunks_evicted));
        report.push_str(&format!(
            "Total Unique Chunks Accessed: {}\n",
            total_unique_chunks
        ));
        report.push_str("\n");

        // Top 20 hottest zones
        report.push_str(
            "--------------------------------------------------------------------------------\n",
        );
        report.push_str("TOP 20 HOTTEST ZONES (by read count):\n");
        report.push_str(
            "--------------------------------------------------------------------------------\n",
        );
        report.push_str(&format!(
            "{:<8} {:<12} {:<18} {:<12} {:<12} {:<12}\n",
            "Zone", "Evictions", "Chunks Evicted", "Reads", "Writes", "Hot Chunks"
        ));
        report.push_str(
            "--------------------------------------------------------------------------------\n",
        );
        for (zone, evict_count, evict_chunk_count, read_count, write_count, hot_chunks) in
            top_20_hottest
        {
            report.push_str(&format!(
                "{:<8} {:<12} {:<18} {:<12} {:<12} {:<12}\n",
                zone, evict_count, evict_chunk_count, read_count, write_count, hot_chunks
            ));
        }
        report.push_str("\n");

        // Top 20 most evicted zones
        report.push_str(
            "--------------------------------------------------------------------------------\n",
        );
        report.push_str("TOP 20 MOST EVICTED ZONES:\n");
        report.push_str(
            "--------------------------------------------------------------------------------\n",
        );
        report.push_str(&format!(
            "{:<8} {:<12} {:<18} {:<12} {:<12} {:<12}\n",
            "Zone", "Evictions", "Chunks Evicted", "Reads", "Writes", "Hot Chunks"
        ));
        report.push_str(
            "--------------------------------------------------------------------------------\n",
        );
        for (zone, evict_count, evict_chunk_count, read_count, write_count, hot_chunks) in
            top_20_evicted
        {
            report.push_str(&format!(
                "{:<8} {:<12} {:<18} {:<12} {:<12} {:<12}\n",
                zone, evict_count, evict_chunk_count, read_count, write_count, hot_chunks
            ));
        }
        report.push_str("\n");

        // Summary analysis
        report.push_str(
            "--------------------------------------------------------------------------------\n",
        );
        report.push_str("SUMMARY ANALYSIS (zones divided into thirds):\n");
        report.push_str(
            "--------------------------------------------------------------------------------\n",
        );
        report.push_str(&format!(
            "Early Zones (0-{}):      {} evictions, {} hot chunks, {} reads\n",
            early_end - 1,
            early_stats.0,
            early_stats.1,
            early_stats.2
        ));
        report.push_str(&format!(
            "Middle Zones ({}-{}):   {} evictions, {} hot chunks, {} reads\n",
            early_end,
            middle_end - 1,
            middle_stats.0,
            middle_stats.1,
            middle_stats.2
        ));
        report.push_str(&format!(
            "Late Zones ({}-{}):     {} evictions, {} hot chunks, {} reads\n",
            middle_end,
            self.num_zones - 1,
            late_stats.0,
            late_stats.1,
            late_stats.2
        ));
        report.push_str("\n");
        report.push_str(&format!(
            "Early/Late Eviction Ratio: {:.2}\n",
            early_late_eviction_ratio
        ));
        report.push_str(&format!(
            "Early/Late Hot Chunk Ratio: {:.2}\n",
            early_late_hot_chunk_ratio
        ));
        report.push_str(&format!(
            "Early/Late Read Ratio: {:.2}\n",
            early_late_read_ratio
        ));
        report.push_str("\n");
        report.push_str(&format!("VERDICT: {}\n", verdict));
        report.push_str(
            "================================================================================\n",
        );

        report
    }
}
