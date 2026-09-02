//! System metrics collection.

use frogdb_core::MetricsRecorder;
use frogdb_types::clock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::interval;
use tracing::{debug, warn};

use crate::jemalloc;
use crate::shard_arenas::ShardArenaRegistry;
use frogdb_types::metrics::definitions::{
    AllocatorActiveBytes, AllocatorAllocatedBytes, AllocatorFragRatio, AllocatorResidentBytes,
    AllocatorShardActiveBytes, AllocatorShardAllocatedBytes, AllocatorShardDirtyBytes,
    AllocatorShardFragRatio, AllocatorShardMuzzyBytes, AllocatorShardResidentBytes,
    AllocatorShardRetainedBytes, CpuSystemSeconds, CpuUserSeconds, MemoryFragmentationRatio,
    MemoryMaxmemoryBytes, MemoryRssBytes, UptimeSeconds,
};

/// Get cumulative CPU times (user, system) via getrusage.
#[cfg(unix)]
fn get_cpu_times() -> (f64, f64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        let usage = usage.assume_init();
        let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
        let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
        (user, sys)
    }
}

/// Collects and reports system-level metrics.
pub struct SystemMetricsCollector {
    recorder: Arc<dyn MetricsRecorder>,
    start_time: Instant,
    system: System,
    pid: Pid,
    /// Maximum memory limit (0 = unlimited).
    maxmemory: Arc<AtomicU64>,
    /// Per-shard memory usage atomics, summed for total used memory.
    shard_memory: Arc<Vec<AtomicU64>>,
    /// Per-shard jemalloc arenas, kept current by its own sampler. This
    /// collector only *reads* it: the two run at very different rates (5 s here
    /// against 10–100 Hz there), and a broker deciding an eviction cannot wait
    /// five seconds for a figure.
    shard_arenas: Arc<ShardArenaRegistry>,
}

impl SystemMetricsCollector {
    /// Create a new system metrics collector.
    pub fn new(
        recorder: Arc<dyn MetricsRecorder>,
        maxmemory: Arc<AtomicU64>,
        shard_memory: Arc<Vec<AtomicU64>>,
        shard_arenas: Arc<ShardArenaRegistry>,
    ) -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        // Initial refresh to populate process data
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::everything(),
        );

        Self {
            recorder,
            start_time: clock::now(),
            system,
            pid,
            maxmemory,
            shard_memory,
            shard_arenas,
        }
    }

    /// Collect system metrics once.
    pub fn collect(&mut self) {
        // Refresh process-specific metrics
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::everything(),
        );

        // Uptime
        let uptime = clock::elapsed(self.start_time).as_secs_f64();
        UptimeSeconds::set(&*self.recorder, uptime);

        // CPU times via getrusage (cumulative seconds)
        #[cfg(unix)]
        {
            let (user, sys) = get_cpu_times();
            CpuUserSeconds::set(&*self.recorder, user);
            CpuSystemSeconds::set(&*self.recorder, sys);
        }

        // Maxmemory gauge
        let maxmem = self.maxmemory.load(Ordering::Relaxed);
        if maxmem > 0 {
            MemoryMaxmemoryBytes::set(&*self.recorder, maxmem as f64);
        }

        // Process metrics
        if let Some(process) = self.system.process(self.pid) {
            // Memory (RSS)
            let rss = process.memory();
            MemoryRssBytes::set(&*self.recorder, rss as f64);

            // Memory fragmentation ratio = RSS / used
            let used: u64 = self
                .shard_memory
                .iter()
                .map(|a| a.load(Ordering::Relaxed))
                .sum();
            if used > 0 {
                MemoryFragmentationRatio::set(&*self.recorder, rss as f64 / used as f64);
            }

            let cpu_usage = process.cpu_usage() as f64;
            debug!(
                uptime_secs = uptime,
                memory_mb = rss as f64 / 1_048_576.0,
                cpu_percent = cpu_usage,
                "System metrics collected"
            );
        } else {
            warn!(pid = ?self.pid, "Failed to get process info");
        }

        // Allocator gauges, straight from jemalloc's `mallctl` (see
        // `crate::jemalloc`) — `None` on a build where jemalloc isn't
        // linked (msvc), in which case these gauges are simply not
        // emitted rather than reported as a lying zero.
        if let Some(stats) = jemalloc::read_stats() {
            AllocatorAllocatedBytes::set(&*self.recorder, stats.allocated as f64);
            AllocatorActiveBytes::set(&*self.recorder, stats.active as f64);
            AllocatorResidentBytes::set(&*self.recorder, stats.resident as f64);
            if stats.allocated > 0 {
                AllocatorFragRatio::set(
                    &*self.recorder,
                    stats.active as f64 / stats.allocated as f64,
                );
            }
        }

        // Per-shard allocator gauges, from the arena bound to each shard's
        // thread. Read, never sampled here — see the field's docs. A shard with
        // no arena, or one whose arena has not been sampled yet, emits nothing:
        // an absent series is a question a dashboard can ask about, a zero is a
        // wrong answer it cannot.
        for sample in self.shard_arenas.samples() {
            if !sample.is_sampled() {
                continue;
            }
            let shard = sample.shard_id.to_string();
            AllocatorShardAllocatedBytes::set(
                &*self.recorder,
                sample.allocated_upper_bound_bytes() as f64,
                &shard,
            );
            AllocatorShardResidentBytes::set(&*self.recorder, sample.resident_bytes as f64, &shard);
            // The depth behind that resident figure: what the arena is serving
            // from, what it is holding on to, and what it has kept mapped. This
            // is what separates "this shard grew" from "this shard's pages have
            // not decayed yet" without an active defragmenter (PRD R13).
            AllocatorShardActiveBytes::set(&*self.recorder, sample.active_bytes as f64, &shard);
            AllocatorShardDirtyBytes::set(&*self.recorder, sample.dirty_bytes as f64, &shard);
            AllocatorShardMuzzyBytes::set(&*self.recorder, sample.muzzy_bytes as f64, &shard);
            AllocatorShardRetainedBytes::set(&*self.recorder, sample.retained_bytes as f64, &shard);
            if let Some(ratio) = sample.fragmentation_ratio() {
                AllocatorShardFragRatio::set(&*self.recorder, ratio, &shard);
            }
        }
    }

    /// Spawn a background task that collects system metrics periodically.
    pub fn spawn_collector(
        recorder: Arc<dyn MetricsRecorder>,
        collection_interval: Duration,
        maxmemory: Arc<AtomicU64>,
        shard_memory: Arc<Vec<AtomicU64>>,
        shard_arenas: Arc<ShardArenaRegistry>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut collector =
                SystemMetricsCollector::new(recorder, maxmemory, shard_memory, shard_arenas);
            let mut ticker = interval(collection_interval);

            loop {
                ticker.tick().await;
                collector.collect();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::NoopMetricsRecorder;

    fn test_maxmemory() -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(0))
    }

    fn test_shard_memory() -> Arc<Vec<AtomicU64>> {
        Arc::new(vec![])
    }

    fn test_shard_arenas() -> Arc<ShardArenaRegistry> {
        Arc::new(ShardArenaRegistry::empty())
    }

    #[test]
    fn test_system_metrics_collector_creation() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let collector = SystemMetricsCollector::new(
            recorder,
            test_maxmemory(),
            test_shard_memory(),
            test_shard_arenas(),
        );
        assert!(collector.start_time.elapsed().as_secs() < 1);
    }

    #[test]
    fn test_system_metrics_collection() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let mut collector = SystemMetricsCollector::new(
            recorder,
            test_maxmemory(),
            test_shard_memory(),
            test_shard_arenas(),
        );
        // Should not panic
        collector.collect();
    }

    /// A sampled arena becomes a labelled per-shard series; an unsampled one
    /// stays absent. Absence is the honest answer for a shard whose memory is
    /// not separately attributable — a zero would read as "this shard holds
    /// nothing", which is the opposite of what is known.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn per_shard_allocator_gauges_are_emitted_only_for_sampled_arenas() {
        use crate::prometheus_recorder::PrometheusRecorder;

        let arena = jemalloc::create_arena().expect("arenas.create");
        // Shard 0's arena is sampled below; shard 1's is never touched.
        let arenas = Arc::new(ShardArenaRegistry::new([(0, arena), (1, arena + 1)]));

        let recorder = Arc::new(PrometheusRecorder::new());
        let mut collector = SystemMetricsCollector::new(
            recorder.clone(),
            test_maxmemory(),
            test_shard_memory(),
            arenas.clone(),
        );

        collector.collect();
        assert!(
            !recorder.encode().contains("frogdb_allocator_shard_"),
            "nothing has been sampled yet, so no per-shard series may exist"
        );

        assert!(arenas.refresh() > 0, "refresh must sample the live arenas");
        collector.collect();

        let output = recorder.encode();
        assert!(output.contains("frogdb_allocator_shard_allocated_bytes"));
        assert!(output.contains("frogdb_allocator_shard_resident_bytes"));
        for depth in ["active", "dirty", "muzzy", "retained"] {
            assert!(
                output.contains(&format!("frogdb_allocator_shard_{depth}_bytes")),
                "the per-arena {depth} series is missing: {output}"
            );
        }
        assert!(
            output.contains(r#"shard="0""#),
            "the per-shard series must be labelled by shard: {output}"
        );
    }

    #[tokio::test]
    async fn test_spawn_collector() {
        let recorder = Arc::new(NoopMetricsRecorder::new());
        let handle = SystemMetricsCollector::spawn_collector(
            recorder,
            Duration::from_millis(100),
            test_maxmemory(),
            test_shard_memory(),
            test_shard_arenas(),
        );

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Abort the task
        handle.abort();
        let _ = handle.await;
    }
}
