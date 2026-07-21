//! INFO section renderers.
//!
//! One type per section; each owns its data access (via [`InfoSources`]
//! accessors) and its byte format (via [`SectionWriter`]). Real values are
//! emitted at the point of writing — there is no `0` placeholder for another
//! module to patch, so "the patch silently no-oped" is unrepresentable.

use std::time::{SystemTime, UNIX_EPOCH};

use frogdb_cluster::version_gate;
use frogdb_core::histogram::KeysizeType;

use super::{InfoSection, InfoSources, SectionWriter};

/// The standard section registry, in canonical order.
pub(super) fn all_sections() -> Vec<Box<dyn InfoSection>> {
    vec![
        Box::new(ServerSection),
        Box::new(ClientsSection),
        Box::new(MemorySection),
        Box::new(PersistenceSection),
        Box::new(StatsSection),
        Box::new(ReplicationSection),
        Box::new(CpuSection),
        Box::new(KeyspaceSection),
        Box::new(RatelimitSection),
        Box::new(CommandstatsSection),
        Box::new(ErrorstatsSection),
        Box::new(LatencystatsSection),
        Box::new(LatencyBaselineSection),
        Box::new(TieredSection),
        Box::new(KeysizesSection),
    ]
}

// ============================================================================
// Server
// ============================================================================

struct ServerSection;

impl InfoSection for ServerSection {
    fn name(&self) -> &'static str {
        "server"
    }

    fn render(&self, src: &InfoSources) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        let mut w = SectionWriter::new("Server");
        w.field("frogdb_version", env!("CARGO_PKG_VERSION"))
            .field("redis_version", frogdb_core::ADVERTISED_REDIS_VERSION)
            .field("redis_git_sha1", "00000000")
            .field("redis_git_dirty", 0)
            .field("redis_build_id", 0)
            .field("redis_mode", "standalone")
            .field(
                "os",
                format!(
                    "{} {} {}",
                    std::env::consts::OS,
                    std::env::consts::FAMILY,
                    std::env::consts::ARCH
                ),
            )
            .field("arch_bits", std::mem::size_of::<usize>() * 8)
            .field("monotonic_clock", "POSIX clock_gettime")
            .field("multiplexing_api", "tokio")
            .field("atomicvar_api", "std")
            .field("gcc_version", "0.0.0")
            .field("process_id", std::process::id())
            .field("process_supervised", "no")
            .field("run_id", "frogdb0000000000000000000000000000000000")
            .field("tcp_port", 6379)
            .field("server_time_usec", now.as_micros())
            .field("uptime_in_seconds", 0)
            .field("uptime_in_days", 0)
            .field("hz", 10)
            .field("configured_hz", 10)
            .field("lru_clock", 0)
            .field("executable", "/usr/local/bin/frogdb")
            .field("config_file", "")
            .field("io_threads_active", 0);

        // Version-gated: include cluster version fields after finalization.
        if let Some(cs) = src.cluster_state() {
            let active = cs.active_version();
            if version_gate::is_gate_active("extended_info_fields", active.as_deref()) {
                let snapshot = cs.snapshot();
                let cluster_version = snapshot
                    .nodes
                    .values()
                    .filter(|n| !n.version.is_empty())
                    .map(|n| n.version.as_str())
                    .min()
                    .unwrap_or("");
                w.field("active_version", active.as_deref().unwrap_or(""))
                    .field("cluster_version", cluster_version);
            }
        }

        w.finish()
    }
}

// ============================================================================
// Clients
// ============================================================================

struct ClientsSection;

impl InfoSection for ClientsSection {
    fn name(&self) -> &'static str {
        "clients"
    }

    fn render(&self, src: &InfoSources) -> String {
        let c = src.clients();
        let mut w = SectionWriter::new("Clients");
        w.field("connected_clients", c.connected)
            .field("cluster_connections", 0)
            .field("maxclients", c.max_clients)
            .field("client_recent_max_input_buffer", 0)
            .field("client_recent_max_output_buffer", 0)
            .field("blocked_clients", c.blocked)
            .field("tracking_clients", 0)
            .field("clients_in_timeout_table", 0);
        w.finish()
    }
}

// ============================================================================
// Memory
// ============================================================================

struct MemorySection;

impl InfoSection for MemorySection {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn render(&self, src: &InfoSources) -> String {
        let sh = src.shards();
        let cfg = src.memory_config();
        let used = sh.used_memory;
        let peak = sh.peak_memory.max(used as u64);
        let peak_perc = if peak > 0 {
            used as f64 * 100.0 / peak as f64
        } else {
            100.0
        };

        let mut w = SectionWriter::new("Memory");
        w.field("used_memory", used)
            .field("used_memory_human", format!("{}K", used / 1024))
            .field("used_memory_rss", used)
            .field("used_memory_rss_human", format!("{}K", used / 1024))
            .field("used_memory_peak", peak)
            .field("used_memory_peak_human", format!("{}K", peak / 1024))
            .field("used_memory_peak_perc", format!("{peak_perc:.2}%"))
            .field("used_memory_overhead", 0)
            .field("used_memory_startup", 0)
            .field("used_memory_dataset", used)
            .field("used_memory_dataset_perc", "100.00%")
            .field("allocator_allocated", 0)
            .field("allocator_active", 0)
            .field("allocator_resident", 0)
            .field("total_system_memory", 0)
            .field("total_system_memory_human", "0K")
            .field("used_memory_lua", 0)
            .field("used_memory_lua_human", "0B")
            .field("used_memory_scripts", 0)
            .field("used_memory_scripts_human", "0B")
            .field("number_of_cached_scripts", 0)
            .field("maxmemory", cfg.maxmemory)
            .field(
                "maxmemory_human",
                if cfg.maxmemory == 0 {
                    "0B".to_string()
                } else {
                    format!("{}K", cfg.maxmemory / 1024)
                },
            )
            .field("maxmemory_policy", &cfg.policy)
            .field("allocator_frag_ratio", "1.00")
            .field("allocator_frag_bytes", 0)
            .field("allocator_rss_ratio", "1.00")
            .field("allocator_rss_bytes", 0)
            .field("rss_overhead_ratio", "1.00")
            .field("rss_overhead_bytes", 0)
            .field("mem_fragmentation_ratio", "1.00")
            .field("mem_fragmentation_bytes", 0)
            .field("mem_not_counted_for_evict", 0)
            .field("mem_replication_backlog", 0)
            .field("mem_clients_slaves", 0)
            .field("mem_clients_normal", 0)
            .field("mem_aof_buffer", 0)
            .field("mem_allocator", "rust")
            .field("active_defrag_running", 0)
            .field("lazyfree_pending_objects", 0)
            .field("lazyfreed_objects", sh.lazyfreed_objects);
        w.finish()
    }
}

// ============================================================================
// Persistence
// ============================================================================

struct PersistenceSection;

impl InfoSection for PersistenceSection {
    fn name(&self) -> &'static str {
        "persistence"
    }

    fn render(&self, src: &InfoSources) -> String {
        let sh = src.shards();
        let p = src.persistence();

        let mut w = SectionWriter::new("Persistence");
        w.field("loading", 0)
            .field("async_loading", 0)
            .field("persistence_enabled", u8::from(sh.wal.is_some()))
            .field("durability_mode", &p.durability_mode);

        // Real aggregated WAL lag — the fields are present only when
        // persistence is actually enabled, never placeholder zeros.
        if let Some(wal) = &sh.wal {
            w.field("wal_pending_ops", wal.pending_ops)
                .field("wal_pending_bytes", wal.pending_bytes)
                .field("wal_durability_lag_ms", wal.max_durability_lag_ms)
                .field(
                    "wal_last_flush_status",
                    if wal.last_flush_ok { "ok" } else { "err" },
                )
                .field("wal_flush_failures", wal.flush_failures)
                .field("wal_lost_ops", wal.lost_ops)
                .field("wal_last_flush_time", wal.last_flush_time_ms() / 1000)
                .field_opt("wal_writes_total", src.wal_writes_total())
                .field_opt("wal_bytes_total", src.wal_bytes_total());
        }

        w.field("current_cow_peak", 0)
            .field("current_cow_size", 0)
            .field("current_cow_size_age", 0)
            .field("current_fork_perc", "0.00")
            .field("current_save_keys_processed", 0)
            .field("current_save_keys_total", 0)
            .field("rdb_changes_since_last_save", sh.dirty)
            .field("rdb_bgsave_in_progress", u8::from(p.bgsave_in_progress))
            .field("rdb_last_save_time", p.last_save_unix.unwrap_or(0))
            .field("rdb_last_bgsave_status", "ok")
            .field("rdb_last_bgsave_time_sec", -1)
            .field("rdb_current_bgsave_time_sec", -1)
            .field("rdb_saves", 0)
            .field("rdb_last_cow_size", 0)
            .field("rdb_last_load_keys_expired", 0)
            .field("rdb_last_load_keys_loaded", 0)
            .field("aof_enabled", 0)
            .field("aof_rewrite_in_progress", 0)
            .field("aof_rewrite_scheduled", 0)
            .field("aof_last_rewrite_time_sec", -1)
            .field("aof_current_rewrite_time_sec", -1)
            .field("aof_last_bgrewrite_status", "ok")
            .field("aof_rewrites", 0)
            .field("aof_rewrites_consecutive_failures", 0)
            .field("aof_last_write_status", "ok")
            .field("aof_last_cow_size", 0)
            .field("module_fork_in_progress", 0)
            .field("module_fork_last_cow_size", 0);
        w.finish()
    }
}

// ============================================================================
// Stats
// ============================================================================

struct StatsSection;

impl InfoSection for StatsSection {
    fn name(&self) -> &'static str {
        "stats"
    }

    fn render(&self, src: &InfoSources) -> String {
        let sh = src.shards();
        let mut w = SectionWriter::new("Stats");
        w.field("total_connections_received", 1)
            // Real total from the shared `frogdb_commands_total` counter (0 when
            // metrics are disabled), matching `/status` `commands.total_processed`.
            .field(
                "total_commands_processed",
                src.total_commands_processed().unwrap_or(0),
            )
            // No instantaneous-rate sampler exists yet; kept as a Redis-compat
            // stub rather than a fabricated value (see `/status` `ops_per_sec`,
            // which is omitted for the same reason).
            .field("instantaneous_ops_per_sec", 0)
            .field("total_net_input_bytes", 0)
            .field("total_net_output_bytes", 0)
            .field("total_net_repl_input_bytes", 0)
            .field("total_net_repl_output_bytes", 0)
            .field("instantaneous_input_kbps", "0.00")
            .field("instantaneous_output_kbps", "0.00")
            .field("instantaneous_input_repl_kbps", "0.00")
            .field("instantaneous_output_repl_kbps", "0.00")
            .field("rejected_connections", 0)
            .field("sync_full", 0)
            .field("sync_partial_ok", 0)
            .field("sync_partial_err", 0)
            .field("expired_keys", sh.expired_keys)
            .field("expired_stale_perc", "0.00")
            .field("expired_time_cap_reached_count", 0)
            .field("expire_cycle_cpu_milliseconds", 0)
            .field("evicted_keys", sh.evicted_keys)
            .field("evicted_clients", 0)
            .field("total_eviction_exceeded_time", 0)
            .field("current_eviction_exceeded_time", 0)
            // Resettable reported values from the KeyspaceStats accumulator
            // (CONFIG RESETSTAT advances the baseline; the Prometheus _total
            // counters stay monotonic).
            .field_opt("keyspace_hits", src.keyspace_hits())
            .field_opt("keyspace_misses", src.keyspace_misses())
            .field("pubsub_channels", 0)
            .field("pubsub_patterns", 0)
            .field("pubsubshard_channels", 0)
            .field("latest_fork_usec", 0)
            .field("total_forks", 0)
            .field("migrate_cached_sockets", 0)
            .field("slave_expires_tracked_keys", 0)
            .field("active_defrag_hits", 0)
            .field("active_defrag_misses", 0)
            .field("active_defrag_key_hits", 0)
            .field("active_defrag_key_misses", 0)
            .field("total_active_defrag_time", 0)
            .field("current_active_defrag_time", 0)
            // Client-side-caching tracking-table size; FrogDB does not yet
            // count tracked keys (previously this misreported the db size).
            .field("tracking_total_keys", 0)
            .field("tracking_total_items", 0)
            .field("tracking_total_prefixes", 0)
            .field("unexpected_error_replies", 0)
            .field("total_error_replies", src.total_error_replies())
            .field("dump_payload_sanitizations", 0)
            .field("total_reads_processed", 0)
            .field("total_writes_processed", 0)
            .field("io_threaded_reads_processed", 0)
            .field("io_threaded_writes_processed", 0);
        w.finish()
    }
}

// ============================================================================
// Replication
// ============================================================================

struct ReplicationSection;

/// The all-zero `master_replid2` Redis reports when no failover window exists.
const ZERO_REPLID: &str = "0000000000000000000000000000000000000000";

impl InfoSection for ReplicationSection {
    fn name(&self) -> &'static str {
        "replication"
    }

    fn render(&self, src: &InfoSources) -> String {
        let r = src.replication();
        let replid = r.replid();
        // Failover-continuity pair, shared by both role arms below. When a
        // previous-primary window exists we surface it verbatim: replid2 is the
        // old id and second_repl_offset is FrogDB's inclusive boundary (the last
        // offset `window_contains` will still continue via replid2). No window
        // yet -> the all-zero id and the -1 sentinel Redis uses.
        let (replid2, second_repl_offset) = match &r.secondary_window {
            Some((prev_id, boundary)) => (prev_id.as_str(), *boundary),
            None => (ZERO_REPLID, -1),
        };
        let mut w = SectionWriter::new("Replication");

        if let Some(primary) = &r.primary {
            w.field("role", "master")
                .field("connected_slaves", primary.replicas.len());
            for (i, replica) in primary.replicas.iter().enumerate() {
                w.line(&format!(
                    "slave{}:ip={},port={},state=online,offset={},lag=0",
                    i, replica.ip, replica.port, replica.offset
                ));
            }
            w.field("master_failover_state", "no-failover")
                .field("master_replid", &replid)
                .field("master_replid2", replid2)
                .field("master_repl_offset", primary.repl_offset)
                .field("second_repl_offset", second_repl_offset)
                .field(
                    "repl_backlog_active",
                    u8::from(!primary.replicas.is_empty()),
                )
                .field("repl_backlog_size", 1048576)
                .field("repl_backlog_first_byte_offset", 0)
                .field("repl_backlog_histlen", primary.repl_offset);
        } else {
            w.field("role", if r.is_replica { "slave" } else { "master" });
            if r.is_replica {
                w.field_opt("master_host", r.master_host.as_deref())
                    .field_opt("master_port", r.master_port)
                    .field("master_link_status", "up");
            }
            w.field("connected_slaves", 0)
                .field("master_failover_state", "no-failover")
                .field("master_replid", &replid)
                .field("master_replid2", replid2)
                .field("master_repl_offset", 0)
                .field("second_repl_offset", second_repl_offset)
                .field("repl_backlog_active", 0)
                .field("repl_backlog_size", 1048576)
                .field("repl_backlog_first_byte_offset", 0)
                .field("repl_backlog_histlen", 0);
        }
        w.finish()
    }
}

// ============================================================================
// CPU
// ============================================================================

struct CpuSection;

impl InfoSection for CpuSection {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn render(&self, _src: &InfoSources) -> String {
        let mut w = SectionWriter::new("CPU");
        w.field("used_cpu_sys", "0.000000")
            .field("used_cpu_user", "0.000000")
            .field("used_cpu_sys_children", "0.000000")
            .field("used_cpu_user_children", "0.000000")
            .field("used_cpu_sys_main_thread", "0.000000")
            .field("used_cpu_user_main_thread", "0.000000");
        w.finish()
    }
}

// ============================================================================
// Keyspace
// ============================================================================

struct KeyspaceSection;

impl InfoSection for KeyspaceSection {
    fn name(&self) -> &'static str {
        "keyspace"
    }

    fn render(&self, src: &InfoSources) -> String {
        let keys = src.shards().keys;
        let mut w = SectionWriter::new("Keyspace");
        if keys > 0 {
            w.line(&format!("db0:keys={keys},expires=0,avg_ttl=0"));
        }
        w.finish()
    }
}

// ============================================================================
// Ratelimit
// ============================================================================

struct RatelimitSection;

impl InfoSection for RatelimitSection {
    fn name(&self) -> &'static str {
        "ratelimit"
    }

    fn render(&self, src: &InfoSources) -> String {
        let rl = src.rate_limit();
        if !rl.is_active() {
            return String::new();
        }
        let mut w = SectionWriter::new("Ratelimit");
        w.field("ratelimit_users_configured", rl.users)
            .field("ratelimit_total_commands_rejected", rl.commands_rejected)
            .field("ratelimit_total_bytes_rejected", rl.bytes_rejected);
        w.finish()
    }
}

// ============================================================================
// Commandstats
// ============================================================================

struct CommandstatsSection;

impl InfoSection for CommandstatsSection {
    fn name(&self) -> &'static str {
        "commandstats"
    }

    fn render(&self, src: &InfoSources) -> String {
        let mut w = SectionWriter::new("Commandstats");
        for (cmd, stats) in src.command_stats() {
            let usec_per_call = if stats.calls > 0 {
                stats.usec as f64 / stats.calls as f64
            } else {
                0.0
            };
            w.line(&format!(
                "cmdstat_{cmd}:calls={},usec={},usec_per_call={usec_per_call:.2},rejected_calls={},failed_calls={}",
                stats.calls, stats.usec, stats.rejected_calls, stats.failed_calls,
            ));
        }
        w.finish()
    }
}

// ============================================================================
// Errorstats
// ============================================================================

struct ErrorstatsSection;

impl InfoSection for ErrorstatsSection {
    fn name(&self) -> &'static str {
        "errorstats"
    }

    fn render(&self, src: &InfoSources) -> String {
        let mut w = SectionWriter::new("Errorstats");
        for (prefix, count) in src.error_types() {
            w.line(&format!("errorstat_{prefix}:count={count}"));
        }
        w.finish()
    }
}

// ============================================================================
// Latencystats
// ============================================================================

struct LatencystatsSection;

impl InfoSection for LatencystatsSection {
    fn name(&self) -> &'static str {
        "latencystats"
    }

    fn render(&self, src: &InfoSources) -> String {
        let lt = src.latency();
        let mut w = SectionWriter::new("Latencystats");
        if lt.histograms.is_enabled() && !lt.percentiles.is_empty() {
            let mut cmds = lt.histograms.all_commands();
            cmds.sort();
            for cmd in cmds {
                if let Some(pvals) = lt.histograms.percentiles_for(&cmd, &lt.percentiles) {
                    let parts: Vec<String> = pvals
                        .iter()
                        .map(|(p, us)| {
                            // Microseconds to milliseconds, 3 decimal places.
                            format!("p{}={:.3}", format_percentile(*p), us / 1000.0)
                        })
                        .collect();
                    w.line(&format!("latencystats_{cmd}:{}", parts.join(",")));
                }
            }
        }
        w.finish()
    }
}

/// Format a percentile value for display (e.g. 99.9 -> "99.9", 50.0 -> "50").
fn format_percentile(p: f64) -> String {
    if p == p.floor() {
        format!("{}", p as u64)
    } else {
        format!("{p}")
    }
}

// ============================================================================
// Latency_Baseline
// ============================================================================

struct LatencyBaselineSection;

impl InfoSection for LatencyBaselineSection {
    fn name(&self) -> &'static str {
        "latency_baseline"
    }

    fn render(&self, src: &InfoSources) -> String {
        let mut w = SectionWriter::new("Latency_Baseline");
        match src.baseline() {
            Some(b) => {
                w.field("baseline_test_run", 1)
                    .field("baseline_duration_secs", b.duration_secs)
                    .field("baseline_samples", b.samples)
                    .field("baseline_min_us", b.min_us)
                    .field("baseline_max_us", b.max_us)
                    .field("baseline_avg_us", format!("{:.1}", b.avg_us))
                    .field("baseline_p99_us", b.p99_us)
                    .field("baseline_warning_threshold_us", b.warning_threshold_us)
                    .field(
                        "baseline_exceeded_threshold",
                        u8::from(b.max_us > b.warning_threshold_us),
                    );
            }
            None => {
                w.field("baseline_test_run", 0);
            }
        }
        w.finish()
    }
}

// ============================================================================
// Tiered
// ============================================================================

struct TieredSection;

impl InfoSection for TieredSection {
    fn name(&self) -> &'static str {
        "tiered"
    }

    fn render(&self, src: &InfoSources) -> String {
        let t = &src.shards().tiered;
        let enabled = t.warm_keys > 0 || t.spills > 0;
        let mut w = SectionWriter::new("Tiered");
        w.field("tiered_enabled", u8::from(enabled))
            .field("tiered_hot_keys", t.hot_keys)
            .field("tiered_warm_keys", t.warm_keys)
            .field("tiered_unspills", t.unspills)
            .field("tiered_spills", t.spills)
            .field("tiered_expired_on_unspill", t.expired_on_unspill);
        w.finish()
    }
}

// ============================================================================
// Keysizes
// ============================================================================

struct KeysizesSection;

impl InfoSection for KeysizesSection {
    fn name(&self) -> &'static str {
        "keysizes"
    }

    fn render(&self, src: &InfoSources) -> String {
        let keysizes = &src.shards().keysizes;
        let mut w = SectionWriter::new("Keysizes");
        for ty in KeysizeType::ALL {
            let hist = keysizes.get(*ty);
            if !hist.is_empty() {
                w.line(&format!("{}:{}", ty.info_field_name(), hist.format_bins()));
            }
        }
        if src.key_memory_enabled() && !keysizes.key_memory.is_empty() {
            w.line(&format!(
                "distrib_key_sizes:{}",
                keysizes.key_memory.format_bins()
            ));
        }
        w.finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::test_support::sources;
    use super::super::{
        InfoBuilder, PrimarySnapshot, RateLimitSnapshot, ReplicaLine, SectionSelector,
    };
    use super::*;
    use frogdb_core::{ServerCommandStats, WalLagAggregate};

    fn render(section: &dyn InfoSection, src: &InfoSources) -> String {
        section.render(src)
    }

    #[test]
    fn stats_renders_shard_sums_and_real_error_count() {
        let mut src = sources();
        src.shards.expired_keys = 12;
        src.shards.evicted_keys = 7;
        src.total_error_replies = 3;
        let out = render(&StatsSection, &src);
        assert!(out.contains("expired_keys:12\r\n"), "{out}");
        assert!(out.contains("evicted_keys:7\r\n"), "{out}");
        assert!(out.contains("total_error_replies:3\r\n"), "{out}");
    }

    #[test]
    fn stats_total_commands_processed_reflects_the_shared_counter() {
        use frogdb_telemetry::PrometheusRecorder;
        use frogdb_telemetry::definitions::CommandsTotal;

        // With metrics disabled the field is an honest 0, never absent.
        let src = sources();
        let out = render(&StatsSection, &src);
        assert!(out.contains("total_commands_processed:0\r\n"), "{out}");

        // Wired to the same `frogdb_commands_total` counter `/status` reads, so
        // INFO and `/status` cannot disagree.
        let recorder = std::sync::Arc::new(PrometheusRecorder::new());
        CommandsTotal::inc_by(&*recorder, 5, "GET");
        CommandsTotal::inc_by(&*recorder, 2, "SET");
        let mut src = sources();
        src.metrics = recorder;
        let out = render(&StatsSection, &src);
        assert!(out.contains("total_commands_processed:7\r\n"), "{out}");
    }

    #[test]
    fn stats_renders_keyspace_counts_even_with_metrics_disabled() {
        // The accumulator counts at the execution seam, independent of the
        // metrics recorder — a fresh server honestly reports 0, never an
        // absent field (proposal 24).
        let src = sources();
        let out = render(&StatsSection, &src);
        assert!(out.contains("keyspace_hits:0\r\n"), "{out}");
        assert!(out.contains("keyspace_misses:0\r\n"), "{out}");
    }

    #[test]
    fn stats_renders_reported_keyspace_values_and_reset_rebaselines() {
        let src = sources();
        src.keyspace_stats.record(42, 5);
        let out = render(&StatsSection, &src);
        assert!(out.contains("keyspace_hits:42\r\n"), "{out}");
        assert!(out.contains("keyspace_misses:5\r\n"), "{out}");

        // CONFIG RESETSTAT advances the baseline: reported values return to
        // zero while the cumulative view stays monotonic.
        src.keyspace_stats.reset();
        let out = render(&StatsSection, &src);
        assert!(out.contains("keyspace_hits:0\r\n"), "{out}");
        assert!(out.contains("keyspace_misses:0\r\n"), "{out}");
        assert_eq!(src.keyspace_stats.cumulative_hits(), 42);
        assert_eq!(src.keyspace_stats.cumulative_misses(), 5);
    }

    #[test]
    fn persistence_disabled_reports_zero_and_omits_wal_fields() {
        let src = sources();
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("persistence_enabled:0\r\n"), "{out}");
        assert!(!out.contains("wal_pending_ops"), "{out}");
        assert!(!out.contains("wal_durability_lag_ms"), "{out}");
        assert!(out.contains("rdb_changes_since_last_save:0\r\n"), "{out}");
    }

    #[test]
    fn persistence_enabled_renders_aggregated_wal_lag() {
        let mut src = sources();
        src.shards.dirty = 9;
        src.shards.wal = Some(WalLagAggregate {
            pending_ops: 4,
            pending_bytes: 128,
            max_durability_lag_ms: 55,
            flush_failures: 2,
            lost_ops: 1,
            last_flush_ok: false,
            last_flush_time_ms: 1_700_000_000_500,
            per_shard: Vec::new(),
        });
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("persistence_enabled:1\r\n"), "{out}");
        assert!(out.contains("wal_pending_ops:4\r\n"), "{out}");
        assert!(out.contains("wal_pending_bytes:128\r\n"), "{out}");
        assert!(out.contains("wal_durability_lag_ms:55\r\n"), "{out}");
        assert!(out.contains("wal_last_flush_status:err\r\n"), "{out}");
        assert!(out.contains("wal_flush_failures:2\r\n"), "{out}");
        assert!(out.contains("wal_lost_ops:1\r\n"), "{out}");
        assert!(out.contains("wal_last_flush_time:1700000000\r\n"), "{out}");
        assert!(out.contains("rdb_changes_since_last_save:9\r\n"), "{out}");
        // Metrics disabled: totals are honestly absent.
        assert!(!out.contains("wal_writes_total"), "{out}");
    }

    #[test]
    fn clients_renders_registry_counts() {
        let mut src = sources();
        src.clients.connected = 3;
        src.clients.blocked = 1;
        src.clients.max_clients = 500;
        let out = render(&ClientsSection, &src);
        assert!(out.contains("connected_clients:3\r\n"), "{out}");
        assert!(out.contains("blocked_clients:1\r\n"), "{out}");
        assert!(out.contains("maxclients:500\r\n"), "{out}");
    }

    #[test]
    fn memory_renders_fleet_wide_sums() {
        let mut src = sources();
        src.shards.used_memory = 2048;
        src.shards.peak_memory = 4096;
        src.shards.lazyfreed_objects = 6;
        let out = render(&MemorySection, &src);
        assert!(out.contains("used_memory:2048\r\n"), "{out}");
        assert!(out.contains("used_memory_human:2K\r\n"), "{out}");
        assert!(out.contains("used_memory_peak:4096\r\n"), "{out}");
        assert!(out.contains("used_memory_peak_perc:50.00%\r\n"), "{out}");
        assert!(out.contains("lazyfreed_objects:6\r\n"), "{out}");
        assert!(out.contains("maxmemory_policy:noeviction\r\n"), "{out}");
    }

    #[test]
    fn keyspace_empty_and_populated() {
        let mut src = sources();
        assert_eq!(
            render(&KeyspaceSection, &src),
            "# Keyspace\r\n\r\n",
            "no keys: header only"
        );
        src.shards.keys = 42;
        assert_eq!(
            render(&KeyspaceSection, &src),
            "# Keyspace\r\ndb0:keys=42,expires=0,avg_ttl=0\r\n\r\n"
        );
    }

    #[test]
    fn replication_standalone_renders_node_id_replid() {
        let mut src = sources();
        src.replication.node_id = Some(0xabc);
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:master\r\n"), "{out}");
        assert!(
            out.contains(&format!("master_replid:{:040x}\r\n", 0xabc)),
            "{out}"
        );
        assert!(out.contains("connected_slaves:0\r\n"), "{out}");
    }

    #[test]
    fn replication_live_replid_overrides_node_id() {
        let mut src = sources();
        src.replication.node_id = Some(0xabc);
        src.replication.replication_id = Some("f00f".repeat(10));
        let out = render(&ReplicationSection, &src);
        assert!(
            out.contains(&format!("master_replid:{}\r\n", "f00f".repeat(10))),
            "{out}"
        );
        // Default (no failover): the distinct master_replid2 line is all-zero
        // and second_repl_offset is the -1 sentinel Redis reports for "no
        // secondary window".
        assert!(
            out.contains("master_replid2:0000000000000000000000000000000000000000\r\n"),
            "{out}"
        );
        assert!(out.contains("second_repl_offset:-1\r\n"), "{out}");
    }

    #[test]
    fn replication_primary_renders_secondary_window_after_failover() {
        // Primary arm: a promoted node exposes the previous primary's id as
        // master_replid2 and its inclusive boundary as second_repl_offset.
        let mut src = sources();
        src.replication.primary = Some(PrimarySnapshot {
            replicas: vec![],
            repl_offset: 5000,
        });
        let prev_id = "abcd".repeat(10);
        src.replication.secondary_window = Some((prev_id.clone(), 4096));
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:master\r\n"), "{out}");
        assert!(
            out.contains(&format!("master_replid2:{prev_id}\r\n")),
            "{out}"
        );
        // Rendered verbatim from FrogDB's inclusive secondary_offset, not
        // Redis's +1 exclusive convention.
        assert!(out.contains("second_repl_offset:4096\r\n"), "{out}");
    }

    #[test]
    fn replication_replica_renders_secondary_window_after_failover() {
        // Else arm (replica / standalone-master): the same window pair renders
        // when there is no primary tracker.
        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.master_host = Some("10.0.0.1".to_string());
        src.replication.master_port = Some(6380);
        let prev_id = "1234".repeat(10);
        src.replication.secondary_window = Some((prev_id.clone(), 0));
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:slave\r\n"), "{out}");
        assert!(
            out.contains(&format!("master_replid2:{prev_id}\r\n")),
            "{out}"
        );
        // Boundary of 0 is a real (inclusive) window, distinct from the -1
        // no-failover sentinel.
        assert!(out.contains("second_repl_offset:0\r\n"), "{out}");
    }

    #[test]
    fn replication_replica_renders_master_link() {
        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.master_host = Some("10.0.0.1".to_string());
        src.replication.master_port = Some(6380);
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:slave\r\n"), "{out}");
        assert!(out.contains("master_host:10.0.0.1\r\n"), "{out}");
        assert!(out.contains("master_port:6380\r\n"), "{out}");
        assert!(out.contains("master_link_status:up\r\n"), "{out}");
    }

    #[test]
    fn replication_primary_renders_slave_lines() {
        let mut src = sources();
        src.replication.primary = Some(PrimarySnapshot {
            replicas: vec![ReplicaLine {
                ip: "127.0.0.1".to_string(),
                port: 7001,
                offset: 99,
            }],
            repl_offset: 100,
        });
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:master\r\n"), "{out}");
        assert!(out.contains("connected_slaves:1\r\n"), "{out}");
        assert!(
            out.contains("slave0:ip=127.0.0.1,port=7001,state=online,offset=99,lag=0\r\n"),
            "{out}"
        );
        assert!(out.contains("master_repl_offset:100\r\n"), "{out}");
        assert!(out.contains("repl_backlog_active:1\r\n"), "{out}");
        // No failover window on this primary: zero replid2 and -1 sentinel.
        assert!(
            out.contains("master_replid2:0000000000000000000000000000000000000000\r\n"),
            "{out}"
        );
        assert!(out.contains("second_repl_offset:-1\r\n"), "{out}");
    }

    #[test]
    fn commandstats_renders_sorted_lines() {
        let mut src = sources();
        src.command_stats = vec![
            (
                "get".to_string(),
                ServerCommandStats {
                    calls: 2,
                    usec: 10,
                    rejected_calls: 0,
                    failed_calls: 1,
                },
            ),
            (
                "set".to_string(),
                ServerCommandStats {
                    calls: 1,
                    usec: 7,
                    rejected_calls: 2,
                    failed_calls: 0,
                },
            ),
        ];
        let out = render(&CommandstatsSection, &src);
        assert_eq!(
            out,
            "# Commandstats\r\n\
             cmdstat_get:calls=2,usec=10,usec_per_call=5.00,rejected_calls=0,failed_calls=1\r\n\
             cmdstat_set:calls=1,usec=7,usec_per_call=7.00,rejected_calls=2,failed_calls=0\r\n\r\n"
        );
    }

    #[test]
    fn errorstats_renders_prefix_counts() {
        let mut src = sources();
        src.error_types = vec![("ERR".to_string(), 4), ("WRONGTYPE".to_string(), 1)];
        let out = render(&ErrorstatsSection, &src);
        assert_eq!(
            out,
            "# Errorstats\r\nerrorstat_ERR:count=4\r\nerrorstat_WRONGTYPE:count=1\r\n\r\n"
        );
    }

    #[test]
    fn latencystats_disabled_renders_header_only() {
        let src = sources();
        assert_eq!(render(&LatencystatsSection, &src), "# Latencystats\r\n\r\n");
    }

    #[test]
    fn ratelimit_inactive_renders_nothing() {
        let src = sources();
        assert_eq!(render(&RatelimitSection, &src), "");
    }

    #[test]
    fn ratelimit_active_renders_fields() {
        let mut src = sources();
        src.rate_limit = RateLimitSnapshot {
            users: 2,
            commands_rejected: 10,
            bytes_rejected: 0,
        };
        let out = render(&RatelimitSection, &src);
        assert_eq!(
            out,
            "# Ratelimit\r\nratelimit_users_configured:2\r\n\
             ratelimit_total_commands_rejected:10\r\n\
             ratelimit_total_bytes_rejected:0\r\n\r\n"
        );
    }

    #[test]
    fn tiered_renders_summed_counters() {
        let mut src = sources();
        src.shards.tiered.hot_keys = 5;
        src.shards.tiered.warm_keys = 2;
        src.shards.tiered.spills = 1;
        let out = render(&TieredSection, &src);
        assert!(out.contains("tiered_enabled:1\r\n"), "{out}");
        assert!(out.contains("tiered_hot_keys:5\r\n"), "{out}");
        assert!(out.contains("tiered_warm_keys:2\r\n"), "{out}");
    }

    #[test]
    fn latency_baseline_not_run() {
        let src = sources();
        assert_eq!(
            render(&LatencyBaselineSection, &src),
            "# Latency_Baseline\r\nbaseline_test_run:0\r\n\r\n"
        );
    }

    #[test]
    fn builder_renders_default_sections_in_order() {
        let src = sources();
        let sel = SectionSelector::from_args(&[]);
        let out = InfoBuilder::standard().render(&sel, &src);
        let server = out.find("# Server\r\n").expect("server section");
        let clients = out.find("# Clients\r\n").expect("clients section");
        let keyspace = out.find("# Keyspace\r\n").expect("keyspace section");
        assert!(server < clients && clients < keyspace, "{out}");
        // Extras excluded from default.
        assert!(!out.contains("# Commandstats"), "{out}");
        assert!(!out.contains("# Keysizes"), "{out}");
        // Rendered exactly once, straight from the accumulator — a duplicate
        // line would mean a stub anchor plus a patched copy survived.
        assert_eq!(out.matches("keyspace_hits:").count(), 1, "{out}");
    }
}
