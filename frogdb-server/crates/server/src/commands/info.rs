//! INFO command implementation.
//!
//! Returns information and statistics about the server.
//!
//! Sections:
//! - server: General server information
//! - clients: Client connections
//! - memory: Memory usage
//! - persistence: RDB/AOF persistence info
//! - stats: General statistics
//! - replication: Master/replica replication info
//! - cpu: CPU statistics
//! - keyspace: Database key statistics
//! - commandstats: Per-command statistics
//! - latency_baseline: Intrinsic latency test results (if startup test was run)

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy,
};
use frogdb_protocol::Response;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::latency_test;
use frogdb_cluster::version_gate;

// ============================================================================
// INFO - Server information
// ============================================================================

/// Sections included in "default" (no-arg INFO and INFO default).
const DEFAULT_SECTIONS: &[&[u8]] = &[
    b"server",
    b"clients",
    b"memory",
    b"persistence",
    b"stats",
    b"replication",
    b"cpu",
    b"keyspace",
];

/// Additional sections included only in "all" / "everything" (not in "default").
const EXTRA_SECTIONS: &[&[u8]] = &[
    b"commandstats",
    b"errorstats",
    b"latencystats",
    b"latency_baseline",
    b"tiered",
];

pub struct InfoCommand;

impl Command for InfoCommand {
    fn name(&self) -> &'static str {
        "INFO"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(0) // INFO [section ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut info = String::new();

        if args.is_empty() {
            // No-arg INFO returns default sections only.
            append_default_sections(ctx, &mut info, &mut seen);
        } else {
            for arg in args {
                let section = arg.to_ascii_lowercase();
                match section.as_slice() {
                    b"all" | b"everything" => {
                        append_all_sections(ctx, &mut info, &mut seen);
                    }
                    b"default" => {
                        append_default_sections(ctx, &mut info, &mut seen);
                    }
                    b"" => {
                        // Empty string arg is treated like default in Redis.
                        append_default_sections(ctx, &mut info, &mut seen);
                    }
                    _ => {
                        append_section(ctx, &mut info, &mut seen, &section);
                    }
                }
            }
        }

        Ok(Response::bulk(Bytes::from(info)))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless
    }
}

/// Append all default sections, skipping any already seen.
fn append_default_sections(
    ctx: &mut CommandContext,
    info: &mut String,
    seen: &mut HashSet<Vec<u8>>,
) {
    for section in DEFAULT_SECTIONS {
        append_section(ctx, info, seen, section);
    }
}

/// Append all sections (default + extra), skipping any already seen.
fn append_all_sections(ctx: &mut CommandContext, info: &mut String, seen: &mut HashSet<Vec<u8>>) {
    for section in DEFAULT_SECTIONS.iter().chain(EXTRA_SECTIONS.iter()) {
        append_section(ctx, info, seen, section);
    }
}

/// Append a single section if not already seen.
fn append_section(
    ctx: &mut CommandContext,
    info: &mut String,
    seen: &mut HashSet<Vec<u8>>,
    section: &[u8],
) {
    if !seen.insert(section.to_vec()) {
        return;
    }
    let section_info = match section {
        b"server" => build_server_info(ctx),
        b"clients" => build_clients_info(),
        b"memory" => build_memory_info(ctx),
        b"persistence" => build_persistence_info(ctx),
        b"stats" => build_stats_info(ctx),
        b"replication" => build_replication_info(ctx),
        b"cpu" => build_cpu_info(),
        b"keyspace" => build_keyspace_info(ctx),
        b"commandstats" => build_commandstats_info(),
        b"errorstats" => build_errorstats_info(),
        b"latencystats" => build_latencystats_info(),
        b"latency_baseline" => build_latency_baseline_info(),
        b"tiered" => build_tiered_info(ctx),
        _ => String::new(),
    };
    info.push_str(&section_info);
}

fn build_server_info(ctx: &CommandContext) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let mut info = format!(
        "# Server\r\n\
         frogdb_version:{}\r\n\
         redis_version:7.2.0\r\n\
         redis_git_sha1:00000000\r\n\
         redis_git_dirty:0\r\n\
         redis_build_id:0\r\n\
         redis_mode:standalone\r\n\
         os:{} {} {}\r\n\
         arch_bits:{}\r\n\
         monotonic_clock:POSIX clock_gettime\r\n\
         multiplexing_api:tokio\r\n\
         atomicvar_api:std\r\n\
         gcc_version:0.0.0\r\n\
         process_id:{}\r\n\
         process_supervised:no\r\n\
         run_id:frogdb0000000000000000000000000000000000\r\n\
         tcp_port:6379\r\n\
         server_time_usec:{}\r\n\
         uptime_in_seconds:0\r\n\
         uptime_in_days:0\r\n\
         hz:10\r\n\
         configured_hz:10\r\n\
         lru_clock:0\r\n\
         executable:/usr/local/bin/frogdb\r\n\
         config_file:\r\n\
         io_threads_active:0\r\n",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::FAMILY,
        std::env::consts::ARCH,
        std::mem::size_of::<usize>() * 8,
        std::process::id(),
        now.as_micros(),
    );

    // Version-gated: include cluster version fields after finalization
    if let Some(cs) = ctx.cluster_state {
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
            info.push_str(&format!(
                "active_version:{}\r\ncluster_version:{}\r\n",
                active.as_deref().unwrap_or(""),
                cluster_version,
            ));
        }
    }

    info.push_str("\r\n");
    info
}

fn build_clients_info() -> String {
    "# Clients\r\n\
     connected_clients:1\r\n\
     cluster_connections:0\r\n\
     maxclients:10000\r\n\
     client_recent_max_input_buffer:0\r\n\
     client_recent_max_output_buffer:0\r\n\
     blocked_clients:0\r\n\
     tracking_clients:0\r\n\
     clients_in_timeout_table:0\r\n\r\n"
        .to_string()
}

fn build_memory_info(ctx: &mut CommandContext) -> String {
    let used_memory = ctx.store.memory_used();
    format!(
        "# Memory\r\n\
         used_memory:{}\r\n\
         used_memory_human:{}K\r\n\
         used_memory_rss:{}\r\n\
         used_memory_rss_human:{}K\r\n\
         used_memory_peak:{}\r\n\
         used_memory_peak_human:{}K\r\n\
         used_memory_peak_perc:100.00%\r\n\
         used_memory_overhead:0\r\n\
         used_memory_startup:0\r\n\
         used_memory_dataset:{}\r\n\
         used_memory_dataset_perc:100.00%\r\n\
         allocator_allocated:0\r\n\
         allocator_active:0\r\n\
         allocator_resident:0\r\n\
         total_system_memory:0\r\n\
         total_system_memory_human:0K\r\n\
         used_memory_lua:0\r\n\
         used_memory_lua_human:0B\r\n\
         used_memory_scripts:0\r\n\
         used_memory_scripts_human:0B\r\n\
         number_of_cached_scripts:0\r\n\
         maxmemory:0\r\n\
         maxmemory_human:0B\r\n\
         maxmemory_policy:noeviction\r\n\
         allocator_frag_ratio:1.00\r\n\
         allocator_frag_bytes:0\r\n\
         allocator_rss_ratio:1.00\r\n\
         allocator_rss_bytes:0\r\n\
         rss_overhead_ratio:1.00\r\n\
         rss_overhead_bytes:0\r\n\
         mem_fragmentation_ratio:1.00\r\n\
         mem_fragmentation_bytes:0\r\n\
         mem_not_counted_for_evict:0\r\n\
         mem_replication_backlog:0\r\n\
         mem_clients_slaves:0\r\n\
         mem_clients_normal:0\r\n\
         mem_aof_buffer:0\r\n\
         mem_allocator:rust\r\n\
         active_defrag_running:0\r\n\
         lazyfree_pending_objects:0\r\n\
         lazyfreed_objects:0\r\n\r\n",
        used_memory,
        used_memory / 1024,
        used_memory,
        used_memory / 1024,
        used_memory,
        used_memory / 1024,
        used_memory,
    )
}

fn build_persistence_info(ctx: &mut CommandContext) -> String {
    let dirty = ctx.store.dirty();
    // Note: Real-time WAL lag metrics are available via STATUS JSON command
    // which aggregates data from all shards. The values here are placeholders
    // since INFO runs per-shard without server-level aggregation context.
    format!(
        "# Persistence\r\n\
         loading:0\r\n\
         async_loading:0\r\n\
         persistence_enabled:1\r\n\
         durability_mode:periodic\r\n\
         wal_pending_ops:0\r\n\
         wal_pending_bytes:0\r\n\
         wal_durability_lag_ms:0\r\n\
         wal_sync_lag_ms:0\r\n\
         wal_last_flush_time:0\r\n\
         wal_last_sync_time:0\r\n\
         wal_writes_total:0\r\n\
         wal_bytes_total:0\r\n\
         current_cow_peak:0\r\n\
         current_cow_size:0\r\n\
         current_cow_size_age:0\r\n\
         current_fork_perc:0.00\r\n\
         current_save_keys_processed:0\r\n\
         current_save_keys_total:0\r\n\
         rdb_changes_since_last_save:{}\r\n\
         rdb_bgsave_in_progress:0\r\n\
         rdb_last_save_time:0\r\n\
         rdb_last_bgsave_status:ok\r\n\
         rdb_last_bgsave_time_sec:-1\r\n\
         rdb_current_bgsave_time_sec:-1\r\n\
         rdb_saves:0\r\n\
         rdb_last_cow_size:0\r\n\
         rdb_last_load_keys_expired:0\r\n\
         rdb_last_load_keys_loaded:0\r\n\
         aof_enabled:0\r\n\
         aof_rewrite_in_progress:0\r\n\
         aof_rewrite_scheduled:0\r\n\
         aof_last_rewrite_time_sec:-1\r\n\
         aof_current_rewrite_time_sec:-1\r\n\
         aof_last_bgrewrite_status:ok\r\n\
         aof_rewrites:0\r\n\
         aof_rewrites_consecutive_failures:0\r\n\
         aof_last_write_status:ok\r\n\
         aof_last_cow_size:0\r\n\
         module_fork_in_progress:0\r\n\
         module_fork_last_cow_size:0\r\n\r\n",
        dirty,
    )
}

fn build_stats_info(ctx: &mut CommandContext) -> String {
    let key_count = ctx.store.len();
    format!(
        "# Stats\r\n\
         total_connections_received:1\r\n\
         total_commands_processed:0\r\n\
         instantaneous_ops_per_sec:0\r\n\
         total_net_input_bytes:0\r\n\
         total_net_output_bytes:0\r\n\
         total_net_repl_input_bytes:0\r\n\
         total_net_repl_output_bytes:0\r\n\
         instantaneous_input_kbps:0.00\r\n\
         instantaneous_output_kbps:0.00\r\n\
         instantaneous_input_repl_kbps:0.00\r\n\
         instantaneous_output_repl_kbps:0.00\r\n\
         rejected_connections:0\r\n\
         sync_full:0\r\n\
         sync_partial_ok:0\r\n\
         sync_partial_err:0\r\n\
         expired_keys:0\r\n\
         expired_stale_perc:0.00\r\n\
         expired_time_cap_reached_count:0\r\n\
         expire_cycle_cpu_milliseconds:0\r\n\
         evicted_keys:0\r\n\
         evicted_clients:0\r\n\
         total_eviction_exceeded_time:0\r\n\
         current_eviction_exceeded_time:0\r\n\
         keyspace_hits:0\r\n\
         keyspace_misses:0\r\n\
         pubsub_channels:0\r\n\
         pubsub_patterns:0\r\n\
         pubsubshard_channels:0\r\n\
         latest_fork_usec:0\r\n\
         total_forks:0\r\n\
         migrate_cached_sockets:0\r\n\
         slave_expires_tracked_keys:0\r\n\
         active_defrag_hits:0\r\n\
         active_defrag_misses:0\r\n\
         active_defrag_key_hits:0\r\n\
         active_defrag_key_misses:0\r\n\
         total_active_defrag_time:0\r\n\
         current_active_defrag_time:0\r\n\
         tracking_total_keys:{}\r\n\
         tracking_total_items:0\r\n\
         tracking_total_prefixes:0\r\n\
         unexpected_error_replies:0\r\n\
         total_error_replies:0\r\n\
         rejected_calls:0\r\n\
         failed_calls:0\r\n\
         dump_payload_sanitizations:0\r\n\
         total_reads_processed:0\r\n\
         total_writes_processed:0\r\n\
         io_threaded_reads_processed:0\r\n\
         io_threaded_writes_processed:0\r\n\r\n",
        key_count
    )
}

/// Build the commandstats section (header-only placeholder).
///
/// The real per-command data is patched in by `handle_info` in
/// `connection/handlers/scatter.rs`, which has access to `ClientRegistry`
/// and can query the server-wide command call counts. Shard-local code
/// only emits the section header so the scatter-gather patcher has a
/// reliable anchor to rewrite.
fn build_commandstats_info() -> String {
    "# Commandstats\r\n\r\n".to_string()
}

/// Build the errorstats section (header-only placeholder).
///
/// Real error data is patched in by `handle_info` in the scatter handler,
/// which has access to the ErrorStats in ClientRegistry.
fn build_errorstats_info() -> String {
    "# Errorstats\r\n\r\n".to_string()
}

/// Build the latencystats section (header-only placeholder).
///
/// Real latency histogram data is patched in by `handle_info` in the
/// scatter handler, which has access to the CommandLatencyHistograms.
fn build_latencystats_info() -> String {
    "# Latencystats\r\n\r\n".to_string()
}

fn build_replication_info(ctx: &CommandContext) -> String {
    // Check if we have a replication tracker (running as primary)
    if let Some(tracker) = ctx.replication_tracker {
        let replicas = tracker.get_streaming_replicas();
        let repl_offset = tracker.current_offset();
        let connected_slaves = replicas.len();

        let mut info = format!(
            "# Replication\r\n\
             role:master\r\n\
             connected_slaves:{}\r\n",
            connected_slaves
        );

        // Add info for each connected replica
        for (i, replica) in replicas.iter().enumerate() {
            info.push_str(&format!(
                "slave{}:ip={},port={},state=online,offset={},lag=0\r\n",
                i,
                replica.address.ip(),
                replica.listening_port,
                replica.acked_offset
            ));
        }

        // Add replication IDs and offset info
        let repl_id = format!("{:040x}", ctx.node_id.unwrap_or(0));
        info.push_str(&format!(
            "master_failover_state:no-failover\r\n\
             master_replid:{}\r\n\
             master_replid2:0000000000000000000000000000000000000000\r\n\
             master_repl_offset:{}\r\n\
             second_repl_offset:-1\r\n\
             repl_backlog_active:{}\r\n\
             repl_backlog_size:1048576\r\n\
             repl_backlog_first_byte_offset:0\r\n\
             repl_backlog_histlen:{}\r\n\r\n",
            repl_id,
            repl_offset,
            if connected_slaves > 0 { 1 } else { 0 },
            repl_offset,
        ));

        info
    } else {
        // Standalone mode or replica mode - return default info
        let role = if ctx.is_replica { "slave" } else { "master" };
        let repl_id = format!("{:040x}", ctx.node_id.unwrap_or(0));
        let mut info = format!(
            "# Replication\r\n\
             role:{}\r\n",
            role,
        );

        // For replicas, include master_host, master_port, and master_link_status
        if ctx.is_replica {
            if let Some(ref host) = ctx.master_host {
                info.push_str(&format!("master_host:{}\r\n", host));
            }
            if let Some(port) = ctx.master_port {
                info.push_str(&format!("master_port:{}\r\n", port));
            }
            info.push_str("master_link_status:up\r\n");
        }

        info.push_str(&format!(
            "connected_slaves:0\r\n\
             master_failover_state:no-failover\r\n\
             master_replid:{}\r\n\
             master_replid2:0000000000000000000000000000000000000000\r\n\
             master_repl_offset:0\r\n\
             second_repl_offset:-1\r\n\
             repl_backlog_active:0\r\n\
             repl_backlog_size:1048576\r\n\
             repl_backlog_first_byte_offset:0\r\n\
             repl_backlog_histlen:0\r\n\r\n",
            repl_id,
        ));

        info
    }
}

fn build_cpu_info() -> String {
    "# CPU\r\n\
     used_cpu_sys:0.000000\r\n\
     used_cpu_user:0.000000\r\n\
     used_cpu_sys_children:0.000000\r\n\
     used_cpu_user_children:0.000000\r\n\
     used_cpu_sys_main_thread:0.000000\r\n\
     used_cpu_user_main_thread:0.000000\r\n\r\n"
        .to_string()
}

fn build_keyspace_info(ctx: &mut CommandContext) -> String {
    let key_count = ctx.store.len();
    if key_count == 0 {
        "# Keyspace\r\n\r\n".to_string()
    } else {
        format!(
            "# Keyspace\r\n\
             db0:keys={},expires=0,avg_ttl=0\r\n\r\n",
            key_count
        )
    }
}

fn build_tiered_info(ctx: &mut CommandContext) -> String {
    let warm_keys = ctx.store.warm_key_count();
    let hot_keys = ctx.store.hot_key_count();
    let promotions = ctx.store.promotion_count();
    let demotions = ctx.store.demotion_count();
    let expired_on_promote = ctx.store.expired_on_promote_count();
    let enabled = if warm_keys > 0 || demotions > 0 { 1 } else { 0 };

    format!(
        "# Tiered\r\n\
         tiered_enabled:{}\r\n\
         tiered_hot_keys:{}\r\n\
         tiered_warm_keys:{}\r\n\
         tiered_promotions:{}\r\n\
         tiered_demotions:{}\r\n\
         tiered_expired_on_promote:{}\r\n\r\n",
        enabled, hot_keys, warm_keys, promotions, demotions, expired_on_promote,
    )
}

fn build_latency_baseline_info() -> String {
    match latency_test::get_global_baseline() {
        Some(info) => {
            let exceeded = if info.result.max_us > info.warning_threshold_us {
                1
            } else {
                0
            };

            format!(
                "# Latency_Baseline\r\n\
                 baseline_test_run:1\r\n\
                 baseline_duration_secs:{}\r\n\
                 baseline_samples:{}\r\n\
                 baseline_min_us:{}\r\n\
                 baseline_max_us:{}\r\n\
                 baseline_avg_us:{:.1}\r\n\
                 baseline_p99_us:{}\r\n\
                 baseline_warning_threshold_us:{}\r\n\
                 baseline_exceeded_threshold:{}\r\n\r\n",
                info.result.duration_secs,
                info.result.samples,
                info.result.min_us,
                info.result.max_us,
                info.result.avg_us,
                info.result.p99_us,
                info.warning_threshold_us,
                exceeded,
            )
        }
        None => "# Latency_Baseline\r\n\
             baseline_test_run:0\r\n\r\n"
            .to_string(),
    }
}
