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
//! - latency_baseline: Intrinsic latency test results (if startup test was run)

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::latency_test;

// ============================================================================
// INFO - Server information
// ============================================================================

pub struct InfoCommand;

impl Command for InfoCommand {
    fn name(&self) -> &'static str {
        "INFO"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 0, max: 1 } // INFO [section]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let section = if args.is_empty() {
            None
        } else {
            Some(args[0].to_ascii_lowercase())
        };

        let info = match section.as_deref() {
            None | Some(b"all") | Some(b"default") => build_all_info(ctx),
            Some(b"server") => build_server_info(),
            Some(b"clients") => build_clients_info(),
            Some(b"memory") => build_memory_info(ctx),
            Some(b"persistence") => build_persistence_info(),
            Some(b"stats") => build_stats_info(ctx),
            Some(b"replication") => build_replication_info(ctx),
            Some(b"cpu") => build_cpu_info(),
            Some(b"keyspace") => build_keyspace_info(ctx),
            Some(b"latency_baseline") => build_latency_baseline_info(),
            Some(other) => {
                // Unknown section - return empty
                return Err(CommandError::InvalidArgument {
                    message: format!("Invalid section '{}'", String::from_utf8_lossy(other)),
                });
            }
        };

        Ok(Response::bulk(Bytes::from(info)))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless
    }
}

fn build_all_info(ctx: &mut CommandContext) -> String {
    let mut info = String::new();
    info.push_str(&build_server_info());
    info.push_str(&build_clients_info());
    info.push_str(&build_memory_info(ctx));
    info.push_str(&build_persistence_info());
    info.push_str(&build_stats_info(ctx));
    info.push_str(&build_replication_info(ctx));
    info.push_str(&build_cpu_info());
    info.push_str(&build_keyspace_info(ctx));
    info.push_str(&build_latency_baseline_info());
    info
}

fn build_server_info() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    format!(
        "# Server\r\n\
         frogdb_version:0.1.0\r\n\
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
         io_threads_active:0\r\n\r\n",
        std::env::consts::OS,
        std::env::consts::FAMILY,
        std::env::consts::ARCH,
        std::mem::size_of::<usize>() * 8,
        std::process::id(),
        now.as_micros(),
    )
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

fn build_persistence_info() -> String {
    // Note: Real-time WAL lag metrics are available via STATUS JSON command
    // which aggregates data from all shards. The values here are placeholders
    // since INFO runs per-shard without server-level aggregation context.
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
     rdb_changes_since_last_save:0\r\n\
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
     module_fork_last_cow_size:0\r\n\r\n"
        .to_string()
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
         dump_payload_sanitizations:0\r\n\
         total_reads_processed:0\r\n\
         total_writes_processed:0\r\n\
         io_threaded_reads_processed:0\r\n\
         io_threaded_writes_processed:0\r\n\r\n",
        key_count
    )
}

fn build_replication_info(ctx: &CommandContext) -> String {
    // Check if we have a replication tracker (running as primary)
<<<<<<< HEAD
    if let Some(tracker) = ctx.replication_tracker {
||||||| parent of 670778b (more fixing stuff?)
    if let Some(ref tracker) = ctx.replication_tracker {
=======
    if let Some(tracker) = &ctx.replication_tracker {
>>>>>>> 670778b (more fixing stuff?)
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
        let repl_id = format!("{:040x}", ctx.node_id.unwrap_or(0));
        format!(
            "# Replication\r\n\
             role:master\r\n\
             connected_slaves:0\r\n\
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
        )
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
