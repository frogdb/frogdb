//! MEMORY command handlers.
//!
//! This module handles MEMORY subcommands:
//! - MEMORY DOCTOR - Diagnose memory issues
//! - MEMORY HELP - Show help text
//! - MEMORY MALLOC-SIZE - Get allocator usable size
//! - MEMORY PURGE - Force memory release
//! - MEMORY STATS - Get detailed memory statistics
//! - MEMORY USAGE - Get memory for a specific key
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::{ShardMemoryStats, ShardMessage, shard_for_key};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle MEMORY command and dispatch to subcommands.
    pub(crate) async fn handle_memory_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'memory' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "DOCTOR" => self.handle_memory_doctor().await,
            "HELP" => self.handle_memory_help(),
            "MALLOC-SIZE" => self.handle_memory_malloc_size(&args[1..]),
            "PURGE" => self.handle_memory_purge(),
            "STATS" => self.handle_memory_stats().await,
            "USAGE" => self.handle_memory_usage(&args[1..]).await,
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try MEMORY HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle MEMORY DOCTOR - diagnose memory issues.
    async fn handle_memory_doctor(&self) -> Response {
        let collector = frogdb_debug::MemoryDiagCollector::new(
            self.core.shard_senders.clone(),
            self.memory_diag_config.clone(),
        );

        let report = collector.collect().await;
        let formatted = frogdb_debug::format_memory_report(&report);

        Response::bulk(Bytes::from(formatted))
    }

    /// Handle MEMORY HELP - show help text.
    fn handle_memory_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"MEMORY <subcommand> [<arg> ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"DOCTOR")),
            Response::bulk(Bytes::from_static(b"    Return memory problems reports.")),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
            Response::bulk(Bytes::from_static(b"MALLOC-SIZE <size>")),
            Response::bulk(Bytes::from_static(
                b"    Return the allocator usable size for the given input size.",
            )),
            Response::bulk(Bytes::from_static(b"PURGE")),
            Response::bulk(Bytes::from_static(
                b"    Attempt to release memory back to the OS.",
            )),
            Response::bulk(Bytes::from_static(b"STATS")),
            Response::bulk(Bytes::from_static(
                b"    Return information about memory usage.",
            )),
            Response::bulk(Bytes::from_static(b"USAGE <key> [SAMPLES <count>]")),
            Response::bulk(Bytes::from_static(
                b"    Return memory used by a key and its value.",
            )),
        ];
        Response::Array(help)
    }

    /// Handle MEMORY MALLOC-SIZE <size> - get allocator usable size (stub).
    fn handle_memory_malloc_size(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error(
                "ERR wrong number of arguments for 'memory|malloc-size' command",
            );
        }

        // Parse the size argument
        match String::from_utf8_lossy(&args[0]).parse::<i64>() {
            Ok(size) => {
                // Without jemalloc, just return the input size
                // In a real implementation this would query the allocator
                Response::Integer(size)
            }
            Err(_) => Response::error("ERR value is not an integer or out of range"),
        }
    }

    /// Handle MEMORY PURGE - force memory release (stub).
    fn handle_memory_purge(&self) -> Response {
        // Without jemalloc, this is a no-op
        // In a real implementation this would call jemalloc_purge_arena or similar
        Response::ok()
    }

    /// Handle MEMORY STATS - get detailed memory statistics.
    async fn handle_memory_stats(&self) -> Response {
        let stats = self.gather_memory_stats().await;

        let total_data_memory: usize = stats.iter().map(|s| s.data_memory).sum();
        let total_keys: usize = stats.iter().map(|s| s.keys).sum();
        let total_overhead: usize = stats.iter().map(|s| s.overhead_estimate).sum();
        let peak_memory: u64 = stats.iter().map(|s| s.peak_memory).max().unwrap_or(0);
        let total_limit: u64 = stats.iter().map(|s| s.memory_limit).sum();

        // Build a flat array of key-value pairs (Redis MEMORY STATS format)
        let mut result = vec![
            Response::bulk(Bytes::from_static(b"peak.allocated")),
            Response::Integer(peak_memory as i64),
            Response::bulk(Bytes::from_static(b"total.allocated")),
            Response::Integer(total_data_memory as i64),
            Response::bulk(Bytes::from_static(b"startup.allocated")),
            Response::Integer(0), // We don't track startup memory separately
            Response::bulk(Bytes::from_static(b"replication.backlog")),
            Response::Integer(0), // No replication backlog yet
            Response::bulk(Bytes::from_static(b"clients.slaves")),
            Response::Integer(0), // No replica clients yet
            Response::bulk(Bytes::from_static(b"clients.normal")),
            Response::Integer(0), // Would need client tracking
            Response::bulk(Bytes::from_static(b"aof.buffer")),
            Response::Integer(0), // No AOF buffer
            Response::bulk(Bytes::from_static(b"overhead.total")),
            Response::Integer(total_overhead as i64),
            Response::bulk(Bytes::from_static(b"keys.count")),
            Response::Integer(total_keys as i64),
            Response::bulk(Bytes::from_static(b"keys.bytes-per-key")),
            Response::Integer(if total_keys > 0 {
                (total_data_memory / total_keys) as i64
            } else {
                0
            }),
            Response::bulk(Bytes::from_static(b"dataset.bytes")),
            Response::Integer(total_data_memory as i64),
            Response::bulk(Bytes::from_static(b"dataset.percentage")),
            Response::bulk(Bytes::from(if total_data_memory > 0 && total_limit > 0 {
                format!(
                    "{:.2}",
                    (total_data_memory as f64 / total_limit as f64) * 100.0
                )
            } else {
                "0.00".to_string()
            })),
            Response::bulk(Bytes::from_static(b"peak.percentage")),
            Response::bulk(Bytes::from(if peak_memory > 0 && total_limit > 0 {
                format!("{:.2}", (peak_memory as f64 / total_limit as f64) * 100.0)
            } else {
                "0.00".to_string()
            })),
        ];

        // Add per-shard breakdown
        result.push(Response::bulk(Bytes::from_static(b"db.0")));
        let db_stats = vec![
            Response::bulk(Bytes::from_static(b"overhead.hashtable.main")),
            Response::Integer(total_overhead as i64),
            Response::bulk(Bytes::from_static(b"overhead.hashtable.expires")),
            Response::Integer(0),
        ];
        result.push(Response::Array(db_stats));

        Response::Array(result)
    }

    /// Handle MEMORY USAGE <key> [SAMPLES count] - get memory for a specific key.
    async fn handle_memory_usage(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'memory|usage' command");
        }

        let key = &args[0];
        let samples = if args.len() >= 3 && args[1].eq_ignore_ascii_case(b"SAMPLES") {
            match String::from_utf8_lossy(&args[2]).parse::<usize>() {
                Ok(n) => Some(n),
                Err(_) => return Response::error("ERR value is not an integer or out of range"),
            }
        } else {
            None
        };

        // Route to the shard that owns this key
        let shard_id = shard_for_key(key, self.core.shard_senders.len());
        let sender = &self.core.shard_senders[shard_id];

        let (response_tx, response_rx) = oneshot::channel();
        if sender
            .send(ShardMessage::MemoryUsage {
                key: key.clone(),
                samples,
                response_tx,
            })
            .await
            .is_err()
        {
            return Response::error("ERR shard communication error");
        }

        match response_rx.await {
            Ok(Some(usage)) => Response::Integer(usage as i64),
            Ok(None) => Response::Null,
            Err(_) => Response::error("ERR shard response error"),
        }
    }

    /// Gather memory stats from all shards.
    ///
    /// This is a pub(crate) helper used by both MEMORY STATS and STATUS JSON commands.
    pub(crate) async fn gather_memory_stats(&self) -> Vec<ShardMemoryStats> {
        let mut stats = Vec::new();

        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::MemoryStats { response_tx })
                .await
                .is_ok()
                && let Ok(shard_stats) = response_rx.await
            {
                stats.push(shard_stats);
            }
        }

        stats
    }
}
