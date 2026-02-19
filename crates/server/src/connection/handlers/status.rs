//! STATUS command handlers.
//!
//! This module handles STATUS subcommands:
//! - STATUS JSON - Machine-readable server status
//! - STATUS HELP - Show help text

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::connection::util::format_timestamp_iso;
use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle STATUS command and dispatch to subcommands.
    pub(crate) async fn handle_status_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            // STATUS without subcommand shows help (like FrogDB-specific behavior)
            return self.handle_status_help();
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "JSON" => self.handle_status_json().await,
            "HELP" => self.handle_status_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try STATUS HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle STATUS JSON - return machine-readable server status.
    async fn handle_status_json(&self) -> Response {
        // Gather shard stats
        let shard_stats = self.gather_memory_stats().await;

        // Build status response
        let now = std::time::SystemTime::now();
        let timestamp = now
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Calculate ISO 8601 timestamp
        let timestamp_iso = format_timestamp_iso(timestamp);

        // Get client info
        let clients = self.client_registry.list();
        let blocked_clients = clients
            .iter()
            .filter(|c| c.flags.contains(frogdb_core::ClientFlags::BLOCKED))
            .count();

        // Calculate totals from shard stats
        let total_keys: usize = shard_stats.iter().map(|s| s.keys).sum();
        let used_bytes: u64 = shard_stats.iter().map(|s| s.data_memory as u64).sum();
        let peak_bytes: u64 = shard_stats.iter().map(|s| s.peak_memory as u64).sum();

        // Build shards array
        let shards: Vec<serde_json::Value> = shard_stats
            .iter()
            .enumerate()
            .map(|(id, stats)| {
                serde_json::json!({
                    "id": id,
                    "keys": stats.keys,
                    "memory_bytes": stats.data_memory,
                    "peak_memory_bytes": stats.peak_memory
                })
            })
            .collect();

        // Build the status JSON
        let status = serde_json::json!({
            "frogdb": {
                "version": env!("CARGO_PKG_VERSION"),
                "uptime_secs": 0, // Would need start_time tracking
                "process_id": std::process::id(),
                "timestamp": timestamp,
                "timestamp_iso": timestamp_iso
            },
            "cluster": {
                "database_available": true,
                "mode": "standalone",
                "num_shards": self.num_shards
            },
            "health": {
                "status": "healthy",
                "issues": []
            },
            "clients": {
                "connected": clients.len(),
                "max_clients": 0, // Would need config access
                "blocked": blocked_clients
            },
            "memory": {
                "used_bytes": used_bytes,
                "peak_bytes": peak_bytes,
                "limit_bytes": 0, // Would need config access
                "fragmentation_ratio": 1.0
            },
            "persistence": {
                "enabled": false
            },
            "shards": shards,
            "keyspace": {
                "total_keys": total_keys,
                "expired_keys_total": 0
            },
            "commands": {
                "total_processed": 0,
                "ops_per_sec": 0.0
            }
        });

        // Pretty-print the JSON
        let json_str = serde_json::to_string_pretty(&status).unwrap_or_else(|_| "{}".to_string());
        Response::bulk(Bytes::from(json_str))
    }

    /// Handle STATUS HELP - show subcommand help.
    fn handle_status_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"STATUS <subcommand> [<arg> ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"JSON")),
            Response::bulk(Bytes::from_static(
                b"    Return machine-readable server status as JSON.",
            )),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Show this help.")),
        ];
        Response::Array(help)
    }
}
