//! SLOWLOG command handlers and slow query logging.
//!
//! This module handles SLOWLOG subcommands:
//! - SLOWLOG GET - Get recent slow queries
//! - SLOWLOG LEN - Get total number of entries
//! - SLOWLOG RESET - Clear all slow query logs
//! - SLOWLOG HELP - Show help text
//!
//! It also provides the `maybe_log_slow_query` helper used by the run() loop
//! to record slow commands.
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::{CommandFlags, ShardMessage, SlowLog};
use frogdb_protocol::{ParsedCommand, Response};
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle SLOWLOG command and dispatch to subcommands.
    pub(crate) async fn handle_slowlog_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'slowlog' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "GET" => self.handle_slowlog_get(&args[1..]).await,
            "LEN" => self.handle_slowlog_len().await,
            "RESET" => self.handle_slowlog_reset().await,
            "HELP" => self.handle_slowlog_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try SLOWLOG HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle SLOWLOG GET [count] - get recent slow queries.
    async fn handle_slowlog_get(&self, args: &[Bytes]) -> Response {
        // Default count is 10, like Redis
        let count: usize = if args.is_empty() {
            10
        } else {
            match String::from_utf8_lossy(&args[0]).parse::<i64>() {
                Ok(n) if n >= -1 => {
                    if n == -1 {
                        usize::MAX // -1 means all entries
                    } else {
                        n as usize
                    }
                }
                Ok(_) => return Response::error("ERR count should be greater than or equal to -1"),
                Err(_) => return Response::error("ERR value is not an integer or out of range"),
            }
        };

        // Scatter-gather: collect from all shards
        let mut all_entries = Vec::new();

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::SlowlogGet { count, response_tx })
                .await
                .is_ok()
                && let Ok(entries) = response_rx.await
            {
                all_entries.extend(entries);
            }
        }

        // Sort by ID descending (newest first) and limit to count
        all_entries.sort_by(|a, b| b.id.cmp(&a.id));
        all_entries.truncate(count);

        // Convert to Redis response format
        let entries: Vec<Response> = all_entries
            .into_iter()
            .map(|entry| {
                let args: Vec<Response> = entry.command.into_iter().map(Response::bulk).collect();

                Response::Array(vec![
                    Response::Integer(entry.id as i64),
                    Response::Integer(entry.timestamp),
                    Response::Integer(entry.duration_us as i64),
                    Response::Array(args),
                    Response::bulk(Bytes::from(entry.client_addr)),
                    Response::bulk(Bytes::from(entry.client_name)),
                ])
            })
            .collect();

        Response::Array(entries)
    }

    /// Handle SLOWLOG LEN - get total number of entries across all shards.
    async fn handle_slowlog_len(&self) -> Response {
        let mut total_len = 0usize;

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::SlowlogLen { response_tx })
                .await
                .is_ok()
                && let Ok(len) = response_rx.await
            {
                total_len += len;
            }
        }

        Response::Integer(total_len as i64)
    }

    /// Handle SLOWLOG RESET - clear all slow query logs.
    async fn handle_slowlog_reset(&self) -> Response {
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::SlowlogReset { response_tx })
                .await
                .is_ok()
            {
                let _ = response_rx.await;
            }
        }

        Response::ok()
    }

    /// Handle SLOWLOG HELP - show help text.
    fn handle_slowlog_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"SLOWLOG <subcommand> [<arg> ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"GET [<count>]")),
            Response::bulk(Bytes::from_static(
                b"    Return top <count> entries from the slowlog (default 10).",
            )),
            Response::bulk(Bytes::from_static(
                b"    Entries are made of:",
            )),
            Response::bulk(Bytes::from_static(
                b"    id, timestamp, time in microseconds, arguments array, client address, client name",
            )),
            Response::bulk(Bytes::from_static(b"LEN")),
            Response::bulk(Bytes::from_static(
                b"    Return the number of entries in the slowlog.",
            )),
            Response::bulk(Bytes::from_static(b"RESET")),
            Response::bulk(Bytes::from_static(b"    Reset the slowlog.")),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
        ];
        Response::Array(help)
    }

    /// Log a slow query to the appropriate shard if threshold is exceeded.
    pub(crate) async fn maybe_log_slow_query(&self, cmd: &ParsedCommand, elapsed_us: u64) {
        // Check threshold setting
        let threshold = self.config_manager.slowlog_log_slower_than();

        // -1 means disabled
        if threshold < 0 {
            return;
        }

        // Check if elapsed time exceeds threshold (0 means log all)
        if threshold > 0 && elapsed_us < threshold as u64 {
            return;
        }

        // Check if command has SKIP_SLOWLOG flag
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);
        if let Some(handler) = self.registry.get(&cmd_name_str)
            && handler.flags().contains(CommandFlags::SKIP_SLOWLOG)
        {
            return;
        }

        // Prepare command args for logging (including command name)
        let mut command_args = vec![cmd.name.clone()];
        command_args.extend(cmd.args.iter().cloned());

        // Truncate args according to max_arg_len setting
        let max_arg_len = self.config_manager.slowlog_max_arg_len();
        let truncated_args = SlowLog::truncate_args(&command_args, max_arg_len);

        // Get current max_len to propagate to shard
        let max_len = self.config_manager.slowlog_max_len();

        // Get client info
        let client_addr = self.state.addr.to_string();
        let client_name = self
            .state
            .name
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();

        // Send to shard 0 (or we could distribute based on some logic)
        // Using shard 0 is simplest and matches Redis behavior
        if let Some(sender) = self.shard_senders.first() {
            let _ = sender
                .send(ShardMessage::SlowlogAdd {
                    duration_us: elapsed_us,
                    command: truncated_args,
                    client_addr,
                    client_name,
                    max_len,
                })
                .await;
        }
    }
}
