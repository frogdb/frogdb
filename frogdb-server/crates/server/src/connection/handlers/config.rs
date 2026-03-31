//! CONFIG command handlers.
//!
//! This module handles CONFIG subcommands:
//! - CONFIG GET - Get configuration parameters
//! - CONFIG SET - Set configuration parameters
//! - CONFIG RESETSTAT - Reset server statistics
//! - CONFIG REWRITE - Rewrite configuration file (unsupported, returns error)
//! - CONFIG HELP - Show help text
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::ShardMessage;
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle CONFIG command and dispatch to subcommands.
    pub(crate) async fn handle_config_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'config' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "GET" => self.handle_config_get(&args[1..]),
            "SET" => self.handle_config_set(&args[1..]).await,
            "RESETSTAT" => self.handle_config_resetstat().await,
            "REWRITE" => Response::error(
                "ERR CONFIG REWRITE is not supported. Use the configuration file to persist changes.",
            ),
            "HELP" => self.handle_config_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try CONFIG HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle CONFIG GET <pattern> [pattern ...] - return parameters matching patterns.
    fn handle_config_get(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'config|get' command");
        }

        // Collect results from all patterns, deduplicating by parameter name
        let mut seen = std::collections::HashSet::new();
        let mut response = Vec::new();
        for arg in args {
            let pattern = String::from_utf8_lossy(arg);
            let results = self.admin.config_manager.get(&pattern);
            for (name, value) in results {
                if seen.insert(name.clone()) {
                    response.push(Response::bulk(Bytes::from(name)));
                    response.push(Response::bulk(Bytes::from(value)));
                }
            }
        }

        Response::Array(response)
    }

    /// Handle CONFIG SET <param> <value> [param value ...] - set configuration parameters.
    ///
    /// This is async because it may need to propagate eviction config changes
    /// to all shard workers and wait for acknowledgment.
    async fn handle_config_set(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 || !args.len().is_multiple_of(2) {
            return Response::error("ERR wrong number of arguments for 'config|set' command");
        }

        // Check for duplicate parameter names
        {
            let mut seen = std::collections::HashSet::new();
            for pair in args.chunks(2) {
                let param = pair[0].to_ascii_lowercase();
                if !seen.insert(param) {
                    return Response::error(format!(
                        "ERR Duplicate parameter '{}'",
                        String::from_utf8_lossy(&pair[0])
                    ));
                }
            }
        }

        for pair in args.chunks(2) {
            let param = String::from_utf8_lossy(&pair[0]);
            let value = String::from_utf8_lossy(&pair[1]);

            if let Err(e) = self.admin.config_manager.set_async(&param, &value).await {
                return Response::error(e.to_string());
            }
        }

        Response::ok()
    }

    /// Handle CONFIG RESETSTAT - reset server statistics.
    ///
    /// Broadcasts a reset to all shard workers to clear:
    /// - Latency monitor data (all events)
    /// - Slow query log entries
    /// - Peak memory counters
    async fn handle_config_resetstat(&self) -> Response {
        for sender in self.core.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::ResetStats { response_tx })
                .await
                .is_ok()
            {
                let _ = response_rx.await;
            }
        }
        Response::ok()
    }

    /// Handle CONFIG HELP - return help text.
    fn handle_config_help(&self) -> Response {
        let help = self.admin.config_manager.help_text();
        let response: Vec<Response> = help
            .into_iter()
            .map(|s| Response::bulk(Bytes::from(s)))
            .collect();
        Response::Array(response)
    }
}
