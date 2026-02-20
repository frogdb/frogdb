//! Administrative command handlers.
//!
//! This module handles administrative commands:
//! - CLIENT - Client connection management
//! - CONFIG - Server configuration
//! - DEBUG - Debugging commands
//! - MEMORY - Memory diagnostics
//! - LATENCY - Latency monitoring
//! - SLOWLOG - Slow query log
//! - INFO - Server information
//! - SHUTDOWN - Server shutdown stub

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{ClientRegistry, PauseMode};
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;
use crate::runtime_config::ConfigManager;

/// Handler for administrative commands.
#[derive(Clone)]
pub struct AdminHandler {
    /// Client registry for CLIENT commands.
    client_registry: Arc<ClientRegistry>,
    /// Runtime config manager for CONFIG commands.
    config_manager: Arc<ConfigManager>,
}

impl AdminHandler {
    /// Create a new admin handler.
    pub fn new(client_registry: Arc<ClientRegistry>, config_manager: Arc<ConfigManager>) -> Self {
        Self {
            client_registry,
            config_manager,
        }
    }

    /// Handle CLIENT command.
    pub fn handle_client(&self, args: &[Bytes], conn_id: u64) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client' command");
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();
        let sub_args = &args[1..];

        match subcommand.as_str() {
            "ID" => Response::Integer(conn_id as i64),
            "LIST" => self.handle_client_list(),
            "INFO" => self.handle_client_info(conn_id),
            "KILL" => self.handle_client_kill(sub_args),
            "PAUSE" => self.handle_client_pause(sub_args),
            "UNPAUSE" => self.handle_client_unpause(),
            "HELP" => self.handle_client_help(),
            _ => Response::error(format!("ERR Unknown CLIENT subcommand '{}'", subcommand)),
        }
    }

    fn handle_client_list(&self) -> Response {
        let clients = self.client_registry.list();
        let mut output = String::new();

        for client in clients {
            output.push_str(&client.to_client_list_entry());
            output.push('\n');
        }

        Response::bulk(Bytes::from(output))
    }

    fn handle_client_info(&self, conn_id: u64) -> Response {
        match self.client_registry.get(conn_id) {
            Some(client) => {
                let info = client.to_client_list_entry();
                Response::bulk(Bytes::from(info))
            }
            None => Response::bulk(Bytes::new()),
        }
    }

    fn handle_client_kill(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client kill' command");
        }

        // Simple implementation: kill by ID
        if let Ok(id) = frogdb_core::parse_u64(&args[0]) {
            if self.client_registry.kill_by_id(id) {
                return Response::Integer(1);
            }
        }

        Response::Integer(0)
    }

    fn handle_client_pause(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client pause' command");
        }

        match frogdb_core::parse_u64(&args[0]) {
            Ok(timeout_ms) => {
                self.client_registry.pause(PauseMode::All, timeout_ms);
                Response::ok()
            }
            Err(_) => Response::error("ERR timeout is not an integer or out of range"),
        }
    }

    fn handle_client_unpause(&self) -> Response {
        self.client_registry.unpause();
        Response::ok()
    }

    fn handle_client_help(&self) -> Response {
        let help = vec![
            "CLIENT ID",
            "    Return the ID of the current connection.",
            "CLIENT LIST [TYPE <type>] [ID <id> ...]",
            "    List client connections.",
            "CLIENT INFO",
            "    Return information about the current client.",
            "CLIENT KILL [ID <id>] [TYPE <type>] [ADDR <addr>]",
            "    Kill client connections.",
            "CLIENT PAUSE <timeout>",
            "    Pause all clients for the specified time.",
            "CLIENT UNPAUSE",
            "    Resume clients after a pause.",
            "CLIENT SETNAME <name>",
            "    Set the connection name.",
            "CLIENT GETNAME",
            "    Get the connection name.",
            "CLIENT HELP",
            "    Show this help.",
        ];
        Response::Array(help.into_iter().map(|s| Response::bulk(s)).collect())
    }

    /// Handle CONFIG command.
    pub fn handle_config(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'config' command");
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();
        let sub_args = &args[1..];

        match subcommand.as_str() {
            "GET" => self.handle_config_get(sub_args),
            "SET" => self.handle_config_set(sub_args),
            "REWRITE" => Response::ok(),   // Not implemented
            "RESETSTAT" => Response::ok(), // Not implemented
            "HELP" => self.handle_config_help(),
            _ => Response::error(format!("ERR Unknown CONFIG subcommand '{}'", subcommand)),
        }
    }

    fn handle_config_get(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'config get' command");
        }

        let pattern = String::from_utf8_lossy(&args[0]);
        let values = self.config_manager.get(&pattern);

        let mut result = Vec::new();
        for (key, value) in values {
            result.push(Response::bulk(Bytes::from(key)));
            result.push(Response::bulk(Bytes::from(value)));
        }

        Response::Array(result)
    }

    fn handle_config_set(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Response::error("ERR wrong number of arguments for 'config set' command");
        }

        for pair in args.chunks(2) {
            let key = String::from_utf8_lossy(&pair[0]);
            let value = String::from_utf8_lossy(&pair[1]);

            if let Err(e) = self.config_manager.set(&key, &value) {
                return Response::error(format!("ERR {}", e));
            }
        }

        Response::ok()
    }

    fn handle_config_help(&self) -> Response {
        let help = vec![
            "CONFIG GET <pattern>",
            "    Get configuration parameters matching the pattern.",
            "CONFIG SET <parameter> <value> [<parameter> <value> ...]",
            "    Set configuration parameters.",
            "CONFIG REWRITE",
            "    Rewrite the configuration file.",
            "CONFIG RESETSTAT",
            "    Reset statistics.",
            "CONFIG HELP",
            "    Show this help.",
        ];
        Response::Array(help.into_iter().map(|s| Response::bulk(s)).collect())
    }

    /// Handle DEBUG command.
    pub fn handle_debug(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'debug' command");
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

        match subcommand.as_str() {
            "SLEEP" => {
                if args.len() < 2 {
                    return Response::error("ERR wrong number of arguments for 'debug sleep'");
                }
                // Sleep would need to be async
                Response::ok()
            }
            "QUICKLIST-PACKED-THRESHOLD" => Response::ok(),
            "SET-ACTIVE-EXPIRE" => Response::ok(),
            "HELP" => self.handle_debug_help(),
            _ => Response::error(format!("ERR Unknown DEBUG subcommand '{}'", subcommand)),
        }
    }

    fn handle_debug_help(&self) -> Response {
        let help = vec![
            "DEBUG SLEEP <seconds>",
            "    Sleep for the specified number of seconds.",
            "DEBUG HELP",
            "    Show this help.",
        ];
        Response::Array(help.into_iter().map(|s| Response::bulk(s)).collect())
    }

    /// Handle MEMORY command.
    pub fn handle_memory(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'memory' command");
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

        match subcommand.as_str() {
            "DOCTOR" => Response::bulk("Sam, I have no memory problems"),
            "STATS" => self.handle_memory_stats(),
            "USAGE" => self.handle_memory_usage(&args[1..]),
            "PURGE" => Response::ok(),
            "MALLOC-SIZE" => Response::Integer(0),
            "HELP" => self.handle_memory_help(),
            _ => Response::error(format!("ERR Unknown MEMORY subcommand '{}'", subcommand)),
        }
    }

    fn handle_memory_stats(&self) -> Response {
        // Basic memory stats
        Response::Array(vec![
            Response::bulk("peak.allocated"),
            Response::Integer(0),
            Response::bulk("total.allocated"),
            Response::Integer(0),
            Response::bulk("startup.allocated"),
            Response::Integer(0),
        ])
    }

    fn handle_memory_usage(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'memory usage' command");
        }
        // Would need shard access to get actual memory usage
        Response::Integer(0)
    }

    fn handle_memory_help(&self) -> Response {
        let help = vec![
            "MEMORY DOCTOR",
            "    Check for memory problems.",
            "MEMORY STATS",
            "    Show memory statistics.",
            "MEMORY USAGE <key> [SAMPLES <count>]",
            "    Show memory usage of a key.",
            "MEMORY PURGE",
            "    Attempt to release memory back to OS.",
            "MEMORY MALLOC-SIZE <pointer>",
            "    Show memory allocation size.",
            "MEMORY HELP",
            "    Show this help.",
        ];
        Response::Array(help.into_iter().map(|s| Response::bulk(s)).collect())
    }

    /// Handle SLOWLOG command.
    pub fn handle_slowlog(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'slowlog' command");
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

        match subcommand.as_str() {
            "GET" => Response::Array(vec![]), // Would need slowlog access
            "LEN" => Response::Integer(0),
            "RESET" => Response::ok(),
            "HELP" => self.handle_slowlog_help(),
            _ => Response::error(format!("ERR Unknown SLOWLOG subcommand '{}'", subcommand)),
        }
    }

    fn handle_slowlog_help(&self) -> Response {
        let help = vec![
            "SLOWLOG GET [<count>]",
            "    Get slowlog entries.",
            "SLOWLOG LEN",
            "    Get the number of entries in the slowlog.",
            "SLOWLOG RESET",
            "    Reset the slowlog.",
            "SLOWLOG HELP",
            "    Show this help.",
        ];
        Response::Array(help.into_iter().map(|s| Response::bulk(s)).collect())
    }

    /// Handle LATENCY command.
    pub fn handle_latency(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'latency' command");
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

        match subcommand.as_str() {
            "DOCTOR" => Response::bulk("I have no latency reports to show you."),
            "GRAPH" => Response::bulk(Bytes::new()),
            "HISTOGRAM" => Response::Array(vec![]),
            "HISTORY" => Response::Array(vec![]),
            "LATEST" => Response::Array(vec![]),
            "RESET" => Response::ok(),
            "HELP" => self.handle_latency_help(),
            _ => Response::error(format!("ERR Unknown LATENCY subcommand '{}'", subcommand)),
        }
    }

    fn handle_latency_help(&self) -> Response {
        let help = vec![
            "LATENCY DOCTOR",
            "    Analyze latency issues and provide advice.",
            "LATENCY GRAPH <event>",
            "    Show latency graph for an event.",
            "LATENCY HISTOGRAM",
            "    Show latency histograms.",
            "LATENCY HISTORY <event>",
            "    Show latency history for an event.",
            "LATENCY LATEST",
            "    Show latest latency samples.",
            "LATENCY RESET [<event> ...]",
            "    Reset latency data.",
            "LATENCY HELP",
            "    Show this help.",
        ];
        Response::Array(help.into_iter().map(|s| Response::bulk(s)).collect())
    }
}

impl ConnectionHandler {
    /// Handle SHUTDOWN command.
    pub(crate) async fn handle_shutdown(&self, _args: &[Bytes]) -> Response {
        // Note: Actual shutdown requires signaling the main server
        // For now, we just return an error suggesting manual shutdown
        Response::error(
            "ERR SHUTDOWN is not supported in this mode. Use Ctrl+C to stop the server.",
        )
    }
}
