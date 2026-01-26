//! Replication command implementations.
//!
//! This module contains implementations for:
//! - REPLICAOF/SLAVEOF: Configure replication
//! - REPLCONF: Replication configuration exchange
//! - PSYNC: Partial synchronization protocol
//! - WAIT: Wait for replica acknowledgments

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

// ============================================================================
// REPLICAOF / SLAVEOF
// ============================================================================

/// REPLICAOF command - configure this node as a replica of another node.
///
/// Usage:
/// - `REPLICAOF <host> <port>` - Become replica of the specified primary
/// - `REPLICAOF NO ONE` - Stop replication, become standalone
pub struct ReplicaofCommand;

impl Command for ReplicaofCommand {
    fn name(&self) -> &'static str {
        "REPLICAOF"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::STALE
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.len() != 2 {
            return Err(CommandError::WrongArity {
                command: "REPLICAOF",
            });
        }

        let arg1 = std::str::from_utf8(&args[0]).map_err(|_| CommandError::InvalidArgument {
            message: "invalid host encoding".to_string(),
        })?;

        let arg2 = std::str::from_utf8(&args[1]).map_err(|_| CommandError::InvalidArgument {
            message: "invalid port encoding".to_string(),
        })?;

        // Check for "NO ONE" to stop replication
        if arg1.eq_ignore_ascii_case("no") && arg2.eq_ignore_ascii_case("one") {
            // Stop replication, become standalone
            // Note: The actual state change will be handled by the server's
            // replication manager which has access to the connection state
            tracing::info!("REPLICAOF NO ONE - stopping replication");

            // Return OK - the actual reconfiguration happens asynchronously
            // The server's replication manager will pick up this change
            return Ok(Response::ok());
        }

        // Parse host and port
        let host = arg1.to_string();
        let port: u16 = arg2.parse().map_err(|_| CommandError::InvalidArgument {
            message: "invalid port number".to_string(),
        })?;

        if port == 0 {
            return Err(CommandError::InvalidArgument {
                message: "port cannot be 0".to_string(),
            });
        }

        tracing::info!(
            host = %host,
            port = port,
            "REPLICAOF - configuring as replica"
        );

        // Return OK - the actual connection happens asynchronously
        // The server's replication manager will initiate the connection
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

/// SLAVEOF command - alias for REPLICAOF (deprecated).
pub struct SlaveofCommand;

impl Command for SlaveofCommand {
    fn name(&self) -> &'static str {
        "SLAVEOF"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::STALE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Delegate to REPLICAOF
        ReplicaofCommand.execute(ctx, args)
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// ============================================================================
// REPLCONF
// ============================================================================

/// REPLCONF command - replication configuration exchange.
///
/// Used during the replication handshake:
/// - `REPLCONF listening-port <port>` - Announce replica's listening port
/// - `REPLCONF capa <cap> [<cap> ...]` - Announce capabilities
/// - `REPLCONF ACK <offset>` - Acknowledge replication offset
/// - `REPLCONF GETACK *` - Request ACK from replica
pub struct ReplconfCommand;

impl Command for ReplconfCommand {
    fn name(&self) -> &'static str {
        "REPLCONF"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Ok(Response::ok());
        }

        let subcommand = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid subcommand encoding".to_string(),
            })?
            .to_ascii_lowercase();

        match subcommand.as_str() {
            "listening-port" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "REPLCONF listening-port",
                    });
                }

                let port: u16 = std::str::from_utf8(&args[1])
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "invalid port encoding".to_string(),
                    })?
                    .parse()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "invalid port number".to_string(),
                    })?;

                tracing::debug!(port = port, "REPLCONF listening-port");
                // The server will store this in the replica connection state
                Ok(Response::ok())
            }

            "capa" => {
                // Capability negotiation
                let capabilities: Vec<&str> = args[1..]
                    .iter()
                    .filter_map(|b| std::str::from_utf8(b).ok())
                    .collect();

                tracing::debug!(capabilities = ?capabilities, "REPLCONF capa");
                // The server will store these in the replica connection state
                Ok(Response::ok())
            }

            "ack" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "REPLCONF ACK",
                    });
                }

                let offset: u64 = std::str::from_utf8(&args[1])
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "invalid offset encoding".to_string(),
                    })?
                    .parse()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "invalid offset number".to_string(),
                    })?;

                tracing::trace!(offset = offset, "REPLCONF ACK");
                // The server's replication tracker will record this ACK
                // In Redis, REPLCONF ACK doesn't send a response - it's one-way
                // We return OK but the connection handler should suppress the response
                Ok(Response::ok())
            }

            "getack" => {
                // Primary requesting ACK from replica
                // The replica should respond with REPLCONF ACK <offset>
                tracing::debug!("REPLCONF GETACK");
                // Return OK - the replica connection handler will handle sending the ACK
                Ok(Response::ok())
            }

            "ip-address" => {
                // Replica announcing its IP address
                if args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "REPLCONF ip-address",
                    });
                }
                tracing::debug!(ip = ?args[1], "REPLCONF ip-address");
                Ok(Response::ok())
            }

            "capa-eof" | "eof" => {
                // EOF capability announcement
                tracing::debug!("REPLCONF eof capability");
                Ok(Response::ok())
            }

            "rdb-only" => {
                // Request RDB-only sync (no replication stream)
                tracing::debug!("REPLCONF rdb-only");
                Ok(Response::ok())
            }

            "rdb-filter-only" => {
                // Request filtered RDB transfer
                tracing::debug!("REPLCONF rdb-filter-only");
                Ok(Response::ok())
            }

            _ => {
                tracing::warn!(subcommand = %subcommand, "Unknown REPLCONF subcommand");
                // Unknown subcommands should still return OK for forward compatibility
                Ok(Response::ok())
            }
        }
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// ============================================================================
// PSYNC
// ============================================================================

/// PSYNC command - partial/full synchronization request.
///
/// Usage:
/// - `PSYNC <replication_id> <offset>` - Request sync from given position
/// - `PSYNC ? -1` - Request full sync (new replica)
///
/// Responses:
/// - `+FULLRESYNC <replication_id> <offset>` - Full sync will follow
/// - `+CONTINUE` - Partial sync, streaming will continue
/// - `+CONTINUE <replication_id>` - Partial sync with new ID
pub struct PsyncCommand;

impl Command for PsyncCommand {
    fn name(&self) -> &'static str {
        "PSYNC"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.len() != 2 {
            return Err(CommandError::WrongArity {
                command: "PSYNC",
            });
        }

        let replication_id = std::str::from_utf8(&args[0]).map_err(|_| {
            CommandError::InvalidArgument {
                message: "invalid replication_id encoding".to_string(),
            }
        })?;

        let offset_str =
            std::str::from_utf8(&args[1]).map_err(|_| CommandError::InvalidArgument {
                message: "invalid offset encoding".to_string(),
            })?;

        let offset: i64 = offset_str.parse().map_err(|_| CommandError::InvalidArgument {
            message: "invalid offset number".to_string(),
        })?;

        tracing::info!(
            replication_id = %replication_id,
            offset = offset,
            "PSYNC request"
        );

        // The actual PSYNC logic is handled by the replication connection handler,
        // not in this command. This command just parses and validates.
        //
        // The real implementation will be in the primary replication handler which
        // has access to the replication state and can initiate the appropriate sync type.
        //
        // PSYNC responses (+FULLRESYNC, +CONTINUE) bypass normal command response
        // handling and are sent directly by the replication handler.

        // Check if this is a full sync request
        if replication_id == "?" && offset == -1 {
            // Full sync requested - will be handled by replication handler
            tracing::info!("Full sync requested");
        }

        // Return OK as placeholder - actual response comes from replication handler
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// ============================================================================
// WAIT
// ============================================================================

/// WAIT command - wait for replica acknowledgments.
///
/// Usage: `WAIT <numreplicas> <timeout_ms>`
///
/// Blocks until the specified number of replicas have acknowledged all
/// writes up to this point, or until the timeout expires.
///
/// Returns the number of replicas that acknowledged within the timeout.
pub struct WaitCommand;

impl Command for WaitCommand {
    fn name(&self) -> &'static str {
        "WAIT"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::NOSCRIPT
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.len() != 2 {
            return Err(CommandError::WrongArity {
                command: "WAIT",
            });
        }

        let num_replicas: u32 = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid numreplicas encoding".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "numreplicas must be a non-negative integer".to_string(),
            })?;

        let timeout_ms: u64 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid timeout encoding".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "timeout must be a non-negative integer".to_string(),
            })?;

        tracing::debug!(
            num_replicas = num_replicas,
            timeout_ms = timeout_ms,
            "WAIT command"
        );

        // Return a BlockingNeeded response that tells the connection handler
        // to perform the wait operation using the replication tracker.
        Ok(Response::BlockingNeeded {
            keys: vec![], // WAIT doesn't use keys, it waits for replica ACKs
            timeout: timeout_ms as f64 / 1000.0, // Convert ms to seconds for consistency
            op: frogdb_protocol::BlockingOp::Wait { num_replicas, timeout_ms },
        })
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// ============================================================================
// ROLE
// ============================================================================

/// ROLE command - return the replication role of this node.
///
/// Returns an array with role information:
/// - For master: ["master", <replication_offset>, [[<replica_ip>, <replica_port>, <replica_offset>], ...]]
/// - For slave: ["slave", <master_host>, <master_port>, <state>, <replication_offset>]
pub struct RoleCommand;

impl Command for RoleCommand {
    fn name(&self) -> &'static str {
        "ROLE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
            | CommandFlags::LOADING
            | CommandFlags::STALE
            | CommandFlags::FAST
            | CommandFlags::NOSCRIPT
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // For now, return master role with no replicas
        // The full implementation will check the replication configuration
        Ok(Response::Array(vec![
            Response::bulk(Bytes::from_static(b"master")),
            Response::Integer(0), // replication offset
            Response::Array(vec![]), // no replicas yet
        ]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}
