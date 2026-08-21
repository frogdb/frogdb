//! Replication command implementations.
//!
//! This module contains implementations for:
//! - REPLICAOF/SLAVEOF: Configure replication
//! - REPLCONF: Replication configuration exchange
//! - PSYNC: Partial synchronization protocol
//! - WAIT: Wait for replica acknowledgments

use bytes::Bytes;
use frogdb_core::clock;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use frogdb_replication::{AnnouncedOption, AnnouncementError, ReplicaInfo};

/// The client-visible error for a `REPLCONF` option that describes the replica
/// but could not be read.
///
/// One mapping, used by both halves of the split: the connection-level
/// handshake stage (`DispatchStage::ReplicationHandshake`) and the shard-path
/// `REPLCONF` executor below. The wire errors are therefore identical no matter
/// which path the option took, which is what lets the handshake move to the
/// connection without becoming a second dialect.
pub(crate) fn announcement_error(err: AnnouncementError) -> CommandError {
    match err {
        AnnouncementError::MissingPort => CommandError::WrongArity {
            command: "replconf listening-port",
        },
        // Distinct from `InvalidPort` below: this is the pre-refactor wire
        // text for a port argument that is not even valid UTF-8, restored
        // here because it cannot be told apart from "valid text that isn't a
        // number" once both collapse to the same message.
        AnnouncementError::InvalidPortEncoding => CommandError::InvalidArgument {
            message: "invalid port encoding".to_string(),
        },
        AnnouncementError::InvalidPort => CommandError::InvalidArgument {
            message: "invalid port number".to_string(),
        },
        AnnouncementError::MissingVersion => CommandError::WrongArity {
            command: "replconf frogdb-version",
        },
        AnnouncementError::InvalidVersionEncoding => CommandError::InvalidArgument {
            message: "invalid version encoding".to_string(),
        },
    }
}

/// One replica's `ROLE` entry: `[ip, announced-port, acked-offset]`.
///
/// Redis reports the port the replica *announced* over `REPLCONF
/// listening-port`, not its ephemeral source port, so an orchestrator reading
/// `ROLE` on the primary can connect straight to the replica. Split out of
/// `RoleCommand::execute` so the projection is testable without a live
/// `CommandContext` (FM-REPLICATION-049).
pub(crate) fn role_replica_entry(replica: &ReplicaInfo) -> Response {
    Response::Array(vec![
        Response::bulk(Bytes::from(replica.address.ip().to_string())),
        Response::Integer(replica.listening_port as i64),
        Response::Integer(replica.acked_offset as i64),
    ])
}

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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "REPLICAOF",
            docs: frogdb_core::CommandDocs {
                summary: "Configures a server as replica of another, or promotes it to a master.",
                since: "5.0.0",
                group: "server",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(2),
            flags: CommandFlags::ADMIN
                .union(CommandFlags::NOSCRIPT)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() != 2 {
            return Err(CommandError::WrongArity {
                command: "replicaof",
            });
        }

        // In cluster mode, replication topology is owned by the Raft metadata
        // plane (see ADR-0001). A client `REPLICAOF` would race consensus, so —
        // as Redis does — reject it; Role Demotion there is driven only by the
        // failover event consumer calling the RoleManager.
        if ctx.cluster_state.is_some() {
            return Ok(Response::error(
                "ERR REPLICAOF not allowed in cluster mode.",
            ));
        }

        let arg1 = std::str::from_utf8(&args[0]).map_err(|_| CommandError::InvalidArgument {
            message: "invalid host encoding".to_string(),
        })?;

        let arg2 = std::str::from_utf8(&args[1]).map_err(|_| CommandError::InvalidArgument {
            message: "invalid port encoding".to_string(),
        })?;

        // Role Promotion: `REPLICAOF NO ONE` — become a standalone primary.
        if arg1.eq_ignore_ascii_case("no") && arg2.eq_ignore_ascii_case("one") {
            tracing::info!("REPLICAOF NO ONE - Role Promotion to primary");
            // The RoleManager owns the flag and the streaming lifecycle: it
            // clears the replica flag and stops any inbound stream. A promotion
            // that could not mint or persist its identity returns an error and
            // leaves the node a replica, so the local view is only updated after
            // it succeeds.
            if let Some(ref controller) = ctx.role_controller {
                controller.request_promote()?;
                ctx.is_replica = false;
            } else if let Some(ref flag) = ctx.is_replica_flag {
                ctx.is_replica = false;
                // No manager wired (e.g. a bare test harness): still reflect the
                // new role rather than silently doing nothing.
                flag.store(false, std::sync::atomic::Ordering::Release);
            } else {
                ctx.is_replica = false;
            }
            return Ok(Response::ok());
        }

        // Role Demotion: `REPLICAOF <host> <port>` — become a replica.
        let host = arg1.to_string();
        let port: u16 = arg2.parse().map_err(|_| CommandError::InvalidArgument {
            message: "invalid port number".to_string(),
        })?;

        if port == 0 {
            return Err(CommandError::InvalidArgument {
                message: "port cannot be 0".to_string(),
            });
        }

        let addr = resolve_primary(&host, port)?;

        tracing::info!(host = %host, port = port, "REPLICAOF - Role Demotion to replica");

        // Hand the transition to the RoleManager: it sets the read-only flag
        // (so ROLE/INFO/the write guard report replica immediately) and opens
        // the inbound stream to `addr`. This is the operation the old stub was
        // missing — it parsed, logged, and returned +OK while flipping nothing.
        if let Some(ref controller) = ctx.role_controller {
            controller.request_demote(addr);
        } else if let Some(ref flag) = ctx.is_replica_flag {
            // No manager wired: at minimum flip the flag so the node is not left
            // lying about its role. (No stream can be opened without a manager.)
            flag.store(true, std::sync::atomic::Ordering::Release);
        }
        ctx.is_replica = true;

        Ok(Response::ok())
    }
}

/// Resolve a `REPLICAOF <host> <port>` target to a socket address.
///
/// Accepts a literal IP or a hostname (resolved via the system resolver, as
/// Redis does); the first resolved address wins.
fn resolve_primary(host: &str, port: u16) -> Result<std::net::SocketAddr, CommandError> {
    use std::net::ToSocketAddrs;

    // Fast path: a literal IP address needs no name resolution.
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(std::net::SocketAddr::new(ip, port));
    }

    (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .ok_or_else(|| CommandError::InvalidArgument {
            message: format!("cannot resolve primary host '{host}'"),
        })
}

/// SLAVEOF command - alias for REPLICAOF (deprecated).
pub struct SlaveofCommand;

impl Command for SlaveofCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "SLAVEOF",
            docs: frogdb_core::CommandDocs {
                summary: "Sets a Redis server as a replica of another, or promotes it to being a master.",
                since: "1.0.0",
                group: "server",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(2),
            flags: CommandFlags::ADMIN
                .union(CommandFlags::NOSCRIPT)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Delegate to REPLICAOF
        ReplicaofCommand.execute(ctx, args)
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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "REPLCONF",
            docs: frogdb_core::CommandDocs {
                summary: "An internal command for configuring the replication stream.",
                since: "3.0.0",
                group: "server",
                complexity: Some("O(1)"),
            },
            arity: Arity::AtLeast(0),
            flags: CommandFlags::ADMIN
                .union(CommandFlags::NOSCRIPT)
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, _ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Ok(Response::ok());
        }

        let subcommand = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid subcommand encoding".to_string(),
            })?
            .to_ascii_lowercase();

        match subcommand.as_str() {
            // The announcing options are normally intercepted by
            // `DispatchStage::ReplicationHandshake`, which is where the
            // connection that will become a replica session can actually hold
            // them. Reaching here means the option arrived on a path with no
            // connection to record it on — queued inside `MULTI`, or replayed
            // by a script. The same parser runs so validation and the reply are
            // identical on both paths; only the recording is missing, and it is
            // logged rather than silently dropped.
            "listening-port" | "capa" | "frogdb-version" => {
                let option = AnnouncedOption::parse(args).map_err(announcement_error)?;
                tracing::debug!(
                    ?option,
                    "REPLCONF announcement reached the shard path; not recorded"
                );
                Ok(Response::ok())
            }

            "ack" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "replconf ack",
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
                // A real ack solicitation never arrives here: the primary sends
                // `REPLCONF GETACK *` through the replication stream (see
                // `PrimaryReplicationHandler::request_acks`), and the replica's
                // streaming loop answers it inline with an immediate
                // `REPLCONF ACK` (`replica/streaming.rs`). This arm only serves
                // a GETACK issued as an ordinary client command, which — as in
                // Redis when no master link is involved — has nothing to ack.
                tracing::debug!("REPLCONF GETACK (client command, ignored)");
                Ok(Response::ok())
            }

            "ip-address" => {
                // Replica announcing its IP address
                if args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "replconf ip-address",
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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "PSYNC",
            docs: frogdb_core::CommandDocs {
                summary: "An internal command used in replication.",
                since: "2.8.0",
                group: "server",
                complexity: None,
            },
            arity: Arity::Fixed(2),
            flags: CommandFlags::ADMIN.union(CommandFlags::NOSCRIPT),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Replication),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // PSYNC never reaches the shard executor: `DispatchStage::ReplicationHandshake`
        // (connection/dispatch.rs) owns the whole PSYNC decision — it presence-gates
        // the primary replication handler, parses the args into a typed
        // [`PsyncHandoff`], and yields `StageOutcome::Handoff`, which the connection
        // task carries out as a raw-socket takeover. The queued-in-MULTI path is
        // short-circuited to `+OK` at EXEC (transaction.rs) *without* calling this.
        //
        // This registered `Command` executor is retained only so the `CommandSpec`
        // still carries arity/flags/`COMMAND DOCS`; it must never actually run.
        // It returns an internal error (rather than `unreachable!()`) so a future
        // dispatch-order regression that routed PSYNC here degrades gracefully with
        // an error `Response` instead of panicking and taking down the shard worker.
        Err(CommandError::Internal {
            message: "PSYNC must be intercepted before shard execution".to_string(),
        })
    }
}

/// A validated request to hand a connection's raw socket to the
/// `PrimaryReplicationHandler`. The sole product of PSYNC dispatch: it is parsed
/// exactly once (here) and consumed directly by `handle_psync`, which already
/// takes `&str` + `i64`, so there is no serialize/re-parse round-trip across the
/// command→connection seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PsyncHandoff {
    pub replication_id: String,
    pub offset: i64,
}

impl PsyncHandoff {
    /// The single PSYNC arg parse. Mirrors the branches the old
    /// `PsyncCommand::execute` performed verbatim: two args, utf8 id, `i64`
    /// offset. Arg errors surface as `CommandError` (returned to the client
    /// exactly as before via the error `Response`).
    ///
    /// The `WrongArity` branch is dead in the live dispatch path — the `Arity`
    /// stage validates PSYNC's `Fixed(2)` arity before `ReplicationHandshake` is
    /// reached — but is kept as defensive validation and unit-tested in isolation.
    pub(crate) fn from_args(args: &[Bytes]) -> Result<Self, CommandError> {
        if args.len() != 2 {
            return Err(CommandError::WrongArity { command: "psync" });
        }

        let replication_id = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid replication_id encoding".to_string(),
            })?
            .to_string();

        let offset_str =
            std::str::from_utf8(&args[1]).map_err(|_| CommandError::InvalidArgument {
                message: "invalid offset encoding".to_string(),
            })?;

        let offset: i64 = offset_str
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid offset number".to_string(),
            })?;

        tracing::info!(
            replication_id = %replication_id,
            offset = offset,
            "PSYNC request parsed - connection takeover pending"
        );

        Ok(Self {
            replication_id,
            offset,
        })
    }
}

// ============================================================================
// WAIT
// ============================================================================

/// The error Redis returns when WAIT runs on a replica (`replication.c`,
/// checked before argument parsing). Shared by the connection-level handler
/// and the in-shard (MULTI/EXEC) execution path.
pub(crate) const WAIT_ON_REPLICA_ERR: &str = "ERR WAIT cannot be used with replica instances. \
     Please also note that since Redis 4.0 if a replica is connected to a master, \
     writes are only accepted with the WRITE command flag.";

/// The error a WAIT parked across a demotion is released with, matching Redis's
/// `disconnectAllBlockedClients` (`replication.c`) verbatim. A count would be a
/// worse answer than an error: the stream it counted acks on is gone.
///
/// The same string the shards answer their own parked waiters with on a
/// demotion, aliased rather than repeated so the two halves of one event cannot
/// drift apart (`specs/blocking.md` FM-BLOCKING-007, FM-BLOCKING-010).
pub(crate) const WAIT_ROLE_CHANGED_ERR: &str = frogdb_core::ROLE_CHANGED_UNBLOCK_ERR;

/// Parse and validate `WAIT <numreplicas> <timeout_ms>` arguments.
///
/// One definition shared by the connection-level blocking path and the
/// in-shard non-blocking path, so the two can never diverge on validation.
/// Mirrors Redis: `numreplicas` is a non-negative integer; `timeout` is a
/// non-negative i64 in milliseconds that must survive `now + timeout` without
/// overflow (`mstime() + timeout` in Redis); `timeout = 0` means no deadline.
pub(crate) fn parse_wait_args(args: &[Bytes]) -> Result<(u32, u64), CommandError> {
    if args.len() != 2 {
        return Err(CommandError::WrongArity { command: "wait" });
    }

    let num_replicas: u32 = std::str::from_utf8(&args[0])
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid numreplicas encoding".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "numreplicas must be a non-negative integer".to_string(),
        })?;

    // Parse timeout as i64 first to detect overflow
    let timeout_str = std::str::from_utf8(&args[1]).map_err(|_| CommandError::InvalidArgument {
        message: "invalid timeout encoding".to_string(),
    })?;
    let timeout_i64: i64 = timeout_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "timeout is not an integer or out of range".to_string(),
        })?;
    if timeout_i64 < 0 {
        return Err(CommandError::InvalidArgument {
            message: "timeout is negative".to_string(),
        });
    }
    // Check for overflow when adding current time (same as Redis mstime() + timeout)
    let now_ms = clock::system_now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    if timeout_i64.checked_add(now_ms).is_none() {
        return Err(CommandError::InvalidArgument {
            message: "timeout is out of range".to_string(),
        });
    }

    Ok((num_replicas, timeout_i64 as u64))
}

/// WAIT command - wait for replica acknowledgments.
///
/// Usage: `WAIT <numreplicas> <timeout_ms>`
///
/// Blocks until the specified number of replicas have acknowledged all
/// writes up to this point, or until the timeout expires (`timeout 0` blocks
/// until the quorum is reached). Returns the number of replicas that
/// acknowledged.
///
/// Dispatch shape (mirrors PSYNC's special case): the blocking path lives in
/// the connection handler (`handle_wait_command`, intercepted by name in
/// dispatch), which drives the replication crate's `WaitCoordinator`. The
/// strategy stays `Standard` so that inside MULTI/EXEC the command is queued
/// and executed on the shard, where [`Command::execute`] below mirrors Redis's
/// `CLIENT_DENY_BLOCKING` fast path: return the current acked count
/// immediately, never block.
///
/// WAIT is keyless and per-node in both standalone and cluster mode: it counts
/// the replicas attached to *this* node and never redirects. See
/// [`crate::connection::ConnectionHandler::handle_wait_command`] for the full
/// contract.
pub struct WaitCommand;

impl Command for WaitCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "WAIT",
            docs: frogdb_core::CommandDocs {
                summary: "Blocks until the asynchronous replication of all preceding write commands sent by the connection is completed.",
                since: "3.0.0",
                group: "generic",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(2),
            flags: CommandFlags::NOSCRIPT,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Redis rejects WAIT on replicas before parsing arguments.
        if ctx.is_replica {
            return Ok(Response::error(WAIT_ON_REPLICA_ERR));
        }

        let (_num_replicas, _timeout_ms) = parse_wait_args(args)?;

        // Deny-blocking context (MULTI/EXEC executes queued commands on the
        // shard): return the acked count for the current live offset without
        // blocking, exactly like Redis's CLIENT_DENY_BLOCKING branch.
        let count = ctx
            .replication_tracker
            .map(|t| t.count_acked(t.current_offset()))
            .unwrap_or(0);
        Ok(Response::Integer(count as i64))
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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ROLE",
            docs: frogdb_core::CommandDocs {
                summary: "Returns the replication role.",
                since: "2.8.12",
                group: "server",
                complexity: Some("O(1)"),
            },
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE)
                .union(CommandFlags::FAST)
                .union(CommandFlags::NOSCRIPT),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, _args: &[Bytes]) -> Result<Response, CommandError> {
        if ctx.is_replica {
            // Replica role: ["slave", <master_host>, <master_port>, <state>, <offset>]
            //
            // `master_host`/`master_port` come from the RoleManager's live
            // `primary_target` (via `ShardIdentity`, threaded into every
            // `CommandContext`) — the same single source INFO's replication
            // section reads, seeded at boot from `replicaof` config and kept
            // current by every runtime Role Demotion.
            let master_host = ctx.master_host.clone().unwrap_or_default();
            let master_port = ctx.master_port.unwrap_or(0);
            Ok(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"slave")),
                Response::bulk(Bytes::from(master_host)),
                Response::Integer(master_port as i64),
                Response::bulk(Bytes::from_static(b"connected")),
                Response::Integer(0), // replication offset
            ]))
        } else {
            // Master/standalone role: ["master", <offset>, [<replicas>]]
            let offset = ctx
                .replication_tracker
                .map(|t| t.current_offset() as i64)
                .unwrap_or(0);

            // `get_streaming_replicas` is the shared acked-offset projection:
            // the replicas listed here are exactly the ones WAIT's quorum
            // count (`count_acked`) considers, with the same acked offsets.
            let replicas = ctx
                .replication_tracker
                .map(|t| {
                    t.get_streaming_replicas()
                        .iter()
                        .map(role_replica_entry)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            Ok(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"master")),
                Response::Integer(offset),
                Response::Array(replicas),
            ]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PsyncHandoff, announcement_error, role_replica_entry};
    use bytes::Bytes;
    use frogdb_core::CommandError;
    use frogdb_protocol::Response;
    use frogdb_replication::{
        AnnouncementError, Phase, ReplicaCapabilities, ReplicaInfo, ReplicaSession,
    };
    use std::time::Instant;

    /// A snapshot shaped like one a completed handshake produces.
    fn streaming_replica(listening_port: u16) -> ReplicaInfo {
        let session = ReplicaSession::new(1, "127.0.0.1:54321".parse().unwrap());
        ReplicaInfo {
            listening_port,
            acked_offset: 99,
            phase: Phase::Streaming,
            capabilities: ReplicaCapabilities {
                eof: true,
                psync2: true,
            },
            last_ack_time: Instant::now(),
            ..session.snapshot()
        }
    }

    // FM-REPLICATION-049
    /// `ROLE` on a primary reports the port the replica *announced*, so an
    /// orchestrator can dial it — not the placeholder `0` that the discarded
    /// `REPLCONF listening-port` used to leave behind (issue 16).
    #[test]
    fn role_reports_the_announced_port() {
        let entry = role_replica_entry(&streaming_replica(7001));
        let Response::Array(fields) = entry else {
            panic!("a ROLE replica entry is an array");
        };
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], Response::bulk(Bytes::from_static(b"127.0.0.1")));
        assert_eq!(fields[1], Response::Integer(7001));
        assert_eq!(fields[2], Response::Integer(99));
    }

    // FM-REPLICATION-018
    /// The announcement rejections keep the wire errors the shard-path
    /// `REPLCONF` executor used to raise, so moving the handshake to the
    /// connection did not change what a client sees. `InvalidPortEncoding` and
    /// `InvalidPort` are pinned to their exact, distinct wire text: a
    /// non-UTF-8 port argument and a UTF-8 argument that just isn't a `u16`
    /// were told apart before this crate's refactor, and collapsing them to
    /// one message would be a silent regression a `matches!` on the error
    /// variant alone would not catch.
    #[test]
    fn announcement_errors_keep_their_wire_shape() {
        assert!(matches!(
            announcement_error(AnnouncementError::MissingPort),
            CommandError::WrongArity {
                command: "replconf listening-port"
            }
        ));
        assert_eq!(
            announcement_error(AnnouncementError::InvalidPortEncoding).to_string(),
            "ERR invalid port encoding"
        );
        assert_eq!(
            announcement_error(AnnouncementError::InvalidPort).to_string(),
            "ERR invalid port number"
        );
        // The two `frogdb-version` shapes keep the wire text the hand-rolled
        // arm produced before the option joined the announcement parser.
        assert!(matches!(
            announcement_error(AnnouncementError::MissingVersion),
            CommandError::WrongArity {
                command: "replconf frogdb-version"
            }
        ));
        assert_eq!(
            announcement_error(AnnouncementError::InvalidVersionEncoding).to_string(),
            "ERR invalid version encoding"
        );
    }

    /// The happy path: a well-formed `PSYNC <replid> <offset>` parses into a
    /// typed [`PsyncHandoff`] carrying exactly those values — asserted directly,
    /// with no socket and no live primary handler. This is the seam the old
    /// sentinel-array `Response` could not be unit-tested at.
    #[test]
    fn from_args_parses_id_and_offset() {
        let handoff = PsyncHandoff::from_args(&[
            Bytes::from_static(b"repl-abc"),
            Bytes::from_static(b"12345"),
        ])
        .expect("valid PSYNC args parse");
        assert_eq!(handoff.replication_id, "repl-abc");
        assert_eq!(handoff.offset, 12345);
    }

    /// A negative offset (`?  -1`, the initial-sync sentinel) is a valid `i64`
    /// and must parse — this is the most common real-world PSYNC.
    #[test]
    fn from_args_accepts_negative_offset() {
        let handoff =
            PsyncHandoff::from_args(&[Bytes::from_static(b"?"), Bytes::from_static(b"-1")])
                .expect("`? -1` initial sync parses");
        assert_eq!(handoff.replication_id, "?");
        assert_eq!(handoff.offset, -1);
    }

    /// Wrong arg count → `WrongArity`. NOTE: this branch is *dead in the live
    /// dispatch path* — the `Arity` stage validates PSYNC's `Fixed(2)` arity
    /// before `ReplicationHandshake` is reached, so `from_args` never sees a wrong arg
    /// count in production. This asserts `from_args` as a standalone function
    /// (defensive validation), not production wire behavior (already covered by
    /// the `Arity` stage and `test_psync_invalid_args`).
    #[test]
    fn from_args_wrong_arity() {
        let err = PsyncHandoff::from_args(&[Bytes::from_static(b"only-one")]).unwrap_err();
        assert!(matches!(err, CommandError::WrongArity { command: "psync" }));
    }

    /// Non-utf8 replication id → `InvalidArgument`. This branch *is* on the live
    /// path: the intercept reaches it after arity has passed.
    #[test]
    fn from_args_non_utf8_id() {
        let err =
            PsyncHandoff::from_args(&[Bytes::from_static(&[0xff, 0xfe]), Bytes::from_static(b"0")])
                .unwrap_err();
        assert!(matches!(err, CommandError::InvalidArgument { .. }));
    }

    /// Non-numeric offset → `InvalidArgument` — mirrors the wire assertion in
    /// `test_psync_invalid_args` (`PSYNC ? not_a_number`). Live path.
    #[test]
    fn from_args_non_numeric_offset() {
        let err = PsyncHandoff::from_args(&[
            Bytes::from_static(b"?"),
            Bytes::from_static(b"not_a_number"),
        ])
        .unwrap_err();
        assert!(matches!(err, CommandError::InvalidArgument { .. }));
    }
}
