//! The CLIENT connection command.
//!
//! CLIENT is the god-object exemplar of this refactor: its ~18 subcommands
//! (ID, GETNAME, SETNAME, SETINFO, LIST, INFO, KILL, PAUSE, UNPAUSE, NO-EVICT,
//! NO-TOUCH, REPLY, UNBLOCK, TRACKING, TRACKINGINFO, CACHING, GETREDIR, STATS,
//! HELP) all mutate or read per-connection and server-wide client state. It is
//! migrated behind the [`ConnectionCommand`] seam as a *mutating* connection
//! command: it dispatches through [`ConnectionHandler::conn_ctx_clientmut`],
//! whose [`ConnCtx`] carries both the mutable connection-state capability
//! ([`ConnCtx::conn_state`] = `Some`) and the client-tracking IO plumbing
//! ([`ConnCtx::tracking`] = `Some`).
//!
//! Subsystem access:
//! * per-connection state (name, reply mode, tracking flags/redirect, caching
//!   override, buffered stats) → [`ConnCtx::conn_state`] ([`ConnStateMut`]);
//! * the shared client registry (LIST/INFO/KILL/PAUSE/flags/UNBLOCK/stats) →
//!   [`ConnCtx::client_registry`];
//! * CLIENT TRACKING's channel/task/shard registration → [`ConnCtx::tracking`]
//!   ([`ClientTrackingProvider`]) plus [`ConnCtx::shard_senders`].

use std::net::SocketAddr;

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, ClientFlags, ClientInfo, ClientRegistry, ClientStats,
    CommandFlags, CommandSpec, ConnCtx, ConnectionCommand, ConnectionLevelOp, EventSpec,
    ExecutionStrategy, KeySpec, KillFilter, LookupSpec, PauseMode, TrackingModeView, UnblockMode,
    WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// The `CommandSpec` for CLIENT. Strategy is `ConnectionLevel(Admin)`, validated
/// by the registry against the `Connection` executor variant.
static CLIENT_SPEC: CommandSpec = CommandSpec {
    name: "CLIENT",
    arity: Arity::AtLeast(1),
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
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` CLIENT executor.
pub(crate) static CLIENT_CONN_COMMAND: ClientConnCommand = ClientConnCommand;

/// CLIENT — connection management (mutates connection + client state).
pub(crate) struct ClientConnCommand;

impl ConnectionCommand for ClientConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &CLIENT_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            if args.is_empty() {
                return Response::error("ERR wrong number of arguments for 'client' command");
            }

            let subcommand = args[0].to_ascii_uppercase();
            let subcommand_str = String::from_utf8_lossy(&subcommand);

            match subcommand_str.as_ref() {
                "ID" => client_id(ctx),
                "SETNAME" => client_setname(ctx, &args[1..]),
                "GETNAME" => client_getname(ctx),
                "LIST" => client_list(ctx, &args[1..]),
                "INFO" => client_info(ctx),
                "KILL" => client_kill(ctx, &args[1..]),
                "PAUSE" => client_pause(ctx, &args[1..]),
                "UNPAUSE" => client_unpause(ctx),
                "SETINFO" => client_setinfo(ctx, &args[1..]),
                "NO-EVICT" => client_no_evict(ctx, &args[1..]),
                "NO-TOUCH" => client_no_touch(ctx, &args[1..]),
                "TRACKING" => client_tracking(ctx, &args[1..]).await,
                "TRACKINGINFO" => client_trackinginfo(ctx),
                "GETREDIR" => client_getredir(ctx),
                "CACHING" => client_caching(ctx, &args[1..]),
                "REPLY" => client_reply(ctx, &args[1..]),
                "UNBLOCK" => client_unblock(ctx, &args[1..]),
                "STATS" => client_stats(ctx, &args[1..]),
                "HELP" => client_help(),
                _ => Response::error(format!(
                    "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                    subcommand_str
                )),
            }
        })
    }
}

/// This connection's id, read through the mutable connection-state capability.
fn conn_id(ctx: &ConnCtx<'_>) -> u64 {
    ctx.conn_state
        .as_deref()
        .expect("CLIENT is dispatched with a mutable conn_state")
        .id()
}

/// Build a CLIENT LIST entry that includes `tot-cmds` from per-client stats.
///
/// Redis includes `tot-cmds` in CLIENT LIST output. The base
/// `ClientInfo::to_client_list_entry()` does not include it, so we append it
/// here by looking up stats from the registry.
fn client_list_entry_with_stats(info: &ClientInfo, registry: &ClientRegistry) -> String {
    let base = info.to_client_list_entry();
    let tot_cmds = registry
        .get_stats(info.id)
        .map(|s| s.commands_total)
        .unwrap_or(0);
    format!("{base} tot-cmds={tot_cmds}")
}

/// Format a CLIENT STATS entry for a single client.
fn format_client_stats_entry(info: &ClientInfo, stats: &ClientStats) -> String {
    use std::fmt::Write;

    let mut output = String::new();

    // Basic info line
    let name_str = info
        .name
        .as_ref()
        .map(|n| String::from_utf8_lossy(n).to_string())
        .unwrap_or_default();

    writeln!(
        &mut output,
        "id={} addr={} name={}",
        info.id, info.addr, name_str
    )
    .unwrap();

    // Stats line
    writeln!(
        &mut output,
        "cmd_total={} bytes_recv={} bytes_sent={}",
        stats.commands_total, stats.bytes_recv, stats.bytes_sent
    )
    .unwrap();

    // Latency metrics
    writeln!(
        &mut output,
        "latency_avg_us={} latency_p99_us={} latency_max_us={}",
        stats.avg_latency_us(),
        stats.p99_latency_us(),
        stats.latency_max_us
    )
    .unwrap();

    // Command breakdown (top 10 by count)
    if !stats.command_counts.is_empty() {
        output.push_str("# command breakdown (top 10):\n");

        // Sort by count descending
        let mut cmd_entries: Vec<_> = stats.command_counts.iter().collect();
        cmd_entries.sort_by(|a, b| b.1.count.cmp(&a.1.count));

        for (cmd_name, cmd_stats) in cmd_entries.iter().take(10) {
            writeln!(
                &mut output,
                "{}: count={} avg_us={} max_us={}",
                cmd_name,
                cmd_stats.count,
                cmd_stats.avg_latency_us(),
                cmd_stats.latency_max_us
            )
            .unwrap();
        }
    }

    // Remove trailing newline
    if output.ends_with('\n') {
        output.pop();
    }

    output
}

/// CLIENT ID — return connection ID.
fn client_id(ctx: &ConnCtx<'_>) -> Response {
    Response::Integer(conn_id(ctx) as i64)
}

/// CLIENT SETNAME — set connection name.
fn client_setname(ctx: &mut ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|setname' command");
    }

    let name = &args[0];

    // Validate name: no spaces allowed
    if name.contains(&b' ') {
        return Response::error("ERR Client names cannot contain spaces");
    }

    // Empty name clears the name
    let name_opt = if name.is_empty() {
        None
    } else {
        Some(name.clone())
    };

    let id = conn_id(ctx);
    // Update local state
    ctx.conn_state
        .as_deref_mut()
        .expect("CLIENT is dispatched with a mutable conn_state")
        .set_name(name_opt.clone());

    // Update in registry
    ctx.client_registry.update_name(id, name_opt);

    Response::ok()
}

/// CLIENT GETNAME — get connection name.
fn client_getname(ctx: &ConnCtx<'_>) -> Response {
    match ctx
        .conn_state
        .as_deref()
        .expect("CLIENT is dispatched with a mutable conn_state")
        .name()
    {
        Some(name) => Response::bulk(name),
        None => Response::null(),
    }
}

/// CLIENT LIST — list all connections.
fn client_list(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    // Parse optional TYPE filter
    let mut filter_type: Option<&str> = None;

    let mut i = 0;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"TYPE" => {
                i += 1;
                if i >= args.len() {
                    return Response::error("ERR syntax error");
                }
                let type_str = String::from_utf8_lossy(&args[i]).to_lowercase();
                match type_str.as_str() {
                    "normal" | "master" | "replica" | "pubsub" => {
                        filter_type = match type_str.as_str() {
                            "normal" => Some("normal"),
                            "master" => Some("master"),
                            "replica" => Some("replica"),
                            "pubsub" => Some("pubsub"),
                            _ => None,
                        };
                    }
                    _ => {
                        return Response::error(format!("ERR Unknown client type '{}'", type_str));
                    }
                }
            }
            b"ID" => {
                // CLIENT LIST ID id1 id2 ... - filter by client IDs
                i += 1;
                let mut ids = Vec::new();
                while i < args.len() {
                    let id_str = String::from_utf8_lossy(&args[i]);
                    match id_str.parse::<u64>() {
                        Ok(id) => ids.push(id),
                        Err(_) => {
                            return Response::error("ERR Invalid client ID");
                        }
                    }
                    i += 1;
                }
                if ids.is_empty() {
                    return Response::error(
                        "ERR wrong number of arguments for 'client|list' command",
                    );
                }
                // Return only clients matching these IDs
                let clients = ctx.client_registry.list();
                let mut output = String::new();
                for info in clients {
                    if ids.contains(&info.id) {
                        output.push_str(&client_list_entry_with_stats(&info, ctx.client_registry));
                        output.push('\n');
                    }
                }
                return Response::bulk(Bytes::from(output));
            }
            _ => {
                return Response::error(format!(
                    "ERR syntax error, expected 'TYPE' but got '{}'",
                    String::from_utf8_lossy(&opt)
                ));
            }
        }
        i += 1;
    }

    // Get all clients from registry
    let clients = ctx.client_registry.list();

    // Build output
    let mut output = String::new();
    for info in clients {
        // Apply type filter
        if let Some(ft) = filter_type
            && info.client_type() != ft
        {
            continue;
        }

        output.push_str(&client_list_entry_with_stats(&info, ctx.client_registry));
        output.push('\n');
    }

    Response::bulk(Bytes::from(output))
}

/// CLIENT INFO — get current connection info.
fn client_info(ctx: &ConnCtx<'_>) -> Response {
    match ctx.client_registry.get(conn_id(ctx)) {
        Some(info) => {
            let entry = client_list_entry_with_stats(&info, ctx.client_registry);
            Response::bulk(Bytes::from(entry + "\n"))
        }
        None => Response::error("ERR client not found"),
    }
}

/// CLIENT KILL — terminate connections.
fn client_kill(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|kill' command");
    }

    let id = conn_id(ctx);

    // Old-style syntax: CLIENT KILL addr:port
    if args.len() == 1 && !args[0].contains(&b' ') {
        let addr_str = String::from_utf8_lossy(&args[0]);
        let addr: SocketAddr = match addr_str.parse() {
            Ok(a) => a,
            Err(_) => return Response::error("ERR Invalid IP:port pair"),
        };

        let filter = KillFilter {
            addr: Some(addr),
            skip_me: true,
            current_conn_id: Some(id),
            ..Default::default()
        };

        let killed = ctx.client_registry.kill_by_filter(&filter);
        if killed > 0 {
            return Response::ok();
        } else {
            return Response::error("ERR No such client");
        }
    }

    // New-style syntax: CLIENT KILL [ID id] [ADDR ip:port] [LADDR ip:port] [TYPE type] [USER username] [SKIPME yes|no]
    let mut filter = KillFilter {
        skip_me: true, // Default is to skip ourselves
        current_conn_id: Some(id),
        ..Default::default()
    };

    let mut i = 0;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"ID" => {
                i += 1;
                if i >= args.len() {
                    return Response::error("ERR syntax error");
                }
                let id_str = String::from_utf8_lossy(&args[i]);
                let id: u64 = match id_str.parse() {
                    Ok(id) if id > 0 => id,
                    Ok(_) => return Response::error("ERR client-id should be greater than 0"),
                    Err(_) => return Response::error("ERR client-id is not an integer"),
                };
                filter.id = Some(id);
            }
            b"ADDR" => {
                i += 1;
                if i >= args.len() {
                    return Response::error("ERR syntax error");
                }
                let addr_str = String::from_utf8_lossy(&args[i]);
                filter.addr = match addr_str.parse() {
                    Ok(a) => Some(a),
                    Err(_) => return Response::error("ERR Invalid IP:port pair"),
                };
            }
            b"LADDR" => {
                i += 1;
                if i >= args.len() {
                    return Response::error("ERR syntax error");
                }
                let addr_str = String::from_utf8_lossy(&args[i]);
                filter.laddr = match addr_str.parse() {
                    Ok(a) => Some(a),
                    Err(_) => return Response::error("ERR Invalid IP:port pair"),
                };
            }
            b"TYPE" => {
                i += 1;
                if i >= args.len() {
                    return Response::error("ERR syntax error");
                }
                let type_str = String::from_utf8_lossy(&args[i]).to_lowercase();
                match type_str.as_str() {
                    "normal" | "master" | "replica" | "pubsub" => {
                        filter.client_type = Some(type_str);
                    }
                    _ => {
                        return Response::error(format!("ERR Unknown client type '{}'", type_str));
                    }
                }
            }
            b"USER" => {
                // USER filter - noop for now (ACL not implemented)
                i += 1;
                if i >= args.len() {
                    return Response::error("ERR syntax error");
                }
                // TODO: Implement in Phase 10.5 (ACL)
            }
            b"SKIPME" => {
                i += 1;
                if i >= args.len() {
                    return Response::error("ERR syntax error");
                }
                let val = args[i].to_ascii_lowercase();
                match val.as_slice() {
                    b"yes" => filter.skip_me = true,
                    b"no" => filter.skip_me = false,
                    _ => return Response::error("ERR syntax error"),
                }
            }
            _ => {
                return Response::error(format!(
                    "ERR syntax error, expected filter but got '{}'",
                    String::from_utf8_lossy(&opt)
                ));
            }
        }
        i += 1;
    }

    // Must have at least one filter, or SKIPME (which implies "all clients")
    let has_filter = filter.id.is_some()
        || filter.addr.is_some()
        || filter.laddr.is_some()
        || filter.client_type.is_some();
    let has_skipme = args.iter().any(|a| a.to_ascii_uppercase() == b"SKIPME");
    if !has_filter && !has_skipme {
        return Response::error("ERR syntax error");
    }

    let killed = ctx.client_registry.kill_by_filter(&filter);
    Response::Integer(killed as i64)
}

/// CLIENT PAUSE — pause command execution.
fn client_pause(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|pause' command");
    }

    // Parse timeout in milliseconds
    let timeout_str = String::from_utf8_lossy(&args[0]);
    let timeout_ms: u64 = match timeout_str.parse() {
        Ok(t) => t,
        Err(_) => return Response::error("ERR timeout is not an integer or out of range"),
    };

    // Parse optional mode (WRITE or ALL)
    let mode = if args.len() > 1 {
        let mode_str = args[1].to_ascii_uppercase();
        match mode_str.as_slice() {
            b"WRITE" => PauseMode::Write,
            b"ALL" => PauseMode::All,
            _ => {
                return Response::error("ERR pause mode must be either WRITE or ALL");
            }
        }
    } else {
        PauseMode::All // Default mode per Redis 7.x
    };

    ctx.client_registry.pause(mode, timeout_ms);
    Response::ok()
}

/// CLIENT UNPAUSE — resume command execution.
fn client_unpause(ctx: &ConnCtx<'_>) -> Response {
    ctx.client_registry.unpause();
    Response::ok()
}

/// CLIENT SETINFO — set client library info.
fn client_setinfo(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.len() < 2 {
        return Response::error("ERR wrong number of arguments for 'client|setinfo' command");
    }

    let attr = args[0].to_ascii_uppercase();
    let value = &args[1];
    let id = conn_id(ctx);

    match attr.as_slice() {
        b"LIB-NAME" => {
            // Validate: no spaces or newlines allowed
            if value.iter().any(|&b| b == b' ' || b == b'\n' || b == b'\r') {
                return Response::error(
                    "ERR lib-name cannot contain spaces, newlines or special characters",
                );
            }
            ctx.client_registry
                .update_lib_info(id, Some(value.clone()), None);
            Response::ok()
        }
        b"LIB-VER" => {
            // Validate: no spaces or newlines allowed
            if value.iter().any(|&b| b == b' ' || b == b'\n' || b == b'\r') {
                return Response::error(
                    "ERR lib-ver cannot contain spaces, newlines or special characters",
                );
            }
            ctx.client_registry
                .update_lib_info(id, None, Some(value.clone()));
            Response::ok()
        }
        _ => Response::error(format!(
            "ERR unknown attribute '{}'",
            String::from_utf8_lossy(&attr)
        )),
    }
}

/// CLIENT NO-EVICT — protect client from eviction.
fn client_no_evict(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|no-evict' command");
    }

    let mode = args[0].to_ascii_uppercase();
    let id = conn_id(ctx);
    let info = match ctx.client_registry.get(id) {
        Some(info) => info,
        None => return Response::error("ERR client not found"),
    };

    let mut flags = info.flags;

    match mode.as_slice() {
        b"ON" => {
            flags |= ClientFlags::NO_EVICT;
        }
        b"OFF" => {
            flags.remove(ClientFlags::NO_EVICT);
        }
        _ => {
            return Response::error("ERR argument must be 'ON' or 'OFF'");
        }
    }

    ctx.client_registry.update_flags(id, flags);
    Response::ok()
}

/// CLIENT NO-TOUCH — don't update LRU time on access.
fn client_no_touch(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|no-touch' command");
    }

    let mode = args[0].to_ascii_uppercase();
    let id = conn_id(ctx);
    let info = match ctx.client_registry.get(id) {
        Some(info) => info,
        None => return Response::error("ERR client not found"),
    };

    let mut flags = info.flags;

    match mode.as_slice() {
        b"ON" => {
            flags |= ClientFlags::NO_TOUCH;
        }
        b"OFF" => {
            flags.remove(ClientFlags::NO_TOUCH);
        }
        _ => {
            return Response::error("ERR argument must be 'ON' or 'OFF'");
        }
    }

    ctx.client_registry.update_flags(id, flags);
    Response::ok()
}

/// CLIENT TRACKING ON/OFF with optional flags.
///
/// The state transition ([`ConnStateMut::enable_tracking`]/`disable_tracking`)
/// runs first; its computed prefixes drive the IO half
/// ([`ClientTrackingProvider`] + shard registration).
async fn client_tracking(ctx: &mut ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|tracking' command");
    }

    let on_off = args[0].to_ascii_uppercase();
    match on_off.as_slice() {
        b"ON" => {
            let mut optin = false;
            let mut optout = false;
            let mut noloop = false;
            let mut bcast = false;
            let mut prefixes: Vec<Bytes> = Vec::new();
            let mut redirect: u64 = 0;

            let id = conn_id(ctx);

            // Parse flags. REDIRECT is validated against the client registry
            // during parsing (matching Redis); every other rejection rule lives
            // in `ConnStateMut::enable_tracking`.
            let mut i = 1;
            while i < args.len() {
                let flag = args[i].to_ascii_uppercase();
                match flag.as_slice() {
                    b"OPTIN" => optin = true,
                    b"OPTOUT" => optout = true,
                    b"NOLOOP" => noloop = true,
                    b"BCAST" => bcast = true,
                    b"PREFIX" => {
                        i += 1;
                        if i >= args.len() {
                            return Response::error("ERR PREFIX requires an argument");
                        }
                        prefixes.push(args[i].clone());
                    }
                    b"REDIRECT" => {
                        i += 1;
                        if i >= args.len() {
                            return Response::error("ERR REDIRECT requires an argument");
                        }
                        let id_str = String::from_utf8_lossy(&args[i]);
                        let target = match id_str.parse::<u64>() {
                            Ok(target) => target,
                            Err(_) => {
                                return Response::error("ERR Invalid REDIRECT client ID");
                            }
                        };
                        if target > 0 {
                            if target == id {
                                return Response::error(
                                    "ERR It is not possible to redirect tracking notifications to the same connection",
                                );
                            }
                            // Validate target exists
                            if ctx.client_registry.get(target).is_none() {
                                return Response::error(
                                    "ERR The client ID you want redirect to does not exist",
                                );
                            }
                        }
                        redirect = target;
                    }
                    _ => {
                        return Response::error(format!(
                            "ERR Unrecognized option '{}'",
                            String::from_utf8_lossy(&flag)
                        ));
                    }
                }
                i += 1;
            }

            // State transition: applies Redis's flag-compatibility rules and
            // returns the BCAST prefixes to register with the shards.
            let computed_prefixes = match ctx
                .conn_state
                .as_deref_mut()
                .expect("CLIENT is dispatched with a mutable conn_state")
                .enable_tracking(bcast, optin, optout, noloop, prefixes, redirect)
            {
                Ok(prefixes) => prefixes,
                Err(err) => return Response::error(format!("ERR {err}")),
            };

            // IO half: wire the invalidation delivery path and register with
            // every shard. Copy the shared `shard_senders` handle out first so
            // it does not overlap the `&mut` borrow of `ctx.tracking`.
            let shard_senders = ctx.shard_senders;
            ctx.tracking
                .as_deref_mut()
                .expect("CLIENT is dispatched with tracking IO")
                .enable(
                    id,
                    redirect,
                    bcast,
                    noloop,
                    computed_prefixes,
                    shard_senders,
                )
                .await;

            Response::ok()
        }
        b"OFF" => {
            let id = conn_id(ctx);
            let was_enabled = ctx
                .conn_state
                .as_deref_mut()
                .expect("CLIENT is dispatched with a mutable conn_state")
                .disable_tracking();
            if was_enabled {
                let shard_senders = ctx.shard_senders;
                ctx.tracking
                    .as_deref_mut()
                    .expect("CLIENT is dispatched with tracking IO")
                    .disable(id, shard_senders)
                    .await;
            }
            Response::ok()
        }
        _ => Response::error("ERR Tracking requires ON or OFF as first argument"),
    }
}

/// CLIENT TRACKINGINFO — return tracking state.
fn client_trackinginfo(ctx: &ConnCtx<'_>) -> Response {
    let tracking = ctx
        .conn_state
        .as_deref()
        .expect("CLIENT is dispatched with a mutable conn_state")
        .tracking_info();

    let mut flags = Vec::new();
    if tracking.enabled {
        flags.push(Response::bulk(Bytes::from_static(b"on")));
        match tracking.mode {
            TrackingModeView::OptIn => {
                flags.push(Response::bulk(Bytes::from_static(b"optin")));
            }
            TrackingModeView::OptOut => {
                flags.push(Response::bulk(Bytes::from_static(b"optout")));
            }
            TrackingModeView::Broadcast => {
                flags.push(Response::bulk(Bytes::from_static(b"bcast")));
            }
            TrackingModeView::Default => {}
        }
        if tracking.noloop {
            flags.push(Response::bulk(Bytes::from_static(b"noloop")));
        }
        match tracking.caching_override {
            Some(true) => {
                flags.push(Response::bulk(Bytes::from_static(b"caching-yes")));
            }
            Some(false) => {
                flags.push(Response::bulk(Bytes::from_static(b"caching-no")));
            }
            None => {}
        }
    } else {
        flags.push(Response::bulk(Bytes::from_static(b"off")));
    }

    let redirect = if !tracking.enabled {
        -1
    } else if tracking.redirect > 0 {
        tracking.redirect as i64
    } else {
        0
    };

    let prefix_responses: Vec<Response> = tracking
        .prefixes
        .iter()
        .map(|p| Response::bulk(p.clone()))
        .collect();

    Response::Map(vec![
        (
            Response::bulk(Bytes::from_static(b"flags")),
            Response::Array(flags),
        ),
        (
            Response::bulk(Bytes::from_static(b"redirect")),
            Response::Integer(redirect),
        ),
        (
            Response::bulk(Bytes::from_static(b"prefixes")),
            Response::Array(prefix_responses),
        ),
    ])
}

/// CLIENT GETREDIR — return tracking redirect ID.
fn client_getredir(ctx: &ConnCtx<'_>) -> Response {
    let tracking = ctx
        .conn_state
        .as_deref()
        .expect("CLIENT is dispatched with a mutable conn_state")
        .tracking_info();
    if !tracking.enabled {
        return Response::Integer(-1);
    }
    if tracking.redirect > 0 {
        Response::Integer(tracking.redirect as i64)
    } else {
        Response::Integer(0)
    }
}

/// CLIENT CACHING YES/NO — control per-command tracking override.
fn client_caching(ctx: &mut ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|caching' command");
    }

    let tracking = ctx
        .conn_state
        .as_deref()
        .expect("CLIENT is dispatched with a mutable conn_state")
        .tracking_info();

    if !tracking.enabled {
        return Response::error(
            "ERR CLIENT CACHING can be called only after CLIENT TRACKING is enabled",
        );
    }

    if tracking.mode == TrackingModeView::Broadcast {
        return Response::error("ERR CLIENT CACHING is not compatible with BCAST mode");
    }

    if tracking.mode == TrackingModeView::Default {
        return Response::error("ERR CLIENT CACHING YES/NO is only valid in OPTIN or OPTOUT mode");
    }

    let mode = args[0].to_ascii_uppercase();
    match mode.as_slice() {
        b"YES" => {
            ctx.conn_state
                .as_deref_mut()
                .expect("CLIENT is dispatched with a mutable conn_state")
                .set_caching_override(true);
            Response::ok()
        }
        b"NO" => {
            ctx.conn_state
                .as_deref_mut()
                .expect("CLIENT is dispatched with a mutable conn_state")
                .set_caching_override(false);
            Response::ok()
        }
        _ => Response::error("ERR argument must be 'YES' or 'NO'"),
    }
}

/// CLIENT REPLY — control reply mode.
fn client_reply(ctx: &mut ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|reply' command");
    }

    let state = ctx
        .conn_state
        .as_deref_mut()
        .expect("CLIENT is dispatched with a mutable conn_state");

    let mode = args[0].to_ascii_uppercase();
    match mode.as_slice() {
        b"ON" => {
            state.reply_on();
            Response::ok()
        }
        b"OFF" => {
            state.reply_off();
            // Note: This command itself should still return OK
            Response::ok()
        }
        b"SKIP" => {
            state.reply_skip_next();
            Response::ok()
        }
        _ => Response::error("ERR argument must be 'ON', 'OFF' or 'SKIP'"),
    }
}

/// CLIENT UNBLOCK — unblock a blocked client.
fn client_unblock(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'client|unblock' command");
    }

    // Parse client ID
    let id_str = String::from_utf8_lossy(&args[0]);
    let client_id: u64 = match id_str.parse() {
        Ok(id) => id,
        Err(_) => return Response::error("ERR client ID is not an integer or out of range"),
    };

    // Parse optional mode (TIMEOUT or ERROR)
    let mode = if args.len() > 1 {
        let mode_str = args[1].to_ascii_uppercase();
        match mode_str.as_slice() {
            b"TIMEOUT" => UnblockMode::Timeout,
            b"ERROR" => UnblockMode::Error,
            _ => {
                return Response::error("ERR unblock mode must be either TIMEOUT or ERROR");
            }
        }
    } else {
        UnblockMode::Timeout // Default mode
    };

    // Try to unblock the client
    if ctx.client_registry.unblock(client_id, mode) {
        Response::Integer(1)
    } else {
        Response::Integer(0)
    }
}

/// CLIENT STATS [ID <client-id>] — return per-client command statistics.
fn client_stats(ctx: &mut ConnCtx<'_>, args: &[Bytes]) -> Response {
    // Force sync our local stats first.
    ctx.conn_state
        .as_deref_mut()
        .expect("CLIENT is dispatched with a mutable conn_state")
        .sync_stats_to_registry(ctx.client_registry);

    if args.is_empty() {
        // Return stats for all clients
        return format_all_client_stats(ctx);
    }

    // CLIENT STATS ID <client-id>
    if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"ID") {
        let client_id_str = String::from_utf8_lossy(&args[1]);
        match client_id_str.parse::<u64>() {
            Ok(client_id) => return format_single_client_stats(ctx, client_id),
            Err(_) => {
                return Response::error(format!("ERR Invalid client ID: {}", client_id_str));
            }
        }
    }

    Response::error("ERR syntax error. Try CLIENT STATS [ID <client-id>]")
}

/// Format stats for a single client.
fn format_single_client_stats(ctx: &ConnCtx<'_>, client_id: u64) -> Response {
    match ctx.client_registry.get_with_stats(client_id) {
        Some((info, stats)) => {
            let output = format_client_stats_entry(&info, &stats);
            Response::bulk(Bytes::from(output))
        }
        None => Response::error(format!("ERR No such client with ID {}", client_id)),
    }
}

/// Format stats for all clients.
fn format_all_client_stats(ctx: &ConnCtx<'_>) -> Response {
    let all_stats = ctx.client_registry.get_all_stats();

    let mut output = String::new();
    for (_, info, stats) in &all_stats {
        let entry = format_client_stats_entry(info, stats);
        output.push_str(&entry);
        output.push('\n');
    }

    // Remove trailing newline
    if output.ends_with('\n') {
        output.pop();
    }

    Response::bulk(Bytes::from(output))
}

/// CLIENT HELP — show help.
fn client_help() -> Response {
    let help = vec![
        Response::bulk(Bytes::from_static(
            b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        )),
        Response::bulk(Bytes::from_static(b"CACHING <YES|NO>")),
        Response::bulk(Bytes::from_static(
            b"    Control client-side caching for the current connection.",
        )),
        Response::bulk(Bytes::from_static(b"GETNAME")),
        Response::bulk(Bytes::from_static(
            b"    Return the name of the current connection.",
        )),
        Response::bulk(Bytes::from_static(b"GETREDIR")),
        Response::bulk(Bytes::from_static(
            b"    Return the client tracking redirection ID (-1 if not tracking).",
        )),
        Response::bulk(Bytes::from_static(b"ID")),
        Response::bulk(Bytes::from_static(
            b"    Return the ID of the current connection.",
        )),
        Response::bulk(Bytes::from_static(b"INFO")),
        Response::bulk(Bytes::from_static(
            b"    Return information about the current connection.",
        )),
        Response::bulk(Bytes::from_static(
            b"KILL <ip:port>|<filter> [value] ... [<filter> [value] ...]",
        )),
        Response::bulk(Bytes::from_static(b"    Kill connection(s).")),
        Response::bulk(Bytes::from_static(
            b"LIST [TYPE <normal|master|replica|pubsub>]",
        )),
        Response::bulk(Bytes::from_static(
            b"    Return information about client connections.",
        )),
        Response::bulk(Bytes::from_static(b"NO-EVICT <ON|OFF>")),
        Response::bulk(Bytes::from_static(b"    Protect client from eviction.")),
        Response::bulk(Bytes::from_static(b"NO-TOUCH <ON|OFF>")),
        Response::bulk(Bytes::from_static(
            b"    Don't update LRU time on key access.",
        )),
        Response::bulk(Bytes::from_static(b"PAUSE <timeout> [WRITE|ALL]")),
        Response::bulk(Bytes::from_static(
            b"    Suspend clients for specified time.",
        )),
        Response::bulk(Bytes::from_static(b"REPLY <ON|OFF|SKIP>")),
        Response::bulk(Bytes::from_static(b"    Control server replies.")),
        Response::bulk(Bytes::from_static(b"SETINFO <LIB-NAME|LIB-VER> <value>")),
        Response::bulk(Bytes::from_static(b"    Set client library info.")),
        Response::bulk(Bytes::from_static(b"SETNAME <name>")),
        Response::bulk(Bytes::from_static(
            b"    Set the name of the current connection.",
        )),
        Response::bulk(Bytes::from_static(b"STATS [ID <client-id>]")),
        Response::bulk(Bytes::from_static(
            b"    Return per-client command statistics.",
        )),
        Response::bulk(Bytes::from_static(b"TRACKINGINFO")),
        Response::bulk(Bytes::from_static(
            b"    Return tracking state for the current connection.",
        )),
        Response::bulk(Bytes::from_static(b"UNBLOCK <client-id> [TIMEOUT|ERROR]")),
        Response::bulk(Bytes::from_static(b"    Unblock a blocked client.")),
        Response::bulk(Bytes::from_static(b"UNPAUSE")),
        Response::bulk(Bytes::from_static(b"    Resume processing commands.")),
        Response::bulk(Bytes::from_static(b"HELP")),
        Response::bulk(Bytes::from_static(b"    Print this help.")),
    ];
    Response::Array(help)
}
