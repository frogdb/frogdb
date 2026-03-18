//! CLIENT command handlers.
//!
//! This module handles CLIENT subcommands:
//! - CLIENT ID - Return the ID of the current connection
//! - CLIENT SETNAME/GETNAME - Set/get connection name
//! - CLIENT LIST/INFO - List connections or get info
//! - CLIENT KILL - Kill connections
//! - CLIENT PAUSE/UNPAUSE - Pause/resume command processing
//! - CLIENT REPLY - Control reply mode
//! - CLIENT STATS - Return per-client statistics
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::{ClientFlags, KillFilter, PauseMode, UnblockMode};
use frogdb_protocol::Response;
use std::net::SocketAddr;

use crate::connection::{ConnectionHandler, ReplyMode};

// Helper function from connection.rs - moved here for use by CLIENT STATS
fn format_client_stats_entry(
    info: &frogdb_core::ClientInfo,
    stats: &frogdb_core::ClientStats,
) -> String {
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

impl ConnectionHandler {
    /// Handle CLIENT command and dispatch to subcommands.
    pub(crate) async fn handle_client_command(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "ID" => self.handle_client_id(),
            "SETNAME" => self.handle_client_setname(&args[1..]),
            "GETNAME" => self.handle_client_getname(),
            "LIST" => self.handle_client_list(&args[1..]),
            "INFO" => self.handle_client_info(),
            "KILL" => self.handle_client_kill(&args[1..]),
            "PAUSE" => self.handle_client_pause(&args[1..]),
            "UNPAUSE" => self.handle_client_unpause(),
            "SETINFO" => self.handle_client_setinfo(&args[1..]),
            "NO-EVICT" => self.handle_client_no_evict(&args[1..]),
            "NO-TOUCH" => self.handle_client_no_touch(&args[1..]),
            "TRACKING" => self.handle_client_tracking(&args[1..]).await,
            "TRACKINGINFO" => self.handle_client_trackinginfo(),
            "GETREDIR" => self.handle_client_getredir(),
            "CACHING" => self.handle_client_caching(&args[1..]),
            "REPLY" => self.handle_client_reply(&args[1..]),
            "UNBLOCK" => self.handle_client_unblock(&args[1..]),
            "STATS" => self.handle_client_stats(&args[1..]),
            "HELP" => self.handle_client_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle CLIENT ID - return connection ID.
    fn handle_client_id(&self) -> Response {
        Response::Integer(self.state.id as i64)
    }

    /// Handle CLIENT SETNAME - set connection name.
    fn handle_client_setname(&mut self, args: &[Bytes]) -> Response {
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

        // Update local state
        self.state.name = name_opt.clone();

        // Update in registry
        self.admin
            .client_registry
            .update_name(self.state.id, name_opt);

        Response::ok()
    }

    /// Handle CLIENT GETNAME - get connection name.
    fn handle_client_getname(&self) -> Response {
        match &self.state.name {
            Some(name) => Response::bulk(name.clone()),
            None => Response::null(),
        }
    }

    /// Handle CLIENT LIST - list all connections.
    fn handle_client_list(&self, args: &[Bytes]) -> Response {
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
                            return Response::error(format!(
                                "ERR Unknown client type '{}'",
                                type_str
                            ));
                        }
                    }
                }
                b"ID" => {
                    // CLIENT LIST ID id1 id2 ... - not implemented yet
                    return Response::error("ERR CLIENT LIST ID not implemented");
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
        let clients = self.admin.client_registry.list();

        // Build output
        let mut output = String::new();
        for info in clients {
            // Apply type filter
            if let Some(ft) = filter_type
                && info.client_type() != ft
            {
                continue;
            }

            output.push_str(&info.to_client_list_entry());
            output.push('\n');
        }

        Response::bulk(Bytes::from(output))
    }

    /// Handle CLIENT INFO - get current connection info.
    fn handle_client_info(&self) -> Response {
        match self.admin.client_registry.get(self.state.id) {
            Some(info) => {
                let entry = info.to_client_list_entry();
                Response::bulk(Bytes::from(entry + "\n"))
            }
            None => Response::error("ERR client not found"),
        }
    }

    /// Handle CLIENT KILL - terminate connections.
    fn handle_client_kill(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|kill' command");
        }

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
                current_conn_id: Some(self.state.id),
                ..Default::default()
            };

            let killed = self.admin.client_registry.kill_by_filter(&filter);
            if killed > 0 {
                return Response::ok();
            } else {
                return Response::error("ERR No such client");
            }
        }

        // New-style syntax: CLIENT KILL [ID id] [ADDR ip:port] [LADDR ip:port] [TYPE type] [USER username] [SKIPME yes|no]
        let mut filter = KillFilter {
            skip_me: true, // Default is to skip ourselves
            current_conn_id: Some(self.state.id),
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
                    filter.id = match id_str.parse() {
                        Ok(id) => Some(id),
                        Err(_) => return Response::error("ERR client-id is not an integer"),
                    };
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
                            return Response::error(format!(
                                "ERR Unknown client type '{}'",
                                type_str
                            ));
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

        // Must have at least one filter
        if filter.id.is_none()
            && filter.addr.is_none()
            && filter.laddr.is_none()
            && filter.client_type.is_none()
        {
            return Response::error("ERR syntax error");
        }

        let killed = self.admin.client_registry.kill_by_filter(&filter);
        Response::Integer(killed as i64)
    }

    /// Handle CLIENT PAUSE - pause command execution.
    fn handle_client_pause(&self, args: &[Bytes]) -> Response {
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

        self.admin.client_registry.pause(mode, timeout_ms);
        Response::ok()
    }

    /// Handle CLIENT UNPAUSE - resume command execution.
    fn handle_client_unpause(&self) -> Response {
        self.admin.client_registry.unpause();
        Response::ok()
    }

    /// Handle CLIENT SETINFO - set client library info.
    fn handle_client_setinfo(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'client|setinfo' command");
        }

        let attr = args[0].to_ascii_uppercase();
        let value = &args[1];

        match attr.as_slice() {
            b"LIB-NAME" => {
                // Validate: no spaces or newlines allowed
                if value.iter().any(|&b| b == b' ' || b == b'\n' || b == b'\r') {
                    return Response::error(
                        "ERR lib-name cannot contain spaces, newlines or special characters",
                    );
                }
                self.admin.client_registry.update_lib_info(
                    self.state.id,
                    Some(value.clone()),
                    None,
                );
                Response::ok()
            }
            b"LIB-VER" => {
                // Validate: no spaces or newlines allowed
                if value.iter().any(|&b| b == b' ' || b == b'\n' || b == b'\r') {
                    return Response::error(
                        "ERR lib-ver cannot contain spaces, newlines or special characters",
                    );
                }
                self.admin.client_registry.update_lib_info(
                    self.state.id,
                    None,
                    Some(value.clone()),
                );
                Response::ok()
            }
            _ => Response::error(format!(
                "ERR unknown attribute '{}'",
                String::from_utf8_lossy(&attr)
            )),
        }
    }

    /// Handle CLIENT NO-EVICT - protect client from eviction.
    fn handle_client_no_evict(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|no-evict' command");
        }

        let mode = args[0].to_ascii_uppercase();
        let info = match self.admin.client_registry.get(self.state.id) {
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

        self.admin
            .client_registry
            .update_flags(self.state.id, flags);
        Response::ok()
    }

    /// Handle CLIENT NO-TOUCH - don't update LRU time on access.
    fn handle_client_no_touch(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|no-touch' command");
        }

        let mode = args[0].to_ascii_uppercase();
        let info = match self.admin.client_registry.get(self.state.id) {
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

        self.admin
            .client_registry
            .update_flags(self.state.id, flags);
        Response::ok()
    }

    /// Handle CLIENT TRACKING ON/OFF with optional flags.
    async fn handle_client_tracking(&mut self, args: &[Bytes]) -> Response {
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

                // Parse flags
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
                            match id_str.parse::<u64>() {
                                Ok(id) => redirect = id,
                                Err(_) => {
                                    return Response::error("ERR Invalid REDIRECT client ID");
                                }
                            }
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

                // PREFIX requires BCAST
                if !prefixes.is_empty() && !bcast {
                    return Response::error("ERR PREFIX option requires BCAST mode to be enabled");
                }

                // OPTIN and OPTOUT are mutually exclusive
                if optin && optout {
                    return Response::error("ERR OPTIN and OPTOUT are mutually exclusive");
                }

                // BCAST is incompatible with OPTIN/OPTOUT
                if bcast && (optin || optout) {
                    return Response::error("ERR OPTIN and OPTOUT are not compatible with BCAST");
                }

                // REDIRECT validation
                if redirect > 0 {
                    if redirect == self.state.id {
                        return Response::error(
                            "ERR It is not possible to redirect tracking notifications to the same connection",
                        );
                    }
                    // Validate target exists
                    if self.admin.client_registry.get(redirect).is_none() {
                        return Response::error(
                            "ERR The client ID you want redirect to does not exist".to_string(),
                        );
                    }
                }

                let mode = if bcast {
                    crate::connection::TrackingMode::Broadcast
                } else if optin {
                    crate::connection::TrackingMode::OptIn
                } else if optout {
                    crate::connection::TrackingMode::OptOut
                } else {
                    crate::connection::TrackingMode::Default
                };

                self.state.tracking.enabled = true;
                self.state.tracking.mode = mode;
                self.state.tracking.noloop = noloop;
                self.state.tracking.caching_override = None;
                self.state.tracking.prefixes = prefixes.clone();
                self.state.tracking.redirect = redirect;

                // Set up invalidation channel
                let sender = if redirect > 0 {
                    // REDIRECT mode: create forwarding channel that publishes to
                    // __redis__:invalidate via shard 0's pub/sub
                    let (fwd_tx, mut fwd_rx) =
                        tokio::sync::mpsc::unbounded_channel::<frogdb_core::InvalidationMessage>();
                    let shard0 = self.core.shard_senders[0].clone();
                    let task = tokio::spawn(async move {
                        while let Some(msg) = fwd_rx.recv().await {
                            let payload = match &msg {
                                frogdb_core::InvalidationMessage::Keys(keys) => {
                                    // Encode as space-separated key names for pub/sub
                                    let key_strs: Vec<&[u8]> =
                                        keys.iter().map(|k| k.as_ref()).collect();
                                    Bytes::copy_from_slice(&key_strs.join(&b' '))
                                }
                                frogdb_core::InvalidationMessage::FlushAll => {
                                    Bytes::from_static(b"")
                                }
                            };
                            let (resp_tx, _) = tokio::sync::oneshot::channel();
                            let _ = shard0
                                .send(frogdb_core::ShardMessage::Publish {
                                    channel: Bytes::from_static(b"__redis__:invalidate"),
                                    message: payload,
                                    response_tx: resp_tx,
                                })
                                .await;
                        }
                    });
                    self.redirect_task = Some(task);
                    fwd_tx
                } else {
                    self.ensure_invalidation_channel()
                };

                // Register with all shards
                if bcast {
                    for shard_sender in self.core.shard_senders.iter() {
                        let _ = shard_sender
                            .send(frogdb_core::ShardMessage::TrackingBroadcastRegister {
                                conn_id: self.state.id,
                                sender: sender.clone(),
                                noloop,
                                prefixes: prefixes.clone(),
                            })
                            .await;
                    }
                } else {
                    for shard_sender in self.core.shard_senders.iter() {
                        let _ = shard_sender
                            .send(frogdb_core::ShardMessage::TrackingRegister {
                                conn_id: self.state.id,
                                sender: sender.clone(),
                                noloop,
                            })
                            .await;
                    }
                }

                Response::ok()
            }
            b"OFF" => {
                if !self.state.tracking.enabled {
                    return Response::ok();
                }

                self.state.tracking = crate::connection::TrackingState::default();

                // Unregister from all shards
                for shard_sender in self.core.shard_senders.iter() {
                    let _ = shard_sender
                        .send(frogdb_core::ShardMessage::TrackingUnregister {
                            conn_id: self.state.id,
                        })
                        .await;
                }

                // Drop invalidation channels
                self.invalidation_tx = None;
                self.invalidation_rx = None;

                // Abort redirect forwarding task if any
                if let Some(task) = self.redirect_task.take() {
                    task.abort();
                }

                Response::ok()
            }
            _ => Response::error("ERR Tracking requires ON or OFF as first argument"),
        }
    }

    /// Handle CLIENT TRACKINGINFO - return tracking state.
    fn handle_client_trackinginfo(&self) -> Response {
        let tracking = &self.state.tracking;

        let mut flags = Vec::new();
        if tracking.enabled {
            flags.push(Response::bulk(Bytes::from_static(b"on")));
            match tracking.mode {
                crate::connection::TrackingMode::OptIn => {
                    flags.push(Response::bulk(Bytes::from_static(b"optin")));
                }
                crate::connection::TrackingMode::OptOut => {
                    flags.push(Response::bulk(Bytes::from_static(b"optout")));
                }
                crate::connection::TrackingMode::Broadcast => {
                    flags.push(Response::bulk(Bytes::from_static(b"bcast")));
                }
                crate::connection::TrackingMode::Default => {}
            }
            if tracking.noloop {
                flags.push(Response::bulk(Bytes::from_static(b"noloop")));
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

        Response::Array(vec![
            Response::bulk(Bytes::from_static(b"flags")),
            Response::Array(flags),
            Response::bulk(Bytes::from_static(b"redirect")),
            Response::Integer(redirect),
            Response::bulk(Bytes::from_static(b"prefixes")),
            Response::Array(prefix_responses),
        ])
    }

    /// Handle CLIENT GETREDIR - return tracking redirect ID.
    fn handle_client_getredir(&self) -> Response {
        if !self.state.tracking.enabled {
            return Response::Integer(-1);
        }
        if self.state.tracking.redirect > 0 {
            Response::Integer(self.state.tracking.redirect as i64)
        } else {
            Response::Integer(0)
        }
    }

    /// Handle CLIENT CACHING YES/NO - control per-command tracking override.
    fn handle_client_caching(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|caching' command");
        }

        if !self.state.tracking.enabled {
            return Response::error(
                "ERR CLIENT CACHING can be called only after CLIENT TRACKING is enabled",
            );
        }

        if self.state.tracking.mode == crate::connection::TrackingMode::Broadcast {
            return Response::error("ERR CLIENT CACHING is not compatible with BCAST mode");
        }

        if self.state.tracking.mode == crate::connection::TrackingMode::Default {
            return Response::error(
                "ERR CLIENT CACHING YES/NO is only valid in OPTIN or OPTOUT mode",
            );
        }

        let mode = args[0].to_ascii_uppercase();
        match mode.as_slice() {
            b"YES" => {
                self.state.tracking.caching_override = Some(true);
                Response::ok()
            }
            b"NO" => {
                self.state.tracking.caching_override = Some(false);
                Response::ok()
            }
            _ => Response::error("ERR argument must be 'YES' or 'NO'"),
        }
    }

    /// Handle CLIENT REPLY - control reply mode.
    fn handle_client_reply(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|reply' command");
        }

        let mode = args[0].to_ascii_uppercase();
        match mode.as_slice() {
            b"ON" => {
                self.state.reply_mode = ReplyMode::On;
                Response::ok()
            }
            b"OFF" => {
                self.state.reply_mode = ReplyMode::Off;
                // Note: This command itself should still return OK
                Response::ok()
            }
            b"SKIP" => {
                self.state.skip_next_reply = true;
                Response::ok()
            }
            _ => Response::error("ERR argument must be 'ON', 'OFF' or 'SKIP'"),
        }
    }

    /// Handle CLIENT UNBLOCK - unblock a blocked client.
    fn handle_client_unblock(&self, args: &[Bytes]) -> Response {
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
        if self.admin.client_registry.unblock(client_id, mode) {
            Response::Integer(1)
        } else {
            Response::Integer(0)
        }
    }

    /// Handle CLIENT STATS [ID <client-id>] - return per-client command statistics.
    fn handle_client_stats(&mut self, args: &[Bytes]) -> Response {
        // Force sync our local stats first
        self.sync_stats_to_registry();

        if args.is_empty() {
            // Return stats for all clients
            return self.format_all_client_stats();
        }

        // CLIENT STATS ID <client-id>
        if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"ID") {
            let client_id_str = String::from_utf8_lossy(&args[1]);
            match client_id_str.parse::<u64>() {
                Ok(client_id) => return self.format_single_client_stats(client_id),
                Err(_) => {
                    return Response::error(format!("ERR Invalid client ID: {}", client_id_str));
                }
            }
        }

        Response::error("ERR syntax error. Try CLIENT STATS [ID <client-id>]")
    }

    /// Format stats for a single client.
    fn format_single_client_stats(&self, client_id: u64) -> Response {
        match self.admin.client_registry.get_with_stats(client_id) {
            Some((info, stats)) => {
                let output = format_client_stats_entry(&info, &stats);
                Response::bulk(Bytes::from(output))
            }
            None => Response::error(format!("ERR No such client with ID {}", client_id)),
        }
    }

    /// Format stats for all clients.
    fn format_all_client_stats(&self) -> Response {
        let all_stats = self.admin.client_registry.get_all_stats();

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

    /// Handle CLIENT HELP - show help.
    fn handle_client_help(&self) -> Response {
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
}
