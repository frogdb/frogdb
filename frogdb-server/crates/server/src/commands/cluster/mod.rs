//! CLUSTER command implementation.
//!
//! Provides Redis Cluster protocol commands for:
//! - Cluster state inspection (INFO, NODES, SLOTS, MYID)
//! - Cluster topology management (MEET, FORGET, ADDSLOTS, DELSLOTS)
//! - Key routing (KEYSLOT, COUNTKEYSINSLOT)
//! - Failover coordination (FAILOVER)
//!
//! Cluster commands that modify state (MEET, FORGET, ADDSLOTS, etc.) return
//! `Response::RaftNeeded` which is intercepted by the connection handler.
//! The connection handler executes the Raft operation asynchronously and
//! updates the NetworkFactory after successful commit.

mod admin;

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ConnectionLevelOp,
    ExecutionStrategy, slot_for_key,
};
use frogdb_protocol::Response;

// ============================================================================
// CLUSTER - Cluster management command
// ============================================================================

/// Get the local node's replication offset from Raft metrics or replication tracker.
fn local_replication_offset(ctx: &CommandContext) -> i64 {
    if let Some(raft) = ctx.raft {
        return raft
            .metrics()
            .borrow()
            .last_applied
            .map(|log_id| log_id.index as i64)
            .unwrap_or(0);
    }
    if let Some(tracker) = ctx.replication_tracker {
        return tracker.current_offset() as i64;
    }
    0
}

pub struct ClusterCommand;

impl Command for ClusterCommand {
    fn name(&self) -> &'static str {
        "cluster"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::STALE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArgCount {
                command: "cluster".to_string(),
            });
        }

        let subcommand = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid subcommand".to_string(),
            })?
            .to_ascii_uppercase();

        match subcommand.as_str() {
            "INFO" => cluster_info(ctx),
            "NODES" => cluster_nodes(ctx),
            "MYID" => cluster_myid(ctx),
            "SLOTS" => cluster_slots(ctx),
            "SHARDS" => cluster_shards(ctx),
            "KEYSLOT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster keyslot".to_string(),
                    });
                }
                cluster_keyslot(&args[1])
            }
            "COUNTKEYSINSLOT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster countkeysinslot".to_string(),
                    });
                }
                cluster_countkeysinslot(ctx, &args[1])
            }
            "GETKEYSINSLOT" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster getkeysinslot".to_string(),
                    });
                }
                cluster_getkeysinslot(ctx, &args[1], &args[2])
            }
            "MEET" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster meet".to_string(),
                    });
                }
                // CLUSTER MEET <ip> <port> [<cluster-bus-port>]
                let cluster_bus_port = if args.len() > 3 { Some(&args[3]) } else { None };
                admin::cluster_meet(ctx, &args[1], &args[2], cluster_bus_port)
            }
            "FORGET" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster forget".to_string(),
                    });
                }
                admin::cluster_forget(ctx, &args[1])
            }
            "ADDSLOTS" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster addslots".to_string(),
                    });
                }
                admin::cluster_addslots(ctx, &args[1..])
            }
            "DELSLOTS" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster delslots".to_string(),
                    });
                }
                admin::cluster_delslots(ctx, &args[1..])
            }
            "FAILOVER" => admin::cluster_failover(ctx, &args[1..]),
            "REPLICATE" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster replicate".to_string(),
                    });
                }
                admin::cluster_replicate(ctx, &args[1])
            }
            "RESET" => admin::cluster_reset(ctx, &args[1..]),
            "SAVECONFIG" => admin::cluster_saveconfig(ctx),
            "SET-CONFIG-EPOCH" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster set-config-epoch".to_string(),
                    });
                }
                admin::cluster_set_config_epoch(ctx, &args[1])
            }
            "SETSLOT" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster setslot".to_string(),
                    });
                }
                admin::cluster_setslot(ctx, &args[1..])
            }
            "HELP" => cluster_help(),
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "Unknown subcommand or wrong number of arguments for '{}'",
                    subcommand
                ),
            }),
        }
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![] // Keyless
    }
}

/// CLUSTER INFO - Returns cluster state information.
fn cluster_info(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // Use ClusterState if available, otherwise return standalone mode info
    if let Some(cluster_state) = ctx.cluster_state {
        let snapshot = cluster_state.snapshot();
        let slots_assigned = snapshot.slot_assignment.len() as u16;
        let known_nodes = snapshot.nodes.len();

        // Count primaries (cluster_size)
        let cluster_size = snapshot.nodes.values().filter(|n| n.is_primary()).count();

        // Get current node's epoch
        let my_epoch = ctx
            .node_id
            .and_then(|id| snapshot.nodes.get(&id))
            .map(|n| n.config_epoch)
            .unwrap_or(0);

        // Check if we have a leader (quorum) via Raft metrics
        // We use millis_since_quorum_ack to detect if quorum has been lost.
        // If too much time has passed since a quorum ack, the cluster is unhealthy.
        const QUORUM_TIMEOUT_MS: u64 = 2000; // 2 seconds without quorum = fail

        // Check for failed primaries - cluster is in fail state if any primary is marked failed
        let has_failed_primary = snapshot
            .nodes
            .values()
            .any(|n| n.is_primary() && n.flags.fail);

        // Check if we can form a quorum with reachable nodes (local perspective)
        let has_local_quorum = ctx.quorum_checker.map(|qc| qc.has_quorum()).unwrap_or(true); // If no quorum checker, assume healthy

        // Extract Raft term to incorporate into epoch (leader elections bump the term)
        let raft_term: u64 = ctx
            .raft
            .map(|r| r.metrics().borrow().current_term)
            .unwrap_or(0);

        let cluster_state_str = if has_failed_primary || !has_local_quorum {
            "fail"
        } else if let Some(raft) = ctx.raft {
            use openraft::ServerState;
            let metrics = raft.metrics().borrow().clone();

            match (
                metrics.state,
                metrics.current_leader,
                metrics.millis_since_quorum_ack,
            ) {
                (ServerState::Candidate, _, _) => "fail", // Trying to elect but can't get quorum
                (_, None, _) => "fail",                   // No leader known
                // Leader: require RECENT quorum ack (within timeout) to report healthy
                (ServerState::Leader, _, Some(millis)) if millis <= QUORUM_TIMEOUT_MS => "ok",
                // Leader: stale quorum ack OR no quorum ack (None) = partitioned/unhealthy
                (ServerState::Leader, _, _) => "fail",
                // Follower: trust that we have a leader (they'll discover otherwise via election)
                (ServerState::Follower, Some(_), _) => "ok",
                _ => "ok", // Learner with leader, etc.
            }
        } else {
            "ok" // No Raft = standalone mode, always ok
        };

        // Effective epoch: max of config epoch and Raft term.  Raft leader
        // elections bump the term, which represents a cluster topology event
        // (new coordinator), matching Redis's epoch-on-failover semantics.
        let effective_epoch = snapshot.config_epoch.max(raft_term);

        let info = format!(
            "\
cluster_state:{}\r\n\
cluster_slots_assigned:{}\r\n\
cluster_slots_ok:{}\r\n\
cluster_slots_pfail:0\r\n\
cluster_slots_fail:0\r\n\
cluster_known_nodes:{}\r\n\
cluster_size:{}\r\n\
cluster_current_epoch:{}\r\n\
cluster_my_epoch:{}\r\n\
cluster_stats_messages_ping_sent:0\r\n\
cluster_stats_messages_pong_sent:0\r\n\
cluster_stats_messages_sent:0\r\n\
cluster_stats_messages_ping_received:0\r\n\
cluster_stats_messages_pong_received:0\r\n\
cluster_stats_messages_received:0\r\n\
total_cluster_links_buffer_limit_exceeded:0\r\n",
            cluster_state_str,
            slots_assigned,
            slots_assigned, // slots_ok = slots_assigned for now
            known_nodes,
            cluster_size,
            effective_epoch,
            my_epoch,
        );

        Ok(Response::bulk(Bytes::from(info)))
    } else {
        // Standalone mode - return default info
        let info = "\
cluster_state:ok\r\n\
cluster_slots_assigned:16384\r\n\
cluster_slots_ok:16384\r\n\
cluster_slots_pfail:0\r\n\
cluster_slots_fail:0\r\n\
cluster_known_nodes:1\r\n\
cluster_size:1\r\n\
cluster_current_epoch:0\r\n\
cluster_my_epoch:0\r\n\
cluster_stats_messages_ping_sent:0\r\n\
cluster_stats_messages_pong_sent:0\r\n\
cluster_stats_messages_sent:0\r\n\
cluster_stats_messages_ping_received:0\r\n\
cluster_stats_messages_pong_received:0\r\n\
cluster_stats_messages_received:0\r\n\
total_cluster_links_buffer_limit_exceeded:0\r\n";

        Ok(Response::bulk(Bytes::from(info)))
    }
}

/// CLUSTER NODES - Returns the cluster nodes configuration.
fn cluster_nodes(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // Format: <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
    if let Some(cluster_state) = ctx.cluster_state {
        let snapshot = cluster_state.snapshot();
        let my_id = ctx.node_id.unwrap_or(0);

        let mut lines = Vec::new();
        for node in snapshot.nodes.values() {
            // Build flags string
            let mut flags = Vec::new();
            if node.id == my_id {
                flags.push("myself");
            }
            if node.is_primary() {
                flags.push("master");
            }
            if node.is_replica() {
                flags.push("slave");
            }
            if node.flags.fail {
                flags.push("fail");
            }
            if node.flags.pfail {
                flags.push("fail?");
            }
            if node.flags.handshake {
                flags.push("handshake");
            }
            if node.flags.noaddr {
                flags.push("noaddr");
            }
            let flags_str = if flags.is_empty() {
                "noflags".to_string()
            } else {
                flags.join(",")
            };

            // Get master id (- for primary)
            let master_id = node
                .primary_id
                .map(|id| format!("{:040x}", id))
                .unwrap_or_else(|| "-".to_string());

            // Get slot ranges for this node
            let slot_ranges = snapshot.get_node_slots(node.id);
            let slots_str: String = slot_ranges
                .iter()
                .map(|r| {
                    if r.start == r.end {
                        format!("{}", r.start)
                    } else {
                        format!("{}-{}", r.start, r.end)
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");

            // Append migration markers for this node
            let mut migration_str = String::new();
            for migration in snapshot.migrations.values() {
                if migration.source_node == node.id {
                    migration_str.push_str(&format!(
                        " [{}->-{:040x}]",
                        migration.slot, migration.target_node
                    ));
                } else if migration.target_node == node.id {
                    migration_str.push_str(&format!(
                        " [{}-<-{:040x}]",
                        migration.slot, migration.source_node
                    ));
                }
            }

            // Build line
            let line = format!(
                "{:040x} {}:{}@{} {} {} 0 0 {} connected {}{}",
                node.id,
                node.addr.ip(),
                node.addr.port(),
                node.cluster_addr.port(),
                flags_str,
                master_id,
                node.config_epoch,
                slots_str,
                migration_str
            );

            lines.push(line);
        }

        Ok(Response::bulk(Bytes::from(lines.join("\n") + "\n")))
    } else {
        // Standalone mode - return self as a single primary with all slots
        let node_id = ctx.node_id.unwrap_or(1);
        let info = format!(
            "{:040x} 127.0.0.1:6379@16379 myself,master - 0 0 0 connected 0-16383\n",
            node_id
        );

        Ok(Response::bulk(Bytes::from(info)))
    }
}

/// CLUSTER MYID - Returns this node's unique ID.
fn cluster_myid(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // Return this node's ID (40-character hex string)
    let node_id = ctx.node_id.unwrap_or(1);
    Ok(Response::bulk(Bytes::from(format!("{:040x}", node_id))))
}

/// CLUSTER SLOTS - Returns slot to node mappings (deprecated, use CLUSTER SHARDS).
fn cluster_slots(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // Format: [[start, end, [ip, port, id], [replica_ip, replica_port, replica_id], ...], ...]
    if let Some(cluster_state) = ctx.cluster_state {
        let snapshot = cluster_state.snapshot();
        let mut slot_info = Vec::new();

        // Group slots by primary node
        let mut node_slots: std::collections::HashMap<u64, Vec<(u16, u16)>> =
            std::collections::HashMap::new();

        for (slot, node_id) in &snapshot.slot_assignment {
            // Find the primary for this slot
            let primary_id = if let Some(node) = snapshot.nodes.get(node_id) {
                node.primary_id.unwrap_or(*node_id)
            } else {
                *node_id
            };
            node_slots
                .entry(primary_id)
                .or_default()
                .push((*slot, *slot));
        }

        // Convert to slot ranges and build response
        for (node_id, _slots) in node_slots {
            if let Some(node) = snapshot.nodes.get(&node_id) {
                // Merge consecutive slots into ranges
                let ranges = snapshot.get_node_slots(node_id);

                for range in ranges {
                    let mut entry = vec![
                        Response::Integer(range.start as i64),
                        Response::Integer(range.end as i64),
                        Response::Array(vec![
                            Response::bulk(Bytes::from(node.addr.ip().to_string())),
                            Response::Integer(node.addr.port() as i64),
                            Response::bulk(Bytes::from(format!("{:040x}", node.id))),
                        ]),
                    ];

                    // Add replicas
                    for replica in snapshot.nodes.values() {
                        if replica.primary_id == Some(node_id) {
                            entry.push(Response::Array(vec![
                                Response::bulk(Bytes::from(replica.addr.ip().to_string())),
                                Response::Integer(replica.addr.port() as i64),
                                Response::bulk(Bytes::from(format!("{:040x}", replica.id))),
                            ]));
                        }
                    }

                    slot_info.push(Response::Array(entry));
                }
            }
        }

        Ok(Response::Array(slot_info))
    } else {
        // Standalone mode - return all slots assigned to self
        let node_id = ctx.node_id.unwrap_or(1);
        let slot_info = vec![Response::Array(vec![
            Response::Integer(0),
            Response::Integer(16383),
            Response::Array(vec![
                Response::bulk(Bytes::from("127.0.0.1")),
                Response::Integer(6379),
                Response::bulk(Bytes::from(format!("{:040x}", node_id))),
            ]),
        ])];

        Ok(Response::Array(slot_info))
    }
}

/// CLUSTER SHARDS - Returns information about cluster shards (Redis 7.0+).
fn cluster_shards(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    if let Some(cluster_state) = ctx.cluster_state {
        let snapshot = cluster_state.snapshot();
        let mut shards = Vec::new();

        // Group nodes by primary (each primary + its replicas = one shard)
        let mut primary_nodes: Vec<_> =
            snapshot.nodes.values().filter(|n| n.is_primary()).collect();
        primary_nodes.sort_by_key(|n| n.id);

        let my_offset = local_replication_offset(ctx);

        for primary in primary_nodes {
            let slot_ranges = snapshot.get_node_slots(primary.id);

            // Build slots array [start1, end1, start2, end2, ...]
            let mut slots = Vec::new();
            for range in &slot_ranges {
                slots.push(Response::Integer(range.start as i64));
                slots.push(Response::Integer(range.end as i64));
            }

            // Build nodes array
            let mut nodes = Vec::new();

            // Add primary node
            let primary_offset = if Some(primary.id) == ctx.node_id {
                my_offset
            } else {
                0
            };
            nodes.push(Response::Array(vec![
                Response::bulk(Bytes::from("id")),
                Response::bulk(Bytes::from(format!("{:040x}", primary.id))),
                Response::bulk(Bytes::from("port")),
                Response::Integer(primary.addr.port() as i64),
                Response::bulk(Bytes::from("ip")),
                Response::bulk(Bytes::from(primary.addr.ip().to_string())),
                Response::bulk(Bytes::from("endpoint")),
                Response::bulk(Bytes::from(primary.addr.ip().to_string())),
                Response::bulk(Bytes::from("role")),
                Response::bulk(Bytes::from("master")),
                Response::bulk(Bytes::from("replication-offset")),
                Response::Integer(primary_offset),
                Response::bulk(Bytes::from("health")),
                Response::bulk(Bytes::from(if primary.flags.fail {
                    "fail"
                } else if primary.flags.pfail {
                    "loading"
                } else {
                    "online"
                })),
            ]));

            // Add replicas
            for replica in snapshot.nodes.values() {
                if replica.primary_id == Some(primary.id) {
                    let replica_offset = if Some(replica.id) == ctx.node_id {
                        my_offset
                    } else {
                        0
                    };
                    nodes.push(Response::Array(vec![
                        Response::bulk(Bytes::from("id")),
                        Response::bulk(Bytes::from(format!("{:040x}", replica.id))),
                        Response::bulk(Bytes::from("port")),
                        Response::Integer(replica.addr.port() as i64),
                        Response::bulk(Bytes::from("ip")),
                        Response::bulk(Bytes::from(replica.addr.ip().to_string())),
                        Response::bulk(Bytes::from("endpoint")),
                        Response::bulk(Bytes::from(replica.addr.ip().to_string())),
                        Response::bulk(Bytes::from("role")),
                        Response::bulk(Bytes::from("slave")),
                        Response::bulk(Bytes::from("replication-offset")),
                        Response::Integer(replica_offset),
                        Response::bulk(Bytes::from("health")),
                        Response::bulk(Bytes::from(if replica.flags.fail {
                            "fail"
                        } else if replica.flags.pfail {
                            "loading"
                        } else {
                            "online"
                        })),
                    ]));
                }
            }

            let shard = Response::Array(vec![
                Response::bulk(Bytes::from("slots")),
                Response::Array(slots),
                Response::bulk(Bytes::from("nodes")),
                Response::Array(nodes),
            ]);

            shards.push(shard);
        }

        Ok(Response::Array(shards))
    } else {
        // Standalone mode - return single shard
        let node_id = ctx.node_id.unwrap_or(1);
        let shard = Response::Array(vec![
            Response::bulk(Bytes::from("slots")),
            Response::Array(vec![Response::Integer(0), Response::Integer(16383)]),
            Response::bulk(Bytes::from("nodes")),
            Response::Array(vec![Response::Array(vec![
                Response::bulk(Bytes::from("id")),
                Response::bulk(Bytes::from(format!("{:040x}", node_id))),
                Response::bulk(Bytes::from("port")),
                Response::Integer(6379),
                Response::bulk(Bytes::from("ip")),
                Response::bulk(Bytes::from("127.0.0.1")),
                Response::bulk(Bytes::from("endpoint")),
                Response::bulk(Bytes::from("127.0.0.1")),
                Response::bulk(Bytes::from("role")),
                Response::bulk(Bytes::from("master")),
                Response::bulk(Bytes::from("replication-offset")),
                Response::Integer(local_replication_offset(ctx)),
                Response::bulk(Bytes::from("health")),
                Response::bulk(Bytes::from("online")),
            ])]),
        ]);

        Ok(Response::Array(vec![shard]))
    }
}

/// CLUSTER KEYSLOT - Returns the hash slot for a key.
fn cluster_keyslot(key: &Bytes) -> Result<Response, CommandError> {
    let slot = slot_for_key(key);
    Ok(Response::Integer(slot as i64))
}

/// CLUSTER COUNTKEYSINSLOT - Returns the number of keys in a slot.
fn cluster_countkeysinslot(
    ctx: &mut CommandContext,
    slot_arg: &Bytes,
) -> Result<Response, CommandError> {
    let slot: u16 = std::str::from_utf8(slot_arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?;

    if slot >= 16384 {
        return Err(CommandError::InvalidArgument {
            message: "Invalid or out of range slot".to_string(),
        });
    }

    // Query the actual key count for this slot from the store
    let count = ctx.store.count_keys_in_slot(slot);
    Ok(Response::Integer(count as i64))
}

/// CLUSTER GETKEYSINSLOT - Returns keys in a slot.
fn cluster_getkeysinslot(
    ctx: &mut CommandContext,
    slot_arg: &Bytes,
    count_arg: &Bytes,
) -> Result<Response, CommandError> {
    let slot: u16 = std::str::from_utf8(slot_arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?;

    let count: usize = std::str::from_utf8(count_arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid count".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid count".to_string(),
        })?;

    if slot >= 16384 {
        return Err(CommandError::InvalidArgument {
            message: "Invalid or out of range slot".to_string(),
        });
    }

    // Query the actual keys in this slot from the store
    let keys = ctx.store.keys_in_slot(slot, count);
    let response_keys: Vec<Response> = keys.into_iter().map(Response::bulk).collect();
    Ok(Response::Array(response_keys))
}

/// CLUSTER HELP - Returns help for CLUSTER commands.
fn cluster_help() -> Result<Response, CommandError> {
    let help = vec![
        Response::bulk(Bytes::from(
            "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        )),
        Response::bulk(Bytes::from("ADDSLOTS <slot> [<slot> ...]")),
        Response::bulk(Bytes::from("    Assign slots to this node.")),
        Response::bulk(Bytes::from("COUNTKEYSINSLOT <slot>")),
        Response::bulk(Bytes::from("    Return the number of keys in slot.")),
        Response::bulk(Bytes::from("DELSLOTS <slot> [<slot> ...]")),
        Response::bulk(Bytes::from("    Delete slots from this node.")),
        Response::bulk(Bytes::from("FAILOVER [FORCE|TAKEOVER]")),
        Response::bulk(Bytes::from("    Trigger a manual failover.")),
        Response::bulk(Bytes::from("FORGET <node-id>")),
        Response::bulk(Bytes::from("    Remove a node from the cluster.")),
        Response::bulk(Bytes::from("GETKEYSINSLOT <slot> <count>")),
        Response::bulk(Bytes::from("    Return keys in slot.")),
        Response::bulk(Bytes::from("HELP")),
        Response::bulk(Bytes::from("    Prints this help.")),
        Response::bulk(Bytes::from("INFO")),
        Response::bulk(Bytes::from("    Return information about the cluster.")),
        Response::bulk(Bytes::from("KEYSLOT <key>")),
        Response::bulk(Bytes::from("    Return the hash slot for key.")),
        Response::bulk(Bytes::from("MEET <ip> <port>")),
        Response::bulk(Bytes::from("    Connect nodes into a working cluster.")),
        Response::bulk(Bytes::from("MYID")),
        Response::bulk(Bytes::from("    Return this node's ID.")),
        Response::bulk(Bytes::from("NODES")),
        Response::bulk(Bytes::from("    Return cluster node information.")),
        Response::bulk(Bytes::from("REPLICATE <node-id>")),
        Response::bulk(Bytes::from(
            "    Configure this node as a replica of the specified node.",
        )),
        Response::bulk(Bytes::from("RESET [HARD|SOFT]")),
        Response::bulk(Bytes::from("    Reset the cluster state.")),
        Response::bulk(Bytes::from("SAVECONFIG")),
        Response::bulk(Bytes::from(
            "    Force saving cluster configuration on disk.",
        )),
        Response::bulk(Bytes::from("SET-CONFIG-EPOCH <epoch>")),
        Response::bulk(Bytes::from("    Set config epoch in this node.")),
        Response::bulk(Bytes::from(
            "SETSLOT <slot> IMPORTING|MIGRATING|NODE|STABLE [<node-id>]",
        )),
        Response::bulk(Bytes::from("    Set slot state.")),
        Response::bulk(Bytes::from("SHARDS")),
        Response::bulk(Bytes::from("    Return information about cluster shards.")),
        Response::bulk(Bytes::from("SLOTS")),
        Response::bulk(Bytes::from(
            "    Return slot range information (deprecated, use SHARDS).",
        )),
    ];

    Ok(Response::Array(help))
}

// ============================================================================
// ASKING - Indicates client is in an ASKING state for slot migration
// ============================================================================

pub struct AskingCommand;

impl Command for AskingCommand {
    fn name(&self) -> &'static str {
        "ASKING"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::FAST | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // In cluster mode, this sets a flag on the connection that the next
        // command should be executed even if the slot is being migrated.
        // For standalone mode, this is a no-op.
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// ============================================================================
// READONLY - Sets connection to read-only mode for cluster replicas
// ============================================================================

pub struct ReadonlyCommand;

impl Command for ReadonlyCommand {
    fn name(&self) -> &'static str {
        "READONLY"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // In cluster mode, this enables reading from replica nodes.
        // For standalone mode, this is a no-op.
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// ============================================================================
// READWRITE - Clears read-only mode
// ============================================================================

pub struct ReadwriteCommand;

impl Command for ReadwriteCommand {
    fn name(&self) -> &'static str {
        "READWRITE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // In cluster mode, this clears the read-only flag.
        // For standalone mode, this is a no-op.
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}
