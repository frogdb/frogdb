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
use frogdb_cluster::wire;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy, slot_for_key,
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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "cluster",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::ADMIN.union(CommandFlags::STALE),
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
///
/// Rendering is owned by `frogdb_cluster::wire::render_cluster_nodes`; this
/// adapter just picks the snapshot (live cluster state, or a synthetic
/// single-primary standalone snapshot) and the local node id.
fn cluster_nodes(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    let text = if let Some(cluster_state) = ctx.cluster_state {
        let snapshot = cluster_state.snapshot();
        let my_id = ctx.node_id.unwrap_or(0);
        wire::render_cluster_nodes(&snapshot, my_id)
    } else {
        // Standalone mode - single primary owning all slots.
        let node_id = ctx.node_id.unwrap_or(1);
        wire::render_cluster_nodes(&wire::standalone_snapshot(node_id), node_id)
    };

    Ok(Response::bulk(Bytes::from(text)))
}

/// CLUSTER MYID - Returns this node's unique ID.
fn cluster_myid(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // Return this node's ID (40-character hex string)
    let node_id = ctx.node_id.unwrap_or(1);
    Ok(Response::bulk(Bytes::from(format!("{:040x}", node_id))))
}

/// CLUSTER SLOTS - Returns slot to node mappings (deprecated, use CLUSTER SHARDS).
///
/// Format: `[[start, end, [ip, port, id], [replica_ip, replica_port, replica_id], ...], ...]`.
/// Grouping/sorting is owned by `frogdb_cluster::wire::shard_views`; this adapter
/// maps each shard's slot ranges to RESP. Shards whose primary owns zero slots are
/// skipped (matching the historical SLOTS behavior; SHARDS keeps them).
fn cluster_slots(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    let snapshot = match ctx.cluster_state {
        Some(cluster_state) => cluster_state.snapshot(),
        // Standalone mode - single primary owning all slots.
        None => wire::standalone_snapshot(ctx.node_id.unwrap_or(1)),
    };

    Ok(map_slots_response(&wire::shard_views(&snapshot)))
}

/// Map grouped [`wire::ShardView`]s to the `CLUSTER SLOTS` RESP array. Skips
/// shards whose primary owns no slots (their `slots` vec is empty).
fn map_slots_response(views: &[wire::ShardView<'_>]) -> Response {
    let mut slot_info = Vec::new();
    for view in views {
        if view.slots.is_empty() {
            continue;
        }
        for range in &view.slots {
            let mut entry = vec![
                Response::Integer(range.start as i64),
                Response::Integer(range.end as i64),
                Response::Array(vec![
                    Response::bulk(Bytes::from(view.primary.node.addr.ip().to_string())),
                    Response::Integer(view.primary.node.addr.port() as i64),
                    Response::bulk(Bytes::from(wire::format_node_id(view.primary.id))),
                ]),
            ];
            for replica in &view.replicas {
                entry.push(Response::Array(vec![
                    Response::bulk(Bytes::from(replica.node.addr.ip().to_string())),
                    Response::Integer(replica.node.addr.port() as i64),
                    Response::bulk(Bytes::from(wire::format_node_id(replica.id))),
                ]));
            }
            slot_info.push(Response::Array(entry));
        }
    }
    Response::Array(slot_info)
}

/// CLUSTER SHARDS - Returns information about cluster shards (Redis 7.0+).
///
/// Grouping/sorting is owned by `frogdb_cluster::wire::shard_views`; this adapter
/// maps each shard to RESP, overlaying the server-only replication offset for the
/// local node (which the `cluster` crate does not and must not see).
fn cluster_shards(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    let my_offset = local_replication_offset(ctx);
    let (snapshot, my_id) = match ctx.cluster_state {
        Some(cluster_state) => (cluster_state.snapshot(), ctx.node_id),
        // Standalone mode - single primary owning all slots. Attribute the local
        // offset to that node regardless of whether `ctx.node_id` is set.
        None => {
            let node_id = ctx.node_id.unwrap_or(1);
            (wire::standalone_snapshot(node_id), Some(node_id))
        }
    };

    Ok(map_shards_response(
        &wire::shard_views(&snapshot),
        my_id,
        my_offset,
    ))
}

/// Map grouped [`wire::ShardView`]s to the `CLUSTER SHARDS` RESP array,
/// overlaying `my_offset` as the `replication-offset` of the node whose id equals
/// `my_id` (every other node reports 0).
fn map_shards_response(
    views: &[wire::ShardView<'_>],
    my_id: Option<frogdb_cluster::NodeId>,
    my_offset: i64,
) -> Response {
    let node_entry = |view: &wire::NodeView<'_>, role: &'static str| -> Response {
        let offset = if Some(view.id) == my_id { my_offset } else { 0 };
        Response::Array(vec![
            Response::bulk(Bytes::from("id")),
            Response::bulk(Bytes::from(wire::format_node_id(view.id))),
            Response::bulk(Bytes::from("port")),
            Response::Integer(view.node.addr.port() as i64),
            Response::bulk(Bytes::from("ip")),
            Response::bulk(Bytes::from(view.node.addr.ip().to_string())),
            Response::bulk(Bytes::from("endpoint")),
            Response::bulk(Bytes::from(view.node.addr.ip().to_string())),
            Response::bulk(Bytes::from("role")),
            Response::bulk(Bytes::from(role)),
            Response::bulk(Bytes::from("replication-offset")),
            Response::Integer(offset),
            Response::bulk(Bytes::from("health")),
            Response::bulk(Bytes::from(view.health)),
        ])
    };

    let mut shards = Vec::new();
    for view in views {
        let mut slots = Vec::new();
        for range in &view.slots {
            slots.push(Response::Integer(range.start as i64));
            slots.push(Response::Integer(range.end as i64));
        }

        let mut nodes = Vec::with_capacity(1 + view.replicas.len());
        nodes.push(node_entry(&view.primary, "master"));
        for replica in &view.replicas {
            nodes.push(node_entry(replica, "slave"));
        }

        shards.push(Response::Array(vec![
            Response::bulk(Bytes::from("slots")),
            Response::Array(slots),
            Response::bulk(Bytes::from("nodes")),
            Response::Array(nodes),
        ]));
    }
    Response::Array(shards)
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

// ASKING / READONLY / READWRITE were migrated behind the ConnCtx seam as
// mutating connection commands (they set per-connection cluster-redirect flags).
// See `crate::connection::connection_state_conn_command`.

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_cluster::{ClusterSnapshot, NodeInfo};

    fn addr(s: &str) -> std::net::SocketAddr {
        s.parse().unwrap()
    }

    fn as_arr(r: &Response) -> &[Response] {
        match r {
            Response::Array(v) => v,
            _ => panic!("expected array"),
        }
    }

    fn as_int(r: &Response) -> i64 {
        match r {
            Response::Integer(i) => *i,
            _ => panic!("expected integer"),
        }
    }

    fn as_bulk(r: &Response) -> String {
        match r {
            Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
            _ => panic!("expected bulk"),
        }
    }

    /// Look up a value in a flat `[k1, v1, k2, v2, ...]` bulk-keyed node entry.
    fn field<'a>(entry: &'a [Response], key: &str) -> &'a Response {
        entry
            .chunks_exact(2)
            .find(|pair| as_bulk(&pair[0]) == key)
            .map(|pair| &pair[1])
            .unwrap_or_else(|| panic!("field {key} not found"))
    }

    /// Primary 1 owns slots 0-9 with replica 3; primary 2 owns zero slots.
    fn fixture() -> ClusterSnapshot {
        let mut snap = ClusterSnapshot::new();
        snap.nodes.insert(
            1,
            NodeInfo::new_primary(1, addr("127.0.0.1:7001"), addr("127.0.0.1:17001")),
        );
        snap.nodes.insert(
            2,
            NodeInfo::new_primary(2, addr("127.0.0.1:7002"), addr("127.0.0.1:17002")),
        );
        snap.nodes.insert(
            3,
            NodeInfo::new_replica(3, addr("127.0.0.1:7003"), addr("127.0.0.1:17003"), 1),
        );
        for slot in 0..10 {
            snap.slot_assignment.insert(slot, 1);
        }
        snap
    }

    #[test]
    fn test_map_slots_response_skips_zero_slot_primary() {
        let snap = fixture();
        let resp = map_slots_response(&wire::shard_views(&snap));
        let entries = as_arr(&resp);

        // Only primary 1 (owns slots) is emitted; primary 2 (zero slots) skipped.
        assert_eq!(entries.len(), 1);
        let entry = as_arr(&entries[0]);
        assert_eq!(as_int(&entry[0]), 0);
        assert_eq!(as_int(&entry[1]), 9);

        // [ip, port, id] for the primary, then the replica.
        let primary = as_arr(&entry[2]);
        assert_eq!(as_bulk(&primary[0]), "127.0.0.1");
        assert_eq!(as_int(&primary[1]), 7001);
        assert_eq!(
            as_bulk(&primary[2]),
            "0000000000000000000000000000000000000001"
        );
        let replica = as_arr(&entry[3]);
        assert_eq!(
            as_bulk(&replica[2]),
            "0000000000000000000000000000000000000003"
        );
    }

    #[test]
    fn test_map_shards_response_overlays_offset_for_local_node_only() {
        let snap = fixture();
        // Local node is the primary (id 1); its offset must be overlaid, every
        // other node reports 0. Zero-slot primary 2 is still present (SHARDS keeps
        // it, unlike SLOTS).
        let resp = map_shards_response(&wire::shard_views(&snap), Some(1), 42);
        let shards = as_arr(&resp);
        assert_eq!(shards.len(), 2);

        // Shard 0 = primary 1 + replica 3.
        let shard0 = as_arr(&shards[0]);
        let nodes0 = as_arr(field(shard0, "nodes"));
        assert_eq!(nodes0.len(), 2);

        let primary = as_arr(&nodes0[0]);
        assert_eq!(as_bulk(field(primary, "role")), "master");
        assert_eq!(as_int(field(primary, "replication-offset")), 42);
        assert_eq!(as_bulk(field(primary, "health")), "online");

        let replica = as_arr(&nodes0[1]);
        assert_eq!(as_bulk(field(replica, "role")), "slave");
        assert_eq!(as_int(field(replica, "replication-offset")), 0);

        // Shard 1 = zero-slot primary 2: present, empty slots, offset 0.
        let shard1 = as_arr(&shards[1]);
        assert!(as_arr(field(shard1, "slots")).is_empty());
        let p2 = as_arr(&as_arr(field(shard1, "nodes"))[0]);
        assert_eq!(as_int(field(p2, "replication-offset")), 0);
    }
}
