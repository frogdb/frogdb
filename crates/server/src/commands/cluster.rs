//! CLUSTER command implementation.
//!
//! Provides Redis Cluster protocol commands for:
//! - Cluster state inspection (INFO, NODES, SLOTS, MYID)
//! - Cluster topology management (MEET, FORGET, ADDSLOTS, DELSLOTS)
//! - Key routing (KEYSLOT, COUNTKEYSINSLOT)
//! - Failover coordination (FAILOVER)

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::cluster::{ClusterCommand as RaftClusterCommand, ClusterRaft, NodeInfo, NodeRole, SlotRange};
use frogdb_core::{slot_for_key, Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

// ============================================================================
// CLUSTER - Cluster management command
// ============================================================================

pub struct ClusterCommand;

impl Command for ClusterCommand {
    fn name(&self) -> &'static str {
        "CLUSTER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::STALE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArgCount {
                command: "CLUSTER".to_string(),
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
                        command: "CLUSTER KEYSLOT".to_string(),
                    });
                }
                cluster_keyslot(&args[1])
            }
            "COUNTKEYSINSLOT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER COUNTKEYSINSLOT".to_string(),
                    });
                }
                cluster_countkeysinslot(ctx, &args[1])
            }
            "GETKEYSINSLOT" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER GETKEYSINSLOT".to_string(),
                    });
                }
                cluster_getkeysinslot(ctx, &args[1], &args[2])
            }
            "MEET" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER MEET".to_string(),
                    });
                }
                cluster_meet(ctx, &args[1], &args[2])
            }
            "FORGET" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER FORGET".to_string(),
                    });
                }
                cluster_forget(ctx, &args[1])
            }
            "ADDSLOTS" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER ADDSLOTS".to_string(),
                    });
                }
                cluster_addslots(ctx, &args[1..])
            }
            "DELSLOTS" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER DELSLOTS".to_string(),
                    });
                }
                cluster_delslots(ctx, &args[1..])
            }
            "FAILOVER" => cluster_failover(ctx, &args[1..]),
            "REPLICATE" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER REPLICATE".to_string(),
                    });
                }
                cluster_replicate(ctx, &args[1])
            }
            "RESET" => cluster_reset(ctx, &args[1..]),
            "SAVECONFIG" => cluster_saveconfig(ctx),
            "SET-CONFIG-EPOCH" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER SET-CONFIG-EPOCH".to_string(),
                    });
                }
                cluster_set_config_epoch(ctx, &args[1])
            }
            "SETSLOT" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "CLUSTER SETSLOT".to_string(),
                    });
                }
                cluster_setslot(ctx, &args[1..])
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
    if let Some(ref cluster_state) = ctx.cluster_state {
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

        let info = format!(
            "\
cluster_state:ok\r\n\
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
            slots_assigned,
            slots_assigned, // slots_ok = slots_assigned for now
            known_nodes,
            cluster_size,
            snapshot.config_epoch,
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
    if let Some(ref cluster_state) = ctx.cluster_state {
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

            // Build line
            let line = format!(
                "{:040x} {}:{}@{} {} {} 0 0 {} connected {}",
                node.id,
                node.addr.ip(),
                node.addr.port(),
                node.cluster_addr.port(),
                flags_str,
                master_id,
                node.config_epoch,
                slots_str
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
    if let Some(ref cluster_state) = ctx.cluster_state {
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
        for (node_id, slots) in node_slots {
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
    if let Some(ref cluster_state) = ctx.cluster_state {
        let snapshot = cluster_state.snapshot();
        let mut shards = Vec::new();

        // Group nodes by primary (each primary + its replicas = one shard)
        let mut primary_nodes: Vec<_> = snapshot
            .nodes
            .values()
            .filter(|n| n.is_primary())
            .collect();
        primary_nodes.sort_by_key(|n| n.id);

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
                Response::Integer(0), // TODO: Track replication offset in cluster state
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
                        Response::Integer(0), // TODO: Track replication offset in cluster state
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
                Response::Integer(0),
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
    _ctx: &mut CommandContext,
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

    // In standalone mode, we don't track keys by slot
    // When cluster is enabled, this will query the actual slot key count
    Ok(Response::Integer(0))
}

/// CLUSTER GETKEYSINSLOT - Returns keys in a slot.
fn cluster_getkeysinslot(
    _ctx: &mut CommandContext,
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

    let _count: usize = std::str::from_utf8(count_arg)
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

    // In standalone mode, return empty array
    // When cluster is enabled, this will return actual keys
    Ok(Response::Array(vec![]))
}

/// Execute a Raft command and return the result.
/// Uses block_on since Command::execute is synchronous.
fn execute_raft_command(
    raft: &Arc<ClusterRaft>,
    cmd: RaftClusterCommand,
) -> Result<(), CommandError> {
    let handle = tokio::runtime::Handle::current();
    handle.block_on(async {
        raft.client_write(cmd)
            .await
            .map_err(|e| CommandError::Internal {
                message: format!("Raft error: {}", e),
            })?;
        Ok(())
    })
}

/// CLUSTER MEET - Adds a node to the cluster.
fn cluster_meet(
    ctx: &mut CommandContext,
    host: &Bytes,
    port: &Bytes,
) -> Result<Response, CommandError> {
    let raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;

    let host_str = std::str::from_utf8(host)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid host".to_string(),
        })?;

    let port_num: u16 = std::str::from_utf8(port)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid port".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid port number".to_string(),
        })?;

    // Parse client address
    let addr: SocketAddr = format!("{}:{}", host_str, port_num)
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid address".to_string(),
        })?;

    // Cluster bus port is typically client port + 10000
    let cluster_port = port_num + 10000;
    let cluster_addr: SocketAddr = format!("{}:{}", host_str, cluster_port)
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid cluster address".to_string(),
        })?;

    // Generate node ID from address hash
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    addr.hash(&mut hasher);
    let node_id = hasher.finish();

    let node = NodeInfo::new_primary(node_id, addr, cluster_addr);
    let cmd = RaftClusterCommand::AddNode { node };

    execute_raft_command(raft, cmd)?;
    Ok(Response::ok())
}

/// CLUSTER FORGET - Removes a node from the cluster.
fn cluster_forget(ctx: &mut CommandContext, node_id_arg: &Bytes) -> Result<Response, CommandError> {
    let raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;

    // Node ID is a 40-character hex string
    let node_id_str = std::str::from_utf8(node_id_arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid node ID".to_string(),
        })?;

    let node_id = u64::from_str_radix(node_id_str, 16)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid node ID format".to_string(),
        })?;

    let cmd = RaftClusterCommand::RemoveNode { node_id };
    execute_raft_command(raft, cmd)?;
    Ok(Response::ok())
}

/// CLUSTER ADDSLOTS - Assigns hash slots to this node.
fn cluster_addslots(
    ctx: &mut CommandContext,
    slots: &[Bytes],
) -> Result<Response, CommandError> {
    let raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;
    let node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;

    // Parse slot numbers
    let mut slot_ranges = Vec::new();
    for slot_arg in slots {
        let slot_str = std::str::from_utf8(slot_arg)
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid slot".to_string(),
            })?;

        let slot: u16 = slot_str.parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid slot number".to_string(),
            })?;

        if slot >= 16384 {
            return Err(CommandError::InvalidArgument {
                message: "slot out of range".to_string(),
            });
        }

        slot_ranges.push(SlotRange::single(slot));
    }

    let cmd = RaftClusterCommand::AssignSlots {
        node_id,
        slots: slot_ranges,
    };
    execute_raft_command(raft, cmd)?;
    Ok(Response::ok())
}

/// CLUSTER DELSLOTS - Removes hash slot assignments from this node.
fn cluster_delslots(
    ctx: &mut CommandContext,
    slots: &[Bytes],
) -> Result<Response, CommandError> {
    let raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;
    let node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;

    // Parse slot numbers
    let mut slot_ranges = Vec::new();
    for slot_arg in slots {
        let slot_str = std::str::from_utf8(slot_arg)
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid slot".to_string(),
            })?;

        let slot: u16 = slot_str.parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid slot number".to_string(),
            })?;

        if slot >= 16384 {
            return Err(CommandError::InvalidArgument {
                message: "slot out of range".to_string(),
            });
        }

        slot_ranges.push(SlotRange::single(slot));
    }

    let cmd = RaftClusterCommand::RemoveSlots {
        node_id,
        slots: slot_ranges,
    };
    execute_raft_command(raft, cmd)?;
    Ok(Response::ok())
}

/// CLUSTER FAILOVER - Initiates a manual failover.
fn cluster_failover(
    ctx: &mut CommandContext,
    args: &[Bytes],
) -> Result<Response, CommandError> {
    let _raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;

    // Parse options: FORCE or TAKEOVER
    let _force = args.iter().any(|a| {
        std::str::from_utf8(a)
            .map(|s| s.eq_ignore_ascii_case("FORCE"))
            .unwrap_or(false)
    });

    let _takeover = args.iter().any(|a| {
        std::str::from_utf8(a)
            .map(|s| s.eq_ignore_ascii_case("TAKEOVER"))
            .unwrap_or(false)
    });

    // TODO: Implement actual failover logic
    // For now, return OK to indicate the command is recognized
    // Full implementation requires:
    // 1. Check if this node is a replica
    // 2. Request promotion from primary (or force it)
    // 3. Update cluster state via Raft
    Ok(Response::ok())
}

/// CLUSTER REPLICATE - Configures this node as a replica of another.
fn cluster_replicate(
    ctx: &mut CommandContext,
    primary_id_arg: &Bytes,
) -> Result<Response, CommandError> {
    let raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;
    let node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;

    // Parse primary node ID (40-character hex string)
    let primary_id_str = std::str::from_utf8(primary_id_arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid node ID".to_string(),
        })?;

    let primary_id = u64::from_str_radix(primary_id_str, 16)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid node ID format".to_string(),
        })?;

    let cmd = RaftClusterCommand::SetRole {
        node_id,
        role: NodeRole::Replica,
        primary_id: Some(primary_id),
    };
    execute_raft_command(raft, cmd)?;
    Ok(Response::ok())
}

/// CLUSTER RESET - Resets cluster state.
fn cluster_reset(
    ctx: &mut CommandContext,
    args: &[Bytes],
) -> Result<Response, CommandError> {
    let _raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;

    // Parse options: HARD or SOFT (default is SOFT)
    let _hard = args.iter().any(|a| {
        std::str::from_utf8(a)
            .map(|s| s.eq_ignore_ascii_case("HARD"))
            .unwrap_or(false)
    });

    // TODO: Implement actual reset logic
    // SOFT: Remove slots, make this node a primary
    // HARD: Also reset config epoch and change node ID
    // For now, return OK to indicate the command is recognized
    Ok(Response::ok())
}

/// CLUSTER SAVECONFIG - Saves cluster configuration to disk.
fn cluster_saveconfig(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    let _raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;

    // Raft already persists cluster state automatically via its log storage.
    // This command is a no-op but returns OK for compatibility.
    Ok(Response::ok())
}

/// CLUSTER SET-CONFIG-EPOCH - Sets the configuration epoch.
fn cluster_set_config_epoch(
    ctx: &mut CommandContext,
    epoch: &Bytes,
) -> Result<Response, CommandError> {
    let raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;

    let _epoch_num: u64 = std::str::from_utf8(epoch)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid epoch".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid epoch number".to_string(),
        })?;

    // For now, just increment the epoch via Raft
    // A full implementation would set it to a specific value
    let cmd = RaftClusterCommand::IncrementEpoch;
    execute_raft_command(raft, cmd)?;
    Ok(Response::ok())
}

/// CLUSTER SETSLOT - Sets slot state for migration.
/// Syntax: CLUSTER SETSLOT <slot> IMPORTING|MIGRATING|NODE|STABLE [<node-id>]
fn cluster_setslot(
    ctx: &mut CommandContext,
    args: &[Bytes],
) -> Result<Response, CommandError> {
    let raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;
    let my_node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;

    if args.is_empty() {
        return Err(CommandError::WrongArgCount {
            command: "CLUSTER SETSLOT".to_string(),
        });
    }

    // Parse slot number
    let slot: u16 = std::str::from_utf8(&args[0])
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot number".to_string(),
        })?;

    if slot >= 16384 {
        return Err(CommandError::InvalidArgument {
            message: "slot out of range".to_string(),
        });
    }

    if args.len() < 2 {
        return Err(CommandError::WrongArgCount {
            command: "CLUSTER SETSLOT".to_string(),
        });
    }

    let subcommand = std::str::from_utf8(&args[1])
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid subcommand".to_string(),
        })?
        .to_ascii_uppercase();

    match subcommand.as_str() {
        "IMPORTING" => {
            // Mark slot as importing from source node
            if args.len() < 3 {
                return Err(CommandError::WrongArgCount {
                    command: "CLUSTER SETSLOT IMPORTING".to_string(),
                });
            }
            let source_id_str = std::str::from_utf8(&args[2])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID".to_string(),
                })?;
            let source_node = u64::from_str_radix(source_id_str, 16)
                .map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID format".to_string(),
                })?;

            let cmd = RaftClusterCommand::BeginSlotMigration {
                slot,
                source_node,
                target_node: my_node_id,
            };
            execute_raft_command(raft, cmd)?;
        }
        "MIGRATING" => {
            // Mark slot as migrating to target node
            if args.len() < 3 {
                return Err(CommandError::WrongArgCount {
                    command: "CLUSTER SETSLOT MIGRATING".to_string(),
                });
            }
            let target_id_str = std::str::from_utf8(&args[2])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID".to_string(),
                })?;
            let target_node = u64::from_str_radix(target_id_str, 16)
                .map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID format".to_string(),
                })?;

            let cmd = RaftClusterCommand::BeginSlotMigration {
                slot,
                source_node: my_node_id,
                target_node,
            };
            execute_raft_command(raft, cmd)?;
        }
        "NODE" => {
            // Assign slot to specified node (completes migration)
            if args.len() < 3 {
                return Err(CommandError::WrongArgCount {
                    command: "CLUSTER SETSLOT NODE".to_string(),
                });
            }
            let node_id_str = std::str::from_utf8(&args[2])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID".to_string(),
                })?;
            let target_node = u64::from_str_radix(node_id_str, 16)
                .map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID format".to_string(),
                })?;

            // Check if there's an active migration to complete
            if let Some(ref cluster_state) = ctx.cluster_state {
                if let Some(migration) = cluster_state.get_slot_migration(slot) {
                    if migration.target_node == target_node {
                        // Complete the migration
                        let cmd = RaftClusterCommand::CompleteSlotMigration {
                            slot,
                            source_node: migration.source_node,
                            target_node,
                        };
                        execute_raft_command(raft, cmd)?;
                        return Ok(Response::ok());
                    }
                }
            }

            // No migration in progress, just assign the slot
            let cmd = RaftClusterCommand::AssignSlots {
                node_id: target_node,
                slots: vec![SlotRange::single(slot)],
            };
            execute_raft_command(raft, cmd)?;
        }
        "STABLE" => {
            // Cancel any migration for this slot
            let cmd = RaftClusterCommand::CancelSlotMigration { slot };
            execute_raft_command(raft, cmd)?;
        }
        _ => {
            return Err(CommandError::InvalidArgument {
                message: format!("Unknown SETSLOT subcommand: {}", subcommand),
            });
        }
    }

    Ok(Response::ok())
}

/// CLUSTER HELP - Returns help for CLUSTER commands.
fn cluster_help() -> Result<Response, CommandError> {
    let help = vec![
        Response::bulk(Bytes::from("CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")),
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
        Response::bulk(Bytes::from("    Configure this node as a replica of the specified node.")),
        Response::bulk(Bytes::from("RESET [HARD|SOFT]")),
        Response::bulk(Bytes::from("    Reset the cluster state.")),
        Response::bulk(Bytes::from("SAVECONFIG")),
        Response::bulk(Bytes::from("    Force saving cluster configuration on disk.")),
        Response::bulk(Bytes::from("SET-CONFIG-EPOCH <epoch>")),
        Response::bulk(Bytes::from("    Set config epoch in this node.")),
        Response::bulk(Bytes::from("SETSLOT <slot> IMPORTING|MIGRATING|NODE|STABLE [<node-id>]")),
        Response::bulk(Bytes::from("    Set slot state.")),
        Response::bulk(Bytes::from("SHARDS")),
        Response::bulk(Bytes::from("    Return information about cluster shards.")),
        Response::bulk(Bytes::from("SLOTS")),
        Response::bulk(Bytes::from("    Return slot range information (deprecated, use SHARDS).")),
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
