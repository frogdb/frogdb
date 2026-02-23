//! Cluster admin commands - state-modifying operations.
//!
//! These commands modify cluster state and return `Response::RaftNeeded`
//! which is intercepted by the connection handler.

use std::net::SocketAddr;

use bytes::Bytes;
use frogdb_core::{CommandContext, CommandError};
use frogdb_protocol::{RaftClusterOp, Response};

/// CLUSTER MEET - Adds a node to the cluster.
/// Syntax: CLUSTER MEET <ip> <port> [<cluster-bus-port>]
///
/// Returns `RaftNeeded` response which is intercepted by the connection handler.
pub(super) fn cluster_meet(
    ctx: &mut CommandContext,
    host: &Bytes,
    port: &Bytes,
    cluster_bus_port_arg: Option<&Bytes>,
) -> Result<Response, CommandError> {
    // Verify cluster mode is enabled
    if ctx.raft.is_none() {
        return Err(CommandError::ClusterDisabled);
    }

    let host_str = std::str::from_utf8(host).map_err(|_| CommandError::InvalidArgument {
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
    let addr: SocketAddr = format!("{}:{}", host_str, port_num).parse().map_err(|_| {
        CommandError::InvalidArgument {
            message: "invalid address".to_string(),
        }
    })?;

    // Cluster bus port: use explicit value if provided, otherwise client port + 10000
    let cluster_port: u16 = if let Some(cbp) = cluster_bus_port_arg {
        std::str::from_utf8(cbp)
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid cluster bus port".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid cluster bus port number".to_string(),
            })?
    } else {
        port_num
            .checked_add(10000)
            .ok_or(CommandError::InvalidArgument {
                message: "port number too high (would overflow when adding cluster bus offset)"
                    .to_string(),
            })?
    };
    let cluster_addr: SocketAddr =
        format!("{}:{}", host_str, cluster_port)
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid cluster address".to_string(),
            })?;

    // Generate node ID from address hash
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    addr.hash(&mut hasher);
    let node_id = hasher.finish();

    // Return RaftNeeded - the connection handler will execute this asynchronously
    // and update the NetworkFactory after successful Raft commit
    Ok(Response::RaftNeeded {
        op: RaftClusterOp::AddNode {
            node_id,
            addr,
            cluster_addr,
        },
        register_node: Some((node_id, cluster_addr)),
        unregister_node: None,
    })
}

/// CLUSTER FORGET - Removes a node from the cluster.
///
/// Returns `RaftNeeded` response which is intercepted by the connection handler.
pub(super) fn cluster_forget(
    ctx: &mut CommandContext,
    node_id_arg: &Bytes,
) -> Result<Response, CommandError> {
    // Verify cluster mode is enabled
    if ctx.raft.is_none() {
        return Err(CommandError::ClusterDisabled);
    }

    // Node ID is a 40-character hex string
    let node_id_str =
        std::str::from_utf8(node_id_arg).map_err(|_| CommandError::InvalidArgument {
            message: "invalid node ID".to_string(),
        })?;

    let node_id =
        u64::from_str_radix(node_id_str, 16).map_err(|_| CommandError::InvalidArgument {
            message: "invalid node ID format".to_string(),
        })?;

    // Return RaftNeeded - the connection handler will execute this asynchronously
    // and update the NetworkFactory after successful Raft commit
    Ok(Response::RaftNeeded {
        op: RaftClusterOp::RemoveNode { node_id },
        register_node: None,
        unregister_node: Some(node_id),
    })
}

/// CLUSTER ADDSLOTS - Assigns hash slots to this node.
///
/// Returns `RaftNeeded` response which is intercepted by the connection handler.
pub(super) fn cluster_addslots(
    ctx: &mut CommandContext,
    slots: &[Bytes],
) -> Result<Response, CommandError> {
    let node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;

    // Verify cluster mode is enabled
    if ctx.raft.is_none() {
        return Err(CommandError::ClusterDisabled);
    }

    // Parse slot numbers
    let mut parsed_slots = Vec::new();
    for slot_arg in slots {
        let slot_str =
            std::str::from_utf8(slot_arg).map_err(|_| CommandError::InvalidArgument {
                message: "invalid slot".to_string(),
            })?;

        let slot: u16 = slot_str
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid slot number".to_string(),
            })?;

        if slot >= 16384 {
            return Err(CommandError::InvalidArgument {
                message: "slot out of range".to_string(),
            });
        }

        parsed_slots.push(slot);
    }

    // Return RaftNeeded - the connection handler will execute this asynchronously
    Ok(Response::RaftNeeded {
        op: RaftClusterOp::AssignSlots {
            node_id,
            slots: parsed_slots,
        },
        register_node: None,
        unregister_node: None,
    })
}

/// CLUSTER DELSLOTS - Removes hash slot assignments from this node.
///
/// Returns `RaftNeeded` response which is intercepted by the connection handler.
pub(super) fn cluster_delslots(
    ctx: &mut CommandContext,
    slots: &[Bytes],
) -> Result<Response, CommandError> {
    let node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;

    // Verify cluster mode is enabled
    if ctx.raft.is_none() {
        return Err(CommandError::ClusterDisabled);
    }

    // Parse slot numbers
    let mut parsed_slots = Vec::new();
    for slot_arg in slots {
        let slot_str =
            std::str::from_utf8(slot_arg).map_err(|_| CommandError::InvalidArgument {
                message: "invalid slot".to_string(),
            })?;

        let slot: u16 = slot_str
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid slot number".to_string(),
            })?;

        if slot >= 16384 {
            return Err(CommandError::InvalidArgument {
                message: "slot out of range".to_string(),
            });
        }

        parsed_slots.push(slot);
    }

    // Return RaftNeeded - the connection handler will execute this asynchronously
    Ok(Response::RaftNeeded {
        op: RaftClusterOp::RemoveSlots {
            node_id,
            slots: parsed_slots,
        },
        register_node: None,
        unregister_node: None,
    })
}

/// CLUSTER FAILOVER - Initiates a manual failover.
///
/// This command must be run on a replica node. It promotes the replica
/// to primary and takes over the slots from its former primary.
///
/// Options:
/// - FORCE: Force failover even if the primary appears to be down
/// - TAKEOVER: Force takeover without waiting for primary acknowledgment
pub(super) fn cluster_failover(
    ctx: &mut CommandContext,
    args: &[Bytes],
) -> Result<Response, CommandError> {
    let node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;
    let cluster_state = ctx.cluster_state.ok_or(CommandError::ClusterDisabled)?;

    // Verify cluster mode is enabled
    if ctx.raft.is_none() {
        return Err(CommandError::ClusterDisabled);
    }

    // Parse options: FORCE or TAKEOVER
    let force = args.iter().any(|a| {
        std::str::from_utf8(a)
            .map(|s| s.eq_ignore_ascii_case("FORCE"))
            .unwrap_or(false)
    });

    let takeover = args.iter().any(|a| {
        std::str::from_utf8(a)
            .map(|s| s.eq_ignore_ascii_case("TAKEOVER"))
            .unwrap_or(false)
    });

    // Get this node's info
    let snapshot = cluster_state.snapshot();
    let this_node = snapshot
        .nodes
        .get(&node_id)
        .ok_or_else(|| CommandError::InvalidArgument {
            message: "Node not found in cluster state".to_string(),
        })?;

    // Verify this node is a replica
    if this_node.is_primary() {
        return Err(CommandError::InvalidArgument {
            message: "CLUSTER FAILOVER can only be run on a replica".to_string(),
        });
    }

    // Get the primary we're replicating
    let primary_id = this_node
        .primary_id
        .ok_or_else(|| CommandError::InvalidArgument {
            message: "Replica has no primary configured".to_string(),
        })?;

    let primary = snapshot.nodes.get(&primary_id);

    // If not FORCE/TAKEOVER, check primary is healthy
    if !force && !takeover {
        if let Some(p) = primary {
            if p.flags.fail {
                return Err(CommandError::InvalidArgument {
                    message: "Primary is marked as failed. Use FORCE or TAKEOVER.".to_string(),
                });
            }
        } else {
            return Err(CommandError::InvalidArgument {
                message: "Primary not found. Use FORCE or TAKEOVER.".to_string(),
            });
        }
    }

    // Return RaftNeeded to perform the failover asynchronously
    // This will: 1) Change role to Primary, 2) Take over slots
    Ok(Response::RaftNeeded {
        op: RaftClusterOp::Failover {
            replica_id: node_id,
            primary_id,
            force: force || takeover,
        },
        register_node: None,
        unregister_node: None,
    })
}

/// CLUSTER REPLICATE - Configures this node as a replica of another.
///
/// Returns `RaftNeeded` response which is intercepted by the connection handler.
pub(super) fn cluster_replicate(
    ctx: &mut CommandContext,
    primary_id_arg: &Bytes,
) -> Result<Response, CommandError> {
    let node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;

    // Verify cluster mode is enabled
    if ctx.raft.is_none() {
        return Err(CommandError::ClusterDisabled);
    }

    // Parse primary node ID (40-character hex string)
    let primary_id_str =
        std::str::from_utf8(primary_id_arg).map_err(|_| CommandError::InvalidArgument {
            message: "invalid node ID".to_string(),
        })?;

    let primary_id =
        u64::from_str_radix(primary_id_str, 16).map_err(|_| CommandError::InvalidArgument {
            message: "invalid node ID format".to_string(),
        })?;

    // Return RaftNeeded - the connection handler will execute this asynchronously
    Ok(Response::RaftNeeded {
        op: RaftClusterOp::SetRole {
            node_id,
            is_replica: true,
            primary_id: Some(primary_id),
        },
        register_node: None,
        unregister_node: None,
    })
}

/// CLUSTER RESET - Resets cluster state.
pub(super) fn cluster_reset(
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
pub(super) fn cluster_saveconfig(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    let _raft = ctx.raft.ok_or(CommandError::ClusterDisabled)?;

    // Raft already persists cluster state automatically via its log storage.
    // This command is a no-op but returns OK for compatibility.
    Ok(Response::ok())
}

/// CLUSTER SET-CONFIG-EPOCH - Sets the configuration epoch.
///
/// Returns `RaftNeeded` response which is intercepted by the connection handler.
pub(super) fn cluster_set_config_epoch(
    ctx: &mut CommandContext,
    epoch: &Bytes,
) -> Result<Response, CommandError> {
    // Verify cluster mode is enabled
    if ctx.raft.is_none() {
        return Err(CommandError::ClusterDisabled);
    }

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
    Ok(Response::RaftNeeded {
        op: RaftClusterOp::IncrementEpoch,
        register_node: None,
        unregister_node: None,
    })
}

/// CLUSTER SETSLOT - Sets slot state for migration.
/// Syntax: CLUSTER SETSLOT <slot> IMPORTING|MIGRATING|NODE|STABLE [<node-id>]
///
/// Returns `RaftNeeded` response which is intercepted by the connection handler.
pub(super) fn cluster_setslot(
    ctx: &mut CommandContext,
    args: &[Bytes],
) -> Result<Response, CommandError> {
    let my_node_id = ctx.node_id.ok_or(CommandError::ClusterDisabled)?;

    // Verify cluster mode is enabled
    if ctx.raft.is_none() {
        return Err(CommandError::ClusterDisabled);
    }

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
            let source_id_str =
                std::str::from_utf8(&args[2]).map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID".to_string(),
                })?;
            let source_node = u64::from_str_radix(source_id_str, 16).map_err(|_| {
                CommandError::InvalidArgument {
                    message: "invalid node ID format".to_string(),
                }
            })?;

            Ok(Response::RaftNeeded {
                op: RaftClusterOp::BeginSlotMigration {
                    slot,
                    source_node,
                    target_node: my_node_id,
                },
                register_node: None,
                unregister_node: None,
            })
        }
        "MIGRATING" => {
            // Mark slot as migrating to target node
            if args.len() < 3 {
                return Err(CommandError::WrongArgCount {
                    command: "CLUSTER SETSLOT MIGRATING".to_string(),
                });
            }
            let target_id_str =
                std::str::from_utf8(&args[2]).map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID".to_string(),
                })?;
            let target_node = u64::from_str_radix(target_id_str, 16).map_err(|_| {
                CommandError::InvalidArgument {
                    message: "invalid node ID format".to_string(),
                }
            })?;

            Ok(Response::RaftNeeded {
                op: RaftClusterOp::BeginSlotMigration {
                    slot,
                    source_node: my_node_id,
                    target_node,
                },
                register_node: None,
                unregister_node: None,
            })
        }
        "NODE" => {
            // Assign slot to specified node (completes migration)
            if args.len() < 3 {
                return Err(CommandError::WrongArgCount {
                    command: "CLUSTER SETSLOT NODE".to_string(),
                });
            }
            let node_id_str =
                std::str::from_utf8(&args[2]).map_err(|_| CommandError::InvalidArgument {
                    message: "invalid node ID".to_string(),
                })?;
            let target_node = u64::from_str_radix(node_id_str, 16).map_err(|_| {
                CommandError::InvalidArgument {
                    message: "invalid node ID format".to_string(),
                }
            })?;

            // Check if there's an active migration to complete
            if let Some(cluster_state) = ctx.cluster_state
                && let Some(migration) = cluster_state.get_slot_migration(slot)
                && migration.target_node == target_node
            {
                // Complete the migration
                return Ok(Response::RaftNeeded {
                    op: RaftClusterOp::CompleteSlotMigration {
                        slot,
                        source_node: migration.source_node,
                        target_node,
                    },
                    register_node: None,
                    unregister_node: None,
                });
            }

            // No migration in progress, just assign the slot
            Ok(Response::RaftNeeded {
                op: RaftClusterOp::AssignSlots {
                    node_id: target_node,
                    slots: vec![slot],
                },
                register_node: None,
                unregister_node: None,
            })
        }
        "STABLE" => {
            // Cancel any migration for this slot
            Ok(Response::RaftNeeded {
                op: RaftClusterOp::CancelSlotMigration { slot },
                register_node: None,
                unregister_node: None,
            })
        }
        _ => Err(CommandError::InvalidArgument {
            message: format!("Unknown SETSLOT subcommand: {}", subcommand),
        }),
    }
}
