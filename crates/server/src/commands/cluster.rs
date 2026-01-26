//! CLUSTER command implementation.
//!
//! Provides Redis Cluster protocol commands for:
//! - Cluster state inspection (INFO, NODES, SLOTS, MYID)
//! - Cluster topology management (MEET, FORGET, ADDSLOTS, DELSLOTS)
//! - Key routing (KEYSLOT, COUNTKEYSINSLOT)
//! - Failover coordination (FAILOVER)

use bytes::Bytes;
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
fn cluster_info(_ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // For now, return standalone mode info
    // When cluster is enabled, this will be populated from ClusterState
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

/// CLUSTER NODES - Returns the cluster nodes configuration.
fn cluster_nodes(_ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // For standalone mode, return self as a single primary with all slots
    // Format: <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
    let node_id = "0000000000000000000000000000000000000001";
    let info = format!(
        "{} 127.0.0.1:6379@16379 myself,master - 0 0 0 connected 0-16383\n",
        node_id
    );

    Ok(Response::bulk(Bytes::from(info)))
}

/// CLUSTER MYID - Returns this node's unique ID.
fn cluster_myid(_ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // For standalone mode, return a static ID
    // When cluster is enabled, this will return the actual node ID
    Ok(Response::bulk(Bytes::from(
        "0000000000000000000000000000000000000001",
    )))
}

/// CLUSTER SLOTS - Returns slot to node mappings (deprecated, use CLUSTER SHARDS).
fn cluster_slots(_ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // For standalone mode, return all slots assigned to self
    // Format: [[start, end, [ip, port, id], [replica_ip, replica_port, replica_id], ...], ...]
    let slot_info = vec![Response::Array(vec![
        Response::Integer(0),      // start slot
        Response::Integer(16383),  // end slot
        Response::Array(vec![
            Response::bulk(Bytes::from("127.0.0.1")),
            Response::Integer(6379),
            Response::bulk(Bytes::from("0000000000000000000000000000000000000001")),
        ]),
    ])];

    Ok(Response::Array(slot_info))
}

/// CLUSTER SHARDS - Returns information about cluster shards (Redis 7.0+).
fn cluster_shards(_ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // Return shard information in the new format
    let shard = Response::Array(vec![
        Response::bulk(Bytes::from("slots")),
        Response::Array(vec![
            Response::Integer(0),
            Response::Integer(16383),
        ]),
        Response::bulk(Bytes::from("nodes")),
        Response::Array(vec![Response::Array(vec![
            Response::bulk(Bytes::from("id")),
            Response::bulk(Bytes::from("0000000000000000000000000000000000000001")),
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

/// CLUSTER MEET - Adds a node to the cluster.
fn cluster_meet(
    _ctx: &mut CommandContext,
    _host: &Bytes,
    _port: &Bytes,
) -> Result<Response, CommandError> {
    // When cluster is enabled, this will send a Raft AddNode command
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER FORGET - Removes a node from the cluster.
fn cluster_forget(_ctx: &mut CommandContext, _node_id: &Bytes) -> Result<Response, CommandError> {
    // When cluster is enabled, this will send a Raft RemoveNode command
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER ADDSLOTS - Assigns hash slots to this node.
fn cluster_addslots(
    _ctx: &mut CommandContext,
    _slots: &[Bytes],
) -> Result<Response, CommandError> {
    // When cluster is enabled, this will send a Raft AssignSlots command
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER DELSLOTS - Removes hash slot assignments from this node.
fn cluster_delslots(
    _ctx: &mut CommandContext,
    _slots: &[Bytes],
) -> Result<Response, CommandError> {
    // When cluster is enabled, this will send a Raft RemoveSlots command
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER FAILOVER - Initiates a manual failover.
fn cluster_failover(
    _ctx: &mut CommandContext,
    _args: &[Bytes],
) -> Result<Response, CommandError> {
    // When cluster is enabled, this will coordinate failover via Raft
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER REPLICATE - Configures this node as a replica of another.
fn cluster_replicate(
    _ctx: &mut CommandContext,
    _node_id: &Bytes,
) -> Result<Response, CommandError> {
    // When cluster is enabled, this will send a Raft SetRole command
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER RESET - Resets cluster state.
fn cluster_reset(
    _ctx: &mut CommandContext,
    _args: &[Bytes],
) -> Result<Response, CommandError> {
    // When cluster is enabled, this will reset local cluster state
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER SAVECONFIG - Saves cluster configuration to disk.
fn cluster_saveconfig(_ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // When cluster is enabled, this will persist cluster state
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER SET-CONFIG-EPOCH - Sets the configuration epoch.
fn cluster_set_config_epoch(
    _ctx: &mut CommandContext,
    _epoch: &Bytes,
) -> Result<Response, CommandError> {
    // When cluster is enabled, this will update the config epoch via Raft
    Err(CommandError::ClusterDisabled)
}

/// CLUSTER SETSLOT - Sets slot state for migration.
fn cluster_setslot(
    _ctx: &mut CommandContext,
    _args: &[Bytes],
) -> Result<Response, CommandError> {
    // When cluster is enabled, this will manage slot migration state
    Err(CommandError::ClusterDisabled)
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
