//! Standalone utility functions for the connection module.
//!
//! These functions have no coupling to `ConnectionHandler` and are used
//! by various connection submodules.

use frogdb_core::{
    StreamId,
    cluster::{ClusterCommand, NodeInfo, NodeRole, SlotRange},
};
use frogdb_protocol::{ParsedCommand, RaftClusterOp};

// The ACL layer's three name/flag→access mappings live in `frogdb-core`
// (`command.rs`), not here: the shard-level `ShardWriteSeam` authorizes a Lua
// script's runtime writes with the *same* derivation the connection uses, and a
// second copy of the mapping is exactly the drift `PermissionGuard` was created
// to prevent. Re-exported so the connection call sites keep their short path.
pub(crate) use frogdb_core::command::{
    extract_subcommand, key_access_type_for_flags, required_access_for_key_flags,
};

/// Convert protocol BlockingOp to core BlockingOp.
pub(crate) fn convert_blocking_op(op: frogdb_protocol::BlockingOp) -> frogdb_core::BlockingOp {
    match op {
        frogdb_protocol::BlockingOp::BLPop => frogdb_core::BlockingOp::BLPop,
        frogdb_protocol::BlockingOp::BRPop => frogdb_core::BlockingOp::BRPop,
        frogdb_protocol::BlockingOp::BLMove {
            dest,
            src_dir,
            dest_dir,
        } => frogdb_core::BlockingOp::BLMove {
            dest,
            src_dir: convert_direction(src_dir),
            dest_dir: convert_direction(dest_dir),
        },
        frogdb_protocol::BlockingOp::BLMPop { direction, count } => {
            frogdb_core::BlockingOp::BLMPop {
                direction: convert_direction(direction),
                count,
            }
        }
        frogdb_protocol::BlockingOp::BZPopMin => frogdb_core::BlockingOp::BZPopMin,
        frogdb_protocol::BlockingOp::BZPopMax => frogdb_core::BlockingOp::BZPopMax,
        frogdb_protocol::BlockingOp::BZMPop { min, count } => {
            frogdb_core::BlockingOp::BZMPop { min, count }
        }
        frogdb_protocol::BlockingOp::XRead { after_ids, count } => frogdb_core::BlockingOp::XRead {
            after_ids: after_ids
                .into_iter()
                .map(|(ms, seq)| StreamId::new(ms, seq))
                .collect(),
            count,
        },
        frogdb_protocol::BlockingOp::XReadGroup {
            group,
            consumer,
            noack,
            count,
        } => frogdb_core::BlockingOp::XReadGroup {
            group,
            consumer,
            noack,
            count,
        },
    }
}

/// Convert protocol Direction to core Direction.
pub(crate) fn convert_direction(dir: frogdb_protocol::Direction) -> frogdb_core::Direction {
    match dir {
        frogdb_protocol::Direction::Left => frogdb_core::Direction::Left,
        frogdb_protocol::Direction::Right => frogdb_core::Direction::Right,
    }
}

/// Estimate the size of a RESP2 frame in bytes.
/// This is an approximation based on the frame structure.
pub(crate) fn estimate_resp2_frame_size(frame: &redis_protocol::resp2::types::BytesFrame) -> usize {
    use redis_protocol::resp2::types::BytesFrame;
    match frame {
        BytesFrame::SimpleString(s) => 1 + s.len() + 2, // +, string, CRLF
        BytesFrame::Error(e) => 1 + e.len() + 2,        // -, message, CRLF
        BytesFrame::Integer(i) => 1 + format!("{}", i).len() + 2, // :, number, CRLF
        BytesFrame::BulkString(bs) => {
            1 + format!("{}", bs.len()).len() + 2 + bs.len() + 2 // $, len, CRLF, data, CRLF
        }
        BytesFrame::Array(arr) => {
            let header = 1 + format!("{}", arr.len()).len() + 2; // *, count, CRLF
            let elements: usize = arr.iter().map(estimate_resp2_frame_size).sum();
            header + elements
        }
        BytesFrame::Null => 5, // $-1\r\n
    }
}

/// Estimate the size of a command in bytes (received from client).
pub(crate) fn estimate_command_size(cmd: &ParsedCommand) -> usize {
    // Account for RESP array header + name + all args
    // Format: *<n>\r\n$<len>\r\n<name>\r\n$<len>\r\n<arg>\r\n...
    let n = 1 + cmd.args.len(); // command name + args
    let header = 1 + format!("{}", n).len() + 2; // *<n>\r\n

    let name_size = 1 + format!("{}", cmd.name.len()).len() + 2 + cmd.name.len() + 2;
    let args_size: usize = cmd
        .args
        .iter()
        .map(|a| 1 + format!("{}", a.len()).len() + 2 + a.len() + 2)
        .sum();

    header + name_size + args_size
}

/// Convert a protocol [`RaftClusterOp`] into its core [`ClusterCommand`].
///
/// This adapter is **total**: every `RaftClusterOp` variant maps to a
/// `ClusterCommand`. The `match` has no `_` arm, so adding a new
/// `RaftClusterOp` variant is a compile error here until it is mapped — and
/// because the return type is `ClusterCommand` (not `Option`), a new variant
/// cannot be discharged with `=> None` and silently deferred to a runtime
/// error; it must produce a real command.
///
/// `RaftClusterOp` lives in the `protocol` crate (which cannot depend on
/// `cluster`) and `ClusterCommand` lives in `cluster`; both are foreign to the
/// `server` crate, so the orphan rule rules out a `From` impl. `server` is the
/// only crate that sees both types, hence a free function.
///
/// Note on `ResetCluster`: `handle_reset_command` builds its own
/// `ClusterCommand::ResetCluster` from already-destructured fields and never
/// routes through this adapter (it needs a post-commit `set_self_node_id` side
/// effect). The arm here exists purely to keep the match total.
pub(crate) fn raft_op_to_command(op: &RaftClusterOp) -> ClusterCommand {
    match op {
        RaftClusterOp::AddNode {
            node_id,
            addr,
            cluster_addr,
        } => ClusterCommand::AddNode {
            node: NodeInfo::new_primary(*node_id, *addr, *cluster_addr),
        },
        RaftClusterOp::RemoveNode { node_id } => ClusterCommand::RemoveNode { node_id: *node_id },
        RaftClusterOp::AssignSlots { node_id, slots } => ClusterCommand::AssignSlots {
            node_id: *node_id,
            slots: slots.iter().map(|&s| SlotRange::single(s)).collect(),
        },
        RaftClusterOp::RemoveSlots { node_id, slots } => ClusterCommand::RemoveSlots {
            node_id: *node_id,
            slots: slots.iter().map(|&s| SlotRange::single(s)).collect(),
        },
        RaftClusterOp::SetRole {
            node_id,
            is_replica,
            primary_id,
        } => ClusterCommand::SetRole {
            node_id: *node_id,
            role: if *is_replica {
                NodeRole::Replica
            } else {
                NodeRole::Primary
            },
            primary_id: *primary_id,
        },
        RaftClusterOp::IncrementEpoch => ClusterCommand::IncrementEpoch,
        RaftClusterOp::SetConfigEpoch { node_id, epoch } => ClusterCommand::SetConfigEpoch {
            node_id: *node_id,
            epoch: *epoch,
        },
        RaftClusterOp::MarkNodeFailed { node_id } => {
            ClusterCommand::MarkNodeFailed { node_id: *node_id }
        }
        RaftClusterOp::MarkNodeRecovered { node_id } => {
            ClusterCommand::MarkNodeRecovered { node_id: *node_id }
        }
        RaftClusterOp::FinalizeUpgrade { version } => ClusterCommand::FinalizeUpgrade {
            version: version.clone(),
        },
        // Failover maps to the atomic composite command: role change, slot
        // transfer, and epoch bump are one replicated state-machine transition.
        RaftClusterOp::Failover {
            replica_id,
            primary_id,
            force,
        } => ClusterCommand::Failover {
            old_primary_id: *primary_id,
            new_primary_id: *replica_id,
            force: *force,
        },
        RaftClusterOp::ResetCluster {
            node_id,
            new_node_id,
        } => ClusterCommand::ResetCluster {
            node_id: *node_id,
            new_node_id: *new_node_id,
        },
    }
}

/// Validate a client name for `CLIENT SETNAME` / `HELLO … SETNAME`.
///
/// Redis (`networking.c` `clientSetNameOrReply`) rejects any byte outside the
/// printable ASCII range `0x21..=0x7e`, so a name can never contain a space,
/// newline, or NUL. `CLIENT LIST` renders one space-and-newline-delimited row
/// per connection with the name echoed verbatim; an unsanitised name would
/// forge an extra row or split a field. An empty name is valid — it clears the
/// connection name.
pub(crate) fn validate_client_name(name: &[u8]) -> Result<(), &'static str> {
    if name.iter().any(|&b| !(0x21..=0x7e).contains(&b)) {
        return Err("ERR Client names cannot contain spaces, newlines or special characters.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::KeyAccessFlag::{OW, R, RW, W};
    use frogdb_core::acl::KeyAccessType;

    #[test]
    fn read_only_flag_maps_to_read() {
        assert_eq!(
            required_access_for_key_flags(&[R], KeyAccessType::ReadWrite),
            KeyAccessType::Read
        );
    }

    #[test]
    fn validate_client_name_rejects_non_printable() {
        // Empty clears the name — allowed.
        assert!(validate_client_name(b"").is_ok());
        assert!(validate_client_name(b"good-name_123").is_ok());
        // Every byte outside 0x21..=0x7e is rejected: space, newline, CR, NUL,
        // tab, DEL, high bit.
        for bad in [
            b"has space".as_slice(),
            b"line\ninject",
            b"cr\rinject",
            b"nul\0byte",
            b"tab\there",
            b"del\x7f",
            b"high\x80bit",
        ] {
            assert!(
                validate_client_name(bad).is_err(),
                "{:?} must be rejected",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn write_flags_map_to_write() {
        assert_eq!(
            required_access_for_key_flags(&[W], KeyAccessType::Read),
            KeyAccessType::Write
        );
        assert_eq!(
            required_access_for_key_flags(&[OW], KeyAccessType::Read),
            KeyAccessType::Write
        );
    }

    #[test]
    fn rw_flag_maps_to_readwrite() {
        assert_eq!(
            required_access_for_key_flags(&[RW], KeyAccessType::Read),
            KeyAccessType::ReadWrite
        );
    }

    #[test]
    fn read_plus_write_flags_map_to_readwrite() {
        assert_eq!(
            required_access_for_key_flags(&[R, W], KeyAccessType::Read),
            KeyAccessType::ReadWrite
        );
        assert_eq!(
            required_access_for_key_flags(&[R, OW], KeyAccessType::Read),
            KeyAccessType::ReadWrite
        );
    }

    #[test]
    fn empty_flags_fall_back_to_command_level() {
        assert_eq!(
            required_access_for_key_flags(&[], KeyAccessType::Write),
            KeyAccessType::Write
        );
        assert_eq!(
            required_access_for_key_flags(&[], KeyAccessType::Read),
            KeyAccessType::Read
        );
    }

    fn addr(port: u16) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    }

    /// Every `RaftClusterOp` variant maps to its expected `ClusterCommand`
    /// discriminant — including `Failover` and `ResetCluster`, which the old
    /// `Option`-returning converter left as untested `None` cases. This is the
    /// regression guard the previous design could not express.
    #[test]
    fn raft_op_to_command_maps_every_variant() {
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::AddNode {
                node_id: 1,
                addr: addr(6379),
                cluster_addr: addr(16379),
            }),
            ClusterCommand::AddNode { .. }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::RemoveNode { node_id: 1 }),
            ClusterCommand::RemoveNode { node_id: 1 }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::AssignSlots {
                node_id: 1,
                slots: vec![0, 1, 2],
            }),
            ClusterCommand::AssignSlots { node_id: 1, .. }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::RemoveSlots {
                node_id: 1,
                slots: vec![0, 1, 2],
            }),
            ClusterCommand::RemoveSlots { node_id: 1, .. }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::SetRole {
                node_id: 1,
                is_replica: true,
                primary_id: Some(2),
            }),
            ClusterCommand::SetRole { node_id: 1, .. }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::IncrementEpoch),
            ClusterCommand::IncrementEpoch
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::MarkNodeFailed { node_id: 1 }),
            ClusterCommand::MarkNodeFailed { node_id: 1 }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::MarkNodeRecovered { node_id: 1 }),
            ClusterCommand::MarkNodeRecovered { node_id: 1 }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::FinalizeUpgrade {
                version: "1.2.3".to_string(),
            }),
            ClusterCommand::FinalizeUpgrade { .. }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::Failover {
                replica_id: 2,
                primary_id: 1,
                force: false,
            }),
            ClusterCommand::Failover { .. }
        ));
        assert!(matches!(
            raft_op_to_command(&RaftClusterOp::ResetCluster {
                node_id: 1,
                new_node_id: Some(9),
            }),
            ClusterCommand::ResetCluster {
                node_id: 1,
                new_node_id: Some(9),
            }
        ));
    }

    /// Pin the `Failover` field cross-wiring that previously lived in a
    /// hand-written dispatch arm in `cluster.rs`: the protocol's `primary_id`
    /// becomes `old_primary_id` and `replica_id` becomes `new_primary_id`.
    #[test]
    fn failover_maps_old_and_new_primary() {
        let cmd = raft_op_to_command(&RaftClusterOp::Failover {
            replica_id: 2,
            primary_id: 1,
            force: true,
        });
        match cmd {
            ClusterCommand::Failover {
                old_primary_id,
                new_primary_id,
                force,
            } => {
                assert_eq!(old_primary_id, 1);
                assert_eq!(new_primary_id, 2);
                assert!(force);
            }
            other => panic!("expected Failover, got {other:?}"),
        }
    }

    /// The `is_replica` bool selects between `NodeRole::Replica` and
    /// `NodeRole::Primary`.
    #[test]
    fn set_role_replica_vs_primary() {
        let replica = raft_op_to_command(&RaftClusterOp::SetRole {
            node_id: 1,
            is_replica: true,
            primary_id: Some(2),
        });
        assert!(matches!(
            replica,
            ClusterCommand::SetRole {
                role: NodeRole::Replica,
                primary_id: Some(2),
                ..
            }
        ));

        let primary = raft_op_to_command(&RaftClusterOp::SetRole {
            node_id: 1,
            is_replica: false,
            primary_id: None,
        });
        assert!(matches!(
            primary,
            ClusterCommand::SetRole {
                role: NodeRole::Primary,
                primary_id: None,
                ..
            }
        ));
    }
}
