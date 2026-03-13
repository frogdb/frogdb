//! Standalone utility functions for the connection module.
//!
//! These functions have no coupling to `ConnectionHandler` and are used
//! by various handler submodules.

use bytes::Bytes;
use frogdb_core::{
    CommandFlags, KeyAccessType, StreamId,
    cluster::{ClusterCommand, NodeInfo, NodeRole, SlotRange},
};
use frogdb_protocol::{ParsedCommand, RaftClusterOp};

/// Determine key access type from command flags.
pub(crate) fn key_access_type_for_flags(flags: CommandFlags) -> KeyAccessType {
    if flags.contains(CommandFlags::READONLY) {
        KeyAccessType::Read
    } else if flags.contains(CommandFlags::WRITE) {
        KeyAccessType::Write
    } else {
        // Commands with neither flag (admin commands, etc.) - check both
        KeyAccessType::ReadWrite
    }
}

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
        frogdb_protocol::BlockingOp::Wait {
            num_replicas,
            timeout_ms,
        } => frogdb_core::BlockingOp::Wait {
            num_replicas,
            timeout_ms,
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

/// Format a Unix timestamp as ISO 8601 string.
pub(crate) fn format_timestamp_iso(secs: u64) -> String {
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Calculate date from days since epoch (1970-01-01)
    let mut year = 1970i32;
    let mut remaining_days = days_since_epoch as i32;

    loop {
        let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        let days_in_year = if is_leap { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let days_in_months: [i32; 12] = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days_in_month in &days_in_months {
        if remaining_days < days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }
    let day = remaining_days + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Convert protocol RaftClusterOp to core ClusterCommand.
/// Returns None for operations that require special handling (e.g., Failover).
pub(crate) fn convert_raft_cluster_op(op: &RaftClusterOp) -> Option<ClusterCommand> {
    match op {
        RaftClusterOp::AddNode {
            node_id,
            addr,
            cluster_addr,
        } => Some(ClusterCommand::AddNode {
            node: NodeInfo::new_primary(*node_id, *addr, *cluster_addr),
        }),
        RaftClusterOp::RemoveNode { node_id } => {
            Some(ClusterCommand::RemoveNode { node_id: *node_id })
        }
        RaftClusterOp::AssignSlots { node_id, slots } => Some(ClusterCommand::AssignSlots {
            node_id: *node_id,
            slots: slots.iter().map(|&s| SlotRange::single(s)).collect(),
        }),
        RaftClusterOp::RemoveSlots { node_id, slots } => Some(ClusterCommand::RemoveSlots {
            node_id: *node_id,
            slots: slots.iter().map(|&s| SlotRange::single(s)).collect(),
        }),
        RaftClusterOp::SetRole {
            node_id,
            is_replica,
            primary_id,
        } => Some(ClusterCommand::SetRole {
            node_id: *node_id,
            role: if *is_replica {
                NodeRole::Replica
            } else {
                NodeRole::Primary
            },
            primary_id: *primary_id,
        }),
        RaftClusterOp::BeginSlotMigration {
            slot,
            source_node,
            target_node,
        } => Some(ClusterCommand::BeginSlotMigration {
            slot: *slot,
            source_node: *source_node,
            target_node: *target_node,
        }),
        RaftClusterOp::CompleteSlotMigration {
            slot,
            source_node,
            target_node,
        } => Some(ClusterCommand::CompleteSlotMigration {
            slot: *slot,
            source_node: *source_node,
            target_node: *target_node,
        }),
        RaftClusterOp::CancelSlotMigration { slot } => {
            Some(ClusterCommand::CancelSlotMigration { slot: *slot })
        }
        RaftClusterOp::IncrementEpoch => Some(ClusterCommand::IncrementEpoch),
        RaftClusterOp::MarkNodeFailed { node_id } => {
            Some(ClusterCommand::MarkNodeFailed { node_id: *node_id })
        }
        RaftClusterOp::MarkNodeRecovered { node_id } => {
            Some(ClusterCommand::MarkNodeRecovered { node_id: *node_id })
        }
        // Failover requires special handling - multiple Raft commands
        RaftClusterOp::Failover { .. } => None,
        // ResetCluster requires special handling (update self_node_id after commit)
        RaftClusterOp::ResetCluster { .. } => None,
    }
}

/// Commands that have subcommands (container commands in Redis terminology).
pub(crate) const CONTAINER_COMMANDS: &[&str] = &[
    "ACL", "CLIENT", "CONFIG", "CLUSTER", "DEBUG", "MEMORY", "MODULE", "OBJECT", "SCRIPT",
    "SLOWLOG", "XGROUP", "XINFO", "COMMAND", "PUBSUB", "FUNCTION", "LATENCY", "STATUS",
];

/// Extract subcommand from args for container commands.
pub(crate) fn extract_subcommand(command: &str, args: &[Bytes]) -> Option<String> {
    if CONTAINER_COMMANDS
        .iter()
        .any(|c| c.eq_ignore_ascii_case(command))
    {
        args.first()
            .map(|a| String::from_utf8_lossy(a).to_uppercase())
    } else {
        None
    }
}
