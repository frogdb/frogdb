//! A [`ReplicationBroadcaster`] that records what a shard propagated.
//!
//! The shard-driver harness previously had no way to see the replication side
//! of an effect: [`frogdb_core::NoopBroadcaster`] discards everything and
//! reports `is_active() == false`, which also means the shard's propagation
//! gate short-circuits and no frames are produced at all. Tests whose subject
//! is "what did (or did not) reach the replica" therefore had nothing to
//! assert against.
//!
//! This records every `(shard_id, command, args)` the worker emits and reports
//! itself active, so the gate stays open. It is a *frame recorder*, not a
//! replication link: nothing is applied anywhere, and the returned offset is
//! just a monotonic count of recorded frames.

use std::sync::Mutex;

use bytes::Bytes;
use frogdb_core::ReplicationBroadcaster;

/// One propagated frame, as the shard handed it to replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastFrame {
    /// The shard the write executed on, as stamped into the frame.
    pub shard_id: u16,
    /// Uppercase command name (`SET`, `DEL`, `MULTI`, ...).
    pub command: String,
    /// The command's arguments, excluding the name.
    pub args: Vec<Bytes>,
}

impl BroadcastFrame {
    /// The first argument rendered as a lossy string — for most write commands
    /// this is the key, which is what assertions usually want.
    pub fn first_arg_lossy(&self) -> String {
        self.args
            .first()
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .unwrap_or_default()
    }
}

/// Records every frame a shard broadcasts. Always reports active.
#[derive(Debug, Default)]
pub struct RecordingBroadcaster {
    frames: Mutex<Vec<BroadcastFrame>>,
}

impl RecordingBroadcaster {
    /// A recorder with an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every frame recorded so far, in propagation order.
    pub fn frames(&self) -> Vec<BroadcastFrame> {
        self.frames.lock().unwrap().clone()
    }

    /// The recorded command names, in order — the usual shape for asserting
    /// what a shard did or did not propagate.
    pub fn command_names(&self) -> Vec<String> {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .map(|f| f.command.clone())
            .collect()
    }

    /// Frames whose command matches `name` (case-insensitively).
    pub fn frames_named(&self, name: &str) -> Vec<BroadcastFrame> {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f.command.eq_ignore_ascii_case(name))
            .cloned()
            .collect()
    }

    /// Drop everything recorded so far, so a test can ignore setup traffic.
    pub fn clear(&self) {
        self.frames.lock().unwrap().clear();
    }
}

impl ReplicationBroadcaster for RecordingBroadcaster {
    fn broadcast_command_on_shard(&self, shard_id: u16, cmd_name: &str, args: &[Bytes]) -> u64 {
        let mut frames = self.frames.lock().unwrap();
        frames.push(BroadcastFrame {
            shard_id,
            command: cmd_name.to_ascii_uppercase(),
            args: args.to_vec(),
        });
        frames.len() as u64
    }

    /// Always true: a recorder that reported itself inactive would close the
    /// shard's propagation gate and record nothing, which is the opposite of
    /// what it exists for.
    fn is_active(&self) -> bool {
        true
    }

    fn current_offset(&self) -> u64 {
        self.frames.lock().unwrap().len() as u64
    }
}
