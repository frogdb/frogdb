//! Server-side implementation of the replica apply seam.
//!
//! Transaction reconstruction, tagged routing, and result-checking live in the
//! `frogdb-replication` crate ([`frogdb_replication::apply`]). This module
//! provides only the mechanical shard-touching half behind the
//! [`ReplicaApplier`] seam: route a group of replicated commands to the shard
//! the primary tagged the frame with, execute them with
//! `REPLICA_INTERNAL_CONN_ID` (so they are not re-broadcast), and report whether
//! they applied cleanly.
//!
//! Unlike the previous consumer, routing comes from the frame's origin-shard tag
//! (not re-derived from `args[0]`), a `MULTI … EXEC` group is applied atomically
//! via [`CoreMsg::ExecTransaction`], and the shard's response is checked so
//! a failed apply surfaces as a divergence instead of being silently dropped.

use std::sync::Arc;

use frogdb_core::{CoreMsg, REPLICA_INTERNAL_CONN_ID, ShardSender, TransactionResult};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use frogdb_replication::{ApplyError, ReplicaApplier};
use tokio::sync::oneshot;

/// Applies replicated command groups to shards on behalf of the replication
/// consume loop.
///
/// Holds the shard channels and routes strictly by the origin-shard tag carried
/// on each frame (validated against `num_shards`).
pub struct ReplicaCommandExecutor {
    /// Shard message senders, indexed by shard id.
    shard_senders: Arc<Vec<ShardSender>>,
    /// Number of shards, for validating the tagged origin shard.
    num_shards: usize,
}

impl ReplicaCommandExecutor {
    /// Create a new replica command executor.
    pub fn new(shard_senders: Arc<Vec<ShardSender>>, num_shards: usize) -> Self {
        Self {
            shard_senders,
            num_shards,
        }
    }

    /// Resolve the sender for a tagged origin shard, or an [`ApplyError`].
    fn sender_for(&self, shard_id: u16) -> Result<&ShardSender, ApplyError> {
        let idx = shard_id as usize;
        if idx >= self.num_shards {
            return Err(ApplyError::ShardOutOfRange(shard_id, self.num_shards));
        }
        self.shard_senders
            .get(idx)
            .ok_or(ApplyError::ShardOutOfRange(shard_id, self.num_shards))
    }

    /// Apply a single replicated command on `shard_id`, checking the response.
    async fn apply_single(&self, shard_id: u16, command: ParsedCommand) -> Result<(), ApplyError> {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = CoreMsg::Execute {
            command: Arc::new(command),
            conn_id: REPLICA_INTERNAL_CONN_ID,
            txid: None,
            protocol_version: ProtocolVersion::Resp2,
            track_reads: false,
            no_touch: false,
            response_tx,
        };
        self.sender_for(shard_id)?
            .send(msg)
            .await
            .map_err(|_| ApplyError::ShardUnavailable(shard_id))?;

        let response = response_rx
            .await
            .map_err(|_| ApplyError::ShardUnavailable(shard_id))?;
        match response {
            Response::Error(e) | Response::BlobError(e) => Err(ApplyError::Rejected {
                shard: shard_id,
                detail: String::from_utf8_lossy(&e).into_owned(),
            }),
            _ => Ok(()),
        }
    }

    /// Apply a reconstructed `MULTI … EXEC` group atomically on `shard_id`,
    /// checking the transaction result.
    async fn apply_transaction(
        &self,
        shard_id: u16,
        commands: Vec<ParsedCommand>,
    ) -> Result<(), ApplyError> {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = CoreMsg::ExecTransaction {
            commands,
            watches: Vec::new(),
            conn_id: REPLICA_INTERNAL_CONN_ID,
            protocol_version: ProtocolVersion::Resp2,
            response_tx,
        };
        self.sender_for(shard_id)?
            .send(msg)
            .await
            .map_err(|_| ApplyError::ShardUnavailable(shard_id))?;

        match response_rx
            .await
            .map_err(|_| ApplyError::ShardUnavailable(shard_id))?
        {
            TransactionResult::Success(_) => Ok(()),
            TransactionResult::WatchAborted => Err(ApplyError::Rejected {
                shard: shard_id,
                detail: "transaction aborted by WATCH conflict".to_string(),
            }),
            TransactionResult::Error(e) => Err(ApplyError::Rejected {
                shard: shard_id,
                detail: e,
            }),
        }
    }
}

impl ReplicaApplier for ReplicaCommandExecutor {
    async fn apply_group(
        &self,
        shard_id: u16,
        mut commands: Vec<ParsedCommand>,
    ) -> Result<(), ApplyError> {
        match commands.len() {
            0 => Ok(()),
            // A bare replicated command applies directly; the atomic-transaction
            // machinery is reserved for real MULTI/EXEC groups.
            1 => self.apply_single(shard_id, commands.pop().unwrap()).await,
            _ => self.apply_transaction(shard_id, commands).await,
        }
    }
}
