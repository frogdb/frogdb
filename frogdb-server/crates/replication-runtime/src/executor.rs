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
use frogdb_replication::{ApplyError, ControlApplier, ReplicaApplier};
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
    /// Where control-shard commands go — process-wide state with no shard to
    /// route to (`FUNCTION LOAD/DELETE/FLUSH/RESTORE`, issue 48).
    ///
    /// `None` on a node wired without one, in which case a control frame is
    /// counted and stepped over rather than failing the link: an old replica
    /// meeting a newer primary should fall behind on a feature, not diverge on
    /// every frame.
    control: Option<Arc<dyn ControlApplier>>,
}

impl ReplicaCommandExecutor {
    /// Create a new replica command executor.
    pub fn new(shard_senders: Arc<Vec<ShardSender>>, num_shards: usize) -> Self {
        Self {
            shard_senders,
            num_shards,
            control: None,
        }
    }

    /// Wire the control-shard seam (see [`Self::control`]).
    pub fn with_control_applier(mut self, control: Arc<dyn ControlApplier>) -> Self {
        self.control = Some(control);
        self
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
            // Replica apply: the primary admitted this write for the user that
            // issued it, and re-checking here would refuse every replicated
            // write (the slot belongs to the primary).
            admission: frogdb_core::write_seam::WriteAdmission::pre_authorized(),
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

    async fn apply_control(&self, command: ParsedCommand) -> Result<(), ApplyError> {
        let Some(control) = self.control.as_ref() else {
            tracing::warn!(
                command = %command.name_uppercase_string(),
                "No control applier wired; stepping over a control-shard frame"
            );
            return Ok(());
        };
        control
            .apply(&command)
            .map_err(|detail| ApplyError::ControlRejected {
                command: command.name_uppercase_string(),
                detail,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_shards::{Reply, Seen, cmd, fake_shards, render, serve_command};
    use std::time::Duration;
    use tokio::time::timeout;

    /// A group that reaches no shard must not wait on one either, so the
    /// "nothing was sent" assertions are made under a bound rather than by
    /// hanging.
    const NO_WAIT: Duration = Duration::from_secs(3);

    // FM-REPLICATION-051
    /// A replicated bare command is one `CoreMsg::Execute` on the shard the
    /// primary tagged the frame with, carrying the internal connection id that
    /// keeps it from being re-broadcast.
    #[tokio::test]
    async fn a_single_replicated_command_executes_directly_on_its_tagged_shard() {
        let mut shards = fake_shards(2);
        let executor = ReplicaCommandExecutor::new(shards.senders(), 2);

        let (applied, seen) = tokio::join!(
            executor.apply_group(1, vec![cmd("SET", &["user:1", "alice"])]),
            serve_command(shards.shard(1), Reply::Ok),
        );

        assert!(
            applied.is_ok(),
            "a clean apply must not report a divergence"
        );
        match seen {
            Seen::Execute {
                command,
                conn_id,
                txid,
                track_reads,
                no_touch,
            } => {
                assert_eq!(render(&command), "SET user:1 alice");
                assert_eq!(
                    conn_id, REPLICA_INTERNAL_CONN_ID,
                    "a replicated write applied under a client's id would be \
                     re-broadcast, looping the write back to the primary"
                );
                assert_eq!(txid, None);
                assert!(
                    !track_reads,
                    "a replicated apply has no client to invalidate"
                );
                assert!(!no_touch);
            }
            other => panic!("a bare command must not be wrapped in a transaction: {other:?}"),
        }
        assert!(
            shards.untouched(0),
            "the tag chooses the shard; no other shard may see the write"
        );
    }

    // FM-REPLICATION-051
    // FM-REPLICATION-034
    /// A reconstructed `MULTI … EXEC` is **one** `ExecTransaction` on the tagged
    /// shard — not N separate applies, which would make intermediate
    /// transaction state readable on the replica.
    #[tokio::test]
    async fn a_reconstructed_transaction_is_one_atomic_shard_message() {
        let mut shards = fake_shards(2);
        let executor = ReplicaCommandExecutor::new(shards.senders(), 2);

        let group = vec![
            cmd("SET", &["a", "1"]),
            cmd("INCR", &["a"]),
            cmd("DEL", &["b"]),
        ];
        let (applied, seen) = tokio::join!(
            executor.apply_group(0, group),
            serve_command(shards.shard(0), Reply::Ok),
        );

        assert!(applied.is_ok());
        match seen {
            Seen::Transaction {
                commands,
                watches,
                conn_id,
            } => {
                assert_eq!(
                    commands.iter().map(render).collect::<Vec<_>>(),
                    vec!["SET a 1", "INCR a", "DEL b"],
                    "the group must arrive whole and in order"
                );
                assert_eq!(
                    watches, 0,
                    "the primary already resolved the WATCH set; re-watching on \
                     the replica could abort a transaction the primary committed"
                );
                assert_eq!(conn_id, REPLICA_INTERNAL_CONN_ID);
            }
            other => panic!("the group was split into per-command applies: {other:?}"),
        }
        assert!(
            shards.untouched(0),
            "the group is one message, so nothing follows it"
        );
        assert!(shards.untouched(1));
    }

    // FM-REPLICATION-051
    /// An empty group is a no-op that reaches no shard at all — not an empty
    /// transaction, which a shard would answer and which would show up as a
    /// WATCH-version bump on the replica.
    #[tokio::test]
    async fn an_empty_group_reaches_no_shard() {
        let mut shards = fake_shards(2);
        let executor = ReplicaCommandExecutor::new(shards.senders(), 2);

        let applied = timeout(NO_WAIT, executor.apply_group(0, Vec::new()))
            .await
            .expect("an empty group must not wait on a shard response");

        assert!(applied.is_ok());
        assert!(shards.untouched(0), "an empty group must send nothing");
        assert!(shards.untouched(1));
    }

    // FM-REPLICATION-051
    /// A shard that refuses a replicated command is a divergence, surfaced with
    /// the shard's own reason — never swallowed as a clean apply, which would
    /// let the replica keep ACKing offsets whose writes it never took.
    #[tokio::test]
    async fn a_refused_command_is_reported_as_a_divergence_with_its_reason() {
        for (label, reply) in [
            ("-ERR", Reply::Error("WRONGTYPE Operation against a key")),
            ("blob error", Reply::BlobError("OOM command not allowed")),
        ] {
            let mut shards = fake_shards(2);
            let executor = ReplicaCommandExecutor::new(shards.senders(), 2);

            let (applied, _) = tokio::join!(
                executor.apply_group(1, vec![cmd("LPUSH", &["k", "v"])]),
                serve_command(shards.shard(1), reply),
            );

            match applied {
                Err(ApplyError::Rejected { shard, detail }) => {
                    assert_eq!(shard, 1, "the divergence names the shard it happened on");
                    assert!(
                        detail.contains("WRONGTYPE") || detail.contains("OOM"),
                        "the shard's own reason must survive into the divergence \
                         ({label}), got {detail:?}"
                    );
                }
                other => panic!("a refused {label} apply must not report success: {other:?}"),
            }
        }
    }

    // FM-REPLICATION-051
    /// The same for a group: a transaction the shard could not run is a
    /// divergence, and a `WATCH` abort is reported as one rather than as a
    /// silent success.
    #[tokio::test]
    async fn a_failed_transaction_is_reported_as_a_divergence() {
        let mut shards = fake_shards(1);
        let executor = ReplicaCommandExecutor::new(shards.senders(), 1);
        let group = || vec![cmd("SET", &["a", "1"]), cmd("SET", &["b", "2"])];

        let (applied, _) = tokio::join!(
            executor.apply_group(0, group()),
            serve_command(shards.shard(0), Reply::Error("EXECABORT")),
        );
        match applied {
            Err(ApplyError::Rejected { shard, detail }) => {
                assert_eq!(shard, 0);
                assert_eq!(detail, "EXECABORT");
            }
            other => panic!("a failed transaction must not report success: {other:?}"),
        }

        let (applied, _) = tokio::join!(
            executor.apply_group(0, group()),
            serve_command(shards.shard(0), Reply::WatchAborted),
        );
        match applied {
            Err(ApplyError::Rejected { shard, detail }) => {
                assert_eq!(shard, 0);
                assert!(
                    detail.to_ascii_uppercase().contains("WATCH"),
                    "the abort must be attributable to the WATCH conflict, got {detail:?}"
                );
            }
            other => panic!("a WATCH-aborted group must not report success: {other:?}"),
        }
    }

    // FM-REPLICATION-051
    /// Every shard id the node actually has is applied to; an id the node does
    /// not have is refused **before** any send, so a mis-tagged frame can never
    /// land on the wrong shard.
    #[tokio::test]
    async fn an_origin_shard_tag_outside_the_shard_count_is_refused_before_any_send() {
        let mut shards = fake_shards(2);
        let executor = ReplicaCommandExecutor::new(shards.senders(), 2);

        match executor.apply_group(2, vec![cmd("SET", &["k", "v"])]).await {
            Err(ApplyError::ShardOutOfRange(shard, count)) => {
                assert_eq!((shard, count), (2, 2));
            }
            other => panic!("a tag past the last shard must be refused: {other:?}"),
        }
        assert!(shards.untouched(0), "a refused tag must reach no shard");
        assert!(shards.untouched(1));

        // ...and every in-range tag is served, so the bound is a bound and not
        // a blanket refusal.
        for shard_id in 0..2u16 {
            let (applied, _) = tokio::join!(
                executor.apply_group(shard_id, vec![cmd("SET", &["k", "v"])]),
                serve_command(shards.shard(shard_id as usize), Reply::Ok),
            );
            assert!(applied.is_ok(), "shard {shard_id} is a shard this node has");
        }

        // A count that over-states the wired senders is caught by the same
        // refusal rather than panicking on an index.
        let short = ReplicaCommandExecutor::new(shards.senders(), 4);
        match short.apply_group(3, vec![cmd("SET", &["k", "v"])]).await {
            Err(ApplyError::ShardOutOfRange(shard, count)) => {
                assert_eq!((shard, count), (3, 4));
            }
            other => panic!("a tag with no sender behind it must be refused: {other:?}"),
        }
    }

    // FM-REPLICATION-051
    /// A shard that is gone, or that dies without answering, is reported as
    /// unavailable — distinct from a divergence, because the write may or may
    /// not have landed and the link must be re-established rather than the
    /// history abandoned.
    #[tokio::test]
    async fn a_shard_that_is_gone_or_silent_is_reported_as_unavailable() {
        let mut shards = fake_shards(2);
        let executor = ReplicaCommandExecutor::new(shards.senders(), 2);

        // The worker dropped its receiver: the send itself fails.
        shards.disconnect(0);
        let single = executor.apply_group(0, vec![cmd("SET", &["k", "v"])]).await;
        assert!(
            matches!(single, Err(ApplyError::ShardUnavailable(0))),
            "a closed shard channel is unavailability, got {single:?}"
        );
        let group = executor
            .apply_group(0, vec![cmd("SET", &["k", "v"]), cmd("DEL", &["k"])])
            .await;
        assert!(
            matches!(group, Err(ApplyError::ShardUnavailable(0))),
            "a closed shard channel is unavailability for a group too, got {group:?}"
        );

        // The worker took the message and died before answering.
        let (applied, _) = tokio::join!(
            executor.apply_group(1, vec![cmd("SET", &["k", "v"])]),
            serve_command(shards.shard(1), Reply::Silent),
        );
        assert!(
            matches!(applied, Err(ApplyError::ShardUnavailable(1))),
            "a dropped response channel is unavailability, got {applied:?}"
        );

        let (applied, _) = tokio::join!(
            executor.apply_group(1, vec![cmd("SET", &["k", "v"]), cmd("DEL", &["k"])]),
            serve_command(shards.shard(1), Reply::Silent),
        );
        assert!(
            matches!(applied, Err(ApplyError::ShardUnavailable(1))),
            "a dropped transaction ack is unavailability, got {applied:?}"
        );
    }
}
