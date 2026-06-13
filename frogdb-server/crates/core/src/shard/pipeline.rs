use bytes::Bytes;

use crate::command::{Command, CommandFlags};

use super::post_execution::{EffectScope, WalPhase, WriteSummary};
use super::worker::ShardWorker;

impl ShardWorker {
    /// Run the full post-execution pipeline after a single write command.
    ///
    /// Thin wrapper over [`ShardWorker::run_write_effects`] (the canonical
    /// effect order). The WRITE-flag guard is preserved here because, unlike the
    /// transaction path, this entry point historically accepted any handler.
    pub(crate) async fn run_post_execution(
        &mut self,
        handler: &dyn Command,
        args: &[Bytes],
        dirty_delta: i64,
        conn_id: u64,
    ) {
        if !handler.flags().contains(CommandFlags::WRITE) {
            return;
        }
        self.run_write_effects(
            WriteSummary {
                writes: &[(handler, args)],
                dirty_delta,
                conn_id,
            },
            WalPhase::Persist,
            EffectScope::Command,
        )
        .await;
    }

    /// Post-execution pipeline for rollback mode (WAL already persisted/confirmed).
    pub(crate) async fn run_post_execution_after_wal(
        &mut self,
        handler: &dyn Command,
        args: &[Bytes],
        dirty_delta: i64,
        conn_id: u64,
    ) {
        if !handler.flags().contains(CommandFlags::WRITE) {
            return;
        }
        self.run_write_effects(
            WriteSummary {
                writes: &[(handler, args)],
                dirty_delta,
                conn_id,
            },
            WalPhase::AlreadyPersisted,
            EffectScope::Command,
        )
        .await;
    }

    /// Batched post-execution pipeline for a MULTI/EXEC transaction.
    pub(crate) async fn run_transaction_post_execution(
        &mut self,
        write_infos: &[(&dyn Command, &[Bytes])],
        total_dirty: i64,
        conn_id: u64,
    ) {
        self.run_write_effects(
            WriteSummary {
                writes: write_infos,
                dirty_delta: total_dirty,
                conn_id,
            },
            WalPhase::Persist,
            EffectScope::Transaction,
        )
        .await;
    }

    /// Batched post-execution for rollback mode (WAL already persisted/confirmed).
    pub(crate) async fn run_transaction_post_execution_after_wal(
        &mut self,
        write_infos: &[(&dyn Command, &[Bytes])],
        total_dirty: i64,
        conn_id: u64,
    ) {
        self.run_write_effects(
            WriteSummary {
                writes: write_infos,
                dirty_delta: total_dirty,
                conn_id,
            },
            WalPhase::AlreadyPersisted,
            EffectScope::Transaction,
        )
        .await;
    }

    /// Emit keyspace hit/miss counters from lookup-level accounting.
    ///
    /// `hits`/`misses` are tallied by the executing command from actual key
    /// existence (see [`crate::command::CommandContext::record_keyspace_lookup`]),
    /// matching Redis's `lookupKeyReadWithFlags` semantics. This deliberately
    /// does not infer hit/miss from the reply shape: a nil bulk reply (e.g. GET
    /// on a missing key vs. HGET on a missing field) is ambiguous and would
    /// misclassify lookups.
    ///
    /// This is not a write effect — it runs for every command (read or write) at
    /// lookup level — which is why it lives outside `run_write_effects`.
    pub(super) fn record_keyspace_lookups(&self, hits: u64, misses: u64) {
        if hits > 0 {
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keyspace_hits_total",
                hits,
                &[],
            );
        }
        if misses > 0 {
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keyspace_misses_total",
                misses,
                &[],
            );
        }
    }
}
