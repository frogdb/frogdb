use bytes::Bytes;

use crate::command::{Command, WalAction};
use crate::store::Store;

use super::connection::NewConnection;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle a new connection assigned to this shard.
    pub(crate) async fn handle_new_connection(&self, new_conn: NewConnection) {
        tracing::debug!(
            shard_id = self.shard_id(),
            conn_id = new_conn.conn_id,
            addr = %new_conn.addr,
            "New connection assigned to shard"
        );

        // Connection handling is spawned as a separate task
        // The actual connection loop is implemented in the server crate
    }

    // =========================================================================
    // WAL persistence helpers
    // =========================================================================
    //
    // These helpers return `io::Result` so callers can decide whether to
    // propagate (rollback path: durability is required) or log-and-discard
    // (hot path: see `persist_by_strategy` in `pipeline.rs`).

    /// Persist a key's current state to WAL.
    pub(crate) async fn persist_key_to_wal(&self, key: &[u8]) -> std::io::Result<()> {
        if let Some(wal) = self.persistence.wal_writer()
            && let Some(value) = self.store.get_hot(key)
        {
            let metadata = self
                .store
                .get_metadata(key)
                .unwrap_or_else(|| crate::types::KeyMetadata::new(value.memory_size()));
            wal.write_set(key, &value, &metadata).await?;
        }
        Ok(())
    }

    /// Persist a deletion to WAL.
    pub(crate) async fn persist_delete_to_wal(&self, key: &[u8]) -> std::io::Result<()> {
        if let Some(wal) = self.persistence.wal_writer() {
            wal.write_delete(key).await?;
        }
        Ok(())
    }

    /// Apply a single resolved [`WalAction`] to the WAL.
    ///
    /// This is the only place that maps a `WalAction` to a `WalWriter` call.
    /// Adding a new action variant requires extending this match — and only this match.
    pub(crate) async fn execute_wal_action(&self, action: &WalAction<'_>) -> std::io::Result<()> {
        match action {
            WalAction::Persist(key) => self.persist_key_to_wal(key).await,
            WalAction::DeleteIfMissing(key) => {
                if !self.store.contains(key) {
                    self.persist_delete_to_wal(key).await
                } else {
                    Ok(())
                }
            }
            WalAction::PersistOrDelete(key) => {
                if self.store.contains(key) {
                    self.persist_key_to_wal(key).await
                } else {
                    self.persist_delete_to_wal(key).await
                }
            }
            WalAction::PersistIfExists(key) => {
                if self.store.contains(key) {
                    self.persist_key_to_wal(key).await
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Persist a command's effects to WAL and confirm they are durable,
    /// returning error on failure.
    ///
    /// This is the rollback-mode equivalent of `persist_by_strategy()`. It
    /// writes WAL entries for the command and then confirms durability through
    /// the command's last WAL sequence: the confirmation fails if the flush
    /// fails *or* if a background (size-threshold/timeout) flush that carried
    /// any of this command's entries already failed — so an acked write can
    /// never outrun a swallowed flush failure.
    pub(crate) async fn persist_and_confirm(
        &self,
        handler: &dyn Command,
        args: &[Bytes],
    ) -> std::io::Result<()> {
        let wal = match self.persistence.wal_writer() {
            Some(w) => w,
            None => return Ok(()),
        };

        let start_seq = wal.sequence();
        for action in handler.wal_strategy().actions(args) {
            self.execute_wal_action(&action).await?;
        }

        // Confirm durability of every entry this command produced.
        wal.flush_through(start_seq).await
    }

    /// Persist all write commands in a transaction to WAL, confirming
    /// durability once at the end.
    ///
    /// This is the rollback-mode batch equivalent of calling `persist_and_confirm()`
    /// for each write command individually. Returns an error if any WAL write
    /// fails, if the final flush fails, or if a background flush that carried
    /// any of the transaction's entries already failed.
    pub(crate) async fn persist_transaction_to_wal(
        &self,
        write_infos: &[(&dyn Command, &[Bytes])],
    ) -> std::io::Result<()> {
        let wal = match self.persistence.wal_writer() {
            Some(w) => w,
            None => return Ok(()),
        };

        let start_seq = wal.sequence();
        for &(handler, args) in write_infos {
            for action in handler.wal_strategy().actions(args) {
                self.execute_wal_action(&action).await?;
            }
        }

        // Confirm durability of the entire transaction's entries.
        wal.flush_through(start_seq).await
    }
}
