use bytes::Bytes;

use crate::command::{Command, WalStrategy};
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

    /// Persist a key's current state to WAL after a write operation.
    pub(crate) async fn persist_key_to_wal(&self, key: &[u8]) {
        if let Some(ref wal) = self.persistence.wal_writer
            && let Some(value) = self.store.get_hot(key)
        {
            let metadata = self
                .store
                .get_metadata(key)
                .unwrap_or_else(|| crate::types::KeyMetadata::new(value.memory_size()));
            if let Err(e) = wal.write_set(key, &value, &metadata).await {
                tracing::error!(
                    key = %String::from_utf8_lossy(key),
                    error = %e,
                    "Failed to persist key to WAL"
                );
            }
        }
    }

    /// Persist a deletion to WAL.
    pub(crate) async fn persist_delete_to_wal(&self, key: &[u8]) {
        if let Some(ref wal) = self.persistence.wal_writer
            && let Err(e) = wal.write_delete(key).await
        {
            tracing::error!(
                key = %String::from_utf8_lossy(key),
                error = %e,
                "Failed to persist delete to WAL"
            );
        }
    }

    /// Legacy string-match WAL persistence. Only called from the `Infer` fallback path.
    pub(crate) async fn persist_command_to_wal_legacy(&self, cmd_name: &str, args: &[Bytes]) {
        if self.persistence.wal_writer.is_none() {
            return;
        }

        match cmd_name {
            // SET-like: persist current value
            "SET" | "SETNX" | "SETEX" | "PSETEX" | "SETRANGE" | "APPEND" | "INCR" | "DECR"
            | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "HSET" | "HSETNX" | "HMSET" | "HINCRBY"
            | "HINCRBYFLOAT" | "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LSET" | "LINSERT"
            | "SADD" | "SMOVE" | "ZADD" | "ZINCRBY" | "PFADD" | "PFMERGE" | "GEOADD" | "BF.ADD"
            | "BF.MADD" | "BF.INSERT" | "BF.RESERVE" | "XADD" | "XTRIM" | "SETBIT" | "BITOP"
            | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "GETEX" => {
                // These commands have the key as the first argument
                if !args.is_empty() {
                    self.persist_key_to_wal(&args[0]).await;
                }
            }

            // BITOP has destination as first arg after operation type
            // Handled above with BITOP

            // DELETE-like: persist deletion only if key was deleted
            "DEL" | "UNLINK" | "GETDEL" => {
                for arg in args {
                    if !self.store.contains(arg) {
                        self.persist_delete_to_wal(arg).await;
                    }
                }
            }

            // POP/REMOVE: check if key still exists
            "LPOP" | "RPOP" | "LMPOP" | "SPOP" | "SREM" | "ZPOPMIN" | "ZPOPMAX" | "ZREM"
            | "ZMPOP" | "HDEL" | "LTRIM" | "LREM" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYLEX" => {
                if !args.is_empty() {
                    let key = &args[0];
                    if self.store.contains(key) {
                        self.persist_key_to_wal(key).await;
                    } else {
                        self.persist_delete_to_wal(key).await;
                    }
                }
            }

            // RENAME: delete old key, set new key
            "RENAME" | "RENAMENX" => {
                if args.len() >= 2 {
                    let old_key = &args[0];
                    let new_key = &args[1];
                    if !self.store.contains(old_key) {
                        self.persist_delete_to_wal(old_key).await;
                    }
                    self.persist_key_to_wal(new_key).await;
                }
            }

            // Store operations: persist destination
            "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE" | "ZINTERSTORE" | "ZUNIONSTORE"
            | "ZDIFFSTORE" | "ZRANGESTORE" => {
                // Destination is first argument
                if !args.is_empty() {
                    let dest = &args[0];
                    if self.store.contains(dest) {
                        self.persist_key_to_wal(dest).await;
                    }
                }
            }

            // LMOVE/COPY: destination is second argument
            "LMOVE" | "COPY" => {
                if args.len() >= 2 {
                    let dest = &args[1];
                    if self.store.contains(dest) {
                        self.persist_key_to_wal(dest).await;
                    }
                }
            }

            // FLUSHDB/FLUSHALL: handled by RocksDB clear, no WAL marker needed
            "FLUSHDB" | "FLUSHALL" => {
                // No-op: RocksDB column family is cleared directly
            }

            _ => {
                // Unknown write command: log and persist first key to be safe
                tracing::warn!(
                    command = cmd_name,
                    "Unknown write command for WAL persistence"
                );
                if !args.is_empty() && self.store.contains(&args[0]) {
                    self.persist_key_to_wal(&args[0]).await;
                }
            }
        }
    }

    // =========================================================================
    // Checked WAL persistence helpers (return errors instead of logging)
    // =========================================================================

    /// Persist a key's current state to WAL, returning error on failure.
    pub(crate) async fn persist_key_to_wal_checked(&self, key: &[u8]) -> std::io::Result<()> {
        if let Some(ref wal) = self.persistence.wal_writer
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

    /// Persist a deletion to WAL, returning error on failure.
    pub(crate) async fn persist_delete_to_wal_checked(&self, key: &[u8]) -> std::io::Result<()> {
        if let Some(ref wal) = self.persistence.wal_writer {
            wal.write_delete(key).await?;
        }
        Ok(())
    }

    /// Persist a command's effects to WAL and flush, returning error on failure.
    ///
    /// This is the rollback-mode equivalent of `persist_by_strategy()` + `flush_async()`.
    /// It writes WAL entries for the command and then forces a flush to ensure
    /// the entries are durable before returning.
    pub(crate) async fn persist_and_confirm(
        &self,
        handler: &dyn Command,
        args: &[Bytes],
    ) -> std::io::Result<()> {
        let wal = match self.persistence.wal_writer {
            Some(ref w) => w,
            None => return Ok(()),
        };

        match handler.wal_strategy() {
            WalStrategy::PersistFirstKey => {
                if !args.is_empty() {
                    self.persist_key_to_wal_checked(&args[0]).await?;
                }
            }
            WalStrategy::DeleteKeys => {
                for arg in args {
                    if !self.store.contains(arg) {
                        self.persist_delete_to_wal_checked(arg).await?;
                    }
                }
            }
            WalStrategy::PersistOrDeleteFirstKey => {
                if !args.is_empty() {
                    let key = &args[0];
                    if self.store.contains(key) {
                        self.persist_key_to_wal_checked(key).await?;
                    } else {
                        self.persist_delete_to_wal_checked(key).await?;
                    }
                }
            }
            WalStrategy::RenameKeys => {
                if args.len() >= 2 {
                    let old_key = &args[0];
                    let new_key = &args[1];
                    if !self.store.contains(old_key) {
                        self.persist_delete_to_wal_checked(old_key).await?;
                    }
                    self.persist_key_to_wal_checked(new_key).await?;
                }
            }
            WalStrategy::MoveKeys => {
                if args.len() >= 2 {
                    let source = &args[0];
                    let dest = &args[1];
                    if self.store.contains(source) {
                        self.persist_key_to_wal_checked(source).await?;
                    } else {
                        self.persist_delete_to_wal_checked(source).await?;
                    }
                    self.persist_key_to_wal_checked(dest).await?;
                }
            }
            WalStrategy::PersistDestination(idx) => {
                if let Some(dest) = args.get(idx)
                    && self.store.contains(dest)
                {
                    self.persist_key_to_wal_checked(dest).await?;
                }
            }
            WalStrategy::NoOp => {}
            WalStrategy::Infer => {
                self.persist_command_to_wal_legacy_checked(handler.name(), args)
                    .await?;
            }
        }

        // Force flush to guarantee durability.
        wal.flush_async().await
    }

    /// Persist all write commands in a transaction to WAL, flushing once at the end.
    ///
    /// This is the rollback-mode batch equivalent of calling `persist_and_confirm()`
    /// for each write command individually. Returns an error if any WAL write or
    /// the final flush fails.
    pub(crate) async fn persist_transaction_to_wal(
        &self,
        write_infos: &[(&dyn Command, &[Bytes])],
    ) -> std::io::Result<()> {
        let wal = match self.persistence.wal_writer {
            Some(ref w) => w,
            None => return Ok(()),
        };

        for &(handler, args) in write_infos {
            match handler.wal_strategy() {
                WalStrategy::PersistFirstKey => {
                    if !args.is_empty() {
                        self.persist_key_to_wal_checked(&args[0]).await?;
                    }
                }
                WalStrategy::DeleteKeys => {
                    for arg in args {
                        if !self.store.contains(arg) {
                            self.persist_delete_to_wal_checked(arg).await?;
                        }
                    }
                }
                WalStrategy::PersistOrDeleteFirstKey => {
                    if !args.is_empty() {
                        let key = &args[0];
                        if self.store.contains(key) {
                            self.persist_key_to_wal_checked(key).await?;
                        } else {
                            self.persist_delete_to_wal_checked(key).await?;
                        }
                    }
                }
                WalStrategy::RenameKeys => {
                    if args.len() >= 2 {
                        let old_key = &args[0];
                        let new_key = &args[1];
                        if !self.store.contains(old_key) {
                            self.persist_delete_to_wal_checked(old_key).await?;
                        }
                        self.persist_key_to_wal_checked(new_key).await?;
                    }
                }
                WalStrategy::MoveKeys => {
                    if args.len() >= 2 {
                        let source = &args[0];
                        let dest = &args[1];
                        if self.store.contains(source) {
                            self.persist_key_to_wal_checked(source).await?;
                        } else {
                            self.persist_delete_to_wal_checked(source).await?;
                        }
                        self.persist_key_to_wal_checked(dest).await?;
                    }
                }
                WalStrategy::PersistDestination(idx) => {
                    if let Some(dest) = args.get(idx)
                        && self.store.contains(dest)
                    {
                        self.persist_key_to_wal_checked(dest).await?;
                    }
                }
                WalStrategy::NoOp => {}
                WalStrategy::Infer => {
                    self.persist_command_to_wal_legacy_checked(handler.name(), args)
                        .await?;
                }
            }
        }

        // Flush once at the end for the entire transaction
        wal.flush_async().await
    }

    /// Legacy string-match WAL persistence that returns errors instead of logging.
    async fn persist_command_to_wal_legacy_checked(
        &self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> std::io::Result<()> {
        if self.persistence.wal_writer.is_none() {
            return Ok(());
        }

        match cmd_name {
            "SET" | "SETNX" | "SETEX" | "PSETEX" | "SETRANGE" | "APPEND" | "INCR" | "DECR"
            | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "HSET" | "HSETNX" | "HMSET" | "HINCRBY"
            | "HINCRBYFLOAT" | "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LSET" | "LINSERT"
            | "SADD" | "SMOVE" | "ZADD" | "ZINCRBY" | "PFADD" | "PFMERGE" | "GEOADD" | "BF.ADD"
            | "BF.MADD" | "BF.INSERT" | "BF.RESERVE" | "XADD" | "XTRIM" | "SETBIT" | "BITOP"
            | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "GETEX" => {
                if !args.is_empty() {
                    self.persist_key_to_wal_checked(&args[0]).await?;
                }
            }
            "DEL" | "UNLINK" | "GETDEL" => {
                for arg in args {
                    if !self.store.contains(arg) {
                        self.persist_delete_to_wal_checked(arg).await?;
                    }
                }
            }
            "LPOP" | "RPOP" | "LMPOP" | "SPOP" | "SREM" | "ZPOPMIN" | "ZPOPMAX" | "ZREM"
            | "ZMPOP" | "HDEL" | "LTRIM" | "LREM" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYLEX" => {
                if !args.is_empty() {
                    let key = &args[0];
                    if self.store.contains(key) {
                        self.persist_key_to_wal_checked(key).await?;
                    } else {
                        self.persist_delete_to_wal_checked(key).await?;
                    }
                }
            }
            "RENAME" | "RENAMENX" => {
                if args.len() >= 2 {
                    let old_key = &args[0];
                    let new_key = &args[1];
                    if !self.store.contains(old_key) {
                        self.persist_delete_to_wal_checked(old_key).await?;
                    }
                    self.persist_key_to_wal_checked(new_key).await?;
                }
            }
            "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE" | "ZINTERSTORE" | "ZUNIONSTORE"
            | "ZDIFFSTORE" | "ZRANGESTORE" => {
                if !args.is_empty() {
                    let dest = &args[0];
                    if self.store.contains(dest) {
                        self.persist_key_to_wal_checked(dest).await?;
                    }
                }
            }
            "LMOVE" | "COPY" => {
                if args.len() >= 2 {
                    let dest = &args[1];
                    if self.store.contains(dest) {
                        self.persist_key_to_wal_checked(dest).await?;
                    }
                }
            }
            "FLUSHDB" | "FLUSHALL" => {}
            _ => {
                if !args.is_empty() && self.store.contains(&args[0]) {
                    self.persist_key_to_wal_checked(&args[0]).await?;
                }
            }
        }
        Ok(())
    }
}
