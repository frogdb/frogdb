use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

use super::message::ScatterOp;
use super::types::{PartialResult, TransactionResult};
use super::worker::ShardWorker;
use crate::command::CommandContext;
use crate::store::Store;
use crate::types::{KeyMetadata, Value};

impl ShardWorker {
    /// Execute a command locally.
    pub(crate) async fn execute_command(
        &mut self,
        command: &ParsedCommand,
        conn_id: u64,
        protocol_version: ProtocolVersion,
        track_reads: bool,
    ) -> Response {
        let cmd_name = command.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        let handler = match self.registry.get(&cmd_name_str) {
            Some(h) => h,
            None => {
                return Response::error(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                ));
            }
        };

        // Validate arity
        if !handler.arity().check(command.args.len()) {
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name().to_ascii_lowercase()
            ));
        }

        // Check memory before write operations
        let is_write = handler
            .flags()
            .contains(crate::command::CommandFlags::WRITE);
        if is_write && let Err(err) = self.check_memory_for_write() {
            return err.to_response();
        }

        // Determine if rollback mode applies:
        // - Write command
        // - WAL writer is present
        // - Failure policy is Rollback
        // (Scripts bypass execute_command entirely, so no script check needed)
        let rollback_mode =
            is_write && self.persistence.has_wal() && self.persistence.should_rollback();

        // Capture pre-execution snapshot for rollback (before the mutable borrow scope)
        let snapshot = if rollback_mode {
            Some(self.capture_write_snapshot(handler.as_ref(), &command.args))
        } else {
            None
        };

        // Create command context and execute in a block so the mutable borrow
        // on self.store (via ctx) is released before we need self.store again.
        let (response, dirty_delta) = {
            let store = &mut self.store as &mut dyn Store;
            let mut ctx = CommandContext::with_cluster(
                store,
                &self.shard_senders,
                self.identity.shard_id,
                self.identity.num_shards,
                conn_id,
                protocol_version,
                self.cluster.replication_tracker.as_ref(),
                self.cluster.cluster_state.as_ref(),
                self.cluster.node_id,
                self.cluster.raft.as_ref(),
                self.cluster.network_factory.as_ref(),
                self.cluster.quorum_checker.as_ref().map(|q| q.as_ref()),
            );
            ctx.command_registry = Some(&self.registry);
            ctx.is_replica = self
                .identity
                .is_replica
                .load(std::sync::atomic::Ordering::Relaxed);
            ctx.is_replica_flag = Some(self.identity.is_replica.clone());
            ctx.master_host = self.identity.master_host.clone();
            ctx.master_port = self.identity.master_port;

            let response = match handler.execute(&mut ctx, &command.args) {
                Ok(response) => response,
                Err(err) => err.to_response(),
            };
            (response, ctx.dirty_delta)
        };

        // Client tracking: record reads for invalidation
        if track_reads && !is_write && self.tracking.has_tracking_clients() {
            let keys = handler.keys(&command.args);
            for key in &keys {
                self.tracking.record_read(key, conn_id);
            }
        }

        // Post-execution: rollback mode vs default path
        if rollback_mode {
            match self
                .persist_and_confirm(handler.as_ref(), &command.args)
                .await
            {
                Ok(()) => {
                    // WAL succeeded — run remaining post-execution steps
                    self.run_post_execution_after_wal(
                        handler.as_ref(),
                        &command.args,
                        &response,
                        dirty_delta,
                        conn_id,
                    )
                    .await;
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        cmd = handler.name(),
                        "WAL persistence failed, rolling back"
                    );
                    self.rollback_snapshot(snapshot.unwrap());
                    self.observability.metrics_recorder.increment_counter(
                        "frogdb_wal_rollbacks_total",
                        1,
                        &[],
                    );
                    return Response::error(format!("IOERR WAL persistence failed: {}", e));
                }
            }
        } else {
            // Default path — zero overhead for continue mode
            self.run_post_execution(
                handler.as_ref(),
                &command.args,
                &response,
                dirty_delta,
                conn_id,
            )
            .await;
        }

        response
    }

    /// Execute a transaction atomically.
    ///
    /// This method:
    /// 1. Checks all watched keys' versions against their watched versions
    /// 2. If any mismatch, returns WatchAborted (EXEC returns nil)
    /// 3. Executes all queued commands in sequence
    /// 4. Returns Success with all command results
    pub(crate) async fn execute_transaction(
        &mut self,
        commands: Vec<ParsedCommand>,
        watches: &[(Bytes, u64)],
        conn_id: u64,
        protocol_version: ProtocolVersion,
    ) -> TransactionResult {
        // Check WATCH conditions
        if !self.check_watches(watches) {
            return TransactionResult::WatchAborted;
        }

        // Execute all commands (reads not tracked in Phase 1); in rollback mode, abort on WAL failure
        let mut results = Vec::with_capacity(commands.len());
        for (i, command) in commands.iter().enumerate() {
            let response = self
                .execute_command(command, conn_id, protocol_version, false)
                .await;
            let is_wal_failure = matches!(
                &response,
                Response::Error(msg) if msg.starts_with(b"IOERR WAL")
            );
            results.push(response);
            if is_wal_failure {
                // Abort remaining commands — already-executed commands remain committed
                for _ in (i + 1)..commands.len() {
                    results.push(Response::error(
                        "EXECABRT transaction aborted due to WAL failure",
                    ));
                }
                break;
            }
        }

        TransactionResult::Success(results)
    }

    /// Execute part of a scatter-gather operation.
    pub(crate) async fn execute_scatter_part(
        &mut self,
        keys: &[Bytes],
        operation: &ScatterOp,
        conn_id: u64,
    ) -> PartialResult {
        let results = match operation {
            ScatterOp::MGet => self.scatter_mget(keys, conn_id),
            ScatterOp::MSet { pairs } => self.scatter_mset(pairs, conn_id).await,
            ScatterOp::Del | ScatterOp::Unlink => self.scatter_del(keys, conn_id).await,
            ScatterOp::Exists => keys
                .iter()
                .map(|key| {
                    let exists = self.store.contains(key);
                    (key.clone(), Response::Integer(if exists { 1 } else { 0 }))
                })
                .collect(),
            ScatterOp::Touch => keys
                .iter()
                .map(|key| {
                    let touched = self.store.touch(key);
                    (key.clone(), Response::Integer(if touched { 1 } else { 0 }))
                })
                .collect(),
            ScatterOp::Keys { pattern } => {
                // Get all keys matching pattern
                let all_keys = self.store.all_keys();
                let matching_keys: Vec<_> = all_keys
                    .into_iter()
                    .filter(|key| crate::glob::glob_match(pattern, key))
                    .map(|key| (key.clone(), Response::bulk(key)))
                    .collect();
                matching_keys
            }
            ScatterOp::DbSize => {
                // Return the key count for this shard
                let count = self.store.len();
                vec![(
                    Bytes::from_static(b"__dbsize__"),
                    Response::Integer(count as i64),
                )]
            }
            ScatterOp::FlushDb => self.scatter_flushdb(),
            ScatterOp::Scan {
                cursor,
                count,
                pattern,
                key_type,
            } => {
                // Scan keys in this shard
                let pattern_ref = pattern.as_ref().map(|p| p.as_ref());
                let (next_cursor, found_keys) =
                    self.store
                        .scan_filtered(*cursor, *count, pattern_ref, *key_type);
                // Return cursor and keys as a special response
                let mut results = Vec::with_capacity(found_keys.len() + 1);
                results.push((
                    Bytes::from_static(b"__cursor__"),
                    Response::Integer(next_cursor as i64),
                ));
                for key in found_keys {
                    results.push((key.clone(), Response::bulk(key)));
                }
                results
            }
            ScatterOp::Copy { source_key } => {
                // Get the value and expiry from source key for cross-shard copy.
                // Returns an array with: [value_type, serialized_value, expiry_ms_or_nil]
                match self.store.get(source_key) {
                    Some(value) => {
                        // Get expiry if any
                        let expiry = self.store.get_expiry(source_key);
                        let expiry_ms = expiry.map(|exp| {
                            exp.duration_since(std::time::Instant::now()).as_millis() as i64
                        });

                        // Serialize the value based on its type
                        let (type_str, serialized) = value.serialize_for_copy();

                        let expiry_resp = match expiry_ms {
                            Some(ms) if ms > 0 => Response::Integer(ms),
                            _ => Response::null(),
                        };

                        vec![(
                            source_key.clone(),
                            Response::Array(vec![
                                Response::bulk(Bytes::from(type_str)),
                                Response::bulk(serialized),
                                expiry_resp,
                            ]),
                        )]
                    }
                    None => {
                        // Source key doesn't exist
                        vec![(source_key.clone(), Response::null())]
                    }
                }
            }
            ScatterOp::CopySet {
                dest_key,
                value_type,
                value_data,
                expiry_ms,
                replace,
            } => {
                return PartialResult {
                    results: self
                        .scatter_copy_set(dest_key, value_type, value_data, expiry_ms, *replace)
                        .await,
                };
            }
            ScatterOp::RandomKey => {
                // Return a random key from this shard
                match self.store.random_key() {
                    Some(key) => vec![(Bytes::from_static(b"__randomkey__"), Response::bulk(key))],
                    None => vec![(Bytes::from_static(b"__randomkey__"), Response::null())],
                }
            }
            ScatterOp::Dump => {
                // Serialize keys with full metadata for MIGRATE.
                // Returns serialized data in our internal format (compatible with RESTORE).
                use crate::persistence::serialize;

                keys.iter()
                    .map(|key| {
                        match self.store.get(key) {
                            Some(value) => {
                                // Get expiry if any
                                let expires_at = self.store.get_expiry(key);
                                let mut metadata = KeyMetadata::new(value.memory_size());
                                metadata.expires_at = expires_at;

                                // Serialize with full metadata
                                let serialized = serialize(&value, &metadata);
                                (key.clone(), Response::bulk(Bytes::from(serialized)))
                            }
                            None => {
                                // Key doesn't exist
                                (key.clone(), Response::null())
                            }
                        }
                    })
                    .collect()
            }
            ScatterOp::TsQueryIndex { args } => self.execute_ts_queryindex(args),
            ScatterOp::TsMget { args } => self.execute_ts_mget(args),
            ScatterOp::TsMrange { args, reverse } => self.execute_ts_mrange(args, *reverse),
            ScatterOp::FtCreate { index_def_json } => self.execute_ft_create(index_def_json).await,
            ScatterOp::FtSearch {
                index_name,
                query_args,
            } => self.execute_ft_search(index_name, query_args),
            ScatterOp::FtDropIndex { index_name } => self.execute_ft_dropindex(index_name).await,
            ScatterOp::FtInfo { index_name } => self.execute_ft_info(index_name),
            ScatterOp::FtList => self.execute_ft_list(),
            ScatterOp::FtAlter {
                index_name,
                new_fields_json,
            } => self.execute_ft_alter(index_name, new_fields_json).await,
            ScatterOp::FtSynupdate {
                index_name,
                group_id,
                terms,
            } => self.execute_ft_synupdate(index_name, group_id, terms).await,
            ScatterOp::FtSyndump { index_name } => self.execute_ft_syndump(index_name),
            ScatterOp::FtAggregate {
                index_name,
                query_args,
            } => self.execute_ft_aggregate(index_name, query_args),
            ScatterOp::FtHybrid {
                index_name,
                query_args,
            } => self.execute_ft_hybrid(index_name, query_args),
            ScatterOp::FtAliasadd {
                alias_name,
                index_name,
            } => self.execute_ft_aliasadd(alias_name, index_name),
            ScatterOp::FtAliasdel { alias_name } => self.execute_ft_aliasdel(alias_name),
            ScatterOp::FtAliasupdate {
                alias_name,
                index_name,
            } => self.execute_ft_aliasupdate(alias_name, index_name),
            ScatterOp::FtTagvals {
                index_name,
                field_name,
            } => self.execute_ft_tagvals(index_name, field_name),
            ScatterOp::FtDictadd { dict_name, terms } => self.execute_ft_dictadd(dict_name, terms),
            ScatterOp::FtDictdel { dict_name, terms } => self.execute_ft_dictdel(dict_name, terms),
            ScatterOp::FtDictdump { dict_name } => self.execute_ft_dictdump(dict_name),
            ScatterOp::FtConfig { args } => self.execute_ft_config(args),
            ScatterOp::FtSpellcheck {
                index_name,
                query_args,
            } => self.execute_ft_spellcheck(index_name, query_args),
            ScatterOp::FtExplain {
                index_name,
                query_str,
            } => self.execute_ft_explain(index_name, query_str),
            ScatterOp::EsAll { count, after_id } => self.execute_es_all(count, after_id),
        };

        PartialResult { results }
    }

    fn scatter_mget(&mut self, keys: &[Bytes], conn_id: u64) -> Vec<(Bytes, Response)> {
        let results: Vec<_> = keys
            .iter()
            .map(|key| {
                let response = match self.store.get(key) {
                    Some(value) => {
                        if let Some(sv) = value.as_string() {
                            Response::bulk(sv.as_bytes())
                        } else {
                            Response::null()
                        }
                    }
                    None => Response::null(),
                };
                (key.clone(), response)
            })
            .collect();
        // Client tracking: record reads for MGET
        if self.tracking.has_tracking_clients() {
            for key in keys {
                self.tracking.record_read(key, conn_id);
            }
        }
        results
    }

    async fn scatter_mset(
        &mut self,
        pairs: &[(Bytes, Bytes)],
        conn_id: u64,
    ) -> Vec<(Bytes, Response)> {
        let mut results = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            let val = Value::string(value.clone());
            self.store.set(key.clone(), val.clone());

            // Persist to WAL if enabled
            if let Some(ref wal) = self.persistence.wal_writer {
                let metadata = KeyMetadata::new(val.memory_size());
                if let Err(e) = wal.write_set(key, &val, &metadata).await {
                    tracing::error!(key = %String::from_utf8_lossy(key), error = %e, "Failed to persist MSET");
                }
            }

            results.push((key.clone(), Response::ok()));
        }
        // Increment version for MSET (write operation)
        if !pairs.is_empty() {
            self.increment_version();
            // Client tracking: invalidate written keys
            if self.tracking.has_tracking_clients() {
                let key_refs: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_ref()).collect();
                self.tracking.invalidate_keys(&key_refs, conn_id);
            }
        }
        results
    }

    async fn scatter_del(&mut self, keys: &[Bytes], conn_id: u64) -> Vec<(Bytes, Response)> {
        let mut results = Vec::with_capacity(keys.len());
        let mut any_deleted = false;
        for key in keys {
            let deleted = self.store.delete(key);

            // Persist delete to WAL if enabled
            if deleted {
                any_deleted = true;
                if let Some(ref wal) = self.persistence.wal_writer
                    && let Err(e) = wal.write_delete(key).await
                {
                    tracing::error!(key = %String::from_utf8_lossy(key), error = %e, "Failed to persist DEL");
                }
            }

            results.push((key.clone(), Response::Integer(if deleted { 1 } else { 0 })));
        }
        // Increment version for DEL/UNLINK if any key was deleted
        if any_deleted {
            self.increment_version();
            // Client tracking: invalidate deleted keys
            if self.tracking.has_tracking_clients() {
                let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
                self.tracking.invalidate_keys(&key_refs, conn_id);
            }
        }
        results
    }

    fn scatter_flushdb(&mut self) -> Vec<(Bytes, Response)> {
        // Clear all keys in this shard.
        // Only increment version if there were keys to clear,
        // so WATCH on non-existing keys is not aborted.
        let had_keys = self.store.len() > 0;
        self.store.clear();
        if had_keys {
            self.increment_version();
        }
        // Client tracking: flush-all invalidation
        if self.tracking.has_tracking_clients() {
            self.tracking.flush_all_tracking();
        }
        vec![(Bytes::from_static(b"__flushdb__"), Response::ok())]
    }

    async fn scatter_copy_set(
        &mut self,
        dest_key: &Bytes,
        value_type: &Bytes,
        value_data: &Bytes,
        expiry_ms: &Option<i64>,
        replace: bool,
    ) -> Vec<(Bytes, Response)> {
        // Write a value from cross-shard copy to destination key.
        // Check if destination exists (when not using REPLACE)
        if !replace && self.store.contains(dest_key) {
            return vec![(dest_key.clone(), Response::Integer(0))];
        }

        // Deserialize the value
        match Value::deserialize_for_copy(value_type.as_ref(), value_data.as_ref()) {
            Some(value) => {
                // If REPLACE, delete existing first
                if replace {
                    self.store.delete(dest_key);
                }

                // Set the value
                self.store.set(dest_key.clone(), value.clone());

                // Set expiry if provided
                if let Some(ms) = expiry_ms
                    && *ms > 0
                {
                    let expires_at = Instant::now() + Duration::from_millis(*ms as u64);
                    self.store.set_expiry(dest_key, expires_at);
                }

                // Persist to WAL if enabled
                if let Some(ref wal) = self.persistence.wal_writer {
                    let metadata = KeyMetadata::new(value.memory_size());
                    if let Err(e) = wal.write_set(dest_key, &value, &metadata).await {
                        tracing::error!(
                            key = %String::from_utf8_lossy(dest_key),
                            error = %e,
                            "Failed to persist COPY"
                        );
                    }
                }

                // Increment version
                self.increment_version();

                vec![(dest_key.clone(), Response::Integer(1))]
            }
            None => {
                // Failed to deserialize value
                vec![(
                    dest_key.clone(),
                    Response::error("ERR failed to deserialize value for COPY"),
                )]
            }
        }
    }

    /// Execute ES.ALL on this shard — read from the per-shard `__frogdb:es:all` stream.
    fn execute_es_all(
        &mut self,
        count: &Option<usize>,
        after_id: &Option<crate::types::StreamId>,
    ) -> Vec<(Bytes, Response)> {
        use crate::types::StreamRangeBound;

        let all_key = Bytes::from_static(b"__frogdb:es:all");

        // Read entries from the stream — collect into owned Vec to avoid borrow issues
        let entries: Vec<crate::types::StreamEntry> = match self.store.get(&all_key) {
            Some(val) => match val.as_stream() {
                Some(stream) => {
                    if let Some(after) = after_id {
                        stream.read_after(after, *count)
                    } else {
                        stream.range(StreamRangeBound::Min, StreamRangeBound::Max, *count)
                    }
                }
                None => return vec![],
            },
            None => return vec![],
        };

        entries
            .into_iter()
            .map(|entry| {
                let id_str = entry.id.to_string();
                let mut fields_resp: Vec<Response> = Vec::with_capacity(entry.fields.len() * 2);
                for (k, v) in &entry.fields {
                    fields_resp.push(Response::bulk(k.clone()));
                    fields_resp.push(Response::bulk(v.clone()));
                }
                let entry_resp = Response::Array(vec![
                    Response::bulk(Bytes::from(id_str)),
                    Response::Array(fields_resp),
                ]);
                (all_key.clone(), entry_resp)
            })
            .collect()
    }
}
