use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

use frogdb_types::metrics::definitions::{KeyspaceHits, KeyspaceMisses, WalRollbacks};

use super::message::ScatterOp;
use super::post_execution::{EffectScope, WalPhase, WriteSummary};
use super::rollback::WriteSnapshot;
use super::types::{PartialResult, TransactionResult};
use super::worker::ShardWorker;
use crate::command::{Command, CommandEffects, WriteRecord};
use crate::store::Store;
use crate::types::{KeyMetadata, Value};

/// Metadata from executing a write command, used for deferred post-execution in transactions.
pub(crate) struct WriteCommandMeta {
    pub handler: Arc<dyn Command>,
    pub dirty_delta: i64,
    /// HyperLogLog register delta deposited by the command (dense PFADD), routed
    /// into the WAL as a `Merge` operand instead of a full `Put`. `None` for
    /// every other write.
    pub hll_wal_delta: Option<smallvec::SmallVec<[(u16, u8); 8]>>,
    /// Keyspace events deposited by an [`EventSpec::Dynamic`] command via
    /// [`CommandContext::notify_event`]; the notifications write effect emits
    /// exactly these. Empty for every other write.
    ///
    /// [`EventSpec::Dynamic`]: crate::command_spec::EventSpec::Dynamic
    /// [`CommandContext::notify_event`]: crate::command::CommandContext::notify_event
    pub keyspace_events: crate::command::KeyspaceEventDeposits,
}

impl CommandEffects {
    /// The write-effect payload of a drained [`CommandEffects`], or `None` when
    /// the write declared itself a no-op — the **single home** of the no-op
    /// suppression rule documented on
    /// [`CommandEffects::write_was_noop`](crate::command::CommandEffects::write_was_noop):
    /// a no-op write's deposits (`keyspace_events`, `hll_wal_delta`,
    /// `dirty_delta`) are discarded wholesale because the whole write-effect
    /// pipeline is skipped. The script drain sites reuse the rule through
    /// [`CommandEffects::into_script_record`], which chains here.
    ///
    /// The caller still owns the "is this a WRITE command?" classification —
    /// this method owns only what the *effects* say. The destructure is
    /// exhaustive on purpose: adding a `CommandEffects` field stops this from
    /// compiling until the new effect's routing is decided.
    pub(crate) fn into_write_meta(self, handler: Arc<dyn Command>) -> Option<WriteCommandMeta> {
        let CommandEffects {
            dirty_delta,
            // Accounting scalars: consumed by the execution seam before the
            // meta is built (lazyfree counter, keyspace hit/miss stats); a
            // script sub-command drops them, matching the pre-consolidation
            // drains. `script_writes` only ever populates on an outer
            // EVAL/EVALSHA/FCALL context, never on a single command's.
            lazyfreed_delta: _,
            keyspace_hits: _,
            keyspace_misses: _,
            write_was_noop,
            hll_wal_delta,
            keyspace_events,
            script_writes: _,
        } = self;
        if write_was_noop {
            return None;
        }
        Some(WriteCommandMeta {
            handler,
            dirty_delta,
            hll_wal_delta,
            keyspace_events,
        })
    }
}

impl ShardWorker {
    /// Execute a command's handler without running the post-execution pipeline.
    ///
    /// Returns the response and, for write commands, metadata needed by the
    /// post-execution pipeline. Read commands return `None` for the metadata.
    ///
    /// Wraps [`Self::execute_command_body`] so that any keys the command lazily
    /// purged (`Store::check_and_delete_expired` → reported via
    /// `take_lazily_purged`) have their parity effects applied once, after the
    /// command, regardless of which of the body's many early returns fired.
    fn execute_command_inner(
        &mut self,
        command: &ParsedCommand,
        conn_id: u64,
        protocol_version: ProtocolVersion,
        track_reads: bool,
    ) -> (Response, Option<WriteCommandMeta>) {
        let out = self.execute_command_body(command, conn_id, protocol_version, track_reads);
        // Apply parity effects for any keys the command lazily purged (gaps 1-3).
        // Task 1: this is a no-op drain; Task 2 fills in the effects.
        self.apply_lazy_purge_effects();
        out
    }

    /// The command handler body proper (see [`Self::execute_command_inner`],
    /// which wraps this to drain lazy-purge effects afterward).
    fn execute_command_body(
        &mut self,
        command: &ParsedCommand,
        conn_id: u64,
        protocol_version: ProtocolVersion,
        track_reads: bool,
    ) -> (Response, Option<WriteCommandMeta>) {
        let cmd_name = command.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        let handler = match self.registry.get(&cmd_name_str) {
            Some(h) => h,
            None => {
                return (
                    Response::error(format!(
                        "ERR unknown command '{}', with args beginning with:",
                        cmd_name_str
                    )),
                    None,
                );
            }
        };

        // Validate arity
        if !handler.arity().check(command.args.len()) {
            return (
                Response::error(format!(
                    "ERR wrong number of arguments for '{}' command",
                    handler.name().to_ascii_lowercase()
                )),
                None,
            );
        }

        // Check memory before write operations
        let is_write = handler
            .flags()
            .contains(crate::command::CommandFlags::WRITE);
        if is_write && let Err(err) = self.check_memory_for_write() {
            return (err.to_response(), None);
        }

        // Keyspace hit/miss accounting is declared on the spec, not remembered
        // by the handler. For `FirstKey`/`EveryKey` the seam owns the counting:
        // snapshot key existence *before* the handler runs (so a deleting read
        // like GETDEL is still counted as a hit) using the Redis `lookupKeyRead`
        // existence test. `Reported` commands deposit their outcome on the
        // context; `None` commands are not counted.
        let lookup = handler.spec().lookup;
        let seam_lookup_counts: Option<(u64, u64)> = match lookup {
            crate::command_spec::LookupSpec::FirstKey
            | crate::command_spec::LookupSpec::EveryKey => {
                let keys = handler.keys(&command.args);
                let probed: &[&[u8]] =
                    if matches!(lookup, crate::command_spec::LookupSpec::FirstKey) {
                        &keys[..keys.len().min(1)]
                    } else {
                        &keys
                    };
                let mut hits = 0u64;
                let mut misses = 0u64;
                for key in probed {
                    if self.store.exists_unexpired(key) {
                        hits += 1;
                    } else {
                        misses += 1;
                    }
                }
                Some((hits, misses))
            }
            crate::command_spec::LookupSpec::None | crate::command_spec::LookupSpec::Reported => {
                None
            }
        };

        // Create command context and execute. `command_context` is the single
        // builder that wires cluster + replica identity + registry from `self`.
        // The deposits come back as ONE value: everything the handler produced
        // besides the response.
        let (response, effects) = {
            let mut ctx = self.command_context(conn_id, protocol_version);

            let response = match handler.execute(&mut ctx, &command.args) {
                Ok(response) => response,
                Err(err) => err.to_response(),
            };
            (response, std::mem::take(&mut ctx.effects))
        };
        // Exhaustive by-ref destructure of the drained effects: the accounting
        // scalars are consumed here, the write payload below (via
        // `into_write_meta`, which is itself exhaustive). Adding a
        // `CommandEffects` field stops this site from compiling until its
        // routing is decided.
        let CommandEffects {
            dirty_delta: _,
            lazyfreed_delta,
            keyspace_hits,
            keyspace_misses,
            write_was_noop: _,
            hll_wal_delta: _,
            keyspace_events: _,
            script_writes: _,
        } = &effects;
        let (lazyfreed_delta, keyspace_hits, keyspace_misses) =
            (*lazyfreed_delta, *keyspace_hits, *keyspace_misses);
        // Track lazyfreed objects (from UNLINK). Applied after the context is
        // dropped so `self` is no longer borrowed by it.
        if lazyfreed_delta > 0 {
            self.observability.record_lazyfreed(lazyfreed_delta);
        }

        // Emit keyspace hit/miss stats once, at this single seam (Redis-
        // compatible, lookup-level). `FirstKey`/`EveryKey` use the pre-execution
        // existence snapshot; `Reported` commands deposited their outcome on the
        // context. Deriving from key existence — never the reply shape — means
        // HGET on a missing field counts as a hit and GET on a missing key as a
        // miss. Centralizing here covers both the single-command and MULTI/EXEC
        // paths (both route through this method).
        let (ks_hits, ks_misses) = match lookup {
            crate::command_spec::LookupSpec::None => (0, 0),
            crate::command_spec::LookupSpec::FirstKey
            | crate::command_spec::LookupSpec::EveryKey => seam_lookup_counts.unwrap_or((0, 0)),
            crate::command_spec::LookupSpec::Reported => (keyspace_hits, keyspace_misses),
        };
        if ks_hits > 0 || ks_misses > 0 {
            self.record_keyspace_lookups(ks_hits, ks_misses);
        }

        // Flush keysizes histogram updates from in-place mutations via get_mut()
        self.store.flush_keysizes_refreshes();

        // Client tracking: record reads for invalidation
        if track_reads && !is_write && self.tracking.has_tracking_clients() {
            let keys = handler.keys(&command.args);
            for key in &keys {
                self.tracking.record_read(key, conn_id);
            }
        }

        // A write that declared itself a no-op gets no WriteCommandMeta
        // (`into_write_meta` owns that rule), so the post-execution match runs
        // the read path (no write effects) — this covers the single-command,
        // rollback, and MULTI/EXEC paths in one place. Redis parity: no-op
        // writes do not propagate, notify, or dirty WATCH.
        let meta = if is_write {
            effects.into_write_meta(handler)
        } else {
            None
        };

        (response, meta)
    }

    /// Emit keyspace hit/miss counters from lookup-level accounting.
    ///
    /// `hits`/`misses` are derived from actual key existence — by the execution
    /// seam for `FirstKey`/`EveryKey` commands, or reported by the handler for
    /// `LookupSpec::Reported` — matching Redis's `lookupKeyReadWithFlags`
    /// semantics. This deliberately does not infer hit/miss from the reply shape:
    /// a nil bulk reply (e.g. GET on a missing key vs. HGET on a missing field)
    /// is ambiguous and would misclassify lookups.
    ///
    /// This is not a write effect — it runs for every command (read or write) at
    /// lookup level — which is why it lives here rather than in `run_write_effects`.
    /// It feeds both the resettable [`crate::KeyspaceStats`] accumulator and the
    /// monotonic Prometheus counters.
    pub(super) fn record_keyspace_lookups(&self, hits: u64, misses: u64) {
        // The atomic accumulator is the source of truth (INFO reads it, RESETSTAT
        // rebases it); the Prometheus counters are fed from the same tallies and
        // stay strictly monotonic so `rate()` / `increase()` are unaffected.
        self.observability.keyspace_stats().record(hits, misses);
        if hits > 0 {
            KeyspaceHits::inc_by(self.observability.metrics(), hits);
        }
        if misses > 0 {
            KeyspaceMisses::inc_by(self.observability.metrics(), misses);
        }
    }

    /// Tally and record keyspace hit/miss accounting from a per-key existence
    /// sequence — the single home of the *existence == hit* rule for the
    /// cross-shard scatter path (the counterpart of the [`LookupSpec`] seam that
    /// owns it for the single-shard path).
    ///
    /// Each `true` is a hit, each `false` a miss; the caller supplies the
    /// existence verdict from *its own* lookup primitive (EXISTS/TOUCH probe
    /// `exists_unexpired`; MGET uses the expiry-aware value it fetched via
    /// `get_with_expiry_check`, so a hot-but-expired key counts as a miss
    /// exactly like the single-shard path), which is why this takes booleans
    /// rather than keys — the accounting rule is shared even though the
    /// existence test legitimately differs per command.
    ///
    /// [`LookupSpec`]: crate::command_spec::LookupSpec
    pub(super) fn record_lookup_existence(&self, existed: impl IntoIterator<Item = bool>) {
        let mut hits = 0u64;
        let mut misses = 0u64;
        for exists in existed {
            if exists {
                hits += 1;
            } else {
                misses += 1;
            }
        }
        self.record_keyspace_lookups(hits, misses);
    }

    /// Serialize a key's current value into the self-describing transport frame
    /// shared by DUMP/RESTORE, cross-shard COPY, and RDB — the **single
    /// producer** of that frame in the scatter path. The value fetch, expiry
    /// lookup, and persistence-codec call happen here once, so the COPY and DUMP
    /// arms can no longer drift in how they build the frame or extract expiry.
    ///
    /// Returns the frame bytes plus the key's relative TTL in milliseconds (or
    /// `None` when the key has no expiry / is absent). `expiry_in_header` selects
    /// how the expiry travels: DUMP embeds it in the frame header (RESTORE reads
    /// it back when no TTL override is supplied); cross-shard COPY writes an
    /// expiry-free header and ships the TTL out-of-band via the returned value.
    /// Either way the extraction lives in one place.
    ///
    /// The value fetch is expiry-aware (`Store::get_with_expiry_check`), so a
    /// hot key past its TTL but not yet lazily purged reads as absent here too
    /// — COPY reports the source as missing and DUMP replies nil, matching
    /// Redis (COPY/DUMP of an expired key behave as if the key never existed).
    fn serialize_key_for_transport(
        &mut self,
        key: &[u8],
        expiry_in_header: bool,
    ) -> Option<(Bytes, Option<i64>)> {
        let value = self.store.get_with_expiry_check(key)?;
        let expires_at = self.store.get_expiry(key);
        let mut metadata = KeyMetadata::new(value.memory_size());
        if expiry_in_header {
            metadata.expires_at = expires_at;
        }
        let frame = crate::persistence::serialize(&value, &metadata);
        let ttl_ms = expires_at.map(|exp| exp.duration_since(Instant::now()).as_millis() as i64);
        Some((Bytes::from(frame), ttl_ms))
    }

    /// Decode a self-describing transport frame produced by
    /// [`ShardWorker::serialize_key_for_transport`] / DUMP — the single decode
    /// site for the scatter path (the RESTORE side of cross-shard COPY), sharing
    /// the persistence codec instead of reaching into it inline.
    fn deserialize_transport_frame(
        data: &[u8],
    ) -> Result<(Value, KeyMetadata), crate::persistence::SerializationError> {
        crate::persistence::deserialize(data)
    }

    /// Execute a command locally.
    pub(crate) async fn execute_command(
        &mut self,
        command: &ParsedCommand,
        conn_id: u64,
        protocol_version: ProtocolVersion,
        track_reads: bool,
    ) -> Response {
        // Determine if rollback mode applies before calling inner
        // (we need to capture the snapshot before execution)
        let cmd_name = command.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);
        let handler = self.registry.get(&cmd_name_str);
        let is_write = handler
            .as_ref()
            .map(|h| h.flags().contains(crate::command::CommandFlags::WRITE))
            .unwrap_or(false);
        let rollback_mode =
            is_write && self.persistence.has_wal() && self.persistence.should_rollback();

        // Capture pre-execution snapshot for rollback (before the mutable borrow in inner)
        let snapshot = if rollback_mode {
            handler.map(|h| self.capture_write_snapshot(h.as_ref(), &command.args))
        } else {
            None
        };

        let (response, meta) =
            self.execute_command_inner(command, conn_id, protocol_version, track_reads);

        // Post-execution: rollback mode vs default path. The WAL phase becomes a
        // value (Persist vs AlreadyPersisted) rather than a separate function.
        match meta {
            Some(ref write_meta) if rollback_mode => {
                let record = WriteRecord {
                    handler: write_meta.handler.as_ref(),
                    args: command.args.as_slice(),
                    hll_wal_delta: write_meta.hll_wal_delta.as_deref(),
                    keyspace_events: write_meta.keyspace_events.as_slice(),
                };
                match self
                    .persist(
                        std::slice::from_ref(&record),
                        super::persistence::Durability::Confirm,
                    )
                    .await
                {
                    Ok(()) => {
                        self.run_write_effects(
                            WriteSummary {
                                writes: std::slice::from_ref(&record),
                                dirty_delta: write_meta.dirty_delta,
                                conn_id,
                            },
                            WalPhase::AlreadyPersisted,
                            EffectScope::Command,
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            cmd = write_meta.handler.name(),
                            "WAL persistence failed, rolling back"
                        );
                        self.rollback_snapshot(snapshot.unwrap());
                        WalRollbacks::inc(self.observability.metrics());
                        return Response::error(format!("IOERR WAL persistence failed: {}", e));
                    }
                }
            }
            Some(ref write_meta) => {
                let record = WriteRecord {
                    handler: write_meta.handler.as_ref(),
                    args: command.args.as_slice(),
                    hll_wal_delta: write_meta.hll_wal_delta.as_deref(),
                    keyspace_events: write_meta.keyspace_events.as_slice(),
                };
                self.run_write_effects(
                    WriteSummary {
                        writes: std::slice::from_ref(&record),
                        dirty_delta: write_meta.dirty_delta,
                        conn_id,
                    },
                    WalPhase::Persist,
                    EffectScope::Command,
                )
                .await;
            }
            None => {
                // Read command — keyspace hit/miss stats are already recorded in
                // execute_command_inner; the write-only post-execution pipeline
                // has nothing to do for reads.
            }
        }

        response
    }

    /// Execute a transaction with atomic side effects.
    ///
    /// Commands execute against the real store sequentially (safe because the shard
    /// is single-threaded), but all post-execution side effects (version increment,
    /// WAL persistence, replication broadcast, client tracking) are deferred and
    /// applied as a single atomic batch after all commands complete.
    ///
    /// This prevents replicas from observing intermediate transaction state.
    pub(crate) async fn execute_transaction(
        &mut self,
        commands: Vec<ParsedCommand>,
        watches: &[(Bytes, u64)],
        conn_id: u64,
        protocol_version: ProtocolVersion,
    ) -> TransactionResult {
        // F3: a watched key may have expired only *lazily* — its TTL elapsed
        // with no active sweep and no access to trigger a purge since it was
        // watched. WATCH is validated against the per-shard version, not the
        // keys themselves, and `check_watches` below is a pure version compare;
        // it cannot see a still-physically-present but logically-expired key.
        // Purge any expired watched keys here, at the EXEC watch-validation
        // seam, so a removal bumps the shard version and the check below
        // observes the change. This must run BEFORE `check_watches`: the queued
        // commands' own lazy purges happen only after this point, too late for
        // this EXEC's watch check. `HashMapStore` stays version-ignorant — the
        // version bump lives at this worker seam, mirroring active expiry
        // (`apply_expiry_effects`, event_loop.rs). Matches Redis/Valkey/
        // Dragonfly, where an expired watched key counts as modified at EXEC
        // (redis PR #7920 / issue #7918).
        self.purge_expired_watches(watches);

        // Check WATCH conditions
        if !self.check_watches(watches) {
            return TransactionResult::WatchAborted;
        }

        let rollback_mode = self.persistence.has_wal() && self.persistence.should_rollback();

        // Execute all commands, deferring side effects
        let mut results = Vec::with_capacity(commands.len());
        // (write metadata, originating command index)
        let mut write_metas: Vec<(WriteCommandMeta, usize)> = Vec::new();
        let mut total_dirty: i64 = 0;
        let mut snapshots: Vec<WriteSnapshot> = Vec::new();
        let mut had_writes = false;

        for (i, command) in commands.iter().enumerate() {
            // Capture pre-execution snapshot for rollback if this is a write
            if rollback_mode {
                let cmd_name = command.name_uppercase();
                let cmd_name_str = String::from_utf8_lossy(&cmd_name);
                if let Some(handler) = self.registry.get(&cmd_name_str)
                    && handler
                        .flags()
                        .contains(crate::command::CommandFlags::WRITE)
                {
                    snapshots.push(self.capture_write_snapshot(handler.as_ref(), &command.args));
                }
            }

            let (response, meta) =
                self.execute_command_inner(command, conn_id, protocol_version, false);

            // Keyspace hit/miss metrics are recorded inside execute_command_inner
            // (lookup level), so MULTI/EXEC commands are counted the same way as
            // the single-command path, matching Redis (INFO stats count
            // hits/misses inside transactions).

            if let Some(write_meta) = meta {
                had_writes = true;
                total_dirty += write_meta.dirty_delta;
                write_metas.push((write_meta, i));
            }

            // Inside MULTI/EXEC, blocking commands execute non-blocking: if no
            // data is available they return BlockingNeeded, which we convert to
            // nil (matching Redis semantics where blocking commands in a
            // transaction never actually block).
            let response = if matches!(&response, Response::BlockingNeeded { .. }) {
                Response::Null
            } else {
                response
            };

            results.push(response);
        }

        // Run batched post-execution for all write commands
        if had_writes {
            // Collect write command info for batched post-execution
            let write_infos: Vec<WriteRecord<'_>> = write_metas
                .iter()
                .map(|(meta, idx)| WriteRecord {
                    handler: meta.handler.as_ref() as &dyn Command,
                    args: commands[*idx].args.as_slice(),
                    hll_wal_delta: meta.hll_wal_delta.as_deref(),
                    keyspace_events: meta.keyspace_events.as_slice(),
                })
                .collect();

            if rollback_mode {
                // Batch WAL persistence with rollback on failure
                if let Err(e) = self
                    .persist(&write_infos, super::persistence::Durability::Confirm)
                    .await
                {
                    tracing::error!(
                        error = %e,
                        "Transaction WAL persistence failed, rolling back"
                    );
                    // Rollback all snapshots in reverse order
                    for snapshot in snapshots.into_iter().rev() {
                        self.rollback_snapshot(snapshot);
                    }
                    WalRollbacks::inc(self.observability.metrics());
                    // Mark all results as aborted
                    results.clear();
                    for _ in 0..commands.len() {
                        results.push(Response::error(
                            "EXECABORT transaction aborted due to WAL failure",
                        ));
                    }
                    return TransactionResult::Success(results);
                }
                // WAL succeeded — run remaining post-execution (without WAL)
                self.run_write_effects(
                    WriteSummary {
                        writes: &write_infos,
                        dirty_delta: total_dirty,
                        conn_id,
                    },
                    WalPhase::AlreadyPersisted,
                    EffectScope::Transaction,
                )
                .await;
            } else {
                self.run_write_effects(
                    WriteSummary {
                        writes: &write_infos,
                        dirty_delta: total_dirty,
                        conn_id,
                    },
                    WalPhase::Persist,
                    EffectScope::Transaction,
                )
                .await;
            }
        }

        TransactionResult::Success(results)
    }

    /// Execute one shard's slice of a cross-shard scatter operation.
    ///
    /// Wraps [`Self::execute_scatter_part_body`] so that any keys a scatter part
    /// lazily purged (MGET/DEL/TOUCH/COPY/DUMP funnel through
    /// `check_and_delete_expired`) have their parity effects applied once, after
    /// the part, regardless of which of the body's early returns fired. This is
    /// the scatter-path counterpart of the [`Self::execute_command_inner`] seam —
    /// scatter dispatch (`ScatterRequest`) never routes through
    /// `execute_command_inner`, so the drain must be anchored here too.
    pub(crate) async fn execute_scatter_part(
        &mut self,
        keys: &[Bytes],
        operation: &ScatterOp,
        conn_id: u64,
    ) -> PartialResult {
        let out = self
            .execute_scatter_part_body(keys, operation, conn_id)
            .await;
        // Apply parity effects for any keys the scatter part lazily purged
        // (gaps 1-3). Task 1: this is a no-op drain; Task 2 fills in the effects.
        self.apply_lazy_purge_effects();
        out
    }

    /// The scatter-part body proper (see [`Self::execute_scatter_part`], which
    /// wraps this to drain lazy-purge effects afterward).
    async fn execute_scatter_part_body(
        &mut self,
        keys: &[Bytes],
        operation: &ScatterOp,
        conn_id: u64,
    ) -> PartialResult {
        let results = match operation {
            ScatterOp::MGet => self.scatter_mget(keys, conn_id),
            ScatterOp::MSet { pairs } => self.scatter_mset(pairs, conn_id).await,
            ScatterOp::Del | ScatterOp::Unlink => {
                self.scatter_del(keys, conn_id, matches!(operation, ScatterOp::Unlink))
                    .await
            }
            ScatterOp::Exists => {
                // Keyspace hit/miss counted per key through the shared accounting
                // seam (this cross-shard path bypasses the single-shard LookupSpec
                // seam), consistent with the `LookupSpec::EveryKey` rule: the
                // unexpired-existence probe is the hit, independent of the reply's
                // own `contains` test.
                let mut existed = Vec::with_capacity(keys.len());
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    existed.push(self.store.exists_unexpired(key));
                    let exists = self.store.contains(key);
                    results.push((key.clone(), Response::Integer(if exists { 1 } else { 0 })));
                }
                self.record_lookup_existence(existed);
                results
            }
            ScatterOp::Touch => {
                let mut existed = Vec::with_capacity(keys.len());
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    existed.push(self.store.exists_unexpired(key));
                    let touched = self.store.touch(key);
                    results.push((key.clone(), Response::Integer(if touched { 1 } else { 0 })));
                }
                self.record_lookup_existence(existed);
                results
            }
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
            ScatterOp::FlushDb => self.scatter_flushdb(conn_id).await,
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
                // Returns an array with: [serialized_value, expiry_ms_or_nil].
                // COPY ships the expiry out-of-band (the array's second element),
                // so the transport frame is produced with an expiry-free header.
                match self.serialize_key_for_transport(source_key, false) {
                    Some((serialized, expiry_ms)) => {
                        let expiry_resp = match expiry_ms {
                            Some(ms) if ms > 0 => Response::Integer(ms),
                            _ => Response::null(),
                        };

                        vec![(
                            source_key.clone(),
                            Response::Array(vec![Response::bulk(serialized), expiry_resp]),
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
                value_data,
                expiry_ms,
                replace,
            } => {
                return PartialResult::from_results(
                    self.scatter_copy_set(dest_key, value_data, expiry_ms, *replace, conn_id)
                        .await,
                );
            }
            ScatterOp::RandomKey => {
                // Return a random key from this shard
                match self.store.random_key() {
                    Some(key) => vec![(Bytes::from_static(b"__randomkey__"), Response::bulk(key))],
                    None => vec![(Bytes::from_static(b"__randomkey__"), Response::null())],
                }
            }
            ScatterOp::Dump => {
                // Serialize keys with full metadata for MIGRATE through the single
                // transport-frame producer. DUMP embeds the expiry in the frame
                // header (the format RESTORE reads back), so it requests
                // `expiry_in_header` and ignores the returned out-of-band TTL.
                keys.iter()
                    .map(|key| match self.serialize_key_for_transport(key, true) {
                        Some((serialized, _ttl_ms)) => (key.clone(), Response::bulk(serialized)),
                        // Key doesn't exist
                        None => (key.clone(), Response::null()),
                    })
                    .collect()
            }
            ScatterOp::TsQueryIndex { args } => self.execute_ts_queryindex(args),
            ScatterOp::TsMget { args } => self.execute_ts_mget(args),
            ScatterOp::TsMrange { args, reverse } => self.execute_ts_mrange(args, *reverse),
            ScatterOp::FtCreate { index_def_json } => self.execute_ft_create(index_def_json).await,
            ScatterOp::FtSearch {
                index_name,
                request,
            } => {
                return PartialResult::from_ft(frogdb_search::FtShardReply::Search(
                    self.execute_ft_search(index_name, request),
                ));
            }
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
                request,
            } => {
                return PartialResult::from_ft(frogdb_search::FtShardReply::Aggregate(
                    self.execute_ft_aggregate(index_name, request),
                ));
            }
            ScatterOp::FtHybrid {
                index_name,
                query_args,
            } => {
                return PartialResult::from_ft(frogdb_search::FtShardReply::Search(
                    self.execute_ft_hybrid(index_name, query_args),
                ));
            }
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

        PartialResult::from_results(results)
    }

    fn scatter_mget(&mut self, keys: &[Bytes], conn_id: u64) -> Vec<(Bytes, Response)> {
        // Keyspace hit/miss is counted per key at lookup level (one per key),
        // matching Redis MGET (each key is an independent lookupKeyRead). A
        // non-string existing key still counts as a hit — the key lookup
        // succeeded even though the value is replied as nil.
        let mut results = Vec::with_capacity(keys.len());
        let mut existed = Vec::with_capacity(keys.len());
        for key in keys {
            let response = match self.store.get_with_expiry_check(key) {
                Some(value) => {
                    existed.push(true);
                    if let Some(sv) = value.as_string() {
                        Response::bulk(sv.as_bytes())
                    } else {
                        Response::null()
                    }
                }
                None => {
                    existed.push(false);
                    Response::null()
                }
            };
            results.push((key.clone(), response));
        }
        self.record_lookup_existence(existed);
        // Client tracking: record reads for MGET
        if self.tracking.has_tracking_clients() {
            for key in keys {
                self.tracking.record_read(key, conn_id);
            }
        }
        results
    }

    /// Resolve the command handler used to represent a cross-shard scatter write
    /// in the canonical post-execution pipeline.
    ///
    /// A scatter part reconstructs the writes it performed as ordinary commands
    /// (`SET`/`DEL`/`UNLINK`/`FLUSHDB`/`RESTORE`) so they flow through the shared
    /// [`ShardWorker::run_write_effects`] pipeline. Those are always-registered
    /// write commands in any running server — the parent scatter command (MSET,
    /// DEL, COPY, …) could not have been dispatched here otherwise — so a miss is
    /// a server-construction bug, surfaced loudly rather than silently dropping
    /// WAL/replication effects.
    fn scatter_write_handler(&self, name: &str) -> Arc<dyn Command> {
        self.registry.get(name).unwrap_or_else(|| {
            panic!("scatter effect pipeline requires the `{name}` command to be registered")
        })
    }

    async fn scatter_mset(
        &mut self,
        pairs: &[(Bytes, Bytes)],
        conn_id: u64,
    ) -> Vec<(Bytes, Response)> {
        let mut results = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            self.store.set(key.clone(), Value::string(value.clone()));
            results.push((key.clone(), Response::ok()));
        }
        // Route this shard's slice of the MSET through the canonical write-effect
        // pipeline as one `SET` per pair. This is where cross-shard MSET now
        // (correctly) gets keyspace notifications, replication broadcast, waiter
        // satisfaction, the dirty counter, and search-index upkeep — effects the
        // old inline path silently skipped (it did WAL + version + tracking only).
        if !pairs.is_empty() {
            let set = self.scatter_write_handler("SET");
            let writes: Vec<(Arc<dyn Command>, Vec<Bytes>)> = pairs
                .iter()
                .map(|(key, value)| (set.clone(), vec![key.clone(), value.clone()]))
                .collect();
            self.run_scatter_effects(writes, pairs.len() as i64, conn_id)
                .await;
        }
        results
    }

    async fn scatter_del(
        &mut self,
        keys: &[Bytes],
        conn_id: u64,
        is_unlink: bool,
    ) -> Vec<(Bytes, Response)> {
        let mut results = Vec::with_capacity(keys.len());
        let mut deleted_keys: Vec<Bytes> = Vec::new();
        for key in keys {
            // Trigger lazy expiry first: if the key is stale (expired metadata),
            // it gets cleaned up here and the subsequent delete() returns false.
            // This matches Redis behavior where DEL on an expired key returns 0
            // and does not dirty WATCH state.
            let _ = self.store.get_with_expiry_check(key);

            let deleted = self.store.delete(key);
            if deleted {
                deleted_keys.push(key.clone());
            }
            results.push((key.clone(), Response::Integer(if deleted { 1 } else { 0 })));
        }
        // Route the keys actually removed through the canonical write-effect
        // pipeline as a single DEL/UNLINK over exactly those keys. Cross-shard
        // DEL/UNLINK now (correctly) gets keyspace notifications, replication
        // broadcast, waiter satisfaction (a DEL wakes blocked BLPOP/XREAD/…), the
        // dirty counter, and search-index removal — effects the old inline path
        // skipped (it did WAL + version + tracking + UNLINK lazyfree only).
        if !deleted_keys.is_empty() {
            // Lazyfree accounting for UNLINK is not a pipeline effect (it is
            // derived from the command context on the single-command path), so
            // it stays here.
            if is_unlink {
                self.observability
                    .record_lazyfreed(deleted_keys.len() as u64);
            }
            let name = if is_unlink { "UNLINK" } else { "DEL" };
            let handler = self.scatter_write_handler(name);
            let dirty_delta = deleted_keys.len() as i64;
            self.run_scatter_effects(vec![(handler, deleted_keys)], dirty_delta, conn_id)
                .await;
        }
        results
    }

    async fn scatter_flushdb(&mut self, conn_id: u64) -> Vec<(Bytes, Response)> {
        // Clear all keys in this shard.
        // Only increment version if there were live (non-expired) keys to clear,
        // so WATCH on non-existing keys or stale (expired) keys is not aborted.
        // This matches Redis behavior where FLUSHDB of only-expired keys does
        // not dirty WATCH state.
        let total_count = self.store.len() as u64;
        let expired_count = self.store.get_expired_keys(std::time::Instant::now()).len() as u64;
        let live_count = total_count.saturating_sub(expired_count);
        self.store.clear();
        if total_count > 0 {
            // Track lazyfreed objects for FLUSHDB/FLUSHALL. This is not a pipeline
            // effect (it is derived from the command context on the single-command
            // path), so it stays here.
            self.observability.record_lazyfreed(total_count);
        }
        // Route FLUSHDB through the canonical write-effect pipeline. This is where
        // cross-shard FLUSHDB now (correctly) broadcasts to replicas and runs in
        // canonical order; the `FLUSHDB` handler drives the flush-all tracking
        // invalidation via `invalidate_written_keys`. A negative `dirty_delta`
        // when only expired keys were cleared suppresses the version bump (WATCH
        // no-op rule), preserving the previous "bump iff live keys" behavior.
        let handler = self.scatter_write_handler("FLUSHDB");
        let dirty_delta = if live_count > 0 {
            live_count as i64
        } else {
            -1
        };
        self.run_scatter_effects(vec![(handler, Vec::new())], dirty_delta, conn_id)
            .await;
        vec![(Bytes::from_static(b"__flushdb__"), Response::ok())]
    }

    async fn scatter_copy_set(
        &mut self,
        dest_key: &Bytes,
        value_data: &Bytes,
        expiry_ms: &Option<i64>,
        replace: bool,
        conn_id: u64,
    ) -> Vec<(Bytes, Response)> {
        // Write a value from cross-shard copy to destination key.
        // Check if destination exists (when not using REPLACE)
        if !replace && self.store.contains(dest_key) {
            return vec![(dest_key.clone(), Response::Integer(0))];
        }

        // Deserialize the value through the shared transport-frame seam. The frame
        // is self-describing (it carries its own type marker), so no separate type
        // tag is needed. Expiry is applied from `expiry_ms` below, not the header.
        match Self::deserialize_transport_frame(value_data.as_ref()) {
            Ok((value, _metadata)) => {
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

                // Route the destination write through the canonical write-effect
                // pipeline as a RESTORE — exactly how cross-shard MIGRATE ships a
                // value, and the one command whose payload uses this same
                // persistence codec. This is where the cross-shard COPY
                // destination now (correctly) gets WAL persistence (with the
                // applied expiry), keyspace notification, replication broadcast
                // (so replicas reconstruct the copied value), the dirty counter,
                // and tracking invalidation — all effects the old inline path
                // skipped except WAL + version. `ttl_ms` is the relative expiry
                // that travels alongside the (expiry-free) serialized frame.
                let ttl_ms: i64 = expiry_ms.filter(|ms| *ms > 0).unwrap_or(0);
                let mut restore_args = vec![
                    dest_key.clone(),
                    Bytes::from(ttl_ms.to_string()),
                    value_data.clone(),
                ];
                if replace {
                    restore_args.push(Bytes::from_static(b"REPLACE"));
                }
                let handler = self.scatter_write_handler("RESTORE");
                self.run_scatter_effects(vec![(handler, restore_args)], 1, conn_id)
                    .await;

                vec![(dest_key.clone(), Response::Integer(1))]
            }
            Err(_) => {
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

#[cfg(test)]
mod scatter_effect_tests {
    //! End-to-end tests that the cross-shard scatter write path now emits the
    //! effects the old hand-rolled inline path silently skipped: keyspace
    //! notifications, replication broadcast, and waiter satisfaction — all via
    //! the single canonical `run_write_effects` pipeline.
    use super::*;

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    use tokio::sync::{mpsc, oneshot};

    use crate::command::{
        Arity, CommandContext, CommandFlags, WaiterKind, WaiterWake, WalStrategy,
    };
    use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
    use crate::eviction::EvictionConfig;
    use crate::keyspace_event::KeyspaceEventFlags;
    use crate::noop::NoopMetricsRecorder;
    use crate::pubsub::PubSubMessage;
    use crate::registry::CommandRegistry;
    use crate::replication::{ReplicationBroadcaster, SharedBroadcaster};
    use crate::shard::message::{ShardReceiver, ShardSender};
    use crate::shard::wait_queue::WaitEntry;
    use crate::types::BlockingOp;

    /// A broadcaster that records every command it is asked to replicate and
    /// reports itself active, so the pipeline actually invokes it.
    #[derive(Default)]
    struct RecordingBroadcaster {
        commands: Mutex<Vec<(String, Vec<Bytes>)>>,
    }

    impl ReplicationBroadcaster for RecordingBroadcaster {
        fn broadcast_command_on_shard(
            &self,
            _shard_id: u16,
            cmd_name: &str,
            args: &[Bytes],
        ) -> u64 {
            let mut g = self.commands.lock().unwrap();
            g.push((cmd_name.to_string(), args.to_vec()));
            g.len() as u64
        }
        fn is_active(&self) -> bool {
            true
        }
        fn current_offset(&self) -> u64 {
            self.commands.lock().unwrap().len() as u64
        }
    }

    /// Mock `SET` (what an MSET scatter part reconstructs per pair): emits a
    /// `set` string event and wakes all waiter kinds, exactly like the real one.
    struct MockSet;
    impl Command for MockSet {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "SET",
                arity: Arity::AtLeast(2),
                flags: CommandFlags::WRITE,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::PersistFirstKey,
                wakes: WaiterWake::All,
                event: EventSpec::Emits {
                    class: KeyspaceEventFlags::STRING,
                    name: "set",
                },
                requires_same_slot: false,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    /// Mock `DEL` (what a DEL scatter part reconstructs over the removed keys):
    /// emits a `del` generic event and wakes all waiter kinds.
    struct MockDel;
    impl Command for MockDel {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "DEL",
                arity: Arity::AtLeast(1),
                flags: CommandFlags::WRITE,
                keys: KeySpec::All,
                access: AccessSpec::Uniform,
                wal: WalStrategy::DeleteKeys,
                wakes: WaiterWake::All,
                event: EventSpec::Emits {
                    class: KeyspaceEventFlags::GENERIC,
                    name: "del",
                },
                requires_same_slot: false,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    /// Build a single-shard worker (so keyspace notifications deliver into the
    /// local subscription table) with a recording broadcaster and all keyspace
    /// notification classes/channels enabled.
    fn scatter_worker(bc: SharedBroadcaster) -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_conn_tx, conn_rx) = mpsc::channel(16);
        let shard_senders = Arc::new(vec![ShardSender::new(msg_tx)]);
        let mut registry = CommandRegistry::new();
        registry.register(MockSet);
        registry.register(MockDel);
        let mut worker = ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            shard_senders,
            Arc::new(registry),
            EvictionConfig::default(),
            Arc::new(NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            bc,
        );
        let flags = KeyspaceEventFlags::KEYSPACE
            | KeyspaceEventFlags::KEYEVENT
            | KeyspaceEventFlags::ALL_TYPES;
        worker.set_notify_keyspace_events(Arc::new(AtomicU32::new(flags.bits())));
        worker
    }

    #[tokio::test]
    async fn scatter_mset_emits_notification_and_broadcast() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc.clone() as SharedBroadcaster);

        // Observe the `set` key-event notification the old inline MSET skipped.
        let (ntx, mut nrx) = mpsc::unbounded_channel();
        worker
            .subscriptions
            .subscribe(Bytes::from_static(b"__keyevent@0__:set"), 1, ntx);

        let pairs = [(Bytes::from_static(b"mk"), Bytes::from_static(b"mv"))];
        let results = worker.scatter_mset(&pairs, 42).await;
        assert!(matches!(&results[0].1, Response::Simple(s) if &s[..] == b"OK"));

        // Value landed.
        assert!(worker.store.contains(b"mk"));
        // Version bumped exactly once for the whole scatter part.
        assert_eq!(worker.shard_version, 1);

        // Keyspace notification emitted (previously skipped).
        match nrx.try_recv() {
            Ok(PubSubMessage::Message { channel, payload }) => {
                assert_eq!(&channel[..], b"__keyevent@0__:set");
                assert_eq!(&payload[..], b"mk");
            }
            other => panic!("expected a `set` key-event notification, got {other:?}"),
        }

        // Replication broadcast emitted (previously skipped): one SET per pair.
        let cmds = bc.commands.lock().unwrap();
        assert_eq!(cmds.len(), 1, "one SET should be broadcast for one pair");
        assert_eq!(cmds[0].0, "SET");
        assert_eq!(
            cmds[0].1,
            vec![Bytes::from_static(b"mk"), Bytes::from_static(b"mv")]
        );
    }

    #[tokio::test]
    async fn scatter_del_emits_notification_broadcast_and_wakes_waiter() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc.clone() as SharedBroadcaster);

        // Seed the key so DEL actually removes it.
        worker
            .store
            .set(Bytes::from_static(b"sk"), Value::string("v"));

        // A blocked XREADGROUP waiter on the key: deleting the key must wake it
        // (NOGROUP). The old inline DEL never invoked waiter satisfaction.
        let (wtx, mut wrx) = oneshot::channel();
        let entry = WaitEntry {
            conn_id: 7,
            keys: vec![Bytes::from_static(b"sk")],
            op: BlockingOp::XReadGroup {
                group: Bytes::from_static(b"g"),
                consumer: Bytes::from_static(b"c"),
                noack: false,
                count: None,
            },
            response_tx: wtx,
            deadline: None,
            protocol_version: ProtocolVersion::default(),
        };
        worker.wait_queue.register(entry).unwrap();
        assert!(
            worker
                .wait_queue
                .has_waiters_for_kind(&Bytes::from_static(b"sk"), WaiterKind::Stream)
        );

        // Observe the `del` key-event notification.
        let (ntx, mut nrx) = mpsc::unbounded_channel();
        worker
            .subscriptions
            .subscribe(Bytes::from_static(b"__keyevent@0__:del"), 1, ntx);

        let results = worker
            .scatter_del(&[Bytes::from_static(b"sk")], 42, false)
            .await;
        assert!(matches!(results[0].1, Response::Integer(1)));
        assert_eq!(worker.shard_version, 1);

        // Waiter woken (previously skipped): drained with NOGROUP, queue empty.
        assert!(
            !worker
                .wait_queue
                .has_waiters_for_kind(&Bytes::from_static(b"sk"), WaiterKind::Stream),
            "the blocked waiter should have been woken and removed"
        );
        match wrx.try_recv() {
            Ok(Response::Error(msg)) => {
                assert!(
                    msg.starts_with(b"NOGROUP"),
                    "expected NOGROUP wake, got {}",
                    String::from_utf8_lossy(&msg)
                );
            }
            other => panic!("expected the waiter to be woken with an error, got {other:?}"),
        }

        // Keyspace notification emitted (previously skipped).
        match nrx.try_recv() {
            Ok(PubSubMessage::Message { channel, payload }) => {
                assert_eq!(&channel[..], b"__keyevent@0__:del");
                assert_eq!(&payload[..], b"sk");
            }
            other => panic!("expected a `del` key-event notification, got {other:?}"),
        }

        // Replication broadcast emitted (previously skipped): DEL over the key.
        let cmds = bc.commands.lock().unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].0, "DEL");
        assert_eq!(cmds[0].1, vec![Bytes::from_static(b"sk")]);
    }

    #[tokio::test]
    async fn scatter_del_of_missing_keys_is_a_noop() {
        // DEL of only-missing keys removes nothing → no writes → the pipeline
        // short-circuits: no version bump, no broadcast (WATCH no-op rule).
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc.clone() as SharedBroadcaster);

        let results = worker
            .scatter_del(&[Bytes::from_static(b"absent")], 42, false)
            .await;
        assert!(matches!(results[0].1, Response::Integer(0)));
        assert_eq!(
            worker.shard_version, 0,
            "no version bump when nothing deleted"
        );
        assert!(
            bc.commands.lock().unwrap().is_empty(),
            "nothing to replicate when nothing was deleted"
        );
    }

    // ------------------------------------------------------------------------
    // Item D pins (proposal 62): the scatter path no longer reaches past the
    // transport-codec seam (D1) or the keyspace-accounting seam (D2). These
    // pin the behavior each helper now owns for exactly one place instead of
    // three inline copies.
    // ------------------------------------------------------------------------

    /// D1: the transport frame has one producer. DUMP embeds the key's expiry in
    /// the frame header (RESTORE reads it back when given no TTL override);
    /// cross-shard COPY writes an expiry-free header and ships the TTL
    /// out-of-band. The expiry extraction lives once, so the two variants can
    /// only differ in that one documented way — asserted here — and both decode
    /// back to the same value through the shared decode seam.
    #[tokio::test]
    async fn transport_frame_producer_dump_embeds_copy_omits_expiry() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc as SharedBroadcaster);

        worker.store.set(
            Bytes::from_static(b"k"),
            Value::string(Bytes::from_static(b"v")),
        );
        worker
            .store
            .set_expiry(b"k", Instant::now() + Duration::from_secs(100));

        // DUMP variant: expiry embedded in header; COPY variant: expiry-free.
        let (dump_bytes, dump_ttl) = worker
            .serialize_key_for_transport(b"k", true)
            .expect("key present");
        let (copy_bytes, copy_ttl) = worker
            .serialize_key_for_transport(b"k", false)
            .expect("key present");

        // Same out-of-band relative TTL from the same source expiry (~100s):
        // the extraction is shared, so it cannot drift between the two.
        assert_eq!(dump_ttl, copy_ttl);
        let ttl = copy_ttl.expect("expiry present");
        assert!((99_000..=100_000).contains(&ttl), "unexpected ttl {ttl}");

        // The two frames differ only in the header expiry field.
        assert_ne!(dump_bytes, copy_bytes);

        let (dv, dmeta) = ShardWorker::deserialize_transport_frame(&dump_bytes).unwrap();
        let (cv, cmeta) = ShardWorker::deserialize_transport_frame(&copy_bytes).unwrap();
        assert_eq!(
            dv.as_string().map(|s| s.as_bytes()),
            Some(Bytes::from_static(b"v"))
        );
        assert_eq!(
            cv.as_string().map(|s| s.as_bytes()),
            Some(Bytes::from_static(b"v"))
        );
        assert!(dmeta.expires_at.is_some(), "DUMP frame must embed expiry");
        assert!(cmeta.expires_at.is_none(), "COPY frame must omit expiry");
    }

    /// D1: the cross-shard COPY scatter arm ships the transport frame (decodable
    /// through the shared seam) plus the relative TTL as the array's second
    /// element — the arm keeps only its reply shaping, not its own serialize.
    #[tokio::test]
    async fn scatter_copy_arm_ships_frame_and_out_of_band_ttl() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc as SharedBroadcaster);

        worker.store.set(
            Bytes::from_static(b"src"),
            Value::string(Bytes::from_static(b"hello")),
        );
        worker
            .store
            .set_expiry(b"src", Instant::now() + Duration::from_secs(50));

        let r = worker
            .execute_scatter_part(
                &[],
                &ScatterOp::Copy {
                    source_key: Bytes::from_static(b"src"),
                },
                1,
            )
            .await;
        let arr = match &r.results[0].1 {
            Response::Array(a) => a,
            other => panic!("expected COPY array, got {other:?}"),
        };
        assert_eq!(arr.len(), 2);
        let frame = match &arr[0] {
            Response::Bulk(Some(b)) => b.clone(),
            other => panic!("expected bulk frame, got {other:?}"),
        };
        let (val, meta) = ShardWorker::deserialize_transport_frame(&frame).unwrap();
        assert_eq!(
            val.as_string().map(|s| s.as_bytes()),
            Some(Bytes::from_static(b"hello"))
        );
        assert!(
            meta.expires_at.is_none(),
            "COPY frame header is expiry-free"
        );
        match &arr[1] {
            Response::Integer(ms) => {
                assert!((49_000..=50_000).contains(ms), "unexpected ttl {ms}")
            }
            other => panic!("expected integer ttl, got {other:?}"),
        }
    }

    /// D2: EXISTS, TOUCH, and MGET all route hit/miss accounting through the one
    /// `record_lookup_existence` seam — *existence == hit*. Two present keys and
    /// one absent key must record 2 hits + 1 miss for each op.
    #[tokio::test]
    async fn scatter_lookup_accounting_counts_existence_as_hits() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc as SharedBroadcaster);

        worker.store.set(
            Bytes::from_static(b"a"),
            Value::string(Bytes::from_static(b"1")),
        );
        worker.store.set(
            Bytes::from_static(b"b"),
            Value::string(Bytes::from_static(b"2")),
        );
        // "c" is absent.
        let keys = [
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ];

        // EXISTS: existence probe (unexpired) == hit.
        let h0 = worker.observability.keyspace_stats().cumulative_hits();
        let m0 = worker.observability.keyspace_stats().cumulative_misses();
        worker
            .execute_scatter_part(&keys, &ScatterOp::Exists, 1)
            .await;
        assert_eq!(
            worker.observability.keyspace_stats().cumulative_hits() - h0,
            2
        );
        assert_eq!(
            worker.observability.keyspace_stats().cumulative_misses() - m0,
            1
        );

        // MGET: the fetched value == hit (get-based existence).
        let h1 = worker.observability.keyspace_stats().cumulative_hits();
        let m1 = worker.observability.keyspace_stats().cumulative_misses();
        let mg = worker.scatter_mget(&keys, 1);
        assert_eq!(mg.len(), 3);
        assert_eq!(
            worker.observability.keyspace_stats().cumulative_hits() - h1,
            2
        );
        assert_eq!(
            worker.observability.keyspace_stats().cumulative_misses() - m1,
            1
        );

        // TOUCH: existence probe (unexpired) == hit.
        let h2 = worker.observability.keyspace_stats().cumulative_hits();
        let m2 = worker.observability.keyspace_stats().cumulative_misses();
        worker
            .execute_scatter_part(&keys, &ScatterOp::Touch, 1)
            .await;
        assert_eq!(
            worker.observability.keyspace_stats().cumulative_hits() - h2,
            2
        );
        assert_eq!(
            worker.observability.keyspace_stats().cumulative_misses() - m2,
            1
        );
    }

    /// D3 (round-7 follow-up item 3): a hot key past its TTL but not yet
    /// lazily purged must read as absent through scatter MGET — the hot
    /// fast-path (`Store::get`) has no expiry check, so without routing
    /// through `get_with_expiry_check` this would return the stale value.
    /// Covers both the reply (nil) and the accounting (miss, not hit).
    #[tokio::test]
    async fn scatter_mget_treats_hot_expired_key_as_miss() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc as SharedBroadcaster);

        // Plain `set` lands the key hot; `set_expiry` with a past deadline
        // makes it logically expired without triggering any purge.
        worker.store.set(
            Bytes::from_static(b"stale"),
            Value::string(Bytes::from_static(b"old")),
        );
        worker
            .store
            .set_expiry(b"stale", Instant::now() - Duration::from_secs(60));
        assert!(
            worker.store.contains(b"stale"),
            "key must still be physically present before the scatter read"
        );

        let h0 = worker.observability.keyspace_stats().cumulative_hits();
        let m0 = worker.observability.keyspace_stats().cumulative_misses();

        let keys = [Bytes::from_static(b"stale")];
        let results = worker.scatter_mget(&keys, 1);

        assert_eq!(results.len(), 1);
        match &results[0].1 {
            Response::Bulk(None) => {}
            other => panic!("expected nil for a hot expired key, got {other:?}"),
        }
        assert_eq!(
            worker.observability.keyspace_stats().cumulative_hits() - h0,
            0,
            "a hot expired key must not count as a hit"
        );
        assert_eq!(
            worker.observability.keyspace_stats().cumulative_misses() - m0,
            1,
            "a hot expired key must count as a miss"
        );
        assert!(
            !worker.store.contains(b"stale"),
            "the expiry-aware read must have lazily purged the key"
        );
    }

    /// D4 (round-7 follow-up item 3): the transport-frame producer shared by
    /// scatter COPY and DUMP must treat a hot expired key as absent, matching
    /// Redis (COPY/DUMP of an expired key behave as if the key never
    /// existed) instead of shipping a stale payload.
    #[tokio::test]
    async fn scatter_copy_and_dump_of_hot_expired_key_report_missing() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc as SharedBroadcaster);

        worker.store.set(
            Bytes::from_static(b"stale"),
            Value::string(Bytes::from_static(b"old")),
        );
        worker
            .store
            .set_expiry(b"stale", Instant::now() - Duration::from_secs(60));
        assert!(worker.store.contains(b"stale"));

        let copy = worker
            .execute_scatter_part(
                &[],
                &ScatterOp::Copy {
                    source_key: Bytes::from_static(b"stale"),
                },
                1,
            )
            .await;
        match &copy.results[0].1 {
            Response::Bulk(None) => {}
            other => panic!("expected COPY of an expired source to be nil, got {other:?}"),
        }

        // Re-arm the key for the DUMP half of the test (COPY's fetch already
        // purged it above).
        worker.store.set(
            Bytes::from_static(b"stale2"),
            Value::string(Bytes::from_static(b"old")),
        );
        worker
            .store
            .set_expiry(b"stale2", Instant::now() - Duration::from_secs(60));

        let dump = worker
            .execute_scatter_part(&[Bytes::from_static(b"stale2")], &ScatterOp::Dump, 1)
            .await;
        match &dump.results[0].1 {
            Response::Bulk(None) => {}
            other => panic!("expected DUMP of an expired key to be nil, got {other:?}"),
        }
    }

    /// A scatter part that lazily purges an expired key must drain the store's
    /// `lazily_purged` report at the `execute_scatter_part` seam. Scatter
    /// dispatch (`ScatterRequest`) never routes through `execute_command_inner`,
    /// so without the wrapper drain the report would leak into the NEXT,
    /// unrelated message and be applied at the wrong site (a spurious version
    /// bump / waiter drain under Task 2). Pins both halves: the purge physically
    /// fired AND its report was drained at the scatter seam.
    #[tokio::test]
    async fn scatter_mget_drains_lazy_purge_report() {
        let bc = Arc::new(RecordingBroadcaster::default());
        let mut worker = scatter_worker(bc as SharedBroadcaster);

        // Seed an already-expired key.
        worker.store.set(
            Bytes::from_static(b"k"),
            Value::string(Bytes::from_static(b"v")),
        );
        worker
            .store
            .set_expiry(b"k", Instant::now() - Duration::from_secs(60));
        assert!(worker.store.contains(b"k"));

        // A scatter MGET part reads the key via `get_with_expiry_check`, which
        // physically purges it (`check_and_delete_expired` -> `uninstall`).
        let _ = worker
            .execute_scatter_part(&[Bytes::from_static(b"k")], &ScatterOp::MGet, 1)
            .await;

        // The purge physically fired ...
        assert!(
            !worker.store.contains(b"k"),
            "scatter MGET must have physically purged the expired key"
        );
        // ... and the `execute_scatter_part` wrapper drained the report, so no
        // stale entry leaks to the next message.
        assert!(
            worker.store.take_lazily_purged().is_empty(),
            "execute_scatter_part must drain the lazy-purge report (no leak to the next message)"
        );
    }
}

#[cfg(test)]
mod command_effects_tests {
    //! Unit pins for the no-op suppression rule, stated once on
    //! [`CommandEffects::into_write_meta`] (proposal 55): a `write_was_noop`
    //! write drops its whole effect payload — `keyspace_events`,
    //! `hll_wal_delta`, `dirty_delta` — instead of relying on each drain site
    //! independently getting the gate right. `into_script_record` chains
    //! through the same rule for the two script drain sites.
    use super::*;

    use crate::command::{
        Arity, CommandContext, CommandEffects, CommandFlags, WaiterWake, WalStrategy,
    };
    use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
    use crate::keyspace_event::KeyspaceEventFlags;

    /// Minimal WRITE handler: the tests only need an `Arc<dyn Command>` to
    /// thread through the constructors; it is never executed.
    struct ProbeWrite;
    impl Command for ProbeWrite {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "PROBEWRITE",
                arity: Arity::AtLeast(1),
                flags: CommandFlags::WRITE,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::PersistFirstKey,
                wakes: WaiterWake::None,
                event: EventSpec::Dynamic,
                requires_same_slot: false,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    /// A fully-loaded deposit set, as a PFADD-like dynamic-event write would
    /// leave it on the context.
    fn loaded_effects(write_was_noop: bool) -> CommandEffects {
        CommandEffects {
            dirty_delta: 3,
            write_was_noop,
            hll_wal_delta: Some(smallvec::smallvec![(7u16, 5u8)]),
            keyspace_events: smallvec::smallvec![(
                Bytes::from_static(b"k"),
                "pfadd",
                KeyspaceEventFlags::STRING,
            )],
            ..Default::default()
        }
    }

    /// The exact contract documented on `CommandEffects::write_was_noop`: a
    /// no-op write's deposits (including keyspace_events and hll_wal_delta)
    /// are discarded wholesale — no `WriteCommandMeta` is built.
    #[test]
    fn into_write_meta_suppresses_noop_write_payload() {
        let meta = loaded_effects(true).into_write_meta(Arc::new(ProbeWrite));
        assert!(
            meta.is_none(),
            "a write_was_noop write must not produce a WriteCommandMeta"
        );
    }

    /// A non-no-op write carries its payload through unchanged.
    #[test]
    fn into_write_meta_carries_effect_payload() {
        let meta = loaded_effects(false)
            .into_write_meta(Arc::new(ProbeWrite))
            .expect("an effective write produces a WriteCommandMeta");
        assert_eq!(meta.dirty_delta, 3);
        assert_eq!(meta.hll_wal_delta.as_deref(), Some(&[(7u16, 5u8)][..]));
        assert_eq!(meta.keyspace_events.len(), 1);
        assert_eq!(meta.keyspace_events[0].0, Bytes::from_static(b"k"));
        assert_eq!(meta.keyspace_events[0].1, "pfadd");
        assert_eq!(meta.handler.name(), "PROBEWRITE");
    }

    /// The script drain sites reuse the same single-home rule: a no-op scripted
    /// sub-command records nothing.
    #[test]
    fn into_script_record_applies_same_noop_rule() {
        let args = vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")];
        assert!(
            loaded_effects(true)
                .into_script_record(Arc::new(ProbeWrite), args.clone())
                .is_none(),
            "a write_was_noop sub-command must not produce a ScriptWriteRecord"
        );

        let record = loaded_effects(false)
            .into_script_record(Arc::new(ProbeWrite), args.clone())
            .expect("an effective scripted write produces a ScriptWriteRecord");
        assert_eq!(record.args, args);
        assert_eq!(record.dirty_delta, 3);
        assert_eq!(record.hll_wal_delta.as_deref(), Some(&[(7u16, 5u8)][..]));
        assert_eq!(record.keyspace_events.len(), 1);
        assert_eq!(record.keyspace_events[0].1, "pfadd");
    }
}
