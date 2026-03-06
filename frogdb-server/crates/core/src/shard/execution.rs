use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

use crate::command::CommandContext;
use crate::store::Store;
use crate::types::{KeyMetadata, Value};
use crate::{Aggregation, LabelFilter, TimeSeriesValue};

use super::helpers::REPLICA_INTERNAL_CONN_ID;
use super::message::ScatterOp;
use super::types::{PartialResult, TransactionResult};
use super::worker::ShardWorker;

impl ShardWorker {
    /// Execute a command locally.
    pub(crate) async fn execute_command(
        &mut self,
        command: &ParsedCommand,
        conn_id: u64,
        protocol_version: ProtocolVersion,
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
                None, // replication_tracker - not available in shard
                None, // replication_state - not available in shard
                self.cluster.cluster_state.as_ref(),
                self.cluster.node_id,
                self.cluster.raft.as_ref(),
                self.cluster.network_factory.as_ref(),
                self.cluster.quorum_checker.as_ref().map(|q| q.as_ref()),
            );
            ctx.command_registry = Some(&self.registry);

            let response = match handler.execute(&mut ctx, &command.args) {
                Ok(response) => response,
                Err(err) => err.to_response(),
            };
            (response, ctx.dirty_delta)
        };

        // Track keyspace hits/misses for GET-like commands
        let is_get_command = matches!(
            cmd_name_str.as_ref(),
            "GET" | "GETEX" | "GETDEL" | "HGET" | "LINDEX"
        );
        if is_get_command {
            if matches!(response, Response::Null) {
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_keyspace_misses_total",
                    1,
                    &[],
                );
            } else {
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_keyspace_hits_total",
                    1,
                    &[],
                );
            }
        }

        // Increment version and dirty counter on write operations
        if is_write {
            self.increment_version();
            // Use the command's dirty_delta for dirty tracking.
            // - dirty_delta == 0 (default): command didn't set it, count as 1
            // - dirty_delta > 0: command explicitly set dirty count
            // - dirty_delta < 0: command explicitly says "no dirty change"
            let dirty_amount = if dirty_delta > 0 {
                dirty_delta as u64
            } else if dirty_delta < 0 {
                0
            } else {
                1 // Default: most write commands count as 1 dirty change
            };
            self.store.increment_dirty(dirty_amount);

            // Try to satisfy any blocking waiters after list/zset write operations
            let keys = handler.keys(&command.args);
            match cmd_name_str.as_ref() {
                // List push commands that may satisfy BLPOP/BRPOP/BLMOVE/BLMPOP waiters
                "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LINSERT" => {
                    for key in keys {
                        let key_bytes = Bytes::copy_from_slice(key);
                        self.try_satisfy_list_waiters(&key_bytes);
                    }
                }
                // Sorted set commands that may satisfy BZPOPMIN/BZPOPMAX/BZMPOP waiters
                "ZADD" => {
                    for key in keys {
                        let key_bytes = Bytes::copy_from_slice(key);
                        self.try_satisfy_zset_waiters(&key_bytes);
                    }
                }
                // Stream commands that may satisfy XREAD/XREADGROUP waiters
                "XADD" => {
                    for key in keys {
                        let key_bytes = Bytes::copy_from_slice(key);
                        self.try_satisfy_stream_waiters(&key_bytes);
                    }
                }
                _ => {}
            }

            // Persist to WAL for write operations
            self.persist_command_to_wal(&cmd_name_str, &command.args)
                .await;

            // Broadcast to replicas (if running as primary with connected replicas)
            // Skip broadcast if this command came from replication (to avoid infinite loops)
            if conn_id != REPLICA_INTERNAL_CONN_ID && self.replication_broadcaster.is_active() {
                self.replication_broadcaster
                    .broadcast_command(&cmd_name_str, &command.args);
            }
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

        // Execute all commands
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            let response = self
                .execute_command(&command, conn_id, protocol_version)
                .await;
            results.push(response);
        }

        TransactionResult::Success(results)
    }

    /// Execute part of a scatter-gather operation.
    pub(crate) async fn execute_scatter_part(
        &mut self,
        keys: &[Bytes],
        operation: &ScatterOp,
    ) -> PartialResult {
        let results = match operation {
            ScatterOp::MGet => keys
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
                .collect(),
            ScatterOp::MSet { pairs } => {
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
                }
                results
            }
            ScatterOp::Del | ScatterOp::Unlink => {
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
                }
                results
            }
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
            ScatterOp::FlushDb => {
                // Clear all keys in this shard
                // Only increment version if there were keys to clear,
                // so WATCH on non-existing keys is not aborted
                let had_keys = self.store.len() > 0;
                self.store.clear();
                if had_keys {
                    self.increment_version();
                }
                vec![(Bytes::from_static(b"__flushdb__"), Response::ok())]
            }
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
                // Write a value from cross-shard copy to destination key.
                // Check if destination exists (when not using REPLACE)
                if !replace && self.store.contains(dest_key) {
                    return PartialResult {
                        results: vec![(dest_key.clone(), Response::Integer(0))],
                    };
                }

                // Deserialize the value
                match Value::deserialize_for_copy(value_type, value_data) {
                    Some(value) => {
                        // If REPLACE, delete existing first
                        if *replace {
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
            ScatterOp::TsQueryIndex { args } => {
                let label_index = match self.store.ts_label_index() {
                    Some(idx) => idx,
                    None => return PartialResult { results: vec![] },
                };
                let filters = parse_ts_filters(args);
                let matching = label_index.query(&filters);
                matching
                    .into_iter()
                    .map(|key| (key.clone(), Response::bulk(key)))
                    .collect()
            }
            ScatterOp::TsMget { args } => {
                let label_index = match self.store.ts_label_index() {
                    Some(idx) => idx,
                    None => return PartialResult { results: vec![] },
                };
                let (label_mode, filters) = parse_mget_args(args);
                let matching = label_index.query(&filters);
                let mut results = Vec::new();
                for key in matching {
                    if let Some(val) = self.store.get(&key)
                        && let Some(ts) = val.as_timeseries()
                    {
                        let sample = match ts.get_last() {
                            Some((t, v)) => Response::Array(vec![
                                Response::Integer(t),
                                Response::bulk(Bytes::from(format_float(v))),
                            ]),
                            None => Response::Array(vec![]),
                        };
                        let labels_resp = build_labels_response(ts, &label_mode);
                        let entry =
                            Response::Array(vec![Response::bulk(key.clone()), labels_resp, sample]);
                        results.push((key, entry));
                    }
                }
                results
            }
            ScatterOp::TsMrange { args, reverse } => {
                let label_index = match self.store.ts_label_index() {
                    Some(idx) => idx,
                    None => return PartialResult { results: vec![] },
                };
                let params = parse_mrange_args(args);
                let matching = label_index.query(&params.filters);
                let mut results = Vec::new();
                for key in matching {
                    if let Some(val) = self.store.get(&key)
                        && let Some(ts) = val.as_timeseries()
                    {
                        let mut samples = if let Some((agg, bucket)) = params.aggregation {
                            ts.range_aggregated(params.from, params.to, bucket, agg)
                        } else if *reverse {
                            ts.revrange(params.from, params.to)
                        } else {
                            ts.range(params.from, params.to)
                        };

                        // Apply FILTER_BY_TS
                        if let Some(ref allowed_ts) = params.filter_by_ts {
                            samples.retain(|(t, _)| allowed_ts.contains(t));
                        }
                        // Apply FILTER_BY_VALUE
                        if let Some((min, max)) = params.filter_by_value {
                            samples.retain(|(_, v)| *v >= min && *v <= max);
                        }
                        // Apply COUNT
                        if let Some(limit) = params.count {
                            samples.truncate(limit);
                        }

                        let sample_responses: Vec<Response> = samples
                            .iter()
                            .map(|(t, v)| {
                                Response::Array(vec![
                                    Response::Integer(*t),
                                    Response::bulk(Bytes::from(format_float(*v))),
                                ])
                            })
                            .collect();

                        let labels_resp = build_labels_response(ts, &params.label_mode);
                        let entry = Response::Array(vec![
                            Response::bulk(key.clone()),
                            labels_resp,
                            Response::Array(sample_responses),
                        ]);
                        results.push((key, entry));
                    }
                }
                results
            }
        };

        PartialResult { results }
    }
}

// =========================================================================
// Helpers for TS scatter operations
// =========================================================================

/// Label output mode for MGET/MRANGE.
enum LabelMode {
    /// Don't include labels (default).
    None,
    /// Include all labels (WITHLABELS).
    WithLabels,
    /// Include selected labels (SELECTED_LABELS l1 l2 ...).
    SelectedLabels(Vec<String>),
}

/// Parsed parameters for MRANGE/MREVRANGE.
struct MrangeParams {
    from: i64,
    to: i64,
    filters: Vec<LabelFilter>,
    count: Option<usize>,
    aggregation: Option<(Aggregation, i64)>,
    label_mode: LabelMode,
    filter_by_ts: Option<Vec<i64>>,
    filter_by_value: Option<(f64, f64)>,
}

fn parse_ts_filters(args: &[Bytes]) -> Vec<LabelFilter> {
    args.iter()
        .filter_map(|arg| std::str::from_utf8(arg).ok().and_then(LabelFilter::parse))
        .collect()
}

fn parse_mget_args(args: &[Bytes]) -> (LabelMode, Vec<LabelFilter>) {
    let mut label_mode = LabelMode::None;
    let mut filter_args = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"WITHLABELS" => {
                label_mode = LabelMode::WithLabels;
            }
            b"SELECTED_LABELS" => {
                let mut selected = Vec::new();
                i += 1;
                while i < args.len() {
                    let u = args[i].to_ascii_uppercase();
                    if u == b"FILTER" {
                        break;
                    }
                    if let Ok(s) = std::str::from_utf8(&args[i]) {
                        selected.push(s.to_string());
                    }
                    i += 1;
                }
                label_mode = LabelMode::SelectedLabels(selected);
                continue; // Don't increment again
            }
            b"FILTER" => {
                i += 1;
                while i < args.len() {
                    filter_args.push(args[i].clone());
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }

    let filters = parse_ts_filters(&filter_args);
    (label_mode, filters)
}

fn parse_mrange_args(args: &[Bytes]) -> MrangeParams {
    let from = parse_range_bound_or_default(&args[0], i64::MIN);
    let to = parse_range_bound_or_default(&args[1], i64::MAX);

    let mut filters = Vec::new();
    let mut count = None;
    let mut aggregation = None;
    let mut label_mode = LabelMode::None;
    let mut filter_by_ts = None;
    let mut filter_by_value = None;

    let mut i = 2;
    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"FILTER" => {
                i += 1;
                let mut filter_args = Vec::new();
                while i < args.len() {
                    let u = args[i].to_ascii_uppercase();
                    // Stop at known keywords
                    if matches!(
                        u.as_slice(),
                        b"COUNT"
                            | b"AGGREGATION"
                            | b"WITHLABELS"
                            | b"SELECTED_LABELS"
                            | b"FILTER_BY_TS"
                            | b"FILTER_BY_VALUE"
                    ) {
                        break;
                    }
                    filter_args.push(args[i].clone());
                    i += 1;
                }
                filters = parse_ts_filters(&filter_args);
                continue;
            }
            b"COUNT" => {
                i += 1;
                if i < args.len()
                    && let Ok(s) = std::str::from_utf8(&args[i])
                {
                    count = s.parse().ok();
                }
            }
            b"AGGREGATION" => {
                i += 1;
                if i + 1 < args.len()
                    && let Ok(agg_str) = std::str::from_utf8(&args[i])
                    && let Some(agg) = Aggregation::parse(agg_str)
                {
                    i += 1;
                    if let Ok(bucket_str) = std::str::from_utf8(&args[i])
                        && let Ok(bucket) = bucket_str.parse::<i64>()
                    {
                        aggregation = Some((agg, bucket));
                    }
                }
            }
            b"WITHLABELS" => {
                label_mode = LabelMode::WithLabels;
            }
            b"SELECTED_LABELS" => {
                let mut selected = Vec::new();
                i += 1;
                while i < args.len() {
                    let u = args[i].to_ascii_uppercase();
                    if matches!(
                        u.as_slice(),
                        b"FILTER"
                            | b"COUNT"
                            | b"AGGREGATION"
                            | b"FILTER_BY_TS"
                            | b"FILTER_BY_VALUE"
                    ) {
                        break;
                    }
                    if let Ok(s) = std::str::from_utf8(&args[i]) {
                        selected.push(s.to_string());
                    }
                    i += 1;
                }
                label_mode = LabelMode::SelectedLabels(selected);
                continue;
            }
            b"FILTER_BY_TS" => {
                i += 1;
                let mut timestamps = Vec::new();
                while i < args.len()
                    && let Ok(s) = std::str::from_utf8(&args[i])
                    && let Ok(ts) = s.parse::<i64>()
                {
                    timestamps.push(ts);
                    i += 1;
                }
                filter_by_ts = Some(timestamps);
                continue;
            }
            b"FILTER_BY_VALUE" => {
                i += 1;
                if i + 1 < args.len()
                    && let (Ok(min_s), Ok(max_s)) = (
                        std::str::from_utf8(&args[i]),
                        std::str::from_utf8(&args[i + 1]),
                    )
                    && let (Ok(min), Ok(max)) = (min_s.parse::<f64>(), max_s.parse::<f64>())
                {
                    filter_by_value = Some((min, max));
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    MrangeParams {
        from,
        to,
        filters,
        count,
        aggregation,
        label_mode,
        filter_by_ts,
        filter_by_value,
    }
}

fn parse_range_bound_or_default(arg: &[u8], default: i64) -> i64 {
    match std::str::from_utf8(arg) {
        Ok("-") => i64::MIN,
        Ok("+") => i64::MAX,
        Ok(s) => s.parse().unwrap_or(default),
        Err(_) => default,
    }
}

fn build_labels_response(ts: &TimeSeriesValue, mode: &LabelMode) -> Response {
    match mode {
        LabelMode::None => Response::Array(vec![]),
        LabelMode::WithLabels => {
            let labels: Vec<Response> = ts
                .labels()
                .iter()
                .map(|(k, v)| {
                    Response::Array(vec![
                        Response::bulk(Bytes::from(k.clone())),
                        Response::bulk(Bytes::from(v.clone())),
                    ])
                })
                .collect();
            Response::Array(labels)
        }
        LabelMode::SelectedLabels(selected) => {
            let labels: Vec<Response> = selected
                .iter()
                .map(|name| {
                    let value = ts.get_label(name).unwrap_or("");
                    Response::Array(vec![
                        Response::bulk(Bytes::from(name.clone())),
                        Response::bulk(Bytes::from(value.to_string())),
                    ])
                })
                .collect();
            Response::Array(labels)
        }
    }
}

fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.0}", f)
    } else {
        format!("{}", f)
    }
}
