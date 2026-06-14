//! Scatter-gather command handlers.
//!
//! This module handles commands that need to be executed across
//! multiple shards and have their results merged:
//! - SCAN - Scan keys with cursor
//! - KEYS - Find keys matching pattern
//! - DBSIZE - Count total keys
//! - RANDOMKEY - Get a random key
//! - FLUSHDB/FLUSHALL - Flush databases

use bytes::Bytes;
use frogdb_core::{KeyType, PartialResult, ScatterOp, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::{ConnectionHandler, next_txid};
use crate::scatter::{AllOk, ScatterGather, SortedUnion, SumIntegers};

impl ConnectionHandler {
    /// Bind a lock-free broadcast coordinator over this connection's shard
    /// senders, using the default scatter-gather timeout. Every broadcast
    /// command states only its per-shard message and its merge; the fan-out,
    /// the single shared deadline, and the send-failure / drop / timeout error
    /// mapping live in [`ScatterGather::run`].
    pub(crate) fn scatter_gather(&self) -> ScatterGather<'_> {
        ScatterGather::new(
            self.core.shard_senders.as_slice(),
            self.scatter_gather_timeout,
            self.state.id,
        )
    }

    /// Bind a broadcast coordinator with a per-command timeout (e.g. FT.SEARCH's
    /// `TIMEOUT` override, already clamped to the scatter-gather timeout).
    pub(crate) fn scatter_gather_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> ScatterGather<'_> {
        ScatterGather::new(self.core.shard_senders.as_slice(), timeout, self.state.id)
    }

    /// Handle SCAN command - scan keys across all shards with cursor.
    pub(crate) async fn handle_scan(&self, args: &[Bytes]) -> Response {
        use frogdb_commands::scan::cursor;

        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'scan' command");
        }

        // Parse cursor
        let cursor_str = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR invalid cursor"),
        };
        let encoded_cursor: u64 = match cursor_str.parse() {
            Ok(c) => c,
            Err(_) => return Response::error("ERR invalid cursor"),
        };

        // Decode cursor to get shard_id and position
        let (shard_id, position) = cursor::decode(encoded_cursor);

        // Parse optional arguments
        let mut pattern: Option<Bytes> = None;
        let mut count: usize = 10;
        let mut key_type: Option<KeyType> = None;

        let mut i = 1;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"MATCH" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    pattern = Some(args[i].clone());
                }
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    count = match std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(c) => c,
                        None => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    };
                }
                b"TYPE" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let type_str = args[i].to_ascii_lowercase();
                    key_type = match type_str.as_slice() {
                        b"string" => Some(KeyType::String),
                        b"list" => Some(KeyType::List),
                        b"set" => Some(KeyType::Set),
                        b"zset" => Some(KeyType::SortedSet),
                        b"hash" => Some(KeyType::Hash),
                        b"stream" => Some(KeyType::Stream),
                        _ => {
                            return Response::error(format!(
                                "ERR unknown type: {}",
                                String::from_utf8_lossy(&type_str)
                            ));
                        }
                    };
                }
                _ => return Response::error("ERR syntax error"),
            }
            i += 1;
        }

        // If shard_id is beyond our shards, we're done
        if shard_id as usize >= self.num_shards {
            return Response::Array(vec![
                Response::bulk(Bytes::from_static(b"0")),
                Response::Array(vec![]),
            ]);
        }

        // Iterate through shards, collecting keys
        let mut all_keys = Vec::new();
        let mut next_shard = shard_id as usize;
        let mut next_position = position;

        while all_keys.len() < count && next_shard < self.num_shards {
            // Send scan request to current shard
            let (response_tx, response_rx) = oneshot::channel();
            let remaining = count - all_keys.len();

            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::Scan {
                    cursor: next_position,
                    count: remaining,
                    pattern: pattern.clone(),
                    key_type,
                },
                conn_id: self.state.id,
                response_tx,
            };

            // SCAN keeps its bespoke cursor walk but borrows the shared
            // per-shard send/timeout helper for each step.
            let partial = match self
                .scatter_gather()
                .query_one(next_shard, msg, response_rx)
                .await
            {
                Ok(partial) => partial,
                Err(resp) => return resp,
            };

            // Extract cursor and keys from response
            let mut shard_next_cursor = 0u64;
            for (key, response) in partial.results {
                if key.as_ref() == b"__cursor__" {
                    if let Response::Integer(c) = response {
                        shard_next_cursor = c as u64;
                    }
                } else {
                    all_keys.push(key);
                }
            }

            if shard_next_cursor == 0 {
                // Shard exhausted, move to next shard
                next_shard += 1;
                next_position = 0;
            } else {
                // More keys in this shard
                next_position = shard_next_cursor;
                break; // We have a valid cursor, stop
            }
        }

        // Encode next cursor
        let final_cursor = if next_shard >= self.num_shards {
            0 // Done
        } else {
            cursor::encode(next_shard as u16, next_position)
        };

        // Build response
        let key_responses: Vec<Response> = all_keys.into_iter().map(Response::bulk).collect();

        Response::Array(vec![
            Response::bulk(Bytes::from(final_cursor.to_string())),
            Response::Array(key_responses),
        ])
    }

    /// Handle KEYS command - scatter-gather across all shards.
    pub(crate) async fn handle_keys(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'keys' command");
        }

        let pattern = args[0].clone();

        self.scatter_gather()
            .run(Box::new(SortedUnion::default()), |_shard, response_tx| {
                ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::Keys {
                        pattern: pattern.clone(),
                    },
                    conn_id: self.state.id,
                    response_tx,
                }
            })
            .await
    }

    /// Handle DBSIZE command - sum key counts from all shards.
    pub(crate) async fn handle_dbsize(&self) -> Response {
        self.scatter_gather()
            .run(
                Box::<SumIntegers<PartialResult>>::default(),
                |_shard, response_tx| ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::DbSize,
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Handle RANDOMKEY command - return a random key using weighted shard selection.
    pub(crate) async fn handle_randomkey(&self) -> Response {
        use rand::RngExt;

        // Phase 1 (select): tally per-shard key counts via the shared per-shard
        // send/timeout helper, weighting the shard selection below.
        let mut shard_counts: Vec<(usize, i64)> = Vec::with_capacity(self.num_shards);
        let mut total_keys: i64 = 0;
        for shard_id in 0..self.num_shards {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::DbSize,
                conn_id: self.state.id,
                response_tx,
            };
            let partial = match self
                .scatter_gather()
                .query_one(shard_id, msg, response_rx)
                .await
            {
                Ok(partial) => partial,
                Err(resp) => return resp,
            };
            for (_, response) in partial.results {
                if let Response::Integer(count) = response {
                    shard_counts.push((shard_id, count));
                    total_keys += count;
                }
            }
        }

        // If database is empty, return nil
        if total_keys == 0 {
            return Response::null();
        }

        // Phase 2: Select shard probabilistically (weighted by key count)
        let selected_shard = {
            let mut rng = rand::rng();
            let selection = rng.random_range(0..total_keys);
            let mut cumulative: i64 = 0;
            let mut selected: usize = 0;

            for (shard_id, count) in &shard_counts {
                cumulative += count;
                if selection < cumulative {
                    selected = *shard_id;
                    break;
                }
            }
            selected
        };

        // Phase 3 (fetch): request a random key from the selected shard.
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::RandomKey,
            conn_id: self.state.id,
            response_tx,
        };
        match self
            .scatter_gather()
            .query_one(selected_shard, msg, response_rx)
            .await
        {
            // Return the random key (or null if the shard is now empty).
            Ok(partial) => partial
                .results
                .into_iter()
                .next()
                .map(|(_, response)| response)
                .unwrap_or_else(Response::null),
            Err(resp) => resp,
        }
    }

    /// Handle FLUSHDB command - clear all shards.
    pub(crate) async fn handle_flushdb(&self, args: &[Bytes]) -> Response {
        // Parse optional ASYNC/SYNC argument (we only support SYNC for now)
        if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            if mode.as_slice() != b"ASYNC" && mode.as_slice() != b"SYNC" {
                return Response::error("ERR syntax error");
            }
        }

        self.scatter_gather()
            .run(
                Box::<AllOk<PartialResult>>::default(),
                |_shard, response_tx| ShardMessage::ScatterRequest {
                    request_id: next_txid(),
                    keys: vec![],
                    operation: ScatterOp::FlushDb,
                    conn_id: self.state.id,
                    response_tx,
                },
            )
            .await
    }

    /// Handle FLUSHALL command - same as FLUSHDB (single database).
    pub(crate) async fn handle_flushall(&self, args: &[Bytes]) -> Response {
        self.handle_flushdb(args).await
    }

    /// Handle INFO command - gather info from all shards.
    pub(crate) async fn handle_info(&self, args: &[Bytes]) -> Response {
        // Execute on local shard (INFO mostly returns static data or aggregate stats)
        let cmd = std::sync::Arc::new(frogdb_protocol::ParsedCommand {
            name: Bytes::from_static(b"INFO"),
            args: args.to_vec(),
        });
        let mut response = self.execute_on_shard(self.shard_id, cmd).await;

        // Gather per-shard stats and aggregate evicted/expired/lazyfreed keys.
        let shard_stats = self.gather_memory_stats().await;
        let evicted: u64 = shard_stats.iter().map(|s| s.evicted_keys).sum();
        let expired: u64 = shard_stats.iter().map(|s| s.expired_keys).sum();
        let lazyfreed: u64 = shard_stats.iter().map(|s| s.lazyfreed_objects).sum();

        // Patch the Clients section and stats with live data.
        if let Response::Bulk(Some(ref bytes)) = response {
            let s = String::from_utf8_lossy(bytes);
            let blocked = self.admin.client_registry.blocked_client_count();
            let connected = self.admin.client_registry.client_count();
            let mut patched = s
                .replace(
                    "blocked_clients:0\r\n",
                    &format!("blocked_clients:{blocked}\r\n"),
                )
                .replace(
                    "connected_clients:1\r\n",
                    &format!("connected_clients:{connected}\r\n"),
                )
                .replace("evicted_keys:0\r\n", &format!("evicted_keys:{evicted}\r\n"))
                .replace("expired_keys:0\r\n", &format!("expired_keys:{expired}\r\n"))
                .replace(
                    "lazyfreed_objects:0\r\n",
                    &format!("lazyfreed_objects:{lazyfreed}\r\n"),
                )
                .replace(
                    "maxclients:10000\r\n",
                    &format!("maxclients:{}\r\n", self.admin.config_manager.max_clients()),
                );

            // Append rate limit stats section
            let rl_registry = self.core.acl_manager.rate_limit_registry();
            let rl_users = rl_registry.user_count();
            let rl_cmds_rejected = rl_registry.total_commands_rejected();
            let rl_bytes_rejected = rl_registry.total_bytes_rejected();
            if rl_users > 0 || rl_cmds_rejected > 0 || rl_bytes_rejected > 0 {
                patched.push_str("\r\n# Ratelimit\r\n");
                patched.push_str(&format!("ratelimit_users_configured:{rl_users}\r\n"));
                patched.push_str(&format!(
                    "ratelimit_total_commands_rejected:{rl_cmds_rejected}\r\n"
                ));
                patched.push_str(&format!(
                    "ratelimit_total_bytes_rejected:{rl_bytes_rejected}\r\n"
                ));
            }

            // Patch error stats in the Stats section
            {
                use std::sync::atomic::Ordering;
                let error_stats = &self.admin.client_registry.error_stats;
                let total_errors = error_stats.total_error_replies.load(Ordering::Relaxed);
                let rejected = error_stats.rejected_calls.load(Ordering::Relaxed);
                let failed = error_stats.failed_calls.load(Ordering::Relaxed);
                patched = patched
                    .replace(
                        "total_error_replies:0\r\n",
                        &format!("total_error_replies:{total_errors}\r\n"),
                    )
                    .replace(
                        "rejected_calls:0\r\n",
                        &format!("rejected_calls:{rejected}\r\n"),
                    )
                    .replace("failed_calls:0\r\n", &format!("failed_calls:{failed}\r\n"));
            }

            // Patch keyspace hit/miss stats in the Stats section. These read
            // from the metrics recorder — the same cumulative counters
            // (`frogdb_keyspace_{hits,misses}_total`) Prometheus scrapes,
            // which shard workers increment via `record_keyspace_lookups`.
            // When metrics are disabled the recorder cannot read them back, so
            // the placeholder 0 is left in place (consistent with Prometheus,
            // which reports nothing in that mode).
            {
                use frogdb_telemetry::metric_names;
                let recorder = &self.observability.metrics_recorder;
                if let Some(hits) = recorder.counter_value(metric_names::KEYSPACE_HITS) {
                    patched = patched
                        .replace("keyspace_hits:0\r\n", &format!("keyspace_hits:{hits}\r\n"));
                }
                if let Some(misses) = recorder.counter_value(metric_names::KEYSPACE_MISSES) {
                    patched = patched.replace(
                        "keyspace_misses:0\r\n",
                        &format!("keyspace_misses:{misses}\r\n"),
                    );
                }
            }

            // Patch master_replid with the real replication id exchanged in
            // PSYNC/FULLRESYNC. The shard-local INFO builder only knows the node
            // id and emits it (or the zero id) as a placeholder; the live
            // replication identity lives in the role's ReplicationState. Both
            // roles report their current replication id (a replica reports the
            // primary's id it is replicating from), and it persists across
            // restarts with the state file. Standalone and pure cluster mode
            // have no PSYNC replication identity, so the node-id placeholder is
            // left untouched. Note `master_replid:` does not match the distinct
            // `master_replid2:` line.
            if let Some(state) = &self.cluster.replication_state {
                let replid = state.read().await.replication_id.clone();
                if !replid.is_empty()
                    && let Some(start) = patched.find("master_replid:")
                    && let Some(rel_end) = patched[start..].find("\r\n")
                {
                    let end = start + rel_end;
                    patched.replace_range(start..end, &format!("master_replid:{replid}"));
                }
            }

            // Patch the Commandstats section with real per-command counts
            // from the client registry. The shard-local stub emits only the
            // section header + blank line; we replace that range with one
            // `cmdstat_<name>:calls=N,...` line per command that has been
            // called at least once.
            //
            // Also merge in this connection's un-synced local stats so the
            // currently-executing INFO and any recent commands appear
            // immediately without waiting for the periodic sync threshold.
            if let Some(cs_start) = patched.find("# Commandstats\r\n") {
                // Section ends at the next blank line (\r\n\r\n) after the header,
                // or at end of string.
                let after_header = cs_start + "# Commandstats\r\n".len();
                let section_end = patched[after_header..]
                    .find("\r\n\r\n")
                    .map(|off| after_header + off + "\r\n\r\n".len())
                    .unwrap_or(patched.len());

                // Combine global stats with this connection's pending local stats.
                let mut stats_map = self.admin.client_registry.command_stats_snapshot();
                for (cmd, usec) in &self.state.local_stats.command_latencies {
                    let entry = stats_map.entry(cmd.to_ascii_lowercase()).or_default();
                    entry.calls += 1;
                    entry.usec += usec;
                }

                let mut sorted: Vec<(String, frogdb_core::ServerCommandStats)> =
                    stats_map.into_iter().collect();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));

                let mut section = String::from("# Commandstats\r\n");
                for (cmd, stats) in sorted {
                    let usec_per_call = if stats.calls > 0 {
                        stats.usec as f64 / stats.calls as f64
                    } else {
                        0.0
                    };
                    section.push_str(&format!(
                        "cmdstat_{cmd}:calls={},usec={},usec_per_call={usec_per_call:.2},rejected_calls={},failed_calls={}\r\n",
                        stats.calls, stats.usec, stats.rejected_calls, stats.failed_calls,
                    ));
                }
                section.push_str("\r\n");

                patched.replace_range(cs_start..section_end, &section);
            }

            // Patch the Errorstats section with real per-prefix error counts
            if let Some(es_start) = patched.find("# Errorstats\r\n") {
                let after_header = es_start + "# Errorstats\r\n".len();
                let section_end = patched[after_header..]
                    .find("\r\n\r\n")
                    .map(|off| after_header + off + "\r\n\r\n".len())
                    .unwrap_or(patched.len());

                let error_types = self.admin.client_registry.error_stats.error_type_snapshot();
                let mut sorted: Vec<(String, u64)> = error_types.into_iter().collect();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));

                let mut section = String::from("# Errorstats\r\n");
                for (prefix, count) in sorted {
                    section.push_str(&format!("errorstat_{prefix}:count={count}\r\n"));
                }
                section.push_str("\r\n");

                patched.replace_range(es_start..section_end, &section);
            }

            // Patch the Keysizes section with merged data from all shards
            if let Some(ks_start) = patched.find("# Keysizes\r\n") {
                let after_header = ks_start + "# Keysizes\r\n".len();
                let section_end = patched[after_header..]
                    .find("\r\n\r\n")
                    .map(|off| after_header + off + "\r\n\r\n".len())
                    .unwrap_or(patched.len());

                // Gather keysizes from all shards
                let mut merged = frogdb_core::KeysizeHistograms::new();
                for sender in self.core.shard_senders.iter() {
                    let (response_tx, response_rx) = oneshot::channel();
                    if sender
                        .send(frogdb_core::ShardMessage::KeysizesSnapshot { response_tx })
                        .await
                        .is_ok()
                        && let Ok(Some(snap)) = response_rx.await
                    {
                        merged.merge(&snap);
                    }
                }

                let mut section = String::from("# Keysizes\r\n");
                for ty in frogdb_core::KeysizeType::ALL {
                    let hist = merged.get(*ty);
                    if !hist.is_empty() {
                        section.push_str(&format!(
                            "{}:{}\r\n",
                            ty.info_field_name(),
                            hist.format_bins()
                        ));
                    }
                }
                if self.admin.config_manager.key_memory_histograms_enabled()
                    && !merged.key_memory.is_empty()
                {
                    section.push_str(&format!(
                        "distrib_key_sizes:{}\r\n",
                        merged.key_memory.format_bins()
                    ));
                }
                section.push_str("\r\n");

                patched.replace_range(ks_start..section_end, &section);
            }

            // Patch the Latencystats section with real histogram data
            if let Some(ls_start) = patched.find("# Latencystats\r\n") {
                let after_header = ls_start + "# Latencystats\r\n".len();
                let section_end = patched[after_header..]
                    .find("\r\n\r\n")
                    .map(|off| after_header + off + "\r\n\r\n".len())
                    .unwrap_or(patched.len());

                let histograms = &self.observability.latency_histograms;
                let percentiles_config = self.admin.config_manager.latency_tracking_percentiles();

                let mut section = String::from("# Latencystats\r\n");
                if histograms.is_enabled() && !percentiles_config.is_empty() {
                    let mut cmds = histograms.all_commands();
                    cmds.sort();

                    for cmd in cmds {
                        if let Some(pvals) = histograms.percentiles_for(&cmd, &percentiles_config) {
                            section.push_str(&format!("latencystats_{cmd}:"));
                            let parts: Vec<String> = pvals
                                .iter()
                                .map(|(p, us)| {
                                    // Convert microseconds to milliseconds with 3 decimal places
                                    let ms = us / 1000.0;
                                    format!("p{}={:.3}", format_percentile(*p), ms)
                                })
                                .collect();
                            section.push_str(&parts.join(","));
                            section.push_str("\r\n");
                        }
                    }
                }
                section.push_str("\r\n");

                patched.replace_range(ls_start..section_end, &section);
            }

            response = Response::bulk(Bytes::from(patched));
        }
        response
    }
}

/// Format a percentile value for display (e.g., 99.9 -> "99.9", 50.0 -> "50").
fn format_percentile(p: f64) -> String {
    if p == p.floor() {
        format!("{}", p as u64)
    } else {
        format!("{}", p)
    }
}
