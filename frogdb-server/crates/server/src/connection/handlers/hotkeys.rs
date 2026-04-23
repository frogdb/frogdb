//! HOTKEYS command handlers.
//!
//! This module handles HOTKEYS subcommands:
//! - HOTKEYS START METRICS <count> <metric...> [COUNT n] [DURATION ms] [SAMPLE ratio] [SLOTS count slot...]
//! - HOTKEYS STOP - Stop an active session
//! - HOTKEYS RESET - Reset a stopped session to idle
//! - HOTKEYS GET - Retrieve session data
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{
    CLUSTER_SLOTS, HotkeyMetric, HotkeySession, HotkeySessionConfig, HotkeySessionState,
    slot_for_key,
};
use frogdb_protocol::{ParsedCommand, Response};

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle HOTKEYS command and dispatch to subcommands.
    pub(crate) async fn handle_hotkeys_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'hotkeys' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "START" => self.handle_hotkeys_start(&args[1..]),
            "STOP" => self.handle_hotkeys_stop(),
            "RESET" => self.handle_hotkeys_reset(),
            "GET" => self.handle_hotkeys_get(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try HOTKEYS START|STOP|RESET|GET.",
                subcommand_str
            )),
        }
    }

    /// Handle HOTKEYS START - parse parameters and start a new session.
    fn handle_hotkeys_start(&self, args: &[Bytes]) -> Response {
        // Check if a session is already active
        {
            let session = self.observability.hotkey_session.lock().unwrap();
            if let Some(ref s) = *session
                && s.state == HotkeySessionState::Active
            {
                return Response::error("ERR hotkeys session already started");
            }
        }

        let mut config = HotkeySessionConfig::default();
        let mut i = 0;
        let mut metrics_seen = false;
        let mut slots_seen = false;

        while i < args.len() {
            let param = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();

            match param.as_str() {
                "METRICS" => {
                    if metrics_seen {
                        return Response::error("ERR METRICS already specified");
                    }
                    metrics_seen = true;
                    i += 1;

                    // Next arg is the count of metrics
                    if i >= args.len() {
                        return Response::error(
                            "ERR wrong number of arguments for 'hotkeys|start' command",
                        );
                    }

                    let count_str = String::from_utf8_lossy(&args[i]);
                    let metric_count: usize = match count_str.parse() {
                        Ok(n) if (1..=2).contains(&n) => n,
                        Ok(_) => {
                            return Response::error("ERR METRICS count must be between 1 and 2");
                        }
                        Err(_) => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    };
                    i += 1;

                    // Read metric_count metric names
                    if i + metric_count > args.len() {
                        return Response::error(
                            "ERR METRICS count does not match the number of metrics",
                        );
                    }

                    let mut metrics = Vec::new();
                    for _ in 0..metric_count {
                        let metric_str = String::from_utf8_lossy(&args[i]);
                        match HotkeyMetric::from_str(&metric_str) {
                            Some(m) => {
                                if metrics.contains(&m) {
                                    return Response::error(format!(
                                        "ERR METRICS duplicate metric '{}'",
                                        metric_str.to_ascii_lowercase()
                                    ));
                                }
                                metrics.push(m);
                            }
                            None => {
                                return Response::error(format!(
                                    "ERR METRICS invalid metric '{}'. Valid metrics: cpu, net",
                                    metric_str
                                ));
                            }
                        }
                        i += 1;
                    }
                    config.metrics = metrics;
                }
                "COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error(
                            "ERR wrong number of arguments for 'hotkeys|start' command",
                        );
                    }
                    let count_str = String::from_utf8_lossy(&args[i]);
                    match count_str.parse::<i64>() {
                        Ok(n) if (1..=100).contains(&n) => {
                            config.count = n as usize;
                        }
                        Ok(_) => {
                            return Response::error("ERR COUNT must be between 1 and 100");
                        }
                        Err(_) => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    }
                    i += 1;
                }
                "DURATION" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error(
                            "ERR wrong number of arguments for 'hotkeys|start' command",
                        );
                    }
                    let dur_str = String::from_utf8_lossy(&args[i]);
                    match dur_str.parse::<u64>() {
                        Ok(n) => {
                            config.duration_ms = n;
                        }
                        Err(_) => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    }
                    i += 1;
                }
                "SAMPLE" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error(
                            "ERR wrong number of arguments for 'hotkeys|start' command",
                        );
                    }
                    let sample_str = String::from_utf8_lossy(&args[i]);
                    match sample_str.parse::<i64>() {
                        Ok(n) if n >= 1 => {
                            config.sample_ratio = n as u64;
                        }
                        Ok(_) => {
                            return Response::error("ERR SAMPLE ratio must be >= 1");
                        }
                        Err(_) => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    }
                    i += 1;
                }
                "SLOTS" => {
                    // In non-cluster mode, SLOTS is not allowed
                    if !self.is_cluster_mode() {
                        return Response::error(
                            "ERR SLOTS option is not allowed in non-cluster mode",
                        );
                    }
                    if slots_seen {
                        return Response::error("ERR SLOTS already specified");
                    }
                    slots_seen = true;
                    i += 1;

                    if i >= args.len() {
                        return Response::error(
                            "ERR wrong number of arguments for 'hotkeys|start' command",
                        );
                    }

                    let slot_count_str = String::from_utf8_lossy(&args[i]);
                    let slot_count: usize = match slot_count_str.parse() {
                        Ok(n) => n,
                        Err(_) => {
                            return Response::error("ERR value is not an integer or out of range");
                        }
                    };
                    i += 1;

                    if i + slot_count > args.len() {
                        return Response::error(
                            "ERR SLOTS count does not match the number of slots",
                        );
                    }

                    let mut slots = Vec::new();
                    for _ in 0..slot_count {
                        let slot_str = String::from_utf8_lossy(&args[i]);
                        match slot_str.parse::<i64>() {
                            Ok(n) if n < 0 => {
                                return Response::error(format!(
                                    "ERR Invalid slot '{}'. Valid range: 0-16383",
                                    slot_str
                                ));
                            }
                            Ok(n) if n > 16383 => {
                                return Response::error(format!(
                                    "ERR Invalid slot '{}'. Valid range: 0-16383",
                                    slot_str
                                ));
                            }
                            Ok(n) => {
                                let slot = n as u16;
                                if slots.contains(&slot) {
                                    return Response::error(format!(
                                        "ERR SLOTS duplicate slot '{}'",
                                        slot
                                    ));
                                }
                                // Validate slot is handled by this node
                                if !self.node_handles_slot(slot) {
                                    return Response::error(format!(
                                        "ERR Slot {} is not handled by this node",
                                        slot
                                    ));
                                }
                                slots.push(slot);
                            }
                            Err(_) => {
                                return Response::error(format!(
                                    "ERR Invalid slot '{}'. Valid range: 0-16383",
                                    slot_str
                                ));
                            }
                        }
                        i += 1;
                    }
                    config.selected_slots = Some(slots);
                }
                _ => {
                    return Response::error(format!(
                        "ERR unknown option '{}' for 'hotkeys|start'",
                        param
                    ));
                }
            }
        }

        // METRICS is required
        if !metrics_seen {
            return Response::error("ERR METRICS option is required for HOTKEYS START");
        }

        // In cluster mode without explicit SLOTS, use this node's slot ranges
        if self.is_cluster_mode() && !slots_seen {
            config.selected_slots = Some(self.node_slot_list());
        }

        // Start the session
        let session = HotkeySession::new(config);
        *self.observability.hotkey_session.lock().unwrap() = Some(session);

        Response::ok()
    }

    /// Handle HOTKEYS STOP - stop an active session.
    fn handle_hotkeys_stop(&self) -> Response {
        let mut session = self.observability.hotkey_session.lock().unwrap();
        match session.as_mut() {
            Some(s) if s.state == HotkeySessionState::Active => {
                s.stop();
                Response::ok()
            }
            _ => Response::error("ERR no active hotkeys session to stop"),
        }
    }

    /// Handle HOTKEYS RESET - reset a stopped session back to idle.
    fn handle_hotkeys_reset(&self) -> Response {
        let mut session = self.observability.hotkey_session.lock().unwrap();
        match session.as_ref() {
            Some(s) if s.state == HotkeySessionState::Active => {
                Response::error("ERR cannot reset an active hotkeys session. Stop it first")
            }
            _ => {
                *session = None;
                Response::ok()
            }
        }
    }

    /// Handle HOTKEYS GET - retrieve session data.
    fn handle_hotkeys_get(&self) -> Response {
        let session = self.observability.hotkey_session.lock().unwrap();
        let session = match session.as_ref() {
            Some(s) => s,
            None => return Response::Bulk(None), // nil when no session
        };

        let is_resp3 = self.state.protocol_version.is_resp3();

        // Build the metrics list
        let metrics_list: Vec<String> = session
            .config
            .metrics
            .iter()
            .map(|m| m.as_str().to_string())
            .collect();

        // Build the hotkeys array
        let top_keys = session.top_keys();
        let hotkeys_entries: Vec<Response> = top_keys
            .iter()
            .map(|(key, entry)| {
                let key_str = String::from_utf8_lossy(key).to_string();
                let mut fields: Vec<Response> = vec![
                    // key
                    Response::bulk(Bytes::from("key")),
                    Response::bulk(Bytes::from(key_str)),
                    // access-count
                    Response::bulk(Bytes::from("access-count")),
                    Response::Integer(entry.access_count as i64),
                ];

                // Conditional CPU metric
                if session.config.metrics.contains(&HotkeyMetric::Cpu) {
                    fields.push(Response::bulk(Bytes::from("total-cpu-time-us")));
                    fields.push(Response::Integer(entry.total_cpu_us as i64));
                }

                // Conditional NET metric
                if session.config.metrics.contains(&HotkeyMetric::Net) {
                    fields.push(Response::bulk(Bytes::from("total-net-bytes")));
                    fields.push(Response::Integer(entry.total_net_bytes as i64));
                }

                Response::Array(fields)
            })
            .collect();

        // Build the session duration
        let duration_ms = session.started_at.elapsed().as_millis() as i64;

        // Calculate the count
        let count = session.config.count as i64;

        // Build the sample ratio
        let sample_ratio = session.config.sample_ratio as i64;

        // Build selected-slots
        let selected_slots: Vec<Response> = match &session.config.selected_slots {
            Some(slots) => {
                let mut sorted = slots.clone();
                sorted.sort();
                sorted
                    .iter()
                    .map(|&s| Response::Integer(s as i64))
                    .collect()
            }
            None => {
                // In standalone mode, return full range 0-16383
                (0..CLUSTER_SLOTS as i64).map(Response::Integer).collect()
            }
        };

        if is_resp3 {
            // RESP3: Map type
            let mut pairs: Vec<(Response, Response)> = vec![
                (
                    Response::bulk(Bytes::from("metrics")),
                    Response::Array(
                        metrics_list
                            .iter()
                            .map(|m| Response::bulk(Bytes::from(m.clone())))
                            .collect(),
                    ),
                ),
                (
                    Response::bulk(Bytes::from("count")),
                    Response::Integer(count),
                ),
                (
                    Response::bulk(Bytes::from("duration")),
                    Response::Integer(duration_ms),
                ),
            ];

            // sample-ratio is conditional: only include if > 1
            if sample_ratio > 1 {
                pairs.push((
                    Response::bulk(Bytes::from("sample-ratio")),
                    Response::Integer(sample_ratio),
                ));
            }

            // selected-slots is conditional: only include if sample_ratio > 1 or explicit slots
            if sample_ratio > 1 || session.config.selected_slots.is_some() {
                pairs.push((
                    Response::bulk(Bytes::from("selected-slots")),
                    Response::Array(selected_slots),
                ));
            }

            pairs.push((
                Response::bulk(Bytes::from("hotkeys")),
                Response::Array(hotkeys_entries),
            ));

            Response::Map(pairs)
        } else {
            // RESP2: flat array of alternating key/value pairs
            let mut flat: Vec<Response> = vec![
                Response::bulk(Bytes::from("metrics")),
                Response::Array(
                    metrics_list
                        .iter()
                        .map(|m| Response::bulk(Bytes::from(m.clone())))
                        .collect(),
                ),
                Response::bulk(Bytes::from("count")),
                Response::Integer(count),
                Response::bulk(Bytes::from("duration")),
                Response::Integer(duration_ms),
            ];

            // Conditional fields
            if sample_ratio > 1 {
                flat.push(Response::bulk(Bytes::from("sample-ratio")));
                flat.push(Response::Integer(sample_ratio));
            }

            if sample_ratio > 1 || session.config.selected_slots.is_some() {
                flat.push(Response::bulk(Bytes::from("selected-slots")));
                flat.push(Response::Array(selected_slots));
            }

            flat.push(Response::bulk(Bytes::from("hotkeys")));
            flat.push(Response::Array(hotkeys_entries));

            Response::Array(flat)
        }
    }

    /// Check if the server is in cluster mode.
    fn is_cluster_mode(&self) -> bool {
        self.cluster.is_cluster_mode()
    }

    /// Check if this node handles the given slot.
    fn node_handles_slot(&self, slot: u16) -> bool {
        if let Some(ref cluster_state) = self.cluster.cluster_state
            && let Some(node_id) = self.cluster.node_id
        {
            let snapshot = cluster_state.snapshot();
            snapshot
                .get_slot_owner(slot)
                .is_some_and(|owner| owner == node_id)
        } else if self.cluster.cluster_state.is_some() {
            // Cluster mode but no node_id
            false
        } else {
            // Standalone mode - we handle all slots
            true
        }
    }

    /// Get the list of slots handled by this node.
    fn node_slot_list(&self) -> Vec<u16> {
        if let Some(ref cluster_state) = self.cluster.cluster_state
            && let Some(node_id) = self.cluster.node_id
        {
            let slot_ranges = cluster_state.get_node_slots(node_id);
            let mut slots = Vec::new();
            for range in &slot_ranges {
                for slot in range.start..=range.end {
                    slots.push(slot);
                }
            }
            return slots;
        }
        Vec::new()
    }

    /// Record key accesses to the hotkey session if one is active.
    ///
    /// Called after command execution in `process_one_command`. Extracts keys
    /// from the command, checks slot membership, applies sampling, and updates
    /// the per-key counters.
    pub(crate) fn maybe_record_hotkeys(
        &self,
        cmd: &Arc<ParsedCommand>,
        cmd_name: &str,
        elapsed_us: u64,
        cmd_bytes: usize,
    ) {
        // Fast path: try_lock to avoid contention. If another thread holds the
        // mutex we just skip this sample — acceptable for a probabilistic sampler.
        let mut guard = match self.observability.hotkey_session.try_lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        let session = match guard.as_mut() {
            Some(s) if s.is_active() => s,
            _ => return,
        };

        // Check if session has expired by duration
        if session.is_expired() {
            session.stop();
            return;
        }

        // Look up command handler to extract keys
        let handler = match self.core.registry.get(cmd_name) {
            Some(h) => h,
            None => return,
        };

        let keys = handler.keys(&cmd.args);
        if keys.is_empty() {
            return;
        }

        // Record each key
        for key in &keys {
            let slot = slot_for_key(key);

            // Check slot membership
            if !session.slot_matches(slot) {
                continue;
            }

            session.record_access(key, elapsed_us, cmd_bytes as u64);
        }
    }
}
