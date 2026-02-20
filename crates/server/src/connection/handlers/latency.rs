//! LATENCY command handlers.
//!
//! This module handles LATENCY subcommands:
//! - LATENCY BANDS - Show/reset SLO latency band statistics
//! - LATENCY DOCTOR - Diagnose latency issues
//! - LATENCY GRAPH - Show ASCII latency graph
//! - LATENCY HELP - Show help text
//! - LATENCY HISTOGRAM - Show command latency histogram
//! - LATENCY HISTORY - Get historical latency data
//! - LATENCY LATEST - Get latest latency samples
//! - LATENCY RESET - Clear latency data
//!
//! These handlers are implemented as extension methods on `ConnectionHandler`.

use bytes::Bytes;
use frogdb_core::{generate_latency_graph, LatencyEvent, LatencySample, ShardMessage};
use frogdb_protocol::Response;
use std::collections::HashMap;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle LATENCY command and dispatch to subcommands.
    pub(crate) async fn handle_latency_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'latency' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "BANDS" => self.handle_latency_bands(&args[1..]),
            "DOCTOR" => self.handle_latency_doctor().await,
            "GRAPH" => self.handle_latency_graph(&args[1..]).await,
            "HELP" => self.handle_latency_help(),
            "HISTOGRAM" => self.handle_latency_histogram(&args[1..]).await,
            "HISTORY" => self.handle_latency_history(&args[1..]).await,
            "LATEST" => self.handle_latency_latest().await,
            "RESET" => self.handle_latency_reset(&args[1..]).await,
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try LATENCY HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle LATENCY BANDS [RESET] - show or reset latency band statistics.
    fn handle_latency_bands(&self, args: &[Bytes]) -> Response {
        let Some(tracker) = &self.band_tracker else {
            return Response::error(
                "ERR latency bands not enabled. Set latency_bands.enabled = true in config.",
            );
        };

        // Handle RESET subcommand
        if !args.is_empty() {
            let subcommand = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
            if subcommand == "RESET" {
                tracker.reset();
                return Response::ok();
            } else {
                return Response::error(format!(
                    "ERR unknown option '{}'. Try LATENCY BANDS or LATENCY BANDS RESET.",
                    subcommand
                ));
            }
        }

        // Build report showing band percentages
        let total = tracker.total();
        let percentages = tracker.get_percentages();

        let mut lines = vec![
            format!("Total requests: {}", total),
            String::new(),
            "Band            Count      Percentage".to_string(),
            "----            -----      ----------".to_string(),
        ];

        for (band, count, pct) in &percentages {
            lines.push(format!("{:<15} {:>10} {:>10.2}%", band, count, pct));
        }

        Response::bulk(Bytes::from(lines.join("\r\n")))
    }

    /// Handle LATENCY DOCTOR - diagnose latency issues.
    async fn handle_latency_doctor(&self) -> Response {
        let latest = self.gather_latency_latest().await;

        let mut report = Vec::new();
        report.push("I have a few latency reports to share:".to_string());

        if latest.is_empty() {
            report.push("* No latency events recorded yet.".to_string());
        } else {
            for (event, sample) in &latest {
                if sample.latency_ms > 100 {
                    report.push(format!(
                        "* {} event at {} had HIGH latency of {}ms",
                        event.as_str(),
                        sample.timestamp,
                        sample.latency_ms
                    ));
                } else if sample.latency_ms > 10 {
                    report.push(format!(
                        "* {} event at {} had moderate latency of {}ms",
                        event.as_str(),
                        sample.timestamp,
                        sample.latency_ms
                    ));
                }
            }

            if report.len() == 1 {
                report.push("* All recorded events have acceptable latency.".to_string());
            }
        }

        Response::bulk(Bytes::from(report.join("\n")))
    }

    /// Handle LATENCY GRAPH <event> - show ASCII latency graph.
    async fn handle_latency_graph(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'latency|graph' command");
        }

        let event_str = String::from_utf8_lossy(&args[0]);
        let event = match LatencyEvent::from_str(&event_str) {
            Some(e) => e,
            None => {
                return Response::error(format!(
                    "ERR Unknown event type: {}. Valid events: command, fork, aof-fsync, expire-cycle, eviction-cycle, snapshot-io",
                    event_str
                ));
            }
        };

        let history = self.gather_latency_history(event).await;
        let graph = generate_latency_graph(event, &history);

        Response::bulk(Bytes::from(graph))
    }

    /// Handle LATENCY HELP - show help text.
    fn handle_latency_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"LATENCY <subcommand> [<arg> ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"BANDS [RESET]")),
            Response::bulk(Bytes::from_static(
                b"    Show SLO latency band statistics, or reset counters.",
            )),
            Response::bulk(Bytes::from_static(b"DOCTOR")),
            Response::bulk(Bytes::from_static(b"    Return latency diagnostic report.")),
            Response::bulk(Bytes::from_static(b"GRAPH <event>")),
            Response::bulk(Bytes::from_static(
                b"    Return an ASCII art graph of latency for the event.",
            )),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
            Response::bulk(Bytes::from_static(b"HISTOGRAM [<command> ...]")),
            Response::bulk(Bytes::from_static(
                b"    Return a cumulative distribution of command latencies.",
            )),
            Response::bulk(Bytes::from_static(b"HISTORY <event>")),
            Response::bulk(Bytes::from_static(
                b"    Return timestamp-latency pairs for the event.",
            )),
            Response::bulk(Bytes::from_static(b"LATEST")),
            Response::bulk(Bytes::from_static(
                b"    Return the latest latency samples for all events.",
            )),
            Response::bulk(Bytes::from_static(b"RESET [<event> ...]")),
            Response::bulk(Bytes::from_static(
                b"    Reset latency data for specified events, or all if none given.",
            )),
        ];
        Response::Array(help)
    }

    /// Handle LATENCY HISTOGRAM [command...] - show command latency histogram.
    async fn handle_latency_histogram(&self, _args: &[Bytes]) -> Response {
        // This would require command-level latency tracking which is not yet implemented
        // Return an empty response for now
        Response::Array(vec![])
    }

    /// Handle LATENCY HISTORY <event> - get historical latency data.
    async fn handle_latency_history(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'latency|history' command");
        }

        let event_str = String::from_utf8_lossy(&args[0]);
        let event = match LatencyEvent::from_str(&event_str) {
            Some(e) => e,
            None => {
                return Response::error(format!(
                    "ERR Unknown event type: {}. Valid events: command, fork, aof-fsync, expire-cycle, eviction-cycle, snapshot-io",
                    event_str
                ));
            }
        };

        let history = self.gather_latency_history(event).await;

        // Return as array of [timestamp, latency] pairs
        let entries: Vec<Response> = history
            .into_iter()
            .map(|sample| {
                Response::Array(vec![
                    Response::Integer(sample.timestamp),
                    Response::Integer(sample.latency_ms as i64),
                ])
            })
            .collect();

        Response::Array(entries)
    }

    /// Handle LATENCY LATEST - get latest latency samples.
    async fn handle_latency_latest(&self) -> Response {
        let latest = self.gather_latency_latest().await;

        let entries: Vec<Response> = latest
            .into_iter()
            .map(|(event, sample)| {
                Response::Array(vec![
                    Response::bulk(Bytes::from(event.as_str())),
                    Response::Integer(sample.timestamp),
                    Response::Integer(sample.latency_ms as i64),
                    Response::Integer(sample.latency_ms as i64), // max_latency (same as latest in our impl)
                ])
            })
            .collect();

        Response::Array(entries)
    }

    /// Handle LATENCY RESET [event...] - clear latency data.
    async fn handle_latency_reset(&self, args: &[Bytes]) -> Response {
        // Parse event names
        let events: Vec<LatencyEvent> = args
            .iter()
            .filter_map(|arg| {
                let s = String::from_utf8_lossy(arg);
                LatencyEvent::from_str(&s)
            })
            .collect();

        // Broadcast reset to all shards
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::LatencyReset {
                    events: events.clone(),
                    response_tx,
                })
                .await
                .is_ok()
            {
                let _ = response_rx.await;
            }
        }

        Response::ok()
    }

    /// Gather latest latency samples from all shards.
    async fn gather_latency_latest(&self) -> Vec<(LatencyEvent, LatencySample)> {
        let mut latest_by_event: HashMap<LatencyEvent, LatencySample> = HashMap::new();

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::LatencyLatest { response_tx })
                .await
                .is_ok()
            {
                if let Ok(samples) = response_rx.await {
                    for (event, sample) in samples {
                        // Keep the most recent sample for each event
                        latest_by_event
                            .entry(event)
                            .and_modify(|existing| {
                                if sample.timestamp > existing.timestamp {
                                    *existing = sample;
                                }
                            })
                            .or_insert(sample);
                    }
                }
            }
        }

        latest_by_event.into_iter().collect()
    }

    /// Gather latency history for a specific event from all shards.
    async fn gather_latency_history(&self, event: LatencyEvent) -> Vec<LatencySample> {
        let mut all_samples = Vec::new();

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::LatencyHistory { event, response_tx })
                .await
                .is_ok()
            {
                if let Ok(samples) = response_rx.await {
                    all_samples.extend(samples);
                }
            }
        }

        // Sort by timestamp (newest first)
        all_samples.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        all_samples
    }
}
