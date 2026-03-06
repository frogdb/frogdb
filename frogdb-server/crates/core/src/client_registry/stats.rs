//! Per-client connection statistics.

use std::collections::HashMap;

/// Maximum number of latency samples to keep for p99 calculation.
pub(super) const LATENCY_SAMPLE_SIZE: usize = 100;

/// Maximum number of distinct command types to track per client.
pub(super) const MAX_COMMAND_TYPES: usize = 50;

/// Per-client connection statistics.
#[derive(Debug, Clone, Default)]
pub struct ClientStats {
    /// Total commands processed by this client.
    pub commands_total: u64,
    /// Total latency accumulated in microseconds.
    pub latency_total_us: u64,
    /// Maximum single command latency in microseconds.
    pub latency_max_us: u64,
    /// Circular buffer of recent latencies for p99 calculation (~100 samples).
    pub latency_samples: Vec<u64>,
    /// Index into latency_samples for circular buffer.
    pub latency_sample_idx: usize,
    /// Total bytes received from this client.
    pub bytes_recv: u64,
    /// Total bytes sent to this client.
    pub bytes_sent: u64,
    /// Per-command type breakdown (limited to 50 entries).
    pub command_counts: HashMap<String, CommandTypeStats>,
}

impl ClientStats {
    /// Calculate approximate p99 latency from samples.
    pub fn p99_latency_us(&self) -> u64 {
        if self.latency_samples.is_empty() {
            return 0;
        }

        let mut sorted = self.latency_samples.clone();
        sorted.sort_unstable();

        // p99 index
        let idx = ((sorted.len() as f64 * 0.99).ceil() as usize).saturating_sub(1);
        sorted.get(idx).copied().unwrap_or(0)
    }

    /// Calculate average latency in microseconds.
    pub fn avg_latency_us(&self) -> u64 {
        if self.commands_total == 0 {
            0
        } else {
            self.latency_total_us / self.commands_total
        }
    }

    /// Record a latency sample (circular buffer).
    pub fn record_latency_sample(&mut self, latency_us: u64) {
        if self.latency_samples.len() < LATENCY_SAMPLE_SIZE {
            self.latency_samples.push(latency_us);
        } else {
            self.latency_samples[self.latency_sample_idx] = latency_us;
            self.latency_sample_idx = (self.latency_sample_idx + 1) % LATENCY_SAMPLE_SIZE;
        }
    }

    /// Record a command execution with latency.
    pub fn record_command(&mut self, cmd_name: &str, latency_us: u64) {
        self.commands_total += 1;
        self.latency_total_us += latency_us;
        if latency_us > self.latency_max_us {
            self.latency_max_us = latency_us;
        }
        self.record_latency_sample(latency_us);

        // Update per-command stats (with LRU eviction if at limit)
        if let Some(cmd_stats) = self.command_counts.get_mut(cmd_name) {
            cmd_stats.count += 1;
            cmd_stats.latency_total_us += latency_us;
            if latency_us > cmd_stats.latency_max_us {
                cmd_stats.latency_max_us = latency_us;
            }
        } else {
            // Need to insert new command type
            if self.command_counts.len() >= MAX_COMMAND_TYPES {
                // Evict the entry with the lowest count
                if let Some(min_key) = self
                    .command_counts
                    .iter()
                    .min_by_key(|(_, stats)| stats.count)
                    .map(|(k, _)| k.clone())
                {
                    self.command_counts.remove(&min_key);
                }
            }
            self.command_counts.insert(
                cmd_name.to_string(),
                CommandTypeStats {
                    count: 1,
                    latency_total_us: latency_us,
                    latency_max_us: latency_us,
                },
            );
        }
    }

    /// Merge delta stats into this ClientStats.
    pub fn merge_delta(&mut self, delta: &ClientStatsDelta) {
        self.commands_total += delta.commands_processed;
        self.latency_total_us += delta.total_latency_us;
        self.bytes_recv += delta.bytes_recv;
        self.bytes_sent += delta.bytes_sent;

        // Merge per-command latencies and update max
        for (cmd_name, latency_us) in &delta.command_latencies {
            if *latency_us > self.latency_max_us {
                self.latency_max_us = *latency_us;
            }
            self.record_latency_sample(*latency_us);

            // Update per-command stats
            if let Some(cmd_stats) = self.command_counts.get_mut(cmd_name) {
                cmd_stats.count += 1;
                cmd_stats.latency_total_us += latency_us;
                if *latency_us > cmd_stats.latency_max_us {
                    cmd_stats.latency_max_us = *latency_us;
                }
            } else {
                // Insert new command type with LRU eviction
                if self.command_counts.len() >= MAX_COMMAND_TYPES
                    && let Some(min_key) = self
                        .command_counts
                        .iter()
                        .min_by_key(|(_, stats)| stats.count)
                        .map(|(k, _)| k.clone())
                {
                    self.command_counts.remove(&min_key);
                }
                self.command_counts.insert(
                    cmd_name.clone(),
                    CommandTypeStats {
                        count: 1,
                        latency_total_us: *latency_us,
                        latency_max_us: *latency_us,
                    },
                );
            }
        }
    }
}

/// Statistics for a specific command type.
#[derive(Debug, Clone, Default)]
pub struct CommandTypeStats {
    pub count: u64,
    pub latency_total_us: u64,
    pub latency_max_us: u64,
}

impl CommandTypeStats {
    /// Calculate average latency in microseconds.
    pub fn avg_latency_us(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.latency_total_us / self.count
        }
    }
}

/// Delta for batched stats updates from connection handlers.
#[derive(Debug, Clone, Default)]
pub struct ClientStatsDelta {
    /// Number of commands processed since last sync.
    pub commands_processed: u64,
    /// Total latency accumulated since last sync (microseconds).
    pub total_latency_us: u64,
    /// Bytes received since last sync.
    pub bytes_recv: u64,
    /// Bytes sent since last sync.
    pub bytes_sent: u64,
    /// Per-command latencies: (command_name, latency_us).
    pub command_latencies: Vec<(String, u64)>,
}

impl ClientStatsDelta {
    /// Check if this delta has any data to sync.
    pub fn has_data(&self) -> bool {
        self.commands_processed > 0 || self.bytes_recv > 0 || self.bytes_sent > 0
    }

    /// Clear the delta after syncing.
    pub fn clear(&mut self) {
        self.commands_processed = 0;
        self.total_latency_us = 0;
        self.bytes_recv = 0;
        self.bytes_sent = 0;
        self.command_latencies.clear();
    }
}
