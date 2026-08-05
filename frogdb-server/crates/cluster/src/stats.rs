//! Cluster-bus packet counters behind `CLUSTER INFO`'s
//! `cluster_stats_messages_sent` / `cluster_stats_messages_received`.
//!
//! One counter pair per node, shared by both directions of the bus: the client
//! side ([`crate::network::ClusterNetwork`], which writes requests and reads
//! responses) and the server side (the cluster-bus loop, which reads requests
//! and writes responses). Every frame that actually crosses the wire is counted
//! exactly once, at the seam that moved it — a request that fails to serialize
//! or a connection that never opens has sent nothing and counts nothing.
//!
//! FrogDB has no gossip protocol, so there is no per-message-type breakdown to
//! report: `ping`/`pong` are not merely uncounted, they do not exist. Redis
//! omits a per-type line whose counter is zero, so omitting them entirely is
//! parity rather than divergence (see FM-CLUSTER-077).

use std::sync::atomic::{AtomicU64, Ordering};

/// Live cluster-bus packet counters for this node.
#[derive(Debug, Default)]
pub struct ClusterBusStats {
    sent: AtomicU64,
    received: AtomicU64,
}

impl ClusterBusStats {
    /// A fresh, zeroed counter pair.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one frame written to the bus.
    pub fn record_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one frame read from the bus.
    pub fn record_received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    /// Read both counters.
    ///
    /// The two loads are not atomic with respect to each other; the pair is a
    /// monotonic counter sample for an operator, not a consistent snapshot.
    pub fn snapshot(&self) -> ClusterBusStatsSnapshot {
        ClusterBusStatsSnapshot {
            messages_sent: self.sent.load(Ordering::Relaxed),
            messages_received: self.received.load(Ordering::Relaxed),
        }
    }
}

/// A sample of [`ClusterBusStats`], as reported by `CLUSTER INFO`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClusterBusStatsSnapshot {
    /// Frames this node wrote to the cluster bus (RPC requests it issued plus
    /// RPC responses it served).
    pub messages_sent: u64,
    /// Frames this node read from the cluster bus (RPC responses it awaited
    /// plus RPC requests peers issued to it).
    pub messages_received: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // FM-CLUSTER-077
    #[test]
    fn a_fresh_counter_pair_reads_zero() {
        assert_eq!(
            ClusterBusStats::new().snapshot(),
            ClusterBusStatsSnapshot {
                messages_sent: 0,
                messages_received: 0
            }
        );
    }

    /// The two directions never contaminate each other: a node that only ever
    /// answers must not report having sent requests, and vice versa.
    // FM-CLUSTER-077
    #[test]
    fn the_two_directions_are_counted_independently() {
        let stats = ClusterBusStats::new();
        stats.record_sent();
        stats.record_sent();
        stats.record_received();

        assert_eq!(
            stats.snapshot(),
            ClusterBusStatsSnapshot {
                messages_sent: 2,
                messages_received: 1
            }
        );
    }

    // FM-CLUSTER-077
    #[test]
    fn counters_accumulate_across_threads() {
        let stats = std::sync::Arc::new(ClusterBusStats::new());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let stats = stats.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        stats.record_sent();
                        stats.record_received();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            stats.snapshot(),
            ClusterBusStatsSnapshot {
                messages_sent: 400,
                messages_received: 400
            }
        );
    }
}
