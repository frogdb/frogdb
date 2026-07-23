//! Cross-shard keyspace-notification routing.
//!
//! A keyspace notification has two halves that must agree on *one* subscription
//! table: the half that **registers** a subscriber (SUBSCRIBE, which always
//! lands on the broadcast coordinator shard, shard 0) and the half that
//! **emits** an event (a write/expire/evict on the key-owner shard). In
//! multi-shard mode they used to disagree: the emit half published into the
//! *emitting* shard's own table, so a keyevent for a key on any shard other than
//! shard 0 reached no subscriber and was silently lost.
//!
//! [`KeyspaceNotificationCoordinator`] owns that emit→subscriber routing
//! decision — the single rule "subscribers live on shard 0, so an event emitted
//! on the key-owner shard must be routed to shard 0" — so registration and
//! emission can no longer disagree.

use std::sync::Arc;

use bytes::Bytes;

use frogdb_types::metrics::definitions::KeyspaceNotificationsDropped;

use crate::noop::MetricsRecorder;
use crate::pubsub::ShardSubscriptions;

use super::message::{PubSubMsg, ShardSender};

/// Owns the one rule that would otherwise be split, unwritten, across
/// `handle_subscribe` (subscribers register on the broadcast coordinator shard,
/// shard 0) and `emit_keyspace_notification` (events fire on the key-owner
/// shard): a keyspace event must be published into the *coordinator shard's*
/// table, not the emitting shard's. Centralizing it here makes registration and
/// emission physically unable to disagree.
pub(crate) struct KeyspaceNotificationCoordinator {
    topology: Topology,
}

/// Where a keyspace event must land relative to the shard that emits it.
enum Topology {
    /// Single-shard / standalone, OR this worker *is* the coordinator shard
    /// (shard 0). The emitting table and the subscriber table are the same, so
    /// publish straight into the local table — the synchronous fast path,
    /// byte-for-byte what the code did before the coordinator existed.
    Local,
    /// A non-coordinator shard in multi-shard mode. Forward the event to shard
    /// 0's mailbox; shard 0's worker publishes it into the table where the
    /// broadcast subscribers actually live.
    Sharded {
        /// Mailbox of the coordinator shard (shard 0).
        coordinator_shard: ShardSender,
        /// Records events dropped when the coordinator mailbox is saturated.
        metrics: Arc<dyn MetricsRecorder>,
        /// This (emitting) shard's label, for the dropped-events metric.
        shard_label: String,
    },
}

impl KeyspaceNotificationCoordinator {
    /// Decide the routing topology once, at worker construction.
    ///
    /// Chooses `Local` when this worker's own subscription table already *is*
    /// the broadcast subscriber table — either standalone (`num_shards <= 1`) or
    /// the coordinator shard itself (`shard_id == 0`), which avoids a pointless
    /// enqueue onto its own mailbox. Otherwise `Sharded`, holding the
    /// coordinator shard's mailbox for the non-blocking cross-shard hand-off.
    /// Both `Local` predicates collapse to the same invariant: *the emitting
    /// table is the subscriber table.*
    pub(crate) fn new(
        shard_id: usize,
        num_shards: usize,
        shard_senders: &Arc<Vec<ShardSender>>,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        let topology = if num_shards <= 1 || shard_id == 0 {
            Topology::Local
        } else {
            Topology::Sharded {
                coordinator_shard: shard_senders[0].clone(),
                metrics,
                shard_label: shard_id.to_string(),
            }
        };
        Self { topology }
    }

    /// Route one already-formatted keyspace/keyevent message to the subscriber
    /// table.
    ///
    /// Called ONLY after the enabled + mask checks in
    /// `emit_keyspace_notification` have passed, so the `< 1ns` disabled fast
    /// path never reaches this function. `local` is the emitting worker's own
    /// table; it is used only on the `Local` branch (it is threaded in rather
    /// than owned because [`ShardSubscriptions`] is a plain by-value field on the
    /// worker, mutated under `&mut self` by SUBSCRIBE).
    pub(crate) fn publish(&self, local: &ShardSubscriptions, channel: Bytes, payload: Bytes) {
        match &self.topology {
            // Fast path unchanged: synchronous publish into the local table.
            Topology::Local => {
                local.publish(&channel, &payload);
            }
            // Cross-shard: non-blocking hand-off to the coordinator shard.
            Topology::Sharded {
                coordinator_shard,
                metrics,
                shard_label,
            } => {
                if coordinator_shard
                    .try_send(PubSubMsg::PublishKeyspace { channel, payload })
                    .is_err()
                {
                    // Mailbox full or closed. Keyspace notifications are
                    // best-effort / at-most-once in Redis, so drop + count
                    // rather than block the key-owner worker — awaiting here
                    // would stall this shard's command stream and could deadlock
                    // against the coordinator shard.
                    KeyspaceNotificationsDropped::inc(&**metrics, shard_label);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use tokio::sync::mpsc;

    use super::*;
    use crate::pubsub::{PubSubMessage, PubSubReceiver, PubSubSender};
    use crate::shard::message::{Envelope, ShardMessage};

    /// Records counter increments so tests can read cumulative totals back.
    #[derive(Default)]
    struct RecordingRecorder {
        counters: Mutex<HashMap<String, u64>>,
    }

    impl MetricsRecorder for RecordingRecorder {
        fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
            *self
                .counters
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_insert(0) += value;
        }
        fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn counter_value(&self, name: &str) -> Option<u64> {
            self.counters.lock().unwrap().get(name).copied()
        }
    }

    /// A `ShardSubscriptions` with one connection subscribed to `channel`, plus
    /// the receiver that connection would read delivered messages from.
    fn subscribed_table(channel: &str) -> (ShardSubscriptions, PubSubReceiver) {
        let mut subs = ShardSubscriptions::new();
        let (tx, rx) = PubSubSender::unbounded();
        subs.subscribe(Bytes::from(channel.to_string()), 1, tx);
        (subs, rx)
    }

    /// Build senders whose slot 0 is a bounded mailbox of the given capacity,
    /// returning the raw coordinator receiver so the test can synchronously
    /// inspect what arrived via `try_recv`.
    fn senders_with_coordinator(
        capacity: usize,
    ) -> (Arc<Vec<ShardSender>>, mpsc::Receiver<Envelope>) {
        let (tx, rx) = mpsc::channel::<Envelope>(capacity);
        (Arc::new(vec![ShardSender::new(tx)]), rx)
    }

    #[test]
    fn local_topology_publishes_into_local_table_and_never_enqueues() {
        // num_shards == 1 => Local (standalone).
        let senders = Arc::new(Vec::new());
        let coord = KeyspaceNotificationCoordinator::new(
            0,
            1,
            &senders,
            Arc::new(RecordingRecorder::default()),
        );

        let (local, mut rx) = subscribed_table("__keyevent@0__:set");
        coord.publish(
            &local,
            Bytes::from("__keyevent@0__:set"),
            Bytes::from("mykey"),
        );

        // Delivered straight into the local table's subscriber.
        match rx.try_recv() {
            Ok(PubSubMessage::Message { channel, payload }) => {
                assert_eq!(&channel[..], b"__keyevent@0__:set");
                assert_eq!(&payload[..], b"mykey");
            }
            other => panic!("expected local delivery, got {other:?}"),
        }
    }

    #[test]
    fn coordinator_shard_is_local_even_in_multi_shard() {
        // shard_id == 0 in a multi-shard server still routes locally: shard 0's
        // own table already *is* the subscriber table, so no self-send hop.
        let (senders, _coord_rx) = senders_with_coordinator(8);
        let coord = KeyspaceNotificationCoordinator::new(
            0,
            4,
            &senders,
            Arc::new(RecordingRecorder::default()),
        );

        let (local, mut rx) = subscribed_table("__keyspace@0__:k");
        coord.publish(&local, Bytes::from("__keyspace@0__:k"), Bytes::from("set"));

        assert!(matches!(rx.try_recv(), Ok(PubSubMessage::Message { .. })));
    }

    #[test]
    fn sharded_topology_forwards_one_message_and_leaves_local_untouched() {
        // A non-coordinator shard (shard 2 of 4) forwards to shard 0's mailbox.
        let (senders, mut coord_rx) = senders_with_coordinator(8);
        let coord = KeyspaceNotificationCoordinator::new(
            2,
            4,
            &senders,
            Arc::new(RecordingRecorder::default()),
        );

        // Local table has a subscriber, but a Sharded emit must NOT touch it.
        let (local, mut local_rx) = subscribed_table("__keyevent@0__:set");
        coord.publish(
            &local,
            Bytes::from("__keyevent@0__:set"),
            Bytes::from("mykey"),
        );

        // Nothing delivered locally.
        assert!(
            local_rx.try_recv().is_err(),
            "local table must be untouched"
        );

        // Exactly one PublishKeyspace enqueued to the coordinator, with the
        // expected channel + payload.
        let env = coord_rx.try_recv().expect("one message enqueued");
        match env.message {
            ShardMessage::PubSub(PubSubMsg::PublishKeyspace { channel, payload }) => {
                assert_eq!(&channel[..], b"__keyevent@0__:set");
                assert_eq!(&payload[..], b"mykey");
            }
            other => panic!("expected PublishKeyspace, got {}", other.probe_type_str()),
        }
        assert!(coord_rx.try_recv().is_err(), "exactly one message enqueued");
    }

    #[test]
    fn sharded_mailbox_full_drops_and_counts_without_blocking() {
        // Capacity-1 coordinator mailbox, pre-filled so the next try_send fails.
        let (senders, _coord_rx) = senders_with_coordinator(1);
        senders[0]
            .try_send(PubSubMsg::PublishKeyspace {
                channel: Bytes::from("__keyevent@0__:filler"),
                payload: Bytes::from("x"),
            })
            .expect("first send fills the single slot");

        let recorder = Arc::new(RecordingRecorder::default());
        let coord = KeyspaceNotificationCoordinator::new(3, 4, &senders, recorder.clone());

        let (local, mut local_rx) = subscribed_table("__keyevent@0__:set");
        // Must return promptly (no await, no block) even though the mailbox is full.
        coord.publish(
            &local,
            Bytes::from("__keyevent@0__:set"),
            Bytes::from("mykey"),
        );

        // Dropped, not delivered locally, and counted.
        assert!(local_rx.try_recv().is_err());
        assert_eq!(
            recorder.counter_value("frogdb_keyspace_notifications_dropped_total"),
            Some(1)
        );
    }
}
