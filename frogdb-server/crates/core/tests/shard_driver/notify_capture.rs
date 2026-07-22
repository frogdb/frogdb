//! Keyspace-notification capture seam + order-consistency checker for the
//! shard-driver harness (design doc S8 note: "the shard-driver harness has no
//! notification-capture seam").
//!
//! A single-shard [`ShardDriver`](super::harness::ShardDriver) runs the `Local`
//! keyspace topology, so `emit_keyspace_notification` publishes straight into
//! the worker's own subscription table. This module registers a capture
//! subscriber there (via the worker's `drive_capture_keyspace` seam) and drains
//! the delivered notifications after a driven schedule, so a test can assert the
//! `expired`/`del`/`set`/... keyevents are consistent with the serialization
//! order the schedule chose.

#![allow(dead_code)] // used piecemeal by scenario modules

use bytes::Bytes;
use tokio::sync::mpsc;

use frogdb_core::keyspace_event::KeyspaceEventFlags;
use frogdb_core::pubsub::PubSubMessage;

/// The `__keyevent@0__:<name>` channel prefix all keyevent notifications use.
const KEYEVENT_PREFIX: &[u8] = b"__keyevent@0__:";
/// The `__keyspace@0__:<key>` channel prefix all keyspace notifications use.
const KEYSPACE_PREFIX: &[u8] = b"__keyspace@0__:";

/// The keyevent mask a capture usually wants: every event type on the
/// `__keyevent@0__:*` channels (KEYEVENT + all type classes).
pub fn all_keyevents_mask() -> u32 {
    (KeyspaceEventFlags::KEYEVENT | KeyspaceEventFlags::ALL_TYPES).bits()
}

/// One captured notification, as delivered into the subscriber table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedNotification {
    /// The pub/sub channel (`__keyevent@0__:set`, `__keyspace@0__:k`, ...).
    pub channel: Bytes,
    /// The payload (a key for keyevent channels; an event name for keyspace).
    pub payload: Bytes,
}

impl CapturedNotification {
    /// If this is a `__keyevent@0__:<event>` notification, return
    /// `(event_name, key)` — the payload is the key.
    pub fn as_keyevent(&self) -> Option<(String, Bytes)> {
        self.channel.strip_prefix(KEYEVENT_PREFIX).map(|ev| {
            (
                String::from_utf8_lossy(ev).to_string(),
                self.payload.clone(),
            )
        })
    }

    /// If this is a `__keyspace@0__:<key>` notification, return
    /// `(key, event_name)` — the payload is the event name.
    pub fn as_keyspace(&self) -> Option<(Bytes, String)> {
        self.channel.strip_prefix(KEYSPACE_PREFIX).map(|key| {
            (
                Bytes::copy_from_slice(key),
                String::from_utf8_lossy(&self.payload).to_string(),
            )
        })
    }
}

/// Holds the receiver a worker's `drive_capture_keyspace` seam returned. Drain
/// it after a schedule to read every notification the schedule emitted, in
/// emission order.
pub struct NotificationCapture {
    rx: mpsc::UnboundedReceiver<PubSubMessage>,
}

impl NotificationCapture {
    /// Wrap a capture receiver.
    pub fn new(rx: mpsc::UnboundedReceiver<PubSubMessage>) -> Self {
        Self { rx }
    }

    /// Non-blocking drain of every notification delivered so far, in emission
    /// order. `Message`/`PatternMessage`/`ShardedMessage` frames are flattened
    /// to `(channel, payload)`; confirmations are ignored.
    pub fn drain(&mut self) -> Vec<CapturedNotification> {
        let mut out = Vec::new();
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                PubSubMessage::Message { channel, payload }
                | PubSubMessage::ShardedMessage { channel, payload } => {
                    out.push(CapturedNotification { channel, payload });
                }
                PubSubMessage::PatternMessage {
                    channel, payload, ..
                } => {
                    out.push(CapturedNotification { channel, payload });
                }
                PubSubMessage::Confirmation(_) => {}
            }
        }
        out
    }

    /// Drain and project to the keyevent `(event, key)` pairs in emission order,
    /// dropping any non-keyevent notifications.
    pub fn drain_keyevents(&mut self) -> Vec<(String, Bytes)> {
        self.drain()
            .into_iter()
            .filter_map(|n| n.as_keyevent())
            .collect()
    }
}

/// Assert the observed keyevent stream (in emission order) equals `expected`
/// exactly — same events, same keys, same relative order. This is the
/// order-consistency checker: `expected` is the keyevent sequence the chosen
/// serialization order implies, so any missing, extra, or reordered
/// notification (e.g. a sweep's `expired` published after an EXEC that the
/// schedule ran *after* the sweep) fails.
///
/// Returns `Ok(())` or a human-readable diff.
pub fn assert_keyevents_consistent(
    observed: &[(String, Bytes)],
    expected: &[(&str, &[u8])],
) -> Result<(), String> {
    let want: Vec<(String, Bytes)> = expected
        .iter()
        .map(|(ev, key)| (ev.to_string(), Bytes::copy_from_slice(key)))
        .collect();
    if observed == want.as_slice() {
        Ok(())
    } else {
        Err(format!(
            "keyevent order inconsistent with chosen serialization:\n  expected: {:?}\n  observed: {:?}",
            want.iter()
                .map(|(e, k)| format!("{e}:{}", String::from_utf8_lossy(k)))
                .collect::<Vec<_>>(),
            observed
                .iter()
                .map(|(e, k)| format!("{e}:{}", String::from_utf8_lossy(k)))
                .collect::<Vec<_>>(),
        ))
    }
}

mod capture_tests {
    use super::*;

    #[test]
    fn keyevent_and_keyspace_projection() {
        let ev = CapturedNotification {
            channel: Bytes::from_static(b"__keyevent@0__:expired"),
            payload: Bytes::from_static(b"mykey"),
        };
        assert_eq!(
            ev.as_keyevent(),
            Some(("expired".to_string(), Bytes::from_static(b"mykey")))
        );
        assert_eq!(ev.as_keyspace(), None);

        let ks = CapturedNotification {
            channel: Bytes::from_static(b"__keyspace@0__:mykey"),
            payload: Bytes::from_static(b"expired"),
        };
        assert_eq!(
            ks.as_keyspace(),
            Some((Bytes::from_static(b"mykey"), "expired".to_string()))
        );
        assert_eq!(ks.as_keyevent(), None);
    }

    #[test]
    fn consistency_checker_accepts_matching_and_rejects_reordered() {
        let observed = vec![
            ("expired".to_string(), Bytes::from_static(b"e")),
            ("set".to_string(), Bytes::from_static(b"a")),
            ("set".to_string(), Bytes::from_static(b"b")),
        ];
        assert!(
            assert_keyevents_consistent(
                &observed,
                &[("expired", b"e"), ("set", b"a"), ("set", b"b")]
            )
            .is_ok()
        );
        // Reordered (set before expired) → rejected.
        assert!(
            assert_keyevents_consistent(
                &observed,
                &[("set", b"a"), ("set", b"b"), ("expired", b"e")]
            )
            .is_err()
        );
        // Missing the expired event → rejected.
        assert!(assert_keyevents_consistent(&observed, &[("set", b"a"), ("set", b"b")]).is_err());
    }
}
