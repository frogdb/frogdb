//! Keyspace notification emission for shard workers.
//!
//! When enabled via CONFIG SET notify-keyspace-events, the shard worker
//! publishes events to special pub/sub channels after write commands execute,
//! keys expire, or keys are evicted.
//!
//! Cost when disabled: one atomic load + branch (< 1ns).

use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::command::WriteRecord;
use crate::command_spec::EventSpec;
use crate::keyspace_event::KeyspaceEventFlags;

use super::worker::ShardWorker;

impl ShardWorker {
    /// Emit a keyspace notification if the event mask matches.
    ///
    /// This is the core emission point. Called from:
    /// - Post-execution pipeline (write commands)
    /// - Active expiry loop
    /// - Eviction path
    ///
    /// Cost when disabled: one atomic load + branch (< 1ns).
    pub(crate) fn emit_keyspace_notification(
        &self,
        key: &[u8],
        event_name: &str,
        event_type: KeyspaceEventFlags,
    ) {
        let flags_bits = self.notify_keyspace_events.load(Ordering::Relaxed);
        if flags_bits == 0 {
            return; // Fast path: notifications disabled
        }

        let flags = KeyspaceEventFlags::from_bits_truncate(flags_bits);

        // Check if this event type is enabled
        if !flags.intersects(event_type) {
            return;
        }

        // Where each event lands is owned by the coordinator: on the key-owner
        // shard it forwards to shard 0 (where subscribers register), while shard
        // 0 / single-shard publishes straight into the local table.
        // __keyspace@0__:<key> -> event_name
        if flags.contains(KeyspaceEventFlags::KEYSPACE) {
            let channel = Bytes::from(format!("__keyspace@0__:{}", String::from_utf8_lossy(key)));
            let event_bytes = Bytes::from(event_name.to_string());
            self.keyspace_notify
                .publish(&self.subscriptions, channel, event_bytes);
        }

        // __keyevent@0__:<event_name> -> key
        if flags.contains(KeyspaceEventFlags::KEYEVENT) {
            let channel = Bytes::from(format!("__keyevent@0__:{event_name}"));
            self.keyspace_notify
                .publish(&self.subscriptions, channel, Bytes::copy_from_slice(key));
        }
    }

    /// Emit keyspace notifications after a write command executes.
    ///
    /// Called from the post-execution pipeline for write commands. The record's
    /// spec decides which keys notify:
    /// - [`EventSpec::Emits`]: every extracted key (single-key writes and the
    ///   genuinely per-key multi-key writes like DEL/MSET).
    /// - [`EventSpec::EmitsAt`]: exactly `keys()[key_index]` — STORE-family
    ///   commands notify their destination, never the read-only sources.
    /// - [`EventSpec::Dynamic`]: exactly the events the handler deposited via
    ///   [`notify_event`](crate::command::CommandContext::notify_event), in
    ///   deposit order. A no-op write never reaches here (the effect pipeline
    ///   is skipped), so its deposits are naturally discarded.
    pub(crate) fn emit_keyspace_notifications_for_command(&self, record: &WriteRecord<'_>) {
        // Fast path: skip if notifications are disabled entirely
        let flags_bits = self.notify_keyspace_events.load(Ordering::Relaxed);
        if flags_bits == 0 {
            return;
        }

        // The event class and name live together on the command's spec.
        match record.handler.spec().event {
            EventSpec::Emits { class, name } => {
                for key in &record.handler.keys(record.args) {
                    self.emit_keyspace_notification(key, name, class);
                }
            }
            EventSpec::EmitsAt {
                class,
                name,
                key_index,
            } => {
                let keys = record.handler.keys(record.args);
                if let Some(key) = keys.get(key_index) {
                    self.emit_keyspace_notification(key, name, class);
                } else {
                    // validate() bounds key_index statically for every valid
                    // arity, so this is unreachable for registered specs.
                    debug_assert!(
                        false,
                        "{}: EmitsAt key_index {key_index} out of range ({} extracted keys)",
                        record.handler.name(),
                        keys.len()
                    );
                }
            }
            EventSpec::Dynamic => {
                for (key, name, class) in record.keyspace_events {
                    self.emit_keyspace_notification(key, name, *class);
                }
            }
            // Suppressed or NotApplicable: emit nothing.
            EventSpec::Suppressed | EventSpec::NotApplicable => {}
        }
    }
}
