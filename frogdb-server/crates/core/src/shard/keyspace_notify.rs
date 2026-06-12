//! Keyspace notification emission for shard workers.
//!
//! When enabled via CONFIG SET notify-keyspace-events, the shard worker
//! publishes events to special pub/sub channels after write commands execute,
//! keys expire, or keys are evicted.
//!
//! Cost when disabled: one atomic load + branch (< 1ns).

use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::command::Command;
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

        let event_bytes = Bytes::from(event_name.to_string());

        // __keyspace@0__:<key> -> event_name
        if flags.contains(KeyspaceEventFlags::KEYSPACE) {
            let channel = Bytes::from(format!("__keyspace@0__:{}", String::from_utf8_lossy(key)));
            self.subscriptions.publish(&channel, &event_bytes);
        }

        // __keyevent@0__:<event_name> -> key
        if flags.contains(KeyspaceEventFlags::KEYEVENT) {
            let channel = Bytes::from(format!("__keyevent@0__:{event_name}"));
            let key_bytes = Bytes::copy_from_slice(key);
            self.subscriptions.publish(&channel, &key_bytes);
        }
    }

    /// Emit keyspace notifications after a write command executes.
    ///
    /// Called from the post-execution pipeline for write commands.
    pub(crate) fn emit_keyspace_notifications_for_command(
        &self,
        handler: &dyn Command,
        args: &[Bytes],
    ) {
        // Fast path: skip if notifications are disabled entirely
        let flags_bits = self.notify_keyspace_events.load(Ordering::Relaxed);
        if flags_bits == 0 {
            return;
        }

        // The event class and name live together on the command's spec.
        let EventSpec::Emits { class, name } = handler.spec().event else {
            return; // Suppressed or NotApplicable: emit nothing.
        };

        for key in &handler.keys(args) {
            self.emit_keyspace_notification(key, name, class);
        }
    }
}
