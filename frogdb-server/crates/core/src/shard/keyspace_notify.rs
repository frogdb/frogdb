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

        // Get event type from command trait
        let Some(event_type) = handler.keyspace_event_type() else {
            return;
        };

        let keys = handler.keys(args);
        let event_name = command_to_event_name(handler.name());

        for key in &keys {
            self.emit_keyspace_notification(key, event_name, event_type);
        }
    }
}

/// Map a command name to its Redis-compatible event name.
///
/// In most cases this is simply the command name lowercased.
/// Some commands have special event names for compatibility.
fn command_to_event_name(name: &str) -> &str {
    match name {
        // Generic
        "DEL" => "del",
        "UNLINK" => "del",
        "EXPIRE" => "expire",
        "EXPIREAT" => "expire",
        "PEXPIRE" => "pexpire",
        "PEXPIREAT" => "pexpire",
        "RENAME" => "rename_from",
        "RENAMENX" => "rename_from",
        "PERSIST" => "persist",
        "COPY" => "copy_to",
        // String
        "SET" => "set",
        "SETEX" => "set",
        "PSETEX" => "set",
        "SETNX" => "set",
        "GETSET" => "set",
        "GETDEL" => "getdel",
        "GETEX" => "getex",
        "MSET" => "set",
        "MSETNX" => "set",
        "APPEND" => "append",
        "INCR" => "incrby",
        "DECR" => "decrby",
        "INCRBY" => "incrby",
        "DECRBY" => "decrby",
        "INCRBYFLOAT" => "incrbyfloat",
        "SETRANGE" => "setrange",
        // List
        "LPUSH" => "lpush",
        "RPUSH" => "rpush",
        "LPOP" => "lpop",
        "RPOP" => "rpop",
        "LINSERT" => "linsert",
        "LSET" => "lset",
        "LTRIM" => "ltrim",
        "RPOPLPUSH" => "rpoplpush",
        "LMOVE" => "lmove",
        "BLPOP" => "lpop",
        "BRPOP" => "rpop",
        "BLMOVE" => "lmove",
        // Set
        "SADD" => "sadd",
        "SREM" => "srem",
        "SPOP" => "spop",
        "SMOVE" => "smove",
        "SINTERSTORE" => "sinterstore",
        "SUNIONSTORE" => "sunionstore",
        "SDIFFSTORE" => "sdiffstore",
        // Hash
        "HSET" => "hset",
        "HSETNX" => "hset",
        "HMSET" => "hset",
        "HDEL" => "hdel",
        "HINCRBY" => "hincrby",
        "HINCRBYFLOAT" => "hincrbyfloat",
        "HSETEX" => "hset",
        // Sorted set
        "ZADD" => "zadd",
        "ZREM" => "zrem",
        "ZINCRBY" => "zincrby",
        "ZPOPMIN" => "zpopmin",
        "ZPOPMAX" => "zpopmax",
        "ZRANGESTORE" => "zrangestore",
        "ZUNIONSTORE" => "zunionstore",
        "ZINTERSTORE" => "zinterstore",
        "ZDIFFSTORE" => "zdiffstore",
        "BZPOPMIN" => "zpopmin",
        "BZPOPMAX" => "zpopmax",
        // Stream
        "XADD" => "xadd",
        "XDEL" => "xdel",
        "XTRIM" => "xtrim",
        "XGROUP" => "xgroup-create",
        // Fallback: lowercase the command name
        _ => {
            // Leak a lowercased string -- this path should rarely be hit
            // since all known commands are handled above
            Box::leak(name.to_ascii_lowercase().into_boxed_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_to_event_name() {
        assert_eq!(command_to_event_name("SET"), "set");
        assert_eq!(command_to_event_name("DEL"), "del");
        assert_eq!(command_to_event_name("UNLINK"), "del");
        assert_eq!(command_to_event_name("EXPIRE"), "expire");
        assert_eq!(command_to_event_name("LPUSH"), "lpush");
        assert_eq!(command_to_event_name("ZADD"), "zadd");
        assert_eq!(command_to_event_name("HSET"), "hset");
        assert_eq!(command_to_event_name("XADD"), "xadd");
        assert_eq!(command_to_event_name("RENAME"), "rename_from");
    }
}
