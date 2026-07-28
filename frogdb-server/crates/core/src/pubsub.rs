//! Pub/Sub infrastructure for FrogDB.
//!
//! This module provides Redis-compatible Pub/Sub functionality including:
//! - Broadcast Pub/Sub (SUBSCRIBE, PUBLISH, etc.)
//! - Sharded Pub/Sub (SSUBSCRIBE, SPUBLISH)
//! - Pattern subscriptions (PSUBSCRIBE)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use frogdb_types::metrics::definitions::PubsubShardLimitWarnings;
use frogdb_types::metrics::labels::PubsubLimitResource;
use frogdb_types::traits::MetricsRecorder;
use tokio::sync::{Notify, mpsc};

/// Connection ID type.
pub type ConnId = u64;

/// Default hard cap (32 MiB) on the bytes of pending pub/sub messages buffered
/// for a single slow / non-reading subscriber before further messages are
/// dropped and the connection is torn down. Mirrors Redis's
/// `client-output-buffer-limit pubsub 32mb ...` hard limit.
pub const DEFAULT_PUBSUB_OUTPUT_BUFFER_HARD_LIMIT: usize = 32 * 1024 * 1024;

/// Shared byte budget backing one connection's pub/sub delivery channel.
///
/// Every clone of the connection's [`PubSubSender`] (one per shard the
/// connection has subscribed on) and its single [`PubSubReceiver`] share this
/// via `Arc`, so enqueue (on any shard) and drain (on the connection task)
/// account against the same counter. This is what bounds a slow subscriber's
/// server-side memory footprint: once `queued_bytes` would exceed `hard_limit`,
/// further messages are dropped rather than buffered without bound.
#[derive(Debug)]
struct OutputBudget {
    /// Bytes currently queued but not yet drained by the connection task.
    queued_bytes: AtomicUsize,
    /// Hard cap in bytes; `0` disables the bound (legacy unbounded behavior).
    hard_limit: usize,
    /// Latched once the hard cap is first exceeded, so the connection task can
    /// tear the slow subscriber down (best effort — see [`connection`] loop).
    overflowed: AtomicBool,
    /// Count of messages dropped due to overflow (observability).
    dropped: AtomicUsize,
    /// Fired once, when `overflowed` first latches, to wake a delivery loop that
    /// is parked on an empty channel. Without this a flood that overflows the
    /// budget and is then fully dropped leaves the receiver with nothing to
    /// `recv`, so the latch would never be observed and the slow subscriber
    /// would never be disconnected.
    overflow_notify: Notify,
}

/// Sender for delivering pub/sub messages to a connection.
///
/// Wraps an unbounded [`mpsc`] channel with a shared byte budget
/// ([`OutputBudget`]) so a stalled subscriber cannot force the server to buffer
/// publishes without bound. Cloned onto every shard the connection subscribes
/// on; all clones and the paired [`PubSubReceiver`] share one budget.
#[derive(Clone, Debug)]
pub struct PubSubSender {
    inner: mpsc::UnboundedSender<PubSubMessage>,
    budget: Arc<OutputBudget>,
}

/// Receiver half paired with [`PubSubSender`]. Draining decrements the shared
/// byte budget so the sender side regains headroom.
#[derive(Debug)]
pub struct PubSubReceiver {
    inner: mpsc::UnboundedReceiver<PubSubMessage>,
    budget: Arc<OutputBudget>,
}

/// Error returned by [`PubSubSender::send`] when the receiving connection has
/// gone away (the delivery task dropped its [`PubSubReceiver`]).
#[derive(Debug)]
pub struct PubSubClosed;

/// Outcome of [`PubSubReceiver::recv_or_overflow`]: either a drained message or
/// a signal that the output buffer overflowed and the slow subscriber should be
/// disconnected.
#[derive(Debug)]
pub enum Drained {
    /// A message drained from the queue (the byte budget has been released).
    Message(PubSubMessage),
    /// The output-buffer hard limit was exceeded and no messages remain queued.
    /// The connection task should disconnect this slow subscriber. Reported even
    /// when the flood was entirely dropped and the channel is empty, which a
    /// bare `recv` could never observe.
    Overflowed,
}

/// Wait until the shared budget latches an output-buffer overflow.
///
/// Never resolves for an unbounded channel (`hard_limit == 0`, which never
/// overflows), making it a safe fallback arm in a `select!`.
async fn wait_overflow(budget: &OutputBudget) {
    loop {
        // Register for a wake *before* checking the flag so a latch that races
        // with this check is not missed (`Notify` stores one permit).
        let notified = budget.overflow_notify.notified();
        if budget.overflowed.load(Ordering::Relaxed) {
            return;
        }
        notified.await;
    }
}

impl PubSubSender {
    /// Create a byte-bounded pub/sub channel. `hard_limit` is the maximum bytes
    /// of pending messages tolerated for a slow subscriber before further
    /// messages are dropped; `0` disables the bound.
    pub fn channel(hard_limit: usize) -> (PubSubSender, PubSubReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let budget = Arc::new(OutputBudget {
            queued_bytes: AtomicUsize::new(0),
            hard_limit,
            overflowed: AtomicBool::new(false),
            dropped: AtomicUsize::new(0),
            overflow_notify: Notify::new(),
        });
        (
            PubSubSender {
                inner: tx,
                budget: Arc::clone(&budget),
            },
            PubSubReceiver { inner: rx, budget },
        )
    }

    /// Create an unbounded pub/sub channel (no output-buffer limit).
    ///
    /// Used by internal delivery paths that never stall on a client socket
    /// (e.g. the shard-local keyspace-notification capture used in tests) and
    /// wherever a bound would add no value.
    pub fn unbounded() -> (PubSubSender, PubSubReceiver) {
        Self::channel(0)
    }

    /// Deliver a message to the subscriber.
    ///
    /// When the shared byte budget is exhausted the message is *dropped* (not
    /// enqueued) and the overflow flag is latched so the connection task tears
    /// the slow subscriber down. The subscriber is still counted as a recipient
    /// (`Ok`) — matching Redis, where a client that trips its output-buffer
    /// limit is counted for `PUBLISH` and then disconnected asynchronously.
    /// `Err(PubSubClosed)` is returned only when the receiver has gone away, so
    /// callers keep their "closed => not a subscriber" counting semantics.
    pub fn send(&self, msg: PubSubMessage) -> Result<(), PubSubClosed> {
        let size = msg.approx_buffer_size();
        let budget = &self.budget;
        if budget.hard_limit != 0 {
            // Approximate check: concurrent senders on different shards may each
            // pass and overshoot the cap by a bounded amount (a few in-flight
            // messages), which is fine for a DoS bound.
            let queued = budget.queued_bytes.load(Ordering::Relaxed);
            if queued.saturating_add(size) > budget.hard_limit {
                // Latch overflow and, on the first transition, wake a delivery
                // loop that may be parked on an otherwise-empty channel so it
                // can tear the slow subscriber down.
                if !budget.overflowed.swap(true, Ordering::Relaxed) {
                    budget.overflow_notify.notify_one();
                }
                budget.dropped.fetch_add(1, Ordering::Relaxed);
                // Report closed vs. still-open honestly so subscriber counts
                // stay correct even while dropping for overflow.
                return if self.inner.is_closed() {
                    Err(PubSubClosed)
                } else {
                    Ok(())
                };
            }
        }
        match self.inner.send(msg) {
            Ok(()) => {
                budget.queued_bytes.fetch_add(size, Ordering::Relaxed);
                Ok(())
            }
            Err(_) => Err(PubSubClosed),
        }
    }
}

impl PubSubReceiver {
    /// Await the next message, decrementing the shared byte budget.
    pub async fn recv(&mut self) -> Option<PubSubMessage> {
        let msg = self.inner.recv().await?;
        self.release(&msg);
        Some(msg)
    }

    /// Await either the next queued message or an output-buffer overflow.
    ///
    /// A message drains (and releases the byte budget) preferentially; only when
    /// the channel is empty does a latched overflow surface as
    /// [`Drained::Overflowed`]. This lets the connection task disconnect a slow
    /// subscriber whose flood was dropped to keep memory bounded — the latch
    /// alone never wakes a `recv` parked on the now-empty channel. Returns `None`
    /// only when the channel is closed with nothing left to drain.
    pub async fn recv_or_overflow(&mut self) -> Option<Drained> {
        tokio::select! {
            biased;
            msg = self.inner.recv() => {
                let msg = msg?;
                self.release(&msg);
                Some(Drained::Message(msg))
            }
            () = wait_overflow(&self.budget) => Some(Drained::Overflowed),
        }
    }

    /// Try to dequeue a message without awaiting, decrementing the budget.
    pub fn try_recv(&mut self) -> Result<PubSubMessage, mpsc::error::TryRecvError> {
        let msg = self.inner.try_recv()?;
        self.release(&msg);
        Ok(msg)
    }

    /// Whether the sender side has dropped messages after exceeding the hard
    /// output-buffer limit. The connection task tears the subscriber down when
    /// this latches true.
    pub fn has_overflowed(&self) -> bool {
        self.budget.overflowed.load(Ordering::Relaxed)
    }

    /// Bytes currently queued for this subscriber (test/observability aid).
    pub fn queued_bytes(&self) -> usize {
        self.budget.queued_bytes.load(Ordering::Relaxed)
    }

    /// Number of messages dropped due to output-buffer overflow.
    pub fn dropped(&self) -> usize {
        self.budget.dropped.load(Ordering::Relaxed)
    }

    fn release(&self, msg: &PubSubMessage) {
        let size = msg.approx_buffer_size();
        // Saturating: enqueue/drain use the same per-message size so the
        // counter stays balanced, but never underflow if accounting drifts.
        let mut cur = self.budget.queued_bytes.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(size);
            match self.budget.queued_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
}

/// Maximum subscriptions per connection.
pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 10_000;

/// Maximum pattern subscriptions per connection.
pub const MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION: usize = 1_000;

/// Maximum sharded subscriptions per connection.
pub const MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION: usize = 10_000;

/// Maximum total subscriptions per shard (all connections combined).
pub const MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD: usize = 1_000_000;

/// Maximum unique channels per shard.
pub const MAX_UNIQUE_CHANNELS_PER_SHARD: usize = 100_000;

/// Maximum unique patterns per shard.
pub const MAX_UNIQUE_PATTERNS_PER_SHARD: usize = 10_000;

/// A pub/sub subscribe/unsubscribe confirmation reply.
///
/// This owns the confirmation shape rule in ONE place: RESP3 emits a `Push`
/// frame, RESP2 an `Array`, matching Redis's `addReplyPubsubSubscribed` /
/// `addReplyPubsubUnsubscribed` (both of which use `addReplyPushLen` under
/// RESP3). Handlers and the transaction (MULTI/EXEC) path both build their
/// confirmations through here, so the same confirmation has the same wire
/// shape regardless of the path it rode.
///
/// The unsubscribe variants carry an `Option` channel because
/// `UNSUBSCRIBE` / `PUNSUBSCRIBE` / `SUNSUBSCRIBE` with no active
/// subscriptions reply with a null channel and a count of zero.
#[derive(Debug, Clone)]
pub enum PubSubConfirmation {
    /// `subscribe` confirmation.
    Subscribe { channel: Bytes, count: usize },
    /// `unsubscribe` confirmation (null channel when unsubscribing from none).
    Unsubscribe {
        channel: Option<Bytes>,
        count: usize,
    },
    /// `psubscribe` confirmation.
    PSubscribe { pattern: Bytes, count: usize },
    /// `punsubscribe` confirmation (null pattern when unsubscribing from none).
    PUnsubscribe {
        pattern: Option<Bytes>,
        count: usize,
    },
    /// `ssubscribe` confirmation.
    SSubscribe { channel: Bytes, count: usize },
    /// `sunsubscribe` confirmation (null channel when unsubscribing from none).
    SUnsubscribe {
        channel: Option<Bytes>,
        count: usize,
    },
}

impl PubSubConfirmation {
    /// The `[kind, channel-or-null, count]` items shared by both protocols.
    fn items(&self) -> Vec<Response> {
        let (kind, channel, count): (&'static [u8], Option<Bytes>, usize) = match self {
            PubSubConfirmation::Subscribe { channel, count } => {
                (b"subscribe", Some(channel.clone()), *count)
            }
            PubSubConfirmation::Unsubscribe { channel, count } => {
                (b"unsubscribe", channel.clone(), *count)
            }
            PubSubConfirmation::PSubscribe { pattern, count } => {
                (b"psubscribe", Some(pattern.clone()), *count)
            }
            PubSubConfirmation::PUnsubscribe { pattern, count } => {
                (b"punsubscribe", pattern.clone(), *count)
            }
            PubSubConfirmation::SSubscribe { channel, count } => {
                (b"ssubscribe", Some(channel.clone()), *count)
            }
            PubSubConfirmation::SUnsubscribe { channel, count } => {
                (b"sunsubscribe", channel.clone(), *count)
            }
        };
        vec![
            Response::bulk(Bytes::from_static(kind)),
            match channel {
                Some(ch) => Response::bulk(ch),
                None => Response::null(),
            },
            Response::Integer(count as i64),
        ]
    }

    /// Emit the protocol-correct confirmation: RESP3 `Push`, RESP2 `Array`.
    pub fn to_response(&self, protocol: ProtocolVersion) -> Response {
        let items = self.items();
        if protocol.is_resp3() {
            Response::Push(items)
        } else {
            Response::Array(items)
        }
    }
}

/// Messages delivered to subscribers.
#[derive(Debug, Clone)]
pub enum PubSubMessage {
    /// Standard channel message.
    Message { channel: Bytes, payload: Bytes },
    /// Pattern-matched message.
    PatternMessage {
        pattern: Bytes,
        channel: Bytes,
        payload: Bytes,
    },
    /// Sharded channel message.
    ShardedMessage { channel: Bytes, payload: Bytes },
    /// A subscribe/unsubscribe confirmation delivered out-of-band (e.g. the
    /// `sunsubscribe` notifications emitted when a slot's sharded channels are
    /// drained). Direct command replies build [`PubSubConfirmation`] straight
    /// into a `Response`; this variant carries one over the delivery channel.
    Confirmation(PubSubConfirmation),
}

impl PubSubMessage {
    /// Approximate on-the-wire size in bytes, used to charge the per-connection
    /// output-buffer budget. Counts the variable payload/channel/pattern bytes
    /// plus a small fixed allowance for the RESP framing and message kind.
    pub(crate) fn approx_buffer_size(&self) -> usize {
        /// Fixed allowance covering the RESP array header, kind bulk string,
        /// and per-element length prefixes.
        const FRAMING_OVERHEAD: usize = 48;
        let payload = match self {
            PubSubMessage::Message { channel, payload } => channel.len() + payload.len(),
            PubSubMessage::PatternMessage {
                pattern,
                channel,
                payload,
            } => pattern.len() + channel.len() + payload.len(),
            PubSubMessage::ShardedMessage { channel, payload } => channel.len() + payload.len(),
            PubSubMessage::Confirmation(_) => 0,
        };
        payload + FRAMING_OVERHEAD
    }

    /// Convert to RESP2 array format.
    pub fn to_response(&self) -> Response {
        self.to_response_with_protocol(ProtocolVersion::Resp2)
    }

    /// Convert to response with protocol-specific format.
    ///
    /// In RESP3, pub/sub messages use the Push type.
    /// In RESP2, they use regular Array type.
    pub fn to_response_with_protocol(&self, protocol: ProtocolVersion) -> Response {
        let items = match self {
            PubSubMessage::Message { channel, payload } => {
                vec![
                    Response::bulk(Bytes::from_static(b"message")),
                    Response::bulk(channel.clone()),
                    Response::bulk(payload.clone()),
                ]
            }
            PubSubMessage::PatternMessage {
                pattern,
                channel,
                payload,
            } => {
                vec![
                    Response::bulk(Bytes::from_static(b"pmessage")),
                    Response::bulk(pattern.clone()),
                    Response::bulk(channel.clone()),
                    Response::bulk(payload.clone()),
                ]
            }
            PubSubMessage::ShardedMessage { channel, payload } => {
                vec![
                    Response::bulk(Bytes::from_static(b"smessage")),
                    Response::bulk(channel.clone()),
                    Response::bulk(payload.clone()),
                ]
            }
            // Confirmations own their own Array/Push shaping so both the direct
            // and the out-of-band path go through the same rule.
            PubSubMessage::Confirmation(confirmation) => {
                return confirmation.to_response(protocol);
            }
        };

        if protocol.is_resp3() {
            Response::Push(items)
        } else {
            Response::Array(items)
        }
    }
}

/// A compiled glob pattern for efficient matching.
///
/// Delegates to `frogdb_types::glob_match` which uses an iterative O(nm)
/// algorithm with no catastrophic backtracking.
#[derive(Debug, Clone)]
pub struct GlobPattern {
    /// Original pattern bytes.
    pattern: Bytes,
}

impl GlobPattern {
    /// Compile a new glob pattern.
    pub fn new(pattern: Bytes) -> Self {
        Self { pattern }
    }

    /// Get the original pattern bytes.
    pub fn pattern(&self) -> &Bytes {
        &self.pattern
    }

    /// Check if a string matches this pattern.
    pub fn matches(&self, s: &[u8]) -> bool {
        frogdb_types::glob_match(&self.pattern, s)
    }
}

/// Introspection request for PUBSUB commands.
#[derive(Debug, Clone)]
pub enum IntrospectionRequest {
    /// Get active channels, optionally filtered by pattern.
    Channels { pattern: Option<GlobPattern> },
    /// Get subscriber counts for specific channels.
    NumSub { channels: Vec<Bytes> },
    /// Get total pattern subscription count.
    NumPat,
    /// Get active sharded channels, optionally filtered by pattern.
    ShardChannels { pattern: Option<GlobPattern> },
    /// Get subscriber counts for specific sharded channels.
    ShardNumSub { channels: Vec<Bytes> },
}

/// Introspection response from a shard.
#[derive(Debug)]
pub enum IntrospectionResponse {
    /// List of channel names.
    Channels(Vec<Bytes>),
    /// Subscriber counts per channel: (channel, count).
    NumSub(Vec<(Bytes, usize)>),
    /// Pattern subscription count.
    NumPat(usize),
}

/// Manages subscriptions within a shard.
#[derive(Debug, Default)]
pub struct ShardSubscriptions {
    /// Channel -> (ConnId -> Sender).
    channel_subs: HashMap<Bytes, HashMap<ConnId, PubSubSender>>,
    /// Pattern subscriptions: (pattern_bytes, compiled_pattern, conn_id, sender).
    pattern_subs: Vec<(Bytes, GlobPattern, ConnId, PubSubSender)>,
    /// Sharded channel -> (ConnId -> Sender).
    sharded_subs: HashMap<Bytes, HashMap<ConnId, PubSubSender>>,
    /// Whether the 90% total subscription threshold warning has been emitted.
    warned_total_90: bool,
    /// Whether the 90% unique channel threshold warning has been emitted.
    warned_channels_90: bool,
    /// Whether the 90% unique pattern threshold warning has been emitted.
    warned_patterns_90: bool,
}

impl ShardSubscriptions {
    /// Create new empty subscriptions.
    pub fn new() -> Self {
        Self::default()
    }

    // =========================================================================
    // Broadcast channel subscriptions
    // =========================================================================

    /// Subscribe a connection to a channel.
    /// Returns true if this is a new subscription.
    pub fn subscribe(&mut self, channel: Bytes, conn_id: ConnId, sender: PubSubSender) -> bool {
        use std::collections::hash_map::Entry;
        let conn_map = self.channel_subs.entry(channel).or_default();
        if let Entry::Vacant(e) = conn_map.entry(conn_id) {
            e.insert(sender);
            true
        } else {
            false
        }
    }

    /// Unsubscribe a connection from a channel.
    /// Returns true if the connection was subscribed.
    pub fn unsubscribe(&mut self, channel: &Bytes, conn_id: ConnId) -> bool {
        if let Some(conn_map) = self.channel_subs.get_mut(channel) {
            let removed = conn_map.remove(&conn_id).is_some();
            if conn_map.is_empty() {
                self.channel_subs.remove(channel);
            }
            removed
        } else {
            false
        }
    }

    /// Get all channels a connection is subscribed to (for unsubscribe all).
    pub fn get_connection_channels(&self, conn_id: ConnId) -> Vec<Bytes> {
        self.channel_subs
            .iter()
            .filter_map(|(ch, conns)| {
                if conns.contains_key(&conn_id) {
                    Some(ch.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    // =========================================================================
    // Pattern subscriptions
    // =========================================================================

    /// Subscribe a connection to a pattern.
    /// Returns true if this is a new subscription.
    pub fn psubscribe(&mut self, pattern: Bytes, conn_id: ConnId, sender: PubSubSender) -> bool {
        // Check if already subscribed to this exact pattern
        for (p, _, c, _) in &self.pattern_subs {
            if *c == conn_id && *p == pattern {
                return false;
            }
        }

        let compiled = GlobPattern::new(pattern.clone());
        self.pattern_subs.push((pattern, compiled, conn_id, sender));
        true
    }

    /// Unsubscribe a connection from a pattern.
    /// Returns true if the connection was subscribed.
    pub fn punsubscribe(&mut self, pattern: &Bytes, conn_id: ConnId) -> bool {
        let initial_len = self.pattern_subs.len();
        self.pattern_subs
            .retain(|(p, _, c, _)| !(*c == conn_id && p == pattern));
        self.pattern_subs.len() < initial_len
    }

    /// Get all patterns a connection is subscribed to (for punsubscribe all).
    pub fn get_connection_patterns(&self, conn_id: ConnId) -> Vec<Bytes> {
        self.pattern_subs
            .iter()
            .filter_map(
                |(p, _, c, _)| {
                    if *c == conn_id { Some(p.clone()) } else { None }
                },
            )
            .collect()
    }

    /// Get the total number of pattern subscriptions.
    pub fn pattern_count(&self) -> usize {
        self.pattern_subs.len()
    }

    // =========================================================================
    // Sharded subscriptions
    // =========================================================================

    /// Subscribe a connection to a sharded channel.
    /// Returns true if this is a new subscription.
    pub fn ssubscribe(&mut self, channel: Bytes, conn_id: ConnId, sender: PubSubSender) -> bool {
        use std::collections::hash_map::Entry;
        let conn_map = self.sharded_subs.entry(channel).or_default();
        if let Entry::Vacant(e) = conn_map.entry(conn_id) {
            e.insert(sender);
            true
        } else {
            false
        }
    }

    /// Unsubscribe a connection from a sharded channel.
    /// Returns true if the connection was subscribed.
    pub fn sunsubscribe(&mut self, channel: &Bytes, conn_id: ConnId) -> bool {
        if let Some(conn_map) = self.sharded_subs.get_mut(channel) {
            let removed = conn_map.remove(&conn_id).is_some();
            if conn_map.is_empty() {
                self.sharded_subs.remove(channel);
            }
            removed
        } else {
            false
        }
    }

    /// Drain all sharded subscribers for channels belonging to the given slot.
    ///
    /// Processes channels one at a time (matching Redis's `removeChannelsInSlot`)
    /// so that the remaining subscription count in each `SUnsubscribe` notification
    /// decreases correctly.
    ///
    /// Returns the number of notifications sent.
    pub fn drain_sharded_channels_for_slot(&mut self, slot: u16) -> usize {
        use crate::shard::slot_for_key;

        let channels_to_remove: Vec<Bytes> = self
            .sharded_subs
            .keys()
            .filter(|ch| slot_for_key(ch) == slot)
            .cloned()
            .collect();

        let mut notification_count = 0;

        for channel in channels_to_remove {
            if let Some(subscribers) = self.sharded_subs.remove(&channel) {
                for (conn_id, sender) in subscribers {
                    let remaining = self
                        .sharded_subs
                        .values()
                        .filter(|subs| subs.contains_key(&conn_id))
                        .count();
                    let _ = sender.send(PubSubMessage::Confirmation(
                        PubSubConfirmation::SUnsubscribe {
                            channel: Some(channel.clone()),
                            count: remaining,
                        },
                    ));
                    notification_count += 1;
                }
            }
        }

        notification_count
    }

    /// Get all sharded channels a connection is subscribed to.
    pub fn get_connection_sharded_channels(&self, conn_id: ConnId) -> Vec<Bytes> {
        self.sharded_subs
            .iter()
            .filter_map(|(ch, conns)| {
                if conns.contains_key(&conn_id) {
                    Some(ch.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    // =========================================================================
    // Publishing
    // =========================================================================

    /// Publish a message to a broadcast channel.
    /// Returns the number of subscribers that received the message.
    pub fn publish(&self, channel: &Bytes, payload: &Bytes) -> usize {
        let mut count = 0;

        // Direct channel subscribers
        if let Some(conns) = self.channel_subs.get(channel) {
            for sender in conns.values() {
                if sender
                    .send(PubSubMessage::Message {
                        channel: channel.clone(),
                        payload: payload.clone(),
                    })
                    .is_ok()
                {
                    count += 1;
                }
            }
        }

        // Pattern subscribers
        for (pattern, compiled, _, sender) in &self.pattern_subs {
            if compiled.matches(channel)
                && sender
                    .send(PubSubMessage::PatternMessage {
                        pattern: pattern.clone(),
                        channel: channel.clone(),
                        payload: payload.clone(),
                    })
                    .is_ok()
            {
                count += 1;
            }
        }

        count
    }

    /// Publish a message to a sharded channel.
    /// Returns the number of subscribers that received the message.
    pub fn spublish(&self, channel: &Bytes, payload: &Bytes) -> usize {
        let mut count = 0;

        if let Some(conns) = self.sharded_subs.get(channel) {
            for sender in conns.values() {
                if sender
                    .send(PubSubMessage::ShardedMessage {
                        channel: channel.clone(),
                        payload: payload.clone(),
                    })
                    .is_ok()
                {
                    count += 1;
                }
            }
        }

        count
    }

    // =========================================================================
    // Connection cleanup
    // =========================================================================

    /// Remove all subscriptions for a connection.
    pub fn remove_connection(&mut self, conn_id: ConnId) {
        // Remove from channel subscriptions
        self.channel_subs.retain(|_, conns| {
            conns.remove(&conn_id);
            !conns.is_empty()
        });

        // Remove from pattern subscriptions
        self.pattern_subs.retain(|(_, _, c, _)| *c != conn_id);

        // Remove from sharded subscriptions
        self.sharded_subs.retain(|_, conns| {
            conns.remove(&conn_id);
            !conns.is_empty()
        });
    }

    // =========================================================================
    // Introspection
    // =========================================================================

    /// Get all active broadcast channels, optionally filtered by pattern.
    pub fn channels(&self, pattern: Option<&GlobPattern>) -> Vec<Bytes> {
        self.channel_subs
            .keys()
            .filter(|ch| pattern.as_ref().is_none_or(|p| p.matches(ch)))
            .cloned()
            .collect()
    }

    /// Get subscriber counts for specific broadcast channels.
    pub fn numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .channel_subs
                    .get(ch)
                    .map(|conns| conns.len())
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// Get all active sharded channels, optionally filtered by pattern.
    pub fn shard_channels(&self, pattern: Option<&GlobPattern>) -> Vec<Bytes> {
        self.sharded_subs
            .keys()
            .filter(|ch| pattern.as_ref().is_none_or(|p| p.matches(ch)))
            .cloned()
            .collect()
    }

    /// Get subscriber counts for specific sharded channels.
    pub fn shard_numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, usize)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .sharded_subs
                    .get(ch)
                    .map(|conns| conns.len())
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    // =========================================================================
    // Shard-level threshold monitoring
    // =========================================================================

    /// Total subscription count across all types (channel + pattern + sharded).
    pub fn total_subscription_count(&self) -> usize {
        let channel_total: usize = self.channel_subs.values().map(|m| m.len()).sum();
        let sharded_total: usize = self.sharded_subs.values().map(|m| m.len()).sum();
        channel_total + self.pattern_subs.len() + sharded_total
    }

    /// Number of unique broadcast channels with at least one subscriber.
    pub fn unique_channel_count(&self) -> usize {
        self.channel_subs.len()
    }

    /// Number of unique patterns across all connections.
    pub fn unique_pattern_count(&self) -> usize {
        self.pattern_subs
            .iter()
            .map(|(p, _, _, _)| p)
            .collect::<HashSet<_>>()
            .len()
    }

    /// Check shard-level 90% thresholds after a subscribe operation.
    /// Logs a warning and emits a metric for each threshold crossed.
    pub fn check_thresholds_after_subscribe(
        &mut self,
        shard_id: usize,
        metrics: &Arc<dyn MetricsRecorder>,
    ) {
        if self.warned_total_90 && self.warned_channels_90 && self.warned_patterns_90 {
            return;
        }

        let total_threshold = MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD * 9 / 10;
        if !self.warned_total_90 {
            let total = self.total_subscription_count();
            if total >= total_threshold {
                self.warned_total_90 = true;
                tracing::warn!(
                    shard_id,
                    current = total,
                    limit = MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD,
                    "Shard approaching total subscription limit (90%)"
                );
                PubsubShardLimitWarnings::inc(&**metrics, PubsubLimitResource::TotalSubscriptions);
            }
        }

        let channel_threshold = MAX_UNIQUE_CHANNELS_PER_SHARD * 9 / 10;
        if !self.warned_channels_90 {
            let channels = self.unique_channel_count();
            if channels >= channel_threshold {
                self.warned_channels_90 = true;
                tracing::warn!(
                    shard_id,
                    current = channels,
                    limit = MAX_UNIQUE_CHANNELS_PER_SHARD,
                    "Shard approaching unique channel limit (90%)"
                );
                PubsubShardLimitWarnings::inc(&**metrics, PubsubLimitResource::UniqueChannels);
            }
        }

        let pattern_threshold = MAX_UNIQUE_PATTERNS_PER_SHARD * 9 / 10;
        if !self.warned_patterns_90 {
            let patterns = self.unique_pattern_count();
            if patterns >= pattern_threshold {
                self.warned_patterns_90 = true;
                tracing::warn!(
                    shard_id,
                    current = patterns,
                    limit = MAX_UNIQUE_PATTERNS_PER_SHARD,
                    "Shard approaching unique pattern limit (90%)"
                );
                PubsubShardLimitWarnings::inc(&**metrics, PubsubLimitResource::UniquePatterns);
            }
        }
    }

    /// Reset threshold warning flags if counts have dropped below 90%.
    pub fn reset_thresholds_if_needed(&mut self) {
        if self.warned_total_90
            && self.total_subscription_count() < MAX_TOTAL_SUBSCRIPTIONS_PER_SHARD * 9 / 10
        {
            self.warned_total_90 = false;
        }
        if self.warned_channels_90
            && self.unique_channel_count() < MAX_UNIQUE_CHANNELS_PER_SHARD * 9 / 10
        {
            self.warned_channels_90 = false;
        }
        if self.warned_patterns_90
            && self.unique_pattern_count() < MAX_UNIQUE_PATTERNS_PER_SHARD * 9 / 10
        {
            self.warned_patterns_90 = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    // =========================================================================
    // Output-buffer bound (slow-subscriber DoS) tests
    // =========================================================================

    fn msg_of(payload_len: usize) -> PubSubMessage {
        PubSubMessage::Message {
            channel: Bytes::from_static(b"ch"),
            payload: Bytes::from(vec![b'x'; payload_len]),
        }
    }

    #[test]
    fn output_budget_bounds_a_stalled_subscriber() {
        // A subscriber that never drains must not let the sender buffer without
        // bound. With a small hard limit, queued bytes stay bounded and excess
        // sends are dropped (still `Ok` — counted as delivered, Redis-style),
        // with the overflow flag latched for the connection task to disconnect.
        let limit = 4 * 1024;
        let (tx, rx) = PubSubSender::channel(limit);

        let mut dropped_seen = false;
        for _ in 0..10_000 {
            // Every send reports Ok while the receiver is alive, even when the
            // message is dropped for overflow.
            assert!(tx.send(msg_of(256)).is_ok());
            if rx.has_overflowed() {
                dropped_seen = true;
            }
        }

        assert!(rx.has_overflowed(), "overflow flag must latch");
        assert!(dropped_seen);
        assert!(rx.dropped() > 0, "some messages must be dropped");
        // Memory stays bounded: queued bytes never exceed the hard limit plus at
        // most one in-flight message.
        assert!(
            rx.queued_bytes() <= limit + msg_of(256).approx_buffer_size(),
            "queued_bytes {} exceeded bound {}",
            rx.queued_bytes(),
            limit
        );
    }

    #[test]
    fn output_budget_draining_frees_space_and_flag_stays_latched() {
        let limit = 2 * 1024;
        let (tx, mut rx) = PubSubSender::channel(limit);

        // Fill past the limit.
        for _ in 0..1_000 {
            let _ = tx.send(msg_of(256));
        }
        assert!(rx.has_overflowed());
        let filled = rx.queued_bytes();
        assert!(filled > 0);

        // Draining releases the byte budget.
        let mut drained = 0;
        while rx.try_recv().is_ok() {
            drained += 1;
        }
        assert!(drained > 0);
        assert_eq!(rx.queued_bytes(), 0, "drain must return budget to zero");
        // The latch is sticky: overflow already happened, disconnect is pending.
        assert!(rx.has_overflowed());

        // After draining there is room again, so a fresh send enqueues.
        assert!(tx.send(msg_of(256)).is_ok());
        assert!(rx.queued_bytes() > 0);
    }

    #[test]
    fn unbounded_channel_never_overflows() {
        // `unbounded()` (hard_limit == 0) preserves the old behavior: no bound,
        // no drops. Used by the internal keyspace-notification hop, which is
        // already bounded upstream and must not be double-capped here.
        let (tx, rx) = PubSubSender::unbounded();
        for _ in 0..5_000 {
            assert!(tx.send(msg_of(512)).is_ok());
        }
        assert!(!rx.has_overflowed());
        assert_eq!(rx.dropped(), 0);
    }

    #[test]
    fn send_reports_closed_when_receiver_dropped() {
        let (tx, rx) = PubSubSender::channel(1024);
        drop(rx);
        assert!(matches!(tx.send(msg_of(16)), Err(PubSubClosed)));
    }

    #[tokio::test]
    async fn recv_or_overflow_surfaces_overflow_on_an_empty_channel() {
        // The production deadlock this guards against: a hostile publisher floods
        // past the limit, the excess is dropped to keep memory bounded, and the
        // subscriber then drains the bounded prefix — leaving the channel EMPTY
        // with overflow latched. A bare `recv` would park forever and the slow
        // subscriber would never be disconnected. `recv_or_overflow` must instead
        // surface the latch so the connection task can tear it down.
        let limit = 1024;
        let (tx, mut rx) = PubSubSender::channel(limit);
        for _ in 0..1_000 {
            let _ = tx.send(msg_of(256));
        }
        assert!(rx.has_overflowed());
        while rx.try_recv().is_ok() {}
        assert_eq!(rx.queued_bytes(), 0, "prefix must be fully drained");

        match tokio::time::timeout(Duration::from_secs(1), rx.recv_or_overflow()).await {
            Ok(Some(Drained::Overflowed)) => {}
            other => panic!("expected Overflowed on empty overflowed channel, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn recv_or_overflow_prefers_draining_a_queued_message() {
        // With a message available, drain it (releasing budget) rather than
        // reporting overflow, even on an unbounded channel that never overflows.
        let (tx, mut rx) = PubSubSender::unbounded();
        tx.send(msg_of(16)).unwrap();
        match rx.recv_or_overflow().await {
            Some(Drained::Message(_)) => {}
            other => panic!("expected Message, got {other:?}"),
        }
        assert_eq!(rx.queued_bytes(), 0);
    }

    // =========================================================================
    // GlobPattern tests
    // =========================================================================

    #[test]
    fn test_glob_exact_match() {
        let pattern = GlobPattern::new(Bytes::from_static(b"hello"));
        assert!(pattern.matches(b"hello"));
        assert!(!pattern.matches(b"hello!"));
        assert!(!pattern.matches(b"hell"));
        assert!(!pattern.matches(b""));
    }

    #[test]
    fn test_glob_star() {
        let pattern = GlobPattern::new(Bytes::from_static(b"h*o"));
        assert!(pattern.matches(b"ho"));
        assert!(pattern.matches(b"heo"));
        assert!(pattern.matches(b"hello"));
        assert!(!pattern.matches(b"helloX"));
    }

    #[test]
    fn test_glob_star_prefix() {
        let pattern = GlobPattern::new(Bytes::from_static(b"*ello"));
        assert!(pattern.matches(b"ello"));
        assert!(pattern.matches(b"hello"));
        assert!(pattern.matches(b"XXXello"));
        assert!(!pattern.matches(b"elloX"));
    }

    #[test]
    fn test_glob_star_suffix() {
        let pattern = GlobPattern::new(Bytes::from_static(b"hell*"));
        assert!(pattern.matches(b"hell"));
        assert!(pattern.matches(b"hello"));
        assert!(pattern.matches(b"helloworld"));
        assert!(!pattern.matches(b"Xhello"));
    }

    #[test]
    fn test_glob_question_mark() {
        let pattern = GlobPattern::new(Bytes::from_static(b"h?llo"));
        assert!(pattern.matches(b"hello"));
        assert!(pattern.matches(b"hallo"));
        assert!(!pattern.matches(b"hllo"));
        assert!(!pattern.matches(b"heello"));
    }

    #[test]
    fn test_glob_char_class() {
        let pattern = GlobPattern::new(Bytes::from_static(b"h[ae]llo"));
        assert!(pattern.matches(b"hello"));
        assert!(pattern.matches(b"hallo"));
        assert!(!pattern.matches(b"hillo"));
        assert!(!pattern.matches(b"hllo"));
    }

    #[test]
    fn test_glob_negated_char_class() {
        let pattern = GlobPattern::new(Bytes::from_static(b"h[^ae]llo"));
        assert!(!pattern.matches(b"hello"));
        assert!(!pattern.matches(b"hallo"));
        assert!(pattern.matches(b"hillo"));
        assert!(pattern.matches(b"hullo"));
    }

    #[test]
    fn test_glob_escape() {
        let pattern = GlobPattern::new(Bytes::from_static(b"h\\*llo"));
        assert!(pattern.matches(b"h*llo"));
        assert!(!pattern.matches(b"hello"));
    }

    #[test]
    fn test_glob_channel_pattern() {
        // Test Redis-like channel patterns
        let pattern = GlobPattern::new(Bytes::from_static(b"news.*"));
        assert!(pattern.matches(b"news."));
        assert!(pattern.matches(b"news.sports"));
        assert!(pattern.matches(b"news.weather.today"));
        assert!(!pattern.matches(b"news"));
        assert!(!pattern.matches(b"oldnews.sports"));
    }

    #[test]
    fn test_glob_multiple_stars() {
        let pattern = GlobPattern::new(Bytes::from_static(b"*foo*bar*"));
        assert!(pattern.matches(b"foobar"));
        assert!(pattern.matches(b"XXfooYYbarZZ"));
        assert!(pattern.matches(b"fooXbar"));
        assert!(!pattern.matches(b"fobar"));
    }

    // =========================================================================
    // ShardSubscriptions tests
    // =========================================================================

    #[test]
    fn test_subscribe_unsubscribe() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = PubSubSender::unbounded();

        // Subscribe
        assert!(subs.subscribe(Bytes::from_static(b"ch1"), 1, tx.clone()));
        assert!(!subs.subscribe(Bytes::from_static(b"ch1"), 1, tx.clone())); // Already subscribed

        // Different connection can subscribe
        assert!(subs.subscribe(Bytes::from_static(b"ch1"), 2, tx.clone()));

        // Unsubscribe
        assert!(subs.unsubscribe(&Bytes::from_static(b"ch1"), 1));
        assert!(!subs.unsubscribe(&Bytes::from_static(b"ch1"), 1)); // Already unsubscribed
    }

    #[test]
    fn test_psubscribe_punsubscribe() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = PubSubSender::unbounded();

        assert!(subs.psubscribe(Bytes::from_static(b"news.*"), 1, tx.clone()));
        assert!(!subs.psubscribe(Bytes::from_static(b"news.*"), 1, tx.clone())); // Already subscribed

        // Different pattern is OK
        assert!(subs.psubscribe(Bytes::from_static(b"weather.*"), 1, tx.clone()));

        assert_eq!(subs.pattern_count(), 2);

        assert!(subs.punsubscribe(&Bytes::from_static(b"news.*"), 1));
        assert!(!subs.punsubscribe(&Bytes::from_static(b"news.*"), 1));

        assert_eq!(subs.pattern_count(), 1);
    }

    #[test]
    fn test_publish_to_channel() {
        let mut subs = ShardSubscriptions::new();
        let (tx1, mut rx1) = PubSubSender::unbounded();
        let (tx2, mut rx2) = PubSubSender::unbounded();

        subs.subscribe(Bytes::from_static(b"ch1"), 1, tx1);
        subs.subscribe(Bytes::from_static(b"ch1"), 2, tx2);

        let count = subs.publish(&Bytes::from_static(b"ch1"), &Bytes::from_static(b"hello"));
        assert_eq!(count, 2);

        // Both receivers should get the message
        let msg1 = rx1.try_recv().unwrap();
        let msg2 = rx2.try_recv().unwrap();

        match (msg1, msg2) {
            (
                PubSubMessage::Message { payload: p1, .. },
                PubSubMessage::Message { payload: p2, .. },
            ) => {
                assert_eq!(p1, Bytes::from_static(b"hello"));
                assert_eq!(p2, Bytes::from_static(b"hello"));
            }
            _ => panic!("Unexpected message type"),
        }
    }

    #[test]
    fn test_publish_to_pattern() {
        let mut subs = ShardSubscriptions::new();
        let (tx, mut rx) = PubSubSender::unbounded();

        subs.psubscribe(Bytes::from_static(b"news.*"), 1, tx);

        let count = subs.publish(
            &Bytes::from_static(b"news.sports"),
            &Bytes::from_static(b"score!"),
        );
        assert_eq!(count, 1);

        match rx.try_recv().unwrap() {
            PubSubMessage::PatternMessage {
                pattern,
                channel,
                payload,
            } => {
                assert_eq!(pattern, Bytes::from_static(b"news.*"));
                assert_eq!(channel, Bytes::from_static(b"news.sports"));
                assert_eq!(payload, Bytes::from_static(b"score!"));
            }
            _ => panic!("Unexpected message type"),
        }

        // Non-matching channel should not deliver
        let count = subs.publish(
            &Bytes::from_static(b"weather.today"),
            &Bytes::from_static(b"sunny"),
        );
        assert_eq!(count, 0);
    }

    #[test]
    fn test_sharded_subscribe_publish() {
        let mut subs = ShardSubscriptions::new();
        let (tx, mut rx) = PubSubSender::unbounded();

        subs.ssubscribe(Bytes::from_static(b"orders"), 1, tx);

        let count = subs.spublish(
            &Bytes::from_static(b"orders"),
            &Bytes::from_static(b"new order"),
        );
        assert_eq!(count, 1);

        match rx.try_recv().unwrap() {
            PubSubMessage::ShardedMessage { channel, payload } => {
                assert_eq!(channel, Bytes::from_static(b"orders"));
                assert_eq!(payload, Bytes::from_static(b"new order"));
            }
            _ => panic!("Unexpected message type"),
        }
    }

    #[test]
    fn test_remove_connection() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = PubSubSender::unbounded();

        subs.subscribe(Bytes::from_static(b"ch1"), 1, tx.clone());
        subs.subscribe(Bytes::from_static(b"ch2"), 1, tx.clone());
        subs.psubscribe(Bytes::from_static(b"news.*"), 1, tx.clone());
        subs.ssubscribe(Bytes::from_static(b"orders"), 1, tx.clone());

        // Different connection
        subs.subscribe(Bytes::from_static(b"ch1"), 2, tx.clone());

        subs.remove_connection(1);

        // Connection 1 subscriptions should be gone
        assert!(subs.get_connection_channels(1).is_empty());
        assert!(subs.get_connection_patterns(1).is_empty());
        assert!(subs.get_connection_sharded_channels(1).is_empty());

        // Connection 2 should still have ch1
        assert_eq!(
            subs.get_connection_channels(2),
            vec![Bytes::from_static(b"ch1")]
        );
    }

    #[test]
    fn test_channels_introspection() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = PubSubSender::unbounded();

        subs.subscribe(Bytes::from_static(b"news.sports"), 1, tx.clone());
        subs.subscribe(Bytes::from_static(b"news.weather"), 1, tx.clone());
        subs.subscribe(Bytes::from_static(b"alerts"), 1, tx.clone());

        let all = subs.channels(None);
        assert_eq!(all.len(), 3);

        let pattern = GlobPattern::new(Bytes::from_static(b"news.*"));
        let filtered = subs.channels(Some(&pattern));
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_numsub_introspection() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = PubSubSender::unbounded();

        subs.subscribe(Bytes::from_static(b"ch1"), 1, tx.clone());
        subs.subscribe(Bytes::from_static(b"ch1"), 2, tx.clone());
        subs.subscribe(Bytes::from_static(b"ch2"), 1, tx.clone());

        let result = subs.numsub(&[
            Bytes::from_static(b"ch1"),
            Bytes::from_static(b"ch2"),
            Bytes::from_static(b"ch3"),
        ]);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (Bytes::from_static(b"ch1"), 2));
        assert_eq!(result[1], (Bytes::from_static(b"ch2"), 1));
        assert_eq!(result[2], (Bytes::from_static(b"ch3"), 0));
    }

    #[test]
    fn test_pubsub_message_to_response() {
        let msg = PubSubMessage::Message {
            channel: Bytes::from_static(b"test"),
            payload: Bytes::from_static(b"hello"),
        };
        let resp = msg.to_response();

        match resp {
            Response::Array(arr) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("Expected array response"),
        }
    }

    #[test]
    fn test_confirmation_shape_by_protocol() {
        let confirm = PubSubConfirmation::Subscribe {
            channel: Bytes::from_static(b"ch"),
            count: 1,
        };

        // RESP2 -> Array.
        match confirm.to_response(ProtocolVersion::Resp2) {
            Response::Array(items) => {
                assert_eq!(items[0], Response::bulk(Bytes::from_static(b"subscribe")));
                assert_eq!(items[1], Response::bulk(Bytes::from_static(b"ch")));
                assert_eq!(items[2], Response::Integer(1));
            }
            other => panic!("expected Array, got {other:?}"),
        }

        // RESP3 -> Push with the same items.
        match confirm.to_response(ProtocolVersion::Resp3) {
            Response::Push(items) => {
                assert_eq!(items[0], Response::bulk(Bytes::from_static(b"subscribe")));
                assert_eq!(items[2], Response::Integer(1));
            }
            other => panic!("expected Push, got {other:?}"),
        }
    }

    #[test]
    fn test_confirmation_null_channel_when_empty() {
        // UNSUBSCRIBE with no active subscriptions replies with a null channel.
        let confirm = PubSubConfirmation::Unsubscribe {
            channel: None,
            count: 0,
        };
        for proto in [ProtocolVersion::Resp2, ProtocolVersion::Resp3] {
            let items = match confirm.to_response(proto) {
                Response::Array(items) | Response::Push(items) => items,
                other => panic!("expected Array/Push, got {other:?}"),
            };
            assert_eq!(items[0], Response::bulk(Bytes::from_static(b"unsubscribe")));
            assert_eq!(items[1], Response::null());
            assert_eq!(items[2], Response::Integer(0));
        }
    }

    #[test]
    fn test_confirmation_delivered_via_message() {
        // The out-of-band delivery path (slot drain) routes through the same rule.
        let msg = PubSubMessage::Confirmation(PubSubConfirmation::SUnsubscribe {
            channel: Some(Bytes::from_static(b"ch")),
            count: 2,
        });
        assert!(matches!(
            msg.to_response_with_protocol(ProtocolVersion::Resp3),
            Response::Push(_)
        ));
        assert!(matches!(
            msg.to_response_with_protocol(ProtocolVersion::Resp2),
            Response::Array(_)
        ));
    }

    // =========================================================================
    // Counting and threshold tests
    // =========================================================================

    #[test]
    fn test_total_subscription_count() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = PubSubSender::unbounded();

        assert_eq!(subs.total_subscription_count(), 0);

        // 2 channel subs on ch1 (conn 1 and 2), 1 on ch2 = 3
        subs.subscribe(Bytes::from_static(b"ch1"), 1, tx.clone());
        subs.subscribe(Bytes::from_static(b"ch1"), 2, tx.clone());
        subs.subscribe(Bytes::from_static(b"ch2"), 1, tx.clone());
        assert_eq!(subs.total_subscription_count(), 3);

        // 1 pattern sub
        subs.psubscribe(Bytes::from_static(b"news.*"), 1, tx.clone());
        assert_eq!(subs.total_subscription_count(), 4);

        // 1 sharded sub
        subs.ssubscribe(Bytes::from_static(b"orders"), 1, tx.clone());
        assert_eq!(subs.total_subscription_count(), 5);
    }

    #[test]
    fn test_unique_channel_count() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = PubSubSender::unbounded();

        assert_eq!(subs.unique_channel_count(), 0);

        subs.subscribe(Bytes::from_static(b"ch1"), 1, tx.clone());
        subs.subscribe(Bytes::from_static(b"ch1"), 2, tx.clone()); // same channel
        assert_eq!(subs.unique_channel_count(), 1);

        subs.subscribe(Bytes::from_static(b"ch2"), 1, tx.clone());
        assert_eq!(subs.unique_channel_count(), 2);
    }

    #[test]
    fn test_unique_pattern_count() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = PubSubSender::unbounded();

        assert_eq!(subs.unique_pattern_count(), 0);

        // Same pattern from two connections = 1 unique pattern
        subs.psubscribe(Bytes::from_static(b"news.*"), 1, tx.clone());
        subs.psubscribe(Bytes::from_static(b"news.*"), 2, tx.clone());
        assert_eq!(subs.unique_pattern_count(), 1);

        subs.psubscribe(Bytes::from_static(b"weather.*"), 1, tx.clone());
        assert_eq!(subs.unique_pattern_count(), 2);
    }

    #[test]
    fn test_threshold_reset() {
        let mut subs = ShardSubscriptions::new();

        // Manually set flags
        subs.warned_total_90 = true;
        subs.warned_channels_90 = true;
        subs.warned_patterns_90 = true;

        // With no subscriptions, all are below 90% — should reset
        subs.reset_thresholds_if_needed();

        assert!(!subs.warned_total_90);
        assert!(!subs.warned_channels_90);
        assert!(!subs.warned_patterns_90);
    }
}
