//! Pub/sub delivery oracle (Phase 4c).
//!
//! Regular `PUBLISH` / `SUBSCRIBE` / `PSUBSCRIBE` traffic is **not**
//! per-key-linearizability-checkable: a channel has no value-state a WGL model
//! could step, and delivery is a fan-out side effect on subscriber connections
//! rather than a request/reply against a key. This module is its own oracle: a
//! [`PubSubHistory`] of bracketed subscribe/publish/receive events plus two
//! whole-history checkers.
//!
//! # Delivery guarantee asserted
//!
//! The server-side facts this oracle relies on (confirmed against the code —
//! `crates/core/src/pubsub.rs`, `crates/core/src/shard/dispatch_pubsub.rs`,
//! `crates/server/src/connection/pubsub_conn_command.rs`):
//!
//! - Every broadcast SUBSCRIBE registers on the single coordinator shard
//!   (`BROADCAST_SHARD = 0`) and the client's subscribe **confirmation is only
//!   sent after registration completes** — so a PUBLISH the server processes
//!   after a subscriber has *received* its confirmation is guaranteed to see
//!   that subscription.
//! - Regular PUBLISH also routes to shard 0 and delivers **synchronously**
//!   (within the PUBLISH call, before its integer reply) into each live
//!   subscriber's **unbounded** mpsc queue. There is no `try_send`/drop on the
//!   regular-PUBLISH path: a message is delivered to a live subscriber's queue
//!   or the subscriber's connection is already gone. (The only dropping path in
//!   the codebase is the *cross-shard keyspace-notification* hop, which regular
//!   PUBLISH never takes.)
//!
//! Therefore the oracle asserts, per (subscriber, subscription, channel):
//!
//! - **Exactly-once** for a message whose PUBLISH is *firmly in the confirmed
//!   window*: the subscriber's confirmation was recorded before the PUBLISH was
//!   sent (`open < publish.send`) **and** the PUBLISH's integer reply was
//!   recorded before the subscriber began to unsubscribe / tear down
//!   (`publish.reply < close`). Both edges use the global recording sequence,
//!   which under the deterministic sim is a linearization of the real
//!   interleaving.
//! - **At-most-once** for a message racing either edge (published before the
//!   confirmation was observed, or after the unsubscribe began): 0 or 1
//!   deliveries, never 2. This is the conservative bracket — we never *require*
//!   delivery of a message we cannot prove the server processed while the
//!   subscription was certainly live, but we still forbid duplication.
//! - **No phantom**: every received message must correspond to a real PUBLISH
//!   on the same channel.
//! - **Per-publisher order**: messages from one publisher to one channel arrive
//!   at each subscriber in publish (send) order — never reordered.
//!
//! Message tokens are **globally unique** by construction (the generator tags
//! each with its publisher id and a per-publisher counter), so a received
//! message identifies exactly one PUBLISH without value-collision ambiguity —
//! the same discipline the list exactly-once checker relies on.

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::HashMap;

// ============================================================================
// Seeded workload generator
// ============================================================================

/// The fixed, global channel space a pub/sub workload draws from. Regular
/// PUBLISH is NOT sharded (all broadcast traffic funnels through the single
/// coordinator shard), so channels carry no hash tag — unlike the keyed
/// workload generator, shard placement is irrelevant here.
fn channel_space() -> Vec<Bytes> {
    (0..3).map(|i| Bytes::from(format!("ch{i}"))).collect()
}

/// The glob pattern the pattern-subscriber uses — matches every `ch*` channel.
const WORKLOAD_PATTERN: &str = "ch*";

/// One subscriber's plan: the channels it `SUBSCRIBE`s and patterns it
/// `PSUBSCRIBE`s, plus how long it thinks before subscribing (small, so it is
/// registered before most publishes).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubscriberScript {
    /// Subscriber client id.
    pub client_id: u64,
    /// Exact channels to SUBSCRIBE.
    #[serde(with = "crate::history::bytes_vec_serde_pub")]
    pub channels: Vec<Bytes>,
    /// Glob patterns to PSUBSCRIBE (may be empty).
    #[serde(with = "crate::history::bytes_vec_serde_pub")]
    pub patterns: Vec<Bytes>,
    /// Sim-ms to think before subscribing.
    pub subscribe_think_ms: u64,
}

/// One PUBLISH: a target channel, a globally-unique message token, and a
/// pre-publish think delay.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PubMsg {
    /// Target channel.
    #[serde(with = "crate::history::bytes_serde_pub")]
    pub channel: Bytes,
    /// Globally-unique message token (`m{pub_client}-{n}`).
    #[serde(with = "crate::history::bytes_serde_pub")]
    pub message: Bytes,
    /// Sim-ms to think before issuing this PUBLISH.
    pub think_ms: u64,
}

/// One publisher's plan: an initial delay (some publishers start near t=0 to
/// race the subscribe edge; others delay so their publishes land firmly
/// in-window) followed by a sequence of unique PUBLISHes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublisherScript {
    /// Publisher client id.
    pub client_id: u64,
    /// Sim-ms before the first PUBLISH (edge-race knob).
    pub start_delay_ms: u64,
    /// The ordered PUBLISHes this publisher issues.
    pub messages: Vec<PubMsg>,
}

/// A complete seeded pub/sub workload. Deterministic: same
/// `(seed, num_subscribers, num_publishers, msgs_per_publisher)` ⇒
/// byte-identical workload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PubSubWorkload {
    /// Generating seed.
    pub seed: u64,
    /// The channel space.
    #[serde(with = "crate::history::bytes_vec_serde_pub")]
    pub channels: Vec<Bytes>,
    /// Subscriber plans.
    pub subscribers: Vec<SubscriberScript>,
    /// Publisher plans.
    pub publishers: Vec<PublisherScript>,
}

impl PubSubWorkload {
    /// Generate a deterministic workload. Client ids: subscribers take
    /// `0..num_subscribers`, publishers take
    /// `num_subscribers..num_subscribers + num_publishers` (disjoint, so the
    /// history's `sub_client`/`pub_client` never collide).
    pub fn generate(
        seed: u64,
        num_subscribers: usize,
        num_publishers: usize,
        msgs_per_publisher: usize,
    ) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let channels = channel_space();

        let mut subscribers = Vec::with_capacity(num_subscribers);
        for client_id in 0..num_subscribers as u64 {
            // Subscribe to a random non-empty subset of channels.
            let mut subs: Vec<Bytes> = channels
                .iter()
                .filter(|_| rng.random_range(0..100) < 60)
                .cloned()
                .collect();
            if subs.is_empty() {
                subs.push(channels[rng.random_range(0..channels.len())].clone());
            }
            // The last subscriber also PSUBSCRIBEs the catch-all pattern, so
            // pattern-delivery conservation is exercised every run.
            let patterns = if client_id as usize == num_subscribers.saturating_sub(1) {
                vec![Bytes::from(WORKLOAD_PATTERN)]
            } else {
                Vec::new()
            };
            subscribers.push(SubscriberScript {
                client_id,
                channels: subs,
                patterns,
                // Small pre-subscribe think so subscriptions register early.
                subscribe_think_ms: rng.random_range(0..15),
            });
        }

        let mut publishers = Vec::with_capacity(num_publishers);
        for p in 0..num_publishers {
            let client_id = (num_subscribers + p) as u64;
            // Half the publishers start near t=0 (racing the subscribe edge);
            // the rest delay so their traffic is firmly in-window.
            let start_delay_ms = if p % 2 == 0 {
                rng.random_range(0..10)
            } else {
                rng.random_range(120..260)
            };
            let mut messages = Vec::with_capacity(msgs_per_publisher);
            for n in 0..msgs_per_publisher {
                let channel = channels[rng.random_range(0..channels.len())].clone();
                let message = Bytes::from(format!("m{client_id}-{n}"));
                messages.push(PubMsg {
                    channel,
                    message,
                    think_ms: rng.random_range(0..40),
                });
            }
            publishers.push(PublisherScript {
                client_id,
                start_delay_ms,
                messages,
            });
        }

        PubSubWorkload {
            seed,
            channels,
            subscribers,
            publishers,
        }
    }
}

/// What a subscription covers: an exact channel or a glob pattern.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SubKind {
    /// `SUBSCRIBE <channel>` — delivers `message` frames for that exact channel.
    Channel(#[serde(with = "crate::history::bytes_serde_pub")] Bytes),
    /// `PSUBSCRIBE <pattern>` — delivers `pmessage` frames for any channel the
    /// glob pattern matches.
    Pattern(#[serde(with = "crate::history::bytes_serde_pub")] Bytes),
}

impl SubKind {
    /// Does this subscription cover a publish to `channel`?
    fn covers(&self, channel: &Bytes) -> bool {
        match self {
            SubKind::Channel(c) => c == channel,
            SubKind::Pattern(p) => glob_match(p, channel),
        }
    }

    /// The pattern bytes when this is a pattern subscription (a receive from a
    /// pattern sub carries the matched pattern; a channel sub carries none).
    fn pattern(&self) -> Option<&Bytes> {
        match self {
            SubKind::Channel(_) => None,
            SubKind::Pattern(p) => Some(p),
        }
    }
}

/// Minimal Redis-style glob matcher (`*`, `?`, literal). Self-contained so the
/// oracle has no dependency on the server crate; the workload only ever uses
/// trailing-`*` patterns, but the full matcher keeps the checker honest.
fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
    // Iterative backtracking matcher, O(n*m) worst case, no recursion blowup.
    let (mut p, mut c) = (0usize, 0usize);
    let (mut star_p, mut star_c): (Option<usize>, usize) = (None, 0);
    while c < s.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == s[c]) {
            p += 1;
            c += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star_p = Some(p);
            star_c = c;
            p += 1;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            star_c += 1;
            c = star_c;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

/// One recorded pub/sub event. Every event carries a global monotonic `seq`
/// assigned in recording order; under the deterministic turmoil sim that order
/// is a linearization of the real cross-connection interleaving.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PubSubEvent {
    /// A subscriber read its SUBSCRIBE/PSUBSCRIBE confirmation: the subscription
    /// is now registered server-side (the ack is sent only after registration).
    /// Opens the delivery window for `(sub_client, kind)`.
    SubscribeAck {
        /// Subscriber client id.
        sub_client: u64,
        /// What was subscribed.
        kind: SubKind,
        /// Global recording sequence.
        seq: u64,
    },
    /// A subscriber is about to send UNSUBSCRIBE / tear down its connection —
    /// the earliest point the subscription may deactivate. Closes the window.
    Unsubscribe {
        /// Subscriber client id.
        sub_client: u64,
        /// What is being unsubscribed.
        kind: SubKind,
        /// Global recording sequence.
        seq: u64,
    },
    /// A publisher issued `PUBLISH channel message`. `send_seq` is stamped
    /// before the command is written; `reply_seq` after its integer reply is
    /// read (so the server has finished delivering to every live subscriber).
    Publish {
        /// Publisher client id.
        pub_client: u64,
        /// Target channel.
        #[serde(with = "crate::history::bytes_serde_pub")]
        channel: Bytes,
        /// Globally-unique message token.
        #[serde(with = "crate::history::bytes_serde_pub")]
        message: Bytes,
        /// Recording sequence stamped before the command was written.
        send_seq: u64,
        /// Recording sequence stamped after the integer reply was read.
        reply_seq: u64,
    },
    /// A subscriber read a `message` (channel sub) or `pmessage` (pattern sub)
    /// frame off its connection.
    Receive {
        /// Subscriber client id.
        sub_client: u64,
        /// The channel the frame carried.
        #[serde(with = "crate::history::bytes_serde_pub")]
        channel: Bytes,
        /// The message payload (a unique publish token).
        #[serde(with = "crate::history::bytes_serde_pub")]
        message: Bytes,
        /// `Some(pattern)` for a `pmessage`, `None` for a `message`.
        #[serde(with = "crate::history::bytes_option_serde_pub")]
        pattern: Option<Bytes>,
        /// Global recording sequence.
        seq: u64,
    },
}

/// A pub/sub history: an append-only event log with a monotonic sequence.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PubSubHistory {
    events: Vec<PubSubEvent>,
    #[serde(skip)]
    next_seq: u64,
}

impl PubSubHistory {
    /// Create an empty history.
    pub fn new() -> Self {
        Self::default()
    }

    fn bump(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    /// Record that `sub_client` observed its confirmation for `kind`.
    pub fn record_subscribe_ack(&mut self, sub_client: u64, kind: SubKind) -> u64 {
        let seq = self.bump();
        self.events.push(PubSubEvent::SubscribeAck {
            sub_client,
            kind,
            seq,
        });
        seq
    }

    /// Record that `sub_client` began unsubscribing `kind`.
    pub fn record_unsubscribe(&mut self, sub_client: u64, kind: SubKind) -> u64 {
        let seq = self.bump();
        self.events.push(PubSubEvent::Unsubscribe {
            sub_client,
            kind,
            seq,
        });
        seq
    }

    /// Record a PUBLISH send, returning the event index to later stamp its
    /// reply via [`Self::record_publish_reply`].
    pub fn record_publish_send(
        &mut self,
        pub_client: u64,
        channel: Bytes,
        message: Bytes,
    ) -> usize {
        let send_seq = self.bump();
        let idx = self.events.len();
        self.events.push(PubSubEvent::Publish {
            pub_client,
            channel,
            message,
            send_seq,
            // Provisional; overwritten by record_publish_reply. Until then it
            // equals send_seq (a zero-width window), which the checker treats
            // as "reply not yet observed".
            reply_seq: send_seq,
        });
        idx
    }

    /// Stamp the reply sequence for a previously-recorded PUBLISH.
    pub fn record_publish_reply(&mut self, idx: usize) {
        let seq = self.bump();
        if let Some(PubSubEvent::Publish { reply_seq, .. }) = self.events.get_mut(idx) {
            *reply_seq = seq;
        }
    }

    /// Record a delivered frame read by `sub_client`.
    pub fn record_receive(
        &mut self,
        sub_client: u64,
        channel: Bytes,
        message: Bytes,
        pattern: Option<Bytes>,
    ) -> u64 {
        let seq = self.bump();
        self.events.push(PubSubEvent::Receive {
            sub_client,
            channel,
            message,
            pattern,
            seq,
        });
        seq
    }

    /// All recorded events.
    pub fn events(&self) -> &[PubSubEvent] {
        &self.events
    }

    /// Number of `Receive` events (a run's smoke test asserts this is non-zero
    /// so a broken reader that captures nothing fails loudly instead of
    /// silent-greening the conservation checker).
    pub fn receive_count(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, PubSubEvent::Receive { .. }))
            .count()
    }

    /// Number of `Publish` events.
    pub fn publish_count(&self) -> usize {
        self.events
            .iter()
            .filter(|e| matches!(e, PubSubEvent::Publish { .. }))
            .count()
    }

    /// Export to JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Import from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// A pub/sub oracle violation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PubSubViolation {
    /// A message firmly in the confirmed window was not delivered.
    #[error(
        "lost delivery: subscriber {sub_client} ({kind:?}) missed message {message:?} on channel {channel:?} (published firmly in-window)"
    )]
    LostDelivery {
        /// Subscriber that missed it.
        sub_client: u64,
        /// The subscription that should have delivered it.
        kind: SubKind,
        /// Channel.
        channel: Vec<u8>,
        /// Message token.
        message: Vec<u8>,
    },
    /// A message was delivered more than once to one subscription.
    #[error(
        "duplicate delivery: subscriber {sub_client} ({kind:?}) received message {message:?} on channel {channel:?} {times} times"
    )]
    DuplicateDelivery {
        /// Subscriber.
        sub_client: u64,
        /// The subscription.
        kind: SubKind,
        /// Channel.
        channel: Vec<u8>,
        /// Message token.
        message: Vec<u8>,
        /// Observed delivery count (>1).
        times: usize,
    },
    /// A subscriber received a message that was never published to that channel.
    #[error(
        "phantom delivery: subscriber {sub_client} received message {message:?} on channel {channel:?} that was never published there"
    )]
    PhantomDelivery {
        /// Subscriber.
        sub_client: u64,
        /// Channel the frame claimed.
        channel: Vec<u8>,
        /// Message token.
        message: Vec<u8>,
    },
    /// Messages from one publisher on one channel arrived out of publish order.
    #[error(
        "order violated: subscriber {sub_client} received publisher {pub_client}'s messages on channel {channel:?} out of publish order (message {later:?} before {earlier:?})"
    )]
    OrderViolation {
        /// Subscriber.
        sub_client: u64,
        /// Publisher whose stream was reordered.
        pub_client: u64,
        /// Channel.
        channel: Vec<u8>,
        /// The message received earlier that was published later.
        later: Vec<u8>,
        /// The message received later that was published earlier.
        earlier: Vec<u8>,
    },
}

/// Resolved facts about a single PUBLISH, keyed by its unique message token.
#[derive(Debug, Clone)]
struct PublishInfo {
    pub_client: u64,
    channel: Bytes,
    send_seq: u64,
    reply_seq: u64,
}

/// A subscriber's confirmed subscription window for one `SubKind`.
#[derive(Debug, Clone)]
struct Window {
    open: u64,
    close: u64,
}

/// Build the `message -> PublishInfo` index (messages are globally unique).
fn index_publishes(h: &PubSubHistory) -> HashMap<Bytes, PublishInfo> {
    let mut m = HashMap::new();
    for e in h.events() {
        if let PubSubEvent::Publish {
            pub_client,
            channel,
            message,
            send_seq,
            reply_seq,
        } = e
        {
            m.insert(
                message.clone(),
                PublishInfo {
                    pub_client: *pub_client,
                    channel: channel.clone(),
                    send_seq: *send_seq,
                    reply_seq: *reply_seq,
                },
            );
        }
    }
    m
}

/// Build `(sub_client, SubKind) -> Window`. First ack opens; first later
/// unsubscribe closes (else `u64::MAX`, i.e. subscribed through the run).
fn build_windows(h: &PubSubHistory) -> HashMap<(u64, SubKind), Window> {
    let mut w: HashMap<(u64, SubKind), Window> = HashMap::new();
    for e in h.events() {
        match e {
            PubSubEvent::SubscribeAck {
                sub_client,
                kind,
                seq,
            } => {
                w.entry((*sub_client, kind.clone())).or_insert(Window {
                    open: *seq,
                    close: u64::MAX,
                });
            }
            PubSubEvent::Unsubscribe {
                sub_client,
                kind,
                seq,
            } => {
                if let Some(win) = w.get_mut(&(*sub_client, kind.clone()))
                    && win.close == u64::MAX
                    && *seq > win.open
                {
                    win.close = *seq;
                }
            }
            _ => {}
        }
    }
    w
}

/// Count `Receive`s by `sub_client` on `channel` of `message`, split by whether
/// the frame was a `pmessage` from `pattern` (pattern sub) or a plain `message`
/// (channel sub) — so a channel sub and a pattern sub on the same subscriber are
/// accounted independently.
fn count_receives(
    h: &PubSubHistory,
    sub_client: u64,
    channel: &Bytes,
    message: &Bytes,
    want_pattern: Option<&Bytes>,
) -> usize {
    h.events()
        .iter()
        .filter(|e| match e {
            PubSubEvent::Receive {
                sub_client: sc,
                channel: ch,
                message: m,
                pattern,
                ..
            } => {
                *sc == sub_client
                    && ch == channel
                    && m == message
                    && pattern.as_ref() == want_pattern
            }
            _ => false,
        })
        .count()
}

/// Per-channel delivery conservation: exactly-once for firmly-in-window
/// publishes, at-most-once for edge races, and no phantom deliveries. See the
/// module docs for the precise guarantee and its edge-window semantics.
pub fn check_pubsub_conservation(h: &PubSubHistory) -> Result<(), PubSubViolation> {
    let publishes = index_publishes(h);
    let windows = build_windows(h);

    // (1) No phantom: every received token was really published to that
    // channel. Checked first — it is an independent invariant, and a receive
    // whose token was rewritten to a phantom would otherwise surface as a
    // spurious LostDelivery for the real (now-unreceived) message.
    for e in h.events() {
        if let PubSubEvent::Receive {
            sub_client,
            channel,
            message,
            ..
        } = e
        {
            match publishes.get(message) {
                Some(info) if info.channel == *channel => {}
                _ => {
                    return Err(PubSubViolation::PhantomDelivery {
                        sub_client: *sub_client,
                        channel: channel.to_vec(),
                        message: message.to_vec(),
                    });
                }
            }
        }
    }

    // (2) Conservation per (subscription window, covered publish).
    for ((sub_client, kind), win) in &windows {
        let want_pattern = kind.pattern();
        for (message, info) in &publishes {
            if !kind.covers(&info.channel) {
                continue;
            }
            let times = count_receives(h, *sub_client, &info.channel, message, want_pattern);
            let firm = win.open < info.send_seq && info.reply_seq < win.close;
            if firm {
                if times == 0 {
                    return Err(PubSubViolation::LostDelivery {
                        sub_client: *sub_client,
                        kind: kind.clone(),
                        channel: info.channel.to_vec(),
                        message: message.to_vec(),
                    });
                }
                if times > 1 {
                    return Err(PubSubViolation::DuplicateDelivery {
                        sub_client: *sub_client,
                        kind: kind.clone(),
                        channel: info.channel.to_vec(),
                        message: message.to_vec(),
                        times,
                    });
                }
            } else if times > 1 {
                return Err(PubSubViolation::DuplicateDelivery {
                    sub_client: *sub_client,
                    kind: kind.clone(),
                    channel: info.channel.to_vec(),
                    message: message.to_vec(),
                    times,
                });
            }
        }
    }

    Ok(())
}

/// A `(publisher, channel)` publish stream, or `(subscriber, channel,
/// publisher)` receive stream, key.
type StreamKey = (u64, Bytes);
/// `(recv_seq, publish_rank, message)` for one delivered message.
type ReceiveEntry = (u64, usize, Bytes);
/// A `(subscriber, channel, publisher)` receive-stream key.
type RecvKey = (u64, Bytes, u64);

/// Per-publisher order preservation per channel: for each
/// (subscriber, channel, publisher), the subsequence of received messages must
/// appear in the same order the publisher sent them.
///
/// Deliberately merges `message` and `pmessage` receives into one stream per
/// (subscriber, channel, publisher) — both frame kinds funnel through the
/// same subscriber connection in publish order (channel-subs delivered before
/// pattern-subs for a given publish), so a single FIFO comparison here can
/// neither false-positive nor mask a real reorder.
pub fn check_pubsub_order(h: &PubSubHistory) -> Result<(), PubSubViolation> {
    let publishes = index_publishes(h);

    // Publisher publish order per (pub_client, channel): message -> rank.
    // Rank = ascending send_seq position within that publisher+channel stream.
    let mut streams: HashMap<StreamKey, Vec<(u64, Bytes)>> = HashMap::new();
    for (msg, info) in &publishes {
        streams
            .entry((info.pub_client, info.channel.clone()))
            .or_default()
            .push((info.send_seq, msg.clone()));
    }
    let mut rank: HashMap<Bytes, usize> = HashMap::new();
    for stream in streams.values_mut() {
        stream.sort_by_key(|(seq, _)| *seq);
        for (r, (_, msg)) in stream.iter().enumerate() {
            rank.insert(msg.clone(), r);
        }
    }

    // Per subscriber, per (channel, publisher): the ranks of received messages,
    // in receive-seq order, must be strictly increasing.
    // key: (sub_client, channel, pub_client) -> Vec<(recv_seq, rank, message)>.
    let mut received: HashMap<RecvKey, Vec<ReceiveEntry>> = HashMap::new();
    for e in h.events() {
        if let PubSubEvent::Receive {
            sub_client,
            channel,
            message,
            seq,
            ..
        } = e
        {
            let (Some(info), Some(r)) = (publishes.get(message), rank.get(message)) else {
                continue; // phantom is the conservation checker's job
            };
            if info.channel != *channel {
                continue;
            }
            received
                .entry((*sub_client, channel.clone(), info.pub_client))
                .or_default()
                .push((*seq, *r, message.clone()));
        }
    }

    for ((sub_client, channel, pub_client), mut recvs) in received {
        recvs.sort_by_key(|(seq, _, _)| *seq);
        for w in recvs.windows(2) {
            // w[0] received before w[1]; its publish rank must be <= w[1]'s.
            if w[0].1 > w[1].1 {
                return Err(PubSubViolation::OrderViolation {
                    sub_client,
                    pub_client,
                    channel: channel.to_vec(),
                    later: w[0].2.to_vec(),
                    earlier: w[1].2.to_vec(),
                });
            }
        }
    }

    Ok(())
}

// ============================================================================
// Fault injection (silent-green guard). A correct checker must reject each
// corruption of an otherwise-valid history and accept the original.
// ============================================================================

/// Corruption: drop a `Receive` whose publish is firmly inside the confirmed
/// window — guaranteed to trip [`check_pubsub_conservation`] (LostDelivery),
/// unlike dropping an arbitrary receive (an edge-race delivery is at-most-once,
/// so dropping it is legal and would not be caught). Falls back to the first
/// receive if none is provably firm (e.g. a synthetic history with no windows).
pub fn drop_delivery(h: &PubSubHistory) -> PubSubHistory {
    let publishes = index_publishes(h);
    let windows = build_windows(h);

    // Find the first receive that is firmly in-window for its subscription.
    let firm_pos = h.events().iter().position(|e| {
        let PubSubEvent::Receive {
            sub_client,
            message,
            pattern,
            ..
        } = e
        else {
            return false;
        };
        let kind = match pattern {
            Some(p) => SubKind::Pattern(p.clone()),
            None => {
                // A channel-sub receive: the subscription kind is Channel(that
                // receive's channel).
                let PubSubEvent::Receive { channel, .. } = e else {
                    return false;
                };
                SubKind::Channel(channel.clone())
            }
        };
        let (Some(info), Some(win)) = (publishes.get(message), windows.get(&(*sub_client, kind)))
        else {
            return false;
        };
        win.open < info.send_seq && info.reply_seq < win.close
    });

    let pos = firm_pos.or_else(|| {
        h.events()
            .iter()
            .position(|e| matches!(e, PubSubEvent::Receive { .. }))
    });

    let mut out = h.clone();
    if let Some(pos) = pos {
        out.events.remove(pos);
    }
    out
}

/// Corruption: duplicate the first `Receive`. Trips
/// [`check_pubsub_conservation`] (DuplicateDelivery).
pub fn duplicate_delivery(h: &PubSubHistory) -> PubSubHistory {
    let mut out = h.clone();
    if let Some(recv) = out
        .events
        .iter()
        .find(|e| matches!(e, PubSubEvent::Receive { .. }))
        .cloned()
    {
        out.events.push(recv);
    }
    out
}

/// Corruption: rewrite the first `Receive`'s message token to one never
/// published. Trips [`check_pubsub_conservation`] (PhantomDelivery).
pub fn phantom_delivery(h: &PubSubHistory) -> PubSubHistory {
    let mut out = h.clone();
    for e in out.events.iter_mut() {
        if let PubSubEvent::Receive { message, .. } = e {
            *message = Bytes::from_static(b"__never_published__");
            break;
        }
    }
    out
}

/// Corruption: swap the receive-sequence of two `Receive`s that share a
/// `(sub_client, channel, publisher)` stream but carry **different** messages
/// (hence different publish ranks), so a later-published message is received
/// first. Trips [`check_pubsub_order`] (OrderViolation). Requiring distinct
/// messages matters: a dual channel+pattern subscriber receives the *same*
/// message twice (message + pmessage) at the same rank, and swapping those
/// would create no inversion.
pub fn reorder_delivery(h: &PubSubHistory) -> PubSubHistory {
    let mut out = h.clone();
    let publishes = index_publishes(h);
    // stream key -> first (event index, message) seen; find a second entry in
    // the same stream with a different message and swap the two recv seqs.
    let mut first: HashMap<(u64, Bytes, u64), (usize, Bytes)> = HashMap::new();
    let mut pair: Option<(usize, usize)> = None;
    for (i, e) in out.events.iter().enumerate() {
        if let PubSubEvent::Receive {
            sub_client,
            channel,
            message,
            ..
        } = e
            && let Some(info) = publishes.get(message)
            && info.channel == *channel
        {
            let key = (*sub_client, channel.clone(), info.pub_client);
            match first.get(&key) {
                Some((j, m0)) if m0 != message => {
                    pair = Some((*j, i));
                    break;
                }
                Some(_) => {} // same message (message+pmessage dup): keep looking
                None => {
                    first.insert(key, (i, message.clone()));
                }
            }
        }
    }
    if let Some((a, b)) = pair
        && let (Some(sa), Some(sb)) = (seq_of(&out.events[a]), seq_of(&out.events[b]))
    {
        set_seq(&mut out.events[a], sb);
        set_seq(&mut out.events[b], sa);
    }
    out
}

fn seq_of(e: &PubSubEvent) -> Option<u64> {
    match e {
        PubSubEvent::Receive { seq, .. } => Some(*seq),
        _ => None,
    }
}

fn set_seq(e: &mut PubSubEvent, v: u64) {
    if let PubSubEvent::Receive { seq, .. } = e {
        *seq = v;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    /// A valid history: one channel subscriber, one pattern subscriber, two
    /// publishers each sending two ordered messages, all delivered once.
    fn valid_history() -> PubSubHistory {
        let mut h = PubSubHistory::new();
        // Subscriber 1 subscribes to ch0; subscriber 2 psubscribes ch*.
        h.record_subscribe_ack(1, SubKind::Channel(b("ch0")));
        h.record_subscribe_ack(2, SubKind::Pattern(b("ch*")));
        // Publisher 10 sends p10-0, p10-1; publisher 11 sends p11-0. All firm.
        for (pc, msgs) in [
            (10u64, ["p10-0", "p10-1"].as_slice()),
            (11, ["p11-0"].as_slice()),
        ] {
            for m in msgs {
                let idx = h.record_publish_send(pc, b("ch0"), b(m));
                // Both subscribers receive it in send order.
                h.record_receive(1, b("ch0"), b(m), None);
                h.record_receive(2, b("ch0"), b(m), Some(b("ch*")));
                h.record_publish_reply(idx);
            }
        }
        h.record_unsubscribe(1, SubKind::Channel(b("ch0")));
        h.record_unsubscribe(2, SubKind::Pattern(b("ch*")));
        h
    }

    #[test]
    fn valid_history_passes_both_checkers() {
        let h = valid_history();
        assert_eq!(check_pubsub_conservation(&h), Ok(()));
        assert_eq!(check_pubsub_order(&h), Ok(()));
    }

    #[test]
    fn drop_delivery_is_caught() {
        let h = drop_delivery(&valid_history());
        assert!(matches!(
            check_pubsub_conservation(&h),
            Err(PubSubViolation::LostDelivery { .. })
        ));
    }

    #[test]
    fn duplicate_delivery_is_caught() {
        let h = duplicate_delivery(&valid_history());
        assert!(matches!(
            check_pubsub_conservation(&h),
            Err(PubSubViolation::DuplicateDelivery { .. })
        ));
    }

    #[test]
    fn phantom_delivery_is_caught() {
        let h = phantom_delivery(&valid_history());
        assert!(matches!(
            check_pubsub_conservation(&h),
            Err(PubSubViolation::PhantomDelivery { .. })
        ));
    }

    #[test]
    fn reorder_delivery_is_caught() {
        let h = reorder_delivery(&valid_history());
        assert!(matches!(
            check_pubsub_order(&h),
            Err(PubSubViolation::OrderViolation { .. })
        ));
    }

    #[test]
    fn edge_race_publish_before_ack_is_at_most_once_not_lost() {
        // A publish sent BEFORE the subscriber's ack (open >= send) is not
        // required to be delivered: zero receives must NOT be a LostDelivery.
        let mut h = PubSubHistory::new();
        // Publish first (send_seq 0), reply (seq 1)...
        let idx = h.record_publish_send(10, b("ch0"), b("early"));
        h.record_publish_reply(idx);
        // ...then the subscriber's ack (seq 2). open(2) > send(0) is false.
        h.record_subscribe_ack(1, SubKind::Channel(b("ch0")));
        // No receive. Conservation must still pass (at-most-once).
        assert_eq!(check_pubsub_conservation(&h), Ok(()));
    }

    #[test]
    fn edge_race_publish_before_ack_still_forbids_duplication() {
        // Even an edge-race publish may not be delivered twice.
        let mut h = PubSubHistory::new();
        let idx = h.record_publish_send(10, b("ch0"), b("early"));
        h.record_publish_reply(idx);
        h.record_subscribe_ack(1, SubKind::Channel(b("ch0")));
        h.record_receive(1, b("ch0"), b("early"), None);
        h.record_receive(1, b("ch0"), b("early"), None);
        assert!(matches!(
            check_pubsub_conservation(&h),
            Err(PubSubViolation::DuplicateDelivery { .. })
        ));
    }

    #[test]
    fn publish_after_unsubscribe_is_not_required() {
        // A publish whose reply lands after the unsubscribe began is not
        // required to be delivered.
        let mut h = PubSubHistory::new();
        h.record_subscribe_ack(1, SubKind::Channel(b("ch0")));
        h.record_unsubscribe(1, SubKind::Channel(b("ch0")));
        let idx = h.record_publish_send(10, b("ch0"), b("late"));
        h.record_publish_reply(idx);
        assert_eq!(check_pubsub_conservation(&h), Ok(()));
    }

    #[test]
    fn pattern_sub_must_receive_matching_channel() {
        // A pattern subscriber firmly in-window that misses a matching publish
        // is a LostDelivery.
        let mut h = PubSubHistory::new();
        h.record_subscribe_ack(2, SubKind::Pattern(b("ch*")));
        let idx = h.record_publish_send(10, b("ch7"), b("m"));
        h.record_publish_reply(idx);
        // No receive by subscriber 2.
        assert!(matches!(
            check_pubsub_conservation(&h),
            Err(PubSubViolation::LostDelivery { .. })
        ));
    }

    #[test]
    fn pattern_non_match_is_not_required() {
        // A publish to a channel the pattern does NOT match is not required.
        let mut h = PubSubHistory::new();
        h.record_subscribe_ack(2, SubKind::Pattern(b("ch*")));
        let idx = h.record_publish_send(10, b("other"), b("m"));
        h.record_publish_reply(idx);
        assert_eq!(check_pubsub_conservation(&h), Ok(()));
    }

    #[test]
    fn json_round_trips() {
        let h = valid_history();
        let restored = PubSubHistory::from_json(&h.to_json()).unwrap();
        assert_eq!(restored.receive_count(), h.receive_count());
        assert_eq!(check_pubsub_conservation(&restored), Ok(()));
    }

    #[test]
    fn workload_generate_is_deterministic() {
        let a = PubSubWorkload::generate(7, 3, 3, 8);
        let b = PubSubWorkload::generate(7, 3, 3, 8);
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
        let c = PubSubWorkload::generate(8, 3, 3, 8);
        assert_ne!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&c).unwrap()
        );
    }

    #[test]
    fn workload_message_tokens_are_globally_unique() {
        for seed in 0..40 {
            let w = PubSubWorkload::generate(seed, 3, 4, 10);
            let mut seen = std::collections::HashSet::new();
            for p in &w.publishers {
                for m in &p.messages {
                    assert!(
                        seen.insert(m.message.clone()),
                        "seed {seed}: duplicate message token {:?}",
                        m.message
                    );
                }
            }
        }
    }

    #[test]
    fn workload_has_disjoint_ids_and_a_pattern_sub() {
        let w = PubSubWorkload::generate(1, 3, 3, 5);
        let sub_ids: std::collections::HashSet<u64> =
            w.subscribers.iter().map(|s| s.client_id).collect();
        for p in &w.publishers {
            assert!(
                !sub_ids.contains(&p.client_id),
                "publisher id {} collides with a subscriber",
                p.client_id
            );
        }
        assert!(
            w.subscribers.iter().any(|s| !s.patterns.is_empty()),
            "at least one subscriber must PSUBSCRIBE"
        );
        assert!(
            w.subscribers.iter().all(|s| !s.channels.is_empty()),
            "every subscriber must SUBSCRIBE at least one channel"
        );
    }

    #[test]
    fn glob_matches_trailing_star() {
        assert!(glob_match(b"ch*", b"ch0"));
        assert!(glob_match(b"ch*", b"ch"));
        assert!(!glob_match(b"ch*", b"xch0"));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"c?0", b"ch0"));
        assert!(!glob_match(b"c?0", b"ch1"));
    }
}
