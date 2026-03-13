//! Pub/Sub infrastructure for FrogDB.
//!
//! This module provides Redis-compatible Pub/Sub functionality including:
//! - Broadcast Pub/Sub (SUBSCRIBE, PUBLISH, etc.)
//! - Sharded Pub/Sub (SSUBSCRIBE, SPUBLISH)
//! - Pattern subscriptions (PSUBSCRIBE)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use frogdb_types::traits::MetricsRecorder;
use tokio::sync::mpsc;

/// Connection ID type.
pub type ConnId = u64;

/// Sender for delivering pub/sub messages to connections.
pub type PubSubSender = mpsc::UnboundedSender<PubSubMessage>;

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
    /// Subscription confirmation.
    Subscribe { channel: Bytes, count: usize },
    /// Unsubscription confirmation.
    Unsubscribe { channel: Bytes, count: usize },
    /// Pattern subscription confirmation.
    PSubscribe { pattern: Bytes, count: usize },
    /// Pattern unsubscription confirmation.
    PUnsubscribe { pattern: Bytes, count: usize },
    /// Sharded subscription confirmation.
    SSubscribe { channel: Bytes, count: usize },
    /// Sharded unsubscription confirmation.
    SUnsubscribe { channel: Bytes, count: usize },
}

impl PubSubMessage {
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
            PubSubMessage::Subscribe { channel, count } => {
                vec![
                    Response::bulk(Bytes::from_static(b"subscribe")),
                    Response::bulk(channel.clone()),
                    Response::Integer(*count as i64),
                ]
            }
            PubSubMessage::Unsubscribe { channel, count } => {
                vec![
                    Response::bulk(Bytes::from_static(b"unsubscribe")),
                    Response::bulk(channel.clone()),
                    Response::Integer(*count as i64),
                ]
            }
            PubSubMessage::PSubscribe { pattern, count } => {
                vec![
                    Response::bulk(Bytes::from_static(b"psubscribe")),
                    Response::bulk(pattern.clone()),
                    Response::Integer(*count as i64),
                ]
            }
            PubSubMessage::PUnsubscribe { pattern, count } => {
                vec![
                    Response::bulk(Bytes::from_static(b"punsubscribe")),
                    Response::bulk(pattern.clone()),
                    Response::Integer(*count as i64),
                ]
            }
            PubSubMessage::SSubscribe { channel, count } => {
                vec![
                    Response::bulk(Bytes::from_static(b"ssubscribe")),
                    Response::bulk(channel.clone()),
                    Response::Integer(*count as i64),
                ]
            }
            PubSubMessage::SUnsubscribe { channel, count } => {
                vec![
                    Response::bulk(Bytes::from_static(b"sunsubscribe")),
                    Response::bulk(channel.clone()),
                    Response::Integer(*count as i64),
                ]
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
#[derive(Debug, Clone)]
pub struct GlobPattern {
    /// Original pattern bytes.
    pattern: Bytes,
    /// Compiled segments for matching.
    segments: Vec<PatternSegment>,
}

/// Segment of a compiled glob pattern.
#[derive(Debug, Clone)]
enum PatternSegment {
    /// Literal string to match exactly.
    Literal(Vec<u8>),
    /// Match any single character.
    AnyChar,
    /// Match any sequence (including empty).
    AnySequence,
    /// Match one of the characters in the set.
    CharClass { chars: Vec<u8>, negated: bool },
}

impl GlobPattern {
    /// Compile a new glob pattern.
    pub fn new(pattern: Bytes) -> Self {
        let segments = Self::compile(&pattern);
        Self { pattern, segments }
    }

    /// Get the original pattern bytes.
    pub fn pattern(&self) -> &Bytes {
        &self.pattern
    }

    /// Compile pattern into segments.
    fn compile(pattern: &[u8]) -> Vec<PatternSegment> {
        let mut segments = Vec::new();
        let mut i = 0;
        let mut literal = Vec::new();

        while i < pattern.len() {
            match pattern[i] {
                b'\\' if i + 1 < pattern.len() => {
                    // Escape sequence
                    literal.push(pattern[i + 1]);
                    i += 2;
                }
                b'*' => {
                    if !literal.is_empty() {
                        segments.push(PatternSegment::Literal(std::mem::take(&mut literal)));
                    }
                    segments.push(PatternSegment::AnySequence);
                    i += 1;
                }
                b'?' => {
                    if !literal.is_empty() {
                        segments.push(PatternSegment::Literal(std::mem::take(&mut literal)));
                    }
                    segments.push(PatternSegment::AnyChar);
                    i += 1;
                }
                b'[' => {
                    if !literal.is_empty() {
                        segments.push(PatternSegment::Literal(std::mem::take(&mut literal)));
                    }
                    let (segment, consumed) = Self::parse_char_class(&pattern[i..]);
                    segments.push(segment);
                    i += consumed;
                }
                c => {
                    literal.push(c);
                    i += 1;
                }
            }
        }

        if !literal.is_empty() {
            segments.push(PatternSegment::Literal(literal));
        }

        segments
    }

    /// Parse a character class [abc] or [^abc].
    fn parse_char_class(input: &[u8]) -> (PatternSegment, usize) {
        debug_assert!(input[0] == b'[');

        let mut i = 1;
        let negated = if i < input.len() && input[i] == b'^' {
            i += 1;
            true
        } else {
            false
        };

        let mut chars = Vec::new();

        while i < input.len() && input[i] != b']' {
            if input[i] == b'\\' && i + 1 < input.len() {
                chars.push(input[i + 1]);
                i += 2;
            } else {
                chars.push(input[i]);
                i += 1;
            }
        }

        // Skip closing bracket
        if i < input.len() && input[i] == b']' {
            i += 1;
        }

        (PatternSegment::CharClass { chars, negated }, i)
    }

    /// Check if a string matches this pattern.
    pub fn matches(&self, s: &[u8]) -> bool {
        Self::match_segments(&self.segments, s)
    }

    /// Match segments against input.
    fn match_segments(segments: &[PatternSegment], mut input: &[u8]) -> bool {
        let mut seg_idx = 0;

        while seg_idx < segments.len() {
            match &segments[seg_idx] {
                PatternSegment::Literal(lit) => {
                    if !input.starts_with(lit) {
                        return false;
                    }
                    input = &input[lit.len()..];
                    seg_idx += 1;
                }
                PatternSegment::AnyChar => {
                    if input.is_empty() {
                        return false;
                    }
                    input = &input[1..];
                    seg_idx += 1;
                }
                PatternSegment::CharClass { chars, negated } => {
                    if input.is_empty() {
                        return false;
                    }
                    let c = input[0];
                    let in_class = chars.contains(&c);
                    if (*negated && in_class) || (!*negated && !in_class) {
                        return false;
                    }
                    input = &input[1..];
                    seg_idx += 1;
                }
                PatternSegment::AnySequence => {
                    // Try matching remaining segments starting at each position
                    let remaining = &segments[seg_idx + 1..];
                    if remaining.is_empty() {
                        return true; // * at end matches anything
                    }

                    // Try each starting position
                    for start in 0..=input.len() {
                        if Self::match_segments(remaining, &input[start..]) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        }

        input.is_empty()
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
                    let _ = sender.send(PubSubMessage::SUnsubscribe {
                        channel: channel.clone(),
                        count: remaining,
                    });
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
                metrics.increment_counter(
                    "frogdb_pubsub_shard_limit_warnings_total",
                    1,
                    &[("type", "total_subscriptions")],
                );
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
                metrics.increment_counter(
                    "frogdb_pubsub_shard_limit_warnings_total",
                    1,
                    &[("type", "unique_channels")],
                );
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
                metrics.increment_counter(
                    "frogdb_pubsub_shard_limit_warnings_total",
                    1,
                    &[("type", "unique_patterns")],
                );
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
    use super::*;

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
        let (tx, _rx) = mpsc::unbounded_channel();

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
        let (tx, _rx) = mpsc::unbounded_channel();

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
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();

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
        let (tx, mut rx) = mpsc::unbounded_channel();

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
        let (tx, mut rx) = mpsc::unbounded_channel();

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
        let (tx, _rx) = mpsc::unbounded_channel();

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
        let (tx, _rx) = mpsc::unbounded_channel();

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
        let (tx, _rx) = mpsc::unbounded_channel();

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

    // =========================================================================
    // Counting and threshold tests
    // =========================================================================

    #[test]
    fn test_total_subscription_count() {
        let mut subs = ShardSubscriptions::new();
        let (tx, _rx) = mpsc::unbounded_channel();

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
        let (tx, _rx) = mpsc::unbounded_channel();

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
        let (tx, _rx) = mpsc::unbounded_channel();

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
