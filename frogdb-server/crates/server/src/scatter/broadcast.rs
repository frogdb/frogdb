//! Lock-free broadcast scatter/gather.
//!
//! Where [`ScatterGatherExecutor`](super::ScatterGatherExecutor) drives the
//! *keyed*, VLL-locked path (MGET/MSET/DEL/…), this module owns the *broadcast*
//! path: fan a per-shard message out to every shard under a single shared
//! deadline. [`ScatterGather`] exposes one method per fan-out shape, and each
//! owns the same "enumerate shards, one `oneshot` per shard, send and handle
//! send failure, collect under one deadline" choreography so no call site
//! hand-rolls it:
//!
//! * [`run`](ScatterGather::run) — fold every shard's reply into one client
//!   [`Response`] via a [`MergeStrategy`] (KEYS, DBSIZE, FLUSHDB, SCRIPT
//!   LOAD/EXISTS/FLUSH, PUBSUB introspection, FT.*).
//! * [`gather_all`](ScatterGather::gather_all) — best-effort typed `Vec<R>` for
//!   callers that merge/sum/sort at the call site (DEBUG introspection, MEMORY
//!   STATS, LATENCY/SLOWLOG aggregation, and the await-and-discard resets).
//! * [`find_first`](ScatterGather::find_first) — short-circuit walk returning the
//!   first reply a predicate accepts (SCRIPT KILL / FUNCTION KILL).
//! * [`broadcast_all`](ScatterGather::broadcast_all) — fire-and-forget to every
//!   shard, awaiting no reply (client-tracking register/unregister, the
//!   `ConnectionClosed` teardown fan-outs).
//!
//! A broadcast command therefore states only its per-shard message (the
//! `make_msg` closure) and, for [`run`](ScatterGather::run), how replies fold
//! (the [`MergeStrategy`]). The single shared deadline and the send-failure /
//! drop / timeout mapping live here and nowhere else — there is no longer a
//! per-shard await to forget the timeout on, which is the bug class the
//! `pubsub.rs` / `script.rs` / FUNCTION KILL missing timeouts all came from.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{IntrospectionResponse, PartialResult, ShardMessage, ShardSender};
use frogdb_protocol::Response;
use tokio::sync::oneshot;
use tracing::warn;

/// Fallback fan-out deadline for broadcast helpers invoked from a
/// [`ConnCtx`](frogdb_core::ConnCtx), which does not carry the connection's
/// runtime `scatter_gather_timeout`. Matches the config default (5s). Handlers
/// that hold a `ConnectionHandler` use the runtime value via
/// [`scatter_gather`](crate::connection::ConnectionHandler::scatter_gather)
/// instead.
pub const DEFAULT_SCATTER_GATHER_TIMEOUT: Duration = Duration::from_secs(5);

/// What to do when a shard send fails, drops its sender, or times out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialPolicy {
    /// Return an error reply if any shard is missing (the safe default — never
    /// under-report an aggregate by silently dropping a shard).
    FailFast,
    /// Skip the missing shard and fold whatever the survivors returned.
    BestEffort,
}

/// How a broadcast command folds per-shard replies into one client `Response`.
///
/// This is the entire interface a broadcast command must implement: the fan-out,
/// the shared timeout, and the send-failure / dropped-shard / timeout error
/// mapping all live in [`ScatterGather::run`] behind it.
pub trait MergeStrategy {
    /// What one shard returns (e.g. [`PartialResult`], [`IntrospectionResponse`],
    /// `Vec<bool>`, `String`).
    type Reply: Send + 'static;

    /// Static strategy label for tracing / metrics.
    fn name(&self) -> &'static str;

    /// Fold one shard's reply into the running accumulator.
    fn absorb(&mut self, shard_id: usize, reply: Self::Reply);

    /// Produce the final client response once every surviving shard has been folded.
    fn finish(self: Box<Self>) -> Response;

    /// What to do when a shard send fails / drops / times out.
    ///
    /// `FailFast` (default) returns an error reply; `BestEffort` skips that shard.
    fn on_missing(&self) -> PartialPolicy {
        PartialPolicy::FailFast
    }
}

/// Lock-free broadcast coordinator.
///
/// Binds the shard senders, the one timeout policy, and the connection id so
/// call sites stay terse. Build one via
/// [`ConnectionHandler::scatter_gather`](crate::connection::ConnectionHandler::scatter_gather).
pub struct ScatterGather<'a> {
    senders: &'a [ShardSender],
    timeout: Duration,
    conn_id: u64,
}

impl<'a> ScatterGather<'a> {
    /// Create a broadcast coordinator over `senders` with one shared `timeout`.
    pub fn new(senders: &'a [ShardSender], timeout: Duration, conn_id: u64) -> Self {
        Self {
            senders,
            timeout,
            conn_id,
        }
    }

    /// Create a coordinator for a fire-and-forget [`broadcast_all`](Self::broadcast_all).
    ///
    /// That shape awaits no reply, so it needs neither a deadline nor a
    /// connection id; this exists so call sites that only hold `&[ShardSender]`
    /// (the client-tracking / teardown paths, which do not carry a
    /// `scatter_gather_timeout`) can still route through the seam. The stored
    /// timeout is never observed — only [`run`](Self::run), [`query_one`](Self::query_one),
    /// [`gather_all`](Self::gather_all), and [`find_first`](Self::find_first)
    /// await replies, and none of those are reachable through this constructor's
    /// intended use.
    pub fn broadcast(senders: &'a [ShardSender]) -> Self {
        Self {
            senders,
            timeout: Duration::ZERO,
            conn_id: 0,
        }
    }

    /// Fan `make_msg(shard_id, tx)` out to every shard, await all replies under a
    /// single shared deadline, fold them with `merge`, and map send-failure /
    /// drop / timeout to the canonical error replies — once, for every broadcast
    /// command.
    pub async fn run<M, F, S>(&self, mut merge: Box<M>, make_msg: F) -> Response
    where
        M: MergeStrategy + ?Sized,
        F: Fn(usize, oneshot::Sender<M::Reply>) -> S,
        S: Into<ShardMessage>,
    {
        let mut rxs = Vec::with_capacity(self.senders.len());
        for (shard_id, sender) in self.senders.iter().enumerate() {
            let (tx, rx) = oneshot::channel();
            if sender.send(make_msg(shard_id, tx)).await.is_err() {
                match merge.on_missing() {
                    PartialPolicy::FailFast => {
                        warn!(
                            conn_id = self.conn_id,
                            shard_id,
                            cmd = merge.name(),
                            "shard unavailable"
                        );
                        return Response::error("ERR shard unavailable");
                    }
                    PartialPolicy::BestEffort => continue,
                }
            }
            rxs.push((shard_id, rx));
        }

        // One deadline for the whole gather, not one-per-receiver.
        let deadline = tokio::time::sleep(self.timeout);
        tokio::pin!(deadline);
        for (shard_id, rx) in rxs {
            tokio::select! {
                reply = rx => match reply {
                    Ok(reply) => merge.absorb(shard_id, reply),
                    Err(_) => match merge.on_missing() {
                        PartialPolicy::FailFast => {
                            warn!(
                                conn_id = self.conn_id,
                                shard_id,
                                cmd = merge.name(),
                                "shard dropped request"
                            );
                            return Response::error("ERR shard dropped request");
                        }
                        PartialPolicy::BestEffort => continue,
                    },
                },
                _ = &mut deadline => {
                    warn!(
                        conn_id = self.conn_id,
                        shard_id,
                        cmd = merge.name(),
                        "scatter timeout"
                    );
                    return Response::error("ERR timeout");
                }
            }
        }
        merge.finish()
    }

    /// Send `message` to a single shard and await its reply under the shared
    /// timeout. Multi-phase commands (SCAN's cursor walk, RANDOMKEY's
    /// count-then-fetch) keep their own control flow but reuse this so they
    /// inherit the same send-failure / drop / timeout mapping.
    ///
    /// Returns `Ok(reply)` or an `Err(Response)` carrying the canonical error.
    pub async fn query_one<R: Send + 'static>(
        &self,
        shard_id: usize,
        message: impl Into<ShardMessage>,
        rx: oneshot::Receiver<R>,
    ) -> Result<R, Response> {
        if self.senders[shard_id].send(message).await.is_err() {
            warn!(conn_id = self.conn_id, shard_id, "shard unavailable");
            return Err(Response::error("ERR shard unavailable"));
        }
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => {
                warn!(conn_id = self.conn_id, shard_id, "shard dropped request");
                Err(Response::error("ERR shard dropped request"))
            }
            Err(_) => {
                warn!(conn_id = self.conn_id, shard_id, "scatter timeout");
                Err(Response::error("ERR timeout"))
            }
        }
    }

    /// Fan `make_msg(shard_id, tx)` out to every shard concurrently, then collect
    /// every reply that arrives before the one shared deadline into a `Vec<R>`.
    ///
    /// Best-effort: a shard whose send fails, whose sender drops, or that misses
    /// the deadline is skipped rather than surfaced as an error — callers do
    /// their own merge/sum/sort over the survivors at the call site. Replies are
    /// collected in shard order (each receiver is awaited in ascending shard id),
    /// so order-sensitive callers see the same layout the old sequential loops
    /// produced. The whole gather is bounded by one deadline, replacing the
    /// per-shard 5s timeouts (worst case N×5s) and the unbounded awaits these
    /// callers used to hand-roll.
    pub async fn gather_all<R, F, S>(&self, make_msg: F) -> Vec<R>
    where
        R: Send + 'static,
        F: Fn(usize, oneshot::Sender<R>) -> S,
        S: Into<ShardMessage>,
    {
        let mut rxs = Vec::with_capacity(self.senders.len());
        for (shard_id, sender) in self.senders.iter().enumerate() {
            let (tx, rx) = oneshot::channel();
            if sender.send(make_msg(shard_id, tx)).await.is_err() {
                warn!(
                    conn_id = self.conn_id,
                    shard_id, "shard unavailable (gather)"
                );
                continue;
            }
            rxs.push((shard_id, rx));
        }

        // One deadline for the whole gather, not one-per-receiver.
        let mut results = Vec::with_capacity(rxs.len());
        let deadline = tokio::time::sleep(self.timeout);
        tokio::pin!(deadline);
        for (shard_id, rx) in rxs {
            tokio::select! {
                reply = rx => match reply {
                    Ok(reply) => results.push(reply),
                    Err(_) => warn!(
                        conn_id = self.conn_id,
                        shard_id,
                        "shard dropped request (gather)"
                    ),
                },
                _ = &mut deadline => {
                    warn!(conn_id = self.conn_id, shard_id, "gather timeout");
                    break;
                }
            }
        }
        results
    }

    /// Fan `make_msg(shard_id, tx)` out to every shard concurrently, then walk the
    /// replies in shard order and return the first one `predicate` accepts.
    ///
    /// Send-failure, dropped, and predicate-rejected shards are skipped; if the
    /// shared deadline fires before any shard is accepted the walk stops and
    /// returns `None`. Used by the KILL walks (SCRIPT KILL / FUNCTION KILL): the
    /// predicate decides "did this shard produce a decisive reply?" and the
    /// caller classifies the returned reply, so the exact NOTBUSY / UNKILLABLE /
    /// hard-error precedence stays at the call site. The per-shard await that
    /// FUNCTION KILL used to leave unbounded is now structurally bounded here.
    pub async fn find_first<R, F, P, S>(&self, make_msg: F, predicate: P) -> Option<R>
    where
        R: Send + 'static,
        F: Fn(usize, oneshot::Sender<R>) -> S,
        S: Into<ShardMessage>,
        P: Fn(&R) -> bool,
    {
        let mut rxs = Vec::with_capacity(self.senders.len());
        for (shard_id, sender) in self.senders.iter().enumerate() {
            let (tx, rx) = oneshot::channel();
            if sender.send(make_msg(shard_id, tx)).await.is_err() {
                // Best-effort: a shard we cannot even reach cannot be the answer.
                continue;
            }
            rxs.push((shard_id, rx));
        }

        // One deadline for the whole walk, not one-per-receiver.
        let deadline = tokio::time::sleep(self.timeout);
        tokio::pin!(deadline);
        for (_shard_id, rx) in rxs {
            tokio::select! {
                reply = rx => {
                    if let Ok(reply) = reply
                        && predicate(&reply)
                    {
                        return Some(reply);
                    }
                    // Dropped, or predicate rejected: try the next shard.
                }
                _ = &mut deadline => return None,
            }
        }
        None
    }

    /// Fire `make_msg(shard_id)` at every shard and return without awaiting any
    /// reply. Send failures are silently ignored (best-effort), matching the
    /// teardown/registration fan-outs this replaces: they must never surface an
    /// error to the caller. No reply is awaited, so there is no deadline.
    pub async fn broadcast_all<F, S>(&self, make_msg: F)
    where
        F: Fn(usize) -> S,
        S: Into<ShardMessage>,
    {
        for (shard_id, sender) in self.senders.iter().enumerate() {
            let _ = sender.send(make_msg(shard_id)).await;
        }
    }
}

// =============================================================================
// Ready-made merge strategies
// =============================================================================

/// A shard reply that contributes a running total of integers.
///
/// Lets [`SumIntegers`] fold both [`PartialResult`] (DBSIZE) and
/// [`IntrospectionResponse`] (PUBSUB NUMPAT / NUMSUB) without re-typing the sum.
pub trait IntegerTotal: Send + 'static {
    /// Sum of every integer this reply contributes.
    fn integer_total(&self) -> i64;
}

impl IntegerTotal for PartialResult {
    fn integer_total(&self) -> i64 {
        match self {
            // DBSIZE replies with a typed count.
            PartialResult::Count(n) => *n,
            // A keyed reply of integers contributes their sum (generic fold).
            PartialResult::Keyed(results) => results
                .iter()
                .filter_map(|(_, r)| match r {
                    Response::Integer(n) => Some(*n),
                    _ => None,
                })
                .sum(),
            _ => 0,
        }
    }
}

impl IntegerTotal for IntrospectionResponse {
    fn integer_total(&self) -> i64 {
        match self {
            IntrospectionResponse::NumPat(count) => *count as i64,
            IntrospectionResponse::NumSub(counts) => counts.iter().map(|(_, c)| *c as i64).sum(),
            IntrospectionResponse::Channels(_) => 0,
        }
    }
}

/// Sum every integer across every shard reply into a single `Integer`.
///
/// Replaces the inline DBSIZE fold and PUBSUB NUMPAT's `total += count`.
pub struct SumIntegers<R> {
    total: i64,
    _marker: PhantomData<fn(R)>,
}

impl<R> Default for SumIntegers<R> {
    fn default() -> Self {
        Self {
            total: 0,
            _marker: PhantomData,
        }
    }
}

impl<R: IntegerTotal> MergeStrategy for SumIntegers<R> {
    type Reply = R;

    fn name(&self) -> &'static str {
        "SUM_INTEGERS"
    }

    fn absorb(&mut self, _shard_id: usize, reply: R) {
        self.total += reply.integer_total();
    }

    fn finish(self: Box<Self>) -> Response {
        Response::Integer(self.total)
    }
}

/// Collect every key across all shards, sort, and emit a bulk-string array.
///
/// Replaces the inline KEYS and TS.QUERYINDEX sorted-union folds.
#[derive(Default)]
pub struct SortedUnion {
    keys: Vec<Bytes>,
}

impl MergeStrategy for SortedUnion {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "SORTED_UNION"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        for (key, _) in reply.into_keyed_results() {
            self.keys.push(key);
        }
    }

    fn finish(mut self: Box<Self>) -> Response {
        self.keys.sort();
        Response::Array(self.keys.into_iter().map(Response::bulk).collect())
    }
}

/// Collect `(key, response)` pairs across all shards, sort by key, and emit the
/// responses. Replaces the inline TS.MGET / TS.MRANGE folds.
#[derive(Default)]
pub struct SortedByKey {
    results: Vec<(Bytes, Response)>,
}

impl MergeStrategy for SortedByKey {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "SORTED_BY_KEY"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        self.results.extend(reply.into_keyed_results());
    }

    fn finish(mut self: Box<Self>) -> Response {
        self.results.sort_by(|a, b| a.0.cmp(&b.0));
        Response::Array(self.results.into_iter().map(|(_, r)| r).collect())
    }
}

/// Deduplicate channel names across all shards, sort, and emit a bulk-string
/// array. Replaces PUBSUB CHANNELS / SHARDCHANNELS.
#[derive(Default)]
pub struct DedupSorted {
    channels: HashSet<Bytes>,
}

impl MergeStrategy for DedupSorted {
    type Reply = IntrospectionResponse;

    fn name(&self) -> &'static str {
        "DEDUP_SORTED"
    }

    fn absorb(&mut self, _shard_id: usize, reply: IntrospectionResponse) {
        if let IntrospectionResponse::Channels(channels) = reply {
            self.channels.extend(channels);
        }
    }

    fn finish(self: Box<Self>) -> Response {
        let mut channels: Vec<Bytes> = self.channels.into_iter().collect();
        channels.sort();
        Response::Array(channels.into_iter().map(Response::bulk).collect())
    }
}

/// Sum subscriber counts per channel across all shards, emitting them in the
/// requested channel order as `[channel, count, …]`. Channels with no
/// subscribers report `0`. Replaces PUBSUB NUMSUB / SHARDNUMSUB.
pub struct CountByKey {
    /// Requested channels, in order — defines the output layout.
    order: Vec<Bytes>,
    counts: HashMap<Bytes, i64>,
}

impl CountByKey {
    /// Build a strategy reporting `order`'s channels in the given order.
    pub fn new(order: Vec<Bytes>) -> Self {
        Self {
            order,
            counts: HashMap::new(),
        }
    }
}

impl MergeStrategy for CountByKey {
    type Reply = IntrospectionResponse;

    fn name(&self) -> &'static str {
        "COUNT_BY_KEY"
    }

    fn absorb(&mut self, _shard_id: usize, reply: IntrospectionResponse) {
        if let IntrospectionResponse::NumSub(counts) = reply {
            for (channel, count) in counts {
                *self.counts.entry(channel).or_insert(0) += count as i64;
            }
        }
    }

    fn finish(self: Box<Self>) -> Response {
        let CountByKey { order, counts } = *self;
        let mut result = Vec::with_capacity(order.len() * 2);
        for channel in order {
            let count = counts.get(&channel).copied().unwrap_or(0);
            result.push(Response::bulk(channel));
            result.push(Response::Integer(count));
        }
        Response::Array(result)
    }
}

/// A shard reply whose shard-0 value is what the client sees (shard-0-wins
/// commands), plus an optional embedded error scanned across all shards.
pub trait ShardZeroProjection: Send + 'static {
    /// Project this reply (from shard 0) into the client response.
    fn into_response(self) -> Response;

    /// An error embedded in this reply, if any (scanned across all shards when
    /// [`ShardZeroReply::checked`] is used).
    fn embedded_error(&self) -> Option<Response> {
        None
    }
}

impl ShardZeroProjection for PartialResult {
    fn into_response(self) -> Response {
        self.into_keyed_results()
            .into_iter()
            .next()
            .map(|(_, resp)| resp)
            .unwrap_or_else(Response::ok)
    }

    fn embedded_error(&self) -> Option<Response> {
        self.keyed_slice().iter().find_map(|(_, resp)| match resp {
            Response::Error(_) => Some(resp.clone()),
            _ => None,
        })
    }
}

impl ShardZeroProjection for String {
    fn into_response(self) -> Response {
        Response::bulk(Bytes::from(self))
    }
}

/// Return shard 0's reply, optionally erroring if any shard embedded an error.
///
/// Replaces `broadcast_and_check_shard0` (checked) /
/// `broadcast_and_return_shard0_response` (unchecked) and SCRIPT LOAD (over a
/// SHA `String`). `FailFast` guarantees every shard participated — so SCRIPT
/// LOAD can no longer return a SHA while some shard never cached the script.
pub struct ShardZeroReply<R> {
    check_errors: bool,
    error: Option<Response>,
    shard0: Option<R>,
}

impl<R> ShardZeroReply<R> {
    /// Return the first embedded error from any shard, else shard 0's reply.
    pub fn checked() -> Self {
        Self {
            check_errors: true,
            error: None,
            shard0: None,
        }
    }

    /// Return shard 0's reply, ignoring embedded errors from other shards.
    pub fn unchecked() -> Self {
        Self {
            check_errors: false,
            error: None,
            shard0: None,
        }
    }
}

impl<R: ShardZeroProjection> MergeStrategy for ShardZeroReply<R> {
    type Reply = R;

    fn name(&self) -> &'static str {
        "SHARD_ZERO"
    }

    fn absorb(&mut self, shard_id: usize, reply: R) {
        if self.check_errors && self.error.is_none() {
            self.error = reply.embedded_error();
        }
        if shard_id == 0 {
            self.shard0 = Some(reply);
        }
    }

    fn finish(self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }
        match self.shard0 {
            Some(reply) => reply.into_response(),
            None => Response::ok(),
        }
    }
}

/// Return `OK` once every shard has replied. Relies on `FailFast` to surface a
/// dropped/timed-out shard as an error. Replaces FLUSHDB and SCRIPT FLUSH.
pub struct AllOk<R> {
    _marker: PhantomData<fn(R)>,
}

impl<R> Default for AllOk<R> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<R: Send + 'static> MergeStrategy for AllOk<R> {
    type Reply = R;

    fn name(&self) -> &'static str {
        "ALL_OK"
    }

    fn absorb(&mut self, _shard_id: usize, _reply: R) {}

    fn finish(self: Box<Self>) -> Response {
        Response::ok()
    }
}

/// Combine per-index boolean vectors across shards with OR, emitting `1`/`0`
/// integers. Replaces SCRIPT EXISTS' cross-shard existence check.
pub struct BoolOr {
    combined: Vec<bool>,
}

impl BoolOr {
    /// Build a strategy over `len` flags, all initially `false`.
    pub fn new(len: usize) -> Self {
        Self {
            combined: vec![false; len],
        }
    }
}

impl MergeStrategy for BoolOr {
    type Reply = Vec<bool>;

    fn name(&self) -> &'static str {
        "BOOL_OR"
    }

    fn absorb(&mut self, _shard_id: usize, reply: Vec<bool>) {
        for (i, exists) in reply.into_iter().enumerate() {
            if exists && i < self.combined.len() {
                self.combined[i] = true;
            }
        }
    }

    fn finish(self: Box<Self>) -> Response {
        Response::Array(
            self.combined
                .into_iter()
                .map(|e| Response::Integer(i64::from(e)))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::{CoreMsg, ShardReceiver};
    use tokio::sync::mpsc;

    // -------------------------------------------------------------------------
    // Merge strategy unit tests (socket-free: fold a Vec of replies)
    // -------------------------------------------------------------------------

    /// Fold a sequence of `(shard_id, reply)` through a strategy.
    fn fold<M: MergeStrategy>(mut merge: Box<M>, replies: Vec<(usize, M::Reply)>) -> Response {
        for (shard_id, reply) in replies {
            merge.absorb(shard_id, reply);
        }
        merge.finish()
    }

    fn partial(results: Vec<(Bytes, Response)>) -> PartialResult {
        PartialResult::keyed(results)
    }

    #[test]
    fn sum_integers_partial_result() {
        // DBSIZE replies as a typed `Count`; SumIntegers folds them.
        let merge: Box<SumIntegers<PartialResult>> = Box::default();
        let resp = fold(
            merge,
            vec![(0, PartialResult::Count(2)), (1, PartialResult::Count(3))],
        );
        assert!(matches!(resp, Response::Integer(5)));
    }

    #[test]
    fn sum_integers_introspection() {
        let merge: Box<SumIntegers<IntrospectionResponse>> = Box::default();
        let resp = fold(
            merge,
            vec![
                (0, IntrospectionResponse::NumPat(4)),
                (1, IntrospectionResponse::NumPat(6)),
            ],
        );
        assert!(matches!(resp, Response::Integer(10)));
    }

    #[test]
    fn sorted_union_sorts_keys() {
        let resp = fold(
            Box::new(SortedUnion::default()),
            vec![
                (0, partial(vec![(Bytes::from("c"), Response::null())])),
                (
                    1,
                    partial(vec![
                        (Bytes::from("a"), Response::null()),
                        (Bytes::from("b"), Response::null()),
                    ]),
                ),
            ],
        );
        match resp {
            Response::Array(arr) => {
                let keys: Vec<&[u8]> = arr
                    .iter()
                    .map(|r| match r {
                        Response::Bulk(Some(b)) => b.as_ref(),
                        _ => panic!("expected bulk"),
                    })
                    .collect();
                assert_eq!(keys, vec![b"a".as_ref(), b"b".as_ref(), b"c".as_ref()]);
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn sorted_by_key_orders_responses() {
        let resp = fold(
            Box::new(SortedByKey::default()),
            vec![
                (0, partial(vec![(Bytes::from("k2"), Response::Integer(2))])),
                (1, partial(vec![(Bytes::from("k1"), Response::Integer(1))])),
            ],
        );
        match resp {
            Response::Array(arr) => {
                assert!(matches!(arr[0], Response::Integer(1)));
                assert!(matches!(arr[1], Response::Integer(2)));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn dedup_sorted_dedups_and_sorts() {
        let resp = fold(
            Box::new(DedupSorted::default()),
            vec![
                (
                    0,
                    IntrospectionResponse::Channels(vec![Bytes::from("z"), Bytes::from("a")]),
                ),
                (
                    1,
                    IntrospectionResponse::Channels(vec![Bytes::from("a"), Bytes::from("m")]),
                ),
            ],
        );
        match resp {
            Response::Array(arr) => {
                let chans: Vec<&[u8]> = arr
                    .iter()
                    .map(|r| match r {
                        Response::Bulk(Some(b)) => b.as_ref(),
                        _ => panic!("expected bulk"),
                    })
                    .collect();
                assert_eq!(chans, vec![b"a".as_ref(), b"m".as_ref(), b"z".as_ref()]);
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn count_by_key_sums_in_request_order() {
        let order = vec![Bytes::from("ch1"), Bytes::from("ch2"), Bytes::from("ch3")];
        let resp = fold(
            Box::new(CountByKey::new(order)),
            vec![
                (
                    0,
                    IntrospectionResponse::NumSub(vec![
                        (Bytes::from("ch1"), 1),
                        (Bytes::from("ch2"), 2),
                    ]),
                ),
                (
                    1,
                    IntrospectionResponse::NumSub(vec![(Bytes::from("ch1"), 4)]),
                ),
            ],
        );
        match resp {
            Response::Array(arr) => {
                // ch1 -> 5, ch2 -> 2, ch3 -> 0 (interleaved channel, count)
                assert!(matches!(&arr[0], Response::Bulk(Some(b)) if b.as_ref() == b"ch1"));
                assert!(matches!(arr[1], Response::Integer(5)));
                assert!(matches!(&arr[2], Response::Bulk(Some(b)) if b.as_ref() == b"ch2"));
                assert!(matches!(arr[3], Response::Integer(2)));
                assert!(matches!(&arr[4], Response::Bulk(Some(b)) if b.as_ref() == b"ch3"));
                assert!(matches!(arr[5], Response::Integer(0)));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn shard_zero_checked_returns_embedded_error() {
        let resp = fold(
            Box::new(ShardZeroReply::<PartialResult>::checked()),
            vec![
                (0, partial(vec![(Bytes::from("k"), Response::ok())])),
                (
                    1,
                    partial(vec![(Bytes::from("k"), Response::error("ERR boom"))]),
                ),
            ],
        );
        assert!(matches!(resp, Response::Error(ref e) if e.as_ref() == b"ERR boom"));
    }

    #[test]
    fn shard_zero_unchecked_ignores_errors() {
        let resp = fold(
            Box::new(ShardZeroReply::<PartialResult>::unchecked()),
            vec![
                (0, partial(vec![(Bytes::from("k"), Response::Integer(7))])),
                (
                    1,
                    partial(vec![(Bytes::from("k"), Response::error("ERR boom"))]),
                ),
            ],
        );
        assert!(matches!(resp, Response::Integer(7)));
    }

    #[test]
    fn shard_zero_string_returns_shard0_sha() {
        let resp = fold(
            Box::new(ShardZeroReply::<String>::unchecked()),
            vec![(0, "abc123".to_string()), (1, "abc123".to_string())],
        );
        assert!(matches!(resp, Response::Bulk(Some(ref b)) if b.as_ref() == b"abc123"));
    }

    #[test]
    fn all_ok_returns_ok() {
        let resp = fold(
            Box::new(AllOk::<PartialResult>::default()),
            vec![(0, partial(vec![])), (1, partial(vec![]))],
        );
        assert!(matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"));
    }

    #[test]
    fn bool_or_combines_per_index() {
        let resp = fold(
            Box::new(BoolOr::new(3)),
            vec![(0, vec![true, false, false]), (1, vec![false, false, true])],
        );
        match resp {
            Response::Array(arr) => {
                assert!(matches!(arr[0], Response::Integer(1)));
                assert!(matches!(arr[1], Response::Integer(0)));
                assert!(matches!(arr[2], Response::Integer(1)));
            }
            _ => panic!("expected array"),
        }
    }

    // -------------------------------------------------------------------------
    // Runner tests (mock shard senders)
    // -------------------------------------------------------------------------

    /// Per-shard behavior for the mock cluster.
    enum Behavior {
        /// Reply with these `(key, response)` pairs.
        Reply(Vec<(Bytes, Response)>),
        /// Drop the oneshot sender without replying.
        Drop,
        /// Receive but never reply (hold the sender forever).
        Hang,
        /// Close the channel so `send` fails.
        Closed,
    }

    /// Build mock shard senders. Returns the senders plus responder task handles
    /// (kept alive for the duration of the test).
    fn mock_cluster(
        behaviors: Vec<Behavior>,
    ) -> (Vec<ShardSender>, Vec<tokio::task::JoinHandle<()>>) {
        let mut senders = Vec::new();
        let mut tasks = Vec::new();
        for behavior in behaviors {
            let (tx, rx) = mpsc::channel(16);
            let sender = ShardSender::new(tx);
            senders.push(sender);
            if matches!(behavior, Behavior::Closed) {
                drop(rx);
                continue;
            }
            let mut receiver = ShardReceiver::new(rx);
            tasks.push(tokio::spawn(async move {
                let mut held = Vec::new();
                while let Some(env) = receiver.recv().await {
                    if let ShardMessage::Core(CoreMsg::ScatterRequest { response_tx, .. }) =
                        env.message
                    {
                        match &behavior {
                            Behavior::Reply(results) => {
                                let _ = response_tx.send(partial(results.clone()));
                            }
                            Behavior::Drop => drop(response_tx),
                            Behavior::Hang => held.push(response_tx),
                            Behavior::Closed => unreachable!(),
                        }
                    }
                }
                drop(held);
            }));
        }
        (senders, tasks)
    }

    fn dbsize_msg(_shard: usize, response_tx: oneshot::Sender<PartialResult>) -> CoreMsg {
        CoreMsg::ScatterRequest {
            request_id: 1,
            keys: vec![],
            operation: frogdb_core::ScatterOp::DbSize,
            conn_id: 0,
            response_tx,
        }
    }

    #[tokio::test]
    async fn run_sums_all_shards() {
        let (senders, _tasks) = mock_cluster(vec![
            Behavior::Reply(vec![(Bytes::from("a"), Response::Integer(10))]),
            Behavior::Reply(vec![(Bytes::from("b"), Response::Integer(20))]),
            Behavior::Reply(vec![(Bytes::from("c"), Response::Integer(30))]),
        ]);
        let sg = ScatterGather::new(&senders, Duration::from_secs(5), 0);
        let resp = sg
            .run(
                Box::new(SumIntegers::<PartialResult>::default()),
                dbsize_msg,
            )
            .await;
        assert!(matches!(resp, Response::Integer(60)));
    }

    #[tokio::test]
    async fn run_fail_fast_on_dropped_shard() {
        let (senders, _tasks) = mock_cluster(vec![
            Behavior::Reply(vec![(Bytes::from("a"), Response::Integer(10))]),
            Behavior::Drop,
        ]);
        let sg = ScatterGather::new(&senders, Duration::from_secs(5), 0);
        let resp = sg
            .run(
                Box::new(SumIntegers::<PartialResult>::default()),
                dbsize_msg,
            )
            .await;
        assert!(
            matches!(resp, Response::Error(ref e) if e.as_ref() == b"ERR shard dropped request")
        );
    }

    #[tokio::test]
    async fn run_best_effort_skips_dropped_shard() {
        // A best-effort strategy folds survivors and ignores the dropped shard.
        struct BestEffortSum(i64);
        impl MergeStrategy for BestEffortSum {
            type Reply = PartialResult;
            fn name(&self) -> &'static str {
                "BEST_EFFORT_SUM"
            }
            fn absorb(&mut self, _: usize, reply: PartialResult) {
                self.0 += reply.integer_total();
            }
            fn finish(self: Box<Self>) -> Response {
                Response::Integer(self.0)
            }
            fn on_missing(&self) -> PartialPolicy {
                PartialPolicy::BestEffort
            }
        }
        let (senders, _tasks) = mock_cluster(vec![
            Behavior::Reply(vec![(Bytes::from("a"), Response::Integer(10))]),
            Behavior::Drop,
        ]);
        let sg = ScatterGather::new(&senders, Duration::from_secs(5), 0);
        let resp = sg.run(Box::new(BestEffortSum(0)), dbsize_msg).await;
        assert!(matches!(resp, Response::Integer(10)));
    }

    #[tokio::test]
    async fn run_unavailable_on_closed_sender() {
        let (senders, _tasks) = mock_cluster(vec![
            Behavior::Reply(vec![(Bytes::from("a"), Response::Integer(10))]),
            Behavior::Closed,
        ]);
        let sg = ScatterGather::new(&senders, Duration::from_secs(5), 0);
        let resp = sg
            .run(
                Box::new(SumIntegers::<PartialResult>::default()),
                dbsize_msg,
            )
            .await;
        assert!(matches!(resp, Response::Error(ref e) if e.as_ref() == b"ERR shard unavailable"));
    }

    #[tokio::test(start_paused = true)]
    async fn run_times_out_on_single_deadline() {
        // Four hung shards: the gather must time out after exactly one timeout
        // (the shared deadline), not num_shards × timeout.
        let timeout = Duration::from_secs(2);
        let (senders, _tasks) = mock_cluster(vec![
            Behavior::Hang,
            Behavior::Hang,
            Behavior::Hang,
            Behavior::Hang,
        ]);
        let sg = ScatterGather::new(&senders, timeout, 0);
        let start = tokio::time::Instant::now();
        let resp = sg
            .run(
                Box::new(SumIntegers::<PartialResult>::default()),
                dbsize_msg,
            )
            .await;
        let elapsed = start.elapsed();
        assert!(matches!(resp, Response::Error(ref e) if e.as_ref() == b"ERR timeout"));
        assert_eq!(elapsed, timeout, "gather must use a single shared deadline");
    }

    // -------------------------------------------------------------------------
    // gather_all / find_first / broadcast_all (mock shard senders)
    //
    // One stalled-shard test per fan-out shape: every command routing through a
    // helper inherits the single-deadline guarantee, so a new gather/walk cannot
    // reintroduce the unbounded-await bug — it has no await to leave unbounded.
    // -------------------------------------------------------------------------

    /// A fire-and-forget message with a throwaway reply channel — `broadcast_all`
    /// discards replies, so the receiver is dropped immediately.
    fn dbsize_broadcast_msg(_shard: usize) -> CoreMsg {
        let (response_tx, _rx) = oneshot::channel();
        CoreMsg::ScatterRequest {
            request_id: 1,
            keys: vec![],
            operation: frogdb_core::ScatterOp::DbSize,
            conn_id: 0,
            response_tx,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn gather_all_collects_survivors_under_one_deadline() {
        // Shard 0 answers; shard 1 stalls forever. The gather must return the one
        // survivor's reply bounded by a single shared deadline (not hang, and not
        // num_shards × timeout).
        let timeout = Duration::from_secs(2);
        let (senders, _tasks) = mock_cluster(vec![
            Behavior::Reply(vec![(Bytes::from("a"), Response::Integer(10))]),
            Behavior::Hang,
        ]);
        let sg = ScatterGather::new(&senders, timeout, 0);
        let start = tokio::time::Instant::now();
        let results = sg.gather_all(dbsize_msg).await;
        let elapsed = start.elapsed();
        assert_eq!(
            results.len(),
            1,
            "the responsive shard's reply is collected"
        );
        assert_eq!(
            results[0].integer_total(),
            10,
            "the survivor's payload is intact"
        );
        assert_eq!(elapsed, timeout, "bounded by the single shared deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn find_first_returns_none_when_the_match_stalls() {
        // Shard 0 replies but is rejected by the predicate; shard 1 stalls. The
        // walk must not hang: it returns None at the shared deadline.
        let timeout = Duration::from_secs(2);
        let (senders, _tasks) = mock_cluster(vec![
            Behavior::Reply(vec![(Bytes::from("a"), Response::Integer(1))]),
            Behavior::Hang,
        ]);
        let sg = ScatterGather::new(&senders, timeout, 0);
        let start = tokio::time::Instant::now();
        let found = sg.find_first(dbsize_msg, |_r: &PartialResult| false).await;
        let elapsed = start.elapsed();
        assert!(found.is_none(), "no shard satisfied the predicate");
        assert_eq!(elapsed, timeout, "bounded by the single shared deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn find_first_short_circuits_before_the_stalled_shard() {
        // Shard 0 is accepted; shard 1 stalls. The walk returns on shard 0
        // without ever awaiting the stalled shard.
        let timeout = Duration::from_secs(2);
        let (senders, _tasks) = mock_cluster(vec![
            Behavior::Reply(vec![(Bytes::from("a"), Response::Integer(7))]),
            Behavior::Hang,
        ]);
        let sg = ScatterGather::new(&senders, timeout, 0);
        let start = tokio::time::Instant::now();
        let found = sg.find_first(dbsize_msg, |_r: &PartialResult| true).await;
        let elapsed = start.elapsed();
        assert_eq!(
            found.map(|r| r.integer_total()),
            Some(7),
            "shard 0's reply is returned"
        );
        assert!(
            elapsed < timeout,
            "short-circuits without awaiting the stalled shard"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn broadcast_all_returns_without_awaiting_replies() {
        // Both shards stall. `broadcast_all` awaits no reply, so it returns
        // immediately regardless — a stalled shard cannot block a teardown.
        let (senders, _tasks) = mock_cluster(vec![Behavior::Hang, Behavior::Hang]);
        let sg = ScatterGather::new(&senders, Duration::from_secs(2), 0);
        let start = tokio::time::Instant::now();
        sg.broadcast_all(dbsize_broadcast_msg).await;
        let elapsed = start.elapsed();
        assert_eq!(elapsed, Duration::ZERO, "fire-and-forget awaits no reply");
    }
}
