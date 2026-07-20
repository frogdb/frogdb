//! Whole-history conservation checkers (pure scans, not WGL-based).

use crate::history::{CompletedOperation, History};
use crate::partition::{default_keys_of, is_errored_exec_result, parse_exec_commands};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

/// A conservation-invariant violation. Carries the offending op id(s) and a
/// human-readable description (via the `Display`/`Error` message).
#[derive(Debug, thiserror::Error)]
pub enum ConservationViolation {
    /// An element was delivered (or left in final state) more times than pushed.
    #[error(
        "element {element:?} (pushed by op {pushed_by}) delivered {times} times, over-consumed"
    )]
    MultipleDelivery {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of a push that introduced it.
        pushed_by: u64,
        /// Observed delivery count.
        times: usize,
    },
    /// An element was pushed but neither delivered nor present at quiesce.
    #[error(
        "element {element:?} (pushed by op {pushed_by}) was neither delivered nor in final state"
    )]
    LostElement {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of the push that introduced it.
        pushed_by: u64,
    },
    /// An element was delivered that was never pushed.
    #[error("element {element:?} delivered by op {delivered_by} was never pushed")]
    PhantomDelivery {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of the delivering pop.
        delivered_by: u64,
    },
    /// A transfer workload failed to conserve the sum over the tracked keys.
    #[error("transaction sum not conserved on {keys:?}: expected {expected}, computed {computed}")]
    SumMismatch {
        /// The tracked keys.
        keys: Vec<String>,
        /// The invariant target sum.
        expected: i64,
        /// The sum implied by the recorded history.
        computed: i64,
    },
    /// A committed EXEC ignored a concurrent write to a watched key.
    #[error(
        "watch false-negative: exec op {exec_op} committed though op {writer_op} wrote watched key {key:?} after watch op {watch_op}"
    )]
    WatchFalseNegative {
        /// The committed exec op id.
        exec_op: u64,
        /// The watch op id.
        watch_op: u64,
        /// The interfering writer op id.
        writer_op: u64,
        /// The watched key.
        key: Vec<u8>,
    },
    /// Blocked poppers on a key were not served in registration (invoke) order.
    #[error(
        "FIFO wake order violated on key {key:?}: op {served} (later waiter) served before op {waiter}"
    )]
    FifoViolation {
        /// The key.
        key: Vec<u8>,
        /// The op served out of order.
        served: u64,
        /// The earlier waiter it jumped ahead of.
        waiter: u64,
    },
    /// A stream entry is reported pending for two different consumers at once.
    #[error("PEL entry {id:?} double-owned: consumers {a:?} and {b:?} (op {op})")]
    PelDoubleOwned {
        /// The double-owned entry id.
        id: String,
        /// One reported owner.
        a: String,
        /// The other reported owner.
        b: String,
        /// The op id of the XPENDING summary that exposed the contradiction.
        op: u64,
    },
    /// A stream entry was acked yet later reported still pending.
    #[error("PEL entry {id:?} acked by op {ack_op} but reported pending by op {pending_op}")]
    PelAckedButPending {
        /// The entry id.
        id: String,
        /// Op id of the XACK.
        ack_op: u64,
        /// Op id of the later read that still reported it pending.
        pending_op: u64,
    },
    /// An XADD'd stream entry was skipped over: a later `>` read delivered a
    /// higher id while this entry was never delivered, PEL'd, or acked.
    #[error("stream entry {id:?} (added by op {added_by}) was lost (skipped by a later '>' read)")]
    StreamEntryLost {
        /// The lost entry id.
        id: String,
        /// Op id of the XADD that introduced it.
        added_by: u64,
    },
}

/// Every pushed element is delivered to exactly one popper XOR present at
/// quiesce; no element delivered twice or lost. `final_elements` maps each key
/// to the elements remaining in its list after the workload drains.
///
/// Accounting is by element *value* across all keys combined, not per-key:
/// count conservation (pushed == delivered + left-over) is checked, but an
/// element that gets misrouted to the wrong key (e.g. via a buggy `lmove`)
/// is not detected as long as the total counts still balance. This is a
/// deliberate tradeoff — it catches loss and duplication cheaply without
/// needing per-key push/delivery bookkeeping.
pub fn check_exactly_once_delivery(
    history: &History,
    final_elements: &HashMap<Bytes, Vec<Bytes>>,
) -> Result<(), ConservationViolation> {
    fn record_push(
        value: Bytes,
        op: u64,
        pushed: &mut HashMap<Bytes, i64>,
        push_op: &mut HashMap<Bytes, u64>,
    ) {
        *pushed.entry(value.clone()).or_default() += 1;
        push_op.entry(value).or_insert(op);
    }

    let mut pushed: HashMap<Bytes, i64> = HashMap::new();
    let mut push_op: HashMap<Bytes, u64> = HashMap::new();
    let mut delivered: HashMap<Bytes, (i64, u64)> = HashMap::new(); // count, last op id

    for op in history.completed_operations() {
        match op.function.as_str() {
            // Plain pushes plus the list-effect script pseudo-ops, which
            // LPUSH/RPUSH `ARGV[1]` (`op.args[1]`) and return the new LLEN —
            // observably introducing exactly one element, same as a bare push.
            "lpush" | "rpush" | "script_lpush_llen" | "script_rpush_llen" => {
                // A push with no result is a failed/indeterminate op: it
                // never observably introduced its elements, so counting it
                // as pushed would make any later non-delivery of those
                // elements register as a false LostElement.
                if op.result.is_some() {
                    for v in op.args.iter().skip(1) {
                        record_push(v.clone(), op.id, &mut pushed, &mut push_op);
                    }
                }
            }
            "lpop" | "rpop" => {
                if let Some(r) = &op.result {
                    let e = delivered.entry(r.clone()).or_insert((0, op.id));
                    e.0 += 1;
                    e.1 = op.id;
                }
            }
            "blpop" | "brpop" => {
                if let Some(r) = &op.result
                    && let Some((_, elem)) = String::from_utf8_lossy(r).split_once('|')
                {
                    let key = Bytes::from(elem.to_string());
                    let e = delivered.entry(key).or_insert((0, op.id));
                    e.0 += 1;
                    e.1 = op.id;
                }
            }
            "lmove" | "blmove" => {
                if let Some(r) = &op.result {
                    // Counts as both a delivery (from src) and a push (to dst).
                    let e = delivered.entry(r.clone()).or_insert((0, op.id));
                    e.0 += 1;
                    e.1 = op.id;
                    record_push(r.clone(), op.id, &mut pushed, &mut push_op);
                }
            }
            _ => {}
        }
    }

    let mut final_counts: HashMap<Bytes, i64> = HashMap::new();
    for elems in final_elements.values() {
        for e in elems {
            *final_counts.entry(e.clone()).or_default() += 1;
        }
    }

    let mut values: HashSet<Bytes> = HashSet::new();
    values.extend(pushed.keys().cloned());
    values.extend(delivered.keys().cloned());
    values.extend(final_counts.keys().cloned());

    for v in values {
        let p = pushed.get(&v).copied().unwrap_or(0);
        let (d, last_op) = delivered.get(&v).copied().unwrap_or((0, 0));
        let f = final_counts.get(&v).copied().unwrap_or(0);
        if p == 0 && d > 0 {
            return Err(ConservationViolation::PhantomDelivery {
                element: v.to_vec(),
                delivered_by: last_op,
            });
        }
        if d + f > p {
            return Err(ConservationViolation::MultipleDelivery {
                element: v.to_vec(),
                pushed_by: push_op.get(&v).copied().unwrap_or(0),
                // Total observed count, not just deliveries: when final-
                // state duplication contributes to the over-count,
                // reporting `d` alone would understate what was observed.
                times: (d + f) as usize,
            });
        }
        if d + f < p {
            return Err(ConservationViolation::LostElement {
                element: v.to_vec(),
                pushed_by: push_op.get(&v).copied().unwrap_or(0),
            });
        }
    }
    Ok(())
}

/// Blocked poppers (BLPOP/BRPOP hits) on a key are served in invoke order.
///
/// Waiters are grouped by the key each was actually *served* from — parsed
/// out of the hit-encoding `"served_key|elem"` in the op result — rather
/// than by `op.args.first()`. For multi-key blocking pops (e.g.
/// `blpop k1 k2 0`), the first watched key need not be the key that ended
/// up serving the op, so grouping by it can silently split one logical
/// wake-order queue across multiple key buckets and hide real violations.
/// Ops that timed out (`result == None`) are skipped: they were never
/// served and carry no wake-order information.
///
/// Known limitation: this check uses invoke-time order as a proxy for the
/// server's actual registration order. Two waiters with near-simultaneous,
/// overlapping invokes can legitimately register with the server in either
/// order, so this can flag a false FIFO violation in that narrow race
/// window. Phase 2's `DEBUG WAITQUEUE` registration-order dumps will let
/// this check use exact registration order instead of invoke order;
/// phase 3's generator will stagger blocking invokes to keep generated
/// histories out of that window.
pub fn check_fifo_wake_order(history: &History) -> Result<(), ConservationViolation> {
    // served_key -> [(invoke_time, return_time, op_id)] for served blocking
    // pops, grouped by the key each waiter was actually served from.
    let mut by_key: HashMap<Bytes, Vec<(u64, u64, u64)>> = HashMap::new();
    for op in history.completed_operations() {
        if !matches!(op.function.as_str(), "blpop" | "brpop") {
            continue;
        }
        let Some(result) = &op.result else {
            // Timed out: never served, no ordering information to check.
            continue;
        };
        let result_str = String::from_utf8_lossy(result);
        let Some((served_key, _)) = result_str.split_once('|') else {
            continue;
        };
        by_key
            .entry(Bytes::from(served_key.to_string()))
            .or_default()
            .push((op.invoke_time, op.return_time, op.id));
    }
    for (key, mut served) in by_key {
        served.sort_by_key(|x| x.1); // by serve (return) order
        for w in served.windows(2) {
            if w[0].0 > w[1].0 {
                // Served earlier but invoked later -> jumped an earlier waiter.
                return Err(ConservationViolation::FifoViolation {
                    key: key.to_vec(),
                    served: w[0].2,
                    waiter: w[1].2,
                });
            }
        }
    }
    Ok(())
}

/// Net integer delta a single command applies to keys in `keyset`.
fn cmd_delta(name: &str, args: &[Bytes], keyset: &HashSet<Bytes>) -> i64 {
    if args.is_empty() || !keyset.contains(&args[0]) {
        return 0;
    }
    let by = || {
        args.get(1)
            .and_then(|a| String::from_utf8_lossy(a).parse::<i64>().ok())
            .unwrap_or(0)
    };
    match name {
        "incr" => 1,
        "decr" => -1,
        "incrby" => by(),
        "decrby" => -by(),
        _ => 0,
    }
}

/// Bank-transfer conservation: the total over `keys` must not change, so the
/// final sum equals `expected_sum`. Sums INCR/DECR(BY) deltas from committed
/// EXECs and standalone counter ops; a nonzero net delta is a violation.
pub fn check_tx_sum_conservation(
    history: &History,
    keys: &[Bytes],
    expected_sum: i64,
) -> Result<(), ConservationViolation> {
    let keyset: HashSet<Bytes> = keys.iter().cloned().collect();
    let mut delta: i64 = 0;
    for op in history.completed_operations() {
        match op.function.as_str() {
            "exec" => {
                if !exec_committed(op.result.as_ref()) {
                    continue; // aborted or CROSSSLOT-rejected: applied no deltas
                }
                for (name, cargs) in parse_exec_commands(&op.args).unwrap_or_default() {
                    delta += cmd_delta(&name, &cargs, &keyset);
                }
            }
            "incr" | "decr" | "incrby" | "decrby" => {
                delta += cmd_delta(&op.function, &op.args, &keyset);
            }
            _ => {}
        }
    }
    if delta != 0 {
        return Err(ConservationViolation::SumMismatch {
            keys: keys
                .iter()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .collect(),
            expected: expected_sum,
            computed: expected_sum + delta,
        });
    }
    Ok(())
}

/// True iff an `exec` op's recorded result denotes a committed transaction (a
/// non-nil, non-errored result). A `None` (WATCH-abort) or an `"ERR:…"`
/// (CROSSSLOT/EXECABORT) result is NOT a commit.
fn exec_committed(result: Option<&Bytes>) -> bool {
    result.is_some_and(|r| !is_errored_exec_result(r))
}

fn is_write(function: &str) -> bool {
    matches!(
        function,
        "set"
            | "write"
            | "cas"
            | "del"
            | "delete"
            | "incr"
            | "incrby"
            | "decr"
            | "decrby"
            | "lpush"
            | "rpush"
            | "lpop"
            | "rpop"
            | "hset"
            | "hdel"
            | "hincrby"
            | "zadd"
            | "zrem"
            | "mset"
            | "xadd"
            | "lmove"
            | "blmove"
            | "blpop"
            | "brpop"
            | "bzpopmin"
            | "bzpopmax"
    )
}

/// Keys actually *written* by a completed op, as opposed to the keys it
/// merely touched/watched (`default_keys_of`). Two corrections matter here:
///
/// - Pop/move ops (`lpop`/`rpop`/`blpop`/`brpop`/`bzpopmin`/`bzpopmax`/
///   `lmove`/`blmove`) only mutate state when they actually served an
///   element; a nil/timeout result (`result == None`) is a no-op and must
///   not count as a write.
/// - For the blocking multi-key pops, the key that was actually served is
///   encoded in the result (`"served_key|elem"` / `"served_key|member|score"`)
///   and need not be `args[0]` — mirrors the parsing
///   [`check_fifo_wake_order`] uses to group waiters by served key, rather
///   than by the full watched-key list `default_keys_of` would return.
/// - `lmove`/`blmove` write *both* the source (pop) and destination (push)
///   keys once they've actually served (non-nil result).
fn written_keys_of(function: &str, args: &[Bytes], result: Option<&Bytes>) -> Vec<Bytes> {
    match function {
        "lpop" | "rpop" => {
            if result.is_some() {
                args.first().cloned().into_iter().collect()
            } else {
                Vec::new()
            }
        }
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => {
            let Some(r) = result else {
                return Vec::new();
            };
            let served = String::from_utf8_lossy(r);
            match served.split_once('|') {
                Some((key, _)) => vec![Bytes::from(key.to_string())],
                None => Vec::new(),
            }
        }
        "lmove" | "blmove" => {
            if result.is_some() {
                args.iter().take(2).cloned().collect()
            } else {
                Vec::new()
            }
        }
        _ => default_keys_of(function, args),
    }
}

/// Find a completed write to `key` by a client other than `exclude_client`
/// that is *definitely between* the WATCH and the EXEC invoke: invoked
/// strictly after `lo` (the WATCH's return time) and returned strictly
/// before `hi` (the EXEC's invoke time), i.e. fully contained in the
/// `(lo, hi)` window rather than merely overlapping it.
///
/// This containment requirement matters: a writer that merely *overlaps*
/// the window — e.g. one that invoked before the WATCH returned, or
/// returned after the EXEC was invoked — is concurrent with the WATCH's
/// snapshot point (or the EXEC's), and a real Redis server is free to
/// linearize it on either side. Flagging such an overlapping writer as a
/// false negative would reject legal histories; only a writer with no
/// possible linearization outside the gap proves the EXEC should have
/// aborted.
///
/// A committed `exec` by another client is also treated as a writer: its
/// sub-commands are parsed via [`parse_exec_commands`] and checked against
/// the write vocabulary and per-command key extraction, so an interfering
/// write hidden inside another client's transaction is not invisible here.
fn writer_between(
    ops: &[CompletedOperation],
    key: &Bytes,
    lo: u64,
    hi: u64,
    exclude_client: u64,
) -> Option<u64> {
    for op in ops {
        if op.client_id == exclude_client {
            continue;
        }
        // Definitely-between: fully contained in the (lo, hi) gap, not just
        // overlapping it.
        if !(op.invoke_time > lo && op.return_time < hi) {
            continue;
        }
        if is_write(&op.function)
            && written_keys_of(&op.function, &op.args, op.result.as_ref())
                .iter()
                .any(|k| k == key)
        {
            return Some(op.id);
        }
        if op.function == "exec"
            && exec_committed(op.result.as_ref())
            && let Some(cmds) = parse_exec_commands(&op.args)
            && cmds
                .iter()
                .any(|(name, cargs)| is_write(name) && default_keys_of(name, cargs).contains(key))
        {
            return Some(op.id);
        }
    }
    None
}

/// WATCH no-false-negative: a committed EXEC must not have ignored another
/// client's write to a watched key that was *definitely* concurrent with the
/// watch window, i.e. invoked after the WATCH returned (so it could not have
/// been visible to the WATCH's snapshot) and returned before the EXEC was
/// invoked (so it could not have been ordered after the EXEC's dirty-key
/// check). A writer that merely overlaps either endpoint may legally
/// linearize on either side of the WATCH snapshot and is not checked here
/// (see [`writer_between`]). Over-abort is legal and not checked here.
///
/// Deliberate narrowing: this only considers writes by *other* clients.
/// Real Redis also dirties a key's watch when the *same* client writes it
/// before its own MULTI/EXEC (a self-write between WATCH and EXEC aborts the
/// transaction too), but that case is excluded here. This is conservative —
/// it can only miss violations, never manufacture a false one — so it does
/// not compromise the no-false-negative soundness claim; it merely means
/// same-client dirtying is not yet covered by this checker.
pub fn check_watch_no_false_negative(history: &History) -> Result<(), ConservationViolation> {
    let ops = history.completed_operations();
    let mut by_client: HashMap<u64, Vec<&CompletedOperation>> = HashMap::new();
    for op in &ops {
        by_client.entry(op.client_id).or_default().push(op);
    }
    for (_client, mut cops) in by_client {
        cops.sort_by_key(|o| o.invoke_time);
        // (key, watch_return_time, watch_op_id)
        let mut watched: Vec<(Bytes, u64, u64)> = Vec::new();
        for op in cops {
            match op.function.as_str() {
                "watch" => {
                    for k in &op.args {
                        watched.push((k.clone(), op.return_time, op.id));
                    }
                }
                "exec" => {
                    if exec_committed(op.result.as_ref()) {
                        for (k, wt, wid) in &watched {
                            if let Some(writer) =
                                writer_between(&ops, k, *wt, op.invoke_time, op.client_id)
                            {
                                return Err(ConservationViolation::WatchFalseNegative {
                                    exec_op: op.id,
                                    watch_op: *wid,
                                    writer_op: writer,
                                    key: k.to_vec(),
                                });
                            }
                        }
                    }
                    watched.clear();
                }
                "discard" | "reset" | "unwatch" => watched.clear(),
                _ => {}
            }
        }
    }
    Ok(())
}

/// PEL conservation for consumer-group streams. Scans the whole history:
/// (i) no entry pending for two consumers at once (partial — see note below);
/// (ii) no entry both acked and later reported pending; (iii) no entry skipped:
/// a `>` read that starts after an entry's XADD completes and delivers a
/// HIGHER id must have delivered (or previously delivered/acked) that entry —
/// `>` delivers in id order, so a higher id with the entry absent everywhere
/// means the server lost it. Entries added after the last read are
/// legitimately undelivered and are NOT flagged. Delivery-count monotonicity
/// is unobservable with summary-form XPENDING; deferred to Phase-4b's
/// extended-form vocabulary.
///
/// Double-ownership detection is necessarily partial: XPENDING's summary
/// form (`total|min|max|consumer:n,…`) gives a total count, an id *range*,
/// and per-consumer counts, but never the full id list, so most
/// double-ownership is invisible to it. The one self-contradiction it can
/// expose is a degenerate range (`min == max`, i.e. exactly one distinct id
/// in range) whose total exceeds 1: that single id cannot legitimately be
/// pending more than once, so such a summary is direct proof it is
/// multiply-owned.
pub fn check_pel_conservation(history: &History) -> Result<(), ConservationViolation> {
    let ops = history.completed_operations();

    // Parse "id,f,v|..." into the entry ids it delivered.
    fn delivered_ids(r: &Bytes) -> Vec<String> {
        String::from_utf8_lossy(r)
            .split('|')
            .filter(|e| !e.is_empty())
            .filter_map(|e| e.split(',').next().map(str::to_string))
            .collect()
    }

    // "ms-seq" -> (ms, seq) for order comparison.
    fn id_tuple(s: &str) -> Option<(u64, u64)> {
        let (ms, seq) = s.split_once('-')?;
        Some((ms.parse().ok()?, seq.parse().ok()?))
    }

    // A "c:n" consumer:count token -> just the consumer name.
    fn consumer_name(tok: &str) -> String {
        tok.rsplit_once(':')
            .map_or(tok, |(name, _)| name)
            .to_string()
    }

    // XADD's stream key is args[0]; it has no group.
    fn xadd_stream(args: &[Bytes]) -> Option<String> {
        Some(String::from_utf8_lossy(args.first()?).to_string())
    }

    // XACK / XPENDING / XCLAIM all start "key group ...".
    fn stream_group_prefix(args: &[Bytes]) -> Option<(String, String)> {
        Some((
            String::from_utf8_lossy(args.first()?).to_string(),
            String::from_utf8_lossy(args.get(1)?).to_string(),
        ))
    }

    // XREADGROUP's group and stream key are positional after the GROUP and
    // STREAMS keywords respectively: "GROUP g c [COUNT n] STREAMS key id".
    fn xreadgroup_stream_group(args: &[Bytes]) -> Option<(String, String)> {
        let gi = args.iter().position(|a| a.eq_ignore_ascii_case(b"GROUP"))?;
        let group = String::from_utf8_lossy(args.get(gi + 1)?).to_string();
        let si = args
            .iter()
            .position(|a| a.eq_ignore_ascii_case(b"STREAMS"))?;
        let stream = String::from_utf8_lossy(args.get(si + 1)?).to_string();
        Some((stream, group))
    }

    // (stream, id) -> add op. XADD has no group: an added entry is visible
    // to every group on its stream, so it is scoped by stream only.
    let mut added: HashMap<(String, String), u64> = HashMap::new();
    // (stream, id) -> ever observed delivered/PEL'd in ANY group of that
    // stream. Used only for the lost-check (iii), which is inherently a
    // per-stream property ("readable via `>`, PEL'd, or acked" somewhere),
    // not a per-group one — a group that hasn't read up to an id yet is not
    // "losing" it, so group identity is deliberately erased here.
    let mut ever_pending: HashSet<(String, String)> = HashSet::new();
    // (stream, group, id) -> ever observed pending in that EXACT group.
    // Used to gate XACK legitimacy (i) and scoped acked lookups (ii): PELs
    // are per (stream, group), so a delivery/pending observation in one
    // group must never satisfy a check about a different group.
    let mut pel_pending: HashSet<(String, String, String)> = HashSet::new();
    // (stream, group, id) -> ack op, only for ids gated as genuinely
    // pending at ack time (see the "xack" arm below).
    let mut acked: HashMap<(String, String, String), u64> = HashMap::new();

    for op in &ops {
        match op.function.as_str() {
            "xadd" => {
                if let (Some(stream), Some(r)) = (xadd_stream(&op.args), &op.result) {
                    added
                        .entry((stream, String::from_utf8_lossy(r).to_string()))
                        .or_insert(op.id);
                }
            }
            "xreadgroup" => {
                if let (Some((stream, group)), Some(r)) =
                    (xreadgroup_stream_group(&op.args), &op.result)
                {
                    for id in delivered_ids(r) {
                        ever_pending.insert((stream.clone(), id.clone()));
                        pel_pending.insert((stream.clone(), group.clone(), id));
                    }
                }
            }
            "xclaim" => {
                if let (Some((stream, group)), Some(r)) =
                    (stream_group_prefix(&op.args), &op.result)
                {
                    for id in delivered_ids(r) {
                        ever_pending.insert((stream.clone(), id.clone()));
                        pel_pending.insert((stream.clone(), group.clone(), id));
                    }
                }
            }
            "xack" => {
                if let Some((stream, group)) = stream_group_prefix(&op.args) {
                    for id_arg in op.args.iter().skip(2) {
                        let id = String::from_utf8_lossy(id_arg).to_string();
                        // Only a genuine ack: the id must have already been
                        // observed pending in this EXACT (stream, group)
                        // earlier in the scan. A no-op XACK of an id never
                        // delivered to this group (returns 0) must not
                        // register as an ack — otherwise the id's later,
                        // legitimate first delivery would look like
                        // "delivered after ack".
                        let key = (stream.clone(), group.clone(), id);
                        if pel_pending.contains(&key) {
                            acked.entry(key).or_insert(op.id);
                        }
                    }
                }
            }
            "xpending" => {
                if let (Some((stream, group)), Some(r)) =
                    (stream_group_prefix(&op.args), &op.result)
                {
                    let s = String::from_utf8_lossy(r);
                    if s != "0" {
                        let fields: Vec<&str> = s.split('|').collect();
                        if fields.len() >= 4 {
                            let total: usize = fields[0].parse().unwrap_or(0);
                            let (min, max) = (fields[1], fields[2]);
                            if min == max && total > 1 {
                                let mut names = fields[3].split(',').map(consumer_name);
                                let a = names.next().unwrap_or_default();
                                let b = names.next().unwrap_or_else(|| a.clone());
                                return Err(ConservationViolation::PelDoubleOwned {
                                    id: min.to_string(),
                                    a,
                                    b,
                                    op: op.id,
                                });
                            }
                            // Summary form only exposes the range endpoints,
                            // not the full pending set, so only min/max are
                            // known to be pending as of this observation.
                            ever_pending.insert((stream.clone(), min.to_string()));
                            ever_pending.insert((stream.clone(), max.to_string()));
                            pel_pending.insert((stream.clone(), group.clone(), min.to_string()));
                            pel_pending.insert((stream.clone(), group.clone(), max.to_string()));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // (ii) acked-but-pending: an id present in a re-read AFTER its ack, in
    // the SAME (stream, group) as the ack — an ack in one group's PEL says
    // nothing about a different group's independent copy of the entry.
    for op in &ops {
        if op.function == "xreadgroup"
            && let Some((stream, group)) = xreadgroup_stream_group(&op.args)
            && let Some(r) = &op.result
        {
            for id in delivered_ids(r) {
                let key = (stream.clone(), group.clone(), id.clone());
                if let Some(&ack_op) = acked.get(&key) {
                    let ack_return = ops
                        .iter()
                        .find(|o| o.id == ack_op)
                        .map_or(u64::MAX, |o| o.return_time);
                    if op.invoke_time > ack_return {
                        return Err(ConservationViolation::PelAckedButPending {
                            id,
                            ack_op,
                            pending_op: op.id,
                        });
                    }
                }
            }
        }
    }

    // (iii) nothing skipped: an entry absent from every delivery/PEL/ack on
    // its stream while a `>` read on THAT SAME STREAM, started after its
    // add completed, delivered a HIGHER id. Restricted to `>` reads: a "0"
    // re-read shows old PEL entries whose delivery may predate this add,
    // which implies nothing about a skip. Restricted to the same stream key
    // (extracted from XREADGROUP's STREAMS clause): id order only holds
    // within a single stream, so a read on an unrelated stream delivering a
    // numerically higher id is not evidence that this stream lost anything.
    //
    // Note: a `>` read with COUNT truncates the *tail* of the in-order
    // delivery, never the middle, so COUNT cannot cause a false skip.
    // Entries added after the last `>` read on their stream are never
    // flagged (no later read exists to compare against).
    for ((stream, id), add_op) in &added {
        if ever_pending.contains(&(stream.clone(), id.clone())) {
            continue;
        }
        let Some(eid) = id_tuple(id) else { continue };
        let add_return = ops
            .iter()
            .find(|o| o.id == *add_op)
            .map_or(u64::MAX, |o| o.return_time);
        let skipped = ops.iter().any(|o| {
            o.function == "xreadgroup"
                && o.args.last().is_some_and(|a| a.as_ref() == b">")
                && xreadgroup_stream_group(&o.args).is_some_and(|(s, _)| s == *stream)
                && o.invoke_time > add_return
                && o.result.as_ref().is_some_and(|r| {
                    delivered_ids(r)
                        .iter()
                        .any(|d| id_tuple(d).is_some_and(|dt| dt > eid))
                })
        });
        if skipped {
            return Err(ConservationViolation::StreamEntryLost {
                id: id.clone(),
                added_by: *add_op,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::History;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn push_pop_history() -> History {
        let mut h = History::new();
        let p1 = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(p1, Some(b("1")));
        let p2 = h.invoke(1, "rpush", vec![b("k"), b("b")]);
        h.respond(p2, Some(b("2")));
        let q1 = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q1, Some(b("a")));
        let q2 = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q2, Some(b("b")));
        h
    }

    #[test]
    fn delivery_ok_when_all_consumed() {
        let h = push_pop_history();
        assert!(check_exactly_once_delivery(&h, &HashMap::new()).is_ok());
    }

    #[test]
    fn delivery_ok_with_leftover_in_final_state() {
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![b("k"), b("x")]);
        h.respond(p, Some(b("1")));
        let mut final_state = HashMap::new();
        final_state.insert(b("k"), vec![b("x")]);
        assert!(check_exactly_once_delivery(&h, &final_state).is_ok());
    }

    #[test]
    fn delivery_counts_list_effect_scripts_as_pushes() {
        // script_lpush_llen / script_rpush_llen push ARGV[1] and return LLEN;
        // the checker must count them as pushes so a later pop of that element
        // is not a PhantomDelivery.
        let mut h = History::new();
        let p1 = h.invoke(1, "script_rpush_llen", vec![b("k"), b("a")]);
        h.respond(p1, Some(b("1")));
        let p2 = h.invoke(1, "script_lpush_llen", vec![b("k"), b("b")]);
        h.respond(p2, Some(b("2")));
        // Deliver "a" via a plain pop; "b" remains in final state.
        let q = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q, Some(b("a")));
        let mut final_state = HashMap::new();
        final_state.insert(b("k"), vec![b("b")]);
        assert!(check_exactly_once_delivery(&h, &final_state).is_ok());
    }

    #[test]
    fn delivery_detects_double_pop() {
        let mut h = push_pop_history();
        // A second, illegal delivery of "a".
        let q = h.invoke(3, "lpop", vec![b("k")]);
        h.respond(q, Some(b("a")));
        match check_exactly_once_delivery(&h, &HashMap::new()) {
            Err(ConservationViolation::MultipleDelivery { times, .. }) => assert_eq!(times, 2),
            other => panic!("expected MultipleDelivery, got {other:?}"),
        }
    }

    #[test]
    fn delivery_detects_lost_element() {
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(p, Some(b("1")));
        // Never delivered, not in final state -> lost.
        assert!(matches!(
            check_exactly_once_delivery(&h, &HashMap::new()),
            Err(ConservationViolation::LostElement { .. })
        ));
    }

    #[test]
    fn fifo_ok_when_served_in_invoke_order() {
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(w1, Some(b("k|a")));
        h.respond(w2, Some(b("k|b")));
        assert!(check_fifo_wake_order(&h).is_ok());
    }

    #[test]
    fn fifo_detects_out_of_order_wake() {
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        // w2 (later waiter) served before w1 (earlier waiter).
        h.respond(w2, Some(b("k|b")));
        h.respond(w1, Some(b("k|a")));
        assert!(matches!(
            check_fifo_wake_order(&h),
            Err(ConservationViolation::FifoViolation { .. })
        ));
    }

    #[test]
    fn delivery_lmove_double_role() {
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![b("a"), b("x")]);
        h.respond(p, Some(b("1")));
        let m = h.invoke(2, "lmove", vec![b("a"), b("b"), b("left"), b("right")]);
        h.respond(m, Some(b("x")));

        // "x" ends up in b's final list: pushed once (rpush), moved once
        // (lmove counts as both a delivery from "a" and a push to "b"), and
        // present once at quiesce -> conserved.
        let mut final_state = HashMap::new();
        final_state.insert(b("b"), vec![b("x")]);
        assert!(check_exactly_once_delivery(&h, &final_state).is_ok());

        // Same history, but "x" never actually landed in "b"'s final list
        // -> the lmove's push-side contribution is unaccounted for, so the
        // element is lost overall.
        assert!(matches!(
            check_exactly_once_delivery(&h, &HashMap::new()),
            Err(ConservationViolation::LostElement { .. })
        ));
    }

    #[test]
    fn delivery_blpop_hit_parsed() {
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![b("k"), b("x")]);
        h.respond(p, Some(b("1")));
        let bl = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(bl, Some(b("k|x")));

        assert!(check_exactly_once_delivery(&h, &HashMap::new()).is_ok());
    }

    #[test]
    fn delivery_phantom_pop() {
        let mut h = History::new();
        let p = h.invoke(1, "lpop", vec![b("k")]);
        h.respond(p, Some(b("ghost")));

        assert!(matches!(
            check_exactly_once_delivery(&h, &HashMap::new()),
            Err(ConservationViolation::PhantomDelivery { .. })
        ));
    }

    #[test]
    fn fifo_multikey_served_key_grouping() {
        let mut h = History::new();
        // waiter1 invokes first, watching two keys (k1 and k2).
        let w1 = h.invoke(1, "blpop", vec![b("k1"), b("k2"), b("0")]);
        // waiter2 invokes later, watching only k2.
        let w2 = h.invoke(2, "blpop", vec![b("k2"), b("0")]);
        // waiter2 (later invoke) is served from k2 first...
        h.respond(w2, Some(b("k2|a")));
        // ...and waiter1 (earlier invoke) is served from k2 second: this is
        // a FIFO violation, but only detectable when both waiters are
        // grouped by the *served* key (k2), not by op.args.first() (which
        // would put waiter1 under k1 and waiter2 under k2, hiding the
        // violation).
        h.respond(w1, Some(b("k2|b")));

        assert!(matches!(
            check_fifo_wake_order(&h),
            Err(ConservationViolation::FifoViolation { .. })
        ));
    }

    fn transfer(h: &mut History, client: u64, from: &str, to: &str, amt: i64) {
        // EXEC: DECRBY from amt, INCRBY to amt -> two integer replies.
        let op = h.invoke(
            client,
            "exec",
            vec![
                b("2"),
                b("decrby"),
                b("2"),
                b(from),
                Bytes::from(amt.to_string()),
                b("incrby"),
                b("2"),
                b(to),
                Bytes::from(amt.to_string()),
            ],
        );
        h.respond(op, Some(b("0|0")));
    }

    #[test]
    fn tx_sum_conserved_under_transfers() {
        let mut h = History::new();
        transfer(&mut h, 1, "a", "b", 5);
        transfer(&mut h, 2, "b", "a", 3);
        let keys = vec![b("a"), b("b")];
        assert!(check_tx_sum_conservation(&h, &keys, 100).is_ok());
    }

    #[test]
    fn tx_sum_detects_leak() {
        let mut h = History::new();
        // Only credit b, never debit a -> +5 net, not conserved.
        let op = h.invoke(1, "exec", vec![b("1"), b("incrby"), b("2"), b("b"), b("5")]);
        h.respond(op, Some(b("0")));
        let keys = vec![b("a"), b("b")];
        assert!(matches!(
            check_tx_sum_conservation(&h, &keys, 100),
            Err(ConservationViolation::SumMismatch { .. })
        ));
    }

    #[test]
    fn watch_ok_when_no_interfering_write() {
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(check_watch_no_false_negative(&h).is_ok());
    }

    #[test]
    fn watch_detects_false_negative() {
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        // Another client writes the watched key after the WATCH...
        let other = h.invoke(2, "set", vec![b("k"), b("z")]);
        h.respond(other, Some(b("OK")));
        // ...yet this client's EXEC commits -> false negative.
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(matches!(
            check_watch_no_false_negative(&h),
            Err(ConservationViolation::WatchFalseNegative { .. })
        ));
    }

    #[test]
    fn watch_aborted_exec_is_fine() {
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "set", vec![b("k"), b("z")]);
        h.respond(other, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, None); // aborted -> correct behavior
        assert!(check_watch_no_false_negative(&h).is_ok());
    }

    // --- Definitely-between window soundness ---------------------------

    #[test]
    fn watch_overlapping_writer_not_flagged() {
        // The other client's writer INVOKES before the WATCH RETURNS (so it
        // overlaps the WATCH itself, rather than being fully contained in
        // the watch->exec gap), but it RETURNS before the EXEC is invoked.
        // This writer is concurrent with the WATCH's snapshot point and can
        // legally linearize on either side of it, so a sound checker must
        // not flag it. Pre-fix, the old code used the WATCH's invoke_time
        // as the lower bound and only checked the writer's return_time,
        // so it incorrectly flagged this legal history.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        let other = h.invoke(2, "set", vec![b("k"), b("z")]); // invoked before watch returns
        h.respond(w, Some(b("OK"))); // watch returns after the writer's invoke
        h.respond(other, Some(b("OK"))); // writer returns before exec is invoked
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(check_watch_no_false_negative(&h).is_ok());
    }

    #[test]
    fn watch_contained_writer_flagged() {
        // The other client's writer invokes and returns entirely inside the
        // watch.return -> exec.invoke gap: definitely between, so a
        // committed EXEC that ignored it is a genuine false negative.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "set", vec![b("k"), b("z")]);
        h.respond(other, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(matches!(
            check_watch_no_false_negative(&h),
            Err(ConservationViolation::WatchFalseNegative { .. })
        ));
    }

    #[test]
    fn watch_multi_key_second_key_flagged() {
        // WATCH k1 k2; another client's write to k2 (the *second* watched
        // key) is fully contained in the gap; the EXEC only touches k1 but
        // still commits -> violation. Pre-fix, only args.first() (k1) was
        // registered, so the interfering write to k2 was invisible.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k1"), b("k2")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "set", vec![b("k2"), b("z")]);
        h.respond(other, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k1"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(matches!(
            check_watch_no_false_negative(&h),
            Err(ConservationViolation::WatchFalseNegative { .. })
        ));
    }

    #[test]
    fn watch_blpop_writer_flagged() {
        // Watcher WATCHes k; another client's BLPOP (fully contained in the
        // watch->exec gap) serves "k|x" -- a mutating pop of the watched key
        // -- yet the watcher's EXEC still commits -> false negative. BLPOP
        // is not in the historical write vocabulary, so pre-fix this write
        // is invisible to the checker.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(other, Some(b("k|x")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(matches!(
            check_watch_no_false_negative(&h),
            Err(ConservationViolation::WatchFalseNegative { .. })
        ));
    }

    #[test]
    fn watch_nil_pop_not_flagged() {
        // Another client's LPOP on the watched key times out/misses (nil
        // result -- no mutation occurred), fully contained in the gap. The
        // watcher's EXEC still commits, which is correct: a non-mutating
        // pop is not a write and must not be flagged. Pre-fix, lpop/rpop
        // were treated as unconditional writers regardless of result, so
        // this legal history was incorrectly flagged.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(other, None);
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(check_watch_no_false_negative(&h).is_ok());
    }

    #[test]
    fn watch_errored_exec_is_not_a_commit() {
        // Watcher's EXEC is CROSSSLOT-rejected (ERR:) despite an interfering
        // write: a rejected transaction did not commit, so it is NOT a false
        // negative.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "set", vec![b("k"), b("z")]);
        h.respond(other, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(
            e,
            Some(b(
                "ERR:EXECABORT Transaction discarded because of previous errors.",
            )),
        );
        assert!(check_watch_no_false_negative(&h).is_ok());
    }

    #[test]
    fn tx_sum_ignores_errored_exec() {
        // An errored EXEC applied no deltas; counting them would falsely leak.
        let mut h = History::new();
        let op = h.invoke(1, "exec", vec![b("1"), b("incrby"), b("2"), b("b"), b("5")]);
        h.respond(
            op,
            Some(b(
                "ERR:CROSSSLOT Keys in request don't hash to the same slot",
            )),
        );
        let keys = vec![b("a"), b("b")];
        assert!(check_tx_sum_conservation(&h, &keys, 100).is_ok());
    }

    #[test]
    fn watch_errored_other_exec_not_a_writer() {
        // Another client's EXEC that was CROSSSLOT-rejected is not an
        // interfering write, so the watcher's committed EXEC is fine.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("z")]);
        h.respond(
            other,
            Some(b(
                "ERR:CROSSSLOT Keys in request don't hash to the same slot",
            )),
        );
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(check_watch_no_false_negative(&h).is_ok());
    }

    fn group_history() -> History {
        // XADD 1-1 ; XGROUP CREATE ; XREADGROUP > (c1 owns 1-1, dc=1) ; XACK 1-1
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a, Some(b("1-1")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        let r = h.invoke(
            2,
            "xreadgroup",
            vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b(">")],
        );
        h.respond(r, Some(b("1-1,f,v")));
        h
    }

    #[test]
    fn pel_ok_when_delivered_then_acked() {
        let mut h = group_history();
        let ack = h.invoke(2, "xack", vec![b("st"), b("g"), b("1-1")]);
        h.respond(ack, Some(b("1")));
        assert!(check_pel_conservation(&h).is_ok());
    }

    #[test]
    fn pel_detects_acked_but_still_pending() {
        let mut h = group_history();
        let ack = h.invoke(2, "xack", vec![b("st"), b("g"), b("1-1")]);
        h.respond(ack, Some(b("1")));
        // A later re-read reports 1-1 still pending for c1 -> both acked & pending.
        let rr = h.invoke(
            2,
            "xreadgroup",
            vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b("0")],
        );
        h.respond(rr, Some(b("1-1,f,v")));
        assert!(matches!(
            check_pel_conservation(&h),
            Err(ConservationViolation::PelAckedButPending { .. })
        ));
    }

    #[test]
    fn pel_detects_double_owned() {
        // 1-1 claimed to two consumers concurrently reported as pending for both.
        let mut h = group_history();
        let c1 = h.invoke(2, "xpending", vec![b("st"), b("g")]);
        h.respond(c1, Some(b("1|1-1|1-1|c1:1")));
        // A claim moves 1-1 to c2, but a stale reader still sees c1 owning it too.
        let cl = h.invoke(
            3,
            "xclaim",
            vec![b("st"), b("g"), b("c2"), b("0"), b("1-1")],
        );
        h.respond(cl, Some(b("1-1,f,v")));
        let p2 = h.invoke(3, "xpending", vec![b("st"), b("g")]);
        h.respond(p2, Some(b("2|1-1|1-1|c1:1,c2:1"))); // two owners of the same id
        assert!(matches!(
            check_pel_conservation(&h),
            Err(ConservationViolation::PelDoubleOwned { .. })
        ));
    }

    #[test]
    fn pel_noop_xack_not_treated_as_ack() {
        // A no-op XACK of an id that hasn't been delivered to this group
        // yet (returns 0) must NOT register as a real ack: the id's first
        // genuine delivery, which happens later, must not be mistaken for
        // "delivered after ack".
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("5-0"), b("f"), b("v")]);
        h.respond(a, Some(b("5-0")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        // No-op ack: 5-0 has never been delivered to this group.
        let ack = h.invoke(2, "xack", vec![b("st"), b("g"), b("5-0")]);
        h.respond(ack, Some(b("0")));
        // First real delivery of 5-0 happens AFTER the no-op ack.
        let r = h.invoke(
            2,
            "xreadgroup",
            vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b(">")],
        );
        h.respond(r, Some(b("5-0,f,v")));
        assert!(check_pel_conservation(&h).is_ok());
    }

    #[test]
    fn pel_cross_stream_not_contaminated() {
        // Two independent stream+group pairs. Stream A's entry is never
        // read by stream A's own group; stream B's group delivers a HIGHER
        // id on an unrelated `>` read. Id-order delivery only holds within
        // a single (stream, group), so stream B's read must not be treated
        // as evidence that stream A's entry was skipped.
        let mut h = History::new();
        let a1 = h.invoke(1, "xadd", vec![b("stA"), b("1-1"), b("f"), b("v")]);
        h.respond(a1, Some(b("1-1")));
        let ga = h.invoke(1, "xgroup", vec![b("CREATE"), b("stA"), b("g"), b("0")]);
        h.respond(ga, Some(b("OK")));
        let a2 = h.invoke(1, "xadd", vec![b("stB"), b("9-9"), b("f"), b("w")]);
        h.respond(a2, Some(b("9-9")));
        let gb = h.invoke(1, "xgroup", vec![b("CREATE"), b("stB"), b("g"), b("0")]);
        h.respond(gb, Some(b("OK")));
        let r = h.invoke(
            2,
            "xreadgroup",
            vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("stB"), b(">")],
        );
        h.respond(r, Some(b("9-9,f,w")));
        // stA's 1-1 was never read by stA's own group -> must NOT be
        // flagged as lost just because stB's unrelated group delivered a
        // higher id.
        assert!(check_pel_conservation(&h).is_ok());
    }

    #[test]
    fn watch_exec_writer_flagged() {
        // Another client's committed EXEC contains a `set` on the watched
        // key, fully contained in the gap; the watcher's own EXEC still
        // commits -> violation. Pre-fix, `is_write` did not recognize
        // "exec", so a write hidden inside another client's transaction
        // was invisible.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("z")]);
        h.respond(other, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(matches!(
            check_watch_no_false_negative(&h),
            Err(ConservationViolation::WatchFalseNegative { .. })
        ));
    }
}
