//! Every [`TransactionOutcome`] forced through [`execute_transaction`].
//!
//! The point of extracting the EXEC algorithm behind [`TxnHost`] is that these
//! failure modes stop needing a running server: a shard that never answers, a
//! `-MOVED` from the cluster seam, a WATCH conflict, an exhausted rate limiter
//! are all one field on [`MockTxnHost`]. Each test below drives exactly one
//! outcome, and `every_outcome_variant_has_a_forcing_test` matches the outcome
//! enum exhaustively (no wildcard arm, mirroring `metric_label`), so a new
//! variant fails compilation until someone writes the test that forces it.

use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use bytes::Bytes;
use frogdb_core::{
    MetricsRecorder, RateLimitExceeded, ServerWideOp, TransactionResult, WatchEntry,
};
use frogdb_protocol::{ParsedCommand, Response};
use frogdb_txn::{
    Deferral, ShardTxnReply, TransactionOutcome, TransactionTarget, TxnHost, TxnSummary,
    WatchedKey, execute_transaction, handle_exec,
};

/// The shard [`MockTxnHost`] reports as its own.
const MOCK_SHARD: usize = 7;

// ---------------------------------------------------------------------------
// Test host
// ---------------------------------------------------------------------------

/// One effect the algorithm asked the host to perform, in call order.
#[derive(Debug, PartialEq, Eq)]
enum Effect {
    Validate {
        asking: bool,
    },
    /// The pause wait, with the batch that was handed to it — the slot-scoped
    /// barrier can only narrow its parking if the queue actually reaches the
    /// host.
    WaitIfPaused {
        commands: usize,
    },
    /// The watch set's "are these slots still mine?" check. Recorded so a test
    /// can pin *when* it runs relative to the pause and the batch verdict.
    WatchCheck,
    ShardRoundTrip {
        target_shard: usize,
        commands: usize,
    },
    ConnectionLevel(String),
    ServerWide(ServerWideOp),
}

/// One sample recorded through [`MetricsRecorder`], captured verbatim so a
/// test can assert on the exact name/value/labels a call site emitted.
#[derive(Debug, Clone, PartialEq)]
enum MetricSample {
    Counter {
        name: String,
        value: u64,
        labels: Vec<(String, String)>,
    },
    Histogram {
        name: String,
        value: f64,
        labels: Vec<(String, String)>,
    },
}

/// A [`MetricsRecorder`] that records every call instead of discarding it, so
/// a test can assert `handle_exec`/`record_transaction_metrics` actually
/// recorded something rather than merely returning without panicking.
#[derive(Debug, Default)]
struct RecordingMetricsRecorder {
    samples: std::sync::Mutex<Vec<MetricSample>>,
}

impl MetricsRecorder for RecordingMetricsRecorder {
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        self.samples.lock().unwrap().push(MetricSample::Counter {
            name: name.to_string(),
            value,
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        });
    }

    fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}

    fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        self.samples.lock().unwrap().push(MetricSample::Histogram {
            name: name.to_string(),
            value,
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        });
    }
}

/// A [`TxnHost`] whose every answer is a field. Defaults describe the boring
/// standalone case: no rate limit, no redirect, no pause, shard says success.
struct MockTxnHost {
    shard_id: usize,
    conn_id: u64,
    recorder: RecordingMetricsRecorder,
    /// Command name (uppercase) -> how it must be deferred. Absent = shard.
    deferrals: HashMap<String, Deferral>,
    queue_has_writes: bool,
    rate_limit: Option<RateLimitExceeded>,
    /// One verdict per `validate_queued_batch` call; exhausted = `None`.
    validate_verdicts: VecDeque<Option<Response>>,
    /// One verdict per `watched_slots_still_local` call (false = a watched
    /// key's slot has left this node); exhausted = `true`. Scripted rather
    /// than fixed so a test can move a watched slot *between* two calls.
    watched_slots_local: VecDeque<bool>,
    /// What `wait_if_paused` reports (true = it actually blocked).
    paused: bool,
    /// One reply per shard round-trip; exhausted = empty success.
    shard_replies: VecDeque<ShardTxnReply>,
    connection_level_reply: (Response, Vec<Response>),
    server_wide_reply: Response,
    effects: Vec<Effect>,
}

impl Default for MockTxnHost {
    fn default() -> Self {
        Self {
            shard_id: MOCK_SHARD,
            conn_id: 42,
            recorder: RecordingMetricsRecorder::default(),
            deferrals: HashMap::new(),
            queue_has_writes: false,
            rate_limit: None,
            validate_verdicts: VecDeque::new(),
            watched_slots_local: VecDeque::new(),
            paused: false,
            shard_replies: VecDeque::new(),
            connection_level_reply: (Response::ok(), vec![]),
            server_wide_reply: Response::Integer(0),
            effects: Vec::new(),
        }
    }
}

#[async_trait]
impl TxnHost for MockTxnHost {
    fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn conn_id(&self) -> u64 {
        self.conn_id
    }

    fn metrics_recorder(&self) -> &dyn MetricsRecorder {
        &self.recorder
    }

    fn deferral_of(&self, name: &str) -> Option<Deferral> {
        self.deferrals.get(name).copied()
    }

    fn queue_has_writes(&self, _queue: &[ParsedCommand]) -> bool {
        self.queue_has_writes
    }

    fn try_acquire_batch(&self, _queue: &[ParsedCommand]) -> Result<(), RateLimitExceeded> {
        match self.rate_limit {
            Some(exceeded) => Err(exceeded),
            None => Ok(()),
        }
    }

    async fn validate_queued_batch(
        &mut self,
        _queue: &[ParsedCommand],
        asking: bool,
    ) -> Option<Response> {
        self.effects.push(Effect::Validate { asking });
        self.validate_verdicts.pop_front().flatten()
    }

    fn watched_slots_still_local(&mut self, _watches: &[WatchEntry], _asking: bool) -> bool {
        self.effects.push(Effect::WatchCheck);
        self.watched_slots_local.pop_front().unwrap_or(true)
    }

    async fn wait_if_paused(&mut self, queue: &[ParsedCommand]) -> bool {
        self.effects.push(Effect::WaitIfPaused {
            commands: queue.len(),
        });
        self.paused
    }

    async fn send_shard_transaction(
        &mut self,
        target_shard: usize,
        commands: Vec<ParsedCommand>,
        _watches: Vec<WatchEntry>,
    ) -> ShardTxnReply {
        self.effects.push(Effect::ShardRoundTrip {
            target_shard,
            commands: commands.len(),
        });
        self.shard_replies.pop_front().unwrap_or_else(|| {
            // Default: one `+OK` per command, so the merge has something to zip.
            ShardTxnReply::Replied(TransactionResult::Success(
                commands.iter().map(|_| Response::ok()).collect(),
            ))
        })
    }

    fn routing_unsettled_reply(&self) -> Response {
        Response::error("TRYAGAIN slot handoff in progress")
    }

    async fn run_connection_level(
        &mut self,
        name: &str,
        _args: &[Bytes],
    ) -> (Response, Vec<Response>) {
        self.effects.push(Effect::ConnectionLevel(name.to_string()));
        self.connection_level_reply.clone()
    }

    async fn run_server_wide(&mut self, op: ServerWideOp, _args: &[Bytes]) -> Response {
        self.effects.push(Effect::ServerWide(op));
        self.server_wide_reply.clone()
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A watched key on the mock host's own shard — which is also the fallback
/// target for a queue that folds no key, so a watch built this way rides with
/// the batch instead of taking a round-trip of its own.
fn watched(key: &'static [u8], version: u64, live_at_watch: bool) -> WatchedKey {
    watched_on(MOCK_SHARD, key, version, live_at_watch)
}

/// A watched key on an explicit shard, as [`TransactionState::take`] records it.
fn watched_on(
    shard_id: usize,
    key: &'static [u8],
    version: u64,
    live_at_watch: bool,
) -> WatchedKey {
    WatchedKey {
        shard_id,
        entry: WatchEntry {
            key: Bytes::from_static(key),
            version,
            live_at_watch,
        },
    }
}

fn cmd(name: &'static str) -> ParsedCommand {
    ParsedCommand::new(
        Bytes::from_static(name.as_bytes()),
        vec![Bytes::from_static(b"k")],
    )
}

/// A summary with `queue` and everything else at its benign default.
fn summary(queue: Vec<ParsedCommand>) -> TxnSummary {
    TxnSummary {
        queue,
        watches: vec![],
        target: TransactionTarget::None,
        exec_abort: false,
        asking: false,
        start_time: Some(std::time::Instant::now()),
    }
}

fn error_text(resp: &Response) -> String {
    match resp {
        Response::Error(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("expected an error response, got {other:?}"),
    }
}

/// The one reply an outcome that answers with a bare (non-array) frame produces.
fn only(responses: Vec<Response>) -> Response {
    assert_eq!(responses.len(), 1, "expected exactly one reply frame");
    responses.into_iter().next().expect("length checked")
}

// ---------------------------------------------------------------------------
// One test per TransactionOutcome variant
// ---------------------------------------------------------------------------

// FM-TXN-008, FM-TXN-016
#[tokio::test]
async fn exec_abort_when_queuing_poisoned_the_transaction() {
    let mut host = MockTxnHost::default();
    let mut s = summary(vec![cmd("SET")]);
    s.exec_abort = true;

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::ExecAbort);
    assert_eq!(
        error_text(&only(responses)),
        "EXECABORT Transaction discarded because of previous errors."
    );
    // A poisoned transaction never reaches the shard, the redirect seam, or the
    // pause barrier.
    assert!(host.effects.is_empty(), "effects: {:?}", host.effects);
}

// FM-TXN-017
#[tokio::test]
async fn rate_limited_names_the_dimension_that_was_exceeded() {
    for (exceeded, msg) in [
        (
            RateLimitExceeded::Commands,
            "ERR rate limit exceeded: commands per second",
        ),
        (
            RateLimitExceeded::Bytes,
            "ERR rate limit exceeded: bytes per second",
        ),
    ] {
        let mut host = MockTxnHost {
            rate_limit: Some(exceeded),
            ..Default::default()
        };

        let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

        assert_eq!(outcome, TransactionOutcome::RateLimited);
        assert_eq!(error_text(&only(responses)), msg);
        assert!(host.effects.is_empty(), "effects: {:?}", host.effects);
    }
}

// FM-TXN-018
#[tokio::test]
async fn committed_empty_answers_an_empty_array_without_touching_a_shard() {
    let mut host = MockTxnHost::default();

    let (outcome, responses) = execute_transaction(&mut host, summary(vec![])).await;

    assert_eq!(outcome, TransactionOutcome::CommittedEmpty);
    assert_eq!(only(responses), Response::Array(vec![]));
    // Notably *before* the slot re-validation: an empty EXEC is never redirected.
    assert!(host.effects.is_empty(), "effects: {:?}", host.effects);
}

// FM-TXN-019
#[tokio::test]
async fn cross_slot_when_the_queue_folded_to_more_than_one_shard() {
    let mut host = MockTxnHost::default();
    let mut s = summary(vec![cmd("SET")]);
    s.target = TransactionTarget::Multi(vec![0, 1]);

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::CrossSlot);
    assert!(
        error_text(&only(responses)).starts_with("CROSSSLOT"),
        "cross-slot EXEC must answer the redirect seam's CROSSSLOT"
    );
    // Validation ran (standalone said "serve local"), but nothing was executed.
    assert_eq!(host.effects, vec![Effect::Validate { asking: false }]);
}

// FM-TXN-022
#[tokio::test]
async fn redirected_returns_the_bare_redirect_not_an_array() {
    // The redirect frame comes from the seam that owns its wire format, so this
    // test cannot drift from what the cluster path actually emits.
    let redirect = frogdb_core::redirect::moved(1234, "10.0.0.2:6379".parse().expect("literal"));
    let mut host = MockTxnHost {
        validate_verdicts: VecDeque::from([Some(redirect)]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("SET")]);
    s.asking = true;

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::Redirected);
    assert_eq!(error_text(&only(responses)), "MOVED 1234 10.0.0.2:6379");
    // The connection's sticky ASKING is handed to the validator verbatim.
    assert_eq!(host.effects, vec![Effect::Validate { asking: true }]);
}

// FM-TXN-026
#[tokio::test]
async fn a_validation_verdict_that_is_a_plain_error_short_circuits_the_same_way() {
    // EXEC-time validation fails closed: when the key-presence probe behind an
    // open slot migration cannot reach the shard, `validate_queued_batch`
    // answers `ERR shard unavailable` instead of guessing. The algorithm treats
    // every verdict alike -- one bare frame, no shard round-trip -- and files it
    // under `Redirected`, because the verdict came from the redirect gate.
    let mut host = MockTxnHost {
        validate_verdicts: VecDeque::from([Some(Response::error("ERR shard unavailable"))]),
        ..Default::default()
    };

    let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

    assert_eq!(outcome, TransactionOutcome::Redirected);
    assert_eq!(error_text(&only(responses)), "ERR shard unavailable");
    assert_eq!(host.effects, vec![Effect::Validate { asking: false }]);
}

// FM-TXN-031
#[tokio::test]
async fn error_when_the_shard_reports_one() {
    let mut host = MockTxnHost {
        shard_replies: VecDeque::from([ShardTxnReply::Replied(TransactionResult::Error(
            "ERR transaction failed".to_string(),
        ))]),
        ..Default::default()
    };

    let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

    assert_eq!(outcome, TransactionOutcome::Error);
    assert_eq!(error_text(&only(responses)), "ERR transaction failed");
}

// FM-TXN-032
#[tokio::test]
async fn error_when_the_shard_channel_is_closed_or_the_request_is_dropped() {
    for (reply, msg) in [
        (ShardTxnReply::Unavailable, "ERR shard unavailable"),
        (ShardTxnReply::Dropped, "ERR shard dropped request"),
    ] {
        let mut host = MockTxnHost {
            shard_replies: VecDeque::from([reply]),
            ..Default::default()
        };

        let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

        assert_eq!(outcome, TransactionOutcome::Error);
        assert_eq!(error_text(&only(responses)), msg);
    }
}

// FM-TXN-033
#[tokio::test]
async fn watch_aborted_answers_nil() {
    let mut host = MockTxnHost {
        shard_replies: VecDeque::from([ShardTxnReply::Replied(TransactionResult::WatchAborted)]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("SET")]);
    s.watches = vec![watched(b"k", 3, true)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::WatchAborted);
    assert_eq!(only(responses), Response::null());
}

// FM-TXN-049
#[tokio::test]
async fn a_watched_slot_that_left_this_node_aborts_the_watch() {
    // The queue names no key of its own, so the batch verdict is "serve here".
    // The watch set is what still points at a slot this node no longer owns:
    // its version can no longer observe the real owner's writes, so the CAS
    // must fail rather than commit against a stale local copy.
    let mut host = MockTxnHost {
        watched_slots_local: VecDeque::from([false]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("PING")]);
    s.watches = vec![watched(b"k", 3, true)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::WatchAborted);
    assert_eq!(only(responses), Response::null());
    // Nothing ran: the batch never reached a shard.
    assert_eq!(
        host.effects,
        vec![Effect::Validate { asking: false }, Effect::WatchCheck]
    );
}

// FM-TXN-049
#[tokio::test]
async fn a_queue_redirect_outranks_the_watched_slot_abort() {
    // Both gates fire. The queue's own verdict wins, because a client that must
    // be sent elsewhere to run the batch at all gains nothing from being told
    // its CAS failed here first.
    let redirect = frogdb_core::redirect::moved(99, "10.0.0.2:6379".parse().expect("literal"));
    let mut host = MockTxnHost {
        validate_verdicts: VecDeque::from([Some(redirect)]),
        watched_slots_local: VecDeque::from([false]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("SET")]);
    s.watches = vec![watched(b"k", 3, true)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::Redirected);
    assert_eq!(error_text(&only(responses)), "MOVED 99 10.0.0.2:6379");
}

// FM-TXN-035
#[tokio::test]
async fn committed_returns_the_shard_results_in_an_array() {
    let mut host = MockTxnHost {
        shard_replies: VecDeque::from([ShardTxnReply::Replied(TransactionResult::Success(vec![
            Response::ok(),
            Response::Integer(1),
        ]))]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("SET"), cmd("INCR")]);
    s.target = TransactionTarget::Single(3);

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert_eq!(
        only(responses),
        Response::Array(vec![Response::ok(), Response::Integer(1)])
    );
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::ShardRoundTrip {
                target_shard: 3,
                commands: 2
            },
        ]
    );
}

// FM-TXN-046
/// Compile-time completeness gate: the match has no wildcard arm, so a new
/// [`TransactionOutcome`] variant breaks this file until a forcing test exists.
#[test]
fn every_outcome_variant_has_a_forcing_test() {
    fn forcing_test(outcome: TransactionOutcome) -> &'static str {
        match outcome {
            TransactionOutcome::ExecAbort => "exec_abort_when_queuing_poisoned_the_transaction",
            TransactionOutcome::RateLimited => "rate_limited_names_the_dimension_that_was_exceeded",
            TransactionOutcome::CommittedEmpty => {
                "committed_empty_answers_an_empty_array_without_touching_a_shard"
            }
            TransactionOutcome::CrossSlot => {
                "cross_slot_when_the_queue_folded_to_more_than_one_shard"
            }
            TransactionOutcome::Redirected => "redirected_returns_the_bare_redirect_not_an_array",
            TransactionOutcome::Error => "error_when_the_shard_reports_one",
            TransactionOutcome::WatchAborted => "watch_aborted_answers_nil",
            TransactionOutcome::Committed => "committed_returns_the_shard_results_in_an_array",
        }
    }

    for outcome in [
        TransactionOutcome::ExecAbort,
        TransactionOutcome::RateLimited,
        TransactionOutcome::CommittedEmpty,
        TransactionOutcome::CrossSlot,
        TransactionOutcome::Redirected,
        TransactionOutcome::Error,
        TransactionOutcome::WatchAborted,
        TransactionOutcome::Committed,
    ] {
        assert!(
            !forcing_test(outcome).is_empty(),
            "{outcome:?} needs a test that forces it"
        );
    }
}

// FM-TXN-046
#[tokio::test]
async fn handle_exec_returns_the_reply_and_records_exactly_the_outcome_metric_triple() {
    // `handle_exec` is the single metric-recording exit: it must both hand
    // back whatever `execute_transaction` produced (not swallow it) and
    // record exactly the counter/histogram triple `record_transaction_metrics`
    // defines for that outcome (not silently record nothing).
    let mut host = MockTxnHost::default();

    let responses = handle_exec(&mut host, summary(vec![cmd("SET")])).await;

    assert_eq!(
        responses,
        vec![Response::Array(vec![Response::ok()])],
        "handle_exec must return execute_transaction's replies, not an empty Vec"
    );

    let samples = host.recorder.samples.lock().unwrap();
    assert_eq!(
        samples.len(),
        3,
        "expected one committed-count sample plus the queued-commands and \
         duration histograms: {samples:?}"
    );
    assert_eq!(
        samples[0],
        MetricSample::Counter {
            name: "frogdb_transactions_total".to_string(),
            value: 1,
            labels: vec![("outcome".to_string(), "committed".to_string())],
        }
    );
    match &samples[1] {
        MetricSample::Histogram {
            name,
            value,
            labels,
        } => {
            assert_eq!(name, "frogdb_transactions_queued_commands");
            assert_eq!(*value, 1.0, "one command was queued");
            assert_eq!(labels, &[("outcome".to_string(), "committed".to_string())]);
        }
        other => panic!("expected the queued-commands histogram, got {other:?}"),
    }
    match &samples[2] {
        MetricSample::Histogram { name, labels, .. } => {
            assert_eq!(name, "frogdb_transactions_duration_seconds");
            assert_eq!(labels, &[("outcome".to_string(), "committed".to_string())]);
        }
        other => panic!("expected the duration histogram, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Paths between the outcomes
// ---------------------------------------------------------------------------

// FM-TXN-040
#[tokio::test]
async fn a_blocking_pause_forces_a_second_slot_verdict() {
    // The topology can move while EXEC sits in an unbounded `CLIENT PAUSE`, so a
    // pause that actually blocked must be followed by a fresh verdict — and that
    // verdict is allowed to redirect even though the first one said "serve here".
    let mut host = MockTxnHost {
        queue_has_writes: true,
        paused: true,
        validate_verdicts: VecDeque::from([None, Some(frogdb_core::redirect::tryagain())]),
        ..Default::default()
    };

    let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

    assert_eq!(outcome, TransactionOutcome::Redirected);
    assert_eq!(only(responses), frogdb_core::redirect::tryagain());
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::WaitIfPaused { commands: 1 },
            Effect::Validate { asking: false },
        ],
        "a blocking pause must be bracketed by two verdicts"
    );
}

// FM-TXN-040
#[tokio::test]
async fn a_non_blocking_pause_keeps_the_batch_at_exactly_one_slot_verdict() {
    let mut host = MockTxnHost {
        queue_has_writes: true,
        paused: false,
        ..Default::default()
    };

    let (outcome, _) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::WaitIfPaused { commands: 1 },
            Effect::ShardRoundTrip {
                target_shard: 7,
                commands: 1
            },
        ],
        "an unblocked pause must not re-take the snapshot"
    );
}

// FM-TXN-040
#[tokio::test]
async fn a_watched_slot_lost_during_the_pause_aborts_the_cas() {
    // A `CLIENT PAUSE` is exactly the window a slot changes hands in — the
    // migration barrier *is* a pause. The watch set is re-checked after the
    // park, so a watched slot that departed while the transaction sat there
    // breaks the CAS instead of being decided against this node's stale copy.
    let mut host = MockTxnHost {
        queue_has_writes: true,
        paused: true,
        // Two verdicts: the queue is servable here both before and after the
        // park. Only the *watch* set moved.
        validate_verdicts: VecDeque::from([None, None]),
        watched_slots_local: VecDeque::from([false]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("SET")]);
    s.watches = vec![watched(b"k", 3, true)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::WatchAborted);
    assert_eq!(only(responses), Response::null());
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::WaitIfPaused { commands: 1 },
            Effect::Validate { asking: false },
            Effect::WatchCheck,
        ],
        "the watch verdict must be cut from the post-pause topology"
    );
}

// FM-TXN-040
#[tokio::test]
async fn the_watch_check_is_ordered_after_the_pause_barrier() {
    // The complement of the test above: the watch set survives the park, so the
    // batch commits — but the ordering is the same, and it is the ordering that
    // is the invariant. A mutant that hoists the watch check above
    // `wait_if_paused` still commits here and still aborts there; only the
    // effect sequence catches it.
    let mut host = MockTxnHost {
        queue_has_writes: true,
        paused: true,
        validate_verdicts: VecDeque::from([None, None]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("SET")]);
    s.watches = vec![watched(b"k", 3, true)];

    let (outcome, _) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::WaitIfPaused { commands: 1 },
            Effect::Validate { asking: false },
            Effect::WatchCheck,
            Effect::ShardRoundTrip {
                target_shard: MOCK_SHARD,
                commands: 1
            },
        ],
        "exactly one watch check, after both the barrier and the second verdict"
    );
}

// FM-TXN-052
#[tokio::test]
async fn a_topology_change_is_retried_after_revalidation() {
    // The shard refused the apply because the routing generation moved under
    // the batch. Nothing ran, so the batch is still perfectly runnable — the
    // coordinator re-validates (which re-stamps the generation) and re-sends.
    let mut host = MockTxnHost {
        shard_replies: VecDeque::from([
            ShardTxnReply::Replied(TransactionResult::TopologyChanged),
            ShardTxnReply::Replied(TransactionResult::Success(vec![Response::ok()])),
        ]),
        ..Default::default()
    };

    let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert_eq!(only(responses), Response::Array(vec![Response::ok()]));
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::ShardRoundTrip {
                target_shard: MOCK_SHARD,
                commands: 1
            },
            Effect::Validate { asking: false },
            Effect::ShardRoundTrip {
                target_shard: MOCK_SHARD,
                commands: 1
            },
        ],
        "a refused apply must be re-validated before it is re-sent"
    );
}

// FM-TXN-052
#[tokio::test]
async fn a_redirect_on_revalidation_ends_the_transaction() {
    // Same refusal, but the fresh topology says the slot really has moved on.
    // That verdict is the answer — retrying it would be a loop with a known end.
    let redirect = frogdb_core::redirect::moved(99, "10.0.0.2:6379".parse().expect("literal"));
    let mut host = MockTxnHost {
        validate_verdicts: VecDeque::from([None, Some(redirect)]),
        shard_replies: VecDeque::from([ShardTxnReply::Replied(TransactionResult::TopologyChanged)]),
        ..Default::default()
    };

    let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

    assert_eq!(outcome, TransactionOutcome::Redirected);
    assert_eq!(error_text(&only(responses)), "MOVED 99 10.0.0.2:6379");
    assert_eq!(
        host.effects
            .iter()
            .filter(|e| matches!(e, Effect::ShardRoundTrip { .. }))
            .count(),
        1,
        "the batch must not be re-sent after a redirect: {:?}",
        host.effects
    );
}

// FM-TXN-053
#[tokio::test]
async fn a_routing_generation_that_never_settles_answers_tryagain() {
    // A slot flapping between owners must not hold the EXEC forever. The budget
    // counts sends; when it runs out the host builds the reply, because the
    // redirect vocabulary does not live in this crate.
    let mut host = MockTxnHost {
        shard_replies: VecDeque::from([
            ShardTxnReply::Replied(TransactionResult::TopologyChanged),
            ShardTxnReply::Replied(TransactionResult::TopologyChanged),
            ShardTxnReply::Replied(TransactionResult::TopologyChanged),
            // A fourth reply that must never be consumed.
            ShardTxnReply::Replied(TransactionResult::Success(vec![Response::ok()])),
        ]),
        ..Default::default()
    };

    let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("SET")])).await;

    assert_eq!(outcome, TransactionOutcome::Redirected);
    assert_eq!(
        error_text(&only(responses)),
        "TRYAGAIN slot handoff in progress"
    );
    assert_eq!(
        host.effects
            .iter()
            .filter(|e| matches!(e, Effect::ShardRoundTrip { .. }))
            .count(),
        3,
        "the retry budget is three sends: {:?}",
        host.effects
    );
    // One verdict per send and no more: the last refusal has no attempt left to
    // spend, so it must not pay for a re-validation it cannot use.
    assert_eq!(
        host.effects
            .iter()
            .filter(|e| matches!(e, Effect::Validate { .. }))
            .count(),
        3,
        "the exhausted attempt re-validates nothing: {:?}",
        host.effects
    );
}

// FM-CLUSTER-096 FM-CLUSTER-083
#[tokio::test]
async fn the_pause_barrier_is_handed_the_whole_batch() {
    // The seam's whole point: a slot-scoped barrier can only park the batches
    // that reach its slot if the host is told which commands the batch runs.
    // Hand the algorithm a three-command queue and pin that all three arrive.
    let mut host = MockTxnHost {
        queue_has_writes: true,
        paused: false,
        ..Default::default()
    };

    let (outcome, _) = execute_transaction(
        &mut host,
        summary(vec![cmd("SET"), cmd("INCR"), cmd("APPEND")]),
    )
    .await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert!(
        host.effects.contains(&Effect::WaitIfPaused { commands: 3 }),
        "the pause barrier must see the batch, not just the fact of a batch: {:?}",
        host.effects
    );
}

// FM-TXN-041
#[tokio::test]
async fn a_read_only_batch_never_reaches_the_pause_barrier() {
    let mut host = MockTxnHost {
        queue_has_writes: false,
        ..Default::default()
    };

    let (outcome, _) = execute_transaction(&mut host, summary(vec![cmd("GET")])).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert!(
        !host
            .effects
            .iter()
            .any(|e| matches!(e, Effect::WaitIfPaused { .. })),
        "PAUSE WRITE must not block a read-only MULTI: {:?}",
        host.effects
    );
}

// FM-TXN-037
#[tokio::test]
async fn deferred_replies_land_at_their_queued_positions() {
    // Queue: SET (shard), CONFIG (connection-level), INCR (shard), DBSIZE
    // (server-wide). The shard sees only the two shard commands; the merge must
    // still put every reply back at its queued index.
    let mut host = MockTxnHost {
        deferrals: HashMap::from([
            ("CONFIG".to_string(), Deferral::ConnectionLevel),
            (
                "DBSIZE".to_string(),
                Deferral::ServerWide(ServerWideOp::DbSize),
            ),
        ]),
        shard_replies: VecDeque::from([ShardTxnReply::Replied(TransactionResult::Success(vec![
            Response::Bulk(Some(Bytes::from_static(b"set"))),
            Response::Bulk(Some(Bytes::from_static(b"incr"))),
        ]))]),
        connection_level_reply: (
            Response::Bulk(Some(Bytes::from_static(b"config"))),
            vec![Response::Bulk(Some(Bytes::from_static(b"push")))],
        ),
        server_wide_reply: Response::Integer(9),
        ..Default::default()
    };

    let (outcome, responses) = execute_transaction(
        &mut host,
        summary(vec![cmd("SET"), cmd("CONFIG"), cmd("INCR"), cmd("DBSIZE")]),
    )
    .await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    // The EXEC array first, then the deferred push confirmations.
    assert_eq!(
        responses,
        vec![
            Response::Array(vec![
                Response::Bulk(Some(Bytes::from_static(b"set"))),
                Response::Bulk(Some(Bytes::from_static(b"config"))),
                Response::Bulk(Some(Bytes::from_static(b"incr"))),
                Response::Integer(9),
            ]),
            Response::Bulk(Some(Bytes::from_static(b"push"))),
        ]
    );
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::ShardRoundTrip {
                target_shard: 7,
                commands: 2
            },
            Effect::ConnectionLevel("CONFIG".to_string()),
            Effect::ServerWide(ServerWideOp::DbSize),
        ]
    );
}

// FM-TXN-034
#[tokio::test]
async fn an_all_deferred_queue_with_watches_still_takes_the_shard_round_trip() {
    // Nothing to run on the shard, but the watch set has to be version-checked
    // and cleared — so the round trip happens with an empty command list, and a
    // WATCH conflict still aborts the whole EXEC.
    let mut host = MockTxnHost {
        deferrals: HashMap::from([("CONFIG".to_string(), Deferral::ConnectionLevel)]),
        shard_replies: VecDeque::from([ShardTxnReply::Replied(TransactionResult::WatchAborted)]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("CONFIG")]);
    s.watches = vec![watched(b"k", 1, true)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::WatchAborted);
    assert_eq!(only(responses), Response::null());
    assert!(
        host.effects.contains(&Effect::ShardRoundTrip {
            target_shard: 7,
            commands: 0
        }),
        "a watch-only EXEC still needs the version check: {:?}",
        host.effects
    );
}

// FM-TXN-034
#[tokio::test]
async fn a_genuinely_empty_queue_with_watches_still_takes_the_shard_round_trip() {
    // `WATCH k`, another client writes `k`, then `MULTI; EXEC` with nothing
    // queued at all. The empty-queue fast path must not fire: the CAS
    // precondition is already broken, and answering `*0` would be a committed
    // transaction whose watch was silently ignored. Redis's `execCommand`
    // tests `CLIENT_DIRTY_CAS` *before* it looks at the queue length for
    // exactly this reason.
    let mut host = MockTxnHost {
        shard_replies: VecDeque::from([ShardTxnReply::Replied(TransactionResult::WatchAborted)]),
        ..Default::default()
    };
    let mut s = summary(vec![]);
    s.watches = vec![watched(b"k", 1, true)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::WatchAborted);
    assert_eq!(only(responses), Response::null());
    assert!(
        host.effects.contains(&Effect::ShardRoundTrip {
            target_shard: 7,
            commands: 0
        }),
        "an empty but watched EXEC still needs the version check: {:?}",
        host.effects
    );
}

// FM-TXN-034
#[tokio::test]
async fn a_genuinely_empty_queue_with_a_clean_watch_commits_an_empty_array() {
    // The other half of the same guard: taking the round-trip must not change
    // what a *clean* watched empty EXEC replies. It is still `*0` — only the
    // outcome variant differs from the unwatched fast path (`Committed`
    // rather than `CommittedEmpty`, both labelled `committed`, FM-TXN-046).
    let mut host = MockTxnHost::default();
    let mut s = summary(vec![]);
    s.watches = vec![watched(b"k", 1, true)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert_eq!(only(responses), Response::Array(vec![]));
    assert!(
        host.effects.contains(&Effect::ShardRoundTrip {
            target_shard: 7,
            commands: 0
        }),
        "the version check is taken whether or not the watch turns out dirty: {:?}",
        host.effects
    );
}

// FM-TXN-020
#[tokio::test]
async fn a_dead_watch_off_the_target_gets_its_own_version_check() {
    // The create-if-absent CAS across shards: `WATCH counter` (absent, shard 3)
    // plus a queued write on shard 7. `take` leaves shard 3 unfolded, so the
    // batch commits instead of `-CROSSSLOT`ing — but the watch is still
    // version-checked, on shard 3, because only shard 3 keeps the slot stamp
    // that a creation of `counter` would bump. The target's own round trip
    // carries no foreign entries.
    let mut host = MockTxnHost::default();
    let mut s = summary(vec![cmd("SET")]);
    s.target = TransactionTarget::Single(MOCK_SHARD);
    s.watches = vec![watched_on(3, b"counter", 5, false)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert_eq!(only(responses), Response::Array(vec![Response::ok()]));
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::WatchCheck,
            // The off-target watch's own check, before the batch.
            Effect::ShardRoundTrip {
                target_shard: 3,
                commands: 0
            },
            Effect::ShardRoundTrip {
                target_shard: MOCK_SHARD,
                commands: 1
            },
        ]
    );
}

// FM-TXN-020
#[tokio::test]
async fn a_dirtied_off_target_watch_aborts_before_the_batch_runs() {
    // The safety half of the unfolded dead watch: another client created
    // `counter` on shard 3 during the MULTI window, so shard 3's version check
    // fails. The abort has to land *before* the target shard runs anything —
    // there is no rollback to undo a batch that already committed.
    let mut host = MockTxnHost {
        shard_replies: VecDeque::from([ShardTxnReply::Replied(TransactionResult::WatchAborted)]),
        ..Default::default()
    };
    let mut s = summary(vec![cmd("SET")]);
    s.target = TransactionTarget::Single(MOCK_SHARD);
    s.watches = vec![watched_on(3, b"counter", 5, false)];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::WatchAborted);
    assert_eq!(only(responses), Response::null());
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::WatchCheck,
            Effect::ShardRoundTrip {
                target_shard: 3,
                commands: 0
            },
        ],
        "the batch must never reach its own shard"
    );
}

// FM-TXN-020
#[tokio::test]
async fn off_target_watches_are_grouped_one_round_trip_per_shard_in_shard_order() {
    // Two dead watches on one foreign shard and one on another: two extra round
    // trips, not three, and in shard order — the watch set comes out of a
    // `HashMap`, so anything derived from its iteration order would make the
    // sequence vary run to run.
    let mut host = MockTxnHost::default();
    let mut s = summary(vec![cmd("SET")]);
    s.target = TransactionTarget::Single(MOCK_SHARD);
    s.watches = vec![
        watched_on(5, b"c", 1, false),
        watched_on(2, b"a", 1, false),
        watched_on(5, b"d", 1, false),
        watched_on(MOCK_SHARD, b"local", 1, true),
    ];

    let (outcome, _) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::WatchCheck,
            Effect::ShardRoundTrip {
                target_shard: 2,
                commands: 0
            },
            Effect::ShardRoundTrip {
                target_shard: 5,
                commands: 0
            },
            Effect::ShardRoundTrip {
                target_shard: MOCK_SHARD,
                commands: 1
            },
        ]
    );
}

// FM-TXN-038
#[tokio::test]
async fn an_all_deferred_queue_without_watches_skips_the_shard_entirely() {
    let mut host = MockTxnHost {
        deferrals: HashMap::from([("CONFIG".to_string(), Deferral::ConnectionLevel)]),
        ..Default::default()
    };

    let (outcome, responses) = execute_transaction(&mut host, summary(vec![cmd("CONFIG")])).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert_eq!(only(responses), Response::Array(vec![Response::ok()]));
    assert_eq!(
        host.effects,
        vec![
            Effect::Validate { asking: false },
            Effect::ConnectionLevel("CONFIG".to_string()),
        ]
    );
}

// FM-TXN-042
#[tokio::test]
async fn an_unfolded_target_falls_back_to_the_connections_own_shard() {
    let mut host = MockTxnHost {
        shard_id: 5,
        ..Default::default()
    };

    let (outcome, _) = execute_transaction(&mut host, summary(vec![cmd("PING")])).await;

    assert_eq!(outcome, TransactionOutcome::Committed);
    assert!(
        host.effects.contains(&Effect::ShardRoundTrip {
            target_shard: 5,
            commands: 1
        }),
        "effects: {:?}",
        host.effects
    );
}
