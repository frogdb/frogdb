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
    MetricsRecorder, NoopMetricsRecorder, RateLimitExceeded, ServerWideOp, TransactionResult,
    WatchEntry,
};
use frogdb_protocol::{ParsedCommand, Response};
use frogdb_txn::{
    Deferral, ShardTxnReply, TransactionOutcome, TransactionTarget, TxnHost, TxnSummary,
    execute_transaction,
};

// ---------------------------------------------------------------------------
// Test host
// ---------------------------------------------------------------------------

/// One effect the algorithm asked the host to perform, in call order.
#[derive(Debug, PartialEq, Eq)]
enum Effect {
    Validate {
        asking: bool,
    },
    WaitIfPaused,
    ShardRoundTrip {
        target_shard: usize,
        commands: usize,
    },
    ConnectionLevel(String),
    ServerWide(ServerWideOp),
}

/// A [`TxnHost`] whose every answer is a field. Defaults describe the boring
/// standalone case: no rate limit, no redirect, no pause, shard says success.
struct MockTxnHost {
    shard_id: usize,
    conn_id: u64,
    recorder: NoopMetricsRecorder,
    /// Command name (uppercase) -> how it must be deferred. Absent = shard.
    deferrals: HashMap<String, Deferral>,
    queue_has_writes: bool,
    rate_limit: Option<RateLimitExceeded>,
    /// One verdict per `validate_queued_batch` call; exhausted = `None`.
    validate_verdicts: VecDeque<Option<Response>>,
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
            shard_id: 7,
            conn_id: 42,
            recorder: NoopMetricsRecorder::new(),
            deferrals: HashMap::new(),
            queue_has_writes: false,
            rate_limit: None,
            validate_verdicts: VecDeque::new(),
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

    async fn wait_if_paused(&mut self) -> bool {
        self.effects.push(Effect::WaitIfPaused);
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
    s.watches = vec![WatchEntry {
        key: Bytes::from_static(b"k"),
        version: 3,
        live_at_watch: true,
    }];

    let (outcome, responses) = execute_transaction(&mut host, s).await;

    assert_eq!(outcome, TransactionOutcome::WatchAborted);
    assert_eq!(only(responses), Response::null());
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
            Effect::WaitIfPaused,
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
            Effect::WaitIfPaused,
            Effect::ShardRoundTrip {
                target_shard: 7,
                commands: 1
            },
        ],
        "an unblocked pause must not re-take the snapshot"
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
        !host.effects.contains(&Effect::WaitIfPaused),
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
    s.watches = vec![WatchEntry {
        key: Bytes::from_static(b"k"),
        version: 1,
        live_at_watch: true,
    }];

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
