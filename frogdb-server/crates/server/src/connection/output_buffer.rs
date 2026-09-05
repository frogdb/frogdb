//! The one seam where buffered client output is charged and limited.
//!
//! Everything the server buffers *for* a client passes through
//! [`OutputBufferAccount`]: RESP2 frames queued in the codec's write buffer,
//! RESP3 frames staged in the encode buffer, pub/sub messages queued for a
//! subscriber that is not reading. One place decides two things about those
//! bytes:
//!
//! 1. **May they exist at all?** They are charged to this core's
//!    [`Subsystem::NetworkOutput`] budget before they are buffered. The budget's
//!    declared disposition is [`Disposition::Shed`] — a refused charge closes
//!    the connection, because buffered output is exactly the work the server is
//!    entitled to drop (`specs/memory.md`, "shed").
//! 2. **Has this client fallen too far behind?** Redis's
//!    `client-output-buffer-limit <class> <hard> <soft> <soft-seconds>`, with
//!    Redis's three classes and Redis's defaults.
//!
//! # Why one seam and not three checks
//!
//! Before this, the server had a hard limit for pub/sub subscribers and nothing
//! at all for a normal client draining a huge reply or for a replica-class
//! connection. Three sites would drift; one site with a class *parameter*
//! cannot. The class is a property of the connection, so widening the policy
//! (say, a fourth class) is a variant here rather than a new call site
//! somewhere on the write path.
//!
//! # Redis compatibility
//!
//! Redis's semantics, deliberately: the hard limit closes the connection the
//! moment buffered output reaches it; the soft limit closes it only after
//! output has stayed above the soft mark *continuously* for `soft_seconds`, so
//! a burst that drains promptly is not a kill. `0` disables a limit. Redis's
//! shipped defaults are `normal 0 0 0`, `replica 256mb 64mb 60`,
//! `pubsub 32mb 8mb 60` — normal clients are unlimited by default because a
//! normal client's output is bounded by what it asked for.
//!
//! [`Subsystem::NetworkOutput`]: frogdb_memory::Subsystem::NetworkOutput
//! [`Disposition::Shed`]: frogdb_memory::Disposition::Shed

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use frogdb_memory::{Budget, Charge};

/// The `client-output-buffer-limit` classes, as Redis names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputBufferClass {
    /// An ordinary client. Unlimited by default: what it buffers is what it
    /// asked for.
    Normal,
    /// A replica link. Its output is the primary's replication stream, which the
    /// replica does not ask for and cannot slow down, so it gets a real cap.
    Replica,
    /// A subscriber. Its output is other clients' publishes, which arrive
    /// whether or not it reads.
    PubSub,
}

impl OutputBufferClass {
    /// The stable name used in metric labels, `CONFIG` keys and log lines.
    pub const fn as_str(self) -> &'static str {
        match self {
            OutputBufferClass::Normal => "normal",
            OutputBufferClass::Replica => "replica",
            OutputBufferClass::PubSub => "pubsub",
        }
    }
}

impl std::fmt::Display for OutputBufferClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One class's limit triple. `0` disables that limit, as in Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputBufferLimit {
    /// Close the connection as soon as buffered output reaches this.
    pub hard_bytes: u64,
    /// Close the connection if buffered output stays at or above this for
    /// `soft_seconds` continuously.
    pub soft_bytes: u64,
    /// How long output may sit above `soft_bytes` before the connection is
    /// closed.
    pub soft_seconds: u64,
}

impl OutputBufferLimit {
    /// A class with no limits — Redis's `normal` default.
    pub const UNLIMITED: Self = Self {
        hard_bytes: 0,
        soft_bytes: 0,
        soft_seconds: 0,
    };

    /// Redis's shipped `replica` default: `256mb 64mb 60`.
    pub const REPLICA_DEFAULT: Self = Self {
        hard_bytes: 256 * 1024 * 1024,
        soft_bytes: 64 * 1024 * 1024,
        soft_seconds: 60,
    };

    /// Redis's shipped `pubsub` default: `32mb 8mb 60`.
    pub const PUBSUB_DEFAULT: Self = Self {
        hard_bytes: 32 * 1024 * 1024,
        soft_bytes: 8 * 1024 * 1024,
        soft_seconds: 60,
    };
}

/// The three classes' limits, as one configuration value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputBufferLimits {
    /// `client-output-buffer-limit normal`.
    pub normal: OutputBufferLimit,
    /// `client-output-buffer-limit replica`.
    pub replica: OutputBufferLimit,
    /// `client-output-buffer-limit pubsub`.
    pub pubsub: OutputBufferLimit,
}

impl Default for OutputBufferLimits {
    /// Redis's shipped defaults.
    fn default() -> Self {
        Self {
            normal: OutputBufferLimit::UNLIMITED,
            replica: OutputBufferLimit::REPLICA_DEFAULT,
            pubsub: OutputBufferLimit::PUBSUB_DEFAULT,
        }
    }
}

impl OutputBufferLimits {
    /// The limit for `class`.
    pub fn for_class(&self, class: OutputBufferClass) -> OutputBufferLimit {
        match class {
            OutputBufferClass::Normal => self.normal,
            OutputBufferClass::Replica => self.replica,
            OutputBufferClass::PubSub => self.pubsub,
        }
    }

    /// A mutable handle on one class's limit.
    fn class_mut(&mut self, class: OutputBufferClass) -> &mut OutputBufferLimit {
        match class {
            OutputBufferClass::Normal => &mut self.normal,
            OutputBufferClass::Replica => &mut self.replica,
            OutputBufferClass::PubSub => &mut self.pubsub,
        }
    }

    /// Parse Redis's `client-output-buffer-limit` value: whitespace-separated
    /// groups of `<class> <hard> <soft> <soft-seconds>`, one group per class, in
    /// any order. A class the spec does not mention keeps its default, which is
    /// how Redis's config file behaves when only one directive is given.
    ///
    /// Byte counts accept Redis's suffixes — `1k` = 1000, `1kb` = 1024, and the
    /// same for `m`/`mb`/`g`/`gb` — so a config lifted verbatim from a
    /// `redis.conf` means here exactly what it meant there. `slave` is accepted
    /// as an alias for `replica`, again as Redis does.
    pub fn parse(spec: &str) -> Result<Self, OutputBufferLimitsParseError> {
        let mut limits = Self::default();
        let mut tokens = spec.split_whitespace();

        while let Some(class_token) = tokens.next() {
            let class = match class_token.to_ascii_lowercase().as_str() {
                "normal" => OutputBufferClass::Normal,
                "replica" | "slave" => OutputBufferClass::Replica,
                "pubsub" => OutputBufferClass::PubSub,
                other => {
                    return Err(OutputBufferLimitsParseError(format!(
                        "unknown client-output-buffer-limit class '{other}' \
                         (expected normal, replica or pubsub)"
                    )));
                }
            };

            let mut next_value = |what: &str| -> Result<&str, OutputBufferLimitsParseError> {
                tokens.next().ok_or_else(|| {
                    OutputBufferLimitsParseError(format!(
                        "client-output-buffer-limit class '{class}' is missing its {what}; \
                         each class needs '<hard> <soft> <soft-seconds>'"
                    ))
                })
            };
            let hard = next_value("hard limit")?.to_string();
            let soft = next_value("soft limit")?.to_string();
            let seconds = next_value("soft-limit seconds")?.to_string();

            *limits.class_mut(class) = OutputBufferLimit {
                hard_bytes: parse_bytes(&hard, class, "hard limit")?,
                soft_bytes: parse_bytes(&soft, class, "soft limit")?,
                soft_seconds: seconds.parse().map_err(|_| {
                    OutputBufferLimitsParseError(format!(
                        "client-output-buffer-limit {class}: '{seconds}' is not a number of seconds"
                    ))
                })?,
            };
        }

        Ok(limits)
    }

    /// Render back to the parseable spelling, the way `CONFIG GET` reports it.
    pub fn to_config_string(&self) -> String {
        let mut out = String::new();
        for class in [
            OutputBufferClass::Normal,
            OutputBufferClass::Replica,
            OutputBufferClass::PubSub,
        ] {
            let limit = self.for_class(class);
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(&format!(
                "{class} {} {} {}",
                limit.hard_bytes, limit.soft_bytes, limit.soft_seconds
            ));
        }
        out
    }
}

/// Why a `client-output-buffer-limit` spec could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBufferLimitsParseError(String);

impl std::fmt::Display for OutputBufferLimitsParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OutputBufferLimitsParseError {}

/// Redis's memory-size spelling: a bare count, or a count with a `k`/`m`/`g`
/// (powers of 1000) or `kb`/`mb`/`gb` (powers of 1024) suffix.
fn parse_bytes(
    raw: &str,
    class: OutputBufferClass,
    what: &str,
) -> Result<u64, OutputBufferLimitsParseError> {
    let lowered = raw.to_ascii_lowercase();
    let (digits, multiplier) = match lowered.as_str() {
        s if s.ends_with("kb") => (&s[..s.len() - 2], 1024),
        s if s.ends_with("mb") => (&s[..s.len() - 2], 1024 * 1024),
        s if s.ends_with("gb") => (&s[..s.len() - 2], 1024 * 1024 * 1024),
        s if s.ends_with('k') => (&s[..s.len() - 1], 1_000),
        s if s.ends_with('m') => (&s[..s.len() - 1], 1_000_000),
        s if s.ends_with('g') => (&s[..s.len() - 1], 1_000_000_000),
        s if s.ends_with('b') => (&s[..s.len() - 1], 1),
        s => (s, 1),
    };

    let bad = || {
        OutputBufferLimitsParseError(format!(
            "client-output-buffer-limit {class}: '{raw}' is not a valid {what} \
             (a byte count, optionally suffixed k/kb/m/mb/g/gb)"
        ))
    };
    digits
        .parse::<u64>()
        .map_err(|_| bad())?
        .checked_mul(multiplier)
        .ok_or_else(bad)
}

/// Why the seam is closing a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShedReason {
    /// Buffered output reached the class's hard limit.
    HardLimit,
    /// Buffered output stayed at or above the soft limit for the whole soft
    /// window.
    SoftLimit,
    /// This core's `NetworkOutput` budget refused the growth. The connection is
    /// shed because that is the budget's declared disposition.
    BudgetRefused,
}

impl ShedReason {
    /// The stable name used in metric labels and log lines.
    pub const fn as_str(self) -> &'static str {
        match self {
            ShedReason::HardLimit => "hard_limit",
            ShedReason::SoftLimit => "soft_limit",
            ShedReason::BudgetRefused => "budget_refused",
        }
    }
}

/// The seam's answer about a connection whose output just grew.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "an ignored verdict is a client-output-buffer-limit that does not exist"]
pub enum OutputVerdict {
    /// Keep the connection; the bytes are charged and within the class limit.
    Keep,
    /// Close the connection and release its buffered output.
    Shed(ShedReason),
}

impl OutputVerdict {
    /// Whether this verdict ends the connection.
    pub fn is_shed(self) -> bool {
        matches!(self, OutputVerdict::Shed(_))
    }
}

/// One connection's buffered-output accounting and limit state.
///
/// Holds a single [`Charge`] for the connection's lifetime that grows and
/// shrinks with the buffer, rather than a charge per frame: the quantity the
/// budget and the limit both care about is "bytes buffered right now", and one
/// charge is the only representation of it that cannot drift from the other.
#[derive(Debug)]
pub struct OutputBufferAccount {
    class: OutputBufferClass,
    limits: OutputBufferLimits,
    /// This connection's slice of the core's `NetworkOutput` budget.
    charge: Charge,
    /// When output first went at or above the class's soft limit and has stayed
    /// there since. Cleared the moment it drops back under.
    soft_since: Option<Instant>,
}

impl OutputBufferAccount {
    /// Open an account on `budget` for a connection of `class`.
    pub fn new(class: OutputBufferClass, limits: OutputBufferLimits, budget: &Budget) -> Self {
        Self {
            class,
            limits,
            charge: budget.open_charge(),
            soft_since: None,
        }
    }

    /// This connection's output class.
    pub fn class(&self) -> OutputBufferClass {
        self.class
    }

    /// Move this connection to another class — a client that subscribes becomes
    /// a subscriber, a client that completes `PSYNC` becomes a replica.
    ///
    /// The soft-limit timer restarts: the window belongs to the class, so
    /// seconds accrued as a normal client are not evidence against the pubsub
    /// limit.
    pub fn set_class(&mut self, class: OutputBufferClass) {
        if self.class != class {
            self.class = class;
            self.soft_since = None;
        }
    }

    /// Replace the limits (a live `CONFIG SET`).
    pub fn set_limits(&mut self, limits: OutputBufferLimits) {
        self.limits = limits;
        self.soft_since = None;
    }

    /// Bytes currently buffered for this connection — `CLIENT INFO`'s `omem`.
    pub fn buffered_bytes(&self) -> u64 {
        self.charge.bytes()
    }

    /// The limit this connection is judged against.
    pub fn limit(&self) -> OutputBufferLimit {
        self.limits.for_class(self.class)
    }

    /// All three classes' limits, for the one place that needs a class this
    /// connection is not currently in: the pub/sub delivery queue is sized from
    /// the `pubsub` hard limit when it is allocated, before the connection has
    /// become a subscriber.
    pub fn limits(&self) -> OutputBufferLimits {
        self.limits
    }

    /// **The seam.** Tell the account how many bytes are buffered for this
    /// connection *right now*, and get the verdict on it.
    ///
    /// Absolute rather than incremental on purpose: the write path already knows
    /// the true length of its buffers, and a running total maintained by
    /// matched `+`/`-` calls is a running total that eventually drifts — an
    /// `omem` that grows without bound, or a budget that leaks a core's worth of
    /// allowance. There is exactly one number, it is read from the buffers, and
    /// every caller of this function agrees with them by construction.
    ///
    /// Charge first, then judge: a budget that will not authorize the bytes is a
    /// refusal handled here, not a charge taken anyway. `now` is passed in
    /// rather than read here so the soft-limit window is drivable by a test
    /// without a real sixty-second wait; production callers pass
    /// `frogdb_core::clock::now()`.
    pub fn set_buffered(&mut self, total_bytes: u64, now: Instant) -> OutputVerdict {
        let held = self.charge.bytes();
        match total_bytes.cmp(&held) {
            std::cmp::Ordering::Greater => {
                if self.charge.grow(total_bytes - held).is_err() {
                    return OutputVerdict::Shed(ShedReason::BudgetRefused);
                }
            }
            std::cmp::Ordering::Less => self.charge.shrink(held - total_bytes),
            std::cmp::Ordering::Equal => {}
        }
        self.judge(now)
    }

    /// The connection is being torn down: drop every charged byte and forget
    /// the soft-limit window.
    ///
    /// Only for a connection whose buffers have been *discarded*
    /// (`shed_output`). A successful flush does **not** call this — it
    /// re-measures through [`set_buffered`](Self::set_buffered) instead, so a
    /// flush that leaves bytes still queued for this client (a subscriber's
    /// delivery queue) keeps its soft-limit window running rather than being
    /// credited with a drain that did not happen.
    pub fn release_all(&mut self) {
        let held = self.charge.bytes();
        self.charge.shrink(held);
        self.soft_since = None;
    }

    /// The class limit applied to the bytes currently held.
    fn judge(&mut self, now: Instant) -> OutputVerdict {
        let limit = self.limit();
        let buffered = self.charge.bytes();

        if limit.hard_bytes != 0 && buffered >= limit.hard_bytes {
            return OutputVerdict::Shed(ShedReason::HardLimit);
        }

        if limit.soft_bytes == 0 || buffered < limit.soft_bytes {
            self.soft_since = None;
            return OutputVerdict::Keep;
        }

        match self.soft_since {
            None => {
                self.soft_since = Some(now);
                OutputVerdict::Keep
            }
            Some(since) => {
                // `saturating_duration_since` rather than `-`: a caller passing
                // a `now` older than the mark (a test driving the window
                // backwards, a clock the seam does not own) must not panic, and
                // "no time has passed" is the conservative answer.
                let above_for = now.saturating_duration_since(since);
                if above_for >= Duration::from_secs(limit.soft_seconds) {
                    OutputVerdict::Shed(ShedReason::SoftLimit)
                } else {
                    OutputVerdict::Keep
                }
            }
        }
    }
}

/// The same account, after `PSYNC` has turned the connection into a
/// replication feed.
///
/// A replica does not stop costing the primary output memory when it stops
/// being a client — it starts costing more. What changes is only *who
/// measures*: the connection's write path is gone, and the feed's own buffers
/// (staged dataset blobs, the backlog handoff tail, frames held behind a
/// slot-handoff barrier) take its place. So the account moves rather than being
/// released and reopened: the same [`Charge`] on the same core's budget, the
/// same class limits, the same [`OutputBufferAccount::judge`], and no instant
/// in between in which a replica's bytes are charged to nothing.
///
/// This is the server half of `frogdb_replication`'s [`FeedOutputAccount`] seam
/// (`specs/replication.md` FM-REPLICATION-069). The replication crate reports
/// one figure and acts on the verdict; everything the figure *means* — budget,
/// class, clock, `omem`, the metric and the log line — is here.
///
/// # The soft window needs a clock of its own
///
/// A report-driven verdict can only rule on a feed that is still moving, and
/// the case `client-output-buffer-limit`'s soft seconds exist for is a feed that
/// has *stopped*: a replica that stalls mid-full-sync makes exactly one report,
/// opens the window, and then parks inside `write_all` forever. Redis has the
/// same problem and solves it by polling — `closeClientOnOutputBufferLimitReached`
/// runs from `serverCron`, not from the write path. So does this: a task owned
/// by the account re-judges the last reported figure every
/// [`SOFT_WINDOW_POLL`], and turns a `Shed` into the same link drop a hard
/// limit takes, through the shed signal the session armed over its socket.
pub struct ReplicaFeedAccount {
    /// Behind a lock because the feed reports from three places at once: the
    /// session driver on the connection's task, the spawned write task, and the
    /// re-judge tick.
    account: std::sync::Mutex<OutputBufferAccount>,
    /// Where `CLIENT LIST` / `CLIENT INFO` read `omem` from. The connection
    /// stays registered across the handoff — that is what makes a replica
    /// visible as a client at all — so this keeps its entry honest instead of
    /// freezing at whatever it held when `PSYNC` arrived.
    registry: std::sync::Arc<frogdb_core::ClientRegistry>,
    conn_id: u64,
    /// The rest of the connection's memory breakdown as it stood at the
    /// handoff, so republishing `omem` does not blank the fields the feed has
    /// no opinion about (notably `rbp`, a lifetime high-water mark).
    base: frogdb_core::ClientMemoryUsage,
    metrics: std::sync::Arc<dyn frogdb_core::MetricsRecorder>,
    /// The last figure written to the registry, or `u64::MAX` before the first.
    ///
    /// `update_memory` takes a process-wide write lock shared with every
    /// connection's stats sync and every `CLIENT LIST`, and the feed reports
    /// after every sequencer step — three times per replicated frame, almost
    /// always with an unchanged `0`. Publishing only what changed elides
    /// essentially all of that. The client side throttles the same write for
    /// the same reason (`lifecycle.rs`, `STATS_SYNC_INTERVAL_*`).
    last_published: AtomicU64,
    /// Set before the charge is released, and checked by everything that could
    /// re-take it. A write task aborted at its next await point can still run
    /// its post-write tail and report *after* the session's exit path released,
    /// which would re-charge the budget and republish a nonzero `omem` for a
    /// connection that is gone.
    released: AtomicBool,
    /// The session's end of the out-of-band shed, until it takes it.
    shed_rx: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<&'static str>>>,
    /// This end of it, until the re-judge fires it.
    shed_tx: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<&'static str>>>,
}

/// How often a stalled feed's soft window is re-judged.
///
/// Redis polls its equivalent from `serverCron`, which runs at `hz` (10 by
/// default, so every 100 ms). This is coarser because the window it is
/// resolving is measured in seconds — the shipped default is 60 — and every
/// tick is a mutex and a comparison per connected replica.
const SOFT_WINDOW_POLL: Duration = Duration::from_millis(500);

impl std::fmt::Debug for ReplicaFeedAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `try_lock`, not `lock`: `std::sync::Mutex` is not reentrant, so a
        // future `?feed` inside a locked region would self-deadlock the
        // formatter. A figure that is momentarily unavailable is not worth
        // that.
        let buffered = self
            .account
            .try_lock()
            .map(|account| account.buffered_bytes());
        f.debug_struct("ReplicaFeedAccount")
            .field("conn_id", &self.conn_id)
            .field("buffered", &buffered.ok())
            .finish()
    }
}

impl ReplicaFeedAccount {
    /// Take over `account` for the feed of connection `conn_id`, and start
    /// watching its soft window.
    ///
    /// The class is forced to [`OutputBufferClass::Replica`] here: whatever the
    /// connection was judged as while it was still speaking `REPLCONF`, what it
    /// is now is a replica link, and `client-output-buffer-limit replica` is the
    /// line that governs it.
    ///
    /// Must be called from a Tokio context — it spawns the re-judge tick, which
    /// holds only a `Weak` back to the account and so ends when the session that
    /// owns it does.
    pub fn new(
        mut account: OutputBufferAccount,
        registry: std::sync::Arc<frogdb_core::ClientRegistry>,
        conn_id: u64,
        base: frogdb_core::ClientMemoryUsage,
        metrics: std::sync::Arc<dyn frogdb_core::MetricsRecorder>,
    ) -> std::sync::Arc<Self> {
        account.set_class(OutputBufferClass::Replica);
        let (shed_tx, shed_rx) = tokio::sync::oneshot::channel();
        let this = std::sync::Arc::new(Self {
            account: std::sync::Mutex::new(account),
            registry,
            conn_id,
            base,
            metrics,
            last_published: AtomicU64::new(u64::MAX),
            released: AtomicBool::new(false),
            shed_rx: std::sync::Mutex::new(Some(shed_rx)),
            shed_tx: std::sync::Mutex::new(Some(shed_tx)),
        });
        this.watch_soft_window();
        this
    }

    /// Re-judge the last reported figure every [`SOFT_WINDOW_POLL`] until the
    /// feed ends, the account is dropped, or the window expires.
    fn watch_soft_window(self: &std::sync::Arc<Self>) {
        let weak = std::sync::Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(SOFT_WINDOW_POLL).await;
                // A `Weak` and not an `Arc`: the tick must never be the reason
                // a finished feed's charge stays alive.
                let Some(account) = weak.upgrade() else { break };
                if account.released.load(Ordering::Acquire) {
                    break;
                }
                if let Some(reason) = account.re_judge(frogdb_core::clock::now()) {
                    account.shed(reason);
                    break;
                }
            }
        });
    }

    /// Judge the figure already on the books against the class limits at `now`,
    /// reporting the shed reason if it is over.
    ///
    /// `now` is a parameter rather than a clock read so a test can drive the
    /// window without waiting out a real `soft_seconds`; the tick passes
    /// `frogdb_core::clock::now()`.
    fn re_judge(&self, now: Instant) -> Option<ShedReason> {
        let (verdict, buffered, limit) = {
            let mut account = self.lock();
            let buffered = account.buffered_bytes();
            (account.judge(now), buffered, account.limit())
        };
        match verdict {
            OutputVerdict::Keep => None,
            OutputVerdict::Shed(reason) => {
                self.record_shed(reason, buffered, limit);
                Some(reason)
            }
        }
    }

    /// Fire the out-of-band shed, which fails the socket the session armed it
    /// over — including one parked inside a write on a replica that has stopped
    /// reading, which is the whole point.
    fn shed(&self, reason: ShedReason) {
        let sender = self
            .shed_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(sender) = sender {
            let _ = sender.send(reason.as_str());
        }
    }

    /// The account, with a poisoned lock recovered rather than propagated: a
    /// panic in one feed report must not turn every later report — including
    /// the release on teardown — into a second panic that leaks the charge.
    fn lock(&self) -> std::sync::MutexGuard<'_, OutputBufferAccount> {
        self.account.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Log and count a shed, wherever it was decided.
    fn record_shed(&self, reason: ShedReason, buffered: u64, limit: OutputBufferLimit) {
        tracing::warn!(
            conn_id = self.conn_id,
            class = OutputBufferClass::Replica.as_str(),
            reason = reason.as_str(),
            buffered,
            hard_limit = limit.hard_bytes,
            soft_limit = limit.soft_bytes,
            "replica feed output buffer limit exceeded; disconnecting"
        );
        // The same counter the connection-side seam moves, with the same
        // labels: an operator asking "how many clients did the output-buffer
        // limit kill, and which class" gets one answer, whichever side of the
        // handoff the kill happened on.
        frogdb_telemetry::definitions::ClientOutputBufferDisconnects::inc(
            &*self.metrics,
            OutputBufferClass::Replica.as_str(),
            reason.as_str(),
        );
    }

    /// Republish this connection's `omem` from the figure the limit was taken
    /// on, so the two can never disagree — skipping the registry write when the
    /// figure has not moved.
    fn publish_omem(&self, buffered: u64) {
        if self.last_published.swap(buffered, Ordering::Relaxed) == buffered {
            return;
        }
        let mut mem = self.base.clone();
        mem.output_list_mem = buffered as usize;
        self.registry.update_memory(self.conn_id, mem);
    }
}

impl frogdb_replication::FeedOutputAccount for ReplicaFeedAccount {
    fn set_buffered(&self, total_bytes: u64) -> frogdb_replication::FeedVerdict {
        if self.released.load(Ordering::Acquire) {
            // A write task aborted at its next await point can still finish the
            // step it was in. The feed is over; re-charging for it would leave
            // a stale `omem` on a connection that is gone.
            return frogdb_replication::FeedVerdict::Keep;
        }
        // The clock read and the lock are both held for one `judge`. Nothing
        // awaits inside, so the write task cannot park holding it.
        let (verdict, limit) = {
            let mut account = self.lock();
            let verdict = account.set_buffered(total_bytes, frogdb_core::clock::now());
            (verdict, account.limit())
        };
        self.publish_omem(total_bytes);

        match verdict {
            OutputVerdict::Keep => frogdb_replication::FeedVerdict::Keep,
            OutputVerdict::Shed(reason) => {
                self.record_shed(reason, total_bytes, limit);
                frogdb_replication::FeedVerdict::Shed {
                    reason: reason.as_str(),
                }
            }
        }
    }

    fn release(&self) {
        // Marked released *before* the charge is dropped, so a report racing
        // this from an aborted write task cannot re-take it afterwards.
        self.released.store(true, Ordering::Release);
        self.lock().release_all();
        self.publish_omem(0);
    }

    fn take_shed_signal(&self) -> Option<tokio::sync::oneshot::Receiver<&'static str>> {
        self.shed_rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
}

#[cfg(test)]
mod tests {
    use frogdb_memory::{Disposition, Subsystem};

    use super::*;

    fn budget(limit: u64) -> Budget {
        Budget::new(Subsystem::NetworkOutput, Disposition::Shed, limit)
    }

    fn account(class: OutputBufferClass, limits: OutputBufferLimits) -> OutputBufferAccount {
        OutputBufferAccount::new(class, limits, &budget(u64::MAX))
    }

    #[test]
    fn redis_defaults_are_the_shipped_triples() {
        let limits = OutputBufferLimits::default();
        assert_eq!(limits.normal, OutputBufferLimit::UNLIMITED);
        assert_eq!(limits.replica.hard_bytes, 256 * 1024 * 1024);
        assert_eq!(limits.replica.soft_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.replica.soft_seconds, 60);
        assert_eq!(limits.pubsub.hard_bytes, 32 * 1024 * 1024);
        assert_eq!(limits.pubsub.soft_bytes, 8 * 1024 * 1024);
        assert_eq!(limits.pubsub.soft_seconds, 60);
    }

    #[test]
    fn a_normal_client_is_unlimited_by_default() {
        let mut acct = account(OutputBufferClass::Normal, OutputBufferLimits::default());
        let now = Instant::now();
        assert_eq!(
            acct.set_buffered(1 << 30, now),
            OutputVerdict::Keep,
            "Redis ships normal 0 0 0; a gigabyte of buffered reply is not a kill"
        );
        assert_eq!(acct.buffered_bytes(), 1 << 30);
    }

    // FM-MEMORY-002
    #[test]
    fn buffered_bytes_are_charged_to_the_network_output_budget() {
        let budget = budget(1_000);
        let mut acct =
            OutputBufferAccount::new(OutputBufferClass::Normal, Default::default(), &budget);
        let now = Instant::now();

        assert_eq!(acct.set_buffered(400, now), OutputVerdict::Keep);
        assert_eq!(budget.charged(), 400, "the budget sees the buffered bytes");

        assert_eq!(acct.set_buffered(150, now), OutputVerdict::Keep);
        assert_eq!(
            budget.charged(),
            150,
            "a partial flush releases the difference"
        );

        assert_eq!(acct.set_buffered(0, now), OutputVerdict::Keep);
        assert_eq!(budget.charged(), 0, "a full flush releases them");

        drop(acct);
        assert_eq!(budget.charged(), 0);
    }

    // FM-MEMORY-002
    #[test]
    fn a_refused_charge_sheds_the_connection() {
        let budget = budget(100);
        let mut acct =
            OutputBufferAccount::new(OutputBufferClass::Normal, Default::default(), &budget);
        let now = Instant::now();

        assert_eq!(acct.set_buffered(60, now), OutputVerdict::Keep);
        assert_eq!(
            acct.set_buffered(120, now),
            OutputVerdict::Shed(ShedReason::BudgetRefused),
            "NetworkOutput's disposition is Shed, so a refusal closes the client"
        );
        assert_eq!(
            acct.buffered_bytes(),
            60,
            "a refused charge leaves the account where it was"
        );
        assert_eq!(budget.refusals(), 1);
    }

    // FM-MEMORY-001
    #[test]
    fn the_hard_limit_sheds_at_once() {
        let limits = OutputBufferLimits {
            replica: OutputBufferLimit {
                hard_bytes: 1_000,
                soft_bytes: 0,
                soft_seconds: 0,
            },
            ..Default::default()
        };
        let mut acct = account(OutputBufferClass::Replica, limits);
        let now = Instant::now();

        assert_eq!(acct.set_buffered(999, now), OutputVerdict::Keep);
        assert_eq!(
            acct.set_buffered(1_000, now),
            OutputVerdict::Shed(ShedReason::HardLimit),
            "Redis kills at the hard limit, not past it"
        );
    }

    // FM-MEMORY-001
    #[test]
    fn the_soft_limit_sheds_only_after_the_window() {
        let limits = OutputBufferLimits {
            replica: OutputBufferLimit {
                hard_bytes: 0,
                soft_bytes: 1_000,
                soft_seconds: 60,
            },
            ..Default::default()
        };
        let mut acct = account(OutputBufferClass::Replica, limits);
        let start = Instant::now();

        assert_eq!(
            acct.set_buffered(1_000, start),
            OutputVerdict::Keep,
            "crossing the soft limit starts the timer, it does not kill"
        );
        assert_eq!(
            acct.set_buffered(1_000, start + Duration::from_secs(59)),
            OutputVerdict::Keep,
            "still inside the window"
        );
        assert_eq!(
            acct.set_buffered(1_000, start + Duration::from_secs(60)),
            OutputVerdict::Shed(ShedReason::SoftLimit),
            "above the soft limit for the whole window"
        );
    }

    // FM-MEMORY-001
    #[test]
    fn dropping_under_the_soft_limit_restarts_the_window() {
        let limits = OutputBufferLimits {
            replica: OutputBufferLimit {
                hard_bytes: 0,
                soft_bytes: 1_000,
                soft_seconds: 60,
            },
            ..Default::default()
        };
        let mut acct = account(OutputBufferClass::Replica, limits);
        let start = Instant::now();

        assert_eq!(acct.set_buffered(1_200, start), OutputVerdict::Keep);
        assert_eq!(
            acct.set_buffered(1_200, start + Duration::from_secs(59)),
            OutputVerdict::Keep
        );

        // The replica caught up for a moment: Redis's soft limit requires the
        // overage to be *continuous*.
        assert_eq!(
            acct.set_buffered(700, start + Duration::from_secs(60)),
            OutputVerdict::Keep
        );
        assert_eq!(
            acct.set_buffered(700, start + Duration::from_secs(61)),
            OutputVerdict::Keep,
            "the window restarts from zero once output falls back under"
        );

        assert_eq!(
            acct.set_buffered(1_200, start + Duration::from_secs(62)),
            OutputVerdict::Keep
        );
        assert_eq!(
            acct.set_buffered(1_200, start + Duration::from_secs(121)),
            OutputVerdict::Keep,
            "only 59s of the new window have passed"
        );
        assert_eq!(
            acct.set_buffered(1_200, start + Duration::from_secs(122)),
            OutputVerdict::Shed(ShedReason::SoftLimit)
        );
    }

    // FM-MEMORY-001
    #[test]
    fn changing_class_restarts_the_soft_window_and_switches_the_limit() {
        let limits = OutputBufferLimits {
            normal: OutputBufferLimit {
                hard_bytes: 0,
                soft_bytes: 1_000,
                soft_seconds: 60,
            },
            pubsub: OutputBufferLimit {
                hard_bytes: 4_000,
                soft_bytes: 1_000,
                soft_seconds: 60,
            },
            ..Default::default()
        };
        let mut acct = account(OutputBufferClass::Normal, limits);
        let start = Instant::now();

        assert_eq!(acct.set_buffered(2_000, start), OutputVerdict::Keep);
        acct.set_class(OutputBufferClass::PubSub);
        assert_eq!(acct.class(), OutputBufferClass::PubSub);
        assert_eq!(
            acct.set_buffered(2_000, start + Duration::from_secs(60)),
            OutputVerdict::Keep,
            "seconds accrued under the normal class are not evidence against pubsub"
        );
        assert_eq!(
            acct.set_buffered(2_000, start + Duration::from_secs(121)),
            OutputVerdict::Shed(ShedReason::SoftLimit)
        );
    }

    // FM-MEMORY-002
    #[test]
    fn draining_releases_every_charged_byte() {
        let budget = budget(10_000);
        let mut acct =
            OutputBufferAccount::new(OutputBufferClass::PubSub, Default::default(), &budget);
        let now = Instant::now();
        assert_eq!(acct.set_buffered(2_048, now), OutputVerdict::Keep);
        assert_eq!(acct.set_buffered(4_096, now), OutputVerdict::Keep);

        acct.release_all();
        assert_eq!(acct.buffered_bytes(), 0);
        assert_eq!(budget.charged(), 0);
    }

    #[test]
    fn the_default_spec_round_trips() {
        let rendered = OutputBufferLimits::default().to_config_string();
        assert_eq!(
            rendered, "normal 0 0 0 replica 268435456 67108864 60 pubsub 33554432 8388608 60",
            "CONFIG GET reports the same three triples Redis reports"
        );
        assert_eq!(
            OutputBufferLimits::parse(&rendered).expect("its own rendering parses"),
            OutputBufferLimits::default()
        );
    }

    #[test]
    fn a_spec_overrides_only_the_classes_it_names() {
        let limits = OutputBufferLimits::parse("normal 1mb 512kb 30").expect("valid");
        assert_eq!(
            limits.normal,
            OutputBufferLimit {
                hard_bytes: 1024 * 1024,
                soft_bytes: 512 * 1024,
                soft_seconds: 30,
            }
        );
        assert_eq!(
            limits.replica,
            OutputBufferLimit::REPLICA_DEFAULT,
            "an unmentioned class keeps Redis's default"
        );
        assert_eq!(limits.pubsub, OutputBufferLimit::PUBSUB_DEFAULT);
    }

    #[test]
    fn redis_size_suffixes_mean_what_redis_means() {
        let limits = OutputBufferLimits::parse("normal 1k 1kb 0 pubsub 1g 2mb 1").expect("valid");
        assert_eq!(limits.normal.hard_bytes, 1_000, "'k' is 1000 in redis.conf");
        assert_eq!(limits.normal.soft_bytes, 1024, "'kb' is 1024");
        assert_eq!(limits.pubsub.hard_bytes, 1_000_000_000);
        assert_eq!(limits.pubsub.soft_bytes, 2 * 1024 * 1024);
    }

    #[test]
    fn slave_is_an_alias_for_replica() {
        let limits = OutputBufferLimits::parse("slave 10 5 1").expect("valid");
        assert_eq!(
            limits.replica,
            OutputBufferLimit {
                hard_bytes: 10,
                soft_bytes: 5,
                soft_seconds: 1,
            }
        );
    }

    #[test]
    fn a_malformed_spec_is_rejected_rather_than_ignored() {
        for spec in [
            "bogus 1 2 3",
            "normal 1 2",
            "normal 1 2 abc",
            "normal 1 nonsense 3",
        ] {
            assert!(
                OutputBufferLimits::parse(spec).is_err(),
                "'{spec}' must not parse: a silently-dropped limit is a limit that does not exist"
            );
        }
    }

    /// A feed account with a `replica` class of exactly these limits, holding
    /// nothing yet.
    fn replica_feed(limit: OutputBufferLimit) -> std::sync::Arc<ReplicaFeedAccount> {
        let limits = OutputBufferLimits {
            replica: limit,
            ..OutputBufferLimits::default()
        };
        ReplicaFeedAccount::new(
            account(OutputBufferClass::Normal, limits),
            std::sync::Arc::new(frogdb_core::ClientRegistry::new()),
            7,
            frogdb_core::ClientMemoryUsage::default(),
            std::sync::Arc::new(frogdb_core::noop::NoopMetricsRecorder),
        )
    }

    // FM-REPLICATION-069
    /// The case `client-output-buffer-limit`'s soft seconds exist for is a feed
    /// that has *stopped*: a replica stalls, the feed makes one report over the
    /// soft mark, and then nothing on that link ever runs again. A verdict that
    /// only arrives with the next report would never arrive at all, so the
    /// account re-judges the figure already on its books on its own clock and
    /// fires the shed the session armed over its socket.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_feed_is_shed_when_its_soft_window_expires() {
        use frogdb_replication::FeedOutputAccount;

        let feed = replica_feed(OutputBufferLimit {
            hard_bytes: 1024 * 1024,
            soft_bytes: 512,
            soft_seconds: 10,
        });
        let mut shed = feed
            .take_shed_signal()
            .expect("the session arms the account's shed over its socket");

        // One report, over the soft mark: the window opens and the link is
        // kept, exactly as Redis's first crossing does.
        assert!(matches!(
            feed.set_buffered(4096),
            frogdb_replication::FeedVerdict::Keep
        ));
        assert_eq!(
            shed.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty),
            "the first crossing is not a kill"
        );

        // The replica is gone. No further report is made — the only thing that
        // happens from here is time passing.
        tokio::time::sleep(Duration::from_secs(9)).await;
        assert_eq!(
            shed.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty),
            "a feed still inside its window is not a kill either"
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        // Bounded, so a re-judge that never happens fails here rather than
        // hanging the test until the runner's own timeout.
        let reason = tokio::time::timeout(Duration::from_secs(5), shed)
            .await
            .expect("the window expired: nothing further will move this feed")
            .expect("the account is still alive and owns the signal");
        assert_eq!(
            reason,
            ShedReason::SoftLimit.as_str(),
            "the window expiring drops the link, naming the limit it breached"
        );
    }

    // FM-REPLICATION-069
    /// The window has to be *continuous*: a feed that drops back under the soft
    /// mark and stays there is not killed for a burst it has already drained,
    /// however long it then sits idle.
    #[tokio::test(start_paused = true)]
    async fn a_feed_that_drains_below_the_soft_mark_survives_the_window() {
        use frogdb_replication::FeedOutputAccount;

        let feed = replica_feed(OutputBufferLimit {
            hard_bytes: 1024 * 1024,
            soft_bytes: 512,
            soft_seconds: 10,
        });
        let mut shed = feed.take_shed_signal().expect("a shed signal is armed");

        assert!(matches!(
            feed.set_buffered(4096),
            frogdb_replication::FeedVerdict::Keep
        ));
        tokio::time::sleep(Duration::from_secs(5)).await;
        // Drained: the mark is cleared, and the re-judge must clear it too
        // rather than remembering a window that is over.
        assert!(matches!(
            feed.set_buffered(16),
            frogdb_replication::FeedVerdict::Keep
        ));

        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            shed.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty),
            "a drained feed must outlive the window its burst opened"
        );
    }

    // FM-REPLICATION-069
    /// `release` is the last thing that happens to a feed. A write task aborted
    /// at its next await point can still finish the step it was in and report
    /// afterwards; that report must not re-charge the budget or put a nonzero
    /// `omem` back on a connection that is gone.
    #[tokio::test]
    async fn a_report_after_release_takes_nothing_back() {
        use frogdb_replication::FeedOutputAccount;

        let feed = replica_feed(OutputBufferLimit::REPLICA_DEFAULT);
        let _ = feed.set_buffered(4096);
        assert_eq!(feed.lock().buffered_bytes(), 4096);

        feed.release();
        assert_eq!(
            feed.lock().buffered_bytes(),
            0,
            "the exit path gives everything back"
        );

        // The straggler, from a write task aborted mid-step.
        let _ = feed.set_buffered(4096);
        assert_eq!(
            feed.lock().buffered_bytes(),
            0,
            "a report racing the release must not re-charge a feed that is over"
        );
    }

    #[test]
    fn class_and_reason_names_are_stable() {
        assert_eq!(OutputBufferClass::Normal.as_str(), "normal");
        assert_eq!(OutputBufferClass::Replica.as_str(), "replica");
        assert_eq!(OutputBufferClass::PubSub.as_str(), "pubsub");
        assert_eq!(ShedReason::HardLimit.as_str(), "hard_limit");
        assert_eq!(ShedReason::SoftLimit.as_str(), "soft_limit");
        assert_eq!(ShedReason::BudgetRefused.as_str(), "budget_refused");
    }
}
