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

use std::time::{Duration, Instant};

use frogdb_memory::{Budget, Charge, Disposition, Subsystem};

/// An unlimited `NetworkOutput` budget, for a connection built outside a shard
/// runtime — unit tests, and the fallback path where no per-core budget was
/// handed down.
///
/// Unlimited rather than absent so the accounting seam is unconditional: a
/// connection always has somewhere to charge, and the only thing that varies is
/// whether anything is watching. The class limits still apply.
pub fn detached_budget() -> Budget {
    Budget::new(Subsystem::NetworkOutput, Disposition::Shed, u64::MAX)
}

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

    /// The whole buffer reached the socket. Releases every charged byte and
    /// stops the soft-limit timer; a connection with nothing buffered is by
    /// definition not behind.
    pub fn note_drained(&mut self) {
        let held = self.charge.bytes();
        self.charge.shrink(held);
        self.soft_since = None;
    }

    /// Re-rule on a connection whose buffer has not grown — this is what makes
    /// the soft limit a *timer* rather than a threshold only checked on write.
    /// Called from the connection's periodic sync.
    pub fn tick(&mut self, now: Instant) -> OutputVerdict {
        self.judge(now)
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

#[cfg(test)]
mod tests {
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

        acct.note_drained();
        assert_eq!(budget.charged(), 0, "a full flush releases them");

        drop(acct);
        assert_eq!(budget.charged(), 0);
    }

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
            acct.tick(start + Duration::from_secs(59)),
            OutputVerdict::Keep,
            "still inside the window"
        );
        assert_eq!(
            acct.tick(start + Duration::from_secs(60)),
            OutputVerdict::Shed(ShedReason::SoftLimit),
            "above the soft limit for the whole window"
        );
    }

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
            acct.tick(start + Duration::from_secs(59)),
            OutputVerdict::Keep
        );

        // The replica caught up for a moment: Redis's soft limit requires the
        // overage to be *continuous*.
        assert_eq!(
            acct.set_buffered(700, start + Duration::from_secs(60)),
            OutputVerdict::Keep
        );
        assert_eq!(
            acct.tick(start + Duration::from_secs(61)),
            OutputVerdict::Keep,
            "the window restarts from zero once output falls back under"
        );

        assert_eq!(
            acct.set_buffered(1_200, start + Duration::from_secs(62)),
            OutputVerdict::Keep
        );
        assert_eq!(
            acct.tick(start + Duration::from_secs(121)),
            OutputVerdict::Keep,
            "only 59s of the new window have passed"
        );
        assert_eq!(
            acct.tick(start + Duration::from_secs(122)),
            OutputVerdict::Shed(ShedReason::SoftLimit)
        );
    }

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
            acct.tick(start + Duration::from_secs(60)),
            OutputVerdict::Keep,
            "seconds accrued under the normal class are not evidence against pubsub"
        );
        assert_eq!(
            acct.tick(start + Duration::from_secs(121)),
            OutputVerdict::Shed(ShedReason::SoftLimit)
        );
    }

    #[test]
    fn draining_releases_every_charged_byte() {
        let budget = budget(10_000);
        let mut acct =
            OutputBufferAccount::new(OutputBufferClass::PubSub, Default::default(), &budget);
        let now = Instant::now();
        assert_eq!(acct.set_buffered(2_048, now), OutputVerdict::Keep);
        assert_eq!(acct.set_buffered(4_096, now), OutputVerdict::Keep);

        acct.note_drained();
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
