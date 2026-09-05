//! Per-replica session state machine.
//!
//! A `ReplicaSession` owns the entire lifecycle of one replica connection on
//! the primary side: from initial registration through optional FULLRESYNC to
//! live WAL streaming, and finally disconnect.
//!
//! This module is the **I/O half** of that lifecycle. Where the session goes
//! next is [`crate::session_machine`]'s: a pure
//! `step(view, event) -> Transition { phase, effects }` over the phases below.
//! [`ReplicaSession::run`] stands up a `SessionDriver`, which publishes the
//! phase each transition names and performs its effects in order — the socket,
//! the filesystem, the tracker and the spawned read/write tasks live here and
//! the decisions do not.
//!
//! The live feed inside [`Phase::Streaming`] has the same split of its own:
//! [`crate::feed_sequencer`] decides when a frame ships, when it is buffered
//! behind an armed slot-handoff barrier and what ends the link, and the write
//! task below performs the actions it returns.
//!
//! # Phases
//!
//! ```text
//! Connecting ─► PreparingCheckpoint ─► StreamingCheckpoint ─► Streaming ─► Disconnecting
//!     │                                                          ▲
//!     └────────── partial sync (CONTINUE) ───────────────────────┘
//! ```
//!
//! `Phase::Disconnecting` is reached from any prior phase, and is terminal by
//! construction — no arm of the transition table leaves it. Its effects
//! unregister the session, clean up any checkpoint directory it staged, and log
//! the disconnect, and they run regardless of which `?`-propagated error or
//! task termination ended the sync.

use frogdb_types::clock;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use parking_lot::RwLock;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::broadcast;

use crate::BoxedStream;
use crate::feed_account::{FeedVerdict, SharedFeedAccount};
use crate::feed_sequencer::{FeedAction, FeedInput, FeedSequencer};
use crate::frame::{ReplconfCodec, ReplicationFrame};
use crate::fullsync::{
    CheckpointChecksum, CheckpointFileHeader, CheckpointStreamCodec, FullSyncMetadata,
    ShardCoverage, calculate_bytes_checksum, calculate_file_checksum, stream_file_to_writer,
};
use crate::primary::{
    FullSyncCapture, LAG_CHECK_INTERVAL, LagThresholds, PrimaryReplicationHandler,
};
use crate::session_machine::{
    BeginSync, Effect, LinkOutcome, ResumeSource, SessionEvent, SessionView, StepOutcome,
    SyncFailure, Transition, step,
};
use crate::sync_counters::SyncOutcome;
use crate::tracker::ReplicationTrackerImpl;

/// Lifecycle phase of a replica session.
///
/// Each session moves monotonically forward through its phases. External
/// observers (INFO replication, ROLE, cluster bus) read the phase via
/// [`ReplicaSession::phase`] or via [`ReplicaInfo`] snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Registered, awaiting server-side decision (FULLRESYNC vs CONTINUE).
    Connecting,
    /// `spawn_blocking(rocks.create_checkpoint)` is in flight.
    PreparingCheckpoint,
    /// Sending checkpoint files to the replica.
    StreamingCheckpoint,
    /// Live WAL stream is flowing; partial syncs enter directly here.
    Streaming,
    /// Terminal — cleanup is running.
    Disconnecting,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Phase::Connecting => write!(f, "connecting"),
            Phase::PreparingCheckpoint => write!(f, "preparing-checkpoint"),
            Phase::StreamingCheckpoint => write!(f, "streaming-checkpoint"),
            Phase::Streaming => write!(f, "streaming"),
            Phase::Disconnecting => write!(f, "disconnecting"),
        }
    }
}

/// How a *streaming* replica's link ended (FM-REPLICATION-062).
///
/// The self-fence is the only consumer: a primary that lost its last replica
/// keeps refusing writes, while one whose last replica closed the link keeps
/// accepting them. The distinction it can actually draw is between **silence**
/// (a link that stopped answering — timeouts, lag, transport errors, a
/// partition, and the case the fence exists for) and **closure** (a link this
/// primary saw end). It is not a claim about operator intent: a killed replica
/// closes its socket the way an orderly one does.
///
/// Sessions that never reached [`Phase::Streaming`] produce no departure at
/// all — they never armed the fence, so they must not be able to disarm it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaDeparture {
    /// The link ended the way a finished link ends: an orderly EOF from the
    /// replica's half, the primary's own broadcaster going away, or a teardown
    /// this primary asked for (demotion / decommission).
    Graceful,
    /// The link was lost: a transport error either way, a write timeout, a lag
    /// disconnect, a broadcast overrun, or a frame that could not be encoded.
    /// Also every error propagated out of the sync phases, so a new failure
    /// path is classified conservatively without being touched.
    Lost,
}

impl ReplicaDeparture {
    /// The atomic encoding the tracker stores. `0` is reserved for "no
    /// streaming replica has departed", which is why neither variant uses it.
    pub(crate) const NONE: u8 = 0;

    pub(crate) fn as_code(self) -> u8 {
        match self {
            ReplicaDeparture::Graceful => 1,
            ReplicaDeparture::Lost => 2,
        }
    }

    /// The inverse of [`Self::as_code`]. Anything that is not a departure this
    /// crate wrote — [`Self::NONE`], or a value no writer can produce — reads
    /// as `None`, i.e. *unknown*, which every consumer must treat as the
    /// unsafe-to-assume case (FM-REPLICATION-062).
    pub(crate) fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(ReplicaDeparture::Graceful),
            2 => Some(ReplicaDeparture::Lost),
            _ => None,
        }
    }
}

/// Capabilities advertised by the replica during REPLCONF capa negotiation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplicaCapabilities {
    /// Supports EOF marker in RDB transfer.
    pub eof: bool,
    /// Supports PSYNC2 protocol.
    pub psync2: bool,
}

impl ReplicaCapabilities {
    pub fn parse_capa(capabilities: &[&str]) -> Self {
        let mut caps = Self::default();
        for cap in capabilities {
            // Case-insensitive like every other REPLCONF token on this path
            // (subcommand matching in `AnnouncedOption::parse` is the same):
            // a replica sending `capa EOF` must not be recorded as having
            // announced nothing.
            if cap.eq_ignore_ascii_case("eof") {
                caps.eof = true;
            } else if cap.eq_ignore_ascii_case("psync2") {
                caps.psync2 = true;
            }
        }
        caps
    }
}

/// What a replica said about *itself* in the pre-PSYNC `REPLCONF` exchange.
///
/// The handshake is two-phase: the replica announces its identity with one or
/// more `REPLCONF` options and only then sends `PSYNC`, which is what creates
/// the [`ReplicaSession`]. There is therefore no session to write these into
/// when they arrive — they are accumulated on the *connection* and handed to
/// [`crate::tracker::ReplicationTrackerImpl::register_announced_replica`] at
/// the PSYNC handoff, so a session is never registered with a placeholder
/// identity that INFO could observe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicaAnnouncement {
    /// The port the replica itself serves on — Redis's `slave_listening_port`,
    /// and what `INFO replication` / `ROLE` report so an operator or an
    /// orchestrator can reach the replica from the primary's view. `0` means
    /// the peer never announced one.
    pub listening_port: u16,
    /// The capabilities it claims.
    pub capabilities: ReplicaCapabilities,
    /// The replica's FrogDB binary version, as sent by `REPLCONF
    /// frogdb-version` (`CARGO_PKG_VERSION` on the replica).
    ///
    /// `None` means **unknown**, and unknown is not the same as old-and-fine:
    /// it is what a peer that never sent the option produces, which is any
    /// replica predating the option *and* any non-FrogDB peer. A consumer that
    /// gates on versions must therefore treat `None` as blocking; treating it
    /// as satisfied is how a version gate fails open. Deliberately kept as the
    /// raw announced string rather than a parsed semver: this is a record of
    /// what the peer *said*, and a peer can say anything.
    pub version: Option<String>,
}

impl ReplicaAnnouncement {
    /// Fold one parsed option into the announcement. Later options of the same
    /// kind win, matching Redis, which simply overwrites `slave_listening_port`
    /// / `slave_capa` each time.
    pub fn absorb(&mut self, option: AnnouncedOption) {
        match option {
            AnnouncedOption::ListeningPort(port) => self.listening_port = port,
            AnnouncedOption::Capabilities(caps) => self.capabilities = caps,
            AnnouncedOption::Version(version) => self.version = Some(version),
        }
    }
}

/// A single `REPLCONF` option that describes the replica rather than acting on
/// the link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnnouncedOption {
    /// `REPLCONF listening-port <port>`.
    ListeningPort(u16),
    /// `REPLCONF capa <cap> [<cap> …]`.
    Capabilities(ReplicaCapabilities),
    /// `REPLCONF frogdb-version <version>` — the replica's binary version.
    Version(String),
}

/// Why an announcing `REPLCONF` could not be read.
///
/// A rejection is local to the option: the connection stays open and the
/// replica may carry on to `PSYNC` (FM-REPLICATION-018).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnouncementError {
    /// `listening-port` with no value.
    MissingPort,
    /// `listening-port` whose value is not valid UTF-8, so it cannot even be
    /// handed to the `u16` parser. Distinct from [`Self::InvalidPort`] because
    /// the wire text differs: `ERR invalid port encoding` vs `ERR invalid port
    /// number`.
    InvalidPortEncoding,
    /// `listening-port` whose value is valid UTF-8 but not a `u16`.
    InvalidPort,
    /// `frogdb-version` with no value.
    MissingVersion,
    /// `frogdb-version` whose value is not valid UTF-8. There is no
    /// "invalid version *number*" twin: the version is recorded verbatim and
    /// never parsed here, so the only way it can fail is by not being text.
    InvalidVersionEncoding,
}

impl AnnouncedOption {
    /// Parse a `REPLCONF` invocation's arguments.
    ///
    /// `Ok(None)` means "this is not an option that describes the replica" —
    /// `ACK`, `GETACK`, `ip-address`, bare `REPLCONF`, and every option a
    /// future or foreign peer knows that this primary does not. Those keep
    /// flowing to the ordinary `REPLCONF` handler, so the forward-compatible
    /// `+OK` of FM-REPLICATION-018 is untouched.
    ///
    /// This is the one parser for both halves of the split: the connection-level
    /// handshake stage calls it to *record* the announcement, and the ordinary
    /// `REPLCONF` command calls it so a `REPLCONF` that reaches the shard path
    /// (queued inside `MULTI`) validates and replies identically.
    pub fn parse(args: &[Bytes]) -> Result<Option<Self>, AnnouncementError> {
        let Some(subcommand) = args.first() else {
            return Ok(None);
        };
        let subcommand = String::from_utf8_lossy(subcommand).to_ascii_lowercase();
        match subcommand.as_str() {
            "listening-port" => {
                let raw = args.get(1).ok_or(AnnouncementError::MissingPort)?;
                let port = std::str::from_utf8(raw)
                    .map_err(|_| AnnouncementError::InvalidPortEncoding)?
                    .parse::<u16>()
                    .map_err(|_| AnnouncementError::InvalidPort)?;
                Ok(Some(Self::ListeningPort(port)))
            }
            "capa" => {
                // Unknown capability names are dropped rather than refused: a
                // replica that claims something this primary has never heard of
                // is recorded without it and still completes its handshake.
                let named: Vec<&str> = args[1..]
                    .iter()
                    .filter_map(|b| std::str::from_utf8(b).ok())
                    .collect();
                Ok(Some(Self::Capabilities(ReplicaCapabilities::parse_capa(
                    &named,
                ))))
            }
            "frogdb-version" => {
                let raw = args.get(1).ok_or(AnnouncementError::MissingVersion)?;
                let version = std::str::from_utf8(raw)
                    .map_err(|_| AnnouncementError::InvalidVersionEncoding)?;
                Ok(Some(Self::Version(version.to_string())))
            }
            _ => Ok(None),
        }
    }
}

/// Seconds since an ACK landed — the one spelling of Redis's `slaveN:lag`.
///
/// Read by [`ReplicationTrackerImpl::replica_lag_secs`] for the proactive
/// lag-disconnect policy and by [`ReplicaInfo::lag_secs`] for `INFO
/// replication`, so the number an operator reads is the number the primary
/// acts on.
///
/// [`ReplicationTrackerImpl::replica_lag_secs`]: crate::tracker::ReplicationTrackerImpl::replica_lag_secs
pub fn ack_age_secs(last_ack_time: Instant) -> f64 {
    clock::elapsed(last_ack_time).as_secs_f64()
}

/// Snapshot view of a replica session for read consumers (INFO, ROLE, cluster bus).
///
/// Built on demand via [`ReplicaSession::snapshot`]. The snapshot is decoupled
/// from the session so callers don't need to hold any locks while reading.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub id: u64,
    pub address: SocketAddr,
    pub listening_port: u16,
    pub acked_offset: u64,
    pub last_ack_time: Instant,
    pub connected_at: Instant,
    pub phase: Phase,
    pub capabilities: ReplicaCapabilities,
    /// What the replica announced over `REPLCONF frogdb-version`, verbatim.
    /// `None` is **unknown**, not "old" — see [`ReplicaAnnouncement::version`].
    pub replica_version: Option<String>,
}

impl ReplicaInfo {
    /// Returns true if this replica is in the live-streaming phase.
    pub fn is_streaming(&self) -> bool {
        matches!(self.phase, Phase::Streaming)
    }

    /// Whole seconds since this replica's last ACK — Redis's `slaveN:lag`.
    ///
    /// The same measure [`ack_age_secs`] hands the proactive-disconnect policy,
    /// truncated to Redis's integer field, read off this snapshot instead of
    /// re-locking the registry.
    pub fn lag_secs(&self) -> u64 {
        ack_age_secs(self.last_ack_time) as u64
    }
}

/// What sync flow to drive for this session.
#[derive(Debug)]
pub enum SyncKind {
    /// Partial resync (`+CONTINUE`): the replica's replid + offset are inside the
    /// continuable window AND the backlog still covers `(replay_from, current]`.
    /// The session replays that backlog tail before joining the live tail.
    /// `replay_from` is the replica's offset; the streamer re-extracts the tail
    /// after subscribing to the broadcast so no write made during the handshake
    /// slips through the gap (see [`ReplicaSession::start_streaming`]).
    Partial { replay_from: u64 },
    /// Send a full database snapshot. The snapshot's replication offset is
    /// captured from the live tracker at checkpoint-cut time inside
    /// [`SessionDriver::new`], not threaded in here, so it corresponds to the
    /// data actually contained in the checkpoint.
    Full { replication_id: String },
}

struct SessionInner {
    phase: Phase,
    last_ack_time: Instant,
    listening_port: u16,
    capabilities: ReplicaCapabilities,
    /// Seeded from the announcement at construction, exactly like the port and
    /// the capabilities — there is no setter, so it cannot be observed in a
    /// placeholder state (FM-REPLICATION-049).
    replica_version: Option<String>,
    /// Set once the checkpoint dir has been created and is owed cleanup.
    sync_checkpoint_path: Option<PathBuf>,
    /// Total bytes for the in-flight checkpoint stream (set when files are enumerated).
    sync_total_bytes: u64,
    /// Wall clock when the checkpoint stream started (for rate logging).
    sync_started_at: Option<Instant>,
}

/// A primary-side session that owns one replica's lifecycle from registration
/// to disconnect.
///
/// Constructed by [`crate::tracker::ReplicationTrackerImpl::register_replica`] and
/// driven to completion by [`ReplicaSession::run`]. A single exit handler in
/// `run()` runs cleanup (registry removal, checkpoint dir delete, disconnect log)
/// regardless of which path returned an error.
pub struct ReplicaSession {
    id: u64,
    address: SocketAddr,
    connected_at: Instant,

    // Hot atomic counters — written from the read/write tasks and queried by
    // INFO/ROLE consumers without needing to lock the inner state.
    /// What the replica has acknowledged **on the wire**, and nothing else.
    /// Written only by [`Self::record_ack`]. See [`Self::seed_resume_position`]
    /// for why the primary's own bookkeeping is not allowed in here.
    acked_offset: AtomicU64,
    /// Where this replica *resumed* — its `PSYNC` offset for a partial resync,
    /// the checkpoint's `snapshot_offset` for a full one. Primary bookkeeping,
    /// written only by [`Self::seed_resume_position`].
    resume_offset: AtomicU64,
    sync_bytes_transferred: AtomicU64,

    /// Primary-initiated teardown signal (see [`Self::request_disconnect`]).
    /// `notify_one`, not `notify_waiters`, so a request that races ahead of the
    /// streaming loop's `notified()` is stored as a permit rather than lost.
    disconnect: tokio::sync::Notify,

    inner: RwLock<SessionInner>,
}

impl ReplicaSession {
    /// Create a new session in the `Connecting` phase for a replica that
    /// announced nothing about itself.
    pub fn new(id: u64, address: SocketAddr) -> Arc<Self> {
        Self::announced(id, address, ReplicaAnnouncement::default())
    }

    /// Create a new session in the `Connecting` phase, seeded with what the
    /// replica announced over `REPLCONF` before it sent `PSYNC`.
    ///
    /// The announcement is applied at construction rather than through a setter
    /// so there is no window in which a registered session reports a
    /// placeholder port to `INFO replication` / `ROLE`.
    pub fn announced(id: u64, address: SocketAddr, announcement: ReplicaAnnouncement) -> Arc<Self> {
        let now = clock::now();
        Arc::new(Self {
            id,
            address,
            connected_at: now,
            acked_offset: AtomicU64::new(0),
            resume_offset: AtomicU64::new(0),
            sync_bytes_transferred: AtomicU64::new(0),
            disconnect: tokio::sync::Notify::new(),
            inner: RwLock::new(SessionInner {
                phase: Phase::Connecting,
                last_ack_time: now,
                listening_port: announcement.listening_port,
                capabilities: announcement.capabilities,
                replica_version: announcement.version,
                sync_checkpoint_path: None,
                sync_total_bytes: 0,
                sync_started_at: None,
            }),
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn connected_at(&self) -> Instant {
        self.connected_at
    }
    /// What this replica has acknowledged on the wire — Redis's `repl_ack_off`.
    ///
    /// This is the durability number: `WAIT`'s count, `min_acked_offset` and
    /// `INFO`'s `slaveN:offset=` all read it, and it moves only when a
    /// `REPLCONF ACK` arrives. `0` means "has not acked yet", never "resumed at
    /// 0" — see [`Self::resume_offset`].
    pub fn acked_offset(&self) -> u64 {
        self.acked_offset.load(Ordering::Acquire)
    }
    /// Where this replica resumed the stream, as recorded by the primary.
    ///
    /// A sender-side fact: it says which byte the primary started forwarding
    /// from, not which byte the replica applied. Only the byte-lag measure
    /// reads it (through [`Self::stream_position`]).
    pub fn resume_offset(&self) -> u64 {
        self.resume_offset.load(Ordering::Acquire)
    }
    /// How far along the stream this link is, for *lag* purposes only.
    ///
    /// `max(acked_offset, resume_offset)`: a replica that just resumed at the
    /// live head is not behind, even though it has acked nothing yet, and
    /// measuring its lag from `acked_offset` alone would hand every
    /// freshly-online replica to [`LagPolicy`] for an immediate disconnect. The
    /// two positions are folded together *here and nowhere else* — no consumer
    /// that answers a durability question may use this (FM-REPLICATION-039).
    pub fn stream_position(&self) -> u64 {
        self.acked_offset().max(self.resume_offset())
    }
    pub fn last_ack_time(&self) -> Instant {
        self.inner.read().last_ack_time
    }
    pub fn phase(&self) -> Phase {
        self.inner.read().phase
    }
    pub fn is_streaming(&self) -> bool {
        matches!(self.phase(), Phase::Streaming)
    }
    pub fn listening_port(&self) -> u16 {
        self.inner.read().listening_port
    }
    pub fn capabilities(&self) -> ReplicaCapabilities {
        self.inner.read().capabilities
    }
    /// The replica's announced FrogDB version, or `None` for a peer that never
    /// announced one — unknown, which a version gate must treat as blocking.
    pub fn replica_version(&self) -> Option<String> {
        self.inner.read().replica_version.clone()
    }

    /// Ask this session's streaming loop to tear itself down.
    ///
    /// Redis's `replicationSetMaster` calls `disconnectSlaves` when a primary
    /// becomes a replica: the stream this node was serving describes a history
    /// it no longer heads, so the downstream replicas must resync against
    /// whoever does. The session ends as if the socket had closed — the replica
    /// reconnects and PSYNCs afresh.
    pub fn request_disconnect(&self) {
        self.disconnect.notify_one();
    }

    /// Resolves once [`Self::request_disconnect`] has been called.
    async fn disconnect_requested(&self) {
        self.disconnect.notified().await;
    }

    /// Build a snapshot for read-only consumers (INFO, ROLE, cluster bus).
    pub fn snapshot(&self) -> ReplicaInfo {
        let inner = self.inner.read();
        ReplicaInfo {
            id: self.id,
            address: self.address,
            listening_port: inner.listening_port,
            acked_offset: self.acked_offset.load(Ordering::Acquire),
            last_ack_time: inner.last_ack_time,
            connected_at: self.connected_at,
            phase: inner.phase,
            capabilities: inner.capabilities,
            replica_version: inner.replica_version.clone(),
        }
    }

    /// Record a REPLCONF ACK from the replica.
    ///
    /// Always refreshes `last_ack_time` (any ACK proves liveness, even on an
    /// idle primary). Returns `true` only if the offset advanced — callers
    /// use this to decide whether to notify WAIT waiters via the broadcast channel.
    pub fn record_ack(&self, sequence: u64) -> bool {
        // Refresh liveness regardless
        let now = clock::now();
        self.inner.write().last_ack_time = now;
        // Conditional offset update
        let prev = self.acked_offset.load(Ordering::Acquire);
        if sequence > prev {
            self.acked_offset.store(sequence, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Record where the replica resumed when its session enters streaming.
    ///
    /// A *primary bookkeeping* fact — the offset this replica restarted the
    /// stream from (its PSYNC offset for a partial resync, or the checkpoint's
    /// `snapshot_offset` for a full one) — not a replica acknowledgement read
    /// off the wire. It therefore writes its **own** monotonic atomic and
    /// leaves [`Self::acked_offset`] alone.
    ///
    /// The two used to share one field, and that was a false-durability bug
    /// (issue 28): the phase is published at the top of
    /// [`Self::start_streaming`] and the seed is the backlog tail's last
    /// offset, so a replica that had not decoded — let alone applied — a single
    /// byte was counted by `WAIT` at the primary's live offset, and `WAIT 1`
    /// returned 1 against an empty replica keyspace. What a replica has is what
    /// it says it has; Redis moves `repl_ack_off` from `replconfCommand`'s ACK
    /// branch and nowhere else, for exactly this reason.
    ///
    /// The lag clock (`last_ack_time`) *is* reset to the resume instant: the
    /// time-based lag threshold must measure from where streaming (re)started,
    /// not from registration — a long FULLRESYNC checkpoint stream would
    /// otherwise trip it immediately. Liveness and durability are different
    /// questions, and only the first one a resume can answer.
    ///
    /// Returns `true` iff the resume position advanced (`offset > prev`).
    pub fn seed_resume_position(&self, offset: u64) -> bool {
        self.inner.write().last_ack_time = clock::now();
        let prev = self.resume_offset.load(Ordering::Acquire);
        if offset > prev {
            self.resume_offset.store(offset, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// This session's row in the invariant projection.
    ///
    /// `announced_id` is `None` — *unknown* — until the replica sends
    /// `REPLCONF listening-port`, never the placeholder `0`: two unannounced
    /// sessions are no evidence of the same replica, and `INV-SESSION-2` must
    /// not treat them as one.
    pub fn view(&self) -> crate::view::ReplicaView {
        let listening_port = self.listening_port();
        crate::view::ReplicaView {
            id: self.id,
            addr: self.address(),
            announced_id: (listening_port != 0).then(|| (self.address().ip(), listening_port)),
            phase: self.phase(),
            acked: self.acked_offset(),
            resume_floor: self.resume_offset(),
            last_ack_age: clock::now().saturating_duration_since(self.last_ack_time()),
        }
    }

    /// Publish the phase [`step`] landed on.
    ///
    /// The single writer of [`SessionInner::phase`], and reachable from exactly
    /// two places: the driver's commit point (once per step) and
    /// [`Self::force_phase_for_test`]. Where the phase moves is
    /// [`crate::session_machine`]'s decision; this only makes it visible to
    /// `INFO` / `ROLE` / the cluster bus.
    fn commit_phase(&self, phase: Phase) {
        let mut inner = self.inner.write();
        let old = inner.phase;
        inner.phase = phase;
        drop(inner);
        // The widest view reachable from a session: itself. The registry-wide
        // and offset-relative entries declare fields a session cannot see and
        // are skipped here — `INV-SESSION-1` is the claim this seam forces.
        #[cfg(any(test, debug_assertions))]
        crate::invariants::debug_assert_view_clean(
            &crate::view::ReplicationView::empty()
                .with_replicas(vec![self.view()], None)
                .with_phase_change(crate::view::PhaseChange {
                    replica_id: self.id,
                    from: old,
                    to: phase,
                }),
            "ReplicaSession::commit_phase",
        );
        // Equivalent-mutant note: this guard suppresses a duplicate debug line
        // and nothing else — both readings leave identical session state, and
        // the phase written above is the same either way — so no assertion
        // short of capturing the tracing output can distinguish `!=` from `==`
        // here.
        if old != phase {
            tracing::debug!(
                replica_id = self.id,
                old_phase = %old,
                new_phase = %phase,
                "Replica phase change"
            );
        }
    }

    /// Test-only: force the session's phase without driving `run()`.
    ///
    /// Production code transitions phases inside `ReplicaSession::run`; this
    /// helper exists so unit/integration tests in other crates can stage a
    /// session in a particular phase without standing up the full I/O loop.
    #[doc(hidden)]
    pub fn force_phase_for_test(&self, phase: Phase) {
        self.commit_phase(phase);
    }

    /// Test-only: pretend the last ACK landed `by` ago.
    ///
    /// The freshness gates ([`crate::ack_is_fresh`] and its callers) compare
    /// `last_ack_time.elapsed()` against a window. Without this hook a test for
    /// "a replica that has gone silent" would have to *sleep* the window, which
    /// makes the window a lower bound on suite runtime and makes the assertion
    /// racy on a loaded machine. Backdating the instant instead makes staleness
    /// exact and instantaneous.
    #[doc(hidden)]
    pub fn backdate_last_ack_for_test(&self, by: Duration) {
        let mut inner = self.inner.write();
        inner.last_ack_time = clock::now() - by;
    }

    /// Drive the session to completion.
    ///
    /// This is the single owner of the session lifecycle, and it owns exactly
    /// half of it: the I/O. Where the session goes next is
    /// [`crate::session_machine::step`]'s decision — `run` builds the plain-data
    /// [`SessionView`], hands it the event that just happened, publishes the
    /// phase it returns and performs the [`Effect`]s it returns, in order.
    ///
    /// Regardless of where the sync exits — a `?` out of any effect, or a link
    /// that simply ended — the exit step runs: checkpoint cleanup, the departure
    /// record, registry removal, and the disconnect log.
    pub async fn run(
        self: Arc<Self>,
        stream: BoxedStream,
        sync_kind: SyncKind,
        handler: Arc<PrimaryReplicationHandler>,
        feed: SharedFeedAccount,
    ) -> io::Result<()> {
        let mut driver = SessionDriver::new(self.clone(), stream, sync_kind, &handler, feed);
        let result = driver.drive().await;

        // The exit step reads the phase the sync left off on — sampled by the
        // machine *before* it publishes `Disconnecting`, because only a session
        // that actually reached `Streaming` armed the self-fence and so only one
        // that did may report a departure that could disarm it
        // (FM-REPLICATION-062). An error out of any phase is a lost link.
        let outcome = match &result {
            Ok(departure) => LinkOutcome::Ended(*departure),
            Err(_) => LinkOutcome::Errored,
        };
        driver.exit(outcome).await;

        result.map(|_| ())
    }

    /// Stream checkpoint files to the replica.
    ///
    /// The on-wire grammar is owned by [`CheckpointStreamCodec`]; this method
    /// drives it: prelude, then a per-file header + raw payload for each file,
    /// then the trailing metadata frame.
    async fn stream_checkpoint(
        &self,
        stream: &mut BoxedStream,
        handler: &Arc<PrimaryReplicationHandler>,
        checkpoint_path: &Path,
        replication_id: &str,
        replication_offset: u64,
        coverage: ShardCoverage,
    ) -> io::Result<()> {
        // Enumerate all files in the checkpoint directory.
        let mut files: Vec<(String, u64, PathBuf)> = Vec::new();
        let mut total_size = 0u64;
        let mut dir = fs::read_dir(checkpoint_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                let metadata = fs::metadata(&path).await?;
                let file_name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let file_size = metadata.len();
                total_size += file_size;
                files.push((file_name, file_size, path));
            }
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));

        self.inner.write().sync_total_bytes = total_size;
        self.sync_bytes_transferred.store(0, Ordering::Release);

        tracing::info!(
            replica_id = self.id,
            file_count = files.len(),
            total_size = total_size,
            "Streaming checkpoint to replica"
        );

        // Envelope prelude: marker + file count.
        CheckpointStreamCodec::write_prelude(stream, files.len()).await?;

        // Bodies: per-file header via the codec, then the raw payload bytes.
        // The combined checksum (owned by `CheckpointChecksum`) is fed one file
        // at a time, in the same order the codec frames them.
        let mut combined = CheckpointChecksum::new();
        for (file_name, file_size, file_path) in &files {
            CheckpointStreamCodec::write_file_header(
                stream,
                &CheckpointFileHeader {
                    name: file_name.clone(),
                    size: *file_size,
                },
            )
            .await?;
            let bytes_written =
                stream_file_to_writer(file_path, stream, Some(&self.sync_bytes_transferred))
                    .await?;
            let file_hash = calculate_file_checksum(file_path).await?;
            combined.update_file(file_name, &file_hash);
            tracing::debug!(
                file = %file_name,
                size = bytes_written,
                progress = format!("{:.1}%", self.progress_percent()),
                "Streamed checkpoint file"
            );
        }
        let checksum = combined.finalize();

        let metadata = FullSyncMetadata {
            rdb_size: total_size,
            checksum,
            replication_id: replication_id.to_string(),
            replication_offset,
            coverage,
        };
        CheckpointStreamCodec::write_metadata(stream, &metadata).await?;

        // The full-sync payload lane: counted here, once, as the real
        // checksum-verified size, so it is never confused with — or dropped
        // from — the separately-counted frame lane (hardening issue 29).
        handler
            .tracker()
            .net_bytes_handle()
            .record_output(total_size);

        let elapsed = self
            .inner
            .read()
            .sync_started_at
            .map(clock::elapsed)
            .unwrap_or_default();
        let rate_mbps = transfer_rate_mbps(total_size, elapsed);
        tracing::info!(
            replica_id = self.id,
            files = files.len(),
            total_bytes = total_size,
            elapsed_ms = elapsed.as_millis() as u64,
            rate_mbps = format!("{:.2}", rate_mbps),
            "Checkpoint streaming complete"
        );

        Ok(())
    }

    /// Progress as a percentage (0-100). Returns 100 if no bytes are expected yet.
    fn progress_percent(&self) -> f64 {
        let total = self.inner.read().sync_total_bytes;
        if total == 0 {
            return 100.0;
        }
        let transferred = self.sync_bytes_transferred.load(Ordering::Relaxed);
        (transferred as f64 / total as f64) * 100.0
    }

    /// Stream the live keyspace to the replica: the full-sync payload of a
    /// primary that has no RocksDB to checkpoint (issue 67).
    ///
    /// Structurally identical to [`Self::stream_checkpoint`] — same envelope,
    /// same combined checksum, same trailing metadata — with per-shard dataset
    /// blobs in place of checkpoint files. Only the prelude marker differs, so
    /// the replica knows to install the bodies directly instead of staging them
    /// to a disk it is not using either.
    ///
    /// **No pre-checkpoint drain.** The drain exists to push acknowledged writes
    /// out of the shards' flush engines *into RocksDB* before the cut. Here the
    /// shards themselves are the source, so an acknowledged write is in the
    /// export by construction; there is nothing to wait for.
    ///
    /// **`replication_offset` is captured by `run` before this runs**, which
    /// keeps the `offset <= data` direction the checkpoint path relies on: writes
    /// landing during the export only add data, and `(offset, current]` is
    /// replayed from the backlog at the streaming handoff.
    ///
    /// The blobs arrive already exported ([`Effect::ExportLiveDataset`]) because
    /// producing them and shipping them are two separate steps of the machine —
    /// the export is what moves the session onto `StreamingCheckpoint`.
    async fn stream_live_dataset(
        &self,
        stream: &mut BoxedStream,
        handler: &Arc<PrimaryReplicationHandler>,
        blobs: &[Vec<u8>],
        replication_id: &str,
        replication_offset: u64,
        coverage: ShardCoverage,
    ) -> io::Result<()> {
        let total_size: u64 = blobs.iter().map(|b| b.len() as u64).sum();
        self.inner.write().sync_started_at = Some(clock::now());

        tracing::info!(
            replica_id = self.id,
            blob_count = blobs.len(),
            total_size,
            "Streaming live dataset to replica"
        );

        CheckpointStreamCodec::write_snapshot_prelude(stream, blobs.len()).await?;

        // The blob names are positional (`shard-<n>.dataset`) and feed the
        // combined checksum in wire order, exactly as filenames do for a
        // checkpoint — so a reordered or dropped blob fails verification.
        let mut combined = CheckpointChecksum::new();
        for (shard_id, blob) in blobs.iter().enumerate() {
            let name = format!("shard-{shard_id}.dataset");
            CheckpointStreamCodec::write_file_header(
                stream,
                &CheckpointFileHeader {
                    name: name.clone(),
                    size: blob.len() as u64,
                },
            )
            .await?;
            stream.write_all(blob).await?;
            self.sync_bytes_transferred
                .fetch_add(blob.len() as u64, Ordering::Relaxed);
            combined.update_file(&name, &calculate_bytes_checksum(blob));
        }

        CheckpointStreamCodec::write_metadata(
            stream,
            &FullSyncMetadata {
                rdb_size: total_size,
                checksum: combined.finalize(),
                replication_id: replication_id.to_string(),
                replication_offset,
                coverage,
            },
        )
        .await?;

        // The full-sync payload lane: counted here, once, as the real size
        // that was actually written (hardening issue 29) — see the matching
        // comment in `stream_checkpoint`.
        handler
            .tracker()
            .net_bytes_handle()
            .record_output(total_size);

        tracing::info!(
            replica_id = self.id,
            blobs = blobs.len(),
            total_bytes = total_size,
            elapsed_ms = self
                .inner
                .read()
                .sync_started_at
                .map(|t| clock::elapsed(t).as_millis() as u64)
                .unwrap_or_default(),
            "Live dataset streaming complete"
        );
        Ok(())
    }

    /// Enter the live-streaming phase: replay the backlog handoff tail, then
    /// subscribe to WAL frames and forward them to the replica while a read task
    /// consumes REPLCONF ACKs.
    ///
    /// `replay_from` is the offset the replica already holds (its PSYNC offset
    /// for a partial resync, or the checkpoint's `snapshot_offset` for a full
    /// resync). The backlog tail `(replay_from, current]` is streamed before the
    /// live tail so no write is lost at the handoff:
    ///
    /// 1. Subscribe to `wal_broadcast` **first** — every frame broadcast from
    ///    here on is captured, so nothing can fall between the replayed tail and
    ///    the live tail.
    /// 2. Read the live head and replay `(replay_from, current]` from the
    ///    backlog. Subscribing before reading the head guarantees coverage: any
    ///    write whose broadcast preceded the subscribe is in the backlog; any
    ///    that followed is in the live receiver.
    /// 3. Forward the live tail, skipping frames at or below the replayed
    ///    `resume_offset` so the overlap between the two is sent exactly once.
    ///
    /// This single path fixes both the full-sync handoff gap (F1) and the
    /// partial-sync gap (F2). When the backlog is disabled or empty, the replay
    /// is a no-op and the behaviour reduces to forwarding the live tail.
    async fn start_streaming(
        self: Arc<Self>,
        mut stream: BoxedStream,
        handler: &Arc<PrimaryReplicationHandler>,
        replay_from: u64,
        resume: ResumeSource,
    ) -> io::Result<ReplicaDeparture> {
        // `Phase::Streaming` is already published and the previous generation's
        // departure already cleared — both are steps of the transition that got
        // here ([`Effect::ClearDeparture`] immediately after the phase commit),
        // so the phase is visible before any of this tail reaches the socket,
        // exactly as it was when this method wrote it itself.

        // Subscribe BEFORE reading the head / extracting the backlog so the live
        // receiver and the replayed tail cannot leave a gap (step 1 above).
        let mut wal_rx = handler.wal_broadcast.subscribe();

        // What this feed does — when a frame goes on the wire, when it is
        // buffered behind an armed slot-handoff barrier, what ends the link —
        // is [`FeedSequencer`]'s; this method and the write task below only
        // perform what it asks for.
        let mut sequencer = FeedSequencer::new();

        // Its first demand is the barrier wait: the replayed tail is feed too,
        // so a session that handshakes mid-barrier would otherwise pull the
        // held writes straight out of the backlog (FM-CLUSTER-097). Waiting
        // here rather than capping the tail keeps one rule for both lanes, and
        // it is safe to wait *after* subscribing — anything broadcast meanwhile
        // is in the live receiver, and the head is only read below, so the tail
        // this session replays is whatever the barrier left behind. The wait is
        // bounded by the barrier's own deadline (see `ReplicaFeedGate`).
        if matches!(sequencer.start(), FeedAction::AwaitRelease) {
            handler.feed_gate.released().await;
        }

        // Replay the backlog handoff tail `(replay_from, current]` ahead of the
        // live tail (steps 2). `resume_offset` tracks the last offset actually
        // streamed; the live tail dedups against it (step 3).
        let current = handler.offsets.current();
        let mut resume_offset = replay_from;
        // The window can close between the grant and here — the grant is written
        // before the checkpoint is cut, and the whole payload transfer sits in
        // between, which on a busy primary is long enough to evict the resume
        // point outright. A short tail would be streamed as if it were the whole
        // thing (the live tail then dedups against the last frame actually sent),
        // leaving the replica a permanent hole with a contiguous-looking offset,
        // so a truncated window abandons the resume instead: the link drops, the
        // replica reconnects, and its `PSYNC` is answered `+FULLRESYNC` because
        // the same floor check fails at grant time too (round-2 issue 52).
        let tail = handler
            .replay
            .extract_backlog(replay_from, current)
            .map_err(|truncated| {
                tracing::warn!(
                    replica_id = self.id,
                    replay_from,
                    error = %truncated,
                    "Backlog window closed before the resume could be streamed; \
                     dropping the link so the replica comes back for a full sync"
                );
                io::Error::new(io::ErrorKind::InvalidData, truncated)
            })?;
        // The grant is only now a partial resync that happened: the tail exists
        // and the link is still up. A grant abandoned above is deliberately
        // *not* counted at all — the replica reconnects, its second `PSYNC` is
        // refused against the same floor, and that refusal is what moves
        // `sync_partial_err` + `sync_full`. Counting the abandoned grant as well
        // would report two refusals and two full resyncs for one replica that
        // received one.
        if resume == ResumeSource::PartialGrant {
            handler.tracker.record_sync_outcome(SyncOutcome::PartialOk);
        }
        for (offset, shard_id, payload) in tail {
            let encoded = ReplicationFrame::new_on_shard(offset, shard_id, payload).encode()?;
            stream.write_all(&encoded).await?;
            // The backlog-replay lane of the frame lane (hardening issue 29):
            // a resumed replica's tail is real bytes on the wire, written
            // directly here rather than through `forward_frame`, so it needs
            // its own record — omitting it would undercount every partial
            // resync by the size of the tail it replayed.
            handler
                .tracker
                .net_bytes_handle()
                .record_output(encoded.len() as u64);
            resume_offset = offset;
        }
        // Record where the replica resumed, so the lag monitor measures from
        // the resumed position instead of treating a freshly-online replica as
        // maximally behind. This is a primary bookkeeping fact ("where this
        // replica started"), not a replica ACK: it uses the coordinator's
        // `seed_replica_position` verb rather than `ingest_replica_ack`, it
        // lands in its own field, and it credits the replica with nothing that
        // `WAIT` or `INFO` can read as durability — those wait for the wire
        // (FM-REPLICATION-039, issue 28). Note the phase above was published
        // before any of this tail reached the socket, so "streaming" here is a
        // statement about the sender, not the receiver.
        handler
            .offsets
            .seed_replica_position(self.id, resume_offset);

        let (mut read_half, mut write_half) = tokio::io::split(stream);

        let read_offsets = handler.offsets.clone();
        let read_replica_id = self.id;
        let read_task = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024);
            loop {
                match read_half.read_buf(&mut buf).await {
                    // The replica closed its half: the link ended the way a
                    // finished link ends (FM-REPLICATION-062).
                    Ok(0) => break ReplicaDeparture::Graceful,
                    Ok(_) => {
                        while let Some((ack_offset, consumed)) = ReplconfCodec::parse_ack(&buf) {
                            read_offsets.ingest_replica_ack(read_replica_id, ack_offset);
                            buf.advance(consumed);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Error reading from replica");
                        break ReplicaDeparture::Lost;
                    }
                }
            }
        });

        let lag_tracker = handler.tracker.clone();
        let lag_replica_id = self.id;
        let mut lag_policy = LagPolicy::new(handler.lag_thresholds.clone(), handler.lag_cooldown);
        let write_timeout = frame_write_timeout(handler.write_timeout_ms);

        let feed_gate = handler.feed_gate.clone();
        // The handoff lane is behind us: the barrier released and the tail is on
        // the wire up to `resume_offset`. From here the sequencer decides and
        // this task performs.
        let mut action = sequencer.step(FeedInput::HandoffReplayed { resume_offset });
        let write_task = tokio::spawn(async move {
            loop {
                let input = match action {
                    FeedAction::AwaitRelease => {
                        feed_gate.released().await;
                        FeedInput::HandoffReplayed { resume_offset }
                    }
                    FeedAction::Receive => classify_feed_frame(wal_rx.recv().await, lag_replica_id),
                    // A barrier is armed: race the broadcast against its release
                    // so the session keeps draining the channel instead of
                    // lagging off it (FM-CLUSTER-097).
                    FeedAction::ReceiveOrRelease => tokio::select! {
                        received = wal_rx.recv() => classify_feed_frame(received, lag_replica_id),
                        _ = feed_gate.released() => FeedInput::Released,
                    },
                    FeedAction::ConsultGate => FeedInput::GateHeld(feed_gate.is_held()),
                    FeedAction::Send(frame) => {
                        send_feed_frame(
                            &mut write_half,
                            &frame,
                            write_timeout,
                            lag_replica_id,
                            &lag_tracker,
                            &mut lag_policy,
                        )
                        .await
                    }
                    FeedAction::End(departure) => break departure,
                };
                action = sequencer.step(input);
            }
        });

        // When either half ends — the write task after a lag / write-timeout
        // disconnect, or the read task on a replica-initiated close — abort the
        // sibling so BOTH halves of the split stream drop and the TCP socket
        // actually closes. Dropping the `JoinHandle`s alone does *not* abort the
        // tasks: a detached read task would otherwise keep its half alive after
        // a lag-disconnect, leaving the replica in a zombie half-open link
        // (unregistered here, but never sent a FIN, so it never resyncs).
        // The third exit is primary-initiated: a demotion asks every downstream
        // session to end so the replicas resync against the new primary.
        let mut read_task = read_task;
        let mut write_task = write_task;
        // A task that panicked or was cancelled tells us nothing about how the
        // link ended, so it classifies as `Lost`: the fence's safe direction is
        // to stay armed (FM-REPLICATION-062).
        let departure = tokio::select! {
            read = &mut read_task => { write_task.abort(); read.unwrap_or(ReplicaDeparture::Lost) }
            write = &mut write_task => { read_task.abort(); write.unwrap_or(ReplicaDeparture::Lost) }
            _ = self.disconnect_requested() => {
                tracing::info!(
                    replica_id = self.id,
                    "Disconnecting replica session on role change"
                );
                read_task.abort();
                write_task.abort();
                ReplicaDeparture::Graceful
            }
        };
        Ok(departure)
    }
}

/// What performing one [`Effect`] leaves the driver to do next.
enum Next {
    /// Nothing; go on to the next effect of this transition.
    Continue,
    /// Feed this event back into the machine.
    Event(SessionEvent),
    /// The link is over; `run`'s exit step takes it from here.
    Ended(ReplicaDeparture),
}

/// The I/O half of a replica session: it performs what
/// [`crate::session_machine::step`] decides, and reports back what happened.
///
/// The driver owns everything that has to survive between two effects — the
/// socket, and the exported dataset blobs handed from
/// [`Effect::ExportLiveDataset`] to [`Effect::SendLiveDataset`] — so the machine
/// itself can stay memoryless over `(view, event)`.
struct SessionDriver<'a> {
    session: Arc<ReplicaSession>,
    handler: &'a Arc<PrimaryReplicationHandler>,
    /// What the handshake assigned. Fixed for the session's whole life.
    sync: BeginSync,
    /// `None` once [`Effect::Stream`] has taken it.
    stream: Option<BoxedStream>,
    /// Produced by [`Effect::ExportLiveDataset`], consumed by
    /// [`Effect::SendLiveDataset`].
    blobs: Option<Vec<Vec<u8>>>,
    /// What the payload about to be cut already covers, plus the hold that
    /// keeps that true. Produced by [`Effect::DrainBeforeCheckpoint`] (or, with
    /// no hold, by [`Effect::ExportLiveDataset`]); the hold is dropped by
    /// [`Effect::CutCheckpoint`] — which abandons the sync outright if a hold
    /// lapsed first — and the coverage travels on into the trailer written by
    /// [`Effect::SendCheckpoint`] / [`Effect::SendLiveDataset`].
    capture: Option<FullSyncCapture>,
    /// What accounts for the replica-bound bytes this session holds
    /// (FM-REPLICATION-069). Shared with the spawned write task, which reports
    /// what the slot-handoff barrier is holding; released once, in
    /// [`Self::exit`].
    feed: SharedFeedAccount,
}

impl<'a> SessionDriver<'a> {
    /// Resolve the handshake assignment and stand up the driver.
    ///
    /// The assignment is resolved once, here, because the two facts it needs are
    /// reads of live shared state the machine is not allowed to touch: the
    /// current replication id for a `+CONTINUE`, and the live stream head for a
    /// `+FULLRESYNC`. That head is captured **before** any payload is produced
    /// and is the single value used for the reply, the payload trailer and the
    /// handoff replay, so the granted offset cannot name data the payload does
    /// not contain (FM-REPLICATION-004): writes landing after this point only
    /// *add* data, keeping the direction at `offset <= data`. Capturing it after
    /// the cut would invert that and silently lose writes.
    fn new(
        session: Arc<ReplicaSession>,
        stream: BoxedStream,
        sync_kind: SyncKind,
        handler: &'a Arc<PrimaryReplicationHandler>,
        feed: SharedFeedAccount,
    ) -> Self {
        let sync = match sync_kind {
            SyncKind::Partial { replay_from } => BeginSync::Partial {
                replication_id: handler.state.read().replication_id.clone(),
                replay_from,
            },
            SyncKind::Full { replication_id } => BeginSync::Full {
                replication_id,
                snapshot_offset: handler.offsets.current(),
            },
        };
        Self {
            session,
            handler,
            sync,
            stream: Some(stream),
            blobs: None,
            capture: None,
            feed,
        }
    }

    /// The plain-data facts the machine reads. Every one of them is a snapshot
    /// taken here, so `step` itself takes no lock and reads no clock.
    fn view(&self) -> SessionView {
        SessionView {
            phase: self.session.phase(),
            sync: self.sync.clone(),
            replica_id: self.session.id,
            data_dir: self.handler.data_dir.clone(),
            checkpoint_source: self.handler.rocks_store.is_some(),
            pre_checkpoint_drain: self.handler.pre_checkpoint_hook().is_some(),
            function_snapshot: self.handler.function_snapshot_hook().is_some(),
            live_snapshot_source: self.handler.live_snapshot_source().is_some(),
            checkpoint_owed: self.session.inner.read().sync_checkpoint_path.is_some(),
        }
    }

    /// Run the machine from the handshake to the end of the link.
    ///
    /// One pass of the loop is one transition: publish the phase, then perform
    /// its effects in order. Exactly one effect of every non-exit transition
    /// reports back — with the next event, or with the departure that ends the
    /// link — so the loop needs no schedule of its own.
    async fn drive(&mut self) -> io::Result<ReplicaDeparture> {
        let mut event = SessionEvent::Begin;
        loop {
            let Transition { phase, effects } = step(&self.view(), &event);
            self.session.commit_phase(phase);

            let mut next = None;
            for effect in effects {
                match self.perform(effect).await? {
                    Next::Continue => {}
                    Next::Event(reported) => next = Some(reported),
                    Next::Ended(departure) => return Ok(departure),
                }
            }

            match next {
                Some(reported) => event = reported,
                // A transition that neither reported an event nor ended the link
                // would spin. Only the exit transition is silent, and that one is
                // driven by `exit` rather than from here, so this is a machine
                // bug — surfaced as a dropped link rather than a hang.
                None => {
                    return Err(io::Error::other(SyncFailure::UnexpectedEvent.reason()));
                }
            }
        }
    }

    /// Run the exit transition. Every one of its effects is best-effort, so
    /// unlike [`Self::drive`] there is nothing here to propagate.
    async fn exit(&mut self, outcome: LinkOutcome) {
        let Transition { phase, effects } = step(&self.view(), &SessionEvent::Ended(outcome));
        self.session.commit_phase(phase);
        // The single release point (FM-REPLICATION-069). Every way this session
        // can end — a departure out of `drive`, or a `?` out of any effect —
        // arrives here, so nothing the feed held stays charged after the link.
        self.feed.release();
        for effect in effects {
            match effect {
                Effect::CleanCheckpointDir => self.clean_checkpoint_dir().await,
                Effect::RecordDeparture(departure) => {
                    self.handler.tracker.record_streaming_departure(departure);
                }
                Effect::Unregister => self.handler.tracker.unregister_replica(self.session.id),
                Effect::LogDisconnect => tracing::info!(
                    replica_id = self.session.id,
                    addr = %self.session.address,
                    "Replica disconnected"
                ),
                // The exit transition returns these four and nothing else.
                other => debug_assert!(false, "not an exit effect: {other:?}"),
            }
        }
    }

    async fn perform(&mut self, effect: Effect) -> io::Result<Next> {
        match effect {
            Effect::PublishFunctionSnapshot => {
                // Process-wide state that is not in the keyspace — the
                // function-library registry — rides the frame lane rather than
                // the payload (see [`crate::primary::FunctionSnapshotHook`]).
                // The machine emits this only after `run` captured
                // `snapshot_offset`, so the frame falls inside the
                // `(snapshot_offset, current]` window the handoff replays; one
                // broadcast before the capture would fall inside the payload's
                // own range and be skipped.
                if let Some(hook) = self.handler.function_snapshot_hook() {
                    hook(self.handler);
                }
                Ok(Next::Continue)
            }

            Effect::SendReply(reply) => {
                self.stream()?.write_all(reply.render().as_bytes()).await?;
                Ok(Next::Event(SessionEvent::ReplySent))
            }

            Effect::DrainBeforeCheckpoint => {
                // The checkpoint is a snapshot of what RocksDB *holds*, and a
                // write is acknowledged as soon as it is staged in its shard's
                // WAL flush-engine (default durability commits to RocksDB on a
                // later size/timeout trigger). Cut without draining those
                // engines, the checkpoint silently omits the primary's most
                // recent writes — and for a full resync that is unrecoverable:
                // with no replica attached when they were made they were never
                // broadcast, so there is no backlog tail to replay them from and
                // the replica is missing them forever. So: drain first, cut
                // second. Same contract the snapshot coordinator's pre-snapshot
                // hook honours (issue 13).
                //
                // The drain also reports what the checkpoint will already
                // contain (its per-shard coverage watermarks) and leaves the
                // flush engines held until the cut, so a write landing in
                // between cannot enter the payload *above* the offset the
                // replica was granted and then be replayed a second time from
                // the backlog.
                let outcome = match self.handler.pre_checkpoint_hook() {
                    Some(drain) => match drain().await {
                        Ok(capture) => {
                            self.capture = Some(capture);
                            StepOutcome::Ok
                        }
                        Err(e) => StepOutcome::Failed(e.to_string()),
                    },
                    // The machine only asks for a drain when the view said one
                    // was wired; an unwiring in between is a drain that had
                    // nothing to do — and a payload that claims no coverage.
                    None => StepOutcome::Ok,
                };
                Ok(Next::Event(SessionEvent::Drained(outcome)))
            }

            Effect::CutCheckpoint { path } => {
                let Some(rocks) = self.handler.rocks_store.as_ref().cloned() else {
                    return Err(io::Error::other(
                        "checkpoint source disappeared between the decision and the cut",
                    ));
                };
                let cut = tokio::task::spawn_blocking(move || rocks.create_checkpoint(&path))
                    .await
                    .map_err(io::Error::other);
                // The cut is done, so the flush engines go free — on success and
                // on failure alike. Holding past this point would stall this
                // node's `sync`-durability write acks for nothing. The release
                // also reports the shards whose hold lapsed before we got here:
                // their watermarks no longer describe the artefact that was
                // just cut, and there is no sound weaker claim to ship in their
                // place, so the sync is abandoned (FM-REPLICATION-066).
                let breached = match self.capture.as_mut() {
                    Some(capture) => capture.release_hold(),
                    None => Vec::new(),
                };
                // A join failure is the runtime going away under the session,
                // not a checkpoint verdict, so it leaves the machine entirely —
                // after the release above.
                let cut = cut?;
                Ok(Next::Event(match cut {
                    // A cut that failed wins over a breach: there is no payload
                    // and no directory, so the breach has nothing left to say.
                    Err(e) => SessionEvent::CheckpointCut(StepOutcome::Failed(e.to_string())),
                    Ok(()) if !breached.is_empty() => {
                        SessionEvent::CoverageBreached(format!("shard(s) {breached:?}"))
                    }
                    Ok(()) => SessionEvent::CheckpointCut(StepOutcome::Ok),
                }))
            }

            Effect::OwnCheckpointDir { path } => {
                self.session.inner.write().sync_checkpoint_path = Some(path);
                Ok(Next::Continue)
            }

            Effect::SendCheckpoint {
                path,
                replication_id,
                offset,
            } => {
                self.session.inner.write().sync_started_at = Some(clock::now());
                let session = self.session.clone();
                let handler = self.handler;
                let stream = self
                    .stream
                    .as_mut()
                    .ok_or_else(|| io::Error::other("session stream already handed off"))?;
                // The coverage the drain reported. Reaching here at all means
                // every hold kept it true — a lapsed one abandoned the sync at
                // the cut. A session that never drained claims nothing, which is
                // the pre-existing behaviour: replay the whole window.
                let coverage = self
                    .capture
                    .as_ref()
                    .map(|c| c.coverage.clone())
                    .unwrap_or_default();
                session
                    .stream_checkpoint(stream, handler, &path, &replication_id, offset, coverage)
                    .await?;
                Ok(Next::Event(SessionEvent::PayloadSent))
            }

            Effect::ExportLiveDataset => {
                let Some(source) = self.handler.live_snapshot_source() else {
                    return Err(io::Error::other(SyncFailure::NoLiveSnapshotSource.reason()));
                };
                let (blobs, coverage) = source().await?;
                // No hold on this path: each blob is serialized inside its own
                // shard's task, so its watermark is exact by construction.
                self.capture = Some(FullSyncCapture {
                    coverage,
                    hold: None,
                });
                // Published before the phase moves on, exactly as they were when
                // this ran inline, so `progress_percent` never reports the
                // previous sync's totals against this one's transfer.
                self.session.inner.write().sync_total_bytes =
                    blobs.iter().map(|b| b.len() as u64).sum();
                self.session
                    .sync_bytes_transferred
                    .store(0, Ordering::Release);
                self.blobs = Some(blobs);
                Ok(Next::Event(SessionEvent::DatasetExported))
            }

            Effect::SendLiveDataset {
                replication_id,
                offset,
            } => {
                let blobs = self
                    .blobs
                    .take()
                    .ok_or_else(|| io::Error::other("live dataset was never exported"))?;
                let session = self.session.clone();
                let handler = self.handler;
                let stream = self
                    .stream
                    .as_mut()
                    .ok_or_else(|| io::Error::other("session stream already handed off"))?;
                let coverage = self
                    .capture
                    .as_ref()
                    .map(|c| c.coverage.clone())
                    .unwrap_or_default();
                session
                    .stream_live_dataset(stream, handler, &blobs, &replication_id, offset, coverage)
                    .await?;
                Ok(Next::Event(SessionEvent::PayloadSent))
            }

            Effect::LogFullResyncComplete { offset } => {
                tracing::info!(
                    replica_id = self.session.id,
                    addr = %self.session.address,
                    offset,
                    "Completed FULLRESYNC"
                );
                Ok(Next::Continue)
            }

            Effect::FailSync { failure, cause } => {
                match &cause {
                    Some(cause) => tracing::error!(error = %cause, "{}", failure.log_message()),
                    None => tracing::error!("{}", failure.log_message()),
                }
                // The one alertable signal for a cut that is chronically losing
                // the race against `FULL_SYNC_HOLD` (FM-REPLICATION-066).
                // Counted here rather than at the breach report, because this
                // is where the abort is *taken*: one count per abandoned sync,
                // whatever the release said about how many shards lapsed.
                // Without it a slow cut retries forever behind nothing but
                // `WARN`/`ERROR` lines, which no alert rule reads.
                if failure == SyncFailure::CoverageHoldBreached {
                    self.handler.tracker.record_full_sync_hold_breach();
                }
                Err(io::Error::other(match cause {
                    Some(cause) => format!("{}: {cause}", failure.reason()),
                    None => failure.reason().to_string(),
                }))
            }

            Effect::ClearDeparture => {
                // A new streaming generation begins here, so the departure
                // recorded by the *previous* one stops answering for the replica
                // set (FM-REPLICATION-062). Without this, a predecessor's
                // graceful departure would still be on record when this
                // session's link dies and the self-fence would read it as this
                // replica having left cleanly.
                self.handler.tracker.clear_streaming_departure();
                Ok(Next::Continue)
            }

            Effect::Stream {
                replay_from,
                resume,
            } => {
                let stream = self
                    .stream
                    .take()
                    .ok_or_else(|| io::Error::other("session stream already handed off"))?;
                let departure = self
                    .session
                    .clone()
                    .start_streaming(stream, self.handler, replay_from, resume)
                    .await?;
                Ok(Next::Ended(departure))
            }

            // Exit-only effects; `exit` performs these.
            Effect::CleanCheckpointDir
            | Effect::RecordDeparture(_)
            | Effect::Unregister
            | Effect::LogDisconnect => {
                debug_assert!(false, "exit effect reached the sync loop");
                Ok(Next::Continue)
            }
        }
    }

    /// The socket, while the handshake still owns it.
    fn stream(&mut self) -> io::Result<&mut BoxedStream> {
        self.stream
            .as_mut()
            .ok_or_else(|| io::Error::other("session stream already handed off"))
    }

    /// Best-effort delete of the directory this session staged. Only owed after
    /// a cut that succeeded, so `NotFound` should not occur in practice.
    async fn clean_checkpoint_dir(&mut self) {
        let path = self.session.inner.read().sync_checkpoint_path.clone();
        if let Some(p) = path
            && let Err(e) = fs::remove_dir_all(&p).await
        {
            tracing::warn!(
                checkpoint_path = %p.display(),
                error = %e,
                "Failed to clean up checkpoint directory"
            );
        }
    }
}

/// Outcome of a single frame write to a replica.
enum Forward {
    /// The frame was written; keep streaming.
    Continue,
    /// The session must end (write timeout or I/O error); the caller stops
    /// streaming.
    Break,
}

/// Write one encoded frame to the replica, honoring the optional write timeout.
///
/// This is the single home for "send a frame to a replica": with the dead
/// per-replica frame channel removed there is one live frame source, so the
/// write+timeout+error path is defined exactly once here instead of being
/// duplicated across two `select!` arms.
/// Full-sync transfer rate in MiB/s, as the completion log reports it.
///
/// A pure function of the two numbers rather than an expression inside the log
/// call: an unmeasurably fast transfer (`elapsed == 0`) must report `0.0`
/// instead of dividing by zero and telling an operator the link ran at
/// `inf MiB/s`, and that boundary is only assertable if the arithmetic can be
/// called without standing up a checkpoint stream.
fn transfer_rate_mbps(total_bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs > 0.0 {
        (total_bytes as f64 / 1024.0 / 1024.0) / secs
    } else {
        0.0
    }
}

/// The per-frame write deadline for a streaming session, or `None` when the
/// operator disabled it.
///
/// `replication-write-timeout-ms 0` is the documented "never time out a write"
/// setting, so the boundary is strict: `1` is a real (if brutal) deadline and
/// `0` is no deadline at all. Extracted from the streaming loop because the
/// difference between the two readings of `0` — no timeout, versus an already
/// expired one that drops every replica link on its first frame — is otherwise
/// only visible through a live session.
fn frame_write_timeout(write_timeout_ms: u64) -> Option<Duration> {
    if write_timeout_ms > 0 {
        Some(Duration::from_millis(write_timeout_ms))
    } else {
        None
    }
}

/// Classify what the WAL broadcast handed the write task, for
/// [`FeedSequencer::step`].
///
/// The logging is the driver's: the sequencer is told *that* the receiver
/// lagged, not by how much.
fn classify_feed_frame(
    received: Result<ReplicationFrame, broadcast::error::RecvError>,
    replica_id: u64,
) -> FeedInput {
    match received {
        Ok(frame) => FeedInput::Received(frame),
        // The primary's own broadcaster went away (this node is shutting down
        // or ending its primary stint): the link is being closed from this
        // side, not lost.
        Err(broadcast::error::RecvError::Closed) => FeedInput::SourceClosed,
        Err(broadcast::error::RecvError::Lagged(n)) => {
            tracing::warn!(
                replica_id,
                lagged = n,
                "Replica lagged in WAL stream, disconnecting for resync"
            );
            FeedInput::SourceLagged
        }
    }
}

/// Put one frame the sequencer released on the wire, and report back the two
/// facts it decides on: whether the frame landed, and whether the lag policy
/// has had enough of this replica.
async fn send_feed_frame(
    write_half: &mut (impl AsyncWrite + Unpin),
    frame: &ReplicationFrame,
    timeout: Option<Duration>,
    replica_id: u64,
    tracker: &ReplicationTrackerImpl,
    lag_policy: &mut LagPolicy,
) -> FeedInput {
    let encoded = match frame.encode() {
        Ok(encoded) => encoded,
        Err(e) => {
            // Unreplicable, and no reconnect can change that — the same frame
            // comes back from the backlog. Drop the link loudly rather than put
            // a frame with a wrapped length prefix on the wire.
            tracing::error!(
                replica_id,
                sequence = frame.sequence,
                error = %e,
                "Replication frame exceeds the link's frame ceiling; dropping the replica link"
            );
            return FeedInput::SendFailed;
        }
    };
    match forward_frame(write_half, &encoded, timeout, replica_id).await {
        // The frame lane (hardening issue 29) — only bytes actually written to
        // the wire, never bytes merely encoded: a `Break` means the write did
        // not land, or landed partially, and must not be counted as sent.
        Forward::Continue => tracker
            .net_bytes_handle()
            .record_output(encoded.len() as u64),
        Forward::Break => return FeedInput::SendFailed,
    }
    let lag_breached = match lag_policy.should_disconnect(tracker, replica_id) {
        Some(breach) => {
            tracing::warn!(
                replica_id,
                byte_exceeded = breach.byte_exceeded,
                time_exceeded = breach.time_exceeded,
                "Replica exceeded lag threshold, disconnecting for FULLRESYNC"
            );
            tracker.record_lag_disconnect(replica_id);
            true
        }
        None => false,
    };
    FeedInput::Sent { lag_breached }
}

async fn forward_frame(
    write_half: &mut (impl AsyncWrite + Unpin),
    encoded: &[u8],
    timeout: Option<Duration>,
    replica_id: u64,
) -> Forward {
    match timeout {
        Some(dur) => match tokio::time::timeout(dur, write_half.write_all(encoded)).await {
            Ok(Ok(())) => Forward::Continue,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "Error writing to replica");
                Forward::Break
            }
            Err(_) => {
                tracing::warn!(
                    replica_id,
                    timeout_ms = dur.as_millis() as u64,
                    "Write to replica timed out, disconnecting"
                );
                Forward::Break
            }
        },
        None => match write_half.write_all(encoded).await {
            Ok(()) => Forward::Continue,
            Err(e) => {
                tracing::warn!(error = %e, "Error writing to replica");
                Forward::Break
            }
        },
    }
}

/// Proactive lag-disconnect policy for one streaming session.
///
/// Owns the forwarded-frame counter and the threshold comparison so the
/// streaming loop consults "when do we proactively disconnect a lagging
/// replica" as a value rather than inlining it. Checks fire every
/// [`LAG_CHECK_INTERVAL`] frames (cadence unchanged by the extraction).
struct LagPolicy {
    /// Live byte/time thresholds, shared with the owning
    /// [`PrimaryReplicationHandler`]. Re-read on every evaluation, never copied
    /// into the session, so a `CONFIG SET` retunes this already-streaming
    /// session instead of only the next one.
    thresholds: Arc<LagThresholds>,
    /// Cooldown after a proactive disconnect before allowing another.
    cooldown: Duration,
    /// Frames forwarded so far (the check cadence counter).
    frames: u64,
}

impl LagPolicy {
    fn new(thresholds: Arc<LagThresholds>, cooldown: Duration) -> Self {
        Self {
            thresholds,
            cooldown,
            frames: 0,
        }
    }

    /// Count one forwarded frame and, every [`LAG_CHECK_INTERVAL`] frames,
    /// decide whether this replica has exceeded a threshold and is out of
    /// cooldown. Returns `Some(LagBreach)` naming which threshold(s) fired when
    /// a proactive disconnect is warranted, or `None` otherwise. On a `Some`
    /// return the caller records the disconnect (logging the breach detail) and
    /// breaks the streaming loop.
    ///
    /// Both thresholds are loaded from the shared [`LagThresholds`] here, at the
    /// point of use. A policy that is disabled at this instant neither counts
    /// the frame nor fires — so arming a threshold mid-session starts the check
    /// cadence from the next forwarded frame.
    fn should_disconnect(
        &mut self,
        tracker: &ReplicationTrackerImpl,
        id: u64,
    ) -> Option<LagBreach> {
        let threshold_bytes = self.thresholds.threshold_bytes();
        let threshold_secs = self.thresholds.threshold_secs();
        if threshold_bytes == 0 && threshold_secs == 0 {
            return None;
        }
        self.frames += 1;
        if !self.frames.is_multiple_of(LAG_CHECK_INTERVAL) {
            return None;
        }
        let byte_exceeded = threshold_bytes > 0
            && tracker
                .replica_lag(id)
                .is_some_and(|lag| lag >= threshold_bytes);
        let time_exceeded = threshold_secs > 0
            && tracker
                .replica_lag_secs(id)
                .is_some_and(|secs| secs >= threshold_secs as f64);
        if (byte_exceeded || time_exceeded) && !tracker.is_in_lag_cooldown(id, self.cooldown) {
            Some(LagBreach {
                byte_exceeded,
                time_exceeded,
            })
        } else {
            None
        }
    }
}

/// Which lag threshold(s) a proactive-disconnect decision tripped. Returned by
/// [`LagPolicy::should_disconnect`] so the disconnect warning can name the
/// specific threshold that fired (byte-lag vs. time-since-last-ACK) rather than
/// logging a bare boolean.
#[derive(Debug, Clone, Copy)]
struct LagBreach {
    /// The byte-lag threshold was met or exceeded.
    byte_exceeded: bool,
    /// The time-since-last-ACK threshold was met or exceeded.
    time_exceeded: bool,
}

#[cfg(test)]
mod tests {
    //! `ReplicaSession::run` lifecycle tests.
    //!
    //! These tests drive `run()` end-to-end against in-memory streams so we can
    //! verify that the single exit handler runs cleanup regardless of where in
    //! the lifecycle the connection drops. The `mid_fullsync_drop` case is the
    //! regression test for the leak that motivated this refactor: under the
    //! pre-refactor code, a `?`-propagated error from inside `handle_full_sync`
    //! left the replica registered as `Syncing` until process restart.
    use super::*;
    use crate::feed_account::testing::RecordingFeedAccount;
    use crate::frame::serialize_command_to_resp;
    use crate::net_bytes::NetByteCountersSnapshot;
    use crate::primary::BacklogConfig;
    use crate::primary::{LagThresholdConfig, PrimaryReplicationHandler};
    use crate::state::ReplicationState;
    use crate::sync_counters::SyncCountersSnapshot;
    use crate::tracker::ReplicationTrackerImpl;
    use crate::version_compat::PRIMARY_VERSION;
    use bytes::Bytes;
    use frogdb_persistence::{RocksConfig, RocksStore};
    use frogdb_types::ReplicationTracker;
    use std::net::SocketAddr;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    fn addr() -> SocketAddr {
        "127.0.0.1:9001".parse().unwrap()
    }

    // ------------------------------------------------------------------
    // The replica handshake: what a replica announces about itself.
    // ------------------------------------------------------------------

    fn arg(text: &str) -> Bytes {
        Bytes::from(text.to_string())
    }

    fn parse_announcement(args: &[&str]) -> Result<Option<AnnouncedOption>, AnnouncementError> {
        let args: Vec<Bytes> = args.iter().map(|a| arg(a)).collect();
        AnnouncedOption::parse(&args)
    }

    // FM-REPLICATION-049
    /// A replica announces its identity before `PSYNC` exists to carry it, so
    /// the announcement is folded on the connection and applied when the
    /// session is constructed. What the replica said is what the session
    /// reports — the half that had no writer at all before (issue 16).
    #[test]
    fn an_announced_session_reports_the_port_and_capabilities_it_was_told() {
        let mut announcement = ReplicaAnnouncement::default();
        announcement.absorb(
            parse_announcement(&["listening-port", "7001"])
                .expect("a well-formed listening-port parses")
                .expect("listening-port describes the replica"),
        );
        announcement.absorb(
            parse_announcement(&["capa", "eof", "psync2"])
                .expect("capa never fails")
                .expect("capa describes the replica"),
        );

        let session = ReplicaSession::announced(1, addr(), announcement);
        let info = session.snapshot();
        assert_eq!(
            info.listening_port, 7001,
            "the announced port must survive into the session INFO/ROLE read"
        );
        assert!(info.capabilities.eof, "eof was announced");
        assert!(info.capabilities.psync2, "psync2 was announced");
    }

    // FM-REPLICATION-049
    /// A capability this primary has never heard of is dropped, not refused:
    /// the announcement is still recorded (without it) and the replica carries
    /// on to `PSYNC`. The complement of FM-REPLICATION-018's "an unknown
    /// `REPLCONF` still answers `+OK`" — here the *recording* also survives.
    #[test]
    fn an_unknown_capability_is_recorded_as_absent_not_rejected() {
        let option = parse_announcement(&["capa", "eof", "quantum-psync"])
            .expect("an unknown capability must not fail the option")
            .expect("capa describes the replica");
        let mut announcement = ReplicaAnnouncement::default();
        announcement.absorb(option);

        assert!(announcement.capabilities.eof, "the known half is kept");
        assert!(
            !announcement.capabilities.psync2,
            "an unknown name must not set an unrelated capability"
        );

        let info = ReplicaSession::announced(1, addr(), announcement).snapshot();
        assert_eq!(
            info.listening_port, 0,
            "a capa-only handshake announces no port"
        );
        assert!(info.capabilities.eof);
    }

    // FM-REPLICATION-049
    /// `REPLCONF` token matching is case-insensitive everywhere else in this
    /// dispatch path (subcommands, `ACK`/`GETACK`), so a `capa` token must be
    /// too: `REPLCONF capa EOF` was silently recorded as no capabilities
    /// before `parse_capa` matched exact-lowercase strings only. The "unknown
    /// token is dropped, handshake still succeeds" behaviour of
    /// FM-REPLICATION-018 is unaffected — mixing in an unrecognized token
    /// alongside a differently-cased known one still keeps only the known
    /// half.
    #[test]
    fn parse_capa_matches_case_insensitively() {
        let caps = ReplicaCapabilities::parse_capa(&["EOF", "PSYNC2"]);
        assert!(caps.eof, "uppercase EOF must still set the capability");
        assert!(
            caps.psync2,
            "uppercase PSYNC2 must still set the capability"
        );

        let mixed = ReplicaCapabilities::parse_capa(&["Eof", "quantum-Psync"]);
        assert!(mixed.eof, "mixed-case Eof must still set the capability");
        assert!(
            !mixed.psync2,
            "an unrecognized token must not set an unrelated capability"
        );
    }

    // FM-REPLICATION-049
    /// Later options of the same kind win, and the two kinds are independent:
    /// re-announcing a port must not clear the capabilities already recorded.
    #[test]
    fn a_repeated_option_overwrites_only_its_own_kind() {
        let mut announcement = ReplicaAnnouncement::default();
        announcement.absorb(AnnouncedOption::Capabilities(ReplicaCapabilities {
            eof: true,
            psync2: true,
        }));
        announcement.absorb(AnnouncedOption::ListeningPort(7001));
        announcement.absorb(AnnouncedOption::ListeningPort(7002));

        assert_eq!(announcement.listening_port, 7002, "the later port wins");
        assert!(
            announcement.capabilities.psync2,
            "a port announcement must not clear capabilities"
        );
    }

    // FM-REPLICATION-049
    /// The third announcing option. The replica has always sent `REPLCONF
    /// frogdb-version` on every handshake; the primary parsed it, logged it and
    /// dropped it, so `ReplicaInfo::replica_version` was `None` for every
    /// replica that ever connected (issue 22). It rides the same announcement
    /// as the other two.
    #[test]
    fn a_replica_that_announced_its_version_is_recorded_with_it() {
        let mut announcement = ReplicaAnnouncement::default();
        announcement.absorb(
            parse_announcement(&["frogdb-version", "0.1.0"])
                .expect("a well-formed version parses")
                .expect("frogdb-version describes the replica"),
        );
        assert_eq!(announcement.version.as_deref(), Some("0.1.0"));

        let session = ReplicaSession::announced(1, addr(), announcement);
        assert_eq!(session.replica_version().as_deref(), Some("0.1.0"));
        assert_eq!(
            session.snapshot().replica_version.as_deref(),
            Some("0.1.0"),
            "the announced version must survive into the ReplicaInfo consumers read"
        );
    }

    // FM-REPLICATION-049
    /// A peer that never announced a version is recorded as **unknown**, and
    /// unknown must stay distinguishable from any real version: it is what a
    /// replica predating the option and any non-FrogDB peer both produce, so a
    /// gate that reads it as satisfied fails open.
    #[test]
    fn a_replica_that_announced_no_version_is_recorded_as_unknown() {
        let mut announcement = ReplicaAnnouncement::default();
        announcement.absorb(AnnouncedOption::ListeningPort(7001));

        let session = ReplicaSession::announced(1, addr(), announcement);
        assert_eq!(
            session.replica_version(),
            None,
            "no announcement means unknown, never a default version string"
        );
        assert_eq!(session.snapshot().replica_version, None);
    }

    // FM-REPLICATION-049
    /// The overwrite-only-its-own-kind rule extends to the third option: a
    /// re-announced version must not clear the port or the capabilities, and a
    /// re-announced port must not clear the version.
    #[test]
    fn re_announcing_the_version_does_not_clear_the_port_or_capabilities() {
        let mut announcement = ReplicaAnnouncement::default();
        announcement.absorb(AnnouncedOption::ListeningPort(7001));
        announcement.absorb(AnnouncedOption::Capabilities(ReplicaCapabilities {
            eof: true,
            psync2: true,
        }));
        announcement.absorb(AnnouncedOption::Version("0.1.0".to_string()));
        announcement.absorb(AnnouncedOption::Version("0.2.0".to_string()));

        assert_eq!(
            announcement.version.as_deref(),
            Some("0.2.0"),
            "the later version wins"
        );
        assert_eq!(announcement.listening_port, 7001);
        assert!(announcement.capabilities.eof && announcement.capabilities.psync2);

        announcement.absorb(AnnouncedOption::ListeningPort(7002));
        assert_eq!(
            announcement.version.as_deref(),
            Some("0.2.0"),
            "a port announcement must not clear the version"
        );
    }

    // FM-REPLICATION-049
    /// A `frogdb-version` that cannot be read is refused at the option, like an
    /// unreadable port — the connection stays open and the replica may carry on
    /// to `PSYNC` (FM-REPLICATION-018).
    #[test]
    fn an_unreadable_frogdb_version_is_refused() {
        assert_eq!(
            parse_announcement(&["frogdb-version"]),
            Err(AnnouncementError::MissingVersion)
        );
        assert_eq!(
            AnnouncedOption::parse(&[
                Bytes::from_static(b"frogdb-version"),
                Bytes::from_static(&[0xFF, 0xFE]),
            ]),
            Err(AnnouncementError::InvalidVersionEncoding)
        );
        // Anything that *is* text is recorded verbatim: this records what the
        // peer said, and a peer can say anything.
        assert_eq!(
            parse_announcement(&["FROGDB-VERSION", "not-a-semver"]).unwrap(),
            Some(AnnouncedOption::Version("not-a-semver".to_string())),
            "the subcommand is case-insensitive and the value is not validated"
        );
    }

    /// The options that act on the *link* rather than describing the replica
    /// are not announcements: they fall through to the ordinary `REPLCONF`
    /// executor, which is what keeps `ACK`/`GETACK` working unchanged.
    #[test]
    fn link_options_are_not_announcements() {
        for args in [
            vec!["ack", "12345"],
            vec!["getack", "*"],
            vec!["ip-address", "10.0.0.1"],
            vec!["some-future-option", "x"],
            vec![],
        ] {
            assert_eq!(
                parse_announcement(&args).expect("non-announcing options never error"),
                None,
                "{args:?} must not be treated as an announcement"
            );
        }
    }

    /// A `listening-port` that cannot be read is refused at the option, not at
    /// the connection: the caller turns this into an error reply and the
    /// replica may still carry on.
    #[test]
    fn an_unreadable_listening_port_is_refused() {
        assert_eq!(
            parse_announcement(&["listening-port"]),
            Err(AnnouncementError::MissingPort)
        );
        assert_eq!(
            parse_announcement(&["listening-port", "not-a-port"]),
            Err(AnnouncementError::InvalidPort)
        );
        assert_eq!(
            parse_announcement(&["listening-port", "70000"]),
            Err(AnnouncementError::InvalidPort),
            "a value past u16 is not a port"
        );
        // Not valid UTF-8 at all — a distinct failure from "valid text that
        // isn't a number", with its own wire text (`announcement_error` in
        // `server::commands::replication` maps the two differently).
        assert_eq!(
            AnnouncedOption::parse(&[
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(&[0xFF, 0xFE]),
            ]),
            Err(AnnouncementError::InvalidPortEncoding),
            "a non-UTF-8 port argument cannot even reach the number parser"
        );
    }

    /// Subcommand matching is case-insensitive, as every other `REPLCONF`
    /// subcommand is — a replica sending `LISTENING-PORT` must be recorded.
    #[test]
    fn announcement_subcommands_are_case_insensitive() {
        assert_eq!(
            parse_announcement(&["LISTENING-PORT", "7001"]).unwrap(),
            Some(AnnouncedOption::ListeningPort(7001))
        );
    }

    // FM-REPLICATION-049
    /// `lag` is Redis's seconds-since-last-ACK, and INFO must read the same
    /// measure the proactive lag-disconnect policy acts on rather than a
    /// literal (GAP-3).
    #[test]
    fn lag_secs_is_the_age_of_the_last_ack() {
        let session = ReplicaSession::new(1, addr());
        assert_eq!(
            session.snapshot().lag_secs(),
            0,
            "a session that just ACKed lags by zero whole seconds"
        );

        let aged = Instant::now() - Duration::from_millis(2_500);
        assert_eq!(
            ack_age_secs(aged).round() as u64,
            3,
            "the shared measure keeps sub-second precision"
        );
        let info = ReplicaInfo {
            last_ack_time: aged,
            ..session.snapshot()
        };
        assert_eq!(
            info.lag_secs(),
            2,
            "INFO truncates to Redis's whole-second field"
        );
    }

    /// Wire a live-snapshot source returning `blobs`, standing in for the
    /// server crate's shard export. A persistence-disabled primary needs one to
    /// serve a full resync at all (issue 67).
    fn with_live_dataset(handler: &Arc<PrimaryReplicationHandler>, blobs: Vec<Vec<u8>>) {
        handler.set_live_snapshot_source(Arc::new(move || {
            let blobs = blobs.clone();
            Box::pin(async move { Ok((blobs, ShardCoverage::none())) })
        }));
    }

    /// One dataset blob holding `entries`, in the framing the shard workers
    /// produce and the replica's installer consumes.
    fn dataset_blob(entries: &[(&str, &str)]) -> Vec<u8> {
        use frogdb_types::types::{KeyMetadata, Value};
        let mut blob = Vec::new();
        for (key, val) in entries {
            let value = Value::string(Bytes::from(val.to_string()));
            let metadata = KeyMetadata::new(value.memory_size());
            frogdb_persistence::append_entry(&mut blob, key.as_bytes(), &value, &metadata);
        }
        blob
    }

    /// Read a whole live-dataset envelope off the wire, returning the blob
    /// bodies and the trailing metadata frame.
    ///
    /// Byte-at-a-time on the raw stream rather than through a `BufReader`, so
    /// the caller can keep decoding replication frames from the same client
    /// afterwards without losing buffered bytes to a reader it dropped.
    async fn drain_live_dataset(
        client: &mut tokio::io::DuplexStream,
    ) -> (Vec<Vec<u8>>, FullSyncMetadata) {
        async fn dollar_len(client: &mut tokio::io::DuplexStream) -> usize {
            read_response_line(client)
                .await
                .trim()
                .trim_start_matches('$')
                .parse()
                .unwrap()
        }

        let marker = read_response_line(client).await;
        assert_eq!(
            marker.trim(),
            "$FROGDB_SNAPSHOT",
            "a persistence-disabled primary must announce a dataset envelope"
        );
        let count: usize = read_response_line(client).await.trim().parse().unwrap();

        let mut blobs = Vec::with_capacity(count);
        for _ in 0..count {
            // `$<name_len>\r\n<name>\r\n$<size>\r\n<size bytes>`.
            let name_len = dollar_len(client).await;
            let mut name = vec![0u8; name_len + 2];
            client.read_exact(&mut name).await.unwrap();
            let size = dollar_len(client).await;
            let mut blob = vec![0u8; size];
            client.read_exact(&mut blob).await.unwrap();
            blobs.push(blob);
        }

        let meta_len = dollar_len(client).await;
        let mut meta = vec![0u8; meta_len + 2];
        client.read_exact(&mut meta).await.unwrap();
        let metadata = FullSyncMetadata::from_bytes(&meta[..meta_len]).expect("metadata parses");
        (blobs, metadata)
    }

    fn make_handler(
        tracker: Arc<ReplicationTrackerImpl>,
        rocks: Option<Arc<RocksStore>>,
        data_dir: PathBuf,
    ) -> Arc<PrimaryReplicationHandler> {
        let state_path = data_dir.join("replication_state.json");
        let identity =
            crate::identity::ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
        Arc::new(PrimaryReplicationHandler::new(
            identity,
            state_path,
            tracker,
            rocks,
            data_dir,
            LagThresholdConfig {
                threshold_bytes: 0,
                threshold_secs: 0,
                cooldown: Duration::from_secs(0),
            },
            BacklogConfig {
                enabled: false,
                max_entries: 0,
                max_bytes: 0,
                ttl_secs: 0,
            },
            0,
            crate::feed_gate::ReplicaFeedGate::open(),
        ))
    }

    /// Like [`make_handler`] but with the replication backlog enabled, so the
    /// partial-sync replay path has frames to serve.
    fn make_handler_with_backlog(
        tracker: Arc<ReplicationTrackerImpl>,
        rocks: Option<Arc<RocksStore>>,
        data_dir: PathBuf,
    ) -> Arc<PrimaryReplicationHandler> {
        make_handler_with_backlog_entries(tracker, rocks, data_dir, 10_000)
    }

    /// Same, with the backlog's entry cap under the test's control — a cap of a
    /// few entries makes eviction happen on demand instead of after 10 000
    /// writes.
    fn make_handler_with_backlog_entries(
        tracker: Arc<ReplicationTrackerImpl>,
        rocks: Option<Arc<RocksStore>>,
        data_dir: PathBuf,
        max_entries: usize,
    ) -> Arc<PrimaryReplicationHandler> {
        let state_path = data_dir.join("replication_state.json");
        let identity =
            crate::identity::ReplicationIdentity::adopting(ReplicationState::new(), &tracker);
        Arc::new(PrimaryReplicationHandler::new(
            identity,
            state_path,
            tracker,
            rocks,
            data_dir,
            LagThresholdConfig {
                threshold_bytes: 0,
                threshold_secs: 0,
                cooldown: Duration::from_secs(0),
            },
            BacklogConfig {
                enabled: true,
                max_entries,
                max_bytes: 64 * 1024 * 1024,
                ttl_secs: 0,
            },
            0,
            crate::feed_gate::ReplicaFeedGate::open(),
        ))
    }

    /// Read the leading `+CONTINUE` line, then decode exactly `n` replication
    /// frames off the same stream.
    async fn read_continue_then_frames(
        client: &mut tokio::io::DuplexStream,
        n: usize,
    ) -> Vec<ReplicationFrame> {
        use crate::frame::ReplicationFrameCodec;
        use tokio_util::codec::Decoder;

        // Leading simple-string line.
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let read = client.read(&mut byte).await.unwrap();
            assert!(read > 0, "stream closed before +CONTINUE line");
            line.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        let line = String::from_utf8(line).unwrap();
        assert!(
            line.starts_with("+CONTINUE"),
            "expected +CONTINUE, got {line:?}"
        );

        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        while frames.len() < n {
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(frame);
                if frames.len() == n {
                    break;
                }
            }
            if frames.len() == n {
                break;
            }
            let read = client.read_buf(&mut buf).await.unwrap();
            assert!(read > 0, "stream closed before {n} frames arrived");
        }
        frames
    }

    /// Decode exactly `n` replication frames off the stream.
    async fn decode_n_frames(
        client: &mut tokio::io::DuplexStream,
        n: usize,
    ) -> Vec<ReplicationFrame> {
        use crate::frame::ReplicationFrameCodec;
        use tokio_util::codec::Decoder;

        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        while frames.len() < n {
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(frame);
                if frames.len() == n {
                    break;
                }
            }
            if frames.len() == n {
                break;
            }
            let read = client.read_buf(&mut buf).await.unwrap();
            assert!(read > 0, "stream closed before {n} frames arrived");
        }
        frames
    }

    /// F1: writes broadcast during the full-sync handoff (after the snapshot
    /// offset is captured, before the live stream is joined) must NOT be lost.
    ///
    /// A tiny duplex buffer blocks the session inside `stream_live_dataset`,
    /// opening a deterministic window: the test reads the FULLRESYNC line
    /// (proving the snapshot offset is captured), then broadcasts commands while
    /// the session is blocked, then drains the dataset. When the session reaches
    /// `start_streaming` it must replay those commands from the backlog. Under
    /// the pre-fix code (subscribe-only, no replay) they would have been dropped
    /// and this test would hang waiting for frames that never arrive.
    // FM-REPLICATION-004
    #[tokio::test]
    async fn full_sync_replays_writes_made_during_handoff() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // No rocks store -> live-dataset full sync; backlog enabled so the
        // handoff window can be replayed.
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        with_live_dataset(&handler, vec![dataset_blob(&[("seed", "v")])]);
        let repl_id = handler.state.read().replication_id.clone();

        // Tiny buffer forces the session to block writing the dataset, giving us
        // a window to broadcast "during" the handoff.
        let (mut client, server) = tokio::io::duplex(32);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        // 1. FULLRESYNC line — snapshot offset (0) is now captured.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        // 2. Broadcast while the session is blocked streaming the dataset. These
        //    advance the live offset and land in the backlog, after the snapshot
        //    offset and before start_streaming's replay extract.
        let mut expected = Vec::new();
        for i in 0..4 {
            let key = format!("during{i}");
            handler.broadcast_control_command("SET", &[Bytes::from(key.clone()), Bytes::from("v")]);
            expected.push(serialize_command_to_resp(
                "SET",
                &[Bytes::from(key), Bytes::from("v")],
            ));
        }

        // 3. Drain the dataset envelope, unblocking the session so it proceeds
        //    to the streaming handoff.
        let (blobs, _) = drain_live_dataset(&mut client).await;
        assert_eq!(blobs.len(), 1);

        // 4. The handoff replays exactly the 4 writes — none lost. A regression
        //    (subscribe-only handoff) drops them, so the frames never arrive;
        //    bound the wait so that fails fast instead of hanging.
        let frames = tokio::time::timeout(
            Duration::from_secs(5),
            decode_n_frames(&mut client, expected.len()),
        )
        .await
        .expect("handoff writes were not replayed within 5s (F1 regression)");
        let got: Vec<bytes::Bytes> = frames.iter().map(|f| f.payload.clone()).collect();
        assert_eq!(got, expected, "all writes during handoff must be replayed");

        drop(client);
        let _ = task.await.unwrap();
    }

    // FM-REPLICATION-015
    /// F2: a partial resync replays the backlog tail `(replay_from, current]`
    /// before joining the live tail — it never silently drops the gap. Then a
    /// fresh write streams once (no duplicate of the replayed frames).
    #[tokio::test]
    async fn handle_partial_replays_backlog_then_live_tail() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());

        // Seed the backlog. `off1` is the offset the reconnecting replica holds;
        // it must be replayed `(off1, off3]` == {off2, off3}.
        let _off1_first =
            handler.broadcast_control_command("SET", &[Bytes::from("k0"), Bytes::from("v0")]);
        let off1 =
            handler.broadcast_control_command("SET", &[Bytes::from("k1"), Bytes::from("v1")]);
        let off2 =
            handler.broadcast_control_command("SET", &[Bytes::from("k2"), Bytes::from("v2")]);
        let off3 =
            handler.broadcast_control_command("SET", &[Bytes::from("k3"), Bytes::from("v3")]);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Partial { replay_from: off1 },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        // The replayed tail is exactly {off2, off3}, in offset order.
        let replayed = read_continue_then_frames(&mut client, 2).await;
        assert_eq!(replayed[0].sequence, off2);
        assert_eq!(replayed[1].sequence, off3);
        assert_eq!(
            replayed[0].payload,
            serialize_command_to_resp("SET", &[Bytes::from("k2"), Bytes::from("v2")])
        );
        assert_eq!(
            replayed[1].payload,
            serialize_command_to_resp("SET", &[Bytes::from("k3"), Bytes::from("v3")])
        );

        // A fresh write after the handoff arrives on the live tail exactly once.
        let off4 =
            handler.broadcast_control_command("SET", &[Bytes::from("k4"), Bytes::from("v4")]);
        let mut codec = {
            use crate::frame::ReplicationFrameCodec;
            ReplicationFrameCodec::new()
        };
        let mut buf = BytesMut::new();
        let live = loop {
            use tokio_util::codec::Decoder;
            if let Some(frame) = codec.decode(&mut buf).unwrap() {
                break frame;
            }
            let read = client.read_buf(&mut buf).await.unwrap();
            assert!(read > 0, "stream closed before live frame");
        };
        assert_eq!(live.sequence, off4, "live tail must continue after replay");

        drop(client);
        let _ = task.await.unwrap();
        assert_eq!(tracker.replica_count(), 0);
    }

    // FM-REPLICATION-063
    /// Hardening issue 29: the frame lane of `total_net_repl_output_bytes` —
    /// a forwarded write's real encoded length lands in the tracker, on the
    /// primary side, the moment the write task actually puts it on the wire.
    #[tokio::test]
    async fn repl_output_bytes_grow_when_a_write_is_forwarded() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let off1 =
            handler.broadcast_control_command("SET", &[Bytes::from("k1"), Bytes::from("v1")]);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Partial { replay_from: off1 },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        // The replay window (off1, off1] is empty, so the grant carries no
        // backlog frames — nothing forwarded yet counts as nothing sent.
        let off2 =
            handler.broadcast_control_command("SET", &[Bytes::from("k2"), Bytes::from("v2")]);
        let frames = read_continue_then_frames(&mut client, 1).await;
        assert_eq!(frames[0].sequence, off2);
        let expected_output: u64 = frames.iter().map(|f| f.encoded_size() as u64).sum();

        await_net_bytes(
            &tracker,
            NetByteCountersSnapshot {
                output: expected_output,
                input: 0,
            },
        )
        .await;

        drop(client);
        let _ = task.await.unwrap();
    }

    /// Read to EOF and assert nothing more was streamed.
    ///
    /// An abandoned resume must leave the wire empty: not a short tail, not a
    /// frame, just the close.
    async fn assert_no_frames_then_eof(client: &mut tokio::io::DuplexStream) {
        let mut rest = Vec::new();
        loop {
            let mut buf = [0u8; 256];
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => rest.extend_from_slice(&buf[..n]),
            }
        }
        assert!(
            !rest.windows(4).any(|w| w == crate::frame::FRAME_MAGIC),
            "an abandoned resume must stream no frames, got {} trailing bytes",
            rest.len()
        );
    }

    /// The window can close between the grant and the replay, and when it does
    /// the resume is abandoned — never streamed short.
    ///
    /// The `+CONTINUE` is already on the wire when the eviction happens (a
    /// 1-byte duplex parks the session inside that write), which is exactly the
    /// production shape: the grant is decided against one window and streamed
    /// against a later one. Pre-fix, `extract_backlog` returned the surviving
    /// suffix, `resume_offset` was seeded from the last frame actually sent, the
    /// live tail deduped against it, and the replica was permanently missing the
    /// evicted range with an offset that looked contiguous (round-2 issue 52).
    // FM-REPLICATION-012, FM-REPLICATION-050
    #[tokio::test]
    async fn a_resume_evicted_after_the_grant_is_abandoned_not_truncated() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // Two entries of backlog: eviction is a few writes away, not 10 000.
        let handler =
            make_handler_with_backlog_entries(tracker.clone(), None, dir.path().to_path_buf(), 2);

        // The offset the reconnecting replica holds, granted while it is still
        // inside the window.
        let held =
            handler.broadcast_control_command("SET", &[Bytes::from("k0"), Bytes::from("v0")]);
        assert!(handler.replay.backlog_start().unwrap() <= held);

        let (mut client, server) = tokio::io::duplex(1);
        let session = tracker.register_replica(addr());
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Partial { replay_from: held },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        // The session is parked writing `+CONTINUE`; evict the resume point out
        // from under the grant it just made.
        for i in 1..=4 {
            handler.broadcast_control_command(
                "SET",
                &[Bytes::from(format!("k{i}")), Bytes::from("v")],
            );
        }
        assert!(
            handler.replay.backlog_start().unwrap() > held,
            "the test must actually evict the resume point"
        );

        // Unblock the session by draining the grant line.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");

        // The replay refuses, so the session fails the link instead of streaming
        // the surviving suffix.
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the session must not hang on a closed window")
            .unwrap();
        let err = result.expect_err("a truncated replay must fail the link");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "got: {err}");

        assert_no_frames_then_eof(&mut client).await;
        assert_eq!(tracker.replica_count(), 0, "the session must be cleaned up");
        // The grant served nothing, so it is not a partial resync that happened.
        // The replica's *next* PSYNC is what moves the counters, and it is
        // refused against the same floor it just lost.
        assert_eq!(
            tracker.sync_counters().partial_ok,
            0,
            "an abandoned resume must not be counted as a served partial resync"
        );
    }

    /// The same window, on the path that opens it widest: a full sync, where the
    /// grant is written before the dataset is even cut and the whole payload
    /// transfer sits between the grant and the replay.
    // FM-REPLICATION-012
    #[tokio::test]
    async fn a_full_sync_whose_handoff_window_is_evicted_abandons_the_link() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler =
            make_handler_with_backlog_entries(tracker.clone(), None, dir.path().to_path_buf(), 2);
        with_live_dataset(&handler, vec![dataset_blob(&[("seed", "v")])]);
        let repl_id = handler.state.read().replication_id.clone();

        // Tiny buffer: the session parks inside the dataset write, which is the
        // production stand-in for a multi-gigabyte checkpoint transfer.
        let (mut client, server) = tokio::io::duplex(32);
        let session = tracker.register_replica(addr());
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        // Writes during the transfer overrun the backlog, so the snapshot offset
        // the replica was granted is no longer inside the window.
        for i in 0..4 {
            handler.broadcast_control_command(
                "SET",
                &[Bytes::from(format!("during{i}")), Bytes::from("v")],
            );
        }
        assert!(handler.replay.backlog_start().unwrap() > 0);

        let (blobs, _) = drain_live_dataset(&mut client).await;
        assert_eq!(blobs.len(), 1);

        // The replica gets a whole dataset and then a dropped link — it comes
        // back for a fresh full sync rather than resuming past the hole.
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the session must not hang on a closed window")
            .unwrap();
        let err = result.expect_err("a truncated handoff replay must fail the link");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "got: {err}");

        assert_no_frames_then_eof(&mut client).await;
        assert_eq!(tracker.replica_count(), 0, "the session must be cleaned up");
    }

    /// Streaming drop: a partial sync that completes and enters `Streaming`,
    /// then the replica disconnects. The exit handler must remove the session
    /// from the tracker.
    #[tokio::test]
    async fn run_cleans_up_on_streaming_drop_partial() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());

        let (mut client, server) = tokio::io::duplex(1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Partial { replay_from: 0 },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        // Read the +CONTINUE response so the session has reached Streaming.
        let mut buf = [0u8; 64];
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0);
        assert!(buf[..n].starts_with(b"+CONTINUE"));

        // Drop client to trigger EOF on the read half.
        drop(client);

        let result = task.await.unwrap();
        assert!(result.is_ok());

        assert_eq!(tracker.replica_count(), 0);
        assert_eq!(session.phase(), Phase::Disconnecting);
    }

    /// Mid-handshake drop: the writer of `+CONTINUE` fails before the session
    /// can reach `Streaming`. This is the case the old code path did NOT clean
    /// up: a `?` from `write_all` returned without unregistering.
    #[tokio::test]
    async fn run_cleans_up_on_mid_handshake_drop() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());

        // Tiny buffer (1 byte) so write_all blocks; drop the reader so it errors.
        let (client, server) = tokio::io::duplex(1);
        drop(client);
        let session = tracker.register_replica(addr());

        let server: BoxedStream = Box::new(server);
        let result = session
            .clone()
            .run(
                server,
                SyncKind::Partial { replay_from: 0 },
                handler.clone(),
                RecordingFeedAccount::unlimited(),
            )
            .await;

        assert!(result.is_err(), "expected write_all to error after drop");
        assert_eq!(tracker.replica_count(), 0);
        assert_eq!(session.phase(), Phase::Disconnecting);
    }

    /// Issue 67: a full sync served without a RocksStore carries the primary's
    /// **dataset**, not an empty envelope.
    ///
    /// This is the forcing test at the wire level. The old behaviour answered
    /// `PSYNC ? -1` with a data-less minimal RDB (`REDIS0011`… + EOF): the
    /// replica adopted the replid/offset, flipped to `Streaming`, and kept every
    /// key it already had. Here the envelope must be a `$FROGDB_SNAPSHOT` whose
    /// bodies decode to the primary's keys, and whose combined checksum and
    /// replid/offset match what was granted — everything a replica needs to
    /// replace its keyspace rather than keep it.
    ///
    /// Closing the client triggers normal cleanup; no checkpoint directory
    /// should exist (it was never created).
    // FM-REPLICATION-001
    #[tokio::test]
    async fn run_full_sync_without_rocks_streams_the_live_dataset() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        with_live_dataset(
            &handler,
            vec![
                dataset_blob(&[("a", "1"), ("b", "2")]),
                dataset_blob(&[("c", "3")]),
            ],
        );
        let repl_id = handler.state.read().replication_id.clone();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id.clone(),
                        },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        let (blobs, metadata) = drain_live_dataset(&mut client).await;

        // The dataset is really on the wire: one blob per shard, decoding to
        // the primary's keys. The old minimal-RDB payload had none of this.
        let keys: Vec<String> = blobs
            .iter()
            .flat_map(|b| frogdb_persistence::read_entries(b).expect("blob decodes"))
            .map(|e| String::from_utf8(e.key.to_vec()).unwrap())
            .collect();
        assert_eq!(keys, vec!["a", "b", "c"]);

        // Verified end to end the same way a checkpoint is, so a truncated or
        // reordered dataset cannot pass as the primary's keyspace.
        let mut combined = CheckpointChecksum::new();
        for (shard_id, blob) in blobs.iter().enumerate() {
            combined.update_file(
                &format!("shard-{shard_id}.dataset"),
                &calculate_bytes_checksum(blob),
            );
        }
        assert_eq!(combined.finalize(), metadata.checksum);
        assert_eq!(
            metadata.rdb_size,
            blobs.iter().map(Vec::len).sum::<usize>() as u64
        );
        assert_eq!(
            metadata.replication_id,
            handler.state.read().replication_id,
            "the dataset carries the identity the FULLRESYNC granted"
        );
        assert_eq!(
            metadata.replication_offset, 0,
            "and the offset captured before the export"
        );

        drop(client);
        let result = task.await.unwrap();
        assert!(result.is_ok());

        assert_eq!(tracker.replica_count(), 0);
        // The dataset path never sets sync_checkpoint_path, so no dir to clean.
        assert!(session.inner.read().sync_checkpoint_path.is_none());
    }

    // FM-REPLICATION-063
    /// Hardening issue 29: the full-sync payload lane of
    /// `total_net_repl_output_bytes` — the whole dataset a persistence-disabled
    /// primary streams, not just the (empty, here) frame lane. Summing only
    /// forwarded frames would undercount every resync by exactly this much,
    /// which is the issue's "undercount trap": a wrong number that looks more
    /// plausible than `0`.
    #[tokio::test]
    async fn repl_output_bytes_include_the_full_sync_payload() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        with_live_dataset(
            &handler,
            vec![
                dataset_blob(&[("a", "1"), ("b", "2")]),
                dataset_blob(&[("c", "3")]),
            ],
        );
        let repl_id = handler.state.read().replication_id.clone();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        let (blobs, metadata) = drain_live_dataset(&mut client).await;
        let total_size = blobs.iter().map(Vec::len).sum::<usize>() as u64;
        assert_eq!(metadata.rdb_size, total_size);

        await_net_bytes(
            &tracker,
            NetByteCountersSnapshot {
                output: total_size,
                input: 0,
            },
        )
        .await;

        drop(client);
        let _ = task.await.unwrap();
    }

    /// A primary that cannot read its own keyspace fails the sync instead of
    /// sending an envelope with nothing in it — the shape issue 67 was about.
    // FM-REPLICATION-001
    #[tokio::test]
    async fn full_sync_without_a_live_snapshot_source_fails_the_sync() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // No rocks store *and* no live-snapshot source: nothing to send.
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let session = tracker.register_replica(addr());
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                SessionDriver::new(
                    session,
                    server,
                    SyncKind::Full {
                        replication_id: repl_id,
                    },
                    &handler,
                    RecordingFeedAccount::unlimited(),
                )
                .drive()
                .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        let err = task.await.unwrap().expect_err("the sync must fail");
        assert!(
            err.to_string().contains("no live-snapshot source"),
            "got: {err}"
        );
    }

    /// Mid-fullsync drop with a real checkpoint directory — the regression
    /// test for the leak that motivated this refactor.
    ///
    /// Drives a FULLRESYNC against a real RocksStore so a checkpoint is
    /// actually created (and `sync_checkpoint_path` is set), then drops the
    /// client mid-stream. The exit handler must:
    ///   1. unregister the session from the tracker
    ///   2. delete the on-disk checkpoint directory
    #[tokio::test]
    async fn run_cleans_up_checkpoint_dir_on_mid_fullsync_drop() {
        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        // Insert at least one key so the checkpoint contains real data.
        store.put(0, b"k", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();

        let (mut client, server) = tokio::io::duplex(64);
        let session = tracker.register_replica(addr());
        let session_id = session.id();
        let expected_checkpoint = dir.path().join(format!("fullsync_{}", session_id));

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        // Read the grant line first, then wait for the checkpoint to actually
        // exist on disk. Dropping the client before the cut would fail the sync
        // in the pre-checkpoint drain, there would be no directory to leak, and
        // the assertion below would hold no matter what the exit path does —
        // which is how a mutant that deletes `clean_checkpoint_dir` outright
        // survived this test.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        let cut = tokio::time::timeout(Duration::from_secs(5), async {
            while !expected_checkpoint.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            cut.is_ok() && expected_checkpoint.exists(),
            "the test must reach a real checkpoint, else it asserts nothing: {}",
            expected_checkpoint.display()
        );

        // Drop the client so the checkpoint stream's writes start to fail
        // partway through. With a 64-byte duplex buffer, the writer blocks
        // long before the full checkpoint is sent.
        drop(client);

        let _ = task.await.unwrap();

        assert_eq!(tracker.replica_count(), 0);
        assert_eq!(session.phase(), Phase::Disconnecting);
        assert!(
            !expected_checkpoint.exists(),
            "checkpoint dir should have been removed by exit handler: {}",
            expected_checkpoint.display()
        );
    }

    /// The pre-checkpoint hook runs *before* the checkpoint is cut, and what it
    /// makes durable is inside the cut checkpoint.
    ///
    /// In production the hook drains every shard's WAL flush-engine into
    /// RocksDB; here it writes the key directly, which is the same step reduced
    /// to its essence — a write that reaches RocksDB only because the hook ran.
    /// Both halves matter: run the hook *after* `create_checkpoint` and the key
    /// is missing from the checkpoint even though the primary already
    /// acknowledged it, and for a full resync (nothing in the backlog to replay)
    /// the replica never sees it again.
    ///
    /// Drives the sync half ([`SessionDriver::drive`]) rather than `run`, so no
    /// exit step deletes the checkpoint directory before the assertions can read
    /// it.
    #[tokio::test]
    async fn fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook() {
        use std::sync::atomic::{AtomicBool, AtomicUsize};

        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        // Already committed before the resync starts.
        store.put(0, b"early", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();

        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));

        let runs = Arc::new(AtomicUsize::new(0));
        let ran_before_the_cut = Arc::new(AtomicBool::new(false));
        {
            let store = store.clone();
            let runs = runs.clone();
            let ran_before_the_cut = ran_before_the_cut.clone();
            let checkpoint_path = checkpoint_path.clone();
            handler.set_pre_checkpoint_hook(Arc::new(move || {
                let store = store.clone();
                let runs = runs.clone();
                let ran_before_the_cut = ran_before_the_cut.clone();
                let checkpoint_path = checkpoint_path.clone();
                Box::pin(async move {
                    if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                        ran_before_the_cut.store(!checkpoint_path.exists(), Ordering::SeqCst);
                    }
                    store.put(0, b"drained", b"v").unwrap();
                    Ok(FullSyncCapture::none())
                })
            }));
        }

        // A tiny duplex buffer stalls the checkpoint stream. Reading the
        // `+FULLRESYNC` line first pins the session past the reply, so dropping
        // the client afterwards fails a *checkpoint stream* write — i.e. returns
        // the sync after the cut, not before it.
        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                SessionDriver::new(
                    session,
                    server,
                    SyncKind::Full {
                        replication_id: repl_id,
                    },
                    &handler,
                    RecordingFeedAccount::unlimited(),
                )
                .drive()
                .await
            }
        });
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        drop(client);
        let _ = task.await.unwrap();

        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "the drain must run exactly once per full resync"
        );
        assert!(
            ran_before_the_cut.load(Ordering::SeqCst),
            "the drain must run before the checkpoint is cut, not after"
        );

        let checkpoint = RocksStore::open(&checkpoint_path, 1, &RocksConfig::default())
            .expect("cut checkpoint must be a readable database");
        assert_eq!(
            checkpoint.get(0, b"early").unwrap().as_deref(),
            Some(&b"v"[..]),
            "the checkpoint must carry writes committed before the resync"
        );
        assert_eq!(
            checkpoint.get(0, b"drained").unwrap().as_deref(),
            Some(&b"v"[..]),
            "the checkpoint must carry writes the drain committed"
        );
    }

    // FM-PERSISTENCE-020
    /// A drain that cannot complete fails the resync instead of shipping a
    /// dataset that is missing acknowledged writes.
    ///
    /// The checkpoint is the replica's entire base dataset: writes made before
    /// it attached were never broadcast, so nothing in the backlog can replay
    /// what an undrained shard left behind — the hole would be permanent and
    /// invisible. Failing drops the connection and the replica retries `PSYNC ?
    /// -1` on its backoff, which costs one reconnect. No checkpoint may be cut
    /// or staged for cleanup on that path.
    #[tokio::test]
    async fn fullresync_fails_when_the_pre_checkpoint_drain_fails() {
        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"early", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();
        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));

        handler.set_pre_checkpoint_hook(Arc::new(move || {
            Box::pin(async {
                Err(io::Error::other(
                    "WAL drain: 1 of 2 shard(s) did not drain (shard [1])",
                ))
            })
        }));

        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                SessionDriver::new(
                    session,
                    server,
                    SyncKind::Full {
                        replication_id: repl_id,
                    },
                    &handler,
                    RecordingFeedAccount::unlimited(),
                )
                .drive()
                .await
            }
        });
        // The `+FULLRESYNC` line is written before the cut, so it still arrives;
        // what must not follow is a checkpoint. Dropping the client afterwards
        // keeps the assertions honest about *why* the sync failed: without the
        // drain check it would fail on a checkpoint-stream write instead, with a
        // cut checkpoint on disk.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        drop(client);

        let err = task
            .await
            .unwrap()
            .expect_err("an incomplete drain must fail the full resync");
        assert!(
            err.to_string().contains("did not drain"),
            "the drain's cause must reach the failure: {err}"
        );
        assert!(
            !checkpoint_path.exists(),
            "no checkpoint may be cut once the drain has failed"
        );
        assert!(
            session.inner.read().sync_checkpoint_path.is_none(),
            "nothing was staged, so nothing may be marked for cleanup"
        );
    }

    /// Without a hook installed the full-sync path still cuts a checkpoint —
    /// a handler whose owner wired no drain (every unit test here, and any
    /// embedder that keeps its writes in RocksDB synchronously) must not stall
    /// or fail.
    #[tokio::test]
    async fn fullresync_without_a_pre_checkpoint_hook_still_cuts_the_checkpoint() {
        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"early", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();
        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));

        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                SessionDriver::new(
                    session,
                    server,
                    SyncKind::Full {
                        replication_id: repl_id,
                    },
                    &handler,
                    RecordingFeedAccount::unlimited(),
                )
                .drive()
                .await
            }
        });
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        drop(client);
        let _ = task.await.unwrap();

        let checkpoint = RocksStore::open(&checkpoint_path, 1, &RocksConfig::default())
            .expect("cut checkpoint must be a readable database");
        assert_eq!(
            checkpoint.get(0, b"early").unwrap().as_deref(),
            Some(&b"v"[..])
        );
    }

    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use tokio::io::{AsyncBufReadExt as _, BufReader};

    /// A hold that records when it was released, so a test can prove the
    /// release happens *after* the cut rather than merely happening.
    struct RecordingHold {
        released: Arc<AtomicUsize>,
        /// Whether the checkpoint existed at release time.
        cut_first: Arc<AtomicBool>,
        checkpoint_path: PathBuf,
        breached: Vec<u16>,
    }

    impl crate::primary::CaptureHold for RecordingHold {
        fn release(&self) -> Vec<u16> {
            if self.released.fetch_add(1, Ordering::SeqCst) == 0 {
                self.cut_first
                    .store(self.checkpoint_path.exists(), Ordering::SeqCst);
            }
            self.breached.clone()
        }
    }

    /// The drain's coverage vector reaches the replica: it is what the trailer
    /// carries, so the replica can install per-shard skip floors and not
    /// re-apply the writes the checkpoint already holds. And the hold that made
    /// that claim true is released once the cut is done — the engines must not
    /// stay pinned for the whole payload transfer.
    // FM-REPLICATION-066
    #[tokio::test]
    async fn the_drains_coverage_reaches_the_trailer_and_the_hold_is_released_after_the_cut() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            RocksStore::open(&dir.path().join("rocks"), 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"k", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), Some(store), dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));

        let released = Arc::new(AtomicUsize::new(0));
        let cut_first = Arc::new(AtomicBool::new(false));
        {
            let released = released.clone();
            let cut_first = cut_first.clone();
            let checkpoint_path = checkpoint_path.clone();
            handler.set_pre_checkpoint_hook(Arc::new(move || {
                let hold = RecordingHold {
                    released: released.clone(),
                    cut_first: cut_first.clone(),
                    checkpoint_path: checkpoint_path.clone(),
                    breached: Vec::new(),
                };
                Box::pin(async move {
                    Ok(FullSyncCapture {
                        coverage: ShardCoverage::from_watermarks(vec![7, 11]),
                        hold: Some(Box::new(hold)),
                    })
                })
            }));
        }

        let (client, server) = tokio::io::duplex(1024 * 1024);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                SessionDriver::new(
                    session,
                    server,
                    SyncKind::Full {
                        replication_id: repl_id,
                    },
                    &handler,
                    RecordingFeedAccount::unlimited(),
                )
                .drive()
                .await
            }
        });

        let mut reader = BufReader::new(client);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        // Envelope: header, file count, then each file's name/size/body.
        let mut header = String::new();
        reader.read_line(&mut header).await.unwrap();
        assert_eq!(header.trim(), "$FROGDB_CHECKPOINT");
        let mut count_line = String::new();
        reader.read_line(&mut count_line).await.unwrap();
        let file_count: usize = count_line.trim().parse().unwrap();
        for _ in 0..file_count {
            let mut discard = String::new();
            reader.read_line(&mut discard).await.unwrap(); // $<name_len>
            discard.clear();
            reader.read_line(&mut discard).await.unwrap(); // <name>
            let mut size_line = String::new();
            reader.read_line(&mut size_line).await.unwrap();
            let size: usize = size_line.trim().trim_start_matches('$').parse().unwrap();
            let mut body = vec![0u8; size];
            reader.read_exact(&mut body).await.unwrap();
        }
        let mut mlen_line = String::new();
        reader.read_line(&mut mlen_line).await.unwrap();
        let mlen: usize = mlen_line.trim().trim_start_matches('$').parse().unwrap();
        let mut meta_buf = vec![0u8; mlen];
        reader.read_exact(&mut meta_buf).await.unwrap();
        let metadata = FullSyncMetadata::from_bytes(&meta_buf).expect("metadata parses");

        assert_eq!(
            metadata.coverage.as_slice(),
            &[7, 11],
            "the trailer carries the drain's per-shard coverage watermarks"
        );
        assert_eq!(
            released.load(Ordering::SeqCst),
            1,
            "the hold is released exactly once, before the payload is streamed"
        );
        assert!(
            cut_first.load(Ordering::SeqCst),
            "the release must come after the cut — releasing earlier would let a \
             write land in the checkpoint above the watermark the trailer claims"
        );

        drop(reader);
        let _ = task.await.unwrap();
    }

    /// A cut that fails still lets the flush engines go. The hold is a hold, not
    /// a latch: leaving it armed on the failure path would stall this node's
    /// `sync`-durability write acks until the deadline for a sync that is
    /// already over.
    // FM-REPLICATION-066
    #[tokio::test]
    async fn a_failed_cut_still_releases_the_hold() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            RocksStore::open(&dir.path().join("rocks"), 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"k", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), Some(store), dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));
        // RocksDB refuses to check a checkpoint into a path that already exists,
        // so this is a cut that fails for a reason the session cannot control.
        std::fs::create_dir_all(&checkpoint_path).unwrap();

        let released = Arc::new(AtomicUsize::new(0));
        {
            let released = released.clone();
            let checkpoint_path = checkpoint_path.clone();
            handler.set_pre_checkpoint_hook(Arc::new(move || {
                let hold = RecordingHold {
                    released: released.clone(),
                    cut_first: Arc::new(AtomicBool::new(false)),
                    checkpoint_path: checkpoint_path.clone(),
                    breached: Vec::new(),
                };
                Box::pin(async move {
                    Ok(FullSyncCapture {
                        coverage: ShardCoverage::from_watermarks(vec![7]),
                        hold: Some(Box::new(hold)),
                    })
                })
            }));
        }

        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                SessionDriver::new(
                    session,
                    server,
                    SyncKind::Full {
                        replication_id: repl_id,
                    },
                    &handler,
                    RecordingFeedAccount::unlimited(),
                )
                .drive()
                .await
            }
        });
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        drop(client);
        let _ = task.await.unwrap();

        assert_eq!(
            released.load(Ordering::SeqCst),
            1,
            "a failed cut releases the hold exactly as a successful one does"
        );
        // The negative half of the abort tally: this sync was abandoned too,
        // but for a reason that is not a breach, and charging it to
        // `full_sync_hold_breaches` would make an operator size a hold that was
        // never the problem.
        assert_eq!(
            tracker.full_sync_hold_breaches(),
            0,
            "a cut that failed is not a hold that lapsed"
        );
    }

    /// A hold that lapsed before the cut **fails the sync**: the cut itself
    /// succeeded, but the payload may hold writes above the watermark its
    /// trailer would claim, so no payload goes on the wire at all, the link
    /// drops, and the replica retries from scratch (one reconnect backoff).
    ///
    /// The amended semantics (user ruling, 2026-08-21). The landed behaviour —
    /// zero that shard's watermark and ship the payload anyway — reads as
    /// graceful degradation and is not: `0` means "no floor", so the
    /// overshipped range re-executes on the replica and the D1 silent
    /// divergence this row closes is back, in the slow-cut shape, invisible to
    /// both nodes. The staged directory is still cleaned: the abort owns it
    /// first, exactly as a shipped payload does.
    // FM-REPLICATION-066
    #[tokio::test]
    async fn a_breached_hold_aborts_the_sync() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            RocksStore::open(&dir.path().join("rocks"), 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"k", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), Some(store), dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let session = tracker.register_replica(addr());
        let checkpoint_path = dir.path().join(format!("fullsync_{}", session.id()));

        let released = Arc::new(AtomicUsize::new(0));
        let cut_first = Arc::new(AtomicBool::new(false));
        {
            let released = released.clone();
            let cut_first = cut_first.clone();
            let checkpoint_path = checkpoint_path.clone();
            handler.set_pre_checkpoint_hook(Arc::new(move || {
                let hold = RecordingHold {
                    released: released.clone(),
                    cut_first: cut_first.clone(),
                    checkpoint_path: checkpoint_path.clone(),
                    // Shard 1's hold lapsed before the cut.
                    breached: vec![1],
                };
                Box::pin(async move {
                    Ok(FullSyncCapture {
                        coverage: ShardCoverage::from_watermarks(vec![7, 11]),
                        hold: Some(Box::new(hold)),
                    })
                })
            }));
        }

        let (mut client, server) = tokio::io::duplex(1024 * 1024);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");

        let err = task
            .await
            .unwrap()
            .expect_err("a breached hold must fail the sync");
        let rendered = err.to_string();
        assert!(
            rendered.contains("flush hold lapsed") && rendered.contains("shard(s) [1]"),
            "the abort must name itself and the shard: {rendered}"
        );

        // Nothing but the handshake reply ever reached the replica: the next
        // read is the closed link, not `$FROGDB_CHECKPOINT`.
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).await.unwrap();
        assert!(
            rest.is_empty(),
            "no payload may follow a breached hold, got: {:?}",
            String::from_utf8_lossy(&rest)
        );

        assert_eq!(
            released.load(Ordering::SeqCst),
            1,
            "the hold is released exactly once, at the cut"
        );
        assert!(
            cut_first.load(Ordering::SeqCst),
            "the cut really did happen — this is an abort of a staged payload, \
             not a cut that failed"
        );
        assert!(
            !checkpoint_path.exists(),
            "the abandoned sync must not leak the directory it staged"
        );
        // The abort is counted, once, on the tracker that outlives the session
        // — `INFO stats`'s `full_sync_hold_breaches`. Two shards' watermarks
        // were captured and one lapsed; the tally counts the *sync*, not the
        // shard, so an operator alerting on it reads "syncs are failing N
        // times" and not a number that also moves with the shard count.
        assert_eq!(
            tracker.full_sync_hold_breaches(),
            1,
            "a breached hold must leave an alertable counter behind, not only logs"
        );
    }

    /// The acceptance test for the defect this row closes, end to end over the
    /// wire, with a **non-idempotent** write pinned inside the capture→cut
    /// window: `INCR n` executes after `snapshot_offset` was captured and before
    /// the checkpoint is cut, so its effect is in the payload the replica
    /// installs *and* its frame is inside the `(snapshot_offset, current]`
    /// range the handoff replays out of the backlog.
    ///
    /// Before FM-REPLICATION-066 the replica had nothing to tell it the payload
    /// already held that effect, so it re-executed the `INCR` and its counter
    /// reached 2 where the primary's is 1 — with both nodes reporting the same
    /// offset, which is what made the divergence silent. The trailer's coverage
    /// vector describes the pinned write's offset exactly, and the frame is
    /// skipped against its shard's floor instead.
    ///
    /// Everything between the two halves is real: the primary's own session
    /// driver cuts the checkpoint, writes the trailer and replays the backlog
    /// onto the socket, and the frames come back off that socket into the
    /// replica's own consume loop.
    // FM-REPLICATION-066
    #[tokio::test]
    async fn a_write_pinned_in_the_capture_to_cut_window_lands_exactly_once() {
        use crate::ReplicationBroadcaster;
        use crate::apply::{ApplyError, ReplicaApplier, StreamedFrame, consume_frames};
        use crate::frame::ReplicationFrameCodec;
        use crate::replica::AppliedOffset;
        use crate::replica::offset::ReplicaOffset;
        use frogdb_protocol::ParsedCommand;
        use std::sync::atomic::AtomicI64;
        use tokio::sync::mpsc;
        use tokio_util::codec::Decoder;

        /// The replica's keyspace, reduced to the one counter this test is
        /// about. `INCR` is the whole point: re-applying it is not a no-op.
        #[derive(Clone, Default)]
        struct Counter(Arc<AtomicI64>);

        impl ReplicaApplier for Counter {
            async fn apply_group(
                &self,
                _shard_id: u16,
                commands: Vec<ParsedCommand>,
            ) -> Result<(), ApplyError> {
                for command in commands {
                    assert_eq!(
                        String::from_utf8_lossy(&command.name).to_uppercase(),
                        "INCR"
                    );
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            }

            async fn apply_control(&self, _command: ParsedCommand) -> Result<(), ApplyError> {
                Ok(())
            }
        }

        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            RocksStore::open(&dir.path().join("rocks"), 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();
        let session = tracker.register_replica(addr());

        // The pin. The hook runs where the real drain runs — after the grant's
        // offset is captured, before the cut — so a write executed here is
        // exactly the shape this row is about: broadcast (so it is in the
        // backlog window the handoff replays) *and* flushed into the store the
        // checkpoint is about to be cut from (so the payload holds its effect).
        let pinned = Arc::new(AtomicU64::new(0));
        {
            let handler_for_hook = handler.clone();
            let store = store.clone();
            let pinned = pinned.clone();
            handler.set_pre_checkpoint_hook(Arc::new(move || {
                let handler = handler_for_hook.clone();
                let store = store.clone();
                let pinned = pinned.clone();
                Box::pin(async move {
                    let after =
                        handler.broadcast_command_on_shard(0, "INCR", &[Bytes::from_static(b"n")]);
                    store.put(0, b"n", b"1").unwrap();
                    pinned.store(after, Ordering::SeqCst);
                    Ok(FullSyncCapture {
                        coverage: ShardCoverage::from_watermarks(vec![after]),
                        hold: None,
                    })
                })
            }));
        }

        let (client, server) = tokio::io::duplex(1024 * 1024);
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                SessionDriver::new(
                    session,
                    server,
                    SyncKind::Full {
                        replication_id: repl_id,
                    },
                    &handler,
                    RecordingFeedAccount::unlimited(),
                )
                .drive()
                .await
            }
        });

        let mut reader = BufReader::new(client);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let granted: u64 = line
            .split_whitespace()
            .nth(2)
            .expect("+FULLRESYNC <id> <offset>")
            .parse()
            .expect("offset parses");

        // Envelope, then the trailer.
        let mut header = String::new();
        reader.read_line(&mut header).await.unwrap();
        assert_eq!(header.trim(), "$FROGDB_CHECKPOINT");
        let mut count_line = String::new();
        reader.read_line(&mut count_line).await.unwrap();
        let file_count: usize = count_line.trim().parse().unwrap();
        for _ in 0..file_count {
            let mut discard = String::new();
            reader.read_line(&mut discard).await.unwrap(); // $<name_len>
            discard.clear();
            reader.read_line(&mut discard).await.unwrap(); // <name>
            let mut size_line = String::new();
            reader.read_line(&mut size_line).await.unwrap();
            let size: usize = size_line.trim().trim_start_matches('$').parse().unwrap();
            let mut body = vec![0u8; size];
            reader.read_exact(&mut body).await.unwrap();
        }
        let mut mlen_line = String::new();
        reader.read_line(&mut mlen_line).await.unwrap();
        let mlen: usize = mlen_line.trim().trim_start_matches('$').parse().unwrap();
        let mut meta_buf = vec![0u8; mlen];
        reader.read_exact(&mut meta_buf).await.unwrap();
        let metadata = FullSyncMetadata::from_bytes(&meta_buf).expect("metadata parses");
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await.unwrap();
        assert_eq!(&crlf, b"\r\n", "the trailer closes the envelope");

        let pinned_at = pinned.load(Ordering::SeqCst);
        assert_eq!(
            metadata.replication_offset, granted,
            "the trailer's offset is the offset the grant named"
        );
        assert!(
            pinned_at > granted,
            "the pinned write is above the grant: {pinned_at} <= {granted}"
        );
        assert_eq!(
            metadata.coverage.as_slice(),
            &[pinned_at],
            "and the vector says how far above -- exactly, per shard"
        );

        // The replica installs the payload: its keyspace holds `n = 1`, its
        // claim is the granted offset, and its floors are the vector.
        let counter = Counter(Arc::new(AtomicI64::new(1)));
        let applied = AppliedOffset::detached(0);
        let offsets = ReplicaOffset::new(
            Arc::new(RwLock::new(ReplicationState::new())),
            Arc::new(AtomicU64::new(0)),
            applied.clone(),
        );
        assert!(offsets.reset_to_payload(metadata.replication_offset, metadata.coverage.clone()));
        let stint = applied.begin_replica_stint();

        // The handoff replay comes off the same socket the payload did.
        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        while frames.is_empty() {
            while let Some(frame) = codec.decode(&mut buf).unwrap() {
                frames.push(frame);
            }
            if !frames.is_empty() {
                break;
            }
            let read = reader.read_buf(&mut buf).await.unwrap();
            assert!(read > 0, "stream closed before the handoff replay arrived");
        }
        assert_eq!(frames.len(), 1, "the replayed window holds the pinned INCR");
        assert_eq!(frames[0].sequence, pinned_at);

        let (tx, rx) = mpsc::channel(8);
        let epoch = applied.epoch();
        for frame in frames {
            tx.send(StreamedFrame::new(epoch, frame)).await.unwrap();
        }
        drop(tx);
        let stats = consume_frames(
            rx,
            counter.clone(),
            Arc::new(AtomicBool::new(true)),
            None,
            stint,
            Arc::new(crate::apply::ReplicaTxnBound::default()),
        )
        .await;

        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "the pinned INCR is in the payload, so replaying it must not run it \
             again -- 2 here is the silent divergence this row closes"
        );
        assert_eq!(
            stats.floor_skipped, 1,
            "and the skip is counted, not silent"
        );
        assert_eq!(stats.frames_processed, 0);
        assert_eq!(
            applied.current(),
            pinned_at,
            "the skip still claims the frame's bytes, so the head tracks the primary"
        );
        assert_eq!(
            applied.floors(),
            ShardCoverage::none(),
            "the head reached the ceiling, so the floors retire with nothing left above the claim"
        );

        drop(reader);
        let _ = task.await;
    }

    #[test]
    fn force_phase_for_test_drives_phase() {
        let session = ReplicaSession::new(1, addr());
        assert_eq!(session.phase(), Phase::Connecting);
        session.force_phase_for_test(Phase::Streaming);
        assert_eq!(session.phase(), Phase::Streaming);
    }

    // FM-REPLICATION-039
    #[test]
    fn record_ack_is_monotonic_and_refreshes_liveness() {
        let session = ReplicaSession::new(1, addr());
        let t0 = session.last_ack_time();
        assert_eq!(session.acked_offset(), 0);

        // First ACK advances offset and refreshes time.
        std::thread::sleep(Duration::from_millis(2));
        assert!(session.record_ack(100));
        assert_eq!(session.acked_offset(), 100);
        assert!(session.last_ack_time() > t0);

        // Re-ACKing the same offset is treated as liveness only.
        let t1 = session.last_ack_time();
        std::thread::sleep(Duration::from_millis(2));
        assert!(!session.record_ack(100));
        assert_eq!(session.acked_offset(), 100);
        assert!(session.last_ack_time() > t1);

        // Stale ACK (lower offset) does not regress.
        assert!(!session.record_ack(50));
        assert_eq!(session.acked_offset(), 100);
    }

    // FM-REPLICATION-039
    /// Seeding shares `record_ack`'s monotonic contract on its *own* field: it
    /// reports an advance, so a re-seed of the position the session already
    /// holds is not one, and a stale seed never regresses it.
    #[test]
    fn seed_resume_position_advances_only_strictly_forward() {
        let session = ReplicaSession::new(1, addr());

        assert!(session.seed_resume_position(100), "0 → 100 is an advance");
        assert_eq!(session.resume_offset(), 100);
        assert!(
            !session.seed_resume_position(100),
            "re-seeding the position the session already holds is not an advance"
        );
        assert!(
            !session.seed_resume_position(50),
            "a stale seed is not an advance"
        );
        assert_eq!(
            session.resume_offset(),
            100,
            "and never regresses the position"
        );
        assert!(
            session.seed_resume_position(101),
            "one byte forward is enough"
        );
        assert_eq!(session.resume_offset(), 101);
    }

    // FM-REPLICATION-039
    /// Issue 28, at the field that caused it: the primary's record of where a
    /// replica resumed must not show up as something the replica acknowledged.
    /// `WAIT`, `min_acked_offset` and `INFO`'s `slaveN:offset=` all read
    /// `acked_offset`, so a seed leaking into it is a durability claim nobody
    /// made — the shape that had `WAIT 1` answer 1 against an empty replica
    /// keyspace.
    #[test]
    fn a_resume_seed_never_moves_the_wire_acked_offset() {
        let session = ReplicaSession::new(1, addr());

        assert!(session.seed_resume_position(5_000));
        assert_eq!(
            session.acked_offset(),
            0,
            "the primary streaming to offset 5000 is not the replica acking it"
        );
        assert_eq!(session.resume_offset(), 5_000);

        // And the reverse: a wire ACK is not a resume. The two fields are
        // written by exactly one caller each.
        assert!(session.record_ack(6_000));
        assert_eq!(session.acked_offset(), 6_000);
        assert_eq!(
            session.resume_offset(),
            5_000,
            "an ACK says nothing about where the replica resumed"
        );
    }

    // FM-REPLICATION-043
    /// The one consumer allowed to read both: byte lag. A replica resumed at
    /// the live head is not behind, and one that has acked past its resume
    /// point is measured from the ACK — whichever is further along the stream.
    #[test]
    fn stream_position_is_the_max_of_the_wire_ack_and_the_resume_seed() {
        let session = ReplicaSession::new(1, addr());
        assert_eq!(session.stream_position(), 0);

        session.seed_resume_position(5_000);
        assert_eq!(
            session.stream_position(),
            5_000,
            "a freshly-resumed replica is at its resume point, not at 0"
        );

        session.record_ack(4_000);
        assert_eq!(
            session.stream_position(),
            5_000,
            "an ACK behind the resume point does not drag the stream position back"
        );

        session.record_ack(9_000);
        assert_eq!(
            session.stream_position(),
            9_000,
            "and an ACK past it moves the position forward"
        );
    }

    /// The read accessors report what the session was built with — the surface
    /// `INFO replication`, `ROLE` and the version gate read. The session and a
    /// snapshot taken from it must also agree about the streaming phase: they
    /// are the same question asked by different callers (the write path asks
    /// the session, the renderers ask the snapshot).
    #[test]
    fn session_accessors_report_the_announced_identity_and_live_phase() {
        let session = ReplicaSession::announced(
            42,
            addr(),
            ReplicaAnnouncement {
                listening_port: 7001,
                capabilities: ReplicaCapabilities {
                    eof: true,
                    psync2: false,
                },
                version: Some("9.9.9".to_string()),
            },
        );

        assert_eq!(session.id(), 42);
        assert_eq!(session.address(), addr());
        assert_eq!(session.listening_port(), 7001);
        assert!(session.capabilities().eof, "eof was announced");
        assert!(
            !session.capabilities().psync2,
            "psync2 was not, and must not be invented"
        );
        assert_eq!(session.replica_version().as_deref(), Some("9.9.9"));
        assert_eq!(session.snapshot().listening_port, 7001);

        assert!(
            !session.is_streaming(),
            "a session that has not finished its handshake is not streaming"
        );
        assert!(!session.snapshot().is_streaming());

        session.force_phase_for_test(Phase::StreamingCheckpoint);
        assert!(
            !session.is_streaming(),
            "shipping the checkpoint is not the live stream"
        );
        assert!(!session.snapshot().is_streaming());

        session.force_phase_for_test(Phase::Streaming);
        assert!(session.is_streaming());
        assert!(session.snapshot().is_streaming());

        session.force_phase_for_test(Phase::Disconnecting);
        assert!(
            !session.is_streaming(),
            "a session in teardown must stop counting as streaming"
        );
        assert!(!session.snapshot().is_streaming());
    }

    /// Full-sync progress is a fraction of the enumerated total, and an
    /// un-enumerated transfer reports 100 rather than dividing by zero.
    #[test]
    fn progress_percent_is_the_transferred_fraction_of_the_expected_total() {
        let session = ReplicaSession::new(1, addr());
        assert_eq!(
            session.progress_percent(),
            100.0,
            "nothing expected yet reads as complete, not as 0/0"
        );

        session.inner.write().sync_total_bytes = 400;
        assert_eq!(session.progress_percent(), 0.0);
        session.sync_bytes_transferred.store(100, Ordering::Release);
        assert_eq!(session.progress_percent(), 25.0);
        session.sync_bytes_transferred.store(400, Ordering::Release);
        assert_eq!(session.progress_percent(), 100.0);
    }

    /// The completion log's MiB/s figure, including the unmeasurably-fast case:
    /// a transfer that finished inside the clock's resolution reports 0, not
    /// `inf`.
    #[test]
    fn transfer_rate_is_mib_per_second_and_survives_a_zero_duration() {
        assert_eq!(
            transfer_rate_mbps(2 * 1024 * 1024, Duration::from_secs(2)),
            1.0
        );
        assert_eq!(
            transfer_rate_mbps(1024 * 1024, Duration::from_secs(4)),
            0.25
        );
        assert_eq!(
            transfer_rate_mbps(1024 * 1024, Duration::ZERO),
            0.0,
            "an immeasurably fast transfer reports 0 MiB/s, never a division by zero"
        );
        assert_eq!(transfer_rate_mbps(0, Duration::from_secs(1)), 0.0);
    }

    /// `replication-write-timeout-ms 0` disables the per-frame write deadline.
    /// The boundary is the whole point: reading `0` as a zero-length timeout
    /// would expire before the first frame and drop every replica link.
    #[test]
    fn a_zero_write_timeout_means_no_deadline_at_all() {
        assert_eq!(frame_write_timeout(0), None);
        assert_eq!(
            frame_write_timeout(1),
            Some(Duration::from_millis(1)),
            "the smallest armed value is still an armed value"
        );
        assert_eq!(frame_write_timeout(5_000), Some(Duration::from_secs(5)));
    }

    // FM-REPLICATION-013
    /// The FULLRESYNC reply line and the streamed checkpoint metadata must both
    /// carry the primary's *live* offset (the tracker's write position), not the
    /// stale `state.replication_offset` (left at 0 here). This is the offset/data
    /// correspondence the staged `replication_metadata.json` relies on.
    #[tokio::test]
    async fn fullresync_offset_and_metadata_come_from_live_tracker() {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"k", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // Simulate writes having advanced the live stream head. The handler's
        // `state.replication_offset` stays 0, so a stale-field read would attach
        // offset 0 to the checkpoint.
        let live_offset = 4096u64;
        tracker.set_offset(live_offset);

        let handler = make_handler(
            tracker.clone(),
            Some(store.clone()),
            dir.path().to_path_buf(),
        );
        let repl_id = handler.state.read().replication_id.clone();

        let (client, server) = tokio::io::duplex(1024 * 1024);
        let session = tracker.register_replica(addr());

        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let mut reader = BufReader::new(client);

        // 1. FULLRESYNC line carries the live tracker offset, not the stale 0.
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(parts[0], "+FULLRESYNC");
        assert_eq!(parts[1], repl_id);
        assert_eq!(parts[2].parse::<u64>().unwrap(), live_offset);

        // 2. Checkpoint header + file count.
        let mut header = String::new();
        reader.read_line(&mut header).await.unwrap();
        assert_eq!(header.trim(), "$FROGDB_CHECKPOINT");
        let mut count_line = String::new();
        reader.read_line(&mut count_line).await.unwrap();
        let file_count: usize = count_line.trim().parse().unwrap();

        // 3. Drain each file body: "$<name_len>\r\n<name>\r\n$<size>\r\n<raw bytes>".
        assert!(file_count > 0, "a cut checkpoint has files in it");
        let mut streamed_bytes = 0u64;
        for _ in 0..file_count {
            let mut name_len_line = String::new();
            reader.read_line(&mut name_len_line).await.unwrap();
            let mut name_line = String::new();
            reader.read_line(&mut name_line).await.unwrap();
            let mut size_line = String::new();
            reader.read_line(&mut size_line).await.unwrap();
            let size: usize = size_line.trim().trim_start_matches('$').parse().unwrap();
            let mut body = vec![0u8; size];
            reader.read_exact(&mut body).await.unwrap();
            streamed_bytes += size as u64;
        }

        // 4. Metadata frame: "$<mlen>\r\n<metadata bytes>\r\n".
        let mut mlen_line = String::new();
        reader.read_line(&mut mlen_line).await.unwrap();
        let mlen: usize = mlen_line.trim().trim_start_matches('$').parse().unwrap();
        let mut meta_buf = vec![0u8; mlen];
        reader.read_exact(&mut meta_buf).await.unwrap();
        let metadata = crate::fullsync::FullSyncMetadata::from_bytes(&meta_buf).unwrap();
        assert_eq!(
            metadata.replication_offset, live_offset,
            "streamed checkpoint metadata must carry the live tracker offset"
        );
        assert_eq!(metadata.replication_id, repl_id);
        assert!(streamed_bytes > 0, "the checkpoint files are not empty");
        assert_eq!(
            metadata.rdb_size, streamed_bytes,
            "the announced payload size must be the sum of the file sizes \
             actually put on the wire — the replica sizes its staging area \
             from this number"
        );
        assert_eq!(
            session.inner.read().sync_total_bytes,
            streamed_bytes,
            "and the same total is what the session's sync progress is measured against"
        );

        drop(reader);
        let _ = task.await.unwrap();
    }

    /// Read the leading simple-string response line from the stream.
    async fn read_response_line(client: &mut tokio::io::DuplexStream) -> String {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = client.read(&mut byte).await.unwrap();
            if n == 0 {
                break;
            }
            line.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        String::from_utf8(line).unwrap()
    }

    // FM-REPLICATION-015
    /// The inverse of the old pinning test: with the backlog enabled and the
    /// requested offset still covered, a matching window now yields `+CONTINUE`
    /// — the gate is gone, partial resync is granted end to end.
    #[tokio::test]
    async fn partial_window_with_backlog_grants_continue() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        // Advance the live offset and populate the backlog with real commands.
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);
        handler.broadcast_control_command("SET", &[Bytes::from("b"), Bytes::from("2")]);
        handler.broadcast_control_command("SET", &[Bytes::from("c"), Bytes::from("3")]);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                handler
                    .handle_psync(
                        server,
                        addr(),
                        &repl_id,
                        resume_point as i64,
                        ReplicaAnnouncement::default(),
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+CONTINUE"),
            "in-window PSYNC with backlog coverage must grant +CONTINUE, got: {line:?}"
        );
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(
            parts[1], repl_id,
            "+CONTINUE carries the live replication id"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // FM-REPLICATION-017
    /// Shutdown must end a streaming session even though its peer keeps the
    /// socket open: the session is served inline by the accepting connection
    /// task, so nothing else would ever release it — and it holds the storage
    /// engine open behind the shutdown.
    #[tokio::test]
    async fn shutdown_downstream_sessions_ends_a_streaming_session() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                handler
                    .handle_psync(
                        server,
                        addr(),
                        &repl_id,
                        resume_point as i64,
                        ReplicaAnnouncement::default(),
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        // Streaming: the session is registered and parked on the WAL stream.
        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");
        assert_eq!(tracker.get_all_replicas().len(), 1);

        // The peer never closes — only the shutdown signal can end this.
        let signalled = handler
            .shutdown_downstream_sessions(Duration::from_secs(5))
            .await;
        assert_eq!(signalled, 1, "the streaming session must be signalled");
        assert!(
            tracker.get_all_replicas().is_empty(),
            "shutdown must return only once every session has left the registry"
        );
        task.await.unwrap().unwrap();
    }

    // FM-REPLICATION-017
    /// A connection accepted a moment before the acceptors were aborted can
    /// still reach PSYNC while the drain runs. It must be refused: a session
    /// registered behind the drain would stream past the shutdown that was
    /// meant to end them all, holding the storage engine open.
    #[tokio::test]
    async fn psync_after_the_shutdown_drain_is_refused() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        // No sessions yet, so the drain returns immediately — it still latches.
        assert_eq!(
            handler
                .shutdown_downstream_sessions(Duration::from_secs(5))
                .await,
            0
        );

        let (_client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let err = handler
            .handle_psync(
                server,
                addr(),
                &repl_id,
                resume_point as i64,
                ReplicaAnnouncement::default(),
                RecordingFeedAccount::unlimited(),
            )
            .await
            .expect_err("PSYNC must not be served once the drain has started");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);
        assert!(
            tracker.get_all_replicas().is_empty(),
            "a refused PSYNC must not register a session"
        );
    }

    /// An announcement carrying just a version, folded the way the connection
    /// folds it — `absorb`, not a struct literal, so these tests exercise the
    /// same path `DispatchStage::ReplicationHandshake` uses.
    fn announced_version(version: &str) -> ReplicaAnnouncement {
        let mut announcement = ReplicaAnnouncement::default();
        announcement.absorb(AnnouncedOption::Version(version.to_string()));
        announcement
    }

    /// A version on this build's major line but a minor no build will ever
    /// carry, so the pair is a skew whatever the workspace version becomes.
    fn skewed_minor_version() -> String {
        let major = PRIMARY_VERSION
            .split('.')
            .next()
            .expect("split always yields one segment");
        format!("{major}.9999.0")
    }

    // FM-REPLICATION-064
    /// The gate: a replica on a different major is refused, and refused
    /// *before* the primary commits anything to it. No session in the registry
    /// (INFO would otherwise report a replica that will never stream), no
    /// resync counted (the refusal is not a full resync this primary served),
    /// and the error the peer is sent names both versions so either operator
    /// can act on it from their own node's log.
    #[tokio::test]
    async fn psync_from_an_incompatible_major_is_refused_before_anything_is_registered() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        // Far enough ahead that no future workspace version can collide.
        let their_version = "999.0.0";
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let err = handler
            .handle_psync(
                server,
                addr(),
                &repl_id,
                resume_point as i64,
                announced_version(their_version),
                RecordingFeedAccount::unlimited(),
            )
            .await
            .expect_err("a replica on another major must not be served");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("-ERR PSYNC refused"),
            "the replica must be told why, got: {line:?}"
        );
        assert!(
            line.contains(their_version) && line.contains(PRIMARY_VERSION),
            "the refusal must name both versions, got: {line:?}"
        );
        assert!(
            tracker.get_all_replicas().is_empty(),
            "a refused PSYNC must not register a session"
        );
        assert_eq!(
            tracker.sync_counters(),
            SyncCountersSnapshot::default(),
            "a refusal is not a resync this primary served"
        );
    }

    // FM-REPLICATION-064
    /// The other half of the rule: a minor skew is a rolling upgrade in
    /// flight, so it is *served* — and the version it announced is on the
    /// session, which is what the warning reports.
    #[tokio::test]
    async fn psync_from_a_minor_skewed_replica_is_served() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);
        let their_version = skewed_minor_version();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            let announcement = announced_version(&their_version);
            async move {
                handler
                    .handle_psync(
                        server,
                        addr(),
                        &repl_id,
                        resume_point as i64,
                        announcement,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+CONTINUE"),
            "a same-major replica must be served, got: {line:?}"
        );
        let replicas = tracker.get_all_replicas();
        assert_eq!(replicas.len(), 1, "the session is registered");
        assert_eq!(
            replicas[0].replica_version.as_deref(),
            Some(their_version.as_str()),
            "and carries what the admitted replica announced"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // FM-REPLICATION-064
    /// Unknown is not incompatible. A peer whose version this primary cannot
    /// read — a pre-option replica, a non-FrogDB client, anything — is served,
    /// because refusing what cannot be proved incompatible takes a data path
    /// down on a suspicion.
    #[tokio::test]
    async fn psync_from_a_replica_with_an_unreadable_version_is_served() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            let announcement = announced_version("not-a-version");
            async move {
                handler
                    .handle_psync(
                        server,
                        addr(),
                        &repl_id,
                        resume_point as i64,
                        announcement,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+CONTINUE"),
            "an unreadable version must not stop the handshake, got: {line:?}"
        );
        assert_eq!(
            tracker.get_all_replicas().len(),
            1,
            "the session is registered like any other"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // FM-REPLICATION-013
    /// With the backlog disabled there is nothing to replay, so even a matching
    /// window falls back to FULLRESYNC — and the offset is the live tracker's.
    #[tokio::test]
    async fn partial_falls_back_to_full_when_backlog_disabled() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let live_offset = 1000u64;
        tracker.set_offset(live_offset);

        // make_handler disables the backlog; no rocks → minimal-RDB FULLRESYNC.
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                // Offset 500 <= live 1000 with a matching replid is a valid
                // window, but the disabled backlog cannot replay the gap.
                handler
                    .handle_psync(
                        server,
                        addr(),
                        &repl_id,
                        500,
                        ReplicaAnnouncement::default(),
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+FULLRESYNC"),
            "disabled backlog must force FULLRESYNC, got: {line:?}"
        );
        let parts: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(parts[1], repl_id);
        assert_eq!(
            parts[2].parse::<u64>().unwrap(),
            live_offset,
            "FULLRESYNC offset must be the live tracker offset"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // FM-REPLICATION-014
    /// A replica whose resume point has been evicted from the backlog falls back
    /// to FULLRESYNC — the lower bound guards against a truncated replay.
    #[tokio::test]
    async fn partial_falls_back_to_full_when_offset_evicted() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // Tiny backlog (3 entries) so the early resume point is evicted.
        let handler = Arc::new(PrimaryReplicationHandler::new(
            crate::identity::ReplicationIdentity::adopting(ReplicationState::new(), &tracker),
            dir.path().join("replication_state.json"),
            tracker.clone(),
            None,
            dir.path().to_path_buf(),
            LagThresholdConfig {
                threshold_bytes: 0,
                threshold_secs: 0,
                cooldown: Duration::from_secs(0),
            },
            BacklogConfig {
                enabled: true,
                max_entries: 3,
                max_bytes: 64 * 1024 * 1024,
                ttl_secs: 0,
            },
            0,
            crate::feed_gate::ReplicaFeedGate::open(),
        ));
        let repl_id = handler.state.read().replication_id.clone();

        // First command's offset is the replica's resume point; later writes
        // evict it from the 3-entry backlog.
        let evicted_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);
        for i in 0..5 {
            handler.broadcast_control_command(
                "SET",
                &[Bytes::from(format!("k{i}")), Bytes::from("v")],
            );
        }
        assert!(
            handler.replay.oldest_offset().unwrap() > evicted_point,
            "resume point should have been evicted"
        );

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                handler
                    .handle_psync(
                        server,
                        addr(),
                        &repl_id,
                        evicted_point as i64,
                        ReplicaAnnouncement::default(),
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+FULLRESYNC"),
            "evicted resume point must force FULLRESYNC, got: {line:?}"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // ------------------------------------------------------------------
    // The PSYNC-outcome counters (INFO's sync_full / sync_partial_ok /
    // sync_partial_err).
    // ------------------------------------------------------------------

    // FM-REPLICATION-050
    /// Each arm of the `+FULLRESYNC` / `+CONTINUE` fork moves the counter it is
    /// named after, driven through the real `handle_psync` rather than the
    /// classifier alone — the counters were literal zeros in both INFO
    /// renderers, which reads identically to a healthy link (issue 17).
    ///
    /// The three handshakes are run against one handler in sequence because the
    /// counters are cumulative: the assertions pin the *deltas*, including the
    /// two that must NOT move.
    #[tokio::test]
    async fn each_psync_fork_moves_the_counter_it_is_named_after() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        // A six-entry backlog: large enough for the in-window reconnect below,
        // small enough to evict the resume point on demand.
        let handler =
            make_handler_with_backlog_entries(tracker.clone(), None, dir.path().to_path_buf(), 6);
        let repl_id = handler.state.read().replication_id.clone();

        assert_eq!(
            tracker.sync_counters(),
            SyncCountersSnapshot::default(),
            "a primary that has served no PSYNC reports all three at zero"
        );

        // 1. First attach: `PSYNC ? -1`. No partial was attempted, so only
        //    sync_full moves.
        let line = run_psync(&handler, "?", -1).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        assert_eq!(
            tracker.sync_counters(),
            SyncCountersSnapshot {
                full: 1,
                partial_ok: 0,
                partial_err: 0,
            },
            "an outright full-resync request is not a refused partial"
        );

        // 2. Reconnect inside the backlog window: `+CONTINUE`, and sync_full
        //    must stay where it was — the whole point of a partial resync is
        //    that no checkpoint was transferred.
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);
        handler.broadcast_control_command("SET", &[Bytes::from("b"), Bytes::from("2")]);
        let line = run_psync(&handler, &repl_id, resume_point as i64).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");
        // A grant is counted where it becomes true — after the backlog tail is
        // extracted, which is downstream of the reply this test just read — so
        // this arm waits for the counter instead of reading it immediately.
        await_counters(
            &tracker,
            SyncCountersSnapshot {
                full: 1,
                partial_ok: 1,
                partial_err: 0,
            },
        )
        .await;

        // 3. Reconnect that overran the backlog: the partial was attempted and
        //    refused, and the refusal falls through to a full resync — so both
        //    sync_partial_err and sync_full advance, exactly as Redis does it.
        for i in 0..8 {
            handler.broadcast_control_command(
                "SET",
                &[Bytes::from(format!("k{i}")), Bytes::from("v")],
            );
        }
        assert!(
            handler.replay.oldest_offset().unwrap() > resume_point,
            "the resume point must have been evicted for this arm to be a refusal"
        );
        let line = run_psync(&handler, &repl_id, resume_point as i64).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        assert_eq!(
            tracker.sync_counters(),
            SyncCountersSnapshot {
                full: 2,
                partial_ok: 1,
                partial_err: 1,
            },
            "a refused partial advances sync_partial_err AND sync_full"
        );
    }

    /// Poll until the counters reach `expected`, or fail with what they were.
    ///
    /// Only the grant needs this: it is recorded by the streaming session once
    /// the backlog tail exists, so it lands after the reply the caller read.
    async fn await_counters(tracker: &ReplicationTrackerImpl, expected: SyncCountersSnapshot) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let seen = tracker.sync_counters();
            if seen == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "counters never reached {expected:?}; last read {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Poll until the net-byte counters reach `expected` (hardening issue 29),
    /// mirroring [`await_counters`]: a write recorded on the write task lands
    /// after a client that only reads the wire can observe its effect.
    async fn await_net_bytes(tracker: &ReplicationTrackerImpl, expected: NetByteCountersSnapshot) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let seen = tracker.net_bytes();
            if seen == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "net bytes never reached {expected:?}; last read {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Drive one `PSYNC` to its decision line and let the session go.
    ///
    /// A refusal is recorded at the fork, so reading the reply line is enough
    /// for those arms; the session is then dropped by closing the client end.
    async fn run_psync(
        handler: &Arc<PrimaryReplicationHandler>,
        replication_id: &str,
        offset: i64,
    ) -> String {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let replication_id = replication_id.to_string();
            async move {
                handler
                    .handle_psync(
                        server,
                        addr(),
                        &replication_id,
                        offset,
                        ReplicaAnnouncement::default(),
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });
        let line = read_response_line(&mut client).await;
        drop(client);
        let _ = task.await.unwrap();
        line
    }

    // FM-REPLICATION-049
    /// The end-to-end shape of issue 16: a replica that announced port 7001
    /// before `PSYNC` is registered with that port, so the primary's INFO/ROLE
    /// projection reports it instead of the placeholder `0`.
    #[tokio::test]
    async fn a_psync_carries_the_announcement_into_the_registry() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        let announcement = ReplicaAnnouncement {
            listening_port: 7001,
            capabilities: ReplicaCapabilities {
                eof: true,
                psync2: true,
            },
            version: Some("0.1.0".to_string()),
        };

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let server: BoxedStream = Box::new(server);
        let task = tokio::spawn({
            let handler = handler.clone();
            let repl_id = repl_id.clone();
            async move {
                handler
                    .handle_psync(
                        server,
                        addr(),
                        &repl_id,
                        resume_point as i64,
                        announcement,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");

        // `+CONTINUE` is written before `start_streaming` sets the phase to
        // `Streaming`, so reading the line first does not order-after the
        // registration this test cares about — poll for it instead of relying
        // on an ordering the test does not control.
        let replicas = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let replicas = tracker.get_streaming_replicas();
                if !replicas.is_empty() {
                    return replicas;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replica must reach the Streaming phase and register within 5s");

        assert_eq!(replicas.len(), 1);
        assert_eq!(
            replicas[0].listening_port, 7001,
            "the registry must carry the announced port, not 0"
        );
        assert!(replicas[0].capabilities.psync2);
        assert_eq!(
            replicas[0].replica_version.as_deref(),
            Some("0.1.0"),
            "the registry must carry the announced version, not None (issue 22)"
        );

        drop(client);
        let _ = task.await.unwrap();
    }

    // ------------------------------------------------------------------
    // forward_frame: the single write+timeout+error path, no socket needed.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn forward_frame_clean_write_continues() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let outcome = forward_frame(&mut server, b"hello", None, 1).await;
        assert!(matches!(outcome, Forward::Continue));
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[tokio::test]
    async fn forward_frame_write_timeout_breaks() {
        // 1-byte duplex buffer and nobody reading: write_all can never
        // complete, so the timeout must fire. Keep `client` alive so the
        // failure is a timeout, not an I/O error.
        let (_client, mut server) = tokio::io::duplex(1);
        let payload = vec![0u8; 64];
        let outcome =
            forward_frame(&mut server, &payload, Some(Duration::from_millis(20)), 1).await;
        assert!(matches!(outcome, Forward::Break));
    }

    #[tokio::test]
    async fn forward_frame_io_error_breaks() {
        let (client, mut server) = tokio::io::duplex(64);
        drop(client);
        let outcome = forward_frame(&mut server, b"data", None, 1).await;
        assert!(matches!(outcome, Forward::Break));
    }

    // ------------------------------------------------------------------
    // LagPolicy: the proactive lag-disconnect decision, no live session.
    // ------------------------------------------------------------------

    /// Drive the policy through one full check interval and report whether it
    /// fired (`should_disconnect` only evaluates thresholds every
    /// `LAG_CHECK_INTERVAL` forwarded frames).
    fn drive_one_interval(
        policy: &mut LagPolicy,
        tracker: &ReplicationTrackerImpl,
        id: u64,
    ) -> bool {
        drive_one_interval_breach(policy, tracker, id).is_some()
    }

    /// Like [`drive_one_interval`] but returns the [`LagBreach`] detail from the
    /// evaluating call so tests can assert *which* threshold fired.
    fn drive_one_interval_breach(
        policy: &mut LagPolicy,
        tracker: &ReplicationTrackerImpl,
        id: u64,
    ) -> Option<LagBreach> {
        let mut breach = None;
        for _ in 0..LAG_CHECK_INTERVAL {
            if let Some(b) = policy.should_disconnect(tracker, id) {
                breach = Some(b);
            }
        }
        breach
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_disabled_never_fires() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(1_000_000); // enormous byte lag, but nothing armed
        let mut policy = LagPolicy::new(Arc::new(LagThresholds::new(0, 0)), Duration::ZERO);
        for _ in 0..(2 * LAG_CHECK_INTERVAL) {
            assert!(policy.should_disconnect(&tracker, session.id()).is_none());
        }
    }

    /// The check cadence is exactly `LAG_CHECK_INTERVAL` forwarded frames, and
    /// it is a *counter*, not a coincidence: an armed policy over a replica that
    /// is already far past the threshold stays silent for the whole interval and
    /// fires on the frame that completes it, then starts the interval again.
    /// Evaluating on every frame would put a `replica_lag` lookup (two locks) on
    /// the per-frame write path.
    #[test]
    fn lag_policy_evaluates_once_per_check_interval() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(10_000); // acked = 0 → far past the threshold
        let mut policy = LagPolicy::new(Arc::new(LagThresholds::new(1_000, 0)), Duration::ZERO);

        for frame in 1..LAG_CHECK_INTERVAL {
            assert!(
                policy.should_disconnect(&tracker, session.id()).is_none(),
                "frame {frame} is inside the interval, so nothing is evaluated"
            );
        }
        assert!(
            policy.should_disconnect(&tracker, session.id()).is_some(),
            "the {LAG_CHECK_INTERVAL}th frame completes the interval and evaluates"
        );

        for frame in 1..LAG_CHECK_INTERVAL {
            assert!(
                policy.should_disconnect(&tracker, session.id()).is_none(),
                "frame {frame} of the second interval evaluates nothing either"
            );
        }
        assert!(policy.should_disconnect(&tracker, session.id()).is_some());
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_byte_threshold_triggers() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(10_000); // acked = 0 → lag = 10_000 bytes
        let mut policy = LagPolicy::new(
            Arc::new(LagThresholds::new(1_000, 0)),
            Duration::from_secs(60),
        );
        let breach = drive_one_interval_breach(&mut policy, &tracker, session.id())
            .expect("byte threshold should fire a breach");
        assert!(breach.byte_exceeded, "byte threshold must be flagged");
        assert!(
            !breach.time_exceeded,
            "time threshold is disabled, must not be flagged"
        );
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_byte_threshold_not_exceeded_does_not_fire() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(500); // lag 500 < threshold 1_000
        let mut policy = LagPolicy::new(
            Arc::new(LagThresholds::new(1_000, 0)),
            Duration::from_secs(60),
        );
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_time_threshold_triggers() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        // The smallest armable time threshold is 1s; age the last-ACK time
        // past it.
        std::thread::sleep(Duration::from_millis(1100));
        let mut policy =
            LagPolicy::new(Arc::new(LagThresholds::new(0, 1)), Duration::from_secs(60));
        let breach = drive_one_interval_breach(&mut policy, &tracker, session.id())
            .expect("time threshold should fire a breach");
        assert!(breach.time_exceeded, "time threshold must be flagged");
        assert!(
            !breach.byte_exceeded,
            "byte threshold is disabled, must not be flagged"
        );
    }

    // FM-REPLICATION-043
    #[test]
    fn lag_policy_cooldown_suppresses_retrigger() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(10_000);
        // A prior proactive disconnect for this replica's address...
        tracker.record_lag_disconnect(session.id());
        // ...suppresses a re-trigger inside the cooldown window...
        let mut in_cooldown = LagPolicy::new(
            Arc::new(LagThresholds::new(1_000, 0)),
            Duration::from_secs(60),
        );
        assert!(!drive_one_interval(
            &mut in_cooldown,
            &tracker,
            session.id()
        ));
        // ...but a zero cooldown (window already elapsed) fires again.
        let mut expired = LagPolicy::new(Arc::new(LagThresholds::new(1_000, 0)), Duration::ZERO);
        assert!(drive_one_interval(&mut expired, &tracker, session.id()));
    }

    // FM-REPLICATION-043
    /// Propagation truth for `replication-lag-threshold-bytes`: the policy of an
    /// *already-running* session reads the shared thresholds at evaluation time,
    /// so storing a new byte threshold changes the very next decision — no
    /// reconnect, no new `LagPolicy`.
    #[test]
    fn lag_policy_byte_threshold_retunes_live() {
        let tracker = ReplicationTrackerImpl::new();
        let session = tracker.register_replica(addr());
        tracker.set_offset(10_000); // acked = 0 → lag = 10_000 bytes

        // Booted disabled: the same policy object never fires, whatever the lag.
        let thresholds = Arc::new(LagThresholds::new(0, 0));
        let mut policy = LagPolicy::new(thresholds.clone(), Duration::ZERO);
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));

        // CONFIG SET replication-lag-threshold-bytes 1000 — armed above the lag.
        thresholds.set_threshold_bytes(1_000);
        let breach = drive_one_interval_breach(&mut policy, &tracker, session.id())
            .expect("a live-armed byte threshold must fire on the running policy");
        assert!(breach.byte_exceeded);
        assert!(!breach.time_exceeded);

        // Raising it back above the lag disarms the same running policy.
        thresholds.set_threshold_bytes(1_000_000);
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));

        // ...and disabling it entirely also takes effect immediately.
        thresholds.set_threshold_bytes(1_000);
        assert!(drive_one_interval(&mut policy, &tracker, session.id()));
        thresholds.set_threshold_bytes(0);
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));
    }

    // FM-REPLICATION-043
    /// Propagation truth for `replication-lag-threshold-secs`, via the handler's
    /// own setter — the exact call `ConfigManager` will make. The handler and the
    /// session's policy share one [`LagThresholds`], so the store is visible to
    /// the running policy's next evaluation.
    #[test]
    fn lag_policy_time_threshold_retunes_live_via_handler() {
        let tmp = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let session = tracker.register_replica(addr());
        // `make_handler` boots with both lag thresholds disabled.
        let handler = make_handler(tracker.clone(), None, tmp.path().to_path_buf());

        // A session's policy, built exactly as the streaming loop builds it.
        let mut policy = LagPolicy::new(handler.lag_thresholds(), handler.lag_cooldown);
        // Age the last-ACK time past one second.
        std::thread::sleep(Duration::from_millis(1100));
        assert!(
            !drive_one_interval(&mut policy, &tracker, session.id()),
            "disabled at boot: nothing fires"
        );

        // CONFIG SET replication-lag-threshold-secs 1
        handler.set_lag_threshold_secs(1);
        let breach = drive_one_interval_breach(&mut policy, &tracker, session.id())
            .expect("a live-armed time threshold must fire on the running policy");
        assert!(breach.time_exceeded);
        assert!(!breach.byte_exceeded);

        // And back off again on the same policy object.
        handler.set_lag_threshold_secs(0);
        assert!(!drive_one_interval(&mut policy, &tracker, session.id()));
    }

    // ------------------------------------------------------------------
    // Departure classification: how a streaming link ended (FM-REPLICATION-062).
    // ------------------------------------------------------------------

    /// Serve one `PSYNC` on a background task, exactly as the connection loop
    /// does, so a test can watch the session end and read the classification it
    /// left behind.
    fn spawn_psync(
        handler: Arc<PrimaryReplicationHandler>,
        stream: BoxedStream,
        replication_id: String,
        offset: i64,
    ) -> tokio::task::JoinHandle<io::Result<()>> {
        tokio::spawn(async move {
            handler
                .handle_psync(
                    stream,
                    addr(),
                    &replication_id,
                    offset,
                    ReplicaAnnouncement::default(),
                    RecordingFeedAccount::unlimited(),
                )
                .await
        })
    }

    /// A primary with a streaming replica, parked on the WAL stream. Returns the
    /// client half, the session task, and the handler.
    async fn streaming_primary(
        tracker: Arc<ReplicationTrackerImpl>,
        dir: &TempDir,
    ) -> (
        tokio::io::DuplexStream,
        tokio::task::JoinHandle<io::Result<()>>,
        Arc<PrimaryReplicationHandler>,
    ) {
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let task = spawn_psync(
            handler.clone(),
            Box::new(server),
            repl_id,
            resume_point as i64,
        );
        let line = read_response_line(&mut client).await;
        assert!(
            line.starts_with("+CONTINUE"),
            "the session must reach streaming, got: {line:?}"
        );
        // `+CONTINUE` is written *before* the streaming phase is entered, so the
        // caller waits for the phase itself rather than for the line.
        for _ in 0..1_000 {
            if tracker.has_streaming_replica() {
                return (client, task, handler);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the session never reached the streaming phase");
    }

    // FM-REPLICATION-062
    /// A replica that closes its socket has *left*, and that is what an orderly
    /// EOF means on the primary side. It is the observable a decommission
    /// produces, and the only one that may drop the self-fence — so it must be
    /// classified apart from every other way a link can end.
    #[tokio::test]
    async fn an_orderly_eof_is_a_graceful_departure() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let (client, task, _handler) = streaming_primary(tracker.clone(), &dir).await;

        drop(client);
        task.await
            .unwrap()
            .expect("an orderly close is not an error");

        assert_eq!(
            tracker.last_streaming_departure(),
            Some(ReplicaDeparture::Graceful),
            "a replica that closed its end departed cleanly"
        );
    }

    /// A stream whose reads always fail: a link that broke rather than closed.
    /// Writes still succeed, so the session reaches streaming before the read
    /// half reports the failure.
    struct ReadErrorStream(tokio::io::DuplexStream);

    impl tokio::io::AsyncRead for ReadErrorStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection reset by peer",
            )))
        }
    }

    impl AsyncWrite for ReadErrorStream {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.0).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    // FM-REPLICATION-062
    /// A reset link is the failure the self-fence exists for. It must never be
    /// confused with the replica closing its end: both end the session and both
    /// unregister it, and the only thing separating "my replica was
    /// decommissioned" from "my replica is unreachable" is this classification.
    #[tokio::test]
    async fn a_read_error_is_a_lost_departure() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("a"), Bytes::from("1")]);

        let (_client, server) = tokio::io::duplex(64 * 1024);
        let task = spawn_psync(
            handler.clone(),
            Box::new(ReadErrorStream(server)),
            repl_id,
            resume_point as i64,
        );
        let _ = task.await.unwrap();

        assert_eq!(
            tracker.last_streaming_departure(),
            Some(ReplicaDeparture::Lost),
            "a reset link is a lost replica, not a departed one"
        );
    }

    // FM-REPLICATION-062
    /// A replica the primary drops for lagging is *unreachable enough to
    /// matter* — the primary gave up on it, it did not leave. Classifying this
    /// as graceful would disarm the fence on the one case where the replica is
    /// demonstrably not keeping up with the writes being fenced.
    #[tokio::test]
    async fn a_lag_disconnect_is_a_lost_departure() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let (_client, task, handler) = streaming_primary(tracker.clone(), &dir).await;

        // Any lag at all is a breach, and the replica never ACKs.
        handler.set_lag_threshold_bytes(1);
        for i in 0..LAG_CHECK_INTERVAL {
            handler.broadcast_control_command(
                "SET",
                &[Bytes::from(format!("k{i}")), Bytes::from("v")],
            );
        }

        let _ = task.await.unwrap();

        assert_eq!(
            tracker.last_streaming_departure(),
            Some(ReplicaDeparture::Lost),
            "the primary dropping a lagging replica is a loss, not a departure"
        );
    }

    // FM-REPLICATION-062
    /// The primary ending its own downstream sessions — shutdown, demotion —
    /// is the other clean ending. The replicas are being sent away on purpose,
    /// so a node that comes back as a primary must not find itself fenced by
    /// the sessions its previous stint closed.
    #[tokio::test]
    async fn a_primary_initiated_disconnect_is_graceful() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let (_client, task, handler) = streaming_primary(tracker.clone(), &dir).await;

        let signalled = handler
            .shutdown_downstream_sessions(Duration::from_secs(5))
            .await;
        assert_eq!(signalled, 1, "the streaming session must be signalled");
        task.await.unwrap().unwrap();

        assert_eq!(
            tracker.last_streaming_departure(),
            Some(ReplicaDeparture::Graceful),
            "sessions this primary closed itself did not go missing"
        );
    }

    // FM-REPLICATION-062
    /// Only a session that reached `Streaming` can arm the self-fence, so only
    /// one that reached `Streaming` may report a departure. A sync that died
    /// half-way through never armed anything, and a departure record from it
    /// would answer for a replica set it was never part of.
    #[tokio::test]
    async fn a_session_that_never_streamed_records_no_departure() {
        let dir = TempDir::new().unwrap();
        let rocks_path = dir.path().join("rocks");
        let store = Arc::new(
            RocksStore::open(&rocks_path, 1, &RocksConfig::default())
                .expect("open rocksdb for test"),
        );
        store.put(0, b"k", b"v").unwrap();

        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), Some(store), dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();

        // A 64-byte duplex: the checkpoint stream blocks long before it is done.
        let (client, server) = tokio::io::duplex(64);
        let session = tracker.register_replica(addr());
        let task = tokio::spawn({
            let session = session.clone();
            let handler = handler.clone();
            let server: BoxedStream = Box::new(server);
            async move {
                session
                    .run(
                        server,
                        SyncKind::Full {
                            replication_id: repl_id,
                        },
                        handler,
                        RecordingFeedAccount::unlimited(),
                    )
                    .await
            }
        });
        drop(client);
        let _ = task.await.unwrap();

        assert_ne!(
            session.phase(),
            Phase::Streaming,
            "this session died during the full sync"
        );
        assert_eq!(
            tracker.last_streaming_departure(),
            None,
            "a session that never streamed must leave the record untouched"
        );
    }

    // FM-REPLICATION-062
    /// A departure describes the replica set *before* the current one. The
    /// moment a replica starts streaming, the previous record stops answering
    /// for it — otherwise a predecessor's clean departure would still be on
    /// record when this replica's link dies, and the fence would read the death
    /// as a decommission.
    #[tokio::test]
    async fn a_new_streaming_generation_clears_the_previous_departure() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());

        // A predecessor left cleanly.
        tracker.record_streaming_departure(ReplicaDeparture::Graceful);

        let (client, task, _handler) = streaming_primary(tracker.clone(), &dir).await;
        assert_eq!(
            tracker.last_streaming_departure(),
            None,
            "a streaming replica cancels the record its predecessor left"
        );

        // ...and this one's own ending is what the record then reports.
        drop(client);
        task.await.unwrap().unwrap();
        assert_eq!(
            tracker.last_streaming_departure(),
            Some(ReplicaDeparture::Graceful)
        );
    }

    // ── FM-REPLICATION-069: the feed's memory accounting ─────────────────────

    /// Like [`spawn_psync`] but with the test's own account, so a test can see
    /// what the feed reported and decide what to answer.
    fn spawn_psync_with_feed(
        handler: Arc<PrimaryReplicationHandler>,
        stream: BoxedStream,
        replication_id: String,
        offset: i64,
        feed: Arc<RecordingFeedAccount>,
    ) -> tokio::task::JoinHandle<io::Result<()>> {
        tokio::spawn(async move {
            handler
                .handle_psync(
                    stream,
                    addr(),
                    &replication_id,
                    offset,
                    ReplicaAnnouncement::default(),
                    feed,
                )
                .await
        })
    }

    /// A primary with a backlog holding `writes` commands, and the offset a
    /// replica would resume from to receive all but the first of them.
    fn primary_with_backlog(
        tracker: &Arc<ReplicationTrackerImpl>,
        dir: &TempDir,
        writes: usize,
    ) -> (Arc<PrimaryReplicationHandler>, String, u64) {
        let handler = make_handler_with_backlog(tracker.clone(), None, dir.path().to_path_buf());
        let repl_id = handler.state.read().replication_id.clone();
        let resume_point =
            handler.broadcast_control_command("SET", &[Bytes::from("k0"), Bytes::from("v0")]);
        for n in 1..writes {
            handler.broadcast_control_command(
                "SET",
                &[
                    Bytes::from(format!("k{n}")),
                    Bytes::from("v".repeat(4 * 1024)),
                ],
            );
        }
        (handler, repl_id, resume_point)
    }

    // FM-REPLICATION-069
    /// The backlog handoff tail is materialized in the primary's memory before a
    /// byte of it is written, and on a resumed replica with a long window that
    /// is the largest thing the feed holds. It must be charged as what it is —
    /// and the charge must come *down* as the tail drains, or a feed would be
    /// condemned for bytes it had already let go of.
    #[tokio::test]
    async fn feed_charges_the_backlog_handoff_tail_and_drains_it() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let (handler, repl_id, resume_point) = primary_with_backlog(&tracker, &dir, 4);

        let feed = RecordingFeedAccount::unlimited();
        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let task = spawn_psync_with_feed(
            handler,
            Box::new(server),
            repl_id,
            resume_point as i64,
            feed.clone(),
        );

        let frames = read_continue_then_frames(&mut client, 3).await;
        assert_eq!(frames.len(), 3, "the whole tail is replayed");
        drop(client);
        let _ = task.await.unwrap();

        let tail_bytes: u64 = frames.iter().map(|f| f.payload.len() as u64).sum();
        assert!(
            feed.peak() >= tail_bytes,
            "the tail the primary staged must be charged in full: peak {} < tail {tail_bytes}",
            feed.peak()
        );
        let reports = feed.reports();
        let peak_at = reports.iter().position(|&r| r == feed.peak()).unwrap();
        assert!(
            reports[peak_at..].contains(&0),
            "the charge must fall back to zero as the tail is written, got {reports:?}"
        );
    }

    // FM-REPLICATION-069
    /// A tail over the `replica` class's hard limit is not written. The feed is
    /// dropped where the bytes are, before they go on the wire — which is the
    /// whole point of a hard limit on staged output.
    #[tokio::test]
    async fn feed_over_the_hard_limit_drops_the_link_at_the_handoff_tail() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let (handler, repl_id, resume_point) = primary_with_backlog(&tracker, &dir, 4);

        // Any tail at all is over this limit.
        let feed = RecordingFeedAccount::shedding_at(1, "hard_limit");
        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let task = spawn_psync_with_feed(
            handler,
            Box::new(server),
            repl_id,
            resume_point as i64,
            feed.clone(),
        );

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");
        let err = task
            .await
            .unwrap()
            .expect_err("a feed over its hard limit must not be served");
        assert!(
            err.to_string().contains("hard_limit"),
            "the shed reason travels into the error the link dies of: {err}"
        );
        assert_eq!(
            tracker.get_all_replicas().len(),
            0,
            "the shed session is gone from the registry"
        );
        assert_eq!(feed.releases(), 1, "and its charge was released");
    }

    // FM-REPLICATION-069
    /// The soft window is a *second* verdict on a feed the first one kept. The
    /// feed must act on it exactly as it acts on a hard limit — a link that
    /// survived its first report and is dropped on a later one is the shape
    /// `client-output-buffer-limit`'s soft seconds produce.
    #[tokio::test]
    async fn feed_over_the_soft_window_drops_the_link() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let (handler, repl_id, resume_point) = primary_with_backlog(&tracker, &dir, 4);

        // Over the mark from the first report, shed on the second: the window
        // expiring, without a clock.
        let feed = RecordingFeedAccount::shedding_after(1, 1, "soft_limit");
        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let task = spawn_psync_with_feed(
            handler,
            Box::new(server),
            repl_id,
            resume_point as i64,
            feed.clone(),
        );

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");
        let err = task
            .await
            .unwrap()
            .expect_err("a feed past its soft window must not be served");
        assert!(
            err.to_string().contains("soft_limit"),
            "the link must die of the window it actually breached: {err}"
        );
        assert!(
            feed.reports().len() >= 2,
            "the window can only expire on a report after the first, got {:?}",
            feed.reports()
        );
    }

    // FM-REPLICATION-069
    /// A full sync to a persistence-disabled primary stages the whole dataset in
    /// memory before it is written. That is the largest single thing a feed ever
    /// holds and the one Redis's `slave` class exists for, so it is charged.
    #[tokio::test]
    async fn feed_charges_the_staged_live_dataset() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        let blob = dataset_blob(&[("alpha", "one"), ("beta", "two")]);
        let staged = blob.len() as u64;
        with_live_dataset(&handler, vec![blob]);
        let repl_id = handler.state.read().replication_id.clone();

        let feed = RecordingFeedAccount::unlimited();
        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let task = spawn_psync_with_feed(handler, Box::new(server), repl_id, -1, feed.clone());

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        let (blobs, _meta) = drain_live_dataset(&mut client).await;
        assert_eq!(blobs.len(), 1);
        drop(client);
        let _ = task.await.unwrap();

        assert_eq!(
            feed.peak(),
            staged,
            "the staged dataset is charged at its real size, got {:?}",
            feed.reports()
        );
    }

    // FM-REPLICATION-069
    /// ...and stops being charged the moment it has been written, rather than
    /// staying on the books until the link ends. A full sync followed by hours
    /// of streaming must not carry the dataset's charge for all of them.
    #[tokio::test]
    async fn feed_releases_the_staged_live_dataset_once_it_is_written() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        let blob = dataset_blob(&[("alpha", "one"), ("beta", "two")]);
        let staged = blob.len() as u64;
        with_live_dataset(&handler, vec![blob]);
        let repl_id = handler.state.read().replication_id.clone();

        let feed = RecordingFeedAccount::unlimited();
        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let task = spawn_psync_with_feed(handler, Box::new(server), repl_id, -1, feed.clone());

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        let _ = drain_live_dataset(&mut client).await;
        drop(client);
        let _ = task.await.unwrap();

        let reports = feed.reports();
        let staged_at = reports
            .iter()
            .position(|&r| r == staged)
            .expect("the dataset was charged");
        assert!(
            reports[staged_at..].contains(&0),
            "the charge is dropped when the payload is, not when the link is: {reports:?}"
        );
    }

    // FM-REPLICATION-069
    /// A dataset the `replica` class will not pay for is refused before it goes
    /// on the wire: the sync fails, the replica reconnects, and the primary is
    /// not left holding a payload for a peer it was never going to serve.
    #[tokio::test]
    async fn feed_over_the_hard_limit_abandons_the_full_sync() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let handler = make_handler(tracker.clone(), None, dir.path().to_path_buf());
        let blob = dataset_blob(&[("alpha", "one"), ("beta", "two")]);
        with_live_dataset(&handler, vec![blob]);
        let repl_id = handler.state.read().replication_id.clone();

        let feed = RecordingFeedAccount::shedding_at(1, "hard_limit");
        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let task = spawn_psync_with_feed(handler, Box::new(server), repl_id, -1, feed.clone());

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+FULLRESYNC"), "got: {line:?}");
        let err = task
            .await
            .unwrap()
            .expect_err("a dataset over the hard limit must not be streamed");
        assert!(
            err.to_string().contains("hard_limit"),
            "the sync fails naming what refused it: {err}"
        );
        assert_eq!(
            tracker.get_all_replicas().len(),
            0,
            "and the session leaves no registry entry behind"
        );
    }

    // FM-REPLICATION-069
    /// Whatever a feed was holding, it stops being charged when the link ends.
    /// The release is on the session's single exit path, so it happens for a
    /// clean departure exactly as for a failed sync.
    #[tokio::test]
    async fn feed_charge_is_released_when_the_link_ends() {
        let dir = TempDir::new().unwrap();
        let tracker = Arc::new(ReplicationTrackerImpl::new());
        let (handler, repl_id, resume_point) = primary_with_backlog(&tracker, &dir, 2);

        let feed = RecordingFeedAccount::unlimited();
        let (mut client, server) = tokio::io::duplex(256 * 1024);
        let task = spawn_psync_with_feed(
            handler,
            Box::new(server),
            repl_id,
            resume_point as i64,
            feed.clone(),
        );

        let line = read_response_line(&mut client).await;
        assert!(line.starts_with("+CONTINUE"), "got: {line:?}");
        assert_eq!(feed.releases(), 0, "a live link holds its charge");

        drop(client);
        task.await.unwrap().unwrap();
        assert_eq!(
            feed.releases(),
            1,
            "the exit path releases exactly once, however the link ended"
        );
    }
}
