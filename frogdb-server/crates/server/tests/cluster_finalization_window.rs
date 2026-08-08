//! Measurement harness for the slot-migration finalization **residual window**
//! (rework issue 02, sequencing step 1).
//!
//! `CLUSTER SETSLOT <slot> NODE <target>` finalizes a migration by proposing
//! `ClusterCommand::CompleteSlotMigration` to Raft
//! (`cluster/src/writer.rs` → `cluster/src/commands.rs`). The entry commits on
//! the Raft leader, and every other node applies it once an `AppendEntries`
//! carries the new commit index to it. Between those two instants the
//! *losing* (source) node's published `ClusterSnapshot` still names itself the
//! slot's owner, so `route_with_snapshot` answers `LocalServe` /
//! `LocalServeMigrating` and the node validates and serves — and acknowledges —
//! a write for a slot the cluster has already handed away.
//!
//! This file measures that window, and — since phase 2b — asserts the one
//! property the barrier was built to deliver.
//!
//! - The **measurement** cases sweep timings and load, record latencies and
//!   print a table. They are `#[ignore]`d, because a table is not a verdict:
//!
//!   ```text
//!   cargo test -p frogdb-server --test cluster_finalization_window -- \
//!       --ignored --nocapture --test-threads=1
//!   ```
//!
//! - The **acceptance** case
//!   ([`no_write_is_acknowledged_after_the_slot_is_handed_over_under_load`])
//!   runs the same loaded scenario and asserts the criterion: across at least
//!   120 finalizations under load, *zero* of them acknowledged a write on the
//!   source after the cluster had committed the handoff. It is not `#[ignore]`d.
//!
//! The 2026-08-05 pre-barrier results and the build-vs-accept recommendation
//! they support live in `.scratch/replication-cluster-rework/`
//! `finalization-window-measurement-2026-08-05.md`. That run measured 118 of 120
//! loaded finalizations acking a write past the commit; the acceptance case
//! exists so the number cannot drift back up unnoticed.
//!
//! Everything here is test-only. No production code is instrumented: the
//! per-node `Arc<ClusterState>` the harness already exposes
//! (`ClusterTestNode::cluster_state`) is polled from a dedicated OS thread, so
//! the apply instant is observed rather than reported.
//!
//! Four instants are captured per finalization:
//!
//! | Instant | How |
//! |---|---|
//! | `t_ack` | `CLUSTER SETSLOT … NODE` returns `+OK` to the admin client |
//! | `t_leader` | the *leader's* `ClusterState` names the target as owner |
//! | `t_source` | the *source's* `ClusterState` names the target as owner |
//! | `t_target` | the *target's* `ClusterState` names the target as owner |
//!
//! and one behavioral instant: `t_last_ok`, the last `SET` on a key of the
//! migrating slot that the **source** answered `+OK` while a prober hammered it
//! across the handover. `t_last_ok - t_leader > 0` is an acknowledged write that
//! landed on the former owner after the cluster had committed the handoff.
#![cfg(not(feature = "turmoil"))]

use frogdb_core::ClusterState;
use frogdb_test_harness::cluster_harness::{ClusterNodeConfig, ClusterTestHarness};
use frogdb_test_harness::cluster_helpers::{is_error, slot_for_key};
use frogdb_test_harness::server::TestClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

/// `a - b` in microseconds, signed (`Instant` subtraction panics when negative).
fn signed_us(a: Instant, b: Instant) -> f64 {
    if a >= b {
        (a - b).as_nanos() as f64 / 1000.0
    } else {
        -((b - a).as_nanos() as f64 / 1000.0)
    }
}

/// Nearest-rank percentile over an unsorted sample set.
fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = (p * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

/// One key per slot, computed once for the whole test binary.
///
/// `cluster_helpers::key_for_slot` rescans up to 100k candidate keys per call;
/// at ~500 calls per run that dominates the wall clock, so the table is built
/// in a single pass instead.
fn slot_keys() -> &'static HashMap<u16, String> {
    static TABLE: OnceLock<HashMap<u16, String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table: HashMap<u16, String> = HashMap::with_capacity(16384);
        let mut i = 0u32;
        while table.len() < 16384 && i < 2_000_000 {
            let key = format!("fw{i}");
            table.entry(slot_for_key(key.as_bytes())).or_insert(key);
            i += 1;
        }
        table
    })
}

fn key_for(slot: u16) -> &'static str {
    slot_keys()
        .get(&slot)
        .expect("every slot has a key in the table")
}

// ---------------------------------------------------------------------------
// Apply-instant observation
// ---------------------------------------------------------------------------

/// Watch one node's `ClusterState` until `slot` is owned by `want`, returning
/// the instant that first became true.
///
/// Runs on its own OS thread so it neither occupies a tokio worker nor is
/// descheduled behind the server's own tasks. It spins (yielding) for the first
/// few milliseconds — where the interesting resolution is — then falls back to
/// short sleeps so a long wait costs no CPU.
///
/// `started` is incremented once the thread is live; the caller waits on it so
/// thread-spawn latency is never charged to the measured window.
fn watch_owner_flip(
    state: Arc<ClusterState>,
    slot: u16,
    want: u64,
    timeout: Duration,
    started: Arc<AtomicUsize>,
) -> std::thread::JoinHandle<Option<Instant>> {
    std::thread::spawn(move || {
        started.fetch_add(1, Ordering::SeqCst);
        let begin = Instant::now();
        loop {
            if state.get_slot_owner(slot) == Some(want) {
                return Some(Instant::now());
            }
            let waited = begin.elapsed();
            if waited >= timeout {
                return None;
            }
            if waited < Duration::from_millis(5) {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(200));
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Behavioral prober: hammer the source with writes across the handover
// ---------------------------------------------------------------------------

/// Independent client connections hammering the source across the handover.
const PROBER_CONNECTIONS: usize = 8;

struct ProbeRequest {
    key: String,
    reply: oneshot::Sender<ProbeResult>,
}

#[derive(Debug, Default)]
struct ProbeResult {
    /// Last `+OK` the source answered for a write on the migrating slot.
    last_ok: Option<Instant>,
    /// First non-OK (i.e. `-MOVED`) reply — the source has applied the handoff.
    first_reject: Option<Instant>,
    /// Writes acknowledged during this iteration.
    ok_count: u64,
    /// Summed round-trip time of every `SET` issued, and how many were issued.
    /// Their ratio is the prober's temporal resolution: `last_ok` can trail the
    /// true last acked write by up to one round trip.
    rtt_sum_us: f64,
    attempts: u64,
}

impl ProbeResult {
    /// Fold a second prober's result into this one. `last_ok` is the latest of
    /// the two (the concern is the *last* write the source acked, from any
    /// connection); counters add.
    fn merge(&mut self, other: ProbeResult) {
        self.last_ok = match (self.last_ok, other.last_ok) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        self.first_reject = match (self.first_reject, other.first_reject) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.ok_count += other.ok_count;
        self.rtt_sum_us += other.rtt_sum_us;
        self.attempts += other.attempts;
    }
}

/// Drive `SET key <n>` against the source until it stops answering `+OK`.
///
/// The terminator is the handover itself: once the source applies
/// `CompleteSlotMigration` its snapshot names the target, so routing answers
/// `-MOVED` and the loop ends. A hard deadline keeps a stuck run bounded.
async fn run_prober(mut client: TestClient, mut rx: mpsc::Receiver<ProbeRequest>) {
    let mut seq = 0u64;
    while let Some(req) = rx.recv().await {
        let mut result = ProbeResult::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            seq += 1;
            let value = seq.to_string();
            let sent = Instant::now();
            let resp = client.command(&["SET", &req.key, &value]).await;
            let now = Instant::now();
            result.attempts += 1;
            result.rtt_sum_us += signed_us(now, sent);
            if is_error(&resp) {
                result.first_reject = Some(now);
                break;
            }
            result.last_ok = Some(now);
            result.ok_count += 1;
            if now >= deadline {
                break;
            }
        }
        let _ = req.reply.send(result);
    }
}

// ---------------------------------------------------------------------------
// Scenario definition
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    /// Raft heartbeat interval (ms). 250 is the shipped default
    /// (`config::cluster::DEFAULT_HEARTBEAT_INTERVAL_MS`); 100 is the harness
    /// default used by every other cluster test.
    heartbeat_ms: u64,
    /// Raft election timeout (ms). 1000 is the shipped default.
    election_ms: u64,
    /// Finalizations to measure.
    iterations: usize,
    /// Background write connections against the source node.
    load_writers: usize,
    /// Whether an extra task keeps the Raft log busy with unrelated entries.
    raft_churn: bool,
    /// Migrate slots *off the Raft leader* instead of off a follower. The
    /// follower case is the common one (the source is the leader with
    /// probability 1/N); the leader case is the control.
    source_is_leader: bool,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    /// `t_source - t_leader`: the residual window, state-machine definition.
    window_us: f64,
    /// `t_source - t_ack`: the residual window as the admin client sees it.
    ack_window_us: f64,
    /// `t_source - t_target`: how long both nodes claim the slot.
    overlap_us: f64,
    /// `t_last_ok - t_leader`: acknowledged-write exposure past the commit.
    /// `None` when the prober recorded no `+OK` at all.
    write_exposure_us: Option<f64>,
    /// Writes the source acknowledged during the iteration.
    probe_ok_count: u64,
    /// Mean `SET` round trip during the iteration — the resolution floor of
    /// `write_exposure_us` divided by the number of prober connections.
    probe_rtt_us: f64,
}

struct Stats {
    n: usize,
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
}

fn stats(mut values: Vec<f64>) -> Stats {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Stats {
        n: values.len(),
        p50: pct(&values, 0.50),
        p90: pct(&values, 0.90),
        p99: pct(&values, 0.99),
        max: values.last().copied().unwrap_or(f64::NAN),
    }
}

// ---------------------------------------------------------------------------
// The measurement
// ---------------------------------------------------------------------------

async fn measure(scenario: Scenario) -> Vec<Sample> {
    let iterations = std::env::var("FROGDB_FINALIZATION_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(scenario.iterations);

    let mut harness = ClusterTestHarness::with_config(ClusterNodeConfig {
        num_shards: Some(4),
        election_timeout_ms: scenario.election_ms,
        heartbeat_interval_ms: scenario.heartbeat_ms,
        ..Default::default()
    });
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(30))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(60))
        .await
        .unwrap();

    let leader = harness.get_leader().await.expect("leader");
    let followers: Vec<u64> = harness
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();

    // The source is the node that loses the slot — the node whose residual
    // window this file exists to measure.
    let (source, target) = if scenario.source_is_leader {
        (leader, followers[0])
    } else {
        (followers[0], followers[1])
    };

    let leader_state = harness
        .node(leader)
        .unwrap()
        .cluster_state()
        .unwrap()
        .clone();
    let source_state = harness
        .node(source)
        .unwrap()
        .cluster_state()
        .unwrap()
        .clone();
    let target_state = harness
        .node(target)
        .unwrap()
        .cluster_state()
        .unwrap()
        .clone();

    // Slots the source currently owns, per the leader's applied view.
    let owned: Vec<u16> = (0u16..16384)
        .filter(|slot| leader_state.get_slot_owner(*slot) == Some(source))
        .collect();
    assert!(
        owned.len() > iterations + 64,
        "source owns {} slots, need {} + reserve",
        owned.len(),
        iterations
    );
    // Migrated slots come from the front, load slots from the back, so the
    // background writers never touch a slot that is changing hands.
    let migrate_slots: Vec<u16> = owned.iter().copied().take(iterations).collect();
    let load_slots: Vec<u16> = owned.iter().rev().copied().take(64).collect();

    let source_hex = harness.get_node_id_str(source).unwrap();
    let target_hex = harness.get_node_id_str(target).unwrap();

    // --- background load -------------------------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let mut load_tasks = Vec::new();
    for w in 0..scenario.load_writers {
        let mut client = harness.node(source).unwrap().connect().await;
        let stop = stop.clone();
        let keys: Vec<String> = load_slots
            .iter()
            .map(|slot| key_for(*slot).to_string())
            .collect();
        load_tasks.push(tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let key = &keys[(i + w) % keys.len()];
                let _ = client.command(&["SET", key, "load"]).await;
                i += 1;
            }
        }));
    }

    if scenario.raft_churn {
        // `SETSLOT <slot> STABLE` proposes a `CancelSlotMigration` for a slot
        // with no migration: a real, replicated, harmless Raft entry. It keeps
        // the log (and every node's apply loop) busy without perturbing slot
        // ownership.
        let mut client = harness.node(leader).unwrap().connect().await;
        let stop = stop.clone();
        let churn_slot = load_slots[0].to_string();
        load_tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let _ = client
                    .command(&["CLUSTER", "SETSLOT", &churn_slot, "STABLE"])
                    .await;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }));
    }

    // --- behavioral probers ------------------------------------------------
    // A single serial prober resolves the acked-write exposure only to one
    // round trip, which is the same order as the window being measured.
    // `PROBER_CONNECTIONS` independent connections cut the inter-arrival gap by
    // that factor, so `last_ok` lands much closer to the true final ack.
    let mut probe_txs = Vec::with_capacity(PROBER_CONNECTIONS);
    let mut probers = Vec::with_capacity(PROBER_CONNECTIONS);
    for _ in 0..PROBER_CONNECTIONS {
        let (probe_tx, probe_rx) = mpsc::channel::<ProbeRequest>(1);
        let prober_client = harness.node(source).unwrap().connect().await;
        probers.push(tokio::spawn(run_prober(prober_client, probe_rx)));
        probe_txs.push(probe_tx);
    }

    // --- admin connection -------------------------------------------------
    let mut admin = harness.node(leader).unwrap().connect().await;

    let mut samples = Vec::with_capacity(iterations);
    let mut misses = 0usize;

    for &slot in &migrate_slots {
        let slot_str = slot.to_string();
        let key = key_for(slot).to_string();

        // Seed the key on the source *before* the migration opens: a key that
        // is absent on a MIGRATING source answers `-ASK`, so only a resident
        // key exercises the "validates and serves" arm.
        let seeded = admin_set_on(&harness, source, &key).await;
        if !seeded {
            misses += 1;
            continue;
        }

        // Open the migration.
        let begin = admin
            .command(&[
                "CLUSTER",
                "SETSLOT",
                &slot_str,
                "IMPORTING",
                &source_hex,
                &target_hex,
            ])
            .await;
        if is_error(&begin) {
            misses += 1;
            continue;
        }

        // Wait for all three nodes to have applied the open, so the finalize is
        // measured from a settled starting state.
        if !wait_for_migration(&[&leader_state, &source_state, &target_state], slot).await {
            misses += 1;
            continue;
        }

        // Arm the three apply watchers and wait until all are live.
        let started = Arc::new(AtomicUsize::new(0));
        let timeout = Duration::from_secs(10);
        let w_leader =
            watch_owner_flip(leader_state.clone(), slot, target, timeout, started.clone());
        let w_source =
            watch_owner_flip(source_state.clone(), slot, target, timeout, started.clone());
        let w_target =
            watch_owner_flip(target_state.clone(), slot, target, timeout, started.clone());
        while started.load(Ordering::SeqCst) < 3 {
            std::thread::yield_now();
        }

        // Start hammering the source with writes on the migrating slot.
        let mut reply_rxs = Vec::with_capacity(probe_txs.len());
        for probe_tx in &probe_txs {
            let (reply_tx, reply_rx) = oneshot::channel();
            probe_tx
                .send(ProbeRequest {
                    key: key.clone(),
                    reply: reply_tx,
                })
                .await
                .expect("prober alive");
            reply_rxs.push(reply_rx);
        }

        // Finalize.
        let resp = admin
            .command(&["CLUSTER", "SETSLOT", &slot_str, "NODE", &target_hex])
            .await;
        let t_ack = Instant::now();

        let t_leader = w_leader.join().unwrap();
        let t_source = w_source.join().unwrap();
        let t_target = w_target.join().unwrap();
        let mut probe = ProbeResult::default();
        for reply_rx in reply_rxs {
            probe.merge(reply_rx.await.unwrap_or_default());
        }

        if is_error(&resp) {
            misses += 1;
            continue;
        }
        let (Some(t_leader), Some(t_source), Some(t_target)) = (t_leader, t_source, t_target)
        else {
            misses += 1;
            continue;
        };

        samples.push(Sample {
            window_us: signed_us(t_source, t_leader),
            ack_window_us: signed_us(t_source, t_ack),
            overlap_us: signed_us(t_source, t_target),
            write_exposure_us: probe.last_ok.map(|t| signed_us(t, t_leader)),
            probe_ok_count: probe.ok_count,
            probe_rtt_us: if probe.attempts > 0 {
                probe.rtt_sum_us / probe.attempts as f64
            } else {
                f64::NAN
            },
        });
    }

    stop.store(true, Ordering::Relaxed);
    drop(probe_txs);
    for prober in probers {
        let _ = prober.await;
    }
    for task in load_tasks {
        let _ = task.await;
    }
    harness.shutdown_all().await;

    if misses > 0 {
        eprintln!(
            "scenario {}: {misses} iteration(s) discarded (setup error or watcher timeout)",
            scenario.name
        );
    }
    samples
}

/// `SET` a key directly on `node`, returning whether it was accepted.
async fn admin_set_on(harness: &ClusterTestHarness, node: u64, key: &str) -> bool {
    let resp = harness
        .node(node)
        .unwrap()
        .send("SET", &[key, "seed"])
        .await;
    !is_error(&resp)
}

/// Poll until every listed state has applied the open migration for `slot`.
async fn wait_for_migration(states: &[&Arc<ClusterState>], slot: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if states.iter().all(|s| s.is_slot_migrating(slot)) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn report(scenario: &Scenario, samples: &[Sample]) {
    let window = stats(samples.iter().map(|s| s.window_us).collect());
    let ack = stats(samples.iter().map(|s| s.ack_window_us).collect());
    let overlap = stats(samples.iter().map(|s| s.overlap_us).collect());
    let exposure = stats(
        samples
            .iter()
            .filter_map(|s| s.write_exposure_us)
            .filter(|v| *v > 0.0)
            .collect(),
    );
    let exposed = samples
        .iter()
        .filter(|s| s.write_exposure_us.is_some_and(|v| v > 0.0))
        .count();
    let acked: u64 = samples.iter().map(|s| s.probe_ok_count).sum();

    println!("\n### scenario: {}", scenario.name);
    println!(
        "heartbeat={}ms election={}ms writers={} raft_churn={} source={} n={}",
        scenario.heartbeat_ms,
        scenario.election_ms,
        scenario.load_writers,
        scenario.raft_churn,
        if scenario.source_is_leader {
            "leader"
        } else {
            "follower"
        },
        window.n,
    );
    println!();
    println!("| metric | n | p50 (µs) | p90 (µs) | p99 (µs) | max (µs) |");
    println!("|---|---|---|---|---|---|");
    for (label, s) in [
        ("residual window (t_source − t_leader)", &window),
        ("client-visible (t_source − t_ack)", &ack),
        ("dual-ownership (t_source − t_target)", &overlap),
        ("acked-write exposure (t_last_ok − t_leader)", &exposure),
    ] {
        println!(
            "| {label} | {} | {:.1} | {:.1} | {:.1} | {:.1} |",
            s.n, s.p50, s.p90, s.p99, s.max
        );
    }
    let rtt = stats(
        samples
            .iter()
            .map(|s| s.probe_rtt_us)
            .filter(|v| v.is_finite())
            .collect(),
    );
    println!();
    println!(
        "iterations with an acknowledged write after commit: {exposed}/{} ({} writes acked in total across all iterations)",
        window.n, acked
    );
    println!(
        "prober: {PROBER_CONNECTIONS} connections, mean SET round trip p50={:.1}µs p99={:.1}µs \
         → exposure resolution ≈ {:.1}µs",
        rtt.p50,
        rtt.p99,
        rtt.p50 / PROBER_CONNECTIONS as f64
    );
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

const FOLLOWER_SOURCE_HARNESS_TIMING: Scenario = Scenario {
    name: "follower-source, idle, harness timing (hb=100ms)",
    heartbeat_ms: 100,
    election_ms: 300,
    iterations: 120,
    load_writers: 0,
    raft_churn: false,
    source_is_leader: false,
};

const FOLLOWER_SOURCE_SHIPPED_TIMING: Scenario = Scenario {
    name: "follower-source, idle, shipped timing (hb=250ms)",
    heartbeat_ms: 250,
    election_ms: 1000,
    iterations: 120,
    load_writers: 0,
    raft_churn: false,
    source_is_leader: false,
};

const FOLLOWER_SOURCE_LOADED: Scenario = Scenario {
    name: "follower-source, loaded (32 writers + Raft churn), shipped timing",
    heartbeat_ms: 250,
    election_ms: 1000,
    iterations: 120,
    load_writers: 32,
    raft_churn: true,
    source_is_leader: false,
};

const LEADER_SOURCE_SHIPPED_TIMING: Scenario = Scenario {
    name: "leader-source (control), idle, shipped timing",
    heartbeat_ms: 250,
    election_ms: 1000,
    iterations: 120,
    load_writers: 0,
    raft_churn: false,
    source_is_leader: true,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement harness: run explicitly with --ignored --nocapture"]
async fn measure_finalization_window_follower_source_harness_timing() {
    let samples = measure(FOLLOWER_SOURCE_HARNESS_TIMING).await;
    report(&FOLLOWER_SOURCE_HARNESS_TIMING, &samples);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement harness: run explicitly with --ignored --nocapture"]
async fn measure_finalization_window_follower_source_shipped_timing() {
    let samples = measure(FOLLOWER_SOURCE_SHIPPED_TIMING).await;
    report(&FOLLOWER_SOURCE_SHIPPED_TIMING, &samples);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement harness: run explicitly with --ignored --nocapture"]
async fn measure_finalization_window_follower_source_loaded() {
    let samples = measure(FOLLOWER_SOURCE_LOADED).await;
    report(&FOLLOWER_SOURCE_LOADED, &samples);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement harness: run explicitly with --ignored --nocapture"]
async fn measure_finalization_window_leader_source() {
    let samples = measure(LEADER_SOURCE_SHIPPED_TIMING).await;
    report(&LEADER_SOURCE_SHIPPED_TIMING, &samples);
}

// ---------------------------------------------------------------------------
// Acceptance
// ---------------------------------------------------------------------------

/// How many loaded finalizations the acceptance claim rests on.
///
/// The residual window is a race, and a race that does not fire in ten attempts
/// has not been closed — it has been under-sampled. 120 is the sample size the
/// 2026-08-05 measurement used to establish the baseline (118/120 exposed), so
/// the acceptance case is directly comparable to it.
const REQUIRED_FINALIZATIONS: usize = 120;

/// The acceptance scenario: [`FOLLOWER_SOURCE_LOADED`]'s load profile, with
/// headroom.
///
/// The extra iterations exist because [`measure`] discards an iteration whose
/// setup errors or whose apply watcher times out. Discards are not evidence
/// about the barrier either way, so they must not be able to shrink the sample
/// below [`REQUIRED_FINALIZATIONS`] — and equally must not be able to *pass* the
/// case by shrinking it to nothing, which is why the sample size is asserted.
const LOADED_ACCEPTANCE: Scenario = Scenario {
    name: "follower-source, loaded (32 writers + Raft churn), ACCEPTANCE",
    heartbeat_ms: 250,
    election_ms: 1000,
    iterations: REQUIRED_FINALIZATIONS + 12,
    load_writers: 32,
    raft_churn: true,
    source_is_leader: false,
};

/// **The** acceptance criterion for the slot-migration finalization barrier.
///
/// Under sustained write load and Raft churn, migrating a slot off a *follower*
/// — the hard case, where the source learns of the commit a full replication lag
/// after the leader does — must never leave a write acknowledged by the former
/// owner after the cluster committed the handoff.
///
/// This is a behavioral claim, not a latency one. The residual window may still
/// be non-zero (`t_source - t_leader` is a replication lag and no barrier can
/// erase it); what the barrier and the execute-seam fence together guarantee is
/// that nothing *client-visible* happens inside it. A write that the fence
/// refuses may still have applied locally — it is simply never acknowledged, so
/// its status is in doubt and the client's retry lands on the new owner, which
/// is the ordinary at-least-once contract rather than a lost write.
///
/// Baseline before the barrier: 118 of 120 loaded finalizations acked a write
/// past the commit. Required now: zero.
///
/// The measurement errs towards *over*-reporting exposure, never under:
/// `t_last_ok` is when the prober's client observed the `+OK`, which is strictly
/// after the source emitted it, while `t_leader` is read straight off the
/// leader's applied state. A starved machine can therefore turn a write the
/// source acked before the handoff into a false positive, and the margin that
/// keeps it from doing so is the several milliseconds the drain round trip and
/// two Raft commits take. A false *negative* has no such mechanism, which is the
/// direction that matters for an acceptance case.
// FM-CLUSTER-095
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn no_write_is_acknowledged_after_the_slot_is_handed_over_under_load() {
    let samples = measure(LOADED_ACCEPTANCE).await;
    report(&LOADED_ACCEPTANCE, &samples);

    assert!(
        samples.len() >= REQUIRED_FINALIZATIONS,
        "only {} of {} finalizations were measured; the criterion needs at least \
         {REQUIRED_FINALIZATIONS} samples to mean anything",
        samples.len(),
        LOADED_ACCEPTANCE.iterations,
    );

    // Guard against a vacuous pass: if the source refused *every* write on the
    // migrating slot the exposure count would also be zero, but for the wrong
    // reason — the barrier would be an availability outage, not a correctness
    // fix.
    //
    // The threshold is a supermajority of iterations rather than all of them.
    // Under 32 writers the probers' `SET` round trip runs to a few milliseconds
    // while a whole finalization takes under one, so an occasional iteration
    // legitimately hands over before any of the eight probers' first write comes
    // back. That is prober jitter, not a refusal; a real outage does not shave a
    // percent off this count, it collapses it to zero.
    let served = samples.iter().filter(|s| s.probe_ok_count > 0).count();
    assert!(
        served * 10 >= samples.len() * 9,
        "only {served} of {} finalizations acknowledged any write before the \
         handoff; the source must keep serving its own slot right up to the \
         handover, otherwise zero exposure is an outage rather than a barrier",
        samples.len(),
    );

    let exposed: Vec<String> = samples
        .iter()
        .enumerate()
        .filter_map(|(i, s)| {
            let us = s.write_exposure_us?;
            (us > 0.0).then(|| format!("#{i}: +{us:.1}µs after commit"))
        })
        .collect();
    assert!(
        exposed.is_empty(),
        "{} of {} loaded finalizations acknowledged a write on the former owner \
         after the cluster committed the handoff: {}",
        exposed.len(),
        samples.len(),
        exposed.join(", "),
    );
}
