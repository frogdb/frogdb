//! The monotonic clock the expiry domain reads.
//!
//! Every deadline the store holds (`KeyMetadata::expires_at`, the hash-field
//! expiry index, `last_access`) is a `std::time::Instant`, and every comparison
//! against those deadlines has to be made against a *now* from the same clock.
//! Reading `std::time::Instant::now()` directly is that same clock — until the
//! process runs under a paused tokio runtime, which is how the turmoil
//! simulation runs every server. There, time advances by fast-forwarding the
//! timer whenever all tasks are idle, so a TTL that is 30 seconds away on the
//! timer's clock is still 30 seconds away on the OS clock a microsecond later:
//! the two clocks disagree about how much time has passed, and which of them a
//! given site happened to read decided whether a key was expired.
//!
//! So the expiry domain reads *here* instead. In a normal build this is
//! `std::time::Instant::now()` with one extra call frame — tokio's `Instant` is
//! a newtype over it and `now()` compiles to the same syscall when the
//! `test-util` feature is off. Under a paused runtime it is the timer's clock,
//! so the whole domain moves together and TTL behaviour under simulation means
//! what the code says it means.
//!
//! The returned value is still a `std::time::Instant` because that is the type
//! the store, the WAL trait, and the persistence formats already speak. It is
//! only meaningful relative to other readings from this function: mixing it
//! with a raw `std::time::Instant::now()` under a paused clock is exactly the
//! bug this module exists to prevent.

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

/// Now, on the clock the expiry domain shares.
pub fn now() -> std::time::Instant {
    tokio::time::Instant::now().into_std()
}

/// How long ago `since` was, on the clock [`now`] reads.
///
/// `Instant::elapsed()` is *not* this function: it is `std::time::Instant::
/// now() - self`, so it reads the OS clock no matter which clock produced
/// `since`. Under a paused runtime the two clocks run at different speeds —
/// virtual time fast-forwards past idle periods while the OS clock crawls —
/// so `clock::now()` + `.elapsed()` mixes timelines and yields a duration
/// that depends on how loaded the host was. Every deadline, freshness check
/// and idle-time reply in the server measures its age through here instead,
/// which is why the clock-seam gate bans `.elapsed()` alongside
/// `Instant::now()`.
///
/// Saturates at zero, as `Instant::elapsed()` does, so a reading taken before
/// the clock was re-anchored cannot panic.
pub fn elapsed(since: Instant) -> Duration {
    now().saturating_duration_since(since)
}

/// Pairs the monotonic reading [`now`] returns with the wall-clock reading it
/// corresponds to, so [`system_now`] can turn later monotonic elapsed time
/// back into a wall-clock value without re-reading the OS clock.
struct SystemEpoch {
    instant: Instant,
    system: SystemTime,
}

/// The anchor [`system_now`] measures elapsed time from. `None` until the
/// first call (or an explicit [`reset_system_epoch`]) latches it.
static SYSTEM_EPOCH: Mutex<Option<SystemEpoch>> = Mutex::new(None);

/// Now, on the wall clock the stream-ID (`XADD *`), claim-idle (`XCLAIM`/
/// `XAUTOCLAIM` `TIME`), and `EXPIRETIME`-style domains share.
///
/// Nothing in `std` or `tokio` offers a virtualizable [`SystemTime`] — tokio's
/// paused clock only covers [`Instant`] (see [`now`]) — so this seam builds
/// one out of the primitive that already *is* virtualized: it latches a
/// `(Instant, SystemTime)` pair the first time it's called (or whenever
/// [`reset_system_epoch`] is), then answers every later call as
/// `latched_system + (now() - latched_instant)`.
///
/// In a normal build this tracks [`SystemTime::now()`] to within the cost of
/// two clock reads, because [`now`] is real time too. Under a paused tokio
/// runtime, [`now`] only advances when the runtime's timer does, so this
/// value only advances then as well — which is what makes `XADD *`'s stream
/// ID, `XCLAIM TIME`'s idle math, and absolute `EXPIRETIME`-style replies
/// drivable from a paused/stepped clock in tests, instead of carrying a real
/// millisecond into a recording that is supposed to be reproducible (see
/// `.scratch/concurrency-testing/issues/17-virtual-wall-clock-for-stream-ids.md`,
/// audit item A15).
pub fn system_now() -> SystemTime {
    let mut guard = SYSTEM_EPOCH.lock().unwrap_or_else(|e| e.into_inner());
    let epoch = guard.get_or_insert_with(|| SystemEpoch {
        instant: now(),
        system: SystemTime::now(),
    });
    let elapsed = now().saturating_duration_since(epoch.instant);
    epoch.system + elapsed
}

/// Re-latch the anchor [`system_now`] measures elapsed time from, pinning its
/// wall-clock half to `system` instead of whatever [`SystemTime::now()`] the
/// next call happens to observe.
///
/// Test-only seam. A single test process runs multiple independent simulated
/// servers back to back (see the turmoil harness), each getting its own fresh
/// paused clock — so [`now`]'s monotonic half naturally restarts per run, but
/// [`system_now`]'s lazily-latched wall-clock half would not: without this,
/// the *first* run's real [`SystemTime::now()`] reading would stay latched
/// for every later run in the same process, and each run's stream IDs /
/// claim times would drift by however much real wall-clock time separated the
/// runs instead of reproducing byte-for-byte. Call this once per simulated
/// run, with the same `system` value every time, before any [`system_now`]
/// read happens on that run's (paused) clock.
pub fn reset_system_epoch(system: SystemTime) {
    let mut guard = SYSTEM_EPOCH.lock().unwrap_or_else(|e| e.into_inner());
    *guard = Some(SystemEpoch {
        instant: now(),
        system,
    });
}

/// Serializes tests (in this crate) that call [`reset_system_epoch`]: it is
/// process-global state, so two such tests running as threads inside one
/// process (as plain `cargo test` — unlike this repo's default `cargo
/// nextest`, which process-isolates every test — would do) could otherwise
/// interleave resets. Hold the guard for the duration of the test; it is an
/// async mutex because the guard spans `tokio::time::advance` awaits.
#[cfg(test)]
pub(crate) fn system_epoch_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    &LOCK
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// `system_now()` advances by exactly the paused clock's virtual
    /// advance, not by whatever real wall-clock time elapsed while the test
    /// executed — the property that makes `XADD *` IDs drivable in a
    /// deterministic test instead of racing the OS clock.
    #[tokio::test(start_paused = true)]
    async fn system_now_tracks_the_paused_clock() {
        let _guard = system_epoch_test_lock().lock().await;
        reset_system_epoch(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000));

        let first = system_now();
        assert_eq!(
            first,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
        );

        tokio::time::advance(Duration::from_secs(5)).await;

        let second = system_now();
        assert_eq!(second, first + Duration::from_secs(5));
    }

    /// Two independent "runs" (re-latched via `reset_system_epoch`, exactly
    /// as the turmoil harness does per simulated server) that each advance
    /// their paused clock the same way produce byte-identical `system_now()`
    /// values — the reproducibility property issue 17 exists to establish.
    #[tokio::test(start_paused = true)]
    async fn system_now_reproduces_across_resets() {
        let _guard = system_epoch_test_lock().lock().await;
        let epoch = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);

        reset_system_epoch(epoch);
        tokio::time::advance(Duration::from_millis(1234)).await;
        let run_a = system_now();

        reset_system_epoch(epoch);
        tokio::time::advance(Duration::from_millis(1234)).await;
        let run_b = system_now();

        assert_eq!(run_a, run_b);
    }

    /// [`elapsed`] measures against the *paused* clock: burning real time
    /// without advancing the runtime's timer must not move it, and advancing
    /// the timer must move it by exactly that amount. This is the property
    /// `Instant::elapsed()` does not have — it subtracts from
    /// `std::time::Instant::now()`, i.e. the OS clock, whatever clock produced
    /// the anchor — which is how host load reached a turmoil trace (see
    /// `.scratch/cluster-correctness/issues/done/23-scheduler-fingerprint-is-load-dependent.md`).
    #[tokio::test(start_paused = true)]
    async fn elapsed_tracks_the_paused_clock_not_the_os_clock() {
        let anchor = now();

        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(elapsed(anchor), Duration::ZERO);

        tokio::time::advance(Duration::from_secs(90)).await;
        assert_eq!(elapsed(anchor), Duration::from_secs(90));
    }
}
