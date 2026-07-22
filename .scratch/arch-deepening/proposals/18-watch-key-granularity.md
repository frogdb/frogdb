# Proposal 18 — WATCH version: give the key-shaped interface key-granular behavior (or rename it honest)

## Summary

`ShardWorker::get_key_version(&self, _key: &[u8])` (`worker.rs:456-459`) takes a key and **ignores
it**, returning the single per-`Internal Shard` `shard_version` counter. `check_watches`
(`worker.rs:462-466`) compares every watched `(key, version)` against that one number, and *every
dirtying* write on the shard bumps it (`increment_version`, `worker.rs:452-454`, driven from the
`VersionIncrement` write-effect at `post_execution.rs:196-208` plus active-expiry, eviction, and
blocking-wake sites). ("Dirtying" is precise: for `EffectScope::Command`/`ScatterPart` the bump is
gated on `summary.dirty_delta >= 0` — the WATCH no-op rule — so a net-negative write does not bump;
`EffectScope::Transaction` bumps unconditionally, once per EXEC.) The consequence, verified below and
already characterized by an existing test:
**`WATCH a` aborts `EXEC` when *any* key on the same Internal Shard is written**, watched or not — a
spurious `WatchAborted` that scales with shard write load and is invisible at the call sites. The
ignored `_key` parameter is the tell: a key-shaped **Interface** over shard-shaped **behavior**.

The core premise holds, but two of the candidate's framings need correction (see
*Evidence discrepancies* and *Test plan*): the coarse behavior is **not an accidental bug** — it is
a *deliberately pinned characterization* (`scenario_s2.rs` asserts the over-abort is *required* to be
observable), and the "test win" is therefore a test *flip*, not a newly-possible assertion. This
proposal presents the per-slot deepening as the primary design, the Redis-style per-key
`watched_keys` map as the precise-but-heavier alternative, and an honest **WONTFIX** rename+ADR as
the legitimate low-effort outcome — because the coarseness is a conscious latency/simplicity trade,
not an oversight.

## Files involved (verified paths + line counts)

| File | Lines | Role in the current design |
| --- | --- | --- |
| `frogdb-server/crates/core/src/shard/worker.rs` | — | `shard_version` field (L52-53); `increment_version` (L452-454); `get_key_version(&self, _key)` — **ignores the key** (L456-459); `check_watches` (L462-466); `purge_expired_watches` (L481-490) |
| `frogdb-server/crates/core/src/shard/post_execution.rs` | — | `WriteEffectKind::VersionIncrement` seam (L124-125); `WRITE_EFFECT_ORDER` (L156-166); scope-dependent bump (L196-208); `WriteSummary.writes: &[WriteRecord]` (L69-81); `invalidate_written_keys` — the proven per-key sibling (L396-413) |
| `frogdb-server/crates/core/src/shard/execution.rs` | — | `execute_transaction` (L438-464): `purge_expired_watches(watches)` then `check_watches(watches)` at L459-462 |
| `frogdb-server/crates/core/src/shard/dispatch_core.rs` | — | `GetVersion { keys, response_tx }` handler: purges expired watched keys, returns `self.shard_version` (L59-71) |
| `frogdb-server/crates/core/src/shard/message.rs` | — | `GetVersion { keys, response_tx: oneshot<u64> }` (L143-157); `ExecTransaction { … watches: Vec<(Bytes,u64)> … }` (L159-168) |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | — | active-expiry sweep: `increment_version()` once per non-empty sweep (L229) |
| `frogdb-server/crates/core/src/shard/eviction.rs` | — | per-key spill (L221) and delete (L279) bumps — already hold the evicted key |
| `frogdb-server/crates/core/src/shard/blocking.rs` | — | waiter-wake bump, gated by `strat.bumps_version()` (L272) |
| `frogdb-server/crates/core/src/lib.rs` | — | exports `slot_for_key`, `shard_for_key`, `extract_hash_tag` (L143) — the slot helper the design needs |
| `frogdb-server/crates/server/src/connection/transaction.rs` | — | EXEC path: cross-shard watch set → `Multi` target → **CROSSSLOT reject** (L228-242); single-shard round-trip `run_shard_transaction` (L318-339) |
| `frogdb-server/crates/server/src/connection/transaction_conn_command.rs` | — | WATCH → `GetVersion` round-trip per owning shard (L308); folds shards at `take_transaction` |
| `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs` | 177 | **Pins the coarse over-abort as required** (`s2_zero_false_negatives_and_over_abort_characterized`, `over_aborts >= 1`) + F3 lazy-expiry abort |
| `frogdb-server/crates/core/tests/shard_driver/scenario_s8.rs` | 177 | Prop test built *around* the coarse behavior (third key `w`, schedule-dependent abort from unrelated writes) |
| `frogdb-server/CONTEXT.md` | — | Canonical terms: **Internal Shard** (L16), **Hash Slot** (16384, L20), **Hash Tag** colocation (L25) |

## Problem (concrete verified evidence)

### The interface is key-shaped; the behavior is shard-shaped

```rust
// worker.rs:456-459
pub(crate) fn get_key_version(&self, _key: &[u8]) -> u64 {
    self.shard_version
}

// worker.rs:462-466
pub(crate) fn check_watches(&self, watches: &[(Bytes, u64)]) -> bool {
    watches
        .iter()
        .all(|(key, watched_ver)| self.get_key_version(key) == *watched_ver)
}
```

`check_watches` iterates per-key and calls `get_key_version(key)` per key, presenting a per-key
contract. But every call returns the same `self.shard_version`, so the per-key loop is arithmetic
theater: all watched keys on a shard share one version, and `all(...)` reduces to "did the shard
version move at all since watch time."

### Every dirtying write on the shard moves that one number

`increment_version` (`worker.rs:452-454`) is called from **five** sites, and none of them is
key-scoped:

- `post_execution.rs:196-208` — the `VersionIncrement` write-effect, for a plain command, a scatter
  part, or a whole `EffectScope::Transaction` (one bump per EXEC). Fires for *any* dirtying key: a
  `Command`/`ScatterPart` bump is gated on `summary.dirty_delta >= 0` (a net-negative write is a
  WATCH no-op and does not bump), while `EffectScope::Transaction` bumps unconditionally.
- `event_loop.rs:229` — one bump per non-empty active-expiry sweep, regardless of which keys expired.
- `eviction.rs:221` (spill) and `eviction.rs:279` (delete) — one bump per evicted key.
- `blocking.rs:272` — one bump per waiter wake whose strategy `bumps_version()`.

So `WATCH a; SET b` (with `a`, `b` on the same Internal Shard) bumps the version via the `SET b`
write-effect, and the subsequent `EXEC` fails `check_watches` and returns `TransactionResult::WatchAborted`
(`execution.rs:462-463`) — even though `a` never changed.

### This is confirmed and already characterized — as *required* behavior

`scenario_s2.rs` (module doc L1-5) states it outright:

> "WATCH is a per-shard version (worker.rs:459-469): any same-shard write or active-expiry removal
> bumps the version and aborts EXEC. Invariant: zero false negatives … Over-aborts are legal and
> characterized."

Its `Interleave::UnrelatedWrite` case (`SET u` while watching `k`, L40-42) and
`Interleave::ActiveExpiryUnrelated` case both over-abort, and the test **asserts the over-abort must
be observable**:

```rust
// scenario_s2.rs — s2_zero_false_negatives_and_over_abort_characterized
assert!(
    over_aborts >= 1,
    "expected the per-shard-version over-abort to be observable"
);
```

`scenario_s8.rs` goes further and *depends structurally* on the coarseness: its prop test
deliberately uses a **third** key `w` (distinct from the watched `a`), explaining (L83-101) that
because the WATCH version is coarse, "any same-shard removal invalidates any outstanding watch,
watched key or not," and if the watched key itself carried the TTL the transaction "would abort
unconditionally." The test's schedule-dependent commit branch exists *only* because unrelated
same-shard events (sweep of `w`, write to `c`) abort a WATCH on `a`.

### A single EXEC only ever compares same-shard watches

Verified nuance the candidate did not state: the coarseness is bounded to *one* Internal Shard per
EXEC. `transaction.rs:228-242` shows a watch set spanning shards promotes the transaction target to
`TransactionTarget::Multi`, which `resolve()` maps to a **CROSSSLOT** error reply — the EXEC never
runs. So `check_watches` is only ever handed watches for keys colocated on the EXEC's single target
shard. The blast radius of an over-abort is "other keys on the same Internal Shard," never
cross-shard. This tightens the problem and shapes the fix: the version store can be **per-shard-local**
(no cross-shard coordination needed).

### The seam that makes a fix cheap already exists next door

The `VersionIncrement` write-effect runs immediately before `TrackingInvalidation` in
`WRITE_EFFECT_ORDER` (`post_execution.rs:156-166`), and `invalidate_written_keys`
(`post_execution.rs:396-413`) already extracts the written key set the exact way a key-granular
version bump would need it:

```rust
for record in writes {
    all_keys.extend(record.handler.keys(record.args));   // per-key, already done here
}
```

Client-tracking is a **proven in-crate precedent** for per-key dirtying at this seam. The version
bump sits one step earlier in the same loop with the same `WriteSummary.writes` in hand — the keys
are already available where the bump happens.

## Why it is shallow (architecture vocabulary)

**The Interface over-promises its Implementation.** A deep **Module** hides substantial behavior
behind a narrow contract. `get_key_version(key) -> u64` does the opposite: it advertises key
resolution and delivers a constant. The `_key` parameter is dead surface — an interface that names a
capability the module does not have. `check_watches`'s per-key `all(...)` inherits the false
promise: it *looks* like it discriminates keys and cannot.

**Poor locality of the abort decision.** "Did my watched key change?" is answered nowhere near the
key. The write to `b` bumps a shard-global counter; the WATCH on `a` reads that same counter; the two
are coupled through a variable neither names. To predict whether an `EXEC` aborts you must know every
other key mapped to the same Internal Shard and whether *any* of them saw a write, an eviction, an
expiry, or a waiter wake — a global fact standing in for a local one.

**Low leverage / honest-seam failure.** The current design buys real things — O(1) `GetVersion`, a
**stateless probe** (the shard holds no watcher state; WATCH is a version read, EXEC a version
compare), and trivially-bounded memory (one `u64`). That is legitimate leverage, which is exactly why
this may resolve as WONTFIX. What it is *not* is honest: the interface should either deliver key
granularity or stop pretending to accept a key.

**Deletion test.** Delete the `_key` parameter today and nothing breaks — proof it carries no
behavior. That is the minimal "honest rename" fix. Delete it *after* making behavior key-granular and
the compiler screams — proof the parameter would then be load-bearing.

## Proposed design

Two real designs plus the honest-rename fallback. All are confined to the `core` crate's `shard`
module; none crosses a crate boundary, touches Raft, or changes the data path (the `GetVersion` /
`ExecTransaction` message shapes are unchanged — only what fills the `u64` slots changes).

### Design A (primary) — per-slot version stamps

Replace the single `shard_version: u64` with a bounded map from **Hash Slot** to version, keyed only
over the slots this Internal Shard actually owns. `get_key_version(key)` stops ignoring its argument:
it computes the key's slot and returns that slot's stamp. A write bumps only the slots of the keys it
wrote.

```rust
/// Per-Internal-Shard WATCH version store, slot-granular.
/// Bounded: at most one u64 per Hash Slot this shard owns.
struct SlotVersions {
    // Dense: index by the shard's local slot ordinal, or sparse HashMap<u16,u64>
    // if a shard owns a wide, non-contiguous slot range. Choice deferred to impl;
    // both are O(1) get/bump and bounded by owned-slot count (<= 16384).
    versions: HashMap<u16, u64>,   // slot -> version; absent == 0 (never written)
    /// Monotonic stamp source so distinct writes to one slot always differ.
    next: u64,
}

impl SlotVersions {
    fn get(&self, slot: u16) -> u64;          // absent -> 0
    fn bump(&mut self, slot: u16);            // versions[slot] = { next += 1; next }
    fn bump_all<I: IntoIterator<Item = u16>>(&mut self, slots: I);
}

impl ShardWorker {
    // Signature is UNCHANGED — the key finally does something.
    pub(crate) fn get_key_version(&self, key: &[u8]) -> u64 {
        self.slot_versions.get(frogdb_core::slot_for_key(key))
    }

    // Replaces the argument-less increment. Every current caller already holds
    // the affected key(s); each maps them to slots and bumps.
    fn bump_versions_for<'a>(&mut self, keys: impl IntoIterator<Item = &'a [u8]>);
}
```

`check_watches` is **unchanged** (`worker.rs:462-466`): it already loops per key and calls
`get_key_version(key)`; making that call slot-aware makes the whole check slot-granular for free.
`GetVersion` (`dispatch_core.rs:59-71`) returns per-key versions instead of one shard version — the
message's `keys` field (`message.rs:154-157`) already carries them; only the response widens from
`u64` to `Vec<u64>` (or the handler resolves each key's slot and returns a parallel vector).

**Why per-slot, not per-key:** it is **bounded without garbage collection**. A per-key version map
(Design B) grows one entry per key ever written and must be GC'd when keys are deleted — otherwise a
delete-heavy workload leaks version entries. Per-slot caps at the shard's owned slot count (≤ 16384
u64s = ≤ 128 KiB per shard, typically far less) and needs no GC (slots are permanent). It is strictly
finer than today and strictly cheaper than per-key.

**Caveat — per-slot is coarser than Redis, not Redis-parity.** Design A still cross-aborts keys that
share a **Hash Slot**. Hash-tag colocation (`CONTEXT.md:25`) is a *routing* guarantee — same slot ⇒
same node, so multi-key ops are legal — **not** a co-invalidation contract. Real Redis WATCH is
strictly per-key: `WATCH {t}a; SET {t}b; EXEC` **commits** in Redis (because `a` is unchanged) even
though `a` and `b` share a slot. Design A *aborts* that case. So per-slot is not fidelity; it is a
deliberately coarser-but-bounded granularity that eliminates the common over-abort (unrelated keys in
*different* slots on the same shard) while accepting residual over-aborts within a slot. Only Design B
matches Redis exactly.

**Bump-site migration (all five already hold their keys):**

| Site | Today | Design A |
| --- | --- | --- |
| `post_execution.rs:196-208` | `increment_version()` | `bump_versions_for(summary.writes.flat_map(keys))` — keys already extracted for tracking one step later |
| `eviction.rs:221,279` | `increment_version()` | `bump_versions_for([key])` — the evicted key is in hand |
| `blocking.rs:272` | `increment_version()` | `bump_versions_for(woken_keys)` — the wake already knows its keys |
| `event_loop.rs:229` | one bump per sweep | `bump_versions_for(&removed_keys)` — the swept keys are **already materialized** as `removed_keys` (`event_loop.rs:198-203`, from `result.deleted_keys` + `result.emptied_keys`) directly above the bump |
| `purge_expired_watches` (`worker.rs:481-490`) | one bump if any purged | bump the slots of the purged watched keys (already iterating them) |

All five sites already hold their keys — including active expiry, contrary to the candidate's framing
of it as the sole friction. `event_loop.rs:198-203` builds `removed_keys` (deleted + emptied keys)
before the `increment_version()` at L229 for the stream-waiter drain; the per-slot bump consumes that
existing `Vec<Bytes>` with **no new sweep-result type**. There is no site needing genuinely new data.

### Design B (alternative) — Redis-style per-key `watched_keys` map (shard holds watcher state)

This mirrors Redis/Valkey exactly and is the most precise, at the cost of abandoning the stateless
probe. Redis keeps `db->watched_keys` (a dict: key → list of watching clients); `signalModifiedKey →
touchWatchedKey` looks up the *written* key and sets `CLIENT_DIRTY_CAS` only on clients watching
*that* key; `EXEC` aborts iff the flag is set. No versions at all.

```rust
// Shard-resident watcher registry (new state the shard must own & GC).
struct WatchedKeys {
    // key -> set of conn_ids watching it; entry removed when its watcher set empties.
    by_key: HashMap<Bytes, HashSet<u64>>,
    // conn -> CAS-dirty flag; set by touch, read+cleared at EXEC.
    dirty: HashMap<u64, bool>,
}
```

WATCH becomes a *registration* (`by_key` insert) rather than a version read; EXEC checks
`dirty[conn]` instead of comparing versions; UNWATCH/DISCARD/connection-close must deregister. This is
zero false positives and zero false negatives, but it:

- **converts WATCH from a stateless probe into stateful shard bookkeeping** — the shard now tracks
  every watcher, with lifecycle tied to connection teardown (a new failure surface: leaked watchers
  on abnormal disconnect);
- **reshapes the `GetVersion`/`ExecTransaction` messages** (`message.rs:143-168`) into
  register/check/deregister messages — a larger seam change than Design A;
- **complicates the existing F3 lazy-expiry handling** (`execution.rs:445-464`,
  `dispatch_core.rs:59-71`), which is currently expressed cleanly in version terms (a purge bumps the
  version; an already-stale watch records "nonexistent"). Redis expresses the same via the
  `wk->expired` flag on the watched-key entry, so it is portable — but it is a rewrite, not a tweak.

Recommend Design B **only** if slot-granularity proves too coarse in practice (many distinct keys per
slot cross-aborting). Design A captures the vast majority of the benefit at a fraction of the churn.

### Design C (fallback / WONTFIX) — honest rename + ADR

If the latency and simplicity of the O(1) stateless probe are judged worth the over-aborts (a
defensible position — the over-abort is *safe*, only ever a spurious retry, never a lost conflict),
then the correct action is to **stop lying in the interface**:

```rust
// Drop the dead parameter; name the real granularity.
pub(crate) fn shard_version(&self) -> u64 { self.shard_version }

pub(crate) fn check_watches(&self, watches: &[(Bytes, u64)]) -> bool {
    let v = self.shard_version();
    watches.iter().all(|(_key, watched)| *watched == v)   // key intentionally unused
}
```

...paired with a short ADR under `frogdb-server/docs/adr/` recording that WATCH granularity is
per-Internal-Shard by deliberate choice (latency + stateless probe + unbounded-map avoidance), that
over-aborts are safe and characterized (`scenario_s2.rs`), and that key/slot granularity was
considered and deferred. This is a genuine outcome, not a cop-out: it makes the coarseness a
*documented contract* instead of an *accidental-looking* ignored parameter.

## Migration plan (Design A, ordered)

1. **Add `SlotVersions`** to the `shard` module with `get`/`bump`/`bump_all` and unit tests; leave it
   unused. Wire it into `ShardWorker` construction (`builder.rs:440` currently sets
   `shard_version: 0`) alongside the existing field.
2. **Point reads at it:** make `get_key_version(key)` resolve `slot_for_key(key)` and read
   `slot_versions` while `shard_version` still exists in parallel. Assert equivalence in a temporary
   test (both should over-abort identically until step 4 flips bump granularity).
3. **Wire active expiry to per-slot bumps** using the `removed_keys` vector already built at
   `event_loop.rs:198-203` (deleted + emptied keys) — no new sweep-result type is needed; the per-slot
   bump replaces the `increment_version()` at L229 and consumes that existing `Vec<Bytes>`.
4. **Convert the five bump sites** (`post_execution.rs`, `eviction.rs` ×2, `blocking.rs`,
   `event_loop.rs`) and `purge_expired_watches` from `increment_version()` to `bump_versions_for(keys)`.
   Remove `increment_version` and the `shard_version` field.
5. **Widen `GetVersion`** to return per-key versions (`Vec<u64>`), updating
   `dispatch_core.rs:59-71`, `message.rs:154-157`, and the WATCH round-trip in
   `transaction_conn_command.rs:308`. `watch_key`'s *storage* (`state.rs:867`) is already per key, but
   the WATCH loop at `transaction_conn_command.rs:327-329` currently applies a single scalar `version`
   to every key (`for key in args { state.watch_key(key, shard, version) }`). Restructure that call
   site to zip each key with its own version from the returned vector and to enforce the
   keys/versions length invariant — a real (small) plumbing change, not a no-op.
6. **Flip the pinning tests** (`scenario_s2.rs`, `scenario_s8.rs`) — see Test plan.
7. **Add the new key-granular tests** and run the full concurrency/turmoil suites on a Blacksmith
   testbox.

Steps 1-2 are safe no-ops (behavior identical); the behavior change lands atomically at step 4.

## Test plan

**Correction to the candidate's "test win":** the assertion *`WATCH a; write b same shard; EXEC
succeeds`* is **not** currently impossible to write — it is currently pinned to the **opposite**
outcome. So the win is a **test flip**, and these existing tests must change:

- `scenario_s2.rs :: s2_zero_false_negatives_and_over_abort_characterized` — today asserts
  `over_aborts >= 1`. Under Design A, `Interleave::UnrelatedWrite` (`SET u` while watching `k`, `u`
  and `k` in different slots) and `Interleave::ActiveExpiryUnrelated` must now **commit**. The test
  inverts: assert those two cases commit, while `WatchedWrite` and `ActiveExpiryWatched` still abort
  (zero false negatives — the invariant that stays). Add a same-slot control (`WATCH {t}a; SET {t}b`
  via hash tag) that *still* aborts, pinning slot granularity precisely.
- `scenario_s8.rs :: prop_s8_exec_atomic_under_permutation` — its commit branch currently assumes
  unrelated same-shard events abort a WATCH on `a`. Under Design A, the sweep of `w` and the write to
  `c` (different slots from `a`) no longer abort; the schedule-dependence collapses toward
  "commits unless `a`'s own slot is touched." Rework the model predicate accordingly; atomicity
  (never one-of-`a`/`b` updated) is unchanged and must stay green.
- `scenario_s2.rs :: s2_f3_lazy_expiry_watched_key_aborts` — the F3 lazy-expiry abort **must stay
  green**: an expired *watched* key still bumps its own slot at `purge_expired_watches`. This pins
  that the fix does not regress the Redis PR #7920 parity.

**New tests (the genuinely-newly-expressible ones):**
- `watch_unrelated_slot_write_commits`: `WATCH a; SET b` (different slots, same shard) → `EXEC`
  commits. The headline win, impossible to assert today.
- `watch_same_slot_write_aborts`: `WATCH {t}a; SET {t}b` → aborts (slot granularity, not per-key).
- `SlotVersions` unit tests: distinct slots independent; same-slot bump invalidates; absent slot
  reads 0; monotonicity across bumps.
- Eviction/blocking-wake over-abort regression: `WATCH a` survives an eviction/waiter-wake on a
  different-slot key.

## Risks & alternatives

- **This is a behavior change to a *characterized* contract, not a bugfix.** The over-abort is safe
  (spurious retry, never a lost CAS conflict). Reviewers may legitimately prefer Design C. The
  proposal's honest position: *either* deliver key/slot granularity *or* rename the dead parameter and
  ADR the trade — the status quo (a key-shaped interface that ignores the key) is the only
  unacceptable option.
- **Active expiry needs no new data.** The swept keys are already materialized as `removed_keys`
  (`event_loop.rs:198-203`), so precise per-slot expiry bumps are the default path — this was
  over-billed as the sole friction. A *bump-all-owned-slots on non-empty sweep* fallback exists only
  if some future sweep variant genuinely could not enumerate its keys, and it is **incompatible with
  the `Interleave::ActiveExpiryUnrelated` commit assertion** (bumping all slots re-aborts the watched
  key's slot). So the two are mutually exclusive by construction: the s2/s8 test-plan flips
  (below) assume the precise path and **must not** be paired with the coarse fallback. If the coarse
  fallback is ever taken for expiry, `ActiveExpiryUnrelated` stays pinned to *abort* and the test plan
  for that one case reverts to `over_aborts >= 1`. Given `removed_keys` is already in hand, the
  precise path is the expected implementation and the fallback should not be needed.
- **`GetVersion` widening to `Vec<u64>`** adds a small allocation per WATCH round-trip. WATCH is not
  hot; negligible. If ever hot, return a single hashed digest of the per-key versions.
- **Design B's watcher-lifecycle leak** (registered watchers not cleaned on abnormal disconnect) is a
  real new failure mode Design A entirely avoids by staying stateless — a strong reason to prefer A.
- **Replication parity:** the replica-apply path drives `ExecTransaction` with empty watches
  (`executor.rs:92`; `transaction.rs` sends `watches` only for client EXECs), so replicas never run
  `check_watches` and are unaffected by granularity. Verified: WATCH is a Primary-side client concern.
- **Config Epoch / Raft untouched:** WATCH versioning is Internal-Shard-local state on the data path;
  ADR-0001 (data path never through Raft) is not implicated. No cross-crate signature churn —
  `persistence`/`replication`/`cluster` do not read `shard_version`.

## Effort

**M** for Design A (one crate; five mechanical bump-site conversions the compiler drives, one
active-expiry data change, one message widening, and a test flip that needs care — the migration is
compiler-guided but the pinned-test inversions require deliberate re-modeling). **L** for Design B
(new shard-resident watcher state, message-shape changes, watcher lifecycle/GC, F3 rewrite). **S** for
Design C (delete the parameter, rename, write the ADR).

## Related

None. May resolve as a **WONTFIX ADR** (Design C) if the stateless-probe / O(1) latency trade is
judged to outweigh the over-aborts — in which case the deliverable is the honest rename plus the ADR,
not the per-slot machinery.

## Adversarial review

**Verdict: AMEND** (core premise VERIFIED and sound; five minor precision/consistency issues, all
resolved in place). Effort estimate **M** for Design A confirmed fair.

The reviewer independently re-verified the premise against source and found it holds:
`get_key_version(&self, _key)` ignores the key and returns `shard_version` (`worker.rs:456-459`);
`check_watches` loops per-key over that constant (`462-466`); all five bump sites are key-agnostic;
`scenario_s2.rs` pins the over-abort as *required* (`over_aborts >= 1`); `slot_for_key` is exported;
cross-shard watch sets are CROSSSLOT-rejected so `check_watches` only sees same-shard watches; no
persistence/replication/cluster crate reads `shard_version`. Design A judged FEASIBLE with no
borrow-checker or crate-boundary obstacle; the `GetVersion` `u64`→`Vec<u64>` widening is the only
wire-shape change and it is Primary-side client-only.

| # | Sev | Issue | Resolution |
| --- | --- | --- | --- |
| 1 | minor | **Internal contradiction:** Test plan requires `Interleave::ActiveExpiryUnrelated` to commit, but the Risks fallback ("bump *all owned slots* on a non-empty sweep") keeps it aborting — mutually exclusive prescriptions for one test case. | **Reconciled.** Rewrote the Risks bullet to state the two are mutually exclusive by construction: the s2/s8 flips assume the *precise* per-slot path and must not be paired with the coarse fallback; if the fallback is ever taken, `ActiveExpiryUnrelated` stays pinned to abort (`over_aborts >= 1`). Since the swept keys are already in hand (issue 5), the precise path is the expected implementation and the fallback should not be needed. |
| 2 | minor | **Overstated:** "every write bumps" / "fires for any written key" — at `post_execution.rs:201-208` the `Command`/`ScatterPart` bump is gated on `summary.dirty_delta >= 0` (WATCH no-op rule); only `Transaction` bumps unconditionally. | **Fixed.** Softened to "every *dirtying* write" throughout (Summary, section heading, bump-site list), with an explicit parenthetical on the `dirty_delta >= 0` gate and the unconditional Transaction bump. |
| 3 | minor | **Rationalization:** framing per-slot as "honors the Hash Tag colocation contract… legitimately co-invalidate" misstates Redis semantics — colocation is a *routing* guarantee, not co-invalidation. Real Redis `WATCH {t}a; SET {t}b; EXEC` commits; Design A aborts it. | **Fixed.** Deleted the "honors Hash Tag colocation" framing; replaced with an explicit **"coarser-than-Redis but bounded"** caveat that spells out the same-slot over-abort, cites the Redis commit case, and states only Design B is Redis-parity. |
| 4 | minor | **Understated:** migration step 5 called the client-side recording a no-op ("already stores per key"). The WATCH loop at `transaction_conn_command.rs:327-329` applies one scalar `version` to every key; widening to `Vec<u64>` requires zipping each key with its own version + a length invariant. | **Fixed.** Step 5 now distinguishes `watch_key`'s per-key *storage* (unchanged) from the *call-site* plumbing (real, small change), quoting the `for key in args` loop and describing the zip + length-invariant restructuring. |
| 5 | minor | **Pessimistic in the proposal's favor:** active expiry billed as the sole friction "needing new data"; in fact `event_loop.rs:198-211` already materializes the swept keys as `removed_keys` before the `increment_version()` at L229. | **Fixed.** Corrected the bump-site table, the "only friction" paragraph, and step 3 to consume the existing `removed_keys` `Vec<Bytes>` — no new sweep-result type. This strengthens feasibility and cheapens step 3; verified at `event_loop.rs:198-203`. |

All five were minor and are resolved in place. None invalidated the premise or forced a different
primary design. The proposal already carried three designs (including the WONTFIX Design C + ADR) and
was noted as "unusually honest"; the amendments tighten precision and remove the one genuine internal
contradiction.
