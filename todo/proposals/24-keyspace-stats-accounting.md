# Proposal: Keyspace-Stats Accounting

Status: proposed
Date: 2026-06-17

## Problem

`keyspace_hits` / `keyspace_misses` answer one question — "of the reads that resolved a key, how
many found it?" — and that question has a single, well-defined decision per command: *is this a
counted read, and did the key exist?* In FrogDB that decision has no owner. It is open-coded, by
hand, in the body of every command that bothers to participate, and most read commands do not
bother. The result is two distinct defects, both documented in `todo/proposals/INDEX.md:227-231` as
deferred follow-ups:

- **(a) Coverage gap.** Only six commands report lookups at all. Redis counts the whole class of
  key-resolving read commands; FrogDB counts a sixth of it, so the published hit/miss totals are a
  severe undercount of real read traffic.
- **(b) RESETSTAT can't reset them.** `CONFIG RESETSTAT` zeroes every *sibling* stat (command-call
  counts, error stats, latency histograms, per-shard counters) but leaves `keyspace_hits` /
  `keyspace_misses` untouched, because those are read out of a monotonic Prometheus counter that
  RESETSTAT has no way to zero. After a RESETSTAT the keyspace numbers are stale relative to
  everything next to them in the same `INFO stats` block.

Both defects are symptoms of the same structural problem: the keyspace-lookup decision is a *shallow
seam*. The contract — "hit means the KEY existed, independent of the reply shape" — is not enforced
by a type or owned by a module; it is re-stated, in English, in **three** different files
(`command.rs:882-892`, `shard/execution.rs:148-158`, `shard/post_execution.rs:44-46`), and then
re-implemented, by hand, in the hit-arm and miss-arm of every participating command's `match`. When
the contract lives in prose and the implementation lives in N handler bodies, two things follow with
certainty: new read commands silently forget to participate (defect a), and the *aggregate* the
contract feeds has no single home that RESETSTAT can reach (defect b).

**Deletion test.** If keyspace accounting were already a deep module, deleting that module would
delete: the twelve hand-placed `ctx.record_keyspace_lookup(bool)` calls across four command files,
the *duplicate* tally inside `scatter_mget` (MGET counts itself in two places — the single-shard
`execute` and the scatter path), and the "absent here by design" comment in `post_execution.rs` that
exists only to explain why the recording does *not* live in the post-execution pipeline. Today none
of those can be deleted, because the decision lives in the callers, not in a module. That is the
signal that the seam is in the wrong place.

This proposal owns the **keyspace-lookup contract**: one home that classifies *which* commands count
(driven by the command spec, not by remembering to call a method), records hit/miss *once* at the
execution seam, and exposes a resettable source of truth so RESETSTAT is correct and Prometheus
`rate()` stays sane. It resolves both INDEX follow-ups. It composes with proposal 01 (the command
spec is where classification belongs) and feeds the INFO surface that reads these counters today.

## Current state

### Six commands, hand-placed, in both arms of a match

The flag `CommandFlags::TRACKS_KEYSPACE` (`command.rs:549`) marks a command as a participant, and the
command's handler body calls `ctx.record_keyspace_lookup(hit)` by hand in each branch. GETDEL is
representative (`commands/src/string.rs:372-388`):

```rust
match ctx.store.get_and_delete(key) {
    Some(value) => {
        // Keyspace hit: the key existed (GETDEL reads it before deleting, …)
        ctx.record_keyspace_lookup(true);          // string.rs:376
        // …
    }
    None => {
        ctx.record_keyspace_lookup(false);         // string.rs:384
        Ok(Response::null())
    }
}
```

The complete census of hand-placed calls (`grep record_keyspace_lookup commands/src/`):

| Command | File:line (hit / miss) | Notes |
|---|---|---|
| GET | `basic.rs:451` / `:459` | single key |
| GETDEL | `string.rs:376` / `:384` | reads-before-delete |
| GETEX | `string.rs:444` / `:448` | reads, may retouch TTL |
| MGET | `string.rs:836` / `:843` | per key, single-shard `execute` |
| MGET (scatter) | `core/.../shard/execution.rs:634` | **duplicate** tally in `scatter_mget` |
| HGET | `hash.rs:160` / `:171` | hit even when the *field* is missing |
| LINDEX | `list.rs:443` / `:454` | hit even when the *index* is out of range |

That is six commands, twelve calls, across four command files, plus a seventh accounting site in the
shard's scatter path. MGET is the tell: because there is no owner, its accounting had to be written
*twice* — once in `MgetCommand::execute` and once in `ShardWorker::scatter_mget` — and the two copies
can drift independently.

### The contract is carried in prose, in three files

`CommandContext::record_keyspace_lookup` (`command.rs:894`) and its two doc-blocks
(`command.rs:882-892`, `command.rs:785-799`) spell out the invariant in English:

```rust
/// `hit` must reflect whether the looked-up KEY existed — independent of the
/// reply shape. For example HGET on an existing hash with a missing field is
/// a hit (the key lookup succeeded), while GET on a missing key is a miss.
/// This matches Redis's lookup-level `keyspace_hits`/`keyspace_misses`
/// accounting (`lookupKeyReadWithFlags`), which a reply-shape heuristic
/// cannot reproduce (a nil bulk reply is ambiguous between the two cases).
pub fn record_keyspace_lookup(&mut self, hit: bool) { /* … */ }
```

The shard's emitter `record_keyspace_lookups` (`shard/execution.rs:159-174`) re-states the same
invariant a second time (`:148-158`), and the post-execution pipeline re-states it a *third* time to
justify why recording is **not** a write effect (`shard/post_execution.rs:44-46`):

```rust
//! - **Keyspace hit/miss metrics are NOT a write effect.** They are recorded at
//!   lookup level for every command (read or write) in `execute_command_inner`
//!   via `record_keyspace_lookups`, so they are absent here by design.
```

Three copies of one rule, maintained by hand. This is exactly the shallow-seam smell proposal 18
calls out for the replication offset: the interface a caller must understand is as large as the
implementation it is supposed to hide.

### The aggregate is a monotonic Prometheus counter with no zero

The shard funnels the per-command tally into the recorder (`shard/execution.rs:121-122,159-174`):

```rust
if tracks_keyspace {
    self.record_keyspace_lookups(keyspace_hits, keyspace_misses);   // execution.rs:122
}
// …
self.observability.metrics_recorder.increment_counter("frogdb_keyspace_hits_total", hits, &[]);
self.observability.metrics_recorder.increment_counter("frogdb_keyspace_misses_total", misses, &[]);
```

`increment_counter` is monotonic by construction (`telemetry/.../prometheus_recorder.rs:477-493`;
trait at `types/src/traits/metrics.rs:14`). INFO reads the totals back out of that same counter
(`server/.../handlers/scatter.rs:424-444`):

```rust
let recorder = &self.observability.metrics_recorder;
if let Some(hits) = recorder.counter_value(metric_names::KEYSPACE_HITS) {        // scatter.rs:434
    patched = patched.replace("keyspace_hits:0\r\n", &format!("keyspace_hits:{hits}\r\n"));
}
if let Some(misses) = recorder.counter_value(metric_names::KEYSPACE_MISSES) {    // scatter.rs:438
    patched = patched.replace("keyspace_misses:0\r\n", …);
}
```

`counter_value` reads counters only; there is no reset, and when metrics are disabled the no-op
recorder returns `None`, so INFO falls back to the hardcoded `0` placeholder (`scatter.rs:428-430`) —
i.e. INFO reports `0` whenever Prometheus is off, a second observability gap riding on the same
single-source design.

### RESETSTAT resets everything except this

`handle_config_resetstat` (`server/.../handlers/config.rs:110-125`) fans a `ResetStats` message to
every shard and clears the registry-level stats:

```rust
async fn handle_config_resetstat(&self) -> Response {
    for sender in self.core.shard_senders.iter() {
        // … ShardMessage::ResetStats → ShardObservability::reset_stats()
    }
    self.admin.client_registry.reset_command_call_counts();
    self.admin.client_registry.error_stats.reset();
    self.observability.latency_histograms.reset();
    Response::ok()
}
```

The per-shard `reset_stats` (`core/.../shard/types.rs:56-62`) zeroes the latency monitor, slow log,
peak memory, evicted keys, and lazyfreed objects. **Nothing in this path touches the keyspace
counters** — they live in the Prometheus registry, which has no zero operation reachable from here.
So after `CONFIG RESETSTAT`, `INFO stats` shows fresh-zeroed command/error/latency stats sitting next
to stale, never-reset `keyspace_hits` / `keyspace_misses`. INDEX records this as a deliberate
divergence pending a design decision (`INDEX.md:227-229`: *baseline-offset vs registry recreation*).
This proposal makes that decision.

## Proposed design

Two halves, matching the two defects: a **classification + recording home** driven by the command
spec (fixes coverage and locality), and a **resettable source of truth** mirrored to a still-monotonic
Prometheus counter (fixes RESETSTAT without breaking `rate()`).

### Half 1 — classify on the spec, record at the seam

The key-existence fact is genuinely owned by the command: only the handler performs the store lookup,
so only it knows whether the key was there. What the command should *not* own is the *decision to
count*, the *gating*, the *emission*, and the *invariant comment*. Those move to one home.

Add a `lookup` field to `CommandSpec` (`command_spec.rs:225-244`, the home proposal 01 established for
exactly this kind of mechanical fact), replacing the `TRACKS_KEYSPACE` flag bit:

```rust
/// How a command contributes to keyspace hit/miss accounting. Declared once on
/// the spec; the execution seam — never the handler body — does the counting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupSpec {
    /// Not a keyspace-counted command: writes, admin, pub/sub, and dictionary-
    /// iterating reads (SCAN, RANDOMKEY) that never resolve a *named* key.
    None,
    /// Exactly one lookup against the command's primary key; hit iff that key
    /// exists. The seam records it; the handler writes no accounting code.
    /// (GET, HGET, LINDEX, STRLEN, LLEN, SCARD, ZCARD, TYPE, HGETALL, LRANGE,
    /// SMEMBERS, ZRANGE, GETRANGE, HKEYS, HVALS, …)
    FirstKey,
    /// One lookup per key in the command's key set; per-key hit/miss.
    /// (MGET, EXISTS, TOUCH, HMGET resolves one key so it is FirstKey.)
    EveryKey,
    /// Irregular read whose counted lookup is neither "first key" nor "every
    /// key"; the handler reports it once via `ctx.record_lookup(outcome)`.
    Reported,
}
```

The execution seam (`shard/execution.rs`, the same `execute_command_inner` that already gates on
`tracks_keyspace` at `:69-71,121-122`) reads `spec.lookup` and records hit/miss from key existence:

- `FirstKey` / `EveryKey`: the seam resolves the keys via the spec's existing `KeySpec` and records
  one hit/miss per key from actual existence. Two viable mechanisms, chosen by what lands first:
  - *Counting accessor* (preferred): route the handler's read of its primary key through a typed
    store accessor that records the lookup at the moment it happens — zero extra probes. This is the
    natural composition with proposal 02 (typed store access) / proposal 11 (probabilistic typed
    accessors); the accessor becomes the single physical point where "a read resolved a key" is
    observed.
  - *Seam re-probe* (interim): after the handler returns, the seam checks existence of the spec's
    key(s). Because participating commands are read-only and a shard turn is synchronous, the probe
    is authoritative (lazy expiry that the handler triggered is already reflected). Cost is one extra
    hash probe per counted command — acceptable, and the migration can ship on this and swap to the
    accessor later with no behavior change.
- `Reported`: the handler calls `ctx.record_lookup(LookupOutcome)` exactly once — the typed
  replacement for today's hand-placed `record_keyspace_lookup(true/false)` bool in two arms.

**Why not a generic `Response → hit/miss` rule.** It is tempting to delete the command's
participation entirely and infer hit/miss from the reply at the seam. This does not work, and the
codebase already paid for learning why: the reply shape is *ambiguous*. A nil bulk reply means a
keyspace **miss** for GET (key absent) but a keyspace **hit** for HGET (key present, field absent) and
for LINDEX (key present, index out of range). "Hit" is `count > 0` for EXISTS, non-null for GET, and
empty-vs-non-empty is *meaningless* for LRANGE (an empty list that exists is a hit). A single
response-shape heuristic cannot distinguish these; the previous reply-shape classifier
(`track_keyspace_metrics` keying on `Response::Null`) misclassified GET/HGET misses and was removed
in commit `23469adc` (`INDEX.md:222-224`). The honest design therefore keeps the *existence fact*
sourced from the command/accessor (never from the reply) and moves only the *classification, gating,
emission, and the one statement of the invariant* into the home. `LookupSpec` + `LookupOutcome` is
deliberately a small typed contract the command contributes to, not a generic rule that pretends the
reply is enough.

### Half 2 — a resettable source of truth, Prometheus stays monotonic

Introduce a process-wide atomic accumulator, shared across shards the way the recorder already is:

```rust
/// Single source of truth for keyspace hit/miss accounting. Lives behind a
/// shared `Arc`; every shard feeds it, INFO reads it, RESETSTAT zeroes it.
pub struct KeyspaceStats {
    /// Cumulative since process start. Feeds the monotonic Prometheus counter;
    /// NEVER decreases, so `rate()` / `increase()` stay exact.
    hits: AtomicU64,
    misses: AtomicU64,
    /// Snapshot taken at the last CONFIG RESETSTAT. INFO reports
    /// `cumulative - baseline`, so the *operator-visible* value resets while
    /// the Prometheus counter does not.
    hits_baseline: AtomicU64,
    misses_baseline: AtomicU64,
}
```

- **Recording** (`record_lookup`) bumps `hits`/`misses` *and* the Prometheus counter
  (`frogdb_keyspace_{hits,misses}_total`). The atomic is the truth; Prometheus is fed from it.
- **INFO** reports `hits − hits_baseline` (resp. misses), reading the atomic directly. This both
  makes RESETSTAT correct and removes INFO's dependence on the recorder — fixing the "metrics
  disabled ⇒ INFO shows 0" gap at `scatter.rs:428-430`, since the atomic exists regardless of whether
  Prometheus is enabled.
- **RESETSTAT** does `hits_baseline.store(hits.load(…))` (and misses). The operator-visible value
  drops to 0; the Prometheus `_total` is left strictly monotonic and untouched. RESETSTAT now zeroes
  the keyspace stats in the same fan-out it already uses for sibling stats (`config.rs:110-125`).

**Recommendation: baseline-offset, reject the gauge mirror.** The task framed two mirror options.

- *Baseline-offset (recommended).* The Prometheus counter is immortal and monotonic, so `rate()` /
  `increase()` keep working without counter-reset artifacts; RESETSTAT is a purely INFO-facing
  reframe via the baseline. This matches Redis (INFO `keyspace_hits` is the since-RESETSTAT value)
  *and* Prometheus convention (a `_total` counter that never rewinds).
- *Gauge mirror (rejected).* Mirroring the atomic to Prometheus via `record_gauge`
  (`metrics.rs:20`) lets the value drop on RESETSTAT, but a gauge breaks `rate()` / `increase()`
  (counter functions are undefined on a gauge), violates the `_total` = counter convention, and would
  *also* break INFO's existing readback, which uses `counter_value` (`metrics.rs:36`,
  `scatter.rs:434-438`) — gauges are not counters and would not be returned. Rejected on every axis.
- *Registry recreation (rejected).* Dropping and recreating the counter vec on RESETSTAT zeroes the
  Prometheus series too, which Prometheus interprets as a process restart — spurious spikes and a
  lost scrape interval in `rate()`. Heavy and rate-breaking; rejected.

### Why this is the right depth

- **Locality.** The three prose copies of the hit-means-key-existed invariant collapse to one
  statement on `LookupSpec`. "Which commands count" becomes a spec field the dispatcher reads, not a
  flag-plus-two-method-calls a handler author must remember. Adding a read command to the accounting
  is a one-line spec edit; today it is "set the flag, then write `record_keyspace_lookup` in both arms
  of every match, and don't forget the scatter path."
- **Deletion test passes.** The migration is a net deletion at call sites: the twelve hand-placed
  calls, the duplicate `scatter_mget` tally, the `TRACKS_KEYSPACE` flag bit, and the
  "absent-by-design" comment all go away, replaced by data on the spec and one recording site.
- **Not a new adapter layer.** This deepens the seam the shard already owns (`execute_command_inner`
  already gates and emits keyspace stats); it pulls classification onto the spec and the aggregate
  behind `KeyspaceStats`, and forbids handlers from reaching past it. The raw
  `ctx.record_keyspace_lookup` calls are removed, not wrapped.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Phases 0-2 are behavior-preserving for
the six existing commands; phase 1 *fixes* RESETSTAT (defect b) and phase 3 *fixes* coverage
(defect a).

1. **Phase 0 — `KeyspaceStats` accumulator, no behavior change.** Add `KeyspaceStats` as a shared
   `Arc`, fed by the existing `record_keyspace_lookups` emitter alongside the Prometheus counter. No
   command or INFO change yet. Unit-test the arithmetic (record, cumulative, baseline subtraction) in
   isolation — no shard needed. `just check frogdb-server`.
2. **Phase 1 — INFO + RESETSTAT read/zero the accumulator (fixes defect b).** Repoint INFO
   (`scatter.rs:424-444`) to `stats.hits() - stats.hits_baseline()` instead of
   `recorder.counter_value`, and have `handle_config_resetstat` (`config.rs:110-125`) advance the
   baselines. Prometheus counters unchanged. After this phase RESETSTAT correctly zeroes the
   operator-visible keyspace stats and INFO works with metrics disabled. Add the regression test that
   fails today (below).
3. **Phase 2 — `LookupSpec` on the spec; move recording to the seam.** Add `LookupSpec`, default
   `None`; set `FirstKey` on GET/GETDEL/GETEX/HGET/LINDEX and `EveryKey` on MGET. Wire the seam to
   record from `LookupSpec` (via counting accessor or seam re-probe). Delete the twelve hand-placed
   `record_keyspace_lookup` calls and the duplicate `scatter_mget` tally; delete the
   `TRACKS_KEYSPACE` flag and the "absent-by-design" comment. Behavior identical for the six
   commands; verified by the existing `integration_metrics.rs` keyspace tests.
4. **Phase 3 — extend coverage to match Redis (fixes defect a).** Set `LookupSpec` on the rest of the
   key-resolving read commands — STRLEN, LLEN, SCARD, ZCARD, HGETALL, TYPE, EXISTS, TOUCH, SMEMBERS,
   ZRANGE, LRANGE, GETRANGE, HKEYS, HVALS, HMGET, SINTERCARD, … — each a one-line spec edit now that
   the seam owns recording. Leave dictionary-iterating reads (SCAN, RANDOMKEY) as `None` to match
   Redis, which does not run them through `lookupKeyRead` (see Risks).
5. **Gate.** Add a `just lint` grep gate: `record_keyspace_lookup` / `record_lookup` must not appear
   in command handler bodies (the seam owns recording), and any `READONLY` command that resolves a
   key must declare a non-`None` `LookupSpec` or be on an explicit allow-list. A table-driven test
   enumerates the Redis read-command set against FrogDB's `LookupSpec` so a newly-added read command
   that forgets to classify fails CI.

## Testing impact

- **Accumulator arithmetic, no cluster.** `KeyspaceStats` record / cumulative / baseline-subtraction
  are pure atomics, unit-testable directly. Pin: after `record(hit×3, miss×1)` then a RESETSTAT then
  `record(hit×1)`, INFO sees `hits=1, misses=0` while the Prometheus `_total` is `hits=4, misses=1`.
- **RESETSTAT regression (fails today).** Drive some GETs (hits and misses), `CONFIG RESETSTAT`, then
  read `INFO stats`: assert `keyspace_hits:0` / `keyspace_misses:0`. This fails on `main` (the
  counters stay stale) and passes after Phase 1. This is the defect-b pin.
- **Prometheus monotonicity pin.** After a RESETSTAT, assert `frogdb_keyspace_hits_total` did **not**
  decrease (so `rate()` is unaffected) even though INFO reset — the property that distinguishes
  baseline-offset from the gauge/registry-recreation alternatives.
- **Coverage table (fails today).** Parameterize over the read-command set: each command, run against
  a present and an absent key, must move `keyspace_hits` / `keyspace_misses` by exactly one.
  STRLEN/LLEN/SCARD/EXISTS/TYPE/SMEMBERS/etc. fail on `main` (they never count) and pass after Phase
  3. This is the defect-a pin.
- **Classification-nuance pins (must stay green).** Keep the existing lookup-level cases that a
  reply-shape rule would break: HGET on an existing hash with a missing field is a **hit**
  (`hash.rs:160` semantics), LINDEX with an out-of-range index is a **hit** (`list.rs:443`), GET on a
  missing key is a **miss**. These guard against anyone "simplifying" the design back into a
  response-shape heuristic and re-introducing `23469adc`.
- **MGET de-duplication.** Assert MGET counts one lookup per key on *both* the single-shard and the
  scatter-gather path, with no double counting once the duplicate `scatter_mget` tally is removed
  (`execution.rs:634`). The existing transaction-path keyspace test
  (`integration_metrics.rs:264-282`) must stay green.

## Risks / open questions

- **Coverage = on-the-wire numbers will jump.** Turning on the full read-command set makes
  `keyspace_hits` / `keyspace_misses` (and the derived hit-rate in the debug web UI,
  `debug/.../web_ui/handlers.rs:366`) step up sharply versus prior FrogDB versions. This is the
  *point* — the old numbers were a severe undercount — but it is a discontinuity worth a release note.
- **What exactly does Redis count? (accuracy, not blind parity.)** Redis increments these in
  `lookupKeyReadWithFlags`, i.e. wherever a command resolves a *named* key for reading. That is most
  read commands — but **not** dictionary-iterating ones: SCAN walks the hash table without a per-key
  `lookupKeyRead`, and RANDOMKEY samples; neither moves hit/miss in Redis. FrogDB should match that
  (leave them `LookupSpec::None`) rather than over-count to look thorough. Per the project's
  observability stance, misleading-but-bigger is worse than accurate. The `LookupSpec` table is the
  place this judgment is recorded once and reviewed, instead of being an accident of which handler
  happened to call the method.
- **Seam re-probe double-reads.** The interim `FirstKey` mechanism probes key existence a second time
  at the seam. It is correct for read-only commands within a synchronous shard turn but costs an extra
  hash probe; the counting-accessor mechanism removes the cost and is the target once proposal 02 /
  11 land. Either way the *contract* (one lookup recorded per counted read) is unchanged.
- **Per-shard vs global accumulator.** `KeyspaceStats` is modeled as one process-wide atomic pair
  (matching today's single global counter, which sums across label sets). If keyspace stats ever need
  per-shard labels, the accumulator gains a shard dimension and the baseline becomes per-shard; the
  home is the right place for that policy, but this proposal keeps the global model.
- **INFO surface interplay.** INFO is the consumer of these counters today via the `handle_info`
  patcher (`scatter.rs:424-444`). A future INFO-builder refactor (tracked as proposal 21 in the
  round-4 plan; not yet written) should read the keyspace value from `KeyspaceStats` rather than from
  the raw recorder, so this proposal deliberately makes the accumulator the single read source the
  builder can consume. Until 21 lands, the patcher reads the accumulator directly.
- **`LookupSpec` vs the `TRACKS_KEYSPACE` flag bit.** This proposal folds the flag into the richer
  `LookupSpec` enum. If any non-keyspace consumer of `TRACKS_KEYSPACE` exists, it must migrate;
  grep confirms the only readers are the shard gate (`execution.rs:69-71`) and the command specs, so
  the fold is clean.

## Correctness flags

1. **Keyspace hit/miss is a severe undercount — CONFIRMED.** Only six commands carry
   `TRACKS_KEYSPACE` and call `record_keyspace_lookup` (`basic.rs:451,459`; `string.rs:376,384`,
   `:444,448`, `:836,843`; `hash.rs:160,171`; `list.rs:443,454`; plus the scatter copy at
   `execution.rs:634`). Every other key-resolving read — STRLEN, LLEN, SCARD, ZCARD, HGETALL, TYPE,
   EXISTS, TOUCH, SMEMBERS, ZRANGE, LRANGE, GETRANGE, HKEYS, HVALS, … — contributes nothing, so the
   published `keyspace_hits` / `keyspace_misses` reflect roughly a sixth of real read traffic. This
   is misleading observability: a hit-rate computed from these numbers is wrong, not merely
   incomplete. Documented at `INDEX.md:230-231` as an enhancement; flagged here as a correctness
   defect because the *aggregate is published as if complete*. Fix: classify on the spec and record at
   the seam so all read commands count (Phase 3).

2. **CONFIG RESETSTAT leaves keyspace_hits/misses stale — CONFIRMED.**
   `handle_config_resetstat` (`config.rs:110-125`) resets per-shard stats, command-call counts, error
   stats, and latency histograms, but not the keyspace counters, which are read in INFO from the
   monotonic Prometheus recorder (`scatter.rs:424-444`, `counter_value` at `:434,438`). There is no
   reachable zero for that counter, so after RESETSTAT the `INFO stats` block shows freshly-reset
   sibling stats beside never-reset keyspace stats — internally inconsistent and misleading.
   Documented at `INDEX.md:227-229` as a deliberate divergence pending a baseline-offset-vs-registry-
   recreation decision. Fix: the `KeyspaceStats` accumulator with a RESETSTAT-advanced baseline
   (Phase 1), keeping the Prometheus counter monotonic so `rate()` / `increase()` are unaffected.
</content>
</invoke>
