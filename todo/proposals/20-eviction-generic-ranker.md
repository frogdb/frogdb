# Proposal: Eviction Generic Ranker

Status: proposed
Date: 2026-06-15

## Problem

The eviction subsystem implements three orderings — LRU, LFU, and volatile-TTL — and each ordering
is spread across **three near-identical methods in two files** that differ by a single line. The
sampling loop, the eviction-pool insert/replace/sort algorithm, and the metric increments are
copy-pasted once per ordering. This is a shallow seam in the sense that matters for change cost:
the *implementation* (sample N keys → build candidates → rank → keep the best 16 → pop the worst)
is substantial, but it has no single home — it is restated three times, so the interface a new
policy must satisfy is "go write another ~70 lines across two files" instead of "write a ranking
function."

This candidate is honestly **dedup-with-a-seam**, not a giant deep module like proposal 02. The win
is twofold and concrete: **locality** — the sampling loop, the pool replacement algorithm, and the
metric block each live in exactly one place, so a bug in the replace-the-best logic is fixed once
instead of three times; and **leverage** — a new `maxmemory-policy` (cost-aware, SLRU, GDSF, a
tiered variant) becomes one `EvictionRanker` impl (~3 lines) plus one `evict_one` match arm, not a
copy-pasted sampler + pool inserter + evict method.

Verified evidence (paths relative to `frogdb-server/crates/core/src/`):

| Triplicated method group | The one line that differs | file:line |
|--------------------------|---------------------------|-----------|
| `evict_lru` / `evict_lfu` / `evict_ttl` | which sampler is called: `sample_for_eviction(v)` / `sample_for_eviction_lfu(v)` / `sample_for_eviction_ttl()` | `shard/eviction.rs:158` / `:171` / `:184` |
| `sample_for_eviction` / `_lfu` / `_ttl` | the pool-insert call: `maybe_insert_lru(c)` / `maybe_insert_lfu(c)` / `maybe_insert_ttl(c)` | `shard/eviction.rs:221` / `:253` / `:281` |
| `pool::maybe_insert_lru` / `_lfu` / `_ttl` | the rank fn: `c.lru_rank()` / `c.lfu_rank()` / `c.ttl_rank()` (+ TTL's extra `ttl_remaining.is_none()` admit guard) | `eviction/pool.rs:154` / `:201` / `:252` (TTL guard `:242-245`) |
| the 3× sample-metric block | *nothing* — byte-identical | `shard/eviction.rs:205-210` / `:237-242` / `:265-270` |

The `maybe_insert_*` triplet is the worst offender: each is ~38 lines (`pool.rs:148-186`,
`:195-233`, `:241-284`) of identical duplicate-check → is-full → find-min-rank → replace-worst →
sorted-insert logic, where the **only** semantic difference is the `_rank()` accessor used and (for
TTL) a one-line admit guard. The `sample_for_eviction*` triplet is ~30 lines each, differing only in
the final `maybe_insert_*` call (and TTL hard-codes `sample_volatile_keys`). The metric block is
identical across all three samplers.

**Deletion test (today: fails).** Delete `pool::maybe_insert_lru` and `maybe_insert_lfu` and
`maybe_insert_ttl` survive, each carrying a full copy of the same pool algorithm — the hallmark of
duplication, not abstraction. The ranker seam makes the deletion test pass: there is one
`maybe_insert_with_ranker`, and deleting a ranker removes exactly one policy's worth of behavior and
nothing else.

## Current state

### Two of the three samplers, side by side (`shard/eviction.rs`)

`sample_for_eviction` (LRU), `:195-224`:

```rust
fn sample_for_eviction(&mut self, volatile_only: bool) {
    let samples = self.eviction.config.maxmemory_samples;
    let now = Instant::now();

    let keys = if volatile_only {
        self.store.sample_volatile_keys(samples)
    } else {
        self.store.sample_keys(samples)
    };

    let shard_label = self.shard_id().to_string();
    self.observability.metrics_recorder.increment_counter(
        "frogdb_eviction_samples_total",
        keys.len() as u64,
        &[("shard", &shard_label)],
    );

    for key in keys {
        if let Some(metadata) = self.store.get_metadata(&key) {
            let candidate = EvictionCandidate::from_metadata(
                key,
                metadata.last_access,
                metadata.lfu_counter,
                metadata.expires_at,
                now,
            );
            self.eviction.pool.maybe_insert_lru(candidate); // <-- the only difference
        }
    }
}
```

`sample_for_eviction_lfu`, `:227-256` — character-for-character identical except the last line:

```rust
fn sample_for_eviction_lfu(&mut self, volatile_only: bool) {
    let samples = self.eviction.config.maxmemory_samples;
    let now = Instant::now();

    let keys = if volatile_only {
        self.store.sample_volatile_keys(samples)
    } else {
        self.store.sample_keys(samples)
    };

    let shard_label = self.shard_id().to_string();
    self.observability.metrics_recorder.increment_counter(
        "frogdb_eviction_samples_total",
        keys.len() as u64,
        &[("shard", &shard_label)],
    );

    for key in keys {
        if let Some(metadata) = self.store.get_metadata(&key) {
            let candidate = EvictionCandidate::from_metadata(
                key,
                metadata.last_access,
                metadata.lfu_counter,
                metadata.expires_at,
                now,
            );
            self.eviction.pool.maybe_insert_lfu(candidate); // <-- the only difference
        }
    }
}
```

`sample_for_eviction_ttl` (`:259-284`) is the same body with `maybe_insert_ttl` and a hard-coded
`self.store.sample_volatile_keys(samples)` (TTL is always volatile-only). The metric block at
`:205-210`, `:237-242`, `:265-270` is identical in all three.

### The eviction methods that select a sampler (`shard/eviction.rs:156-192`)

```rust
fn evict_lru(&mut self, volatile_only: bool) -> bool {
    self.sample_for_eviction(volatile_only);             // <-- only difference
    if let Some(candidate) = self.eviction.pool.pop_worst() {
        self.delete_for_eviction(&candidate.key)
    } else {
        false
    }
}

fn evict_lfu(&mut self, volatile_only: bool) -> bool {
    self.sample_for_eviction_lfu(volatile_only);         // <-- only difference
    if let Some(candidate) = self.eviction.pool.pop_worst() {
        self.delete_for_eviction(&candidate.key)
    } else {
        false
    }
}

fn evict_ttl(&mut self) -> bool {
    self.sample_for_eviction_ttl();                      // <-- only difference
    if let Some(candidate) = self.eviction.pool.pop_worst() {
        self.delete_for_eviction(&candidate.key)
    } else {
        false
    }
}
```

`demote_lru` (`:287-297`) and `demote_lfu` (`:300-310`) are the same skeleton, calling the LRU/LFU
sampler and then `demote_for_eviction` instead of `delete_for_eviction` — two more copies of the
sample-then-pop-worst loop.

### The pool inserter (`eviction/pool.rs:148-186`), differing only by `_rank()`

```rust
pub fn maybe_insert_lru(&mut self, candidate: EvictionCandidate) -> bool {
    // Don't add duplicates
    if self.candidates.iter().any(|c| c.key == candidate.key) {
        return false;
    }

    let rank = candidate.lru_rank();                       // <-- the only difference

    if self.is_full() {
        let min_rank = self
            .candidates
            .iter()
            .map(|c| c.lru_rank())                          // <-- the only difference
            .min()
            .unwrap_or(0);
        if rank <= min_rank {
            return false;
        }
        if let Some(pos) = self
            .candidates
            .iter()
            .position(|c| c.lru_rank() == min_rank)         // <-- the only difference
        {
            self.candidates.remove(pos);
        }
    }

    let pos = self
        .candidates
        .iter()
        .position(|c| c.lru_rank() < rank)                  // <-- the only difference
        .unwrap_or(self.candidates.len());
    self.candidates.insert(pos, candidate);
    true
}
```

`maybe_insert_lfu` (`:195-233`) is identical with `lfu_rank`; `maybe_insert_ttl` (`:241-284`) is
identical with `ttl_rank` plus one extra admit guard at the top:

```rust
// Only consider keys with TTL
if candidate.ttl_remaining.is_none() {
    return false;
}
```

The ranking primitives already exist as three methods on `EvictionCandidate`
(`eviction/pool.rs:70-88`): `lru_rank`, `lfu_rank`, `ttl_rank`, all returning `u64` where higher =
worse = evicted first (consistent with `pop_worst` removing index 0). They are exactly the seam — the
only thing that varies between the three copies — but they are inlined into duplicated bodies rather
than parameterized over.

## Proposed design

Introduce one seam — `EvictionRanker` — that captures the single varying decision (how to rank a
candidate, and whether the candidate is eligible at all). Three generic methods replace the nine
triplicated bodies.

### The seam (`eviction/ranker.rs`, new)

```rust
/// Total-order key for eviction: higher = worse = evicted first.
/// Matches `EvictionPool::pop_worst`, which removes the highest-ranked candidate.
pub type RankKey = u64;

/// How a `maxmemory-policy` orders eviction candidates.
///
/// `rank` returns `None` for a candidate the policy refuses to evict at all
/// (volatile-ttl rejects keys with no TTL), folding the old TTL admit-guard
/// into the ranking decision. Everything else about sampling and pool
/// maintenance is policy-independent and lives behind the generic methods.
pub trait EvictionRanker {
    fn rank(&self, candidate: &EvictionCandidate) -> Option<RankKey>;
}

pub struct LruRanker;
impl EvictionRanker for LruRanker {
    #[inline]
    fn rank(&self, c: &EvictionCandidate) -> Option<RankKey> {
        Some(c.idle_time.as_micros() as u64) // was EvictionCandidate::lru_rank
    }
}

pub struct LfuRanker;
impl EvictionRanker for LfuRanker {
    #[inline]
    fn rank(&self, c: &EvictionCandidate) -> Option<RankKey> {
        Some(255 - c.lfu_value as u64) // was lfu_rank: lower counter = worse
    }
}

pub struct TtlRanker;
impl EvictionRanker for TtlRanker {
    #[inline]
    fn rank(&self, c: &EvictionCandidate) -> Option<RankKey> {
        // `None` here == the old `ttl_remaining.is_none()` admit guard.
        c.ttl_remaining
            .map(|ttl| u64::MAX - ttl.as_micros() as u64) // was ttl_rank
    }
}
```

The three `lru_rank`/`lfu_rank`/`ttl_rank` methods on `EvictionCandidate` fold into these three impls
and are deleted (the deletion test for the rank seam itself).

### The generic pool inserter (`eviction/pool.rs`)

```rust
pub fn maybe_insert_with_ranker<R: EvictionRanker>(
    &mut self,
    candidate: EvictionCandidate,
    ranker: &R,
) -> bool {
    // Admit guard (replaces TTL's `ttl_remaining.is_none()` early return).
    let Some(rank) = ranker.rank(&candidate) else {
        return false;
    };

    // Don't add duplicates.
    if self.candidates.iter().any(|c| c.key == candidate.key) {
        return false;
    }

    if self.is_full() {
        // Pooled candidates were all admitted, so `rank()` is always `Some` here;
        // `filter_map` drops the impossible `None` without affecting the min.
        let min_rank = self
            .candidates
            .iter()
            .filter_map(|c| ranker.rank(c))
            .min()
            .unwrap_or(0);
        if rank <= min_rank {
            return false;
        }
        if let Some(pos) = self
            .candidates
            .iter()
            .position(|c| ranker.rank(c) == Some(min_rank))
        {
            self.candidates.remove(pos);
        }
    }

    let pos = self
        .candidates
        .iter()
        .position(|c| ranker.rank(c) < Some(rank)) // pooled ranks are Some; ordering identical
        .unwrap_or(self.candidates.len());
    self.candidates.insert(pos, candidate);
    true
}
```

`pop_worst`/`peek_worst`/`remove`/`clear` are already ranking-agnostic and are untouched.

### The generic sampler and evictor (`shard/eviction.rs`)

```rust
fn sample_with_ranker<R: EvictionRanker>(&mut self, volatile_only: bool, ranker: &R) {
    let samples = self.eviction.config.maxmemory_samples;
    let now = Instant::now();

    let keys = if volatile_only {
        self.store.sample_volatile_keys(samples)
    } else {
        self.store.sample_keys(samples)
    };

    let shard_label = self.shard_id().to_string();
    self.observability.metrics_recorder.increment_counter(
        "frogdb_eviction_samples_total",
        keys.len() as u64,
        &[("shard", &shard_label)],
    );

    for key in keys {
        if let Some(metadata) = self.store.get_metadata(&key) {
            let candidate = EvictionCandidate::from_metadata(
                key,
                metadata.last_access,
                metadata.lfu_counter,
                metadata.expires_at,
                now,
            );
            self.eviction.pool.maybe_insert_with_ranker(candidate, ranker);
        }
    }
}

fn evict_with_ranker<R: EvictionRanker>(&mut self, volatile_only: bool, ranker: &R) -> bool {
    self.sample_with_ranker(volatile_only, ranker);
    match self.eviction.pool.pop_worst() {
        Some(candidate) => self.delete_for_eviction(&candidate.key),
        None => false,
    }
}

fn demote_with_ranker<R: EvictionRanker>(&mut self, volatile_only: bool, ranker: &R) -> bool {
    self.sample_with_ranker(volatile_only, ranker);
    match self.eviction.pool.pop_worst() {
        Some(candidate) => self.demote_for_eviction(&candidate.key),
        None => false,
    }
}
```

### How `EvictionPolicy` maps to a ranker

`evict_one` (`shard/eviction.rs:122-135`) stops dispatching to nine bespoke methods and instead picks
a ranker + the `volatile_only` flag. The flag stays a *parameter*, not part of the ranker, because
`AllkeysLru` and `VolatileLru` share `LruRanker` and differ only in scope:

```rust
fn evict_one(&mut self) -> bool {
    match self.eviction.config.policy {
        EvictionPolicy::NoEviction      => false,
        EvictionPolicy::AllkeysRandom   => self.evict_random(false),
        EvictionPolicy::VolatileRandom  => self.evict_random(true),
        EvictionPolicy::AllkeysLru      => self.evict_with_ranker(false, &LruRanker),
        EvictionPolicy::VolatileLru     => self.evict_with_ranker(true,  &LruRanker),
        EvictionPolicy::AllkeysLfu      => self.evict_with_ranker(false, &LfuRanker),
        EvictionPolicy::VolatileLfu     => self.evict_with_ranker(true,  &LfuRanker),
        EvictionPolicy::VolatileTtl     => self.evict_with_ranker(true,  &TtlRanker),
        EvictionPolicy::TieredLru       => self.demote_with_ranker(false, &LruRanker),
        EvictionPolicy::TieredLfu       => self.demote_with_ranker(false, &LfuRanker),
    }
}
```

Random stays separate: it has no pool, no ranking, and no sampling-into-pool step
(`evict_random`, `:138-153`) — forcing it through the ranker seam would be a false abstraction. The
seam covers exactly the policies that share the sample-rank-pool-pop machinery.

### Before / after, by the numbers

| | Before | After |
|---|--------|-------|
| `shard/eviction.rs` | `evict_lru`+`evict_lfu`+`evict_ttl`+`demote_lru`+`demote_lfu` (5 fns) + `sample_for_eviction`+`_lfu`+`_ttl` (3 fns) | `evict_with_ranker`+`demote_with_ranker`+`sample_with_ranker` (3 fns) |
| `eviction/pool.rs` | `maybe_insert_lru`+`_lfu`+`_ttl` (~114 lines) + 3 `*_rank` methods | `maybe_insert_with_ranker` (~40 lines) |
| new policy costs | a sampler + a pool inserter + an evict method + a `*_rank` method + a match arm | one `EvictionRanker` impl + one match arm |

### Why this is the right depth

- **Locality.** The eviction-pool replace-the-best algorithm (the trickiest code: find-min,
  threshold-compare, remove-best, sorted-insert) lives once. The sample-then-insert loop lives once.
  The sample metric lives once. A bug in any of them — e.g. the `rank <= min_rank` threshold, or the
  off-by-one risk in `position(|c| ... < rank)` — is fixed in one place instead of three.
- **Leverage.** `EvictionRanker::rank` is a ~3-line pure function. Adding a real new policy (Redis is
  unlikely to, but a cost/size-aware ranker for FrogDB's tiering story is plausible) is a ranker impl
  + one `evict_one` arm — no new sampler, no new pool method, no new evict wrapper.
- **Deletion test passes.** After migration there is one inserter and one sampler; deleting
  `LfuRanker` removes LFU and only LFU. Today, deleting `maybe_insert_lfu` leaves two intact copies
  of the same algorithm — proof the current shape is duplication, not a seam.
- **Honest scope.** This is not a new adapter layer wrapped around the pool that callers may bypass;
  it parameterizes the *existing* methods on the one axis that already varies (`_rank()`), then
  deletes the copies. The interface (`rank → Option<RankKey>`) is smaller than the implementation it
  fronts (sampling, pool maintenance, metrics), which is the test for a seam worth having.

## Migration plan

Behavior-preserving throughout: the candidate selected for eviction must be bit-identical to today
for every policy. Each phase compiles and passes `just test frogdb-core`.

1. **Phase 0 — add the seam, no callers.** New `eviction/ranker.rs`: `EvictionRanker`, `RankKey`,
   `LruRanker`/`LfuRanker`/`TtlRanker`. Add `maybe_insert_with_ranker` to `EvictionPool` alongside
   the existing three. Add a parity test: for random candidate sequences, assert
   `maybe_insert_with_ranker(c, &LruRanker)` produces the same pool contents+order as
   `maybe_insert_lru(c)` (and likewise LFU/TTL). Nothing else changes.
2. **Phase 1 — collapse the pool inserters.** Rewrite `maybe_insert_lru`/`_lfu`/`_ttl` as one-line
   delegations to `maybe_insert_with_ranker(c, &LruRanker)` etc. The existing pool unit tests
   (`test_pool_insert_lru`/`_lfu`/`_ttl`, `pool.rs:330-416`) keep passing unchanged — they are now
   the equivalence proof. Fold the `*_rank` methods' bodies into the rankers.
3. **Phase 2 — add the generic sampler/evictor.** Add `sample_with_ranker`, `evict_with_ranker`,
   `demote_with_ranker`. Repoint `evict_one` to the ranker map above. The old
   `evict_lru`/`_lfu`/`_ttl`/`demote_lru`/`demote_lfu` and `sample_for_eviction*` are now unused.
4. **Phase 3 — delete the triplicates.** Remove `evict_lru`/`_lfu`/`_ttl`, `demote_lru`/`demote_lfu`,
   `sample_for_eviction`/`_lfu`/`_ttl`, the three thin `maybe_insert_*` delegations, and the three
   `*_rank` methods. FrogDB is pre-production — no deprecation shims. Confirm zero remaining callers
   (`rg -n "maybe_insert_lru|sample_for_eviction_lfu|evict_ttl|lfu_rank"`).
5. **Gate (optional).** A grep gate in `just lint` that fails if a new `fn maybe_insert_[a-z]+\b` or
   `fn sample_for_eviction_` reappears, nudging future policies toward a ranker. Lower value than the
   proposal-02 gate (this duplication is contained to two files) — include only if it earns its keep.

## Testing impact

- **The pool algorithm is tested once, parametrized.** Today the full-pool replace-worst path is
  only meaningfully exercised by `test_pool_capacity` (`pool.rs:397-416`) against the LRU copy; the
  LFU and TTL copies of that branch are effectively untested. A single matrix over
  `[LruRanker, LfuRanker, TtlRanker]` covers duplicate-rejection, fill-to-capacity, replace-worse,
  reject-better, and sorted-insert for every ranker.
- **Each ranker's `rank()` is a pure, isolated unit test.** `candidate → Option<RankKey>`: LRU
  monotonic in idle time, LFU inverted in counter, TTL `None` for no-TTL keys and monotone-decreasing
  in remaining TTL. The TTL admit guard is now a `rank() == None` assertion instead of behavior
  buried inside `maybe_insert_ttl`.
- **New policy = a ranker test, not a method's worth of coverage.** A new ranker needs one `rank()`
  unit test; it inherits the pool-maintenance and sampling coverage for free.
- **Equivalence is pinned during migration.** The Phase 0 parity test and the unchanged
  `test_pool_insert_*` tests are the bit-identical-behavior safety net; the existing eviction
  integration/maxmemory tests are the end-to-end check that no key-selection changed.

## Risks / open questions

- **Monomorphization vs dyn dispatch (hot path).** `evict_one` runs under memory pressure on the
  write path, up to 10× per rejected write (`check_memory_for_write`, `:70-97`), and
  `sample_with_ranker` calls `rank()` once per pooled candidate per inserted sample (O(samples ×
  pool_size) rank calls per eviction). Prefer **generics** (`<R: EvictionRanker>`) with `#[inline]`
  rankers so each call site monomorphizes to the same machine code as today's inlined `*_rank()` —
  no vtable, no indirect call in the inner loop. Code bloat is three instantiations, i.e. exactly
  today's three copies. Use `&dyn EvictionRanker` only if a future config-driven/pluggable ranker
  registry is wanted, and only after measuring.
- **`volatile_only` stays a parameter, not part of the ranker.** `AllkeysLru`/`VolatileLru` share
  `LruRanker`; only the sampling scope differs. Keeping the flag explicit at the `evict_one` call
  site avoids re-coupling scope into the ranker and keeps `TtlRanker` (always `volatile_only = true`)
  honest. Risk: a future contributor passes the wrong flag for a new policy — mitigated by the
  `evict_one` match being the single mapping site.
- **Metric-label correctness per policy.** `frogdb_eviction_samples_total` is incremented identically
  in all three samplers with only a `("shard", …)` label, while its siblings
  `frogdb_eviction_keys_total` (`:392`) and `frogdb_tiered_demotions_total` (`:328`) also carry
  `("policy", …)`. Concentrating the increment into `sample_with_ranker` is the natural moment to add
  the `policy` label for parity — but that changes metric cardinality and dashboards, so treat it as
  a **deliberate, separate decision**, not a silent ride-along. Default Phase 0-3 keeps the metric
  byte-identical (same name, same single label, same `keys.len()` value). See Correctness flags.
- **Keeping behavior bit-identical.** The `Option<RankKey>` ordering must match the old recompute
  semantics exactly. Pooled candidates always rank to `Some`, so `ranker.rank(c) < Some(rank)`
  preserves the old `c.X_rank() < rank` insert position and `== Some(min_rank)` preserves the
  replace target. The admit-before-duplicate-check reordering is observably identical (both paths
  `return false` without mutating the pool). The Phase 0 property test (old vs new over randomized
  candidate streams) is the guard against any of these going subtly wrong.

## Correctness flags

Found and verified while writing this proposal. Both are adjacent to the refactor and separable from
it.

- **Eviction-policy valid list is a 4th hardcoded copy in CONFIG SET — drift risk.**
  `server/src/runtime_config.rs:513-524` hardcodes the ten policy strings (`"noeviction"`,
  `"volatile-lru"`, …) and validates via `valid_policies.contains(&lower.as_str())` (`:526`). This
  duplicates `EvictionPolicy::FromStr` (`core/src/eviction/policy.rs:147-167`) and
  `EvictionPolicy::all_names()` (`policy.rs:106-119`), which already exist precisely as the single
  source of truth for "valid policy names." The drift is sharpened by `build_eviction_config`
  (`server/src/server/util.rs:98-106`), which `parse::<EvictionPolicy>()`s the stored string and
  `unreachable!()`s on failure — it *trusts* the CONFIG SET list to be a perfect mirror of `FromStr`.
  Add a policy to the enum + `FromStr` + `all_names()` (the natural edit) and forget the CONFIG SET
  array, and `CONFIG SET maxmemory-policy <new>` rejects a valid policy; conversely any divergence
  that lets an unparseable value through turns the `unreachable!` into a startup panic. Fix: replace
  the array + `contains` with `val.parse::<EvictionPolicy>()` (or check against
  `EvictionPolicy::all_names()`). **Overlap:** this same CONFIG-SET-vs-canonical-parser duplication is
  called out in the runtime-config-validation proposal (16); whichever lands first should retire the
  hardcoded list, and the other should drop the duplicate flag.

- **`frogdb_eviction_samples_total` is missing the `policy` label its siblings carry (metric-label
  inconsistency, ×3).** All three samplers increment it with only `("shard", &shard_label)`
  (`shard/eviction.rs:206-210`, `:238-242`, `:266-270`), but `frogdb_eviction_keys_total` (`:391-395`)
  and `frogdb_tiered_demotions_total` (`:327-331`) also tag `("policy", …)`. Result: sample volume
  cannot be attributed per policy, and the gap is replicated identically in all three copies — so the
  inconsistency is itself a symptom of the triplication, and the generic `sample_with_ranker` is the
  one place to fix it. This is a label-coverage gap, **not** an off-by-one: the increment value
  `keys.len()` correctly counts keys returned by the sampler. (The loop then skips keys with no
  metadata, so the metric counts "keys sampled," not "candidates evaluated" — defensible given the
  metric name, but worth a doc comment when the code is consolidated.)
