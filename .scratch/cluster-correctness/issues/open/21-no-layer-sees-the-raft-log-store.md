# 21 — No validation layer sees the Raft log store, so FM-CLUSTER-099 survives every generated check

Status: needs-triage

## Parent

[PRD](../../PRD.md) §6.1 — filed by the retro-validation gate (issue 13). FM-CLUSTER-099
(`get_log_reader` handed out a detached, never-invalidated log cache) is one of the two audit
defects that **no** layer catches when the fix is reverted. §3 W4 owns the closest thing to a
witness; the real owner is a layer that does not exist yet.

## What the experiment showed

Revert (inverse of `4cb1c4b4`, `cluster/src/storage.rs::get_log_reader`):

```rust
// fixed
log_cache: Arc::clone(&self.log_cache),
// reverted
log_cache: Arc::new(RwLock::new(self.log_cache.read().clone())),
```

Result with the two spec forcing tests (`a_log_reader_never_serves_an_entry_the_owner_truncated`,
`cache_invalidation_reaches_a_reader`) excluded from the judgment:

| layer | result |
|---|---|
| L1 invariant catalog + hooks (`just test frogdb-cluster`) | **miss** — 289/291, and the only two failures are the forcing tests |
| L2 properties P1–P4 | structurally out of reach — the properties drive `ClusterStateInner::apply_command`; the log store is below the state machine |
| L3 stateright | structurally out of reach — the models use `apply_command` as the transition function and treat Raft as an ordering oracle |
| L4 seeded schedules | **miss** — `just cluster-seeds 100` green (87.9 s); `just cluster-seeds 500` (`CLUSTER_SEEDS_JOBS=6`, 346.6 s) reproduced exactly the 36 known issue-20 `XNODE-SLOT-1` seeds and nothing else |
| seam gates (`just lint-gates`) | green |

So the defect class — Raft log divergence produced entirely below the state machine — is
invisible to the whole PRD. The 500-seed sweep is the strongest evidence: the leadership flap
that the FM row names as the trigger (append while leader → step down → truncate → re-append at
the same indexes → read through the long-lived reader) does happen in those schedules, but
nothing in the harness compares what a reader serves against what is on disk, so a divergent
read is only visible if it later detonates client-side inside the same run.

## Why the existing layers cannot be stretched to cover it

- W1's catalog is by design pure over `&ClusterStateInner`, with no I/O. A log-store invariant
  is not expressible there without breaking that property.
- W2/W3 sit on the same state machine and inherit the same boundary.
- W4 exercises the store for real but only observes it through client-visible outcomes.

## What to build

An **openraft storage conformance layer** for `ClusterLogStore` — openraft ships
`openraft::testing::Suite` (log-store + state-machine conformance) precisely for this seam, and
the FM-CLUSTER-099 note already records that "nothing outside openraft itself had ever exercised
the reader clone". Concretely:

1. Run openraft's storage test suite against a temp-dir `ClusterLogStore` in `frogdb-cluster`
   (so the mutation gate sees it), including the reader-handle cases.
2. Add the coherence properties the suite does not state, as a small proptest over an
   append/truncate/purge/read command sequence executed against *both* the owning handle and a
   reader obtained from `get_log_reader` before the sequence started: every read through either
   handle must agree with the on-disk column family. That is the generated form of the two
   point witnesses and it would have failed on the first shrink.
3. Feed the same shape into the vote/meta path so FM-CLUSTER-098's *behavioral* half (as
   opposed to the classification lint, see issue 22 sibling note) gets a generated witness too,
   once the campaign-2 crash harness can cut power between the ack and the restart.

## Acceptance criteria

- Reverting the FM-CLUSTER-099 fix makes at least one **non-forcing** test fail, and the
  failure names the divergence (reader index vs. disk content), not a downstream symptom.
- The new tests live in `frogdb-cluster` so `cargo mutants -p frogdb-cluster` scores them.
- `just test frogdb-cluster` stays green on a clean tree and the added runtime is under a
  minute.
