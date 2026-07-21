# Spec: architecture/consistency.md
Status: rewrite (guarantees must be re-grounded in what tests actually verify, and
one central config knob it cites does not exist)
Audiences: A2 (skeptics), A3 (architecture-curious)

Goal: The reader understands exactly what consistency FrogDB provides — and, for
each guarantee, whether it is a **tested guarantee** (a named test/checker
verifies it) or a **design intent** (the design implies it but no test proves it).
The reader learns the single-node model (per-key total order, per-shard
linearizability, read-your-writes on a connection), the durability modes and their
crash-loss windows, the cluster model (async command replication, eventual
convergence, what is lost on failover), what is explicitly NOT provided
(cross-node linearizability, snapshot isolation, causal consistency), and the
split-brain behavior (divergent writes discarded and audit-logged). A skeptic
leaves able to check each claim against a specific Jepsen workload or the internal
linearizability checker.

Not in scope:
- Replication/cluster mechanics — architecture/replication and
  architecture/clustering (link). This page is about guarantees, not machinery.
- VLL multi-shard atomicity internals — architecture/vll (link); this page states
  the guarantee and points there.
- The testing pyramid overview — that is the Compatibility & Correctness →
  Testing methodology page (link). This page cites specific consistency checkers
  as evidence, not the whole test story.

Sources of truth (author MUST read; this page's credibility depends on tying each
claim to a test, so verify the test set before writing):
- `frogdb-server/crates/testing/` — `checker.rs` (WGL linearizability checker:
  `check_linearizability`, `check_linearizability_bounded`), `models.rs`
  (`RegisterModel`, `KVModel`), `history.rs`.
- `frogdb-server/crates/server/tests/simulation.rs` — where the checker is applied
  (`test_linearizability_concurrent_writes`, `test_linearizability_single_key_serial`,
  `test_detects_non_linearizable_history`, `test_detects_stale_read`). These are
  single-node / single-key.
- `testing/jepsen/frogdb/src/jepsen/frogdb/` — workload registry (`core.clj`) and
  checkers: `register.clj` / `hash.clj` (Knossos linearizability),
  `list_append.clj` / `elle_rw_register.clj` (Elle strict-serializable, single-slot
  via hash tags), `counter.clj` (counter checker), and the custom-checker
  workloads `split_brain.clj`, `replication.clj`, `leader_election.clj`,
  `partition_recovery.clj`, `migration_recovery.clj`, `cross_slot.clj`, `lag.clj`,
  etc. Read each workload's `:consistency-models` / checker to classify what it
  proves.
- `frogdb-server/crates/persistence/src/wal/config.rs` — `DurabilityMode`
  (`Async`, `Periodic { interval_ms }`, `Sync`; default `Periodic { 1000 }`).
- `frogdb-server/crates/config/src/persistence.rs` — `durability_mode` string
  (`"async"|"periodic"|"sync"`, default `"periodic"`), `sync_interval_ms`.
- `frogdb-server/crates/config/src/cluster.rs` — `self_fence_on_quorum_loss`
  (bool), election/connect/request timeouts.
- `frogdb-server/crates/config/src/replication.rs` — `self_fence_on_replica_loss`
  (bool), `min_replicas_to_write`, `min_replicas_timeout_ms`.
- `frogdb-server/crates/replication/src/split_brain_log.rs` — divergent-write
  discard audit trail.

Existing content: current `architecture/consistency.md`. Keep the overall
structure and the durability table (it is correct). FACTUAL DISCREPANCIES to fix:

1. **`fencing_timeout_ms` (with "default 10s") does not exist.** Grep confirms it
   appears only in this doc. The real fencing controls are the booleans
   `self_fence_on_quorum_loss` and `self_fence_on_replica_loss` — there is no
   fencing *timeout* knob. Rewrite the "Split-Brain Window" section and the
   "Reducing Split-Brain Risk" list: drop "Lower `fencing_timeout_ms`," describe
   the real boolean fencing options and what they do, and describe the split-brain
   window qualitatively (bounded by failover/detection timing, not a named 10s
   knob) unless a real bounding config is found.
2. **Guarantees stated as fact that no test verifies.** The page asserts
   read-your-writes, monotonic reads, and the three "NOT provided" negatives as if
   proven. They are **design intent**, not test-covered. The rewrite MUST label
   each guarantee with its evidence status (see the required treatment below).
3. Durability table is correct — keep it. Verify mode names against
   `DurabilityMode` and note default is `periodic` (1000ms).

REQUIRED TREATMENT — tested vs. design intent. Every guarantee on this page MUST
carry an explicit evidence tag. Use a consistent convention (e.g. a "Verified by"
column or an inline badge) with three values: **Tested** (name the checker/test),
**Design intent** (no test; implied by the design), **Asserted negative** (a
"not provided" claim; note it is not disproven by a passing test either). Baseline
classification from source (re-verify before publishing):
- Per-key total order / per-shard (single-key) linearizability — **Tested**:
  internal WGL checker on the real server (`simulation.rs`) + Jepsen
  `register.clj` / `hash.clj` (Knossos).
- Strict serializability on single-slot keys — **Tested**: Jepsen Elle
  (`list_append.clj`, `elle_rw_register.clj`) — note keys are hash-tagged to one
  slot, so this is per-slot, not cross-slot.
- Per-mode durability (async/periodic/sync crash windows) — **Tested**: crash
  recovery tests (`core/src/persistence/crash_recovery_tests.rs`).
- Split-brain convergence / at-most-one-master / replica-write rejection —
  **Tested**: Jepsen `split_brain.clj` custom checker (verifies convergence and no
  rogue master, NOT that specific acked writes are lost in a timed window).
- CROSSSLOT rejection — **Tested**: Jepsen `cross_slot.clj`.
- Read-your-writes, monotonic reads — **Design intent** (follow from
  linearizability on a connection; no dedicated test).
- No cross-node linearizability — **Asserted negative** (consistent with reality;
  all linearizability checks are per-slot/single-node; no test demonstrates a
  cross-node non-linearizable history as expected).
- No snapshot isolation, no causal consistency — **Asserted negative** (no checker
  for either; Elle runs strict-serializable, not SI or causal).

Structure (H2/H3 outline):

## How to read this page (short preamble)
- Explain the evidence tags up front: Tested (linked to a checker), Design intent,
  Asserted negative. State that FrogDB's consistency claims are deliberately tied
  to tests, and that untested claims are labeled as such. Link the Testing
  methodology page.

## Single-node guarantees
- Per-key total order; per-shard linearizability (Tested). Read-your-writes on a
  connection and across reconnect (Design intent; note the failover caveats).
  Monotonic reads (Design intent). Keep the read-your-writes scenario table but
  add the evidence tag and correct the mitigation (see fencing fix).

## Durability
- The async/periodic/sync table (Tested via crash-recovery). State the default
  (periodic, 1000ms) and the crash-loss window per mode. Link
  architecture/persistence.

## Cluster consistency
- Async command replication; eventual convergence; failover loss window. What is
  preserved vs lost vs discarded (keep the table; retie "discarded" to the
  split-brain audit log, not to a nonexistent timeout). Read-from-replica
  staleness. Evidence tags throughout.

## Split-brain behavior
- The real model: on partition heal, a demoted former primary's divergent writes
  are discarded and written to the `split_brain_discarded_<ts>.log` audit file.
  Fencing options: `self_fence_on_quorum_loss`, `self_fence_on_replica_loss`
  (booleans) and `WAIT`/`min-replicas-to-write` as risk-reduction levers.
  **Tested**: Jepsen `split_brain.clj` (convergence + at-most-one-master). Remove
  every reference to `fencing_timeout_ms`.

## Guarantees not provided
- Cross-node linearizability, snapshot isolation, causal consistency — each an
  **Asserted negative**. Be explicit that these are design boundaries, and that
  "not provided" here means "not offered and not tested for," not "proven absent."

## Transaction consistency
- MULTI/EXEC atomicity + no rollback; keys must share an internal shard (hash
  tags); WATCH optimistic locking. Transaction durability follows the durability
  modes. Verify which of these have tests; tag accordingly (MULTI/EXEC atomicity
  likely has unit/integration coverage — confirm and cite, else mark design
  intent).

## Ordering guarantees
- Within a connection (in-order, pipelining preserves order); no cross-connection
  ordering; pub/sub per-channel order + at-most-once. Tag; most are design intent
  unless a test exists.

Generated data: none. Any config default (durability interval, fencing booleans)
should be pulled via `<ConfigDefault>` (config-reference.json, S6/docs-gen) rather
than hardcoded, so the page tracks the binary.

Drift guards:
- S7 code-path check covers cited `crates/testing/...`, `simulation.rs`,
  `persistence/...`, and config paths. Jepsen paths under `testing/jepsen/...`
  should also be covered by S7 (extend its scope if it is crates-only today —
  flag).
- The evidence tags are the core drift risk: if a checker is removed or a workload
  renamed, a "Tested" claim silently becomes false. Recommend (flag for the test
  owners) that each "Tested" tag reference a stable test/workload identifier that
  a CI grep can assert still exists. Until then, a reviewer must re-verify the tag
  set against `testing/jepsen` and `crates/testing` on every edit.
- No hardcoded config knob names in prose that don't exist in
  `config-reference.json` — that is exactly how `fencing_timeout_ms` slipped in.
  A build check that greps prose config-key mentions against the generated config
  key set would catch this class of error (S8-adjacent; flag).
