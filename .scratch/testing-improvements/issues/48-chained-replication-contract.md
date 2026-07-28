# Pin or reject chained replication (replica-of-replica) contract

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: Replication

## Context

A replica is constructed with a `NoopBroadcaster` (`server/src/replication/replication_init.rs:177-181`),
and frames it applies are attributed to `REPLICA_INTERNAL_CONN_ID`, which suppresses further
broadcast. Practically, this means if a client issues `REPLICAOF` pointing a second replica at an
existing replica (chained replication / replica-of-replica), the write stream silently does not
propagate to the sub-replica — `REPLICAOF`/`PSYNC` targeting a replica is not rejected at the
command layer, so the operation appears to succeed but data never flows.

There is no test and no documented contract for this scenario today (verdict CONFIRMED L2/C1). This
is a silent-failure footgun: an operator building a fan-out replication topology (a common Redis
pattern) gets no error and no data, with the only symptom being an empty/stale sub-replica.

## What to build

Establish and pin the intended contract. Two valid outcomes exist and the choice affects
implementation scope:

- **(a) Support it**: implement actual chained replication so writes flow transitively through a
  replica to its own replicas, and surface chain depth/topology in `INFO`/`CLUSTER NODES`.
- **(b) Reject it explicitly**: validate at `REPLICAOF`/`PSYNC` command-handling time that the target
  is a primary, not a replica, and return a clear, documented error instead of silently accepting a
  connection that never receives data.

Regardless of which is chosen, first add a test that pins the *current* behavior (data does not flow,
per the `NoopBroadcaster`/`REPLICA_INTERNAL_CONN_ID` suppression) so the gap is at least visible and
guarded before the design decision lands.

## Acceptance criteria

- [x] Test pinning current behavior: `REPLICAOF` from node B pointing at existing replica A results
      in B accepting the connection but never receiving primary-origin writes (documents the
      as-is silent-failure state).
- [x] Design decision made and recorded: support chained replication (a) or reject it explicitly (b).
      → (b), and already mechanically enforced (see Resolution).
- [x] Integration test asserting the decided contract end-to-end.
- [x] Docs (operations/replication or clustering docs) updated to state the contract explicitly.
- [ ] If (a) is chosen, chain depth/topology is visible via INFO/CLUSTER NODES. — N/A, (b) was chosen.

## Blocked by

Design decision on target contract (support vs. reject) — flag for human input before implementing
the fix; the regression-pin-only test for current behavior can start immediately.

## References

- .scratch/testing-improvements/audit/E-replication.md #9 (`chained-replication-behavior-undefined`)
- .scratch/testing-improvements/audit/verdicts-E.md #9 (CONFIRMED L2/C1)
- server/src/replication/replication_init.rs:177-181

## Resolution

Contract decision: **(b) reject** — chained replication is not supported, and
FrogDB already, mechanically, rejects it rather than silently dropping data.
Investigating the actual runtime behavior (not just the `NoopBroadcaster`
framing quoted above) showed the situation is better than "silent no data
with no error":

- A node's `PrimaryReplicationHandler` (the thing that answers `PSYNC`) is
  wired only when the node is booted or promoted as a primary
  (`frogdb-server/crates/server/src/server/replication_init.rs:104-206`; a
  replica always gets `NoopBroadcaster` and `primary_replication_handler:
  None`).
- `PSYNC` dispatch (`frogdb-server/crates/server/src/connection/dispatch.rs`,
  `DispatchStage::PsyncIntercept`) presence-gates on that handler and
  short-circuits with `-ERR PSYNC not supported - server is not running as
  primary` when it is absent — for *any* non-primary target, replica or
  standalone.
- So a sub-replica `B` issuing `REPLICAOF <host-of-replica-A> <port>` gets a
  synchronous `+OK` from `B` (matching Redis's async `REPLICAOF` semantics —
  `B` cannot know `A`'s role without a round trip), but `B`'s background
  reconnect loop then has every `PSYNC` attempt against `A` rejected with the
  explicit `-ERR ... not running as primary`. `B` retries with backoff
  forever, `master_link_status` never reaches `up`, and no primary-origin
  write ever reaches it. This is the *same* mechanism already pinned for a
  manually-promoted node in
  `test_promoted_node_via_replicaof_no_one_rejects_downstream_psync` — issue
  48 is that same reject path hit via the far more common trigger (a plain,
  never-promoted replica as the chain target), which had no test coverage.

No code change was needed to *make* the behavior explicit — it already is,
at the protocol layer. What was missing was: a test pinning this exact
scenario, and a documented contract statement (an operator staring at
`master_link_status:down` plus a generic "not running as primary" log line
had no doc to confirm this is intentional rather than a bug to chase).

### What changed

- **Test** (`frogdb-server/crates/server/tests/integration_replication.rs`,
  `test_chained_replication_rejected_sub_replica_never_receives_data`, 2
  cases: in-memory + persistence): boots primary → replica_a → (at runtime)
  `REPLICAOF` from a third node at replica_a. Asserts: `REPLICAOF` returns
  `+OK` and flips `ROLE` to `slave`; `master_link_status` stays `down`;
  neither a pre-existing key nor a write issued after attach ever reaches
  the sub-replica; and a raw `PSYNC` against replica_a returns an explicit
  `-ERR ... not running as primary` (never `CONTINUE`/`FULLRESYNC`).
- **Docs** (`website/src/content/docs/operations/replication.md`, "How
  FrogDB differs from Redis replication"): added a bullet stating FrogDB
  does not support chained replication (replica-of-replica), naming the
  exact error a chain attempt gets and instructing operators to point every
  replica directly at the primary.

### Not done (explicitly out of scope per the pinned contract)

- No chain depth/topology in `INFO`/`CLUSTER NODES` — not applicable, since
  chaining is rejected rather than supported.
- No change to the `PSYNC` error message itself (still the generic "not
  running as primary" shared with the standalone case) — it already
  communicates the real invariant (no live primary handler) accurately;
  inventing a chain-specific message would require the primary-role
  target to know a request came from a chaining attempt specifically,
  which it cannot distinguish from a first-time client probe.

### Verification

`just test frogdb-server test_chained_replication_rejected_sub_replica_never_receives_data`
run 3x clean:

```
Summary [ 3.204s] 2 tests run: 2 passed, 1928 skipped
Summary [ 3.204s] 2 tests run: 2 passed, 1928 skipped
Summary [ 3.167s] 2 tests run: 2 passed, 1928 skipped
```

`cargo clippy -p frogdb-server --tests -- -D warnings`: clean (no warnings).
`cargo fmt -p frogdb-server`: no diff.
