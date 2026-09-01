# Two turmoil sims are pre-existing red on main: expiry-suppression realpath + cluster WAIT-across-failover

Status: needs-triage
Type: AFK
Origin: whole-suite turmoil verification during memory-architecture issue 01 (ShardExecutor
seam), 2026-08-31 — noted in passing in
`.scratch/memory-architecture/issues/done/01-shard-executor-seam.md` ("the two failures
reproduce identically... at the parent commit `41ae1177`... pre-existing red, not
regressions") but never filed as its own issue. This issue is that filing.
Severity: likelihood 3/3 (deterministic — reproduces every run, confirmed by two independent
runs at the same commit), consequence unknown until triaged — see "Open question" below;
provisionally 2/3 since one candidate explanation (WAIT test 2, below) points at a gate-ordering
bug in a **locked** area (cluster) of the same class just fixed in `a859e73c`
("cluster redirect outranks the link-down stale gate")
Area: `frogdb-server` turmoil suite — touches expiry (core, locked) and cluster (locked)

## What was seen

Both confirmed pre-existing on `main` at commit `41ae1177` (2026-08-31), reproduced by two
independent runs (176/178 turmoil tests passed both times, same two failures, same
assertions, same payloads). Not attempted to fix; not re-run as part of filing this issue,
per instructions.

### 1. `simulation::client_pause_write_expiry_suppression_realpath`

`frogdb-server/crates/server/tests/simulation.rs:4157` (test fn), assertion at **:4308**:

```rust
assert!(
    reaped_encoding.starts_with(b"-"),
    "S7 expiry sub-assertion: after UNPAUSE the next sweep must reap the \
     backdated key, so `OBJECT ENCODING` reports `no such key` (an error), got \
     {reaped_encoding:?}. ..."
);
```

Reported failure payload: `got [36, 45, 49, 13, 10]`, i.e. the bytes of `$-1\r\n` (a RESP
null bulk string), not an error reply (`-...`).

**What the test asserts** (see the doc comment at :4125–4155): under `CLIENT PAUSE ... WRITE`,
passive expiry is suppressed — a key whose deadline is backdated into the past (via `DEBUG
EXPIRE-BACKDATE`, avoiding any real-clock race) reads as logically gone to `GET` (nil) but is
still physically retained, observable via the expiry-blind `OBJECT ENCODING` returning the
key's encoding rather than an error. After `CLIENT UNPAUSE` plus one more 100ms active-expiry
sweep (virtual-clock `tokio::time::sleep(300ms)`), suppression lifts and the sweep is expected
to *physically* reap the key, so `OBJECT ENCODING k` should then reply `-ERR no such key`
(starts with `-`). Instead it observed `$-1\r\n` — a null bulk reply.

**Failure class, unconfirmed**: `$-1\r\n` is not an error at all, so this isn't "reap happened
late" (which would still eventually error) — either `OBJECT ENCODING` on a genuinely-absent key
sometimes replies with a null bulk instead of the documented `-ERR no such key` (a product
behavior question, possibly a race between the reap and the read within the shard), or the
single-connection RESP framing in this test (`round_trip`, one `read()` per command, no
multi-segment reassembly) misattributes bytes from a differently-ordered reply. Needs a triager
to single-step the scenario.

### 2. `simulation::test_cluster_wait_unblocked_across_failover`

`frogdb-server/crates/server/tests/simulation.rs:5831` (test fn, loops seeds `[1, 7, 42]` over
`run_cluster_wait_across_failover`, defined at :5659), assertion at **:5763**:

```rust
let refused = conn.cmd(&[b"WAIT", b"0", b"0"]).await?;
assert!(
    matches!(&refused, RespValue::Error(e)
        if e.contains("WAIT cannot be used with replica instances")),
    "seed {seed}: the demoted node must reject WAIT like any replica, got {refused:?}"
);
```

Reported failure payload: a `MASTERDOWN Link with MASTER is down...` error instead of the
expected `WAIT cannot be used with replica instances` error.

**What the test asserts** (see the doc comment at :5638–5658): a real graceful `CLUSTER
FAILOVER` (not `TAKEOVER`/`FORCE`) demotes the old primary and promotes its replica while a
`WAIT 2 0` is parked on the old primary. After the demotion lands, the test checks three
things in order: (a) the parked `WAIT` unblocks with `-UNBLOCKED ... master -> replica`, not a
count; (b) the now-demoted node immediately rejects a fresh `WAIT 0 0` the same way any replica
would, with `ERR WAIT cannot be used with replica instances...`
(`WAIT_ON_REPLICA_ERR`, `frogdb-server/crates/server/src/commands/replication.rs:513`); (c) the
promoted node serves `WAIT` as the shard's new primary. It is assertion (b) that fails: the
demoted node instead replies with the generic stale-data gate's `MASTERDOWN` error
(`command_admission.rs:414`, `frogdb-server/crates/types/src/error.rs`).

**Failure class, unconfirmed but a strong lead**: `WAIT` does not carry `CommandFlags::STALE`
(confirmed by grep — no `STALE` mention near `WAIT` in `command_meta.rs` /
`upstream/generated.rs`), so any command admission path that checks "is this a replica with a
down link to its master" *before* the WAIT-specific "replica instances" check
(`replication.rs`) will short-circuit to `MASTERDOWN` first. Right after a fresh demotion the
new replica's link to its new master may not have synced yet, which would make this a
timing/gate-ordering race, not a data-loss bug. This is the same *shape* of bug as `a859e73c`
("cluster redirect outranks the link-down stale gate"), landed on `main` (not yet in this
worktree's history, which branched at `41ae1177`) — worth checking whether `WAIT`'s
replica-rejection needs the same admission-order fix (WAIT's own check should outrank the
generic stale-data gate, or the demoted node's link-down state needs to sync before assertion
(b) runs). Not confirmed by reading the admission order end-to-end — a triager should trace
`command_admission.rs`'s STALE gate against where `WAIT_ON_REPLICA_ERR` is raised in
`commands/replication.rs` to confirm which check runs first today.

## Open question

Product bug vs. harness/checker defect, per test:

1. **Expiry-suppression realpath** — plausibly a real product timing issue (reap racing the
   read) or an OBJECT ENCODING reply-shape question; could also be a test-harness single-read
   framing bug (this test is one of the few in the file using a raw one-`read()`-per-command
   helper instead of the frame-aware `RespConn` used elsewhere, e.g. by test 2).
2. **Cluster WAIT-across-failover** — plausibly a genuine command-admission gate-ordering bug
   (WAIT's replica check vs. the generic link-down stale gate), of the same class as the
   redirect-vs-stale-gate bug just fixed in `bc89b875`; could also be a test race (asserting
   immediately after demotion, before the new replica's master-link status has synced) rather
   than a product bug.

Neither has been root-caused. Whoever picks this up should determine, per test, which bucket it
falls in and either fix the product code (spec-first, since both `frogdb-core` expiry and
`frogdb-cluster` are locked areas — see `CLAUDE.md` § Locked core areas) or fix/harden the test.

## Verification

Not run as part of filing this issue (investigation only, no build required). The originating
verification: `cargo nextest run` turmoil sim suite, 176/178 passed, both failures reproduced
identically at `41ae1177` with and without the ShardExecutor seam change — see
`.scratch/memory-architecture/issues/done/01-shard-executor-seam.md` "Verification" section.

## References

- `frogdb-server/crates/server/tests/simulation.rs:4157` — `client_pause_write_expiry_suppression_realpath`
- `frogdb-server/crates/server/tests/simulation.rs:5659` — `run_cluster_wait_across_failover`
- `frogdb-server/crates/server/tests/simulation.rs:5831` — `test_cluster_wait_unblocked_across_failover`
- `frogdb-server/crates/server/src/commands/replication.rs:513` — `WAIT_ON_REPLICA_ERR`
- `frogdb-server/crates/core/src/command_admission.rs:414` — the `MASTERDOWN` stale-data gate string
- `specs/replication.md:809`, `:859` — cite `test_cluster_wait_degrades_under_partition` and
  `test_cluster_wait_unblocked_across_failover` by name as `FM-REPLICATION-037` forcing tests
  (spec is **LOCKED**); this issue does not touch the spec, only reports the pre-existing red.
- `.scratch/cluster-correctness/issues/done/09-seeded-fault-scheduler.md:114,121` — prior mention
  of `test_cluster_wait_unblocked_across_failover` (kept as a named regression seed, unrelated
  to this failure)
- `.scratch/memory-architecture/issues/done/01-shard-executor-seam.md:136-140` — origin of the
  "pre-existing red, not regressions" finding this issue formalizes
- Recent related fix on `main` (same bug class as the WAIT lead above, not present in this
  worktree's history): `a859e73c` ("cluster redirect outranks the link-down stale gate")
