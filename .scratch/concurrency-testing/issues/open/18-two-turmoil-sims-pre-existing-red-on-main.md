# Two turmoil sims are pre-existing red on main: expiry-suppression realpath + cluster WAIT-across-failover

Status: ready-for-human
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
("cluster redirect outranks the link-down stale gate") — and that fix is an ancestor of
`41ae1177`, so the failure reproduces with it in the tree: it did not cover this path. Worth
checking whether `WAIT`'s
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
   redirect-vs-stale-gate bug fixed in `a859e73c` — and that fix is an ancestor of `41ae1177`,
   so the failure reproduces *with* it present, meaning it did not cover the WAIT path; could
   also be a test race (asserting
   immediately after demotion, before the new replica's master-link status has synced) rather
   than a product bug.

Neither has been root-caused. Whoever picks this up should determine, per test, which bucket it
falls in and either fix the product code (spec-first, since both `frogdb-core` expiry and
`frogdb-cluster` are locked areas — see `CLAUDE.md` § Locked core areas) or fix/harden the test.

## Verification

Originating verification (at filing): `cargo nextest run` turmoil sim suite, 176/178 passed,
both failures reproduced identically at `41ae1177` with and without the ShardExecutor seam
change — see `.scratch/memory-architecture/issues/done/01-shard-executor-seam.md`
"Verification" section.

Root-cause verification, 2026-09-01, local mode (see "## Root cause"):

1. **Reproduced both**, deterministically, on the unmodified tree — the two assertions and the
   two payloads reported above, verbatim, first try.
2. **Both pass after the test-only fixes**, and the rest of the turmoil sim suite is unchanged.

## Root cause

Root-caused 2026-09-01, local mode. **Both failures are TEST defects — neither is a product bug.**
Same shape in both cases, and the same shape as issue 11's harness findings: a later
Redis-fidelity change to product behavior landed *after* these sims were written, and neither
sim's assertion was updated with it. Both reproduce deterministically and both are fixed here.

Repro (deterministic, both fail on the pre-fix tree, every run):

```
DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib cargo nextest run -p frogdb-server \
  --features turmoil --no-fail-fast \
  -E 'test(client_pause_write_expiry_suppression_realpath) or test(test_cluster_wait_unblocked_across_failover)'
```

Neither sim needs a seed sweep to reproduce: sim 1 takes no seed at all, and sim 2 fails on the
first seed of its `[1, 7, 42]` loop. The "Open question" above is answered: bucket 2
(harness/test defect) for both. Nothing here touches product code, and no locked spec row is
violated by the *product* — but see the FM-REPLICATION-040 wording note below.

### 1. `client_pause_write_expiry_suppression_realpath` — TEST defect (stale assertion)

Confirmed failure, verbatim:

```
panicked at frogdb-server/crates/server/tests/simulation.rs:4308:5:
S7 expiry sub-assertion: after UNPAUSE the next sweep must reap the backdated key, so
`OBJECT ENCODING` reports `no such key` (an error), got [36, 45, 49, 13, 10].
```

**Mechanism.** `[36, 45, 49, 13, 10]` is `$-1\r\n`, and that is the *correct* modern reply.
`OBJECT ENCODING` on a missing key returns a null bulk, not an error:
`frogdb-server/crates/commands/src/generic.rs:410` answers `Response::null()` on the `None` arm,
deliberately, to match Redis 8.6's `kvobjCommandLookupOrReply`, which replies `shared.null` for
ENCODING/REFCOUNT/IDLETIME/FREQ alike (verified against a locally built Redis 8.6.1 per the
comment at `:405-409`). That behavior is itself pinned by a unit test,
`object_tests::encoding_missing_key_is_nil_not_error` (`generic.rs:774`), whose doc comment
records the bug it closed: the old code returned `CommandError::InvalidArgument { message: "ERR
no such key" }`, which `Display`-rendered as the doubled `ERR ERR no such key`. So the sim was
asserting a reply shape that was *wrong* even before it was changed. The key really is reaped —
suppression lifts exactly as the test intends; only the assertion's expected shape is stale.

**Introducing commit.** `git log -S` on both sides dates it precisely: the sim landed in
`0e58efdb` ("feat(debug): DEBUG EXPIRE-BACKDATE subcommand + sim-test migration (issue 07)",
2026-07-21), when `-ERR no such key` was still the reply; `2f71b949` ("feat(compat): redis-feel
wave 1 — real introspection, truthful shims, 8.6.0, cmd-full ship paths", 2026-08-20) changed
the reply to the null bulk and did not update this sim. `0e58efdb` is an ancestor of `2f71b949`,
so the sim has been red since 2026-08-20. No bisect run was needed — the `-S` pickaxe on the
exact code and the exact assertion is direct causal evidence, and the sim's own comments date
its expectations.

**Second-order finding: the test's *middle* assertion had silently stopped discriminating.**
Line 4297 asserted only `paused_encoding.first() == Some(b'$')`. Once a missing key started
replying `$-1\r\n`, that check passes whether the key was retained under suppression *or*
physically reaped — i.e. the one assertion this test exists to make, "suppression retained the
key", had become vacuous. This is the more interesting defect of the two: the loud failure at
:4308 was masking a silent loss of coverage at :4297.

**Fix applied (test only).** In `frogdb-server/crates/server/tests/simulation.rs`:

- the post-UNPAUSE assertion now `assert_eq!`s the whole reply against `$-1\r\n`, with the
  reason (Redis 8.6 fidelity + the pinning unit test) in a comment;
- the under-pause assertion is upgraded from the now-vacuous first-byte check to an
  `assert_eq!` on the whole reply, `$6\r\nembstr\r\n` (`SET k v` is a non-integer short string →
  `Value::encoding_name` → `embstr`, itself pinned by
  `object_tests::encoding_existing_key_still_reports_encoding`). This is the
  regression-guarding half: the two replies are now told apart by their full bytes, so neither
  can degrade into the other unnoticed;
- the doc comment records that both replies are bulk strings and must never be distinguished by
  a leading-type-byte check again.

### 2. `test_cluster_wait_unblocked_across_failover` — TEST defect (the test holds down the very link it then asserts around)

Confirmed failure, verbatim (seed 1, the first of the `[1, 7, 42]` loop):

```
panicked at frogdb-server/crates/server/tests/simulation.rs:5763:9:
seed 1: the demoted node must reject WAIT like any replica, got
Error("MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.")
```

**Mechanism.** The issue's lead ("gate-ordering, same class as `a859e73c`") is *mechanically*
right about which gate fires, and wrong about it being a bug. The decisive detail is in the
harness, not the product: the sim holds the network edge between the old primary and its replica
from phase 1 (`sim.hold(CLUSTER_HOSTS[p], CLUSTER_HOSTS[r])`, :5798) and does not release it
until phase 2 (:5811) — which the driver only sets at :5770, *after* the failing assertion at
:5762. So at the moment of assertion (b), the just-demoted node is a replica whose link to its
new master is held down **by the test itself, deliberately**. `-MASTERDOWN` is the specified
answer for exactly that state:

- `specs/replication.md` FM-REPLICATION-067 (LOCKED, `:2140`) — with `replica-serve-stale-data
  no` (FrogDB's default, a documented deviation from Redis's `yes`), a link-down replica answers
  the verbatim `-MASTERDOWN …` to **every** command except two exempt classes: `CommandFlags::
  STALE`-flagged commands, and cluster keyed commands due a slot redirect. `WAIT` is in neither:
  it carries no `STALE` flag, and it is keyless, so `defers_to_cluster_redirect`
  (`guards.rs:522`) returns `false` at its `keys.is_empty()` arm (:554) and cannot rescue it.
- The rung order is itself a locked invariant (`specs/replication.md:1139`): `… → pub/sub-context
  → CLUSTERDOWN (quorum-stale) → MASTERDOWN`. The gate sits in `run_pre_checks`
  (`frogdb-server/crates/server/src/connection/guards.rs:465-478`), i.e. in the connection
  gauntlet, so it necessarily runs before `WAIT_ON_REPLICA_ERR`
  (`commands/replication.rs:513`), which lives in the command body.
- **This matches Redis.** Redis's `-MASTERDOWN` check is in `processCommand`; `waitCommand`'s
  `"WAIT cannot be used with replica instances"` is inside the command proc, which runs after.
  So MASTERDOWN-outranks-WAIT is Redis's own ordering, not a FrogDB inversion. The only FrogDB
  deviation in play is the *default value* of `replica-serve-stale-data` (`no` vs Redis's
  `yes`), which is deliberate and documented as FM-REPLICATION-067's Redis-deviation row
  (`:2194`).

Unlike `a859e73c` — where a redirect that *did* exist was being shadowed, leaving routed traffic
unroutable — nothing here is shadowed: the node genuinely cannot serve, and the client is
genuinely told to wait for its master. The test simply asserted the healthy-link refusal while
holding the link down.

**Introducing commit.** `df4c2f4f` ("feat(server): refuse stale reads on a link-down replica by
default", 2026-08-21) added the `stale_refusal` rung to `guards.rs`. The sim predates it by
almost a month (`c6625ae9`, "feat(cluster): wire cluster replication end to end — WAIT,
promotion bridge, chaining contract", 2026-07-28), when no such gate existed and the
WAIT-specific error was the only possible refusal. Red since 2026-08-21. (`0e87e7b2`, 2026-08-29,
later reworked the same rung for the stranded-promotion gap; `a859e73c` added the redirect
deferral. Neither could have covered this path — `WAIT` is keyless, so there is no redirect to
defer to.)

**Fix applied (test only).** In `frogdb-server/crates/server/tests/simulation.rs`, assertion (b)
is split into the two claims that are each true in their own window, which is strictly *more*
coverage than before, not less:

- while the link is still held down, the demoted node must return **an error, not a count** —
  either WAIT's role error or `-MASTERDOWN`. The `RespValue::Int` arm now `panic!`s explicitly,
  so the load-bearing half ("never answers with a count describing a shard it no longer heads")
  is guarded rather than weakened;
- after phase 2 releases the hold and the demoted node reattaches, a new polled assertion
  requires the **exact** `WAIT cannot be used with replica instances` string. This restores the
  original intent under the conditions where it actually holds, and is the regression guard: if
  the WAIT-on-replica refusal ever regressed, the sim would now catch it, whereas before the fix
  the test could never reach that check at all.

### Residual, for a human: one LOCKED spec sentence is now known to be imprecise

`specs/replication.md` FM-REPLICATION-040's Observable (`:1393`) says "Every later `WAIT` on that
node is refused with the replica error (FM-REPLICATION-037)", and FM-REPLICATION-037's Observable
(`:1339`) says `WAIT` on a replica is the role error "*before* argument parsing". Both sentences
were written 2026-07-28, before the stale gate existed (2026-08-21), and neither contemplates a
*link-down* replica, where FM-REPLICATION-067 — later, more specific, and explicitly enumerating
its only two exemptions — mandates `-MASTERDOWN` instead. The product is consistent with 067 and
with Redis; it is the two older summary sentences that under-specify.

**Not amended here**: `specs/replication.md` is LOCKED, so the wording change is human-gated.
Suggested amendment, for whoever picks it up: qualify both sentences with "…on a replica whose
link to its primary is up; a link-down replica under `replica-serve-stale-data no` answers
`-MASTERDOWN` first (FM-REPLICATION-067)". This is the only reason this issue is
`ready-for-human` rather than `ready-for-agent` — the two test fixes themselves are complete.

**Also noticed while tracing, not fixed** (worth its own issue if a human agrees): the
integration test FM-REPLICATION-040 names for this contract,
`test_wait_rejected_on_cluster_replica` (`frogdb-server/crates/server/tests/cluster_misc.rs:1156`),
asserts only `msg.contains("replica")`. The `-MASTERDOWN` string —
"…and **replica**-serve-stale-data is set to 'no'." — also contains `"replica"`, so that
assertion cannot distinguish the two refusals either. It is the same class of silently-vacuous
check as finding 1's middle assertion. It passes today, so this is latent coverage loss rather
than a red test.

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
- Recent related fix on `main` (same bug class as the WAIT lead above; ancestor of `41ae1177`,
  so it does not cover this path): `a859e73c` ("cluster redirect outranks the link-down stale
  gate")
