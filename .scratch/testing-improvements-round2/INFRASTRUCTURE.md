# Shared test infrastructure — consolidated backlog

Companion to [`MASTER.md`](MASTER.md). Every item here was requested by one or more of the
15 area audits, in their `## Cross-area notes` sections.

**The `I<N>` labels are a consolidation artifact — they do not appear in the proposals.**
Each row cites the proposal and findings that asked for it, so the original wording is
always one hop away.

`MASTER.md` §6 listed only I1–I10. That was a lossy summary; I11–I18 below were dropped in
the first pass and are restored here. I11 in particular was described by its author as
"the biggest ask" in that area.

## LOE provenance

Items marked **measured** were sized by inspecting the code during consolidation — call
sites counted, existing seams located, current state verified. Items marked *estimated*
were not; treat those as order-of-magnitude only, and re-size before scheduling.

## Tiers

| Tier | Meaning |
|---|---|
| **A** | Cheap, unblocks a lot, existing foothold. Build now. |
| **B** | Real work, build when a scheduled finding needs it. |
| **C** | Expensive or viral. Needs its own design decision before anyone commits. |

---

## Tier A — build now

### I1 · `shard_driver` harness extension

- **Asked by**: 01 (F1, F2 — "should be built once, first"), 02, 06 (F7, F13), 07 (item 1)
- **Unblocks**: 01/F2, F5, F6, F8, F12, F14 · 02/F1, F5, F13(b) · 06/F7, F13 · most of 07
- **LOE**: **1–2 days** (measured)
- **Current state**: `ShardDriver::new(n)` (`core/tests/shard_driver/harness.rs:55`) hardcodes
  a 5-call builder chain — `with_message_rx`, `with_new_conn_rx`, `with_shard_senders`,
  `with_registry`, `build`. Everything the findings need already exists on
  `ShardWorkerBuilder` and is simply not forwarded: `with_eviction` (`builder.rs:207`),
  `with_persistence` (`:225`), `with_replication` (`:201`), `with_wal_mode` (`:219`),
  `with_fake_wal_failure` (`:286`), `with_scripting` (`:213`).
- **Genuinely new work**, i.e. not just forwarding:
  1. `drive_register_tracking(conn_id, mode, prefixes) -> InvalidationReceiver` — a fifth
     `drive_*` seam in `core/src/shard/event_loop.rs` beside the existing four (`:350`,
     `:360`, `:376`, `:410`), mirroring `drive_capture_keyspace`. Requested by 02.
  2. A `ProtocolVersion` parameter — `ShardDriver::execute` hardcodes `Resp3`, so no RESP2
     shape assertion is possible (06).
  3. A wrapper driving a *blocking* command through `blocking.rs::execute()`. Note
     `block_wait` (`harness.rs:202`) already exists but enters at the waiter layer and skips
     argument parsing entirely, which is what 06/F7 and F13 need to cover.
- **Acceptance**: a test can construct a driver with eviction + a warm store, run a command
  that spills, and assert on both the store and the invalidation stream, without a socket.
- **Depends on**: nothing.

### I7 · `ScatterHeavy` workload profile

- **Asked by**: 12 (F1) — "the single cheapest high-severity item in the audit"
- **LOE**: **~0.5 day** (measured)
- **Current state**: `Profile` enum at `testing/src/workload.rs:20`, generation at `:150`.
  The file emits **zero** MGET / MSET / DEL — verified by grep. The quiescence checker,
  the lock-table probe and the runner all already exist and need no changes.
- **Acceptance**: a profile emitting cross-shard ops exists; the existing lock-leak checker
  runs against it and passes (or fails, which is the point).
- **Depends on**: nothing.

### I16 · Promote the fake-WAL failure fixture into `harness.rs`

- **Asked by**: 01
- **LOE**: *~0.5 day (estimated)*
- **Current state**: `scenario_s6.rs:32-59` has a working fixture (`WalMode::Fake` +
  `FakeFailure::AtWriteIndex` + `set_wal_failure_policy_flag`), private to s6. Also
  `FakeFailure::Predicate(fn(write_index, key) -> bool)` **already exists and has no users** —
  it is the right primitive for per-key WAL failure injection.
- **Depends on**: naturally lands with I1.

### I17 · `CORRUPT_KINDS` exhaustive over `TypeMarker`

- **Asked by**: 07 (F12)
- **LOE**: *~0.5 day (estimated)*
- **Current state**: `server/tests/integration_dump_restore.rs:630-641` omits cms, topk,
  tdigest and vectorset. Wants a compile-time exhaustiveness link to `TypeMarker`
  (`persistence/src/serialization/marker.rs:36`) so the next type cannot be silently omitted.

---

## Tier B — build on demand

### I4 · Conservation checker for derived structures

- **Asked by**: 10 — "the single highest-leverage item in the audit"
- **Unblocks**: 10/F3, F4, F5, F9 collapse into **one invariant** rather than four example tests
- **LOE**: *2–3 days (estimated)*
- **Invariant**: `index_docs ≡ {store keys matching prefix, of matching type, not expired}`,
  asserted at every quiescent point of the existing fault-injection and restart workloads.
- **Current state**: `testing/src/conservation.rs` already hosts six checkers of exactly this
  shape (`check_exactly_once_delivery:121`, `check_fifo_wake_order:246`,
  `check_tx_sum_conservation:431`, `check_watch_no_false_negative:621`,
  `check_pel_conservation:682`). This is a seventh, not a new pattern.
- **Generalises to**: store↔expiry-index and store↔DBSIZE (theme T2 in `MASTER.md`).
- **Needs coordination with** whoever owns `crates/testing/`.

### I5 · "Shard busy running a script" fixture

- **Asked by**: 09 (F4, F8, F15)
- **LOE**: *1–2 days (estimated)*
- **Current state**: does not exist. Nothing in the suite starts a long-running EVAL and then
  talks to the same shard on a second connection.
- **Shape**: spawn a bounded-but-slow script, wait until the shard is observably busy, hand
  back both connections, guarantee teardown **even if the script cannot be killed** — the last
  clause matters because 09/F4 is precisely the `Unkillable` path.

### I9 · TLS harness extension

- **Asked by**: 03 (F9, F13)
- **LOE**: *1–2 days (estimated)*
- **Needs**: `TestServerConfig.tls_watch_certs` + `tls_additional_certs`; `TlsFixture`
  (`test-harness/src/tls.rs`, currently a single `generate()`) gains an ECDSA variant and an
  in-place regeneration helper so rotation can happen while the server runs.
- **Note**: TLS-replication and cluster-TLS tests elsewhere likely want the same. One owner.

### I11 · Registry-wide argument-fuzz property harness

- **Asked by**: 06 (F5) — *"the biggest ask"* in that area. **Dropped from `MASTER.md` §6.**
- **LOE**: *2–4 days (estimated)*
- **Shape**: built on `shard_driver`; for every registered command, drive adversarial scalars
  into every arity position and assert "never unwinds".
- **Why it ranks**: one harness closes an entire bug class across all ~250 commands rather
  than per-area. Several of the unbounded-allocation findings (06/F9, 07/F14, 10/F6) are
  instances of what it would catch generically.
- **Depends on**: **I1**.

### I12 · Config observability seams

- **Asked by**: 05 (four separate asks, listed under "Shared infrastructure requested")
- **LOE**: *1–2 days total (estimated)*
  1. `is_published()` accessor (or a single publication bitmask) on `ConfigManager`, so
     05/F10 can assert wiring completeness without reflection.
  2. A small IO seam for `ConfigPersister` (05/F15), which also unlocks its untested error arms.
  3. A shared const list of protected-vs-public HTTP routes exported from
     `observability_server.rs`, so 05/F2's test **fails when a route is added outside the
     guarded group** — this is the durable form of the default-open admin-gate finding.
  4. A `TestServer` restart-in-place helper (05/F9), if one does not already exist.
- **Note**: 15 asks that 05 own the registry round-trip (F9) and `noop:false ⇒ observable`
  (F11) tests, written **once** in `server/tests/`, with 15's findings as the spec.

### I13 · Bounded-duration partition primitive

- **Asked by**: 04 (F4). **Dropped from `MASTER.md` §6.**
- **LOE**: *2–4 days (estimated)*
- **Shape**: turmoil or `crates/testing/partition` scenario that partitions a *specific* node
  from the leader for a *bounded number of health-check intervals*.
- **Why**: unlocks both false-positive and quorum-loss failure-detector testing;
  `cluster_flags`' `SelfFenceGate` becomes end-to-end testable as a side effect.
- **Caveat**: round 1 hit an upstream turmoil 0.7.1 port leak that makes *indefinite*
  partitions impossible. A *bounded* partition may sidestep it — confirm before committing.

### I14 · Mockable `ClusterWriter` / propose seam

- **Asked by**: 04 → 11. **Dropped from `MASTER.md` §6.**
- **LOE**: *1–2 days (estimated)*
- **Effect**: `ProposeError::Redirect` is unreachable from server-side tests today. A seam
  drops 04/F7 from **boundary 5 to boundary 2** and helps several other server-side callers.
- **Related, same file**: `MAX_FRAME_SIZE` and `parse_rpc_message`'s error taxonomy live in
  `cluster/src/network.rs`. 04/F10 needs those errors *typed* rather than string-matched —
  `server/src/cluster_bus.rs:167-181` currently string-matches. If that file is refactored,
  do both at once.

### I15 · Cross-shard EVAL test helper

- **Asked by**: 12 (F3). **Dropped from `MASTER.md` §6.**
- **LOE**: *~0.5 day (estimated)*
- **Current state**: the `allow_cross_slot_standalone` knob already exists in
  `test-harness/src/server.rs`; only the scripting-side helper for "run an `EVAL` whose keys
  span shards" is missing.

### I18 · Resync-boundary signal on the replica frame channel

- **Asked by**: 14 (shared-infra item 1, its top priority). **Dropped from `MASTER.md` §6.**
- **LOE**: *1–2 days (estimated)*
- **Distinguishing feature**: this is required to **fix** 14/F3 and 14/F5, not only to test
  them. Today the applier cannot tell a pre-resync frame from a post-resync one. A generation
  counter on `ReplicationFrame`, or an explicit `Barrier` message, is simultaneously the fix
  and the test hook.
- **Sequencing**: therefore belongs with the replication bug work, not with the test work.

---

## Tier C — needs a decision first

### I2 · Subprocess-SIGKILL crash primitive

- **Asked by**: 13 (F10), echoed by 11 and 14
- **LOE**: **1–2 weeks, with CI-flake risk** (measured)
- **Why it is expensive**: `TestServer` is **entirely in-process** — zero `Command::new` in
  `test-harness/src`, verified. A real SIGKILL means adding a subprocess execution mode to the
  whole harness: spawn the actual binary, pass config by file/args, discover the port, connect
  a client, and handle teardown plus orphan reaping in CI. Every existing `start_*` helper
  either has to work under it or be explicitly declared out of scope.
- **Naming bug to fix regardless**: `ClusterNode::kill()` (`cluster_harness.rs:912`) is a
  **graceful shutdown**. The name has probably already misled a test author.
- **Cheaper substitute**: `CrashTestHarness` (`core/src/persistence/test_harness.rs`) already
  does byte-level truncation and covers torn-write recovery. It misses only "process dies
  mid-fsync with OS buffers still in flight."
- **Decision needed**: is that residue worth 1–2 weeks plus ongoing CI flake, or is
  truncation-level crash testing sufficient for production readiness?
- **If built**: 13 asks it live in `frogdb-test-harness` next to `TestServer`, **not** in
  `core/src/persistence/test_harness.rs`.

### I3 · Injectable clock seam

- **Asked by**: 13 (F16), 14 (item 3), 15 (F7)
- **LOE**: **3–5 days scoped to expiry / multi-week full** (measured)
- **Why it is viral**: **313** raw `SystemTime::now()` / `Instant::now()` call sites, and
  **no existing abstraction whatsoever** — no `trait Clock`, no `now_ms()`, nothing to adopt.

  | crate | sites | | crate | sites |
  |---|---:|---|---|---:|
  | core | 121 | | types | 18 |
  | server | 51 | | replication | 11 |
  | persistence | 44 | | vll | 4 |
  | commands | 31 | | acl, scripting | 3 each |

  cluster, search and protocol have zero.
- **Decision needed**: full seam, or scoped to the expiry path (~30–40 sites, covers theme T4
  and the TTL findings) leaving replication and election timeouts wall-clock?
- **Smallest useful slice**: 15/F7 needs only `acl/src/ratelimit.rs:23 now_us` — a single
  production-code seam, and `types` already has shuttle plumbing via `types/src/sync.rs`.
- **Rule**: whoever builds one first owns it; nobody adds a second (13's explicit request).

### I6 · Live-link fault primitive

- **Asked by**: 14 (item 2)
- **LOE**: *1–2 weeks (estimated)*
- **Current state**: `testing/src/fault_injection.rs` mangles **recorded histories after the
  fact**. Nothing can stall a checkpoint transfer, evict a backlog, or delay an ACK on a
  *running* link.
- **Unblocks**: 14/F2, F7, F9, F10.
- **Where**: the turmoil hosts (`real_frogdb_primary` / `real_frogdb_replica`) are the natural
  attachment point.

### I8 · Virtual-time / injectable-timeout primitive for shuttle

- **Asked by**: 12 (F7)
- **LOE**: *1–2 weeks (estimated)*
- **Current state**: round-1 issue 07 already established shuttle in `crates/testing` for the
  MultiWaiter exactly-once guard. The VLL model needs that plus deterministic timeout
  exploration, which does not exist.
- **Note**: 12 explicitly concluded **loom is the wrong tool** for VLL — no atomics, no
  `UnsafeCell`, no interior mutability; state machines are `&mut self` single-owner and all
  cross-task comms are tokio channels. The nondeterminism is message-arrival order, which is
  shuttle's domain.
- **Build once and share** if any other area wants deterministic timeouts.

### I10 · Fuzz CI

- **Asked by**: 08 (item 3), 13
- **LOE**: *2–4 days (estimated)*
- **Current state**: **fuzzing is not running.** `fuzz.py` shows the nightly cron was
  deliberately removed, and the PR `corpus-replay` gate is `-runs=0` restore-only, so it
  silently no-ops on a cold cache. This affects all **34** targets.
- **Decision needed** (08's framing): a weekly campaign, a per-PR time-boxed run for a
  security-critical subset, or accept manual dispatch **and remove the "continuous" framing
  from the docs**. The third option is legitimate; the current state — docs claiming
  continuous fuzzing that does not run — is not.
- **Highest-value targets** (13): `deserialize`, each per-type decoder, `RESTORE` payloads.

---

## Explicitly *not* infrastructure

Worth recording, because they look like infra asks and are not:

- **11 needs nothing.** Every level-1/2 cluster finding lands with existing dev-dependencies
  (`proptest`, `tempfile`, `tokio::test`); `cluster_harness` already exposes `raft()` and
  `cluster_state()` (`cluster_harness.rs:239,244`). 11's conclusion was that **model checking
  beats turmoil decisively** here: `apply_command` is pure, synchronous and `BTreeMap`-only,
  so proptest at level 1–2 catches 11/F5 and 11/F9 as a generalisation with zero new infra.
- **10/F7's `DEBUG FLUSH-SEARCH-INDEX`** is a dispatch entry, not machinery —
  `SearchMsg::FlushSearchIndexes` is already plumbed to every shard and already awaited by the
  BGSAVE hook. It just needs sign-off from whoever owns the `DEBUG` surface.
- **The registry-consistency invariant** (theme T1) needs no harness:
  `CommandRegistry::iter()` (`core/src/registry.rs:256`) already provides the iteration. It
  needs a *home* — which is decision D1 in `MASTER.md`, not an infra item.

## Two structural notes that change infra cost

- **`FrogDbResp2` is not in the protocol crate.** The real decoder lives in
  `server/src/connection/{codec,frame_io,util}.rs`. Relocating it into `protocol` would drop
  08/F2 and 08/F5 from effort 2 to 1, make the decoder fuzzable as one public surface without
  a server dependency, and make "protocol coverage" a meaningful number for the first time.
- **`registry.rs:184`'s `debug_assert!(spec.validate().is_ok())` is release-stripped.** If any
  suite runs in release, whole-registry spec validation is silently unchecked. Someone who
  owns CI should confirm. Cheap to check, and it is load-bearing for several proposed tests.
