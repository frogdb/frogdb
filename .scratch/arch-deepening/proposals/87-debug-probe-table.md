# Proposal 87 — `ShardProbe`: one probe table instead of a five-way smear, and a coverage signal the quiescence checkers currently lack

Round 38 · lane: protocol / net / core · candidate **PN10** · effort **M** (probe table)
+ **S** (H1 coverage hotfix, independently landable first) · **no locked crate edited**
(`frogdb-core`, `frogdb-server`, `frogdb-shard-harness`, `frogdb-testing`), **zero `FM-` tags
in any edited region**

**Verified at HEAD `ddc4b184`** (worktree `arch-round-38-99`, branch `main`). Every file:line
below was re-derived at this SHA; nothing is inherited from the lane brief. Concurrent authors
hold `.scratch/arch-deepening/proposals/80-response-wire-fold.md` (modified in the working
tree); no code file in this set is dirty.

**Six brief claims are corrected, and one of the corrections turns "M, Latent" into a
LIVE test-oracle soundness defect that is independently landable ahead of the refactor.**

| Brief claim | Correction at HEAD |
|---|---|
| "~9 touch points per probe" | **19 sites across 13 files**, of which **12 are pure boilerplate**. The brief's list omits seven live ones: `probe_type_str` (`message.rs:1139-1144`), the `shard/mod.rs` re-export (`:88-92`), the **continuation-lock gate count pin** (`scripts/continuation-lock-gate.py:89`), the `DEBUG HELP` text (`debug_conn_command.rs:290-299`), the shard-harness driver method (`harness.rs:294-358`), the tier-4 RESP parser (`quiescence_probe.rs`), and the docs row (`debugging.md:38-45 (rows `:38`, `:39`, `:40`, `:45`)`). §Problem 1 builds the table from the last real "add one probe" commit. |
| "core shard `message.rs:821` (message variant)" | `:821` is the **`GetWaitQueueInfo` variant specifically**. The enum is `DebugIntrospectionMsg` at **`message.rs:810-855`** (doc `:810-811`, 6 variants). The line is inside the right region; the region is 46 lines, not one. |
| "`diagnostics.rs` ×6 sites" | **5**, not 6, in this family: `collect_lock_table_info :207`, `collect_wait_queue_info :234`, `collect_wait_queue_log :265`, `collect_memory_check :284`, `collect_expiry_index_check :293`. The 6th (`collect_vll_queue_info :167`) serves **`VllMsg::GetVllQueueInfo`**, a different message enum, and `DEBUG VLL` takes a `shard_filter` argument — it is not a uniform probe and is **out of scope** (§Scope boundaries). |
| "`types.rs:1035-1132`" | **`:1033-1128`**. `:1130-1139` is `PubSubLimitsInfo`, which belongs to `SearchMsg` and the `DEBUG PUBSUB LIMITS` path — a different family, not touched. |
| "`debug_conn_command.rs:646` (one site)" | **Two sites per probe in that file, and `:646` is inside neither list**: the routing arms are `:153-161` and the formatters are `:602-817` (five functions, **216 lines**). `:646` is a line *inside* `format_locktable_response`. Plus the `DEBUG HELP` text `:290-299` and the test `StubDebug` impl `:1167-1189` — **four** per-probe regions in this one file. |
| "plus a test noop" | **Two** noop `DebugProvider` impls: `StubDebug` in `frogdb-core` (`conn_command.rs:1037-1105`, `unimplemented!()` bodies) and `StubDebug` in `frogdb-server` (`debug_conn_command.rs:1151-1219`, empty-`Vec` bodies). Both grow by one method per probe. |

**And one finding the brief did not anticipate (§Problem 3, hotfix H1, LIVE):** the four tier-4
quiescence checkers **cannot distinguish "every shard is clean" from "we never heard from most
of the shards"**, because `gather_all` is best-effort, the sentinel formatters fold over
*survivors only*, and every checker returns `Ok(())` on an empty slice. A `DEBUG LOCKTABLE`
that timed out on shard 0 renders the literal bytes `# lock table is empty`. This is the same
class of defect the project already ruled real and fixed for `WAITQUEUE-LOG` (commit
`64d03cab`, concurrency issue 16: *"incomplete ordinals mean 'proves nothing', not a
verdict"*) — the other four probes never got the treatment.

## Files involved

Line counts at `ddc4b184`.

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/core/src/shard/probe.rs` | *new (~130)* | **The change.** `ShardProbe` (5-variant kind), `ProbeReport` (5-variant reply), `impl ShardWorker { fn gather_probe(&self, ShardProbe) -> ProbeReport }` — the one match where a probe kind becomes a snapshot. `&self`, not `&mut self`: read-only becomes a type property (§Proposed change B). |
| `frogdb-server/crates/core/src/shard/message.rs` | 1446 | **Primary.** `DebugIntrospectionMsg` `:810-855` — five snapshot variants (`:814-842`) collapse to one `Probe { probe, response_tx }`; `ExpireBackdate` `:844-854` **stays** (it is the one mutator, §B). Enum goes 6 variants → 2. The doc comment `:810-811` is **stale today** (hotfix H3). `From` impl `:979-983` unchanged. **`probe_type_str` `:1135-1146` is proposal 85's region — see §Scope boundaries; 87 does not edit it.** |
| `frogdb-server/crates/core/src/shard/dispatch_debug_introspection.rs` | 42 | **Primary.** The 6-arm match `:16-40` → 2 arms; the module doc `:1-7` stops hand-enumerating the probe list. |
| `frogdb-server/crates/core/src/shard/diagnostics.rs` | 598 | **Primary, but the five collectors survive verbatim.** `collect_lock_table_info :207-233`, `collect_wait_queue_info :234-258`, `collect_wait_queue_log :265-283`, `collect_memory_check :284-292`, `collect_expiry_index_check :293-299` — all `&self`, all kept, all still individually unit-tested (`:559`, `:579`). Only their *callers* move. **Proposal 81 (PN2) owns `:502` and `:508` in this file** — 200+ lines away, disjoint (§Scope boundaries). |
| `frogdb-server/crates/core/src/shard/types.rs` | 1498 | **Primary, additive-free.** The five reply structs `:1033-1128` are **unchanged** — they become `ProbeReport`'s payloads as-is. **4 `FM-` tags exist in this file (`:409`, `:698`, `:1268`, `:1421`) — all outside `:1033-1128`** (§Spec clearance). |
| `frogdb-server/crates/core/src/shard/mod.rs` | 96 | **Primary, small.** `pub use types::{…}` `:88-92` gains nothing (the reply structs stay exported); adds `pub use probe::{ProbeReport, ShardProbe};` and `mod probe;`. **This is a per-probe touch point that dies.** |
| `frogdb-server/crates/core/src/conn_command.rs` | 1172 | **Primary.** `DebugProvider` `:534-616` — five methods `:551-565` (`gather_lock_table`, `gather_wait_queue`, `gather_wait_queue_log`, `memory_check`, `expiry_index_check`) → one `gather_probe(&self, ShardProbe) -> BoxFuture<ProbeGather>`. `gather_vll :546-549` **stays** (has an argument). `StubDebug` `:1054-1072` — five `unimplemented!()` bodies → one. |
| `frogdb-server/crates/server/src/connection/debug_handler.rs` | 374 | **Primary.** Five near-identical `Box::pin(async move { self.scatter_gather().gather_all(\|_shard, tx\| Msg::X { tx }).await })` bodies `:100-155` (**56 lines, 5 functions**) → one. `gather_vll :75-98` untouched. **Proposal 74 owns `:222-277`** in this file (bundle_generate/bundle_list) — disjoint hunks. **`:178`'s hardcoded 5 s is proposal 67's filed issue — cited, not claimed (H2).** |
| `frogdb-server/crates/server/src/connection/debug_conn_command.rs` | 1445 | **Primary, four regions.** Routing arms `:153-161` → five one-liners over one helper; `DEBUG HELP` `:290-299`; the five formatters `:602-817` (**216 lines**) **survive verbatim** behind one `format_probe(ShardProbe, ProbeGather)` dispatcher; `StubDebug` `:1167-1189` five bodies → one. `shard_count = ctx.shard_senders.len()` `:112` is the coverage denominator H1 needs and it is **already in scope at the formatter**. |
| `frogdb-server/crates/shard-harness/src/harness.rs` | 399 | **Primary.** Five driver methods `:294-358` (`wait_queue_info`, `lock_table_info`, `memory_check`, `expiry_index_check`, plus `backdate_expiry`) — the four snapshot ones (~45 lines) become five two-line typed wrappers over one `probe(shard, ShardProbe)`. `backdate_expiry :331-348` unchanged. **Proposal 81 (PN2) owns `:53`, `:80-81`, `:84`, `:94`** — disjoint. |
| `scripts/continuation-lock-gate.py` | — | **Primary, one number.** `"dispatch_debug_introspection.rs": ("DebugIntrospectionMsg", 6)` **`:89`** → `2`. Rule 2 (enum parity, both directions) still holds by construction. §Seam-lint clearance. |
| `frogdb-server/crates/server/tests/common/quiescence_probe.rs` | 571 | **Primary (H1).** Five hand-written RESP parsers `:157-320` + `QuiescenceSnapshots` `:27-71`. The **only** consumer that pins probe reply shapes (§Reply-shape compatibility). H1 adds `shards_expected` to the bundle. |
| `frogdb-server/crates/server/tests/common/workload_runner.rs` | 856 | **Primary (H1), small.** The five `debug_probe(...)` calls `:268-271`, `:279`; `debug_probe` itself `:382-393`. |
| `frogdb-server/crates/testing/src/quiescence.rs` | — | **Primary (H1).** `check_locktable_empty :81-92`, `check_waitqueue_empty :95-105`, `check_memory_accounting :108-121`, `check_expiry_index_consistent :124-136` — **all four `for s in snapshots { … } Ok(())`, all four vacuously pass on an empty slice** (§Problem 3). |
| `frogdb-server/crates/server/tests/integration_debug_introspection.rs` | 154 | **Read-only regression pin.** 5 tests, one per probe (`:12`, `:55`, `:95`, `:133`, plus the unknown-subcommand guard `:26`). Byte-shape oracle for the refactor. |
| `website/src/content/docs/architecture/debugging.md` | 220 | **Primary, hand-written (no docs-gen source).** The FrogDB-subcommand table `:34-48` (probe rows `:38`, `:39`, `:40`, and the shared `:45`) — nominally one row per probe, except `MEMORY-CHECK` and `EXPIRY-INDEX-CHECK` were lumped into a single `:45` row reading "Internal consistency checks", a third instance of the hand-maintained-list drift in §Problem 2. |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | — | **Read-only, and a brief-adjacent trap.** `:391-394` dispatches `ShardMessage::DebugIntrospection(m)` to `dispatch_debug_introspection` **without naming any variant** — so it is **no longer a per-probe touch point** (it was one at `0a8b5c11`, before the flat message enum was split). **Proposal 81 (PN2) owns `:119-124`** here — a different arm of a different `select!`. |
| `frogctl/**` | — | **Read-only, negative evidence.** `frogctl` consumes **no** probe. Its only `DEBUG` use is `DEBUG HASHING` (`commands/data.rs:51`). §Reply-shape compatibility. |

## Problem

### 1. Adding one probe is a 19-site, 13-file diff — and 12 of the sites carry no information

The ground truth is `0a8b5c11` *"feat(server): DEBUG EXPIRY-INDEX-CHECK introspection
command"* — the last commit that added exactly one probe and nothing else. It touched **10
files, +178/−7**. Since then the surface has *grown* two more mandatory sites (the second
`StubDebug`, the continuation-lock count pin) and lost one (`event_loop.rs`, absorbed by the
message-enum split). The table below is what a contributor writes **today**, re-derived at
`ddc4b184`:

| # | Site | Location | Carries probe-specific information? |
|---|---|---|---|
| 1 | Message variant | `message.rs:810-855` | **no** — name + one `oneshot::Sender<T>` |
| 2 | `probe_type_str` arm | `message.rs:1139-1144` | **no** — variant name as a string *(85's region)* |
| 3 | Reply struct | `types.rs:1033-1128` | yes — the data |
| 4 | `shard/mod.rs` re-export | `:88-92` | **no** |
| 5 | Collector | `diagnostics.rs:207-299` | yes — the work |
| 6 | Dispatch arm | `dispatch_debug_introspection.rs:16-40` | **no** — `response_tx.send(self.collect_x())` |
| 6b | Dispatch module doc list | `dispatch_debug_introspection.rs:1-7` | **no** — hand-maintained enumeration |
| 7 | Continuation-lock count pin | `scripts/continuation-lock-gate.py:89` | **no** — an integer |
| 8 | `DebugProvider` trait method | `conn_command.rs:551-565` | **no** |
| 9 | core `StubDebug` body | `conn_command.rs:1054-1072` | **no** — `unimplemented!()` |
| 10 | Server gather body | `debug_handler.rs:100-155` | **no** — identical modulo the variant name |
| 11 | Routing arm | `debug_conn_command.rs:153-161` | yes — the subcommand spelling |
| 12 | Formatter | `debug_conn_command.rs:602-817` | yes — the wire shape |
| 13 | `DEBUG HELP` line | `debug_conn_command.rs:290-299` | **no** — restates the doc comment |
| 14 | Server `StubDebug` body | `debug_conn_command.rs:1167-1189` | **no** — `Vec::new()` |
| 15 | Harness driver method | `harness.rs:294-358` | **no** |
| 16 | Tier-4 RESP parser | `quiescence_probe.rs:157-320` | yes — inverse of #12 |
| 17 | Tier-4 gather call | `workload_runner.rs:268-271` | **no** |
| 18 | Integration test | `integration_debug_introspection.rs` | yes |
| 19 | Docs table row | `debugging.md:34-48` | yes |

**12 of 19 sites are boilerplate.** Six of them (#1, #6, #8, #9, #10, #14) are the *same
sentence* written six times in six dialects: *"this probe exists."*

The five `debug_handler.rs` bodies `:100-155` are the purest instance — after `gather_all`
(`scatter/broadcast.rs:285`) already generalised the fan-out, deadline and error mapping,
what is left per probe is **eight lines that differ by one identifier**:

```rust
fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<LockTableInfo>> {
    Box::pin(async move {
        self.scatter_gather()
            .gather_all(|_shard, response_tx| {
                frogdb_core::shard::DebugIntrospectionMsg::GetLockTableInfo { response_tx }
            })
            .await
    })
}
```

…times five, at `:100`, `:111`, `:122`, `:134`, `:146`.

### 2. The smear has already produced drift — in all three places that enumerate probes

Every hand-maintained probe list in the repo is wrong or unenforceable at HEAD:

- **`message.rs:810-811`** — *"Always-available DEBUG introspection messages (LOCKTABLE /
  WAITQUEUE / MEMORY-CHECK / EXPIRY-INDEX-CHECK)."* The enum it documents has **six**
  variants. `WAITQUEUE-LOG` and `EXPIRE-BACKDATE` were added below it and the doc was never
  updated. (Hotfix H3.)
- **`dispatch_debug_introspection.rs:2`** carries the *correct* five-name list, and
  `:4-5` then asserts *"The snapshot probes are read-only per-shard collectors"* — a
  claim the code cannot enforce, because `dispatch_debug_introspection` takes `&mut self`
  and one of its arms (`ExpireBackdate`) genuinely mutates.
- **`debugging.md:45`** collapses `MEMORY-CHECK` and `EXPIRY-INDEX-CHECK` into one row
  labelled *"Internal consistency checks"*, while `LOCKTABLE`/`WAITQUEUE`/`WAITQUEUE-LOG`
  each get their own. Nothing enforces the shape; the two probes with the least prose are
  the two whose replies the tier-4 checkers trust most (§Problem 3).

Three enumerations — one stale, one carrying an invariant the code cannot enforce, one
inconsistent — spread across two crates and the website. That is the locality cost stated
concretely: there is no single place to read "what probes exist", so all three lists rot
independently.

### 3. The quiescence checkers accept a partial gather as "clean" (REAL, LIVE)

This is the finding that upgrades the proposal. Three verified facts compound:

**(a) The gather is best-effort and drops a *suffix*.** `ScatterGather::gather_all`
(`scatter/broadcast.rs:285-329`) collects survivors under one shared deadline, and on the
first timeout it **`break`s the loop** (`:325`) — so a slow shard 0 discards shard 0 *and
every higher-numbered shard*. Send failures and dropped senders are `continue`/`warn` and
also vanish. The `Vec<R>` that comes back carries **no record of how many shards were
asked**.

**(b) The sentinel folds over survivors only.** `format_locktable_response`
(`debug_conn_command.rs:602-604`), `format_waitqueue_response` (`:656-659`) and
`format_expiry_index_check_response` (`:783-788`) each open with
`if infos.iter().all(|i| … is_empty()) { return Response::Bulk(Some("# … is empty")) }`.
With `infos` empty, `.all()` is vacuously true. **A gather that returned nothing renders the
exact bytes `# lock table is empty`.**

**(c) The checkers pass on an empty slice.** All four of
`frogdb-testing/src/quiescence.rs:81-136` are `for s in snapshots { if bad { return Err } }
Ok(())`. `quiescence_probe.rs:9-11` documents the sentinel path as *"that parses to zero
snapshots, which the checkers accept."* — which is correct behaviour **only** if zero
snapshots always means clean. It does not.

The tier-4 concurrency pipeline (`workload_runner.rs:268-273`, `invariants.rs:231-232`)
therefore has a **silent false-negative**: under exactly the load that makes a shard miss the
scatter deadline, `check_locktable_empty` / `check_waitqueue_empty` /
`check_memory_accounting` / `check_expiry_index_consistent` report **clean** without having
examined the shards. `MEMORY-CHECK` has no sentinel, so it degrades more quietly still —
fewer `shard:<id>` entries, checker green.

The project has already ruled this class real. `64d03cab` (concurrency issue 16) added
`truncated` to `WAITQUEUE-LOG` precisely so *"incomplete ordinals mean 'proves nothing', not a
verdict"*. **`WaitQueueLogInfo` is the only one of the five reply types with a completeness
flag** (`types.rs:1090`). The other four have none, and none of the five has a *coverage*
flag — `truncated` is per-shard, not per-fleet, so it does not catch a missing shard either.

The denominator is already in scope: `debug_conn_command.rs:112` computes
`shard_count = ctx.shard_senders.len()` for `parse_vll_shard_filter`.

### 4. The DEBUG shard round-trips span four message enums, and only one of them is a probe family

Verified inventory of every shard round-trip `DebugProvider` performs:

| Subcommand | Message enum | Shape |
|---|---|---|
| `LOCKTABLE`, `WAITQUEUE`, `WAITQUEUE-LOG`, `MEMORY-CHECK`, `EXPIRY-INDEX-CHECK` | `DebugIntrospectionMsg` | **argument-free, read-only, all-shards, per-shard reply keyed by `shard_id`** ← this proposal |
| `EXPIRE-BACKDATE` | `DebugIntrospectionMsg` | keyed single-shard **mutator**, takes `(key, ms)` |
| `VLL [shard_id]` | `VllMsg` | takes a `shard_filter`; single-shard *or* all-shards |
| `SET-ACTIVE-EXPIRE`, `KEYSIZES-HIST-ASSERT`, `ALLOCSIZE-SLOTS-ASSERT` | `ObservabilityMsg` | mutator / merge-fold / sum-fold; also INFO-facing |
| `PUBSUB LIMITS` | `SearchMsg` | shard-0 only, bypasses `ScatterGather` entirely (H2) |

Exactly **five** round-trips share one shape. That is the natural table; the other six are
each different and each belong where they are. §Scope boundaries states this as a ruling
rather than leaving it to a reviewer to rediscover.

## Proposed change

### A. The module: `core/src/shard/probe.rs`

```rust
/// Which read-only per-shard snapshot a DEBUG probe asks for.
///
/// Adding a probe adds one variant here, one `ProbeReport` variant, and one
/// `gather_probe` arm. Nothing else in the message plumbing changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardProbe { LockTable, WaitQueue, WaitQueueLog, MemoryCheck, ExpiryIndex }

/// One shard's answer, carrying the existing reply struct unchanged.
#[derive(Debug)]
pub enum ProbeReport {
    LockTable(LockTableInfo),
    WaitQueue(WaitQueueInfo),
    WaitQueueLog(WaitQueueLogInfo),
    MemoryCheck(MemoryCheckInfo),
    ExpiryIndex(ExpiryIndexCheckInfo),
}

impl ShardWorker {
    /// The one place a probe kind becomes a snapshot.
    ///
    /// `&self`: a probe cannot mutate the shard, and that is now a fact the
    /// compiler checks rather than a sentence in a module doc.
    pub(crate) fn gather_probe(&self, probe: ShardProbe) -> ProbeReport {
        match probe {
            ShardProbe::LockTable    => ProbeReport::LockTable(self.collect_lock_table_info()),
            ShardProbe::WaitQueue    => ProbeReport::WaitQueue(self.collect_wait_queue_info()),
            ShardProbe::WaitQueueLog => ProbeReport::WaitQueueLog(self.collect_wait_queue_log()),
            ShardProbe::MemoryCheck  => ProbeReport::MemoryCheck(self.collect_memory_check()),
            ShardProbe::ExpiryIndex  => ProbeReport::ExpiryIndex(self.collect_expiry_index_check()),
        }
    }
}
```

The five `collect_*` functions in `diagnostics.rs` are **not** touched — they are the real,
distinct work, and each keeps its own unit test (`diagnostics.rs:559`, `:579`).

### B. The message enum shrinks 6 → 2, and the mutator stays visible

```rust
pub enum DebugIntrospectionMsg {
    /// A read-only per-shard snapshot; `probe` names which.
    Probe { probe: ShardProbe, response_tx: oneshot::Sender<ProbeReport> },
    ExpireBackdate { key: Bytes, ms: u64, response_tx: oneshot::Sender<BackdateExpiryResult> },
}
```

`ExpireBackdate` deliberately does **not** join the table. It is the one mutator, it carries
arguments, and keeping it as its own variant is what lets the `Probe` arm call a `&self`
method. The dispatch match becomes:

```rust
match msg {
    DebugIntrospectionMsg::Probe { probe, response_tx } => {
        let _ = response_tx.send(self.gather_probe(probe));   // &self — cannot mutate
    }
    DebugIntrospectionMsg::ExpireBackdate { key, ms, response_tx } => {
        let _ = response_tx.send(self.store.backdate_expiry(&key, ms));
    }
}
```

This is the **seam** argument. Today `dispatch_debug_introspection.rs:4-5` *asserts* the
probes are read-only in prose while the function signature says `&mut self`. After, the
read-only guarantee is a borrow, and any future probe that wants to mutate cannot silently
sneak in — it has to leave the table, which is exactly the review moment the
continuation-lock gate's count pin was buying with an integer.

### C. The server side: one provider method, one gather, one formatter dispatcher

```rust
// core/src/conn_command.rs — five methods become one
fn gather_probe<'a>(&'a self, probe: ShardProbe) -> BoxFuture<'a, ProbeGather>;

/// Survivors plus the denominator: how many shards were asked.
pub struct ProbeGather { pub reports: Vec<ProbeReport>, pub shards_expected: usize }
```

```rust
// debug_handler.rs — 56 lines of five bodies become one
fn gather_probe<'a>(&'a self, probe: ShardProbe) -> BoxFuture<'a, ProbeGather> {
    Box::pin(async move {
        let shards_expected = self.core.shard_senders.len();
        let reports = self.scatter_gather()
            .gather_all(|_shard, response_tx| DebugIntrospectionMsg::Probe { probe, response_tx })
            .await;
        ProbeGather { reports, shards_expected }
    })
}
```

```rust
// debug_conn_command.rs — routing
b"LOCKTABLE"          => probe_reply(debug, ShardProbe::LockTable).await,
b"WAITQUEUE"          => probe_reply(debug, ShardProbe::WaitQueue).await,
b"WAITQUEUE-LOG"      => probe_reply(debug, ShardProbe::WaitQueueLog).await,
b"MEMORY-CHECK"       => probe_reply(debug, ShardProbe::MemoryCheck).await,
b"EXPIRY-INDEX-CHECK" => probe_reply(debug, ShardProbe::ExpiryIndex).await,
```

`probe_reply` gathers, then hands `(probe, gather)` to `format_probe`, which sorts the
`ProbeReport`s into the **existing, unmodified** `format_locktable_response` /
`format_waitqueue_response` / `format_waitqueue_log_response` /
`format_memory_check_response` / `format_expiry_index_check_response`. Those 216 lines
(`:602-817`) are the genuine per-probe wire shape and this proposal does not touch a byte of
them — which is precisely why the reply is byte-identical (§Testability).

### D. Depth, leverage, locality — and the honest deletion test

**Depth.** The `DebugProvider` interface today grows **linearly with the number of probes**:
five methods, and each new probe widens the trait, both stubs, and the handler. That is the
textbook shallow module — interface proportional to implementation. After, the interface is
*one* method plus a data enum, and it is constant in the number of probes. The `probe.rs`
module hides five collectors, the kind→collector mapping, and the read-only guarantee behind
three names.

**Leverage.** Per-probe boilerplate sites go **12 → 4**:

| | today | after |
|---|---|---|
| boilerplate sites | 12 | **4** (`ShardProbe` variant, `ProbeReport` variant, `gather_probe` arm, `DEBUG HELP` line) |
| information-bearing sites | 7 | 6 (reply struct, collector, formatter, routing arm, tier-4 parser, test; the docs row folds into the same edit) |
| **total sites** | **19** | **10** |
| files touched to add a probe | 13 | **7** |

Sites that die outright: the message variant, the `probe_type_str` arm, the `shard/mod.rs`
re-export, the dispatch arm *and* its hand-maintained doc list, the continuation-lock count
bump, the trait method, **both** `StubDebug` bodies, the handler gather body, the harness
driver method, the `workload_runner` call.

**Locality.** "What probes exist, and what does each collect" currently answers itself across
`message.rs` + `dispatch_debug_introspection.rs` + `diagnostics.rs` + `conn_command.rs` +
`debug_handler.rs` + `debug_conn_command.rs`. After, one 5-arm match in `probe.rs`, with the
wire shape in one 5-arm match in `debug_conn_command.rs`. Two matches, both exhaustive, both
compiler-checked — replacing six lists, two of which have already drifted (§Problem 2).

**Deletion test, applied honestly.** Net line change is roughly **−130 plumbing lines,
+130 new-module lines** — near zero, and this proposal does not claim otherwise. The five
collectors survive, the five formatters survive, the five reply structs survive. What is
deleted is **the six-fold repetition of "this probe exists"**, and what is bought is that the
next probe costs 4 boilerplate edits instead of 12 and cannot be added without a
compiler-enforced decision about whether it mutates. A refactor whose value is measured in
deleted lines would fail here; the value is the *derivative* — the cost of the 6th probe, not
the size of the 5th.

## Testability improvement

1. **Reply bytes are pinned before, during and after.** `integration_debug_introspection.rs`
   (5 tests) and the five tier-4 parsers in `quiescence_probe.rs:157-320` together assert
   the full RESP shape of every probe, including the sentinel strings. Because the five
   formatters are moved-not-modified, these pass unchanged — that is the refactor's
   acceptance criterion, and it exists today.
2. **Exhaustiveness replaces enumeration.** `gather_probe`'s match and `format_probe`'s match
   are both exhaustive over `ShardProbe`. A new variant fails to compile in exactly the two
   places that must think about it, instead of silently compiling with a stale doc comment
   (§Problem 2) and a missing `DEBUG HELP` row.
3. **A round-trip property test becomes possible for the first time.** With a `ShardProbe`
   enum there is a value to iterate: `for probe in ShardProbe::ALL` — assert every kind
   dispatches, replies with the matching `ProbeReport` variant, and formats to a
   non-error `Response`. Today each of those five assertions has to be hand-written against a
   differently-typed method, which is why only four of the five ever were.
4. **H1 makes the tier-4 checkers sound.** `ProbeGather` carries `shards_expected`; the
   parsers carry it into `QuiescenceSnapshots`; the four checkers in
   `frogdb-testing/src/quiescence.rs` gain a `Coverage` verdict, mirroring the
   `FifoCoverage` precedent from `64d03cab`. **Forcing test:** a fault-injected gather that
   drops shards 3-7 must make `check_locktable_empty` report *incomplete*, not *clean* —
   the test fails on today's code, which is the definition of a real defect.
5. **The `StubDebug` maintenance tax halves twice.** Both stubs
   (`conn_command.rs:1054-1072`, `debug_conn_command.rs:1167-1189`) shrink to one method.
   The server one currently returns `Vec::new()` for all five, which under the sentinel logic
   means every stubbed DEBUG probe test asserts against `# … is empty` — the same
   indistinguishable-from-timeout string as §Problem 3(b). With `shards_expected` the stub
   can state `0 of 0`, which is honest.

## Spec / LOCKED impact

**None.** No locked crate is in the file set: `frogdb-core`, `frogdb-server`,
`frogdb-shard-harness` and `frogdb-testing` are all outside the four locked areas (txn,
persistence, replication, cluster — `adr/0002`–`0004`).

**`FM-` tag clearance, per file, verified by grep at `ddc4b184`:**

| File | `FM-` tags | In an edited region? |
|---|---|---|
| `message.rs` | **0** | — |
| `types.rs` | 4 (`:409`, `:698` `FM-PERSISTENCE-022`; `:1268` `FM-PERSISTENCE-005`; `:1421` `FM-REPLICATION-061`) | **No.** Edited region is `:1033-1128`; nearest tag is 140 lines away. |
| `diagnostics.rs` | **0** | — |
| `dispatch_debug_introspection.rs` | **0** | — |
| `shard/mod.rs` | **0** | — |
| `conn_command.rs` (core) | **0** | — |
| `debug_handler.rs` | **0** | — |
| `debug_conn_command.rs` | **0** | — |
| `harness.rs` | **0** | — |
| `quiescence_probe.rs` | **0** | — |
| `testing/src/quiescence.rs` | **0** | — |

`just lint-failure-modes` is therefore unaffected in both directions: no row loses a forcing
test, no tagged test changes.

**Mutation gates:** not applicable — no locked crate. `just mutants-diff` is not required
before push for this change.

### Seam-lint clearance

Checked every gate in `Justfile:329`'s `lint-gates` list plus the compile-requiring
remainder.

| Gate | Bearing | Disposition |
|---|---|---|
| **`lint-continuation-lock`** (`scripts/continuation-lock-gate.py`) | **Direct.** `:89` pins `("DebugIntrospectionMsg", 6)`; rule 1 is a per-enum arm count, rule 2 is bidirectional enum↔dispatch parity. | **Pin edit required and intended: `6` → `2`.** Parity still holds exactly (2 variants, 2 arms). None of the six arms is in `GATE`, `EXEMPT` or `GATE_GAP` (verified: the pinned sets name only `CoreMsg`, `ScriptingMsg`, `VllMsg` arms), so no disposition is lost. **Note the design consequence and accept it:** collapsing five arms into one means a *future* probe no longer moves the count. That forcing function is replaced by a stronger one — the `Probe` arm calls a `&self` method, so a mutating "probe" cannot compile inside the table at all. The script's own doc (`:20-33`) already reasons that "DEBUG probes … never touch the keyspace the continuation lock protects"; this change makes that reasoning type-checked instead of asserted. |
| **`lint-metrics-chokepoint`** (`Justfile:1198`) | Bans `.increment_counter(` / `.record_gauge(` / `.record_histogram(` outside the typed-handle allowlist. | **Clear — no metric emission moves.** `diagnostics.rs` does emit metrics through typed handles, but exclusively in `collect_shard_metrics` (`:351`), a `&mut self` tick collector that is **not** a probe and is not in the file set's edited regions. None of the five `collect_*` probe collectors (`:207-299`) emits any metric. Grep confirms zero raw-emission calls in every touched file. |
| `lint-clock-seam` (`scripts/clock-seam.py`) | Clock reads must go through the seam. | Clear — no touched region reads a clock. `WaitQueueLogInfo`'s ordinals are a monotonic counter, not a clock. |
| `lint-info-seam`, `lint-redirect-seam`, `lint-durable-ack`, `lint-failover-atomicity`, `lint-pubsub-confirmation-seam`, `lint-keyspace-notify-routing`, `lint-script-gate`, `lint-nested-config`, `lint-format-float`, `lint-error-sanitize`, `lint-no-typed-unwrap` | — | Clear; none has a rule reaching the DEBUG probe surface. The sentinel/error strings the formatters emit are unchanged bytes, so `lint-error-sanitize` sees no new error construction. |
| `lint-turmoil` / `lint-turmoil-features` | The `wait-queue-log` cargo feature. | Clear. The feature gate lives entirely inside `wait_queue.rs` (`:74`, `:79`, `:86`, `:132-166`, `:620-638`); `collect_wait_queue_log` (`diagnostics.rs:265`) and the whole probe surface are **unconditionally compiled** and simply report an empty journal without the feature. `ShardProbe::WaitQueueLog` needs no `cfg`. |

## Reply-shape compatibility

The brief asked what pins the probe reply shapes. Verified answer:

- **`frogctl` pins nothing.** Full grep of `frogctl/src`: the only `DEBUG` use anywhere is
  `DEBUG HASHING` (`commands/data.rs:51`, with a graceful `DEBUG HASHING not available`
  fallback). No probe is parsed by the CLI.
- **`frogdb-debug` (bundles, web UI) pins nothing.** Grep for the probe names across
  `crates/debug`: zero hits. `DiagnosticCollector` gathers via its own shard-sender path.
- **The tier-4 test adapter is the sole pin, and it is thorough.**
  `quiescence_probe.rs:157-320` hand-parses all five replies including nested structure
  (`parse_waiters :192`, `parse_log_entries :273`) and the RESP2-flattened map form
  (`:5-8`). It reads these exact field names: `intents`, `continuation_lock`,
  `total_waiters`, `keys`, `waiters`, `conn_id`, `op`, `registration_seq`, `truncated`,
  `registrations`, `key`, `tracked_bytes`, `recomputed_bytes`, `anomalies`. And these exact
  sentinel strings must keep parsing to zero snapshots: `# lock table is empty`,
  `# wait queue is empty`, `# expiry index is consistent`.
- **`integration_debug_introspection.rs`** (5 tests) additionally pins the RESP2/RESP3 map
  rendering (`:106-109`) and the unknown-subcommand path (`:26`).

Because the five formatters move without modification, **every one of these pins is
preserved by construction**. The only reply-shape change in the proposal is H1's additive
coverage field, which is a new key — and adding a key to a RESP map is the shape change these
parsers tolerate by design (`field(detail, "…")` lookups, not positional).

## Aggregation path (verified)

The brief asked whether `conn_command.rs` fans out. It does not.

- `frogdb-core/src/conn_command.rs` only **declares** `-> BoxFuture<Vec<XInfo>>`; `frogdb-core`
  has no shard senders to fan out with.
- The fan-out lives in `frogdb-server`: `debug_handler.rs:100-155` → `ConnectionHandler::scatter_gather()`
  (`connection/scatter.rs:25-31`) → `ScatterGather::gather_all` (`scatter/broadcast.rs:285-329`).
- **Aggregation is concatenation, not merge.** Each reply struct carries its own `shard_id`
  (`types.rs:1037`, `:1048`, `:1087`, `:1112`, `:1122`) and each formatter emits a
  `shard:<id>` map key. Nothing is summed or merged — unlike `keysizes_snapshot`
  (`debug_handler.rs:331-345`, `merged.merge(&snap)`) and `allocsize_in_slot` (`:348-361`,
  `.sum()`), which is a further reason those two stay out of the probe table.
- The gather is bounded by `self.scatter_gather_timeout`, an operator-visible live-mutable
  param (`config/src/param_id.rs:98` `scatter-gather-timeout-ms`, default 5000,
  `runtime_config.rs:2038-2040`). Every probe honours it. **`DEBUG PUBSUB LIMITS` does
  not** — H2.

## Risks / scope boundaries vs siblings

**Scope ruling — what is *not* in the table.** `DEBUG VLL` (has a `shard_filter`),
`EXPIRE-BACKDATE` (keyed mutator), `SET-ACTIVE-EXPIRE` / `KEYSIZES-HIST-ASSERT` /
`ALLOCSIZE-SLOTS-ASSERT` (`ObservabilityMsg`; mutator / merge-fold / sum-fold, also INFO-facing),
`PUBSUB LIMITS` (`SearchMsg`, shard-0), `CLUSTER CHECK` (no shard round-trip at all —
`debug_handler.rs:368-373` is a plain read-lock borrow). Pulling any of them in would build a
table whose entries do not share a shape, which is the failure mode this proposal exists to
avoid. §Problem 4 is the evidence.

**Sibling edges, most-constraining first:**

| Sibling | Shared file | Edge |
|---|---|---|
| **85** (`frogdb-macros` fate, being authored concurrently — **not read**) | `message.rs` | **Cleanest boundary in the round, and it holds.** 85's PN11 half owns `probe_type_str`, which lives at `message.rs:1000-1026` (the `ShardMessage` fold) and `:1135-1146` (`impl DebugIntrospectionMsg`). **87 owns `:810-855` only** — 145 lines above 85's nearest line and 280 below its lowest. **No overlapping line.** Semantic edge, stated explicitly: **87 changes the *number* of `DebugIntrospectionMsg` variants (6 → 2); 85 changes how each variant's *name string* is produced.** If 85 derives `probe_type_str`, 87 shrinks its input; if 87 lands first, 85 derives over 2 variants instead of 6. Either order works. **Wire consequence 87 must own:** the USDT probe strings `"GetLockTableInfo"`, `"GetWaitQueueInfo"`, `"GetWaitQueueLog"`, `"MemoryCheck"`, `"ExpiryIndexCheck"` are documented byte-stable (`message.rs:1003-1005`, *"downstream USDT probe consumers depend on"*). After 87 there is one variant, so `probe_type_str` **must match on the inner `ShardProbe`** to keep those five strings. This is a two-line requirement on 87, not a request of 85, and it is stated here so 85's author does not have to infer it. |
| **81** (`core-dead-seams`, PN2+PN3) | `event_loop.rs`, `diagnostics.rs`, `harness.rs` | **Correction to the brief: 81 does *not* touch `message.rs`** — grep of `81-core-dead-seams.md` returns zero `message.rs` hits, and its files-involved table names none. **Also: `f73bdd8f` committed the proposal *document*, not the code** — 81 is unlanded, so this is a planning-order edge, not a rebase edge. Overlaps are three files, all disjoint by ≥150 lines: `event_loop.rs` (81 owns the `select!` arm `:119-124`; 87 reads `:391-394` and edits nothing), `diagnostics.rs` (81 owns dummy-channel ceremony `:502`, `:508`; 87 owns `:207-299`), `harness.rs` (81 owns `:53`, `:80-81`, `:84`, `:94`; 87 owns `:294-358`). **Order: either; no textual conflict.** Prefer 81 first only because it is S and shrinks `harness.rs`'s constructor before 87 edits its driver block. |
| **84** (`blocking-op-dedupe`, being authored concurrently — **not read**, per lane brief) | possibly `message.rs`, `wait_queue.rs`, `blocking.rs` | **Unverified edge, flagged as such.** If 84 touches `BlockingMsg` in `message.rs`, it is a *different enum* at a different offset, and the continuation-lock gate pins `BlockingMsg` separately (`continuation-lock-gate.py:85`, count 2) — so even a simultaneous pin edit is two different dictionary entries. The one place a real conflict could arise: 84 may alter `ShardWaitQueue`'s registration journal, which `collect_wait_queue_log` (`diagnostics.rs:265-283`) reads. **87 does not modify that collector**, so a change under it is absorbed. **Declared unverified; re-check before either lands.** 81's PN3 also rewrites `wait_queue.rs` — 84 and 81 should settle that between themselves; 87 is downstream of both and neutral. |
| **74** (`debug-bundle-assembler`) | `debug_handler.rs` | 74 owns `:222-277` (`bundle_generate`, `bundle_list`); 87 owns `:100-155` (+H2's `:178` is claimed by **67**, not by either). **Disjoint hunks, any order.** |
| **79** (`debug-webui-router`) | none | 79 is confined to `crates/debug/web_ui/**` + `observability_server.rs`. No probe surface. **No edge.** |
| **67** (`server-small-dedups`) | `debug_handler.rs:178` | **67 already owns and has filed H2.** Its §"Out of scope, but file an issue" names `:173`/`:178`, the hardcoded `from_secs(5)`, the `scatter_gather_timeout` divergence, and both bespoke error strings, and rules it out of SV7 deliberately. **87 does not claim it.** Noted only because 87's `ProbeGather` makes the eventual fix a two-line adapter onto `query_one`. |

**Behaviour changes:** exactly one, and it is additive — H1's coverage field. Every probe's
existing keys, ordering, sentinel strings and RESP2/RESP3 rendering are byte-identical,
enforced by the pins in §Reply-shape compatibility.

**The one judgement call.** `ProbeReport` is an enum, so `format_probe` must match a
`ProbeReport` variant against the `ShardProbe` the caller asked for. The mismatch case is
unreachable (the same `gather_probe` produced both) but must be *written*. Ruling: make it a
`debug_assert!` plus a defensive skip, **not** a panic and **not** a silent drop — a dropped
report would recreate §Problem 3's false-clean. The alternative (a generic
`gather_probe<R: Probe>` with an associated reply type) buys type-level exhaustiveness but
forces `DebugProvider` to be generic, which breaks its object safety — `DebugProvider` is
consumed as `&dyn DebugProvider` (`debug_conn_command.rs:93-112`). The enum is the right
trade and this is why.

## Effort

| Part | Effort | Notes |
|---|---|---|
| **H1 — coverage signal** (independently landable, **first**) | **S** | `ProbeGather.shards_expected` + parser field + four checker verdicts + one fault-injection forcing test. Touches `debug_handler.rs`, `debug_conn_command.rs`, `quiescence_probe.rs`, `workload_runner.rs`, `testing/src/quiescence.rs`. Lands **before** the refactor so the refactor's acceptance run has a sound oracle. |
| **The probe table** | **M** | New 130-line module; 6→2 message enum; 5→1 in four places (trait, both stubs, handler); routing + formatter dispatcher; harness driver; one lint pin. ~13 files, net line change ≈ 0. Mechanical, but wide, and the `probe_type_str` byte-stability requirement (§85 edge) must not be forgotten. |
| **H3 — stale doc comment** | **XS** | `message.rs:810-811`. Free inside the refactor; also landable alone. |
| **Mutation re-gate** | **none** | No locked crate. |
| **Docs** | **XS** | `debugging.md:34-48` needs no content change (subcommand names are unchanged); a one-line note that the probes share a table is optional. |

**Recommended sequence:** H1 (S) → probe table (M). H3 rides either.

## Hotfix candidates

| ID | Classification | Claimed? | Detail |
|---|---|---|---|
| **H1** | **LIVE** — test-oracle soundness (no production data path) | **CLAIMED** | Four tier-4 quiescence checkers report *clean* on a partial or empty probe gather. Three compounding causes, each verified: `gather_all` breaks on first timeout and drops the shard suffix (`scatter/broadcast.rs:325`) with no coverage record; the sentinel folds are vacuously true on an empty `Vec` (`debug_conn_command.rs:602`, `:657`, `:786`) and render `# lock table is empty` for a gather that heard from nobody; all four checkers `Ok(())` on an empty slice (`testing/src/quiescence.rs:81-136`). Triggered by any shard missing `scatter-gather-timeout-ms` — i.e. exactly the load the concurrency suite generates. In-repo precedent for the fix and its ruling: `64d03cab` / concurrency issue 16 (`WAITQUEUE-LOG`'s `truncated`). Denominator already available at `debug_conn_command.rs:112`. **Landable ahead of, and independently of, the refactor.** |
| **H2** | **LIVE** — operator-visible config divergence | **NOT claimed** | `debug_handler.rs:178` waits on `DEBUG PUBSUB LIMITS`'s shard-0 round-trip with a hardcoded `Duration::from_secs(5)` instead of `self.scatter_gather_timeout`, ignoring the live-mutable `scatter-gather-timeout-ms` param (`config/src/param_id.rs:98`, `runtime_config.rs:2038-2040`). **Already owned, argued and filed by proposal 67** (§"Out of scope, but file an issue", and its effort table's "Issue to file" row). 87 cites it only to record that `ProbeGather` would later reduce the fix to an adapter onto `ScatterGather::query_one`. |
| **H3** | **LATENT** — documentation drift | **CLAIMED (drive-by)** | `message.rs:810-811` documents `DebugIntrospectionMsg` as *"(LOCKTABLE / WAITQUEUE / MEMORY-CHECK / EXPIRY-INDEX-CHECK)"*. The enum has six variants; `WAITQUEUE-LOG` and `EXPIRE-BACKDATE` are missing. Evidence for §Problem 2 and a one-line fix. |

**Security findings: none.** No probe accepts attacker-controlled input beyond
`EXPIRE-BACKDATE`'s key (already length-bounded by the RESP parser and out of scope here); no
probe reply echoes unsanitised client text; every probe is behind `DEBUG`, and the one gated
subcommand (`SLEEP`) is unrelated. Standing policy noted: security findings would be
**classification-only, filed and parked, never a fix proposal**.

## References

- Ground-truth "add one probe" diff: `0a8b5c11` (`feat(server): DEBUG EXPIRY-INDEX-CHECK
  introspection command`, 10 files, +178/−7).
- Coverage-verdict precedent: `64d03cab` (`test(concurrency): repair three checker soundness
  defects`), concurrency issue 16 — *"the checker reports FifoCoverage instead of ever
  proxying — incomplete ordinals mean 'proves nothing', not a verdict."*
- H2's owner: `.scratch/arch-deepening/proposals/67-server-small-dedups.md`,
  §"Out of scope, but file an issue: the sixth shard-0 send".
- Continuation-lock gate design rationale (why a count pin, and what replaces it here):
  `scripts/continuation-lock-gate.py:15-33`.
- Boundary ADRs for locked areas (none apply): `adr/0002`–`adr/0004`.
