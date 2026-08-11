# Proposal 87 — `ShardProbe`: one probe table instead of a five-way smear, and a coverage signal the quiescence checkers currently lack

**Revision 2** (adversarial review verdict: AMEND). Revision 1's headline chain survived attack
at code level; four load-bearing claims did not. Changed in rev 2: **H1's mechanism is now
wire-free** (rev 1's "additive RESP map key" was impossible — the clean-path reply is a bulk
string, not a map); **the 85 boundary is no longer disjoint** (87 must edit `probe_type_str`);
**the leverage arithmetic is corrected downward** (13→7 boilerplate, not 12→4); **H1's trigger
is restated as a soundness hole, not an observed firing**; **B7's CI-gate debit is stated
instead of being papered over**. §Revision-2 digest at the end lists every change with its
evidence.

Round 38 · lane: protocol / net / core · candidate **PN10** · effort **M** (probe table)
+ **S** (H1 coverage hotfix, independently landable first) · **no locked crate edited**
(`frogdb-core`, `frogdb-server`, `frogdb-shard-harness`, `frogdb-testing`), **zero `FM-` tags
in any edited region**

**Verified at `ddc4b184`; re-verified at HEAD `4421aec7`.** `git diff --stat ddc4b184..4421aec7`
touches **only** `.scratch/arch-deepening/proposals/*.md` — no code file moved, so every
file:line below holds unchanged at HEAD. Concurrent authors hold proposals 81, 82 and 84
(modified in the working tree); no code file in this set is dirty.

**Six brief claims are corrected, and one of the corrections turns "M, Latent" into a
LIVE test-oracle soundness defect that is independently landable ahead of the refactor.**

| Brief claim | Correction at HEAD |
|---|---|
| "~9 touch points per probe" | **20 sites across 14 files**, of which **13 are pure boilerplate**. The brief's list omits seven live ones: `probe_type_str` (`message.rs:1139-1144`), the `shard/mod.rs` re-export (`:88-92`), the **continuation-lock gate count pin** (`scripts/continuation-lock-gate.py:89`), the `DEBUG HELP` text (`debug_conn_command.rs:290-299`), the shard-harness driver method (`harness.rs:294-358`), the tier-4 RESP parser (`quiescence_probe.rs`), and the docs row (`debugging.md:34-48`). §Problem 1 builds the table from the last real "add one probe" commit. |
| "core shard `message.rs:821` (message variant)" | `:821` is the **`GetWaitQueueInfo` variant specifically**. The enum is `DebugIntrospectionMsg` at **`message.rs:810-855`** (doc `:810-811`, `pub enum` `:812`, 6 variants). The line is inside the right region; the region is 46 lines, not one. |
| "`diagnostics.rs` ×6 sites" | **5**, not 6, in this family: `collect_lock_table_info :207-231`, `collect_wait_queue_info :234-257`, `collect_wait_queue_log :265-281`, `collect_memory_check :284-290`, `collect_expiry_index_check :293-299`. The 6th (`collect_vll_queue_info :167`) serves **`VllMsg::GetVllQueueInfo`**, a different message enum, and `DEBUG VLL` takes a `shard_filter` argument — it is not a uniform probe and is **out of scope** (§Scope boundaries). |
| "`types.rs:1035-1132`" | **`:1033-1128`**. `:1130-1139` is `PubSubLimitsInfo`, which belongs to `SearchMsg` and the `DEBUG PUBSUB LIMITS` path — a different family, not touched. |
| "`debug_conn_command.rs:646` (one site)" | **Two sites per probe in that file, and `:646` is inside neither list**: the routing arms are `:153-161` and the formatters are `:602-817` (five functions, **216 lines**). `:646` is a line *inside* `format_locktable_response`. Plus the `DEBUG HELP` text `:290-299` and the test `StubDebug` impl `:1167-1189` — **four** per-probe regions in this one file. |
| "plus a test noop" | **Two** noop `DebugProvider` impls: `StubDebug` in `frogdb-core` (`conn_command.rs:1037-1105`, `unimplemented!()` bodies) and `StubDebug` in `frogdb-server` (`debug_conn_command.rs:1151-1219`, `Box::pin(async { Vec::new() })` bodies). Both grow by one method per probe. |

**And one finding the brief did not anticipate (§Problem 3, hotfix H1, LIVE):** the four tier-4
quiescence checkers **cannot distinguish "every shard is clean" from "we never heard from most
of the shards"**, because `gather_all` is best-effort, the sentinel formatters fold over
*survivors only*, and every checker returns `Ok(())` on an empty slice. A `DEBUG LOCKTABLE`
that timed out on shard 0 renders the literal bytes `# lock table is empty`. This is the same
class of defect the project already ruled real and fixed for `WAITQUEUE-LOG` (commit
`64d03cab`, concurrency issue 16: *"incomplete ordinals mean 'proves nothing', not a
verdict"*) — the other four probes never got the treatment.

## Files involved

Line counts at `ddc4b184` (unchanged at HEAD `4421aec7`).

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/core/src/shard/probe.rs` | *new (~130)* | **The change.** `ShardProbe` (5-variant kind), `ProbeReport` (5-variant reply), `impl ShardWorker { fn gather_probe(&self, ShardProbe) -> ProbeReport }` — the one match where a probe kind becomes a snapshot. `&self`, not `&mut self`: read-only becomes a type property (§Proposed change B). |
| `frogdb-server/crates/core/src/shard/message.rs` | 1446 | **Primary, two regions.** `DebugIntrospectionMsg` `:810-855` — five snapshot variants (`:814-842`) collapse to one `Probe { probe, response_tx }`; `ExpireBackdate` `:844-854` **stays** (it is the one mutator, §B). Enum goes 6 variants → 2. The doc comment `:810-811` is **stale today** (hotfix H3). `From` impl `:979-983` unchanged. **`DebugIntrospectionMsg::probe_type_str` `:1135-1148` — 87 *does* edit this** (rev 1 wrongly claimed it did not): its five arms `:1139-1144` name the exact variants 87 deletes, so the code stops compiling otherwise. The arms become a nested match on the inner `ShardProbe`, preserving all six byte-stable USDT strings (`message.rs:1003-1005`). **This region is inside proposal 85's `:1028-1179` — a real overlap, see §Sibling edges.** |
| `frogdb-server/crates/core/src/shard/dispatch_debug_introspection.rs` | 42 | **Primary.** The 6-arm match `:16-40` → 2 arms; the module doc `:1-7` stops hand-enumerating the probe list. |
| `frogdb-server/crates/core/src/shard/diagnostics.rs` | 598 | **Primary, but the five collectors survive verbatim.** `collect_lock_table_info :207-231`, `collect_wait_queue_info :234-257`, `collect_wait_queue_log :265-281`, `collect_memory_check :284-290`, `collect_expiry_index_check :293-299` — all `&self`, all kept, all still individually unit-tested (`:559`, `:579`). Only their *callers* move. **Proposal 81 (PN2) owns `:502` and `:508` in this file** — 200+ lines away, disjoint (§Sibling edges). |
| `frogdb-server/crates/core/src/shard/types.rs` | 1498 | **Primary, additive-free.** The five reply structs `:1033-1128` are **unchanged** — they become `ProbeReport`'s payloads as-is. **4 `FM-` tags exist in this file (`:409`, `:698`, `:1268`, `:1421`) — all outside `:1033-1128`** (§Spec clearance). |
| `frogdb-server/crates/core/src/shard/mod.rs` | 96 | **Primary, small — and this site does *not* die.** `pub use types::{…}` (`:87-94`; the five probe reply structs sit at `:88-92`) **must keep exporting every reply struct**, because the five formatter signatures name them (`debug_conn_command.rs:602`, `:656`, `:717`, `:755`, `:783`). Adds `mod probe;` and `pub use probe::{ProbeReport, ShardProbe};`. A sixth probe still needs a sixth name here. Rev 1 counted this among the sites that die; that contradicted its own §Files note and is withdrawn. |
| `frogdb-server/crates/core/src/conn_command.rs` | 1172 | **Primary.** `DebugProvider` `:534-616` — five methods `:551-565` (`gather_lock_table`, `gather_wait_queue`, `gather_wait_queue_log`, `memory_check`, `expiry_index_check`) → one `gather_probe(&self, ShardProbe) -> BoxFuture<Vec<ProbeReport>>`. `gather_vll :546-549` **stays** (has an argument). `StubDebug` `:1054-1072` — five `unimplemented!()` bodies → one. **Not touched by H1** (rev 1 listed it; see §Revision-2 digest). |
| `frogdb-server/crates/server/src/connection/debug_handler.rs` | 374 | **Primary.** Five near-identical `Box::pin(async move { self.scatter_gather().gather_all(\|_shard, tx\| Msg::X { tx }).await })` bodies `:100-155` (**56 lines, 5 functions**) → one. `gather_vll :75-98` untouched. **Proposal 74 owns `:222-277`** in this file (bundle_generate/bundle_list) — disjoint hunks. **`:178`'s hardcoded 5 s is proposal 67's filed issue — cited, not claimed (H2).** **Not touched by H1.** |
| `frogdb-server/crates/server/src/connection/debug_conn_command.rs` | 1445 | **Primary, four regions.** Routing arms `:153-161` → five one-liners over one helper; `DEBUG HELP` `:290-299` (two lines per probe); the five formatters `:602-817` (**216 lines**) **survive verbatim** behind one `format_probe(ShardProbe, Vec<ProbeReport>)` dispatcher; `StubDebug` `:1167-1189` five bodies → one. `shard_count = ctx.shard_senders.len()` `:112` is already in scope at `:153-161` — relevant to the *deferred* operator-facing variant H1b, not to H1 as claimed. **Not touched by H1.** |
| `frogdb-server/crates/shard-harness/src/harness.rs` | 399 | **Primary.** The probe block `:294-358` holds **four** snapshot drivers — `wait_queue_info :296-304`, `lock_table_info :306-314`, `memory_check :316-324`, `expiry_index_check :350-358` — plus `backdate_expiry :331-348`. **There is no `WAITQUEUE-LOG` driver**, so this site is 4-of-5 today, not universal. The four snapshot drivers become **four** two-line typed wrappers over one `probe(shard, ShardProbe)`; `backdate_expiry` unchanged. **Proposal 81 (PN2) owns `:53`, `:80-81`, `:84`, `:94`** — disjoint. |
| `scripts/continuation-lock-gate.py` | — | **Primary, one number.** `"dispatch_debug_introspection.rs": ("DebugIntrospectionMsg", 6)` **`:89`** → `2`. Rule 2 (enum parity, both directions) still holds by construction. **This is where 87 spends a CI forcing function — see §Seam-lint clearance for the debit and the proposed compensating pin.** |
| `frogdb-server/crates/server/tests/common/quiescence_probe.rs` | 571 | **Primary (H1).** Five hand-written RESP parsers `:157-320` + `QuiescenceSnapshots` `:27-71`. The **only** consumer that pins probe reply shapes (§Reply-shape compatibility). H1 adds `shards_expected` to the bundle and a coverage check to `check_quiescence` `:75-92`. |
| `frogdb-server/crates/server/tests/common/workload_runner.rs` | 856 | **Primary (H1), small.** The five `debug_probe(...)` calls `:268-271`, `:279`; `debug_probe` itself `:382-393`. H1 threads `SimConfig::num_shards` into `QuiescenceSnapshots::from_replies` (`quiescence_probe.rs:57-70`). |
| `frogdb-server/crates/server/tests/common/sim_harness.rs` | — | **Read-only (H1), the denominator's source.** `SimConfig::num_shards` `:39`, default `4` at `:56`. This is the shard count the sim server is actually built with, so it is the correct "how many shards should have answered" for the tier-4 bundle. |
| `frogdb-server/crates/testing/src/quiescence.rs` | — | **Primary (H1).** `check_locktable_empty :81-92`, `check_waitqueue_empty :95-105`, `check_memory_accounting :108-121`, `check_expiry_index_consistent :124-136` — **all four `for s in snapshots { … } Ok(())`, all four vacuously pass on an empty slice** (§Problem 3). H1 adds one `QuiescenceViolation::CoverageIncomplete` variant. |
| `frogdb-server/crates/server/tests/integration_debug_introspection.rs` | 154 | **Read-only regression pin, and a hard constraint on H1.** **7 `#[tokio::test]`s covering 4 of the 5 probes** (`:8` LOCKTABLE-empty, `:25` unknown-subcommand guard, `:35` CLUSTER CHECK, `:52` WAITQUEUE-empty, `:64` WAITQUEUE-blocked, `:95` MEMORY-CHECK, `:133` EXPIRY-INDEX-CHECK). `WAITQUEUE-LOG` has none. `:16` and `:57` assert the sentinel is a **`Response::Bulk`**; `:64-84` uses *"an Array reply means seen"* as its entire heuristic. **Any change to the clean-path RESP type breaks these silently** — which is what killed rev 1's H1 mechanism. |
| `website/src/content/docs/architecture/debugging.md` | 220 | **Primary, hand-written (no docs-gen source).** The FrogDB-subcommand table `:34-48` (probe rows `:38`, `:39`, `:40`, and the shared `:45`) — nominally one row per probe, except `MEMORY-CHECK` and `EXPIRY-INDEX-CHECK` were lumped into a single `:45` row reading "Internal consistency checks", a third instance of the hand-maintained-list drift in §Problem 2. |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | — | **Read-only, and a brief-adjacent trap.** `:391-394` is a **match arm inside the message-dispatch fn** (not a `select!` arm) that hands `ShardMessage::DebugIntrospection(m)` to `dispatch_debug_introspection` **without naming any variant** — so it is **no longer a per-probe touch point** (it was one at `0a8b5c11`, before the flat message enum was split). **Proposal 81 (PN2) owns `:119-124`**, which is **arm 6 of the same `select!`** whose arm 7 reaches that dispatch fn — adjacent, but a different hunk, and 87 edits neither. |
| `frogctl/**` | — | **Read-only, negative evidence.** `frogctl` consumes **no** probe. Its only `DEBUG` use is `DEBUG HASHING` (`commands/data.rs:51`). §Reply-shape compatibility. |

## Problem

### 1. Adding one probe is a 20-site, 14-file diff — and 13 of the sites carry no information

The ground truth is `0a8b5c11` *"feat(server): DEBUG EXPIRY-INDEX-CHECK introspection
command"* — the last commit that added exactly one probe and nothing else. It touched **10
files, +178/−7**. The table below is what a contributor writes **today**, re-derived at
`ddc4b184` — it is **not** `0a8b5c11`'s file list with two entries appended. Four of the
twenty sites postdate that commit entirely: `harness.rs`'s driver block, `quiescence_probe.rs`,
`workload_runner.rs` and `debugging.md` are all tier-4/docs surface grown since. One site was
*lost* (`event_loop.rs`, absorbed by the message-enum split) and two were *gained* (the second
`StubDebug`, the continuation-lock count pin). `0a8b5c11`'s ten files are the historical floor;
fourteen is the number today.

| # | Site | Location | Carries probe-specific information? |
|---|---|---|---|
| 1 | Message variant | `message.rs:810-855` | **no** — name + one `oneshot::Sender<T>` |
| 2 | `probe_type_str` arm | `message.rs:1139-1144` | **no** — variant name as a string |
| 3 | Reply struct | `types.rs:1033-1128` | yes — the data |
| 4 | `shard/mod.rs` re-export | `:88-92` | **no** |
| 5 | Collector | `diagnostics.rs:207-299` | yes — the work |
| 6 | Dispatch arm | `dispatch_debug_introspection.rs:16-40` | **no** — `response_tx.send(self.collect_x())` |
| 7 | Dispatch module doc list | `dispatch_debug_introspection.rs:1-7` | **no** — hand-maintained enumeration |
| 8 | Continuation-lock count pin | `scripts/continuation-lock-gate.py:89` | **no** — an integer |
| 9 | `DebugProvider` trait method | `conn_command.rs:551-565` | **no** |
| 10 | core `StubDebug` body | `conn_command.rs:1054-1072` | **no** — `unimplemented!()` |
| 11 | Server gather body | `debug_handler.rs:100-155` | **no** — identical modulo the variant name |
| 12 | Routing arm | `debug_conn_command.rs:153-161` | yes — the subcommand spelling |
| 13 | Formatter | `debug_conn_command.rs:602-817` | yes — the wire shape |
| 14 | `DEBUG HELP` lines | `debug_conn_command.rs:290-299` | **no** — restates the doc comment |
| 15 | Server `StubDebug` body | `debug_conn_command.rs:1167-1189` | **no** — `Vec::new()` |
| 16 | Harness driver method | `harness.rs:294-358` | **no** — *4 of 5 today; `WAITQUEUE-LOG` has none* |
| 17 | Tier-4 RESP parser | `quiescence_probe.rs:157-320` | yes — inverse of #13 |
| 18 | Tier-4 gather call | `workload_runner.rs:268-271` | **no** — subcommand bytes over the wire |
| 19 | Integration test | `integration_debug_introspection.rs` | yes — *4 of 5 today* |
| 20 | Docs table row | `debugging.md:34-48` | yes |

**13 of 20 sites are boilerplate** (#1, #2, #4, #6, #7, #8, #9, #10, #11, #14, #15, #16, #18);
7 are information-bearing (#3, #5, #12, #13, #17, #19, #20). Distinct files: **14**.

Six of the boilerplate sites (#1, #6, #9, #10, #11, #15) are the *same sentence* written six
times in six dialects: *"this probe exists."*

The five `debug_handler.rs` bodies `:100-155` are the purest instance — after `gather_all`
(`frogdb-server/crates/server/src/scatter/broadcast.rs:285-330`) already generalised the
fan-out, deadline and error mapping, what is left per probe is **eight lines that differ by one
identifier**:

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
- **`dispatch_debug_introspection.rs:1-2`** carries the *correct* five-name list, and
  `:3-4` then asserts *"The snapshot probes are read-only per-shard collectors"* — a
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

### 3. The quiescence checkers accept a partial gather as "clean" (REAL, LIVE — a soundness hole)

This is the finding that upgrades the proposal. Three verified facts compound:

**(a) The gather is best-effort and drops a *suffix*.** `ScatterGather::gather_all`
(`frogdb-server/crates/server/src/scatter/broadcast.rs:285-330`) collects survivors under one
shared deadline, and on the first timeout it **`break`s the loop** (`:325`) — so a slow shard 0
discards shard 0 *and every higher-numbered shard*. Send failures and dropped senders are
`continue`/`warn` and also vanish. The `Vec<R>` that comes back carries **no record of how many
shards were asked**. The crate's own test names this behaviour:
`gather_all_collects_survivors_under_one_deadline` (`:1197`).

**(b) The sentinel folds over survivors only.** `format_locktable_response`
(`debug_conn_command.rs:602-611`), `format_waitqueue_response` (`:656-659`) and
`format_expiry_index_check_response` (`:783-789`) each open with
`if infos.iter().all(|i| … is_empty()) { return Response::Bulk(Some("# … is empty")) }`.
With `infos` empty, `.all()` is vacuously true. **A gather that returned nothing renders the
exact bytes `# lock table is empty`.**

**(c) The checkers pass on an empty slice.** All four of
`frogdb-testing/src/quiescence.rs:81-136` are `for s in snapshots { if bad { return Err } }
Ok(())`. `quiescence_probe.rs:9-11` documents the sentinel path as *"that parses to zero
snapshots, which the checkers accept."* — which is correct behaviour **only** if zero
snapshots always means clean. It does not.

The tier-4 concurrency pipeline (`workload_runner.rs:265-273`, `invariants.rs:230-236`)
therefore has a **silent false negative**: if any shard misses the scatter deadline or its
sender is dropped, `check_locktable_empty` / `check_waitqueue_empty` /
`check_memory_accounting` / `check_expiry_index_consistent` report **clean** without having
examined the shards, and `invariants.rs:231` still sets `report.quiescence_checked = true`.

**Classification, stated precisely: this is LIVE as a soundness hole in the oracle, and no
firing instance is claimed.** Rev 1 asserted the defect fires under "exactly the load the
concurrency suite generates"; that is not supported and is withdrawn. Three facts argue the
opposite way: the probes are issued **post-drain**, against a settled server
(`workload_runner.rs:265-267` — *"the workload has drained, so the four DEBUG introspection
commands report the settled server state"*); tier-4 runs under turmoil's **virtual clock**
(`frogdb-server/crates/server/Cargo.toml:69`); and the deadline is **5000 ms** by default
(`acceptor.rs:420`, `scatter_gather_timeout_ms: 5000`) over **in-process mpsc** channels that
turmoil does not perturb. The defect is that *the oracle cannot tell clean from unheard* — it
would report green either way, so its green carries no information about coverage. That is
sufficient on the project's own precedent: `64d03cab` (concurrency issue 16) was likewise a
soundness repair, adding `truncated` to `WAITQUEUE-LOG` precisely so *"incomplete ordinals mean
'proves nothing', not a verdict"*. The forcing test in §Testability-4 fails on today's code,
which is the operative test for "real".

**`WaitQueueLogInfo` is the only one of the five reply types with a completeness flag**
(`types.rs:1090`). The other four have none, and none of the five has a *coverage* flag —
`truncated` is per-shard, not per-fleet, so it does not catch a missing shard either.

**MEMORY-CHECK is the load-bearing asymmetry, and it is what makes the fix free.**
`format_memory_check_response` (`:755-781`) has **no sentinel**: it always emits
`Response::Map(shards)`, one `shard:<id>` entry per survivor. `quiescence_probe.rs:12` states
this in its own module doc — *"MEMORY-CHECK always replies with a per-shard map."* So
`memory.len()` **is** the survivor count, already parsed, already in the bundle. §H1 uses it.

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
distinct work, and each keeps its own unit test (`diagnostics.rs:559`, `:579`). All five are
already `&self` today (`:207`, `:234`, `:265`, `:284`, `:293`), so `gather_probe` compiles
against them without a signature change.

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

**And `probe_type_str` must move with it** (`message.rs:1135-1148`). Its five arms
`:1139-1144` pattern-match the five variants being deleted, so this is a compile requirement,
not an option. The replacement preserves every byte of the documented contract
(`message.rs:1003-1005`, *"byte-for-byte identical to the pre-split flat variant names …
downstream USDT probe consumers depend on"*) by matching the inner enum — the exact idiom the
file already uses for `DriveTick` (`:1019-1022`, one variant yielding two strings):

```rust
impl DebugIntrospectionMsg {
    pub fn probe_type_str(&self) -> &'static str {
        match self {
            Self::Probe { probe, .. } => match probe {
                ShardProbe::LockTable    => "GetLockTableInfo",
                ShardProbe::WaitQueue    => "GetWaitQueueInfo",
                ShardProbe::WaitQueueLog => "GetWaitQueueLog",
                ShardProbe::MemoryCheck  => "MemoryCheck",
                ShardProbe::ExpiryIndex  => "ExpiryIndexCheck",
            },
            Self::ExpireBackdate { .. } => "ExpireBackdate",
        }
    }
}
```

All six strings are unchanged. The arm count for this impl stays at 6 (as USDT names), while
the *dispatch* arm count drops to 2 — the two counts are pinned by different mechanisms and
§Seam-lint clearance treats them separately.

This is the **seam** argument. Today `dispatch_debug_introspection.rs:3-4` *asserts* the
probes are read-only in prose while the function signature says `&mut self`. After, the
read-only guarantee is a borrow.

### C. The server side: one provider method, one gather, one formatter dispatcher

```rust
// core/src/conn_command.rs — five methods become one
fn gather_probe<'a>(&'a self, probe: ShardProbe) -> BoxFuture<'a, Vec<ProbeReport>>;
```

```rust
// debug_handler.rs — 56 lines of five bodies become one
fn gather_probe<'a>(&'a self, probe: ShardProbe) -> BoxFuture<'a, Vec<ProbeReport>> {
    Box::pin(async move {
        self.scatter_gather()
            .gather_all(|_shard, response_tx| DebugIntrospectionMsg::Probe { probe, response_tx })
            .await
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

`probe_reply` gathers, then hands `(probe, reports)` to `format_probe`, which sorts the
`ProbeReport`s into the **existing, unmodified** `format_locktable_response` /
`format_waitqueue_response` / `format_waitqueue_log_response` /
`format_memory_check_response` / `format_expiry_index_check_response`. Those 216 lines
(`:602-817`) are the genuine per-probe wire shape and this proposal does not touch a byte of
them — which is precisely why the reply is byte-identical (§Testability).

**Rev 1's `ProbeGather { reports, shards_expected }` wrapper is withdrawn.** It existed only
to carry H1's coverage denominator into the formatter; H1 no longer needs a production-side
denominator (§H1), so the return type stays `Vec<ProbeReport>` and the refactor's blast radius
shrinks by one type and three signatures. This also restores H1's independence: with
`ProbeGather` gone, H1 touches **no file this refactor edits in production code**.

### D. Depth, leverage, locality — and the honest deletion test

**Depth.** The `DebugProvider` interface today grows **linearly with the number of probes**:
five methods, and each new probe widens the trait, both stubs, and the handler. That is the
textbook shallow module — interface proportional to implementation. After, the interface is
*one* method plus a data enum, and it is constant in the number of probes. The `probe.rs`
module hides five collectors, the kind→collector mapping, and the read-only guarantee behind
three names.

**Leverage — corrected arithmetic.** Rev 1 claimed 12 → 4 boilerplate. That was inflated by
roughly half: it counted two sites as dying that do not (`probe_type_str`, the `shard/mod.rs`
re-export), a third that does not (`workload_runner`'s gather call is a RESP-level
`DEBUG <SUBCOMMAND>` write, unaffected by any internal refactor), and it under-counted the
baseline by one (the dispatch doc list). Corrected:

| | today | after |
|---|---|---|
| boilerplate sites | **13** | **7** |
| information-bearing sites | 7 | 7 |
| **total sites** | **20** | **14** |
| files touched to add a probe | **14** | **10** |

The seven that remain, named so the number can be audited: `ShardProbe` variant, `ProbeReport`
variant, `gather_probe` arm (all three in `probe.rs`), the `probe_type_str` inner-match arm,
the `shard/mod.rs` re-export of the new reply struct, the `DEBUG HELP` lines, and the tier-4
gather call. **Add an eighth** — a `ShardProbe` count pin — if the compensating seam-lint pin
in §Seam-lint clearance is adopted; that trade buys back the CI forcing function this refactor
otherwise spends.

Sites that genuinely die: the message variant (#1), the dispatch arm (#6) *and* its
hand-maintained doc list (#7), the continuation-lock count bump (#8), the trait method (#9),
**both** `StubDebug` bodies (#10, #15), the handler gather body (#11), and the harness driver
(#16 — a new probe is reachable as `harness.probe(shard, ShardProbe::X)` with zero new harness
code, where today it needs a nine-line driver method). Four files stop being touched at all:
`dispatch_debug_introspection.rs`, `core/conn_command.rs`, `debug_handler.rs`, `harness.rs`.

**Locality.** "What probes exist, and what does each collect" currently answers itself across
`message.rs` + `dispatch_debug_introspection.rs` + `diagnostics.rs` + `conn_command.rs` +
`debug_handler.rs` + `debug_conn_command.rs`. After, one 5-arm match in `probe.rs`, with the
wire shape in one 5-arm match in `debug_conn_command.rs`. Two matches, both exhaustive, both
compiler-checked — replacing six enumerations, two of which have already drifted (§Problem 2).
**This, plus the `&self` type property, is what carries the proposal** — not the site count,
which is a genuine but unspectacular halving.

**Deletion test, applied honestly.** Net line change is roughly **−130 plumbing lines,
+130 new-module lines** — near zero, and this proposal does not claim otherwise. The five
collectors survive, the five formatters survive, the five reply structs survive. What is
deleted is **the six-fold repetition of "this probe exists"**, and what is bought is that the
next probe costs 7 boilerplate edits instead of 13 and cannot be added without a
compiler-enforced decision about whether it mutates. A refactor whose value is measured in
deleted lines would fail here; the value is the *derivative* — the cost of the 6th probe, not
the size of the 5th.

## Testability improvement

1. **Reply bytes are pinned before, during and after.** `integration_debug_introspection.rs`
   (**7 tests, covering 4 of the 5 probes**) and the five tier-4 parsers in
   `quiescence_probe.rs:157-320` together assert the RESP shape of every probe that has a
   consumer, including the sentinel strings. Because the five formatters are
   moved-not-modified, these pass unchanged — that is the refactor's acceptance criterion, and
   it exists today. **`WAITQUEUE-LOG` is the gap**: it has no integration test, because its
   journal only records under the `wait-queue-log` cargo feature
   (`wait_queue.rs:132-134`, `:166`, `:620-624`), which the default server test build does not
   enable — a default-features integration test could only ever observe an empty journal. The
   tier-4 parser (`parse_waitqueue_log`) is its only pin, and tier-4 *does* enable the feature
   (`Cargo.toml:69` → `frogdb-core/wait-queue-log`).
2. **Exhaustiveness replaces enumeration.** `gather_probe`'s match, `format_probe`'s match and
   `probe_type_str`'s inner match are all exhaustive over `ShardProbe`. A new variant fails to
   compile in exactly the three places that must think about it, instead of silently compiling
   with a stale doc comment (§Problem 2) and a missing `DEBUG HELP` row.
3. **A round-trip property test becomes possible for the first time.** With a `ShardProbe`
   enum there is a value to iterate: `for probe in ShardProbe::ALL` — assert every kind
   dispatches, replies with the matching `ProbeReport` variant, and formats to a
   non-error `Response`. Today each of those five assertions has to be hand-written against a
   differently-typed method, which is why only four of the five ever were.
4. **H1 makes the tier-4 checkers sound — with zero production bytes changed.** See §Hotfix
   candidates for the full mechanism. **Forcing test** (pure function, no fault injection
   infrastructure needed): build a `QuiescenceSnapshots` from the exact bytes a fully-timed-out
   gather produces — `# lock table is empty`, `# wait queue is empty`, an empty MEMORY-CHECK
   map, `# expiry index is consistent` — with `shards_expected: 4`, and assert
   `check_quiescence` returns a coverage violation. **Today that call returns an empty
   violation list**, which is the definition of the defect. A second, end-to-end forcing test
   drops shards 3-7 from the gather and asserts the same.
5. **The `StubDebug` maintenance tax halves twice.** Both stubs
   (`conn_command.rs:1054-1072`, `debug_conn_command.rs:1167-1189`) shrink to one method.
   The server one currently returns `Vec::new()` for all five, which under the sentinel logic
   means every stubbed DEBUG probe test asserts against `# … is empty` — the same
   indistinguishable-from-timeout string as §Problem 3(b). That indistinguishability is
   *inherent* to a zero-shard stub and H1 does not change it; the stub's callers are unit
   tests of the routing layer, not coverage oracles, so it is documented rather than fixed.

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
| **`lint-continuation-lock`** (`scripts/continuation-lock-gate.py`) | **Direct.** `:89` pins `("DebugIntrospectionMsg", 6)`; rule 1 is a per-enum arm count, rule 2 is bidirectional enum↔dispatch parity. | **Pin edit required and intended: `6` → `2`.** Parity still holds exactly (2 variants, 2 arms). None of the six arms is in `GATE`, `EXEMPT` or `GATE_GAP` (verified: the pinned sets name only `CoreMsg`, `ScriptingMsg`, `VllMsg` arms), so no disposition is lost. **See the debit below — this is not free.** |
| **`lint-metrics-chokepoint`** (`Justfile:1198`) | Bans `.increment_counter(` / `.record_gauge(` / `.record_histogram(` outside the typed-handle allowlist. | **Clear — no metric emission moves.** `diagnostics.rs` does emit metrics through typed handles, but exclusively in `collect_shard_metrics` (`:351`), a `&mut self` tick collector that is **not** a probe and is not in the file set's edited regions. None of the five `collect_*` probe collectors (`:207-299`) emits any metric. Grep confirms zero raw-emission calls in every touched file. |
| `lint-clock-seam` (`scripts/clock-seam.py`) | Clock reads must go through the seam. | Clear — no touched region reads a clock. `WaitQueueLogInfo`'s ordinals are a monotonic counter, not a clock. |
| `lint-info-seam`, `lint-redirect-seam`, `lint-durable-ack`, `lint-failover-atomicity`, `lint-pubsub-confirmation-seam`, `lint-keyspace-notify-routing`, `lint-script-gate`, `lint-nested-config`, `lint-format-float`, `lint-error-sanitize`, `lint-no-typed-unwrap` | — | Clear; none has a rule reaching the DEBUG probe surface. The sentinel/error strings the formatters emit are unchanged bytes, so `lint-error-sanitize` sees no new error construction. |
| `lint-turmoil` / `lint-turmoil-features` | The `wait-queue-log` cargo feature. | Clear. The feature gates *recording* only (`wait_queue.rs:74`, `:79`, `:86`, `:132-134`, `:166`, `:620-638`); `collect_wait_queue_log` (`diagnostics.rs:265`) and the whole probe surface are **unconditionally compiled** and simply report an empty journal without the feature. `ShardProbe::WaitQueueLog` needs no `cfg`. |

**The debit, stated plainly (rev 1 misrepresented this).** The count pin is the gate's *entire*
forcing function for unclassified arms — its own rationale says so
(`continuation-lock-gate.py:15-33`: *"a new or renamed arm moves the count … so the
unclassified newcomer is the one without a tag"*), and that check runs **unconditionally on
every commit** via lefthook's `lint-gates`. After the collapse, adding `ShardProbe::NewThing`
moves **no** pinned count and trips **no** gate. Rev 1 claimed the `&self` signature is a
"stronger" replacement. It is not stronger, and the claim is withdrawn. What is true: all five
collectors are already `&self` (`diagnostics.rs:207`, `:234`, `:265`, `:284`, `:293`), so
`gather_probe(&self)` makes read-only-ness a **type property** rather than the prose assertion
at `dispatch_debug_introspection.rs:3-4` — a real gain, and one that catches a mutating probe
at compile time. What is also true: a future author can widen `gather_probe` to `&mut self` in
a one-line edit that compiles cleanly and trips no CI gate, where today the equivalent move
would bump the pinned count. **Net: one compile-time invariant gained, one CI forcing function
lost.**

**Proposed compensation (in scope, ~20 lines of Python).** Extend
`continuation-lock-gate.py`'s pin table with a variant-count entry for `ShardProbe` parsed out
of `probe.rs`, alongside the existing `message.rs` enum parsing it already does (`:160`,
`:176-193`). A sixth probe then bumps that integer, restoring the human-decision checkpoint at
the new chokepoint instead of the old one. This costs the eighth after-state boilerplate site
counted in §D. **If the compensating pin is rejected, the honest accounting is 7 sites and one
fewer CI gate** — the proposal does not claim both.

**Stale prose to fix while there (drive-by).** `continuation-lock-gate.py:18` and `:20` say
*"64 arms across 11 `*Msg` enums"*. The pinned counts already sum to **65**
(4+11+3+8+2+18+5+3+6+3+2). After 87 they sum to **61**. Both numbers should be corrected in the
same commit that edits `:89`.

## Reply-shape compatibility

The brief asked what pins the probe reply shapes. Verified answer:

- **`frogctl` pins nothing.** Full grep of `frogctl/src`: the only `DEBUG` use anywhere is
  `DEBUG HASHING` (`commands/data.rs:51`, with a graceful `DEBUG HASHING not available`
  fallback). No probe is parsed by the CLI.
- **`frogdb-debug` (bundles, web UI) pins nothing.** Grep for the probe names across
  `crates/debug`: zero hits. `DiagnosticCollector` gathers via its own shard-sender path.
- **The tier-4 test adapter is the sole machine pin, and it is thorough.**
  `quiescence_probe.rs:157-320` hand-parses all five replies including nested structure
  (`parse_waiters :192`, `parse_log_entries :273`) and the RESP2-flattened map form
  (`:5-8`). It reads these exact field names: `intents`, `continuation_lock`,
  `total_waiters`, `keys`, `waiters`, `conn_id`, `op`, `registration_seq`, `truncated`,
  `registrations`, `key`, `tracked_bytes`, `recomputed_bytes`, `anomalies`. And these exact
  sentinel strings must keep parsing to zero snapshots: `# lock table is empty`,
  `# wait queue is empty`, `# expiry index is consistent`.
- **`integration_debug_introspection.rs` pins the sentinel's RESP *type*, not just its bytes.**
  `:16` and `:57` destructure `Response::Bulk(Some(b))`; `:64-84` polls `DEBUG WAITQUEUE`
  until it sees a `Response::Array`, using *"an Array reply means seen"* as its whole
  heuristic (`:77`). `:106-127` walks the RESP2-flattened map for MEMORY-CHECK. **A clean-path
  reply that stopped being a bulk string would break `:64-84` silently** — it would loop 50
  times and fail on the `assert!(seen, …)`, with a message that names the wrong cause.

Because the five formatters move without modification, **every one of these pins is preserved
by construction, and H1 changes no wire byte at all** (§Hotfix candidates). Rev 1's claim that
H1 adds "a new key to a RESP map" was **wrong on the facts**: in the false-clean case the reply
is not a map, it is `Response::Bulk(Some("# lock table is empty"))`, and there is no key to add.
That mechanism is withdrawn; see H1's rejected-alternative note.

## Aggregation path (verified)

The brief asked whether `conn_command.rs` fans out. It does not.

- `frogdb-core/src/conn_command.rs` only **declares** `-> BoxFuture<Vec<XInfo>>`; `frogdb-core`
  has no shard senders to fan out with.
- The fan-out lives in `frogdb-server`: `debug_handler.rs:100-155` → `ConnectionHandler::scatter_gather()`
  (`connection/scatter.rs:25-31`) → `ScatterGather::gather_all`
  (`frogdb-server/crates/server/src/scatter/broadcast.rs:285-330`).
- **Aggregation is concatenation, not merge.** Each reply struct carries its own `shard_id`
  (`types.rs:1037`, `:1048`, `:1087`, `:1112`, `:1123`) and each formatter emits a
  `shard:<id>` map key. Nothing is summed or merged — unlike `keysizes_snapshot`
  (`debug_handler.rs:331-345`, `merged.merge(&snap)`) and `allocsize_in_slot` (`:348-361`,
  `.sum()`), which is a further reason those two stay out of the probe table.
- The gather is bounded by `self.scatter_gather_timeout`, an operator-visible live-mutable
  param (`config/src/param_id.rs:98` `scatter-gather-timeout-ms`, default 5000 —
  `runtime_config.rs:2038-2040`, and `acceptor.rs:420` for the test-server default). Every
  probe honours it. **`DEBUG PUBSUB LIMITS` does not** — H2.

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
| **85** (`frogdb-macros` fate, PN7+PN11+CT3) | `message.rs`, `scripts/continuation-lock-gate.py` | **Rev 1 claimed "zero overlapping lines". That was false and is withdrawn.** 85's PN11 owns `message.rs:1028-1179` (the 11 category `probe_type_str` impls, 65 arms — `85:22`, `85:115`). `impl DebugIntrospectionMsg::probe_type_str` sits at **`:1135-1148`, inside that range**, and its five arms `:1139-1144` name the exact variants 87 deletes — so **87 must edit 85's region or the tree stops compiling**. Rev 1's own §85-edge row already conceded this (*"must match on the inner `ShardProbe`"*) while the §Files table said 87 does not touch it; that internal contradiction is resolved here in favour of the compiler. **Proposed order: 87 before 85.** 87-first means 85's author finds `DebugIntrospectionMsg` already non-derivable (one variant, five strings selected by an inner enum) and simply excludes it from the derive set — exactly the carve-out 85 already documents for `DriveTick` (`85:241`), leaving 85 with 10 derivable enums instead of 11. 85-first means 87 must *delete* a derive 85 just added. Neither order is blocked; 87-first wastes no work. **Second edge:** 85 lists `continuation-lock-gate.py:80-92` as **read-only, "must stay green"** (`85:137`); 87 writes one integer at `:89` inside that range and optionally adds a `ShardProbe` entry. No textual conflict (different lines than 85 reads for its own clearance), but whoever lands second re-runs `just lint-gates` — declared, not assumed. |
| **82** (`pubsub-channel-table`) | `message.rs` | 82 owns `PubSubMsg` `:275-372` and `PubSubMsg::probe_type_str` `:1040-1057` (`82:75`). 87 owns `:810-855` and `:1135-1148`. **Disjoint in both regions.** Both proposals are bound by the same byte-stability contract at `:1003-1005`, and both resolve it the same way (inner-enum match); 82 cites the `DriveTick` idiom at `82:470` as 87 does. **Any order.** |
| **81** (`core-dead-seams`, PN2+PN3) | `event_loop.rs`, `diagnostics.rs`, `harness.rs` | **Correction to the brief: 81 does *not* touch `message.rs`** — grep of `81-core-dead-seams.md` returns zero `message.rs` hits, and its files-involved table names none. **Also: `f73bdd8f` committed the proposal *document*, not the code** — 81 is unlanded, so this is a planning-order edge, not a rebase edge. Overlaps are three files, all disjoint: `event_loop.rs` (81 owns `select!` arm 6 at `:119-124`; 87 reads the dispatch-fn match arm `:391-394` and edits neither), `diagnostics.rs` (81 owns dummy-channel ceremony `:502`, `:508`; 87 owns `:207-299`), `harness.rs` (81 owns `:53`, `:80-81`, `:84`, `:94`; 87 owns `:294-358`). **Order: either; no textual conflict.** Prefer 81 first only because it is S and shrinks `harness.rs`'s constructor before 87 edits its driver block. |
| **84** (`blocking-op-dedupe`) | possibly `message.rs`, `wait_queue.rs`, `blocking.rs` | **Unverified edge, flagged as such.** If 84 touches `BlockingMsg` in `message.rs`, it is a *different enum* at a different offset, and the continuation-lock gate pins `BlockingMsg` separately (`continuation-lock-gate.py:85`, count 2) — so even a simultaneous pin edit is two different dictionary entries. The one place a real conflict could arise: 84 may alter `ShardWaitQueue`'s registration journal, which `collect_wait_queue_log` (`diagnostics.rs:265-281`) reads. **87 does not modify that collector**, so a change under it is absorbed. **Declared unverified; re-check before either lands.** 81's PN3 also rewrites `wait_queue.rs` — 84 and 81 should settle that between themselves; 87 is downstream of both and neutral. |
| **80** (`response-wire-fold`) and **86** (`resp3-egress-codec`) | `protocol/src/response.rs` — **the encoders 87's pins ride on** | **Declared, and benign in fact.** 87 changes no formatter and no encoder, but every pin in §Reply-shape compatibility is a statement about how `Response::Bulk` / `Response::Map` render, which both siblings own regions of. **80** owns `Response` `:647-743`, `InternalAction` `:40-73`, `WireResult` `:748` and the `into_wire`/`from_wire` recursion, and **explicitly disclaims the encoders**: *"80 preserves all 16 wire variants and does not touch `to_resp2_frame`"* (`80:617-618`). **86** owns `to_resp2_frame` `:274-334` and `to_resp3_frame` `:341-432` but only as **doc edits** — *"Neither encoder body is touched"* (`86:87`). So the RESP2 map-flattening that `quiescence_probe.rs:5-8` and `integration_debug_introspection.rs:106-127` depend on is untouched by all three. **No ordering constraint; declared so a reviewer does not have to re-derive it.** |
| **74** (`debug-bundle-assembler`) | `debug_handler.rs` | 74 owns `:222-277` (`bundle_generate`, `bundle_list`); 87 owns `:100-155` (+H2's `:178` is claimed by **67**, not by either). **Disjoint hunks, any order.** |
| **79** (`debug-webui-router`) | none | 79 is confined to `crates/debug/web_ui/**` + `observability_server.rs`. No probe surface. **No edge.** |
| **67** (`server-small-dedups`) | `debug_handler.rs:178` | **67 already owns and has filed H2.** Its §"Out of scope, but file an issue" names `:173`/`:178`, the hardcoded `from_secs(5)`, the `scatter_gather_timeout` divergence, and both bespoke error strings, and rules it out of SV7 deliberately. **87 does not claim it.** |

**Behaviour changes: none.** Every probe's existing keys, ordering, sentinel strings and
RESP2/RESP3 rendering are byte-identical, enforced by the pins in §Reply-shape compatibility.
H1 is confined to test code. Rev 1 listed "exactly one, and it is additive" — withdrawn along
with the mechanism that would have caused it.

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
| **H1 — coverage signal** (independently landable, **first**) | **S** | **Test-only, zero production bytes.** `QuiescenceSnapshots.shards_expected` + one `QuiescenceViolation` variant + one coverage check in `check_quiescence` + `SimConfig::num_shards` threaded through one call site + two forcing tests. Touches exactly three files: `quiescence_probe.rs`, `workload_runner.rs`, `testing/src/quiescence.rs` (plus `sim_harness.rs` read-only). **Not** `debug_handler.rs`, **not** `debug_conn_command.rs`, **not** `core/conn_command.rs` — rev 1 listed those and was wrong. Shares **no file** with the refactor, so the two are genuinely independent; H1 lands first so the refactor's acceptance run has a sound oracle. |
| **The probe table** | **M** | New 130-line module; 6→2 message enum; `probe_type_str` inner match; 5→1 in four places (trait, both stubs, handler); routing + formatter dispatcher; four harness wrappers; one lint pin (+ optional compensating pin). ~13 files, net line change ≈ 0. Mechanical, but wide. **Rated M with an honest halving (13→7 boilerplate sites, 14→10 files), and with the CI-gate debit in §Seam-lint clearance priced in.** The case rests on locality (six enumerations, two already drifted → two compiler-checked matches) and the `&self` type property, not on the site count alone. |
| **H3 — stale doc comment** | **XS** | `message.rs:810-811`. Free inside the refactor; also landable alone. |
| **Mutation re-gate** | **none** | No locked crate. |
| **Docs** | **XS** | `debugging.md:34-48` needs no content change (subcommand names are unchanged); splitting the lumped `:45` row is optional and independently worthwhile. |

**Recommended sequence:** H1 (S, no shared files) → probe table (M, before 85). H3 rides
either.

## Hotfix candidates

| ID | Classification | Claimed? | Detail |
|---|---|---|---|
| **H1** | **LIVE** — test-oracle soundness hole (no production data path, **no firing instance claimed**) | **CLAIMED** | Four tier-4 quiescence checkers report *clean* on a partial or empty probe gather. Three compounding causes, each verified: `gather_all` breaks on first timeout and drops the shard suffix (`scatter/broadcast.rs:325`) with no coverage record; the sentinel folds are vacuously true on an empty `Vec` (`debug_conn_command.rs:602`, `:657`, `:786`) and render `# lock table is empty` for a gather that heard from nobody; all four checkers `Ok(())` on an empty slice (`testing/src/quiescence.rs:81-136`). **The oracle cannot distinguish clean from unheard**, so its green carries no coverage information — see §Problem 3 for why no trigger is claimed and why that is sufficient on the `64d03cab` precedent. **Mechanism (rev 2, wire-free):** `MEMORY-CHECK` has **no sentinel** — `format_memory_check_response` (`:755-781`) always emits one `shard:<id>` entry per survivor, as `quiescence_probe.rs:12` already documents — and it is gathered **adjacently, over the same sender set and the same deadline** as the other three (`workload_runner.rs:268-271`). So `snap.memory.len()` is a faithful fleet-coverage proxy for the whole bundle. Add `shards_expected` to `QuiescenceSnapshots`, sourced from `SimConfig::num_shards` (`sim_harness.rs:39`, default 4 at `:56`), and have `check_quiescence` (`quiescence_probe.rs:75-92`) report `CoverageIncomplete { heard: memory.len(), expected }` before running the four checkers. **Zero production bytes change; all five reply-shape pins and both integration-test RESP-type assumptions hold untouched.** **Rejected alternative (rev 1's mechanism):** make the sentinel carry `n/m`. That changes the clean-path reply's *type*, breaking `integration_debug_introspection.rs:16` and `:57` (which destructure `Response::Bulk`) and, silently, the `:64-84` "Array reply means seen" heuristic — and would drag the tier-4 parsers and `debugging.md` with it. It contradicts "every pin preserved by construction" and is not worth the operator-visible byte change for a defect whose only demonstrated consumer is the test oracle. **Landable ahead of, and independently of, the refactor.** |
| **H1b** | **LATENT** — operator-facing, deferred | **NOT claimed** | The same blindness exists for a human typing `DEBUG LOCKTABLE`: a timed-out gather prints `# lock table is empty`. If it is ever fixed, the fix is **formatter-local and needs no `ProbeGather`**: `shard_count = ctx.shard_senders.len()` is already bound at `debug_conn_command.rs:112` and in scope at the routing arms `:153-161`, so `probe_reply` can compare it to `infos.len()` and emit a distinct string. Deferred because it *does* change the clean-path bytes and therefore owns the pin updates H1 deliberately avoids. Recorded so a later author does not reach for a plumbing change. |
| **H2** | **LIVE** — operator-visible config divergence | **NOT claimed** | `debug_handler.rs:178` waits on `DEBUG PUBSUB LIMITS`'s shard-0 round-trip with a hardcoded `Duration::from_secs(5)` instead of `self.scatter_gather_timeout`, ignoring the live-mutable `scatter-gather-timeout-ms` param (`config/src/param_id.rs:98`, `runtime_config.rs:2038-2040`). **Already owned, argued and filed by proposal 67** (`67:687`, `67:724`: §"Out of scope, but file an issue", and its effort table's "Issue to file" row). 87 cites it only to record that after the probe table the fix is a two-line adapter onto `ScatterGather::query_one`. |
| **H3** | **LATENT** — documentation drift | **CLAIMED (drive-by)** | `message.rs:810-811` documents `DebugIntrospectionMsg` as *"(LOCKTABLE / WAITQUEUE / MEMORY-CHECK / EXPIRY-INDEX-CHECK)"* — **4 of its 6 variants**; `WAITQUEUE-LOG` and `EXPIRE-BACKDATE` are missing. Evidence for §Problem 2 and a one-line fix. |

**Security findings: none.** No probe accepts attacker-controlled input beyond
`EXPIRE-BACKDATE`'s key (already length-bounded by the RESP parser and out of scope here); no
probe reply echoes unsanitised client text; every probe is behind `DEBUG`, and the one gated
subcommand (`SLEEP`) is unrelated. Standing policy noted: security findings would be
**classification-only, filed and parked, never a fix proposal**.

## Revision-2 digest

| # | Rev 1 claim | Rev 2 disposition | Evidence |
|---|---|---|---|
| B1 | H1 adds "a key to a RESP map" | **Refuted and replaced.** The false-clean reply is `Response::Bulk`, not a map. H1 is now wire-free, using MEMORY-CHECK's arity as the coverage proxy. | `debug_conn_command.rs:609`, `:658`, `:787`; `integration_debug_introspection.rs:16`, `:57`, `:77`; `format_memory_check_response :755-781` has no sentinel; `quiescence_probe.rs:12`; `workload_runner.rs:270`; `sim_harness.rs:39`, `:56` |
| B2 | "Zero overlapping lines with 85" | **Refuted.** 87 must edit `probe_type_str`; §B now shows the replacement. Ordering proposed: 87 before 85. | `message.rs:1139-1144` names all five deleted variants; 85 owns `:1028-1179` (`85:115`) |
| B3 | Boilerplate 12→4, total 19→10 | **Corrected to 13→7, 20→14, files 14→10.** Three survivors identified (`probe_type_str`, `shard/mod.rs`, and — beyond the review's finding — `workload_runner`'s RESP-level gather call, which no internal refactor can touch). | `message.rs:1003-1005`; `shard/mod.rs:87-94` vs formatter signatures `:602`/`:656`/`:717`/`:755`/`:783`; `workload_runner.rs:268-271` |
| B4 | 19 sites / 13 files / 12 boilerplate; site #15 universal | **Corrected to 20 / 14 / 13.** Row 6b numbered. Harness driver marked 4-of-5 (`WAITQUEUE-LOG` has none) and the "five wrappers" contradiction fixed to four. The `0a8b5c11` narrative now states which four sites postdate it. | `harness.rs:296`, `:306`, `:316`, `:331`, `:350` — no `wait_queue_log` driver |
| B5 | H1 file list included `debug_handler.rs` etc. | **Rewritten.** H1 touches three test files; `ProbeGather` withdrawn from §C entirely. The formatter-local production variant is recorded as **H1b, not claimed**. | `debug_conn_command.rs:112` in scope at `:153-161` |
| B6 | "exactly the load the concurrency suite generates" | **Withdrawn.** Restated as a soundness hole with no firing instance claimed; sufficiency argued from the `64d03cab` precedent. | `workload_runner.rs:265-267` (post-drain); `Cargo.toml:69` (turmoil virtual clock); `acceptor.rs:420` (5000 ms over in-process mpsc) |
| B7 | `&self` is a "stronger" forcing function than the count pin | **Withdrawn.** Net: one compile-time invariant gained, one CI gate lost. A compensating `ShardProbe` count pin is proposed in scope; if rejected, the debit stands as stated. | `continuation-lock-gate.py:15-33` (count pin is the whole forcing function, runs per-commit); collectors already `&self` at `diagnostics.rs:207`/`:234`/`:265`/`:284`/`:293` |
| N1 | Verified at `ddc4b184` | Restated: `ddc4b184` → HEAD `4421aec7` moved **only** `.scratch` proposal files, so all cites hold. | `git diff --stat ddc4b184..4421aec7` |
| N2 | `ExpiryIndexCheckInfo.shard_id :1122` | Corrected to **`:1123`**. Also corrected: all five `diagnostics.rs` collector end lines. | `types.rs:1123`; `diagnostics.rs:207-231`, `:234-257`, `:265-281`, `:284-290`, `:293-299` |
| N3 | "5 tests, one per probe" | Corrected to **7 tests covering 4 of 5**; the `WAITQUEUE-LOG` gap explained by the `wait-queue-log` feature. | `integration_debug_introspection.rs:8`/`:25`/`:35`/`:52`/`:64`/`:95`/`:133`; `wait_queue.rs:620-624` |
| N4 | 80/86 absent from the edge table | Added, with both siblings' own encoder disclaimers quoted. | `80:617-618`; `86:87` |
| N5 | `event_loop.rs:119-124` "a different `select!`" | Corrected: same `select!` (arm 6 vs arm 7); `:391-394` is a match arm in the dispatch fn, not a `select!` arm. | `event_loop.rs:119-124`, `:391-394` |
| N6 | Gate prose "64 arms" | Flagged already stale (pins sum to **65**; **61** after 87), with a drive-by fix scheduled in the same commit. | `continuation-lock-gate.py:18`; `85:30` independently derives 65 |
| N7 | `collect_wait_queue_log :265-283`, `collect_memory_check :284-292` | Corrected to `:265-281` and `:284-290`. | `diagnostics.rs` |
| N8 | `scatter/broadcast.rs:285-329` | Corrected to `frogdb-server/crates/server/src/scatter/broadcast.rs:285-330`. | `broadcast.rs:329-330` |

**Hotfix rulings after revision:** **H1 UPHELD, amended** (wire-free mechanism, restated
classification, corrected file list) — still **S**, still lands first, and now shares no file
with the refactor. **H2 CONFIRMED owned by proposal 67** (`67:687`, `67:724`) — cited, not
claimed. **H3 CONFIRMED** — `message.rs:810-811` names 4 of 6 variants. **H1b** added as a
recorded, unclaimed deferral.

## References

- Ground-truth "add one probe" diff: `0a8b5c11` (`feat(server): DEBUG EXPIRY-INDEX-CHECK
  introspection command`, 10 files, +178/−7) — the historical floor, not today's site list.
- Coverage-verdict precedent: `64d03cab` (`test(concurrency): repair three checker soundness
  defects`), concurrency issue 16 — *"the checker reports FifoCoverage instead of ever
  proxying — incomplete ordinals mean 'proves nothing', not a verdict."*
- H2's owner: `.scratch/arch-deepening/proposals/67-server-small-dedups.md`,
  §"Out of scope, but file an issue: the sixth shard-0 send" (`67:687`, `67:724`).
- Continuation-lock gate design rationale (why a count pin, and what 87 spends):
  `scripts/continuation-lock-gate.py:15-33`.
- USDT byte-stability contract both 82, 85 and 87 must preserve: `message.rs:1003-1005`.
- Boundary ADRs for locked areas (none apply): `adr/0002`–`adr/0004`.
