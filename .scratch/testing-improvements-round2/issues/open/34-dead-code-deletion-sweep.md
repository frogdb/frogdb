# Sweep: 10 dead-code items found by the audit — delete, or wire up the five the owning proposals want kept

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §5
Score: aggregate of 10 findings
Area: frogdb-server (connection, cluster) · frogdb-commands · frogdb-types · frogdb-protocol · frogdb-config · frogdb-scripting · frogdb-replication · frogdb-core (persistence)

## Context

Fifteen area audits each ran `rg` for callers of functions their coverage data reported `untested`.
Ten came back with **zero production call sites** — confirmed by grep, not inferred from a coverage
class. Dead code is a testing problem, not tidiness: it inflates the untested-function count, it is
where a maintainer's fix lands with no effect (`scan.rs` already diverges from live `scatter.rs`), and
`CrashTestHarness`'s verify API advertises exactly the capability the persistence audit's biggest gap
says is missing, so the next author stops looking. **MASTER.md §5 titles this list "delete, do not
test"; for five of the ten the owning proposal recommends the opposite** — wire it up, or export it.
Marked **contested** below; resolve item by item, not globally in either direction.

## Evidence

- **03 (Deprioritised)** — `server/src/connection/builder.rs`, 0/175 lines,
  `ConnectionHandlerBuilder::build` `untested`, 21 regions; `rg` finds only the definition and the
  `connection.rs` re-export. Duplicates `ConnectionHandler::from_deps`'s wiring, will silently drift.
  *"delete it, do not test it."*
- **13/F15** — `core/src/persistence/test_harness.rs`: 13 fns `untested`, zero call sites anywhere —
  `corrupt_file:258` (16 regions), `append_garbage:267`, `find_files_with_extension:280` (25 regions),
  `find_wal_files:296`, `find_sst_files:301`, `simulate_crash:190`, `create_wal_writer:120`,
  `count_keys:225`, `total_key_count:233`, `verify_store_contains:459`,
  `verify_expiry_index_contains:489`, `verify_sorted_set:501`. Sev 4, pri 16. **"use them"**.
- **06/F20** — `commands/src/scan.rs` is a dead second SCAN/KEYS impl: `ScanCommand::execute` (63
  regions), `KeysCommand::execute` (15), `parse_key_type` (15), all 0-covered. Dispatch
  (`connection/dispatch.rs:229-230`) routes `ServerWideOp::Scan`/`::Keys` to
  `connection/scatter.rs:43`/`:197`, which hand-roll a second parser (`:68-118`, duplicate `KeyType`
  match `:100-113`). **Already divergent**: `scan.rs:88` returns `"unknown type: X"` vs
  `scatter.rs:107`'s `"ERR unknown type: X"`; `scan.rs:84` uses `ArgParser::try_flag_usize` for COUNT,
  `scatter.rs` hand-parses → `"ERR value is not an integer or out of range"`. Priority 11.
- **04 (Deprioritised)** — `SlotMigrationCoordinator::is_migrating` / `migration_for`
  (`slot_migration/mod.rs:92-99`), both `untested`; `rg` finds only their definitions. *"deletion, not a test."*
- **07 (geo)** — `types/src/geo.rs:335-352 geohash_range_for_bbox`, zero-exec, no callers (file 93.4%).
- **08 (Deprioritised)** — `Response::Attribute` / `WireResponse::Attribute` and `set_frame_attributes`
  (`protocol/src/response.rs:386`, 0/19 regions, 0 tests); **no producer anywhere** in the workspace.
  `redis-regression` already declares RESP3 attributes an `intentional-incompatibility:protocol`.
- **15/F15** — `ConfigParam::default`. `config/src/param.rs` documents it as "ideally the same fn
  serde's `#[serde(default)]` uses, so the file default and the CONFIG default cannot diverge", yet
  `(p.default)()` appears at one site repo-wide — `param.rs:260`, in the module's own test. Severity
  3, priority 13. **Proposal wants the invariant enforced, not the field deleted.**
- **09/F15** — `FunctionRegistry::set_running_function` (`scripting/src/registry.rs:174-176`), no
  callers; the only outside reference is the *reader* at `connection/scripting/function.rs:317`.
  **This is why `FUNCTION STATS` `running_script` is always null** — deleting the setter makes that
  null permanent. Priority 12.
- **14 (Deprioritised)** — `new_replication_id` (`replication/src/state.rs:248`), no production caller
  (`rg`: only `replay.rs:327`, `state.rs:434/440/469/472`, `offset_coordinator.rs:345`, two doc
  comments). It is the fn replid rotation on `REPLICAOF NO ONE` needs.
- **13/F14** — `PageCacheSink` unreachable because `WriteSink` is `pub(super)`; it is *"the only real
  fsync/power-loss model in the repo"*. **Proposal recommends exporting it** under the existing
  `test-support` feature, beside `sync_wal`/`commit_raw_batch`, already `#[cfg(any(test, feature =
  "test-support"))]` at `rocks/mod.rs:540+`.

## What to fix

Item by item; default is deletion. Where a checkbox is **contested**, the owning proposal's
alternative must be accepted or explicitly rejected in the commit message — neither delete nor keep
silently. After each deletion confirm no `#[allow(dead_code)]` or re-export was left behind.

## Acceptance criteria

- [ ] `server/src/connection/builder.rs` deleted along with its `connection.rs` re-export.
- [ ] **contested (13/F15)** — `CrashTestHarness`'s 13 dead fns *wired into real tests* per the
      proposal (`find_sst_files` + `corrupt_file` at an offset + reopen = the missing corrupted-SST
      test; `append_garbage` on a WAL = the missing truncated-tail test); only the residue deleted.
      Coordinate with issue 02, `.scratch/testing-improvements-round2/issues/`.
- [ ] `ScanCommand::execute` / `KeysCommand::execute` / `parse_key_type` removed from
      `commands/src/scan.rs`, and the `TYPE`/`COUNT`/`MATCH` negative matrix added against the live
      `scatter.rs` path.
- [ ] `SlotMigrationCoordinator::is_migrating` and `migration_for` (`slot_migration/mod.rs:92-99`).
- [ ] `types/src/geo.rs:335-352 geohash_range_for_bbox` deleted.
- [ ] `Response::Attribute`, `WireResponse::Attribute`, `set_frame_attributes`
      (`protocol/src/response.rs:386`) deleted; the `redis-regression` intentional-incompatibility
      entry updated.
- [ ] **contested (15/F15)** — `ConfigParam::default` *kept*, invariant wired: for every registry row
      backed by a config field, assert `render((param.default)()) ==
      render(get(&ConfigManager::from(Config::default())))`, with commented exemptions where it cannot
      hold. Delete only if rejected; same placement question as 15/F9.
- [ ] **contested (09/F15)** — `set_running_function` is *called* from the FCALL entry path so
      `FUNCTION STATS` reports a running function, rather than deleted. Pairs with 09/F8's fixture.
- [ ] **contested (14)** — `new_replication_id` **left in place**, blocked on
      `.scratch/replication-cluster-rework/promotion-replid-psync.md` §4.4–4.5 and §7, which specifies
      the replid-rotation test that will be its first caller. Re-evaluate only if that PRD is dropped.
- [ ] **contested (13/F14)** — `WriteSink` + `PageCacheSink` *exported* under the `test-support`
      feature rather than deleted, plus a `shard_driver` case: write in `Periodic` mode, drop the
      unflushed page-cache tail, reopen, assert every write below `durable_sequence()` survived and
      nothing above it did.
- [ ] `just check` and `just lint` clean after every deletion; no new `#[allow(dead_code)]` added.

## Test boundary

Deletions need no test. Retained-and-wired items land at **2** for `CrashTestHarness` (crate-level,
existing harness) and `ConfigParam::default` (registry invariant), **3** for `PageCacheSink`
(`shard_driver`), **4** for `set_running_function` (only meaningful during an in-flight FCALL).
06/F20's replacement matrix is **4** live scatter path, **1** cursor codec.

## Depends on

Issue 02, `.scratch/testing-improvements-round2/issues/`, for the `CrashTestHarness` checkbox — the
crash-primitive decision determines how much of that API survives. Issue 01, same directory, for the
`PageCacheSink` case. `new_replication_id` is blocked on
`.scratch/replication-cluster-rework/promotion-replid-psync.md`, outside this round; the six pure
deletions can start now.

## Re-triage 2026-08-06

**Verdict: partially-fixed**

**2 of 10 items resolved (both "contested" ones that wanted wiring, not deletion); 8 still have zero
production call sites.**

Resolved:

- **14 `new_replication_id` — RESOLVED as the checkbox wanted.** It now has two production callers:
  `replication/src/primary/mod.rs:385` (manual promotion, mints the replid under the state write
  lock) and `replication/src/primary/replay.rs:553`, landed by `f6484219` "manual promotion mints a
  replid and serves PSYNC". Stale ref: `replication/src/state.rs:248` → `:318`. It is no longer dead
  code and no longer blocked on the promotion-replid PRD.
- **13/F14 `PageCacheSink` / `WriteSink` — RESOLVED differently.** Both moved out of
  `core/src/persistence/rocks/` into the extracted `frogdb-persistence` crate: `WriteSink` is
  `pub(super)` at `persistence/src/wal/flush.rs:112`, and `PageCacheSink` now lives *inside the same
  crate's* test tree at `persistence/src/wal/tests.rs:1064`, driven by real fsync/power-loss tests
  (`:987-1120`). The "unreachable because `WriteSink` is `pub(super)`" complaint is void — the
  fsync/page-cache model is exercised. Not done: the `test-support` export and the `shard_driver`
  durable-sequence case in the checkbox; if that case is still wanted it needs the export, otherwise
  close the checkbox as satisfied in-crate.

Still valid, all confirmed by `rg` on today's tree:

- **03 `ConnectionHandlerBuilder`** — `server/src/connection/builder.rs:42`, still only the
  definition plus the `connection.rs:70` re-export.
- **13/F15 `CrashTestHarness`** — `core/src/persistence/test_harness.rs`, all 13 fns still have zero
  callers outside the file (the only repo-wide hits for `count_keys` are the unrelated
  `count_keys_in_slot`).
- **06/F20 `commands/src/scan.rs`** — `ScanCommand`/`KeysCommand`/`parse_key_type` still there,
  still registered (`commands/src/lib.rs:260-261`) while `dispatch.rs:229-230` → `:237-238` routes
  to `scatter.rs`. The divergence is unchanged: `scan.rs:160` `"unknown type: {}"` vs
  `scatter.rs:107` → `:109` `"ERR unknown type: {}"`.
- **04 `SlotMigrationCoordinator::is_migrating` / `migration_for`** — `slot_migration/mod.rs:92-99`
  → `:103`/`:108`, definitions only.
- **07 `geohash_range_for_bbox`** — `types/src/geo.rs:335`, still no *production* caller; the one
  reference is `testing/fuzz/fuzz_targets/geo_ops.rs:161`, which predates the audit (`75385c3b`,
  2026-03-31). Deleting it means deleting that fuzz arm too.
- **08 `Response::Attribute` / `WireResponse::Attribute` / `set_frame_attributes`** —
  `protocol/src/response.rs:386` → `:387` (`Response::Attribute` at `:641`,
  `WireResponse::Attribute` at `:159`). Still no producer anywhere; the only outside reference is a
  *consumer*, `core/src/scripting/bindings.rs:167`.
- **15/F15 `ConfigParam::default`** — `config/src/param.rs:99`; `(p.default)()` still appears at
  exactly one site repo-wide, `param.rs:260`, in the module's own test.
- **09/F15 `set_running_function`** — `scripting/src/registry.rs:174`, still no caller, so
  `FUNCTION STATS` `running_script` is still permanently null (reader moved
  `connection/scripting/function.rs:317` → `:225`).
