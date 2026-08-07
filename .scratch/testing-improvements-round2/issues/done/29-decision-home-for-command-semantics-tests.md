# Decision D1 — where command-semantics tests live

Status: done
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §7 D1 · proposals/06 F0 · proposals/07 cross-area note 1
Area: frogdb-commands · frogdb-core / `core/tests/shard_driver/`

## Context

This is the structural decision that gates how nearly every command-level finding in the audit gets
written, so it should be settled first. Two of the fifteen agents raised it independently, and area
07 asked explicitly that it "be decided once, globally".

The constraint is real and not a matter of taste: the `commands` crate **has no `tests/` directory
and genuinely cannot have one** — a `tests/` dir there needs a dev-dep on `frogdb-core`, which
compiles core twice and produces `E0308` (documented in `core/tests/shard_driver/harness.rs`).

Current state:

- `commands` has **22 inline `#[test]` functions across ~2.5k LOC** of audited area — 6 in
  `geo.rs`, 8 in `bloom.rs`, 8 in `cuckoo.rs`. All of `stream/`, `json/`, `vectorset/`,
  `event_sourcing/`, `bitmap.rs`, `timeseries.rs`, `hyperloglog.rs`, `cms.rs`, `topk.rs`,
  `tdigest.rs` have **zero** inline tests.
- Every inline test uses `Box::leak(HashMapStore::new())` with `num_shards = 1` hardcoded
  (`sort.rs:553+`), and `CommandContext::new(store, senders, 0, /*num_shards*/ 1, …)`.
- `core/tests/shard_driver/` already builds a real `ShardWorker` via `ShardWorkerBuilder` +
  `frogdb_commands::register_all` and exposes `execute`, `execute_conn`, `exec_transaction`,
  `watch_keys`, `block_wait`, `tick_expiry`, `tick_waiter_timeout`, `capture_keyspace`,
  `memory_check`, `expiry_index_check` — and is used by **zero** tests in either command area.
- Consequently ~95–100% of these commands' behavioural coverage comes from boundary 4
  (`redis-regression` and `server/tests/integration_*.rs`), through a real socket, connection, RESP
  codec and routing, which is why negative/error-path coverage is thin: every negative case costs a
  server boot.

## Options

**(a) Extend the inline `#[cfg(test)]` units with a leaked `HashMapStore`** — today's pattern.
*Consequence:* cheapest and fastest, no new files. But `num_shards` is hardcoded to 1 and there is
no effects pipeline, no WAL, no keyspace notifications and no waiter wake — so it **structurally
cannot express** 06/F1, F4, F6, F7, F8, F12, and it hides every cross-shard and every
effect-pipeline bug in this audit. Choosing it means those findings stay unwritable at any
affordable level.

**(b) A new `scenario_commands_*.rs` family under `core/tests/shard_driver/`** — e.g.
`scenario_commands_{expiry,zset,list,string,hash,blocking}.rs` and a
`commands_extended.rs` (or per-type module set) for the extended types. *Consequence:* real
registry, real `ShardWorker`, real store + effects + WAL + notify seams, real N-shard routing via
`ShardDriver::new(n)`, no socket. Covers every command finding in the audit except the pure
parsers. Costs one `tick_expiry`/`drain` call per scenario plus a small harness addition (RESP2
selection, since `ShardDriver::execute` hardcodes RESP3).

**(c) Keep pushing everything into `redis-regression`** (boundary 4). *Consequence:* best Redis
parity fidelity, but slow, its error assertions are prefix-only
(`assert_error_prefix(.., "ERR")`), its keys are hash-tagged (`{t}`) so cross-shard behaviour is
invisible, and it cannot express cross-shard state or effect-pipeline state at all. It also drops
86 upstream test bodies in the core-types area alone on a `- $encoding` name-suffix rule.

## Consequences of not deciding

The registry-consistency module (issue 19, `.scratch/testing-improvements-round2/issues/`) has no
home — `INFRASTRUCTURE.md` records that it needs no harness, only a location, and names this
decision as the blocker. Issues 22 and 24, same directory, are both written against boundary 3 and
would have to be rewritten if (a) or (c) is chosen.

## Recommendation

**Both command agents recommend the `shard_driver` family — option (b).**

- Area 06 (F0): *"(b) as the default home, with (a) retained **only** for pure functions
  (`utils.rs` parsers, `format_float`, `simple_glob_match`, cursor codec). Do not add new inline
  tests that assert store state."*
- Area 07 (cross-area note 1): recommends `core/tests/shard_driver/commands_extended.rs` (or a
  per-type module set) as the default home for command-semantics tests, and that **"needs a
  `TestServer`" be treated as requiring justification**.

## Depends on

Issue 01, `.scratch/testing-improvements-round2/issues/` (`shard_driver` harness extension)
delivers the `ProtocolVersion` parameter and the blocking-command entry wrapper that option (b)
needs — measured at 1–2 days, since every builder option it requires already exists on
`ShardWorkerBuilder` and is simply not forwarded. It does not have to land before the decision, but
option (b) is not fully usable without it.

## Re-triage 2026-08-06

**Verdict: superseded**

Answered by events in favour of option (b). Commit `7dd3cf65` (2026-07-31) moved the harness out of
`frogdb-core/tests/shard_driver/` into its own workspace member, `frogdb-shard-harness`
(`frogdb-server/crates/shard-harness/{src,tests}/`) — which dissolves the `E0308` dev-dep-cycle
constraint this issue rests on: the harness crate can dev-dep both `frogdb-core` and
`frogdb-commands` normally and has an ordinary `tests/` dir. Every round-2 command-semantics finding
closed since then landed its test there as a new file, not inline and not in `redis-regression`:
`tests/eviction_spill_failure.rs` (issue 41), `tests/rendering_incrbyfloat.rs` (issue 55),
`tests/script_timeout_effects.rs` (issue 60), all from `0d727d05` / `4e96e4d5`. Stale refs:
`core/tests/shard_driver/*` → `frogdb-server/crates/shard-harness/tests/*`, harness →
`shard-harness/src/harness.rs`; `frogdb-commands` still has no `tests/` dir and gained no new
store-asserting inline tests. Residual, owned by issue 01 not by this decision: the harness still
hardcodes `ProtocolVersion::Resp3` (`shard-harness/src/harness.rs:122,144,198,222`), so the RESP2
selection option (b) wants does not exist yet.
