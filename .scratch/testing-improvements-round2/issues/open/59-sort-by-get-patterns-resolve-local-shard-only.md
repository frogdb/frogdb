# `SORT … BY`/`GET` pattern keys resolve only against the local shard — silent wrong ordering on a default 4-shard standalone

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/06 F4 · MASTER.md §3 (consistency violations), §7 (decision required)
Score: severity 3 · likelihood 5 · effort 2 · priority 17
Area: frogdb-commands / SORT

## Context

`SORT mylist BY weight_*` resolves its weight and `GET` pattern keys against the *local shard
only*. On a default 4-shard standalone the weights that live on other shards are invisible and
are silently substituted with `0.0` / `""`, so the command returns a wrongly ordered list with
no error — and with `STORE`, writes the wrong result durably. The BY/GET keys are also never
declared as dynamic keys, so neither the router, ACL, nor the CROSSSLOT check sees them.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly. MASTER.md §7 lists cross-shard `SORT` — fix, guard, or document — as a
decision that must be settled before the test can assert anything, hence `needs-triage`.

## Evidence

`commands/src/sort.rs:143 resolve_pattern` reads `ctx.store`, i.e. the local shard
only; `compute_sort_key` (`:209-248`) silently substitutes `Numeric(0.0)` / `Alpha("")` for a
key it cannot see, so the failure is a wrong answer rather than an error.
`SortCommand::dynamic_keys` (`:485`) and `dynamic_keys_with_flags` (`:500`, **untested**, 19
regions) declare only `args[0]` (R) and `sort_store_dest(args)` (OW) — never the BY/GET pattern
keys, so neither the router nor ACL nor CROSSSLOT sees them. `utils.rs:964 require_same_shard` is
applied by `sorted_set/set_ops.rs:170,340,560,663` and `sorted_set/pop.rs:198` but **not** by
SORT, and the rejection path itself is **untested** (0 regions covered).

Why the existing tests pass anyway: every `sort_tcl` weight test either hash-tags all keys
(`sort_tcl.rs:595 tcl_sort_by_external_key` uses `{t}`) or passes by hashing luck
(`:622 tcl_sort_by_external_key_with_limit` uses untagged `tosort`/`weight_*`).

## Options

Reproduced verbatim from proposals/06 F4:

- **(a)** Fix it (declare BY/GET keys in `dynamic_keys_with_flags`, fetch cross-shard) and test
  at boundary 3 with `ShardDriver::new(4)`. Correct, matches Redis-standalone semantics.
- **(b)** Apply `require_same_shard` to SORT, making cross-shard BY/GET an error, and test that
  it errors. Cheap and honest but breaks working single-shard-tested user code.
- **(c)** Declare it a documented incompatibility and pin only the current behaviour.
- **Recommendation: (a)**; (b) only if the cross-shard fetch is judged too invasive for now, and
  then it must be a documented incompatibility, not a silent wrong answer.

## Acceptance criteria

- [ ] A `core/tests/shard_driver/` scenario built with `ShardDriver::new(4)` seeds `tosort` on
      shard A and `weight_1..N` spread across all shards, and asserts `SORT tosort BY weight_*`
      returns the same order as the single-shard case. Fails today under option (a).
- [ ] `SORT tosort BY weight_* GET pat_* STORE dst` writes content identical to the
      single-shard case (option (a)) or errors (option (b)).
- [ ] Whichever option is chosen, the negative case is pinned explicitly — no test may pass by
      key-hash luck; the chosen keys are asserted to land on different shards.
- [ ] If (b) or (c) is chosen, the divergence is recorded as a documented incompatibility, not
      a silent wrong answer.

## Test boundary

Level 3 — cross-shard routing is exactly what `shard_driver`'s N-shard mode exists for. Not
level 4: a server integration test would need non-tagged keys and could still pass by luck,
which is precisely how the current `sort_tcl` tests fail to catch it.

## Depends on

issue 29 (decision D1 — home for command-semantics tests),
`.scratch/testing-improvements-round2/issues/`

## Re-triage 2026-08-06

**Verdict: still-valid**

Reproduces verbatim on today's tree; nothing in the hardening campaign touched `SORT` (commands is
not a locked crate and no FM row mentions it). Path updated: `commands/src/sort.rs` →
**`frogdb-server/crates/commands/src/sort.rs`**. `resolve_pattern` still reads only `ctx.store`
(`sort.rs:143`, and its `ctx.store.get(...)` lookups at `:169`, `:184`, `:198`);
`compute_sort_key` still silently substitutes `SortKey::Alpha(Bytes::new())` (`:242`) /
`SortKey::Numeric(0.0)` (`:244`) for an unresolvable key; `dynamic_keys` (`:485`) and `dynamic_keys_with_flags`
(`:500`) still declare only `args[0]` (R) and `sort_store_dest(args)` (OW) — no BY/GET pattern keys.
`require_same_shard` moved `utils.rs:964` → **`utils.rs:928`** and is still applied only by
`sorted_set/set_ops.rs:170,340,560,663` and `sorted_set/pop.rs:198`, never by SORT. The
options (a)/(b)/(c) decision is still unmade, so `needs-triage` stays correct.
