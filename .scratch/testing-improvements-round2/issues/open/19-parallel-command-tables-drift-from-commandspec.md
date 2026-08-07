# Three hand-maintained command tables silently disagree with `CommandSpec` — no registry-consistency test exists

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §2 T1
Score: aggregate of 3 findings
Area: frogdb-core / registry · frogdb-acl · frogdb-core scripting

## Context

Three area audits independently found the same shape: a second, hand-written copy of command
metadata that has silently drifted from the registry. This is **one piece of work, not three
instance fixes** — the deliverable is a single registry-consistency test module that iterates the
real registry and asserts every parallel table agrees with it, so the *next* instance is caught
too. `INFRASTRUCTURE.md` records that this needs no harness: `CommandRegistry::iter()`
(`core/src/registry.rs:256`) already provides the iteration. What it needs is a home — see
decision D1.

Two of the three instances are live security/consistency defects, filed separately as their own
issues; this issue owns the invariant, not the individual repairs.

## Evidence

- **ACL categories — 185 of 356 registered commands have no `COMMAND_ALL_CATEGORIES` row.**
  *(15/F1)* `acl/src/categories/mod.rs:149-153` — `all_for_command` is
  `COMMAND_ALL_CATEGORIES.get(cmd).cloned().unwrap_or_default()`, i.e. an **empty vec on miss**,
  and `acl/src/permissions.rs:215-268` reads *only* `all_for_command`, so an empty vec makes every
  `-@category` rule a no-op. Missing: all of JSON.\*, FT.\*, TS.\*, TOPK.\*, TDIGEST.\*, BF.\*,
  CF.\*, CMS.\*, V\*, ES.\*, plus `migrate`, `lcs`, `substr`, `rpoplpush`, `zintercard`,
  `zremrangeby{lex,rank,score}`, the HEXPIRE/HTTL family, `hgetdel`, `hgetex`, `hsetex`, `msetex`,
  `delex`, `xackdel`, `xdelex`, `georadius*_ro`, `lolwut`, `psync`, `replconf`, `frogdb.*`. A
  further **20 commands sit in the primary `COMMAND_CATEGORIES` table but are absent from
  `COMMAND_ALL_CATEGORIES`** — `monitor`, `cluster`, `failover`, `latency`, `wait`, `waitaof`,
  `function`, `fcall`, `fcall_ro`, `eval_ro`, `evalsha_ro`, `pfdebug`, `pfselftest`, `bzmpop`,
  `zmpop`, `zdiff`, `zdiffstore`, `zinter`, `zunion`, `zrangestore`. Existing tests are 6
  spot-checks plus `assert_eq!(all.len(), 21)` on the *enum* (`categories/mod.rs:250`);
  `COMMAND_CATEGORIES::{closure#0}` (`acl/src/categories/data.rs:10-293`, 276 regions) is
  `untested`, 0 regions covered, 0 tests.
- **Scripting write-flags — `is_write_command` omits whole type families.** *(09/F1)*
  `core/src/scripting/bindings.rs:44-77` enumerates ~70 write commands by name; missing at minimum
  `SETBIT`, `BITFIELD`, `BITOP`, `PFADD`, `SORT … STORE`, `XSETID`, `GEORADIUS[BYMEMBER] … STORE`,
  `HEXPIRE`/`HPEXPIRE`/`HPERSIST`, and every `JSON.*`, `TS.*`, `BF.*`, `CF.*`, `CMS.*`, `TOPK.*`,
  `TDIGEST.*` write. It is the *only* read-only rejection at `core/src/scripting/gate.rs:232-236`
  — while the same file's replication path already uses the authoritative registry flag
  (`gate.rs:469-476`, `handler.flags().contains(CommandFlags::WRITE)`). Two sources of truth,
  known to be different.
- **WAL declared keys — nothing proves a handler's mutations are confined to its declared key
  set.** *(01/F4)* `core/src/shard/rollback.rs:31-70` derives the rollback snapshot from
  `handler.wal_strategy().actions(args)`; a key mutated but not declared is neither rolled back
  nor written to the WAL. `core/src/registry.rs:180-216`'s `debug_assert!(spec.validate().is_ok())`
  checks only declarative consistency; `core/src/command_spec.rs:1026-1160` — every `validate()`
  test builds a hand-made spec, **none iterates the real registry**.

## What to fix

1. Add one registry-consistency test module that builds a `CommandRegistry`, runs
   `frogdb_commands::register_all`, and iterates `registry.iter()`.
2. Assert, per registered spec: `CommandCategory::all_for_command(name)` is non-empty and
   consistent with the spec's flags (`WRITE ⇒ Write`, `!WRITE ⇒ Read` for keyed commands,
   `ADMIN ⇒ Admin`), and `for_command`'s primary category is a member of `all_for_command`.
3. Assert `is_write_command(name) == registry.get(name).flags().contains(CommandFlags::WRITE)`,
   with an explicit allow-list for the deliberate extras (`PUBLISH`/`SPUBLISH`/`PFCOUNT`/`PFMERGE`,
   marked write for replication safety).
4. Add the WRITE-handler key-containment property (`touched_keys ⊆ wal_strategy().actions(args)`,
   modulo the documented `ClearShard` exemption) over a recording store decorator; precedent is
   `BatchSpyStore` in `core/src/shard/active_expiry.rs:527` and
   `core/tests/shard_driver/generator.rs`. The *enforcement policy* for a violation is a semantics
   call — see issue 30.
5. Every escape hatch is an explicit `const … EXEMPT: &[&str]` with a comment, so adding a real
   command forces a deliberate edit rather than passing by default.

## Acceptance criteria

- [ ] A single test module iterates `CommandRegistry::iter()` (`core/src/registry.rs:256`) and
      hosts all three assertions; adding a fourth parallel table is a one-function change.
- [ ] The ACL assertion **fails today** and names every offending command in its failure message.
- [ ] The `is_write_command` assertion **fails today** and names every offending command.
- [ ] Targeted behavioural pair: `+@all -@admin ⇒ MONITOR denied`, `+@all -@write ⇒ JSON.SET denied`.
- [ ] Behavioural pin: `EVAL_RO "redis.call('JSON.SET', …)"` returns
      `ERR Write commands are not allowed from read-only scripts`.
- [ ] Exemption lists exist, are non-empty only for internal/test stubs (`test`, `teststub`,
      `walmock`, `__seam*`), and each entry carries a comment.

## Test boundary

**Level 2** — a crate-level API test in `commands`, the lowest crate that can see both the registry
and (via `core`→`acl`) the tables; `acl` itself depends only on `types` and cannot. No shard and no
socket are needed to prove the predicate. The WAL key-containment property is **level 3**
(`shard_driver`) because it needs real dispatch against an observable store. The production
*consequences* (replica divergence from an `_RO` script, ACL bypass over RESP) are separate
behavioural tests at level 4/5 and belong with their own defect issues.

## Depends on

Decision D1 (issue 29, `.scratch/testing-improvements-round2/issues/`) settles where this module
lives. Criterion 4's enforcement policy depends on the semantics call tracked in issue 30,
`.scratch/testing-improvements-round2/issues/`. No infrastructure item is required.

## Re-triage 2026-08-06

**Verdict: still-valid**

All three findings reproduce verbatim on today's tree, and no registry-consistency test module
exists (`rg register_all` finds only `commands/src/lib.rs`, `server/src/server/register.rs` and
`shard-harness`; no test iterates `CommandRegistry::iter()`). Per-claim:

- **ACL categories — still valid.** `crates/acl/src/categories/mod.rs:149-154` is unchanged
  (`COMMAND_ALL_CATEGORIES.get(...).cloned().unwrap_or_default()`); `categories/data.rs` has not
  been touched since the repo restructure (`git log -- .../categories/data.rs` → `7ba151f0` only).
  Spot-checks confirm the drift: `wait` (data.rs:209), `monitor` (:244), `failover` (:254) sit in
  `COMMAND_CATEGORIES` (ends :293) and are absent from `COMMAND_ALL_CATEGORIES` (starts :296);
  `json.set` / `ft.search` are in neither. The only tests are still the 6 spot-checks at
  `categories/mod.rs:218-236`.
- **Scripting write-flags — still valid.** `crates/core/src/scripting/bindings.rs:44-73`
  `is_write_command` is still the hand-written `match`; still no `SETBIT`, `BITFIELD`, `BITOP`,
  `PFADD`, `XSETID`, `HEXPIRE*`, `SORT … STORE`, `GEORADIUS … STORE`, or any `JSON.*`/`TS.*`/
  `BF.*`/`CF.*`/`CMS.*`/`TOPK.*`/`TDIGEST.*` write. Callers: `scripting/gate.rs`,
  `server/src/connection/lifecycle.rs`.
- **WAL declared keys — still valid.** `crates/core/src/registry.rs` still has three
  `debug_assert!`s (:184, :207, :213) and no test asserts key containment.

Cross-check with **issue 93**: neither subsumes the other. 93 owns "every registered spec passes
`CommandSpec::validate()`, unconditionally"; this issue owns "the parallel tables agree with the
spec" plus the `touched_keys ⊆ wal_strategy().actions()` property. They share the
`CommandRegistry::iter()` seam and should be scheduled together (93 already says so).
