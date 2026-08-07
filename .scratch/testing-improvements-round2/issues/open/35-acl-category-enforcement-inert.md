# ACL category enforcement is largely inert — 185 of 356 commands have no category row

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/15 F1 · MASTER.md §3
Score: severity 5 · likelihood 5 · effort 2 · priority 23
Area: frogdb-acl / categories + permissions

## Context

`CommandCategory::all_for_command` returns an **empty vec on miss**, and `permissions.rs` is its
sole enforcement consumer, so every `-@category` rule is a silent no-op for any command absent
from `COMMAND_ALL_CATEGORIES`. A user granted `+@all -@admin` can still run `MONITOR` (streaming
every other tenant's commands and keys), `CLUSTER`, `FAILOVER`, `LATENCY`; a user granted
`+@all -@write` can still run `JSON.SET`, `TS.ADD`, `HEXPIRE`, `ZREMRANGEBYSCORE`, `HGETDEL`,
`DELEX`, `MIGRATE`. These are the two rules operators actually write. Any new command lands
category-less by default, so the surface grows with the codebase.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** Verified directly during consolidation: `acl/categories/mod.rs:149
all_for_command` is the most-executed function in the audit (3551 tests, 233 560 executions, 7/7
regions) and the covered path is `unwrap_or_default()` returning `[]` for 185 of 356 commands.

## Evidence

- `acl/src/categories/mod.rs:149-153` — `all_for_command` is
  `COMMAND_ALL_CATEGORIES.get(cmd).cloned().unwrap_or_default()`, i.e. **empty vec on miss**, and
  `acl/src/permissions.rs:215-268` reads *only* `all_for_command`, so an empty vec makes every
  `-@category` rule a no-op for that command.
- Mechanically extracted the 356 `static SPEC: CommandSpec` names and diffed against
  `acl/src/categories/data.rs`: **185 registered commands** (~174 excluding test-only stubs
  `test`/`teststub`/`walmock`/`__seam*`) have no `COMMAND_ALL_CATEGORIES` row — all of JSON.\*,
  FT.\*, TS.\*, TOPK.\*, TDIGEST.\*, BF.\*, CF.\*, CMS.\*, V\*, ES.\*, plus `migrate`, `lcs`,
  `substr`, `rpoplpush`, `zintercard`, `zremrangeby{lex,rank,score}`, the whole HEXPIRE/HTTL
  family, `hgetdel`, `hgetex`, `hsetex`, `msetex`, `delex`, `xackdel`, `xdelex`, `georadius*_ro`,
  `lolwut`, `psync`, `replconf`, `frogdb.*`.
- Separately, **20 commands sit in the primary `COMMAND_CATEGORIES` table but are missing from
  `COMMAND_ALL_CATEGORIES`** — `monitor`, `cluster`, `failover`, `latency`, `wait`, `waitaof`,
  `function`, `fcall`, `fcall_ro`, `eval_ro`, `evalsha_ro`, `pfdebug`, `pfselftest`, `bzmpop`,
  `zmpop`, `zdiff`, `zdiffstore`, `zinter`, `zunion`, `zrangestore` — so even commands someone
  remembered to categorise are unenforceable.
- Coverage: `COMMAND_CATEGORIES::{closure#0}` (`acl/src/categories/data.rs:10-293`, 276 regions)
  is `untested`, `regions_covered: 0`, `test_count: 0`.
- **Why the existing tests pass anyway**: the `categories/mod.rs` tests are 6 spot-checks plus
  `assert_eq!(all.len(), 21)` on the *enum* (`categories/mod.rs:250`), and the only two tests of
  `all_for_command` use `"GET"`/`"SET"`, which *are* in the table. There is no test that catches a
  new command added with no ACL category.

## What to fix

1. Populate `COMMAND_ALL_CATEGORIES` for every registered command, deriving from `CommandSpec`
   flags where possible rather than hand-writing a third table.
2. Make a miss in `all_for_command` non-silent — return an error or a deny-by-default category set
   instead of `unwrap_or_default()`.
3. Add the registry-consistency test below so the tables can never drift again; this is the ACL
   row of theme T1.

## Acceptance criteria

- [ ] New test `commands/tests/acl_category_coverage.rs` builds a `CommandRegistry`, calls
      `frogdb_commands::register_all`, iterates `registry.iter()` and asserts for every spec name
      that `CommandCategory::all_for_command(name)` is non-empty. **Fails today** (185 misses).
- [ ] The same test asserts category/flag consistency: `CommandFlags::WRITE ⇒ contains Write`,
      `!WRITE ⇒ contains Read` for keyed commands, `ADMIN ⇒ contains Admin`, and that
      `for_command`'s primary category is a member of `all_for_command`.
- [ ] An explicit `const CATEGORY_EXEMPT: &[&str]` allowlist covers only internal/test stubs, so
      adding a real command forces a deliberate edit.
- [ ] Targeted assertion pair passes after the fix and fails before: `+@all -@admin ⇒ MONITOR
      denied`, `+@all -@write ⇒ JSON.SET denied`.

## Test boundary

**2** — crate-level API test in `commands`, the lowest crate that can see both the registry and,
via `core`→`acl`, the tables (`acl` itself depends only on `types`). Not level 3+: no server or
shard worker is needed to prove the predicate.

## Depends on

Theme T1 (hand-maintained parallel tables drift from `CommandSpec`) — issue 19,
`.scratch/testing-improvements-round2/issues/`; the same registry-consistency module closes this
and the scripting write-flags and WAL declared-keys instances.

## Re-triage 2026-08-06

**Verdict: still-valid**

Reproduces verbatim. `all_for_command` is still `COMMAND_ALL_CATEGORIES.get(...).cloned().unwrap_or_default()`
at `frogdb-server/crates/acl/src/categories/mod.rs:149-154`, and `permissions.rs:236` is still its
sole enforcement consumer (deny-category loop at `permissions.rs:239-246`), so an empty vec makes
every `-@category` rule a no-op. The gap has *grown*: `COMMAND_ALL_CATEGORIES`
(`categories/data.rs:296-1801`) holds **202** rows against **377** `static SPEC: CommandSpec`
constants in the tree (was 356 at filing). `monitor` (`data.rs:244`) and `failover` (`data.rs:254`)
are still present only in the primary `COMMAND_CATEGORIES` table (lines 9-293) and absent from
`COMMAND_ALL_CATEGORIES`, so `+@all -@admin` still permits `MONITOR`. No `acl_category_coverage`
test or `CATEGORY_EXEMPT` allowlist exists anywhere in the tree; the only history on either file
since filing is `7ba151f0` (the directory-restructure move). Path correction: all `acl/src/...`
refs in the body are now `frogdb-server/crates/acl/src/...` — line numbers are otherwise unchanged.
ACL is not a hardening-campaign locked area, so no `FM-*` row covers it.
