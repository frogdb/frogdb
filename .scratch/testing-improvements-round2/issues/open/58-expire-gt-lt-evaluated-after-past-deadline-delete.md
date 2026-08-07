# `EXPIRE k -10 GT` deletes a key Redis keeps — the past-deadline delete runs before the GT/LT comparison

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/06 F2 · MASTER.md §3 (consistency violations)
Score: severity 5 · likelihood 3 · effort 1 · priority 20
Area: frogdb-commands / expiry

## Context

All four of `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` apply their "deadline is in the past →
delete the key" shortcut *before* evaluating the `GT`/`LT`/`NX`/`XX` condition. Redis
evaluates the condition first: a past TTL is never "greater than" the current one, so
`EXPIRE k -10 GT` returns 0 and leaves the key and its TTL untouched. FrogDB deletes the key
and replicates the `DEL`. The pattern that hits it is clamping a TTL derived from an external
timestamp with `GT` as a safety net — the `GT` is there *precisely so the write is safe*, and
it is the thing that fails.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`commands/src/expiry.rs:449-463` (`EXPIREAT`) — `if timestamp < 0 { delete; return }`
then `unix_secs_to_instant(...)` then `if expires_at <= Instant::now() { delete; return }` at
`:456-459`, and only *then* the GT/LT comparison at `:462+`. Identical ordering at `:282`/`:291`
(EXPIRE), `:371`/`:379` (PEXPIRE), `:533`/`:547` (PEXPIREAT). `ExpireatCommand::execute` is
`single-test` (77 regions, reached only by
`main::expire_tcl::tcl_expireat_check_for_expire_alike_behavior`); `PexpireatCommand::execute`
is `monoculture` (77 regions, 3 tests, `expire_tcl` only).

Why the existing tests pass anyway: the only coverage is the `expire_tcl` parity suite, which
exercises the past-deadline delete and the GT/LT comparison independently and never combines
them in one call.

## What to fix

1. In all four commands, evaluate the `NX`/`XX`/`GT`/`LT` condition **before** the
   past-deadline delete, so a rejected condition leaves the key and its TTL untouched.
2. Keep the negative-timestamp shortcut only for the paths where the condition passes.
3. Confirm the replicated effect follows the corrected decision (no `DEL` propagated when the
   condition rejects the write).

## Acceptance criteria

- [ ] A `core/tests/shard_driver/` case does `SET k v EX 100` then `EXPIRE k -10 GT` and
      asserts reply `0`, `EXISTS k == 1`, `TTL k ≈ 100`. Fails today, passes after the fix.
- [ ] The same case repeated with `LT` asserts the key **is** deleted and the reply is `1`.
- [ ] `NX` and `XX` variants asserted for all four of EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT.
- [ ] `EXPIREAT k <past> GT` on a key with **no** TTL is asserted against the pinned Redis
      behaviour.
- [ ] No `DEL` is propagated for a call whose condition rejected the write.

## Test boundary

Level 3 (`core/tests/shard_driver/`) — the assertion is on store state and key survival after
the effect pipeline runs, which a pure unit on the argument parser cannot see. Not level 4:
no connection, RESP or routing behaviour is involved.

## Depends on

issue 29 (decision D1 — home for command-semantics tests),
`.scratch/testing-improvements-round2/issues/`

## Re-triage 2026-08-06

**Verdict: still-valid**

Confirmed live on today's tree. File moved `commands/src/expiry.rs` → **`frogdb-server/crates/commands/src/expiry.rs`**, and the clock seam (`clock::now()`) shifted every line: EXPIRE past-deadline delete `:296-299` then GT/LT at `:304-318`; PEXPIRE `:385-388` then `:393-406`; EXPIREAT `:463-466`/`:471-475` then `:477-491`; PEXPIREAT `:547-550`/`:555-559` then `:561-575`. One correction to the body: **NX/XX are already evaluated before the delete** in all four commands (`:289`, `:378`, `:455`, `:539`), so only the **GT/LT half of the claim reproduces** — narrow the acceptance criteria accordingly (the `LT` and no-TTL-`LT` cases happen to agree with Redis by coincidence; `EXPIRE k -10 GT` on a key with a TTL, and on a key with no TTL, are the two divergent cases). No new coverage landed: `redis-regression/tests/expire_tcl.rs` GT cases (`:707`, `:721`, `:735`, `:906`) all use positive TTLs, and the expiry crate is not in the hardening campaign's locked set, so no FM row owns this.
