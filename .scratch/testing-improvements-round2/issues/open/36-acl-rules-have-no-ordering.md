# ACL rules are order-insensitive sets — `+config|get -config` leaves CONFIG GET allowed

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/15 F2 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 1 · priority 22
Area: frogdb-acl / PermissionSet

## Context

Redis ACL semantics are strictly ordered — later rule wins. FrogDB stores
`allowed_commands`/`denied_commands`/`allowed_categories`/`denied_categories` as unordered
`HashSet`s plus a `Vec<SubcommandRule>` that is consulted *first* and returns unconditionally, so
`ACL SETUSER u +config|get -config` evaluates to "allowed": the operator's intent (revoke all
CONFIG) is silently inverted. `-get +@read` over-denies in the other direction. Writing a broad
grant then a narrower revoke is the standard way humans author ACLs, and `-command` after
`+command|subcommand` is the natural fix-up, so this is reachable on ordinary configuration.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `acl/src/permissions.rs:215-268` — `is_command_allowed` checks `subcommand_rules` first and
  `return rule.allowed` unconditionally, before `denied_commands`, before `denied_categories`,
  before `allow_all`.
- `acl/src/permissions.rs:320-326` — `deny_command` does
  `self.subcommand_rules.retain(|r| r.allowed || r.command != cmd)`, i.e. it deliberately **keeps**
  allowing subcommand rules when the parent command is denied.
- Also `-get +@read` over-denies relative to Redis (the deny set is consulted before the allow
  category, with only an `allowed_commands` escape).
- **Why no existing test catches it**: zero tests in the crate apply two conflicting rules to the
  same user.

## Options

The proposal raised an explicit `OPTIONS` block; the boundary decision has to be made before the
work starts, because the likely fix replaces the data model.

- *(a)* Unit test on `PermissionSet` (level 1) — fastest, pins the exact semantics, but asserts on
  a structure a fix would likely replace with an ordered `Vec<Rule>`.
- *(b)* Crate-level test through `AclManager::set_user` + `FullAclChecker` (level 2) — survives the
  refactor to an ordered rule list, still no server, marginally slower.
- *(c)* Parity test in `redis-regression/tests/acl_tcl.rs` (level 4) — proves Redis equivalence,
  which is the real spec, but slow and it will not run until the semantics are fixed.
- **Recommendation: (b)**, plus one (c) row per bypass shape once fixed. The ordered-rule-list
  refactor is the likely fix, so do not pin `PermissionSet` internals.

## Acceptance criteria

- [ ] A table-driven test over rule *sequences* exists at the chosen boundary, each row applying
      rules in order to a fresh user and asserting the final verdict:
      `[("+@all -@admin", "monitor", false), ("+config|get -config", "config get", false),
      ("-get +@read", "get", true), ("+@read -get +get", "get", true), ("-@all +get", "get", true)]`.
- [ ] At least the `+config|get -config` row **fails against today's code** and passes after the
      fix. (If the current data model cannot satisfy the table at all, that *is* the finding and
      the fix is the ordered-rule-list refactor.)
- [ ] Companion proptest: generate a random rule sequence, evaluate against a trivially-correct
      ordered reference implementation (a fold over the rule list), assert agreement.
- [ ] Once fixed, one `redis-regression/tests/acl_tcl.rs` parity row per bypass shape.

## Test boundary

**1** as proposed for `PermissionSet` (pure — a socket adds nothing), **2** recommended so the
test survives the refactor. Not level 4: the verdict is a pure function of the rule sequence, and
the socket only re-encodes it.

## Depends on

Nothing. Related: issue 35, `.scratch/testing-improvements-round2/issues/` — both are ACL
enforcement bypasses and should be fixed and re-tested together.

## Re-triage 2026-08-06

**Verdict: still-valid**

Reproduces. `is_command_allowed` (now `frogdb-server/crates/acl/src/permissions.rs:215-266`, was
cited 215-268) still scans `subcommand_rules` first and does `return rule.allowed` unconditionally
at `permissions.rs:225`, ahead of `denied_commands` (:231), `denied_categories` (:239-246) and
`allow_all` (:249). `deny_command` still does
`self.subcommand_rules.retain(|r| r.allowed || r.command != cmd)` at `permissions.rs:325-326`,
deliberately keeping the `+config|get` rule alive across a later `-config`. The model is still four
unordered `HashSet`s plus a `Vec<SubcommandRule>`; the `rule_log: Vec<AclCommandRule>` added
alongside is display-only (`ACL GETUSER` rendering) and is not consulted by `is_command_allowed`.
One body claim is now stale: `frogdb-server/crates/server/tests/integration_acl.rs:1499`
(`test_acl_subcommand_allow_specific`) *does* apply two conflicting rules to one user, but only in
the `-config` → `+config|get` order where the set model happens to agree with Redis; nothing tests
the reversed order. Only history on the file since filing is `7ba151f0` (directory restructure).
