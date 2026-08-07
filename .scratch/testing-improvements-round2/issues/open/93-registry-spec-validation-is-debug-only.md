# Whole-registry `CommandSpec` validation is a `debug_assert!` — unchecked in every release build, and asserted by no test

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: `INFRASTRUCTURE.md` "Two structural notes that change infra cost"
Score: severity 3 · likelihood 2 · effort 1 · priority 11
Area: frogdb-core / command registry

## Context

Every command's `CommandSpec` is validated exactly once, at registration time, inside a
`debug_assert!`. That means the single source of truth for command metadata — arity, key
positions, flags — is checked only when `debug_assertions` is on, and **no test anywhere
asserts it directly**. The guarantee that "the registry is internally consistent" is a
compile-profile side effect, not a tested property.

The audit flagged this because it is load-bearing for several proposed tests: a finding that
says "assert the handler's declared keys match its mutations" is meaningless if the declaration
itself was never validated. It was recorded as a structural note rather than an `I<N>`
infrastructure item, so no other issue owns it.

**Verified during filing**, with one correction to the audit's framing: the audit warned *"if
any suite runs in release, whole-registry spec validation is silently unchecked."* No test
recipe in the `Justfile` and no generated workflow passes `--release` to `cargo test`, so the
assert **is** live in test runs today. The exposure is narrower than stated but real — every
artifact built by `just release` (`Justfile:61`), `just run-release` (`:257`), the
cross-compiled binaries (`:480`, `:484`), and therefore every jepsen/benchmark run against a
release server, has the validation compiled out. A spec that only a release build rejects would
ship.

## Evidence

- `frogdb-server/crates/core/src/registry.rs:182-187` — `register()` wraps the sole validation
  call in `debug_assert!`:
  ```rust
  debug_assert!(
      command.spec().validate().is_ok(),
      "{}: invalid CommandSpec: {:?}",
      command.spec().name,
      command.spec().validate()
  );
  ```
  (The audit cited `:184`, which is inside this block.)
- `grep` for a test calling `spec().validate()` over the registry: none. `CommandRegistry::iter()`
  (`registry.rs:256`) already provides the iteration such a test needs — `INFRASTRUCTURE.md`
  names it as the seam for the registry-consistency theme and notes it "needs no harness".
- `Justfile:61,257,480,484` — release build paths, all with `debug_assertions` off.
- No `cargo test --release` in `Justfile` or `.github/workflows/`, so the assert is currently
  live under test. This is the reason the gap has not bitten yet; it is not a guarantee.

## What to fix

1. Add a `#[test]` that iterates `CommandRegistry::iter()` and asserts `spec().validate().is_ok()`
   for every registered command, reporting the offending command name on failure. This is
   profile-independent and turns an implicit invariant into an asserted one.
2. Decide the fate of the `debug_assert!` in `register()`. Once (1) exists it is redundant for
   the built-in registry, but it still guards *dynamically* registered commands. Either keep it
   (cheap, catches non-registry callers early) or promote it to an unconditional `assert!` — the
   validation runs once per command at startup, so its release cost is negligible. Do not
   silently delete it.
3. Confirm this test runs in the default `just test` path, not only in a crate whose tests are
   commonly filtered out.

## Acceptance criteria

- [ ] A test exists that fails when any registered command's `CommandSpec::validate()` returns
      `Err`, and it does not depend on `debug_assertions`.
- [ ] The test's failure message names the offending command, not just the assertion.
- [ ] Proven to fail on a deliberately corrupted spec (temporarily break one arity/key-position
      declaration, observe the failure, revert) — record the observed message in this issue.
- [ ] `register()`'s `debug_assert!` is either retained with a comment stating that the
      registry-wide case is now covered by the test, or promoted to `assert!` — with the choice
      recorded here.
- [ ] The test is reached by `just test` with no extra flags.

## Test boundary

1 — pure unit over `CommandRegistry::iter()` and `CommandSpec::validate()`, both already public
and both pure. Nothing above level 1 is warranted: no shard, no store, no socket is involved,
and a higher level would only make the failure harder to read.

## Depends on

Nothing. Related to the registry-consistency theme (issue 19,
`.scratch/testing-improvements-round2/issues/`), which uses the same `CommandRegistry::iter()`
seam for a larger invariant — do them together if that one is scheduled first.

## Re-triage 2026-08-06

**Verdict: still-valid**

Unchanged. The `debug_assert!` is still the only whole-registry validation, now at
`frogdb-server/crates/core/src/registry.rs:184-189` (body cited `:182-187`; the audit's `:184` is
still inside the block). One addition the body does not mention: `register_conn_command` carries a
**second** `debug_assert!` over `spec().validate()` at `registry.rs:207-212`, so the sweep has two
call sites to decide about, not one. Repo-wide, every other `spec().validate()` hit is in
`core/src/command_spec.rs`'s own unit tests over hand-built specs (`:1174-1299`) — still no test
that iterates `CommandRegistry::iter()` and validates the *registered* set, and `Justfile`/
`.github/workflows/` still pass no `--release` to `cargo test`, so the exposure remains exactly as
filed. Relationship to issue 19 (still open): 19 owns the broader "parallel command tables drift
from `CommandSpec`" invariant over the same `CommandRegistry::iter()` seam and names issue 29 as its
home-blocker; this issue is the narrow, unblocked subset — assert `validate().is_ok()` for every
registered command — and does not depend on 19 landing. Issue 29 is now resolved (harness crate
`frogdb-shard-harness`), so a registry-wide test has a home.
