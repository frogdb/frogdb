# Seam lints: chokepoint gates

A seam lint states an invariant of the form **"every X must go through Y"**, where `Y` is the one
implementation that gets it right, and fails the build on any `X` that does not. Eleven of these
ship today, plus `lint-failover-atomicity`'s sibling checks; each is a `just lint-<rule>` recipe
and all but one run in well under a second because they are `grep`/`awk` over source text, not
compiled Rust.

`just lint-gates` runs the compile-free ten of them in one shot (~0.9s measured). It is wired into
lefthook `pre-commit` **unconditionally** — no `CLAUDECODE=1` skip, unlike `rust-clippy`, because
these are greps, not a workspace compile — and into CI as the `seam-gates` job
(`.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`, rendered to
`.github/workflows/test.yml`), listed in `ci-pass`'s required-jobs array. `just lint` still runs
the full eleven (plus the turmoil lints) as part of `just check`/CI's `lint` job.

## The family

| recipe | invariant | in `lint-gates` |
|---|---|---|
| `lint-info-seam` | INFO section content is rendered by an `InfoSection` (`crates/server/src/info`), never patched into the wire text with `.replace(...)`/`.replace_range(...)` after the fact | yes |
| `lint-redirect-seam` | every MOVED / ASK / CROSSSLOT reply is built through `frogdb-types/src/redirect.rs`, never an inline `Response::error(...)` literal (which re-opens the IPv6 address-bracketing bug) | yes |
| `lint-pubsub-confirmation-seam` | subscribe/unsubscribe confirmations are built through `frogdb_core::PubSubConfirmation` (the RESP3-Push-vs-RESP2-Array owner), and the RESP2 array-null (`*-1\r\n`) literal appears only in `codec.rs` | yes |
| `lint-failover-atomicity` | a failover or FAIL-marking is one atomic Raft entry (`ClusterCommand::Failover` / `MarkNodeFailed`), never a standalone `IncrementEpoch` write or a hand-rolled `RemoveNode`/`SetRole`/`AssignSlots` saga that can strand other nodes at a stale epoch on a leader crash | yes |
| `lint-metrics-chokepoint` | metrics are emitted through the typed handles `define_metrics!` generates, never a raw string-named `increment_counter`/`record_gauge`/`record_histogram` call (which re-opens registry drift and the first-caller-fixes-arity panic class) | yes |
| `lint-format-float` | exactly one `format_float` definition exists, in `frogdb-protocol`; every other renderer re-exports it (so the reply path and the WAL/replication store path can never disagree on a float's spelling) | yes |
| `lint-clock-seam` | server-crate non-test code reads the clock through `frogdb_types::clock` (`now()` / `system_now()`), never `std::time::Instant::now()` / `SystemTime::now()` directly, so a turmoil run's paused clock and the server's timeline never disagree | yes |
| `lint-failure-modes` | every `Forced by` test named in a `.scratch/hardening/specs/*-failure-modes.md` row exists and carries its `// FM-<AREA>-NNN` tag, and every tag in the Rust sources names a real spec row, checked in both directions | **no** — builds the listed crates' test binaries via `cargo nextest list` |
| `lint-no-typed-unwrap` | command code (`crates/commands/src`) never hand-rolls the `WrongType` invariant — no check-then-unwrap (`as_*_mut().unwrap()` / `get_mut(...).unwrap()`) and no `.ok_or(_else)(...WrongType...)` chain; go through the typed store accessors (`StoreTypedExt`/`StoreTypedFamilyExt`) instead | yes |
| `lint-keyspace-notify-routing` | keyspace/keyevent notifications publish through `KeyspaceNotificationCoordinator`, never `self.subscriptions.publish(...)` directly, except `dispatch_pubsub.rs` (the coordinator shard's own delivery arm) | yes |
| `lint-script-gate` | cross-shard Lua sub-command routing stays behind `ScriptCommandGate`: no `block_in_place` outside `scripting/gate.rs`, and no second key extraction (`extract_keys_from_command`) in `lua_vm.rs` | yes |

Two recipes sit next to this family but are out of scope for both `lint-gates` and this doc's
"the 11": `lint-turmoil` (a `cargo clippy --features turmoil` pass — compiles) and
`lint-turmoil-features` (checks the turmoil cargo-feature is forwarded through every dependent
manifest — does not compile, but polices the turmoil feature rather than a seam, and the
originating issue named "the turmoil lints" as excluded alongside the one that compiles). Both
still run under `just lint`.

## Suppression idioms

Two shipped idioms, no new ones — never an in-code `#[allow]` (clippy cannot express these rules,
and an in-code hatch is invisible to review):

- **Count-pinned per-file allowlist** (`scripts/clock-seam.py:73-134`). A dict of
  `file -> (expected_count, reason)`. Checked in both directions
  (`clock-seam.py:261-274`): a file whose real count no longer matches the pinned number is an
  error whichever way it moved, so a new violation in an already-exempt file fails just like a new
  file would, and a fixed violation forces the entry down (or out) rather than letting it go
  stale. This is the ratchet — no separate baseline file.
- **Named-gap warn-not-fail** (`scripts/failure-modes.py:20-26`). For an invariant that is real but
  blocked on machinery that does not exist yet, `Forced by | MISSING ([gap: <issue>](<link>))`
  warns instead of failing, but only if `<link>` resolves to a real issue file — an unresolvable
  link still fails. This keeps the gap visible in every run without blocking the lint on work
  that hasn't landed.

## Adding a new rule

Anatomy (PRD `.scratch/hardening-2/PRD.md` §3, "W1 — Chokepoint lints"):

1. **Invariant** — one sentence, stated so a violation is a defect, not a style opinion.
2. **The chokepoint** — the single function/type that satisfies it. If there isn't one, the rule
   starts by *creating* one; a lint without a chokepoint is a nag.
3. **A mechanical predicate** over `rg`/AST output — the shape of a violation.
4. **An escape hatch with a justification** — one of the two suppression idioms above, never a
   bare ignore.
5. **A ratchet** — land with every current violation listed in the allowlist (or the named-gap
   form), so the rule can ship before the cleanup does; burn down in batches; delete the list when
   empty (`lint-format-float` and `lint-keyspace-notify-routing` already have).
6. **Wiring** — `scripts/<rule>.py` (PEP-723 header, no dependencies) if the rule needs
   `#[cfg(test)]`-span awareness, a structured allowlist, or two-way checking; otherwise an inline
   `Justfile` recipe (most of the 11 are, at well under 40 lines). Either way: a `just
   lint-<rule>` recipe, membership in `just lint`, and — if the check is compile-free — membership
   in `just lint-gates` so it actually runs on every commit and in CI. A rule that only joins
   `just lint` inherits this family's original hole: convention, not enforcement.

Location convention for the Python form: shared helpers (`cfg_test_spans()`, `is_test_path()`)
belong in a common module rather than a third copy — they exist today as private helpers in
`scripts/clock-seam.py:153-179`.
