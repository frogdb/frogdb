# Seam lints: chokepoint gates

A seam lint states an invariant of the form **"every X must go through Y"**, where `Y` is the one
implementation that gets it right, and fails the build on any `X` that does not. Sixteen of these
ship today, plus `lint-failover-atomicity`'s sibling checks; each is a `just lint-<rule>` recipe
and all but one run in well under a second because they are `grep`/`awk` over source text, not
compiled Rust.

`just lint-gates` runs the compile-free fifteen of them in one shot. It is wired into
lefthook `pre-commit` **unconditionally** — no `CLAUDECODE=1` skip, unlike `rust-clippy`, because
these are greps, not a workspace compile — and into CI as the `seam-gates` job
(`.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`, rendered to
`.github/workflows/test.yml`), listed in `ci-pass`'s required-jobs array. `just lint` runs the
full sixteen (plus the turmoil lints) as part of `just check`/CI's `lint` job — it *depends on*
`lint-gates` rather than re-listing its members, because the two hand-maintained lists had
already drifted (three gates ran on every commit but not under `just lint`).

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
| `lint-spec` | every `Forced by` test named in a `specs/*.md` row exists and carries its `// FM-<AREA>-NNN` tag, and every tag in the Rust sources names a real spec row, checked in both directions | **no** — builds the listed crates' test binaries via `cargo nextest list` |
| `lint-no-typed-unwrap` | command code (`crates/commands/src`) never hand-rolls the `WrongType` invariant — no check-then-unwrap (`as_*_mut().unwrap()` / `get_mut(...).unwrap()`) and no `.ok_or(_else)(...WrongType...)` chain; go through the typed store accessors (`StoreTypedExt`/`StoreTypedFamilyExt`) instead | yes |
| `lint-keyspace-notify-routing` | keyspace/keyevent notifications publish through `KeyspaceNotificationCoordinator`, never `self.subscriptions.publish(...)` directly, except `dispatch_pubsub.rs` (the coordinator shard's own delivery arm) | yes |
| `lint-script-gate` | cross-shard Lua sub-command routing stays behind `ScriptCommandGate`: no `block_in_place` outside `scripting/gate.rs`, and no second key extraction (`extract_keys_from_command`) in `lua_vm.rs` | yes |
| `lint-durable-ack` | a single-file pin on `cluster/src/storage.rs`: each openraft storage method that acks durability (`save`, `save_vote`, `append`) issues its RocksDB write with sync options (`write_opt(..)` + `set_sync(true)`), never a plain `db.write`/`db.flush` that returns before the WAL is fsynced (durability is acked by a callback, not a value a grep can see, so this is a hand-crafted pin, not an `rg` rule) | yes |
| `lint-nested-config` | no figment `.nested()` on a config source anywhere under `frogdb-server` — it files a TOML file's top-level tables under non-default profiles that an `extract()` under `Profile::Default` never reads (round-2 issue 49); the one known site rides the named-gap warn idiom until the fix lands | yes |
| `lint-error-sanitize` | a single-file pin on `protocol/src/response.rs`: every CRLF-framed error frame (`Resp2BytesFrame::Error`, `Resp3BytesFrame::SimpleError`) takes its payload from `sanitize_error_message(..)` so a client's error text cannot inject a second wire frame (#38); the length-framed `Resp3BytesFrame::BlobError` is deliberately exempt | yes |
| `lint-continuation-lock` | every mutating shard-dispatch arm states a disposition against `ShardWorker::can_execute_during_lock` — GATE (calls it, in the arm body), EXEMPT (reason + a forcing test that must still exist), or a tracked named-gap bypass; the 64 arms of the 11 shard `*Msg` enums are count-pinned per enum, so a new or renamed arm cannot land without a classification | yes |
| `lint-script-write-seam` | a script's writes reach the store only through `ShardWriteSeam::admit` (`specs/txn.md` FM-TXN-051): `ScriptCommandGate::dispatch` admits *before* it runs the sub-command and before it marks the script write-dirty, `invoker.run_local`/`run_remote` appear only in `gate.rs`, the seam is assembled only by `ShardWorker::write_seam` (live cluster state / node id / quorum checker / tracker), the two admission bypasses (`pre_authorized` for replica apply, `internal` for the shard harness) are pinned to their file, and every shard message carrying writes the connection never gauntleted declares an `admission` | yes |

Two recipes sit next to this family but are out of scope for both `lint-gates` and this doc's
"the 16": `lint-turmoil` (a `cargo clippy --features turmoil` pass — compiles) and
`lint-turmoil-features` (checks the turmoil cargo-feature is forwarded through every dependent
manifest — does not compile, but polices the turmoil feature rather than a seam, and the
originating issue named "the turmoil lints" as excluded alongside the one that compiles). Both
still run under `just lint`.

### `lint-continuation-lock`: a count pin instead of a full classification

The shard-dispatch surface is 64 match arms across 11 `*Msg` enums, and most of them (pub/sub
registration, observability counters, DEBUG probes, tracking tables) never touch the keyspace a
VLL continuation lock protects. Classifying all 64 by hand — the first attempt — produced a table
that was mostly noise. `scripts/continuation-lock-gate.py` names only the arms that reach store
execution and pins **the per-enum arm count** for everything else:

- **GATE** (5 arms: `CoreMsg::Execute`, `CoreMsg::ScatterRequest`, `ScriptingMsg::EvalScript`,
  `EvalScriptSha`, `ScriptSubCommand`) — the arm body must contain a `can_execute_during_lock(`
  call. The converse holds too: an arm that calls the gate but is not pinned GATE fails, and the
  gate may not be called anywhere *but* a pinned arm, so the disposition is always visible at the
  dispatch site rather than buried in a handler.
- **EXEMPT** (2 arms: `VllMsg::VllExecute`, `CoreMsg::GetVersion`) — mutating but deliberately
  ungated, each with a one-line reason and a **named forcing test the gate checks still exists**.
  The reasoning and evidence are in `.scratch/hardening-2/c3-arm-dispositions.md`; an exemption
  without its forcing test is an unproven claim, so it fails.
- **A tracked named-gap bypass** (2 arms: `CoreMsg::ExecTransaction`,
  `ScriptingMsg::FunctionCall`) — known isolation defects (round-2 issue 50, hardening-2 issue
  05) whose fix has not landed. They warn while their issue link resolves; the moment either arm
  gains a gate call the stale entry hard-fails and forces its promotion to GATE.
- **Everything else** — no third `NONMUTATING` set. A new or renamed arm moves its enum's count,
  and the failure message prints that enum's arms annotated `[GATE]` / `[EXEMPT]` / `[GATE-GAP]` /
  `[-]`, so the unclassified newcomer is the one without a tag. Arms are also cross-checked
  against the enum's variants in `message.rs` in both directions, so a variant handled outside its
  dispatch file cannot slip past the count.

## Suppression idioms

Two shipped idioms, no new ones — never an in-code `#[allow]` (clippy cannot express these rules,
and an in-code hatch is invisible to review):

- **Count-pinned per-file allowlist** (`scripts/clock-seam.py:73-134`). A dict of
  `file -> (expected_count, reason)`. Checked in both directions
  (`clock-seam.py:261-274`): a file whose real count no longer matches the pinned number is an
  error whichever way it moved, so a new violation in an already-exempt file fails just like a new
  file would, and a fixed violation forces the entry down (or out) rather than letting it go
  stale. This is the ratchet — no separate baseline file.
- **Named-gap warn-not-fail** (`scripts/spec-lint.py:27-32`). For an invariant that is real but
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

Location convention for the Python form: shared helpers (`cfg_test_spans()`, `is_test_path()`,
`in_any_span()`) live in `scripts/_rustscan.py` and are imported by every Python gate that needs
`#[cfg(test)]`-span awareness — never re-copied into a third script.

A gate whose scanning is more than a regex (`continuation-lock-gate.py` brace-matches match arms
and enum bodies) carries its own dependency-free assert script under `scripts/tests/`, wired as a
`just test-<rule>` recipe — the parser is what the whole rule rests on, so a silently broken
scanner would report `OK` forever.
