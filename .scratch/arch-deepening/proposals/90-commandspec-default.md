# Proposal 90 — `CommandSpec::DEFAULT`: 296 statics spell out 13 fields; seven of those fields are the same value at ≥82% of sites

Round 38 · lane: commands + types · candidate **CT2** · effort **M, top of band** · **no
locked crate edited**, **zero `FM-` tags anywhere in `crates/commands/src`** (verified: `grep -rn
"FM-" frogdb-server/crates/commands/src` returns nothing), **one commands-scoped seam gate
(`lint-no-typed-unwrap`) verified unaffected** (§Spec / gates).

**Verified at HEAD `eb8760e965f4189e394b06e54d52b83322b9cf0f`** (worktree `arch-round-38-99`).
Every count below was re-derived at this SHA by exact-string grep; the make-or-break compile
question (functional record update in `const`/`static` context) was **verified by compiling and
running two throwaway programs on the workspace's pinned toolchain** (§The compile check). Nothing
is inherited from the lane brief.

## This is not the rejected "strategy folding" candidate

A prior round rejected a candidate that would have *folded/unified strategy-like declarations*.
The distinction here is **structural, and the structure alone is the argument** — it holds
whether or not the earlier rejection was ever written down:

- **Nothing is removed or unified.** All 13 `CommandSpec` fields keep their exact types, names,
  variants, and meanings. `ExecutionStrategy`, `WalStrategy`, `WaiterWake`, `LookupSpec`,
  `AccessSpec`, `ReindexSpec`, `ConnMutation` are untouched as types. No enum loses a variant; no
  two concepts become one; no consumer's read of a spec changes shape.
- **Only the *obligation to spell modal values* is dropped.** A spec site keeps stating every fact
  that is exceptional for its command; the facts equal to the crate-wide mode come from one shared
  `pub const DEFAULT` via struct-update syntax. Every spec still denotes exactly the same 13-field
  value — the golden test (§Testability) proves it bit-for-bit, and `docs-gen` output is
  byte-identical.
- Folding removes distinctions the type system currently makes. This removes *repetition* the type
  system currently forces. They are opposite operations on the same catalog.
- **ADR check (completeness, not the argument):** `adr/` contains only
  `0001-operator-imports-server-config-crate`, `0002-txn-orchestration-behind-txnhost-seam`,
  `0003-persistence-durability-seams`, `0004-replication-runtime-seams`. The strategy-folding
  rejection is not recorded in any ADR (grep for "strategy"/"folding" across `adr/*.md` and
  `proposals/INDEX.md` returns nothing). That absence is *not* offered as licence — the proposal is
  drawn narrowly on the structural grounds above and would be equally drawn that way against a
  written ruling.

## The compile check (make-or-break, done first)

`..base` functional record update (FRU) in `const`/`static` items had historical restrictions, so
the mechanism was compiled before anything was proposed. Toolchain: **rustc 1.92.0 (ded5c06cf
2025-12-08)** — the pinned channel (`rust-toolchain.toml:2`, `channel = "1.92.0"`), workspace
edition 2024 (`Cargo.toml:44`). Two throwaway programs (written outside the repo, in the job tmp
dir) compiled **and ran** with `rustc --edition 2024`:

1. **Plain FRU against a `pub const DEFAULT`** — all three positions the codebase needs:
   - `static GET: Spec = Spec { name: "GET", keys: KeySpec::First, ..Spec::DEFAULT };` (static item)
   - `const SET: Spec = Spec { name: "SET", ..Spec::DEFAULT };` (const item)
   - `static SPEC: Spec = Spec { name: "HSET", ..Spec::DEFAULT };` **inside a fn body** — the exact
     shape of all 296 sites (`fn spec(&self) -> &'static CommandSpec { static SPEC: … }`,
     e.g. `commands/src/hash.rs:37-38`).

   **Verdict: compiles clean, runs, assertions pass. No feature gate, no nightly, no workaround.**

2. **`const fn` base as the FRU expression** — `Spec { keys: …, ..Spec::defaults("HSET", 3) }` in a
   `static` — also compiles and runs (relevant to the rejected alternative (R1) below, and already
   precedented in-tree: `core/src/registry.rs:400-416` builds a `CommandSpec` from a
   `const fn conn_spec(strategy) -> CommandSpec` in test code today).

The test struct mirrored the real field kinds: unit/struct-variant enums, `bool`,
`&'static str`, `&'static [_]` slice. The real `CommandSpec` (`core/src/command_spec.rs:469-507`)
contains nothing FRU-hostile beyond that: every field type is a plain `Copy`-able enum
(`KeySpec` `:24`, `AccessSpec` `:221`, `EventSpec` `:186`, `LookupSpec` `:295`, `ReindexSpec`
`:355`, plus `Arity` `command.rs:878`, `ExecutionStrategy` `:70`, `ConnMutation` `:216`,
`WaiterWake` `:344`, `WalStrategy` `:360`), a `bitflags` u32 (`CommandFlags`, `command.rs:908-911`;
`empty()`/`union()` are `const fn` — sites already call `.union(…)` in statics, `hash.rs:41`), a
`bool`, and a `&'static str`. No `Drop`, no generics, no interior mutability.

## Summary

`CommandSpec` is the crate's declarative command interface: "One `static` per command"
(`command_spec.rs:468`). The commands crate holds **296** such statics across **56 files**
(`grep -rl ': CommandSpec = CommandSpec {' crates/commands/src | wc -l` → 56; site count 296), and
every one spells out **all 13 fields** — 296 `name:`/`arity:`/`flags:` lines each, 296 `wal:`
lines, etc.; **4,966 lines** of spec-block text in total. Seven of the 13 fields have a single
dominant value (82.1%–100% of sites, table in §Problem); those seven fields alone account for
**1,907 lines that repeat the crate-wide mode verbatim**.

The cost is not aesthetic. Each new spec-wide field is a full-catalog sweep: four fields were added
in a **three-week window** this summer, and each one edited every site — `reindex` (`38ce99d1`,
2026-07-22) added **exactly 296 `reindex:` lines to the commands crate** (395 across the tree, 93
files, 989 insertions), **263 of which (89%) are `ReindexSpec::None`**; `mutation` (`ab340681`,
2026-07-20) likewise added 296 commands-crate lines (394 tree-wide, 92 files), **all 296 of them
`ConnMutation::None`** (§Problem 2). A 14th field tomorrow costs the same ~380-site edit again.

The proposal: add **`pub const DEFAULT: CommandSpec`** to the existing `impl CommandSpec`
(`command_spec.rs:793`), holding the modal value for the seven defaultable fields and inert
placeholders for the six identity/semantics fields that stay explicit by policy; then run a
mechanical, exact-string **awk sweep** over the 296 commands-crate sites that deletes the seven
modal lines where present and appends `..CommandSpec::DEFAULT` — **−1,907 / +296 lines, net
−1,611** — followed by a **second scripted pass** pruning the **182 imports** the first pass
orphans across 54 files. Zero semantic change, proven by a 296-row golden test landed *before* the
sweep and run under **both** feature profiles, plus byte-identical `docs-gen` output. After the
change, a spec states its identity and its exceptions; the mode is stated once.

One structural fact governs the acceptance plan: **135 of the 296 sites (45.6%) sit behind the 12
opt-in command-family features**, which the `-p`-scoped dev-loop commands do not compile. The
battery in §Testability is built around that.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/core/src/command_spec.rs` | 1778 | **Primary (the interface).** Struct `:469-507` (13 fields); `impl CommandSpec` `:793` gains `pub const DEFAULT` (~20 lines); `validate()` `:807` gains one clause **inserted at the top of the body, before the current first check at `:810`**: reject `name.is_empty()` (new `SpecError` variant) so a spec that accidentally omits `name:` fails the registry debug-assert (`registry.rs:184-189`) and `every_full_command_spec_validates` (`register.rs:298`) instead of registering as `""`. Top placement matters — see §(a). |
| `frogdb-server/crates/commands/src/**` (56 files) | — | **Primary (the sweep).** 296 sites, uniform shape `static SPEC: CommandSpec = CommandSpec {` at 8-space indent, fields at 12 (verified: the seven exact default strings match 296/282/278/273/272/263/243 times with `^            ` anchoring — indentation is uniform). Largest: `hash.rs` 28 sites, `string.rs` 23, `timeseries.rs`/`set.rs`/`list.rs` 17 each. **135 of the 296 sites (45.6%) live in the 12 feature-gated families** (`lib.rs:27-60`) and **161 in the always-on core profile** — the sweep is per-site and identical either way, but the *verification* is not (§Testability, feature coverage). |
| `frogdb-server/crates/commands/tests/spec_golden.rs` + `tests/spec_golden.txt` | **new** | **Primary (the net).** Golden test pinning all 13 fields of all 296 specs via `Debug` (derived, `command_spec.rs:468`); lands **before** the sweep. §Testability. The crate currently has **no tests/ dir at all** — this is its first integration test. Golden artifact ≈ **75 KB** (296 rows × one fully-expanded 13-field `Debug` line). |
| `frogdb-server/crates/server/src/server/register.rs` | — | **Read-only evidence.** The existing cross-field nets: `every_full_command_spec_validates` `:298`, `every_write_command_declares_event` `:311`, `every_write_command_declares_wal` `:328` + `WAL_NOOP_ALLOWLIST` `:268`, `data_adding_commands_wake_blocked_clients` `:347`, `multi_key_commands_declare_accurate_events` `:382`. All keep passing unedited. |
| `frogdb-server/crates/core/src/registry.rs` | — | **Read-only evidence.** `register()` debug-asserts `spec().validate()` (`:184-189`); `const fn conn_spec(...) -> CommandSpec` (`:400-416`) is the in-tree precedent for const-context spec construction. Its own 32 test-mock spec sites are **not** swept (out of scope). |
| `frogdb-server/ops/docs-gen/src/main.rs` + `Cargo.toml` | — | **Read-only evidence.** Builds the registry "the same way the server does" (`main.rs:480-485`) with `features = ["cmd-full"]` (`Cargo.toml:20`), dumps **4 of the 13 spec fields** — `name`/`arity`/`flags`/`execution_strategy`, plus derived `family`/`full`/`is_stub` — into `commands.json` (`CommandInfo`, `main.rs:129-150`). `just docs-gen-check` (`Justfile:816`, body `:817`) proves *those four* byte-identical across the full surface; it is **not** a 13-field net (§Testability). |
| `frogdb-server/crates/commands/Cargo.toml` | — | **Read-only evidence.** `default = ["core-profile"]` `:15`; `core-profile` `:18`; **12** opt-in families `:19-30`; `full` `:31-45`. The comment `:12-13` names docs-gen / redis-regression / shard-harness as the consumers that must depend on `full`. |
| `.github/workflows/workflow_gen/.../test.py` | — | **Read-only evidence.** The `cmd-full-build` job `:219-251` already runs `cargo check -p frogdb-commands --features full --all-targets` **and** `--no-default-features --features core-profile`, precisely because (its own comment, `:212-218`) `cargo nextest run --all` unifies `cmd-full` in through docs-gen/redis-regression/shard-harness and therefore never exercises either profile in isolation. This is the precedent for the two-profile acceptance step added below. |
| `Justfile` | — | **Read-only evidence.** `check` `:54-55` and `test` `:78-84` are bare `cargo check`/`cargo nextest run` with `-p <crate>` when a crate is named — **default features, i.e. `core-profile`**, when scoped; workspace-wide when not. `docs-gen-check` `:816`; `lint` `:319-320` = `cargo clippy --all-targets -- -D warnings` at workspace scope; `lint-no-typed-unwrap` `:1012-1040` — the only gate that greps `crates/commands/src/`. |
| `rust-toolchain.toml` / `Cargo.toml` | — | **Read-only evidence.** `1.92.0` / edition 2024 — the compile-check environment. |
| Server/core spec sites (72 + 32) | — | **Explicitly not swept.** §Proposed change (scope). |

## Problem

### 1. The census: seven fields have a mode, four do not

Every count is an exact-string line grep over `crates/commands/src` at HEAD (multi-line values
verified to sum to 296 per field — e.g. `wakes` = 278 None + 13 explicit wakes + 5
comment-then-value sites = 296; `strategy` = 272 Standard + 9 `Blocking {` + 15 `ServerWide(…)` =
296; `access` = 243 + 28 + 20 Positional + 5 Dynamic = 296; `wal` and `event` each sum to 296).

| Field | Modal value | Sites | Share | Defaultable? |
|---|---|---|---|---|
| `mutation` | `frogdb_core::ConnMutation::None` | **296/296** | **100%** | **yes** — the field is *only* meaningful for `ConnectionLevel` commands (`command_spec.rs:496-501`), none of which live in this crate; `validate()` cross-checks it against `strategy` (`:843-853`) |
| `requires_same_slot` | `false` | 282/296 | 95.3% | **yes, with a disclosed residual** — the 14 `true` sites (11 plain + 3 with trailing/interleaved comments, e.g. `stream/read.rs:28-29`) are the cross-slot-atomicity exceptions and stay explicit. See the risk note below: this is the one defaulted field with **no** `validate()` clause and **no** test pin beyond the golden |
| `wakes` | `WaiterWake::None` | 278/296 | 93.9% | **yes** — the 18 wakers stay explicit; `data_adding_commands_wake_blocked_clients` (`register.rs:347`) pins the known-waker set independently |
| `lookup` | `LookupSpec::None` | 273/296 | 92.2% | **yes** — 18 `FirstKey`, 4 `EveryKey`, 1 `Reported` stay explicit |
| `strategy` | `ExecutionStrategy::Standard` | 272/296 | 91.9% | **yes** — 9 `Blocking {…}`, 15 `ServerWide(…)` stay explicit; `validate()` cross-checks `ConnectionLevel` against `wal` (`:830-834`) and against `mutation` (`:843-853`) |
| `reindex` | `frogdb_core::ReindexSpec::None` | 263/296 | 88.9% | **yes** — `validate()` rejects non-`None` reindex on non-WRITE (`:912-917`) and shape-checks `Rename`/`RefreshSecondKey` (`:920-931`) |
| `access` | `AccessSpec::Uniform` | 243/296 | 82.1% | **yes** — `Uniform` derives per-key flags from `CommandFlags::WRITE` (`AccessSpec` `:221`, `Uniform` doc `:222`, variant `:223`), so it is semantically "no special access facts", exactly what a default means |
| `keys` | `KeySpec::First` | 222/296 | 75.0% | **no** — see below |
| `wal` | `WalStrategy::NoOp` | 143/296 | 48.3% | **no** — no mode |
| `event` | `EventSpec::NotApplicable` | 143/296 | 48.3% | **no** — no mode |
| `name` / `arity` / `flags` | — | unique / 296 spellings | — | **no** — identity |

**Why `keys` stays explicit despite a 75% mode.** The line is drawn on **blast radius of a silent
omission**, not on modal share — 75% (`keys`) sits below 95.3% (`requires_same_slot`), but that
ordering is not the reason either lands where it does. Defaulting `keys: First` makes *omission*
indistinguishable from *declaration* for the single most safety-critical routing fact. A keyless
command with `Arity::Fixed(0)` would be caught (`ArityTooSmallForKeys`, `command_spec.rs:823-828`),
but a keyless command with `arity: AtLeast(1)` — the SUBSCRIBE shape — would silently treat
`args[0]` as a key: wrong slot routing, wrong ACL key checks, wrong WAL write-set, and `validate()`
cannot see any of it. 222 `keys: KeySpec::First,` lines stay.

**And the residual on `requires_same_slot`, stated plainly.** It is fair to ask why
`requires_same_slot` is defaulted when it is *also* a routing fact with no `validate()` clause. Its
one consumer is `routing.rs:119` (`if handler.requires_same_slot() { return redirect::crossslot(); }`)
— reached only after the keys already span shards — and it is pinned by **no test** anywhere in the
tree (verified: the only non-literal mentions are the struct field `command_spec.rs:487`, the trait
accessor `command.rs:871-872`, and that one call site). So:

- **What the default costs:** a *future* multi-key command that should be CROSSSLOT-refusing but
  omits the field becomes scatter-eligible instead. That is a real, same-class routing risk.
- **Why it is accepted anyway:** the failure mode is **degradation of an atomicity refusal**, not
  misidentification of which bytes are a key — a wrong `keys` sends the request to the wrong shard
  and hands the wrong string to ACL; a wrong `requires_same_slot` executes the right keys on the
  right shards without the CROSSSLOT guard. And today the author types `requires_same_slot: false,`
  by reflex at 282 sites, which has *identical* effect to omitting it — the prompt is not currently
  buying a decision.
- **What the change does not fix:** a new command still adds a `requires_same_slot: false` golden
  row on regen without anyone deciding anything. The golden pins all 296 **existing** values against
  drift; it does not force a **new** command's author to think. That gap is disclosed, not closed,
  and closing it properly would be a `validate()` or `register.rs`-style allowlist net for
  multi-key CROSSSLOT commands — out of scope here, and worth filing separately if the round wants it.

**Why `wal`/`event` stay explicit despite existing nets.** `WRITE` without an event is rejected by
`validate()` (`:810-812`) and `WRITE` with `NoOp` WAL fails `every_write_command_declares_wal`
unless allowlisted (`register.rs:328-341`) — so defaulting them would *usually* be caught. But
"usually" is doing work there (the nets key off `flags`, which is itself hand-written), 48.3% is
not a mode, and durability facts are exactly the ones this codebase's philosophy says to spell out.
They stay.

### 2. The recurring cost is measured, not hypothetical

Four of the 13 fields were added in the last five weeks of spec history, each by sweeping the full
catalog (`git log` on `command_spec.rs`):

| Commit | Date | Field added | Commands-crate lines added | Tree-wide | Of which modal/no-op |
|---|---|---|---|---|---|
| `7462ecf7` | 2026-07-03 | `lookup` | (pre-dates some sites) | — | 273/296 now `None` |
| `462e12a8` | 2026-07-06 | `strategy` | — | — | 272/296 now `Standard` |
| `ab340681` | 2026-07-20 | `mutation` | **+296 `mutation:` lines** | 394 lines, 92 files, 807 insertions | **296/296 `None`** |
| `38ce99d1` | 2026-07-22 | `reindex` | **+296 `reindex:` lines** | 395 lines, 93 files, 989 insertions | 263/296 `None` |

With `DEFAULT`, the `mutation` commit's commands-crate footprint would have been **one line** (the
`DEFAULT` entry) instead of 296; the `reindex` commit's would have been 33 exception sites + one
default. Today the tree holds **400** static spec sites (296 commands + 72 server + 32 core, exact
grep) — a 14th field is a ~400-site sweep and stays one until this lands.

### 3. What the 1,907 modal lines cost besides bytes

- **Reading:** the average spec block is 16.8 lines (4,966/296) of which ~6.4 restate the mode. The
  exceptional facts — the only ones a reader needs — are buried in uniform filler. (Locality: the
  signal-to-boilerplate ratio of a GET-shaped spec is 6 load-bearing lines out of 17.)
- **Review:** a wrong modal line is invisible in review precisely because 1,900 identical siblings
  train the eye to skip it. The one field where that matters most, `wakes`, needed its own
  regression test (`register.rs:341-346`: "LREM/RPOPLPUSH/LMOVE/ZINCRBY were all silent omissions
  before the spec") — evidence that spelled-out boilerplate does not actually protect the values.
- **Diff noise:** every field addition is a ±300-line diff in this crate alone that reviewers must
  verify is mechanically uniform — by hand, since nothing pins spec values today (§Testability).

## Proposed change

### Architecture

`CommandSpec` is the **interface** between command implementations and every mechanical seam
(dispatch, WAL, events, wakes, reindex, lookup accounting — `command_spec.rs:1-7`). The 296
statics are **implementations** of that interface. This change makes the interface **deeper
without moving its boundary**: same module, same type, same 13 facts derivable by every consumer —
but the *declaration contract* shrinks from "state all 13" to "state your identity (6) and your
exceptions (0–7)". The **implementation** is one associated const plus purely mechanical call-site
rewriting. **Leverage:** every future field addition costs `O(exception sites) + 1` instead of
`O(400)`; every future reader of a spec reads only what distinguishes the command. **Locality:**
the crate-wide mode gets exactly one authoritative spelling, next to the struct it defaults, where
the doc comment already says "One `static` per command".

### (a) The const (in `impl CommandSpec`, `command_spec.rs:793`)

```rust
/// The modal spec: every field a command may omit, at its catalog-wide
/// dominant value, plus inert placeholders for the six fields every spec
/// must state (name/arity/flags/keys/wal/event — see the field table).
/// `validate()` rejects the placeholder name, so a spec that forgets
/// `name:` cannot register.
pub const DEFAULT: CommandSpec = CommandSpec {
    // -- always stated explicitly by policy; placeholders only --
    name: "",
    arity: Arity::Fixed(0),
    flags: CommandFlags::empty(),
    keys: KeySpec::None,
    wal: WalStrategy::NoOp,
    event: EventSpec::NotApplicable,
    // -- the seven defaultable fields, at their modal values --
    access: AccessSpec::Uniform,
    wakes: WaiterWake::None,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::Standard,
    mutation: ConnMutation::None,
    reindex: ReindexSpec::None,
};
```

Plus one `validate()` clause (`SpecError::UnnamedSpec`): `self.name.is_empty()` is rejected. That
closes the only *undetectable* placeholder: a forgotten `arity` surfaces via
`ArityTooSmallForKeys` whenever the spec reads keys, forgotten `wal`/`event` on writes trip the
`register.rs:311`/`:328` nets, and forgotten `flags` diverges from the golden row — but a forgotten
`name` today registers a `""` command with no complaint. Note `flags: CommandFlags::empty()` is a
*legal* live value elsewhere (`server/src/connection/acl_conn_command.rs:38`), which is exactly why
policy — not `validate()` — keeps `flags` explicit.

**The clause goes at the top of `validate()`'s body, immediately before the current first check at
`:810`** — and that position is load-bearing, not stylistic. `validate()` **does** read `self.name`
today, once: `!MULTI_KEY_EMITS_ALLOWLIST.contains(&self.name)` at `:907`, where the allowlist
(`:791`) is `["DEL", "UNLINK", "MSET", "MSETNX"]`. The proposal's conclusion is unchanged — no
existing check *rejects* an empty name, so a forgotten `name:` still registers as `""` silently —
but the interaction matters: a DEL/MSET-shaped spec (blanket `EventSpec::Emits` on a multi-key
`KeySpec`) that forgot its `name` would fall out of the allowlist and fail with
`SpecError::MultiKeyBlanketEmits`, pointing the author at the *event* declaration rather than the
missing name. Putting `UnnamedSpec` first makes the diagnostic name the actual defect in every case.

### (b) The sweep — exact-string awk, commands crate only

Per the sweeping-mechanical-changes preference, the sweep is a text-tool pipeline, not 56 hand
edits. All seven default lines are byte-uniform at 12-space indent (exact-line grep matches the
distribution counts: 296/282/278/273/272/263/243), and every block closes with `};` at 8 spaces
(nested braces — `Emits {…}`, `Blocking {…}`, `NumkeysAt {…}` — close at 12 with a comma, so the
8-space `};` uniquely terminates the static; verified on `hash.rs`, 28 blocks / 469 block-lines):

```bash
cd frogdb-server/crates/commands/src
for f in $(grep -rl ': CommandSpec = CommandSpec {' .); do
  awk '
    /: CommandSpec = CommandSpec \{$/ { inblk = 1; print; next }
    inblk && $0 == "        };" {
        print "            ..CommandSpec::DEFAULT"; inblk = 0; print; next }
    inblk && ($0 == "            access: AccessSpec::Uniform," ||
              $0 == "            wakes: WaiterWake::None," ||
              $0 == "            requires_same_slot: false," ||
              $0 == "            lookup: LookupSpec::None," ||
              $0 == "            strategy: ExecutionStrategy::Standard," ||
              $0 == "            mutation: frogdb_core::ConnMutation::None," ||
              $0 == "            reindex: frogdb_core::ReindexSpec::None,") { next }
    { print }
  ' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done
```

Notes the reviewer should check rather than trust:

- **The block-terminator assumption holds exactly 296 times.** A regex anchored on
  `static \w+: CommandSpec = CommandSpec \{` … `\n        };` matches **296** regions across the
  56 files — the same number as the site count, so no block is over- or under-run by the awk.
- **`..CommandSpec::DEFAULT` takes no trailing comma and must be last** — insertion immediately
  before the closing brace guarantees both.
- **rustfmt cannot collapse a post-sweep block onto one line**, so the diff criterion below stays
  meaningful. Rendering every swept block as its hypothetical single line, the **narrowest** result
  is **223 columns** (`set.rs`, 6 surviving fields); the repo has no `rustfmt.toml`, so
  `max_width` is the default **100**. Every block stays multi-line and the only deleted lines are
  the seven exact strings.
- **`clippy::needless_update` cannot fire today**: `mutation:` is deleted at *all 296 sites* (100%
  modal), so the FRU base always supplies at least one field at every site. This immunity is
  **contingent on that 296/296**: a future site that spells all 13 fields *and* appends
  `..CommandSpec::DEFAULT` would trip `needless_update`, which is warn-by-default and therefore an
  error under `just lint`'s `-D warnings`. Self-correcting — the fix is to drop `..DEFAULT` at that
  one site — but it is a real future interaction, not an impossibility.
- **Comment-carrying default-value lines are left alone by construction** (exact-string match);
  they are all non-default values anyway (e.g. `stream/read.rs:28-29`).

**Second mechanical pass: the import prune (scripted, not ad hoc).** This is not a tail of the
work — it is roughly half the Commit-3 diff. Exact counts at HEAD, derived by deleting the seven
strings in a simulator and re-counting each name's surviving mentions per file:

| Name | Files where the import dies | Why |
|---|---|---|
| `LookupSpec` | **48** | only mention was `lookup: LookupSpec::None,` |
| `ExecutionStrategy` | **48** | only mention was `strategy: ExecutionStrategy::Standard,` |
| `WaiterWake` | **48** | only mention was `wakes: WaiterWake::None,` |
| `AccessSpec` | **38** | only mention was `access: AccessSpec::Uniform,` |
| `ConnMutation` / `ReindexSpec` | **0** | written fully qualified (`frogdb_core::…`), never imported |

**182 dead import names across 54 of the 56 files.** And **all 182 sit inside wrapped multi-name
`use frogdb_core::{…};` blocks** — zero are standalone single-name `use` lines that a
`sed '/^use .*LookupSpec;$/d'` could take out. `hash.rs:12-16` is typical: one
`use frogdb_core::{…};` listing 17 names across 3 wrapped lines. So the prune is a *token* removal
inside a brace list followed by a rustfmt re-wrap of the surviving names, which means the **import
hunks, not the spec hunks, dominate the Commit-3 diff** and are where a reviewer's time will go.
Procedure:

1. Run the spec sweep above.
2. Run a scripted prune — a second small awk/Python pass over the same 56 files: for each of the
   four names, if the file no longer mentions it outside its `use` block, drop that token from the
   brace list. Do **not** hand-edit 54 files; do **not** drive it off clippy warnings one at a time.
3. `just fmt frogdb-commands` — rustfmt re-wraps every touched `use` block.
4. Compile-check under **both** feature profiles (below) — this is where the prune is actually
   verified.

**`just check frogdb-commands` alone does not see about half of it.** `check` with a crate argument
is `cargo check -p <crate> --all-targets` (`Justfile:54-55`) at **default features = `core-profile`**,
so the 135 spec sites in the 12 gated families — and therefore their dead imports — do not compile
in the author's loop at all. They first appear at workspace-scope `just check` / `just lint` /
`docs-gen-check`, or in CI's `cmd-full-build` job. The commit-3 procedure therefore runs both
directions explicitly, mirroring that CI job verbatim:

```bash
cargo check -p frogdb-commands --no-default-features --features core-profile --all-targets
cargo check -p frogdb-commands --features full --all-targets
```

**Scope: the commands crate only (296 of 400 sites).** The 72 `frogdb-server` sites are excluded
for two reasons, stated accurately:

- **Ownership, and the biggest cluster is not where the earlier draft said.** Only **25** of the 72
  sit under `server/src/connection/*` (`scripting_conn_command.rs` 8, `observability_conn_command.rs`
  4, three files with 2, seven with 1). The **largest single cluster is 26 sites in
  `server/src/commands/search.rs`**, which is proposal **71**'s primary file — followed by
  `commands/server.rs` and `commands/replication.rs` at 6 each. So the collision surface is
  concentrated in a file this proposal has no claim on, and spread thin across the connection files
  that 80/84/86/87/88 touch. Excluding the server crate is right; "they're all in connection/*" was
  the wrong reason.
- **The tooling does not transfer — only the idiom does.** Server specs are **module-level**
  statics with fields at **4-space** indent (`acl_conn_command.rs:33-46`: `static ACL_SPEC:
  CommandSpec = CommandSpec {` at column 0), whereas the commands-crate sweep is anchored on 8-space
  `static` / 12-space fields / an 8-space `};` terminator. A server sweep needs a **second,
  differently-indented awk** and its own terminator check — not a re-run of the script above. What
  transfers is the *idiom* (`..CommandSpec::DEFAULT` in a module-level static is one of the three
  positions compiled in §The compile check), not the mechanism.

The 32 `frogdb-core` sites are almost all `#[cfg(test)]` mocks (`registry.rs`, `shard/*` test mods)
where `conn_spec`-style helpers already exist. Both crates adopt the const opportunistically later
at zero coordination cost — it is `pub`. Declared as follow-up, not scope.

### (c) Alternatives considered and rejected

- **(R1) `const fn base(name, arity, flags, …) -> CommandSpec`** (compiles, verified; precedent
  `registry.rs:400`). Forces the six explicit-by-policy fields at the type level — stronger than
  policy — but turns six *named* fields into six *positional* arguments at 296 sites, which is a
  readability regression this catalog exists to avoid, and makes future "move a field between the
  explicit and defaulted sets" a signature change at every site instead of a line deletion. The
  named-field + `DEFAULT` form keeps every declaration self-describing. Rejected on locality.
- **(R2) `impl Default for CommandSpec`.** `Default::default()` is not `const`-callable in statics
  on stable; also `Default` implies "a usable value", which the placeholder-name spec is
  deliberately not. An associated const carries no such claim. Rejected.
- **(R3) A `spec!` macro wrapping construction.** Machinery on top of the language feature that
  already does the job; hides the struct literal from grep/rust-analyzer; this tree is actively
  *removing* declarative macros (proposal 85). Rejected.
- **(R4) Also defaulting `keys` (75%)** — rejected in §Problem 1 (silent `args[0]`-as-key on
  keyless `AtLeast(n≥1)` commands is invisible to `validate()`).

### Deletion test

- **`CommandSpec::DEFAULT`** — delete it and 1,907 modal lines must be re-spelled across 56 files,
  and the next field addition reverts to a 400-site sweep. **Earns its keep.**
- **The 1,907 modal field lines** — delete them (this change) and *nothing reappears*: every spec
  denotes the same value, proven by the 296-row golden under `--features full` (all 13 fields) and
  corroborated by `docs-gen-check` (4 of 13, full surface). **They do not earn their keep; deleting
  them is the proposal.**
- **The six explicit-by-policy field lines (name/arity/flags/keys/wal/event, ~1,470 lines)** —
  deleting them would trade visible identity/durability/routing facts for silent placeholders that
  `validate()` only partially catches. **They earn their keep and stay.**

## Testability improvement

Honest framing first: today **no test pins spec values as data**. The `register.rs` nets check
*cross-field invariants* and a curated exception list (`:298`, `:311`, `:328`, `:347`, `:382`);
`docs-gen`'s `commands.json` pins exactly four of 13 spec fields (`CommandInfo`, `main.rs:129-150`:
name, arity, flags, execution_strategy — the other three serialized keys are derived, not spec
fields). Nine fields — `keys`, `access`, `wal`, `wakes`, `event`, `requires_same_slot`, `lookup`,
`mutation`, `reindex` — are asserted nowhere as a catalog. A sweep that rewrites 296 declarations
without such a net would be reviewable only by trusting the awk.

One thing the net does *not* have to worry about: there is no registered-vs-declared blind spot.
`lib.rs` contains exactly **296** `registry.register(` calls and the crate contains exactly **296**
`CommandSpec` statics — a clean 1:1, so "every registered command matches its golden row" and
"every static is pinned" are the same statement.

**Feature coverage is the part that has to be got right.** 135 of the 296 sites (45.6%) sit behind
the 12 opt-in families, and they are exactly the sites the default dev loop never compiles:

| Gated family | Spec sites | Gated family | Spec sites |
|---|---|---|---|
| `json` (6 files) | 21 | `geo` | 10 |
| `timeseries` | 17 | `bloom` | 10 |
| `stream` (5 files) | 17 | `topk` | 7 |
| `cuckoo` | 12 | `cms` | 6 |
| `tdigest` | 12 | `event-sourcing` (6 files) | 6 |
| `vectorset` (12 files) | 12 | `hyperloglog` | 5 |
| | | **gated total** | **135** |
| | | **always-on (`core-profile`)** | **161** |

`just test <crate>` is a bare `cargo nextest run -p <crate>` (`Justfile:78-84`) and
`frogdb-commands`' default is `core-profile` (`Cargo.toml:15`), so **`just test frogdb-commands`
exercises the golden over 161 of 296 sites**. For the other 135, the only surviving check would be
`docs-gen-check`, which serializes **4 of 13** fields — leaving six defaulted fields plus
`keys`/`wal`/`event` verified by *nothing* across 45.6% of the sweep. That is not an acceptable net
for a mechanical rewrite, so commit 3 runs the golden under `full` explicitly (below).

Two facts make that cheap rather than awkward. First, workspace-scope commands already resolve
`frogdb-commands` to `full` by feature unification through docs-gen / redis-regression /
shard-harness (verified with `cargo tree -e features -i frogdb-commands`; CI's own comment at
`test.py:212-218` says the same) — so `just test`, `just check`, and `just lint` with **no** crate
argument do compile and run the gated half. Second, CI's `cmd-full-build` job (`test.py:219-251`)
already runs `cargo check -p frogdb-commands --features full --all-targets` and its
`--no-default-features --features core-profile` twin. The acceptance step added below is that same
pair, plus the golden run, executed locally instead of only on the PR.

**On the CLAUDE.md "don't alternate feature flags in an iteration loop" rule:** these are
end-of-commit verification runs, executed **once each, in sequence, after the work is done** — not
a loop. The rule targets thrashing the build cache by flipping flags between commands while
iterating; a terminal `core-profile` check followed by a `full` check leaves two stable cargo
fingerprints and thrashes nothing. Nor is an allowlist edit owed: nothing in `just lint` or
`scripts/` gates which invocation may pass `--features full` (verified — no Justfile or script
mentions `cmd-full`/`core-profile` at all), and the CLAUDE.md "allowlisted tooling" language is
about which *crates declare* `cmd-full` in `Cargo.toml` (docs-gen, redis-regression, shard-harness —
`commands/Cargo.toml:12-13`). Running the same command CI runs adds no new allowlist surface.

**So the net lands first (red-green):**

1. **Commit 1 — golden test, before any spec is touched.**
   `crates/commands/tests/spec_golden.rs`: build a `CommandRegistry`
   (`frogdb_core::CommandRegistry`), call `frogdb_commands::register_all` (`lib.rs:63`), and for
   every registered full command compare `format!("{:?}", cmd.spec())` (Debug derived,
   `command_spec.rs:468` — covers all 13 fields) against its row in `tests/spec_golden.txt`,
   sorted by name. **Subset semantics**: every *registered* command must match its golden row;
   golden rows whose command is not compiled in (feature-gated family) are skipped — so the same
   golden file is correct under `core-profile` (161 rows checked) and under `full` (all 296).
   Regeneration is an `#[ignore]`d writer test, run once with the crate's own `full` feature so the
   file is authored complete.
2. **Commit 2 — `DEFAULT` + `UnnamedSpec` validate clause** (core; golden still green untouched).
3. **Commit 3 — the sweep + the scripted import prune.** Acceptance, in order:
   - **Diff shape.** `git diff -U0 -- frogdb-server/crates/commands | grep '^-' | grep -v '^---' |
     sed 's/^-//' | sort -u` — the `grep -v '^---'` is required, since `git diff`'s own `--- a/…`
     file headers begin with `-` and would otherwise pollute the set. Result must be **exactly the
     seven default strings plus the import lines** (the latter reviewed against the 182/54 table in
     §(b), so an unexpected import deletion is visible). `git diff -U0 … | grep -c
     '^+.*\.\.CommandSpec::DEFAULT'` must be **296**.
   - **Compile, core profile.** `cargo check -p frogdb-commands --no-default-features --features
     core-profile --all-targets` — 161 sites.
   - **Compile, full surface.** `cargo check -p frogdb-commands --features full --all-targets` —
     the other 135, and where roughly half the import prune is first verified.
   - **Golden, core profile.** `just test frogdb-commands` — 161 rows unchanged.
   - **Golden, full surface (the step that closes MAJOR-1's hole).**
     `cargo nextest run -p frogdb-commands --features full` — all **296** rows unchanged, all 13
     fields each. This is the only check in the battery that verifies the defaulted fields of the
     gated 45.6%.
   - `just docs-gen-check` (`Justfile:816`) — independent full-profile build through docs-gen's
     `cmd-full` dependency (`ops/docs-gen/Cargo.toml:20`) proving `commands.json` et al.
     byte-identical. Corroboration on 4 fields, not the primary net.
   - `just lint` — workspace-scope clippy `-D warnings` (feature-unified to `full`, so it catches
     any import the scripted prune missed *or* over-pruned, in both halves) + the gate family +
     `lint-failure-modes` (vacuously clean here: zero `FM-` tags in the crate).

**The golden test outlives the sweep.** Permanently, any spec edit — including a defaulted field
silently changing meaning if someone edits `DEFAULT` — shows up as a named-command diff in one
file. That converts this catalog's dominant historical bug class ("silent omission of a modal
value that should have been an exception" — the `wakes` incident, `register.rs:341-346`) from
invisible-in-review to a forced golden-file diff. It is also precisely the net the *next* field
addition needs: add the field, regen the golden once, and every subsequent accidental value change
is pinned. No existing test is edited, moved, or retagged by any of the three commits.

## Risks / scope boundaries vs siblings

### The sweep conflicts with every commands-crate proposal — ruling: land SOLO, and land LAST

The sweep rewrites the interior of all 296 spec blocks in 56 files. Any concurrent edit inside or
adjacent to those blocks conflicts textually. Verified sibling overlaps (grep over committed
proposals for `crates/commands`):

| Sibling | Commands-crate footprint | Overlap with 90 |
|---|---|---|
| **67** (server small dedups, SV6) | `commands/src/timeseries.rs` — 4 identical bodies `:1059-1190` | same file as 17 spec sites |
| **70** (acl-registry-consult) | `basic.rs:145-442`, `generic.rs:355-480`, `stream/info.rs`, `stream/consumer_groups.rs`, `json/basic.rs` — subcommand-arm declarations | same files as 8+16+… spec sites |
| **80** (response-wire fold) | `blocking.rs`, `stream/read.rs` — "mechanical churn" at `BlockingNeeded` producers | same files as 8+2 spec sites |
| **84** (blocking-op dedupe) | read-only evidence only | none |
| **86** (resp3 egress codec) | read-only (`lint-no-typed-unwrap` citation) | none |
| **89** (probabilistic chunk codec, **now authored**) | **`bloom.rs` + `cuckoo.rs` only**: ~124 deleted codec-**body** lines (52 from `bloom.rs:612-663`, 72 from `cuckoo.rs:672-743`), two hand-encoder regions replaced (`bloom.rs:532-548`, `cuckoo.rs:591-611`), and 2 lines in `commands/Cargo.toml` (89:125-127) | same **files** as 10+12 = **22** spec sites; **block-level conflict unlikely** — 89's deleted regions are `execute()` bodies, none of which contain a `static SPEC` block (verified: bloom's 10 statics are at `:22/116/162/213/249/290/391/461/499/574`, cuckoo's 12 at `:23/104/…/635`, all outside 89's ranges). Still a shared-file rebase, so the ruling is unchanged |

**Last, not first**, for a concrete asymmetry: this change is *regenerable* — after any sibling
lands, re-running the awk plus the acceptance battery costs minutes and cannot be wrong while the
golden is green. Landing 90 first instead forces every sibling with a commands-crate hunk to
rebase across ~1,900 deleted lines in the exact regions they edit. A mechanical sweep should be
the round's last commands-crate writer. Concretely: **commits 1–2 (golden + `DEFAULT`) can land
any time — early is better, since the golden protects the siblings too; commit 3 (the sweep) lands
after 67/70/80/89 are merged**, re-derived from the merged tree, never merged across them.

### Other risks

- **A reader must know `DEFAULT` to reconstruct a spec's full 13 facts.** True, and bounded: the
  const is one screen in the file that defines the struct, and *runtime* introspection is
  unchanged — `Debug`, `COMMAND` output, `docs-gen`, and the golden rows all show fully-resolved
  values. The declaration form trades 6.4 lines of restated mode for one indirection that is the
  same at all 296 sites.
- **Future omission errors on defaultable fields** (new STORE-family command forgets `wakes`).
  Not a new risk — today the author writes `WaiterWake::None,` by reflex with identical effect —
  and the nets are unchanged: `register.rs:347` pins wakers, `:382` pins events, golden pins
  everything by value. The change does remove the *prompt* that a field exists; the 14th-field
  counter-argument is that the prompt demonstrably didn't prevent the `wakes` omissions the test
  now guards (`:341-346`).
- **`requires_same_slot` residual** — the one defaulted field with no `validate()` clause, no
  `register.rs` net, and exactly one consumer (`routing.rs:119`). Existing values are pinned by the
  golden; *new* commands are not forced to decide. Disclosed and quantified in §Problem 1.
- **`validate()` growth** — one trivial clause, placed first in the body so it pre-empts the
  `MULTI_KEY_EMITS_ALLOWLIST` name read at `:907`; `validate` is already run per-registration in
  debug (`registry.rs:184-189`) and by `register.rs:298` in CI. No hot-path cost (`DEFAULT` is a
  compile-time constant; FRU in a `static` is evaluated at compile time — no runtime init, same
  `&'static CommandSpec` as today).
- **Locked areas / spec-first:** `frogdb-commands` and `frogdb-core` are not locked crates (locked:
  txn/vll, persistence/recovery, replication×2, cluster×2 — ADRs 0002-0004). Zero `FM-` tags in
  `crates/commands/src`; `command_spec.rs` and `registry.rs` carry none in the edited regions
  (`cd52ce0c` tagged cluster tests elsewhere). No failure-mode spec row mentions spec declaration
  shape. `just mutants-diff` not owed.
- **Seam gates:** the only commands-scoped gate is `lint-no-typed-unwrap` (`Justfile:1012-1040`),
  which greps for `as_*_mut().unwrap()` / `.ok_or(…WrongType…)` — the sweep deletes only the seven
  field strings and inserts `..CommandSpec::DEFAULT`; neither pattern can be created. The
  continuation-lock gate parses `core/src/shard/message.rs`, clock-seam allowlists
  `types/src/clock.rs` — no command file in any gate's parse set. `lint-turmoil-features`
  unaffected (no `cfg` edited).
- **Feature matrix — the sharpest operational risk, and the reason the battery grew.** 45.6% of
  the swept sites (135/296, table in §Testability) are invisible to *both* crate-scoped dev-loop
  commands: `just check frogdb-commands` and `just test frogdb-commands` are `-p`-scoped and
  therefore `core-profile`. A battery built only from crate-scoped commands would pin 161 sites by
  value and leave the other 135 covered at 4 of 13 fields by `docs-gen-check`. Commit 3 therefore runs
  both `cargo check` profiles and the golden under `--features full` — one terminal run each, the
  same commands CI's `cmd-full-build` job already runs, no flag alternation inside an iteration
  loop, two stable fingerprints.
- **Security:** no parse boundary, no auth, no untrusted input touched. Nothing to file.

## Effort

| Part | Effort | Notes |
|---|---|---|
| Commit 1 — golden test + golden file | **S code / M artifact** | ~80 lines of test (subset semantics ~15 of them); first test in the crate. The *artifact* is the weight: a ~**75 KB**, 296-row generated file that a reviewer must accept on the strength of its generator, not by reading it |
| Commit 2 — `DEFAULT` + `UnnamedSpec` | **S** | ~30 lines in `command_spec.rs`; one new `SpecError` variant + message; clause inserted at the top of `validate()` |
| Commit 3 — the sweep + prune | **M if scripted, L if not** | Two mechanical passes: the spec awk (56 files, −1,907/+296, net **−1,611**) and the import prune (**182 names in 54 files**, all inside wrapped `use` blocks, so rustfmt re-wrap noise dominates the diff). **M** only if the prune is a script and the compile check runs under **both** feature profiles; done ad hoc off clippy warnings under `core-profile` alone it is **L** — 54 hand edits plus a second discovery round when the gated 135 finally compile |

**Total: M, at the top of the band.** Landable in one PR of three commits; **solo** in the commands
crate, **after** 67 / 70 / 80 / 89 per the ruling above. No behavior change anywhere — the
296-row golden under `--features full`, plus `docs-gen-check` byte-identity, are the proof, not a
claim.

## Independently-landable hotfixes

**None claimed — no live defect found.** The census surfaced two latent oddities, recorded here
rather than inflated: (1) `stream/read.rs:28-29` interleaves a comment *between* `requires_same_slot:`
and its value `true` — legal, rustfmt-stable, merely hard to grep (it is why naive field-line
counts read 282+13 instead of 282+14); the sweep leaves it untouched and correct. (2) The
`register.rs` nets guard `wakes`/`event`/`wal` but no test anywhere pins `access`, `lookup`,
`mutation`, `requires_same_slot`, or `keys` values — `requires_same_slot` is the starkest: one
consumer (`routing.rs:119`), no `validate()` clause, no test. That gap is not fixed by a hotfix but
by commit 1, which is independently landable **today** and valuable even if the sweep never follows.
