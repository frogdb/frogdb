# Proposal 90 — `CommandSpec::DEFAULT`: 296 statics spell out 13 fields; seven of those fields are the same value at ≥82% of sites

Round 38 · lane: commands + types · candidate **CT2** · effort **S per file / M total** · **no
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
This proposal is explicitly distinct, and the distinction is structural, not rhetorical:

- **Nothing is removed or unified.** All 13 `CommandSpec` fields keep their exact types, names,
  variants, and meanings. `ExecutionStrategy`, `WalStrategy`, `WaiterWake`, `LookupSpec`,
  `AccessSpec`, `ReindexSpec`, `ConnMutation` are untouched as types. No enum loses a variant; no
  two concepts become one.
- **Only the *obligation to spell modal values* is dropped.** A spec site keeps stating every fact
  that is exceptional for its command; the facts equal to the crate-wide mode come from one shared
  `pub const DEFAULT` via struct-update syntax. Every spec still denotes exactly the same 13-field
  value — the golden test (§Testability) proves it bit-for-bit.
- **ADR check:** `adr/` contains only `0001-operator-imports-server-config-crate`,
  `0002-txn-orchestration-behind-txnhost-seam`, `0003-persistence-durability-seams`,
  `0004-replication-runtime-seams`. The strategy-folding rejection is **not recorded in any ADR**
  (grep for "strategy"/"folding" across `adr/*.md` and `proposals/INDEX.md` returns nothing), so
  there is no recorded ruling this proposal could violate; it is nonetheless drawn narrowly to stay
  outside that candidate's territory.

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
−1,611**, zero semantic change, proven by a golden test landed *before* the sweep plus
byte-identical `docs-gen` output. After the change, a spec states its identity and its exceptions;
the mode is stated once.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/core/src/command_spec.rs` | 1778 | **Primary (the interface).** Struct `:469-507` (13 fields); `impl CommandSpec` `:793` gains `pub const DEFAULT` (~20 lines); `validate()` `:807+` gains one clause: reject `name.is_empty()` (new `SpecError` variant) so a spec that accidentally omits `name:` fails the registry debug-assert (`registry.rs:184-188`) and `every_full_command_spec_validates` (`register.rs:298`) instead of registering as `""`. |
| `frogdb-server/crates/commands/src/**` (56 files) | — | **Primary (the sweep).** 296 sites, uniform shape `static SPEC: CommandSpec = CommandSpec {` at 8-space indent, fields at 12 (verified: the seven exact default strings match 296/282/278/273/272/263/243 times with `^            ` anchoring — indentation is uniform). Largest: `hash.rs` 28 sites, `string.rs` 23, `timeseries.rs`/`set.rs`/`list.rs` 17 each. Files in 13 feature-gated families (`lib.rs:27-59`) are swept identically — the edit is per-site, not per-feature. |
| `frogdb-server/crates/commands/tests/spec_golden.rs` + `tests/spec_golden.txt` | **new** | **Primary (the net).** Golden test pinning all 13 fields of all 296 specs via `Debug` (derived, `command_spec.rs:468`); lands **before** the sweep. §Testability. The crate currently has **no tests/ dir at all** — this is its first integration test. |
| `frogdb-server/crates/server/src/server/register.rs` | — | **Read-only evidence.** The existing cross-field nets: `every_full_command_spec_validates` `:298`, `every_write_command_declares_event` `:311`, `every_write_command_declares_wal` `:328` + `WAL_NOOP_ALLOWLIST` `:268`, `data_adding_commands_wake_blocked_clients` `:347`, `multi_key_commands_declare_accurate_events` `:382`. All keep passing unedited. |
| `frogdb-server/crates/core/src/registry.rs` | — | **Read-only evidence.** `register()` debug-asserts `spec().validate()` (`:184-188`); `const fn conn_spec(...) -> CommandSpec` (`:400-416`) is the in-tree precedent for const-context spec construction. Its own 32 test-mock spec sites are **not** swept (out of scope). |
| `frogdb-server/ops/docs-gen/src/main.rs` + `Cargo.toml` | — | **Read-only evidence.** Builds the registry "the same way the server does" (`main.rs:480-485`) with `features = ["cmd-full"]` (`Cargo.toml:20` — the allowlisted full-surface consumer per `commands/Cargo.toml:12-14`), dumps `name`/`arity`/`flags`/`execution_strategy` per command into `commands.json` (`main.rs:129-150`). `just docs-gen-check` (`Justfile:816-817`) is acceptance: full-profile compile **and** byte-identity of generated docs in one command. |
| `frogdb-server/crates/commands/Cargo.toml` | — | **Read-only evidence.** `default = ["core-profile"]` `:15`; 13 opt-in families; `full` `:31-45`. |
| `Justfile` | — | **Read-only evidence.** `docs-gen-check` `:816`; `lint-no-typed-unwrap` `:1012-1040` — the only gate that greps `crates/commands/src/`. |
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
| `mutation` | `frogdb_core::ConnMutation::None` | **296/296** | **100%** | **yes** — the field is *only* meaningful for `ConnectionLevel` commands (`command_spec.rs:496-501`), none of which live in this crate; `validate()` cross-checks it against `strategy` (`:844-857`) |
| `requires_same_slot` | `false` | 282/296 | 95.3% | **yes** — the 14 `true` sites (11 plain + 3 with trailing/interleaved comments, e.g. `stream/read.rs:28-29`) are the cross-slot-atomicity exceptions and stay explicit |
| `wakes` | `WaiterWake::None` | 278/296 | 93.9% | **yes** — the 18 wakers stay explicit; `data_adding_commands_wake_blocked_clients` (`register.rs:347`) pins the known-waker set independently |
| `lookup` | `LookupSpec::None` | 273/296 | 92.2% | **yes** — 18 `FirstKey`, 4 `EveryKey`, 1 `Reported` stay explicit |
| `strategy` | `ExecutionStrategy::Standard` | 272/296 | 91.9% | **yes** — 9 `Blocking {…}`, 15 `ServerWide(…)` stay explicit; `validate()` cross-checks `ConnectionLevel` against `wal` and `mutation` (`:831-857`) |
| `reindex` | `frogdb_core::ReindexSpec::None` | 263/296 | 88.9% | **yes** — `validate()` rejects non-`None` reindex on non-WRITE (`:912-917`) and shape-checks `Rename`/`RefreshSecondKey` (`:920-931`) |
| `access` | `AccessSpec::Uniform` | 243/296 | 82.1% | **yes** — `Uniform` derives per-key flags from `CommandFlags::WRITE` (`:221-229` doc), so it is semantically "no special access facts", exactly what a default means |
| `keys` | `KeySpec::First` | 222/296 | 75.0% | **no** — see below |
| `wal` | `WalStrategy::NoOp` | 143/296 | 48.3% | **no** — no mode |
| `event` | `EventSpec::NotApplicable` | 143/296 | 48.3% | **no** — no mode |
| `name` / `arity` / `flags` | — | unique / 296 spellings | — | **no** — identity |

**Why `keys` stays explicit despite a 75% mode.** Defaulting `keys: First` makes *omission*
indistinguishable from *declaration* for the single most safety-critical routing fact. A keyless
command with `Arity::Fixed(0)` would be caught (`ArityTooSmallForKeys`, `command_spec.rs:824-829`),
but a keyless command with `arity: AtLeast(1)` — the SUBSCRIBE shape — would silently treat
`args[0]` as a key: wrong slot routing, wrong ACL key checks, and `validate()` cannot see it. 75%
is not a high enough mode to buy that risk; 222 `keys: KeySpec::First,` lines stay.

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
`register.rs:311/:328` nets, and forgotten `flags` diverges from the golden row — but a forgotten
`name` today would register a `""` command with no complaint (`validate()` never reads `name`;
verified `:807-931`). Note `flags: CommandFlags::empty()` is a *legal* live value elsewhere
(`server/src/connection/acl_conn_command.rs:38`), which is exactly why policy — not `validate()` —
keeps `flags` explicit.

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

- **`..CommandSpec::DEFAULT` takes no trailing comma and must be last** — insertion immediately
  before the closing brace guarantees both.
- **`clippy::needless_update` cannot fire**: `mutation:` is deleted at *all 296 sites* (100%
  modal), so the FRU base always supplies at least one field.
- **Comment-carrying default-value lines are left alone by construction** (exact-string match);
  they are all non-default values anyway (e.g. `stream/read.rs:28-29`).
- **Unused imports are the expected follow-on breakage**: a file whose only `LookupSpec` mention
  was `LookupSpec::None` lines now has a dead import, and `just lint`'s `-D warnings` fails on it.
  `just check frogdb-commands` after the sweep lists them; prune with `sed -i` per warning (a
  dozen-odd files at most). Then `just fmt frogdb-commands`.

**Scope: the commands crate only (296 of 400 sites).** The 72 `frogdb-server` sites sit in
connection/command files owned by concurrent proposals (80, 84, 86, 87, 88 all edit
`server/src/connection/*`), and the 32 `frogdb-core` sites are almost all `#[cfg(test)]` mocks
(`registry.rs`, `shard/*` test mods) where `conn_spec`-style helpers already exist. Both adopt
`..CommandSpec::DEFAULT` opportunistically later at zero coordination cost — the const is `pub`
and the idiom is proven for their shapes too (const item, static item, fn-local static). Declared
as follow-up, not scope.

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
  denotes the same value, proven by the golden file byte-diff and `docs-gen-check`. **They do not
  earn their keep; deleting them is the proposal.**
- **The six explicit-by-policy field lines (name/arity/flags/keys/wal/event, ~1,470 lines)** —
  deleting them would trade visible identity/durability/routing facts for silent placeholders that
  `validate()` only partially catches. **They earn their keep and stay.**

## Testability improvement

Honest framing first: today **no test pins spec values as data**. The `register.rs` nets check
*cross-field invariants* and a curated exception list (`:298`, `:311`, `:328`, `:347`, `:382`);
`docs-gen`'s `commands.json` pins exactly four of 13 fields (`main.rs:131-150`: name, arity, flags,
execution_strategy). Nine fields — `keys`, `access`, `wal`, `wakes`, `event`,
`requires_same_slot`, `lookup`, `mutation`, `reindex` — are asserted nowhere as a catalog. A sweep
that rewrites 296 declarations without such a net would be reviewable only by trusting the awk.

**So the net lands first (red-green):**

1. **Commit 1 — golden test, before any spec is touched.**
   `crates/commands/tests/spec_golden.rs`: build a `CommandRegistry`
   (`frogdb_core::CommandRegistry`), call `frogdb_commands::register_all` (`lib.rs:63`), and for
   every registered full command compare `format!("{:?}", cmd.spec())` (Debug derived,
   `command_spec.rs:468` — covers all 13 fields) against its row in `tests/spec_golden.txt`,
   sorted by name. **Subset semantics**: every *registered* command must match its golden row;
   golden rows whose command is not compiled in (feature-gated family) are skipped — so the same
   golden file is correct under `core-profile` (the default `just test frogdb-commands` run,
   pinning the always-on families) and under `full` (regen mode / any full-surface consumer).
   Regeneration is an `#[ignore]`d writer test, run once with the crate's own `full` feature — a
   one-off, not an iteration loop, so the CLAUDE.md feature-thrash warning doesn't bite (and the
   full-feature build lands under its own cargo fingerprint, leaving the default cache untouched).
2. **Commit 2 — `DEFAULT` + `UnnamedSpec` validate clause** (core; golden still green untouched).
3. **Commit 3 — the sweep.** Acceptance, in order:
   - `git diff -U0 -- frogdb-server/crates/commands | grep '^-' | sed 's/^-//' | sort -u` — must
     be **exactly the seven default strings** (plus import-prune lines, reviewed separately);
     `grep -c '^+.*\.\.CommandSpec::DEFAULT'` must be **296**.
   - `just check frogdb-commands` — core-profile compile.
   - `just test frogdb-commands` — **golden file unchanged, test green**: bit-identical specs.
   - `just docs-gen-check` (`Justfile:816`) — compiles the **full** profile (docs-gen's own
     `cmd-full` dependency, `ops/docs-gen/Cargo.toml:20` — the allowlisted consumer, so both
     feature sets are compile-verified without hand-flipping flags) *and* proves `commands.json`
     et al. byte-identical.
   - `just lint` — clippy `-D warnings` (catches every import orphaned by the sweep) + the gate
     family + `lint-failure-modes` (vacuously clean here: zero `FM-` tags in the crate).

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
| **89** (chunk codec, in-flight, this round) | commands crate (per orchestrator) | unknown until authored — assume full overlap |

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
- **`validate()` growth** — one trivial clause; `validate` is already run per-registration in
  debug (`registry.rs:184-188`) and by `register.rs:298` in CI. No hot-path cost (`DEFAULT` is a
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
- **Feature matrix:** both profiles are compiled by the acceptance battery itself
  (`check` = core-profile, `docs-gen-check` = cmd-full) with no manual flag alternation and no
  default-cache invalidation (separate fingerprints).
- **Security:** no parse boundary, no auth, no untrusted input touched. Nothing to file.

## Effort

| Part | Effort | Notes |
|---|---|---|
| Commit 1 — golden test + golden file | **S** | ~80 lines of test + generated data file; first test in the crate; subset semantics ~15 of those lines |
| Commit 2 — `DEFAULT` + `UnnamedSpec` | **S** | ~30 lines in `command_spec.rs`; one new `SpecError` variant + message |
| Commit 3 — the sweep | **S per file, M in aggregate** | 56 files, −1,907/+296 (net **−1,611**) via one awk invocation; import prune ~a dozen files; then `fmt` + the 5-step acceptance battery. Mechanical throughout — the only judgement calls (which fields default, at what values) are fixed in commit 2 |

**Total: M.** Landable in one PR of three commits; **solo** in the commands crate, **after** 67 /
70 / 80 / 89 per the ruling above. No behavior change anywhere — the golden file and
`docs-gen-check` byte-identity are the proof, not a claim.

## Independently-landable hotfixes

**None claimed — no live defect found.** The census surfaced two latent oddities, recorded here
rather than inflated: (1) `stream/read.rs:28-29` interleaves a comment *between* `requires_same_slot:`
and its value `true` — legal, rustfmt-stable, merely hard to grep (it is why naive field-line
counts read 282+13 instead of 282+14); the sweep leaves it untouched and correct. (2) The
`register.rs` nets guard `wakes`/`event`/`wal` but no test anywhere pins `access`, `lookup`,
`mutation`, `requires_same_slot`, or `keys` values — that gap is not fixed by a hotfix but by
commit 1, which is independently landable **today** and valuable even if the sweep never follows.
