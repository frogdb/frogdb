# Spec: compatibility/command-matrix.mdx
Status: generated (new)
Audiences: A1, A2

Goal: The reader can look up any Redis 8.x command and see whether FrogDB
supports it (supported / partial / unsupported) with a short note, grouped by
command family. This page is the authoritative command-surface documentation
for FrogDB — there are no per-command reference pages (PLAN §3). A skeptic (A2)
should be able to confirm coverage of a specific command in seconds; an
evaluator (A1) should be able to scan a family and judge fit. The table is
generated from the live command registry joined with the Redis 8.x command set
and the regression-test exclusions, so it cannot drift from the binary.

Not in scope:
- Per-command syntax/arguments/return docs. Standard Redis behavior is
  documented at redis.io; FrogDB documents deltas on
  [Overview & differences](/compatibility/overview/). FrogDB-original families
  (ES.*) and Redis-Stack families are documented under Extensions.
- Test-level exclusion detail — that is [Test suite
  results](/compatibility/test-suite/). This page links there for the "how do
  you know" question but does not reproduce per-test tables.
- Prose explanation of *why* a command is unsupported beyond a one-line note —
  the reasons narrative lives on Overview & differences.

Sources of truth (author + generator implementer must read):
- `frogdb-server/crates/commands/src/lib.rs` — `register_all()`. Data-structure
  commands: at the time of writing ~297 `registry.register(...)` calls across
  string/hash/list/set/zset/stream/bitmap/geo/bloom/cuckoo/tdigest/hll/sort/
  timeseries/json/cms/topk/vectorset/event_sourcing modules. DO NOT hardcode
  this count in prose or the generator; count it at generation time.
- `frogdb-server/crates/server/src/server/register.rs` — `register_commands()`.
  Calls `register_all()` then registers server-specific commands: ~82 full
  `register(...)` calls plus 11 `register_metadata(...)` calls (pub/sub +
  RESET + MONITOR are metadata-only, handled at the connection level). Note
  some entries are deliberate stubs in `crate::commands::stub` (SELECT, SWAPDB,
  MOVE, MODULE, BGREWRITEAOF, SYNC, SAVE, WAITAOF) that exist in the registry
  but represent unsupported/no-op behavior — the generator must NOT count a
  stub as "supported". See "Status derivation".
- `frogdb-server/crates/core/src/registry.rs` — `CommandRegistry`,
  `CommandEntry` (`Full` vs `MetadataOnly`), and its iterators (`iter()`,
  `names()`). This is what a registry-dump generator introspects.
- `frogdb-server/crates/core/src/command.rs` — the `Command` /
  `CommandMetadata` traits expose `name()`, `arity()` (`Arity`), `flags()`
  (`CommandFlags`: WRITE, READONLY, FAST, BLOCKING, PUBSUB, ADMIN, ...), and
  `execution_strategy()`. **There is NO `family` field.** Family must be
  derived by the generator (see below). PLAN S1 lists "family" as an output
  field; it is a synthesized field, not a registry field — the spec author must
  not assume the registry provides it.
- `frogdb-server/ops/docs-gen/src/main.rs` — the existing Rust generator (S1
  extends this or adds a sibling). Study its `--check` mode, its
  `GeneratedNotice` header, and its output-to-`website/src/data` convention;
  the commands generator must mirror them.
- `website/src/data/compat-exclusions.json` — produced by
  `website/scripts/compat-gen.py`; supplies per-command excluded-test counts
  used to justify a "partial" status.
- `website/src/components/Compat*.astro` — existing components (see "Astro
  components").

VERIFY-BEFORE-WRITING:
- **The vendored Redis 8.x command list does not exist yet.** S2 requires
  "registry ∪ Redis 8.x command list ∪ compat exclusions". No such vendored
  file is present in the repo today. The generator implementer must add one
  (e.g. `website/src/data/redis-commands-8x.json` or a vendored copy of
  upstream `commands.json` under `frogdb-server/ops/docs-gen/`) sourced from
  the Redis version declared in S6/`versions.json`. Until it exists, the
  "unsupported (present in Redis, absent in FrogDB)" rows cannot be computed;
  the spec author must not fabricate that column. Flag this as the critical S2
  dependency.
- **Family taxonomy is a design choice, not a source fact.** Decide and
  document the mapping (recommended: reuse Redis's own command-group field from
  the vendored list — string, list, set, sorted-set, hash, stream, bitmap,
  geo, hyperloglog, pubsub, scripting, cluster, connection, server,
  generic/keyspace, transactions, plus FrogDB extension groups json,
  timeseries, search, bloom, cuckoo, cms, topk, tdigest, vectorset,
  event-sourcing). Registry-only commands with no Redis group get a FrogDB
  extension family.

Existing content: none (new page). The nearest existing artifact is the
retired hand-maintained `reference/commands/` idea referenced by old "See Also"
cards — this generated matrix replaces that concept entirely.

Data pipeline (this is the core of the spec — implementer builds S1 then S2):
1. **S1 — registry dump (`commands-gen`).** Extend `ops/docs-gen` (or a new
   `commands-gen` bin) to construct the full registry the same way the server
   does — call `frogdb_commands::register_all()` then the server's
   `register_commands()` — and iterate `registry.iter()` emitting per command:
   `name` (upper-case), `arity`, `flags` (decoded to string tokens),
   `execution_strategy`, `is_metadata_only`, and `is_stub` (see below).
   Output `website/src/data/commands.json` with the same `_generated` header
   and `--check` mode as `config-reference.json`.
   - Detecting stubs: the `stub` module commands are registered but are not
     real implementations. The generator needs a reliable signal. Preferred:
     add an explicit marker on the `Command` trait (e.g. a
     `fn implementation_status() -> ...` default) OR maintain an explicit
     stub-name allowlist inside the generator sourced from
     `crate::commands::stub`. Document whichever is chosen; do not guess status
     from flags alone. (VERIFY-BEFORE-WRITING: pick the mechanism with the
     server-crate owner.)
2. **S2 — the join.** A join step (in the same Rust generator, or a Python
   post-step analogous to `compat-gen.py`) computes one row per command over
   the union of (a) `commands.json` names, (b) the vendored Redis 8.x command
   list, (c) `compat-exclusions.json` command_impact. Output
   `website/src/data/command-matrix.json` shaped as:
   `{ _generated, target_redis_version, families: [ { family, commands: [
   { name, status, arity, note } ] } ], summary: { supported, partial,
   unsupported, extension } }`.
3. Both outputs are regenerated by `just docs-build` (S8) and guarded by
   `--check` in CI.

Status derivation (define precisely; DragonflyDB's supported/partial/
unsupported model, PLAN §5):
- **supported** — command is a `Full` registry entry (or an intentional
  metadata-only entry like SUBSCRIBE/PUBLISH), is NOT a stub, and either has no
  excluded regression tests or only exclusions in intentional-incompatibility
  categories that don't reduce functional coverage. Note column empty or a
  brief delta pointer.
- **partial** — registered and functional but with a meaningful behavioral
  delta or non-trivial excluded-test surface (e.g. commands appearing in
  `compat-exclusions.json` command_impact with functional exclusions, or
  commands whose Overview delta narrows behavior — CROSSSLOT-limited multi-key
  ops, single-shard scripting). Note column states the limitation in one line
  and may link to the relevant Overview section.
- **unsupported** — present in the vendored Redis 8.x list but absent from the
  registry, OR present only as a `stub`/no-op (SELECT non-zero, MOVE, SWAPDB,
  MODULE, BGREWRITEAOF, SYNC, SAVE). Note column gives the one-line reason
  (mirror the Overview "Not supported" table; consider sourcing both from the
  same data to stay consistent).
- FrogDB-original commands not in the Redis list (ES.*, HOTKEYS,
  FROGDB.VERSION, XDELEX/XACKDEL, etc.) are labeled supported and tagged as a
  FrogDB extension family; link to Extensions.

Page structure:
- Intro (no heading): one paragraph — this matrix is generated from the running
  command registry joined with the Redis {S6 version} command set; explain the
  three statuses in one sentence each; state that standard command behavior is
  documented at redis.io and that FrogDB deltas are on Overview & differences.
- `## Summary` — a small stat row (counts per status) rendered by a component
  reading `command-matrix.json` summary. No hardcoded totals in prose.
- `## Matrix` — the family/command/status/note table. Column order:
  **Family · Command · Status · Details** (DragonflyDB order). Grouped by
  family with family sub-headings or a family column; a client-side filter
  (search box + status filter) is desirable but optional for v1. Status shown
  as a colored badge (supported/partial/unsupported).
- `## How this is generated` — 2–3 sentences: registry dump (S1) ∪ Redis 8.x
  list ∪ regression exclusions (S2), regenerated by `just docs-build`,
  `--check` in CI. Link to Test suite results and Testing methodology.
- `## See also` — LinkCards: Overview & differences, Test suite results.

Astro components (read them; reuse the styling, add matrix-specific ones):
- Existing `website/src/components/CompatSummary.astro`,
  `CompatCommands.astro`, `CompatSuiteDetail.astro`, `CompatCategories.astro`
  all `import data from '../data/compat-exclusions.json'` and render tables /
  stat grids with `--sl-color-*` tokens. **They are wired to the exclusions
  JSON, not the matrix JSON, so they are not directly reusable** — but their
  markup + `<style>` blocks (stat-grid stat-card, table, category-badge,
  `<details>`) are the pattern to copy. Create new components under
  `website/src/components/`:
  - `CommandMatrix.astro` — imports `command-matrix.json`, renders the grouped
    Family/Command/Status/Details table with status badges; optional
    client-side filter via a small inline `<script>`.
  - `CommandMatrixSummary.astro` — imports `command-matrix.json`, renders the
    per-status stat row (model on `CompatSummary.astro`).
  Reuse the existing badge/table CSS conventions so the page visually matches
  the Test suite results page.

Generated data: `website/src/data/commands.json` (S1),
`website/src/data/command-matrix.json` (S2), consuming
`website/src/data/compat-exclusions.json` (existing) and the vendored Redis 8.x
command list (new, sourced from S6 version). Work items: **S1, S2** (hard
dependencies), S6 (target version string), S8 (CI wiring).

Drift guards:
- `--check` CI job for both `commands.json` and `command-matrix.json` (mirror
  `docs-gen-check` / `compat-gen-check` Justfile recipes); add
  `commands-gen`/`matrix` recipes and fold them into `docs-build` and the S8
  `deploy-docs.yml` fix so a registry change that isn't regenerated fails CI.
- Generator `--check` must also trigger on `website/**` changes (S8), so
  editing the page without regenerating is caught.
- The vendored Redis command list is version-pinned to S6's declared Redis
  target; when S6's Redis version bumps, the list is re-vendored and the
  `--check` diff forces regeneration — no silent staleness.
- No command count is written into prose anywhere; all counts render from the
  summary JSON (PLAN §6, §7).
