# Spec: extensions/overview.md
Status: new
Audiences: A1, A2, A4

Goal: The reader leaves knowing exactly which non-core-Redis command families
FrogDB implements, and — for each family — whether it is *Redis-Stack-compatible*
(behavior documented upstream by redis.io / Redis Stack module docs, and FrogDB
only documents deltas) or *FrogDB-original* (no upstream documentation exists, so
FrogDB is the canonical source). The reader can find, in one click, the canonical
documentation home for every family: an upstream link for compatible families, an
in-site page for originals. The reader also understands that per-command
supported/partial/unsupported status is **not** on this page — it lives in the
generated Command matrix — and how to read that matrix for extension families.

Not in scope:
- Per-command reference or per-command status. That is the Command matrix
  (Compatibility & Correctness section, generated). This page links to it and
  must not duplicate or restate command-level status.
- Behavioral deltas from upstream for the compatible families. Those belong on
  the Compatibility & Correctness "Overview & differences" page (or a future
  per-family delta note), not here. This page is an index, not a delta list.
- A tutorial for any family. Event Sourcing has its own reference page; the
  compatible families are taught by their upstream docs.
- Any performance/latency claim, any hardcoded command count per family, any
  "coming soon" family. Families that are not merged and tested are not listed.

Sources of truth (the author must read these to verify the family list and
classification before writing prose):
- `frogdb-server/crates/commands/src/` — one module per data family. Verified
  present at spec time: `json/` (JSON.*), `timeseries.rs` (TS.*), `bloom.rs`
  (BF.*), `cuckoo.rs` (CF.*), `cms.rs` (CMS.*), `topk.rs` (TOPK.*),
  `tdigest.rs` (TD.*), `vectorset/` (V* commands — VADD, VSIM, VCARD, VDIM,
  VEMB, VGETATTR, VINFO, VLINKS, VRANDMEMBER, VRANGE, VREM, VSETATTR),
  `event_sourcing/` (ES.*), and stream extensions in `stream/basic.rs` /
  `stream/consumer_groups.rs` (XDELEX, XACKDEL).
- `frogdb-server/crates/search/src/` — the FT.* search engine (index, query,
  aggregate, schema, spellcheck, suggest). FT.* command handlers are wired in
  `frogdb-server/crates/server/src/connection/handlers/search/`.
- `frogdb-server/crates/server/src/connection/handlers/hotkeys.rs` and
  `frogdb-server/crates/server/src/commands/hotkeys.rs` — HOTKEYS (FrogDB-original
  diagnostic command; note it is a diagnostic, cross-reference Operations →
  Diagnostics, not a data family).
- `frogdb-server/crates/server/src/server/register.rs` and
  `frogdb-commands::register_all()` — the authoritative registration list.
  Confirm each family is actually registered (a module existing is not proof it
  is wired in).
- Redis target version const (work item S6 `versions.json`) and the upstream
  Redis 8.x command set — needed to correctly classify the stream extensions
  (see verification requirement below).

VERIFICATION REQUIREMENT — stream extension classification: XDELEX and XACKDEL
exist in FrogDB's stream module, but these were also added to upstream Redis
(8.2). Before labeling them "FrogDB-original," the author MUST confirm against the
declared Redis target version (S6) whether they are upstream commands. If they
are upstream in the targeted Redis version, they are Redis-compatible stream
commands belonging in the Command matrix, NOT FrogDB-original extensions, and
should either be omitted from this page or listed under a clearly-labeled
"stream extensions" note that points to the matrix. Do not assert FrogDB-original
status without this check.

VERIFICATION REQUIREMENT — every command name and prefix cited in prose must be
confirmed against a `grep` of registered command names in the sources above at
write time. Do not carry over any command list from memory or from this spec's
snapshot; the snapshot is a starting point, not the source of truth.

Existing content: None. This is a new page. The current site has no extensions
overview; the only adjacent page is `guides/event-sourcing.md`, which becomes the
Event Sourcing reference (see `specs/extensions/event-sourcing.md`) — link to it,
do not absorb it.

Structure:

## Extensions (intro, ~2 short paragraphs)
- Define "extension" for this site: command families beyond the core Redis
  string/hash/list/set/sorted-set/stream/etc. keyspace. Two categories:
  Redis-Stack-compatible and FrogDB-original.
- State the documentation contract up front: for compatible families, upstream
  docs are authoritative and FrogDB documents only deltas (on the Compatibility
  & Correctness pages); for original families, FrogDB is the canonical source.
- Point to the Command matrix as the place to look up whether any specific
  extension command is supported / partial / unsupported. This page tells you a
  family exists and where its docs live; the matrix tells you command-level
  status.

## Redis-Stack-compatible families
- A table, one row per family: Family (name + command prefix) · What it is
  (one line) · Canonical docs (upstream link) · Status (link into the Command
  matrix filtered/anchored to that family — a link, never a restated count or
  percentage).
- Rows (verify each against source before publishing): JSON (`JSON.*`),
  Time Series (`TS.*`), Search (`FT.*`), Probabilistic — Bloom filter (`BF.*`),
  Cuckoo filter (`CF.*`), Count-Min Sketch (`CMS.*`), Top-K (`TOPK.*`),
  t-digest (`TD.*`), Vector sets (`V*` — VADD/VSIM/… ; note the unprefixed
  naming, matching Redis vector sets).
- One sentence per family max in the "what it is" cell. No behavioral detail;
  that is the upstream docs' job.
- Upstream links go to the Redis Stack / redis.io module documentation for each
  family (JSON, Time Series, Search, Bloom/probabilistic, vector sets). Verify
  links resolve at build; a broken upstream link is a drift bug.

## FrogDB-original families
- These have no upstream documentation; FrogDB is canonical.
- Event Sourcing (`ES.*`): one line on what it adds (OCC, versioned reads,
  snapshot-accelerated replay, idempotent append, global `$all` stream), then
  link to the Event Sourcing reference page as its canonical home.
- HOTKEYS: note it is a FrogDB-original *diagnostic* command (hot-key sampling),
  not a data family. Link to Operations → Diagnostics as its home; mention here
  only so readers scanning for FrogDB-original surface find it.
- Stream extensions (XDELEX / XACKDEL): include ONLY if the verification
  requirement above confirms they are FrogDB-original for the targeted Redis
  version; otherwise route them to the Command matrix and omit from this list.

## How to read extension status (short closing section)
- Explain that the Command matrix is generated from the live command registry
  (work items S1/S2), so extension-command coverage shown there is always in
  sync with the binary. Tell the reader how to filter/find a family in the
  matrix.

Generated data:
- No JSON is embedded directly in this page. The page links to the Command
  matrix, which consumes `commands.json` (work item **S1**) joined with the
  Redis command list and `compat-exclusions.json` (work item **S2**). Family
  status references must be links into that generated matrix, honoring the §5
  rule "compat status per family should reference the command matrix, not
  duplicate it."
- Version/target strings (e.g., the Redis version used to classify stream
  extensions) come from `versions.json` (work item **S6**); never hardcode
  them in prose.

Drift guards:
- No hardcoded per-family command counts or percentages (PLAN §6 content
  policy). Status is always a link to the generated matrix.
- The family list is small and slow-moving, but a new extension family
  shipping without a row here is a silent gap. Recommended guard (author to
  flag for the S1/S2 owner): since `commands.json` (S1) carries every
  registered command name, a CI check can derive the set of distinct command
  prefixes/families present in the registry and assert that every family with
  extension commands is represented on this page (by matching the family
  identifiers/prefixes this page declares). This reuses the same S1 dump the
  Event Sourcing page uses for its command-coverage guard — see
  `specs/extensions/event-sourcing.md`. Until that check exists, this page has
  no automated protection against a missing family, so state that limitation in
  the spec and keep the family list minimal and source-verified.
- Upstream doc links must be checked for liveness at build (a dead redis.io
  link is a drift failure); include them in whatever external-link check the
  site runs, or list them for manual review in the doc-sync process.
- The XDELEX/XACKDEL classification is tied to the targeted Redis version; if
  S6's Redis target changes, this classification must be re-verified.
