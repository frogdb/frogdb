# Spec: extensions/event-sourcing.md
Status: rewrite (of guides/event-sourcing.md)
Audiences: A1, A2, A4

Goal: The reader can use every ES.* command correctly — knows each command's
syntax, arguments, return shape, error conditions, and the cross-cutting
semantics (optimistic concurrency control, version semantics, snapshot
colocation, idempotency, the global `$all` stream) — without consulting the
source. This is the ONE extension family that receives full per-command
documentation, because ES.* is FrogDB-original and no upstream documentation
exists. This page is the canonical reference for ES.*.

Not in scope:
- Teaching event sourcing as an architectural pattern beyond the minimum needed
  to use the commands. This is a command reference with usage notes, not an
  event-sourcing tutorial.
- Redis Streams behavior (XADD/XREAD/XDEL/etc.). ES.* is built on top of
  streams; link to redis.io for stream command semantics rather than restating
  them. Document only where ES.* interaction with streams is a delta or a
  hazard (e.g. mixing XADD with ES.APPEND).
- Any performance/latency number. Describe trade-offs qualitatively
  (PLAN §6 policy).
- Replication/cluster internals beyond the reader-facing facts they must know
  (e.g. `$all` is not replicated). Link to Architecture for the "why".

Sources of truth (author MUST read all before writing; the command list and
every arity/flag/behavior claim must be verified against these, not against this
spec or the old page):
- `frogdb-server/crates/commands/src/event_sourcing/` — the implementation, one
  file per command:
  - `append.rs` — ES.APPEND (`Arity::AtLeast(4)`, `CommandFlags::WRITE`,
    `WalStrategy::PersistFirstKey`, wakes stream waiters; OCC via
    `EsAppendError`; `IF_NOT_EXISTS` idempotency; per-shard `$all` maintenance
    with `ES_ALL_MAXLEN`)
  - `read.rs` — ES.READ (version-range read)
  - `replay.rs` — ES.REPLAY (optional `SNAPSHOT` key; slot colocation)
  - `info.rs` — ES.INFO (metadata key-value array)
  - `snapshot.rs` — ES.SNAPSHOT (store snapshot as `version:state`)
  - `all.rs` — ES.ALL (server-wide scatter-gather; note the internal comment
    that ES.ALL "should be dispatched server-wide")
  - `mod.rs` — module overview and `versioned_entry_to_response` shape
    (`[version, stream_id, [field, value, ...]]`)
- `frogdb-server/crates/redis-regression/tests/event_sourcing_regression.rs` —
  the authoritative behavioral contract. Every documented return shape, error
  string (e.g. `VERSIONMISMATCH expected N actual M`), and edge case (idempotent
  duplicate returns `[existing_version, null]`, XDEL gap handling, `$all`
  ordering) must match what these tests assert. Where the prose and the tests
  disagree, the tests win and the prose is a bug.
- `frogdb-server/crates/server/src/server/register.rs` +
  `frogdb-commands::register_all()` — confirm exactly which ES.* commands are
  registered (do not document an unregistered command).

VERIFIED ES.* command set at spec time (author MUST re-verify by grepping
registered command names — `grep -rn '"ES\.' frogdb-server/crates/`): ES.APPEND,
ES.READ, ES.REPLAY, ES.INFO, ES.SNAPSHOT, ES.ALL. Six commands. If a grep turns
up more or fewer, the page follows the source, not this list.

REQUIREMENT — for every command, the page must state syntax, arguments, return
shape, and error conditions, and each of those MUST be verified against the
implementation file AND the regression test named above. In particular:
- Arity and flags must match `arity()` / `flags()` in the command's source
  (e.g. ES.APPEND is `AtLeast(4)`).
- Error strings must be quoted exactly as the implementation/tests produce them.
- Return shapes must match `versioned_entry_to_response` and the test
  assertions, including null cases.

Existing content: `website/src/content/docs/guides/event-sourcing.md` — an
already-solid draft covering all six commands plus Usage Notes and Limitations.
Mine it heavily; it is close to correct. This is a rewrite (re-home under
Extensions, re-verify every claim against source/tests, align to style rules),
NOT a from-scratch page. Preserve its structure and worked examples where they
verify out. The `guides/` topic is dissolved (PLAN §4); after this page exists,
the old path must redirect/be removed and any links to it updated.
Points from the old page to re-verify rather than copy blindly:
- The 10,000-key idempotency FIFO bound and eviction behavior (check the const
  in `append.rs`).
- The 100,000-entry per-shard `$all` bound (`ES_ALL_MAXLEN`) and trim-on-append.
- "ES.ALL returns empty on replicas" and "`$all` is non-durable / not
  replicated" — confirm against source and tests.
- "ES.SNAPSHOT version is not validated (trust-the-client)".

Structure:

## Event Sourcing (ES.*) (intro)
- One paragraph: FrogDB-original extension built on Redis Streams; not available
  in Redis/Valkey/Dragonfly; canonical docs are here (no upstream exists). List
  the capabilities: OCC, version-based reads, snapshot-accelerated replay,
  idempotent writes, global `$all` stream. Link back to Extensions overview.

## Commands
One H3 per command, in a sensible reading order (APPEND, READ, REPLAY, INFO,
SNAPSHOT, ALL). Each H3 contains: a one-line purpose; a syntax block; a bulleted
argument list; a **Returns** line with the exact response shape; an **Errors**
list with exact error strings. Content per command mirrors the old page but
re-verified:
- ### ES.APPEND — OCC append; `expected_version` semantics (0 for first append);
  `event_type`/`data` plus extra field/value pairs; `IF_NOT_EXISTS idem_key`;
  returns `[new_version, stream_id]`; `VERSIONMISMATCH expected N actual M`;
  idempotent duplicate returns `[existing_version, null]`.
- ### ES.READ — 1-based inclusive `start_version`, optional `end_version`,
  `COUNT n`; returns array of `[version, stream_id, [field, value, ...]]`.
- ### ES.REPLAY — optional `SNAPSHOT snapshot_key`; slot-colocation requirement
  (hash tags); returns `[snapshot_state_or_null, [events...]]`.
- ### ES.INFO — returns flat key-value array: version, entries, first-id,
  last-id, idempotency-keys (verify field names and types against `info.rs`).
- ### ES.SNAPSHOT — stores `version:state` at `snapshot_key`; note version is
  not validated.
- ### ES.ALL — `COUNT n`, `AFTER stream_id`; returns `[stream_id, [field,
  value, ...]]` tuples sorted by StreamId; non-durable, scatter-gather across
  shards, approximate cross-shard ordering within a millisecond.

## Usage notes
Cross-cutting semantics that span commands: version semantics (monotonic, counts
appends not entries, survives XDEL); snapshot key colocation with a worked hash-
tag example; idempotency window and eviction; OCC race-freedom under the
thread-per-core model (concurrent same-version appends: exactly one wins). Keep
the worked example from the old page (ES.APPEND/ES.SNAPSHOT/ES.REPLAY on
`{order}:...`) — verify it runs.

## Limitations
State plainly (PLAN §7 honesty rule): XADD+ES.APPEND on the same stream breaks
version semantics; XDEL leaves permanent version numbers (ES.READ handles gaps);
ES.ALL is empty on replicas; ES.SNAPSHOT version is trust-the-client; `$all` is
bounded per shard and trimmed on append. Each bullet must be verifiable against
source/tests.

Generated data:
- This page is hand-written prose (there is no per-command generator for
  reference text). It does NOT embed generated JSON.
- It DOES participate in the command-coverage drift guard below, which consumes
  `commands.json` (work item **S1**).
- Any version/target string uses `versions.json` (**S6**); no hardcoded
  versions/counts in prose.

Drift guards:
- Command-coverage check (primary guard, RECOMMENDED — author to flag for the S1
  owner): work item S1 dumps the full command registry to `commands.json` with
  name/arity/flags. Because ES.* commands share the `ES.` name prefix, the set
  of ES.* commands can be **sliced from `commands.json` by prefix** and compared
  against the set of commands this page documents (each H3 under ## Commands).
  A CI check asserting `{ES.* in commands.json} == {ES.* documented here}` fails
  the build when a new ES.* command is added without documentation, or a
  documented command is removed/renamed. This is the concrete answer to the
  task's "consider whether ES.* command list can be sliced from commands.json to
  CI-check the page's command coverage": yes — prefix-slice S1 output. Specify
  the check so the page carries stable per-command anchors/markers the check can
  parse (e.g. an `ES.APPEND` heading id per command).
- Behavioral drift: the page's claims are anchored to
  `event_sourcing_regression.rs`. Recommend the doc-sync process re-read that
  test file whenever ES.* behavior changes; the regression suite is the
  contract, so a behavior change that updates the tests is the trigger to update
  this page.
- Arity/flags stated in prose can silently diverge from `arity()`/`flags()`.
  Where S1's `commands.json` carries arity, the coverage check can additionally
  assert documented arity matches the registry (stretch; at minimum the author
  must verify by hand at write time).
- Old-path cleanup: after publish, ensure no internal link points at
  `guides/event-sourcing` (S7-style path/link check territory); add a redirect
  from the old slug.
