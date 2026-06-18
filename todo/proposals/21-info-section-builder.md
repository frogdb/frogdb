# Proposal: INFO Section Builder

Status: proposed
Date: 2026-06-17

## Problem

INFO is one logical operation — "render a set of named sections, each filled with live server
state" — but it is split across two modules that each do *half* the job and communicate through a
contract that the type system cannot see. The shard-local generator
(`commands/info.rs`) builds section *skeletons* with `0`/placeholder stubs; the scatter handler
(`connection/handlers/scatter.rs`) then string-patches the real values back in. Neither module is
deep: the generator's "interface" is not "here is the Stats section" but "here is a stub string
containing the exact byte sequence `keyspace_hits:0\r\n`, which a *different* module must find and
rewrite later." That contract — an undocumented set of literal anchor strings that must match
character-for-character across a module boundary — is as large and as fragile as the implementation
it is supposed to hide. It is the signature of a shallow seam: the cost of using the module (knowing
every anchor, in every section, byte-exactly) equals the cost of just writing the section correctly
in one place.

The split, and the implicit rule binding the two halves:

| Section | Stub built by (`info.rs`) | Real value patched by (`scatter.rs`) | Data source |
|---|---|---|---|
| Clients (`blocked_clients`, `connected_clients`, `maxclients`) | `build_clients_info` `:224-235` | `:365-387` (`.replace`) | client registry, config |
| Stats — eviction (`evicted_keys`, `expired_keys`, `lazyfreed_objects`) | `build_stats_info` `:350-404` | `:359-383` (`.replace`) | per-shard scatter (`MemoryStats`) |
| Stats — errors (`total_error_replies`, `rejected_calls`, `failed_calls`) | `build_stats_info` `:350-404` | `:405-422` (`.replace`) | client registry `error_stats` |
| Stats — keyspace (`keyspace_hits`, `keyspace_misses`) | `build_stats_info` `:377-378` | `:424-444` (`.replace`) | Prometheus metrics recorder |
| Ratelimit (whole section) | *not built* | `:389-403` (`push_str`) | ACL rate-limit registry |
| Commandstats (header only) | `build_commandstats_info` `:413-415` | `:467-512` (`find` + `replace_range`) | client registry + conn-local stats |
| Errorstats (header only) | `build_errorstats_info` `:421-423` | `:514-533` (`find` + `replace_range`) | client registry `error_stats` |
| Keysizes (header + shard-local) | `build_keysizes_info` `:560-592` | `:535-579` (`find` + `replace_range`) | **second** per-shard scatter (`KeysizesSnapshot`) |
| Latencystats (header only) | `build_latencystats_info` `:429-431` | `:581-616` (`find` + `replace_range`) | observability latency histograms |
| Replication (`master_replid`) | `build_replication_info` `:459-460` (node-id placeholder) | `:446-465` (`find` + `replace_range`) | replication state |

The implicit rule — **"emit an exact placeholder string in module A so module B can find and rewrite
it"** — is never stated as a type or a test. It survives only by the two modules agreeing, in prose
comments, on the literal bytes. `build_stats_info`'s doc-comment (`info.rs:343-349`) and
`build_commandstats_info`'s (`info.rs:406-412`) both describe their own output as a "placeholder …
patched in by `handle_info` in `connection/handlers/scatter.rs`," i.e. each builder documents that
its real owner lives in another file. That is the contract leaking out of the module: to know what
the Stats section *is*, you must read two files and trust that an anchor string in one matches a
`.replace()` argument in the other.

`handle_info` (`scatter.rs:350-621`) is ~271 lines whose job is almost entirely *undoing* the
generator's stubs. It reads from four independent sources — the Prometheus metrics recorder
(`:433`), the client registry (`:367-368,408,486,522`), a per-shard scatter
(`gather_memory_stats` `:359`, plus a *separate* `KeysizesSnapshot` scatter `:545-555`), and the
replication state (`:456-457`) — and rewrites nine sections with a mix of `.replace("evicted_keys:0\r\n", …)`
(`:378`), `.find("master_replid:")` + `.replace_range(…)` (`:459-463`), and section-range splicing
that re-parses the string it just built to locate `\r\n\r\n` boundaries (`:480-483,517-520,538-541,584-587`).
The handler builds nothing of its own structure; it treats the generator's output as a mutable text
buffer and edits it in place.

**Deletion test.** Delete `info.rs` and the complexity does not vanish — it *moves*. The Stats
fields, Commandstats lines, Keysizes bins, and `master_replid` all still have to be produced, so
`handle_info` would have to both build the skeletons and fill them: the same total work, now in one
function with no seam at all. Conversely, the stub-string contract cannot be deleted because nothing
owns it: it is distributed across ~30 literal byte-strings split between the two files. The fact that
deleting either half relocates rather than removes work — and that the connecting contract has no
home to delete — is the proof the seam is in the wrong place.

The fragility this produces is not hypothetical, and it matters here specifically because **this
project treats misleading observability data as unacceptable**: if a builder ever reformats its
stub — `keyspace_hits:0` → `keyspace_hits: 0`, reorders a field, or changes the line terminator —
the matching `.replace("keyspace_hits:0\r\n", …)` silently no-ops, the patch never lands, and INFO
reports a permanent, plausible-looking `0` while Prometheus reports the truth. There is no compile
error, no test failure unless one happens to assert that exact byte, and no log line. The stub-vs-real
divergence is invisible by construction. That silent-staleness failure mode is the shallow-seam tax
paid as wrong numbers.

This proposal owns the **section-rendering contract**: each INFO section becomes a deep module that
owns *both* its data and its format, pulling from a single source bundle gathered once. The
gather-once batching (today partially present as `gather_memory_stats` plus a second `KeysizesSnapshot`
scatter) is folded into that bundle so INFO stays a single round of shard messaging. The
**per-section accounting of the underlying counters** — what `keyspace_hits`/`misses` actually count,
how eviction/expiry are tallied — is a separate concern; see proposal 24 (keyspace-stats accounting),
which feeds the `keyspace_hits`/`keyspace_misses` source this builder reads. 21 makes each section a
single-owner renderer; 24 makes the numbers it renders correct.

## Current state

### The generator emits placeholder stubs (`commands/info.rs`)

The Stats section hardcodes `0` for every server-wide field and documents that someone else fills
them (`info.rs:343-404`, excerpted):

```rust
/// Build the Stats section.
///
/// Several fields are emitted here as `0` placeholders and patched with real
/// server-wide values by `handle_info` in `connection/handlers/scatter.rs` …
fn build_stats_info(ctx: &mut CommandContext) -> String {
    let key_count = ctx.store.len();
    format!(
        "# Stats\r\n\
         …
         expired_keys:0\r\n\
         …
         evicted_keys:0\r\n\
         …
         keyspace_hits:0\r\n\
         keyspace_misses:0\r\n\
         …
         total_error_replies:0\r\n\
         …",
        key_count
    )
}
```

The "patchable" sections are emitted as bare headers whose only purpose is to be a findable anchor
(`info.rs:413-431`):

```rust
fn build_commandstats_info() -> String { "# Commandstats\r\n\r\n".to_string() }
fn build_errorstats_info()   -> String { "# Errorstats\r\n\r\n".to_string() }
fn build_latencystats_info() -> String { "# Latencystats\r\n\r\n".to_string() }
```

The interface here is *the exact bytes*. `build_commandstats_info` returns nothing a caller could
use; it returns a marker that a different module's `find("# Commandstats\r\n")` (`scatter.rs:476`)
depends on.

### The scatter handler patches the stubs back to real values (`scatter.rs:350-621`)

`handle_info` runs the command on the local shard to get the stub string, then mutates that string
nine times. The eviction aggregation is its own per-shard scatter (`scatter.rs:356-387`):

```rust
let mut response = self.execute_on_shard(self.shard_id, cmd).await;

let shard_stats = self.gather_memory_stats().await;          // scatter #1 (all shards)
let evicted: u64 = shard_stats.iter().map(|s| s.evicted_keys).sum();
let expired: u64 = shard_stats.iter().map(|s| s.expired_keys).sum();
let lazyfreed: u64 = shard_stats.iter().map(|s| s.lazyfreed_objects).sum();

if let Response::Bulk(Some(ref bytes)) = response {
    let s = String::from_utf8_lossy(bytes);
    let blocked = self.admin.client_registry.blocked_client_count();
    let connected = self.admin.client_registry.client_count();
    let mut patched = s
        .replace("blocked_clients:0\r\n",   &format!("blocked_clients:{blocked}\r\n"))
        .replace("connected_clients:1\r\n", &format!("connected_clients:{connected}\r\n"))
        .replace("evicted_keys:0\r\n",      &format!("evicted_keys:{evicted}\r\n"))
        .replace("expired_keys:0\r\n",      &format!("expired_keys:{expired}\r\n"))
        .replace("lazyfreed_objects:0\r\n", &format!("lazyfreed_objects:{lazyfreed}\r\n"))
        .replace("maxclients:10000\r\n",    &format!("maxclients:{}\r\n",
                 self.admin.config_manager.max_clients()));
```

Keyspace hits/misses are read from the Prometheus recorder and patched — but *only if the recorder
can read them back*, otherwise the placeholder `0` is left in place (`scatter.rs:424-444`):

```rust
let recorder = &self.observability.metrics_recorder;
if let Some(hits) = recorder.counter_value(metric_names::KEYSPACE_HITS) {
    patched = patched.replace("keyspace_hits:0\r\n", &format!("keyspace_hits:{hits}\r\n"));
}
if let Some(misses) = recorder.counter_value(metric_names::KEYSPACE_MISSES) {
    patched = patched.replace("keyspace_misses:0\r\n", &format!("keyspace_misses:{misses}\r\n"));
}
```

`master_replid` is rewritten positionally — find the prefix, find the next CRLF, splice between
(`scatter.rs:456-465`):

```rust
if let Some(state) = &self.cluster.replication_state {
    let replid = state.read().await.replication_id.clone();
    if !replid.is_empty()
        && let Some(start) = patched.find("master_replid:")
        && let Some(rel_end) = patched[start..].find("\r\n")
    {
        let end = start + rel_end;
        patched.replace_range(start..end, &format!("master_replid:{replid}"));
    }
}
```

The list-valued sections (Commandstats, Errorstats, Keysizes, Latencystats) are each re-parsed out
of the buffer by locating the header and the next blank line, then the whole range is replaced
(`scatter.rs:467-512` shown; Errorstats/Keysizes/Latencystats are structurally identical):

```rust
if let Some(cs_start) = patched.find("# Commandstats\r\n") {
    let after_header = cs_start + "# Commandstats\r\n".len();
    let section_end = patched[after_header..]
        .find("\r\n\r\n")
        .map(|off| after_header + off + "\r\n\r\n".len())
        .unwrap_or(patched.len());
    // … build real cmdstat_* lines from client_registry + conn-local stats …
    patched.replace_range(cs_start..section_end, &section);
}
```

Keysizes triggers a **second** full-fleet scatter — `info.rs:560-592` already built a *shard-local*
Keysizes section, which `handle_info` discards and rebuilds from a fresh all-shard gather
(`scatter.rs:535-555`):

```rust
let mut merged = frogdb_core::KeysizeHistograms::new();
for sender in self.core.shard_senders.iter() {                // scatter #2 (all shards)
    let (response_tx, response_rx) = oneshot::channel();
    if sender.send(ShardMessage::KeysizesSnapshot { response_tx }).await.is_ok()
        && let Ok(Some(snap)) = response_rx.await
    {
        merged.merge(&snap);
    }
}
```

So one INFO does a local shard execute (`:356`) **plus** two distinct all-shard scatters
(`gather_memory_stats` `:359` and the `KeysizesSnapshot` loop `:545-555`), and the shard-local
Keysizes work in `info.rs` is computed only to be thrown away.

### Persistence is built once and never patched — and silently diverges (`info.rs:292-341`)

The Persistence section emits placeholder WAL metrics with a comment admitting they are wrong
(`info.rs:294-296`):

```rust
// Note: Real-time WAL lag metrics are available via STATUS JSON command
// which aggregates data from all shards. The values here are placeholders
// since INFO runs per-shard without server-level aggregation context.
```

Unlike the Stats fields, no patcher ever rewrites these. INFO therefore reports
`wal_pending_ops:0`, `wal_durability_lag_ms:0`, etc. forever, while `STATUS JSON` reports the real
aggregated values — two server-introspection commands disagreeing about the same quantity. This is
the shallow seam taken to its end state: the section has no access to its data, so it ships a
plausible lie. (See Correctness flags.)

## Proposed design

Make each INFO section a deep module: a renderer that owns *both* its data access and its format,
reading from one `InfoSources` bundle that is gathered exactly once per INFO call. There is no
placeholder string, no `.replace`, no `.replace_range`, and no re-parsing of a buffer the code just
emitted. The shard-local generator and the scatter patcher collapse into a single builder.

### The seam

```rust
/// Everything a section can read, gathered once per INFO request. The single
/// round of shard messaging lives here, so a section's `render` is a pure
/// function of already-collected data — it never scatters.
pub struct InfoSources<'a> {
    /// Connection/server context (local store len, role, config snapshot).
    ctx: &'a CommandContext<'a>,
    /// Server-wide client/error/command stats (the client registry).
    registry: &'a ClientRegistry,
    /// Prometheus counters (keyspace hits/misses live here; see proposal 24).
    metrics: &'a dyn MetricsRecorder,
    /// Live replication identity + offset.
    replication: Option<ReplicationSnapshot>,
    /// ACL rate-limit aggregates.
    rate_limit: RateLimitSnapshot,
    /// Observability latency histograms.
    latency: &'a LatencyHistograms,
    /// Per-shard data, gathered in ONE scatter (see `gather`).
    shards: ShardInfoSnapshot,
}

/// Aggregated per-shard data, collected with a single fleet scatter that
/// replaces today's two passes (`MemoryStats` + `KeysizesSnapshot`).
pub struct ShardInfoSnapshot {
    pub evicted_keys: u64,
    pub expired_keys: u64,
    pub lazyfreed_objects: u64,
    pub keysizes: KeysizeHistograms,
    // … any future per-shard INFO field aggregates join here, not as a new scatter.
}

impl<'a> InfoSources<'a> {
    /// The one place INFO talks to shards. Sends a single combined request to
    /// every shard and folds the replies into `ShardInfoSnapshot`, so adding a
    /// per-shard field never adds a round trip.
    pub async fn gather(handler: &'a ScatterHandler<'a>, ctx: &'a CommandContext<'a>) -> Self { /* … */ }

    pub fn keyspace_hits(&self)   -> Option<u64> { self.metrics.counter_value(KEYSPACE_HITS) }
    pub fn keyspace_misses(&self) -> Option<u64> { self.metrics.counter_value(KEYSPACE_MISSES) }
    pub fn shard_sum(&self, f: impl Fn(&ShardInfoSnapshot) -> u64) -> u64 { f(&self.shards) }
    // … typed accessors for each source; sections never reach past these.
}

/// One section = one owner of its data + its format. No stub, no anchor string.
#[async_trait]
pub trait InfoSection {
    /// Stable section key (`"stats"`, `"commandstats"`, …) used for selection.
    fn name(&self) -> &str;
    /// Render the full section text, including the `# Header\r\n` and trailing
    /// `\r\n`. Pulls every value from `src`; performs no I/O.
    async fn render(&self, src: &InfoSources<'_>) -> String;
}

struct StatsSection;

#[async_trait]
impl InfoSection for StatsSection {
    fn name(&self) -> &str { "stats" }
    async fn render(&self, src: &InfoSources<'_>) -> String {
        let mut w = SectionWriter::new("Stats");
        // Real value at the point of emission — no `0` to be patched later.
        w.field("expired_keys",  src.shard_sum(|s| s.expired_keys));
        w.field("evicted_keys",  src.shard_sum(|s| s.evicted_keys));
        // Metrics-disabled is represented honestly, not as a stale 0 (see flag 1).
        w.field_opt("keyspace_hits",   src.keyspace_hits());
        w.field_opt("keyspace_misses", src.keyspace_misses());
        w.field("total_error_replies", src.registry().total_error_replies());
        // …
        w.finish()
    }
}
```

An `InfoBuilder` owns selection and assembly — the `DEFAULT_SECTIONS` / `EXTRA_SECTIONS` /
`all`/`default`/`everything` logic that lives today in `info.rs:33-127`:

```rust
pub struct InfoBuilder { sections: Vec<Box<dyn InfoSection>> }

impl InfoBuilder {
    pub async fn render(&self, requested: &SectionSelector, src: &InfoSources<'_>) -> String {
        let mut out = String::new();
        for s in self.sections.iter().filter(|s| requested.includes(s.name())) {
            out.push_str(&s.render(src).await);   // already correct; nothing to patch
        }
        out
    }
}
```

`SectionWriter` centralizes the format invariants (`# Title\r\n`, `field:value\r\n`, trailing
`\r\n`) that are today copy-pasted into every `format!` and re-discovered by every `find("\r\n\r\n")`.
The CRLF contract becomes one type's responsibility instead of an unwritten rule the patcher
re-derives.

### Batching the shard gather — INFO stays one round trip

The design must **not** regress to N scatters. Today the work is already partly gather-once
(`gather_memory_stats`) but split into two passes that each fan out to every shard. `InfoSources::gather`
sends a single combined request per shard (an `InfoShardSnapshot` reply carrying both the memory
counters and the keysizes histogram) and folds the replies into `ShardInfoSnapshot`. Every section
then reads from that already-materialized struct. Net shard messaging drops from *two* fleet scatters
plus a local execute to *one* fleet scatter; the shard-local Keysizes computation in `info.rs`
(currently built and discarded) is removed entirely. Adding a future per-shard INFO field is a new
field on `ShardInfoSnapshot`, not a new round trip.

### Before / after: the Stats keyspace fields

Before — `info.rs` emits a literal `0`, and `scatter.rs` must find that exact byte string and
conditionally rewrite it; the two halves live in different files and agree only by convention:

```rust
// info.rs:377-378  (module A: emit anchor)
keyspace_hits:0\r\n\
keyspace_misses:0\r\n\

// scatter.rs:424-444  (module B: find anchor, rewrite — or silently leave the 0)
let recorder = &self.observability.metrics_recorder;
if let Some(hits) = recorder.counter_value(metric_names::KEYSPACE_HITS) {
    patched = patched.replace("keyspace_hits:0\r\n", &format!("keyspace_hits:{hits}\r\n"));
}
if let Some(misses) = recorder.counter_value(metric_names::KEYSPACE_MISSES) {
    patched = patched.replace("keyspace_misses:0\r\n", &format!("keyspace_misses:{misses}\r\n"));
}
```

After — the section reads its own source and writes the final bytes once; there is no anchor to
match and no second module:

```rust
// StatsSection::render — data + format in one place
w.field_opt("keyspace_hits",   src.keyspace_hits());   // None ⇒ honest absence, not stale 0
w.field_opt("keyspace_misses", src.keyspace_misses());
```

The same collapse applies to every row of the table in [Problem](#problem): Commandstats
(`scatter.rs:467-512`) becomes `CommandstatsSection::render`, which reads `registry.command_stats_snapshot()`
and the connection-local stats and emits the `cmdstat_*` lines directly — no `# Commandstats\r\n`
anchor, no `find("\r\n\r\n")` range splice. Keysizes (`scatter.rs:535-579`) reads
`src.shards.keysizes` — the histogram already merged during the single gather — instead of issuing
its own scatter. `master_replid` (`scatter.rs:446-465`) is rendered by `ReplicationSection` from
`src.replication`, so the positional `find`/`replace_range` disappears.

### Why this is the right depth

- **Locality.** Each section's data access and byte format live in one `render`. The "what does the
  Stats section contain" question is answered by reading `StatsSection`, not by cross-referencing an
  anchor in `info.rs` against a `.replace` in `scatter.rs`. The CRLF/section-boundary contract that
  `handle_info` re-derives four times (`find("\r\n\r\n")`) is owned once by `SectionWriter`.
- **Observability accuracy.** Deepening the seam *deletes a whole bug class*: there is no stub to
  diverge from, so "the builder reformatted the field and the patch silently no-oped" becomes
  unrepresentable. Metrics-disabled is rendered as an explicit absence rather than a plausible stale
  `0`. The Persistence/STATUS-JSON divergence (flag 2) is closed by giving the section real access to
  the same aggregated source. For a project that treats misleading observability data as
  unacceptable, removing the silent-staleness mode is the core win, not a side effect.
- **Testability.** A section's `render` is a pure function of an `InfoSources` it can be handed in a
  test — assert the exact bytes of `StatsSection::render` against a constructed source, with no
  primary/replica/multi-shard cluster and no parsing of a 2 KB INFO blob to find one field. The
  selection logic (`default`/`all`/`everything`, dedup) tests against a trivial set of fake sections.
- **Leverage.** A new section or field is one `InfoSection` impl (or one `w.field`) behind the trait;
  it cannot introduce an anchor/patcher pair because the pattern no longer exists. A new per-shard
  aggregate is one field on `ShardInfoSnapshot`, gathered in the existing single scatter.
- **Not a new adapter layer.** This collapses two modules into one owner; it does not wrap them. The
  shard-local stub generation and the scatter-side patching are *removed*, not hidden behind a facade
  callers could bypass. `handle_info` shrinks to "gather sources once, ask the builder to render."

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Behavior-preserving throughout —
INFO's output bytes are unchanged except where a stub was previously left stale (the keyspace/metrics
and Persistence cases, which become *correct*, covered by updated assertions).

1. **Phase 0 — introduce the seam, no call-site changes.** Add the `InfoSection` trait,
   `SectionWriter`, `InfoSources` (with typed accessors over the *existing* sources), and
   `ShardInfoSnapshot`. Keep `InfoSources::gather` doing today's two passes internally for now
   (`MemoryStats` + `KeysizesSnapshot`). Unit-test `SectionWriter` formatting and the selector logic
   in isolation. `just check frogdb-server`.
2. **Phase 1 — port the pure sections.** Convert the sections that need no patching today (Server,
   Memory, Persistence-as-is, CPU, Keyspace, Tiered, Latency_Baseline) to `InfoSection` impls reading
   from `InfoSources`. `InfoBuilder` renders these; `handle_info` still patches the rest of the
   string. Pure refactor; byte-identical output. Pin with a golden-output test.
3. **Phase 2 — port the patched sections, delete the patchers.** Convert Clients, Stats, Ratelimit,
   Commandstats, Errorstats, Latencystats, and the `master_replid` field of Replication to renderers
   that read their real source from `InfoSources`. Delete the corresponding `.replace` /
   `find`+`replace_range` blocks from `handle_info` (`scatter.rs:364-616`) and the placeholder
   builders/stubs from `info.rs` (`build_commandstats_info`/`build_errorstats_info`/
   `build_latencystats_info` and the `0` anchors in `build_stats_info`/`build_clients_info`). After
   this phase `handle_info` is "gather once, render," and `info.rs` no longer emits anchor strings.
4. **Phase 3 — fold the two scatters into one.** Replace the separate `gather_memory_stats` call and
   the `KeysizesSnapshot` loop inside INFO with a single combined per-shard request feeding
   `ShardInfoSnapshot`. Keysizes reads `src.shards.keysizes`; eviction reads `src.shards.*`. Remove
   the now-dead shard-local Keysizes build in `info.rs`. INFO is one fleet round trip. (Phase 3 can
   ship independently of 1-2 if needed, but is cleanest last.)
5. **Phase 4 — close the Persistence divergence.** Give `PersistenceSection` access to the same
   aggregated WAL source `STATUS JSON` uses, so the two commands agree. (Behavior change: corrects
   flag 2. Gate behind the updated golden test.)
6. **Gate.** Add a grep gate to `just lint`: in `commands/info.rs` and `connection/handlers/scatter.rs`,
   no `\.replace("[a-z_]+:0\\r\\n"` and no `\.replace_range\(` in INFO code paths — section content
   must come from a renderer, never a post-hoc string patch.

## Testing impact

- **Per-section unit tests, no cluster.** Each `render(&InfoSources)` is testable against a
  hand-built source: assert the exact `StatsSection` / `CommandstatsSection` bytes without a
  multi-shard server or a parse of the full INFO response. The cases that are integration-only today
  (does the patcher find the anchor? does `replace_range` land on the right span?) become fast unit
  tests, and several of them *cannot regress* because the anchor/range machinery is gone.
- **Selector tests.** `default` vs `all`/`everything`, the `default`/`""` aliases, and the dedup of
  repeated section args (`info.rs:80-105,136-138`) test against fake sections — no real data sources.
- **Metrics-disabled honesty (fails today's intent).** Assert that with the metrics recorder
  disabled, `keyspace_hits`/`keyspace_misses` render as the chosen honest representation (absent or
  explicitly-unavailable) rather than a stale `0`. This pins the divergence-class fix (flag 1).
- **Persistence/STATUS-JSON parity (fails today).** Assert that INFO's Persistence WAL fields equal
  `STATUS JSON`'s aggregated values after some writes. Fails pre-Phase-4 (INFO reports `0`), passes
  after (flag 2).
- **Single-scatter assertion.** Add a test/instrumentation check that one INFO issues exactly one
  per-shard fan-out (not the current two), guarding against a future section re-introducing its own
  scatter.
- **Golden output regression.** A full-INFO golden test captures the byte layout across Phases 1-3 so
  the refactor is provably output-preserving except at the intentionally-corrected fields.

## Risks / open questions

- **Output-byte parity during the refactor.** INFO's text is consumed by clients and test harnesses
  that may match on substrings; the golden test in Phase 1 is the safety net, and Phases 1-3 are
  asserted byte-identical except the two corrected divergences. The corrections (stale `0` →
  real/absent) are the point, not collateral.
- **Async in `render`.** Replication reads an `RwLock` (`scatter.rs:457`), so `render` is `async`.
  The bundle gathers replication once into `InfoSources` (`ReplicationSnapshot`), so most sections'
  `render` is effectively synchronous over already-materialized data; the `async fn` is kept uniform
  for the few that genuinely await. Alternative — make `gather` resolve *everything* up front and
  sections fully sync — is cleaner but front-loads work for sections the request didn't ask for;
  gathering lazily per selected section is the open trade-off.
- **`InfoSources` lifetime / borrow shape.** It borrows the registry, metrics recorder, latency
  histograms, and ctx. Threading those lifetimes through `async` trait objects may push toward
  `Arc`-cloning the cheap handles instead of borrowing. Decide during Phase 0; it does not affect the
  external contract.
- **Metrics-disabled representation is a protocol-visible choice.** Redis always prints
  `keyspace_hits:<n>` (its counters are unconditional). FrogDB's hits/misses live in the optional
  Prometheus recorder, so "metrics off" has no number to print. Rendering `0` is Redis-shaped but
  *misleading*; omitting the field is honest but non-standard. Proposal 24 (keyspace-stats
  accounting) may move these counters out of the optional recorder into an always-on source, which
  would make the question moot — coordinate the representation decision with 24 rather than baking it
  here.
- **Interplay with proposals 22 and 24.** 24 owns *what the keyspace counters mean and where they
  live* — it feeds the `keyspace_hits`/`keyspace_misses` source this builder reads, and may relocate
  that source (see above). 22 (observability surface) overlaps on which fields INFO exposes. This
  proposal owns only the *rendering seam*; it should land its accessor (`InfoSources::keyspace_hits`)
  as the single read point so that 24 changes the source behind one accessor, not nine `.replace`
  sites.
- **Per-shard combined message.** Folding `MemoryStats` + `KeysizesSnapshot` into one
  `InfoShardSnapshot` reply adds a shard-message variant. If other callers (`MEMORY STATS`,
  `STATUS JSON`) still want `MemoryStats` alone, keep that variant and have INFO use the combined one,
  or have the combined reply embed the existing structs to avoid duplicating shard-side logic.

## Correctness flags

1. **Silent stub-vs-real divergence — STRUCTURAL.** The contract between `info.rs` and
   `scatter.rs` is a set of literal anchor strings (`"keyspace_hits:0\r\n"`, `"evicted_keys:0\r\n"`,
   `"blocked_clients:0\r\n"`, `"# Commandstats\r\n"`, …) matched by `.replace`/`find`
   (`scatter.rs:370-444,476,515,536,582`). If a builder reformats its output — extra space, reordered
   field, changed terminator — the match no-ops, the patch never lands, and INFO reports the stale
   placeholder (`0` or an empty section) with no error and no test failure unless one asserts that
   exact byte. Additionally, `keyspace_hits`/`keyspace_misses` are *intentionally* left at `0` when
   the metrics recorder is disabled (`scatter.rs:434-443`): INFO then reports `keyspace_hits:0`
   indistinguishable from "genuinely zero hits." This is a class of misleading-observability bug the
   project's standards reject. Fix: own data+format per section so there is no stub to diverge from,
   and render metrics-absent honestly.

2. **INFO Persistence WAL metrics are permanently `0` while STATUS JSON is correct — CONFIRMED.**
   `build_persistence_info` (`info.rs:292-341`) emits `wal_pending_ops:0`, `wal_durability_lag_ms:0`,
   `wal_sync_lag_ms:0`, etc. as placeholders (comment at `info.rs:294-296`), and *no* patcher in
   `handle_info` ever rewrites them. So INFO and `STATUS JSON` report different values for the same
   WAL lag — two introspection commands contradicting each other. Fix: render the Persistence section
   from the same aggregated source STATUS JSON uses (Phase 4).

3. **INFO does two fleet scatters where one suffices — CONFIRMED (efficiency, not a wrong value).**
   A single INFO issues `gather_memory_stats` (all shards, `scatter.rs:359`) *and* a separate
   `KeysizesSnapshot` loop (all shards, `scatter.rs:545-555`), and additionally computes a
   shard-local Keysizes section in `info.rs:560-592` that `handle_info` discards. Two round trips and
   one wasted computation per INFO. Fix: fold into one combined per-shard gather feeding
   `ShardInfoSnapshot` (Phase 3).
