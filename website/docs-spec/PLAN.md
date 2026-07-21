# FrogDB Documentation Plan

Master plan for the documentation website restructure. Per-page specs live in
`specs/`. Page-writing subagents must read this file first, then the spec for
their page.

FrogDB is unreleased, pre-production software. The documentation must reflect
that honestly: no production-migration guidance, no unbacked performance
claims, no aspirational features presented as shipped.

## 1. Audiences

| # | Audience | Who they are | Disposition |
|---|----------|--------------|-------------|
| A1 | Evaluators / early adopters | Engineers who heard about FrogDB and want to try it in under 15 minutes | Curious, impatient. Will leave if install or first commands fail |
| A2 | Skeptics | Engineers who assume "Redis-compatible" is overstated | Convinced only by evidence: test results, honest limitation lists, methodology |
| A3 | Architecture-curious engineers | Systems programmers, database enthusiasts, people who read Dragonfly/TigerBeetle internals posts | Read for depth. Want real design rationale, not summaries |
| A4 | Test-deployment operators | People running FrogDB in a lab, side project, or staging environment | Need config, deployment, persistence, monitoring answers that match the binary they downloaded |
| A5 | Contributors | People who want to build, test, or patch FrogDB | Need dev setup, test commands, code map |

Explicitly **not** audiences yet (pre-release): production operators at scale,
teams migrating live Redis fleets, client-library authors, managed-service
shoppers. Content aimed at these audiences is cut (see §4).

## 2. What each audience needs

| Audience | Needs | Served by |
|----------|-------|-----------|
| A1 | Install in one command; run first commands; know what works; find config basics | Getting Started, Compatibility matrix |
| A2 | Which commands are supported and how that claim is verified; behavioral deltas from Redis; testing methodology; what is honestly not done | Compatibility & Correctness (all pages) |
| A3 | Threading/shard model, VLL, storage engine, cluster design, consistency guarantees | Architecture |
| A4 | Configuration, deployment artifacts, persistence/durability, replication/cluster setup, metrics, security, diagnostics | Operations, Reference |
| A5 | Build/test commands, code layout, testing infrastructure | Architecture (code map, debugging), Testing methodology page |

## 3. Top-level information architecture

Model: DragonflyDB's flat, unversioned IA (closest analogue: Redis-compatible,
single-binary, systems-language rewrite) combined with Valkey's discipline of
generating command metadata from the source repo. Architecture gets a
first-class section (CockroachDB/ScyllaDB pattern) because A2/A3 are core
audiences and the design is the differentiator.

```
Getting Started · Compatibility & Correctness · Extensions · Operations · Reference · Architecture · Changelog
```

Rationale:
- **Compatibility & Correctness** is one section, not two. For a skeptic the
  question is single: "does it work, and how do you know?" Differences,
  generated command matrix, generated test-suite results, and testing
  methodology form one narrative.
- **Extensions** is separate from the compatibility story. ES.* is
  FrogDB-original (no upstream docs exist — we must document it fully).
  JSON/TS/FT/probabilistic/vector families are Redis-Stack-compatible;
  they get compat status + deltas, not duplicated reference.
- **No per-command reference pages.** ~390 registered commands; duplicating
  redis.io per-command docs is bloat and a permanent drift liability. The
  generated compatibility matrix (family / command / status / notes) is the
  command surface documentation. Full command docs exist only for families
  with no upstream documentation (ES.*) and FrogDB-original commands
  (e.g. HOTKEYS, XDELEX/XACKDEL notes).
- Deferred at pre-1.0 (per survey of CockroachDB/Timescale/Scylla/Valkey/
  Dragonfly): versioned docs, migration-from-Redis guides, client-library
  matrices, integrations catalogs, cloud/managed sections.

## 4. Cuts and merges from the current site

| Current page | Action | Reason |
|---|---|---|
| `compatibility/migration-guide.mdx` | **Cut** | Dual-write/blue-green production migration guidance for unreleased software targets an audience that doesn't exist yet and implies production-readiness |
| `operations/kubernetes.mdx` | **Cut**; fold honest Helm/operator status into Deployment | Page is a list of planned features |
| `reference/benchmarks.mdx` | **Cut** | Placeholder. Re-add when benchmarks are published |
| `operations/monitoring.md` | **Merge** into `operations/observability` | Two pages, one topic, unclear boundary |
| `operations/performance.mdx` + `operations/debug-ui.mdx` | **Merge** into `operations/diagnostics` | Both are "tools for figuring out what the server is doing" |
| `reference/reference-config.mdx` | **Replace** with generated output of `frogdb-server --generate-config` | Hand-written mirror of CLI output; guaranteed drift |
| `architecture/testing.md` | **Move** to Compatibility & Correctness → Testing methodology | Serves skeptics, not just contributors; remove the Loom claim (no loom dependency exists in the codebase) |
| `guides/` topic | **Dissolve** | Single page (event sourcing) moves to Extensions |
| `index.mdx` hype ("extensively tested", "maybe a neck beard", unverified "250+ commands") | **Rewrite** grounded | Style rules §7 |

Current site: 39 pages. Target: ~31 pages, of which 6 are data-driven from
generated JSON.

## 5. Outline and per-page goals

Format: page → goal (what the reader walks away with). Detailed specs in
`specs/<section>/<page>.md`.

### Home (`index.mdx`)
Reader learns in 30 seconds: what FrogDB is (Redis 8.x–compatible in-memory
database in Rust; RocksDB-backed persistence; single binary), its status
(pre-release), and where to go next (quickstart, compatibility evidence,
architecture). Every claim on this page links to a page that substantiates it.

### Getting Started
- **Installation** — reader has a running binary via Docker, Debian package,
  or source build. Toolchain/version numbers sourced from repo files, never
  hardcoded.
- **Quickstart** — reader has connected with `redis-cli`, run commands across
  2–3 data types, enabled persistence, and knows where config lives.

### Compatibility & Correctness
- **Overview & differences** — reader knows the compatibility target
  (Redis 8.x, RESP2/RESP3), the honest list of behavioral deltas
  (persistence model, Lua, cluster semantics, cross-slot ops, config mapping)
  and unsupported areas.
- **Command matrix** *(generated)* — reader can look up any Redis command and
  see supported / partial / unsupported + notes. Generated from the command
  registry joined with the Redis 8.x command set and regression-test
  exclusions.
- **Test suite results** *(generated, exists)* — reader sees which Redis TCL
  regression suites run against FrogDB and every intentional exclusion with
  its reason.
- **Testing methodology** — reader understands the testing pyramid as it
  actually exists: unit/integration → Shuttle randomized concurrency →
  Turmoil network simulation → Jepsen (replication + Raft cluster topologies)
  → cargo-fuzz targets (33 at time of writing; never hardcode the count in
  the page) → Redis regression suites (Rust-native ports of the TCL suites —
  the live TCL runner was removed, do not claim TCL execution) →
  history-based consistency checking. No aspirational entries.

### Extensions
- **Overview** — reader knows which non-core-Redis families FrogDB implements,
  and which are Redis-Stack-compatible (JSON, TimeSeries, Search FT.*,
  probabilistic BF./CF./CMS./TOPK./TD., vector sets) vs FrogDB-original (ES.*),
  with pointers to upstream docs for the compatible ones.
- **Event Sourcing (ES.*)** — reader can use every ES.* command; this is the
  canonical reference (no upstream docs exist).

### Operations
- **Configuration** — reader knows the four config surfaces (TOML file, CLI
  flags, env vars, `CONFIG SET`), precedence, and which params are
  runtime-mutable. Tables generated.
- **Deployment** — reader can deploy via Docker, systemd/deb, or Helm; honest
  status note on the Kubernetes operator.
- **Persistence & durability** — reader can choose a durability mode
  understanding the trade-offs, configure snapshots, understand recovery and
  tiered storage. No latency numbers without benchmark citations.
- **Replication** — reader can set up primary/replica, understands
  `min-replicas-to-write`, and can monitor replication state.
- **Clustering** — reader can bootstrap a Raft-coordinated cluster,
  understands slot ownership and the admin API, and knows failure modes.
- **Security** — reader can configure ACLs, TLS (per-port; certificate
  changes currently require a restart — hot-reload is not wired), and
  per-user rate limiting.
- **Observability** — reader can scrape Prometheus metrics, configure OTLP
  export and tracing, use health endpoints, and read INFO sections.
- **Diagnostics** — reader can use slowlog, latency tooling, hot-shard/hotkey
  detection, the debug web UI, and diagnostic bundles.
- **Backup & restore** — reader can take and restore a backup.

### Reference
- **Configuration reference** *(generated, exists)* — every config parameter:
  type, default, section, mutability.
- **Example config file** *(generated, new)* — canonical annotated TOML,
  produced by `frogdb-server --generate-config` at docs build.
- **frogdb-server CLI** — every server flag. Generated or check-enforced
  against clap definitions.
- **frogctl CLI** — every frogctl command/flag. Same mechanism.
- **Metrics reference** *(generated, new)* — every metric: name, type, labels,
  help text. Generated from `telemetry/src/definitions.rs`.

### Architecture
Existing 16 pages are the strongest asset for A2/A3; keep the structure,
verify every fact, strip unbacked numbers. Pages: overview (fix crate count
against `Cargo.toml`), request flows, concurrency model, storage, execution,
persistence, replication, clustering, consistency, blocking, VLL, protocol,
connection, debugging, glossary. (`testing.md` moves out per §4.)
Goal per page: reader understands the design and *why* — message-passing
shard-per-task model, slot-based shard routing (CRC16 % 16384 % num_shards —
several current pages wrongly claim xxhash64 routing; xxhash64 is used only
by probabilistic structures), VLL intent-based multi-shard atomicity with
Selective Contention Analysis, Raft-for-metadata + PSYNC-for-data cluster
split, forkless snapshots on RocksDB.

### Changelog
Keep as-is (`starlight-changelogs` from `CHANGELOG.md`).

## 6. Keeping docs synced with the implementation

Existing mechanisms (keep): `docs-gen` (Rust → `config-reference.json`),
`compat-gen` (regression-test exclusions → `compat-exclusions.json`), both
with `--check` CI jobs.

New work items (each page spec references the relevant item):

| ID | Work item | Source of truth | Output |
|----|-----------|-----------------|--------|
| S1 | `commands-gen`: extend `ops/docs-gen` to dump the command registry (`register_all()` + server `register.rs`) | `CommandRegistry` entries: name, arity, flags, family | `commands.json` → drives Command matrix page |
| S2 | Command matrix join: registry ∪ Redis 8.x command list ∪ compat exclusions → status per command | S1 + `compat-exclusions.json` + vendored Redis command list | matrix data consumed by Astro components |
| S3 | `metrics-gen`: dump metric definitions | `telemetry/src/definitions.rs` (typed metrics + derive macros) | `metrics.json` → Metrics reference |
| S4 | Example config: run `frogdb-server --generate-config` during `just docs-build` | server binary | generated TOML page |
| S5 | CLI reference: capture `--help` output (or clap introspection) with `--check` mode | clap definitions in `frogdb-server`, `frogctl` | CLI reference data |
| S6 | `versions.json`: emit workspace version, Rust toolchain, and both Redis version constants | `Cargo.toml`, `rust-toolchain.toml`, and a new module in `frogdb-types` declaring `ADVERTISED_REDIS_VERSION` (what INFO `redis_version` and the Lua `REDIS_VERSION` binding report — currently hardcoded `7.2.0` in `server/src/commands/info.rs` and `core/src/scripting/lua_vm.rs`, which must both consume the constant) and `REDIS_COMPAT_TARGET` (what the regression suite and docs claim — currently hardcoded `8.6.0` in `compat-gen.py`) | replaces every hardcoded version string in prose; docs use the compat target, and note the advertised version where client feature-detection matters |
| S7 | Code-path check: script verifying every `crates/...` / `frogdb-server/...` path referenced in architecture pages exists | docs markdown + repo tree | CI check |
| S8 | CI gap fix: `deploy-docs.yml` must run `just docs-build` (regenerates all JSON) instead of raw `bun run build`; generator `--check` jobs must also trigger on `website/**` changes | — | workflow edits |

Content policies (enforced by review, stated in every spec):
- No hardcoded version numbers, command counts, or crate counts in prose —
  use generated data (S6) or omit.
- No performance/latency numbers without a citation to a published,
  reproducible benchmark. Until benchmarks exist, describe trade-offs
  qualitatively (e.g. "sync mode waits for fsync before acknowledging").
- No feature documented before it is merged and tested. Partial features get
  an explicit status label, not a "coming soon" page.

## 7. Style rules (apply to every page)

- Professional engineering language. No hype, no colloquialisms, no
  exaggeration. Not "FrogDB speaks Redis" — "FrogDB implements RESP2 and
  RESP3 and targets compatibility with Redis 8.x commands."
- State limitations plainly and specifically. Honesty is the credibility
  strategy for this project.
- Omit what the reader doesn't need. Every section must serve an audience
  need from §2; delete it otherwise.
- Link, don't repeat (DRY). One topic has one home; other pages link to it.
- Redis-compatible behavior is documented by redis.io; we document deltas.

## 8. Spec files

Every page has a spec at `specs/<section>/<page>.md` using this template:

```markdown
# Spec: <output path under website/src/content/docs/>
Status: new | rewrite | update | generated
Audiences: A1–A5
Goal: <one paragraph — what the reader walks away with>
Not in scope: <explicit exclusions>
Sources of truth: <repo files/registries the author must read>
Existing content: <current page(s) to mine or replace>
Structure: <H2/H3 outline with one line per section on its content>
Generated data: <JSON files / components used, sync work-item IDs from §6>
Drift guards: <what keeps this page true over time>
```
