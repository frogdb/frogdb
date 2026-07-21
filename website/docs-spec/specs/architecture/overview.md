# Spec: architecture/architecture.md
Note: the output page is `architecture/architecture.md`. PLAN §5 refers to it as
the Architecture "overview" and this spec file is named `overview.md`, but the
page to write/update is `architecture/architecture.md` — do not create an
`overview` page.
Status: update
Audiences: A2, A3, A5

Goal: An architecture-curious or skeptical engineer walks away understanding
FrogDB's top-level design and *why* it was chosen: a shared-nothing,
message-passing, shard-per-task model in which each shard worker exclusively
owns a partition of the keyspace (so data access needs no locks), connections
are pinned to one shard for their lifetime and act as coordinators for keys they
don't own, and the codebase is split into crates with strict responsibility
boundaries so that stub and full implementations of a subsystem (persistence,
ACL, scripting) can be swapped behind traits. The reader also gets an accurate
map of the workspace crates and the two request-flow diagrams (general request
path and per-shard architecture) as an entry point to the deeper pages.

Not in scope: Deep dives that have their own pages — concurrency mechanics
(concurrency.md), request sequence diagrams (request-flows.md), command
dispatch (execution.md), storage internals (storage.md), persistence
(persistence.md), VLL (vll.md). This page links to them; it does not duplicate
them. No install/config/ops content.

Sources of truth (author must read and reconcile every claim against these):
- `/Users/nathan/workspace/workspace-3/Cargo.toml` — the authoritative workspace
  `members` list (currently 29 members). This is the source for any crate
  enumeration.
- `frogdb-server/crates/core/src/shard/message.rs` — `ShardMessage` enum (the
  real one is far larger than any illustrative excerpt).
- `frogdb-server/crates/server/src/server/util.rs` — `SHARD_CHANNEL_CAPACITY`
  const (verified `= 1024`).
- `frogdb-server/crates/server/src/acceptor.rs` — `Acceptor`, connection
  assignment (verify round-robin claim).
- `frogdb-server/crates/acl/src/checker.rs` — `AclChecker` trait (real
  signature).
- `frogdb-server/crates/types/src/traits/wal.rs` — `WalWriter` trait (real
  signature).
- `frogdb-server/crates/core/src/command.rs`, `.../store/mod.rs` — `Command` and
  `Store` traits, for the crate-responsibility tables.
- `frogdb-server/crates/protocol/src/` — `ParsedCommand`, `Response` types.

Existing content: `website/src/content/docs/architecture/architecture.md`.
Structure and prose are good and mostly retained; this is a verification and
correction pass, not a rewrite.

Structure (keep the existing H2 skeleton, corrected):
- Intro: one-paragraph statement of what FrogDB is and the four design
  emphases (shared-nothing threading, message-passing, pinned connections,
  clean crate boundaries).
- ## Design Principles — shared-nothing threading; message-passing over shared
  state; channel configuration (bounded, backpressure); pin-based connections;
  key hashing (**both levels are CRC16**: cluster slot = `CRC16(key) % 16384`,
  internal shard routing = `CRC16(key) % 16384 % num_shards`, sharing the same
  hash-tag extraction — xxhash64 is used only by probabilistic structures, never
  key routing; link to concurrency.md as the canonical home);
  separation of concerns.
- ## Crate Architecture — mermaid core dependency graph (verify edges); a
  workspace layout tree; crate-layer table; per-crate responsibility tables for
  the foundational crates (protocol, core, scripting, persistence, server). The
  crate list/tree/count must be reconciled with `Cargo.toml` (see Drift guards).
- ## Component Relationships — request-flow mermaid diagram, shard-architecture
  mermaid diagram, key-interaction table. Cross-link CommandContext to
  execution.md and hashing to concurrency.md rather than restating.
- ## Responsibility Boundaries — protocol/core/server boundary boxes, error
  handling (`CommandError` variants), and the two verified trait interfaces
  (AclChecker, WalWriter).
- ## Data Flow — GET/SET/cross-shard walkthroughs (keep as illustrative;
  cross-reference request-flows.md as the authoritative sequence diagrams).

Generated data: None currently. Candidate future tie-in: S1 (`commands-gen`)
and S6 (`versions.json`) if the crate list is ever generated. No generated
component is required for this update, but the crate count must follow the
no-hardcoded-count policy (see Drift guards).

Drift guards:
- **Key-hashing "dual hash" is WRONG — must be corrected.** Internal shard routing
  is CRC16 slot-derived (`shard_for_key` = `CRC16(key) % 16384 % num_shards` in
  `core/src/shard/helpers.rs`), NOT xxhash64. Both the cluster slot and the
  internal shard derive from the same CRC16 hash-tagged slot; xxhash64 is used only
  by probabilistic structures (bloom/cuckoo/cms/topk), never in key routing. Any
  "dual: xxhash64 vs CRC16" table/prose carried over from the existing page is
  incorrect — align with concurrency.md and clustering.md on CRC16 at both levels.
- **Crate count (known issue).** The page currently states "26 crates" in prose
  and "Workspace manifest (26 members)" in the layout tree. `Cargo.toml` has 29
  members. Do **not** substitute "29": per PLAN §6 content policy, no hardcoded
  crate counts in prose. Either (a) generate the crate list from `Cargo.toml`,
  or (b) describe the workspace without a number ("organized as a Cargo
  workspace of many crates, grouped by layer") and let the layer table enumerate
  them. Reconcile the workspace-layout tree and the crate-layer table against the
  actual `members` list — several members are currently missing or misnamed
  (e.g. the table says `frogdb-benches`; the member is `benchmarks`; `search`,
  `frogdb-admin`, `docs-gen`, `deb-gen` and others must be accounted for).
- **AclChecker trait.** Verify against `acl/src/checker.rs`. The real trait is
  `Send + Sync` and includes `requires_auth(&self) -> bool` and
  `is_auth_exempt(&self, cmd: &str) -> bool` in addition to the three `check_*`
  methods. Parameter names in source are `cmd`/`subcmd`, `access`. Reproduce the
  real signature or clearly mark it abbreviated.
- **WalWriter trait (currently wrong).** The page shows
  `write(&mut self, entry: &WalEntry) -> Result<(), PersistenceError>` plus
  `flush`/`sync`. The real trait (`types/src/traits/wal.rs`) is `Send + Sync`
  with `append(&mut self, operation: &WalOperation) -> u64`,
  `flush(&mut self) -> std::io::Result<()>`, and `current_sequence(&self) -> u64`.
  Replace with the real signature.
- **ShardMessage.** Any excerpt must be labeled illustrative; the full enum lives
  in `core/src/shard/message.rs` and has dozens of variants. Link to
  concurrency.md rather than implying the shown set is complete.
- **Channel capacity.** `SHARD_CHANNEL_CAPACITY = 1024` is a compile-time const
  in `server/src/server/util.rs`, not a runtime config key. Do not present it as
  a tunable `shard-channel-capacity` setting unless a config parameter actually
  exists (it does not at time of writing — see concurrency.md spec).
- **S7 code-path check.** Every `crates/...` / `frogdb-server/...` path this page
  names must exist. The workspace-layout tree lists paths (`frogdb-server/crates/…`)
  — all must resolve. Include this page in the S7 CI script's scan set.
- **No unbacked numbers.** No hardcoded version, command, or crate counts in
  prose (PLAN §6). No latency/throughput figures without a benchmark citation.
