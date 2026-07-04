# Proposal: Deletions + Small Seams Batch

Status: implemented
Date: 2026-07-04

## Problem

Survey rounds 1–5 left a residue of findings that are individually too small to justify a proposal
each, but that share one shape: **code that asserts structure it does not have** — dead trait
hierarchies with zero implementors, enums whose states are never transitioned, observability fields
hardcoded to zero, one contract hand-duplicated across three crates as string literals, and stub
`execute()` bodies that lie about being callable. Each is a small deep-module violation; together
they are a batch of deletions and single-owner seams. This proposal implements them as nine
independent items, one commit each.

## Items

### 1. Delete the dead `Store` sub-traits

**Evidence.** `core/src/store/traits.rs` (322 lines) defined `StorageOps`/`ExpiryOps`/`ScanOps`/
`EvictionOps`/`ClusterSlotOps` — ~41 method signatures duplicated verbatim from the `Store` trait
(`store/mod.rs:389+`), which did **not** use them as supertraits. Zero `impl` blocks, zero callers
outside `store/` — the only references were the `mod.rs` re-export and two doc comments promising
"use the sub-traits for narrower bounds", a promise no code ever took up.

**Change.** Deleted the file, the re-export, and rewrote the module docs to describe the actual
interface (`Store` + the typed-access layer of proposal 02). The deletion test passes trivially:
nothing referenced them. If narrower bounds are ever wanted, the right seam is the typed accessor
layer, not a parallel copy of the trait's own methods.

Commit: `ffce1207`

### 2. Delete `MigrationState` — migration is a two-point ownership swap

**Evidence.** `cluster/src/types.rs` `MigrationState { Initiated, Migrating, Completing }`: only
`Initiated` was ever constructed (`cluster/src/commands.rs`, `BeginSlotMigration`); no transition
code exists anywhere. `CLUSTER SETSLOT IMPORTING` and `MIGRATING` both lower to the identical
`SlotMigrationKind::Begin` (`server/src/commands/cluster/admin.rs`), and routing derives the
importing/migrating role by comparing node ids against the migration's `source_node`/`target_node`
— never by reading `state`. The debug UI rendered the field, so it displayed "initiated" forever:
misleading observability.

**Decision.** Delete the enum and the `SlotMigration.state` field rather than "making it real": a
staged migration state machine is a much larger project (per-slot key-transfer progress tracked
through Raft), and FrogDB's migration model today genuinely *is* two-point — a slot is either
migrating (present in `migrations`) or not; `Begin` inserts, `Complete`/`Cancel` removes. The
`SlotMigration` doc now states this. The debug UI's "State" column is gone (an entry in the table
*means* in-progress). Potential future work: if incremental key transfer ever needs resumability,
reintroduce a state machine with real transitions driven by the migration executor — not a field
that never changes.

**Serde compatibility.** `ClusterSnapshot` (Raft snapshots) serializes `SlotMigration`. Serde
ignores unknown fields by default, so old snapshots carrying `"state": "Initiated"` still
deserialize after the field deletion. New snapshots simply omit it. (Pre-production: the reverse
direction — old binary reading new snapshot — is not supported, which is an accepted break.)

Commit: `df0caef0`

### 3. Typed staged-checkpoint contract + backup retention

**Evidence.** The replica full-sync staging protocol was string constants spread across three
crates: the writer (`replication/src/replica/connection.rs`) renamed the downloaded checkpoint to
`<parent>/checkpoint_ready/` and stamped `replication_metadata.json` inside; the installer
(`persistence/src/rocks/checkpoint.rs`) validated `checkpoint_ready/CURRENT` and performed the
rename surgery; the orchestrator (`server/src/recovery/`) invoked the install and later consumed
the metadata. Two defects: (a) the installer's `<db>_backup_<unix_secs>` directories were **never
cleaned** — every replica full-sync leaked a complete database copy, forever; (b) the two
sequential renames are crash-safe for a subtle reason that was stated nowhere.

**Change.** New `persistence/src/rocks/staged.rs` owns the contract: `STAGED_CHECKPOINT_DIR`,
`STAGED_REPLICATION_METADATA_FILE`, a `StagedCheckpoint` handle (`for_db_dir`/`in_parent`,
`exists`, `is_complete_db`, `replication_metadata_path`), and backup retention. The writer and the
replication crate's `STAGED_METADATA_FILE` now alias/consume these; the installer is rewritten on
the typed handle with the crash-window rationale spelled out in comments (rename 1 = recoverable
window, rename 2 = atomic commit point that consumes the marker).

**Retention decision: keep the newest 1 backup, delete older ones, after a successful install.**
Rationale: the backup exists so an operator can recover the immediately-previous database if a
full sync installed bad data — one generation covers that; N generations of full database copies
is a disk-space liability with no recovery story attached. Pruning is best-effort (a prune failure
warns, never fails the install) and happens only *after* the commit-point rename. Newest is chosen
by the numeric `_backup_<unix_secs>` suffix, not string order.

Tests: the existing 8 install crash-window tests stay green (one — "pre-existing backup left
intact" — is updated to pin pruning instead), plus new tests for retention (numeric-vs-string
ordering, keep-newest across a leftover-backup crash window, prune failure isolation).

Commit: `9e23872c`

### 4. Delete the always-zero snapshot `num_keys` field

**Evidence.** `persistence/src/snapshot/stager.rs` wrote `md.mark_complete(0, size)` — the
`num_keys` metadata field was hardcoded `0` in every snapshot ever written. Counting keys in a
RocksDB checkpoint is not cheap (full iteration), and a permanently-zero count violates the
project's rule that misleading observability is worse than absent observability.

**Change.** Deleted end-to-end: `SnapshotMetadata.num_keys`, `SnapshotMetadataFile.num_keys`,
`mark_complete(num_keys, size)` → `mark_complete(size)`, the noop coordinator's copy. Old
`metadata.json` files carrying `num_keys` still deserialize (serde ignores unknown fields) —
pinned by `test_metadata_deserializes_legacy_num_keys_field`.

Commit: 2045505c

### 5. Fold the per-tier CF accessor triplets into one `CfTier` resolver

**Evidence.** `RocksStore` carried three parallel name lists (`cf_names`, `warm_cf_names`,
`search_meta_cf_names`) and three hand-copied resolver+put/get/delete/iter families
(`rocks/mod.rs` for main, `rocks/columns.rs` ×2 for warm and search-meta) — the same shape
repeated with only the name list and the log string varying.

**Change.** One `CfTier { Main, Warm, SearchMeta }` enum, one resolver
(`tier_cf_handle(tier, sid)`) owning tier availability (warm-enabled check) and shard-bounds
validation, and one op set (`put_tier`/`get_tier`/`delete_tier`/`iter_tier`). **Callers were kept
on the existing named methods** (`put`, `put_warm`, `put_search_meta`, ...) as one-line shims —
they are numerous across `core` (store/hashmap, recovery, search lifecycle) and tests, and the
per-tier names carry intent at call sites; the duplication that mattered was the resolver/op
bodies, not the method names.

Commit: 00483d61

### 6. Single owner for the `WalFailurePolicy` ↔ `u8` encoding

**Evidence.** The 0=Continue/1=Rollback encoding for the shared `AtomicU8` runtime flag was
hand-duplicated at the write sites (`core/src/shard/builder.rs`, `server/src/runtime_config.rs` —
twice, plus the config-string mapping) and read back as bare `== 1` comparisons
(`core/src/shard/types.rs::should_rollback`).

**Change.** The codec now lives on the type in its home (`persistence/src/wal/config.rs`):
`as_u8`/`from_u8` (+ `From` impls both ways; unknown values decode to the `Continue` default,
matching the historical read-side fallback) and `as_config_str`/`from_config_str` for the
CONFIG SET string form. All write/read sites migrated; the shard-worker defaults construct from
`WalFailurePolicy::default()`. The rollback-mode tests that assert raw `0`/`1` are kept as-is —
they pin the stable encoding. (Proposal 29's WAL rework did not touch this seam; verified before
changing.)

Commit: `df9de7e9`

### 7. Connection-level `execute()` stubs stop lying

**Evidence.** `server/src/commands/transaction.rs` (MULTI/EXEC/DISCARD/WATCH/UNWATCH) each carried
a fake `execute()` body — "Actual handling is done in ConnectionHandler / This should not be
called directly" — returning a plausible-but-wrong placeholder (`Ok(Response::ok())`,
`Ok(Response::Array(vec![]))`). If a routing bug ever reached one, the client would receive a
silently fabricated success.

**Decision.** A shared `connection_level_execute_stub(name)` in `frogdb-core` (next to the
`Command` trait), returning a loud `CommandError::Internal` — reached only on a routing bug, and
now diagnosable instead of silent. The five transaction commands delegate to it. The full
trait-split alternative already half-exists as `CommandMetadata`/`register_metadata` (used by
pub/sub); migrating MULTI-family to metadata-only registration was considered and rejected for
this batch: `registry.get()` (the `Full`-only map) feeds the ACL/cross-slot guard key-extraction
paths, and WATCH's `KeySpec::All` keys must keep flowing through them — a behavioral risk far
exceeding this item's budget. Noted as follow-up if the guards move to `get_entry()`.

**PSYNC checked and deliberately not stubbed:** its `execute()` is real — it validates arguments
and returns the `PSYNC_HANDOFF` signal that the connection loop detects to hand the raw stream to
the replication handler (`connection/dispatch.rs`, `connection/lifecycle.rs`). WAIT (proposal 39)
likewise keeps its real deny-blocking execute. Also deleted: the never-registered metadata-only
duplicates in `server/src/commands/metadata.rs` (MultiMetadata, ExecMetadata, ..., HelloMetadata,
BgsaveMetadata, LastsaveMetadata) — dead parallel definitions of commands registered as full
`Command`s, several with flags that silently diverged from the live specs.

Registry exhaustiveness tests (`connection/router.rs`) stay green — they are strategy-driven and
the strategies are unchanged.

Commit: `6e22cf06`

### 8. Test-harness RESP3 `command()` is subscribe-aware

**Evidence.** Since commit 5f288af2 the server correctly sends RESP3 subscribe/unsubscribe
confirmations as Push-only frames. `Resp3TestClient::command()` buffered *every* Push while
waiting for a non-Push reply — so `command(&["SUBSCRIBE", ...])` waited for a frame that never
comes, hanging 6 pub/sub regression tests (and `tcl_resp3_tracking_redirection`, same signature)
into 5s timeouts.

**Change (harness only — the server is correct).** `command()` now mirrors how real RESP3 clients
complete an in-flight subscription call: when the command is one of the (un)subscribe family, a
Push frame whose first element matches the command's confirmation kind (`subscribe`,
`unsubscribe`, ...) **is** the reply; unrelated Push frames (e.g. `message`) are still buffered
for `read_message()`. Non-Push replies (e.g. `+QUEUED` inside MULTI, errors) return as before,
which keeps `SUBSCRIBE`-inside-MULTI working. Multi-channel subscribes return the first
confirmation; the rest arrive via `read_message()` — exactly what the existing tests
(`tcl_unsubscribe_inside_multi_and_publish_to_self`) already expect.

Commit: `1856acd5`

### 9. `BROADCAST_SHARD` stragglers routed through the const

**Evidence.** Proposal 33 introduced `BROADCAST_SHARD` (the shard that coordinates broadcast
pub/sub state) but two hard-coded `shard_senders[0]` broadcast-pub/sub sites remained: the CLIENT
TRACKING REDIRECT invalidation forwarder (`connection/lifecycle.rs` — the survey's
`handlers/client.rs:707` reference predates proposal 34 moving it) and the cluster-bus
`PubSubBroadcast` RPC delivery (`cluster_bus.rs`). Both now index by the const. The remaining
`shard_senders[0]` sites (search coordination, debug) are shard-0 for unrelated reasons and were
left alone — they are not broadcast pub/sub.

Commit: `d658c28b`

## Verification

Per touched crate: `just fmt` / `just check` / `just lint` + crate tests; plus the six previously
hanging pub/sub regression tests and `tcl_resp3_tracking_redirection` green:

- `pubsub_regression::ping_in_resp3_subscribed_mode_returns_pong`
- `pubsub_regression::ping_with_message_in_resp3_subscribed_mode`
- `pubsub_tcl::tcl_publish_to_self_inside_multi`
- `pubsub_tcl::tcl_unsubscribe_inside_multi_and_publish_to_self`
- `pubsub_tcl::tcl_pubsub_messages_with_client_reply_off`
- `pubsub_tcl::tcl_pubsub_ping_resp3`
