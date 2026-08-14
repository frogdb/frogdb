# 30: `replication_state.json` — atomic, durable, single-writer save

Status: ready-for-agent

## Origin

Distsys-review CRIT-6 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

`ReplicationState::save()` (`replication/src/state.rs:279-297`) writes the node's
replication identity with a fixed temp path, no fsync, and no writer exclusion:

```rust
let temp_path = path.with_extension("tmp");
let contents = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
fs::write(&temp_path, contents)?;
fs::rename(temp_path, path)?;
```

Two independent writers reach it — `save_state` (periodic hook + shutdown hook) and
`save_snapshot` (promotion) — sharing that one temp path. `fs::write` truncates then
writes, so a concurrent pair can tear the temp file or interleave renames into a file
whose fields come from two different states. Neither the file nor its parent directory is
ever fsynced, so even a well-ordered rename is not durable. TR-REPLICATION-031 notes the
collision as an observation only; no FM row forbids it.

The failure lands exactly where it hurts: promotion. `save_snapshot` writes the new
`master_replid`/`replid2`/offset while a periodic `save_state` fires concurrently.
Torn file → `validate()` fails on next boot → the node mints a fresh replication
identity → every downstream replica full-resyncs (PSYNC2 failover window destroyed).
Parsed-but-mixed file → pre-promotion `replid` with post-promotion offset — a
correctness fault. Both routes reach FM-REPLICATION-020's forbidden half-promoted node
while `save_snapshot` returns `Ok`, a route that row never considers (it reasons only
about `Err`).

Persistence already owns the correct pattern: `stamp_with` does
write → `sync_file` → rename → `sync_dir`, with a `RecordingFs` trace test asserting the
order (FM-PERSISTENCE-049). The replication-identity file — more safety-critical than
most of what that machinery protects — skips all of it. (TigerBeetle/FDB: fsync before
publish, non-negotiable; etcd: one goroutine owns member-state writes.)

## What to build (spec-first)

1. New FM row in `specs/replication.md`: "a promotion snapshot concurrent with a
   periodic state save leaves a valid, promotion-complete file on disk"; amend
   FM-REPLICATION-020 to cover the `Ok`-but-clobbered route and TR-REPLICATION-031 to
   state the exclusion as a property, not an observation.
2. Reuse persistence's `stamp_with` (fsync file → rename → fsync dir) rather than
   reimplementing; unique temp name (pid + nonce) so no two writers can share one.
3. Serialize the writers: one mutex or one owning task through which both `save_state`
   and `save_snapshot` funnel — last-writer-wins on the *whole* state, never a merge of
   halves.
4. Forcing tests: concurrent promotion-save + periodic-save (loom/shuttle or repeated
   race) asserting the on-disk file always validates and is promotion-complete once
   promotion returns; a `RecordingFs`-style order trace for the new path.

## Acceptance criteria

- [ ] FM row added; FM-REPLICATION-020 / TR-REPLICATION-031 amended; `just lint-spec`
      green
- [ ] Save path = `stamp_with` shape with unique temp; trace test asserts
      write → sync_file → rename → sync_dir
- [ ] Writers serialized; concurrent-save forcing test fails pre-fix, passes post-fix
- [ ] `just mutants-diff` on frogdb-replication (locked, gate 0.85) triaged

## Blocked by

None — can start immediately.
