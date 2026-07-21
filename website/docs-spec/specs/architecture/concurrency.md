# Spec: architecture/concurrency.md
Status: update
Audiences: A2, A3, A5

Goal: A systems-minded reader walks away understanding FrogDB's shared-nothing
concurrency model and *why* it avoids locks: every internal shard is owned by a
single Tokio task (the shard worker), so its data is touched by exactly one task
and needs no synchronization; connections are pinned to one shard for their
lifetime and act as coordinators that reach other shards only by message-passing
over bounded channels (which provide backpressure); multi-shard operations are
made atomic through VLL-style ordered locking driven by a global monotonic txid.
The reader also learns exactly how a key maps to a shard, how hash tags force
colocation, and how the message and channel machinery is shaped.

Not in scope: Full VLL internals (vll.md owns intent tables, SCA, lock modes);
the full scatter-gather sequence (request-flows.md §3); storage/eviction
(storage.md). Reference those pages; do not restate them.

Sources of truth (author must read and reconcile every claim):
- `frogdb-server/crates/core/src/shard/helpers.rs` — **authoritative** routing:
  `shard_for_key(key, num_shards) = CRC16_XMODEM(hash_tag) % 16384 % num_shards`
  and `slot_for_key = CRC16_XMODEM(hash_tag) % 16384`, plus `extract_hash_tag`.
- `frogdb-server/crates/core/src/shard/message.rs` — the real `ShardMessage`
  enum (dozens of variants).
- `frogdb-server/crates/server/src/server/util.rs` — `SHARD_CHANNEL_CAPACITY`
  (`= 1024`) and `NEW_CONN_CHANNEL_CAPACITY`.
- `frogdb-server/crates/server/src/server/init.rs` — where the shard channel is
  built (`mpsc::channel(SHARD_CHANNEL_CAPACITY)`).
- `frogdb-server/crates/config/src/` — confirm whether any of the "config
  settings" the page tabulates (`shard-channel-capacity`,
  `scatter-gather-timeout-ms`, `client-timeout-ms`) actually exist as config
  keys and their real defaults.
- `frogdb-server/crates/vll/src/` — txid counter and queue semantics for the
  transaction-ordering section (`coordinator.rs`, `queue.rs`, `types.rs`).

Existing content: `website/src/content/docs/architecture/concurrency.md`.
Good structure and rationale; contains at least one factually wrong claim (the
routing hash) plus several config figures that must be verified.

Structure (keep existing H2s, corrected):
- ## Terminology — shard worker, thread, Tokio task, thread-per-core. Verify the
  "runtime typically pins long-running tasks to threads" nuance is stated
  honestly (Tokio's multi-thread runtime does not pin tasks; keep the wording
  precise about what is and isn't guaranteed).
- ## Thread Architecture — main/acceptor/shard-worker layout (N = num_cpus;
  verify default shard count source).
- ## Connection Model — pinned connection as coordinator.
- ## Key Hashing — **must be corrected** to the real algorithm (see Drift
  guards). Show `shard_for_key` as it actually is (CRC16-derived).
- ## Hash Tags — extraction rules, edge cases, `extract_hash_tag` code. Verify
  the edge-case table against the real function.
- ## Full Colocation Guarantee — both cluster slot and internal shard derive
  from the *same* CRC16 hash of the same hash tag, so a hash tag colocates at
  both levels. (This is actually a cleaner story than the current "two different
  algorithms" framing — see Drift guards.)
- ## Message Types — show `ShardMessage` as an *illustrative* excerpt clearly
  labeled as partial; link to `message.rs` for the full enum.
- ## Channel Configuration + Backpressure — verify capacity and whether it is a
  const or a config key.
- ## Coordinator Assignment / Scatter-Gather Flow — keep; cross-link
  request-flows.md §3 and vll.md.
- ## Transaction Ordering (VLL) — global txid counter, per-shard ordered queue,
  VLL property table, timeout config. Trim scalability micro-claims (see Drift
  guards).

Generated data: None required. Timeout/capacity figures should ideally come
from S6/config generation once available; until then they must match the config
crate exactly.

Drift guards:
- **Routing hash is WRONG and must be fixed.** The page states internal routing
  is `xxhash64(hash_key) % num_shards`. The real code
  (`shard/helpers.rs::shard_for_key`) uses **CRC16 (XMODEM)**:
  `CRC16(hash_tag) % 16384 % num_shards`. xxhash64 in this codebase is used only
  for probabilistic structures (bloom/cms/topk/cuckoo), **not** shard routing.
  Correct the `shard_for_key` code block, the "Full Colocation Guarantee"
  section (internal level is CRC16-derived, not xxhash64), and align with
  storage.md and architecture.md, whose "dual hashing" tables carry the same
  error. request-flows.md §2 (`slot mod num_shards`) is the correct description;
  make all four pages consistent on CRC16.
- **ShardMessage excerpt.** The shown enum is a small subset. Label it
  illustrative and point to `core/src/shard/message.rs`; never imply it is
  complete. Verify any variant kept (e.g. `VllExecute` field names) against
  source.
- **Config figures.** Verify each tabulated setting exists and its default:
  `SHARD_CHANNEL_CAPACITY` is a **compile-time const (1024)**, not a
  `shard-channel-capacity` config key — do not present it as runtime-tunable
  unless a config parameter is added. Cross-check `scatter-gather-timeout-ms`
  (page shows 5000 in one table, 1000 "default" in another — reconcile) and
  `client-timeout-ms` against the config crate.
- **Scalability micro-claims.** The txid section asserts cycle counts
  ("~10-50 cycles", "~50-100M txids/second per core", "~0.01% of operation
  latency at 1M ops/s"). These are unbacked performance numbers (PLAN §6). Remove
  them or replace with a qualitative statement ("a single atomic fetch-add;
  contention here is negligible relative to network I/O"). Keep the "why SeqCst"
  rationale, which is a design statement, not a measurement.
- **S7 code-path check.** Any `crates/...` path named must exist; include this
  page in the S7 scan.
