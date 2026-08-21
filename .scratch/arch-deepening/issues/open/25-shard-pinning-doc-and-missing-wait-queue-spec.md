# 25 — Architecture page claims connections are pinned to shards; shard wait-queue failure-mode spec was promised and never written

Status: needs-triage

## What to build

Two findings from the same proposal, both process/documentation defects rather than code bugs, both
filed here because each needs an owner and neither belongs inside a deletion commit.

### H4 — "All connections pinned to it" is false

`website/src/content/docs/architecture/architecture.md:31` states, in the Shared-Nothing Threading
bullet list describing what a shard worker owns: "All connections pinned to it". Nothing pins a
connection to a shard. Connections are served by tasks the acceptor spawns directly
(`frogdb-server/crates/server/src/acceptor.rs:358-360`), and each command routes to a shard per key.
The claim almost certainly dates from the dead `NewConnection` seam documented three lines below at
`:39` ("`NewConnection` for connection assignment from the acceptor") and again at `:326` and
`:453` — a mechanism whose handler was moved and whose channel is constructed but never sent on.
Proposal 81's hotfix H3 deletes those three `NewConnection` lines; `:31` is a **broader statement
in the same section** and rewriting it correctly requires stating the actual shard-ownership model
(a shard owns a keyspace partition and its `Store`; connections are not owned by any shard), which
is why it is a follow-up rather than part of that deletion. The page is hand-written — no
`docs-spec` source names `NewConnection`, and no generator produces this content page — so it is
edited directly.

Blast radius is reader-facing only, but it is the load-bearing sentence of the concurrency model on
the public architecture page, and it will keep re-seeding wrong mental models (including in agent
work) until it is corrected.

### H5 — the shard-side wait-queue failure-mode spec does not exist

`.scratch/hardening/specs/blocking-failure-modes.md:8-14` scopes itself to the **connection-side**
blocking path and explicitly cedes the rest: "the shard-side wait queue (registration, FIFO
`pop_oldest_*`, the acknowledged `UnregisterWait` handshake, restore-on-send-failure) lives in
`frogdb-core` and gets its own spec." No such spec exists — the specs directory holds exactly six
files (`blocking`, `cluster`, `persistence`, `replication`, `txn`, `vll`) and none covers the shard
wait queue. `frogdb-server/crates/core/src/shard/wait_queue.rs` carries **zero `FM-` tags**, so no
locked row governs any of its behaviour.

This is precisely the class of gap that let proposal 81's LIVE duplicate-key defect ship: the
invariant it violates — *every index in `waiters_by_key` resolves to a live entry that names that
key, with matching multiplicity* — is one spec row, and would have been forced by one test. The
proposal drafted a five-clause invariant that should serve as the spec's starting point:

1. every index in `waiters_by_key` points at a live entry;
2. that entry's `keys` contains the map key;
3. occurrences of `idx` under `key` == occurrences of `key` in `entry.keys`;
4. `waiter_count == entries.iter().flatten().count()`;
5. `free_slots` ∩ live-entry indices == ∅.

Clause (3) is the one the shipped defect violates. Fix direction: write
`.scratch/hardening/specs/shard-wait-queue-failure-modes.md` following the txn-spec row format,
covering registration, kind-aware pop, `unregister`, `collect_expired`, `drain_waiters_for_slot`
and restore-on-send-failure, with the five clauses as invariants and an `FM-WAITQUEUE-NNN` row per
resolution mode; then tag forcing tests so `just lint-failure-modes` enforces the agreement. Decide
at the same time whether the new spec pulls `frogdb-core` under a mutation gate or stays
spec-only.

## Acceptance criteria

- [ ] `architecture.md:31` no longer claims connections are pinned to a shard; the replacement text
      describes the real model (keyspace partition + `Store` ownership; connections served by
      acceptor-spawned tasks that route per key) and does not reintroduce `NewConnection`.
- [ ] `.scratch/hardening/specs/shard-wait-queue-failure-modes.md` exists, carries the five-clause
      index invariant, and has one `FM-` row per wait-queue resolution mode with named forcing
      tests.
- [ ] `blocking-failure-modes.md:8-14`'s forward reference links to the new spec by name instead of
      promising an unwritten one.
- [ ] `just lint-failure-modes` passes with the new spec included (every row names its forcing
      tests, every tagged test matches a row).
- [ ] `just test frogdb-core wait_queue` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 81 (`.scratch/arch-deepening/proposals/81-core-dead-seams.md`),
defects H4 and H5.

## Comments
