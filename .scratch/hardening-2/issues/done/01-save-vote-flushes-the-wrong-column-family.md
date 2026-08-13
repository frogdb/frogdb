# Raft `save_vote` flushes the wrong column family

Status: done
Type: bug (consensus safety / durability)
Severity: likelihood 2/3 (every crash after a vote and before an unrelated default-CF flush),
consequence 3/3 (Raft's vote-durability precondition — a node can vote twice in one term) — score 5
Area: cluster / raft storage

## Problem

`ClusterRaftStorage::save_vote` (`frogdb-server/crates/cluster/src/storage.rs:483-489`) persists
the vote via `set_meta` (`:307-322`), which does a `put_cf` with **default `WriteOptions`**
(`sync = false`), and then calls `self.db.flush()`.

`KEY_VOTE` lives in the `raft_meta` column family (`:19`, `:264`). `DB::flush()` flushes the
**default** column family. The vote therefore sits in the `raft_meta` memtable with no WAL sync
and no CF flush behind it, and `save_vote` returns `Ok(())` — which openraft treats as "the vote
is durable, you may now respond to the RequestVote".

The crate's own doc comment at `:95-102` states verbatim that a plain `flush()` is not a
durability mechanism — the same reasoning that made `ClusterSnapshotStore::save` use
`write_opts.set_sync(true)` at `:139-144`, the only `set_sync(true)` in the workspace.

Raft's safety argument requires that a node never votes twice in the same term. A power cut that
loses the vote record lets the node grant a second vote after restart, which is a split-brain
precondition, not a performance detail.

This is the sibling of round-2 issue 73 (`append` acks durability with a non-`sync` write at
`:538-542`), found by the campaign-2 durability survey while confirming 73.

## Candidate fix

Mirror `ClusterSnapshotStore::save`: a `WriteOptions` with `set_sync(true)` on the `set_meta`
write, and drop the misleading `flush()`. Same treatment applies to `save_committed` (`:503-513`)
— evaluate whether committed-index durability has the same precondition or is legitimately
recoverable from the log.

## Forcing test

Needs the campaign-2 crash harness (W2): a subprocess node that `SIGKILL`s immediately after
`save_vote` returns, restarts, and asserts the restored vote equals the one that was acked.
Until that exists, a `frogdb-cluster` unit test can at minimum assert that the write options used
on the vote path carry `sync = true` (a seam assertion, not a durability proof) — record it as a
level-2 witness in the durability spec, not as the row's final evidence.

## Resolution

Fixed 2026-08-08 under **FM-CLUSTER-098** (`specs/cluster.md`).

Durability is now a property of the metadata *key* rather than of the caller: `MetaDurability`
(`cluster/src/storage.rs`) classifies `KEY_VOTE` as `Synced` and `KEY_COMMITTED` /
`KEY_LAST_PURGED` as `Buffered`, and `set_meta`/`delete_meta` render that class into the
`WriteOptions` at the single chokepoint every metadata write passes through. `save_vote` is now
just `self.set_meta(KEY_VOTE, vote)` — the wrong-CF `flush()` is gone.

The candidate fix's second half was evaluated as asked: `save_committed` stays buffered, for the
reason its own doc comment already gives (the key is deliberately write-only; openraft re-derives
the commit index from the leader, and reading back an index that names a lost log tail is worse
than not reading it). `purge`'s `KEY_LAST_PURGED` stays buffered too — losing it un-purges a
prefix the snapshot still covers and the next purge redoes the work. Neither had the *flush* bug:
`save_vote` was the only caller that flushed at all.

Seam lint: `scripts/durable-ack.py` had `save_vote` in its count-pinned allowlist as a tracked
defect, and its "sync" test was an inline `write_opt(..)` + `set_sync(true)` in the method body —
which the chokepoint form does not match, so the fix would have gone on being reported as an open
defect. The gate now recognises the delegated shape as durable *only while all three links hold*
(`for_key` classifies the key `Synced`, `write_opts` renders the class into `set_sync`, `set_meta`
passes those options to an options-carrying write), and the `save_vote` allowlist entry is gone.
Both broken-link cases were verified to fail the gate. `append` (round-2 issue 73) remains the one
allowlisted entry.

Forcing test: `the_vote_is_written_synced_to_the_meta_column_family` (level-2 witness, as this
issue proposed) — it asserts the classification the vote path resolves to, that the record is in
`raft_meta` and not the default CF, and that the default CF is empty, so the old `flush()` could
never have been what made the vote durable. Verified to fail when the classification is regressed
to `Buffered`. The level-5 crash witness still needs the campaign-2 crash harness; the sibling
issue 73 (`append` acks non-synced) remains open and carries that machinery with it.

## Comments

Found by the campaign-2 durability-extraction survey, 2026-08-07. Not detectable by mutation
testing: there is no mutant for a `set_sync(true)` call that was never written.
