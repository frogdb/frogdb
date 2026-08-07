# Raft `save_vote` flushes the wrong column family

Status: ready-for-agent
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

## Comments

Found by the campaign-2 durability-extraction survey, 2026-08-07. Not detectable by mutation
testing: there is no mutant for a `set_sync(true)` call that was never written.
