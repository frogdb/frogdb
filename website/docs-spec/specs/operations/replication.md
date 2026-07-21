# Spec: operations/replication.md

Status: rewrite (mostly correct today, but the failover/control-plane framing is
stale — the "external orchestrator" is fictional)
Audiences: A4 (primary), A3

Goal: The reader can set up primary/replica replication (roles, `REPLICAOF`),
understands `min-replicas-to-write` and the write-quorum/self-fencing behavior,
knows that full sync ships a SHA256-verified RocksDB checkpoint and partial sync
uses PSYNC2, and can monitor replication via `INFO replication`. Failover is
Raft-driven or manual — NOT orchestrator-managed.

Not in scope:
- Cluster metadata coordination / slot ownership — that is
  [Clustering](/operations/clustering/); replication is the data plane beneath it.
- Snapshot/checkpoint internals — that is
  [Persistence & durability](/operations/persistence/); link.
- Redis-compatible replication command semantics already documented by redis.io —
  document deltas, link upstream.

Sources of truth (read before writing):
- `frogdb-server/crates/replication/src/lib.rs` — PSYNC2 model doc (~10–32):
  PSYNC → FULLRESYNC/CONTINUE → WAL frames → REPLCONF ACK.
- `frogdb-server/crates/config/src/replication.rs` — every `[replication]` key +
  default (the authoritative list below).
- `frogdb-server/crates/replication/src/fullsync.rs` — RocksDB checkpoint
  transfer, 64KB chunks, SHA256 checksum verification, `fullsync-max-memory-mb`.
- `frogdb-server/crates/server/src/commands/info.rs` (~430–500) — the real `INFO
  replication` fields.
- `frogdb-server/crates/config/src/cluster.rs` — `auto-failover`,
  `self-fence-on-quorum-loss` (failover is Raft-coordinated, see Clustering).

Existing content: `operations/replication.md` — keep the config table and the
full-sync/checkpoint/SHA256/memory-aware facts (all VERIFIED); REWRITE the
"Orchestrated failover" framing and fix the `slave_repl_offset` monitoring field.

## Verified facts (authoritative)

**Model:** PSYNC2 asynchronous replication. Full sync ships a RocksDB checkpoint
(64KB chunks) with a SHA256 checksum verified on receipt; partial sync uses the
replication ID + offset (CONTINUE). Failover is Raft-driven (`auto-failover`, see
Clustering) or manual `CLUSTER FAILOVER` — there is NO external orchestrator.

**`[replication]` config keys (real, kebab-case, `deny_unknown_fields`):**
- `role` (default `"standalone"`; one of standalone/primary/replica)
- `primary-host` (`""`, required when role=replica), `primary-port` (6379)
- `min-replicas-to-write` (0 = disabled), `min-replicas-timeout-ms` (5000) —
  NOTE from source: after the timeout the write still proceeds with fewer acks;
  describe the real semantics, don't overstate it as a hard quorum.
- `ack-interval-ms` (1000), `fullsync-timeout-secs` (300),
  `fullsync-max-memory-mb` (512, rejects FULLRESYNC if exceeded)
- `state-file` (`replication_state.json`, holds repl ID + offset)
- connection tuning: `connect-timeout-ms` (5000), `handshake-timeout-ms` (10000),
  `reconnect-backoff-initial-ms` (100) / `-max-ms` (30000)
- lag guards: `replication-lag-threshold-bytes` (0) / `-secs` (0),
  `fullresync-cooldown-secs` (60)
- split-brain diagnostics: `split-brain-log-enabled` (true),
  `split-brain-buffer-size` (10000), `split-brain-buffer-max-mb` (64)
- self-fencing/freshness: `self-fence-on-replica-loss` (true),
  `replica-freshness-timeout-ms` (3000; should be ≥ 3× `ack-interval-ms` — the
  build warns otherwise), `replica-write-timeout-ms` (5000)

The current page's `[rocksdb] min-wal-retention-secs` / `min-wal-files-to-keep`
"WAL Retention" section is FABRICATED (no `[rocksdb]` section exists) — DELETE it.
`fullsync-max-memory-mb` is real and the memory-aware sync claim is correct.

**`INFO replication` real fields** (`info.rs`): `role`, `connected_slaves`,
`master_replid`, `master_replid2` (currently hardcoded zeros), `master_repl_offset`,
`second_repl_offset` (-1), `repl_backlog_active`, `repl_backlog_size` (1048576),
`repl_backlog_first_byte_offset`, `repl_backlog_histlen`; replica adds
`master_host`, `master_port`, `master_link_status`.
- Fix the monitoring section: the current page says compare `master_repl_offset`
  vs **`slave_repl_offset`** — that field name is NOT emitted; both sides report
  `master_repl_offset`. Correct the lag-measurement guidance accordingly.
- VERIFY-BEFORE-WRITING: the secondary replication ID (`master_replid2`) story on
  failover — INFO hardcodes it to zeros today, so do NOT document a working
  secondary-ID handoff without confirming the failover code path populates it.
  Keep the "new repl ID on promotion" description qualitative.

**Failover (corrected):** driven by Raft when `auto-failover=true` (leader
promotes a replica after `fail-threshold` failures, ordered by `replica-priority`)
or performed manually via `CLUSTER FAILOVER`. On promotion the new primary gets a
new replication ID and begins accepting writes; other replicas reconnect via
PSYNC. There is no orchestrator "detect → select → promote" loop — DELETE that.

Structure (H2/H3 — one line each):

- Intro: FrogDB does primary/replica async replication (PSYNC2); standard Redis
  replication commands work; this page covers setup, write quorum, sync, and
  monitoring. Link redis.io for compatible command semantics; link Clustering for
  the Raft control plane.
- **## How FrogDB differs from Redis replication** — keep the accurate bullets
  (RocksDB checkpoints for full resync instead of RDB; SHA256 verification;
  replicas don't evict independently; memory-aware sync) and FIX the first bullet
  (failover is Raft-driven/manual, not "external orchestrator").
- **## Configuration** — the `[replication]` table with the real keys/defaults
  above; `role` + `primary-host`/`primary-port` minimal setup; note `REPLICAOF`
  works at runtime.
- **## Write quorum (`min-replicas-to-write`)** — real semantics incl. the
  timeout behavior, plus `self-fence-on-replica-loss` / `replica-freshness-timeout-ms`.
- **## Full sync and partial sync** — checkpoint transfer + SHA256 +
  `fullsync-max-memory-mb`; PSYNC2 partial sync via repl ID/offset; qualitative
  note on repl-ID change at promotion. No WAL-retention config (delete).
- **## Failover** — Raft-driven (`auto-failover`) or manual `CLUSTER FAILOVER`;
  link Clustering. Replace the orchestrator steps entirely.
- **## Monitoring** — `INFO replication` with the REAL field names; measure lag
  via `master_repl_offset` on both sides + `master_link_status`; a short
  symptom/cause/fix table (fix the "increase min-wal-retention" row, which
  references a nonexistent key — replace with real guidance, e.g. lag thresholds
  / fullsync cooldown). Link Reference → Metrics for replication metrics (verify
  names).
- **## See also** — Clustering, Persistence, Configuration, Security (replication
  TLS).

Generated data:
- Links to Reference → Configuration reference (`config-reference.json` for
  `[replication]`), Reference → Metrics (S3), Compatibility → Command matrix
  (`compatibility/command-matrix`, REPLICAOF/ROLE via S1/S2). No embedded generated
  component.

Drift guards:
- `just docs-gen-check` keeps `[replication]` keys honest against
  `config/src/replication.rs`.
- S7 code-path check for cited `crates/replication/...` paths.
- INFO field names and metric names must be validated against source / the metrics
  catalog at write time (the current page shipped a nonexistent `slave_repl_offset`
  — exactly the kind of drift to guard).
- Content policy: no orchestrator language; no `[rocksdb]`/WAL-retention keys; no
  throughput/latency numbers.
