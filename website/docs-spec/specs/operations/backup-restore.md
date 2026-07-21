# Spec: operations/backup-restore.md

Status: update (correct the fabricated config keys and the wrong restore paths;
keep the overall shape)
Audiences: A4

Goal: The reader can take a consistent backup of a FrogDB instance (via a
snapshot / BGSAVE and off-host copy, or via a dedicated backup replica) and
restore it correctly, understanding the real on-disk layout (a snapshot directory
contains a RocksDB `checkpoint/`, a `metadata.json`, and copied search indexes —
it is not a flat set of DB files) and how durability mode affects backup
consistency.

Not in scope:
- Durability-mode mechanics and snapshot internals — that is
  [Persistence & durability](/operations/persistence/); link.
- Replication setup and failover — that is
  [Replication](/operations/replication/); link (a replica is a live backup, but
  the setup lives there).
- Point-in-time recovery via WAL retention — the current page describes this on
  top of config keys that DO NOT EXIST; see FABRICATED list. Cut it unless a real
  mechanism is verified.

Sources of truth (read before writing):
- `frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs` — what a
  snapshot directory actually contains (`checkpoint/`, `metadata.json`,
  `search/`).
- `frogdb-server/crates/persistence/src/rocks/checkpoint.rs` — `create_checkpoint`
  and `load_staged_checkpoint` (the `checkpoint_ready` staging path).
- `frogdb-server/crates/config/src/persistence.rs` — real `[persistence]` /
  `[snapshot]` keys; confirm there is NO `[rocksdb]` section and NO WAL-retention
  keys.
- `frogdb-server/crates/server/src/server/startup.rs` — how a restored data-dir
  is opened/recovered (RocksDB replays its own WAL on open).
- `frogctl/src/commands/backup.rs` — the real `frogctl backup` command (it issues
  `BGSAVE`); use this instead of inventing frogctl subcommands.
- `frogdb-server/crates/server/src/connection/handlers/persistence.rs` — `BGSAVE`
  handler (triggers `start_snapshot`).

Existing content: `operations/backup-restore.md` — keep the structure (snapshot
backup, replica backup, restore, durability interaction) but correct every path
and delete the fabricated PITR/WAL-retention config.

## Verified facts

- **Snapshot = RocksDB checkpoint + metadata + search indexes.** A snapshot dir
  (`snapshot-dir`, default `./snapshots`, dirs `snapshot_NNNNN`) contains a
  `checkpoint/` subdir (the hard-linked RocksDB copy), `metadata.json`, and copied
  Tantivy search indexes. This matters for restore.
- **`BGSAVE` triggers a snapshot** (`persistence.rs` handler → `start_snapshot`).
  `frogctl backup` exists and issues `BGSAVE` — use it, do not invent
  `frogctl debug wal-*` (those do not exist).
- **`data-dir` (default `./frogdb-data`) IS the RocksDB directory.** On restart
  RocksDB opens `data-dir` directly and replays its internal WAL — there is no
  separate FrogDB snapshot-load step at startup (see Persistence spec). So a
  restore that "replaces the data dir and restarts" relies on RocksDB recovery.
- **Restore path caveat (VERIFY-BEFORE-WRITING the exact commands):** the current
  page's `cp -r /path/to/backup/snapshot/* /var/lib/frogdb/data/` is WRONG — that
  copies `metadata.json`/`checkpoint/`/`search/` into the spot RocksDB expects DB
  files directly, and it will not open cleanly. The correct restore copies the
  contents of the snapshot's `checkpoint/` into `data-dir` (and the search indexes
  into their expected location). The author MUST confirm the exact target layout
  against `load_staged_checkpoint` / `startup.rs` and write tested commands, or
  present restore via the `checkpoint_ready` staging mechanism if that is the
  supported path. Do not ship the current `cp` command.
- **Backup-consistency vs durability mode** (keep this table, it's directionally
  correct): `async` → snapshot may be ahead of on-disk WAL; `periodic` → within
  `sync-interval-ms`; `sync` → fully consistent. Note the real key is
  `sync-interval-ms` — there is no `fsync-interval-ms` key; avoid that fabricated
  variant seen on other pages.
- **Replica-as-backup:** a replica is a live copy; promotion is Raft-driven or
  manual `CLUSTER FAILOVER` (NOT "orchestrator promotes" — the current page's
  orchestrator sentence is stale, fix it). Offline backup: stop a dedicated backup
  replica, copy its data-dir, restart; it re-syncs on reconnect (full resync via
  checkpoint if it fell too far behind — link Replication).

## FABRICATED content to DELETE

| Current claim | Verdict |
|---|---|
| `[rocksdb] min-wal-retention-secs` / `min-wal-files-to-keep` | FABRICATED — no `[rocksdb]` section, keys do not exist |
| "Point-in-Time Recovery … replays WAL from nearest snapshot up to target sequence, max 1h replay" | UNVERIFIED — no config/code backing; cut |
| Restore `cp -r snapshot/* .../data/` exact commands | WRONG paths — rewrite against snapshot layout |
| "orchestrator promotes the replica" | STALE — Raft/manual failover |

Structure (H2/H3 — one line each):

- Intro: FrogDB backs up by copying RocksDB snapshots off-host or by using a
  replica as a live backup; restore replaces the data directory and lets RocksDB
  recover on open. Link Persistence.
- **## Snapshot-based backup** — configure `[snapshot]` (dir/interval/max), take a
  manual snapshot with `BGSAVE` or `frogctl backup`, then copy the snapshot dir
  off-host (keep the `aws s3 sync` example but point it at `snapshot-dir`). One
  sentence on what a snapshot dir contains so restore makes sense.
- **## Replica-based backup** — a replica is a live backup; for offline backups
  stop a dedicated backup replica, copy its data-dir, restart. Correct the
  failover sentence (Raft/manual). Link Replication.
- **## Restore** — the corrected, verified procedure: stop server, restore the DB
  files into `data-dir` from the snapshot's `checkpoint/` (exact commands to be
  verified against `startup.rs`/`load_staged_checkpoint`), fix ownership, restart,
  verify with `redis-cli ping` + `dbsize`. Flag clearly that the author verifies
  the exact copy target before publishing.
- **## Durability mode and backup consistency** — keep the table; tie to
  Persistence.
- **## See also** — Persistence, Replication, Configuration.

Generated data:
- Links to Reference → Configuration reference for `[snapshot]` keys; Reference →
  frogctl for `frogctl backup`. No embedded generated component.

Drift guards:
- `just docs-gen-check` keeps `[snapshot]` keys honest.
- Reference → frogctl (S5) is the source for the `frogctl backup` invocation;
  link rather than hand-documenting flags.
- S7 code-path check covers the cited `crates/persistence/...` paths.
- Content policy: no config keys that don't exist (this page had fabricated
  `[rocksdb]` keys); no untested restore commands — the restore section must be
  verified against the real snapshot layout before publish.
