# Adversarial review — `specs/persistence.md` (LOCKED)

Reviewer lens: modern storage/distributed-systems practice (PostgreSQL, RocksDB/Pebble+CockroachDB,
FoundationDB, SQLite-WAL, Redis/Valkey). Read-only audit; no files edited, no builds run.
Spec read in full (733 lines, FM-PERSISTENCE-001..052). Code was read only to confirm or refute a
finding, never to propose an edit.

**Verdict: NOT clean.** 1 CRITICAL, 4 HIGH, 5 ADVISORY on the spec itself, plus 3 HIGH / 3 ADVISORY
hazards created by the issue-24 ruling. The spec is unusually good on rename discipline, fail-open
avoidance, and clock hygiene; the gaps are concentrated in **what happens when durability
*fails*** — the fsyncgate family — and in one place where a headline guarantee is conditioned on a
non-default setting the row never names.

---

## CRITICAL

### C1 — FM-PERSISTENCE-002: `sync` durability does not gate the ack under the *default* failure policy

**Row:** FM-PERSISTENCE-002 (with FM-PERSISTENCE-005, FM-PERSISTENCE-043).

**Claim under audit.** The row's Observable is unconditional: *"Every write the client saw a reply
for is in the recovered keyspace."* Its Invariant grounds that in
`persist_records` *under `Durability::Confirm`* snapshotting the sequence and calling
`flush_through(start_seq)` "so the reply cannot outrun the fsync".

**What the code actually does.** `Durability::Confirm` is selected by *one* predicate, and it is not
the durability mode:

- `frogdb-server/crates/core/src/shard/execution.rs:436` —
  `let rollback_mode = is_write && self.persistence.has_wal() && self.persistence.should_rollback();`
- `should_rollback()` (`frogdb-server/crates/core/src/shard/types.rs:426`) is *only*
  `policy == WalFailurePolicy::Rollback`.
- The `Confirm` call sites (`execution.rs:465`, `execution.rs:653`) are both inside
  `if rollback_mode`.
- The default path — `WriteEffectKind::WalPersistence` in
  `frogdb-server/crates/core/src/shard/post_execution.rs:401` — persists with
  `Durability::FireAndForget`, which per FM-PERSISTENCE-005's own forcing test
  (`fire_and_forget_never_flushes_and_continues_on_error`) **never calls `flush_through`**.

`persistence.wal-failure-policy` defaults to `continue` (FM-PERSISTENCE-005 calls it "**the
default**"). So with `durability-mode = sync` and the shipped default failure policy, the WAL entry
is merely *enqueued on the bounded channel* when the reply is produced. The flush thread commits it
later (size threshold / batch timeout) with `sync = true`, but the client already has its `+OK`.
A power loss in that window loses an acknowledged write in the mode whose entire contract is that it
cannot. FM-PERSISTENCE-019 even states the general rule — *"a write is acknowledged as soon as it is
staged in its shard's flush engine"* — and attributes it to "the default non-`sync` durability",
implying a `sync` branch that the code does not contain.

**Why the mutation gate did not catch it.** None of the row's forcing tests exercise the shard ack
path:

- `test_sync_mode_zero_acked_write_loss` is a `PageCacheWal` model that calls `wal.flush()` after
  every write, with the comment *"In sync mode a write is acked only once its commit fsyncs; flush
  after each write models that per-write durability ack."* It **assumes** the property under test.
- `test_sync_mode_crash_recovery` / `test_durability_mode_gates_crash_loss` use
  `CrashTestHarness::put_direct` (`frogdb-server/crates/core/src/persistence/test_harness.rs:158`),
  which serializes and calls `write_tracked(.., sync)` straight at the sink — the shard,
  `persist_records`, and the ack ordering are all bypassed.
- `confirm_snapshots_sequence_then_flushes_once` passes `Durability::Confirm` in by hand.

So the row is forced at the *sink* layer and asserted at the *client* layer, with the layer that
connects them untested. This is precisely the failure mode the spec's own preamble warns about
("a mutant that survives is a row nothing forces") — here the mutant is not even needed.

**Modern practice.** PostgreSQL (`synchronous_commit = on`), MySQL/InnoDB
(`innodb_flush_log_at_trx_commit = 1`), RocksDB (`WriteOptions::sync`), etcd, and Redis
`appendfsync always` all make the *commit acknowledgement* wait on the group-commit fsync, and none
of them couple that decision to an unrelated error-handling policy. Durability level and
failure policy are orthogonal knobs everywhere.

**Recommended change.**

1. Make the `Durability` selection a function of the durability mode *or* the failure policy, not
   the policy alone:
   `Confirm` iff `should_rollback() || durability_mode == Sync` (and keep rollback-on-error gated on
   the policy). Cheapest correct fix; `sync` mode already pays an fsync per batch.
2. Rewrite FM-PERSISTENCE-002's Invariant to name *both* conditions explicitly, and add to its NOT
   observable: *"a `sync`-mode write acknowledged with only a `FireAndForget` stage behind it."*
3. Add an end-to-end forcing test in `frogdb-core` that goes through `execute_command` with
   `durability-mode = sync` **and** `wal-failure-policy = continue`, asserts a `flush_through`
   reached the sink before the reply value was produced, and crashes the `PageCacheSink` immediately
   after the reply.
4. Until (1) lands, FM-PERSISTENCE-002 must be re-scoped in writing to `wal-failure-policy =
   rollback`, and the `sync`+`continue` combination documented as a loss window in
   [Redis deviations] — it is currently a silent, undocumented deviation from `appendfsync always`.

---

## HIGH

### H1 — FM-PERSISTENCE-007: a durability failure is un-latched by a later success (fsyncgate)

**Row:** FM-PERSISTENCE-007, reinforced by FM-PERSISTENCE-005 and FM-PERSISTENCE-043.

FM-PERSISTENCE-007's NOT observable says, verbatim:

> Equally: the failure staying latched forever after the sink recovers — once a later commit
> succeeds past the failed range, `confirm_durable_through` stops reporting it, so a transient error
> does not poison every subsequent write.

Treating a failed durability operation as transient-and-clearable is the fsyncgate anti-pattern in
its canonical form. On Linux (and to varying degrees on other kernels), a writeback error marks the
page clean-but-failed and reports the error to *one* `fsync()` caller; a subsequent `fsync()` on the
same file returns `0` while the data is gone forever. "A later commit succeeded past the failed
range" is therefore **not** evidence that the earlier range is on the device — it is evidence of
nothing at all about that range.

The consequence is concrete: sequence 100's commit fails; sequences 140–150 commit successfully; a
`Confirm` for `(140, 150]` finds `highest_failed_seq = 100` outside its range and returns `Ok`. The
caller acks. Sequence 100 is permanently absent from the recovered keyspace and no client ever
learned it. Under `wal-failure-policy = rollback` (the policy an operator chooses *specifically* to
avoid acking lost writes) this is the exact hazard the policy exists to prevent, reached by a
different door.

The spec has no row anywhere for "the fsync itself returned an error". Every failure row
(005/007/008) is written in terms of "the sink failed" / "a stage call failed", conflating a
*write* error (retryable, the data is still in the buffer) with a *sync* error (poison, the data may
be unrecoverable). The RocksDB options actually set (`frogdb-server/crates/persistence/src/rocks/mod.rs`)
include no `paranoid_checks`, no error-handler/auto-resume configuration, and no statement of what
happens when RocksDB latches a background error — so the spec's "transient" framing is not backed by
a layer that distinguishes the two either.

**Modern practice.**
- PostgreSQL since 2018 (the fsyncgate response): a failed `fsync()` on a data file is a **PANIC**,
  full stop — the process dies and recovery replays from WAL. `data_sync_retry` defaults to `off`
  precisely because retrying is unsound.
- WiredTiger/MongoDB and MySQL/InnoDB likewise abort the process on write/sync failure of the log.
- RocksDB latches a background error, puts the DB in read-only mode, and only auto-resumes for
  IO errors it can *prove* retryable (`IOStatus::IsRetryable()`); everything else requires an
  explicit `Resume()` after operator action.
- CockroachDB/Pebble treat a WAL sync failure as fatal to the node — the node is fenced and the
  range is served by another replica.

None of them clear the condition because a later operation succeeded.

**Recommended change.**

1. Split the failure taxonomy in the spec: `stage`/`write` failure (recoverable — buffer still
   owns the bytes) vs **sync failure** (poison). Add a row, e.g. *FM-PERSISTENCE-053 — a failed
   durability sync poisons the shard until an operator resolves it*.
2. Replace the "un-latch on later success" NOT observable with the opposite invariant:
   `highest_failed_seq` (or a new `poisoned_seq`) is monotone and is cleared **only** by a
   process restart / explicit operator `RESET`, never by a subsequent successful commit.
3. Under `rollback`, a poisoned shard must refuse writes (`-IOERR`, or `-MISCONF` reusing
   FM-PERSISTENCE-046's gate) rather than silently resume acking.
4. State explicitly what FrogDB relies on from RocksDB: whether `paranoid_checks` is on, whether
   auto-resume is enabled, and that a RocksDB background error surfaces as a persist error rather
   than being swallowed. Right now none of that is in the spec and none of it is set in the options
   builder.

### H2 — FM-PERSISTENCE-003: "loss is always a suffix" is falsified by FM-PERSISTENCE-005/007

**Rows:** FM-PERSISTENCE-003 NOT observable vs FM-PERSISTENCE-005 Invariant and FM-PERSISTENCE-007.

FM-PERSISTENCE-003 asserts as NOT observable:

> Also not observable: a *gap* — a recovered keyspace holding a later write while a strictly earlier
> one from the same shard is missing (the WAL commits in enqueue order, so loss is always a suffix).

That reasoning covers only *crash* loss. It does not cover *failure* loss, and FM-PERSISTENCE-005
creates gaps by construction, twice over:

- **Within a batch:** *"`stage_actions` under `FireAndForget` logs each failure and continues to the
  next action."* Action 1 fails, action 2 succeeds → hole inside one command's WAL footprint. For a
  multi-action command (`RENAME` = write + delete; `MSET`) that also breaks
  FM-PERSISTENCE-001's all-or-nothing property — the group markers make the batch one `WriteBatch`,
  but only over the actions that actually staged.
- **Across batches:** batch N fails, batch N+1 succeeds → the recovered keyspace holds N+1 and not
  N. FM-PERSISTENCE-007's un-latching (H1) then lets an operator's `Confirm` above the hole return
  `Ok`.

The blast radius reaches beyond persistence. WAL-order prefix consistency is what replication and
cluster consume; a hole means the recovered state corresponds to no point in the write history —
read-your-writes is violated, and a replica that later full-syncs from this node ships a
never-existed state. Every serious system treats the log as strictly prefix-recoverable: PostgreSQL,
RocksDB's `PointInTime`, Raft logs, and Kafka's log all define recovery as *some prefix*, never a
subset.

**Recommended change.** Either

- (preferred) make a stage failure under `FireAndForget` abort the remainder of that shard's
  in-flight stream — i.e. after the first failure the shard is prefix-truncated and enters the
  poisoned state of H1 — so "loss is a suffix" becomes true again; or
- (minimum) delete the gap claim from FM-PERSISTENCE-003 and add an explicit row stating that under
  `wal-failure-policy = continue` the recovered keyspace may be an arbitrary *subset*, not a prefix,
  of the acknowledged history, with the downstream consequences for replication/full-sync named.

The current text asserts the strong property while another row implements the weak one, which is the
worst of both: readers and future forcing tests will be written against the claim.

### H3 — FM-PERSISTENCE-034: point-in-time recovery over many column families without atomic flush

**Row:** FM-PERSISTENCE-034 (with FM-PERSISTENCE-030/031/036/045).

The row pins `DBRecoveryMode::PointInTime` and argues it correctly against `AbsoluteConsistency` and
`SkipAnyCorruptedRecord`. What it never mentions is that FrogDB's database is a **multi-column-family**
database: `shard_<n>` per shard, `warm_<n>` when tiering is on, plus a search-meta CF
(FM-PERSISTENCE-030/031 exist entirely because of this layout). The options builder
(`frogdb-server/crates/persistence/src/rocks/mod.rs:188`) sets the recovery mode and **does not set
`atomic_flush`**.

RocksDB's own guidance is explicit here: without `atomic_flush`, each CF flushes its memtable
independently, so after a crash different CFs are recovered to different points; combined with
`kPointInTimeRecovery` (which stops replay at the first inconsistency) the result can be a state
where CF A contains writes that CF B, written in the same batch, does not. RocksDB recommends
`atomic_flush = true` for exactly this combination.

For FrogDB this is not academic, because two CFs hold *one logical key*:

- A tiering demotion is (warm put, hot delete). Recover the warm put without the hot delete → both
  copies live; recover the hot delete without the warm put → the key is gone. FM-PERSISTENCE-045's
  `warm_keys_stale` term exists precisely because a warm entry can be shadowed by a hot copy — the
  invariant that decides which wins is a cross-CF invariant that non-atomic recovery can break.
- FM-PERSISTENCE-036 additionally *deletes* expired warm entries from disk at recovery time, so a
  divergent recovery is then made durable.
- The search-meta CF vs the data CF is the same shape (FM-PERSISTENCE-019 drains search indexes and
  the WAL as two separate waves).

Note the rest of FM-PERSISTENCE-001's atomicity story is genuinely safe — a single-shard write group
is one `WriteBatch` into one CF — so this is specifically about the *tier/index* CFs, not shards.

**Recommended change.** Set `db_opts.set_atomic_flush(true)` and pin it with a test the way the
recovery mode is pinned (`wal_recovery_mode_is_pinned_to_point_in_time` is the model to copy).
Extend FM-PERSISTENCE-034's Invariant to state the CF-consistency requirement and its NOT
observable to include *"a recovered database in which the hot and warm column families of the same
shard disagree about a key"*, with a forcing test that crashes between a warm put and its hot
delete. If `atomic_flush` is deliberately declined for write-throughput reasons, that trade must be
written down with the resurrection risk named.

### H4 — FM-PERSISTENCE-024/025: staged-checkpoint completeness is one `stat()`, and a bad install has no rollback

**Rows:** FM-PERSISTENCE-024, FM-PERSISTENCE-025, FM-PERSISTENCE-026 (and FM-PERSISTENCE-017 on the
producing side).

FM-PERSISTENCE-024's guard is `is_complete_db()`, and the implementation
(`frogdb-server/crates/persistence/src/rocks/staged.rs:83`) is:

```rust
pub fn is_complete_db(&self) -> bool {
    self.dir.join(ROCKSDB_CURRENT_MANIFEST).exists()
}
```

That is "the file named `CURRENT` exists". It does not read `CURRENT`, does not open the MANIFEST it
points at, does not check that the SSTs the MANIFEST lists are present, and does not verify a single
byte. Its own forcing test only proves that a directory of stray `.sst` files with no `CURRENT` is
rejected — the complementary case (a `CURRENT` present, SSTs missing or truncated) is untested.

This matters because `checkpoint_ready` is populated by two untrusted-ish paths: an **operator copy**
(FM-PERSISTENCE-021: `cp snapshot_NNNNN/checkpoint/. …`) and a **replica full sync download**. A
truncated download or an interrupted `cp` that happened to complete `CURRENT` first passes the gate.
The install then renames the live database aside and moves the broken checkpoint into its place. The
subsequent `OpenRocks` fails — and there is **no rollback**: `checkpoint_ready` has been consumed by
rename 2, so FM-PERSISTENCE-025's idempotence logic sees a completed install, and every subsequent
boot fails identically with a `<db>_backup_<ts>` sitting next to it that nothing will ever restore.
The node is down until a human diagnoses a rename. FM-PERSISTENCE-026 compounds it: `BACKUP_RETENTION
= 1`, so a *second* install (a retried full sync) overwrites the only copy of the good database.

The producing side is no stronger: `SnapshotMetadataFile`
(`frogdb-server/crates/persistence/src/snapshot/metadata.rs:25`) carries `size_bytes`,
`sequence_number`, `num_shards` and a `completion_marker` string, but nothing at restore time
compares the installed tree against any of them.

**Modern practice.** RocksDB ships `VerifyChecksum()` and `DB::VerifyFileChecksums()`, and
`Checkpoint` can emit file checksums for exactly this purpose. CockroachDB verifies SST checksums on
ingest before the ingest is allowed to mutate the store. PostgreSQL's `pg_basebackup` writes a
`backup_manifest` with per-file sizes and checksums and ships `pg_verifybackup` to check it before a
restore is attempted. SQLite validates the WAL header/salt before applying a single frame. The
common rule: *validate the replacement completely before destroying the thing it replaces.*

**Recommended change.**

1. Strengthen `is_complete_db()` to a real verdict: read `CURRENT`, parse the MANIFEST it names, and
   confirm every referenced SST/blob file exists with the expected size. Cheap, no checksum cost.
2. Have the stager write a payload manifest (relative path, size, and ideally checksum per file)
   into `metadata.json`, and have the install verify the staged tree against it when one is present.
   `size_bytes` already exists and is already unverified — the cheapest first step is to compare it.
3. Add a rollback row: if the post-install `OpenRocks` fails, the boot restores `<db>_backup_<ts>`
   over `<db>` and refuses with a message naming what happened, rather than leaving a node that
   cannot boot and a backup nothing consumes. Alternatively, defer the "delete/replace the live db"
   step until after a trial open of the staged directory.
4. FM-PERSISTENCE-024's NOT observable currently reads *"The live database being renamed aside for a
   checkpoint that cannot replace it"* — which is exactly the outcome the current one-`stat()` check
   permits. Either the check has to earn that sentence or the sentence has to be weakened.

---

## ADVISORY

### A1 — FM-PERSISTENCE-026: backup generation ordering is wall-clock, and the clock is not a safe key

`backup_dir_name` (`frogdb-server/crates/persistence/src/rocks/staged.rs:94`) is
`format!("{db_name}_backup_{unix_secs}")`. FM-PERSISTENCE-026 rightly makes the comparison numeric
rather than lexical, but the *number is a wall-clock second*. Two consequences:

- An NTP step backwards (or a VM restored from a snapshot with a stale RTC) between two installs
  makes the newer backup sort older, so retention-1 deletes the one it should keep.
- Two installs inside the same second collide on the name; the spec does not say what happens
  (overwrite? `ENOTEMPTY` and a failed install? — FM-PERSISTENCE-017 does mention `ENOTEMPTY` on the
  snapshot side). A retried full sync can plausibly do this.

Snapshot epochs already avoid this correctly (a monotone counter, FM-PERSISTENCE-015). The backup
suffix should be the same kind of thing: a monotone counter seeded from the highest existing suffix,
or `<epoch>_<unix_secs>` sorted on the counter, with the timestamp kept only for human readability.
Blast radius is bounded (backups are hygiene, per the row itself), which is why this is advisory —
but the row's own NOT observable, *"`_backup_2` outranking `_backup_10`"*, is about ordering
correctness, and a clock-derived key reintroduces the class it closed.

### A2 — FM-PERSISTENCE-005: `continue` has no fail-stop escalation, only a counter that grows forever

The default policy acks writes it has lost, forever, with the only signal being `INFO` fields and a
log line rate-limited to one per 100 failures. The spec defends this as
"availability-over-durability" and points at FM-PERSISTENCE-046 for the snapshot-side refusal — but
FM-PERSISTENCE-046 is gated on *snapshot* failure, not WAL failure, and the deviations table says so
explicitly ("there is no snapshot involved, so no snapshot policy can gate it").

So the shipped default for a permanently broken data device is: serve reads from memory, ack every
write, lose every write, and never refuse anything. That is a strictly worse position than Redis'
`MISCONF` default, and there is no third option between "ack the loss" (`continue`) and "refuse
per-write with a rollback snapshot cost on every write" (`rollback`).

Recommend a third policy value — `readonly` / `fail-stop` — that keeps the zero-cost hot path of
`continue` but flips a latch on the first (or Nth) failure and refuses WRITE-flagged commands
through the same `PreChecks` gate FM-PERSISTENCE-046 already built. That reuses existing machinery,
gives operators the Redis-parity behaviour for the WAL as well as the snapshot, and pairs naturally
with the poison latch of H1. Worth a ruling rather than an assumed default.

### A3 — FM-PERSISTENCE-043: `Durability::Confirm` means "committed", not "durable", and the name says otherwise

The row is honest and well argued — `committed_seq` vs `synced_seq` is exactly the right split, and
`publish_synced_through` snapshotting `committed_seq` *before* `flush()` so it cannot over-claim is a
detail most implementations get wrong. Two residual concerns:

- The *name* `Durability::Confirm` reads as a durability guarantee at every call site, while in
  `periodic`/`async` it confirms page-cache residency. Rename to `Durability::Committed`, or split
  into `Committed` / `Synced` with `Synced` being what a future `WAIT`/`WAITAOF`-class primitive
  binds to.
- The spec never states which client-visible primitive (if any) is wired to `synced_seq`. Redis
  `WAITAOF` counts *fsynced* replicas specifically. If any FrogDB acknowledgement primitive is wired
  to `committed_seq`, that is a durability claim the mode did not make, and it should be pinned by a
  row here rather than left to the replication spec.

### A4 — FM-PERSISTENCE-017/042: the snapshot's integrity story stops at "a marker file exists"

Adoption of a snapshot requires `metadata.json` carrying `FROGDB_SNAPSHOT_COMPLETE_v1`. That proves
the *stager finished*; it proves nothing about the payload. Since these artifacts are explicitly
long-lived backups ("a month-old artifact reports as a month old", FM-PERSISTENCE-042) sitting on
media that bit-rots, and since they are restored by an operator copy into `checkpoint_ready` where
the only gate is H4's one `stat()`, the end-to-end backup path has no integrity check at any point.

Same recommendation as H4.2 (payload manifest in `metadata.json`), plus a `frogctl`-side verify
command so an operator can validate a backup *before* the outage during which they need it. This is
what `pg_verifybackup` exists for.

### A5 — FM-PERSISTENCE-037 / FM-PERSISTENCE-049: two acknowledged blind spots worth re-rating

Both are already documented as known costs; flagging only that they are each one line away from
being closed:

- **FM-PERSISTENCE-037**: *"The known cost is a silently smaller `FUNCTION LIST` with no counter or
  metric to notice it by."* Everything else in this spec that skips data increments a counter and
  surfaces it in `INFO` (FM-PERSISTENCE-033 is the model). One `frogdb_recovery_functions_failed_total`
  plus an `INFO` field makes this consistent with the rest of the document.
- **FM-PERSISTENCE-049**: the `database_id` is minted, stamped, and *never compared to anything*
  ("nothing compares it across boots yet, because that needs out-of-band expected-id state"). The
  out-of-band state now has an obvious owner — see R4 below, where the issue-24 ruling needs exactly
  this key.

---

## Where the spec already matches or exceeds best practice

Listed because they are load-bearing and should not be lost in a rewrite:

1. **FM-PERSISTENCE-023 — fsync on both sides of every publishing rename.** Payload fsynced before
   the rename, containing directory fsynced after, via a `SnapshotFs` seam so the *order* is
   testable rather than a claim about syscalls, degrading to a no-op on platforms that cannot open
   directories. This is the PostgreSQL/SQLite rule stated correctly, and most implementations skip
   the directory fsync. The `metadata.json.tmp` → fsync → rename → fsync-dir sequencing that makes
   the commit rename unable to become durable ahead of the metadata it publishes is exactly right.
2. **FM-PERSISTENCE-032 — a failed CF enumeration is never coerced to "fresh".** The
   `unwrap_or_default()`-reads-as-empty-database bug, identified and closed with the reasoning
   spelled out. This is the single most common catastrophic fail-open in storage code.
3. **FM-PERSISTENCE-048..052 — the data-directory identity marker.** Correctly better than Redis,
   and close to CockroachDB's store identity files: first-*file* (not entry-count) emptiness so
   `lost+found` and pre-created mount points still boot; `NotFound` the only listing error that means
   "empty"; "unreadable" never collapsing into "absent"; the override a CLI flag with
   `#[serde(skip)]` so it cannot be persisted into a permanent disarm; `require-existing-data` for
   the unmounted-volume case that genuinely cannot be distinguished by inspection. The reasoning
   quality here is the high-water mark of the document.
4. **FM-PERSISTENCE-035 — the deliberately un-fsynced watermark.** The asymmetry argument (the file
   can only lag, so the detector under-reports rather than false-alarms; a torn file parses as
   absent) is correct and correctly chosen. Re-baselining after reporting, so the same loss is not
   re-reported every boot, is the right operational call.
5. **FM-PERSISTENCE-014 — HLL replay idempotence by algebra, not by a dedup table.** Register-max
   folding is idempotent and order-independent, and registering *the same* fold as the partial merge
   operator so operands that outrun their base pre-combine without changing the answer is a genuinely
   elegant piece of design.
6. **FM-PERSISTENCE-025/026 — "the staged directory's *name* is the state".** Rename-2-consumes-it
   makes the install idempotent by construction rather than by a marker anyone has to maintain, and
   rename-1-preserves-rather-than-deletes is the right default. Numeric-not-lexical suffix ordering
   is the classic bug, caught.
7. **FM-PERSISTENCE-019/020 — drain, not freeze; and no timeout on the drain.** Arguing the cut's
   correctness from *enqueue ordering* on the same FIFO the writes travelled (so no lock is needed),
   and fanning out to all shards before awaiting any (so the cost is one flush latency, not
   `num_shards`), is the right shape. FM-PERSISTENCE-020's observation that *because* there is no
   timeout a failure can only mean "the shard is gone" and never "the shard is busy" — which is what
   makes failing the cut safe rather than a liveness hazard — is a better argument than most
   production systems make for the same decision.
8. **FM-PERSISTENCE-022/042 — clock discipline.** Monotonic `Instant` for durations (so a wall-clock
   step cannot invent a negative or hour-long save), `SystemTime` only for the value that must
   legitimately predate the process, sourced from `completed_at_ms` *inside* the durable artifact
   rather than the directory mtime (so a copy that rewrites mtimes cannot lie), and both duration
   fields as `Option`s rendering to Redis' `-1` at the edge. Wall-clock time is nowhere a correctness
   input in this spec — a rarer property than it sounds.
9. **FM-PERSISTENCE-012 — post-clear reclamation.** `delete_file_in_range_cf` + forced bottommost
   compaction is the RocksDB-recommended bulk-delete combo, and the module's argument that L0 files
   are never dropped by `DeleteFilesInRange` (so post-clear writes, which are in the memtable/L0,
   cannot be reclaimed away) is the right justification — this is the place I expected to find a
   real bug and did not.
10. **FM-PERSISTENCE-045/047 — the nothing-decoded refusal, and binary rather than fractional.**
    "A database that yielded not one decodable value is broken by any threshold anyone would pick"
    is the correct predicate to need no configuration, and declining a percentage threshold because
    it needs a force-boot override and a replica policy that are not settled is exactly the right
    reason to decline it.

---

## Issue 24 ruling (2026-08-13) — persistence-layer hazard assessment

**Ruling as recorded:** *option a, structural* — replication identity + offset move INTO the
checkpoint/dataset metadata (Redis RDB-aux shape), so identity cannot outlive the dataset by
construction; a boot that recovers no dataset mints a fresh replid; subsumes option c; closes issue
21's restart-reuse path; new FM row; remove the `restart_tainted_replids` / XREPL-2 sweep exemption.

**Overall assessment.** The direction is right and matches Redis/Valkey (`repl-id`/`repl-offset` as
RDB aux fields restored in `loadDataFromDisk`), and FrogDB already has half of it: FM-PERSISTENCE-039
ships `replication_metadata.json` inside the staged checkpoint and states the principle —
*"Offset durability is coupled to snapshot durability by shipping the metadata inside the checkpoint
(compare Redis' RDB aux fields)"* — with the recovery-order dependency (FM-PERSISTENCE-027) already
justified by it. So this is an extension of an existing, well-reasoned mechanism rather than a new
one. But the ruling's one-line framing ("identity cannot outlive the dataset **by construction**")
is stronger than what the persistence layer can currently deliver, in four specific places.

### R1 (HIGH) — "inside the checkpoint" must mean *inside RocksDB*, not a sidecar file in the snapshot directory

Today's staged metadata is a **file next to the checkpoint**
(`STAGED_REPLICATION_METADATA_FILE`, `frogdb-server/crates/persistence/src/rocks/staged.rs:87`), not
a record inside the database. That is safe *today* only because it is written for the full-sync path
where the offset is fixed at handshake time. If the ruling generalizes it to every checkpoint
(BGSAVE included), a sidecar reintroduces the exact split-lifetime problem the ruling is closing,
one level down: the RocksDB checkpoint is cut at sequence S, and the sidecar is written by the
stager at some later wall moment. Nothing makes the offset in the sidecar the offset that
corresponds to S. Crash between them and you get a checkpoint whose payload and whose identity
describe different instants — which is issue 24 again, at a finer grain.

**Recommendation.** Store `repl_id` / `repl_offset` as a key in a RocksDB column family, written
inside the pre-checkpoint quiesce window of FM-PERSISTENCE-019 — specifically after the WAL-drain
wave acks and before `create_checkpoint` — so the identity is part of the checkpointed dataset and
rides the same atomic cut. This is precisely what Redis' RDB aux fields are: bytes *in* the artifact,
not beside it. If a sidecar is kept for compatibility, it must be a *derived copy* and the in-dataset
record must be authoritative, with the sidecar ignored whenever the two disagree.

If the identity record lands in its own CF, it becomes another instance of H3 — cross-CF consistency
under `PointInTime` recovery — which is a second, independent reason to set `atomic_flush`. Putting
it in the shard-0 CF (or in a CF that is written in the *same* `WriteBatch` as nothing else) avoids
the question entirely.

### R2 (HIGH) — a crash-recovery boot has no install, and the WAL can replay *past* or *short of* the stamped offset

The ruling's mental model is the full-sync/checkpoint boot. The common boot is neither: a restart
against the node's own data directory, where FM-PERSISTENCE-021 is explicit that *"restart recovery
is RocksDB + WAL, not the snapshot"*. On that path:

- The WAL has advanced **past** the last checkpoint's stamped offset, so a stamped `repl_offset` is
  stale-low and must not be adopted as-is. Redis has the same issue and solves it because the AOF/RDB
  pair is replayed to the end; FrogDB's equivalent is "the offset must be reconstructed from what was
  actually replayed", which requires the offset to be derivable from the WAL, not only from the
  checkpoint aux.
- Worse, FM-PERSISTENCE-034/035 mean the WAL can replay **short**: point-in-time truncation discards
  a torn tail (and, after a mid-log corruption, the valid suffix behind it), and
  `frogdb_wal_recovery_dropped_records_total` counts exactly how much. A dataset silently rolled back
  by N records while the identity+offset it carries are unchanged is issue 24 with the file renamed —
  identity survives history it no longer has. The sweep witness for issue 24 (a SIGKILL restart)
  is on precisely this path.

**Recommendation.** The new FM row must state the *restart* case, not just the install case, and
must couple the recovered offset to the recovered data:

- If `frogdb_wal_recovery_dropped_records_total > 0` for this boot (FM-PERSISTENCE-035 already
  computes it and already re-baselines), the node **mints a fresh replid** and must not grant
  `+CONTINUE` on the old one. Records were dropped ⇒ the history is not the one the id names.
- The adopted offset must be the offset the replayed dataset actually reaches, not the checkpoint's
  stamped value. If the WAL does not carry replication offsets today, the row should say how the two
  are reconciled — a stamped-low offset that is then advanced by replay is a *different* mechanism
  from an aux field and needs its own invariant.

This is the single highest-value addition the ruling can make, because it is the only path that
reaches the original sweep failure without any checkpoint being involved.

### R3 (HIGH) — "recovered no dataset" is binary in the ruling and three-valued in the spec

The ruling says *"a boot that recovers no dataset naturally mints a fresh replid"*. Persistence has
three outcomes, not two:

- **nothing recovered** — FM-PERSISTENCE-029 (empty), FM-PERSISTENCE-028 (persistence disabled).
  Mint fresh. This is the case the ruling handles.
- **everything recovered** — adopt. Fine.
- **partially recovered** — FM-PERSISTENCE-033's default `continue`: `keys_failed > 0`,
  `keys_loaded > 0`. The node adopts an identity and offset naming a history whose bytes are
  *partly* absent, and can then grant a partial resync from an offset whose data it no longer has.
  A replica continued from there silently diverges — the same damage as issue 24, reached by
  corruption instead of by restart.

**Recommendation.** Make the predicate *"recovered the dataset **intact**"*, not *"recovered a
dataset"*. Concretely, mint a fresh replid (or at minimum force full resync by refusing `+CONTINUE`)
whenever `RecoveryStats::keys_failed > 0` **or** WAL records were dropped (R2). Both counters already
exist, are already node-wide aggregates at one seam
(`frogdb_recovery::shards::report_decode_failures`), and are already carried to `INFO` on
`AdminDeps` — so the wiring cost is near zero and the seam is already the "one boot, one signal"
place the spec designed for exactly this kind of decision.

### R4 (ADVISORY) — retire `replication_state.json` as an authority, or key it to the data directory's identity

FM-PERSISTENCE-038 currently regenerates a missing/corrupt state file *and writes it back*, and
FM-PERSISTENCE-039 has to give staged metadata an unconditional win over it. Once the dataset is
authoritative, leaving a second durable copy of identity in a file with a different lifetime is how
this bug comes back — it is literally the mechanism the issue's "Precedent" section names
(*"FrogDB splits the pair across two files with different lifetimes, and the identity file is the one
that always survives"*).

Note the install *does* consume it today — `checkpoint_ready` replaces the whole data directory, so
`<data_dir>/replication_state.json` goes with it (this is why FM-PERSISTENCE-039's ordering works).
The residual risk is the **restart** path of R2, where the file survives and the dataset may have
been truncated under it.

**Recommendation.** Either delete the file outright, or demote it to a cache stamped with the data
directory's `database_id` (FM-PERSISTENCE-049 — which currently mints an id that *nothing compares to
anything*; this gives it its first real consumer) plus the recovered sequence number, and ignore it
whenever either does not match the opened database. FM-PERSISTENCE-038's "regenerate and write back"
must not quietly re-establish a durable identity for a node that recovered no dataset — that is the
exact shape the ruling is removing.

### R5 (ADVISORY) — recovery phase order and gating both change; three rows need edits

FM-PERSISTENCE-027 currently justifies phase 5 (`RestoreReplicationState`) running after the install
*because the install carries `replication_metadata.json` into the data dir*. If identity lives in the
dataset, phase 5 additionally depends on phase 2 (`OpenRocks`) and phase 3 (`RestoreShards` — for the
R3 intactness verdict), and it stops being "role-gated, not persistence-gated" in the way
FM-PERSISTENCE-028 describes. That is a *stronger* dependency chain, which is good, but it must be
written down:

- **FM-PERSISTENCE-027** — new data dependency: identity now reads from the opened store, and the
  intactness verdict from `RestoreShards`, so phase 5 can no longer be reordered ahead of either.
  Its NOT observable already contains the right sentence (*"a replication offset restored before the
  install, which would pair the old offset with the new dataset"*) — extend it to "…or before the
  restore that decides whether the dataset is intact".
- **FM-PERSISTENCE-028** — a persistence-disabled node has no dataset and therefore mints a fresh
  identity **every boot**. Today the row says a persistence-less replica "still restores its
  identity"; under the ruling that sentence inverts. This is the persistence-disabled case the ruling
  explicitly wants covered, and it lands in this row.
- **FM-PERSISTENCE-038/039** — 038's regeneration semantics and 039's "staged outranks the state
  file" both need restating against a dataset-authoritative source. 039's Invariant already contains
  the exact principle the ruling is generalizing; it should become the general row rather than the
  full-sync special case.

### R6 (ADVISORY) — reuse the checkpoint's existing sequence anchor rather than inventing one

`SnapshotMetadataFile` already records `sequence_number` (the RocksDB sequence at the cut). Whatever
carries `repl_offset` should be validated against it — same cut, same window — so a mismatch is
detectable as corruption rather than silently adopted. That is the useful half of the ruling's
superseded option (c) ("treat a mismatch with the recovered dataset as corruption"), and it survives
the move to option (a) at almost no cost. Combined with H4/A4's payload manifest, it also gives the
full-sync install path a cheap consistency assertion it does not have today.

---

## Summary table

| # | Severity | Row(s) | One-line |
|---|---|---|---|
| C1 | CRITICAL | 002 (005, 043) | `sync` mode acks before fsync under the default `wal-failure-policy = continue`; `Confirm` is gated on the rollback policy alone, and no forcing test crosses the shard ack path |
| H1 | HIGH | 007 (005, 043) | Durability failure treated as transient and un-latched by a later success — fsyncgate; no row distinguishes a write error from a sync error |
| H2 | HIGH | 003 vs 005/007 | "Loss is always a suffix" is false; `continue` creates recovery holes and acks writes above them |
| H3 | HIGH | 034 (030/031/036/045) | `PointInTime` over many CFs without `atomic_flush` — hot/warm/search CFs can recover to different points |
| H4 | HIGH | 024/025 (017/026) | Staged-checkpoint completeness is `CURRENT.exists()`; no payload verification and no rollback when the installed DB will not open |
| A1 | ADVISORY | 026 | Backup generation key is a wall-clock second — clock step or same-second collision defeats retention-1 |
| A2 | ADVISORY | 005 (046) | No fail-stop option between "ack the loss" and "rollback every write"; add `wal-failure-policy = readonly` |
| A3 | ADVISORY | 043 | `Durability::Confirm` names a guarantee it does not make in `periodic`/`async`; no row binds a client primitive to `synced_seq` |
| A4 | ADVISORY | 017/042 | Backups carry a completion marker but no payload integrity; add a manifest and a verify command |
| A5 | ADVISORY | 037/049 | Two self-documented blind spots (no function-load counter; `database_id` compared to nothing) each one line from closure |
| R1 | HIGH (issue 24) | 039/019 | Identity must live *in* the checkpointed dataset written inside the quiesce window, not in a sidecar beside it |
| R2 | HIGH (issue 24) | 034/035/021 | The restart path has no install; WAL truncation rolls the dataset back under an unchanged identity — mint fresh when records were dropped |
| R3 | HIGH (issue 24) | 033/045 | "Recovered no dataset" must be "recovered the dataset intact"; `keys_failed > 0` must also force a fresh id |
| R4 | ADVISORY (issue 24) | 038/039/049 | Retire `replication_state.json` as an authority, or key it to `database_id` + recovered sequence |
| R5 | ADVISORY (issue 24) | 027/028/038/039 | Phase-order and gating rows all change; persistence-disabled now mints fresh every boot |
| R6 | ADVISORY (issue 24) | 017/039 | Validate `repl_offset` against the checkpoint's existing `sequence_number` — free corruption check |
