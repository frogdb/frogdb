# A wrong or unmounted data-dir boots as a fresh database

Status: done
Type: bug (data safety)
Severity: likelihood 2/3 (a mistyped data-dir, a volume that failed to mount, a container that lost
its bind mount), consequence 3/3 (an empty keyspace comes up healthy, accepts writes, and the
WAL/snapshot cadence starts persisting over the real state) — score 6
Area: recovery / persistence restore

## Problem

Split out of `.scratch/hardening/issues/done/06`, whose other halves (surfacing `keys_failed`, the
nothing-decoded refusal, and the `recovery.on-decode-failure` policy) are now shipped. This is
candidate 3 of that issue, untouched.

`has_data()` is a first-key-exists probe across the shard column families. A boot against an empty
*or wrong-but-writable* directory logs `No existing data found, starting fresh` and comes up with
an empty keyspace (FM-PERSISTENCE-029). That is correct on a genuine first boot and catastrophic
when `--data-dir` was mistyped, a volume failed to mount, or a container lost its bind mount:
"nothing here" and "I am not looking where you think I am" are the same observation.

Neither refusal added by issue 06 helps. Both trigger on values that *failed to decode*; a wrong
directory has no values at all, so it takes the `has_data() == false` path exactly as before. The
shard-count and warm-tier guards (FM-PERSISTENCE-030/031) do refuse loudly, but only once a
column-family layout has been stamped — a directory with no layout passes every guard.

Redis has the same shape for a missing RDB, but its blast radius is smaller: it does not
immediately begin overwriting a directory it believes is fresh.

## Candidate fix

Stamp a small marker (a `frogdb_data_dir` file, or an equivalent metadata record) at first
initialization, and require either the marker or an explicit fresh-start confirmation
(`--init`, or an empty-directory check) to boot with an empty keyspace. This turns "wrong
directory" into a refusal without affecting genuine first boots — the same trick the shard-count
guard already relies on, generalized from the layout to the directory itself.

Open sub-questions for whoever picks this up:

- Does an *empty* directory boot silently, or does it also want the explicit flag? (Container
  orchestration creates the mount point before FrogDB ever runs, so "the directory exists and is
  empty" is the normal first-boot shape, not an anomaly.)
- What does a replica do when it trips? A full resync would repopulate it — but from the primary,
  which is a different failure mode than starting fresh.
- Does the marker record anything worth checking beyond its own presence (a database id, a
  creation timestamp), so that swapping two data dirs is also caught?

## Forcing tests

A recovery-seam test that boots against a directory containing an unrelated file and asserts the
refusal, plus one that boots a genuinely fresh directory and asserts it still comes up. The
existing `frogdb-recovery` tests already have the fixtures.

## Resolution

Fixed fail-closed. Recovery stamps an identity marker into every data directory it initializes and
refuses to boot against a directory that holds files it did not stamp.

**The marker** (`frogdb-persistence`, `data_dir.rs`). `frogdb_data_dir` is JSON holding a generated
128-bit `database_id`, `created_at_unix_ms`, and `layout_version` (1 today). It is published like
every other durable rename in the crate — scratch write, fsync the file, rename onto the final
name, fsync the directory — through the `SnapshotFs` seam, so the ordering is assertable. A failed
publish removes its own scratch file, because a leftover `.tmp` would itself be a file in a
directory with no marker, which is the state the guard refuses. Reading is deliberately narrow:
`Ok(None)` means the file is not there and nothing else. An IO error, unparseable JSON, and a
layout version from the future are all errors, because `None` is the branch that initializes a
database.

**The decision tree** (`frogdb-recovery`, `data_dir.rs`, new `RecoveryPhase::VerifyDataDir`):

| Data directory | Verdict |
|---|---|
| marker present and readable | boot; every existing guard unchanged |
| no marker, no files | first boot: initialize and stamp |
| no marker, files present | refuse |
| marker unreadable, malformed, or from a newer layout | refuse |
| no marker, no files, `persistence.require-existing-data = true` | refuse |

Refusals name the *resolved absolute* path (a relative `data-dir` resolved against a working
directory the operator may not know is the confusion the guard exists to end), the marker file, and
the way out.

**Emptiness is about files, not entries.** `contains_files` walks the tree and stops at the first
non-directory. A freshly formatted ext4/xfs volume arrives with `lost+found`, and orchestration
routinely pre-creates subdirectories (the cluster storage path among them) before FrogDB ever runs;
counting entries would refuse the most common production first boot. Symlinks count as files —
following them would let a link farm read as empty.

**Ordering.** Phase 0 runs before the staged-checkpoint install, which puts it before the RocksDB
open and far before any replication dial: a replica that refused only after a full resync would
have replaced the operator's data with the primary's, which is a different failure with the same
lost bytes. The marker is written *after* the install, because installing a staged checkpoint
renames the whole directory aside and the staged directory carries no marker of its own. The id
written back is the one phase 0 read, so a directory's identity survives having its contents
replaced wholesale and the next boot does not refuse the database this one installed.

**The escape hatch** is `--force-fresh-data-dir`, a CLI flag with no config-file equivalent
(enforced by a test): a persisted override would disable the guard permanently, which is exactly
the situation the guard exists for. It adopts the directory as it is — nothing is deleted, existing
data is recovered normally — and one boot is enough, because the directory is stamped afterwards.
It covers the unreadable-marker refusal too, so a corrupt marker does not leave a directory
permanently unbootable.

**Sub-questions from the triage, answered.** An empty directory boots silently by default
(orchestration pre-creates mount points), with the caveat that an unmounted volume is
indistinguishable from a first boot by inspection — `persistence.require-existing-data` (default
false) is the opt-in that turns an empty directory into a refusal for deployments past their first
boot, documented honestly as a caveat rather than as detection. A replica refuses on the same
rules, before a resync can repopulate. The marker records a database id, but nothing cross-checks
it yet.

**Follow-up (not this round):** cross-directory swap detection. Catching "two nodes' data dirs were
swapped" needs the node's *expected* id held somewhere outside the directory it is checking
(process config, a cluster registry) — otherwise each node reads a valid marker and agrees with it.
The id is stamped now so that check has something stable to compare against later.

**Spec:** FM-PERSISTENCE-048 (emptiness is about files), 049 (the marker and its atomic publish),
050 (unreadable or future-layout markers refuse), 051 (populated-but-unmarked refuses), 052
(`require-existing-data`); 027 (phase order), 029 (the fresh-boot log line), and 032 amended.

**Evidence:** `just core-test persistence` 325/325, `just test frogdb-config` 124/124, `just test
frogdb-server` 1958/1958, `just lint-failure-modes` OK (252 modes, 1265 tags), clippy
`--all-targets` clean on the four touched crates. `just mutants-diff` on the locked crates:
`frogdb-recovery` 10 caught / 0 missed, `frogdb-persistence` 19 caught / 0 missed — the first
persistence run missed both `NotFound` guards (`DataDirMarker::read` and `contains_files`), i.e.
"I could not look" was still collapsing into "there is nothing here" for every IO failure that is
not absence, so two tests were added and the 048/050 rows updated.

**Docs:** `website/src/content/docs/operations/persistence.md` gained a "Data directory identity"
section (decision table, the adoption flag, the failed-mount caveat); `backup-restore.md` notes
which restore shapes need the flag. `website/src/data/*` regenerated with `just docs-gen`.
