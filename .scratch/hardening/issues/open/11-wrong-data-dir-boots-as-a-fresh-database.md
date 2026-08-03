# A wrong or unmounted data-dir boots as a fresh database

Status: needs-triage
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
