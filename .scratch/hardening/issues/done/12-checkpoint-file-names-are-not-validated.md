# A full-sync checkpoint file name is written to disk unvalidated

Status: done
Type: bug (security)
Severity: likelihood 1/3 (needs a hostile or compromised primary, or an attacker who can occupy the
primary's address during a `REPLICAOF`), consequence 3/3 (arbitrary file write outside the data
directory, as the FrogDB process user, before any checksum is verified) — score 3
Area: replication / full sync

## Problem

`CheckpointStreamCodec::read_file_header` (`frogdb-server/crates/replication/src/fullsync.rs:266`)
decodes the per-file name from the wire and returns it with no validation beyond a length bound:

```rust
let name_len = parse_dollar_len(&line, "invalid filename length", MAX_CHECKPOINT_NAME_LEN)?;
let mut name_buf = vec![0u8; name_len + 2];
r.read_exact(&mut name_buf).await?;
let name = String::from_utf8_lossy(&name_buf[..name_len]).to_string();
```

The receiver then joins that name straight onto the staging directory
(`frogdb-server/crates/replication/src/fullsync/receiver.rs:47`):

```rust
let file_path = incoming.join(&header.name);
let checksum = receive_to_file(reader, &file_path, header.size, None).await?;
```

`Path::join` with an absolute component *replaces* the base, so a name of `/etc/cron.d/frogdb`
writes to `/etc/cron.d/frogdb`; a name of `../../..../authorized_keys` escapes the staging
directory by traversal. Both happen before the combined checksum is finalized, so the checkpoint
being rejected afterwards does not undo the write. `MAX_CHECKPOINT_NAME_LEN` bounds only how much
path an attacker gets per file, not where it points, and the file count is attacker-chosen too.

`String::from_utf8_lossy` compounds it: invalid UTF-8 becomes U+FFFD rather than an error, so a
name that is not a legal name at all still produces a path, and the name that goes into
`combined.update_file(&header.name, ...)` is not the bytes the primary sent — two distinct wire
names can fold into the same checksum input.

A replica trusts its primary for *data*, which is unavoidable. It does not have to trust it for
*file system layout*, which is what this grants.

## Candidate fix

Validate inside `read_file_header`, so every caller inherits it and the bad frame is refused at the
codec boundary rather than at the file system:

- decode with `str::from_utf8` and return `io::ErrorKind::InvalidData` on failure (drop the lossy
  conversion — a checkpoint file name is always ASCII in practice);
- reject empty names;
- require the name to be exactly one `Component::Normal` — that rejects `/abs`, `..`, `a/b`, and
  Windows-style prefixes in a single check;
- keep the existing length bound.

`receive_to_file` should additionally assert the resolved parent is the staging directory, as a
belt-and-braces check for any future caller that builds a path some other way.

## Forcing tests

Codec-level unit tests in `fullsync.rs` for each rejected shape (absolute, traversal, nested,
empty, invalid UTF-8), asserting `InvalidData` and that no file was created. One receiver-level
test that drives a full checkpoint stream whose second file header is `../escaped` and asserts the
transfer fails and the parent directory is untouched. Spec row FM-REPLICATION-034 covers the
"partially received checkpoint is never staged" half; this wants its own row for the name.

## Resolution

Fixed as proposed, at the codec boundary. `checkpoint_file_name(name)` in `fullsync.rs` is the one
rule — the name must decompose to exactly one `std::path::Component::Normal` **and** re-encode to
itself — and `read_file_header` decodes with `std::str::from_utf8` (not lossily) before applying it:

```rust
let name = std::str::from_utf8(&name_buf[..name_len]).map_err(|_| { /* InvalidData */ })?;
let name = checkpoint_file_name(name)?.to_string();
```

Two things the triage note did not anticipate:

- The **round-trip half** of the component rule is load-bearing on its own. `Path::components()`
  normalizes, so `CURRENT`, `CURRENT/` and `CURRENT/.` all decompose to a single `Normal("CURRENT")`.
  A component-count check alone would accept all three, letting three distinct wire names land on one
  staged file while `CheckpointChecksum::update_file` folds three *different* byte strings into the
  combined checksum — the same aliasing `from_utf8_lossy` caused, by a different route.
- The suggested "assert the resolved parent is the staging directory" in `receive_to_file` was
  implemented structurally instead of as an assertion: it now takes `(dir, name)` rather than a
  pre-joined `path`, re-validates the name, and joins it itself. Containment is a property of the
  function rather than of every caller, and `receive_checkpoint_files` no longer joins anything.

The existing proptest `prop_file_header_sequence_round_trips` generated names from
`[a-zA-Z0-9._-]{0,64}`, which can emit `""`, `"."` and `".."`; its strategy is now
`[a-zA-Z0-9_-][a-zA-Z0-9._-]{0,63}`, which cannot.

Forcing tests: `read_file_header_refuses_names_that_are_not_one_component` (absolute, `..`,
embedded traversal, nested, empty, `.`, `..`, trailing slash, normalizing suffix, invalid UTF-8 —
each asserted `InvalidData`, plus the legal shapes still decoding),
`receive_to_file_refuses_a_name_that_escapes_its_directory`, and
`receiver_refuses_a_file_name_that_escapes_the_staging_dir` (full envelope; asserts the escape
target does not exist and the staging dir is empty). Spec row **FM-REPLICATION-044**.
