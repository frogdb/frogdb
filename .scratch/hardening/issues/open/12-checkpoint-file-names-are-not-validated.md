# A full-sync checkpoint file name is written to disk unvalidated

Status: needs-triage
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
