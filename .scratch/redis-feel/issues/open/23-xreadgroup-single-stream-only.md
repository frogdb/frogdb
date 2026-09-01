# `XREADGROUP` rejects multiple streams while `XREAD` accepts them

Status: ready-for-agent
Type: bug (compat gap)
Area: commands / stream

## Problem

```rust
// frogdb-server/crates/commands/src/stream/read.rs:271-276
// For now, only support single stream (no cross-shard)
if num_streams != 1 {
    return Err(CommandError::InvalidArgument {
        message: "XREADGROUP with multiple streams not yet supported".to_string(),
    });
}
```

`XREAD` in the same file parses and serves N streams (`read.rs:81-87`), so the limitation is not
a storage-model constraint — it is unfinished work. Redis 8.6 accepts N streams for both.
Consumer-group clients that fan a single `XREADGROUP` across several streams (the common
worker-pool shape) get an error FrogDB invents; the error text itself advertises the gap, so this
is not a documented deviation either.

## Ruling

Support N streams in `XREADGROUP`, matching `XREAD`'s existing multi-stream path.

Cross-slot behavior follows the same rule `XREAD` already applies — reuse it rather than inventing
a second policy. In cluster mode, keys spanning slots must produce `CROSSSLOT`, not a FrogDB-only
message.

Per-stream semantics that must survive the fan-out: `>` vs. explicit-ID reads are resolved per
stream; `NOACK` applies to every stream; consumer creation and the `XREADGROUP`-on-missing-key
`NOGROUP` error stay per-stream; blocking wakes when *any* named stream has data.

## Acceptance criteria

- [ ] `XREADGROUP GROUP g c COUNT 10 STREAMS s1 s2 > >` returns entries from both streams
- [ ] A missing group on one of several streams yields `NOGROUP` naming that stream
- [ ] Blocking `XREADGROUP ... BLOCK 0 STREAMS s1 s2 > >` wakes on an `XADD` to either stream
- [ ] Cluster mode: cross-slot stream keys produce `CROSSSLOT`, matching `XREAD`
- [ ] The "not yet supported" error string is gone from the codebase

Size: M
