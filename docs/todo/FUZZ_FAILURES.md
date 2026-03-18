# Fuzz Failures

## 1. OOM in `frogdb_persistence::deserialize`

**Status:** Open
**Found:** 2026-03-18
**Target:** `deserialize`

### Description

The `deserialize` fuzz target found an out-of-memory bug: a ~34-byte crafted input causes unbounded memory allocation in `frogdb_persistence::deserialize()`.

### Minimized Reproducer

```
[11, 5, 0, 0, 6, 0, 0, 0, 246, 255, 255, 239, 255, 255, 255, 255, 10, 0, 0, 0, 0, 0, 0, 0, 255, 1, 7, 44, 0, 0, 0, 0, 0, 0]
```

### Root Cause

Deserialization reads a length prefix from the input and allocates that many bytes without a size cap. The crafted input encodes a large length value (e.g. `0xFFFFFFEF00000006`), causing the allocator to attempt a multi-gigabyte allocation.

### Suggested Fix

Add a maximum allocation size check early in `deserialize()` — reject inputs claiming unreasonable sizes before allocating. A reasonable cap would be the remaining length of the input buffer, or a configurable hard limit.

### Reproduction

```bash
just fuzz deserialize 0  # runs indefinitely; or use the artifact directly:
cargo +nightly fuzz run deserialize artifacts/deserialize/minimized-from-aedbe5692e1f5e17e17848747a23679f0a6e9dde
```
