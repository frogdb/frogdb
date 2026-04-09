//! Rust port tracker for Redis 8.6.0 `unit/hotkeys.tcl`.
//!
//! The upstream suite exercises Redis 8.6's `HOTKEYS START / STOP / RESET
//! / GET` command family plus per-sample/per-slot tracking and cluster
//! integration. FrogDB does not implement any of these commands: there
//! is no `HOTKEYS` subcommand namespace, no hotkey-detection sampler in
//! the client tracker, and no `total-cpu-time-us` / `by-net-bytes` INFO
//! fields. `CLIENT TRACKING` itself is implemented in FrogDB but only
//! for invalidation-based key tracking (the RESP3 client-side cache),
//! not hotkey statistics.
//!
//! This file exists so the audit (`audit_tcl.py`) knows the upstream
//! file is being tracked and can classify each of its 43 tests as
//! documented exclusions rather than unexplained gaps. When/if FrogDB
//! grows a HOTKEYS feature, tests should move out of this exclusion
//! list and into `#[tokio::test]` functions in this file.
//!
//! ## Intentional exclusions
//!
//! `HOTKEYS START / STOP / RESET / GET` command family (not implemented
//! in FrogDB):
//! - `HOTKEYS START - METRICS required` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - METRICS with CPU only` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - METRICS with NET only` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - METRICS with both CPU and NET` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: session already started` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: invalid METRICS count` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: METRICS count mismatch` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: METRICS invalid metrics` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: METRICS same parameter` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - with COUNT parameter` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: COUNT out of range` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - with DURATION parameter` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - with SAMPLE parameter` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: SAMPLE ratio invalid` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: SLOTS not allowed in non-cluster mode` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS STOP - basic functionality` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS RESET - basic functionality` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS RESET - Error: session in progress` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - returns nil when not started` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - sample-ratio field` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - nested commands` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - commands inside MULTI/EXEC` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - EVAL inside MULTI/EXEC with nested calls` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - no conditional fields without selected slots` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS detection with biased key access, sample ratio = $sample_ratio` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - RESP3 returns map with flat array values for hotkeys` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots returns full range in non-cluster mode` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - with SLOTS parameter in cluster mode` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: SLOTS count mismatch` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: duplicate slots` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: SLOTS already specified` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: invalid slot - negative value` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: invalid slot - out of range` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: invalid slot - non-integer` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots field with individual slots` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots with unordered input slots are sorted` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots returns node's slot ranges when no SLOTS specified in cluster mode` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - conditional fields with sample_ratio > 1 and selected slots` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - no conditional fields with sample_ratio = 1` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - tracks only keys in selected slots` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - multiple selected slots` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: slot not handled by this node` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots returns each node's slot ranges in multi-node cluster` — CLIENT TRACKING HOTKEYS not implemented in FrogDB
