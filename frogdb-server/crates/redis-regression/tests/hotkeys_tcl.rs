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
//! - `HOTKEYS START - METRICS required` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - METRICS with CPU only` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - METRICS with NET only` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - METRICS with both CPU and NET` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: session already started` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: invalid METRICS count` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: METRICS count mismatch` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: METRICS invalid metrics` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: METRICS same parameter` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - with COUNT parameter` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: COUNT out of range` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - with DURATION parameter` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - with SAMPLE parameter` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: SAMPLE ratio invalid` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: SLOTS not allowed in non-cluster mode` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS STOP - basic functionality` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS RESET - basic functionality` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS RESET - Error: session in progress` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - returns nil when not started` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - sample-ratio field` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - nested commands` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - commands inside MULTI/EXEC` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - EVAL inside MULTI/EXEC with nested calls` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - no conditional fields without selected slots` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS detection with biased key access, sample ratio = $sample_ratio` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - RESP3 returns map with flat array values for hotkeys` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots returns full range in non-cluster mode` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - with SLOTS parameter in cluster mode` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: SLOTS count mismatch` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: duplicate slots` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: SLOTS already specified` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: invalid slot - negative value` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: invalid slot - out of range` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: invalid slot - non-integer` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots field with individual slots` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots with unordered input slots are sorted` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots returns node's slot ranges when no SLOTS specified in cluster mode` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - conditional fields with sample_ratio > 1 and selected slots` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - no conditional fields with sample_ratio = 1` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - tracks only keys in selected slots` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS - multiple selected slots` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS START - Error: slot not handled by this node` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
//! - `HOTKEYS GET - selected-slots returns each node's slot ranges in multi-node cluster` — intentional-incompatibility:observability — CLIENT TRACKING HOTKEYS not implemented in FrogDB
