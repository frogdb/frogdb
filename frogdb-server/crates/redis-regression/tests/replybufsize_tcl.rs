//! Rust port of Redis 8.6.0 `unit/replybufsize.tcl` test suite.
//!
//! The entire upstream suite depends on `DEBUG REPLYBUFFER` subcommands
//! (`peak-reset-time`, `reply-copy-avoidance`) and the `rbs=` field in
//! `CLIENT LIST` output to observe the reply buffer shrink/grow behavior.
//! FrogDB does not implement either the DEBUG REPLYBUFFER family or the
//! per-client `rbs` introspection field — the reply buffer sizing
//! heuristic is a Redis-internal implementation detail.
//!
//! ## Intentional exclusions
//!
//! - `verify reply buffer limits` — Redis-internal reply buffer sizing
//!   (requires DEBUG REPLYBUFFER and `rbs=` in CLIENT LIST)
