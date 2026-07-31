//! Integration-test entry point (single binary, `autotests = false`) for the
//! shard-driver scenario suite. The reusable harness these scenarios drive
//! lives in this crate's `src/` (`frogdb_shard_harness::{harness, sink,
//! generator, notify_capture}`); this binary only declares the test modules.

mod shard_driver;

// Scenario submodules (one per targeted scenario; S7 is turmoil-level, server crate).
mod scenario_s1;
mod scenario_s2;
mod scenario_s3;
mod scenario_s4;
mod scenario_s5;
mod scenario_s6;
mod scenario_s8;
