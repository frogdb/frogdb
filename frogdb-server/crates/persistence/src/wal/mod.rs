//! Write-Ahead Log (WAL) implementation using RocksDB.
pub mod config;
mod fake;
mod flush;
mod sink;
#[cfg(test)]
mod tests;
mod writer;
pub use config::{DurabilityMode, WalConfig, WalFailurePolicy, WalLagStats};
pub use fake::{FakeFailure, FakeWalLog, FakeWalSink, RecordedWalEffect, WalEffectKind};
pub use flush::spawn_periodic_sync;
pub use sink::WalSink;
pub use writer::RocksWalWriter;
