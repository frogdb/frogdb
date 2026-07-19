//! Write-Ahead Log (WAL) implementation using RocksDB.
pub mod config;
mod flush;
mod sink;
#[cfg(test)]
mod tests;
mod writer;
pub use config::{DurabilityMode, WalConfig, WalFailurePolicy, WalLagStats};
pub use flush::spawn_periodic_sync;
pub use sink::WalSink;
pub use writer::RocksWalWriter;
