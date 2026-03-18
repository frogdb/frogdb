//! Write-Ahead Log (WAL) implementation using RocksDB.
pub mod config;
mod flush;
#[cfg(test)]
mod tests;
mod writer;
pub use config::{DurabilityMode, WalConfig, WalFailurePolicy, WalLagStats};
pub use flush::spawn_periodic_sync;
pub use writer::RocksWalWriter;
