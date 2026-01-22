//! Conditional network primitives.
//!
//! This module provides network types that can be swapped between
//! Tokio and Turmoil implementations for network simulation testing.
//!
//! When the `turmoil` feature is enabled during tests, these types come from
//! the Turmoil library, enabling deterministic network simulation with fault injection.
//! Otherwise, they come from Tokio for production use.

// TcpListener
#[cfg(all(feature = "turmoil", test))]
pub use turmoil::net::TcpListener;

#[cfg(not(all(feature = "turmoil", test)))]
pub use tokio::net::TcpListener;

// TcpStream
#[cfg(all(feature = "turmoil", test))]
pub use turmoil::net::TcpStream;

#[cfg(not(all(feature = "turmoil", test)))]
pub use tokio::net::TcpStream;

// spawn (turmoil needs to control task scheduling)
#[cfg(all(feature = "turmoil", test))]
pub use turmoil::spawn;

#[cfg(not(all(feature = "turmoil", test)))]
pub use tokio::spawn;

// JoinHandle
#[cfg(all(feature = "turmoil", test))]
pub use turmoil::JoinHandle;

#[cfg(not(all(feature = "turmoil", test)))]
pub use tokio::task::JoinHandle;
