//! Conditional synchronization primitives.
//!
//! This module provides synchronization primitives that can be swapped between
//! standard library and Shuttle implementations for concurrency testing.
//!
//! When the `shuttle` feature is enabled during tests, these types come from
//! the Shuttle library, enabling deterministic concurrency testing.
//! Otherwise, they come from the standard library.

// Atomic types
#[cfg(all(feature = "shuttle", test))]
pub use shuttle::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[cfg(not(all(feature = "shuttle", test)))]
pub use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// Arc
#[cfg(all(feature = "shuttle", test))]
pub use shuttle::sync::Arc;

#[cfg(not(all(feature = "shuttle", test)))]
pub use std::sync::Arc;

// Mutex
#[cfg(all(feature = "shuttle", test))]
pub use shuttle::sync::Mutex;

#[cfg(not(all(feature = "shuttle", test)))]
pub use std::sync::Mutex;

// RwLock
#[cfg(all(feature = "shuttle", test))]
pub use shuttle::sync::RwLock;

#[cfg(not(all(feature = "shuttle", test)))]
pub use std::sync::RwLock;
