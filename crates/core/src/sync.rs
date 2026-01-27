//! Conditional synchronization primitives and lock safety extensions.
//!
//! This module provides synchronization primitives that can be swapped between
//! standard library and Shuttle implementations for concurrency testing.
//!
//! When the `shuttle` feature is enabled during tests, these types come from
//! the Shuttle library, enabling deterministic concurrency testing.
//! Otherwise, they come from the standard library.
//!
//! ## Lock Safety Extensions
//!
//! This module also provides extension traits for safer lock handling:
//!
//! - `RwLockExt` - Extensions for `RwLock` with contextual panic messages or error returns
//! - `MutexExt` - Extensions for `Mutex` with contextual panic messages or error returns
//!
//! ### Usage
//!
//! ```rust,ignore
//! use frogdb_core::sync::{RwLock, RwLockExt};
//!
//! let lock = RwLock::new(42);
//!
//! // Panic with context on poison (for critical paths)
//! let guard = lock.read_or_panic("MyModule::critical_operation");
//!
//! // Return error on poison (for non-critical paths)
//! let guard = lock.try_read_err()?;
//! ```

use std::fmt;
use std::sync::{LockResult, MutexGuard, RwLockReadGuard, RwLockWriteGuard};

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

// =============================================================================
// Lock Error Types
// =============================================================================

/// Error returned when a lock cannot be acquired.
#[derive(Debug, Clone)]
pub enum LockError {
    /// The lock was poisoned by a panicking thread.
    Poisoned {
        /// Description of what operation was attempted.
        context: String,
    },
    /// The lock could not be acquired (for try_* operations).
    WouldBlock {
        /// Description of what operation was attempted.
        context: String,
    },
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockError::Poisoned { context } => {
                write!(f, "lock poisoned during: {}", context)
            }
            LockError::WouldBlock { context } => {
                write!(f, "lock would block during: {}", context)
            }
        }
    }
}

impl std::error::Error for LockError {}

impl LockError {
    /// Create a poisoned lock error with context.
    pub fn poisoned(context: impl Into<String>) -> Self {
        LockError::Poisoned {
            context: context.into(),
        }
    }

    /// Create a would-block error with context.
    pub fn would_block(context: impl Into<String>) -> Self {
        LockError::WouldBlock {
            context: context.into(),
        }
    }
}

// =============================================================================
// RwLock Extension Trait
// =============================================================================

/// Extension trait for `RwLock` providing safer lock acquisition methods.
///
/// This trait provides two patterns for lock acquisition:
///
/// 1. **Panic with context** (`read_or_panic`, `write_or_panic`) - For critical paths
///    where lock poisoning indicates a bug that should crash the process with a
///    helpful error message.
///
/// 2. **Error returning** (`try_read_err`, `try_write_err`) - For non-critical paths
///    where lock poisoning can be handled gracefully.
pub trait RwLockExt<T> {
    /// Acquire a read lock, panicking with context if poisoned.
    ///
    /// Use this for critical paths where lock poisoning indicates a bug.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned, with a message including the context.
    fn read_or_panic(&self, context: &str) -> RwLockReadGuard<'_, T>;

    /// Acquire a write lock, panicking with context if poisoned.
    ///
    /// Use this for critical paths where lock poisoning indicates a bug.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned, with a message including the context.
    fn write_or_panic(&self, context: &str) -> RwLockWriteGuard<'_, T>;

    /// Try to acquire a read lock, returning an error if poisoned.
    ///
    /// Use this for non-critical paths where lock poisoning can be handled.
    fn try_read_err(&self) -> Result<RwLockReadGuard<'_, T>, LockError>;

    /// Try to acquire a write lock, returning an error if poisoned.
    ///
    /// Use this for non-critical paths where lock poisoning can be handled.
    fn try_write_err(&self) -> Result<RwLockWriteGuard<'_, T>, LockError>;
}

impl<T> RwLockExt<T> for std::sync::RwLock<T> {
    fn read_or_panic(&self, context: &str) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(|e| {
            panic!(
                "RwLock poisoned during read in {}: {}",
                context,
                e
            )
        })
    }

    fn write_or_panic(&self, context: &str) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(|e| {
            panic!(
                "RwLock poisoned during write in {}: {}",
                context,
                e
            )
        })
    }

    fn try_read_err(&self) -> Result<RwLockReadGuard<'_, T>, LockError> {
        self.read().map_err(|_| LockError::poisoned("read"))
    }

    fn try_write_err(&self) -> Result<RwLockWriteGuard<'_, T>, LockError> {
        self.write().map_err(|_| LockError::poisoned("write"))
    }
}

// =============================================================================
// Mutex Extension Trait
// =============================================================================

/// Extension trait for `Mutex` providing safer lock acquisition methods.
///
/// This trait provides two patterns for lock acquisition:
///
/// 1. **Panic with context** (`lock_or_panic`) - For critical paths where lock
///    poisoning indicates a bug that should crash the process with a helpful
///    error message.
///
/// 2. **Error returning** (`try_lock_err`) - For non-critical paths where lock
///    poisoning can be handled gracefully.
pub trait MutexExt<T> {
    /// Acquire the mutex lock, panicking with context if poisoned.
    ///
    /// Use this for critical paths where lock poisoning indicates a bug.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned, with a message including the context.
    fn lock_or_panic(&self, context: &str) -> MutexGuard<'_, T>;

    /// Try to acquire the mutex lock, returning an error if poisoned.
    ///
    /// Use this for non-critical paths where lock poisoning can be handled.
    fn try_lock_err(&self) -> Result<MutexGuard<'_, T>, LockError>;
}

impl<T> MutexExt<T> for std::sync::Mutex<T> {
    fn lock_or_panic(&self, context: &str) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|e| {
            panic!(
                "Mutex poisoned during lock in {}: {}",
                context,
                e
            )
        })
    }

    fn try_lock_err(&self) -> Result<MutexGuard<'_, T>, LockError> {
        self.lock().map_err(|_| LockError::poisoned("lock"))
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Unwrap a lock result with a contextual panic message.
///
/// This is a helper for cases where you have a `LockResult` and want to
/// convert it with context.
pub fn unwrap_lock<T, G>(result: LockResult<G>, context: &str) -> G
where
    G: std::ops::Deref<Target = T>,
{
    result.unwrap_or_else(|e| {
        panic!("Lock poisoned in {}: {}", context, e)
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rwlock_read_or_panic() {
        let lock = std::sync::RwLock::new(42);
        let guard = lock.read_or_panic("test::read");
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_rwlock_write_or_panic() {
        let lock = std::sync::RwLock::new(42);
        {
            let mut guard = lock.write_or_panic("test::write");
            *guard = 100;
        }
        let guard = lock.read_or_panic("test::verify");
        assert_eq!(*guard, 100);
    }

    #[test]
    fn test_rwlock_try_read_err() {
        let lock = std::sync::RwLock::new(42);
        let result = lock.try_read_err();
        assert!(result.is_ok());
        assert_eq!(*result.unwrap(), 42);
    }

    #[test]
    fn test_rwlock_try_write_err() {
        let lock = std::sync::RwLock::new(42);
        let result = lock.try_write_err();
        assert!(result.is_ok());
    }

    #[test]
    fn test_mutex_lock_or_panic() {
        let lock = std::sync::Mutex::new(42);
        let guard = lock.lock_or_panic("test::lock");
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_mutex_try_lock_err() {
        let lock = std::sync::Mutex::new(42);
        let result = lock.try_lock_err();
        assert!(result.is_ok());
        assert_eq!(*result.unwrap(), 42);
    }

    #[test]
    fn test_lock_error_display() {
        let err = LockError::poisoned("test context");
        assert_eq!(err.to_string(), "lock poisoned during: test context");

        let err = LockError::would_block("test op");
        assert_eq!(err.to_string(), "lock would block during: test op");
    }
}
