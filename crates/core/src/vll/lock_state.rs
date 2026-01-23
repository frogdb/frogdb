//! Per-key lock state using lock-free atomic operations.
//!
//! Lock state uses an 8-bit counter:
//! - 0: Unlocked
//! - 1-254: Read locked (reader count)
//! - 255: Write locked (exclusive)

use std::sync::atomic::{AtomicU8, Ordering};

use super::LockMode;

/// Maximum number of concurrent readers.
pub const MAX_READERS: u8 = 254;

/// Value indicating exclusive (write) lock.
pub const WRITE_LOCKED: u8 = 255;

/// Per-key lock state with atomic operations.
#[derive(Debug)]
pub struct KeyLockState {
    /// Lock state: 0=unlocked, 1-254=reader count, 255=exclusive.
    state: AtomicU8,
}

impl Default for KeyLockState {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyLockState {
    /// Create a new unlocked state.
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(0),
        }
    }

    /// Try to acquire a read lock.
    ///
    /// Returns true if the lock was acquired, false if blocked by a writer.
    pub fn try_read_lock(&self) -> bool {
        loop {
            let current = self.state.load(Ordering::Acquire);

            // Can't acquire read lock if write locked
            if current == WRITE_LOCKED {
                return false;
            }

            // Can't exceed max readers
            if current >= MAX_READERS {
                return false;
            }

            // Try to increment reader count
            match self.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // Retry on contention
            }
        }
    }

    /// Try to acquire a write lock.
    ///
    /// Returns true if the lock was acquired, false if blocked.
    pub fn try_write_lock(&self) -> bool {
        self.state
            .compare_exchange(0, WRITE_LOCKED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Release a read lock.
    ///
    /// # Panics
    ///
    /// Panics if not read-locked (in debug mode).
    pub fn read_unlock(&self) {
        let prev = self.state.fetch_sub(1, Ordering::Release);
        debug_assert!(
            prev > 0 && prev != WRITE_LOCKED,
            "read_unlock called on non-read-locked state"
        );
    }

    /// Release a write lock.
    ///
    /// # Panics
    ///
    /// Panics if not write-locked (in debug mode).
    pub fn write_unlock(&self) {
        let prev = self.state.swap(0, Ordering::Release);
        debug_assert_eq!(prev, WRITE_LOCKED, "write_unlock called on non-write-locked state");
    }

    /// Check if the lock is currently held (read or write).
    pub fn is_locked(&self) -> bool {
        self.state.load(Ordering::Acquire) != 0
    }

    /// Check if write-locked.
    pub fn is_write_locked(&self) -> bool {
        self.state.load(Ordering::Acquire) == WRITE_LOCKED
    }

    /// Check if read-locked (one or more readers).
    pub fn is_read_locked(&self) -> bool {
        let s = self.state.load(Ordering::Acquire);
        s > 0 && s != WRITE_LOCKED
    }

    /// Get the current reader count (0 if write-locked or unlocked).
    pub fn reader_count(&self) -> u8 {
        let s = self.state.load(Ordering::Acquire);
        if s == WRITE_LOCKED {
            0
        } else {
            s
        }
    }

    /// Try to acquire a lock with the specified mode.
    pub fn try_lock(&self, mode: LockMode) -> bool {
        match mode {
            LockMode::Read => self.try_read_lock(),
            LockMode::Write => self.try_write_lock(),
        }
    }

    /// Release a lock with the specified mode.
    pub fn unlock(&self, mode: LockMode) {
        match mode {
            LockMode::Read => self.read_unlock(),
            LockMode::Write => self.write_unlock(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_unlocked() {
        let lock = KeyLockState::new();
        assert!(!lock.is_locked());
        assert!(!lock.is_read_locked());
        assert!(!lock.is_write_locked());
        assert_eq!(lock.reader_count(), 0);
    }

    #[test]
    fn test_read_lock() {
        let lock = KeyLockState::new();

        assert!(lock.try_read_lock());
        assert!(lock.is_locked());
        assert!(lock.is_read_locked());
        assert!(!lock.is_write_locked());
        assert_eq!(lock.reader_count(), 1);

        lock.read_unlock();
        assert!(!lock.is_locked());
    }

    #[test]
    fn test_multiple_readers() {
        let lock = KeyLockState::new();

        assert!(lock.try_read_lock());
        assert!(lock.try_read_lock());
        assert!(lock.try_read_lock());
        assert_eq!(lock.reader_count(), 3);

        lock.read_unlock();
        assert_eq!(lock.reader_count(), 2);

        lock.read_unlock();
        lock.read_unlock();
        assert!(!lock.is_locked());
    }

    #[test]
    fn test_write_lock() {
        let lock = KeyLockState::new();

        assert!(lock.try_write_lock());
        assert!(lock.is_locked());
        assert!(!lock.is_read_locked());
        assert!(lock.is_write_locked());
        assert_eq!(lock.reader_count(), 0);

        lock.write_unlock();
        assert!(!lock.is_locked());
    }

    #[test]
    fn test_write_blocks_read() {
        let lock = KeyLockState::new();

        assert!(lock.try_write_lock());
        assert!(!lock.try_read_lock());

        lock.write_unlock();
        assert!(lock.try_read_lock());
    }

    #[test]
    fn test_write_blocks_write() {
        let lock = KeyLockState::new();

        assert!(lock.try_write_lock());
        assert!(!lock.try_write_lock());

        lock.write_unlock();
        assert!(lock.try_write_lock());
    }

    #[test]
    fn test_read_blocks_write() {
        let lock = KeyLockState::new();

        assert!(lock.try_read_lock());
        assert!(!lock.try_write_lock());

        lock.read_unlock();
        assert!(lock.try_write_lock());
    }

    #[test]
    fn test_try_lock_mode() {
        let lock = KeyLockState::new();

        assert!(lock.try_lock(LockMode::Read));
        lock.unlock(LockMode::Read);

        assert!(lock.try_lock(LockMode::Write));
        lock.unlock(LockMode::Write);
    }
}
