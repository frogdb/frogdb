//! The monotonic clock the expiry domain reads.
//!
//! Every deadline the store holds (`KeyMetadata::expires_at`, the hash-field
//! expiry index, `last_access`) is a `std::time::Instant`, and every comparison
//! against those deadlines has to be made against a *now* from the same clock.
//! Reading `std::time::Instant::now()` directly is that same clock — until the
//! process runs under a paused tokio runtime, which is how the turmoil
//! simulation runs every server. There, time advances by fast-forwarding the
//! timer whenever all tasks are idle, so a TTL that is 30 seconds away on the
//! timer's clock is still 30 seconds away on the OS clock a microsecond later:
//! the two clocks disagree about how much time has passed, and which of them a
//! given site happened to read decided whether a key was expired.
//!
//! So the expiry domain reads *here* instead. In a normal build this is
//! `std::time::Instant::now()` with one extra call frame — tokio's `Instant` is
//! a newtype over it and `now()` compiles to the same syscall when the
//! `test-util` feature is off. Under a paused runtime it is the timer's clock,
//! so the whole domain moves together and TTL behaviour under simulation means
//! what the code says it means.
//!
//! The returned value is still a `std::time::Instant` because that is the type
//! the store, the WAL trait, and the persistence formats already speak. It is
//! only meaningful relative to other readings from this function: mixing it
//! with a raw `std::time::Instant::now()` under a paused clock is exactly the
//! bug this module exists to prevent.

/// Now, on the clock the expiry domain shares.
pub fn now() -> std::time::Instant {
    tokio::time::Instant::now().into_std()
}
