//! Typed access to the store.
//!
//! Commands that touch a typed key (list, hash, set, sorted set, string,
//! stream, ...) must enforce the same three-step protocol: read the key, check
//! its variant, emit `WrongType` if it mismatches, and only then project to the
//! inner value. Re-implementing that protocol at every call site leaks the
//! `Value` enum across the command/store seam and scatters one invariant across
//! dozens of files; the check-then-`unwrap` shape it produces is also a latent
//! panic class.
//!
//! [`StoreTypedExt`] owns that invariant in one place. Commands ask for the
//! typed inner value and get a total answer: present ([`Ok(Some)`]), absent
//! ([`Ok(None)`]), or wrong type ([`Err(WrongTypeError)`]). The
//! [`From<WrongTypeError>`] impl lets command code propagate with `?`.
//!
//! # Copy-on-write ordering
//!
//! [`Store::get_mut`] is copy-on-write: a value shared via `Arc` is cloned
//! before the mutable reference is handed out. The mutable accessors here
//! type-check on the shared handle from [`Store::get`] *before* calling
//! `get_mut`, so a wrong-typed value is never cloned just to discover the
//! mismatch. That ordering lives here, once, instead of being re-derived (often
//! incorrectly) at every call site.

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::CommandError;
use crate::store::{DefaultValueType, Store, ValueType};
use crate::types::{
    HashValue, ListValue, SetValue, SortedSetValue, StreamValue, StringValue, Value,
};
use crate::{
    BloomFilterValue, CountMinSketchValue, CuckooFilterValue, HyperLogLogValue, TDigestValue,
    TimeSeriesValue, TopKValue, VectorSetValue,
};

/// Key exists but holds a different type than the one requested.
///
/// Lives in the store layer so the typed seam stays free of command-error
/// vocabulary; the [`From`] impl converts it to [`CommandError::WrongType`] so
/// command code can propagate with `?`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrongTypeError;

impl std::fmt::Display for WrongTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WRONGTYPE Operation against a key holding the wrong kind of value")
    }
}

impl std::error::Error for WrongTypeError {}

impl From<WrongTypeError> for CommandError {
    fn from(_: WrongTypeError) -> Self {
        CommandError::WrongType
    }
}

/// Read handle for a value known to hold a `T`.
///
/// Wraps the `Arc<Value>` returned by [`Store::get`] (no copy-on-write) and
/// derefs to `&T`. It is only constructed after a successful type check, so the
/// projection inside [`Deref`] is the single audited site where the invariant
/// is trusted. The wrapped `Arc<Value>` is immutable while held, so that
/// projection cannot fail.
pub struct TypedArc<T: ValueType> {
    arc: Arc<Value>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: ValueType> TypedArc<T> {
    /// Construct from an `Arc` already known to hold a `T`.
    ///
    /// Callers must have checked `T::from_value(&arc).is_some()` first; the
    /// `debug_assert` pins that contract in test builds.
    fn new(arc: Arc<Value>) -> Self {
        debug_assert!(
            T::from_value(&arc).is_some(),
            "TypedArc::new called with a value that is not a {}",
            T::type_name()
        );
        Self {
            arc,
            _marker: PhantomData,
        }
    }
}

impl<T: ValueType> Deref for TypedArc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // Single audited trust site: a `TypedArc` is only constructed after a
        // successful `from_value` check, and `Arc<Value>` is immutable while
        // held, so this projection always succeeds.
        T::from_value(&self.arc).expect("TypedArc holds a checked value")
    }
}

/// Typed access to the store, owning the `WrongType` invariant in one place.
///
/// This is an extension trait rather than methods on [`Store`] because the
/// store is used as `&mut dyn Store` (see `CommandContextCore::store`), and
/// generic methods on the object-safe `Store` trait would break object safety.
/// The blanket impl below makes every method callable directly on `dyn Store`.
///
/// The genuinely polymorphic commands (TYPE, OBJECT, RENAME, DEBUG, ...) keep
/// using the raw [`Store::get`] / [`Store::get_mut`]; those are the only ones
/// that should still see [`Value`].
pub trait StoreTypedExt: Store {
    /// Typed mutable access. `Ok(None)` = key absent (including a key past its
    /// TTL), `Err` = key holds another type. Type-checks on the shared handle
    /// from [`Store::get`] *before* calling [`Store::get_mut`], so a
    /// wrong-typed value is never cloned.
    ///
    /// # Expiry
    ///
    /// Lazy key expiry is honored up front via [`Store::purge_if_expired`]: an
    /// expired key reads as absent (`Ok(None)`). This is required because the
    /// type-check below reads through [`Store::get`], which does *not* check
    /// expiry — without the purge, an expired key of the matching type would
    /// pass the type-check and then vanish under [`Store::get_mut`] (which does
    /// check expiry), spuriously surfacing as [`WrongTypeError`] instead of
    /// absent. `purge_if_expired` does not touch LRU/LFU metadata, so the one
    /// logical access is still counted exactly once (by `get_mut`), matching a
    /// hand-rolled `check-expiry-then-get_mut`.
    fn get_typed_mut<T: ValueType>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut T>, WrongTypeError> {
        if self.purge_if_expired(key) {
            return Ok(None);
        }
        match self.get(key) {
            None => return Ok(None),
            Some(v) if T::from_value(&v).is_none() => return Err(WrongTypeError),
            Some(_) => {}
        }
        // Total even if the impossible happens between the two lookups: no
        // panic path, unlike the `get_mut(key).unwrap().as_*_mut().unwrap()`
        // chains this replaces.
        match self.get_mut(key).map(T::from_value_mut) {
            Some(Some(t)) => Ok(Some(t)),
            _ => Err(WrongTypeError),
        }
    }

    /// Typed read access on the shared `Arc` handle (no copy-on-write).
    /// `Ok(None)` = key absent (including a key past its TTL), `Err` = key
    /// holds another type.
    ///
    /// # Expiry
    ///
    /// Composes the expiry-aware read [`Store::get_with_expiry_check`], so a key
    /// past its TTL reads as absent (`Ok(None)`) and the read observes lazy
    /// expiry exactly as the hand-rolled `get_with_expiry_check → as_X →
    /// WrongType` fallback it replaces did (including the LRU/LFU access touch).
    /// Hash *field*-level TTL is a separate concern: commands that need it still
    /// call [`Store::purge_expired_hash_fields`] before reading (the field purge
    /// mutates the value, which this read-only seam does not do).
    fn get_typed<T: ValueType>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<TypedArc<T>>, WrongTypeError> {
        match self.get_with_expiry_check(key) {
            None => Ok(None),
            Some(arc) if T::from_value(&arc).is_some() => Ok(Some(TypedArc::new(arc))),
            Some(_) => Err(WrongTypeError),
        }
    }

    /// Type check only, for up-front destination checks (RPOPLPUSH, SMOVE,
    /// COPY). `Ok(())` for an absent key (no type to conflict with) or a
    /// matching key; `Err` for a wrong-typed key. A key past its TTL is purged
    /// up front ([`Store::purge_if_expired`]) and reads as absent, so an
    /// expired destination never spuriously conflicts.
    fn check_typed<T: ValueType>(&mut self, key: &[u8]) -> Result<(), WrongTypeError> {
        if self.purge_if_expired(key) {
            return Ok(());
        }
        match self.get(key) {
            None => Ok(()),
            Some(v) if T::from_value(&v).is_some() => Ok(()),
            Some(_) => Err(WrongTypeError),
        }
    }

    /// Create-if-missing typed access. Absorbs the previously triplicated
    /// `get_or_create` helpers. Only calls [`Store::set`] when the key is
    /// absent, so an existing (live) key's TTL is untouched. A key past its TTL
    /// is purged up front ([`Store::purge_if_expired`]) and then treated as
    /// absent — the fresh value is created without the stale TTL, matching
    /// Redis lazy-expire-then-write semantics.
    fn get_or_create_typed<T: DefaultValueType>(
        &mut self,
        key: &Bytes,
    ) -> Result<&mut T, WrongTypeError> {
        self.purge_if_expired(key);
        match self.get(key) {
            Some(v) if T::from_value(&v).is_none() => return Err(WrongTypeError),
            Some(_) => {}
            None => {
                self.set(key.clone(), T::create_default());
            }
        }
        self.get_mut(key)
            .and_then(T::from_value_mut)
            .ok_or(WrongTypeError)
    }
}

impl<S: Store + ?Sized> StoreTypedExt for S {}

/// Generates per-family convenience methods mirroring `impl_value_accessors`
/// for the families commands actually mutate. Each family gets read (`get_*`),
/// mutable (`get_*_mut`), check (`check_*`), and create (`get_or_create_*`)
/// accessors that delegate to the generic methods above.
macro_rules! typed_family_accessors {
    ($(
        $ty:ty {
            $get:ident,
            $get_mut:ident,
            $check:ident
            $(, $get_or_create:ident )?   // omitted for no-default families
        }
    );* $(;)?) => {
        /// Per-family convenience wrappers over [`StoreTypedExt`]'s generic
        /// methods. Blanket-implemented for every [`Store`].
        pub trait StoreTypedFamilyExt: StoreTypedExt {
            $(
                #[doc = concat!("Typed read access to the ", stringify!($ty), " at `key`.")]
                fn $get(&mut self, key: &[u8]) -> Result<Option<TypedArc<$ty>>, WrongTypeError> {
                    self.get_typed::<$ty>(key)
                }

                #[doc = concat!("Typed mutable access to the ", stringify!($ty), " at `key`.")]
                fn $get_mut(&mut self, key: &[u8]) -> Result<Option<&mut $ty>, WrongTypeError> {
                    self.get_typed_mut::<$ty>(key)
                }

                #[doc = concat!("Type-check the value at `key` against ", stringify!($ty), ".")]
                fn $check(&mut self, key: &[u8]) -> Result<(), WrongTypeError> {
                    self.check_typed::<$ty>(key)
                }

                $(
                    // Only emitted when a create ident is supplied; the bound on
                    // `get_or_create_typed` requires `DefaultValueType`, so the
                    // no-default families simply cannot request this slot.
                    #[doc = concat!("Get-or-create a ", stringify!($ty), " at `key`.")]
                    fn $get_or_create(&mut self, key: &Bytes) -> Result<&mut $ty, WrongTypeError> {
                        self.get_or_create_typed::<$ty>(key)
                    }
                )?
            )*
        }

        impl<S: StoreTypedExt + ?Sized> StoreTypedFamilyExt for S {}
    };
}

typed_family_accessors! {
    // --- core (parameterless default; full read/mut/check/create) ---
    ListValue { get_list, get_list_mut, check_list, get_or_create_list };
    HashValue { get_hash, get_hash_mut, check_hash, get_or_create_hash };
    SetValue { get_set, get_set_mut, check_set, get_or_create_set };
    SortedSetValue { get_zset, get_zset_mut, check_zset, get_or_create_zset };
    StringValue { get_string, get_string_mut, check_string, get_or_create_string };
    StreamValue { get_stream, get_stream_mut, check_stream, get_or_create_stream };

    // --- probabilistic / extension (no parameterless default; no create slot) ---
    BloomFilterValue { get_bloom, get_bloom_mut, check_bloom };
    CuckooFilterValue { get_cuckoo, get_cuckoo_mut, check_cuckoo };
    TopKValue { get_topk, get_topk_mut, check_topk };
    TDigestValue { get_tdigest, get_tdigest_mut, check_tdigest };
    CountMinSketchValue { get_cms, get_cms_mut, check_cms };
    HyperLogLogValue { get_hll, get_hll_mut, check_hll };
    TimeSeriesValue { get_timeseries, get_timeseries_mut, check_timeseries };
    VectorSetValue { get_vectorset, get_vectorset_mut, check_vectorset };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::HashMapStore;
    use crate::{VectorDistanceMetric, VectorQuantization};

    /// Generic WrongType matrix base, instantiated per family. Verifies the
    /// projection invariant for *every* [`ValueType`] (including the
    /// no-default probabilistic families): absent → `Ok(None)`; right type →
    /// `Ok(Some)`; wrong type → `Err(WrongTypeError)`.
    ///
    /// `present` is a value of the family under test; `seed_wrong` is a value
    /// of any other family.
    fn typed_matrix_base<T: ValueType>(present: Value, seed_wrong: Value) {
        let key = Bytes::from_static(b"k");
        let wrong = Bytes::from_static(b"wrong");

        // Absent key.
        let mut store = HashMapStore::new();
        assert!(matches!(store.get_typed_mut::<T>(&key), Ok(None)));
        assert!(matches!(store.get_typed::<T>(&key), Ok(None)));
        assert!(store.check_typed::<T>(&key).is_ok());

        // Right type present.
        store.set(key.clone(), present);
        assert!(store.get_typed_mut::<T>(&key).unwrap().is_some());
        assert!(store.get_typed::<T>(&key).unwrap().is_some());
        assert!(store.check_typed::<T>(&key).is_ok());

        // Wrong type present.
        store.set(wrong.clone(), seed_wrong);
        assert!(matches!(
            store.get_typed_mut::<T>(&wrong),
            Err(WrongTypeError)
        ));
        assert!(matches!(store.get_typed::<T>(&wrong), Err(WrongTypeError)));
        assert!(matches!(
            store.check_typed::<T>(&wrong),
            Err(WrongTypeError)
        ));
    }

    /// Extends [`typed_matrix_base`] with the create-if-missing assertions that
    /// only apply to a [`DefaultValueType`]: create on wrong type errors
    /// without mutating; create on absent key creates the default.
    fn typed_matrix<T: DefaultValueType>(seed_wrong: Value) {
        typed_matrix_base::<T>(T::create_default(), seed_wrong.clone());

        let mut store = HashMapStore::new();
        let wrong = Bytes::from_static(b"wrong");
        store.set(wrong.clone(), seed_wrong);
        assert!(
            store.get_or_create_typed::<T>(&wrong).is_err(),
            "create on wrong type must error"
        );

        // get_or_create on absent key creates the default.
        let fresh = Bytes::from_static(b"fresh");
        let created = store.get_or_create_typed::<T>(&fresh);
        assert!(created.is_ok(), "create on absent key must succeed");
        assert!(store.contains(&fresh));
    }

    // Pick a seed value that is never of the family under test. We use a string
    // for non-string families and a list for the string family.
    #[test]
    fn matrix_list() {
        typed_matrix::<ListValue>(Value::string("x"));
    }

    #[test]
    fn matrix_hash() {
        typed_matrix::<HashValue>(Value::string("x"));
    }

    #[test]
    fn matrix_set() {
        typed_matrix::<SetValue>(Value::string("x"));
    }

    #[test]
    fn matrix_zset() {
        typed_matrix::<SortedSetValue>(Value::string("x"));
    }

    #[test]
    fn matrix_stream() {
        typed_matrix::<StreamValue>(Value::string("x"));
    }

    #[test]
    fn matrix_string() {
        typed_matrix::<StringValue>(Value::list());
    }

    // Probabilistic / extension families have no parameterless default, so they
    // exercise the base matrix only (no get_or_create). Seeded with a string as
    // the wrong type.
    #[test]
    fn matrix_bloom() {
        typed_matrix_base::<BloomFilterValue>(Value::bloom_filter(100, 0.01), Value::string("x"));
    }

    #[test]
    fn matrix_cuckoo() {
        typed_matrix_base::<CuckooFilterValue>(
            Value::CuckooFilter(CuckooFilterValue::new(1024)),
            Value::string("x"),
        );
    }

    #[test]
    fn matrix_topk() {
        typed_matrix_base::<TopKValue>(
            Value::TopK(TopKValue::new(8, 8, 7, 0.9)),
            Value::string("x"),
        );
    }

    #[test]
    fn matrix_tdigest() {
        typed_matrix_base::<TDigestValue>(
            Value::TDigest(TDigestValue::new(100.0)),
            Value::string("x"),
        );
    }

    #[test]
    fn matrix_cms() {
        typed_matrix_base::<CountMinSketchValue>(
            Value::CountMinSketch(CountMinSketchValue::new(8, 4)),
            Value::string("x"),
        );
    }

    #[test]
    fn matrix_hll() {
        typed_matrix_base::<HyperLogLogValue>(Value::hyperloglog(), Value::string("x"));
    }

    #[test]
    fn matrix_timeseries() {
        typed_matrix_base::<TimeSeriesValue>(Value::timeseries(), Value::string("x"));
    }

    #[test]
    fn matrix_vectorset() {
        let vs = VectorSetValue::new(
            VectorDistanceMetric::L2,
            VectorQuantization::NoQuant,
            4,
            16,
            200,
        )
        .expect("valid vectorset params");
        typed_matrix_base::<VectorSetValue>(Value::VectorSet(Box::new(vs)), Value::string("x"));
    }

    /// Wrong-typed mutable access must not copy-on-write the value: assert the
    /// `Arc` is still uniquely held (refcount 1) after a failed
    /// `get_typed_mut`. If the accessor had called `get_mut` before the type
    /// check, the COW clone would have replaced the stored `Arc`, and the
    /// handle taken before the call would no longer be shared with the store.
    #[test]
    fn wrong_typed_access_does_not_cow() {
        let key = Bytes::from_static(b"big");
        let mut store = HashMapStore::new();
        store.set(key.clone(), Value::list());

        // Take a shared handle, then attempt a wrong-typed mutable access.
        let before = store.get(&key).unwrap();
        assert_eq!(Arc::strong_count(&before), 2, "store + local handle");

        assert!(matches!(
            store.get_typed_mut::<HashValue>(&key),
            Err(WrongTypeError)
        ));

        // A COW clone on wrong type would have swapped the store's Arc, leaving
        // `before` as the sole owner of the old allocation (count 1). Because
        // the accessor checks before `get_mut`, no clone happened and the store
        // still shares our handle.
        let after = store.get(&key).unwrap();
        assert!(
            Arc::ptr_eq(&before, &after),
            "wrong-typed access must not copy-on-write the value"
        );
    }

    /// A key past its TTL must read as absent through the seam — proving the
    /// typed read composes the expiry-aware path and no command needs to
    /// hand-roll a `get_with_expiry_check` before projecting the type.
    ///
    /// This is the whole point of P6: `get_typed`/`get_typed_mut` (and the
    /// check/create variants) honor TTL themselves, so a command reads a stale
    /// key as `Ok(None)` rather than seeing the pre-expiry value or a spurious
    /// `WrongTypeError`.
    #[test]
    fn typed_read_honors_ttl() {
        use std::time::{Duration, Instant};

        let past = Instant::now()
            .checked_sub(Duration::from_secs(60))
            .expect("monotonic clock is well past 60s of uptime");

        // Helper: seed a hash key that is already past its deadline.
        let seed_expired = || {
            let key = Bytes::from_static(b"k");
            let mut store = HashMapStore::new();
            store.set(key.clone(), Value::hash());
            assert!(store.set_expiry(&key, past), "expiry set on a live key");
            (store, key)
        };

        // get_typed reads an expired key as absent (not the stale hash).
        let (mut store, key) = seed_expired();
        assert!(
            matches!(store.get_typed::<HashValue>(&key), Ok(None)),
            "expired key must read as absent through get_typed"
        );

        // get_typed_mut reads an expired key as absent, and crucially does NOT
        // surface WrongType (the pre-fix bug: the up-front type-check saw the
        // stale hash, then get_mut purged it, yielding a spurious WrongType).
        let (mut store, key) = seed_expired();
        assert!(
            matches!(store.get_typed_mut::<HashValue>(&key), Ok(None)),
            "expired key must read as absent through get_typed_mut"
        );

        // A wrong-typed *request* against an expired key is also just absent —
        // the key is gone, so there is no type to conflict with.
        let (mut store, key) = seed_expired();
        assert!(
            matches!(store.get_typed::<ListValue>(&key), Ok(None)),
            "expired key is absent even for a mismatched type request"
        );

        // check_typed treats an expired key as absent (no conflict).
        let (mut store, key) = seed_expired();
        assert!(
            store.check_typed::<ListValue>(&key).is_ok(),
            "expired key must not conflict in check_typed"
        );

        // get_or_create_typed on an expired key creates a fresh value of the
        // requested type without the stale TTL.
        let (mut store, key) = seed_expired();
        assert!(
            store.get_or_create_typed::<ListValue>(&key).is_ok(),
            "get_or_create on an expired key creates fresh, ignoring stale type/TTL"
        );
        assert_eq!(
            store.get_expiry(&key),
            None,
            "recreated key must not inherit the expired TTL"
        );
    }

    /// The seam still enforces WrongType on a *live* mismatched key without any
    /// hand-rolled expiry check at the call site — TTL-awareness must not
    /// weaken the type invariant.
    #[test]
    fn typed_read_wrongtype_without_manual_expiry_check() {
        let key = Bytes::from_static(b"k");
        let mut store = HashMapStore::new();
        store.set(key.clone(), Value::string("x"));

        assert!(matches!(
            store.get_typed::<HashValue>(&key),
            Err(WrongTypeError)
        ));
        assert!(matches!(
            store.get_typed_mut::<HashValue>(&key),
            Err(WrongTypeError)
        ));
    }

    /// `get_or_create_typed` on an existing key must not touch expiry: it never
    /// calls `set` for a present key.
    #[test]
    fn get_or_create_preserves_existing_ttl() {
        use std::time::{Duration, Instant};

        let key = Bytes::from_static(b"k");
        let mut store = HashMapStore::new();
        store.set(key.clone(), Value::list());
        let deadline = Instant::now() + Duration::from_secs(1000);
        store.set_expiry(&key, deadline);

        let _ = store.get_or_create_typed::<ListValue>(&key).unwrap();

        assert_eq!(
            store.get_expiry(&key),
            Some(deadline),
            "get_or_create on existing key must preserve TTL"
        );
    }
}
