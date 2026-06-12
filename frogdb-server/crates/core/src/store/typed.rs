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
use crate::store::{Store, ValueType};
use crate::types::{
    HashValue, ListValue, SetValue, SortedSetValue, StreamValue, StringValue, Value,
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
    /// Typed mutable access. `Ok(None)` = key absent, `Err` = key holds another
    /// type. Type-checks on the shared handle from [`Store::get`] *before*
    /// calling [`Store::get_mut`], so a wrong-typed value is never cloned.
    fn get_typed_mut<T: ValueType>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut T>, WrongTypeError> {
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
    /// `Ok(None)` = key absent, `Err` = key holds another type.
    fn get_typed<T: ValueType>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<TypedArc<T>>, WrongTypeError> {
        match self.get(key) {
            None => Ok(None),
            Some(arc) if T::from_value(&arc).is_some() => Ok(Some(TypedArc::new(arc))),
            Some(_) => Err(WrongTypeError),
        }
    }

    /// Type check only, for up-front destination checks (RPOPLPUSH, SMOVE,
    /// COPY). `Ok(())` for an absent key (no type to conflict with) or a
    /// matching key; `Err` for a wrong-typed key.
    fn check_typed<T: ValueType>(&mut self, key: &[u8]) -> Result<(), WrongTypeError> {
        match self.get(key) {
            None => Ok(()),
            Some(v) if T::from_value(&v).is_some() => Ok(()),
            Some(_) => Err(WrongTypeError),
        }
    }

    /// Create-if-missing typed access. Absorbs the previously triplicated
    /// `get_or_create` helpers. Only calls [`Store::set`] when the key is
    /// absent, so an existing key's TTL is untouched.
    fn get_or_create_typed<T: ValueType>(&mut self, key: &Bytes) -> Result<&mut T, WrongTypeError> {
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
            $check:ident,
            $get_or_create:ident
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

                #[doc = concat!("Get-or-create a ", stringify!($ty), " at `key`.")]
                fn $get_or_create(&mut self, key: &Bytes) -> Result<&mut $ty, WrongTypeError> {
                    self.get_or_create_typed::<$ty>(key)
                }
            )*
        }

        impl<S: StoreTypedExt + ?Sized> StoreTypedFamilyExt for S {}
    };
}

typed_family_accessors! {
    ListValue { get_list, get_list_mut, check_list, get_or_create_list };
    HashValue { get_hash, get_hash_mut, check_hash, get_or_create_hash };
    SetValue { get_set, get_set_mut, check_set, get_or_create_set };
    SortedSetValue { get_zset, get_zset_mut, check_zset, get_or_create_zset };
    StringValue { get_string, get_string_mut, check_string, get_or_create_string };
    StreamValue { get_stream, get_stream_mut, check_stream, get_or_create_stream };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::HashMapStore;

    /// Generic WrongType matrix, instantiated per family. Verifies:
    /// absent → `Ok(None)`; right type → `Ok(Some)`; wrong type →
    /// `Err(WrongTypeError)`; create-if-missing creates the default; create on
    /// wrong type errors without mutating.
    fn typed_matrix<T: ValueType>(seed_wrong: Value) {
        let key = Bytes::from_static(b"k");
        let wrong = Bytes::from_static(b"wrong");

        // Absent key.
        let mut store = HashMapStore::new();
        assert!(matches!(store.get_typed_mut::<T>(&key), Ok(None)));
        assert!(matches!(store.get_typed::<T>(&key), Ok(None)));
        assert!(store.check_typed::<T>(&key).is_ok());

        // Right type present.
        store.set(key.clone(), T::create_default());
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
