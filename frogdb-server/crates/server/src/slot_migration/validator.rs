//! The single owner of the same-locality rule ("do these keys share a home?").
//!
//! Two notions, one rule applied to two homes:
//! - [`SlotValidator::same_slot`] — CRC16 hash slot (cluster mode; the strict
//!   notion Redis enforces).
//! - [`SlotValidator::same_shard`] — internal shard (standalone multi-shard
//!   routing, scripts, transactions), i.e. `slot % num_shards`.
//!
//! Because `shard == slot % num_shards`, **same_slot implies same_shard** — the
//! cluster check is strictly stronger. The method name (not a traced-through
//! helper call) documents which guarantee a site enforces.
//!
//! The CROSSSLOT wire string is produced only by [`redirect::crossslot`]; this
//! type never inlines it.

use frogdb_core::{shard_for_key, slot_for_key};
use frogdb_protocol::Response;

use crate::slot_migration::redirect;

/// Answers "do all these keys map to one home?" for both the internal-shard and
/// the cluster-slot notions. See the [module docs](self) for the invariant.
pub(crate) struct SlotValidator;

impl SlotValidator {
    /// All keys must map to one internal shard. An empty key set is `Ok(None)`
    /// (no keys ⇒ no check); a single key trivially co-locates. On success
    /// returns the resolved shard the caller would otherwise recompute; on a
    /// mismatch returns the CROSSSLOT reply from the redirect seam.
    #[allow(clippy::result_large_err)]
    pub(crate) fn same_shard<K: AsRef<[u8]>>(
        keys: &[K],
        num_shards: usize,
    ) -> Result<Option<usize>, Response> {
        let mut it = keys.iter().map(|k| shard_for_key(k.as_ref(), num_shards));
        let Some(first) = it.next() else {
            return Ok(None);
        };
        if it.all(|s| s == first) {
            Ok(Some(first))
        } else {
            Err(redirect::crossslot())
        }
    }

    /// Cluster mode: all keys must map to one CRC16 hash slot. Strictly stronger
    /// than [`same_shard`](Self::same_shard). Empty ⇒ `Ok(None)`; on a mismatch
    /// returns the CROSSSLOT reply from the redirect seam.
    #[allow(clippy::result_large_err)]
    pub(crate) fn same_slot<K: AsRef<[u8]>>(keys: &[K]) -> Result<Option<u16>, Response> {
        let mut it = keys.iter().map(|k| slot_for_key(k.as_ref()));
        let Some(first) = it.next() else {
            return Ok(None);
        };
        if it.all(|s| s == first) {
            Ok(Some(first))
        } else {
            Err(redirect::crossslot())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn err_text(resp: &Response) -> String {
        match resp {
            Response::Error(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("expected error response, got {other:?}"),
        }
    }

    #[test]
    fn same_shard_empty_is_none() {
        let keys: [Bytes; 0] = [];
        assert_eq!(SlotValidator::same_shard(&keys, 4), Ok(None));
    }

    #[test]
    fn same_slot_empty_is_none() {
        let keys: [Bytes; 0] = [];
        assert_eq!(SlotValidator::same_slot(&keys), Ok(None));
    }

    #[test]
    fn same_shard_single_key_co_locates() {
        let keys = [Bytes::from_static(b"only")];
        assert_eq!(
            SlotValidator::same_shard(&keys, 4),
            Ok(Some(shard(b"only", 4)))
        );
    }

    #[test]
    fn same_slot_single_key_co_locates() {
        let keys = [Bytes::from_static(b"only")];
        assert_eq!(SlotValidator::same_slot(&keys), Ok(Some(slot(b"only"))));
    }

    #[test]
    fn hash_tagged_keys_co_locate() {
        // Keys sharing a `{tag}` hash to one slot (and therefore one shard).
        let keys = [
            Bytes::from_static(b"{user:1}:profile"),
            Bytes::from_static(b"{user:1}:settings"),
            Bytes::from_static(b"{user:1}:sessions"),
        ];
        assert!(SlotValidator::same_slot(&keys).is_ok());
        assert!(SlotValidator::same_shard(&keys, 4).is_ok());
    }

    #[test]
    fn cross_slot_pair_rejects_with_seam_string() {
        // "a" and "b" hash to different CRC16 slots.
        let keys = [Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        let err = SlotValidator::same_slot(&keys).unwrap_err();
        assert_eq!(err_text(&err), err_text(&redirect::crossslot()));
    }

    #[test]
    fn accepts_borrowed_slice_keys() {
        // Routing passes `&[&[u8]]` (handler.keys); scripts pass `&[Bytes]`. Both
        // must satisfy the one generic primitive.
        let a: &[u8] = b"{t}:1";
        let b: &[u8] = b"{t}:2";
        let keys = [a, b];
        assert!(SlotValidator::same_shard(&keys, 8).is_ok());
        assert!(SlotValidator::same_slot(&keys).is_ok());
    }

    #[test]
    fn same_slot_implies_same_shard_over_many_keys() {
        // The whole reason there are two methods: a set that passes the strict
        // slot check must also pass the shard check, for every shard count.
        let singles: Vec<Bytes> = (0u32..200)
            .map(|i| Bytes::from(format!("key:{i}")))
            .collect();
        for num_shards in [1usize, 2, 3, 4, 7, 16] {
            for pair in singles.windows(2) {
                let same_slot = SlotValidator::same_slot(pair).is_ok();
                let same_shard = SlotValidator::same_shard(pair, num_shards).is_ok();
                if same_slot {
                    assert!(
                        same_shard,
                        "same_slot must imply same_shard (num_shards={num_shards})"
                    );
                }
            }
        }
    }

    fn shard(key: &[u8], n: usize) -> usize {
        frogdb_core::shard_for_key(key, n)
    }
    fn slot(key: &[u8]) -> u16 {
        frogdb_core::slot_for_key(key)
    }
}
