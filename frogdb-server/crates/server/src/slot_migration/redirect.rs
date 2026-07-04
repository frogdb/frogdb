//! Connection-layer view of the redirect seam.
//!
//! The MOVED / ASK / CLUSTERDOWN / CROSSSLOT wire formats are owned by
//! [`frogdb_types::redirect`] (re-exported through `frogdb-core`), the single
//! home that both `frogdb-core`'s shard actor and this crate share so IPv6
//! address rendering is decided once. This module re-exports them so existing
//! `redirect::moved()` / `redirect::crossslot()` call sites are unchanged.

pub use frogdb_core::redirect::*;
