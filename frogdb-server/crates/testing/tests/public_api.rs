//! Smoke test that the phase-1 oracle API is reachable from the crate root.

use bytes::Bytes;
use frogdb_testing::{
    ConservationViolation, HashModel, History, ListModel, StreamModel, ZSetModel,
    check_exactly_once_delivery, check_fifo_wake_order, check_linearizability,
    check_linearizability_bounded, check_tx_sum_conservation, check_watch_no_false_negative,
    default_keys_of, partition_by_key,
};
use std::collections::HashMap;

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

#[test]
fn oracle_api_is_reachable_from_root() {
    let mut h = History::new();
    let p = h.invoke(1, "rpush", vec![b("k"), b("a")]);
    h.respond(p, Some(b("1")));
    let q = h.invoke(2, "lpop", vec![b("k")]);
    h.respond(q, Some(b("a")));

    // Models reachable and usable via the checker.
    assert!(check_linearizability::<ListModel>(&h).is_linearizable);
    // Bounded checker + inconclusive flag reachable from the root.
    assert!(check_linearizability_bounded::<ListModel>(&h, 1).inconclusive);

    // Partitioning + conservation reachable.
    let _parts = partition_by_key(&h, default_keys_of);
    assert!(check_exactly_once_delivery(&h, &HashMap::new()).is_ok());
    assert!(check_fifo_wake_order(&h).is_ok());
    assert!(check_tx_sum_conservation(&h, &[b("k")], 0).is_ok());
    assert!(check_watch_no_false_negative(&h).is_ok());

    // Remaining models are in scope.
    let _ = (HashModel, ZSetModel, StreamModel);
    let _err_ty: fn() -> Option<ConservationViolation> = || None;
}
