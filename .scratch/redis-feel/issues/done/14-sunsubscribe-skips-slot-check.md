# SUNSUBSCRIBE skips shard-channel slot routing

Status: done

## Origin

Wave-D1 key-spec truthfulness test (`02e776b3`, exemption list in
`frogdb-server/.../upstream_metadata_tests.rs`): upstream gives SUNSUBSCRIBE the
same NOT_KEY key-spec as SSUBSCRIBE/SPUBLISH (shard channels are slot-routed
through the key-spec machinery even though they are "not a key"), and FrogDB
routes SSUBSCRIBE/SPUBLISH that way too — but our SUNSUBSCRIBE is registered
`KeySpec::None`, so no slot check runs on unsubscribe.

## Why it matters

In cluster mode a client can issue SUNSUBSCRIBE for a shard channel whose slot
this node does not own without getting the MOVED/redirect treatment its
subscribe path got. Practical blast radius is small (unsubscribing from a
channel you could never have subscribed to here), but the asymmetry is
untruthful metadata and a behavioral divergence from Redis.

## Candidate direction

Give SUNSUBSCRIBE the same shard-channel key extraction as SSUBSCRIBE; the
D1 exemption for it then comes off the truthfulness test (test exemptions can
only shrink, so removal is enforced once fixed). Verify redirect behavior in
cluster mode matches Redis (note: SUNSUBSCRIBE with no args unsubscribes all —
the no-arg form has no slot to check; match Redis handling).

## Resolution

SUNSUBSCRIBE now carries `KeySpec::All` — the same NOT_KEY shard-channel spec
SSUBSCRIBE uses — and `unsubscribe_kind` runs the same
`SlotMigrationCoordinator::route()` per channel that `subscribe_kind` runs, so
the two halves of the pair take the same MOVED/ASK/ASKING treatment. The D1
exemption and its `KEY_SPEC_DIVERGENCES` twin are gone.

Pub/sub is cluster-exempt at `DispatchStage::ClusterSlotValidation`
(`ConnectionLevelOp::PubSub`), so the key spec alone does not produce a slot
check — the routing loop in `unsubscribe_kind` is what enforces it; the key
spec is the truthful metadata that matches it.

No-arg form: only channels the client *named* are routed. Redis derives the
redirect from argv (`getNodeByQuery` over the legacy range spec), so a bare
SUNSUBSCRIBE yields zero keys and is always served locally. Routing the
*expanded* subscription set would also strand a client's registrations on a
node that stopped owning their slot after the subscribe.

Forcing test: `test_sunsubscribe_redirect_matches_keyed_path`
(`frogdb-server/crates/server/tests/integration_pubsub.rs`).

Follow-up (not done here, `specs/cluster.md` is LOCKED): the FM-CLUSTER-070
paragraph and the "Shard pub/sub slot routing" Redis-deviations row still claim
FrogDB does not slot-route shard-channel subscribers. That was already stale
before this change and is now doubly so.
