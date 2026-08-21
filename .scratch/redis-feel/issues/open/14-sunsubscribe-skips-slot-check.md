# SUNSUBSCRIBE skips shard-channel slot routing

Status: needs-triage

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
