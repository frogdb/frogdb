# Proposal: Protocol Response-Shape Builder

Status: proposed
Date: 2026-06-17

## Problem

RESP2 and RESP3 disagree about the *shape* of three recurring reply kinds, and FrogDB resolves that
disagreement **inline, per handler, by hand** every time it builds one:

| Reply kind | RESP2 shape | RESP3 shape | The rule a handler must re-derive each time |
|---|---|---|---|
| key/value map | flat `Array` (`k,v,k,v,…`) | `Map([(k,v),…])` | "RESP3 → `Map`; RESP2 → flatten each pair into the array" |
| pub/sub confirmation | `Array` | `Push` | "RESP3 → `Push`; RESP2 → `Array`" |
| array-null | raw `*-1\r\n` | `_\r\n` (`Null`) | "RESP2 → write the literal `*-1\r\n`; RESP3 → encode `Null`" |

None of those rules lives in one place. Each is re-stated, in long-hand, at every site that emits the
reply — and the two arms of the `if is_resp3()` are written as *independent* literal builders that
just happen to agree today. The interface a handler must hold in its head ("which fields, in which
order, conditionally included, then flatten-or-map depending on protocol") is exactly as large as the
implementation it ought to be hiding. That is the shallow-seam signature: the protocol divergence is
**data** (a shape rule + a field list), but it is encoded as **control flow** copy-pasted across
handlers and, in the pub/sub case, smeared across three layers.

The cost is drift. A `Map` reply built as two parallel arms is one careless edit from a silent
shape bug: add a field to the RESP3 arm and forget the RESP2 arm and the protocols diverge with no
compiler complaint and no test unless one exists for *both* versions of *that* field. The pub/sub
confirmation is worse — its Array→Push rule is applied in the transaction path but **not** in the
normal path, so the same confirmation already has two different wire shapes depending on whether it
rode inside `MULTI`/`EXEC` (see Correctness flags).

There is also a telling near-miss: core *already* owns the pub/sub Array-vs-Push rule in one place —
`PubSubMessage::to_response_with_protocol` (`core/src/pubsub.rs:77-154`) — and the connection loop
uses it for live `message`/`pmessage` delivery (`connection.rs:520,532`). But the *confirmation*
replies (`subscribe`/`unsubscribe`/…) are hand-built as `Array` in the handlers and patched to `Push`
in one branch only. So the deep module exists; the handlers reach *past* it. That is the seam in the
wrong place, and a parallel mechanism growing next to the right one.

**Deletion test.** If a shape builder were already a deep module, deleting it would delete: the
~75-line dual `Map`/flat-`Array` build in HOTKEYS GET, the dual build in HELLO, the nine inline
`Array` confirmation builds in the pub/sub handlers, the transaction's Array→Push patch, and one of
the two hand-rolled `*-1\r\n` blocks in `frame_io`. Today none of those can be deleted because the
shape rule lives in the callers, not in a module. Removing the builder would force every line of that
duplication back into the callers — which is precisely why it earns its keep.

This proposal owns the **reply-shape contract**: one builder for protocol-shaped maps, one typed
confirmation that reuses the existing `to_response_with_protocol` seam, and one helper for the
array-null wire shape. The *value*-level RESP2/RESP3 divergences in the `commands` crate (HGETALL,
ZADD GT/LT, set ops, …) are the same pattern at larger scale — out of scope here but called out as
the leverage this seam unlocks.

## Current state

### HOTKEYS GET — the same map built twice (`handlers/hotkeys.rs:386-461`)

`handle_hotkeys_get` (`hotkeys.rs:313`) computes one logical reply — `metrics`, `count`, `duration`,
two *conditional* fields, then `hotkeys` — and then writes it out **twice**, once as a `Map` and once
as a flat `Array`, branching on `is_resp3` (set at `hotkeys.rs:320`):

```rust
if is_resp3 {
    // RESP3: Map type
    let mut pairs: Vec<(Response, Response)> = vec![
        (Response::bulk(Bytes::from("metrics")), Response::Array(/* … */)),
        (Response::bulk(Bytes::from("count")),    Response::Integer(count)),
        (Response::bulk(Bytes::from("duration")), Response::Integer(duration_ms)),
    ];
    if sample_ratio > 1 {
        pairs.push((Response::bulk(Bytes::from("sample-ratio")), Response::Integer(sample_ratio)));
    }
    if sample_ratio > 1 || session.config.selected_slots.is_some() {
        pairs.push((Response::bulk(Bytes::from("selected-slots")), Response::Array(selected_slots)));
    }
    pairs.push((Response::bulk(Bytes::from("hotkeys")), Response::Array(hotkeys_entries)));
    Response::Map(pairs)
} else {
    // RESP2: flat array of alternating key/value pairs
    let mut flat: Vec<Response> = vec![
        Response::bulk(Bytes::from("metrics")), Response::Array(/* … */),
        Response::bulk(Bytes::from("count")),    Response::Integer(count),
        Response::bulk(Bytes::from("duration")), Response::Integer(duration_ms),
    ];
    if sample_ratio > 1 {
        flat.push(Response::bulk(Bytes::from("sample-ratio")));
        flat.push(Response::Integer(sample_ratio));
    }
    if sample_ratio > 1 || session.config.selected_slots.is_some() {
        flat.push(Response::bulk(Bytes::from("selected-slots")));
        flat.push(Response::Array(selected_slots));
    }
    flat.push(Response::bulk(Bytes::from("hotkeys")));
    flat.push(Response::Array(hotkeys_entries));
    Response::Array(flat)
}
```

Every field appears twice, and — critically — each **conditional** field's predicate appears twice
(`sample_ratio > 1` at lines 409 and 447; `sample_ratio > 1 || selected_slots.is_some()` at 417 and
452). The two arms are consistent *today*; they are one edit from silently disagreeing, with nothing
in the type system to catch it.

(Sub-note: the per-hotkey entries themselves (`hotkeys.rs:332-359`) are built as flat `Array`
regardless of protocol — the nested level is *not* protocol-shaped even in RESP3. Whether RESP3
HOTKEYS should nest `Map`s per entry is an open question, not asserted as a bug — see open questions.)

### HELLO — the same dual build, again (`handlers/auth.rs:201-258`)

`build_hello_response` constructs seven fixed pairs and then forks on `is_resp3()` (`auth.rs:228`)
into a `Map` arm (`auth.rs:230-238`) and a flat-`Array` arm (`auth.rs:241-256`) listing all fourteen
elements by hand:

```rust
if self.state.protocol_version.is_resp3() {
    Response::Map(vec![
        (server, server_val), (version, version_val), (proto, proto_val),
        (id, id_val), (mode, mode_val), (role, role_val), (modules, modules_val),
    ])
} else {
    Response::Array(vec![
        server, server_val, version, version_val, proto, proto_val,
        id, id_val, mode, mode_val, role, role_val, modules, modules_val,
    ])
}
```

Same shape rule, second copy.

### Pub/sub confirmations — `Array` built nine times, `Push` patched once elsewhere

The pub/sub handlers hand-roll the confirmation as `Response::Array` at nine sites
(`handlers/pubsub.rs`): `subscribe` (77), `unsubscribe` empty-arg (98) and per-channel (122),
`psubscribe` (172), `punsubscribe` empty-arg (193) and per-pattern (217), and the sharded
`ssubscribe` (335) / `sunsubscribe` empty-arg (356) and per-channel (381). Each is the same triple:

```rust
responses.push(Response::Array(vec![
    Response::bulk(Bytes::from_static(b"subscribe")),
    Response::bulk(channel.clone()),
    Response::Integer(count as i64),
]));
```

The Array→Push shape selection then happens **outside** these builders, in the transaction path only
(`handlers/transaction.rs:559-567`):

```rust
if self.state.protocol_version.is_resp3() {
    // Convert confirmations to Push frames for RESP3.
    let push_confirmations: Vec<Response> = responses
        .into_iter()
        .map(|r| match r {
            Response::Array(items) => Response::Push(items),
            other => other,
        })
        .collect();
    // … returned as the EXEC tail
}
```

The **normal** (non-`MULTI`) path never does this: `route_and_execute_with_transaction`
(`dispatch.rs:460`) returns the handler's `Vec<Response>` unchanged via `dispatch_pubsub`
(`dispatch.rs:188-216`), and the connection loop feeds each verbatim
(`connection.rs:486-487`). So the shape rule is applied in one path and skipped in the other — the
divergence is structural, not accidental. (See Correctness flag 1.)

Meanwhile the rule already exists, correctly, in core (`core/src/pubsub.rs:77-154`), keyed by the
same variants the handlers are re-encoding by hand:

```rust
PubSubMessage::Subscribe { channel, count } => vec![ /* b"subscribe", channel, count */ ],
// …
if protocol.is_resp3() { Response::Push(items) } else { Response::Array(items) }
```

### Array-null wire shape — hand-encoded twice (`connection/frame_io.rs`)

`WireResponse::NullArray` cannot be produced by the `redis-protocol` crate (its `Null` is always
`$-1\r\n`), so the `*-1\r\n` array-null is special-cased at the connection layer — in **two**
near-identical blocks, `send_wire_response` (`frame_io.rs:41-67`) and `feed_wire_response`
(`frame_io.rs:111-137`):

```rust
if matches!(response, frogdb_protocol::WireResponse::NullArray) {
    match self.state.protocol_version {
        ProtocolVersion::Resp2 => {
            const NULL_ARRAY_BYTES: &[u8] = b"*-1\r\n";
            self.state.local_stats.add_bytes_sent(NULL_ARRAY_BYTES.len() as u64);
            // send-vs-feed differ only in flush/return here
            self.framed.get_mut().write_all(NULL_ARRAY_BYTES).await? /* … */
        }
        ProtocolVersion::Resp3 => { /* encode Null via resp3 extend_encode … */ }
    }
}
```

The protocol-version branch for "what does an array-null look like on the wire" is duplicated; the
two copies differ only in flush/return mechanics, not in the shape decision.

### How widespread is shape branching?

`is_resp3()` is consulted at ~36 non-test source sites across the workspace, plus four direct
`match … protocol_version` shape branches in `frame_io.rs`. In the server connection layer the
shape-deciding sites are: `hotkeys.rs:320`, `auth.rs:210,228`, `transaction.rs:559`, `dispatch.rs:432`
(pub/sub PING), `guards.rs:34`, and the four `frame_io.rs` branches. The same dual `Map`/flat-`Array`
pattern is replicated throughout the `commands` crate — e.g. HGETALL (`commands/src/hash.rs:363-378`)
is byte-for-byte the HOTKEYS pattern (`Response::Map(pairs)` vs flattened `Response::Array`), and it
recurs in `set.rs`, `sorted_set/*`, and `string.rs`. That is the population this seam can eventually
serve; this proposal lands it in the connection layer first.

## Proposed design

Three small, deep pieces behind the protocol seam. Each replaces a per-site `if is_resp3()` with a
single owner of the rule.

### 1. `MapReply` — build a key/value reply once, shape it once

```rust
/// Accumulates a key/value reply and emits the protocol-correct shape.
/// RESP3 -> Map([(k,v),…]); RESP2 -> flattened Array([k,v,k,v,…]). ONE place.
pub struct MapReply {
    proto: ProtocolVersion,
    pairs: Vec<(Response, Response)>,
}

impl MapReply {
    pub fn new(proto: ProtocolVersion) -> Self {
        Self { proto, pairs: Vec::new() }
    }

    /// Add a field unconditionally.
    pub fn field(&mut self, key: &'static [u8], value: Response) -> &mut Self {
        self.pairs.push((Response::bulk(Bytes::from_static(key)), value));
        self
    }

    /// Add a field only when `cond` holds — the predicate lives ONCE, not once per arm.
    /// `value` is a closure so the (possibly expensive) value is built only when included.
    pub fn field_if(&mut self, cond: bool, key: &'static [u8], value: impl FnOnce() -> Response) -> &mut Self {
        if cond {
            self.pairs.push((Response::bulk(Bytes::from_static(key)), value()));
        }
        self
    }

    /// Emit the protocol-correct shape. The flatten/map rule lives here and nowhere else.
    pub fn finish(self) -> Response {
        if self.proto.is_resp3() {
            Response::Map(self.pairs)
        } else {
            let mut flat = Vec::with_capacity(self.pairs.len() * 2);
            for (k, v) in self.pairs {
                flat.push(k);
                flat.push(v);
            }
            Response::Array(flat)
        }
    }
}
```

#### Before / after: HOTKEYS GET

After — each field stated once, each conditional predicate stated once, shape owned by `finish`:

```rust
let mut m = MapReply::new(self.state.protocol_version);
m.field(b"metrics", Response::Array(metrics_resp));
m.field(b"count", Response::Integer(count));
m.field(b"duration", Response::Integer(duration_ms));
m.field_if(sample_ratio > 1, b"sample-ratio", || Response::Integer(sample_ratio));
m.field_if(
    sample_ratio > 1 || session.config.selected_slots.is_some(),
    b"selected-slots",
    || Response::Array(selected_slots),
);
m.field(b"hotkeys", Response::Array(hotkeys_entries));
m.finish()   // RESP3 -> Map, RESP2 -> flattened Array
```

The ~75-line dual build collapses to ~8 lines; adding a field is one line that *cannot* desync the
two protocols because there is only one builder. HELLO collapses the same way (seven `field` calls +
`finish`).

### 2. `PubSubConfirmation` — reuse the existing core seam, don't add a parallel one

The Array-vs-Push rule already lives on `PubSubMessage::to_response_with_protocol`
(`core/src/pubsub.rs:77-154`), whose variants (`Subscribe`/`Unsubscribe`/`PSubscribe`/`PUnsubscribe`/
`SSubscribe`/`SUnsubscribe`) are exactly the confirmations the handlers hand-roll. The fix is to
**route the handler confirmations through that existing seam** rather than building `Array` literals
and patching them later. Concretely the handlers replace each

```rust
responses.push(Response::Array(vec![ b"subscribe", channel, count ]));
```

with the typed, protocol-aware build:

```rust
responses.push(
    PubSubMessage::Subscribe { channel: channel.clone(), count }
        .to_response_with_protocol(self.state.protocol_version),
);
```

This makes the confirmation `Push` in RESP3 and `Array` in RESP2 **at construction**, in both the
normal and transaction paths — which retires the transaction Array→Push patch
(`transaction.rs:559-567`) entirely (the EXEC path can return the confirmations as-is). The
empty-arg `unsubscribe`/`punsubscribe`/`sunsubscribe` replies (a `null` channel) need a small
addition to the core enum — a confirmation with no channel — or a thin `PubSubConfirmation` wrapper in
core if extending `PubSubMessage` is undesirable; either way the rule stays in **one** module.

(If a distinct `PubSubConfirmation { Subscribe { channel, count }, … }` type is preferred over
widening `PubSubMessage`, it lives beside `PubSubMessage` in `core/src/pubsub.rs` and shares the same
`if proto.is_resp3() { Push } else { Array }` tail — the point is one owner, not which name.)

Note this is also strictly less work than today even on the wire: `WireResponse::Push` already
downgrades to an `Array` frame in RESP2 (`protocol/src/response.rs:252-254`), so the Array/Push
divergence the handlers maintain by hand is bookkeeping the frame layer already performs.

### 3. `NullArray` helper — one owner of the array-null wire shape

Collapse the two `frame_io.rs` blocks into a single helper that both `send_wire_response` and
`feed_wire_response` call before their generic match:

```rust
/// Emit the protocol-correct array-null: RESP2 raw `*-1\r\n`, RESP3 `_\r\n`.
/// The `*-1\r\n` literal and the protocol branch live ONCE.
async fn write_null_array(&mut self) -> std::io::Result<()> {
    match self.state.protocol_version {
        ProtocolVersion::Resp2 => {
            const NULL_ARRAY_BYTES: &[u8] = b"*-1\r\n";
            self.state.local_stats.add_bytes_sent(NULL_ARRAY_BYTES.len() as u64);
            self.framed.get_mut().write_all(NULL_ARRAY_BYTES).await
        }
        ProtocolVersion::Resp3 => {
            let frame = frogdb_protocol::WireResponse::NullArray.to_resp3_frame();
            self.resp3_buf.clear();
            redis_protocol::resp3::encode::complete::extend_encode(&mut self.resp3_buf, &frame, false)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            self.state.local_stats.add_bytes_sent(self.resp3_buf.len() as u64);
            self.framed.get_mut().write_all(&self.resp3_buf).await
        }
    }
}
```

Each caller keeps only its own flush/return policy (`send` flushes; `feed` clears and returns) around
the one shared shape decision.

### Why this is the right depth

- **Locality.** The flatten-or-map rule, the Array-or-Push rule, and the array-null wire shape each
  live in exactly one place. RESP2/RESP3 divergence becomes impossible to introduce per-handler,
  because handlers no longer name the shape — they list fields/variants and let the seam choose.
- **Leverage.** A few dozen lines of builder delete ~75 lines in HOTKEYS, the HELLO dual build, the
  nine pub/sub `Array` literals, the transaction patch, and one of the two `NullArray` blocks — and
  every future map/confirmation reply gets correct shaping for free. The `commands`-crate population
  (HGETALL et al.) can adopt the same `MapReply` later with no new concept.
- **Deletion test.** The migration is a net deletion at call sites and removes whole artifacts (the
  transaction Array→Push patch, the duplicate predicates, one `NullArray` block). If the builder could
  not delete those, its shape would be wrong.
- **Not a new adapter layer.** `MapReply` and the `NullArray` helper are thin shape selectors over the
  existing `Response`/`WireResponse` types; the pub/sub piece *reuses the seam core already owns*
  rather than adding a parallel one. Handlers cannot bypass the rule, because the inline literal
  builds they used to bypass it with are removed.

## Migration plan

Each phase compiles and keeps `just test frogdb-server` green. Behavior-preserving **except** the
pub/sub confirmation shape unification in Phase 2, which fixes the normal-path RESP3 shape
(Correctness flag 1) and is the point of that phase.

1. **Phase 0 — introduce `MapReply`** (in the server connection module, or `protocol` crate if the
   `commands` crate is to share it later) with `new`/`field`/`field_if`/`finish`. Unit-test `finish`
   over both protocol versions and the conditional-field paths. No call sites change.
   `just check frogdb-server`.
2. **Phase 1 — migrate the map dual-builds.** Rewrite HOTKEYS GET (`hotkeys.rs:386-461`) and HELLO
   (`auth.rs:228-257`) onto `MapReply`. Pure shape-equivalence; assert RESP2 flat order and RESP3
   `Map` pairs match the pre-migration output field-for-field. Delete the dead arms.
3. **Phase 2 — unify pub/sub confirmations.** Extend the core seam (a channel-less confirmation
   variant/wrapper for the empty-arg replies) and route all nine handler builds
   (`pubsub.rs:77,98,122,172,193,217,335,356,381`) through
   `…to_response_with_protocol(proto)`. Delete the transaction Array→Push patch
   (`transaction.rs:559-567`); EXEC returns the confirmations as built. After this phase the normal
   and transaction paths emit identical confirmation shapes (`Push` in RESP3, `Array` in RESP2).
4. **Phase 3 — collapse the `NullArray` blocks.** Extract `write_null_array` and call it from both
   `send_wire_response` (`frame_io.rs:41-67`) and `feed_wire_response` (`frame_io.rs:111-137`),
   keeping each caller's flush/return policy.
5. **Gate.** Add a `just lint` grep gate: in the pub/sub handlers, no literal
   `Response::Array(vec![ … b"subscribe" … ])`-style confirmation builds (confirmations must go
   through the seam); and the `*-1\r\n` literal appears only inside `write_null_array`.

## Testing impact

- **Shape rule, tested once over both protocols.** `MapReply::finish` gets a direct unit test for
  RESP2 (flat order) and RESP3 (`Map` pairs), including a `field_if` true/false case — the
  conditional-field logic that today is duplicated per arm becomes a single tested function.
- **HOTKEYS / HELLO equivalence pins.** Snapshot the pre-migration RESP2 and RESP3 outputs for a
  representative session (with and without `sample-ratio`/`selected-slots`) and assert the
  `MapReply` rewrite reproduces them exactly — guarding the field order and the conditional inclusion.
- **Pub/sub confirmation parity (fails today in RESP3 normal path).** Subscribe over RESP3 *outside*
  a transaction and assert the confirmation is a `Push`, not an `Array`; do the same *inside*
  `MULTI`/`EXEC` and assert the two paths now agree. The normal-path RESP3 case fails before Phase 2.
- **Array-null wire bytes.** Assert a `NullArray` reply emits exactly `*-1\r\n` in RESP2 and `_\r\n`
  in RESP3 through *both* `send` and `feed`, so the extracted helper is exercised on both paths.

## Risks / open questions

- **Pub/sub confirmation shape is wire-visible.** Switching the normal-path RESP3 confirmation from
  `Array` to `Push` changes the bytes a RESP3 client sees (`*3` → `>3`). This matches Redis (see flag
  1) and most RESP3 clients route both as the subscribe reply, but it is a behavior change and should
  be noted in release docs. FrogDB is pre-production, so no compatibility gate blocks it.
- **Empty-arg confirmation (`null` channel).** `unsubscribe`/`punsubscribe`/`sunsubscribe` with no
  active subscriptions reply with a `null` channel and count `0` (`pubsub.rs:98,193,356`). The core
  seam currently models confirmations with a concrete channel; this case needs either a dedicated
  variant or an `Option<Bytes>` channel. Small, but it is the reason a blind reuse of
  `PubSubMessage` is not quite a drop-in — flag it so the core change is deliberate.
- **Where does `MapReply` live?** Putting it in the `protocol` crate lets the `commands` crate adopt
  it (HGETALL et al.) and maximizes leverage, but widens the protocol crate's surface. Putting it in
  the server connection module keeps the blast radius small but forecloses `commands`-crate reuse.
  Recommendation: protocol crate, since the `Response` types it shapes already live there.
- **Nested protocol shaping.** HOTKEYS per-entry rows are flat `Array`s even in RESP3
  (`hotkeys.rs:332-359`); a `MapReply` could nest, but whether RESP3 HOTKEYS *should* return per-entry
  `Map`s is a product question, not a refactor. Left as-is; the builder makes the change a one-liner
  if desired. HOTKEYS is FrogDB-specific, so there is no Redis shape to match here.
- **`Attribute`/`Set`/`VerbatimString`.** This proposal covers map, push, and array-null shapes only.
  Other RESP3-only shapes are not yet builder-backed; the seam is extensible but this proposal does
  not claim them.

## Correctness flags

1. **Pub/sub subscribe confirmations differ by path in RESP3 — CONFIRMED (internal inconsistency);
   likely Redis non-conformance.** Inside `MULTI`/`EXEC`, confirmations are upgraded
   `Array` → `Push` for RESP3 (`transaction.rs:559-567`). In the normal (non-transaction) path they
   are built as `Response::Array` (`pubsub.rs:77,98,122,172,193,217,335,356,381`) and fed verbatim
   (`route_and_execute_with_transaction` returns them unchanged at `dispatch.rs:460-493`; the loop
   feeds them at `connection.rs:486-487`), with **no** Array→Push step. So the *same* confirmation is
   a `Push` when it rode inside a transaction and an `Array` otherwise — an inconsistency provable
   from the code alone. Redis sends subscribe/unsubscribe confirmations as RESP3 *push* messages
   (`addReplyPubsubSubscribed`/`Unsubscribed` use `addReplyPushLen`), so the normal-path `Array` is
   the non-conforming side. Confidence: the internal inconsistency is certain; the Redis-conformance
   claim is high-confidence from the Redis reply helpers but worth confirming against a live RESP3
   client, since many clients accept either framing for the subscribe reply. The unified
   `to_response_with_protocol` routing (Phase 2) makes both paths emit `Push` in RESP3 and removes the
   asymmetry.

2. **Map dual-builds are consistent today but unguarded — CONFIRMED (drift hazard, not a live bug).**
   In HOTKEYS GET (`hotkeys.rs:386-461`) every field and every conditional predicate
   (`sample_ratio > 1` at 409/447; `sample_ratio > 1 || selected_slots.is_some()` at 417/452) is
   written twice, once per protocol arm. HELLO (`auth.rs:228-257`) repeats its field list twice. The
   arms agree at present, but nothing prevents an edit to one arm only, and a divergence would be
   silent unless a test covers that exact field in *both* protocols. This is a latent shape bug, not a
   current one; `MapReply` removes the possibility by construction.

3. **Array-null wire shape duplicated — CONFIRMED (code-smell, not a runtime bug).** The `*-1\r\n`
   literal and its protocol branch are written twice (`frame_io.rs:41-67` and `:111-137`), differing
   only in flush/return mechanics. Both are correct today; the duplication means a future change to
   the array-null wire encoding (or its byte-accounting in `local_stats`) must be made in two places
   or silently diverge. The `write_null_array` helper makes it one place.
