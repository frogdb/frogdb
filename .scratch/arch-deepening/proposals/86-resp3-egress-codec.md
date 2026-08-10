# Proposal 86 — RESP3 egress: two protocols, two write paths, and a byte count that is re-derived instead of measured

Round 38 · lane: protocol / net / core · candidates **PN9** (LIVE) + **PN8** (latent) · effort **M**
(the codec fold) + **S–M** (the ingress half, separable) · **no locked crate edited** · **zero `FM-`
tags in any edited file** · **two seam lints in scope, both cleared by construction**

**Verified at HEAD `55de0d326600a6d3d3befa7d931bfdf479de1800`** (worktree `arch-round-38-99`,
branch `main`). Proposal 80 was authored against `54baa2bb`; every commit since has been
`.scratch/`-only — `git diff --name-only 54baa2bb..HEAD | grep -v '^\.scratch/'` is **empty**, so
every source line number in 80, 84 and this document refers to the same unchanged tree. Every cite
below was re-derived against the working tree; none is inherited from the lane brief.

## Corrections to the lane brief

| Brief claim | Verified at HEAD |
|---|---|
| "a comment at `:136` says *accumulate* while `:146` `clear()`s per-iteration — contradiction" | **Confirmed**, and independently: `frame_io.rs:136` vs `:146`. But the *consequence* the brief and proposal 80 infer is wrong. |
| 80's note: "`:143-144` therefore reports a cumulative-looking `encoded_len` that is in fact per-frame" | **The byte accounting is correct today.** Both RESP3 arms leave `resp3_buf` empty (`:91` clears before encoding, `:146` clears after writing), so `:143`'s `encoded_len` *is* the per-frame length and `add_bytes_sent` is exact. The `:136` comment is **vestigial**, not a live mis-count. Recorded so the hotfix is filed as what it is — a doc fix — rather than as a stats bug that does not exist. |
| PN8: "`estimate_resp2_frame_size` … the doc names an upper-crate fn it can't see" | Confirmed (`response.rs:199` → `connection/util.rs:130`, a `pub(crate)` fn in a crate *above* `frogdb-protocol`). But the doc's *argument* is also **moot**: `narrow_to_resp2_outbound` measures the frame returned by `to_resp2_frame`, which has **already** been sanitized (`response.rs:277`, `:292`), so length-preservation contributes nothing to the accounting's exactness. Two defects in one sentence, not one. |
| PN8 solution sketch: "`WireResponse::encoded_len(ProtocolVersion)` next to the encoder + property test `encoded_len == encode().len()`" | **Declined as the primary, with reasons** (§Proposed change, alternative B). `encoded_len` is a *second* implementation of the encoder that a property test then pins against the first. The fold makes the number **measurable** at the one place the bytes are produced, which removes both the duplicate and the need for the test that guards it. The property-test design is still given in §Testability, because it is the right pin if `encoded_len` is ever wanted for a *pre-flight* size check (no such caller exists today). |
| PN8 rated "latent" | **Half of it is LIVE.** `estimate_command_size` (`util.rs:149`) feeds the **byte rate limiter** (`connection.rs:403`, `transaction.rs:160-165`), not just stats, and it over-charges inline (telnet-style) commands by **2.3×** (§Problem 6). |
| PN9 framed as a throughput/syscall problem | It is that — **1 write syscall per reply vs 1 per ≤8 KiB, measured** (§Problem 2) — **and it is also a live reply-ordering defect.** A pipelined `GET k` + `HELLO 3` batch delivers the HELLO reply *ahead of* the GET reply, because the RESP3 arm writes straight to the socket while the RESP2 reply is still sitting in the sink's write buffer (§Problem 1). This is the same defect class proposal 49 closed for the RESP2 null-array, still open across the protocol switch. |
| 84's §"vs 66/67": "67's subject is `util.rs`'s other content (`estimate_resp2_frame_size` `:130`, `estimate_command_size` `:149` — candidate PN8 …)" | **Mis-attributed.** Proposal 67 contains **zero** references to `connection/util.rs` (`grep -n 'util.rs\|estimate_' 67-server-small-dedups.md` → no output). PN8 is **this** proposal. 84's conclusion (adjacent regions, either order) is right; the owner name is wrong. |

Three findings the brief did not name, all verified at HEAD: the RESP2→RESP3 **reply reordering**
(§Problem 1), `obl` in `CLIENT LIST`/`CLIENT INFO` being **structurally zero** and feeding client
eviction (§Problem 4), and **two `Encoder` impls with zero call sites** whose only effect is to
force a turbofish (§Problem 7).

## Summary

`frogdb-server` has **two** reply-egress mechanisms for one socket.

RESP2 goes through the `tokio_util::codec::Framed` sink: `feed` encodes into the sink's `BytesMut`
and returns; one `flush_responses()` at the end of a pipelined batch writes it (`frame_io.rs:122-133`,
`:153-160`). RESP3 does not use the sink at all: it hand-encodes into a scratch `BytesMut` and
`write_all`s it **per reply**, straight past the sink into the socket (`frame_io.rs:134-148`,
`:88-104`).

Three consequences, all verified:

1. **Ordering.** A reply written past the sink jumps every reply still buffered *in* the sink. That
   is reachable today, in one pipelined batch, via `HELLO 3` (§Problem 1).
2. **Syscalls.** Measured on the real `tokio-util` sink: **1000 pipelined 5-byte replies cost 1
   write on RESP2 and 1000 on RESP3** (§Problem 2). Under TLS each of those is also a separate TLS
   record.
3. **Accounting.** Because the two paths produce bytes differently, they *count* bytes differently:
   RESP3 measures what it encoded (correct, and free), RESP2 re-derives it with a 17-line
   shadow-encoder in a different crate (`util.rs:130-146`) whose exactness a doc comment in a
   *lower* crate asserts on the wrong grounds (`response.rs:195-199`).

The proposal: **one outbound item, one `Encoder`, one write buffer.** `Outbound { version,
response }` becomes the sink's only item type; `impl Encoder<Outbound> for FrogDbResp2` owns the
per-version branch and both protocols reach the wire through `feed` + one `flush`. The encoder
counts the bytes it appends, so `add_bytes_sent` becomes a measurement rather than a re-derivation
and `estimate_resp2_frame_size` deletes. `narrow_to_resp2_outbound` (`frame_io.rs:19-28`) and
`resp3_buf` (`connection.rs:209`) delete. The two RESP encoders — `to_resp2_frame`
(`response.rs:274-334`) and `to_resp3_frame` (`:341-432`) — **do not move and their bodies do not
change**; only their caller does. That is what keeps `lint-error-sanitize` (which hard-codes
`response.rs`) trivially satisfied.

Five hotfixes are separable and land alone: **H1** the `:136`/`:146` comment contradiction (doc),
**H2** `obl` structurally zero (one line, LIVE), **H3** two dead `Encoder` impls plus the turbofish
they force, **H4** the `response.rs:195-199` cross-crate doc claim, **H5** `util.rs:128-129`
calling an exact function an approximation.

## Files involved

Line counts at `ddc4b184`.

| File | Lines | Role in this proposal |
|---|---:|---|
| `frogdb-server/crates/server/src/connection/frame_io.rs` | 283 (199 code + 84 tests, 2 `#[test]`) | **Primary.** Owns `narrow_to_resp2_outbound` `:19-28`, `send_response`/`send_wire_response` `:63-106`, `feed_response`/`feed_wire_response` `:108-150`, `flush_responses` `:153-160`. **~88 lines become ~40.** |
| `frogdb-server/crates/server/src/connection/codec.rs` | 1024 | **Primary.** Gains `Outbound` + `impl Encoder<Outbound>`; loses `Encoder<BytesFrame>` `:72-78` and `Encoder<BorrowedFrame>` `:94-100` (H3). `RESP2_NULL_ARRAY` `:49` and `Resp2Outbound` `:51-64` fold into the new item. |
| `frogdb-server/crates/server/src/connection/util.rs` | 503 | `estimate_resp2_frame_size` `:128-146` **deleted** (17 lines). `estimate_command_size` `:148-163` deleted by the separable ingress half (item C). |
| `frogdb-server/crates/server/src/connection.rs` | 922 | Three contacts: the `resp3_buf` field `:208-209` and its init `:313` (**deleted**), and the `pub(crate) use util::{…}` re-export `:98`. |
| `frogdb-server/crates/server/src/connection/lifecycle.rs` | 749 | `compute_client_memory` `:235-299`; the one-line `output_buf_len` fix at `:256-257` (**H2**). |
| `frogdb-server/crates/protocol/src/response.rs` | 1770 | **Doc-only edits**: `sanitize_error_message`'s cross-crate claim `:195-199` (H4) and the two `NullArray` notes `:267-273`, `:323-332` that name `Resp2Outbound`. **Neither encoder body is touched.** |
| `frogdb-server/crates/core/src/client_registry/mod.rs` | 1926 | `ClientMemoryUsage::output_buf_len` doc `:323` (false; H2) and `total()` `:344-352`. |
| `frogdb-server/crates/core/src/client_registry/info.rs` | 181 | Read-only evidence: `obl=` is `output_buf_len` (`:83`, `:98`). |
| `frogdb-server/crates/server/tests/resp3.rs` | 926 | Gains the RESP3 sibling of `test_pipelined_null_array_preserves_reply_order_resp2` `:649-684`, and the `HELLO 3`-mid-pipeline forcing test (§Testability). |
| `frogdb-server/crates/protocol/src/command.rs` | 104 | **Item C only.** `ParsedCommand` `:13-18` gains a wire-length field via a new constructor (`new` `:22-24` keeps its arity — see §Item C). |
| `frogdb-server/crates/server/src/connection/transaction.rs` | 243 | **Item C only.** `estimate_command_size` over the MULTI queue `:160-165`. |
| `frogdb-server/crates/server/src/connection/auth_conn_command.rs` | 672 | Read-only evidence: `set_protocol_version` `:55-57`, the HELLO switch `:366-379`. |

Read-only evidence, not edited: `tokio-util-0.7.18/src/codec/framed_impl.rs:261-303` (the sink's
flush policy), `frogdb-server/crates/server/src/tls.rs:26-117` (`MaybeTlsStream`),
`frogdb-server/crates/cluster/src/encoding_golden.rs` (359 lines — Raft/serde only, §Clearance),
`scripts/error-sanitize.py`, `Justfile:329` (`lint-gates`), `Justfile:1128-1156`, `Justfile:1249-1281`.

## Problem

### 1. LIVE: a RESP3 reply overtakes buffered RESP2 replies in the same pipelined batch

`feed_wire_response` branches on `self.state.protocol_version` at **encode time**
(`frame_io.rs:121`). The RESP2 arm buffers into the sink (`:129-132`); the RESP3 arm writes past
the sink into the socket (`:145`). Nothing reconciles the two orderings.

`HELLO 3` flips that branch **mid-batch**. Proof chain, every link at HEAD:

1. The read loop processes the first frame, then **drains every further complete frame already in
   the read buffer without flushing** (`connection.rs:661-687`). The single `flush_responses()` is
   at `:690`, after the drain.
2. `GET k` replies through `ReplyDisposition::Send` → `feed_response` (`connection.rs:550-558`) →
   RESP2 arm → `self.framed.feed(outbound)` (`frame_io.rs:130`). `Framed`'s `poll_ready` flushes
   only when the buffer has reached `backpressure_boundary` = 8 KiB
   (`framed_impl.rs:261-267`), so for a small reply the bytes **stay in the sink buffer**.
3. `HELLO 3` is dispatched next, in the same drain iteration. Its handler calls
   `set_protocol_version(ProtocolVersion::Resp3)` (`auth_conn_command.rs:370-378`, via
   `:55-57`) **before returning its reply**, and validation is deliberately ordered ahead of
   mutation (`:364-365`), so the mutation is committed.
4. The HELLO reply is fed at `connection.rs:550` → `feed_wire_response` now takes the **RESP3**
   arm → `self.framed.get_mut().write_all(&self.resp3_buf)` (`frame_io.rs:145`).
   `Framed::get_mut()` returns the **underlying stream**, not the sink, so these bytes hit the
   socket immediately.
5. `flush_responses()` at `connection.rs:690` then writes the GET reply.

Observable result: a client that pipelines `GET k` and `HELLO 3` in one segment reads the HELLO map
**first** and the GET bulk string second. A pooled client attributes the map to `GET` and is
desynchronized for the life of the connection. The reverse order (`HELLO 3` then `GET k`) is fine,
and so is any batch that does not cross the protocol boundary — which is exactly why this has
survived: the switch is usually the *first* thing a client sends.

This is the same defect class as the null-array reordering proposal 49 fixed
(`frame_io.rs:213-218` records that history, and `resp3.rs:642-684` pins the fix). That fix moved
the raw `*-1\r\n` write *into the sink* so it could not jump the queue. The RESP3 path is the
remaining writer that still bypasses the sink.

**Derived, not executed** — no server was built for this document. The forcing test is written out
in §Testability and should be **red before** the change.

### 2. Measured: one write syscall per reply on RESP3, one per 8 KiB on RESP2

`Framed`'s `start_send` encodes synchronously into the sink's `BytesMut`
(`framed_impl.rs:269-275`); `poll_ready` flushes only at the 8 KiB backpressure boundary
(`:261-267`); `poll_flush` drains the whole buffer, then flushes the inner stream (`:277-303`).
`write_all` on `framed.get_mut()` participates in none of that.

Measured with a throwaway harness (`tokio-util` `0.7.19`, `--release`; a counting `AsyncWrite` whose
`poll_write` is a no-op counter, so the numbers are exact and independent of kernel buffering; both
arms drive the *same* `Framed`, differing only in feed-vs-`write_all`):

| replies | reply size | `feed`×N + flush (**RESP2 shape**) | `write_all` per reply (**RESP3 shape**) |
|---:|---:|---:|---:|
| 1 | 5 B | 1 write | 1 write |
| 10 | 5 B | **1** | **10** |
| 100 | 5 B | **1** | **100** |
| 1 000 | 5 B | **1** | **1 000** |
| 10 000 | 5 B | **7** | **10 000** |
| 100 | 512 B | **7** | **100** |
| 1 000 | 512 B | **63** | **1 000** |

Byte totals are identical in every row; only the call count differs. The workspace pins
`tokio-util` **0.7.18** (`Cargo.lock:5457-5459`); the harness resolved 0.7.19. The measured shape
matches 0.7.18's source exactly (`INITIAL_CAPACITY = 8 * 1024` at `framed_impl.rs:25`,
`backpressure_boundary` initialised from it at `:61`/`:91`): 10 000 × 5 B = 50 000 B ÷ 8192 = 6
boundary flushes + 1 final = 7. The numbers are therefore reported as *derived from 0.7.18's source
and confirmed on 0.7.19*, not as a 0.7.18 measurement.

**TLS multiplies this.** `ConnectionStream` is `MaybeTlsStream` (`net.rs:26`), whose `Tls` arm is a
`tokio_rustls::server::TlsStream` (`tls.rs:36`) and whose `poll_write` delegates straight to it
(`tls.rs:93-102`). Each distinct `poll_write` produces at least one TLS record, so on a TLS RESP3
connection a 1 000-reply pipeline pays ~1 000 record headers + AEAD tags instead of one. The
plaintext byte accounting (`add_bytes_sent`) does not see that overhead, which is consistent with
RESP2 and is not itself a defect — it is noted so the two are not confused.

**Sizing honesty.** RESP2 is the default (`resp3.rs:500` `test_default_is_resp2`), so the population
affected is RESP3 clients that pipeline — `redis-py` with `protocol=3`, `node-redis` v4+, and every
`MULTI`/`EXEC` or `MONITOR`/pub-sub burst on a RESP3 connection (`connection.rs:719-735`, `:764-780`,
`:805-811` all feed N then flush once). This is a real but bounded population, and the fix's cost is
one afternoon, not a redesign.

### 3. The `:136` comment contradicts `:146` — and the accounting is nonetheless correct

```rust
// frame_io.rs:134-148
ProtocolVersion::Resp3 => {
    let frame = response.to_resp3_frame();
    // Don't clear resp3_buf here — accumulate across multiple feeds   // :136
    redis_protocol::resp3::encode::complete::extend_encode(&mut self.resp3_buf, &frame, false)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let encoded_len = self.resp3_buf.len() as u64;                     // :143
    self.state.local_stats.add_bytes_sent(encoded_len);                // :144
    self.framed.get_mut().write_all(&self.resp3_buf).await?;           // :145
    self.resp3_buf.clear();                                            // :146
    Ok(())
}
```

The comment describes an accumulation design that the next-but-two line abandons. It is the fossil
of a coalescing attempt that was made and reverted — which is worth stating plainly, because this
proposal is that attempt, done at the right layer.

**The accounting is correct today**, contra 80's forward note: `:146` clears after every write and
the sibling arm clears before every encode (`:91`), so `resp3_buf` is empty at every entry and
`:143` is a per-frame length. Filing this as a stats bug would produce a fix for a defect that does
not exist; it is a **doc** defect (**H1**).

One genuine, narrow consequence does follow from the clear-after-write ordering: if `write_all` at
`:145` fails, `?` returns **before** `:146`, leaving the frame in `resp3_buf`. The next `feed` on
the same connection would append to it and re-write the previous frame — a duplicated reply plus a
double `add_bytes_sent`. Four feed sites ignore the error and keep going (`connection.rs:335`,
`:395`, `:409`, `:668` all use `let _ =`), so the sequel is reachable *in principle*. In practice a
socket write error is terminal and the following write fails too, so this is **LATENT and low**. It
is listed because it dies with the change rather than needing its own fix.

### 4. LIVE: `obl` is structurally zero, and it feeds client eviction

`compute_client_memory` reads the *ingress* buffer from the real sink and the *egress* buffer from
the scratch buffer:

```rust
// lifecycle.rs:236-237
let query_buf_size = self.framed.read_buffer().len();   // the real codec buffer
...
// lifecycle.rs:256-257
// Output buffer: resp3_buf
let output_buf_len = self.resp3_buf.len();              // always 0
```

`resp3_buf` is empty at every await point outside the two `frame_io` functions (§Problem 3), and on
a RESP2 connection it is never written at all. `compute_client_memory`'s only caller is
`sync_memory_to_registry` (`lifecycle.rs:302-305`) from `maybe_sync_stats`
(`:219-232`), whose only call site is `connection.rs:542` — **inside** `process_one_command`, before
the feed at `:550` and before the batch flush at `:690`. So the field is read at exactly the moment
the *sink's* write buffer legitimately holds pending replies, and it reports the other buffer, which
is zero.

`output_buf_len` is `CLIENT LIST`/`CLIENT INFO`'s **`obl`** (`client_registry/info.rs:83`, `:98`)
and it is summed into `ClientMemoryUsage::total()` (`mod.rs:344-352`), which is `tot-mem` and the
input to `maybe_evict_clients` → `maxmemory-clients` (`lifecycle.rs:307-327`). Its doc claims
*"bytes in write buffer + resp3_buf"* (`mod.rs:323`) and counts **neither**: the write buffer is not
consulted, and `resp3_buf` is always zero.

`tokio_util::codec::Framed` exposes `write_buffer()` (`framed.rs:256`), the exact symmetric
counterpart of the `read_buffer()` already used two lines above. **H2** is that one line.

**Severity, stated honestly.** The under-count is bounded: `feed`'s backpressure flush caps the sink
buffer at ~8 KiB + one frame, so `tot-mem` under-reports by at most that per connection and
eviction is correspondingly late, not absent. The pub/sub backlog — the unbounded part — lives in
the channel and is separately hardcoded to zero (`output_list_len`/`output_list_mem`,
`lifecycle.rs:259-263`, explicitly documented as a gap). So this is primarily a **misleading
observability field** (an operator reading `obl=0` on a stalled client learns nothing) with a
bounded eviction consequence. No test asserts `obl` anywhere in the tree.

### 5. The RESP3 egress path has no test — the "RESP3 test" is a hand-copy of the code

`frame_io.rs:256-282` (`resp3_null_array_feed_order_is_preserved`) is documented as the RESP3
sibling of the RESP2 ordering test. It does not call `feed_wire_response`. It **re-implements** the
arm inline:

```rust
// frame_io.rs:262-272 — "Mirror feed_wire_response's RESP3 arm"
let mut buf = bytes::BytesMut::new();
for frame in [ /* … */ ] {
    buf.clear();
    redis_protocol::resp3::encode::complete::extend_encode(&mut buf, &frame, false).unwrap();
    server.write_all(&buf).await.unwrap();
}
```

A test that re-implements its subject cannot fail when the subject changes. It would pass unchanged
if `feed_wire_response`'s RESP3 arm were deleted outright. The reason it is written that way is
structural: `ConnectionHandler` cannot be constructed in a unit test (the one place that manages it,
`connection/scripting/script.rs:187-198`, needs a `TcpListener`, two mock shard channels and a
`MaybeTlsStream`), so the RESP3 arm is not reachable from a cheap test **while it lives on
`ConnectionHandler`**.

Move the branch into the codec and it becomes reachable: `codec.rs:996-1023` already unit-tests
`Encoder<Resp2Outbound>` directly, with no handler, no socket and no runtime.

### 6. LIVE (accounting): `estimate_command_size` over-charges inline commands to the rate limiter

`estimate_command_size` (`util.rs:148-163`) reconstructs the RESP array framing from a
`ParsedCommand`, which carries only `name` and `args` (`protocol/src/command.rs:13-18`) — the wire
bytes are gone by then. For a well-formed RESP2 array the reconstruction is exact. For an **inline
(telnet-style)** command it is not, because the decoder synthesises an array frame from a bare line
(`codec.rs:356-366`): `PING\r\n` is **6 bytes on the wire** and is charged
`*1\r\n` (4) + `$4\r\nPING\r\n` (10) = **14** — a **2.3×** over-charge. Quoted inline args lose their
quotes to `sdssplitargs` (`codec.rs:350`) and drift further.

This is not merely a stats field. `cmd_bytes` feeds `check_rate_limit` (`connection.rs:379-380`,
`:403`) and the whole MULTI queue is re-estimated for `try_acquire_batch`
(`transaction.rs:160-165`). The drift direction is **conservative** — an over-charge rate-limits a
client *earlier* than it should, so this is a fairness/accuracy defect, **not** a limiter bypass.
The tree exercises the inline path (`redis-regression/tests/protocol_tcl.rs:320`
`tcl_inline_pipelined`), so the mis-charge is live wherever inline clients meet a byte quota.

Note the asymmetry with egress: the **decoder** knows precisely how many bytes it consumed
(`codec.rs:106-…` operates on `src: &mut BytesMut` and splits from it), and that number is thrown
away, then approximated one layer up. That is the same shape as §Problem 3's egress accounting, and
it has the same fix (item C).

### 7. Two `Encoder` impls with zero call sites, and a turbofish that exists only because of them

`FrogDbResp2` implements `Encoder` three times: `BytesFrame` (`codec.rs:72-78`), `Resp2Outbound`
(`:80-92`), `BorrowedFrame` (`:94-100`).

`git grep '\.feed(\|\.send(\|\.encode('` over `connection/` returns exactly four sink/encoder uses,
**all `Resp2Outbound`**: `frame_io.rs:84`, `:130`, `:226`, `:231`, plus the codec's own tests at
`codec.rs:1006` and `:1017`. `BorrowedFrame` has **zero** uses anywhere in the tree outside its own
`impl` and the `use` that imports the type (`codec.rs:27`). `BytesFrame`'s `Encoder` impl is never
used to send anything either — its only appearance is as the **disambiguation token** in

```rust
// frame_io.rs:155-158
// Disambiguate: Resp2 now implements Encoder for both BytesFrame and BorrowedFrame.
SinkExt::<redis_protocol::resp2::types::BytesFrame>::flush(&mut self.framed)
```

The comment is also wrong on its own terms: there are **three** impls, not two, and the turbofish
names a type that is never fed. Delete the two unused impls and the flush is `self.framed.flush()`.

### 8. Why these are one problem

Every item above is a consequence of a single missing decision: **who owns turning a `WireResponse`
into bytes on this connection.** Today the answer is "the connection handler, differently per
protocol version, in two functions, using two buffers, with the size computed in a third place."
The sink already exists, already owns the RESP2 answer, already owns the one byte-sequence
`redis-protocol` cannot produce (`RESP2_NULL_ARRAY`, `codec.rs:44-49`), and is already the thing the
ordering invariant is stated about. RESP3 is the one caller that goes around it.

## Proposed change

### The module and its interface

`connection/codec.rs` becomes the sole owner of RESP egress bytes. Its **interface** is one item
type and one `Encoder`; its **implementation** absorbs the per-version branch that
`ConnectionHandler` currently duplicates in two functions.

```rust
// connection/codec.rs
/// One reply, plus the protocol version the connection speaks at the moment it
/// is queued. The version rides with the item so the codec holds no protocol
/// state of its own — `ConnectionState::protocol_version` stays the single
/// authority (`auth_conn_command.rs:55-57` is its only production setter).
pub struct Outbound {
    pub version: ProtocolVersion,
    pub response: WireResponse,
}

impl Encoder<Outbound> for FrogDbResp2 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: Outbound, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let before = dst.len();
        match item.version {
            // `redis-protocol`'s RESP2 `Null` is always `$-1\r\n`, so the
            // top-level array-null is the one shape this codec emits itself.
            ProtocolVersion::Resp2 => match item.response {
                WireResponse::NullArray => dst.extend_from_slice(RESP2_NULL_ARRAY),
                other => self.inner.encode(other.to_resp2_frame(), dst)?,
            },
            ProtocolVersion::Resp3 => redis_protocol::resp3::encode::complete::extend_encode(
                dst,
                &item.response.to_resp3_frame(),
                false,
            )
            .map(|_| ())?,
        }
        self.bytes_encoded += (dst.len() - before) as u64;
        Ok(())
    }
}
```

`bytes_encoded` is a `u64` on `FrogDbResp2`, drained by `take_bytes_encoded()`. It is *derived*
state — written only by `encode`, read only by the one caller below — not a second authority for
anything.

`frame_io.rs`'s two 40-line functions collapse:

```rust
async fn feed_wire_response(&mut self, response: WireResponse) -> std::io::Result<()> {
    let item = Outbound { version: self.state.protocol_version, response };
    self.framed.feed(item).await.map_err(std::io::Error::other)?;
    let n = self.framed.codec_mut().take_bytes_encoded();
    self.state.local_stats.add_bytes_sent(n);
    Ok(())
}

async fn send_wire_response(&mut self, response: WireResponse) -> std::io::Result<()> {
    self.feed_wire_response(response).await?;
    self.flush_responses().await
}

pub(super) async fn flush_responses(&mut self) -> std::io::Result<()> {
    self.framed.flush().await.map_err(std::io::Error::other)?;   // no turbofish (H3)
    self.framed.get_mut().flush().await
}
```

### Seam, adapter, depth, leverage, locality

**Seam.** "Bytes leave this connection" becomes one function with one buffer. Today it is two
functions, two buffers and — critically — two *destinations* (the sink and the raw stream), which is
why §Problem 1 exists at all. After the change the raw stream is written by exactly one line
(`flush_responses`'s inner flush) and nothing can overtake anything.

**Adapter.** `Outbound` is the adapter between "what the server decided to say"
(`WireResponse`) and "what this socket speaks" (`ProtocolVersion`). Making the version a *field of
the item* rather than *state of the codec* is the load-bearing choice: `ConnectionState` remains the
one authority, there is no second copy to keep in sync across `HELLO`, and — because
`Framed::start_send` encodes **synchronously** (`framed_impl.rs:269-275`) — a version change between
two feeds affects exactly the later one, which is the semantics the current code has.

**Depth.** Interface: one item, one method. Implementation absorbed: a 10-line narrowing function, a
17-line shadow encoder in another crate, a scratch `BytesMut` on the handler, two dead `Encoder`
impls, and one turbofish. The *decisions* preserved are exactly two — "RESP2 top-level null-array is
`*-1\r\n`" and "RESP3 encodes through `extend_encode`" — and both now sit beside the constant and
the frame types they use.

**Leverage.** One `impl` closes: an ordering defect (§1), an N-syscall amplification (§2), an
unmeasurable stats path (§3), a structurally-zero observability field (§4), an untestable egress arm
(§5), and two dead impls (§7). Nothing else in the connection layer changes shape: `feed_response`,
`send_response` and `flush_responses` keep their signatures, so all **17** call sites in
`connection.rs` are untouched.

**Locality.** The question *"what bytes does this reply put on the wire, and how many?"* is today
answered by reading `frame_io.rs`, `codec.rs`, `util.rs` (a different module) and `response.rs` (a
different crate), and by knowing that `Framed::get_mut()` bypasses the sink. After the change it is
answered by one `encode` function, and the byte count is `dst.len()` before and after it.

### Deletion test, applied honestly

**`narrow_to_resp2_outbound` (`frame_io.rs:19-28`).** It does three things:

1. Routes `WireResponse::NullArray` to `Resp2Outbound::NullArray` — **a real decision** (RESP2's
   top-level array-null is not representable as a `Resp2BytesFrame`). It must survive; it moves into
   `Encoder::encode`, three lines from the `RESP2_NULL_ARRAY` constant it ultimately emits.
2. Calls `to_resp2_frame()` — a call, not a decision. Moves with the match.
3. Computes `estimate_resp2_frame_size(&frame)` — a re-derivation of a number the encoder is about
   to produce. **Carries no decision and dies.**

So of ten lines, one decision survives and relocates to where its data already lives. That is what
makes this a fold rather than a move.

**`estimate_resp2_frame_size` (`util.rs:128-146`).** Zero decisions: every arm restates
`redis-protocol`'s RESP2 framing (`+`/`-`/`:`/`$`/`*` + CRLF). It is a second encoder that must be
kept in agreement with the first, forever, with nothing checking that it is. Deleted.

**`resp3_buf` (`connection.rs:208-209`, `:313`).** Its stated purpose — *"Reusable buffer for RESP3
encoding to avoid per-response allocation"* — is served strictly better by the sink's own
`BytesMut`, which is reused across the whole batch instead of per reply. Deleted; `lifecycle.rs:257`
is repointed at the sink (H2).

**`Encoder<BytesFrame>` / `Encoder<BorrowedFrame>` (`codec.rs:72-78`, `:94-100`).** Zero call sites
(§Problem 7). Deleted — but **both together**, in one commit with the turbofish: deleting only one
leaves two impls and the ambiguity remains.

**What is *not* deletable:** `Resp2Outbound` survives in spirit as the RESP2 arm's `NullArray`
branch; `FrogDbResp2`'s `Decoder` (`codec.rs:102-…`) is untouched; both `to_resp*_frame` bodies are
untouched.

### Alternatives weighed

**(A) Give the codec a `protocol_version` field, set on `HELLO`.** Rejected. It creates a second
copy of a value whose authority is `ConnectionState`, to be re-synced at `auth_conn_command.rs:378`
and at `ConnectionState::reset` (`state.rs:1085`, which resets to `Resp2`). A missed sync is a
silently mis-encoded connection. The item-carries-the-version shape has no such failure mode.

**(B) PN8 as briefed: `WireResponse::encoded_len(ProtocolVersion)` + a property test that it agrees
with `encode().len()`.** Rejected as the primary. `encoded_len` is a *third* encoder-shaped function
(after `to_resp*_frame` and `estimate_resp2_frame_size`), and the property test exists precisely to
guard the duplication it introduces. Measuring `dst.len()` across `encode` gives the same number,
exactly, with no second implementation and no test to keep it honest. `encoded_len` becomes
worthwhile only when a caller needs the size **before** committing bytes — a `client-output-buffer-limit`
pre-flight check, which does not exist today. If that caller ever appears, §Testability gives the
property test that must ship with it.

**(C) Compute the byte delta from `framed.write_buffer().len()` around the `feed`, with no counter
in the codec.** Rejected, narrowly. `feed` is `poll_ready` + `start_send`, and `poll_ready` may
*flush* the buffer to zero before the item is encoded (`framed_impl.rs:261-267`), so `after -
before` underflows on exactly the batches this proposal is about. The correct expression
(`after.checked_sub(before).unwrap_or(after)`) needs a paragraph of justification to read as
correct, which the project's own guidance treats as a signal that the code is wrong. The counter
inside `encode` is unconditional and needs no explanation.

### Item C — the ingress half, separable

Symmetric to the egress fold and independently landable. The decoder consumes an exact byte count
from `src`; carry it instead of reconstructing it:

- `ParsedCommand` gains `wire_len: usize`. **`ParsedCommand::new(name, args)` keeps its arity** and
  sets `wire_len: 0` ("synthesized, not from the wire"); a new `from_wire(name, args, wire_len)` is
  used only on the decode path. This matters: of the **22** `ParsedCommand::new` call sites, one is
  in `frogdb-txn/tests/exec_outcomes.rs` — a **LOCKED** crate that must not be touched.
- `FrogDbResp2::decode` records the bytes it split from `src`; `connection.rs:379` and
  `transaction.rs:160-165` read the recorded number instead of calling `estimate_command_size`,
  which deletes (`util.rs:148-163`).
- Closes §Problem 6's inline over-charge by construction.

Rated separately because it changes `Decoder::Item` plumbing (`connection.rs:626`, `:663`,
`try_next_frame` `frame_io.rs:165-183`) and is the smaller-value half.

## Testability improvement

Four pins, in the order they should be written.

**1. Red-green: writes per pipelined batch (unit, `codec.rs`).** The existing frame_io tests already
build a bare `Framed::new(server, FrogDbResp2::default())` over `tokio::io::duplex`
(`frame_io.rs:221-222`), so the shape is established. Add a `CountingStream` wrapping one duplex
half whose `poll_write` increments a counter, then:

```rust
// feed three RESP3 replies through the sink, flush once
for r in [ok(), ok(), ok()] {
    framed.feed(Outbound { version: ProtocolVersion::Resp3, response: r }).await.unwrap();
}
SinkExt::<Outbound>::flush(&mut framed).await.unwrap();
assert_eq!(counter.load(), 1, "a pipelined RESP3 batch must cost one write");
```

**Red before the change** — and red in a specific, useful way: the test cannot even be *written*
against today's path, because today's RESP3 arm does not go through the sink. That is the finding
§Problem 5 names, expressed as a test.

**2. Red-green: the `HELLO 3` reordering (integration, `resp3.rs`).** The forcing test for
§Problem 1, modelled directly on `test_pipelined_null_array_preserves_reply_order_resp2`
(`resp3.rs:649-684`): `SET k v`, then one `write_all` containing `GET k` **followed by** `HELLO 3`,
then `read_exact` the concatenation and assert the GET bulk string precedes the HELLO map. Red
today (the map arrives first), green after. This is the test that must exist **before** any
implementation, because it is the only one that pins the ordering property end to end.

**3. RESP3 pipelined-ordering sibling (integration, `resp3.rs`).** `test_pipelined_null_array_preserves_reply_order_resp2`
(`:649`) has no RESP3 counterpart; the nearest thing is the hand-copy at `frame_io.rs:256`. Add the
real one: `GET k1; LPOP missing 2; GET k2` on a `HELLO 3` connection, asserting
`$2\r\nv1\r\n_\r\n$2\r\nv2\r\n`. Passes before and after — it pins behaviour the fold must preserve.

**4. Byte accounting as a property, with no shadow implementation.** For every `WireResponse` shape
and both versions: encode into a fresh `BytesMut` and assert
`codec.take_bytes_encoded() == dst.len()`, and that `dst` is byte-identical to what the existing
per-variant wire tests expect. This is a genuine property (the counter agrees with the buffer),
unlike `encoded_len == encode().len()`, which only asserts that two implementations of the same
function agree. **If alternative (B) is ever adopted**, its property test is:

```rust
proptest! {
    #[test]
    fn encoded_len_matches_encoding(r in arb_wire_response(), v in arb_version()) {
        let mut dst = BytesMut::new();
        FrogDbResp2::default().encode(Outbound { version: v, response: r.clone() }, &mut dst)?;
        prop_assert_eq!(r.encoded_len(v), dst.len());
    }
}
```

with a generator that reaches every one of the 16 variants including nested `Array`/`Map`/`Set`/
`Attribute` and the top-level-vs-nested `NullArray` split (`response.rs:267-273`, `:323-332`) — the
one place the two protocols disagree structurally and therefore the one place such a function would
most plausibly drift.

**5. `obl` (unit, H2).** After the fix, feed two replies without flushing and assert
`compute_client_memory().output_buf_len > 0`; assert it returns to 0 after `flush_responses`. No
test asserts `obl` today.

**Mutation note.** None of `frogdb-server`, `frogdb-protocol` is a gated crate, so no
`just mutants-gate` threshold applies. The fold removes ~17 lines of shadow-encoder whose mutants
are unkillable-by-construction today (nothing compares the estimate to reality), so it raises the
signal of any future run rather than the score.

## Spec / LOCKED clearance

**Crates edited:** `frogdb-server` (`server`), `frogdb-protocol` (doc-only), `frogdb-core`
(`client_registry` doc-only; `command.rs` under item C). **None is a LOCKED area.** `frogdb-txn`,
`frogdb-vll`, `frogdb-persistence`, `frogdb-recovery`, `frogdb-replication[-runtime]`,
`frogdb-cluster[-runtime]` are **not edited**. The one place item C brushes a locked crate —
`frogdb-txn/tests/exec_outcomes.rs`, a `ParsedCommand::new` call site — is avoided by keeping
`new`'s arity (§Item C). If any `frogdb-txn` file is nonetheless touched at implementation time,
`just mutants-diff frogdb-txn` is required before pushing.

**FM tags:** `git grep 'FM-'` over `connection.rs`, `connection/frame_io.rs`, `connection/util.rs`,
`connection/codec.rs` and `protocol/src/response.rs` returns **zero** hits. No failure-mode row is
forced by, or references, any edited region, so `just lint-failure-modes` is unaffected. Do **not**
add an id-only `// FM-…` comment in `frogdb-protocol`: the crate is absent from `NEXTEST_CRATES` in
`scripts/failure-modes.py`, so a tag there would be unenforceable (this constraint is inherited from
proposal 80's verification and re-checked here).

## Seam-lint clearance

All fourteen gates in `lint-gates` (`Justfile:329`) were read and applied to this plan.

**In scope — cleared by construction:**

- **`lint-error-sanitize`** (`Justfile:1303-1305` → `scripts/error-sanitize.py`). The script
  hard-codes `frogdb-server/crates/protocol/src/response.rs` and requires `sanitize_error_message`
  to be the **immediate first token** of every `Resp2BytesFrame::Error(` and
  `Resp3BytesFrame::SimpleError { data:` construction; compliant sites are `:277`, `:292`, `:348`,
  with `BlobError` `:378-381` deliberately exempt. **This proposal does not move, rename, or edit
  either encoder body** — the fold changes only *who calls* `to_resp2_frame`/`to_resp3_frame`, from
  `frame_io.rs` to `codec.rs`. All three construction sites keep their exact text and their file.
  Run mentally: the script finds `RESPONSE` present, blanks `#[cfg(test)]` spans, matches three
  `*_OPEN` sites, all three also match `*_GOOD` → **0 violations, gate passes non-vacuously**. The
  vacuous-pass hazard 80 identifies (relocating the encoders would leave the gate scanning a file
  with nothing to check) is **not** taken. Alternative (B), if ever adopted, adds `encoded_len` to
  `response.rs`; if implemented by measurement it constructs no error frames, and if implemented by
  encoding it routes through the already-compliant sites — clear either way.
- **`lint-pubsub-confirmation-seam`** (`Justfile:1128-1156`). Its second rule greps
  `b"\*-1` across `frogdb-server/crates/server/src` with `--exclude=codec.rs`. Today `frame_io.rs:9`
  imports `RESP2_NULL_ARRAY` and `:21` reads its `.len()` — legal (it is the constant, not the
  literal), but it is a second consumer. After the fold, `RESP2_NULL_ARRAY`'s **only** reference is
  inside `codec.rs`, where the gate's own comment says the shape belongs. The change **strengthens**
  this gate. No new `b"*-1"` literal is introduced anywhere. Its first rule (pub/sub confirmation
  labels in `pubsub_conn_command.rs`) is untouched.

**Out of scope — verified not applicable:**

| Gate | Why it cannot fire |
|---|---|
| `lint-format-float` (`Justfile:1249-1281`) | Pins exactly **one** `fn format_float`, at `protocol/src/format.rs`. This proposal adds **no** float rendering: the only float path is `response.rs:286`'s existing `format_float(d)` inside `to_resp2_frame`, which is **not edited**, and the RESP3 side delegates to `redis-protocol`'s own `f64` encoder as it does today (`resp3.rs:686-724` documents that division). Count stays at 1. |
| `lint-info-seam` (`:423-441`) | Three named files: `commands/info.rs`, `connection/scatter.rs`, `connection/info_handler.rs`. None edited. |
| `lint-redirect-seam` (`:449-479`) | Greps `Response::error("CROSSSLOT` / `Response::error((format!()?"(MOVED\|ASK) `. No such construction is added or moved. |
| `lint-failover-atomicity` (`:1166-1196`) | `ClusterCommand` writes in `cluster-runtime`/`connection/cluster.rs`. Not edited. |
| `lint-metrics-chokepoint` (`:1198-1247`) | Greps `.increment_counter(`/`.record_gauge(`/`.record_histogram(`. The byte counter goes through `local_stats.add_bytes_sent` (`connection/state.rs:391-393`) as it does today — no raw recorder call added. |
| `lint-clock-seam` (`:1284-1286` → `scripts/clock-seam.py`, scans `frogdb-server/crates`) | No clock read added or moved. |
| `lint-durable-ack` (`:1290-1292`) | Single-file pin on `cluster/src/storage.rs`. |
| `lint-nested-config` (`:1296-1298`) | figment `.nested()`. No config source touched. |
| `lint-no-typed-unwrap` (`:1012-1040`) | Scans `crates/commands/src/` only. |
| `lint-keyspace-notify-routing` (`:1051-1067`) | Scans `core/src/shard`. |
| `lint-script-gate` (`:1080-…`) | Scans the scripting module for `block_in_place` / `extract_keys_from_command`. |
| `lint-continuation-lock` (`:1312-1314`) | Count-pinned arms of the 11 shard `*Msg` enums. No shard message enum is edited. |
| `lint-failure-modes` (`:293`) | Zero `FM-` tags in every edited file (§Spec clearance). |

**Golden tests — nothing re-pins.** The only golden-encoding suite in the tree is
`frogdb-cluster/src/encoding_golden.rs` (359 lines), which pins **`serde_json` encodings of
`ClusterCommand` and `ClusterStateInner`** for Raft log/snapshot cross-version compatibility
(module doc `:1-29`). `grep -n 'WireResponse\|resp2\|resp3'` over that file returns **nothing**; it
has no contact with the RESP wire format. `UPDATE_GOLDEN=1` is not needed.

What *does* pin RESP wire bytes is `frogdb-server/crates/server/tests/resp3.rs` — the null-array
byte tests (`:596`, `:610`), the pipelined-order test (`:649`) and eight non-finite-double byte
tests (`:775-926`). **All of them must stay green unchanged**, and all of them are single-command
round trips (`send_raw_command` `:759` writes one command and reads once), so reply-coalescing
cannot move their bytes: each command is its own read-loop iteration with its own flush at
`connection.rs:690`.

## Behaviour changes

1. **RESP3 replies are buffered until the batch flush** instead of being written per reply. Every
   feed site is already followed by a `flush_responses()` on every path — `connection.rs:690` (the
   command loop, including the `QUIT` and PSYNC-handoff breaks, which set `should_break` and fall
   through the flush at `:690-697`), `:737` (pub/sub), `:781` (invalidation), `:815` (MONITOR) —
   so no reply is lost. **This is the invariant to re-verify at review time**, because after the
   change a future `break` that skips the flush would silently drop replies. The mitigating fact:
   RESP2 has had exactly this property since forever, so the change makes both protocols share one
   already-audited invariant instead of maintaining two.
2. **Reply ordering across a mid-batch `HELLO 3` is fixed** (§Problem 1). This is a wire-visible
   change and the point of the proposal; it is pinned by test 2 in §Testability.
3. **`obl` becomes non-zero** for connections with pending output (H2). Wire-visible in
   `CLIENT LIST`/`CLIENT INFO` and in `tot-mem`. No test asserts the old value.
4. **`add_bytes_sent` totals are unchanged** on both protocols: RESP2's estimator is exact for the
   frames it sees (§Problem 3's correction), and RESP3 already measured. The change is *how* the
   number is obtained, not what it is. `INFO`'s `total_net_output_bytes` is a hardcoded `0`
   (`info/sections.rs:318`) and is unaffected either way.

## Risks / scope boundaries

### vs proposal 80 — `Response`/`WireResponse` fold (PN1) — two shared files; **land 80 first**

80's reviewer-verified partition is adopted verbatim:

| Owned by **80** | Owned by **86** |
|---|---|
| `response.rs` `:40-73`, `:647-743`, `:748`, `:754-940` | `response.rs` `:274-334`, `:341-432` (the two encoders) |
| `frame_io.rs:31-53` (`narrow_to_wire`) | `frame_io.rs` `:19-28`, `:63-150` |
| — | `connection/util.rs:130-163` |

Two amendments, both verified:

**(a) The partition is not quite disjoint, and 80's own text says so.** 80's §Proposed change gives
both encoders a new arm (`Resp::Internal(never) => match never {}`), which is an insert **inside
86's `:274-334` and `:341-432`**. That is a two-line, end-of-match insert and is harmless in either
order — 86 does not modify either match body, only its callers. Recorded so neither author is
surprised by the overlap their own tables deny.

**(b) A real textual conflict at `frame_io.rs:55-65` — this is why order matters.** 80's hotfix
**H4** rewrites the doc comment at `frame_io.rs:61-62` (*"it can never encode a control-flow signal
and cannot panic on one"*). Lines `:55-62` are the doc block **of `send_response`**, whose body
`:63-65` 86 rewrites. 80 declares "landing order is free"; for `response.rs` that is true, for
`frame_io.rs` it is not.

**Recommended ruling: 80 lands first (fold + all four hotfixes); 86 rebases onto it.** Reasons, in
order of weight:

1. 80 is **already committed** (`f1b7d196`) with a revision in flight; 86 is not yet written to the
   tree. Ordering the unwritten work behind the written work is free.
2. 80's H4 corrects prose about `send_response`. If 86 landed first, `send_response` would be a
   two-line delegation and H4 would have to be re-derived against a function that no longer says
   what it corrected. The reverse — 86 absorbing 80's corrected wording into the new, shorter
   function — is a copy-paste.
3. 86 depends on nothing 80 produces (`WireResponse` keeps its name, its 16 variants and both
   encoder bodies; the alias change is source-compatible for every `WireResponse::X` pattern 86
   writes), so the dependency is one-directional and cheap.

**Also transferred from 80:** its `lint-pubsub-confirmation-seam` clearance states that
`narrow_to_resp2_outbound` "is not edited by this proposal (it is proposal 86's), so the constraint
is inherited unchanged." 86 **deletes** that function; §Seam-lint clearance above shows the
constraint is not merely preserved but tightened (the constant's only consumer becomes `codec.rs`).

**And corrected from 80:** its "Recorded, not claimed" note that `:143` "mis-reports encoded size"
is wrong — see §Corrections. The `:136`/`:146` contradiction is real; the mis-count is not.

### vs proposal 84 — `BlockingOp`/`Direction` dedupe (PN6) — same file, adjacent regions

84 deletes `connection/util.rs:74-126` (`convert_blocking_op`, `convert_direction`). 86 deletes
`:128-146` and — under item C — `:148-163`. **Adjacent, non-overlapping, and the ranges do not
share a three-line diff context** (84's last line `:126` and 86's first `:128` are separated by the
blank line and doc comment at `:127-129`, which is 86's). Git will merge these cleanly in either
order; if a conflict does surface it is a one-hunk resolution. 84's own ruling ("same file, adjacent
regions, no overlap … either order") is correct — only its attribution of PN8 to proposal 67 is
wrong (§Corrections). If both land, `util.rs` drops ~90 lines and keeps `raft_op_to_command`
(`:165-…`), which neither proposal touches. **No ordering constraint.**

### vs proposal 70 — ACL registry consult — same file, far apart

70 edits `connection/util.rs:250-267` (`CONTAINER_COMMANDS`, `extract_subcommand`). 86's ranges end
at `:163`. ~87 lines apart. **No contact.**

### vs proposal 78 — test-harness RESP client — same file, additive, no contact

78 adds a frame→`Response` mapper to `response.rs` and lists it as a "candidate home"; its
`response.rs` cites are `:274` and `:770`, both read-only. 86's `response.rs` edits are **doc-only**
at `:195-199`, `:267-273`, `:323-332`. 78 also owns `server/tests/resp3.rs`'s `Resp2Client`
(`:31-56`) and the raw-byte helpers (`:729`, `:742`, `:759`); **86 adds two new `#[tokio::test]`
functions to the same file** (§Testability tests 2 and 3) built on those helpers. That is an
append-at-end contact, not a rewrite. **No ordering constraint; coordinate at land time on
`resp3.rs`.** 78 does not touch `connection.rs`, `frame_io.rs`, `codec.rs` or TLS plumbing — the
brief's suggestion that it does is not borne out: `grep -n 'connection/codec.rs\|frame_io.rs\|connection/util.rs'`
over 78 returns nothing, and its files table names no `server/src/` file except `migrate.rs`.

### vs proposal 67 — server small dedups — disjoint

67's `connection.rs` edits are `:4-6` (module doc), `:21` (`mod builder;`) and `:71` (a `pub use`).
86's `connection.rs` edits are `:98`, `:208-209`, `:313`. **No shared line, no shared diff context.**
67 contains zero `connection/util.rs` references (§Corrections).

### vs proposal 81 — core dead seams — one forward reference, no write contact

81:557 proposes folding `frogdb-types` copies and deleting "the hand converter in
`server/connection/util.rs`" — that is 84's `convert_blocking_op`/`convert_direction` (`:74-126`),
not 86's range. 81's file set is otherwise `core/src/shard/*` + `server/src/acceptor.rs`.
**No overlap.**

### vs proposal 68 — EXEC framing datum — read-only citations

68 cites `frame_io.rs:41` (inside 80's `narrow_to_wire`) and `response.rs:422` (inside 86's
`to_resp3_frame` range) **read-only**, as evidence in a proof chain about `Response::Push` nested in
an EXEC array. 86 does not edit `:422`. **No contact.** Note for both: 68's illegal-RESP3-nesting
argument is about *what* is encoded, 86 is about *when the bytes are written* — the fold neither
fixes nor worsens it.

### Other risks

- **The buffered-reply invariant** (§Behaviour changes 1) is the one genuine regression surface.
  Mitigation: it is already RESP2's invariant, and the `QUIT` path — the most dangerous one, since
  the reply must reach a client that is about to be disconnected — flushes at `connection.rs:690`
  before the `break` at `:695-697`, which is verified above and should be re-verified in review.
- **PSYNC handoff.** `Framed::into_inner()` (`connection.rs:862`) **discards** the sink's write
  buffer. Today the handoff path sets `should_break` and falls through the flush at `:690` before
  reaching `:862`, so nothing is lost. After the change, RESP3 replies also live in that buffer, so
  the flush-before-`into_inner` ordering becomes load-bearing for both protocols. It is already
  correct; it now needs a comment saying why.
- **`bytes_encoded` overflow.** `u64` counting bytes on one connection. Not a real risk; stated so
  the reviewer does not have to ask.
- **`Encoder<Outbound>` and `Decoder` on the same struct.** `FrogDbResp2` already implements both;
  adding one `Encoder` and deleting two changes nothing structurally.
- **Diff size.** ~90 lines deleted, ~50 added, across five files. Land in three steps, each keeping
  the suite green: (1) H1–H5 (docs, `obl`, dead impls), (2) the codec fold + tests, (3) item C.

## Effort

**M** for the codec fold (items 1–2). One 88-line region of `frame_io.rs` becomes ~40; one item type
and one `Encoder` impl added to `codec.rs` and two deleted; 17 lines deleted from `util.rs`; two
lines deleted from `connection.rs`; one line changed in `lifecycle.rs`; doc-only in `response.rs`.
The two forcing tests (§Testability 1–2) are the real work and should be written first. Bounded by
the fact that **no caller signature changes** — all 17 `feed_response`/`send_response`/
`flush_responses` sites in `connection.rs` are untouched.

**S–M** for item C (ingress), separable and landable later: it changes `Decoder::Item` plumbing
through `try_next_frame` and two read sites.

**S** for H2, **XS** each for H1, H3, H4, H5 — all independently landable ahead of the fold.

## Independently-landable hotfixes

### H1 — the `:136` / `:146` contradiction · **LIVE (doc)** · *claimed*

`frame_io.rs:136` says *"Don't clear resp3_buf here — accumulate across multiple feeds"*; `:146`
clears it on every iteration. Comment-only fix: state what the code does (each feed encodes, writes
and empties the scratch buffer, so `:143`'s length is per-frame) and why that is the *problem*
(pointing at this proposal), not the design. **Do not** "fix" it by deleting `:146` — that would
double-write every frame. XS.

### H2 — `obl` / `tot-mem` never count pending output · **LIVE** · *claimed*

`lifecycle.rs:256-257`: replace `self.resp3_buf.len()` with `self.framed.write_buffer().len()`
(`tokio-util` `framed.rs:256`), the symmetric counterpart of `read_buffer()` used at `:237`. Also
correct the doc at `client_registry/mod.rs:323`, which claims the field counts *"bytes in write
buffer + resp3_buf"* and counts neither. Add the unit assertion in §Testability 5.

Lands **before** the fold (it is correct on today's code and stays correct after: `resp3_buf`
disappears, `write_buffer()` remains). One line + one doc line + one test. S.

### H3 — delete two `Encoder` impls with zero call sites, and the turbofish they force · **LATENT** · *claimed*

`codec.rs:72-78` (`Encoder<BytesFrame>`) and `:94-100` (`Encoder<BorrowedFrame>`) — zero uses
outside their own definitions (§Problem 7). Delete **both in one commit**, together with the
turbofish and its incorrect comment at `frame_io.rs:155-158` (which says "both", naming two of
three, and points at a type that is never fed). Deleting only one keeps the ambiguity. Pure
deletion + one expression simplification. XS.

### H4 — `sanitize_error_message`'s doc cites an upper-crate function, for a reason that is moot · **LATENT (doc)** · *claimed*

`response.rs:195-199`: *"a length-preserving transform keeps the byte-accounting in
`estimate_resp2_frame_size` exact."* Two defects: `estimate_resp2_frame_size` is a `pub(crate)` fn in
`frogdb-server`, a crate **above** `frogdb-protocol`, invisible from this file and unlinkable in
rustdoc; and the argument does not hold anyway, because the estimator measures the frame **after**
sanitization (`response.rs:277`/`:292` → `frame_io.rs:23-24`), so length-preservation is irrelevant
to its exactness. Rewrite to the reason that *is* load-bearing and local: the transform is
length-preserving and CR/LF-free so a simple error cannot inject a second wire frame — which is what
the paragraph at `:180-185` already argues. Comment-only; does not touch a construction site, so
`lint-error-sanitize` is unaffected. XS.

### H5 — `estimate_resp2_frame_size` is documented as an approximation and is exact · **LATENT (doc)** · *claimed only if the fold is deferred*

`util.rs:128-129`: *"Estimate the size of a RESP2 frame in bytes. This is an approximation based on
the frame structure."* Every arm is byte-exact against `redis-protocol`'s RESP2 encoder
(`+`/`-`/`:` = 1+len+2, `$` = 1+digits+2+len+2, `*` = header + Σ elements, `Null` = 5). The name and
the doc both mislead: a reader who believes it is approximate will not think to keep it in sync, and
a reader who needs an exact count will not use it. The fold **deletes the function**, which is the
better fix — so this hotfix is claimed **only** if the fold is deferred. XS.

### Recorded, not claimed

- **`resp=2` is hardcoded in `CLIENT INFO`** (`client_registry/info.rs:83`) — the field is a string
  literal inside the format template, so a RESP3 connection reports `resp=2`. Misleading
  observability of exactly the value this proposal's egress path branches on. **Outside 86's owned
  ranges** (`client_registry/info.rs` is otherwise read-only evidence here) — needs an owner.
- **Stale `resp3_buf` after a failed `write_all`** (§Problem 3) → duplicated frame on the next feed.
  LATENT, low, and **deleted by the fold** rather than fixed separately.
- **Inline-command over-charge to the byte rate limiter** (§Problem 6) → item C's motivation, not a
  hotfix: the fix is the plumbing change, not a one-liner.

### Security classification (no fix proposed — standing policy)

One finding in the byte-accounting seam is security-shaped and is therefore **classified and
parked**, per the standing policy that security findings are filed, not fixed, in proposals:

**Rate-limit accounting gap: blank-line flood is charged zero bytes.** The decoder silently consumes
leading `\r\n` pairs before any framing (`codec.rs:113-118`, *"Redis silently ignores these"*).
Those bytes never become a `ParsedCommand`, so `estimate_command_size` never sees them and neither
the byte quota (`connection.rs:379-380`, `:403`) nor the command-count quota is charged. A client can
therefore consume server read bandwidth and read-buffer work at **zero quota cost**. Severity
depends on whether any other limiter covers raw read volume on a connection — **not audited here**.
Direction matters: this is an **under**-charge (unlike §Problem 6's over-charge, which fails safe).
**No fix is proposed.** Item C is justified independently, on accuracy grounds; that it would
incidentally charge decoder-consumed bytes is noted as a property, not offered as the remediation.
