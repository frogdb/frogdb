# Proposal 55 — `adopt_full_sync()`: one landing sequence for every full-sync payload

Round 38 · lane: replication · effort **S** · LOCKED area (replication, mutation gate 0.85)

## Summary

The replica's two full-sync receive paths — `ReplicaConnection::receive_snapshot`
(`replica/connection.rs:368-419`, live-dataset payload) and
`ReplicaConnection::receive_checkpoint` (`:452-498`, staged-checkpoint payload) — differ
entirely in how they get the payload off the wire and converge on an **identical five-step
ordered landing tail**: record the payload bytes, install, adopt the replication history,
reset the offset, flip to `Streaming`. That order is load-bearing — FM-REPLICATION-001's
Invariant states it in prose ("`install_payload` runs before either is adopted, and an install
failure rewinds to 0 again") — and it is enforced today only by convention, written out twice.
This proposal extracts the tail into one `pub(crate)` method, `adopt_full_sync(payload,
identity)`, so the ordering is written once and lives *behind* an interface rather than in
front of two call sites. The **leverage** is two callers today plus every future payload shape;
the **locality** is that an ordering defect can only be introduced in one place and mutation
testing of the ordering happens once instead of twice.

This is the direct analogue of a chokepoint the spec already blesses one layer up:
FM-REPLICATION-005's Invariant says "A third payload shape inherits all of this by
construction: `payload_reader()` is the only sanctioned way to buffer this socket". The
*reading* side of a new payload shape gets its correctness for free. The *landing* side does
not — a third shape must re-derive the order from a doc comment.

**No live bug was found.** The two copies are behaviourally equivalent, and the one delta that
looks like divergence (`metadata` vs `outcome` as the identity source) is provably the same two
values. There *is* an independently-landable gap in the spec↔test binding — the checkpoint
copy of the ordering has no forcing test tagged to any failure mode — written up as the hotfix
candidate below.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/replication/src/replica/connection.rs` | 1884 | **the change.** `receive_snapshot` `:368-419` (tail `:404-418`), `receive_checkpoint` `:452-498` (tail `:475-497`), `install_payload` `:511-557`, `set_state` `:159-163`, `payload_reader` `:172-174`; tests `:966-1065` (checkpoint landing), `:1182-1294` (snapshot landing), `:1620-1680` (continuity) |
| `frogdb-server/crates/replication/src/replica/mod.rs` | 560 | the only production dispatch to both, `:511-519` (`SyncType::FullSyncCheckpoint` / `FullSyncSnapshot`); `snapshot_installer` slot `:196`, `:264`, `:345-346`, `:499` |
| `frogdb-server/crates/replication/src/fullsync/stager.rs` | 313 | `StagedOutcome` `:48-51`, `CheckpointStager::commit` `:100-163` — the source of the checkpoint path's identity |
| `frogdb-server/crates/replication/src/replica/offset.rs` | 998 | `ReplicaOffset::reset_to` `:498-516`, `#[must_use]`, returns `false` when the stream was retired |
| `frogdb-server/crates/replication/src/fullsync/receiver.rs` | 219 | `receive_checkpoint_files` `:38` — transport **seam** the checkpoint path already delegates to; untouched |
| `.scratch/hardening/specs/replication-failure-modes.md` | 1571 | FM-REPLICATION-001 `:95-122` (the ordering row), -005 `:160-172`, -022 `:505-518`, -063 `:1472-1493` |
| `frogdb-server/crates/replication/src/replica_session.rs` | 4574 | **not touched.** Proposal 53's file (emit side, `stream_checkpoint` `:888`, `stream_live_dataset` `:1018`) and the D2 restructure target |
| `frogdb-server/crates/recovery/src/replication.rs` | ~80 | the *third*, structurally different landing (boot-time staged-metadata adoption, `:42-67`) — out of scope, see Risks |

Verified against the current tree at `main` (`08c143d6`); every line citation above was read,
not inferred. The candidate brief cited FM-REPL-049/050 as the governing rows — that is
**wrong**: the spec's id prefix is `FM-REPLICATION-`, and -049/-050 are the replica-identity
announcement and resync-counter rows, unrelated to this code. The governing rows are
FM-REPLICATION-001 (the ordering), -005 (the reader hand-back), -063 (the byte counters) and,
transitively, -022 (`adopt_replication_history` clears the failover window in the same call).

## Problem

### The tail, twice

`receive_snapshot`, `connection.rs:404-418`:

```rust
self.net_bytes.record_input(metadata.rdb_size);                                  // :404
self.install_payload(FullSyncPayload::LiveDataset(blobs)).await?;                // :406
self.state.write()
    .adopt_replication_history(metadata.replication_id.clone());                 // :409-411
if !self.offsets.reset_to(metadata.replication_offset) {                         // :412
    return Err(io::Error::other("replication stream retired during dataset sync"));
}
self.set_state(ConnectionState::Streaming);                                      // :417
```

`receive_checkpoint`, `connection.rs:475-497` (comments elided):

```rust
self.net_bytes.record_input(metadata.rdb_size);                                  // :475
self.install_payload(FullSyncPayload::StagedCheckpoint(stager.staged_dir())).await?; // :477
self.state.write()
    .adopt_replication_history(outcome.replication_id.clone());                  // :483-485
if !self.offsets.reset_to(outcome.replication_offset) {                          // :486
    return Err(io::Error::other("replication stream retired during checkpoint sync"));
}
self.set_state(ConnectionState::Streaming);                                      // :496
```

Every step is ordered against the next by a correctness argument, none of which is expressible
in the type system as written:

1. `record_input` **after** verification (`:394-399` checksum compare / `:470` `stager.commit`)
   — FM-REPLICATION-063 `:1479`: "each record `metadata.rdb_size` after their checksum/commit
   step confirms it real". Recording a size the payload merely *claimed* is the hardening
   issue-29 shape.
2. `install_payload` **before** either half of the identity — FM-REPLICATION-001 `:101`, "the
   granted *identity* — replid or offset — being adopted before the dataset is installed" is
   listed as NOT observable, because it leaves a node "advertising a history it cannot serve
   and having discarded the failover window".
3. `adopt_replication_history` **before** `reset_to` — the id must be in place before the
   offset that belongs to it becomes visible through the shared atomic that the cluster bus and
   `INFO` read (`connection.rs:120-122`).
4. `reset_to` **before** `set_state(Streaming)`, and its `false` return must abandon the sync —
   `offset.rs:513` marks it `#[must_use = "a refused reset means the stream must abandon this
   sync"]`, which catches a dropped result but not a mis-ordered one.
5. `set_state(Streaming)` **last**, because it is what publishes `link_up = true`
   (`connection.rs:159-163`) and therefore `master_link_status:up`. Anything after it that can
   fail advertises a link before the keyspace is ready.

Nothing in the code couples the two copies. The only coupling is a doc comment: `:463-464`
("Scoped like `receive_snapshot`"), `:474` ("see the matching comment in `receive_snapshot`"),
`:495` ("exactly as in `receive_snapshot`"). Three prose cross-references are the mechanism
holding a five-step correctness order together across two functions.

### The deltas between the copies

Enumerated exhaustively; each classified as *genuine parameter*, *drift surface*, or *bug*.

| # | delta | classification |
|---|---|---|
| D1 | payload variant: `FullSyncPayload::LiveDataset(blobs)` (`:406`) vs `::StagedCheckpoint(stager.staged_dir())` (`:477`) | **genuine parameter** — becomes the one moved argument |
| D2 | identity source: `metadata.replication_id` / `.replication_offset` (`:411`, `:412`) vs `outcome.replication_id` / `.replication_offset` (`:485`, `:486`) | **drift surface, not a behavioural delta.** `StagedOutcome` is built at `stager.rs:159-161` as `{ replication_id: meta.replication_id.clone(), replication_offset: meta.replication_offset }` from the *same* `meta` the caller still holds at `connection.rs:470`. Two names, one pair of values. Nothing today makes them diverge, and nothing today stops a future `commit` from rewriting one and silently changing which path adopts what |
| D3 | error string: `"replication stream retired during dataset sync"` vs `"...during checkpoint sync"` | **genuine parameter** — a per-payload label |
| D4 | `rdb_size` comes from `metadata` on **both** paths (`:404`, `:475`), including the checkpoint path where the id/offset come from `outcome` | **drift surface.** The checkpoint tail therefore reads two structs for three values, with no local statement of why. It is correct — `StagedOutcome` carries no size — but it is exactly the kind of split provenance that a reader "tidies up" wrongly |
| D5 | inline commentary: the checkpoint copy carries three explanatory comments (`:480-482`, `:487-488`, `:493-495`); the snapshot copy carries none | **drift surface** — the "why" is documented on one copy, so an edit made against the other copy is made blind |
| D6 | pre-tail verification: snapshot compares the combined checksum inline (`:394-399`, error `"live-dataset checksum mismatch"`); checkpoint delegates to `stager.commit` (`:470`), which additionally scrubs `incoming` and stamps `replication_metadata.json` with two deliberately non-fatal failure arms (`stager.rs:90-99`) | **out of scope** — sits above the tail, stays where it is |
| D7 | forcing-test tags: the three snapshot landing tests (`:1188`, `:1243`, `:1262`) carry `// FM-REPLICATION-001` and are named in the row's `Forced by` list (spec `:104`); the three checkpoint landing tests (`:971`, `:995`, `:1044`) carry **no FM tag** and appear in no row | **gap — the hotfix candidate**, see below |

No delta is a bug. Specifically traced and cleared:

* **`reset_to` returning `false` after the id was already adopted.** Both copies adopt the
  history, then fail the sync without rewinding. This looks like a missing compensation, but
  `reset_to` returns `false` precisely because a promotion or a newer stream now owns the heads
  (`offset.rs:506-512`), so a compensating `reset_to(0)` would be refused for the same reason —
  which is what `install_payload:550-553` already says in prose ("the heads belong to whoever
  retired it and the rewind is neither possible nor needed"). Identical on both paths, correct
  on both.
* **`snapshot_installer: None` letting the checkpoint path reach `Streaming` with a stale
  keyspace** (`install_payload:512-530`, pinned as intended by
  `receive_checkpoint_adopts_offset_and_streams` `:971`). This is the FM-REPLICATION-001
  `:101` shape, but it is unreachable in production: `set_snapshot_installer` is called
  unconditionally by both wirings (`server/replication_init.rs:237-240` and
  `server/role_manager.rs:674`). Test-only degradation, not a live defect. Worth a one-line
  note *at the code* if it is ever touched, not a hotfix.

## Proposed change

Extract the tail into one method on `ReplicaConnection`, in the same **module**
(`replica/connection.rs`). The **interface** is two arguments; the **implementation** is the
five-step order plus its two failure semantics. The `install_payload` **seam** and the
`payload_reader` **seam** are unchanged — this proposal composes them, it does not re-cut them.

```rust
/// The identity a full-sync payload lands with. Built by each receive path from
/// whatever its own arrival route produced, so the split provenance is stated
/// once at the call site instead of being inferred from two struct names.
pub(crate) struct FullSyncIdentity {
    pub replication_id: String,
    pub replication_offset: u64,
    /// The checksum-verified payload size (FM-REPLICATION-063).
    pub rdb_size: u64,
}

impl ReplicaConnection {
    /// Land a verified full-sync payload. The order below is the whole of
    /// FM-REPLICATION-001's landing invariant and is written *here only*:
    /// count -> install -> adopt id -> adopt offset -> publish the link.
    async fn adopt_full_sync(
        &mut self,
        payload: FullSyncPayload,
        identity: FullSyncIdentity,
    ) -> io::Result<()> {
        let label = payload.sync_label();          // "dataset" | "checkpoint" (adapter, D3)
        self.net_bytes.record_input(identity.rdb_size);
        self.install_payload(payload).await?;
        self.state.write()
            .adopt_replication_history(identity.replication_id);
        if !self.offsets.reset_to(identity.replication_offset) {
            return Err(io::Error::other(format!(
                "replication stream retired during {label} sync"
            )));
        }
        self.set_state(ConnectionState::Streaming);
        Ok(())
    }
}
```

Each caller shrinks to one statement:

```rust
// receive_snapshot
self.adopt_full_sync(
    FullSyncPayload::LiveDataset(blobs),
    FullSyncIdentity {
        replication_id: metadata.replication_id.clone(),
        replication_offset: metadata.replication_offset,
        rdb_size: metadata.rdb_size,
    },
).await

// receive_checkpoint
self.adopt_full_sync(
    FullSyncPayload::StagedCheckpoint(stager.staged_dir()),
    FullSyncIdentity {
        replication_id: outcome.replication_id,
        replication_offset: outcome.replication_offset,
        rdb_size: metadata.rdb_size,          // D4, now visibly deliberate
    },
).await
```

`sync_label()` is a two-arm **adapter** on `FullSyncPayload`, taken before the payload moves;
it reproduces both error strings byte-for-byte, so no assertion changes. `FullSyncIdentity`
is the second adapter: it absorbs D2 and D4 by making each caller state its own provenance
once, at the point where the answer is actually known, instead of leaving `metadata`-vs-
`outcome` to be read off two struct names in the middle of an ordered sequence.

### Why this is depth, not a wrapper

The **depth** test is whether the interface hides an invariant the caller would otherwise have
to know. It does: after the change, the *only* way to obtain `link_up = true` on a full-sync
path is to hand a payload and an identity to `adopt_full_sync`, and the order in which they are
consumed is not the caller's business. A third payload shape — the one FM-REPLICATION-005
`:167` already anticipates for the read side — inherits the landing order for free, exactly as
it already inherits the buffered hand-back from `payload_reader()`. The precedent is the
argument: the codebase has already decided that "a future third payload shape inherits this by
construction" is the right shape for this family, and applied it to one of the two halves.

**Deletion test.** Delete `adopt_full_sync` and the five-step order must be re-written at every
call site — two today, N as payload shapes are added — with nothing but a doc comment to keep
them agreeing. That is precisely the state of the code now, and it is why D2/D4/D5 exist. The
proposal passes.

**Leverage:** 2 production callers (`replica/mod.rs:511-519`), 6 test call sites, and every
future full-sync variant. **Locality:** ordering defects, and the mutants that model them,
concentrate in one 12-line body instead of being spread across two functions 50 lines apart in
a 1884-line file.

## Testability improvement

Today the two copies are covered by two disjoint test families in the same `#[cfg(test)]`
module, and they do not cover the same things:

| assertion | snapshot side | checkpoint side |
|---|---|---|
| install runs before the offset is adopted | `receive_snapshot_installs_the_dataset_before_adopting_offset` `:1190` **(tagged FM-REPLICATION-001)** | `receive_checkpoint_installs_staged_dir_before_adopting_offset` `:995` (untagged) |
| install failure rewinds the offset to 0 | — (covered only indirectly by `receive_snapshot_without_an_installer_fails_the_sync` `:1245`) | `receive_checkpoint_install_failure_rewinds_offset_for_full_resync` `:1044` (untagged) |
| verification failure never reaches the installer | `receive_snapshot_rejects_a_corrupted_dataset` `:1264` **(tagged)** | — (`a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone` `:1521` **(tagged)** dies *before* the tail, so it does not exercise it) |
| id + offset + `Streaming` + `link_up` all land | folded into `:1190` | `receive_checkpoint_adopts_offset_and_streams` `:971` (untagged) |
| the trailing live tail survives the landing | `receive_snapshot_streams_the_frames_that_trailed_the_payload` `:1653` **(tagged FM-REPLICATION-005)** | `receive_checkpoint_streams_the_frames_that_trailed_the_payload` `:1622` **(tagged FM-REPLICATION-005)** |

After extraction the two families keep testing what they own — envelope decode, checksum,
staging, payload kind — and the *ordering* becomes a property of one function that both
families exercise. Concretely:

* **Mutation, once.** A `cargo mutants -p frogdb-replication` run currently produces the
  order-relevant mutants twice (delete the `set_state` call, negate the `reset_to` guard,
  swap the adopt/install statements) and each copy needs its own killer. After extraction there
  is one set, killable by either family. All forcing tests already live **in the mutated crate**
  (`frogdb-replication`'s own `#[cfg(test)] mod tests`), so they count toward the 0.85 gate —
  this proposal neither adds nor removes a cross-crate forcing dependency.
* **One place to force the untested arm.** The `reset_to == false` (stream-retired) branch has
  no direct test on either path today. With one body it is worth exactly one new unit test
  against a retired `ReplicaOffset`, not two; that test then forces the branch for every payload
  shape. Recommended as part of the change.
* **Test-only asymmetries become visible.** `FullSyncIdentity` at the call sites makes D4 a
  reviewable line rather than a thing you notice by diffing two functions.

## Risks / scope boundaries vs siblings

**LOCKED area — replication, gate 0.85, spec-first.** This is a pure extraction with no
behaviour change, which is the one class of edit that does *not* start with a failure-mode row.
Discipline for it:

* `just lint-failure-modes` must stay green: it is bidirectional (`scripts/failure-modes.py`
  `:12-16`) — every `Forced by` name must resolve to a real test carrying the row's tag, and
  every tag must name a row that lists it. No tagged test is renamed or moved by this proposal,
  so the lint is unaffected by the extraction itself.
* `just mutants-diff frogdb-replication` before pushing. Flag for the implementer: the
  extraction *removes duplicated lines*, so the mutant population shrinks. A score is a ratio;
  if it moves at all, run the full `just mutants frogdb-replication` +
  `just mutants-gate frogdb-replication 0.85` rather than reasoning from the diff run.

**Spec edits — expected none; one flagged.** FM-REPLICATION-001's Invariant (`:102`) names
`install_payload`, which survives the change unchanged, so the row's prose stays true verbatim.
FM-REPLICATION-005 and -022 are untouched. The single edit needed is FM-REPLICATION-063's
Invariant (`:1479`), which currently reads "`receive_snapshot` and `receive_checkpoint` each
record `metadata.rdb_size`"; after the change both record it *through* `adopt_full_sync`. Under
the D2 execution discipline ("rows may move file:line citations but not meaning") this is a
citation-only edit. The alternative — leaving `record_input` at each caller and extracting only
steps 2-5 — avoids the edit entirely at the cost of leaving one duplicated step and splitting
the "verify then count" argument away from the sequence it belongs to. **Recommendation: take
the citation edit.**

**vs proposal 53 (fullsync-emitter) — no conflict edge, no order edge.** 53 owns the *emit*
side: `ReplicaSession::stream_checkpoint` (`replica_session.rs:888-1000`) and
`stream_live_dataset` (`:1018-1100`) — the primary writing the envelope. 55 owns the *receive
/landing* side: `ReplicaConnection::receive_snapshot` / `receive_checkpoint` in
`replica/connection.rs`. Different files, opposite ends of the wire, no shared symbol beyond
the `FullSyncMetadata` / `CheckpointStreamCodec` types neither proposal modifies. The only
shared resource is the crate-level gate: both land in `frogdb-replication`, so their mutation
runs and `lint-failure-modes` runs should be sequenced (one gate run after both merge), and
both touch FM-REPLICATION-063's four-output/two-input call-site inventory, so the citation edit
above should be made by whichever lands second.

**vs the D2 restructure (`.scratch/replication-correctness/`) — this is the real conflict
edge.** D2 was ruled 2026-08-10 (PRD `:533-556`): the full restructure of `replica_session.rs`
into an explicit `step(view, event) -> (phase, effects)` state machine is authorized, with tiers
(i) and (ii) to land first as stepping stones. Tier (ii) is "split the PSYNC arm selection out
of `ReplicaConnection::psync`" — `connection.rs:224`, **the same file this proposal edits**.
Nothing textually overlaps (`psync` at `:224` vs the two `receive_*` at `:368`/`:452`), but
they are same-file and same-locked-crate:

* Serialize with D2 tier (ii), or land 55 first — 55 is smaller, self-contained, and does not
  move any tagged test, so it is the cheaper thing to put in front.
* 55 is *aligned* with D2, not competing: D2's stated need is "a pure decision function" for
  model-checking, and `adopt_full_sync` is exactly the kind of extraction that makes the
  landing an effect the interpreter can emit rather than an inline sequence.
* If D2's tier (iii) lands first, re-verify the citations — the PRD requires the full 0.85 gate
  re-run after (iii) "because the restructure moves most forcing-test targets".

**Out of scope: the third landing.** `frogdb-recovery/src/replication.rs:42-67` performs the
boot-time equivalent (adopt staged metadata over the loaded state, persist, consume the staging
file). It is structurally different — no `ReplicaOffset` atomic, no `ConnectionState`, no
installer, and it routes through `ReplicationState::apply_staged_metadata`, which
FM-REPLICATION-022 `:1544` already pins as sharing `adopt_replication_history` with the live
path. It lives in a different (persistence-gated) crate and must not be folded in.

**Residual risk:** low. The change is mechanical, both error strings are reproduced exactly,
and every existing assertion — including the `#[must_use]` on `reset_to` and the `link_up`
lockstep in `set_state` — survives unedited. The one thing to watch in review is that
`sync_label()` is read *before* `payload` moves into `install_payload`.

## Effort

**S.** One new 12-line method, one 3-field struct, one two-arm adapter, two call sites
rewritten, one recommended new unit test for the stream-retired branch, one citation-only spec
edit. No test edits required; no behaviour change; no wire or config change.

## Independently-landable hotfix — the checkpoint landing has no forcing test

Not a code defect: a hole in the spec↔test binding, landable now and worth landing **before**
the extraction.

**Evidence.** FM-REPLICATION-001 is the row that owns the landing ordering ("the granted
identity ... being adopted before the dataset is installed" is its NOT-observable, spec `:101`;
"`install_payload` runs before either is adopted" is its Invariant, `:102`). Its `Forced by`
list (`:104`) names three connection-level tests: `receive_snapshot_installs_the_dataset_
before_adopting_offset`, `receive_snapshot_without_an_installer_fails_the_sync`,
`receive_snapshot_rejects_a_corrupted_dataset` — all three on the **snapshot** path — plus
`a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone`, which fails at
`connection.rs:1562` on a truncated transfer and therefore never reaches the landing tail at
all (asserted at `:1566`, `ErrorKind::UnexpectedEof`).

The tests that *do* force the checkpoint landing order —
`receive_checkpoint_adopts_offset_and_streams` (`:971`),
`receive_checkpoint_installs_staged_dir_before_adopting_offset` (`:995`, which asserts the
exact install-before-adopt pairing at `:1027-1031`), and
`receive_checkpoint_install_failure_rewinds_offset_for_full_resync` (`:1044`) — carry no
`// FM-REPLICATION-NNN` tag. Under `scripts/failure-modes.py`'s bidirectional rule an untagged
test is invisible to the gate: the checkpoint half of a LOCKED ordering invariant is currently
forced by nothing the spec knows about.

**Fix.** Add `// FM-REPLICATION-001` above the three tests and add their names to the row's
`Forced by` cell. Purely additive — it strengthens a row, changes no meaning, and is exactly
the kind of edit the D2 discipline permits. Zero code change; `just lint-failure-modes` verifies
it. Landing it first also gives the extraction a spec-forced target on *both* sides, so the
`mutants-diff` run afterwards is measuring something real.
