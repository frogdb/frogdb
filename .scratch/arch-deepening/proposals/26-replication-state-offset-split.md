# Proposal 26 — Split `ReplicationState.replication_offset`: persisted identity vs. live counter

## Summary

`ReplicationState.replication_offset` (`replication/src/state.rs:110-112`) is one `u64` field that
means **two opposite things depending on the node's role**. On a **Primary** it is the
*persisted / reconciled* offset — a save-point checkpoint that deliberately **lags** the live stream
head, because the live write position lives in `OffsetCoordinator.live`
(`offset_coordinator.rs:48-60`), advanced by the broadcast path
(`offset_coordinator.rs:95-98`) and reconciled into the field only at save points
(`offset_coordinator.rs:164-171`). On a **Replica** the *same field* is the **live applied truth**,
advanced per ingested frame by `increment_offset` (`state.rs:290-294`, sole non-test caller
`replica/streaming.rs:37`), because the Replica has no `OffsetCoordinator` — "that type is
primary-only" (`offset_coordinator.rs:100-109`).

The two meanings are held apart only by prose and convention. The single sharpest piece of evidence
is that `window_contains` (`state.rs:267-288`) is forced to take `current_offset` as a **parameter**
precisely so it never reads `self.replication_offset`, whose own doc says reading it on the Primary
"made every reconnect fall outside the window and forced a full resync" (`state.rs:251-262`). The
field is designed to be un-read for its live meaning on one role while being the live meaning on the
other. This proposal splits the durable identity/offset (renamed `offset_at_save`, no live mutator)
from a role-symmetric **live-offset owner** on the Replica, so a reader can no longer treat the
persisted value as live — the invariant "persisted lags live; the window check never reads
persisted" becomes type-level rather than convention. It **pairs with proposal 25** (role-specific
live-offset owners on both sides); see [Related](#related).

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current design |
| --- | --- | --- |
| `frogdb-server/crates/replication/src/state.rs` | 575 | `ReplicationState` struct incl. `replication_offset` (L110-112); `window_contains` (L267-288) with the "never reads `self.replication_offset`" contract (L251-262); `increment_offset` (L290-294); `new_replication_id` (L236-249) freezes `secondary_offset` from the field; `apply_staged_metadata` (L312-315) |
| `frogdb-server/crates/replication/src/offset_coordinator.rs` | 376 | Primary's live-offset owner. `live` atomic (L48-60); `advance` gate (L95-98); `current()` live read (L113-115); `reconcile_for_persist` writes the field at save points (L164-171); `can_serve_partial_sync` threads live into `window_contains` (L153-157) |
| `frogdb-server/crates/replication/src/replica/streaming.rs` | 136 | Replica ingest: `increment_offset(frame_advance)` (L37), reads `state.replication_offset` for the ACK (L38, L56). The live truth on the Replica |
| `frogdb-server/crates/replication/src/replica/connection.rs` | — | Replica PSYNC start reads the field (L141-148); FULLRESYNC overwrites it (L164-170); checkpoint staging overwrites it (L312-315) |
| `frogdb-server/crates/replication/src/primary/mod.rs` | — | Handler docs the contract: broadcast advances the live offset "not `state.replication_offset`" (L75-79, L134-144); `save_state` → `reconcile_for_persist` (L145-148) |
| `frogdb-server/crates/server/src/server/replication_init.rs` | — | Boot seam: `tracker.set_offset(repl_state.replication_offset)` seeds the Primary's live atomic from the persisted field (L72) |
| `frogdb-server/crates/server/src/recovery/replication.rs` | — | `apply_staged_metadata` on recovery (L49) — the persisted offset adopted from a staged checkpoint |
| `frogdb-server/crates/replication/src/tracker.rs` | — | `offset_handle()` (L74) / `set_offset()` (L145) — the atomic the coordinator adopts as `live` |
| `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` | 16 | The data path never goes through Raft; replication offsets are a data-plane concern. Untouched by this proposal. |

## Problem (concrete verified evidence)

### One field, two contradictory contracts

`ReplicationState.replication_offset` is declared once (`state.rs:110-112`):

```rust
/// Current replication offset.
/// This is the byte offset in the replication stream.
pub replication_offset: u64,
```

**On a Primary, this field is NOT the live stream position — and reading it as such is a documented
bug.** The broadcast path advances only `OffsetCoordinator.live`; the field is written *only* by
`reconcile_for_persist` at save points, and it is monotone-guarded so a save never rewinds it
(`offset_coordinator.rs:164-171`):

```rust
pub async fn reconcile_for_persist(&self) -> ReplicationState {
    let offset = self.current();                 // live head, from the coordinator
    let mut state = self.state.write().await;
    if offset > state.replication_offset {       // persisted lags; never moves back
        state.replication_offset = offset;
    }
    state.clone()
}
```

`primary/mod.rs:134-144` states it directly: "the broadcast path advances only the live offset, not
`state.replication_offset`… coupling offset durability to explicit save points."

**On a Replica, the same field IS the live applied truth**, advanced per frame
(`replica/streaming.rs:37`, the sole non-test caller of `increment_offset`):

```rust
state.increment_offset(OffsetCoordinator::frame_advance(&frame));
let offset = state.replication_offset;           // read back immediately as "live"
```

and read back one line later as the value to ACK. There is no `OffsetCoordinator` on the Replica;
`frame_advance` exists as a free function precisely because "the replica has no `OffsetCoordinator`
— that type is primary-only" (`offset_coordinator.rs:100-109`).

### The smoking gun: `window_contains` takes `current_offset` to avoid reading its own field

`window_contains` (`state.rs:267-288`) is a method on `ReplicationState` that must **not** read the
`ReplicationState` field of the same name. Its contract (`state.rs:251-262`):

> `current_offset` is the primary's **live** replication offset… this method never reads
> `self.replication_offset` (the persisted field lags the live stream head, so checking against it
> made every reconnect fall outside the window and forced a full resync).

So the interface is shaped around the hazard: the one live-vs-persisted question the type could
answer wrong is answered by threading the live value in from the coordinator
(`offset_coordinator.rs:153-157`) instead. The field is a trap the method routes around by hand.

### `new_replication_id` freezes the failover boundary from `self.replication_offset`

`new_replication_id` (`state.rs:236-249`) reads `self.replication_offset` to set the failover
continuity boundary:

```rust
self.secondary_id = Some(self.replication_id.clone());
self.secondary_offset = self.replication_offset as i64;   // frozen from the field
```

**This is correct today**, and it is important to be precise about *why* — the earlier draft of
this proposal overstated it as a pre-existing "landmine," which the adversarial review correctly
refuted. `new_replication_id` is a *becoming-primary* operation, and its only realistic caller is
**replica → primary promotion** (or a fresh-boot primary at offset 0). On a Replica the field IS the
live applied offset (`increment_offset` advances it per frame), so freezing `secondary_offset` from
it captures exactly the live position — correct. The "persisted lags live" condition only holds on a
node that is *already* a running Primary, which does not call `new_replication_id` (a running
primary does not re-become one). So there is **no live bug and no latent landmine in today's code**;
every caller is in fact a test (`state.rs:432/465`, `offset_coordinator.rs:346`,
`primary/replay.rs:316`).

The relevance to *this* proposal is the opposite direction: **the redesign is what introduces the
lag on the Replica side.** Once the live offset moves into `ReplicaOffset` and the renamed
`offset_at_save` only advances at save points (steps 3–6), `new_replication_id` reading
`offset_at_save` *would* freeze the failover boundary behind the live head. So growing its signature
to take the live offset (`new_replication_id(live_offset)`) is **correctness-preservation for the
redesign**, not the defusal of an existing hazard — it keeps the promotion path correct *after* the
field stops being live. This distinction also means step 2 is not independently valuable on its own
(see the corrected [migration ordering](#migration-plan-ordered-steps)); it earns its place only as
part of the full split.

### The seams that already treat the two meanings differently

- **Boot (Primary):** `replication_init.rs:72` seeds the live atomic *from* the persisted field
  (`tracker.set_offset(repl_state.replication_offset)`) — persisted → live, once, at startup.
- **Boot / recovery (Replica):** `apply_staged_metadata` (`state.rs:312-315`, called from
  `recovery/replication.rs:49`) writes the persisted field from staged checkpoint metadata; the
  Replica then treats it as live via `increment_offset` from there.
- **FULLRESYNC / checkpoint (Replica):** `connection.rs:164-170` and `:312-315` overwrite the field
  as a fresh live position.

Every one of these is correct *today*, but only because each author knew which meaning applied at
that site. Nothing in the type stops a future caller from reading the persisted value as live (as
`window_contains` explicitly had to be taught not to).

## Why it is shallow/fragmented (architecture vocabulary)

**The field has poor Locality of meaning: its semantics are defined at the call site, not the
declaration.** A `u64` named `replication_offset` on a shared struct says "the offset." Whether that
is *live* or *lagging-persisted* is decided by which role holds the struct and which method touches
it — knowledge that lives in `offset_coordinator.rs`'s module doc, `primary/mod.rs`'s field doc, and
`window_contains`'s method doc, none of them at the declaration. Ousterhout's **deep module** test
is inverted here: the interface (a bare field + `increment_offset`) is *narrower* than the
implementation's real contract, so the contract leaks into prose and into a parameter workaround.

**The Seam is asymmetric by omission.** The Primary got a proper live-offset owner
(`OffsetCoordinator`) that pulls "which home answers which question" behind one interface — its own
module doc (`offset_coordinator.rs:1-19`) is a manifesto for exactly this. The Replica has the *raw
materials* of the same design but no owner **type**: it already keeps a `shared_offset:
Arc<AtomicU64>` in lockstep with the field (`streaming.rs` stores it after every `increment_offset`;
`connection.rs:168/314` on resync; vended via `replica/mod.rs:124-133`), read by the cluster bus
exactly like the Primary's `shared_offset()`. So the live atomic and its lockstep maintenance
**already exist** — the asymmetry is precisely that no module *owns* them: the mutation of the field
and the mirroring into the atomic are two hand-coupled statements at each ingest/resync site rather
than one method behind an interface. That is a **shallow adapter** on the Replica: the mediation
between live and persisted is spread across call sites by convention instead of behind a type, so
nothing enforces that the atomic and the field stay in step or that a reader picks the right one.

**Deletion test.** Rename `replication_offset` to `offset_at_save` and delete `increment_offset`
from `ReplicationState`, and the Primary path is unaffected (it never advanced the field). Only the
Replica breaks — at exactly the three sites (`streaming.rs:37-38,56`, `connection.rs`) that
currently treat a persisted field as a live counter. The compiler enumerates the confusion for you.
That is the signature of a missing seam: the thing that breaks is precisely the thing that was wrong.

## Proposed design (Rust interface sketch — signatures/types only)

Two coordinated moves. (1) Make `ReplicationState` model *only* durable identity + the save-point
offset, with the field renamed so no reader can mistake it for live and with the live mutator
removed. (2) Give the Replica a live-offset owner symmetric to the Primary's `OffsetCoordinator`,
so both roles read "where is the stream now" from a dedicated owner and reconcile into the persisted
field at save points. Move (2) is the shared surface with proposal 25 — see [Related](#related).

### 1. `ReplicationState` becomes persisted identity only

```rust
/// Durable replication identity plus the offset AT THE LAST SAVE POINT.
/// This is NOT a live stream position on either role:
///   - on a Primary the live head lives in `OffsetCoordinator`;
///   - on a Replica the live head lives in `ReplicaOffset` (new below).
/// It is reconciled up to the live head only at save points, and it lags.
pub struct ReplicationState {
    pub replication_id: String,
    pub secondary_id: Option<String>,

    /// Offset reconciled at the last save point. Renamed from
    /// `replication_offset` so no reader mistakes it for the live head.
    pub offset_at_save: u64,

    pub secondary_offset: i64,
    pub active_version: Option<String>,

    #[serde(skip)] pub master_host: Option<String>,
    #[serde(skip)] pub master_port: Option<u16>,
}

impl ReplicationState {
    // REMOVED: pub fn increment_offset(&mut self, bytes: u64)
    //   (a live mutation that does not belong on the persisted identity)

    // window_contains keeps taking the live head as a parameter — now correct
    // *by construction*, because the field it might have read is named
    // `offset_at_save` and carries no pretense of being live.
    pub fn window_contains(&self, requested_id: &str,
                           requested_offset: u64, current_offset: u64) -> bool;

    // apply_staged_metadata still writes `offset_at_save` — a staged checkpoint
    // offset *is* a save-point offset, so this is the correct target.
    pub fn apply_staged_metadata(&mut self, meta: &StagedReplicationMetadata);

    // new_replication_id must freeze `secondary_offset` from the LIVE head, not
    // the persisted field. Its signature grows a live-offset argument so the
    // failover boundary can never be frozen behind the stream (see hazard above):
    pub fn new_replication_id(&mut self, live_offset: u64);
}
```

Persisted serde field name changes; because `#[serde(default)]` is not on it, add a one-release
`#[serde(alias = "replication_offset")]` so existing on-disk `replication_state.json` still loads
(FrogDB is pre-production, but a boot-time rename is free insurance). `StagedReplicationMetadata`'s
own `replication_offset` field (`state.rs:35-43`) is the *wire/staging* contract and stays as-is;
only the in-memory struct field is renamed.

### 2. `ReplicaOffset` — the Replica's live-offset owner (symmetric to `OffsetCoordinator.live`)

```rust
/// The Replica-side live applied offset. Mirror of the Primary's
/// `OffsetCoordinator.live`: the single home of "how far this Replica has
/// applied", advanced per ingested frame, reconciled into `offset_at_save`
/// at save points, and the single vendor of the shared handle the cluster bus
/// (HealthProbe) / INFO read.
pub struct ReplicaOffset {
    live: Arc<AtomicU64>,
    state: Arc<RwLock<ReplicationState>>,
}

impl ReplicaOffset {
    /// Adopt the Replica's EXISTING `shared_offset` atomic (the one already kept
    /// in lockstep with the field and read by the cluster bus / INFO) as `live`,
    /// seeded from the persisted `offset_at_save` at boot / after a resync. The
    /// owner does not mint a second atomic — it takes over maintenance of the one
    /// the scattered call sites update today, so the handle identity is unchanged.
    pub fn new(state: Arc<RwLock<ReplicationState>>,
               shared: Arc<AtomicU64>, seed: u64) -> Self;

    /// Advance by one ingested frame's payload unit — the SAME unit as the
    /// Primary's advance (reuses `OffsetCoordinator::frame_advance`), so a
    /// Replica ACK stays directly comparable to the Primary's live head.
    /// Returns the new live offset (what streaming.rs ACKs).
    pub fn frame_advance(&self, frame: &ReplicationFrame) -> u64;

    /// The live applied position. Replaces `state.replication_offset` reads on
    /// the Replica ingest / ACK path.
    pub fn current(&self) -> u64;

    /// The cluster-bus / INFO handle, vended by the owner of the atomic.
    pub fn shared_offset(&self) -> Arc<AtomicU64>;

    /// Adopt a fresh stream position on FULLRESYNC / staged-checkpoint install.
    /// Replaces the direct `state.replication_offset = new_offset` writes in
    /// `replica/connection.rs`.
    pub fn reset_to(&self, offset: u64);

    /// Reconcile `offset_at_save` up to the live head for persistence —
    /// monotone-guarded, symmetric to `OffsetCoordinator::reconcile_for_persist`.
    pub async fn reconcile_for_persist(&self) -> ReplicationState;
}
```

With this, `replica/streaming.rs:37-38` becomes `let offset = self.offsets.frame_advance(&frame);`
(no `state.write()` in the hot path for the offset), and the two ACK reads (`:38`, `:56`) become
`self.offsets.current()`. `replica/connection.rs`'s FULLRESYNC/checkpoint writes become
`reset_to(...)`. The Replica now mirrors the Primary: a live owner in front, the persisted field
behind, reconciled at save points.

### Alternative: one role-generic owner instead of two types

Rather than a distinct `ReplicaOffset`, make `OffsetCoordinator` role-generic (an enum-free split of
its Primary-only methods — `advance`, `min_acked`, `can_serve_partial_sync` stay Primary; a shared
`current`/`shared_offset`/`reconcile_for_persist` core serves both). This is the larger unification
that **proposal 25** owns; see [Related](#related) for the subsumption call. This proposal's minimum
is the struct rename + `increment_offset` removal + a Replica live owner (whatever its final type).

## Migration plan (ordered steps)

1. **Rename the field** `replication_offset` → `offset_at_save` in `ReplicationState`
   (`state.rs:110-112`) with `#[serde(alias = "replication_offset")]`. Mechanical `sed` across the
   crate for the field path; the compiler then flags every reader. Update the three internal readers
   that are genuinely persisted-offset reads (`new_replication_id` L239, `save`/`load` logs L163/212,
   `reconcile_for_persist` `offset_coordinator.rs:167-168`, `apply_staged_metadata` L314).
2. **Change `new_replication_id`** to take `live_offset: u64` and freeze `secondary_offset` from it.
   Update the (test-only) callers to pass the coordinator's / `ReplicaOffset`'s `current()`. This is
   **correctness-preservation** for the redesign, not a standalone fix: once step 5 stops the field
   advancing on the Replica, reading `offset_at_save` here would freeze the failover boundary behind
   the live head, so this step must land *with* steps 3–6 (it is a no-op against today's still-live
   field — see the ordering note below).
3. **Introduce `ReplicaOffset`** (new module `replication/src/replica/offset.rs`), reusing
   `OffsetCoordinator::frame_advance` for the unit so the two ends cannot drift. Construct it in the
   Replica init path seeded from `offset_at_save`. It **adopts the existing** `shared_offset:
   Arc<AtomicU64>` as its `live` atomic (the replica already keeps one in lockstep with the field —
   see [asymmetric seam](#why-it-is-shallowfragmented-architecture-vocabulary)), so the cluster-bus /
   INFO handle keeps the same identity; the change is that the owner now maintains it, not the
   scattered call sites.
4. **Rewire every Replica read/write of the field onto `ReplicaOffset`.** This is the load-bearing
   step and it covers **all** sites, including the reconnect read the earlier draft omitted:
   - **Ingest / ACK** (`replica/streaming.rs:37-38,56`): `increment_offset` → `frame_advance`; the
     two `state.replication_offset` ACK reads → `current()`.
   - **PSYNC reconnect offset** (`replica/connection.rs:141-148`): `psync()` reads the field to build
     the offset it sends in the reconnect `PSYNC <id> <offset>` request. This **must** be rewired to
     `ReplicaOffset::current()`. *Correctness:* after this redesign `offset_at_save` lags the live
     applied head between save points (step 6), so a reconnect that read `offset_at_save` would ask
     the primary to resume from **behind** where the replica has actually applied — re-receiving
     already-applied data, or forcing an unnecessary full resync if the primary has trimmed that
     range from its backlog. Today this read is correct only *because* the field is the live head;
     the moment the field stops being live, this site is a silent regression unless moved to
     `current()`. (Both the `?/-1` "never synced" branch and the resume branch key off `current()`.)
   - **Resync installs** (`replica/connection.rs:164-170,312-315`): FULLRESYNC and staged-checkpoint
     writes → `reset_to(...)`, which also updates the adopted atomic in one place.
5. **Delete `increment_offset`** from `ReplicationState` once step 4 leaves no live caller. Port
   `test_increment_offset` (`state.rs:558-573`) to `ReplicaOffset::frame_advance`.
6. **Reconcile `offset_at_save` at Replica save points from `ReplicaOffset`.** Today the Replica
   persists the live applied offset *directly* (`save_state`, `replica/mod.rs:114-117`, whose doc
   says "the in-memory state is the source of truth here") — a deliberate *persist-what-you-applied*
   semantic that is simpler than and different from the Primary's save-point-lag model. This proposal
   **preserves that semantic**: because the live offset now lives in `ReplicaOffset`'s atomic rather
   than the struct field, `save_state` must snapshot the live value into `offset_at_save` before
   writing. `ReplicaOffset::reconcile_for_persist` is just the *mechanism* for that snapshot
   (monotone-guarded), and its **net persisted value is identical to today's** (the live applied
   offset at save time). The framing is deliberately *not* "adopt the Primary's lag for symmetry" —
   the reconcile here exists only because the live counter moved out of the struct, and it is the
   reason step 4 must route live readers (esp. `psync()`) to `current()` rather than the field.
7. **`just check replication` → `just check` → `just test replication`.** Run whole-suite build/test
   remotely per the testbox policy.

**Ordering.** Contrary to the earlier draft, steps 1–2 are **not** independently valuable: the rename
(1) is cosmetic on its own and `new_replication_id(live)` (2) is a no-op while `increment_offset`
still keeps the field live (steps 3–5 have not run). The whole set 1–6 is one coherent change and
should land together (co-landing with, or folded into, proposal 25). If a partial landing is desired
for review size, steps 1 + 3 + 4 (the rename plus the `ReplicaOffset` owner plus the full rewire,
**including `psync()`**) are the atomic correctness unit; 2, 5, 6 are cleanups that follow.

## Test plan

- **Rename does not change persisted format** — load a fixture `replication_state.json` written with
  the old `replication_offset` key and assert it deserializes into `offset_at_save` (guards the
  `serde(alias)`).
- **Persisted lags live is now type-visible** — port `reconcile_for_persist_is_monotonic`
  (`offset_coordinator.rs:357-374`) unchanged; add a Replica twin on `ReplicaOffset`:
  `frame_advance` advances live, `offset_at_save` stays put until `reconcile_for_persist`.
- **`window_contains` never reads persisted** — keep `test_window_contains` (`state.rs:442-472`) and
  `can_serve_partial_sync_uses_its_own_live_offset` (`offset_coordinator.rs:317-334`); after the
  rename the test's "leave `offset_at_save` at default to prove the check ignores it" comment becomes
  literally enforced by the field name.
- **Failover boundary frozen from live, not `offset_at_save`** (correctness *after* the field stops
  being live) — a new unit test: advance a live offset to N while `offset_at_save` still reads M < N,
  call `new_replication_id(live=N)`, assert `secondary_offset == N`. Under the new signature this is
  *impossible to state wrong*; it guards the redesign, which is what introduces the M < N gap on the
  Replica (today the field would already read N, so the test only becomes load-bearing post-split).
- **Reconnect PSYNC offset equals live applied, not `offset_at_save`** — the site the earlier draft
  omitted. Advance `ReplicaOffset` to N, leave `offset_at_save` at a save-point M < N, and assert the
  offset `psync()` places in its `PSYNC <id> <offset>` request is N (`current()`), not M. This test
  fails against a mechanical rename that leaves `psync()` reading the field, catching the
  partial-resync regression called out in [Risks](#risks--alternatives).
- **Replica ACK equals live applied** — drive `ReplicaOffset::frame_advance` over a frame sequence
  and assert `current()` equals the summed payload units and matches what `send_ack` would send;
  reuses the payload-unit fixtures from `offset_coordinator.rs` tests.
- **Resync resets live** — `reset_to(offset)` after a simulated FULLRESYNC makes `current()` the new
  offset (and updates the adopted `shared_offset` atomic), leaving `offset_at_save` to be reconciled,
  mirroring `connection.rs` today.
- **Replica persists what it applied** — after `frame_advance` to N and a `save_state`, assert the
  persisted `offset_at_save` equals N (the live applied value), confirming step 6 preserves today's
  persist-what-you-applied semantic rather than introducing a persisted-lag.
- **Integration:** the existing `integration_replication.rs:4161` scenario (the comment already
  says INFO reports the live position "not the stale persisted `state.replication_offset`") should
  pass unchanged — it is the end-to-end proof the split preserves observable behavior.

## Risks & alternatives

- **Pairs with proposal 25 — decide subsumption before splitting the type.** 25 (role-specific
  live-offset owners on both sides) and 26 (persisted-vs-live struct split) attack the same defect
  from two angles. Recommended framing: **26 owns the struct-level type boundary** (rename,
  `increment_offset` removal, `new_replication_id(live)` signature); **25 owns the live-owner
  symmetry** (`ReplicaOffset` / role-generic `OffsetCoordinator`). Neither fully subsumes the other:
  26 alone (rename + remove mutator) still leaves the Replica writing `offset_at_save` directly as if
  live unless 25's owner exists; 25 alone builds the owner but leaves the field misleadingly named.
  **They should land together as one coherent change** (the rename, the `ReplicaOffset` owner, and
  the full read/write rewire — including `psync()` — are interdependent; see the corrected
  [ordering note](#migration-plan-ordered-steps)). If the two are merged into one work item, 26 is
  the naming/contract half of that item.
- **Serde field rename on a persisted file.** Mitigated by `#[serde(alias)]` for one release. FrogDB
  is pre-production (breaking changes acceptable per project policy), so the alias is courtesy, not a
  requirement; it can be dropped once no old state files exist.
- **`StagedReplicationMetadata` keeps `replication_offset`.** Its field is a staging/wire contract
  read by `frogdb-persistence::rocks::staged` (`state.rs:25-43`) and by fullsync
  (`fullsync.rs:55,594,750`); renaming it would ripple across a crate boundary for no gain. Only the
  in-memory `ReplicationState` field is renamed — verified the two are distinct types.
- **Hot-path lock change on the Replica.** Today `streaming.rs:32` takes `state.write().await` per
  frame partly to bump the offset. Moving the offset to an atomic in `ReplicaOffset` *removes* a
  write-lock acquisition from the ingest hot path (net win), but the surrounding `state` read for
  `replication_id` etc. must be re-audited so no ordering the ACK relies on is lost. Covered by the
  integration test above.
- **Crate-dependency direction respected.** `ReplicaOffset` lives in `replication`, over the same
  `Arc<RwLock<ReplicationState>>` + `Arc<AtomicU64>` primitives `OffsetCoordinator` already uses; no
  new dependency on `core` internals, and nothing in `persistence`/`cluster` changes. The cluster
  bus keeps reading a `shared_offset()` handle — now vended by the Replica's owner instead of the
  raw field path, matching how the Primary already vends it (`replication_init.rs:96-98`).
- **ADR-0001 untouched.** Replication offsets are a data-plane quantity; the Raft Metadata Plane owns
  only slot/role/Config-Epoch state and never the offset. This is a pure data-plane refactor.
- **Reconnect-offset regression is the main new hazard this refactor introduces.** Moving the live
  offset out of the struct means `offset_at_save` lags the applied head between save points on the
  Replica too. Any live reader left on the field silently regresses; the highest-stakes one is
  `psync()`'s reconnect-offset read (`connection.rs:141-148`), which drives partial-resync
  correctness. Migration step 4 now rewires it explicitly, and a test asserts the reconnect offset
  equals `current()`, not `offset_at_save`, while the two diverge. This is the single site most
  likely to be missed by a mechanical rename, so it is called out separately from the ingest path.
- **Do-nothing alternative.** Leave the field dual-meaning and rely on the three doc comments +
  `window_contains`'s parameter workaround. Rejected on **clarity/maintainability** grounds, not on a
  claimed pre-existing bug: the field's meaning is decided at the call site, so every new offset
  reader must re-learn "which meaning here" from prose, and the next reader taught to read the field
  as live (as `window_contains` had to be taught *not* to) reintroduces the exact partial-resync
  class of bug by hand. There is no live bug in today's code (the `new_replication_id` framing above
  is corrected to reflect this); the case for the split is preventing future ones and removing the
  convention-only mediation, not defusing an existing landmine.

## Effort

**M.** The rename (step 1) is S and compiler-driven. The `M` is the
`ReplicaOffset` owner and rewiring **every** Replica read/write off the raw field — ingest/ACK, the
`psync()` reconnect read, and the resync installs (`streaming.rs`, `connection.rs`) — plus the
save-point reconcile — new code, but small, single-crate
(`replication`), no async/ordering redesign, and it reuses the Primary's `frame_advance` unit and
`reconcile_for_persist` shape. Not L: no cross-crate signature churn (the cluster-bus/INFO handles
keep their `Arc<AtomicU64>` shape) and the compiler enumerates every site the rename touches. If
co-landed with proposal 25 the `ReplicaOffset` work is shared and 26's incremental cost drops to S.

## Related

- **Proposal 25 — role-specific live-offset owners on both sides.** The direct pair (see
  [Risks](#risks--alternatives) for the subsumption call). 26 provides the type/naming boundary that
  makes 25's second owner unmistakable-for-persisted; 25 provides the owner 26's Replica half needs.
  Recommendation: land together as one interdependent change; if merged, treat 26 as the contract
  half.
- **Proposal 14 — internal-removal write effects / replay availability.** `can_serve_partial_sync`
  (`offset_coordinator.rs:150-157`) notes replay availability is gated separately (14); this
  proposal does not touch that lower bound, only the offset-window upper bound's provenance.
- **`OffsetCoordinator` (prior round).** The Primary-side precedent this proposal makes symmetric on
  the Replica; its module doc (`offset_coordinator.rs:1-19`) already frames "which home answers which
  question" as the abstraction to build — 26 finishes that job on the Replica.

## Adversarial review

**Verdict: AMEND.** The reviewer accepted the core premise as sound and well-evidenced (the field
genuinely means two opposite things by role; `window_contains` is verifiably forced to take
`current_offset` as a parameter to avoid reading its own field; the rename + `increment_offset`
removal is a legitimate clarity refactor; the hot-path write-lock removal is a real minor win; all
call-site facts check out). Three supporting arguments were over-stated or under-specified and one
prose claim was imprecise. All four are resolved below.

- **[major] Step 4 omitted rewiring the `psync()` reconnect-offset read (`connection.rs:141-148`).**
  Confirmed against source: `psync()` reads `state.replication_offset` to build the offset in its
  reconnect `PSYNC <id> <offset>` request. Today that is correct only because the field is the live
  applied head; once `offset_at_save` lags (step 6), a reconnect would request resume from behind
  where the replica actually applied — re-receiving applied data or forcing a needless full resync if
  the primary trimmed its backlog. **Resolved:** migration step 4 now enumerates the `psync()` read
  explicitly as a mandatory rewire to `ReplicaOffset::current()`, with the correctness rationale; a
  new test-plan item asserts the reconnect offset equals `current()` (not `offset_at_save`) while the
  two diverge; and a dedicated risk bullet flags it as the site most likely missed by a mechanical
  rename.

- **[major] The `new_replication_id` "latent hazard / landmine" framing was circular.** Confirmed:
  `new_replication_id` is a becoming-primary op whose realistic caller is replica→primary promotion,
  and on a Replica the field IS the live applied offset (`increment_offset`), so freezing
  `secondary_offset` from it is correct *today*. The lag it "defuses" is created by *this* redesign,
  and landing only steps 1–2 changes nothing because `increment_offset` still keeps the field live
  until step 5. **Resolved:** the section is rewritten to state there is no live bug/landmine in
  current code; the `new_replication_id(live)` signature is reframed as correctness-preservation
  *for the redesign*; step 2 is explicitly marked a no-op on its own; the "steps 1–2 independently
  landable / immediately delete the hazard" claims are removed from the migration ordering, the
  proposal-25 pairing, the Effort section, and the Related section; and the do-nothing alternative is
  re-justified on clarity/maintainability grounds rather than a claimed pre-existing bug.

- **[major] Step 6 grafted the Primary's save-point-lag model onto the Replica under a "symmetry"
  banner without independent justification.** Confirmed: today the Replica persists exactly what it
  applied (`save_state`, `replica/mod.rs:114-117`, doc: "the in-memory state is the source of truth
  here"), a simpler and already-correct semantic. **Resolved:** step 6 is rewritten to *preserve*
  persist-what-you-applied — its net persisted value is identical to today's (live applied offset at
  save time); `reconcile_for_persist` is presented only as the mechanism for snapshotting the live
  atomic into `offset_at_save` now that the live counter has moved out of the struct, explicitly
  *not* as adopting the Primary's lag. A test-plan item ("Replica persists what it applied") pins the
  preserved semantic.

- **[minor] "the raw persisted struct IS the live counter" / "no module mediates" overstated the
  asymmetry.** Confirmed: the Replica already keeps a `shared_offset: Arc<AtomicU64>` in lockstep
  with the field (updated after each `increment_offset` / resync, vended to the cluster bus). The
  atomic and its maintenance exist; only the owner *type* is missing. **Resolved:** the "asymmetric
  seam" prose is corrected to say the raw materials already exist and the defect is the absence of an
  owner (mutation + mirror are two hand-coupled statements per site); the `ReplicaOffset::new`
  sketch and migration step 3 now state it *adopts* the existing atomic rather than minting a new
  one, keeping the handle identity stable.

No claim was disputed; the central Locality-of-meaning defect stands and the amendments tighten the
supporting arguments and close the one real correctness gap (the `psync()` rewire).
