# Distributed-systems design review — FrogDB replication spec + 2026-08-13 rulings

Reviewer lens: wall-clock as correctness input; PSYNC2/raft-log identity semantics; ack/durability
honesty; split-brain; edge-triggered recovery; session identity; comparison against
Redis/Valkey PSYNC2, Kafka ISR/high-watermark, etcd/raft, CockroachDB.

Scope reviewed: `specs/replication.md` (LOCKED, FM-REPLICATION-001..064, GAP-1..7, invariant
catalog, Redis-deviations table) in full, plus the whole of
`.scratch/replication-correctness/issues/open/{15,16,17,18,19,21,22,23,24,26}-*.md` with the
`## Ruling (2026-08-13)` sections as the primary subject.

**Verdict: 13 findings — 1 CRITICAL, 5 HIGH, 7 ADVISORY.** No ruling is wrong in its choice of
option on the axis it reasoned about; every finding below is either (a) a hazard the ruling's
scope did not reach, or (b) a collision between a ruling and a LOCKED row that must be resolved
before implementation. Two rulings (21, 24) as literally written would each re-open a hazard the
spec already closed elsewhere.

---

## CRITICAL

### C1 — Issue 24 ruling (a) leaves the restart-with-a-dataset lineage break wide open

**Location:** issue 24 `## Ruling (2026-08-13)` ("identity + offset move INTO the
checkpoint/dataset metadata … A boot that recovers no dataset naturally mints a fresh replid");
collides with `FM-REPLICATION-021` Observable and `FM-REPLICATION-013`/`-019`'s
`window_contains`.

**What the ruling closes:** the persistence-disabled case from seed 81 — no dataset recovered, so
no identity recovered, so a fresh replid, so every old-lineage replica full-resyncs. That half is
right and is a genuine structural improvement over a separate `replication_state.json`.

**What it does not close.** The ruling's own construction says a boot that *does* recover a
dataset recovers the id and offset with it. So a primary that crashes and restarts on its data
directory comes back as `(R, X)` where `X` is the offset the recovered dataset carries — and `X`
can be **below the offset its replicas already received and acked**, because:

- `FM-REPLICATION-021`'s own deliberate non-guarantee records that the identity write is not
  fsynced, and the analogous exposure exists for any persistence setting that does not fsync every
  write before it is broadcast to replicas (the retro-gate's defect (a) — checkpoint cut before the
  shard WALs drained — is the same family, and its fix does not make a non-fsyncing primary's WAL
  survive a power cut);
- nothing in the spec asserts *stream-after-durable* as an invariant, and under Redis-compatible
  `everysec`-class durability it cannot hold.

Now replay issue 24's own reachable-damage paragraph against the restarted primary: it re-issues
`X+1 …` under `R` with **different bytes**. A replica that reconnects promptly presents
`PSYNC R X+k` with `X+k > X`, is refused for being ahead of the head, and full-resyncs — that is
what heals seed 81. A replica held away (partition, a long GC pause, a slow restart) until the new
stream has passed `X+k` presents `PSYNC R X+j`, and `ReplicationState::window_contains` grants
`+CONTINUE` on replid-equality plus `requested_offset <= current_offset`
(`FM-REPLICATION-013`). Nothing in the grant distinguishes the pre-crash lineage from the
post-crash one — verbatim the sentence issue 24 wrote about the persistence-disabled case. The
armed backlog floor (`FM-REPLICATION-014`) does not help: it is armed at or below the recovered
head, so `X+j` is above it.

The result is **silent history divergence with no full resync and no divergence latch** — the
single worst outcome in this problem domain, and the exact one PSYNC2's `replid2` exists to
prevent.

**What modern practice does.** Redis's `loadDataFromDisk` master branch is *not* "keep the id":
it copies the RDB's `repl-id` into `server.replid2`, sets `second_replid_offset =
rsi.repl_offset + 1`, rebases `master_repl_offset` to `rsi.repl_offset`, and **leaves
`server.replid` as the freshly minted one from `initServerConfig`**. Valkey inherits this. The
point of the shift is not bookkeeping: it makes the lineage boundary a **frozen constant**, so a
replica presenting the old id above the boundary is refused *forever*, not merely until the new
stream catches up. Raft gets the same property from the term/index pair — a log entry is
identified by `(term, index)`, and a follower whose `(term, index)` does not match is truncated,
regardless of how far the leader has since advanced. Kafka's leader epoch fence
(KIP-101/KIP-279, `OffsetsForLeaderEpoch`) was introduced to close precisely this hole after
plain high-watermark truncation was shown to diverge across a leader restart.

**Recommended change.** Ruling (a) must be implemented **together with** option (b), not instead
of it. Concretely: any boot that comes up as a primary — dataset recovered or not — shifts the
recovered id into `secondary_id` with `secondary_offset = recovered_offset` (inclusive, per
`FM-REPLICATION-019`'s convention) and mints a fresh `replication_id`, then arms the backlog floor
at the recovered offset. This is the identical code path `plan_primary_stint` already implements
for a promotion; a restart is a lineage discontinuity for exactly the same reason a promotion is,
and should reuse the same machinery rather than be treated as continuity. Replicas at or below the
boundary still get `+CONTINUE` (so the ruling's stated cost — "one guaranteed full resync per
replica" — is actually *reduced*), and replicas above it are refused permanently. Update the
issue-24 ruling text to say (a)+(b), and state in the new FM row that a restart is a stint
boundary.

Secondary correction to the ruling's justification: the ruling calls (a) "the Redis RDB-aux
shape". The RDB-aux shape is (a)+(b); (a) alone is a *stronger* claim of continuity than Redis
makes and is the unsafe half.

---

## HIGH

### H1 — Issue 21's clamp makes the primary its own ack writer, contradicting FM-REPLICATION-039

**Location:** issue 21 `## Ruling (2026-08-13)` ("`ingest_replica_ack` clamps a `REPLCONF ACK`
beyond the primary's live head down to the live head"); collides with `FM-REPLICATION-039`
Invariant and NOT-observable.

`FM-REPLICATION-039` states, in bold, **"The wire ACK is the only writer of `acked_offset`"**, and
lists as NOT observable: *"crediting a replica with an offset the primary supplied rather than one
the replica sent"* — the whole subject of issue 28, whose fix was to split `resume_offset` out of
`acked_offset` so the sender's optimism could never answer a durability question. A clamp stores
`min(wire_value, self.live)`. Whenever the clamp fires, the value written to `acked_offset` **is
the primary's own number**, sourced from the primary's bookkeeping. That is the same class the
LOCKED row forbids, arriving through a different door.

The behavioural consequence is not theoretical. Take the ruling's own innocent producer: a
promotion settles `live` at the applied head 100 while a replica holds and keeps spontaneously
ACKing 165 from the previous lineage (`FM-REPLICATION-038`'s ack cadence means it re-sends
autonomously). The primary writes to 150. The next spontaneous 165 clamps to **150** and is
recorded as this replica's acked offset. `WaitCoordinator::count_acked(150)` now counts it —
`WAIT 1 0` returns 1 for a write the replica has never received and, being on a divergent tail,
will discard when it truncates. The clamp does not merely lose the misbehaviour signal (which the
ruling accounts for with a counter and a log); it **manufactures durability credit at the live
head on every ack, for as long as the condition lasts**. It also silently keeps
`min_acked_offset` pinned to the head, which is the input to the split-brain divergence window's
lower bound — the third consequence issue 21's own body lists.

**What modern practice does.** Kafka rejects a follower fetch offset beyond the leader's log end
rather than truncating it into a credit; the follower is told to truncate
(`OffsetsForLeaderEpoch`) and contributes nothing to the ISR high watermark until its position is
verifiable. Raft implementations reject an out-of-range `matchIndex` outright — `matchIndex` is
only ever advanced by a successful `AppendEntries` reply describing entries the leader actually
sent. Neither clamps an impossible position into a maximal one.

**Recommended change.** Keep the ruling's disposition (no disconnect — the reasoning about
`settle_at_applied` innocently moving the head down under a healthy replica is correct and is a
better answer than Redis's) but change the action from **clamp** to **ignore**:
`ingest_replica_ack` drops an ack strictly greater than `self.live`, increments the counter, logs,
and leaves `acked_offset` at the last value the replica legitimately sent. This strictly dominates
the clamp:

- liveness is identical — no link is dropped, and the replica converges the moment it sends an
  in-range ack after truncating or resyncing;
- `acked_offset` keeps exactly one writer, so `FM-REPLICATION-039` survives untouched and no new
  exception is needed against a LOCKED row;
- `WAIT` and `min_acked_offset` never carry credit the wire did not supply;
- the observability the ruling wanted (counter + log naming the peer, the offsets, and the delta)
  is unchanged.

If a clamped value is genuinely wanted for the *lag* rendering, put it there and only there —
`stream_position()` is already the one consumer that folds in primary-side bookkeeping
(`FM-REPLICATION-043`), and it is explicitly excluded from durability credit. That is the seam the
spec already built for this exact distinction.

Also: whichever action is chosen, the new FM row must state that `INV-OFFSET-3` becomes a
*rejected-input* invariant rather than a *repaired-input* one — under the clamp as ruled, the
invariant is true only because the ingest seam rewrote its own witness, which is a much weaker
claim than the catalog's HARD tier implies.

### H2 — Issue 22's kick records a departure that can silently un-fence the primary

**Location:** issue 22 `## Ruling (2026-08-13)` ("A session reaching `Phase::Streaming`
disconnects any other session streaming with the same announced identity"); collides with
`FM-REPLICATION-062` Invariant + NOT-observable and `FM-REPLICATION-041`.

`FM-REPLICATION-062`'s classification table is exhaustive and includes
**`disconnect_requested()` → `Graceful`**. The natural implementation of the ruled kick is a
primary-initiated `request_disconnect()` on the predecessor — which therefore records
`ReplicaDeparture::Graceful` into the tracker's single last-writer-wins `AtomicU8`.

The ordering is the problem. `Effect::ClearDeparture` is emitted by the transition that publishes
`Phase::Streaming` for the **successor**; the kick is triggered *by* that same transition, so the
predecessor's `Effect::RecordDeparture(Graceful)` lands **after** the clear. The tracker is then
holding a `Graceful` record that describes a session which died before the currently-streaming
one — verbatim the shape `FM-REPLICATION-062` lists as NOT observable: *"A departure answering for
a replica that arrived after it … without the clear, a predecessor's clean departure would still
be on record while the current replica's link dies, and the fence would read the death as a
decommission."*

Consequence: the successor is later lost in a way that records nothing (a bare
`unregister_replica`, which the same row documents as leaving no record) or is itself superseded;
`disarm_if_departed_cleanly()` then reads `Graceful`, and the self-fence — armed, with no fresh
streaming replica — **disarms**. The row calls the permissive default *"the one failure of this
seam that silently un-fences a primary"*. The ruling reintroduces it.

**Recommended change.** Give supersession its own outcome. Either:

1. add `LinkOutcome::Superseded` whose transition emits `Effect::Unregister` **without**
   `Effect::RecordDeparture` (a superseded session has no opinion about whether the replica
   departed — the replica is still there, on the new socket); or
2. keep the record but make `ClearDeparture` generation-keyed so a record stamped with a
   generation older than the currently-streaming one cannot be read.

(1) is smaller, matches the row's existing "a session that never streamed records no departure"
carve-out in spirit, and needs one new forcing test
(`a_superseded_session_records_no_departure`) plus one asserting the fence stays armed across a
kick. The new FM row for issue 22 must name the interaction with `FM-REPLICATION-062` explicitly,
and `FM-REPLICATION-062`'s classification table needs the new arm.

### H3 — Issue 24 does not state the atomicity the relocated offset requires; the failure direction is data corruption

**Location:** issue 24 `## Ruling (2026-08-13)`, acceptance criteria.

Moving the offset next to the dataset makes the pairing exact *if and only if* the offset is
committed in the **same atomic unit** as the last write it names (same WAL record / same write
batch / same checkpoint manifest), and recovery restores them together after WAL replay — not
merely stamped into the checkpoint manifest at checkpoint time while the WAL replays past it.

Get it wrong in the **high** direction (offset ahead of the data) and the node claims writes it
does not hold — the failure mode issue 17 exists to remove, arriving in a new home. Get it wrong
in the **low** direction (offset behind the data, which is what a manifest-time stamp plus WAL
replay produces) and a restarting **replica** asks its primary to resume from below its true
position, the primary replays that range, and the replica **re-applies non-idempotent replicated
commands** — `INCR`, `INCRBY`, `LPUSH`, `APPEND`, `PFADD`, `SETRANGE`, list/stream appends. That is
silent, unbounded data corruption on the read-scaling path, and it is a *new* exposure: today's
`ReplicationIdentity::adopting` is raise-only (`fetch_max`), which is deliberately biased away from
this direction. Ruling 17(a) additionally removes the raise-only guard, so after both rulings the
low direction is representable where it was not before.

**What modern practice does.** Redis writes `repl-offset` as an RDB aux field of the same snapshot
whose contents it describes, and an AOF-loading replica derives its offset from the replicated
stream itself. Kafka stores the log-end offset *as* the log. etcd stores the raft `applied index`
in the same backend transaction as the state-machine mutation, precisely so replay cannot
double-apply. CockroachDB writes the `RaftAppliedIndex` in the same batch as the command's effects
for the same reason.

**Recommended change.** State as an invariant in the new FM row: *the persisted replication
offset is committed in the same atomic unit as the last write it names, and recovery restores both
or neither*; add a forcing test that crashes between the last write and the checkpoint and asserts
the recovered offset equals the offset of the last recovered write. If exact pairing cannot be
guaranteed for a given persistence configuration, the recovered offset must be biased **high**
(never below the recovered data) and the node must full-resync rather than resume — the same
"freezing low only costs a full resync, freezing high corrupts" reasoning
`FM-REPLICATION-019` already applies to the promotion boundary, with the sign reversed because
here the offset is a *claim about what is held*, not a *bound on what may be served*.

### H4 — Rulings 17 and 24 both relocate the same field and are not sequenced; FM-REPLICATION-021 is left contradicted

**Location:** issue 17 ruling (a) vs issue 24 ruling (a); `FM-REPLICATION-021` Observable,
Invariant, Catalog; `INV-OFFSET-2`.

Ruling 17 keeps `offset_at_save` and re-tiers `INV-OFFSET-2` to `Tier::Hard` scoped
within-a-history. Ruling 24 moves the offset (and the identity) out of `replication_state.json`
into the checkpoint/dataset metadata. After 24, `offset_at_save`, `ReplicationState::save`,
`load_or_create`, and `ReplicationIdentity::adopting`'s `fetch_max` — all four of which are the
literal subject of `FM-REPLICATION-021`'s Invariant and of ruling 17's implementation — may not
exist in the form either ruling assumes. Neither ruling mentions the other; issue 24's ruling says
only "New FM row".

Further, `FM-REPLICATION-021`'s Observable currently *guarantees* the opposite of what 24 (and, per
C1, 24+(b)) will do: *"After the reboot `INFO replication` reports the **same** `master_replid`,
the same `master_replid2`, and the same `second_repl_offset` the node advertised before
shutdown."* That is a LOCKED guarantee with five forcing tests
(`test_promoted_identity_survives_restart`, `test_info_master_replid_survives_restart`, …). It
cannot survive ruling 24 unedited, and the ruling does not say so.

**Recommended change.** Before either lands: (i) sequence 24 first, then re-derive 17's
implementation against whatever home the offset ends up in; (ii) rewrite `FM-REPLICATION-021` as
part of 24's change set — Observable becomes "identity and offset are recovered together from the
dataset, or both are freshly minted", with the restart-as-stint-boundary behaviour from C1; (iii)
restate `INV-OFFSET-2` once, in the new home, rather than twice against two different fields; (iv)
re-point 021's forcing tests (a promoted node's `master_replid2`/`second_repl_offset` still
survive a restart under C1's shift — it is `master_replid` that changes — so most of the tests
survive with amended expectations rather than deletions). Also note that persistence-disabled
deployments lose identity persistence entirely under 24; that is safe (full resync) but is a
behaviour change for every replica restart and belongs in the row and in the Redis-deviations
table.

### H5 — "Monotone within a replication history" is not checkable unless the invariant is keyed on the history

**Location:** issue 17 `## Ruling (2026-08-13)`; `INV-OFFSET-2`; `FM-REPLICATION-001`.

The ruling is right on the merits — Redis's `readSyncBulkPayload` overwrites
`server.master_repl_offset = psync_offset` wholesale, so a backwards full resync persists the lower
offset, and a save point that outlives its history is exactly the "identity outliving its dataset"
anti-pattern. But `Tier::Hard` + "monotone within a replication history" gives the invariant
checker a predicate it cannot evaluate: it sees a save point drop and has no way to tell a legal
cross-history drop from an illegal in-history rewind. Implemented naively it becomes an invariant
that never fires, i.e. a dead detector — the same failure issue 19 was filed for on the other
entry.

**Recommended change.** Key the invariant on the history: `INV-OFFSET-2` holds that
`offset_at_save` (in whatever home 24 leaves it) is monotone **for a fixed
`(replication_id, history epoch)`**, and that any decrease is accompanied by an id-or-epoch change
committed in the same critical section. `reset_pair` already bumps the epoch under the gate lock
(`FM-REPLICATION-007`), so the witness exists; the checker just has to read it. Concretely, the
projection should carry the epoch alongside the offset so the drop and the epoch bump are one
observation.

Second, ordering: the lowering must sit **inside** `FM-REPLICATION-001`'s existing
install-then-adopt unit, never before it. A crash after lowering the save point but before the
dataset install leaves a node claiming *less* than it holds, which on restart asks its primary to
replay a range it already applied — H3's corruption direction. `FM-REPLICATION-001` already
sequences "install_payload before adoption"; the save-point write joins the adoption half.

---

## ADVISORY

### A1 — Issue 18's "unfalsifiable by construction" overclaims; provenance is out of scope and should be said

**Location:** issue 18 `## Ruling (2026-08-13)`.

The chokepoint choice is right (validating inside `shift_replication_id` /
`adopt_replication_history` rather than at three call sites is the seam-lint idiom this codebase
already uses fifteen times, and it makes a forgotten fourth seam impossible). But the ruling says
`INV-REPLID-3` "becomes unfalsifiable by construction", and the issue body's consequence **4** —
*"a peer that echoes a valid-looking id it never owned gets `+CONTINUE` served against a history it
does not have"* — is untouched by a well-formedness check. Consequences 1–3 are closed; 4 is not,
and the ruling does not acknowledge the narrowing.

A replication id is a public identifier (it is in `INFO`, and the primary hands it to every
replica), so it is not a capability and cannot be one. Redis is in the same position and relies on
`requirepass`/`masterauth` plus TLS for provenance. FrogDB has a stronger option available in
cluster mode, where the raft-carried node identity and epoch are authenticated at a different
layer.

**Recommended change.** One sentence in the ruling and in the FM row: id validation is
*well-formedness only*; anti-impersonation depends on `masterauth`/TLS (and, in cluster mode, on
the raft-side identity), and `INV-REPLID-3` is a structural invariant, not a security property. Do
not let a future reader mistake the catalog entry for an authentication guarantee.

### A2 — Issue 22's dedup key is peer-controlled and flaps when replicas share an egress IP

**Location:** issue 22 `## Ruling (2026-08-13)`; `FM-REPLICATION-043` (`ip=address.ip()`,
`port=` announced `REPLCONF listening-port`).

The ruled identity is (peer socket IP, announced listening port). The announced half is
peer-supplied. Two consequences worth pre-empting:

- **Flap.** Several replicas behind one NAT/egress gateway, or any topology where distinct
  replicas present the same source IP and are configured with the same listening port, collapse to
  one identity and will kick each other in a permanent loop — each reconnect kicks the incumbent,
  which reconnects and kicks back. This is precisely the risk the issue body flagged for option 1
  ("a replica that legitimately opens two links … would flap"), reached by a different route, and
  it converts a benign misconfiguration into a total replication outage rather than a
  double-counted `WAIT`.
- **Amplification.** A misconfigured (or hostile) replica that announces a peer's listening port
  can evict it on demand.

**Recommended change.** (i) Where a stable node identity exists — cluster mode's node id — key the
dedup on it and fall back to addr:port only in standalone. (ii) Reuse the existing cooldown idiom:
`LagPolicy`'s `is_in_lag_cooldown` is already keyed by `SocketAddr` *precisely because ids change
on reconnect* (`FM-REPLICATION-043`); a kick cooldown keyed the same way turns a flap into a
bounded, observable oscillation. (iii) Emit a distinct counter for kicks so a flap is visible in
metrics rather than only in logs. (iv) Keep the ruling's `listening_port == 0` carve-out exactly as
stated — that part is right, and worth restating in the FM row's NOT-observable ("an unannounced
session deduped against another unannounced session").

### A3 — Issue 16's "the cluster reconciler already self-heals" is asserted without a witness

**Location:** issue 16 `## Ruling (2026-08-13)`.

The ruling leans on cluster-mode self-healing to justify accepting a terminal standalone state.
That is plausible — `RoleManager::promote` early-returns only when `!is_replica`, and a failed
promotion leaves `is_replica` set (`FM-REPLICATION-020` Invariant), so the 1 Hz reconciler's
re-issued role does reach `begin_primary_stint` again. But nothing tests it, and the retry path has
an unexamined step: `settle_at_applied()` is called a second time on an **already frozen**
`ApplyGate`. Whether that is idempotent (returns the same boundary) or observable
(different boundary, or a panic) decides whether the retry is a self-heal or a second, worse
failure.

**Recommended change.** Add to the acceptance criteria: (i) a unit test that
`begin_primary_stint` retried after a persist failure mints from the *same* boundary and succeeds
once the persist path recovers; (ii) a cluster-mode integration or turmoil case where a node whose
promotion failed on an unwritable data dir is recovered by the reconciler after the dir becomes
writable. Without (ii) the ruling's central liveness argument is an assumption. Also make the new
`INFO` field's name explicit in `FM-REPLICATION-020` so `lint-spec` can hold it, and ensure the
metric is a *state gauge* (level-triggered) rather than an event counter — an operator alert on a
stranded node must be able to fire from a scrape taken long after the failed promotion, which an
edge-triggered counter increment cannot support.

### A4 — A node stranded by issue 16's ruled behaviour serves unboundedly stale reads with no client-side signal

**Location:** issue 16 ruling; `FM-REPLICATION-029` Invariant ("There is deliberately **no** stale
-read gate on the read path: FrogDB has no `replica-serve-stale-data` config knob at all").

The no-knob deviation is defensible in the ordinary lagging-replica case and matches Redis's
`yes` default. Issue 16's ruled outcome changes the shape of the worst case: a node that follows
nobody, applies nothing, and (in standalone) will stay that way until a human intervenes keeps
answering reads from a keyspace frozen at the moment of the failed promotion, for an unbounded
period. The ruled observability is operator-facing (a metric and an `INFO` field); a client has
only `master_link_status` to infer from, and must know to look.

Redis and Valkey offer `replica-serve-stale-data no` for exactly this class. CockroachDB bounds
follower reads by a closed timestamp so a stale read is stale by a *known* amount; etcd defaults
to linearizable reads and makes serializable reads opt-in per request.

**Recommended change.** Not necessarily a new knob — but `FM-REPLICATION-029`'s scope note should
name the stranded-promotion path as an unbounded-staleness source (today it reasons only about
"lagging but connected" plus two inspected neighbours), and the issue-16 ruling should record that
clients detect this only via `master_link_status`/the new field. If a knob is later wanted, the
Redis-compatible spelling is the one to adopt, and it belongs in the deviations table either way.

### A5 — Issue 26's ruling closes the instance; the class it identified is deferred to an external dependency

**Location:** issue 26 `## Ruling (2026-08-13)` ("Option 2 now + option 1 via formal-spec phase
3"); issue 15 `## Comment (2026-08-13)`.

The issue's own framing is that the durable finding is *class-level*: "every model in this campaign
transcribes some control flow, and (d-ii) is the first proof that the transcription can drift from
the tree." Option 2 adds one witness for one consumer of one gate; the general defence is deferred
to a phase-3 quint-connect model that is outside this campaign's control. If phase 3 slips, the
class gap persists with no interim mitigation, and exit criterion 8 will have been declared on the
strength of a point fix.

There is a cheap in-idiom interim: this codebase already runs fifteen chokepoint seam lints
encoding "every X must go through Y", and `just lint-gates` runs the compile-free subset on every
commit. A lint asserting that the streaming path in `replica_session.rs` contains calls to
`feed_gate.released()` and `feed_gate.is_held()` — i.e. that the production consumer still consults
the seam the model transcribes — would have caught (d-ii) in well under a second, and generalises
to every other transcribed control flow by adding one entry per model.

**Recommended change.** (i) Add the seam lint as an interim class-level guard, and record in the
ruling that it is the interim and phase 3 is the structural fix. (ii) Broaden the acceptance test:
the issue names **two** consumption points (`released().await` before the backlog tail, and the
`while feed_gate.is_held()` buffering loop); the acceptance should require that bypassing *each*
independently turns something red, not just the pair together — a barrier assertion that only
catches the first bypass leaves the second in the same position (d-ii) was in.

### A6 — FM-REPLICATION-041 was LOCKED carrying a claim the code did not implement

**Location:** issue 19 ruling; `FM-REPLICATION-041` Invariant.

The ruling picks (a), arming on the `Phase::Streaming` transition, which is correct — Redis needs
no latch because it evaluates the live good-replica count per write, and FrogDB's latch exists to
add a distinction Redis does not make, so a latch that can only be installed by the very write it
is meant to gate is a straightforward dead-detector bug.

Worth recording as a *process* observation rather than a design one: `FM-REPLICATION-041`'s
Invariant **already reads** "`armed` is a latch set by any replica reaching `Phase::Streaming`".
The spec was locked, mutation-gated, and `lint-spec`-checked while the code armed only from
`has_quorum`. So the row's claim was never forced — spec↔test agreement checks that every row names
tests and every tagged test names a row, but not that the named tests exercise the specific clause.

**Recommended change.** The ruling needs no design change. Add to `FM-REPLICATION-041`'s
`Forced by` a test that arms the fence with **no write served** and then asserts a subsequent loss
fences (that is issue 19's own required forcing test), and consider whether other
"latch/counter set by X" clauses in the catalog are in the same unforced position — a quick audit
of the Invariant cells that assert *where* state is written, rather than *what* it contains, would
be cheap and is exactly the class this one fell into.

### A7 — Issue 23: "a clean departure history" must mean *unknown*, not *graceful*

**Location:** issue 23 `## Ruling (2026-08-13)`; `FM-REPLICATION-062` Invariant.

Option (a) is right — both the latch and the record describe the stint that just ended, and
clearing them together is the smallest change that makes `INV-SESSION-3` and `INV-FENCE-1` true as
written. One implementation note that the ruling's wording ("re-promotion starts with a clean
departure history") could be read against: the tracker stores departure as an `AtomicU8` where
`0` = none/unknown and unknown **means keep fencing** — the row calls the permissive reading "the
one failure of this seam that silently un-fences a primary". "Clear" must therefore write `0`, not
`Graceful`. Reading "clean" as "graceful" would hand a re-promoted node a pre-disarmed fence.

**Recommended change.** State the sentinel explicitly in the ruling and in the forcing test's
assertion (`last_streaming_departure() == None` after demotion, not `Some(Graceful)`). Also worth a
line in the FM row: a demote/re-promote cycle deliberately forgets that this node ever had
followers, so the self-fence is not a guarantee that survives a role flap — an operator reasoning
about a flapping node should not expect it to.

---

## Where the spec already matches or exceeds best practice

Listed because they are load-bearing and should not be traded away in any of the changes above.

1. **An ACK is a durability claim on `landed`, not `received`** (`FM-REPLICATION-008`, the
   `landed <= claimed <= received` triple). This is Kafka's high-watermark discipline and is
   *stronger* than Redis, where `REPLCONF ACK` reports bytes received off the socket. It is the
   single most important thing in the document.
2. **The promotion boundary is frozen at the applied head, never the received head**
   (`FM-REPLICATION-019`), with the reasoning stated as "freezing low only costs a full resync —
   freezing high corrupts". That is raft's commit-index safety argument, correctly applied, and the
   inclusive-boundary deviation from Redis's `+1` is well justified (FrogDB replicas request their
   applied offset, so the `+1` would grant a byte the node never had).
3. **`resume_offset` is kept out of `acked_offset`** (`FM-REPLICATION-039`, issue 28's fix), with
   the byte-lag measure as the single deliberate consumer of the union. Folding the sender's
   optimism into the receiver's durability is the classic ISR bug, and the spec names it, splits
   the fields, and forces the split with five separate tests.
4. **The divergence latch is epoch-keyed and cleared only by a fresh dataset install**
   (`FM-REPLICATION-007`, `-010`) — level-triggered, so the signal cannot be lost by a racing
   reader, and `Claim::{Stale, Retired}` makes a stale applier's write unrepresentable rather than
   merely unlikely.
5. **The `RoleFence` is a counter, not a boolean, sampled before the second role read**
   (`FM-REPLICATION-040`), with `biased;` select ordering. That closes the ABA window a
   generation-less flag would leave, and is the same discipline raft uses to fence reads pending
   across a term change.
6. **No synthetic `DEL` on the replication wire** (`FM-REPLICATION-030`), so the offset is not a
   function of wall-clock expiry timing. This is the review's headline anti-pattern, explicitly
   designed out.
7. **`WAIT` refuses Dragonfly's early-exit shortcut, uses `tokio::time::Instant`, never
   re-snapshots the target on the timeout path, and does not fan out across shards**
   (`FM-REPLICATION-037`, `-039`). The NOT-observable cell reads like a list of other systems'
   bugs, which is the right way to write one.
8. **Unknown departure keeps the fence armed** (`FM-REPLICATION-062`) — the safe verdict is the
   default rather than the exception, and every `?` maps to `Lost` so a new error path is
   classified conservatively without being touched.
9. **Backlog floor monotone via `fetch_max`, with the extraction-time re-check under the entries
   lock** (`FM-REPLICATION-012`, `-014`) — the TOCTOU between "the window contains your offset"
   and "here are the bytes" is closed at the point of use, not by a wider lock.
10. **The version gate admits `Unproven` at PSYNC but must block at finalization**
    (`FM-REPLICATION-064`), with the asymmetry reasoned rather than asserted.
11. **The rulings' overall posture on wall-clock**: issue 22 explicitly rejects option 3
    (rely on `repl-timeout` to reap the corpse) on the grounds that it narrows rather than closes a
    `WAIT` correctness window, and issue 19's fix *removes* a dependency on write traffic for a
    safety latch. Both are the right instinct. The remaining time-based inputs
    (`ack_is_fresh`, `min-replicas-max-lag`) gate availability, not safety, are Redis-compatible,
    and are expressed as a pure predicate over an age rather than a clock read at the boundary
    (`FM-REPLICATION-046`) — assessed and found sound.
12. **Clamp-not-disconnect on over-high acks, assessed on merit**: the disposition (do not drop a
    link that is behaving correctly against a head that legitimately moved down under it) is right
    and better than Redis's verbatim ingest. It is the *repair* action, not the disposition, that
    H1 asks to change — from clamp to ignore — which preserves the ruling's reasoning entirely.
