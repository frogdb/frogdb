# 10 — Restructure `replica_session.rs` into an explicit phase state machine

Status: done

## Parent

[PRD](../../PRD.md) §8 D2, ruling tier (iii) — "full restructure authorized", with the execution
discipline the ruling attaches to a locked-crate refactor of this size.

## What to build

`frogdb-server/crates/replication/src/replica_session.rs` (4574 LoC) becomes an explicit phase
state machine: a pure `step(view, event) -> (phase, effects)` and the async loop
(`replica_session::run`, `replica_session.rs:642`) as an interpreter over the returned effects.
This is the interpreter pattern the whole area benefits from, and D2 authorizes it explicitly
after tiers (i) and (ii) land as stepping stones.

The structural payoff beyond model-checkability: `ReplicaSession::set_phase`
(`replica_session.rs:591`) stops being an ad-hoc writer, because the phase transition becomes the
return value of `step`. INV-SESSION-1 — "a session's `Phase` only moves forward in the declared
order; `Disconnecting` is terminal" — is asserted in prose at `replica_session.rs:46` today and
checked by nothing; after this it is structural, with the catalog entry as the backstop.

**Execution discipline, ruled in D2 and not optional:**

- Tiers (i) and (ii) land first (issue 07). This issue is the third step, not a merge of all three.
- Every step is **spec-first** against `.scratch/hardening/specs/replication-failure-modes.md`:
  rows may move their file:line citations but not their meaning, and `just lint-failure-modes`
  stays green at each stage.
- Land in reviewable stages. A 4574-LoC locked-crate file does not land as one diff — this issue
  is expected to split itself into a numbered sub-issue chain, each stage green on the full suite.
- The **full** mutation gate re-runs at the end — `just mutants frogdb-replication` plus
  `just mutants-gate frogdb-replication 0.85` — not just `mutants-diff`, because the restructure
  moves most forcing-test targets. Any surviving mutant no test can kill is documented at the code
  with why it is unobservable, never blanket-skipped.

Issues 08 and 09 inform the shape without blocking it: whatever `Action`/`Outcome` vocabulary the
two models needed is the event/effect vocabulary that should generalize here.

## Acceptance criteria

- [x] Explicit `Phase` state machine with a pure `step(view, event) -> (phase, effects)`; the
      async loop only interprets effects and performs I/O
- [x] Landed as a reviewable chain of stages, each stage green on `just test frogdb-replication`
      and `just test frogdb-server`
- [x] Every FM-REPLICATION row whose citation moves is re-pointed with its meaning unchanged;
      `just lint-failure-modes` green at each stage
- [x] INV-SESSION-1 holds by construction, and its forcing test still goes red when the catalog
      check is deleted
- [x] Full gate at the end: `just mutants frogdb-replication` + `just mutants-gate
      frogdb-replication 0.85` passing, with the in-crate share of forcing tests recorded

## Blocked by

- Issue 07 (`.scratch/replication-correctness/issues/`) — D2 requires tiers (i) and (ii) as
  stepping stones before (iii).
- Issues 08 and 09 (`.scratch/replication-correctness/issues/`) inform the event/effect vocabulary
  but do not block; if they run late, do not wait.

## Resolution (2026-08-11)

Landed on `worktree-agent-a59be3494b58e9676` (base `1cfd9328`) in five commits:

- `173ba853` — **Phase A**: `frogdb-replication/src/session_machine.rs`, the pure decision half:
  `step(view: SessionView, event: SessionEvent) -> Transition { phase, effects }` over the
  `SessionEvent{Begin, ReplySent, Drained, CheckpointCut, DatasetExported, PayloadSent, Ended}` /
  `Effect{…}` vocabulary generalized from issues 08/09. The transition table is total; the
  catch-all keeps `view.phase` and emits `FailSync(UnexpectedEvent)`, which is what makes
  `Disconnecting` terminal *by construction* rather than by convention.
- `bc793892` — **Phase B**: `ReplicaSession::run` becomes an interpreter. `run_inner`,
  `handle_partial` and `handle_full` are deleted; `SessionDriver` holds the socket and performs
  one `Effect` at a time, `set_phase` becomes `commit_phase` (the single place a phase reaches the
  session, hooked into the invariant catalog), and `stream_live_dataset` takes pre-exported blobs
  so the export is an effect rather than a hidden I/O step.
- `c114ecf2` — **Phase C**: old path gone; the promotion model renders its handshake through
  `HandshakeReply::line()` so `model/promotion` drives the new seam, and the FM-REPLICATION
  citations move (001/004/015/055/062) with their meaning unchanged.
- `27dc2357` — the INV-SESSION-1 sweep gains its vacuity guard: it now asserts the catalog *can*
  report a backwards phase move, so deleting the `INV-SESSION-1` check turns the sweep red instead
  of leaving it green-but-silent.
- `e402e2fc` — `run_cleans_up_checkpoint_dir_on_mid_fullsync_drop` made non-vacuous (see survivors
  below).

**Evidence.** `just test frogdb-replication` 525/525 (6 skipped) at each stage;
`just test frogdb-server` 2017 tests, the three cluster-area failures re-run individually green on
the branch and traced to machine load against the 50 ms `HANDOFF_DRAIN_WAIT_MS`, not to this
change (base `1cfd9328` A/B); `just lint-failure-modes` OK (279 rows, 1450 tags);
`cargo clippy -p frogdb-replication --tests -- -D warnings` clean.

**Mutation gate.** `just mutants frogdb-replication` over 1147 mutants: **976 caught, 20 missed,
151 unviable, 0 timeouts → 98.0%**, gate 0.85 **PASS** (`just mutants-gate` on the reconciled
ledger; the run was killed three times by machine sleep and was completed with `--iterate`, so the
dispositions were accumulated by name and checked to partition the 1147 names `cargo mutants
--list` generates). Killing the one new-code survivor takes it to 977/996 = **98.1%**. Prior
figure for reference: 98.7% over 1180 mutants (PRD §1). **In-crate share of forcing tests: 100%** —
`cargo mutants -p frogdb-replication` runs only this crate's own suite (525 tests), so every one
of those kills is forced from inside `frogdb-replication`.

**Survivor triage (20 missed).** One is in new code and was killed:
`SessionDriver::clean_checkpoint_dir` replaced with `()` survived because
`run_cleans_up_checkpoint_dir_on_mid_fullsync_drop` dropped the client before reading the grant
line — the sync failed in the pre-checkpoint drain, no checkpoint directory was ever cut, and the
"no leak" assertion held for the trivial reason. Fixed in `e402e2fc`; `cargo mutants --re
clean_checkpoint_dir` now reports it caught. The other 19 are pre-existing (`git blame` puts every
one of them before this branch) and split three ways:

- **Unobservable allocation arithmetic (10)** — `frame.rs:60` ×3, `frame.rs:167` ×2,
  `frame.rs:225`, `frame.rs:570`, `fullsync.rs:41`, `fullsync/receiver.rs:56`,
  `replica/streaming.rs:59` ×2: capacity hints and buffer pre-sizing, where the mutant changes how
  much is reserved and nothing else.
- **Constructor-equals-Default (3)** — `ReplicaFeedGate::open`, `PrimaryReplicationHandler::feed_gate`,
  `ReplicationTrackerImpl::new_arc` replaced with `Default::default()`.
- **Real boundary gaps (6)**, for issue 20's re-baseline to own: `invariants.rs:464` (`<`→`<=` in
  `inv_offset_4`), `offset_coordinator.rs:230` (`>`→`>=` in `settle_at_applied_inner`),
  `replica_session.rs:1085` (`>`→`>=`, the backlog/live-tail handoff dedup — forcing it needs a
  frame delivered on the broadcast whose sequence equals `resume_offset`, i.e. one sent inside the
  subscribe→replay window, which no current seam can schedule deterministically),
  `state.rs:91` (`!=`→`==` in `consume_staged_replication_metadata`), `frame.rs:167`
  (`ReplconfCodec::is_getack` offsets). `replica_session.rs:638` (`!=`→`==` in `commit_phase`) is
  the one documented at the code: both readings leave identical session state and differ only in
  whether a duplicate `tracing::debug!` line is emitted.

**Known defects preserved.** Issues 16, 17, 19, 21, 22 keep their pins and muzzles
(`properties.rs` diagnostics, the `invariants.rs:116` documented exception, the registry
assertions in `replica_session.rs`), and issue 18 keeps its `#[ignore]` in
`replica/connection.rs:775`. `force_phase_for_test` and `backdate_last_ack_for_test` are unchanged.
No behavior change was made to any of them.
