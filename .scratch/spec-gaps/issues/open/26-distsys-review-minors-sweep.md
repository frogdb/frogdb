# 26: Distsys-review minors sweep (persistence + txn)

Status: ready-for-agent

## Origin

Minor findings from the independent distsys review
(`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`), ruled one at a
time by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).
One sweep issue per area tracker; this one collects persistence and txn minors.
Locked gates apply per touched crate (persistence 0.85, txn 0.90) — spec edits run
`just lint-spec`; code changes get `just mutants-diff` triage.

## Ruled minors

### MIN-6 — `wal_watermark::write`'s no-torn-body reasoning silently depends on ext4 `auto_da_alloc`

Ruling: **add checksum** (filesystem-independent beats named assumption —
Kubernetes-primary deployment means operators pick filesystems; XFS/btrfs/
ext4-`data=writeback` all break the current argument). Watermark body gains a
checksum; a torn or corrupt body parses as **absent**, never as garbage. The
`wal_watermark` rows in `specs/persistence.md` restate the argument
filesystem-independently (the ext4 dependency disappears rather than being
documented). Forcing test: corrupted/truncated body → treated as absent
(fails pre-fix only on non-ext4 semantics, so the test corrupts bytes directly).

- [ ] Checksum in watermark body; torn/corrupt → absent
- [ ] Rows restated filesystem-independent; `just lint-spec` green
- [ ] Corruption forcing test landed

### MIN-7 — backup dirs wall-clock-named, `unwrap_or(0)` pruning

Ruling: **full counter redesign** — graduated to
[issue 27](27-backup-naming-monotonic-counter-prune-refuses-unparseable.md)
(persisted monotonic counter naming + prune refuses unparseable entries).
No checklist entry here; issue 27 carries the work.

### MIN-8 — TR-BLOCKING-003's current-code cell misdescribes the client-visible shape

Ruling: **correct cell now** (spec-only). The cell claims a dropped sender
resolves "exactly as an ordinary timeout would (TR-BLOCKING-009)" — false:
timeout yields `op.timeout_reply()` (`*-1` for BLPOP, `coordinator.rs:101`),
the drop yields RESP nil `$-1` (`coordinator.rs:40`, `response.rs:285`). State
the shape difference honestly and cross-ref
[issue 08](08-blocking-command-rows.md)'s MAJ-11 amendment (sender-drops
eliminated as signaling; drop then uniquely means channel death →
`-ERR shard unavailable`) as the behavioral fix that rewrites this row. Spec
never lies in the interim; this sentence was hiding the admission-limit bug's
severity (observability-accuracy principle).

- [ ] TR-BLOCKING-003 cell corrected ($-1 vs `*-1` stated, issue 08 cross-ref);
      `just lint-spec` green

### MIN-9 — TR-BLOCKING-020 omits the `timer_sweeps` gate

Ruling: **accept + forcing test**. The GC backstop is gated
(`event_loop.rs:83`: `waiter_timeout_interval.tick(), if timer_sweeps`) and
suppressed in driven/deterministic runs (arrives as `DriveTick` via
`ShardWorker::set_driven_ticks`); the row cites the interval unconditionally.
Amend the precondition with the gate + driven-tick path note, and land the
missing forcing test — a driven run drives the tick explicitly and observes
the sweep — clearing the row's `Forced by | MISSING`.

- [ ] TR-BLOCKING-020 precondition gains `timer_sweeps` gate + driven-tick note
- [ ] Forcing test (driven tick → sweep observed) landed; `Forced by` cites it;
      `just lint-spec` green

### MIN-10 — `CLIENT UNBLOCK` is a silent no-op during the registration window

Ruling: **accept**. `register_wait` (`connection/blocking.rs:99-117`) sends
`BlockWait` to the shard *then* sets the registry mirror; `unblock`
(`client_registry/mod.rs:729`) returns `false` unless `ClientFlags::BLOCKED` —
so an UNBLOCK in the window replies `0` while the client is genuinely parked.
Fix: set the mirror **before** the send (inverted window is safe:
`UnregisterWait` is the cleanup path either way, ordered behind `BlockWait` on
the shard channel); if registration errors after the mirror is set, clear it.
State-space "Client-registry blocked mirror" row states the ordering.

- [ ] Mirror set before `BlockWait` send; error path clears it
- [ ] Row states the ordering; `just lint-spec` green
- [ ] Forcing test: UNBLOCK inside the old window → returns `1`, client wakes
      (deterministic interleaving; fails pre-fix with `0`)

## Acceptance criteria

- [ ] Every checklist entry above resolved as ruled
- [ ] `just lint-spec` green after all spec edits
- [ ] `just mutants-diff` triaged for any code-touching entry

## Blocked by

None — can start immediately.
