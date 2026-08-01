# Transactions — failure modes

Every way a FrogDB transaction can fail, refuse, or succeed, one table per mode. This is the
reference the mutation run is measured against: a mutant that survives is a row nothing forces.

Scope: the connection-side transaction path — `frogdb-txn` (`state.rs` queue/watch/fold,
`exec.rs` EXEC algorithm, `host.rs` seams), the queuing rules in
`frogdb-server/crates/server/src/connection/{dispatch,guards,transaction,transaction_conn_command,state}.rs`,
and the EXEC-time batch router in `frogdb-server/crates/server/src/slot_migration/`. The shard-side
engine (WATCH version check, rollback, replication framing) lives in `frogdb-core` and gets its own
spec; rows here stop at what the connection observes across the shard channel.

## How to read a row

| Field | Meaning |
|---|---|
| Trigger | The concrete precondition or input that puts the connection in this mode. |
| Observable | Exactly what the client sees: reply shape, error prefix, exact text where it is pinned. |
| NOT observable | What must never happen in this mode. This is the half mutation testing attacks. |
| Invariant | The internal guarantee — nothing reaches the WAL, the queue is discarded, watches cleared. |
| Outcome variant | `TransactionOutcome::<Variant>` (and its metric label), or `n/a` for queuing-time modes that never reach `execute_transaction`. |
| Forced by | The test(s) that fail if the behavior changes. Every one carries a `// FM-TXN-NNN` tag at its definition site; `just lint-failure-modes` enforces both directions. |
| Bug refs | Known open issues that touch this mode. |

Test names are bare function names, resolved against
`cargo nextest list -p frogdb-txn -p frogdb-vll -p frogdb-server` (core command profile). Tests
behind `--features turmoil` or an exotic command family are deliberately not cited: they do not
run in the profile the campaign gates on. Neither is the frozen `redis-regression` suite.

Deviations from Redis are called out inline and collected in [Redis deviations](#redis-deviations).

---

## FM-TXN-001 — MULTI cannot nest

| Field | Value |
|---|---|
| Trigger | `MULTI` issued while a transaction is already open on the connection. |
| Observable | `-ERR MULTI calls can not be nested`. The open transaction survives untouched: previously queued commands stay queued and a later `EXEC` still runs them. |
| NOT observable | `+OK`; a second queue replacing the first; the outer transaction being discarded or poisoned by the rejection. |
| Invariant | `TransactionState::begin` returns `TxnError::Nested` and mutates nothing — queue, watch set, target accumulator and `exec_abort` are all left as they were. |
| Outcome variant | n/a (`TransactionControl` stage, before the queue) |
| Forced by | `test_nested_multi`, `multi_begins_and_rejects_nested`, `begin_rejects_nesting_and_take_leaves_state_clean` |
| Bug refs | none |

## FM-TXN-002 — EXEC without MULTI

| Field | Value |
|---|---|
| Trigger | `EXEC` on a connection with no open transaction — never opened, or already consumed by a previous `EXEC`/`DISCARD`/`RESET`. |
| Observable | `-ERR EXEC without MULTI`. |
| NOT observable | An empty array (that is FM-TXN-018's reply and means something different: a transaction *was* open); any shard round-trip; a WATCH set being consumed. |
| Invariant | `handle_exec` short-circuits on `take_transaction() == None` before `execute_transaction` is entered, so no outcome metric is recorded and the watch set is left alone. |
| Outcome variant | n/a (no transaction to execute) |
| Forced by | `test_exec_without_multi`, `transaction_lifecycle_begin_queue_take` |
| Bug refs | none |

## FM-TXN-003 — DISCARD without MULTI

| Field | Value |
|---|---|
| Trigger | `DISCARD` on a connection with no open transaction. |
| Observable | `-ERR DISCARD without MULTI`. |
| NOT observable | `+OK`; the connection's watch set being cleared as a side effect of the error; a pending `ASKING` being consumed. |
| Invariant | The errored `DISCARD` is a no-op on connection state: `asking_survives_discard_without_multi` pins that even the one-shot ASKING flag survives. |
| Outcome variant | n/a |
| Forced by | `test_discard_without_multi`, `discard_without_multi_errors_then_drops_open_transaction`, `asking_survives_discard_without_multi` |
| Bug refs | none |

## FM-TXN-004 — DISCARD inside MULTI drops the queue and the watches

| Field | Value |
|---|---|
| Trigger | `DISCARD` with a transaction open, queued commands present or not, `exec_abort` set or not. |
| Observable | `+OK`. A following `EXEC` answers `-ERR EXEC without MULTI` (FM-TXN-002); a following `MULTI` opens a clean transaction whose EXEC is unaffected by concurrent writes to previously watched keys. |
| NOT observable | Any queued command executing; a surviving `exec_abort` poisoning the *next* transaction; a surviving watch causing a later spurious abort; a surviving sticky `ASKING`. |
| Invariant | `TransactionState::discard` resets queue, watch set, slot accumulator, `exec_abort` and `asking` in one move, and reports the discarded queue length to the metrics recorder under the `discarded` label. |
| Outcome variant | n/a (`DISCARD` never enters `execute_transaction`) |
| Forced by | `test_multi_discard`, `discard_resets_everything_including_watches`, `asking_cleared_by_discard`, `test_multi_exec_cross_slot_returns_error` |
| Bug refs | none |

## FM-TXN-005 — An unknown command inside MULTI poisons the queue

| Field | Value |
|---|---|
| Trigger | A command name the registry does not know, sent while a transaction is open. |
| Observable | `-ERR unknown command …` in place of `+QUEUED`, then `-EXECABORT Transaction discarded because of previous errors.` at `EXEC`. |
| NOT observable | `+QUEUED` for the unknown name; an `EXEC` array in which the unknown command is silently skipped; the commands that queued *before* the bad one taking effect. |
| Invariant | `queue_command` calls `abort_transaction` before returning the error, so `TxnSummary::exec_abort` is true and `execute_transaction` exits at its first gate — nothing reaches a shard, hence nothing reaches the WAL. |
| Outcome variant | `TransactionOutcome::ExecAbort` (label `execabort`) at EXEC time |
| Forced by | `test_unknown_command_in_multi_aborts_the_transaction` |
| Bug refs | none |

## FM-TXN-006 — Wrong arity inside MULTI poisons the queue

| Field | Value |
|---|---|
| Trigger | A known command with an argument count its spec rejects (e.g. bare `GET`), sent while a transaction is open. |
| Observable | `-ERR wrong number of arguments …` in place of `+QUEUED`, then `-EXECABORT …` at `EXEC`. |
| NOT observable | `+QUEUED`; a partial `EXEC` array containing only the well-formed commands; the well-formed commands mutating the keyspace. |
| Invariant | Same gate as FM-TXN-005 — arity is checked at queue time, not at execute time, because `TransactionQueue` precedes `CommandLookup` in `PRE_DISPATCH_ORDER`; that ordering is itself pinned. |
| Outcome variant | `TransactionOutcome::ExecAbort` (label `execabort`) |
| Forced by | `test_transaction_syntax_error_aborts`, `load_bearing_ordering_invariants` |
| Bug refs | none |

## FM-TXN-007 — A pre-execution rejection poisons the queue

| Field | Value |
|---|---|
| Trigger | A queued command rejected by the pre-dispatch gauntlet rather than by the queue itself: `NOPERM` (command, key or channel ACL), `NOREPLICAS` under `min-replicas-to-write`, the replication self-fence, `NOAUTH`, `READONLY`, `CLUSTERDOWN`, or the pub/sub-mode gate. |
| Observable | The gate's own error (`-NOPERM …`, `-NOREPLICAS …`, …) in place of `+QUEUED`, then `-EXECABORT …` at `EXEC`. ACL denials also produce an `ACL LOG` entry with the right `context` (`command` / `key` / `channel`), exactly as on the non-transactional path. |
| NOT observable | `+QUEUED` after a denial; an `EXEC` that runs the permitted commands and quietly drops the denied one; a denial that errors the client but leaves the transaction clean (Redis `flagTransaction` parity). |
| Invariant | The `PreChecks` dispatch stage aborts the open transaction on every rejection path before returning the error, so a rejection can never leave a half-populated queue that a later `EXEC` would treat as complete. |
| Outcome variant | `TransactionOutcome::ExecAbort` (label `execabort`) |
| Forced by | `test_acl_denial_in_multi_poisons_the_transaction`, `test_acl_log_entry_for_each_denial_path_in_multi`, `test_self_fence_multi_rejected_at_queue_time`, `test_min_replicas_to_write_multi_and_lua_paths` |
| Bug refs | `.scratch/replication-cluster-rework/issues/03-lua-internal-write-validation.md` (the same gate is *not* applied to writes a Lua script issues from inside a transaction) |

## FM-TXN-008 — A partially queued transaction aborts wholesale

| Field | Value |
|---|---|
| Trigger | A transaction in which some commands queued successfully and at least one was rejected (any of FM-TXN-005 … FM-TXN-010). |
| Observable | `-EXECABORT …` — a bare error frame. |
| NOT observable | An array reply of any length, in particular a one-element array that looks like partial success; the accepted subset being applied. |
| Invariant | `exec_abort` is a latch: once set at queue time nothing clears it except `DISCARD`/`RESET`. `execute_transaction` checks it before the rate limiter, the validator, the pause barrier and the shard. |
| Outcome variant | `TransactionOutcome::ExecAbort` (label `execabort`) |
| Forced by | `test_self_fence_multi_partial_queue_aborts_whole_transaction`, `exec_abort_when_queuing_poisoned_the_transaction`, `transaction_abort_marks_summary`, `abort_is_reported_in_the_summary_and_discard_clears_watches` |
| Bug refs | none |

## FM-TXN-009 — A queue-time MOVED poisons the queue

| Field | Value |
|---|---|
| Trigger | Cluster mode: a keyed command whose slot this node does not serve, queued inside `MULTI` (including a write queued in a `READONLY` session on a replica). |
| Observable | `-MOVED <slot> <owner>` in place of `+QUEUED`, then `-EXECABORT …` at `EXEC`. |
| NOT observable | `+QUEUED` followed by a successful EXEC on the wrong node — the orphan-write shape this campaign exists to prevent. |
| Invariant | `try_queue_in_transaction` validates the slot *before* the command enters the queue and aborts the transaction on failure. This is the queue-time half; the EXEC-time half (FM-TXN-022) covers topology that moves *after* queuing. |
| Outcome variant | `TransactionOutcome::ExecAbort` (label `execabort`) |
| Forced by | `test_multi_exec_write_inside_readonly_session_returns_moved`, `test_multi_exec_after_completed_slot_migration_redirects_with_moved` |
| Bug refs | none |

## FM-TXN-010 — A single command's own keys crossing slots poisons the queue

| Field | Value |
|---|---|
| Trigger | Cluster mode: one queued command whose *own* key set spans slots (e.g. `RENAME a b` with untagged keys). |
| Observable | `-CROSSSLOT Keys in request don't hash to the same slot` in place of `+QUEUED`, then `-EXECABORT …` at `EXEC`. |
| NOT observable | The bare `CROSSSLOT` at EXEC time — that is FM-TXN-019, and the distinction is load-bearing: a per-command violation is a queuing error, a batch-level violation is an execution error. |
| Invariant | Per-command slot validation runs at queue time; the batch fold runs at EXEC. The two produce different replies from the same wire text and must not be conflated. |
| Outcome variant | `TransactionOutcome::ExecAbort` (label `execabort`) |
| Forced by | `test_multi_exec_cross_slot_returns_error` |
| Bug refs | none |

## FM-TXN-011 — WATCH inside MULTI is rejected

| Field | Value |
|---|---|
| Trigger | `WATCH k` with a transaction already open. |
| Observable | `-ERR WATCH inside MULTI is not allowed`. |
| NOT observable | `+QUEUED`; the key entering the watch set; the transaction being poisoned by the rejection (this error does *not* set `exec_abort` — a subsequent `EXEC` still runs, matching Redis). |
| Invariant | `WATCH` is a `TransactionControl` command, handled before the queue stage, so it is never queued regardless of the connection's state. |
| Outcome variant | n/a |
| Forced by | `test_watch_inside_multi_error`, `watch_inside_multi_is_rejected` |
| Bug refs | none |

## FM-TXN-012 — WATCH with no keys is rejected

| Field | Value |
|---|---|
| Trigger | Bare `WATCH`. |
| Observable | `-ERR wrong number of arguments for 'watch' command`. |
| NOT observable | `+OK` for a watch of nothing; a watch set mutation. |
| Invariant | Arity is enforced in the conn-command executor itself, not only by the registry spec, because `TransactionControl` runs before `CommandLookup`. |
| Outcome variant | n/a |
| Forced by | `watch_without_keys_errors` |
| Bug refs | none |

## FM-TXN-013 — UNWATCH clears the watch set, including inside MULTI

| Field | Value |
|---|---|
| Trigger | `UNWATCH`, with or without an open transaction. |
| Observable | `+OK`, immediately — *not* `+QUEUED`, even inside `MULTI` (**deviation**, see [Redis deviations](#redis-deviations)). A concurrent write to a formerly watched key no longer aborts the following `EXEC`. |
| NOT observable | A stale cross-shard fold surviving the unwatch: after `WATCH {a}x` + `WATCH {b}y` + `UNWATCH`, a single-shard `EXEC` must commit rather than answer `CROSSSLOT`. |
| Invariant | `unwatch_all` clears the watch set *and* the watch-derived half of the slot accumulator, so `take` re-folds from the queue alone. |
| Outcome variant | n/a |
| Forced by | `test_unwatch`, `unwatch_is_ok_and_clears_watches`, `test_unwatch_in_multi_clears_stale_cross_shard_watch_fold`, `unwatch_drops_the_stale_cross_shard_fold`, `take_transaction_unwatch_drops_stale_cross_shard_watch_fold` |
| Bug refs | none |

## FM-TXN-014 — RESET clears the transaction, the watches and ASKING

| Field | Value |
|---|---|
| Trigger | `RESET` at any point, including mid-`MULTI` with commands queued. |
| Observable | `+RESET`. A following `MULTI` returns `+OK` on a clean transaction; the queued commands never ran; a concurrent write to a formerly watched key does not abort the next `EXEC`. |
| NOT observable | Queued commands executing on the way out; a surviving watch, `exec_abort` latch, or sticky `ASKING`. |
| Invariant | `clear_transaction` is the QUIT/RESET seam: it drops the queue without running it and clears every transaction-scoped flag. |
| Outcome variant | n/a |
| Forced by | `test_reset_aborts_transaction`, `test_reset_clears_watches`, `test_reset_restores_clean_state`, `asking_cleared_by_clear_transaction`, `reset_clears_covered_state` |
| Bug refs | none |

## FM-TXN-015 — ASKING is sticky for the whole MULTI block and consumed by EXEC

| Field | Value |
|---|---|
| Trigger | Cluster mode: `ASKING` immediately before `MULTI`, then a batch whose slot is importing on this node. |
| Observable | Every queued command and the EXEC-time batch validation are evaluated *with* `asking` set, so the import target serves the batch instead of answering `-MOVED`. The next command after `EXEC` is evaluated without it. |
| NOT observable | ASKING being consumed by the first queued command and silently dropped for the rest of the block; ASKING surviving `EXEC`, `DISCARD` or `RESET` into the next command. |
| Invariant | Inside a transaction the flag is *read* without clearing; `take_transaction` moves it into `TxnSummary::asking`, which `execute_transaction` hands to `validate_queued_batch` verbatim. |
| Outcome variant | n/a at queue time; the flag rides into `Committed` / `Redirected` |
| Forced by | `asking_is_sticky_inside_multi_and_consumed_by_exec`, `asking_is_one_shot`, `asking_absent_inside_multi_stays_absent`, `test_multi_exec_on_import_target_with_asking_serves_the_batch`, `batch_on_import_target_without_asking_is_moved` |
| Bug refs | none |

## FM-TXN-016 — EXEC on a poisoned transaction (EXECABORT)

| Field | Value |
|---|---|
| Trigger | `EXEC` with `exec_abort` latched by any of FM-TXN-005 … FM-TXN-010. |
| Observable | Exactly `-EXECABORT Transaction discarded because of previous errors.` (one bare error frame). |
| NOT observable | Any array; any shard round-trip, redirect probe, rate-limiter charge, or pause wait — the abort gate is the algorithm's first branch, so a poisoned EXEC costs nothing and cannot block. |
| Invariant | `host.effects` is empty on this path: no `Validate`, no `WaitIfPaused`, no `ShardRoundTrip`. The transaction is consumed regardless (FM-TXN-047). |
| Outcome variant | `TransactionOutcome::ExecAbort` (label `execabort`) |
| Forced by | `exec_abort_when_queuing_poisoned_the_transaction` |
| Bug refs | none |

## FM-TXN-017 — EXEC refused by the rate limiter

| Field | Value |
|---|---|
| Trigger | `EXEC` whose whole queue cannot be charged to the connection's user: the commands-per-second or the bytes-per-second dimension is exhausted. |
| Observable | A bare error naming the dimension: `-ERR rate limit exceeded: commands per second` or `-ERR rate limit exceeded: bytes per second`. |
| NOT observable | A generic "rate limited" message that hides which dimension tripped; a partially charged batch; any command executing. Admin, unauthenticated and unlimited users are never charged at all. |
| Invariant | `try_acquire_batch` is all-or-nothing for the queue and runs *before* validation, the pause barrier and the shard — a throttled EXEC performs no work and no effects are recorded. |
| Outcome variant | `TransactionOutcome::RateLimited` (label `ratelimited`) |
| Forced by | `rate_limited_names_the_dimension_that_was_exceeded` |
| Bug refs | none |

## FM-TXN-018 — EXEC of an empty queue

| Field | Value |
|---|---|
| Trigger | `MULTI` then `EXEC` with nothing queued and no watches. |
| Observable | An empty array (`*0`). |
| NOT observable | A nil; an error; any shard round-trip or slot validation — an empty EXEC is never redirected, never `CROSSSLOT`, never rate-limit-refused after the queue check. |
| Invariant | The empty-queue exit is taken *before* `validate_queued_batch`, so an empty transaction commits identically in standalone and cluster mode, on any node, mid-migration. A *watched* empty queue is the exception and is FM-TXN-034. |
| Outcome variant | `TransactionOutcome::CommittedEmpty` (label `committed`) |
| Forced by | `test_multi_exec_empty`, `committed_empty_answers_an_empty_array_without_touching_a_shard` |
| Bug refs | none |

## FM-TXN-019 — EXEC of a batch that folded to more than one shard

| Field | Value |
|---|---|
| Trigger | `EXEC` whose queued commands' keys fold to `TransactionTarget::Multi` — different slots in cluster mode, different shards in standalone. |
| Observable | Exactly `-CROSSSLOT Keys in request don't hash to the same slot`, a bare error frame. |
| NOT observable | `-EXECABORT` (the queue was well-formed; each command queued fine); an array; any command executing on either shard; a cross-shard transaction "succeeding" through the VLL path — transactions are deliberately denied the cross-shard atomicity single ops get, because there is no cross-shard rollback story. |
| Invariant | `TransactionTarget::resolve` maps `Multi` to the redirect seam's `crossslot()` unconditionally, without consulting any config; the wire text has exactly one owner (`frogdb_types::redirect::CROSSSLOT_MSG`). |
| Outcome variant | `TransactionOutcome::CrossSlot` (label `crossslot`) |
| Forced by | `cross_slot_when_the_queue_folded_to_more_than_one_shard`, `test_multi_exec_two_single_key_commands_different_slots_defers_crossslot_to_exec`, `test_multi_cross_shard_plain_keys_crossslot_default_config`, `transaction_target_resolve_maps_multi_to_crossslot`, `fold_keys_promotes_on_slot_mismatch_in_cluster_mode`, `batch_spanning_two_slots_is_crossslot` |
| Bug refs | none |

## FM-TXN-020 — A cross-shard WATCH set alone forces CROSSSLOT

| Field | Value |
|---|---|
| Trigger | `WATCH {a}x` + `WATCH {b}y` (different shards), then a transaction whose *queued commands* all target a single shard. |
| Observable | `-CROSSSLOT …` at `EXEC`, not a commit. |
| NOT observable | A commit that version-checked only the command-target shard and ignored the other watched shard — a silent WATCH false negative (this was a real bug; the regression is pinned in the generated-workload suite). |
| Invariant | `take` folds every *live* watched shard into the target before resolving, so the watch set can promote a `Single` target to `Multi`. Unwatched/stale shards must not (FM-TXN-013). |
| Outcome variant | `TransactionOutcome::CrossSlot` (label `crossslot`) |
| Forced by | `cross_shard_watch_set_folds_to_multi_at_take`, `take_transaction_folds_cross_shard_watch_set_to_multi` |
| Bug refs | `.scratch/replication-cluster-rework/issues/04-watch-slot-validation.md` (done — WATCH keys are slot-validated at WATCH time, FM-TXN-048, and the watch set is re-checked at EXEC, FM-TXN-049; deliberately *not* folded into the queue's `BatchKeys`, which would CROSSSLOT a legitimate two-slot watch set) |

## FM-TXN-021 — allow-cross-slot-standalone does not relax transactions

| Field | Value |
|---|---|
| Trigger | Standalone with `allow_cross_slot_standalone = true` (cross-shard single-key ops permitted via VLL), then a cross-shard `MULTI`. |
| Observable | `-CROSSSLOT …` — identical to the flag being off. A single-shard transaction on the same server still commits normally. |
| NOT observable | The flag leaking into `fold_transaction_keys` or `resolve` and permitting a non-atomic, non-rollbackable cross-shard transaction. |
| Invariant | The transaction fold never reads the config. VLL gives cross-shard *ops* execution atomicity; that is deliberately withheld from transactions. |
| Outcome variant | `TransactionOutcome::CrossSlot` (label `crossslot`) |
| Forced by | `test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone`, `test_multi_cross_shard_crossslot_with_flag_disabled`, `test_multi_single_shard_commits_with_allow_cross_slot_standalone` |
| Bug refs | `.scratch/testing-improvements/issues/19-cross-slot-standalone-multi-invariant.md` (done — these tests are its outcome) |

## FM-TXN-022 — EXEC after the slot moved (MOVED at execute time)

| Field | Value |
|---|---|
| Trigger | Cluster mode: every command queued while this node owned the slot, then the slot migration completes before `EXEC`. |
| Observable | A bare `-MOVED <slot> <new owner>`, *not* an array and *not* `EXECABORT`. |
| NOT observable | The batch committing on the ex-owner — the orphan `MULTI…EXEC` that would replicate to the wrong node's followers. The ex-owner's `DBSIZE` stays 0. |
| Invariant | `EXEC` re-reads the cluster snapshot and re-folds the queued batch; the verdict short-circuits before the shard round-trip, so nothing reaches the WAL. The queue is consumed either way (FM-TXN-047). |
| Outcome variant | `TransactionOutcome::Redirected` (label `redirected`) |
| Forced by | `test_multi_exec_after_completed_slot_migration_redirects_with_moved`, `redirected_returns_the_bare_redirect_not_an_array`, `batch_on_foreign_slot_is_moved_to_the_owner` |
| Bug refs | `.scratch/replication-cluster-rework/issues/01-exec-slot-table-version-fast-path.md`, `.scratch/replication-cluster-rework/issues/02-migration-finalization-pause-barrier.md` (residual commit/apply window) |

## FM-TXN-023 — EXEC during a migration whose keys have already moved (ASK)

| Field | Value |
|---|---|
| Trigger | Cluster mode, slot `MIGRATING` on this node, and the presence probe finds *none* of the batch's keys still here. |
| Observable | A bare `-ASK <slot> <importing node>`. |
| NOT observable | The batch executing on the source and resurrecting keys that were already handed over; a `MOVED` (the slot is still formally owned here). |
| Invariant | The decision is driven by key *presence*, not by the migration flag alone — one snapshot for the whole batch, taken once per EXEC. |
| Outcome variant | `TransactionOutcome::Redirected` (label `redirected`) |
| Forced by | `test_multi_exec_during_in_flight_slot_migration_asks_when_keys_migrated`, `batch_on_migrating_source_probes_with_the_ask_target` |
| Bug refs | none |

## FM-TXN-024 — EXEC of a batch split across a migrating slot (TRYAGAIN)

| Field | Value |
|---|---|
| Trigger | Cluster mode, slot `MIGRATING`, and the probe finds some of the batch's keys here and some already moved. |
| Observable | A bare `-TRYAGAIN Multiple keys request during rehashing of slot`. |
| NOT observable | A partial execution against the keys that happen to be local; an `ASK` that would send the whole batch to a node holding only half of it. |
| Invariant | Mixed presence is unserviceable by construction — the client retries after the migration settles. Nothing is applied. |
| Outcome variant | `TransactionOutcome::Redirected` (label `redirected`) |
| Forced by | `test_multi_exec_during_migration_with_split_keys_returns_tryagain` |
| Bug refs | none |

## FM-TXN-025 — EXEC on an unassigned slot (CLUSTERDOWN)

| Field | Value |
|---|---|
| Trigger | Cluster mode: at `EXEC` time the batch's slot has no owner in the snapshot. |
| Observable | A bare `-CLUSTERDOWN Hash slot <slot> not served`. |
| NOT observable | The batch serving locally because this node used to own the slot; a `READONLY` session rescuing an unassigned slot. |
| Invariant | The unassigned arm is checked before the readonly-eligibility rescue, so no session flag can serve a slot the cluster does not assign. |
| Outcome variant | `TransactionOutcome::Redirected` (label `redirected`) |
| Forced by | `batch_on_unassigned_slot_is_clusterdown`, `batch_readonly_never_rescues_an_unassigned_slot` |
| Bug refs | none |

## FM-TXN-026 — EXEC-time validation fails closed

| Field | Value |
|---|---|
| Trigger | The EXEC-time key-presence probe cannot reach the shard (channel closed, request dropped) during an open migration. |
| Observable | A bare `-ERR shard unavailable`. |
| NOT observable | The batch being served locally on a guess; an `ASK`/`MOVED` invented from an unknown presence result; an array. |
| Invariant | `probe_key_presence` fails closed: an unknown answer is a refusal, never an optimistic serve. Any non-`None` verdict — redirect *or* error — short-circuits `execute_transaction` with one bare frame and no shard round-trip, and is filed under `Redirected` because it came from the redirect gate. |
| Outcome variant | `TransactionOutcome::Redirected` (label `redirected`) |
| Forced by | `a_validation_verdict_that_is_a_plain_error_short_circuits_the_same_way` |
| Bug refs | none |

## FM-TXN-027 — A migration flag alone is not a refusal

| Field | Value |
|---|---|
| Trigger | Cluster mode, slot `MIGRATING` on this node, but the probe finds every one of the batch's keys still local. |
| Observable | The batch commits normally: an array of per-command results. |
| NOT observable | A gratuitous `ASK`/`TRYAGAIN` for a batch that is perfectly serviceable here — the permissive half of the contract, which a mutant that hard-refuses on the flag would violate silently. |
| Invariant | Presence decides; the flag only selects which probe to run. An unknown migration target likewise degrades to serving locally rather than redirecting into the void. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `test_multi_exec_during_migration_serves_when_keys_still_local`, `batch_on_migrating_source_with_unknown_target_serves_locally` |
| Bug refs | none |

## FM-TXN-028 — READONLY eligibility is decided for the whole batch

| Field | Value |
|---|---|
| Trigger | Cluster mode, `READONLY` session on a replica, `MULTI` over keys this node does not own. |
| Observable | An all-reads batch commits locally; adding a single write to the same batch makes the *whole* batch `MOVED` at queue time and `EXECABORT` at `EXEC`. |
| NOT observable | A mixed batch serving its reads locally and its writes anywhere; `READONLY` rescuing a scatter-gather write (see FM-TXN-030). |
| Invariant | Readonly eligibility is a property of the folded batch, not of individual commands — one verdict per EXEC. |
| Outcome variant | `TransactionOutcome::Committed` (eligible) / `ExecAbort` (ineligible, rejected at queue time) |
| Forced by | `test_multi_exec_readonly_batch_eligibility_is_all_or_nothing`, `test_multi_exec_reads_succeed_on_replica_with_readonly`, `batch_readonly_eligible_serves_a_foreign_slot_locally`, `batch_readonly_ineligible_when_it_contains_a_write` |
| Bug refs | none |

## FM-TXN-029 — Keyless batches are never redirected

| Field | Value |
|---|---|
| Trigger | A transaction of node-scoped/keyless commands only (`PING`, `INFO`, …) on any node, any topology. |
| Observable | The batch commits locally and returns its array. |
| NOT observable | A `MOVED`/`CROSSSLOT` for a batch that names no key; a keyless batch being attributed to some arbitrary slot. |
| Invariant | An empty key set means "serve local" — but only when the key set is *genuinely* empty, which is exactly the trap FM-TXN-030 covers. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `test_keyless_multi_exec_is_never_redirected`, `batch_with_no_keyed_command_serves_locally` |
| Bug refs | none |

## FM-TXN-030 — Scatter and script batches are slot-validated like any other

| Field | Value |
|---|---|
| Trigger | A transaction containing a scatter-gather command (`MSET`-shaped) or an `EVAL` with declared `KEYS`, on a node that no longer owns the slot. |
| Observable | A bare `-MOVED …` at `EXEC`, exactly as for a plain keyed batch. |
| NOT observable | The command's keys being invisible to the fold (an empty key set → keyless fast path → local serve), which produced an orphan `MULTI…EXEC` replicated on the ex-owner. `READONLY` must not rescue such a write either. |
| Invariant | Key extraction for the fold covers scatter key specs and script-declared keys; the keyless fast path is only reachable when no command in the batch names a key. |
| Outcome variant | `TransactionOutcome::Redirected` (label `redirected`) |
| Forced by | `test_multi_exec_scatter_gather_batch_is_slot_validated`, `test_multi_exec_eval_with_declared_keys_is_slot_validated`, `test_multi_exec_readonly_does_not_rescue_a_scatter_write` |
| Bug refs | `.scratch/replication-cluster-rework/issues/03-lua-internal-write-validation.md` (a script's *undeclared* runtime writes are still unvalidated) |

## FM-TXN-031 — The shard rejects the transaction

| Field | Value |
|---|---|
| Trigger | The shard answers `TransactionResult::Error(msg)` — it refused to run the batch. |
| Observable | The shard's own message as a bare error frame, verbatim. |
| NOT observable | An array with an error inside it (that is FM-TXN-036 and means the batch *did* run); the message being rewritten, prefixed, or replaced by a generic one. |
| Invariant | A shard-level error means nothing in the batch took effect; the connection does not retry or fall back to another shard. |
| Outcome variant | `TransactionOutcome::Error` (label `error`) |
| Forced by | `error_when_the_shard_reports_one` |
| Bug refs | none |

## FM-TXN-032 — The shard is gone mid-EXEC

| Field | Value |
|---|---|
| Trigger | The shard's command channel is closed (send fails) or the reply oneshot is dropped without an answer — shard death, shutdown, or supervisor restart between EXEC entry and the reply. |
| Observable | `-ERR shard unavailable` (channel closed) or `-ERR shard dropped request` (reply dropped). Two distinct messages: they distinguish "never accepted" from "accepted, fate unknown". |
| NOT observable | A hang; a fabricated success; an empty array that a client would read as a committed empty transaction; the two cases collapsing into one message. |
| Invariant | `ShardTxnReply::Unavailable` and `::Dropped` are separate variants all the way through the seam, and both map to the `Error` outcome — no path invents a `Committed`. |
| Outcome variant | `TransactionOutcome::Error` (label `error`) |
| Forced by | `error_when_the_shard_channel_is_closed_or_the_request_is_dropped` |
| Bug refs | none |

## FM-TXN-033 — A watched key changed (WATCH abort)

| Field | Value |
|---|---|
| Trigger | Any watched key's version moved between `WATCH` and `EXEC` — another client's write, a Lua script's write, a lazy expiry of a key that was live at `WATCH` time. |
| Observable | A nil reply: `Response::Bulk(None)` → `$-1` in RESP2 (**deviation**: Redis sends `*-1`), `_` in RESP3. |
| NOT observable | An array of any length; any queued command taking effect; the watch surviving into the next transaction. A *no-op* write must not abort (the version only moves on real mutation) and a spill to the warm tier must not either. |
| Invariant | The version check happens shard-side, before the first command runs, and the whole batch is refused atomically. The transaction and the watch set are consumed regardless. |
| Outcome variant | `TransactionOutcome::WatchAborted` (label `watch_aborted`) |
| Forced by | `watch_aborted_answers_nil`, `test_watch_exec_abort`, `test_scripted_write_dirties_watch`, `test_watch_exec_success` |
| Bug refs | `.scratch/replication-cluster-rework/issues/04-watch-slot-validation.md` (done — the CAS is now taken only on a node that owns the watched slot: FM-TXN-048, FM-TXN-049) |

## FM-TXN-034 — A watched transaction with nothing to run still version-checks

| Field | Value |
|---|---|
| Trigger | `WATCH k`, then a transaction whose queue is empty of shard commands — either genuinely empty or entirely connection-level/server-wide (`CONFIG GET`, `KEYS`, …). |
| Observable | The nil abort if the watched key changed; the deferred commands' replies in an array if it did not. |
| NOT observable | An empty-queue fast path that skips the shard round-trip and commits a transaction whose CAS precondition was already broken — a silent WATCH false negative. |
| Invariant | The shard round-trip is taken with an empty command list whenever the watch set is non-empty; it is skipped only when there is nothing to run *and* nothing to check. |
| Outcome variant | `TransactionOutcome::WatchAborted` / `Committed` |
| Forced by | `an_all_deferred_queue_with_watches_still_takes_the_shard_round_trip`, `test_watch_with_only_connection_level_commands_abort`, `test_watch_with_only_connection_level_commands_success` |
| Bug refs | none |

## FM-TXN-035 — EXEC commits

| Field | Value |
|---|---|
| Trigger | A well-formed, single-target, unwatched-or-clean transaction that the shard executes. |
| Observable | Exactly one array frame, one element per queued command, in queue order, each element the command's own reply. |
| NOT observable | Extra frames beside the array; results reordered or coalesced; a shorter array than the queue (a dropped command); anything pushed to the connection before the array that a client would read as the EXEC reply. |
| Invariant | The batch is applied by the shard as one unit against one shard's store, and the connection emits `vec![Response::Array(results)]` plus any out-of-band pushes *after* it. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `committed_returns_the_shard_results_in_an_array`, `test_multi_exec_basic`, `test_transaction_increments` |
| Bug refs | none |

## FM-TXN-036 — A command that fails inside a committed transaction

| Field | Value |
|---|---|
| Trigger | A queued command that queues cleanly but errors at run time (e.g. `LPUSH` against a string). |
| Observable | The batch still commits: an array whose element for the failing command is the error (`-WRONGTYPE …`) and whose other elements are normal replies. The other commands' effects are durable. |
| NOT observable | `EXECABORT` (nothing was wrong at queue time); a bare error instead of the array; the surrounding commands being rolled back — Redis has no rollback on runtime errors and neither does FrogDB. |
| Invariant | Run-time errors are values inside the array, not control flow. Only a WAL failure changes this, and it fills *every* slot with an EXECABORT-shaped error rather than tearing the batch. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `test_transaction_with_error`, `test_multi_exec_ft_search_unknown_index_errors` |
| Bug refs | `.scratch/concurrency-testing/issues/06-durability-txn-framing-abort-on-recovery.md` (WAL-failure framing on recovery) |

## FM-TXN-037 — Deferred commands reply at their queued positions

| Field | Value |
|---|---|
| Trigger | A transaction interleaving shard commands with connection-level ones (`CONFIG GET`, `HOTKEYS`, `FT.CURSOR`, …). |
| Observable | One array, in queue order, with each deferred command's reply at its own index — the deferred replies are merged back into the gaps, not appended. |
| NOT observable | Deferred replies bunched at the end or the front; an array whose length differs from the queue; a shard reply landing at a deferred index. |
| Invariant | The partition records each deferred command's original index and the merge writes results back by index, so reply position is independent of execution order. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `deferred_replies_land_at_their_queued_positions`, `test_transaction_connection_level_merge_order`, `test_transaction_conn_command_hotkeys_ftcursor_execute` |
| Bug refs | none |

## FM-TXN-038 — An all-deferred, unwatched transaction never reaches a shard

| Field | Value |
|---|---|
| Trigger | Every queued command is connection-level or server-wide, and the watch set is empty. |
| Observable | An array of the deferred replies. |
| NOT observable | A shard round-trip with an empty command list (pointless work, and in cluster mode a pointless redirect risk); a `CROSSSLOT` for a batch that names no shard. |
| Invariant | The shard is contacted only when there is something to execute or something to version-check — the exact complement of FM-TXN-034. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `an_all_deferred_queue_without_watches_skips_the_shard_entirely` |
| Bug refs | none |

## FM-TXN-039 — Server-wide commands run after the shard batch and are not atomic with it

| Field | Value |
|---|---|
| Trigger | A transaction mixing shard writes with server-wide commands (`KEYS`, `SCAN`, `FLUSHDB`, `DBSIZE`) that fan out to every shard. |
| Observable | One array in queue order. A server-wide read placed *after* a write in the queue observes that write; `FLUSHDB` inside the transaction clears all shards; `SCAN` returns its full cursor reply shape unchanged by being in a transaction. |
| NOT observable | Server-wide replies that predate the transaction's own writes; a claim of atomicity — a fan-out command is a sequence of per-shard operations that another client can interleave with. |
| Invariant | Deferred commands run after the shard round-trip, deliberately outside its atomicity envelope. This is a documented property of the seam, not an accident. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `test_multi_exec_server_wide_reply_ordering`, `test_multi_exec_keys_spans_all_shards`, `test_multi_exec_flushdb_clears_all_shards`, `test_multi_exec_scan_returns_full_cursor_reply` |
| Bug refs | none |

## FM-TXN-040 — A write transaction waits at the pause barrier and re-validates after it

| Field | Value |
|---|---|
| Trigger | `CLIENT PAUSE … WRITE` (or a full pause) in effect when a write transaction reaches `EXEC`. |
| Observable | The `EXEC` reply is withheld until the pause lifts, then the batch commits (`*1\r\n+OK\r\n` for a single `SET`); concurrent reads keep answering and do not see the pending write. |
| NOT observable | The batch committing during the pause; the batch committing after the pause on a *stale* slot verdict — the topology may have moved while the transaction was parked, so the batch is validated a second time when the barrier actually blocked. |
| Invariant | Exactly two `validate_queued_batch` calls when the barrier blocked, exactly one when it did not. The first verdict is never reused across a wait. |
| Outcome variant | `TransactionOutcome::Committed`, or any redirect outcome if the second verdict refuses |
| Forced by | `a_blocking_pause_forces_a_second_slot_verdict`, `a_non_blocking_pause_keeps_the_batch_at_exactly_one_slot_verdict` |
| Bug refs | `.scratch/replication-cluster-rework/issues/02-migration-finalization-pause-barrier.md` |

## FM-TXN-041 — A read-only transaction never reaches the pause barrier

| Field | Value |
|---|---|
| Trigger | `CLIENT PAUSE … WRITE` in effect, `EXEC` of a queue containing no writes. |
| Observable | The batch commits immediately. |
| NOT observable | A read-only transaction being parked by a write pause — that would turn a failover-window pause into a read outage. |
| Invariant | `queue_has_writes` gates the barrier; a batch with no writes skips `wait_if_paused` entirely (no `WaitIfPaused` effect) and therefore also skips the second validation. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `a_read_only_batch_never_reaches_the_pause_barrier` |
| Bug refs | none |

## FM-TXN-042 — A transaction that folded to no target runs on the connection's own shard

| Field | Value |
|---|---|
| Trigger | `EXEC` whose queue names no key at all, so the accumulator is still `TransactionTarget::None`, but which must still execute (keyless shard commands, or an empty queue with watches). |
| Observable | The batch commits. |
| NOT observable | A panic, a `CROSSSLOT`, or an arbitrary shard 0 — the round-trip goes to `host.shard_id()`, the shard this connection is bound to. |
| Invariant | `None` resolves to the connection's own shard; only `Multi` is an error. This keeps keyless batches and watch-only batches on a deterministic shard. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `an_unfolded_target_falls_back_to_the_connections_own_shard`, `accumulator_shard_fold_none_single_multi` |
| Bug refs | none |

## FM-TXN-043 — Subscribe-family commands execute inside MULTI

| Field | Value |
|---|---|
| Trigger | `SUBSCRIBE`/`PSUBSCRIBE`/`UNSUBSCRIBE`/`PUNSUBSCRIBE`/`SUNSUBSCRIBE`/`PUBSUB` queued inside `MULTI`, in RESP2 or RESP3. |
| Observable | `+QUEUED` at queue time and a real subscription at `EXEC`. In RESP2 the confirmation is an `Array` nested inside the EXEC array; in RESP3 confirmations are `Push` frames delivered around the EXEC reply. `SSUBSCRIBE` is the one member genuinely rejected inside `MULTI`, with Redis's exact error text. |
| NOT observable | A bespoke "not allowed in MULTI" refusal for the allowed members (FrogDB used to reject `SUNSUBSCRIBE` and `PUBSUB`); a confirmation shape that differs from the direct path; the RESP3 push being folded into the array. |
| Invariant | Queue eligibility is decided by the command's spec, not by an ad-hoc list; the subscribe confirmation encoder is shared with the non-transactional path. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `test_subscribe_confirmation_in_multi_exec_resp2`, `test_subscribe_confirmation_in_multi_exec_resp3`, `test_ssubscribe_inside_multi_rejected`, `test_subscribe_inside_multi_executes`, `test_unsubscribe_inside_multi_executes`, `test_pubsub_inside_multi_executes` |
| Bug refs | none |

## FM-TXN-044 — Blocking and socket-handoff commands never block inside MULTI

| Field | Value |
|---|---|
| Trigger | `WAIT` (any quorum/timeout) or `PSYNC` queued inside `MULTI`. |
| Observable | `WAIT` returns the current acked-replica count immediately, even with an unsatisfiable quorum and a non-zero timeout; `PSYNC` replies `+OK` and the connection stays request/reply. |
| NOT observable | The `EXEC` blocking for the timeout (Redis semantics: blocking commands inside `MULTI` return their immediate answer); `PSYNC` handing off the socket to the replication stream from inside a transaction; a `WAIT` interceptor running on the transactional path. |
| Invariant | Inside a transaction these commands take their non-blocking branch — the blocking interceptors sit on the direct dispatch path, which `EXEC` does not use. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `test_wait_inside_multi_returns_count_immediately`, `test_wait_inside_multi_nonzero_timeout_does_not_block`, `test_psync_inside_multi_replies_ok` |
| Bug refs | none |

## FM-TXN-045 — A null array inside the EXEC array stays a nested null

| Field | Value |
|---|---|
| Trigger | A queued command whose reply is a null array (RESP2 `*-1`), inside a committed transaction. |
| Observable | The EXEC array contains a nested null-array element; the codec's top-level null-array diversion does not apply to it. |
| NOT observable | The nested null being flattened to `$-1`, promoted to the top level, or truncating the array. |
| Invariant | Nesting depth is preserved by the encoder; the RESP2 top-level `*-1` special case is a property of the outermost frame only. |
| Outcome variant | `TransactionOutcome::Committed` (label `committed`) |
| Forced by | `test_exec_nested_null_array_encodes_as_nested_null_resp2` |
| Bug refs | none |

## FM-TXN-046 — Every EXEC exit is counted, under a stable label

| Field | Value |
|---|---|
| Trigger | Any `EXEC`, on any path above. |
| Observable | Exactly one transaction-outcome metric sample per `EXEC`, labelled `execabort`, `ratelimited`, `crossslot`, `redirected`, `error`, `watch_aborted` or `committed` (both `Committed` and `CommittedEmpty` map to `committed`); `DISCARD` records `discarded`. Per-command keyspace hit/miss metrics are counted inside a transaction exactly as on the direct path. |
| NOT observable | An `EXEC` that records nothing; two samples for one `EXEC`; a renamed label breaking existing dashboards and alerts silently. |
| Invariant | `handle_exec` is the single place the outcome metric is recorded, and the label map is exhaustive over the enum (no wildcard arm), so a new variant fails compilation rather than defaulting to some existing bucket. |
| Outcome variant | all |
| Forced by | `outcome_metric_labels_are_stable`, `every_outcome_variant_has_a_forcing_test`, `test_keyspace_metrics_counted_inside_transaction`, `stage_error_disposition_is_the_guard_dispatch_split` |
| Bug refs | none |

## FM-TXN-047 — EXEC consumes the transaction on every exit path

| Field | Value |
|---|---|
| Trigger | Any `EXEC`: committed, aborted, redirected, rate-limited, errored. |
| Observable | A second bare `EXEC` immediately after answers `-ERR EXEC without MULTI`. |
| NOT observable | A retryable transaction left open after a redirect or an error, which a client could re-`EXEC` and double-apply; a `EXEC`-carrying spec fabricating a `+OK` when dispatched outside `handle_exec`. |
| Invariant | `take_transaction` runs once, at EXEC entry, before any gate can fail — the queue, the watch set and the sticky ASKING are consumed whatever the outcome. The registry's `EXEC` executor is a spec carrier that never runs: it `debug_assert`s in debug and returns `ERR internal: EXEC must be dispatched via handle_exec` in release. |
| Outcome variant | all |
| Forced by | `exec_spec_carrier_execute_never_fabricates_success`, `test_multi_exec_after_completed_slot_migration_redirects_with_moved`, `transaction_lifecycle_begin_queue_take` |
| Bug refs | none |

## FM-TXN-048 — WATCH is slot-validated like any other keyed command

| Field | Value |
|---|---|
| Trigger | Cluster mode: `WATCH` naming a key whose slot this node does not own (migrated away, never assigned, or owned by a peer), or naming keys that span two slots. |
| Observable | A bare `-MOVED <slot> <owner>` (or `-CLUSTERDOWN Hash slot <slot> not served` when the slot has no owner, `-CROSSSLOT …` when the keys span slots). The watch is **not** recorded. |
| NOT observable | `+OK` for a CAS registration against a slot this node does not serve — the client would believe it holds a watch that no writer on the real owner can ever dirty, i.e. a CAS that silently never fires. An open migration (`MIGRATING`/`IMPORTING`) must **not** refuse: the watch is still serviceable here and the EXEC-time probe (FM-TXN-049) owns that decision. `WATCH` must also not consume the one-shot `ASKING` flag the following `EXEC` needs. |
| Invariant | `WATCH` short-circuits at the `TransactionControl` dispatch stage (position 5), *before* `ClusterSlotValidation` (position 14) is ever reached, so the stage gauntlet structurally cannot cover it — the verdict is taken in the stage itself, against one `ClusterState::snapshot`, through the same `route_with_snapshot` seam every keyed command uses. `WATCH` inside `MULTI` keeps its own rejection (FM-TXN-011), which outranks any slot verdict. |
| Outcome variant | n/a (`TransactionControl` stage, before any transaction exists) |
| Forced by | `watch_on_a_foreign_slot_is_refused_with_moved`, `watch_across_two_slots_is_crossslot`, `watch_on_an_open_migration_is_accepted`, `watch_on_an_unassigned_slot_is_clusterdown`, `test_watch_on_a_slot_this_node_does_not_own_is_moved` |
| Bug refs | `.scratch/replication-cluster-rework/issues/04-watch-slot-validation.md` (done — these tests are its outcome) |

## FM-TXN-049 — A watched key whose slot left this node fails the CAS at EXEC

| Field | Value |
|---|---|
| Trigger | Cluster mode: `WATCH {S}k` while this node owns slot `S`, then `S` changes hands (a migration completes, the slot is reassigned, or it is left unassigned), then `MULTI` … `EXEC` — including a `MULTI` whose queued body names no key of its own (`PING`, `INFO`, …). |
| Observable | The nil abort (`*-1`), exactly as if a writer had touched `{S}k`: `TransactionOutcome::WatchAborted`, label `watch_aborted`. The client's ordinary retry loop re-issues `WATCH`, which now answers `-MOVED` (FM-TXN-048) and sends it to the owner. |
| NOT observable | The batch committing because the queue folded no key of the departed slot — the CAS decision would be taken against this node's stale, no-longer-owned copy of `{S}k` while the real owner serves writes to it, an undetectable WATCH false negative. Also **not** observable: a `-CROSSSLOT` for a watch set legitimately spanning two slots this node owns (only the *queue* is co-location-constrained, FM-TXN-019 — watch sets are not), nor a `-MOVED` naming a slot the queued batch never touches. |
| Invariant | EXEC re-checks every watched key's slot through the same seam the queue uses — `route_with_snapshot` on one snapshot — and requires a local-serve arm (`LocalServe` / `LocalServeMigrating` / `AcceptImporting`). An *open* migration stays serviceable on purpose: `MIGRATE`'s delete on the source bumps the watched key's version, so the ordinary version check already fires; only losing the slot outright makes that version unobservable. The queue's own verdict (FM-TXN-022/025, `validate_queued_batch`) is taken first, so a redirect the batch itself earns outranks the abort. |
| Outcome variant | `TransactionOutcome::WatchAborted` (label `watch_aborted`) |
| Forced by | `a_watched_slot_that_left_this_node_aborts_the_watch`, `a_queue_redirect_outranks_the_watched_slot_abort`, `watch_slot_locally_served_accepts_an_open_migration`, `watch_slot_locally_served_rejects_a_slot_owned_elsewhere`, `test_watch_then_slot_reassignment_then_keyless_exec_aborts_the_watch` |
| Bug refs | `.scratch/replication-cluster-rework/issues/04-watch-slot-validation.md` (done — these tests are its outcome) |

---

## Redis deviations

Deliberate, known differences from Redis 8.x semantics. Each is pinned by the tests named above,
so a change here is a visible spec edit rather than a silent drift.

| Mode | FrogDB | Redis | Rationale |
|---|---|---|---|
| FM-TXN-033 | WATCH-aborted `EXEC` replies `$-1` (null bulk) in RESP2 | `*-1` (null array) | Both decode as "nil" in every mainstream client, and RESP3 (`_`) is identical. Recorded rather than fixed because the wire shape is observable by raw-protocol clients; changing it is a separate, testable change. |
| FM-TXN-013 | `UNWATCH` inside `MULTI` executes immediately and replies `+OK` | `UNWATCH` is queued like any other command | FrogDB routes `UNWATCH` through the `TransactionControl` stage with the rest of the transaction verbs. Redis exempts only `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`QUIT`/`RESET`. The practical effect is the same for the common "cancel my CAS" usage, but a client that queues `UNWATCH` expecting it to run at `EXEC` sees it run earlier. |
| FM-TXN-019, FM-TXN-021 | A shard-spanning transaction is refused with `CROSSSLOT` even in standalone | Standalone Redis has one keyspace and no such refusal | FrogDB is sharded in standalone too; transactions have no cross-shard rollback story, so the refusal is the safe answer. Single-key ops may cross shards via VLL; transactions deliberately may not. |
| FM-TXN-043 | `SSUBSCRIBE` is rejected inside `MULTI`; the rest of the subscribe family executes | Same | Listed for completeness — this matches Redis 8.6.4, including the error text. |
