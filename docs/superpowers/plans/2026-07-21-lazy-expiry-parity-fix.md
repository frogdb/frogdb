# Lazy-Expiry Effect Parity — Fix Stage

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Each task is a single reviewable commit and the branch is green after every task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four confirmed lazy-expiry parity gaps in the problem statement (`.scratch/concurrency-testing/proposals/lazy-expiry-parity.md`). Stage 1 (D8 verification) is DONE — all four gaps are confirmed with `#[ignore]`d real-path repros (commit `29454617`). This plan is the **fix stage**: land the production fix behind those repros, flipping each repro from `#[ignore]` to an active `regression_`-prefixed pin as its gap closes.

The four gaps share one root cause: the store-layer lazy purge (`check_and_delete_expired`, `frogdb-server/crates/core/src/store/hashmap.rs`, reached via `get_with_expiry_check` / `purge_if_expired`) is **version- and wait-queue-ignorant**. Active expiry (`apply_expiry_effects`, `event_loop.rs`) bumps the shard version and — since F1 (`63eff9a2`) — drains blocked XREADGROUP waiters to NOGROUP; the lazy path does neither. The gaps:

1. **Lazy read doesn't drain XREADGROUP waiters.** A blocked XREADGROUP waiter on a TTL-expired stream stays parked until BLOCK timeout when the key is lazily purged by any value read.
2. **Racing lazy read nullifies the F1 drain.** A lazy purge removes the key before the active sweep sees it, so `ExpiryResult::deleted_keys` never contains it and the F1 drain never fires.
3. **Read-path lazy purge doesn't bump the WATCH version.** A watched key lazily purged by a third party's read leaves the per-shard version untouched → WATCH false negative (under-abort).
4. **Concurrent lazy-purge under-abort for a second watcher.** Watcher B watches `k` live; `k` expires; watcher A's WATCH-time purge (`dispatch_core.rs`, deliberately no-bump per F3) removes `k`; B's EXEC finds nothing to purge, doesn't bump, and commits over a live→gone transition.

**Solution shape (two complementary mechanisms).** The design was settled in stage 1 and is bound by the three constraints below; it splits into:

- **Mechanism 1 — lazy-purge effects seam (fixes gaps 1, 2, 3).** The store *reports* the keys it physically removed at the lazy-purge site (`check_and_delete_expired` → `uninstall`); the worker drains that report after each command and applies the same **externally observable** effects active expiry applies: **bump the shard version** + **drain XREADGROUP waiters to NOGROUP**. Both effects are idempotent, so they can be applied at more than one drain point without double-firing. The store stays version- and wait-queue-ignorant — it only reports which keys it removed.
- **Mechanism 2 — per-key `wk->expired` tracking (fixes gap 4).** WATCH captures, per key, whether the key was **live at watch time** (`live_at_watch`, the inverse of Redis `wk->expired`). This bit travels connection-side alongside the existing per-key version and is checked at EXEC: a watch is dirty if `live_at_watch && the key is now expired-or-gone`, even when no version bump reached this watcher. This is the only gap that a coarse per-shard bump cannot fix (bumping at WATCH would over-abort every unrelated watcher on the shard and make WATCH a mutating op — the F3 design deliberately makes WATCH-time purge no-bump).

**Redis precedent:** `signalModifiedKey` / `keyModified` → `touchWatchedKey` fires on expiry-at-lookup, plus `wk->expired` recorded at WATCH time (redis/redis PR #7920, issue #7918). Valkey mirrors this; DragonflyDB matches the observable outcome. FrogDB's coarse per-shard version is the cheaper over-approximation for writes; this plan brings expiry-driven invalidation up to parity.

**Tech Stack:** Rust; `bytes::Bytes`; `tokio`; the shard-driver integration harness (`frogdb-server/crates/core/tests/shard_driver/`, phase-4b); the `frogdb-server` turmoil sim harness (`crates/server/tests/simulation.rs`); `just` + `cargo nextest`.

## Global Constraints

- **Run `pwd` first.** You are in a git worktree, not the main checkout. Only edit files under the directory `pwd` reports; use absolute paths.
- **Binding design constraints (from stage 1 — the fix MUST respect these):**
  1. **Anchor the effects seam at the physical-removal site** (`check_and_delete_expired` → `uninstall`), NOT a per-command is-a-read predicate. `TYPE` (`key_type`), `EXISTS`/`TOUCH` (`exists_unexpired`), and the FirstKey-hit seam read expiry non-destructively and must **not** fire effects — they never reach `check_and_delete_expired`, so anchoring there is exactly correct.
  2. **The store stays version- and wait-queue-ignorant** (the F3 encapsulation constraint). The store may *report* lazy removals; the worker applies the effects (version bump + stream-waiter drain). Do **not** give `HashMapStore` a `shard_version`, a `wait_queue`, or a `check_watches`.
  3. **Gap 4 requires per-key `wk->expired` state** — a coarse per-shard bump cannot distinguish the stale watcher (watched `k` already-expired → must **not** abort) from the live watcher (watched `k` live → must abort).
  4. **Each fix lands with its repro flipped** from `#[ignore]` to an active `regression_`-prefixed pin (rename the test, drop `#[ignore]`, invert the assertion to the correct outcome).
  5. **Shard-driver scenarios extended** with lazy arms: S2 gains a lazy-read arm; S5 already has the gap-1/gap-2 arms (flip them). Per the proposal acceptance criteria.
- **Keep the F3 tests green.** WATCH-time purge in the `GetVersion` handler (`dispatch_core.rs`) deliberately does **not** bump the version — a WATCH on an already-expired key must record a "nonexistent" watch, not invalidate other watchers. Mechanism 1's drain must **discard** (not apply) at the WATCH seam; Mechanism 2 must leave the no-bump WATCH path intact. Both fix tasks explicitly re-run the F3 pins (`s2_f3_lazy_expiry_watched_key_aborts`, `watch_lazy_expiry_false_negative_realpath`, and the `s2_zero_false_negatives_and_over_abort_characterized` characterization) and require them green.
- **Turmoil clock trap.** TTL expiry is evaluated against real-clock `std::time::Instant`, not turmoil's virtual clock. Any new/changed sim test must elapse a TTL with **`std::thread::sleep`**, never `tokio::time::sleep` (a tokio sleep advances only virtual time and leaves the key physically live). The stage-1 repros already do this (`DEBUG SET-ACTIVE-EXPIRE 0` + `PEXPIRE k 10` + `std::thread::sleep(Duration::from_millis(50))`); preserve it when flipping.
- **Build/test commands** (verified in `Justfile`):
  - `frogdb-core`: `just check frogdb-core`, `just test frogdb-core <pattern>` (expands to `-E 'test(/<pattern>/)'`), `just lint frogdb-core`, `just fmt frogdb-core`.
  - Shard-driver tests are `frogdb-core` integration tests: `just test frogdb-core <name>`.
  - Turmoil tests need the feature: `just concurrency-turmoil <PATTERN>` (= `cargo nextest run -p frogdb-server --features turmoil -E 'test(/<PATTERN>/)'`). **Note:** this recipe does **not** pass `--run-ignored`. To run a still-`#[ignore]`d repro before it is flipped, use `cargo nextest run -p frogdb-server --features turmoil --run-ignored all -E 'test(/<name>/)'`. After a repro is flipped to an active `regression_` pin, plain `just concurrency-turmoil <name>` runs it.
  - If `sccache` errors, re-run prefixed with `RUSTC_WRAPPER=""`.
- **Watchdog rules (CLAUDE.md).** Any build/test expected to take >2 min runs in the **foreground with a 600000ms Bash timeout**, output redirected to a `$TMPDIR` log (`cmd > "$TMPDIR/log" 2>&1` then `tail` it) — never pipe a long run through `grep`/`tail`. On a timeout, re-run the SAME command. Commit before launching a long run.
- **lefthook sandbox-denial → retry the identical `git commit` unsandboxed.**
- **Commit per task** with the exact `git` command shown in the task's final step. **No `Co-Authored-By` lines.** House style (from `673a52c9` / `63eff9a2`): `fix(core): <imperative summary> (gap<n>)`, body opens by stating the corrected behavior + the diverging seam, cites redis PR #7920 / issue #7918 where relevant, closes with a `Tests:` paragraph naming the flipped pins and their pre/post RESP bytes (`$-1\r\n`, `NOGROUP`, `*1\r\n+OK\r\n`).
- **Never weaken a checker or invariant to make a test pass.** A red test is signal.
- **Ledger:** `.superpowers/sdd/` phase notes and the proposal's Acceptance criteria checkboxes.

---

## Reference: current seams (verified 2026-07-21; re-confirm line numbers before editing)

Store (`frogdb-server/crates/core/src/store/`):
- `Store` trait: `mod.rs:393`. `exists_unexpired(&self, key) -> bool` default `mod.rs:421` (non-destructive: `!is_expired`). `get_with_expiry_check(&mut self, key) -> Option<Arc<Value>>` `mod.rs:494`. `purge_if_expired(&mut self, key) -> bool` `mod.rs:501`. `key_type(&self, key) -> KeyType` `mod.rs:426`.
- `HashMapStore::check_and_delete_expired(&mut self, key) -> bool` `hashmap.rs:421` — the **physical-removal site**. On removal it calls `self.uninstall(key)` (`hashmap.rs:430`) and `self.expired_keys += 1` (`hashmap.rs:431`). The `expiry_suppressed` branch (`hashmap.rs:426`) returns `true` **without** removing — must **not** report. Every lazy-purge trait method funnels through it (`get_with_expiry_check` `hashmap.rs:1022`, `purge_if_expired` `hashmap.rs:1056`, `set_with_options` `1062`, `set_expiry` `1096`, `touch` `1127`, `get_and_delete` `1141`, `get_mut` `1167`).
- Active expiry does **not** touch `check_and_delete_expired`: `ActiveExpiryCoordinator::run_cycle` (`active_expiry.rs:121`) deletes via `store.delete(&key)` (`active_expiry.rs:151`), producing `ExpiryResult { deleted_keys, emptied_keys, .. }`. So anchoring the report in `check_and_delete_expired` will **not** double-report active sweeps.

Worker (`frogdb-server/crates/core/src/shard/`):
- `ShardWorker.shard_version: u64` `worker.rs:53`; `increment_version(&mut self)` `worker.rs:452`; `get_key_version(&self, _key) -> u64` returns `shard_version` ignoring the key `worker.rs:457`.
- `check_watches(&self, watches: &[(Bytes, u64)]) -> bool` `worker.rs:462` — pure version compare.
- `purge_expired_watches(&mut self, watches: &[(Bytes, u64)])` `worker.rs:481` — loops `store.purge_if_expired`, bumps once if any removed (F3).
- `drain_stream_waiters_with_error(&mut self, key: &Bytes)` `blocking.rs:313` — already `pub(crate)`; pops only XREADGROUP waiters → `NOGROUP …`, leaves plain XREAD blocked. Write-path precedent: `blocking.rs:233` (`KeyReady::DrainNoGroup`). F1 precedent: `event_loop.rs:205`.
- `apply_expiry_effects(&mut self, result)` `event_loop.rs:154` — the active-expiry effect set (tracking, search, keyspace notify, probe, F1 drain, version bump). Mechanism 1 mirrors only its **version bump + F1 drain** (see Honest Scoping).
- `execute_command_inner(&mut self, command, conn_id, protocol_version, track_reads) -> (Response, Option<WriteCommandMeta>)` `execution.rs:82` — the universal command funnel: called by `execute_command` (`execution.rs:358`, single-command path) **and** the transaction command loop (`execution.rs:~493`). VLL `VllExecute` runs scatter ops (writes; they bump anyway).
- `execute_transaction` `execution.rs:438`: `purge_expired_watches(watches)` at `:459` → `if !check_watches(watches) { return WatchAborted }` at `:462-464` → command loop.
- `GetVersion { keys, response_tx }` handler `dispatch_core.rs:59` — loops `store.purge_if_expired(key)` **without** bumping, returns `shard_version`. This is the F3 no-bump WATCH seam.

Connection (`frogdb-server/crates/server/src/connection/`):
- `TransactionState.watches: HashMap<Bytes, (usize, u64)>` `state.rs:133` (key → (shard, version)). Written by `watch_key` `state.rs:867`; assembled into the wire `Vec<(Bytes, u64)>` by `take_transaction` `state.rs:894`.
- `handle_watch` `transaction_conn_command.rs:267` sends `GetVersion { keys: args.to_vec(), response_tx }`, awaits `version: u64`, stores per key.
- `ShardMessage::GetVersion` `message.rs:154` (`response_tx: oneshot::Sender<u64>`); `ShardMessage::ExecTransaction { commands, watches, .. }` `message.rs:160`, `watches: Vec<(Bytes, u64)>` `message.rs:162`.
- `run_shard_transaction` `transaction.rs:323` builds `ExecTransaction`; replication executor sends `watches: Vec::new()` (`replication/executor.rs:94`).

Stage-1 repros (all `#[ignore]`d, added in `29454617`):
- Gap 1: `s5_gap1_lazy_read_does_not_drain_xreadgroup` — `crates/core/tests/shard_driver/scenario_s5.rs`.
- Gap 2: `s5_gap2_lazy_read_nullifies_active_drain` — same file.
- Gap 3: `watch_read_lazy_purge_no_bump_realpath` — `crates/server/tests/simulation.rs`.
- Gap 4: `watch_second_watcher_under_abort_realpath` — same file.

---

## Task 1 — Store reports lazy removals (behavior-neutral seam)

**Goal:** Give the store a way to *report* the keys it physically removed via lazy expiry, and wire the worker to drain that report at every lazy-purge site — but **discard** it for now (no effects). This is a **behavior-neutral** task: it establishes the seam (constraint 2) and keeps the branch green with all four repros still `#[ignore]`d. Effects land in Tasks 2 and 4; the wiring here stays stable so those tasks touch only the helper body.

**Files touched:**
- `frogdb-server/crates/core/src/store/mod.rs` — new `Store::take_lazily_purged` trait method (default empty).
- `frogdb-server/crates/core/src/store/hashmap.rs` — `lazily_purged` field, push in `check_and_delete_expired`, override `take_lazily_purged`, unit tests.
- `frogdb-server/crates/core/src/shard/worker.rs` — `apply_lazy_purge_effects` + `discard_lazy_purges` helpers (both drain-and-discard for now).
- `frogdb-server/crates/core/src/shard/execution.rs` — wrap `execute_command_inner` so it drains after the command; call the drain in `purge_expired_watches`'s path.
- `frogdb-server/crates/core/src/shard/dispatch_core.rs` — discard the report after the WATCH-time purge loop.

- [ ] **Step 1: Add the trait method.** In `frogdb-server/crates/core/src/store/mod.rs`, in `trait Store` (near `purge_if_expired`, `mod.rs:501`), add:

```rust
    /// Drain the buffer of keys physically removed by **lazy** expiry
    /// (`check_and_delete_expired` → `uninstall`) since the last drain.
    ///
    /// The store reports *which* keys it lazily removed; it does not act on the
    /// report — it stays version- and wait-queue-ignorant. The worker drains
    /// this after each command and applies the parity effects (shard-version
    /// bump + XREADGROUP drain), exactly as active expiry applies them from
    /// `ExpiryResult::deleted_keys`. Active expiry deletes via `delete`, not
    /// `check_and_delete_expired`, so it never populates this buffer.
    ///
    /// Default: no lazy-purge reporting (stores that do not lazily purge).
    fn take_lazily_purged(&mut self) -> Vec<Bytes> {
        Vec::new()
    }
```

- [ ] **Step 2: Add the buffer field to `HashMapStore`.** In `hashmap.rs`, add to the `HashMapStore` struct (after `expired_keys: u64,`, `hashmap.rs:132`):

```rust
    /// Keys physically removed by lazy expiry (`check_and_delete_expired`)
    /// since the last `take_lazily_purged`. Reported to the worker so it can
    /// apply the same version-bump + XREADGROUP-drain effects active expiry
    /// applies; the store itself never reads or acts on it.
    lazily_purged: Vec<Bytes>,
```

Initialize it (`Vec::new()`) in **both** constructors — `HashMapStore::new` (`hashmap.rs:164`) and `with_expiry_index` (`hashmap.rs:184`). (Do **not** clear it in `clear()` — a FLUSHDB should not silently swallow a pending report; but if a test flags an anomaly, revisit. It is drained every command, so it is effectively always empty at rest.)

- [ ] **Step 3: Push at the physical-removal site.** In `check_and_delete_expired` (`hashmap.rs:421`), record the key in the **actual-removal** branch only (not the `expiry_suppressed` early return):

```rust
    fn check_and_delete_expired(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.get(key)
            && entry.metadata.is_expired()
        {
            // During CLIENT PAUSE, suppress physical deletion but treat as expired.
            if self.expiry_suppressed {
                return true;
            }
            debug!(key_len = key.len(), "Key expired via lazy deletion");
            self.uninstall(key);
            self.expired_keys += 1;
            // Report the lazy removal so the worker can apply the parity
            // effects (version bump + XREADGROUP drain). The store stays
            // version- and wait-queue-ignorant.
            self.lazily_purged.push(Bytes::copy_from_slice(key));
            return true;
        }
        false
    }
```

- [ ] **Step 4: Override the drain in `HashMapStore`'s `Store` impl.** Add (near `purge_if_expired`, `hashmap.rs:1056`):

```rust
    fn take_lazily_purged(&mut self) -> Vec<Bytes> {
        std::mem::take(&mut self.lazily_purged)
    }
```

- [ ] **Step 5: Store unit tests.** In the `#[cfg(test)]` module of `hashmap.rs`, add tests proving the report fires **only** at the destructive lazy-purge site:

```rust
    #[test]
    fn lazy_value_read_reports_purge_but_nondestructive_probes_do_not() {
        let mut s = HashMapStore::new();
        let past = Instant::now() - Duration::from_secs(1);
        // Seed an already-expired key.
        let mut md = KeyMetadata::new(0);
        md.expires_at = Some(past);
        s.restore_entry(Bytes::from_static(b"k"), Value::from_static_str("v"), md);

        // Non-destructive probes must NOT purge or report.
        assert!(!s.exists_unexpired(b"k"));
        let _ = s.key_type(b"k");
        assert!(s.take_lazily_purged().is_empty(), "TYPE/EXISTS must not report a purge");

        // A value read via the expiry-checking path purges and reports.
        assert!(s.get_with_expiry_check(b"k").is_none());
        assert_eq!(
            s.take_lazily_purged(),
            vec![Bytes::from_static(b"k")],
            "get_with_expiry_check must report the physical lazy removal"
        );
        // Drained: a second take is empty.
        assert!(s.take_lazily_purged().is_empty());
    }

    #[test]
    fn suppressed_expiry_does_not_report() {
        let mut s = HashMapStore::new();
        s.set_expiry_suppressed(true);
        let mut md = KeyMetadata::new(0);
        md.expires_at = Some(Instant::now() - Duration::from_secs(1));
        s.restore_entry(Bytes::from_static(b"k"), Value::from_static_str("v"), md);
        // Suppressed: logically expired (read as absent) but NOT physically removed.
        assert!(s.get_with_expiry_check(b"k").is_none());
        assert!(
            s.take_lazily_purged().is_empty(),
            "CLIENT PAUSE suppression must not report a purge (key not removed)"
        );
    }
```

(Match the exact `Value` / `KeyMetadata` / `restore_entry` constructors used elsewhere in this test module — the shapes above are indicative; fix them to compile against the real APIs.)

- [ ] **Step 6: Add the worker drain helpers (drain-and-discard for now).** In `frogdb-server/crates/core/src/shard/worker.rs`, in `impl ShardWorker` near `purge_expired_watches` (`worker.rs:481`), add:

```rust
    /// Drain and apply the effects of any lazy purges the store reported during
    /// the current command: a shard-version bump + an XREADGROUP-waiter drain to
    /// NOGROUP for each removed key — the same externally observable effects
    /// active expiry applies (`apply_expiry_effects`, event_loop.rs), so a key
    /// that died lazily is indistinguishable from one the sweep removed.
    ///
    /// Both effects are idempotent (a second bump only advances the counter; a
    /// second drain finds no waiters), so calling this at more than one seam is
    /// safe. Task 1 lands it as drain-and-discard; Task 2 fills in the effects.
    pub(crate) fn apply_lazy_purge_effects(&mut self) {
        let purged = self.store.take_lazily_purged();
        let _ = purged; // Task 2: apply version bump + drain_stream_waiters_with_error.
    }

    /// Drain and DISCARD any lazy-purge report without applying effects — used
    /// at the WATCH-time (`GetVersion`) seam, which must stay no-bump (F3): a
    /// WATCH on an already-expired key records a "nonexistent" watch and must
    /// not invalidate other watchers on the shard.
    pub(crate) fn discard_lazy_purges(&mut self) {
        let _ = self.store.take_lazily_purged();
    }
```

- [ ] **Step 7: Wire the command seam.** In `execution.rs`, make `execute_command_inner` drain after the command. The method has many early returns, so **wrap** it rather than editing each return: rename the current `execute_command_inner` body to `execute_command_body` and add a thin wrapper:

```rust
    fn execute_command_inner(
        &mut self,
        command: &ParsedCommand,
        conn_id: u64,
        protocol_version: ProtocolVersion,
        track_reads: bool,
    ) -> (Response, Option<WriteCommandMeta>) {
        let out = self.execute_command_body(command, conn_id, protocol_version, track_reads);
        // Apply parity effects for any keys the command lazily purged (gaps 1-3).
        // Task 1: this is a no-op drain; Task 2 fills in the effects.
        self.apply_lazy_purge_effects();
        out
    }
```

(Keep `execute_command_body` with the exact current signature and body. Both callers of `execute_command_inner` — `execute_command` `execution.rs:358` and the transaction loop `execution.rs:~493` — are unchanged.)

- [ ] **Step 8: Wire the WATCH + EXEC-watch seams.** In `dispatch_core.rs`, after the `GetVersion` purge loop (`dispatch_core.rs:68`), discard the report before returning the version:

```rust
                for key in &keys {
                    self.store.purge_if_expired(key);
                }
                // WATCH stays no-bump (F3): drop the lazy-purge report unread.
                self.discard_lazy_purges();
                let _ = response_tx.send(self.shard_version);
```

In `worker.rs`, in `purge_expired_watches` (`worker.rs:481`), drain the report the watched-key purges produced so it does not leak into the transaction's command loop. For Task 1 keep the existing explicit bump and discard the report:

```rust
    pub(crate) fn purge_expired_watches(&mut self, watches: &[(Bytes, u64)]) {
        let mut purged = false;
        for (key, _watched_ver) in watches {
            if self.store.purge_if_expired(key) {
                purged = true;
            }
        }
        // Task 1: discard the report (the explicit bump below preserves F3).
        // Task 2 replaces both with apply_lazy_purge_effects().
        self.discard_lazy_purges();
        if purged {
            self.increment_version();
        }
    }
```

- [ ] **Step 9: Verify behavior-neutral.** `just test frogdb-core store::hashmap` (new store tests pass). Then confirm no behavior change: `just test frogdb-core s2` and `just test frogdb-core s5` (base arms green; the four repros stay `#[ignore]`d and are not run). Then the F3 pins: `just concurrency-turmoil watch_lazy_expiry_false_negative_realpath`.

- [ ] **Step 10: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/src/store/mod.rs frogdb-server/crates/core/src/store/hashmap.rs frogdb-server/crates/core/src/shard/worker.rs frogdb-server/crates/core/src/shard/execution.rs frogdb-server/crates/core/src/shard/dispatch_core.rs
git commit -m "test(core): store reports lazy removals to the worker (version-ignorant seam)"
```

---

## Task 2 — Apply lazy-purge effects: fix gaps 1, 2, 3

**Goal:** Fill in `apply_lazy_purge_effects` so a reported lazy purge bumps the shard version and drains XREADGROUP waiters to NOGROUP. This fixes all three lazy-read gaps at once: gap 1 (a lazy read now drains the waiter), gap 2 (the lazy read drains *at the point of removal*, so a later empty sweep is irrelevant), and gap 3 (the lazy read now bumps the WATCH version, so a watcher of the purged key aborts). Flip the three repros to `regression_` pins and add the S2 lazy-read arm.

**Files touched:**
- `frogdb-server/crates/core/src/shard/worker.rs` — implement `apply_lazy_purge_effects`; simplify `purge_expired_watches` to use it.
- `frogdb-server/crates/core/tests/shard_driver/scenario_s5.rs` — flip gap-1 / gap-2 repros to `regression_` pins.
- `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs` — add a lazy-read arm pin.
- `frogdb-server/crates/server/tests/simulation.rs` — flip gap-3 repro to a `regression_` pin.

- [ ] **Step 1: Watch gaps 1-3 fail (as `#[ignore]`d repros).** Before changing code, confirm the shard-driver repros still fail when run:
  `cargo nextest run -p frogdb-core --run-ignored all -E 'test(/s5_gap1|s5_gap2/)' > "$TMPDIR/gaps12.log" 2>&1 ; tail "$TMPDIR/gaps12.log"` — expected: both **fail** (gap-1 asserts the waiter is still parked / `total_waiters == 1`; that assertion holds *pre-fix*, so the `#[ignore]`d test as written passes-while-ignored — it documents the bug). Read each repro's body: the flip target is `is_nogroup(...)` + `total_waiters == 0`. Likewise the gap-3 turmoil repro currently asserts `COMMITTED = *1\r\n+OK\r\n`.
  (These repros assert *current* behavior, so they pass as written. "Watch it fail" here means: after the fix, the current assertion must break — proving the fix changed behavior — which is why Step 4 inverts them.)

- [ ] **Step 2: Implement the effects.** In `worker.rs`, replace the Task-1 body of `apply_lazy_purge_effects`:

```rust
    pub(crate) fn apply_lazy_purge_effects(&mut self) {
        let purged = self.store.take_lazily_purged();
        if purged.is_empty() {
            return;
        }
        // Drain blocked XREADGROUP waiters for each lazily-removed stream key,
        // mirroring the DEL write path and the F1 active-expiry drain
        // (drain_stream_waiters_with_error → NOGROUP; plain XREAD waiters stay
        // blocked). No-op for non-stream keys.
        for key in &purged {
            self.drain_stream_waiters_with_error(key);
        }
        // One version bump for the batch, mirroring active expiry's
        // one-bump-per-cycle: a watched key that died lazily is now observed
        // changed by check_watches (gap 3).
        self.increment_version();
    }
```

Then simplify `purge_expired_watches` to route its watched-key purges through the same seam (the report the purges produced is now applied — bump + drain — before `check_watches` runs):

```rust
    pub(crate) fn purge_expired_watches(&mut self, watches: &[(Bytes, u64)]) {
        for (key, _watched_ver) in watches {
            self.store.purge_if_expired(key);
        }
        // Apply the bump + drain for any watched key that expired during the
        // WATCH window — this must run before check_watches so the version
        // change is visible (F3). Subsumes the previous explicit increment.
        self.apply_lazy_purge_effects();
    }
```

**Do not** add the `discard_lazy_purges` call here anymore — it is replaced by `apply_lazy_purge_effects`. Keep `discard_lazy_purges` at the `GetVersion` WATCH seam (that one must stay discard).

- [ ] **Step 3: Confirm the F3 base + WATCH-no-bump invariants still hold — trace it.** Record in the commit body:
  - `s2_f3_lazy_expiry_watched_key_aborts`: watched `k` expires during B's own window; B's EXEC `purge_expired_watches` purges `k`, `apply_lazy_purge_effects` bumps → `check_watches` sees the change → abort. Green.
  - WATCH no-bump: the `GetVersion` handler still discards (Step 8 of Task 1). A WATCH on an already-expired key does not bump → unrelated watchers not over-aborted. Green.

- [ ] **Step 4: Flip the gap-1 / gap-2 shard-driver repros.** In `scenario_s5.rs`, for each repro: remove `#[ignore]`, rename to a `regression_` pin, and invert the assertions to the correct (drained) outcome — the `is_nogroup` + `total_waiters == 0` shape the base `s5_del_arm` / `s5_ttl_arm` arms already use. Example for gap 1:

```rust
    /// Regression pin (gap 1): a third-party lazy value read that physically
    /// purges a TTL-expired stream now drains the blocked XREADGROUP waiter to
    /// NOGROUP — the same outcome as DEL and the active sweep. Redis serves
    /// NOGROUP here (lookupKeyReadWithFlags → expireIfNeeded → signalKeyAsReady).
    #[tokio::test]
    async fn regression_gap1_lazy_read_drains_xreadgroup() {
        let mut d = ShardDriver::new(1);
        setup_stream_group(&mut d).await;
        let group_rx = block_xreadgroup(&mut d, 10).await;

        let _ = d.execute(0, "PEXPIRE", &["st", "1"]).await;
        tokio::time::sleep(Duration::from_millis(3)).await; // real-clock TTL elapse (unit-test tokio clock is real)

        // Third-conn lazy value read purges the expired stream.
        let _ = d.execute_conn(0, 20, "GET", &["st"]).await;

        let group_resp = group_rx.await.expect("group waiter replied");
        assert!(is_nogroup(&group_resp), "gap 1: lazy read must drain to NOGROUP, got {group_resp:?}");
        assert_eq!(d.wait_queue_info(0).await.total_waiters, 0, "gap 1: waiter drained by the lazy read");
        assert!(d.expiry_index_check(0).await.anomalies.is_empty());
    }
```

For gap 2, keep the racing structure (lazy `GET` then `tick_expiry`) but assert the waiter was already drained by the lazy read (`is_nogroup` + `total_waiters == 0`), documenting that the drain happened at the point of removal, not at the sweep. Update the module doc comment at the top of `scenario_s5.rs` to note both lazy arms are now closed. **Note (turmoil clock trap does not apply here):** these are `frogdb-core` unit tests under the real tokio clock, so `tokio::time::sleep(3ms)` genuinely elapses a real-`Instant` TTL — keep it (this is what the base S5 arms do).

- [ ] **Step 5: Add the S2 lazy-read arm** (constraint 5 — "S2 lazy arm"). In `scenario_s2.rs`, add a shard-driver pin for gap 3 (third-party lazy read purges a watched key → EXEC aborts):

```rust
    /// Regression pin (gap 3): a watched key lazily purged by a THIRD party's
    /// value read bumps the shard version, so the watcher's EXEC aborts —
    /// previously the read-path lazy purge was version-ignorant (under-abort).
    #[tokio::test]
    async fn regression_gap3_third_party_lazy_read_aborts_watch() {
        let mut d = ShardDriver::new(1);
        let _ = d.execute(0, "SET", &["k", "v0"]).await;
        let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
        // Snapshot the version AFTER PEXPIRE (watch the still-live key).
        let v0 = d.get_version(0).await;
        tokio::time::sleep(Duration::from_millis(3)).await; // elapse the TTL

        // Third conn's lazy read physically purges k (version-ignorant before the fix).
        let _ = d.execute_conn(0, 2, "GET", &["k"]).await;

        let result = d
            .exec_transaction(0, 1, vec![cmd("SET", &["k", "x"])], vec![(Bytes::from_static(b"k"), v0)])
            .await;
        assert!(
            matches!(result, TransactionResult::WatchAborted),
            "gap 3: a third-party lazy read of an expired watched key must abort EXEC, got {result:?}"
        );
    }
```

- [ ] **Step 6: Flip the gap-3 turmoil repro.** In `simulation.rs`, `watch_read_lazy_purge_no_bump_realpath`: remove `#[ignore]`, rename to `regression_watch_read_lazy_purge_aborts_realpath`, and invert the final assertion from `COMMITTED = b"*1\r\n+OK\r\n"` to the aborted null bulk `b"$-1\r\n"`. **Keep the `std::thread::sleep(Duration::from_millis(50))` and `DEBUG SET-ACTIVE-EXPIRE 0`** (real-clock TTL trap) and the Notify handshake untouched — only the ignore attribute, name, and terminal assertion change:

```rust
    const ABORTED: &[u8] = b"$-1\r\n";
    assert_eq!(
        reply.as_slice(),
        ABORTED,
        "gap 3: a third-party GET lazily purged the watched key and bumped the \
         shard version, so EXEC ABORTS ($-1). Redis/Valkey/Dragonfly abort here \
         (expireIfNeeded → keyModified → touchWatchedKey). Got {reply:?}.",
    );
```

- [ ] **Step 7: Run.**
  - `just test frogdb-core 'regression_gap1|regression_gap2|regression_gap3|s5_|s2_'` — expected PASS (flipped pins + all base S2/S5 arms).
  - `cargo nextest run -p frogdb-server --features turmoil -E 'test(/regression_watch_read_lazy_purge|watch_lazy_expiry_false_negative/)' > "$TMPDIR/t2turmoil.log" 2>&1 ; tail "$TMPDIR/t2turmoil.log"` — expected PASS (gap-3 pin flipped; F3 pin still green). Note gap-4 repro `watch_second_watcher_under_abort_realpath` stays `#[ignore]` (Task 4).
  - `just test frogdb-core effect_tests` — expected PASS (no active-expiry regression).

- [ ] **Step 8: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/src/shard/worker.rs frogdb-server/crates/core/tests/shard_driver/scenario_s5.rs frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs frogdb-server/crates/server/tests/simulation.rs
git commit -m "fix(core): lazy purge bumps version and drains XREADGROUP waiters (gaps 1-3)"
```

---

## Task 3 — Per-key `live_at_watch` plumbing (behavior-neutral)

**Goal:** Thread a per-key `live_at_watch` flag (the inverse of Redis `wk->expired`) from the WATCH-time `GetVersion` handler, through connection-side watch state, into `ExecTransaction`, and down to `check_watches` — which **ignores it for now**. Behavior-neutral: gap-4 repro stays `#[ignore]`d and every existing pin stays green. This isolates the risky message/connection plumbing from the gap-4 behavior change (Task 4).

This is the constraint-3 "per-key `wk->expired` state." FrogDB has no server-side watch registry (watch state is connection-side), so the per-key bit rides alongside the existing per-key version.

**Files touched:**
- `frogdb-server/crates/core/src/shard/message.rs` — `GetVersion` response type; `ExecTransaction.watches` element type.
- `frogdb-server/crates/core/src/shard/dispatch_core.rs` — `GetVersion` returns per-key `live_at_watch`.
- `frogdb-server/crates/core/src/shard/worker.rs` — `check_watches` / `purge_expired_watches` signatures.
- `frogdb-server/crates/core/src/shard/execution.rs` — `execute_transaction` signature.
- `frogdb-server/crates/server/src/connection/state.rs` — `TransactionState.watches` element type; `watch_key`; `take_transaction`.
- `frogdb-server/crates/server/src/connection/transaction_conn_command.rs` — `handle_watch`.
- `frogdb-server/crates/server/src/connection/transaction.rs` + `replication/executor.rs` — call-site updates.
- `frogdb-server/crates/core/tests/shard_driver/harness.rs` — `exec_transaction` helper signature.

**Design note — encoding.** Extend the watch element from `(Bytes, u64)` to a 3-tuple `(Bytes, u64, bool)` where the `bool` is `live_at_watch`. (A named `WatchEntry { key, version, live_at_watch }` struct in `frogdb-core::shard` is an acceptable alternative and reads better across the plumbing; if you introduce it, put it beside the `ExecTransaction` message definition and use it end-to-end. The tuple is the lower-churn choice and is used in the sketches below.) The `GetVersion` reply becomes `(u64, Vec<bool>)`: the shard version plus one `live_at_watch` flag per requested key, in `keys` order.

- [ ] **Step 1: `GetVersion` reply carries per-key liveness.** In `message.rs`, change `GetVersion`'s response channel (`message.rs:154`) from `oneshot::Sender<u64>` to `oneshot::Sender<(u64, Vec<bool>)>`. Update the doc comment: the `Vec<bool>` aligns with `keys` and reports, per key, whether it was live (present and unexpired) at watch time — the `wk->expired` discriminator.

- [ ] **Step 2: Compute it in the handler.** In `dispatch_core.rs`, the `GetVersion` handler computes `live_at_watch` **before** the no-bump purge (an expired-but-present key reads `exists_unexpired == false`, so order does not matter, but compute-then-purge is clearest):

```rust
            ShardMessage::GetVersion { keys, response_tx } => {
                // Per-key liveness at watch time (wk->expired inverse): a key
                // present and unexpired is "live"; an absent or already-expired
                // key is a nonexistent/stale watch. Non-destructive probe.
                let live_at_watch: Vec<bool> =
                    keys.iter().map(|k| self.store.exists_unexpired(k)).collect();
                // WATCH-time no-bump lazy purge (F3): align physical state to
                // logical without invalidating other watchers.
                for key in &keys {
                    self.store.purge_if_expired(key);
                }
                self.discard_lazy_purges();
                let _ = response_tx.send((self.shard_version, live_at_watch));
            }
```

- [ ] **Step 3: Widen the wire + core signatures (behavior-neutral).**
  - `message.rs`: `ExecTransaction.watches: Vec<(Bytes, u64)>` → `Vec<(Bytes, u64, bool)>` (`message.rs:162`).
  - `worker.rs`: `check_watches(&self, watches: &[(Bytes, u64, bool)]) -> bool` — **ignore the bool for now**, keep the pure version compare so behavior is unchanged:
    ```rust
    pub(crate) fn check_watches(&self, watches: &[(Bytes, u64, bool)]) -> bool {
        watches
            .iter()
            .all(|(key, watched_ver, _live_at_watch)| self.get_key_version(key) == *watched_ver)
    }
    ```
  - `worker.rs`: `purge_expired_watches(&mut self, watches: &[(Bytes, u64, bool)])` — destructure the extra field with `_`.
  - `execution.rs`: `execute_transaction(&mut self, commands, watches: &[(Bytes, u64, bool)], ...)` and any local `Vec<(Bytes, u64)>` typed for watches.

- [ ] **Step 4: Connection-side plumbing.**
  - `state.rs:133`: `watches: HashMap<Bytes, (usize, u64)>` → `HashMap<Bytes, (usize, u64, bool)>`.
  - `watch_key` (`state.rs:867`): accept and store `live_at_watch`.
  - `take_transaction` (`state.rs:894`): map to `(key, version, live_at_watch)`.
  - `handle_watch` (`transaction_conn_command.rs:267`): the `GetVersion` reply is now `(version, live_flags)`; zip `args` with `live_flags` and pass each key's flag into `watch_key`.
  - `transaction.rs` `run_shard_transaction` (`transaction.rs:323`): build `ExecTransaction` with the 3-tuple `watches`.
  - `replication/executor.rs:94`: `watches: Vec::new()` is still valid (empty vec of the new element type) — confirm it compiles; no semantic change (replicas replay validated txns).

- [ ] **Step 5: Harness plumbing.** In `frogdb-server/crates/core/tests/shard_driver/harness.rs`, `exec_transaction` (`harness.rs:162`) currently takes `watches: Vec<(Bytes, u64)>`. Widen to `Vec<(Bytes, u64, bool)>` and forward. Update the existing S2/S8 callers (`scenario_s2.rs`, `scenario_s8.rs`) that build `watches` to pass the flag — for a key snapshotted while live pass `true`; this is behavior-neutral because `check_watches` still ignores it. (Grep `exec_transaction(` under `tests/shard_driver/` for all call sites.)

- [ ] **Step 6: Verify behavior-neutral.** `just check frogdb-core` and `just check frogdb-server` (the plumbing compiles). Then `just test frogdb-core s2` / `s5` / `s8` and `just concurrency-turmoil 'watch_lazy_expiry_false_negative|regression_watch_read_lazy_purge'` — all green, unchanged. Gap-4 repro `watch_second_watcher_under_abort_realpath` stays `#[ignore]`.

- [ ] **Step 7: Format, lint, commit.**

Run: `just fmt frogdb-core && just fmt frogdb-server && just lint frogdb-core && just lint frogdb-server`

```bash
git add frogdb-server/crates/core/src/shard/message.rs frogdb-server/crates/core/src/shard/dispatch_core.rs frogdb-server/crates/core/src/shard/worker.rs frogdb-server/crates/core/src/shard/execution.rs frogdb-server/crates/server/src/connection frogdb-server/crates/core/tests/shard_driver
git commit -m "test(core): thread per-key live_at_watch through WATCH/EXEC plumbing (no-op)"
```

---

## Task 4 — Fix gap 4: `check_watches` honors `live_at_watch`

**Goal:** With the per-key flag plumbed, make `check_watches` abort when a key watched **while live** is now expired-or-gone, even absent a version bump this watcher observed — the second-watcher case the coarse per-shard version cannot express. Flip the gap-4 repro to a `regression_` pin and add a shard-driver arm.

**Files touched:**
- `frogdb-server/crates/core/src/shard/worker.rs` — the `check_watches` clause.
- `frogdb-server/crates/server/tests/simulation.rs` — flip the gap-4 repro.
- `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs` — add the two-watcher arm.

- [ ] **Step 1: Watch gap 4 fail (as an `#[ignore]`d repro).** `cargo nextest run -p frogdb-server --features turmoil --run-ignored all -E 'test(/watch_second_watcher_under_abort_realpath/)' > "$TMPDIR/gap4.log" 2>&1 ; tail "$TMPDIR/gap4.log"` — the repro asserts the current buggy `COMMITTED = *1\r\n+OK\r\n`. Read its body: the flip target is the aborted `$-1\r\n`.

- [ ] **Step 2: Add the clause.** In `worker.rs`, `check_watches`:

```rust
    /// A watch is satisfied iff the key's version is unchanged AND it did not
    /// transition live -> expired/gone. The version compare catches every write
    /// and every expiry that bumped (active sweep, lazy read-path purge). The
    /// second clause catches the one death that does NOT bump for this watcher:
    /// a key watched while live that another watcher's no-bump WATCH-time purge
    /// (or its own already-elapsed TTL) removed — the gap-4 second-watcher case.
    /// `live_at_watch == false` means a stale/nonexistent watch (Redis
    /// `wk->expired`), which must NOT abort when the key stays gone. Uses the
    /// non-destructive `exists_unexpired` probe (constraint 1).
    pub(crate) fn check_watches(&self, watches: &[(Bytes, u64, bool)]) -> bool {
        watches.iter().all(|(key, watched_ver, live_at_watch)| {
            if self.get_key_version(key) != *watched_ver {
                return false; // changed via a version-bumping path
            }
            if *live_at_watch && !self.store.exists_unexpired(key) {
                return false; // watched live, now expired/gone with no bump (gap 4)
            }
            true
        })
    }
```

- [ ] **Step 3: Confirm the no-over-abort direction.** Record in the commit body why this adds no spurious aborts: the clause fires only for keys *this* watcher watched while live that are now gone (a true death) — it is per-key, not a shard-wide broadcast, so an unrelated watcher (`live_at_watch == false`, or a still-live key) is untouched. A WATCH on an already-expired key still records `live_at_watch == false` and still does not bump → no over-abort. This is why gap 4 needed per-key state (constraint 3).

- [ ] **Step 4: Flip the gap-4 turmoil repro.** In `simulation.rs`, `watch_second_watcher_under_abort_realpath`: remove `#[ignore]`, rename to `regression_watch_second_watcher_aborts_realpath`, invert the terminal assertion from `COMMITTED` to `ABORTED = b"$-1\r\n"`. Keep the `std::thread::sleep(50ms)`, `DEBUG SET-ACTIVE-EXPIRE 0`, and the B→A→B Notify handshake exactly as-is (only ignore/name/assertion change). The comment should note this is the case unfixable at the coarse version, now closed by per-key `live_at_watch`.

- [ ] **Step 5: Add the shard-driver two-watcher arm** (belt-and-suspenders; the turmoil repro is the real-path pin). In `scenario_s2.rs`, express the two-watcher race via the harness. The live watcher snapshots its version+liveness while `k` is live; the stale watcher's WATCH runs after `k` expired (its `GetVersion` no-bump purge removes `k`); the live watcher's EXEC must abort:

```rust
    /// Regression pin (gap 4): B watches k while live; k expires; A's WATCH-time
    /// no-bump purge removes k; B's EXEC must still abort (per-key live_at_watch),
    /// even though no version bump reached B. Coarse per-shard version cannot
    /// distinguish B (must abort) from A (must not).
    #[tokio::test]
    async fn regression_gap4_second_watcher_aborts() {
        let mut d = ShardDriver::new(1);
        let _ = d.execute(0, "SET", &["k", "v0"]).await;
        let _ = d.execute(0, "PEXPIRE", &["k", "1"]).await;
        // B watches k LIVE: version snapshot + live_at_watch = true.
        let vb = d.get_version(0).await;
        tokio::time::sleep(Duration::from_millis(3)).await; // elapse the TTL

        // A watches k already-expired: GetVersion no-bump purge removes k.
        // (Model A's WATCH-time purge via the same GetVersion path a WATCH takes.)
        let _ = d.get_version(0).await; // TODO: confirm harness get_version drives the keyed GetVersion purge;
                                        // if it does not (pure probe), drive ShardMessage::GetVersion { keys:[k] }
                                        // directly so A's no-bump purge fires.

        // B's EXEC watching k live at vb must abort.
        let result = d
            .exec_transaction(0, 1, vec![cmd("SET", &["k", "x"])], vec![(Bytes::from_static(b"k"), vb, true)])
            .await;
        assert!(
            matches!(result, TransactionResult::WatchAborted),
            "gap 4: second (live) watcher must abort after the first watcher's no-bump purge, got {result:?}"
        );
    }
```

**Implementer note:** the harness `get_version(shard)` sends `GetVersion` with an **empty** `keys` vec (a pure probe — no purge). To reproduce A's WATCH-time purge you must send `ShardMessage::GetVersion { keys: vec![Bytes::from_static(b"k")], response_tx }` via `d.dispatch(...)` (add a small `watch_keys(shard, keys)` helper to the harness if cleaner). Verify this drives the no-bump purge; if the two-watcher ordering proves awkward in the single-task shard driver, the turmoil repro (Step 4) is the authoritative gap-4 pin and this arm may be simplified to assert the `check_watches` clause directly against a manually-expired watched key. Do not weaken the turmoil pin to compensate.

- [ ] **Step 6: Run.**
  - `just test frogdb-core 'regression_gap4|s2_'` — expected PASS (new arm + all S2 arms including F3 base + characterization).
  - `cargo nextest run -p frogdb-server --features turmoil -E 'test(/regression_watch_second_watcher|watch_lazy_expiry_false_negative|regression_watch_read_lazy_purge/)' > "$TMPDIR/t4turmoil.log" 2>&1 ; tail "$TMPDIR/t4turmoil.log"` — expected PASS (gap-4 pin flipped; F3 + gap-3 pins still green).

- [ ] **Step 7: Format, lint, commit.**

Run: `just fmt frogdb-core && just lint frogdb-core`

```bash
git add frogdb-server/crates/core/src/shard/worker.rs frogdb-server/crates/server/tests/simulation.rs frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs
git commit -m "fix(core): per-key live_at_watch aborts second watcher on lazy expiry (gap 4)"
```

---

## Task 5 — Acceptance-criteria update + full-suite verification

**Goal:** Mark the proposal's acceptance criteria met, refresh the S2/S5 scenario module docs, and run the full suite green.

**Files touched:**
- `.scratch/concurrency-testing/proposals/lazy-expiry-parity.md` — check the remaining acceptance boxes; add a "Fix stage" verdict line.
- `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs`, `scenario_s5.rs` — module-doc touch-ups (the lazy arms are now closed pins, not gaps).

- [ ] **Step 1: Update the proposal.** In `.scratch/concurrency-testing/proposals/lazy-expiry-parity.md`, check the four open acceptance boxes (lines 39-42): lazy purge produces the same observable effects (WATCH abort + XREADGROUP NOGROUP drain); store stays version/wait-queue-ignorant; S2/S5 lazy arms landed; `regression_`-pinned tests for each gap. Add a short "Fix stage (2026-07-21)" verdict block listing the four `regression_` pins and their fixing commits, mirroring the "Stage-1 verdicts" block's style. Set `Status:` accordingly.

- [ ] **Step 2: Refresh scenario module docs.** Update the `scenario_s5.rs` and `scenario_s2.rs` top-of-file `//!` comments so they describe the closed state (lazy read now drains XREADGROUP; read-path + second-watcher lazy purge now abort WATCH via version bump + per-key `live_at_watch`), replacing the "F1 gap" / "covered in Tasks 8-9" phrasing.

- [ ] **Step 3: Full verification** (foreground, 600000ms, log to `$TMPDIR`, re-run the SAME command on timeout).

```bash
just test frogdb-core > "$TMPDIR/final_core.log" 2>&1 ; tail -30 "$TMPDIR/final_core.log"
```
Expected: green (store tests, all S2/S5 arms including the flipped `regression_` pins, effect_tests, driver_tests).

```bash
just test frogdb-server > "$TMPDIR/final_server.log" 2>&1 ; tail -30 "$TMPDIR/final_server.log"
```
Expected: green (connection plumbing; note plain `just test frogdb-server` does not compile `#[cfg(feature = "turmoil")]` tests).

```bash
just concurrency > "$TMPDIR/final_conc.log" 2>&1 ; tail -40 "$TMPDIR/final_conc.log"
```
Expected: green (shuttle + turmoil `simulation` sweep, which now includes the four flipped `regression_` pins — none `#[ignore]`d).

```bash
just lint && just fmt
```
Expected: clean (no clippy warnings; formatter idempotent).

- [ ] **Step 4: Commit.**

```bash
git add .scratch/concurrency-testing/proposals/lazy-expiry-parity.md frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs frogdb-server/crates/core/tests/shard_driver/scenario_s5.rs
git commit -m "docs(proposal): lazy-expiry parity fix stage complete — 4 gaps closed, regression pins active"
```

---

## Honest Scoping Notes

**What this plan does NOT cover (deliberate non-goals):**

- **Full active-expiry effect parity for lazy purge.** Mechanism 1 applies only the two effects the acceptance criteria enumerate — **shard-version bump** and **XREADGROUP-waiter drain** — because both are idempotent (safe to apply at multiple drain seams). `apply_expiry_effects` also fires: client-tracking invalidation, search-index deletion, the `expired` keyspace notification, and the USDT key-expired probe. Lazy purge does **not** fire these here. Reason: they are **not idempotent-safe** across the multiple seams a single lazy removal can be reported through (a command's own purge, `StreamSatisfaction::check_key`'s purge, the EXEC watch purge), so applying them risks double-notifications; and they are outside the four confirmed gaps. **Lazy-expiry keyspace-notification / client-tracking / search-index parity is a follow-up.** If pursued, it needs a single canonical drain point (e.g. once at the end of `dispatch_message`) so effects fire exactly once per removal, plus its own D8 repros.

- **Simplifying the coarse EXEC version bump.** After Mechanism 2, the `purge_expired_watches` version bump and the `check_watches` `live_at_watch` clause both independently abort the single-watcher F3 case — the bump is now partly redundant. This plan keeps the bump (lower risk, F3 pins stay green by the established path). Collapsing to the per-key model alone (fewer over-aborts) is a possible follow-up, gated on re-characterizing S2 over-aborts.

- **Cross-shard WATCH validation via VLL-EXEC.** Single-shard EXEC only (same-slot ⟹ same shard in standalone mode). The proposal already lists cross-shard WATCH as out of scope.

- **`get_and_delete` / `get_mut` / `set_with_options` / `touch` / `set_expiry` lazy-purge reporting.** These funnel through `check_and_delete_expired` and so *do* report — and the report is drained at the `execute_command_inner` seam. They are write/mutation paths that already bump the version themselves, so the extra bump is a harmless no-op-in-effect; no separate handling is needed. Called out so the implementer does not add per-method reporting logic (constraint 1: anchor at the removal site, not per-method).

- **Reducing coarse over-aborts from Mechanism 1.** A lazy-expiry-triggering **read** now bumps the shard version, so it can over-abort unrelated watchers on the same shard (the accepted coarse-version behavior, characterized by `s2_zero_false_negatives_and_over_abort_characterized`). This matches Redis `signalModifiedKey`-on-expiry for the expired key itself; the over-abort of *unrelated* keys is FrogDB's existing coarse approximation, not a regression. Not narrowed here.

- **Persistence / durability.** No WAL or recovery interaction; lazy purge is an in-memory removal already replicated via the normal DEL propagation path.

---

## Open design questions for the human (rule on before execution)

1. **Reads now bump the shard version (Mechanism 1).** A third party's lazy-expiry-triggering read incrementing `shard_version` is a new source of WATCH over-aborts for *unrelated* watchers on the same shard. This is the accepted coarse-version envelope and matches Redis's touch-on-expiry for the expired key, but it is a behavior change. **Confirm acceptable**, or require Mechanism 1's WATCH invalidation to also go per-key (larger change, folds into Mechanism 2).

2. **Watch-tuple encoding: `(Bytes, u64, bool)` vs a named `WatchEntry` struct.** Task 3 uses the tuple to minimize churn across ~8 files. A struct reads better and is easier to extend (e.g. if lazy-expiry keyspace-notification parity later needs more per-watch state). **Pick one before Task 3** — switching mid-plumbing is costly.

3. **Effect scope (Honest Scoping item 1).** This plan intentionally omits keyspace-notification / client-tracking / search-index effects for lazy purge (idempotency + scope). **Confirm** that lazy-expiry `expired` keyspace notifications are acceptable to defer, or promote that parity into this stage (adds a canonical single-drain-point refactor + new D8 repros).

4. **Gap-4 shard-driver arm expressibility (Task 4 Step 5).** The two-watcher race may be awkward to drive deterministically in the single-task shard driver (the stale watcher's WATCH-time purge must land between B's version snapshot and B's EXEC). The turmoil repro is the authoritative real-path pin. **Confirm** it is acceptable for the shard-driver arm to assert the `check_watches` clause more directly (manually-expired watched key) if the full two-watcher interleave is not cleanly expressible, rather than adding harness machinery.
