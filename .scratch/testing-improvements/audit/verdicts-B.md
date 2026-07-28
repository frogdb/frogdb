# Verdicts B (transactions/scripting)
1 subscribe-family-in-multi: ADJUSTED L1/C1 — HEADLINE REFUTED. Redis src (pubsub.c, reset.json, subscribe.json): subscribe guard = (DENY_BLOCKING && !CLIENT_MULTI) — MULTI-exempted, so SUBSCRIBE/PSUBSCRIBE EXECUTE at EXEC; UNSUBSCRIBE family unguarded; no NO_MULTI on SUBSCRIBE or RESET. FrogDB plain-family + ResetIntercept MATCH Redis. Residue (real, trivial): FrogDB rejects SUNSUBSCRIBE/PUBSUB in MULTI where Redis allows; SSUBSCRIBE reject uses non-Redis string; zero subscribe-in-MULTI tests. Reframe task accordingly.
2 allow-cross-slot-standalone-must-not-relax-multi: CONFIRMED L2/C3. Note: real config field named differently than audit assumed (verify actual name when writing task); no test enables flag + proves MULTI still CROSSSLOTs; no plain-key non-WATCH standalone case.
3 cross-slot-multi-weak-assertion: CONFIRMED L2/C1.
4 wait-in-multi-untested: ADJUSTED L1/C2. Correct-by-design: WAIT = ExecutionStrategy::Standard → execute_transaction (transaction.rs:277-298) → shard non-blocking branch (replication.rs:568-575); WaitIntercept documented reached only outside MULTI (dispatch.rs:491-494). Missing regression guard vs hang only.
5 blmpop-bzmpop-nonblocking-in-multi-omitted: CONFIRMED L1/C2 (blocking.rs:307-318,614-625).
6 dead-undeclared-key-validation-and-misdocumented-behavior: CONFIRMED L2/C2.
Dedup: none vs issues 05/06/10/11/16.
