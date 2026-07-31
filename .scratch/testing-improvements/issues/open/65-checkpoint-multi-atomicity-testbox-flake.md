# 65 — checkpoint single-shard MULTI atomicity test flakes under full-suite load

Status: needs-triage

Observed 2026-07-28 on a Blacksmith testbox (8 vcpu, aarch64) during the config-mutability
round's final full-suite gate:

```
FAIL integration_persistence::test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave
single-shard transaction cut captured mixed generations for tag g2: [Some("470"), Some("458"), Some("458"), Some("458"), Some("458")]
```

- Failed under full-suite parallelism **including its nextest retry**; passed **5/5
  consecutive `--retries 0` runs in isolation** on the same box at the same commit.
- The same tree passed the full suite earlier the same day (different load pattern), so it
  is load-dependent, same operational profile as the tracked
  `test_broadcast_lag_disconnect_and_resync` testbox-only failures.
- Test introduced by issue 43 (`204922bc`, BGSAVE checkpoint cut contract).

Why this deserves triage rather than a shrug: the asserted contract — a single-shard
MULTI must be captured atomically by a concurrent BGSAVE checkpoint cut — is a real
product guarantee. A load-only tear could be (a) a residual race in the checkpoint cut
the issue-43 work pinned, visible only when the BGSAVE thread is starved into a wider cut
window, or (b) a test-synchronization gap (e.g. the generation-writer loop overlapping the
cut in a way the assertion doesn't intend). Distinguishing (a) from (b) is the work.

Repro direction: run the test with the suite's parallelism (`cargo nextest run --all` or a
CPU-contention harness alongside), not solo; instrument the checkpoint cut boundaries vs
the MULTI apply span for the failing tag.
