# WATCH is never slot-validated — CAS over a slot the node no longer owns

Status: needs-triage
Type: AFK
Origin: adversarial review of exec-slot-revalidation implementation (2026-07-28)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: cluster / transactions

## Problem

`WATCH_SPEC` is `KeySpec::All` + `ExecutionStrategy::ConnectionLevel(Transaction)`
(`transaction_conn_command.rs:236-244`). The `TransactionControl` dispatch stage
(position 5 of `PRE_DISPATCH_ORDER`) handles WATCH before `ClusterSlotValidation`
(position 14) is ever reached, so WATCH bypasses slot validation structurally — the
exec-slot-revalidation fix to `is_cluster_exempt` cannot cover it (un-exempting
`Transaction` is a no-op given the stage ordering).

Watch keys also never enter `BatchKeys` at EXEC time: `fold_queued_batch` folds the
*queued commands'* keys only.

## Failure scenario

Node A formerly owned slot S, now migrated away.

```
WATCH {S}k        -> +OK on A (no MOVED, wrong per Redis: watch on non-owner is MOVED)
MULTI
PING              -> queued (keyless body)
EXEC              -> executes on A after CAS-checking {S}k, a slot A no longer owns
```

The EXEC-entry validation sees an empty batch (PING is keyless) and serves locally.
The CAS decision is made against a stale, non-owned copy of `{S}k`. Narrow — requires
a keyless (or same-slot-exempt) transaction body — but a real correctness hole in the
WATCH contract.

## Fix direction

Slot-validate in the WATCH handler (or the `TransactionControl` stage): reply `-MOVED`
when a watched key's slot is not owned, matching Redis (`clusterRedirectClient` runs
for WATCH like any keyed command). Additionally consider folding watched keys into
`BatchKeys` at EXEC entry so a migration between WATCH and EXEC redirects the
transaction rather than CAS-ing a stale slot.
