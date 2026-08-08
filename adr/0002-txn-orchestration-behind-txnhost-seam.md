# Transaction orchestration lives in frogdb-txn behind the TxnHost seam

MULTI/EXEC correctness kept regressing because the algorithm was welded into the
134K-LOC server crate, where its failure modes (redirects, watch aborts, pause-barrier
re-validation, shard death) could only be forced through live-topology integration tests
and the suite was too big to mutation-test. We extracted the connection-side orchestration
— transaction state, the EXEC algorithm, outcome/metric mapping — into `frogdb-txn` behind
an object-safe `TxnHost` trait; `ConnectionHandler` implements the trait and everything
touching connection dispatch, the registry, `SlotValidator`, or TLS stays server-side.
The shard-side execution engine deliberately stays in `frogdb-core` (test-binary isolation
instead of a risky move), and `transaction_conn_command.rs` stays with the server because
its executors are inseparable from `ConnCtx` dispatch.

Consequences: a `MockTxnHost` forces every `TransactionOutcome` variant in sub-second unit
tests (a wildcard-free match makes a new variant a compile error until it has a forcing
test); the crate is small enough that a full cargo-mutants run takes ~2 minutes, so the
90% mutation gate is enforceable — frogdb-txn and frogdb-vll both hold 100% (caught /
caught+missed) as of lock. The failure-mode contract is
`.scratch/hardening/specs/txn-failure-modes.md` (+ vll), enforced two-directionally by
`just lint-failure-modes`. The cost is the trait indirection: new transaction behavior
needs a seam decision (algorithm side or host side); when in doubt, plain-data signatures
through the trait.
