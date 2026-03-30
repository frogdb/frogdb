---
name: distributed-audit
description: >
  Correctness audit for distributed systems, concurrency bugs, replication correctness,
  failover safety, split-brain, WAL ordering, cross-shard atomicity, consistency violations,
  race conditions, deadlocks, lost wakeups, linearizability, epoch monotonicity, fencing,
  VLL lock ordering, channel backpressure, snapshot consistency. Trigger on: "audit",
  "is this safe", "could this lose data", "does this break linearizability",
  "correctness check", "race condition", "split-brain", "replication lag",
  "review for correctness", "distributed systems review", "concurrency review",
  "is this correct under failure", "what happens if this crashes".
---

# FrogDB Distributed Systems Auditor

Structured correctness audits for changes touching distributed or concurrent subsystems.
Covers multi-node concerns (replication, consensus, failover, partitions) AND single-node
concerns (races, lock ordering, atomicity, channel safety).

## When to Use

- After implementing changes to cluster, replication, persistence, or concurrency code
- During PR review of changes that touch distributed subsystems
- When designing new features that affect consistency or durability
- When investigating production anomalies (data loss, inconsistency, deadlock)

## Audit Tiers

Calibrate depth to the change scope:

| Tier | When | Output |
|------|------|--------|
| **Quick Assessment** | Single-file change, bug fix, new command handler | 3-5 sentence risk assessment + test recommendation |
| **Focused Audit** | Cross-crate change, new feature touching cluster/replication/persistence | Risk matrix + invariant checks + testing plan |
| **Full Report** | Architectural change, new subsystem, protocol change, consensus changes | Full structured report with fault analysis, invariant proofs, test matrix |

**Tier Selection:**
- If the change is within a single crate and doesn't touch data flow → Quick Assessment
- If the change crosses crate boundaries OR touches persistence/replication/cluster → Focused Audit
- If the change modifies protocols, adds subsystems, or changes consensus behavior → Full Report

---

## Audit Workflow

### Step 1: Identify Change Scope

Read the diff (or design doc). Map changed files to crates using the crate-map in
`~/.claude/skills/db-architect/references/crate-map.md`. Identify:

- Which crates are affected
- Whether the change crosses crate boundaries
- Whether the change touches hot paths (shard worker loop, connection handler, WAL write)

### Step 2: Classify Risk Domains

For each changed file, classify which risk domains are affected:

| Risk Domain | Trigger Files/Paths |
|-------------|---------------------|
| Shard-local concurrency | `crates/core/src/shard/`, `crates/core/src/store/` |
| Cross-shard atomicity | `crates/vll/src/`, scatter-gather in `crates/server/src/connection/` |
| Persistence / WAL | `crates/persistence/src/`, `crates/core/src/persistence/` |
| Replication | `crates/replication/src/`, `crates/server/src/commands/replication.rs` |
| Cluster topology | `crates/cluster/src/`, `crates/server/src/commands/cluster/` |
| Failover | `crates/cluster/src/state.rs`, promotion/demotion paths |
| Client-observable consistency | `crates/server/src/connection/`, routing, dispatch |
| Pub/sub routing | `crates/server/src/connection/handlers/pubsub.rs`, `crates/core/src/pubsub/` |

### Step 3: Check Invariants

Read `references/invariants.md` for the full invariant catalog. For each affected risk domain,
check whether the change PRESERVES, puts AT RISK, or VIOLATES the relevant invariants.

**For each invariant, state one of:**
- **PRESERVED** — Change does not affect this invariant (explain briefly)
- **AT RISK** — Change could violate under certain conditions (explain conditions + mitigations)
- **VIOLATED** — Change breaks this invariant (explain how and what to fix)

### Step 4: Analyze Fault Scenarios

Read `references/fault-taxonomy.md`. For each affected risk domain, identify which fault
scenarios apply. Focus on:

1. What happens if a crash occurs mid-operation?
2. What happens under network partition?
3. What happens under resource exhaustion?
4. Are there new race conditions or ordering violations?
5. Can the change cause a previously-impossible state?

### Step 5: Recommend Testing

Read `references/testing-patterns.md`. Based on risk domains and fault scenarios, recommend:

1. Which existing test suites to run
2. Whether new tests are needed (and which type)
3. Specific Jepsen tests if the change affects cluster/replication/failover

### Step 6: Produce Output

Use the appropriate output template (see Output Templates section below).

---

## Change-to-Risk Mapping

Core lookup table — "if you changed X, check Y":

### `crates/core/src/shard/`
- **Risk:** Shard-local concurrency, message ordering
- **Check:** Per-shard linearizability (Invariant 1), shared-nothing (Invariant 7)
- **Watch for:** Mutable references across `.await` points, blocking calls in async context,
  new shared state between shard workers
- **Faults:** Lost wakeups, message reordering, shard worker panic

### `crates/core/src/store/`
- **Risk:** Data structure correctness, expiry handling
- **Check:** Per-shard linearizability (Invariant 1), total order per key
- **Watch for:** TOCTOU between key existence check and modification, missing WAL writes

### `crates/vll/src/`
- **Risk:** Cross-shard atomicity, deadlock freedom
- **Check:** VLL sorted lock order (Invariant 2), deadlock freedom
- **Watch for:** Lock acquisition NOT in sorted shard order, missing lock releases on error
  paths, new lock modes that break ordering guarantees
- **Faults:** Deadlock (if ordering violated), livelock, starvation, timeout handling

### `crates/persistence/src/`
- **Risk:** Durability, WAL sequence ordering, snapshot consistency
- **Check:** WAL sequence ordering (Invariant 3), recovery consistency (Invariant 6)
- **Watch for:** WAL writes without sequence numbers, non-atomic WriteBatch operations,
  snapshot COW semantics violations, corruption recovery edge cases
- **Faults:** Mid-write crash, mid-snapshot crash, disk full, WAL corruption, sequence gaps

### `crates/persistence/src/wal.rs`
- **Risk:** All persistence risks, plus replication ordering
- **Check:** WAL sequence ordering (Invariant 3), replication consistency (Invariant 4)
- **Critical:** WAL is the source of truth for replication. Any ordering violation here
  propagates to all replicas.

### `crates/replication/src/`
- **Risk:** Replication consistency, PSYNC protocol correctness
- **Check:** Replication consistency (Invariant 4), failover correctness (Invariant 5)
- **Watch for:** Replication ID handling errors, sequence number gaps, FULLRESYNC checkpoint
  integrity, frame checksum validation, partial sync edge cases
- **Faults:** Replica streaming stall, corruption detection, OOM during checkpoint,
  asymmetric network failure, CPU starvation of replication thread

### `crates/replication/src/primary.rs`
- **Additional risk:** WAL streaming order, backpressure propagation
- **Check:** WAL entries sent in sequence order, TCP backpressure not causing data loss

### `crates/replication/src/replica.rs`
- **Additional risk:** Stale reads, promotion correctness
- **Check:** Replica applies WAL in order, secondary replication ID preserved on promotion

### `crates/cluster/src/`
- **Risk:** Raft state machine correctness, epoch monotonicity, topology consistency
- **Check:** Epoch monotonicity (Invariant 5), failover correctness (Invariant 5)
- **Watch for:** Epoch comparisons using `<` instead of `<=`, topology updates not gated
  on promotion completion, slot assignment gaps or overlaps
- **Faults:** Split-brain window, stale epoch, Raft divergence, orchestrator isolation

### `crates/cluster/src/state.rs`
- **Critical:** Raft state transitions, leader election
- **Check:** State machine transitions are valid, no state reached without proper predecessor

### `crates/server/src/connection/`
- **Risk:** Routing correctness, pipeline ordering, client-observable consistency
- **Check:** Connection ordering (Invariant 10), pub/sub routing (Invariant 8)
- **Watch for:** MOVED/ASK redirect loops, scatter-gather partial results leaking,
  pipeline response ordering violations, transaction state leaks between connections
- **Faults:** Connection exhaustion, scatter-gather timeout, MOVED/ASK loops

### `crates/server/src/connection/handlers/pubsub.rs`
- **Risk:** Pub/sub message ordering, broadcast routing
- **Check:** Pub/sub routing via shard 0 (Invariant 8)
- **Watch for:** Broadcast pub/sub fanning out to ALL shards (causes N× message delivery),
  sharded pub/sub using wrong routing

### `crates/server/src/connection/dispatch.rs`
- **Risk:** Command routing, execution strategy selection
- **Check:** Correct ExecutionStrategy for each command, CROSSSLOT validation
- **Watch for:** Read commands routed as writes, missing CROSSSLOT checks for new
  multi-key commands

---

## FrogDB Invariants (Summary)

Read `references/invariants.md` for full details. Brief listing:

1. **Per-shard linearizability** — Within a single shard, operations are linearizable
2. **Cross-shard atomicity (VLL)** — Multi-shard ops use sorted lock order, preventing deadlocks
3. **WAL sequence ordering** — WAL entries have monotonically increasing sequence numbers
4. **Replication consistency** — WAL streaming preserves operation order to replicas
5. **Failover correctness** — Epoch monotonicity + fencing prevent stale-primary writes
6. **Recovery consistency** — WAL replay + snapshot produces correct state
7. **Shared-nothing** — No shared mutable state between shard workers
8. **Pub/sub routing** — Broadcast pub/sub coordinated via shard 0 only
9. **Channel backpressure** — Bounded channels provide natural flow control
10. **Connection ordering** — Pipeline responses match request order per connection

---

## Fault Taxonomy (Summary)

Read `references/fault-taxonomy.md` for the full catalog. Categories:

- **Timing faults** — Race conditions, TOCTOU, message reordering, lost wakeups
- **Crash faults** — Mid-write, mid-snapshot, shard panic, OOM kill, crash during replication
- **Network faults** — Symmetric/asymmetric partition, split-brain window, slow network
- **Resource exhaustion** — Memory OOM, disk full, connection exhaustion, channel backpressure
- **Protocol faults** — Stale epoch, replication ID mismatch, WAL sequence gap, MOVED/ASK loops
- **Concurrency faults** — Deadlock (prevented by VLL), livelock, mutable ref across `.await`

---

## Testing Strategy

Decision tree mapping risk domains to testing tools. Read `references/testing-patterns.md`
for full templates and guidance.

| Risk Domain | Primary Tool | Secondary Tool | When to Add Tests |
|-------------|-------------|----------------|-------------------|
| Shard-local concurrency | Shuttle | Unit tests | Any change to shard worker loop or store |
| Cross-shard atomicity | Shuttle + Turmoil | Integration tests | Any change to VLL or scatter-gather |
| Network partition | Turmoil | Jepsen | Changes to replication or cluster |
| WAL / recovery | Integration tests | Jepsen crash suite | Changes to persistence or WAL |
| Replication | Jepsen replication suite | Integration tests | Changes to replication crate |
| Failover | Jepsen raft suite | Turmoil | Changes to cluster state or promotion |
| Linearizability | Jepsen register + WGL | Shuttle | Any change affecting operation ordering |
| Pub/sub | Integration tests | — | Changes to pub/sub routing |

**Existing test suites to check:**
- `just test` — All unit and integration tests
- `just concurrency` — Shuttle + Turmoil concurrency tests
- `just jepsen-suite single` — Single-node baseline
- `just jepsen-suite crash` — Crash recovery
- `just jepsen-suite replication` — Replication correctness
- `just jepsen-suite raft` — Raft cluster correctness

---

## Output Templates

### Quick Assessment

```
## Quick Assessment: [Change Description]

**Risk level:** Low / Medium / High
**Risk domains:** [list]

[3-5 sentences explaining what could go wrong, or confirming safety]

**Invariants:** All preserved / [list any at-risk]
**Recommended tests:** [specific test commands]
```

### Focused Audit

```
## Focused Audit: [Change Description]

### Change Scope
| Crate | Files | Change Type |
|-------|-------|-------------|

### Risk Matrix
| Risk Domain | Level | Concern |
|-------------|-------|---------|

### Invariant Check
| # | Invariant | Status | Notes |
|---|-----------|--------|-------|
| 1 | Per-shard linearizability | PRESERVED / AT RISK / VIOLATED | |
| 2 | Cross-shard atomicity | ... | |
| ... | ... | ... | |

### Fault Scenarios
[For each applicable fault scenario from the taxonomy:]
- **[Fault name]:** [What happens, whether the change handles it correctly]

### Testing Plan
| Test Type | Command | Purpose |
|-----------|---------|---------|
| Existing | `just test <crate>` | Verify basic correctness |
| Existing | `just concurrency` | Verify concurrency safety |
| New | [description] | [what it verifies] |

### Recommendations
[Numbered list of specific actions]
```

### Full Report

```
## Full Distributed Systems Audit: [Change Description]

### Executive Summary
[2-3 sentences: what changed, overall risk assessment, key finding]

### Change Scope
| Crate | Files | Change Type | Risk Domain |
|-------|-------|-------------|-------------|

### Architectural Impact
[How this change affects the overall system architecture and data flow]

### Invariant Analysis
| # | Invariant | Status | Evidence |
|---|-----------|--------|----------|
[All 10 invariants checked with evidence for each status]

### Fault Analysis
[For each fault category from the taxonomy:]

#### Timing Faults
[Applicable scenarios, analysis, mitigations]

#### Crash Faults
[Applicable scenarios, analysis, mitigations]

#### Network Faults
[Applicable scenarios, analysis, mitigations]

#### Resource Exhaustion
[Applicable scenarios, analysis, mitigations]

#### Protocol Faults
[Applicable scenarios, analysis, mitigations]

#### Concurrency Faults
[Applicable scenarios, analysis, mitigations]

### Testing Matrix
| Risk Domain | Existing Coverage | Gaps | Recommended New Tests |
|-------------|-------------------|------|----------------------|

### Jepsen Coverage
| Test | Relevance | Run? | Expected Result |
|------|-----------|------|-----------------|

### Recommendations
[Ordered by priority]
1. **Critical:** [Must fix before merge]
2. **Important:** [Should fix before merge]
3. **Advisory:** [Consider for follow-up]

### Open Questions
[Unresolved concerns requiring team discussion]
```

---

## Design Review Mode

When auditing a design (not code), adjust the workflow:

1. **Skip** Step 1 (no diff to read). Instead, read the design doc or plan.
2. **Focus** Step 3 on invariant preservation *arguments* — does the design explain
   how each affected invariant is maintained?
3. **Focus** Step 4 on proposed data flows — trace message sequences under failure.
4. **Check** fault scenario coverage — does the design address what happens when
   each relevant component fails?
5. **Check** that the design includes test strategy covering concurrency and failure modes.

**Key questions for design review:**
- Under which failure modes does this design lose data?
- Under which failure modes does this design violate linearizability?
- Is there a state that can be reached but never left (deadlock, stuck state)?
- Are all timeouts bounded? What happens when they fire?
- Does this design introduce new shared mutable state?
- Does this change the consistency model visible to clients?

---

## Reference File Pointers

| When | Read |
|------|------|
| Checking specific invariants | `references/invariants.md` |
| Analyzing fault scenarios | `references/fault-taxonomy.md` |
| Choosing tests or writing new ones | `references/testing-patterns.md` |
| Mapping files to crates | `~/.claude/skills/db-architect/references/crate-map.md` |
| Understanding request flows | `website/src/content/docs/architecture/request-flows.md` |
| Jepsen test catalog | `~/.claude/skills/jepsen-testing/references/test-catalog.md` |
| Architectural design | Use `/db-architect` skill |
| Running Jepsen tests | Use `/jepsen-testing` skill |
| Running builds/tests | Use `/check` skill |

---

## When to Defer

| Situation | Skill |
|-----------|-------|
| Designing a new feature (not auditing) | `/db-architect` |
| Running Jepsen tests | `/jepsen-testing` |
| Running build/lint/test | `/check` |
| Performance testing | `/benchmark` |
| Refactoring code structure | `/refactorer` |
