# Admission model — documented mutation battery + gap closure (lifecycle steps 4–5)

Base: `9c21b30c` (admission model rework: `canMeet` physicality, intent write-once ghost,
durable-record keying with `effective_id`/`configured_id`/monotone `next_id`).
Model files: `specs/quint/cluster_admission{,_logic,_machine,_types}.qnt`.
Authority: `.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md` (W1, "model
lifecycle standardized" — battery + honest-miss closure is mandatory for every model).
Format follows the migration model's Q4 report
(`.scratch/cluster-correctness/quint-rework-reports/issue31-quint-q4-report.md`).

Base surface: 8 invariants, 6 witnesses, 8 run tests.

## Verdict vocabulary

| Verdict | Meaning |
|---|---|
| **CAUGHT-T** | a `run` test fails (`quint test specs/quint/cluster_admission.qnt`) |
| **CAUGHT-P** | an `inv_*` is violated by a sampled `quint run` (property oracle) |
| **MISSED** | model still green on both oracles — every such row carries an honest-miss analysis below |
| **N/A** | the mutation is not a well-formed single edit of this model (does not typecheck, or targets a sizing knob with no semantics) |

## Mechanics (discipline actually followed)

Every mutation is one exact text replacement, applied to a pristine copy, run, then restored
**byte-for-byte** from a backup taken before the battery started. `git checkout -- specs/quint/`
was never used (other agents hold untracked WIP in that directory); after each row
`git --no-optional-locks diff --stat -- specs/quint/cluster_admission*` was confirmed empty.
Driver: `battery.py` in the session scratchpad (rows encoded verbatim as the `old`/`new`
strings in the table below; the driver refuses a pattern that does not occur exactly once).

Per row:

```bash
# test oracle
quint test specs/quint/cluster_admission.qnt
# property oracle (both seeds; per-invariant re-run identifies the violated one)
INV="inv_no_usurper inv_single_routable_group inv_restart_deterministic inv_intent_write_once \
inv_identity_survives_id_churn inv_configured_id_stable inv_effective_ids_distinct \
inv_meet_no_absorption inv_member_carries_raft_state"   # 9th added by the closure pass
quint run specs/quint/cluster_admission.qnt --max-samples=500 --max-steps=20 --seed=0x1 --invariants $INV
quint run specs/quint/cluster_admission.qnt --max-samples=500 --max-steps=20 --seed=0x2 --invariants $INV
# witness oracle (only consulted for rows green on both of the above)
quint run specs/quint/cluster_admission.qnt --max-samples=500 --max-steps=20 --witnesses \
  witnessBootstrapped witnessJoinedViaMeet witnessRepurposedHazard witnessFreshIdAfterRestart \
  witnessConfiguredIdPinned witnessStrangerIdentity
```

(Quint's CLI takes `--invariants`/`--witnesses` **space-separated**, and the repo's shell is
zsh, which does not word-split an unquoted variable — hence every invocation runs under
`bash -c`, as the campaign note requires.)

Budget justification: the base walk reaches every witness at 500×20 (lowest,
`witnessJoinedViaMeet`, 21.6% of traces; the rest 66–92%), so 500×20 is not a thin budget for
this model. Rows that came out MISSED were escalated to 4000×40 across three seeds before the
verdict was recorded (see the escalation note per row).

## Battery table

The table was authored in full — one row per guard conjunct, per effect field-update, and per
load-bearing invariant clause — **before** any mutation was run. `Expected` is the
pre-registered prediction, `Verdict` the observed outcome; rows where the two disagree are
called out in the analysis section.

| Row | Target (file:line — invariant of the code the edit breaks) | Mutation (`old` → `new`, one exact replacement) | Expected catcher (pre-registered) | Verdict | Evidence |
|---|---|---|---|---|---|
| A01 | _logic.qnt:43 `configuredIdFor` — guard: pin off ⇒ no configured id | `if (not(pin)) None` → `if (pin) None` | configuredIdStableAcrossRestartTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,configuredIdStableAcrossRestartTest,pinAdoptsRunningIdTest,restartMintsFreshIdTest QNT508 |
| A02 | _logic.qnt:43 `configuredIdFor` — pin adopts the *running* id | `else match ns.effective_id { \| Some(e) => Some(e) \| None => Some(boxId) }` → `else match ns.effective_id { \| Some(e) => Some(boxId) \| None => Some(boxId) }` | *(pre-registered miss)* pin re-derives from the record key, not the running id | **CAUGHT-T** | pinAdoptsRunningIdTest QNT508 |
| A03 | _logic.qnt:43 `configuredIdFor` — pin on a never-booted record uses the box id | `else match ns.effective_id { \| Some(e) => Some(e) \| None => Some(boxId) }` → `else match ns.effective_id { \| Some(e) => Some(e) \| None => None }` | configuredIdStableAcrossRestartTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest QNT508 |
| A04 | _logic.qnt:49 `idForBoot` — a configured id is used as the boot id | `match cfg { \| Some(c) => c \| None => fresh }` → `match cfg { \| Some(c) => fresh \| None => fresh }` | inv_configured_id_stable + configuredIdStableAcrossRestartTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest,pinAdoptsRunningIdTest QNT508 + inv inv_configured_id_stable,inv_effective_ids_distinct,inv_single_routable_group |
| A05 | _logic.qnt:54 `mintsFreshId` — mints iff no configured id (inverted) | `pure def mintsFreshId(cfg: Option[NodeId]): bool = cfg == None` → `pure def mintsFreshId(cfg: Option[NodeId]): bool = cfg != None` | restartMintsFreshIdTest + inv_effective_ids_distinct | **CAUGHT-T** | adminResetMayRewriteIntentTest,configuredIdStableAcrossRestartTest,meetRefusesForeignRaftStateTest,restartMintsFreshIdTest QNT508 + inv inv_effective_ids_distinct,inv_single_routable_group |
| A06 | _logic.qnt:54 `mintsFreshId` — mints iff no configured id (blanked) | `pure def mintsFreshId(cfg: Option[NodeId]): bool = cfg == None` → `pure def mintsFreshId(cfg: Option[NodeId]): bool = false` | restartMintsFreshIdTest + inv_effective_ids_distinct | **CAUGHT-T** | adminResetMayRewriteIntentTest,configuredIdStableAcrossRestartTest,meetRefusesForeignRaftStateTest,pinAdoptsRunningIdTest,restartMintsFreshIdTest QNT508 + inv inv_effective_ids_distinct,inv_single_routable_group |
| A07 | _logic.qnt:59 `canBoot` — only an Undecided record first-boots | `pure def canBoot(ns: NodeState): bool = ns.intent == Undecided` → `pure def canBoot(ns: NodeState): bool = true` | issue25UsurpationUnreachableTest + inv_restart_deterministic (decision_epoch) | **CAUGHT-T** | issue25UsurpationUnreachableTest QNT511 + inv inv_meet_no_absorption,inv_restart_deterministic |
| A08 | _logic.qnt:75 `applyBoot.identified` — a boot always sets `effective_id` | `val identified = { ...ns, effective_id: Some(selfId), configured_id: cfg,` → `val identified = { ...ns, effective_id: ns.effective_id, configured_id: cfg,` | bootstrapThenMeetTest / restartMintsFreshIdTest | **CAUGHT-T** | bootstrapThenMeetTest,configuredIdStableAcrossRestartTest,meetRefusedFromNonMemberTest,meetRefusesRepurposedNodeTest,pinAdoptsRunningIdTest,restartMintsFreshIdTest QNT508 + inv inv_configured_id_stable |
| A09 | _logic.qnt:75 `applyBoot.identified` — a boot records the configured id | `val identified = { ...ns, effective_id: Some(selfId), configured_id: cfg,` → `val identified = { ...ns, effective_id: Some(selfId), configured_id: None,` | configuredIdStableAcrossRestartTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest QNT508 |
| A10 | _logic.qnt:77 `applyBoot.identified` — a boot counts its mint | `id_mints: ns.id_mints + (if (mintsFreshId(cfg)) 1 else 0) }` → `id_mints: ns.id_mints }` | restartMintsFreshIdTest | **CAUGHT-T** | pinAdoptsRunningIdTest,restartMintsFreshIdTest QNT508 |
| A11 | _logic.qnt:79 `applyBoot` bootstrap — Raft is initialized | `{ ...identified, intent: Bootstrap, configured_bootstrap: cb, raft: Initialized(1),` → `{ ...identified, intent: Bootstrap, configured_bootstrap: cb, raft: Empty,` | inv_restart_deterministic c1 + bootstrapThenMeetTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,bootstrapThenMeetTest,meetRefusesForeignRaftStateTest,meetRefusesRepurposedNodeTest,repurposeRefusedOnUnclaimedNodeTest,restartMintsFreshIdTest QNT508,QNT511 + inv inv_meet_no_absorption,inv_member_carries_raft_state,inv_restart_deterministic,inv_single_routable_group |
| A12 | _logic.qnt:81 `applyBoot` bootstrap — the bootstrapper is routable | `routable_leader: true, member_of: Some(selfId),` → `routable_leader: false, member_of: Some(selfId),` | bootstrapThenMeetTest | **CAUGHT-T** | bootstrapThenMeetTest,restartMintsFreshIdTest QNT508 |
| A13 | _logic.qnt:81 `applyBoot` bootstrap — the bootstrapper is its own member | `routable_leader: true, member_of: Some(selfId),` → `routable_leader: true, member_of: None,` | inv_single_routable_group + bootstrapThenMeetTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,bootstrapThenMeetTest,meetRefusesForeignRaftStateTest,meetRefusesRepurposedNodeTest,repurposeRefusedOnUnclaimedNodeTest,restartMintsFreshIdTest QNT508 + inv inv_single_routable_group |
| A14 | _logic.qnt:82 `applyBoot` bootstrap — the decision bumps the epoch | `decision_epoch: ns.decision_epoch + 1, raft_origin: Some(selfId) }` → `decision_epoch: ns.decision_epoch, raft_origin: Some(selfId) }` | restartMintsFreshIdTest | **CAUGHT-T** | restartMintsFreshIdTest QNT508 |
| A15 | _logic.qnt:82 `applyBoot` bootstrap — Raft provenance is this node | `decision_epoch: ns.decision_epoch + 1, raft_origin: Some(selfId) }` → `decision_epoch: ns.decision_epoch + 1, raft_origin: None }` | restartMintsFreshIdTest | **CAUGHT-T** | meetRefusesRepurposedNodeTest,restartMintsFreshIdTest QNT508 |
| A16 | _logic.qnt:79 `applyBoot` bootstrap — intent is Bootstrap | `{ ...identified, intent: Bootstrap, configured_bootstrap: cb, raft: Initialized(1),` → `{ ...identified, intent: Join, configured_bootstrap: cb, raft: Initialized(1),` | inv_no_usurper + adminResetMayRewriteIntentTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,bootstrapThenMeetTest,restartMintsFreshIdTest QNT508 + inv inv_no_usurper |
| A17 | _logic.qnt:85 `applyBoot` join — a joiner has no Raft state | `{ ...identified, intent: Join, configured_bootstrap: cb, raft: Empty,` → `{ ...identified, intent: Join, configured_bootstrap: cb, raft: Initialized(1),` | issue25UsurpationUnreachableTest / bootstrapThenMeetTest | **CAUGHT-T** | bootstrapThenMeetTest,issue25UsurpationUnreachableTest,meetRefusedFromNonMemberTest QNT508 |
| A18 | _logic.qnt:86 `applyBoot` join — a joiner is not routable | `routable_leader: false, member_of: None,` → `routable_leader: true, member_of: None,` | inv_no_usurper + inv_restart_deterministic c2 + bootstrapThenMeetTest | **CAUGHT-T** | bootstrapThenMeetTest,configuredIdStableAcrossRestartTest,issue25UsurpationUnreachableTest QNT508 + inv inv_no_usurper,inv_restart_deterministic,inv_single_routable_group |
| A19 | _logic.qnt:86 `applyBoot` join — a joiner is in no cluster | `routable_leader: false, member_of: None,` → `routable_leader: false, member_of: Some(selfId),` | bootstrapThenMeetTest | **CAUGHT-T** | bootstrapThenMeetTest,meetRefusedFromNonMemberTest QNT508 + inv inv_member_carries_raft_state |
| A20 | _logic.qnt:87 `applyBoot` join — the decision bumps the epoch | `decision_epoch: ns.decision_epoch + 1 }` → `decision_epoch: ns.decision_epoch }` | *(pre-registered miss)* no test pins the join branch's epoch bump | **CAUGHT-T** | issue25UsurpationUnreachableTest QNT508 |
| A21 | _logic.qnt:87 `applyBoot` join — a joiner has no Raft provenance | `decision_epoch: ns.decision_epoch + 1 }` → `decision_epoch: ns.decision_epoch + 1, raft_origin: Some(selfId) }` | inv_meet_no_absorption | **CAUGHT-P** | inv_meet_no_absorption |
| A22 | _logic.qnt:92 `canRestart` — only a decided record restarts | `pure def canRestart(ns: NodeState): bool = ns.intent != Undecided` → `pure def canRestart(ns: NodeState): bool = true` | *(pre-registered miss)* restart admitted before the first boot | **CAUGHT-T** | restartRefusedBeforeFirstBootTest QNT511 |
| A23 | _logic.qnt:104 `applyRestart` — `intent` is not rewritten (write-once) | `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` → `{ ...ns, intent: if (cb) Bootstrap else Join, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` | inv_intent_write_once + issue25UsurpationUnreachableTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,configuredIdStableAcrossRestartTest,issue25UsurpationUnreachableTest,restartMintsFreshIdTest QNT508 + inv inv_intent_write_once,inv_no_usurper,inv_restart_deterministic |
| A24 | _logic.qnt:104 `applyRestart` — cluster identity is not re-minted | `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` → `{ ...ns, member_of: Some(selfId), raft_origin: Some(selfId), configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` | inv_identity_survives_id_churn + restartMintsFreshIdTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,restartMintsFreshIdTest QNT508 + inv inv_identity_survives_id_churn,inv_meet_no_absorption,inv_member_carries_raft_state |
| A25 | _logic.qnt:104 `applyRestart` — a reboot re-derives `effective_id` | `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` → `{ ...ns, configured_bootstrap: cb, effective_id: ns.effective_id, configured_id: cfg,` | restartMintsFreshIdTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,configuredIdStableAcrossRestartTest,restartMintsFreshIdTest QNT508 |
| A26 | _logic.qnt:104 `applyRestart` — a reboot re-reads `configured_id` | `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` → `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: ns.configured_id,` | inv_configured_id_stable + configuredIdStableAcrossRestartTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest,pinAdoptsRunningIdTest QNT508 + inv inv_configured_id_stable |
| A27 | _logic.qnt:105 `applyRestart` — a reboot counts its mint | `cfg, ⏎       id_mints: ns.id_mints + (if (mintsFreshId(cfg)) 1 else 0) }` → `cfg, ⏎       id_mints: ns.id_mints }` | restartMintsFreshIdTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest,restartMintsFreshIdTest QNT508 |
| A28 | _logic.qnt:104 `applyRestart` — a reboot does not make the node routable | `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` → `{ ...ns, routable_leader: true, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` | inv_no_usurper + issue25UsurpationUnreachableTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest,issue25UsurpationUnreachableTest QNT508 + inv inv_no_usurper,inv_restart_deterministic,inv_single_routable_group |
| A29 | _logic.qnt:104 `applyRestart` — a reboot records the current `cluster-enabled` config | `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` → `{ ...ns, configured_bootstrap: ns.configured_bootstrap, effective_id: Some(selfId), configured_id: cfg,` | *(pre-registered miss)* nothing pins that restart records the current config | **CAUGHT-T** | adminResetMayRewriteIntentTest,issue25UsurpationUnreachableTest QNT508 |
| A30 | _logic.qnt:104 `applyRestart` — a reboot preserves persisted Raft state | `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` → `{ ...ns, raft: Empty, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` | inv_restart_deterministic c1 + adminResetMayRewriteIntentTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,restartMintsFreshIdTest QNT508 + inv inv_meet_no_absorption,inv_member_carries_raft_state,inv_restart_deterministic,inv_single_routable_group |
| A31 | _logic.qnt:104 `applyRestart` — a reboot is not a new decision (no epoch bump) | `{ ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` → `{ ...ns, decision_epoch: ns.decision_epoch + 1, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,` | inv_restart_deterministic c3 + restartMintsFreshIdTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest,issue25UsurpationUnreachableTest,restartMintsFreshIdTest QNT508 + inv inv_restart_deterministic |
| A32 | _logic.qnt:127 `canMeet` c1 — the *inviter* must be in a cluster | `nNode.member_of != None, ⏎     mNode.effective_id != None,` → `mNode.effective_id != None,` | *(pre-registered miss)* MEET issued by a node that is in no cluster | **CAUGHT-T** | meetRefusedFromNonMemberTest QNT511 |
| A33 | _logic.qnt:127 `canMeet` c2 — the *joiner* must be a running process | `mNode.effective_id != None, ⏎     mNode.raft == Empty,` → `mNode.raft == Empty,` | meetRefusesUnbootedNodeTest | **CAUGHT-T** | meetRefusesUnbootedNodeTest QNT511 |
| A34 | _logic.qnt:127 `canMeet` c3 — the joiner must have no Raft state | `mNode.effective_id != None, ⏎     mNode.raft == Empty,` → `mNode.effective_id != None,` | meetRefusesForeignRaftStateTest + meetRefusesRepurposedNodeTest + inv_meet_no_absorption | **CAUGHT-T** | meetRefusesForeignRaftStateTest,meetRefusesRepurposedNodeTest QNT511 + inv inv_meet_no_absorption,inv_single_routable_group |
| A35 | _logic.qnt:139 `applyMeetJoiner` — the joiner adopts the inviter's Raft state | `{ ...mNode, raft: nNode.raft, member_of: nNode.member_of }` → `{ ...mNode, raft: mNode.raft, member_of: nNode.member_of }` | bootstrapThenMeetTest | **CAUGHT-T** | bootstrapThenMeetTest QNT508 + inv inv_member_carries_raft_state |
| A36 | _logic.qnt:139 `applyMeetJoiner` — the joiner adopts the inviter's cluster | `{ ...mNode, raft: nNode.raft, member_of: nNode.member_of }` → `{ ...mNode, raft: nNode.raft, member_of: mNode.member_of }` | bootstrapThenMeetTest | **CAUGHT-T** | bootstrapThenMeetTest QNT508 |
| A37 | _logic.qnt:139 `applyMeetJoiner` — MEET does not confer routability | `{ ...mNode, raft: nNode.raft, member_of: nNode.member_of }` → `{ ...mNode, raft: nNode.raft, member_of: nNode.member_of, routable_leader: true }` | inv_no_usurper + bootstrapThenMeetTest | **CAUGHT-T** | bootstrapThenMeetTest QNT508 + inv inv_no_usurper,inv_single_routable_group |
| A38 | _logic.qnt:139 `applyMeetJoiner` — MEET does not forge Raft provenance | `{ ...mNode, raft: nNode.raft, member_of: nNode.member_of }` → `{ ...mNode, raft: nNode.raft, member_of: nNode.member_of, raft_origin: mNode.effective_id }` | inv_meet_no_absorption | **CAUGHT-P** | inv_meet_no_absorption |
| A39 | _logic.qnt:148 `canRepurpose` c1 — the node must currently claim membership | `ns.member_of != None, ⏎     ns.raft != Empty,` → `ns.raft != Empty,` | *(pre-registered miss)* reset admitted against an already-unclaimed node | **CAUGHT-T** | repurposeRefusedOnUnclaimedNodeTest QNT511 |
| A40 | _logic.qnt:148 `canRepurpose` c2 — the node must hold Raft state | `ns.member_of != None, ⏎     ns.raft != Empty,` → `ns.member_of != None,` | *(pre-registered miss)* structurally redundant conjunct | **MISSED** | — |
| A41 | _logic.qnt:165 `applyRepurpose` — reset rewrites intent to Join | `{ ...ns, intent: Join, member_of: None, routable_leader: false }` → `{ ...ns, intent: ns.intent, member_of: None, routable_leader: false }` | meetRefusesRepurposedNodeTest + adminResetMayRewriteIntentTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,meetRefusesRepurposedNodeTest QNT508 |
| A42 | _logic.qnt:165 `applyRepurpose` — reset clears membership | `{ ...ns, intent: Join, member_of: None, routable_leader: false }` → `{ ...ns, intent: Join, member_of: ns.member_of, routable_leader: false }` | meetRefusesRepurposedNodeTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,meetRefusesRepurposedNodeTest,repurposeRefusedOnUnclaimedNodeTest QNT508 |
| A43 | _logic.qnt:165 `applyRepurpose` — reset clears routability | `{ ...ns, intent: Join, member_of: None, routable_leader: false }` → `{ ...ns, intent: Join, member_of: None, routable_leader: ns.routable_leader }` | inv_no_usurper | **CAUGHT-P** | inv_no_usurper,inv_restart_deterministic,inv_single_routable_group |
| A44 | _logic.qnt:165 `applyRepurpose` — reset leaves Raft state behind (the hazard) | `{ ...ns, intent: Join, member_of: None, routable_leader: false }` → `{ ...ns, intent: Join, member_of: None, routable_leader: false, raft: Empty }` | meetRefusesRepurposedNodeTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,meetRefusesRepurposedNodeTest,repurposeRefusedOnUnclaimedNodeTest QNT508 + inv inv_meet_no_absorption |
| A45 | _logic.qnt:165 `applyRepurpose` — reset leaves the residue's provenance | `{ ...ns, intent: Join, member_of: None, routable_leader: false }` → `{ ...ns, intent: Join, member_of: None, routable_leader: false, raft_origin: None }` | *(pre-registered miss)* the reset erases the residue's provenance | **CAUGHT-T** | meetRefusesRepurposedNodeTest QNT508 |
| A46 | _machine.qnt:92 `bootAs` — a fresh boot advances the mint source | `nodes' = nodes.set(n, applyBoot(ns, idForBoot(cfg, next_id), cfg, cb)), ⏎       next_id' = if (mintsFreshId(cfg)) next_id + 1 else next_id,` → `nodes' = nodes.set(n, applyBoot(ns, idForBoot(cfg, next_id), cfg, cb)), ⏎       next_id' = next_id,` | inv_effective_ids_distinct + meetRefusesForeignRaftStateTest | **CAUGHT-T** | adminResetMayRewriteIntentTest,meetRefusesForeignRaftStateTest,restartMintsFreshIdTest QNT508 + inv inv_effective_ids_distinct,inv_single_routable_group |
| A47 | _machine.qnt:114 `restartAs` — a fresh reboot advances the mint source | `nodes' = nodes.set(n, rebooted), ⏎       next_id' = if (mintsFreshId(cfg)) next_id + 1 else next_id,` → `nodes' = nodes.set(n, rebooted), ⏎       next_id' = next_id,` | inv_effective_ids_distinct | **CAUGHT-P** | inv_effective_ids_distinct |
| A48 | _machine.qnt:116 `restartAs` — `restartIntentWrite` detector | `restartIntentWrite: defects.restartIntentWrite or rebooted.intent != ns.intent,` → `restartIntentWrite: false,` | *(pre-registered miss)* detector ghost, never trips in the unmutated model | **MISSED** | — |
| A49 | _machine.qnt:117 `restartAs` — `restartIdentityRemint` detector | `restartIdentityRemint: defects.restartIdentityRemint ⏎           or rebooted.member_of != ns.member_of ⏎           or rebooted.raft_origin != ns.raft_origin,` → `restartIdentityRemint: defects.restartIdentityRemint ⏎           or rebooted.member_of != ns.member_of,` | *(pre-registered miss)* detector ghost, never trips in the unmutated model | **MISSED** | — |
| A50 | _machine.qnt:141 `meet` — a node cannot MEET itself | `n != m, ⏎       canMeet(nNode, mNode),` → `canMeet(nNode, mNode),` | *(pre-registered miss)* self-MEET structurally disabled | **MISSED** | — |
| A51 | _machine.qnt:143 `meet` — MEET writes the *joiner*, not the inviter | `nodes' = nodes.set(m, applyMeetJoiner(mNode, nNode)),` → `nodes' = nodes.set(n, applyMeetJoiner(nNode, mNode)),` | bootstrapThenMeetTest | **CAUGHT-T** | bootstrapThenMeetTest QNT508 + inv inv_restart_deterministic,inv_single_routable_group |
| A52 | _machine.qnt:79 `init` — mint source starts above every box id | `next_id' = FRESH_ID_BASE,` → `next_id' = 1,` | restartMintsFreshIdTest + inv_effective_ids_distinct | **CAUGHT-T** | adminResetMayRewriteIntentTest,configuredIdStableAcrossRestartTest,pinAdoptsRunningIdTest,restartMintsFreshIdTest QNT508 + inv inv_effective_ids_distinct,inv_single_routable_group |
| A53 | _machine.qnt:76 `init` — no id exists before the first boot | `effective_id: None, configured_id: None,` → `effective_id: Some(0), configured_id: None,` | meetRefusesUnbootedNodeTest + inv_effective_ids_distinct | **CAUGHT-T** | configuredIdStableAcrossRestartTest,meetRefusesForeignRaftStateTest,meetRefusesUnbootedNodeTest,restartMintsFreshIdTest,restartRefusedBeforeFirstBootTest QNT508 + inv inv_effective_ids_distinct,inv_single_routable_group |
| A54 | _machine.qnt:77 `init` — the epoch ghost starts at 0 | `decision_epoch: 0, id_mints: 0, raft_origin: None,` → `decision_epoch: 1, id_mints: 0, raft_origin: None,` | inv_restart_deterministic c3 + restartMintsFreshIdTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest,issue25UsurpationUnreachableTest,restartMintsFreshIdTest QNT508 + inv inv_restart_deterministic |
| A55 | _machine.qnt:73 `init` — every record starts Undecided | `intent: Undecided, configured_bootstrap: false, raft: Empty,` → `intent: Join, configured_bootstrap: false, raft: Empty,` | every run test (canBoot never admits) | **CAUGHT-T** | adminResetMayRewriteIntentTest,bootstrapThenMeetTest,configuredIdStableAcrossRestartTest,issue25UsurpationUnreachableTest,meetRefusedFromNonMemberTest,meetRefusesForeignRaftStateTest,meetRefusesRepurposedNodeTest,meetRefusesUnbootedNodeTest,pinAdoptsRunningIdTest,repurposeRefusedOnUnclaimedNodeTest,restartMintsFreshIdTest,restartRefusedBeforeFirstBootTest QNT508,QNT513 |
| A56 | _machine.qnt:88 `bootAs` — the pin is derived from the record key (box id) | `val cfg = configuredIdFor(ns, n, pin) ⏎     all { ⏎       canBoot(ns),` → `val cfg = configuredIdFor(ns, next_id, pin) ⏎     all { ⏎       canBoot(ns),` | configuredIdStableAcrossRestartTest | **CAUGHT-T** | configuredIdStableAcrossRestartTest QNT508 + inv inv_effective_ids_distinct,inv_single_routable_group |
| A57 | _machine.qnt:186 `step` — `repurpose` is reachable | `repurpose(n), ⏎       stutter,` → `stutter,` | *(pre-registered miss)* step-unwiring, witness-only observable | **MISSED** | witness-zero: witnessRepurposedHazard |
| A58 | _machine.qnt:185 `step` — `meet` is reachable | `meet(n, m), ⏎       repurpose(n),` → `repurpose(n),` | *(pre-registered miss)* step-unwiring, witness-only observable | **MISSED** | witness-zero: witnessJoinedViaMeet |
| A59 | _types.qnt:78 `FRESH_ID_BASE` — mints never collide with a box id | `pure val FRESH_ID_BASE: NodeId = 101` → `pure val FRESH_ID_BASE: NodeId = 2` | inv_effective_ids_distinct + restartMintsFreshIdTest | **CAUGHT-P** | inv_effective_ids_distinct,inv_single_routable_group |

**Totals — 59 rows: 48 CAUGHT-T, 5 CAUGHT-P, 6 MISSED, 0 N/A.**
(Verdicts are the *post-closure* re-run of the whole battery; the first pass ended
48+5 caught / 13 MISSED, and the seven rows closed in the gap-closure pass are the
ones whose `Expected` cell reads *(pre-registered miss)* yet whose verdict is CAUGHT-T.)


## Coverage argument

### What the 59 rows cover

Every **guard conjunct** in the model has its own row (12 rows), and each row deletes or inverts
exactly that conjunct:

| Guard | Conjuncts | Rows |
|---|---|---|
| `configuredIdFor` (pin branch) | pin off ⇒ `None`; pinned-and-running; pinned-and-never-booted | A01, A02, A03 |
| `mintsFreshId` | `cfg == None` | A05, A06 |
| `canBoot` | `intent == Undecided` | A07 |
| `canRestart` | `intent != Undecided` | A22 |
| `canMeet` | inviter `member_of != None`; joiner `effective_id != None`; joiner `raft == Empty` | A32, A33, A34 |
| `canRepurpose` | `member_of != None`; `raft != Empty` | A39, A40 |
| `meet` wiring | `n != m` | A50 |

Every **effect field-update** has its own row (44 rows): `idForBoot` (A04); `applyBoot`'s
`identified` prefix (A08–A10) and both branches field-by-field — bootstrap `intent`/`raft`/
`routable_leader`/`member_of`/`decision_epoch`/`raft_origin` (A11–A16), join the same six
(A17–A21); `applyRestart`'s nine writes and non-writes (A23–A31); `applyMeetJoiner`'s two writes
plus the two fields it must *not* write (A35–A38); `applyRepurpose`'s three writes plus the two
fields it must *not* write (A41–A45); the machine's mint-source wiring (A46, A47), meet's
write target (A51), the box-id source of a pin (A56); `init`'s four cells (A52–A55); and the
`FRESH_ID_BASE` constant the distinctness argument rests on (A59). The remaining three rows are
the two defect-ghost recomputes (A48, A49) and `step`'s action wiring (A57, A58).

### Every invariant is falsifiable, and by more than its own clause

An invariant no mutation can break is decoration. Each of the nine is broken by at least one row:

| Invariant | Rows that violate it |
|---|---|
| `inv_no_usurper` | A16, A18, A23, A28, A37, A43 |
| `inv_single_routable_group` | A04, A05, A06, A11, A13, A18, A28, A30, A34, A37, A43, A46, A51, A52, A53, A56, A59 |
| `inv_restart_deterministic` (3 conjuncts) | A07, A11, A18, A23, A28, A30, A31, A43, A51, A54 |
| `inv_intent_write_once` | A23 |
| `inv_identity_survives_id_churn` | A24 |
| `inv_configured_id_stable` | A04, A08, A26 |
| `inv_effective_ids_distinct` | A04, A05, A06, A46, A47, A52, A53, A56, A59 |
| `inv_meet_no_absorption` | A07, A11, A21, A24, A30, A34, A38, A44 |
| `inv_member_carries_raft_state` (added by this pass) | A11, A19, A24, A30, A35 |

The two ghost-backed invariants have exactly one falsifier each **by construction**: they assert a
detector never trips, and only the corresponding defect (A23 rewriting `intent`, A24 re-minting the
identity) trips it. Those are also the *only* invariant catchers for their rows — remove either
invariant and the property oracle goes silent on the defect it exists for.

`inv_restart_deterministic` is checked clause-wise, not just as a whole: c1 (Raft state survives) by
A11/A30, c2 (routability unchanged) by A18/A28/A43, c3 (no epoch bump) by A31/A54.

### Every test kills something, and every witness is forced

All 12 `run` tests appear as the catcher of at least one row — the thinnest are
`restartRefusedBeforeFirstBootTest` (A22, A53, A55) and `meetRefusesUnbootedNodeTest` (A33, A53,
A55), each of which is the *sole* catcher of its refusal row. No test is dead weight.

All six witnesses are asserted inside a deterministic `run` test
(`witnessBootstrapped`/`witnessJoinedViaMeet` in `bootstrapThenMeetTest`,
`witnessRepurposedHazard` in `meetRefusesRepurposedNodeTest`,
`witnessFreshIdAfterRestart`/`witnessStrangerIdentity` in `restartMintsFreshIdTest`,
`witnessConfiguredIdPinned` in `configuredIdStableAcrossRestartTest`), so a witness that stops
being reachable *from a specific forcing trace* fails `quint test` rather than silently reporting
0%. That is not the same as detecting step-unwiring (see A57/A58 below), which breaks reachability
from `step` only.

### Prediction vs. observation

`Expected` was written before any run. Every row's *oracle class* matched or beat the prediction
except one: **A59** (`FRESH_ID_BASE = 101 → 2`) was predicted to fail `restartMintsFreshIdTest`,
but that test is written in terms of the constant rather than the literal, so it stayed green and
only the property oracle fired (`inv_effective_ids_distinct`, `inv_single_routable_group`) —
CAUGHT-P, not CAUGHT-T. Recorded rather than retro-fitted: a test that hard-codes `101` would kill
the row but would also be a worse test.

## Rows still MISSED — per-row honest-miss analysis

Six rows survive. All six were escalated to **4000 samples × 40 steps across seeds
`0x1`/`0xbeef`/`0xfeed`** before the verdict was recorded — none produced a violation, and the two
step-unwiring rows additionally reported their witness at 0 traces at that budget. Each is one of
two honest kinds: an **equivalent mutant** (the edit provably does not change the model's behaviour)
or a **structural blind spot** of the oracles this model has (documented at the code, not papered
over).

#### A40 — `canRepurpose`'s `ns.raft != Empty` conjunct — *equivalent mutant*

Dropping it admits `repurpose` on a node that claims membership but holds no Raft state. No such
state is reachable: every transition that sets `member_of` to `Some(_)` also sets `raft` to a
non-`Empty` value (`applyBoot`'s bootstrap branch writes `Initialized(1)`; `applyMeetJoiner` copies
the inviter's non-`Empty` state under `canMeet`), and no transition clears `raft` while leaving
`member_of` set. The mutant therefore has exactly the same transition relation as the original, and
*no* oracle — invariant, witness or test — can distinguish them.

Rather than assert that in prose, the closure pass turned it into a checked fact: the new
`inv_member_carries_raft_state` (`member_of != None implies raft != Empty`) is exactly the coupling
the equivalence argument uses, it is clean on the unmutated model at 4000×40 across three seeds, and
it is *falsifiable* — rows A11, A19, A24, A30 and A35 all break it. So the argument is now a
tripwire: the day someone adds a transition that assigns membership without Raft state, that
invariant fails, and A40 becomes a killable row instead of an equivalent mutant. The conjunct stays
in the code because the implementation's guard reads both cells and must not depend on a coupling
that is a property of the *modeled* transition set.

#### A50 — `meet`'s `n != m` conjunct — *equivalent mutant, same coupling*

Dropping it admits a node MEETing itself. `canMeet(nNode, nNode)` requires
`nNode.member_of != None` **and** `nNode.raft == Empty` of one and the same record — the exact
negation of `inv_member_carries_raft_state`. The conjunction is unsatisfiable in every reachable
state, so self-MEET is disabled whether or not `n != m` is written, and the mutant is equivalent.
Covered by the same tripwire as A40. The conjunct stays: in the implementation `CLUSTER MEET` takes
an address, self-meet is a distinct real refusal, and it must not silently rely on the coupling.

#### A48 / A49 — blanking a defect-ghost recompute — *detector self-mutation*

`defects.restartIntentWrite` and `defects.restartIdentityRemint` are postcondition-derived
detectors: each is recomputed inside `restartAs` from that action's own outcome, and in the ruled
model **neither ever trips**. Blanking one (A48: `restartIntentWrite: false`; A49: dropping the
`raft_origin` disjunct) therefore removes nothing observable — a detector that never fires and a
detector that cannot fire have identical traces. Their value is measured in the *mutated* model, and
the battery measures it: A23 (config-driven `intent` rewrite) is caught by `inv_intent_write_once`
and A24 (identity re-mint) by `inv_identity_survives_id_churn`, in both cases as the invariant
catchers no other property duplicates. Killing A48/A49 needs a **compound** mutation — detector plus
the defect it detects — which is outside the one-edit discipline this battery is defined by.
Recorded MISSED with this analysis at the code (`cluster_admission_machine.qnt`, above the `defects`
declaration) rather than skipped.

#### A57 / A58 — unwiring an action from `step` — *structural blind spot (witness-only observable)*

A57 removes `repurpose(n)` from `step`, A58 removes `meet(n, m)`. Both oracles stay green **by
construction**: the `run` tests drive actions directly, so they never consult `step`; and every
`inv_*` is a safety predicate over reachable states, which dropping a verb can only *shrink* —
no safety property is falsifiable by removing behaviour. The observable is reachability, and it is
unambiguous: at 4000×40 the corresponding witness drops to **0 traces**
(`witnessRepurposedHazard` for A57, `witnessJoinedViaMeet` for A58, against 78.8% and 22.2% on the
unmutated model).

That lane is run by hand: `just quint-run` checks invariants only, so nothing in CI would notice.
This is recorded as a **documented detection hole, not a closed one** — closing it needs either
witness-count gating in the runner or a temporal operator the simulator does not offer. Same
structural limit the migration model recorded as row M22. Noted at the code above `action step`, and
carried to the campaign ledger as the shared follow-up (gate witness reachability in
`just quint-run`), which is a runner change, not a model change.

## Gap closure

The first pass left **13 MISSED**. Seven were real gaps and were closed; six are the analyses
above. Nothing was weakened to make a row pass, and no row was quarantined.

### New `run` tests (4) — refusal traces the model never asserted

| Test | Kills | What it asserts |
|---|---|---|
| `restartRefusedBeforeFirstBootTest` | A22 | `restartAs(4, true, false).fail()` on a never-booted record — `canRestart`'s `intent != Undecided` was previously unforced, so weakening it to `true` was invisible. |
| `meetRefusedFromNonMemberTest` | A32 | two `Join` boots, then `meet(1, 2).fail()` — MEET issued by a node that is itself in no cluster. This is `canMeet`'s inviter conjunct, the amended TR-CLUSTER-005 join-safety gate; it had no forcing trace. |
| `repurposeRefusedOnUnclaimedNodeTest` | A39 | bootstrap → `repurpose` → `repurpose(3).fail()` — a second reset against an already-unclaimed node. |
| `pinAdoptsRunningIdTest` | A02 | `bootAs(1, true, false)` then `restartAs(1, true, true)` expecting `configured_id == Some(FRESH_ID_BASE)` — pinning adopts the id the process is *currently running under*, not the box/record key. This was the subtlest gap: the mutant re-derived the pin from the record key, which is right for a never-booted node and wrong for a running one. |

### Strengthened expects on existing tests (4)

| Test | Added expect | Kills |
|---|---|---|
| `issue25UsurpationUnreachableTest` | `n2.decision_epoch == 1` on the first expect | A20 (join branch's epoch bump was unpinned) |
| `issue25UsurpationUnreachableTest` | `n2.configured_bootstrap`, `not(n2.routable_leader)`, `n2.raft == Empty` on the second expect | A29 (restart must record the *current* config, not keep the old one) |
| `adminResetMayRewriteIntentTest` | `not(n3.configured_bootstrap)` | A29 (second angle: the reset path) |
| `meetRefusesRepurposedNodeTest` | `n3.raft_origin == n3.effective_id` | A45 (the reset must leave the residue's provenance intact — erasing it is what makes the TR-CLUSTER-035 hazard invisible) |
| `bootstrapThenMeetTest` | `witnessBootstrapped`, `witnessJoinedViaMeet` | no row; closes the "witness with no forcing trace" hole so all six witnesses are test-forced |

### New invariant (1)

```quint
val inv_member_carries_raft_state: bool =
  NODES.forall(n => {
    val ns = nodes.get(n)
    ns.member_of != None implies ns.raft != Empty
  })
```

Added not to catch a row that was missed, but to make the **equivalence arguments for A40 and A50
falsifiable** — it is precisely the coupling those arguments rely on. It earns its place
independently: it is the sole invariant catcher on A19 (join branch wrongly claiming membership)
and also fires on A11, A24, A30 and A35. Clean on the unmutated model at 4000×40 across three
seeds, so the counterexample protocol was not triggered and no `t5-blocked.md`/`t4-blocked.md`
finding was raised.

### Code comments (2)

Two honest-miss analyses live **at the code**, not only in this report, so the next reader of the
model sees why those spots are unmutable: above the `defects` record in
`cluster_admission_machine.qnt` (A48/A49) and above `action step` (A57/A58). Both cite this file by
row id.

### Surface change

| | Base (`9c21b30c`) | After closure |
|---|---|---|
| invariants | 8 | **9** |
| witnesses | 6 | 6 (all now test-forced) |
| `run` tests | 8 | **12** |
| MISSED rows | 13 (first pass) | **6**, each with an analysis above |

## Final gate results

On the final (post-closure) state of the four model files:

```
$ quint test specs/quint/cluster_admission.qnt
  12 passing (154ms)

$ for s in 0x1 0xbeef 0xfeed; do
    quint run specs/quint/cluster_admission.qnt --max-samples=4000 --max-steps=40 --seed=$s \
      --invariants inv_no_usurper inv_single_routable_group inv_restart_deterministic \
      inv_intent_write_once inv_identity_survives_id_churn inv_configured_id_stable \
      inv_effective_ids_distinct inv_meet_no_absorption inv_member_carries_raft_state
  done
  [ok] No violation found (3328ms at 1202 traces/second).   # seed 0x1
  [ok] No violation found (3390ms at 1180 traces/second).   # seed 0xbeef
  [ok] No violation found (3329ms at 1202 traces/second).   # seed 0xfeed
```

Witness reachability at 500×20, seed `0x1` (the budget the battery ran at):

| Witness | Traces |
|---|---|
| `witnessBootstrapped` | 466/500 (93.2%) |
| `witnessJoinedViaMeet` | 111/500 (22.2%) |
| `witnessRepurposedHazard` | 394/500 (78.8%) |
| `witnessFreshIdAfterRestart` | 418/500 (83.6%) |
| `witnessConfiguredIdPinned` | 461/500 (92.2%) |
| `witnessStrangerIdentity` | 319/500 (63.8%) |

Repo gates:

- `just quint-check` — exit 0 (typecheck + tests over every model).
- `just lint-spec` — exit 0: `OK: 304 failure modes …, 241 quint citations over 20 models, 90
  invariant citations over 27 catalog entries`. (One pre-existing warning about FM-CLUSTER-104,
  unrelated to this model and present before this task.)
- Footprint: `git --no-optional-locks diff --stat -- specs/quint/cluster_admission*` shows only the
  intentional gap-closure additions —
  `cluster_admission.qnt | 138 +++`, `cluster_admission_machine.qnt | 21 +++`
  (155 insertions, 4 deletions). `cluster_admission_logic.qnt` and `cluster_admission_types.qnt` are
  byte-identical to `9c21b30c`; `git checkout -- specs/quint/` was never run, and no file outside
  `cluster_admission*` was written or reverted (other agents' untracked `replication_*.qnt` WIP in
  that directory was left alone).

## Follow-up carried to the campaign ledger

`just quint-run` checks invariants only, so the witness lane — the only oracle that sees an action
unwired from `step` (rows A57/A58) — is a manual step today. Gating witness counts in the runner
would close that hole for **every** model at once; it is a runner change, tracked with the campaign
rather than with this model.
