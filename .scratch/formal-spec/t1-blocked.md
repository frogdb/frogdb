# T1 blocked item — arm 4b of `isRefusalTerminal` is NOT dead in the design doc

> **RULED 2026-08-20 (R2, campaign ledger): option 1 — model the narrowing.**
> `identityOrderOk` becomes kind-sensitive (false for `kind = Demotion` against `None`),
> arm 4b goes live in the model with a forcing run test, the doc stays as written, and the
> 2026-08-19 "delete arm 4b" ruling is withdrawn.

Date: 2026-08-19. Raised by the W1 task that applies the
[2026-08-19 campaign rulings](2026-08-19-quint-completeness-campaign.md) to the cluster
migration/failover Quint model. Five of the six ruled changes landed; this one stopped under
the counterexample protocol ("if it reveals the design doc or a ruling is inconsistent,
STOP").

## The ruling

> **Dead arm 4b of `isRefusalTerminal` (`Ordering ∧ stored == None`)** — **Delete from the
> design doc** — dead by construction: an Ordering refusal presupposes a stored cell;
> Fenced/Membership own the empty-cell refusal cases. Comment at `refusalIsTerminal` cites
> this ruling. If the spec amendment surfaces an impl path emitting Ordering against an
> empty cell, that is a new ruling with a forcing test.

## Why it cannot be applied as written

The premise — *an `Ordering` refusal presupposes a stored cell* — is **true of the Quint
model** and **false of the design doc**. The two disagree because the model never
implemented **V18-M1's narrowed absent-operand rule**.

**Model** (`specs/quint/cluster_migration_failover_logic.qnt`):

```quint
pure def identityOrderOk(stored: Option[RunIdentity], inc: int, seq: int): bool =
  match stored {
    | Some(st) => inc > st.inc or (inc == st.inc and seq > st.seq)
    | None => true          // <-- vacuously true, and kind-insensitive
  }
```

`refusalClassOf` returns `Ordering` only when `identityOrderOk` is false, so in the model
`Ordering ∧ stored == None` is unreachable. That is the whole of the "dead by construction"
observation.

**Design doc** (`.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md`),
three independent normative statements that the same conjunct is **false** for
`kind = Demotion` against an absent cell — i.e. arm 4b is *reachable*:

- ~L5651 (the cross-registration walk): "`ordering` **fails**, because V18-M1's narrowed
  absent-operand rule makes the ordering conjunct false for `kind = Demotion` against an
  absent stored `run_identity`. **Class `ordering`** … The `ordering` arm splits on the
  stored operand, which is **absent**, so the arm is **4b** — the **terminal clearing**
  arm."
- ~L6354 (the fence-freshness row, leg (a)): "**The re-created cell before the rejoined
  node's first report** — `run_identity` absent, and §0's rule (narrowed by V18-M1) makes
  the ordering conjunct **false for `kind = Demotion`**: refused, class `ordering`."
- ~L6434 (the refusal-class table): the `ordering` row's producer is literally "V18-M1's
  narrowing: a `ReportRunIdentity` with `kind = Demotion` refused against an **absent**
  stored `run_identity` cell", and 4b's disposition is "clear the record fsynced, drop the
  whole-node fence, answer the initiating client with an error naming the lost
  registration".

So the model's dead arm is a **model gap** (the narrowing is unmodelled), not evidence that
the doc's arm is dead.

## Why deleting it anyway would be destructive

Arm 4b is not an isolated line. ~40 passages depend on it, several of them the resolutions of
CRITICAL/MAJOR review findings:

- **V19-M1** created the 4a/4b split, and 4b *is* the exit that closes the wedge V19-M1
  traced ("without this exit the node stays fenced whole-node and mute forever").
- **V20-C1** (CRITICAL) is argued over which operand arm 4b binds.
- **V20-M1 / V21-M2** establish exit-list totality for the staged-flip fence row; 4b is a
  named exit there and arm 5's reply is defined as "the **same** lost-registration error arm
  4b gives".
- **V28-M5** derives the necessity of the record-binding conjunct's second component from
  4b's reply operand.

Deleting arm 4b would leave every one of those dangling and would remove a declared exit
from a fence whose exit list the doc makes a mechanically-checked totality claim about.

## What was done instead

- The model's `isRefusalTerminal` keeps both arms, with a comment at the definition citing
  the ruling, the V18-M1 conflict, and this file.
- **No edit was made to the design doc.**
- The other five ruled changes (M37 discard leg, V12-M1 effect-based closure, M32 ghost,
  issue 33 tombstones, anti-churn no-bump) landed as ruled.

## What the design owner needs to rule

Pick one:

1. **Model the narrowing** — make `identityOrderOk` kind-sensitive (false for
   `kind = Demotion` against `None`), which makes arm 4b live in the model too and turns
   `isRefusalTerminal`'s second disjunct into a checkable arm with a forcing run test. This
   keeps the doc as written and closes the gap in the model's favour.
2. **Re-derive the doc** — if V18-M1's narrowing is itself to be withdrawn, that is a
   separate amendment that must also re-derive V19-M1's wedge exit, V20-C1's operand
   argument, V21-M2's exit-list totality, and V28-M5's necessity argument. This is a
   doc-scale change, not a line deletion.

Option 1 is the smaller change and the one the model gap points at; it is *not* what the
2026-08-19 ledger ruled, so it needs a ruling before anyone builds it.
