# TLC run evidence

TLC actually ran on every committed instance; these are the recorded results.

- Date: 2026-08-13
- Machine: macOS (Darwin 26.6, aarch64), 18 TLC workers
- Java: Temurin OpenJDK 21.0.12
- TLA+ tools: TLC2 version 2.19 of 08 August 2024 (tla2tools.jar, release line >= 1.8.0)
- Invocation shape: `java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto -config <cfg> <spec>`
- Both specifications also parse standalone with SANY (`tla2sany.SANY`).

| Instance                                                                       | Expected | Result                                                       | States generated | Distinct states | Depth |     Time |
| ------------------------------------------------------------------------------ | -------- | ------------------------------------------------------------ | ---------------: | --------------: | ----: | -------: |
| `DurableSubmission.cfg` (safety: 8 invariants, 2 workers)                      | pass     | **pass — no error found**                                    |      144,036,108 |      37,736,499 |    72 | 1min 11s |
| `DurableSubmissionLiveness.cfg` (8 invariants + `EventuallySettled`, 1 worker) | pass     | **pass — no error found**                                    |        4,541,406 |       1,333,275 |    64 |      52s |
| `DurableSubmissionNoFencing.cfg` (negative control: fencing disabled)          | fail     | **failed as expected — `FencingSafety` violated**            |            2,611 |           1,161 |    12 |      <1s |
| `SubagentEstablishment.cfg` (6 invariants + 2 liveness, current discipline)    | pass     | **pass — no error found**                                    |           43,136 |          14,238 |    44 |      <1s |
| `SubagentEstablishmentRace.cfg` (negative control: plan §7(a) race)            | fail     | **failed as expected — `ChildTurnRequiresLineage` violated** |            3,130 |           1,231 |    24 |      <1s |
| `SubagentEstablishmentFix.cfg` (`AwaitParentEstablishment` discipline)         | pass     | **pass — no error found**                                    |           25,166 |           8,434 |    43 |      <1s |

## The §7(a) race counterexample (SubagentEstablishmentRace.cfg)

TLC's minimal 17-state trace is exactly the interleaving plan §7(a) predicts:

1. parent claims, responds, reserves the child budget, appends
   `SubagentRequested`, admits the child — then stalls before
   `ensureChildLineage`;
2. the CHILD lane's own recovery classifies its `admitted` Submission through
   the ordinary rows (`CompleteMaterialization`, `RepairReadiness`) without
   consulting lineage (recovery.ts rows 11);
3. the child worker claims the now-ready child, applies its input, and runs a
   model Turn while no `SubagentLineageRecorded` record exists
   (`ranBeforeLineage` witness set at `CTurn`).

Under `AwaitParentEstablishment = TRUE` (`SubagentEstablishmentFix.cfg`) the
same instance passes with `ChildTurnRequiresLineage` checked, and both
liveness properties still hold — the fix removes the interleaving without
starving the child. WP7 implements the corresponding classifier decision.

## Model-development findings worth keeping

Counterexamples TLC produced against earlier drafts of the model, each
resolved by making the model MORE faithful to the implementation (the
implementation already handled them):

1. `FIFOPerLane` originally demanded canonical input for every earlier
   Submission; durability §13 legitimately settles accepted-but-inactive
   work aborted with no input — the invariant now carries that exemption.
2. An early `markJoined` model allowed a lease-expired-but-alive host to mark
   a recovery-reverted row joined, corrupting the host linkage; the SQLite
   adapter's strict `joining → joined` + linkage check rejects exactly that,
   and the committed model mirrors it.
3. A superseded Attempt can re-mark a just-resolved lane `unknown` through
   the ownership-free `ledger.markUnknown` (canonical audit dedupes as an
   identity conflict). Benign and self-healing — classifier row 7 wakes the
   lane — so the `UnknownBlocksContinuation` invariant asserts the ghost
   no-continuation counter rather than open-call presence. Recorded in
   CORRESPONDENCE.md §1.3 as a candidate hardening note.

## Reproduction

```sh
bun run formal:check          # runs all six instances and asserts each verdict
```

or the exact `java` invocations in [README.md](./README.md). The scheduled
GitHub Actions workflow (`.github/workflows/formal.yml`) runs the same set
weekly and on demand; it is intentionally NOT part of `bun run ready` or the
PR gate.
