# Architecture decision records

ADRs explain durable architectural choices, their context, and their tradeoffs.
The records below reflect explicit owner decisions.

| ADR                                                            | Decision                                                            |
| -------------------------------------------------------------- | ------------------------------------------------------------------- |
| [ADR-0001](0001-effect-native-core.md)                         | Build an Effect-native core                                         |
| [ADR-0002](0002-use-effect-ai-primitives.md)                   | Use Effect AI primitives directly                                   |
| [ADR-0003](0003-canonical-log-and-ledger.md)                   | Separate the canonical Conversation Log from the Submission Ledger  |
| [ADR-0004](0004-uncertain-external-effects.md)                 | Represent uncertain external effects                                |
| [ADR-0005](0005-bounded-parallel-tool-scheduling.md)           | Use bounded parallel Tool execution with deterministic result order |
| [ADR-0006](0006-package-only-vite-plus-monorepo.md)            | Use a phase-gated, package-only Vite+ monorepo                      |
| [ADR-0007](0007-slim-toolchain-and-canonical-effect-source.md) | Slim the toolchain and use the canonical Effect source              |
| [ADR-0008](0008-turn-boundary-input-delivery.md)               | Deliver steering, follow-up, and joined input only at safe seams    |
| [ADR-0009](0009-leaf-example-workspaces.md)                    | Keep runnable consumer benches in leaf example workspaces           |

## Accepted by default

| ADR                                                      | Decision                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| -------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [ADR-0011](0011-durable-runtime-placement-and-leases.md) | Durable coordinator in session; leases as liveness, epochs as correctness; exact-match storage v2; Turn-granular canonical model records — adopted as the P4 plan default, pending owner review ([evidence](../PHASE-4-EVIDENCE.md))                                                                                                                                                                                                                                                   |
| [ADR-0012](0012-durable-tool-uncertainty-and-steps.md)   | Execution-class-scoped prepared/settled Tool records with a fail-closed `uncertain` default; engine-owned Durable Steps recorded as settled-success-only canonical records; `step.do(name, OutputSchema, effect)`; first-class `ModelResponseInterrupted`; no canonical suspension record — adopted as the P5 plan default, pending owner review ([evidence](../PHASE-5-EVIDENCE.md))                                                                                                  |
| [ADR-0013](0013-durable-subagent-establishment.md)       | Engine-owned durable delegation seam with an ephemeral default; `waitingForChild` as an additive `SuspensionReason` plus the cross-lane `recordChildSettled` wake; idempotent handler re-entry with binding-free repair executors; tri-state `resolveAdmission`; exact-digest `AgentBindingResolver` fail-closed; reservations as generic ledger rows; bounded durable child-failure projection — adopted as the S2 plan default, pending owner review ([evidence](../S2-EVIDENCE.md)) |

## Proposed

| ADR                                             | Recommendation                                                                                                                                                          |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [ADR-0010](0010-declared-attached-subagents.md) | Model Subagents as declared attached delegation Tools — S1 and S2 implemented as proposed defaults ([S1 evidence](../S1-EVIDENCE.md), [S2 evidence](../S2-EVIDENCE.md)) |

## Status transitions

- `Proposed` → `Accepted` after explicit owner approval.
- `Proposed` → `Rejected` when another alternative is selected.
- `Accepted` → `Superseded by ADR-NNNN` when the architecture changes.

Do not rewrite an accepted ADR to make a later decision appear original. Add a new
record and preserve the history.
