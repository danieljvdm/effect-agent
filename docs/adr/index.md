---
title: Architecture decisions
description: The accepted and proposed decisions that constrain Effect Agent's public interfaces.
---

# Architecture decisions

Decision records explain why the interfaces in these docs have their current shape. Accepted ADRs
are authoritative; a Proposed ADR is design material, not an owner-approved feature promise.

| ADR                                                       | Status                                                                                                 | Decision                                                                              |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| [0001](./0001-effect-native-core)                         | Accepted                                                                                               | Build an Effect-native core                                                           |
| [0002](./0002-use-effect-ai-primitives)                   | Accepted                                                                                               | Use Effect AI primitives directly                                                     |
| [0003](./0003-canonical-log-and-ledger)                   | Accepted                                                                                               | Separate canonical history from operational obligations                               |
| [0004](./0004-uncertain-external-effects)                 | Accepted                                                                                               | Represent uncertainty instead of blindly replaying effects                            |
| [0005](./0005-bounded-parallel-tool-scheduling)           | Accepted                                                                                               | Bound parallel Tools and commit in declaration order                                  |
| [0006](./0006-package-only-vite-plus-monorepo)            | Accepted / partially superseded                                                                        | Use a phase-gated Vite+ monorepo                                                      |
| [0007](./0007-slim-toolchain-and-canonical-effect-source) | Accepted                                                                                               | Keep a slim toolchain and canonical Effect source                                     |
| [0008](./0008-turn-boundary-input-delivery)               | Accepted                                                                                               | Deliver new input only at safe Turn seams                                             |
| [0009](./0009-leaf-example-workspaces)                    | Accepted                                                                                               | Keep runnable benches as leaf examples                                                |
| [0010](./0010-declared-attached-subagents)                | **Proposed** — S1 and S2 implemented as proposed defaults ([S1](../S1-EVIDENCE), [S2](../S2-EVIDENCE)) | Model attached Subagents as declared delegation Tools                                 |
| [0011](./0011-durable-runtime-placement-and-leases)       | Accepted by default — pending owner review ([evidence](../PHASE-4-EVIDENCE))                           | Place the durable coordinator in session; leases for liveness, epochs for correctness |
| [0012](./0012-durable-tool-uncertainty-and-steps)         | Accepted by default — pending owner review ([evidence](../PHASE-5-EVIDENCE))                           | Annotation-scoped Tool uncertainty, log-native Steps, and record-free suspension      |
| [0013](./0013-durable-subagent-establishment)             | Accepted by default — pending owner review ([evidence](../S2-EVIDENCE))                                | Durable Subagent establishment, waiting suspension, and exact binding resolution      |
| [0014](./0014-cloudflare-conversation-objects)            | Accepted by default — pending owner review ([evidence](../PHASE-6-EVIDENCE))                           | Cloudflare Conversation Objects: serialized owner, multiplexed alarm, routed ports    |
| [0015](./0015-hardening-shape)                            | Accepted by default — pending owner review ([evidence](../PHASE-7-EVIDENCE))                           | Phase 7 hardening shape: certification tiers, formal-model scope, admin authorization |

The [decision register](../DECISIONS) is the owner-facing status source. ADRs preserve rationale,
consequences, rejected alternatives, and validation criteria.
