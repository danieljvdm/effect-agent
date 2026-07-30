# Phase 2 operational local runtime evidence

Status: **Implemented**

Phase 2 adds scoped, bounded operational capabilities around the single Phase 1 interpreter. It
does not add persistence or claim durable accepted work.

## Delivered package surface

- `@effect-agent/capabilities` owns ephemeral Conversations, safe-seam command queues, audited
  approval, hierarchical usage/time budgets, ordered context transforms and compaction artifacts,
  scheduler overrides, structural redaction, and bounded native Effect AI MCP discovery.
- `@effect-agent/engine` exposes dependency-neutral hooks for those capabilities while retaining
  one `run`/`stream` state machine and authoritative Effect AI `Prompt` history.
- `@effect-agent/sandbox` defines bounded Schema-first process requests, events, failures, and the
  Sandbox service.
- `@effect-agent/sandbox-local` owns child processes in `Scope`, labels execution as unisolated,
  copies only allowlisted environment values, bounds aggregate output, and rejects unsupported
  isolation policy.

## Executable exit-gate evidence

| Phase 2 claim                                                              | Deterministic evidence                                                                                                                                |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Slow observers cannot deadlock completion                                  | `packages/engine/test/agent-runtime.test.ts` — “does not let a slow detached replay observer determine completion”                                    |
| Steering enters only after a completed response/Tool batch                 | engine test — “delivers steering offered during Tool execution only before the next model request”; Travel Planner Phase 2 suite                      |
| Follow-up input waits for the otherwise-stop seam                          | engine test — “buffers follow-up until the otherwise-stop seam and honors all-drain input”; Travel Planner Phase 2 suite                              |
| Approval settles before handler scheduling                                 | engine test — “never starts a handler while native Tool approval is unresolved”; audited approval tests; Travel Planner hold test                     |
| Tool concurrency stays finite and commits in declaration order             | engine scheduler override tests and the cumulative Travel Planner reverse-completion test                                                             |
| Official history survives model-context compaction                         | engine compaction-source test plus digest/range tests in `packages/capabilities`                                                                      |
| Token, cost, usage, and hierarchical deadlines fail through typed channels | engine policy/cost/deadline tests and atomic hierarchy tests in `packages/capabilities`                                                               |
| MCP discovery is bounded and correlated                                    | `packages/capabilities/test/capabilities.test.ts` MCP identity, schema, size, count, and test-clock timeout cases                                     |
| Local process resources finalize                                           | `packages/sandbox-local/test/local-sandbox.test.ts` success, failure, output, timeout, interruption, spawn, environment, and unsupported-policy cases |

The cumulative Travel Planner remains deployment class `E`. Its Phase 2 Agent adds the
`hold_itinerary` Tool with native Effect AI approval, while its scenarios exercise date-change
steering, missing-preference follow-up, budget rejection, source-preserving compaction, and denied
or unresolved holds without starting the mutation handler.

## Non-claims

- Conversation state and approval audit state are process-local and disposable.
- The local sandbox adapter is not an OS security boundary.
- MCP and ordinary Tool side effects are not exactly-once.
- Phase 2 does not acknowledge durable work, resume after process loss, or persist canonical
  Conversation records. Those concerns begin in later phases.
