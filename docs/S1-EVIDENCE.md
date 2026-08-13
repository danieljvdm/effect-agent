# S1 attached ephemeral Subagent evidence

Status: **Implemented** (as the roadmap-assigned proposed default; ADR-0010 remains **Proposed**)

S1 lets a parent Agent delegate bounded work to a declared Subagent through an ordinary Effect AI
Tool: one fresh child Conversation per invocation, an explicit child Binding on the same
interpreter, Schema-backed input/result projections, a fail-closed authority ceiling, an in-memory
hierarchical budget reservation, and stable lifecycle events on the parent stream. The
deployment-class label for this slice is `E`, never `P` child history or `DN`: process loss ends
the work, and there is no durable accepted-child claim.

## Delivered package surface

- `@effect-agent/core` owns the branded `DelegationId` (`packages/core/src/identifiers.ts`), the
  `DelegationDepth` and `SubagentParentLink` lineage Schemas (`packages/core/src/subagent.ts`),
  and seven versioned Subagent lifecycle members of the public `RunEvent` union
  (`packages/core/src/events.ts`): `SubagentRequested`, `SubagentStarted`, `SubagentProgress`,
  `SubagentCompleted`, `SubagentFailed`, `SubagentInterrupted`, and `SubagentJoined`, each carrying
  parent identity, the delegation Tool Call, child Conversation/Run identity, target Agent, and
  depth.
- `@effect-agent/engine` owns the execution-options seam for preallocated child Run identity and a
  non-model-visible Parent Link (`packages/engine/src/run-options.ts`), the batch-scoped
  `RunEventSink` through which a delegation handler surfaces Subagent events onto the parent
  stream with an engine-stamped, unforgeable base (`packages/engine/src/run-events.ts`, woven in
  `packages/engine/src/index.ts`), and the engine-provided `AgentSpawner` service that runs an
  Attached Child through the same interpreter inside the caller-provided Scope
  (`packages/engine/src/index.ts`). Both services are provided locally per Run/Tool batch and are
  excluded from the runtime's public requirements (`EngineProvidedToolServices`).
- `@effect-agent/capabilities` owns the pure `Subagent.define` helper, the `SubagentPolicy` and
  `SubagentGrant` ceiling, the delegation Tool-failure family (`SubagentPrestartDenied`,
  `SubagentBudgetExhausted`, `SubagentProjectionFailure`), and `SubagentRuntime.layer`
  (`packages/capabilities/src/subagent.ts`), plus the in-memory hierarchical reservation service
  `SubagentReservations` with its `SubagentReservationsMemoryLive` Layer
  (`packages/capabilities/src/subagent-reservation.ts`).

## Executable exit-gate evidence

| S1 deliverable (spec §17)                                      | Deterministic evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pure `Subagent.define`, explicit `AgentSpawner` dependency     | `packages/capabilities/test/subagent.test.ts`: the `Subagent.define` cases ("rejects delegation names outside the naming convention", "marks delegation Tools recognizably for preflight", "derives caps and allocation from the delegation policy")                                                                                                                                                                                                                                                                                                                                                            |
| `SubagentRuntime.layer` with an explicit child Binding         | `SubagentRuntime.layer(delegation, childBinding, options)` requires a `RuntimeBinding`; an unbound Definition is a compile error; type proof "keeps per-call and construction requirements distinct" and engine test "keeps engine-provided Tool services out of the public requirements"                                                                                                                                                                                                                                                                                                                       |
| Engine options for preallocated child identity and Parent Link | `runId`/`parentLink` in `packages/engine/src/run-options.ts`; `packages/engine/test/subagent-seam.test.ts` "honors preallocated Conversation and Run identity in every emitted event"                                                                                                                                                                                                                                                                                                                                                                                                                           |
| Fresh child Conversation per invocation                        | `AgentSpawner.spawn` allocates child `ConversationId`/`RunId` through `IdGenerator` and builds the Parent Link at `depth + 1`; engine test "spawns a scripted child with preallocated identity, Parent Link, and observable events"                                                                                                                                                                                                                                                                                                                                                                             |
| Context and result projection                                  | `prepareInput`/`projectResult` cross the target input and delegation success Schemas; escapes fail closed as `SubagentProjectionFailure` ("joins one attached child through explicit projections", "fails closed when the projected result escapes the success Schema")                                                                                                                                                                                                                                                                                                                                         |
| Fail-closed authority ceiling, non-transitive approvals        | `SubagentGrant` fixes `maxDepth` to literal `1` and the allowed child Tool roster; preflight denies outside the ceiling before any reservation ("denies a child Tool outside the grant ceiling before any budget exists"); `needsApproval` authorizes establishment only (SUB-026)                                                                                                                                                                                                                                                                                                                              |
| In-memory hierarchical reservation with conservation tests     | `packages/capabilities/test/subagent-reservation.test.ts`: "settles through releasePending and returns unused allocation exactly once", "parallel reserve calls never oversubscribe the parent budget", "records covered consumption then overrun without clipping and blocks new work"                                                                                                                                                                                                                                                                                                                         |
| Depth `1` and normative nested-delegation rejection (SUB-029)  | Define-time throw plus handler preflight: "rejects nested delegation at preflight before any reservation (SUB-029)", "rejects a target whose Toolkit already contains a delegation Tool"                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Scoped Fiber ownership and bounded parallel children           | Handler-owned `Effect.scoped` region, `Effect.forkScoped` child, Scope-owned concurrency slots: "interrupts the spawned child and runs its finalizers on parent interruption", "bounds concurrent children through the Scope-owned slot gate", "bounds concurrent children and frees a queued slot on interruption"                                                                                                                                                                                                                                                                                             |
| Stable live events and child observation                       | Core round-trip tests ("round-trips every Subagent lifecycle event through the public union", "rejects malformed Subagent lifecycle payloads"); engine "weaves sink-emitted Subagent events into the parent stream with a stamped base", "fails closed when a handler emits after its Tool batch settled"; `SpawnedChildRun.observe` exposes the child stream                                                                                                                                                                                                                                                   |
| Total child-failure mapping; interruption/defects distinct     | Required `mapChildFailure` is compile-enforced over the child Binding's full expected failure union (SUB-028): "total-maps expected child failures to the declared Tool failure", "keeps child defects defects and still settles the reservation", "settles the reservation when parent interruption reaches the child"                                                                                                                                                                                                                                                                                         |
| Budget denial, duration, and result-size bounds                | "denies the invocation beyond maxChildren and settles accounting", "exhausts the delegation duration budget deterministically", "fails closed on a zero concurrent-children cap override"                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Deterministic scripted specialist delegation                   | The Travel Planner reference application gains a `travel-coordinator` parent that delegates through `delegate_destination_research` to a scripted `destination-researcher` child (`packages/testing/test/travel-planner-subagents.test.ts`, 7 tests: declaration-order commits under reverse completion, lifecycle correlation, failure mapping with sibling interruption, parent interruption finalizers, nested-delegation rejection, reservation conservation, child prompt isolation); a second coordinator→research fixture exercises the full parent Run in `packages/capabilities/test/subagent.test.ts` |

All suites are deterministic: scripted `LanguageModel` Layers, `Deferred` choreography, and
Exit-based failure assertions; no wall-clock sleeps.

## Allowed claim

`E` Subagents only. An attached ephemeral child is a supervised Fiber owned by the parent Tool
batch Scope; process loss ends the work. This slice does not claim durable accepted children, and
it does not wire child Conversations into a Conversation Store, so the optional "persisted child
history under `P`" claim from spec §17 is also not made.

## Governance status

[ADR-0010](adr/0010-declared-attached-subagents.md) is formally unresolved: the owner has not
accepted it. Per `AGENTS.md`, implementation proceeded against the proposed default because the
roadmap assigned the S1 slice before owner resolution and the delegation surface is locally
reversible. Acceptance, rejection, or amendment of ADR-0010 remains an open owner decision.

## Non-claims

- No S2 machinery exists: no durable child records, no ledger admission, no Receipt, no
  `waitingForChild` suspension, no cross-process recovery, no durable abort propagation.
- No detachment, no child Conversation reuse, no nested delegation: every nested delegation is
  rejected at preflight, and the grant depth ceiling is the literal `1`.
- Live Subagent events are stream events, not canonical records; `SubagentJoined` carries no
  result digest because requested/started/joined durable records are S2 scope (spec §11).
- No Principal/Tenant authorization service exists yet; the S1 ceiling is enforced structurally at
  preflight (grant Tool roster and depth), and per-action Principal/Tenant reauthorization ships
  with the rest of spec §6 in later slices.
- Token and cost dimensions are enforced only as honestly observed usage from provider reporting:
  overruns are recorded and block new work, never clipped (spec §7).
- `SubagentProgress` forwarding from child Turn completion is not emitted by the delegation
  handler yet; the event schema and sink path exist and are tested, but the handler skips
  progress forwarding for S1 determinism.
