# Runtime engine

Status: **Draft**  
Related decisions: D-004, D-006, D-007, D-008, D-011, D-012

## 1. One interpreter

The framework has one semantic Run interpreter. Convenience methods reduce the same event stream:

```ts
AgentRuntime.stream(agentBinding, input): Stream<RunEvent, RunFailure, R>
AgentRuntime.run(agentBinding, input): Effect<AgentResult, RunFailure, R | Scope>
```

`run` MUST NOT implement a separate loop. Golden tests compare its result to reducing `stream`.

## 2. Run lifecycle

Ephemeral lifecycle:

```text
created → preparing → running → completed | failed | interrupted | suspended
```

Durable lifecycle adds Submission and Attempt state described in the durability specification.

One Run owns:

- decoded input;
- Agent Binding, its Agent Definition, and version digest;
- Conversation projection;
- current Turn state;
- cumulative budgets and usage;
- owned Scope;
- semantic event sequence;
- steering/follow-up queues;
- child fibers and capabilities.

## 3. Turn state machine

Normative sequence:

1. Decode Run input.
2. Evaluate instructions and initial context.
3. Prepare Turn Plan.
4. Check budgets and abort/interruption.
5. Materialize the Effect AI Prompt and model request.
6. Stream Effect AI Response parts.
7. Reduce them into one complete assistant response.
8. Reject malformed or incomplete provider sequences.
9. If the assistant response is final, decode Agent output.
10. Otherwise validate the complete Tool Call batch.
11. Apply approvals and scheduling policy.
12. Execute Tool Handlers.
13. Encode outcomes and construct deterministic Tool result messages.
14. Advance the Conversation.
15. Drain steering commands.
16. Evaluate Stop Policy.
17. If otherwise complete, drain follow-ups.
18. Begin another Turn or emit one terminal Run event.

The transition reducer should be pure wherever possible. Effects interpret decisions; they do not
hide transition rules.

## 4. Effect AI response reduction

The reducer accepts Effect AI `Response.StreamPart` values. It validates:

- exactly one response start and completion;
- monotonic part identities;
- Tool Call ID uniqueness;
- complete Tool arguments before declaration;
- usage monotonicity/consistency;
- provider-declared stop reason compatibility;
- no content after terminal completion.

Malformed output remains an Effect AI error or becomes a framework protocol failure when it
violates an Agent-loop invariant. It is not silently repaired.

In ephemeral mode, text/reasoning deltas are live events. In durable mode, returned
text/reasoning deltas may be appended canonically, while partial Tool arguments remain live-only.
Only a completed response may execute Tools or enter the next Effect AI Prompt.

## 5. Tool Call processing

### Preflight

Before any Handler starts:

- the assistant message must be complete;
- stop reason must permit Tool execution;
- all Tool names must resolve;
- every parameter value must decode;
- budgets and authorization must pass;
- approval decisions must be known or represented as suspension.

A length-truncated response never executes Tool Calls.

### Scheduling

The default is bounded parallel execution owned by the engine.

- the complete batch passes preflight before any Handler starts;
- the engine requests unresolved Effect AI Tool Calls;
- Effect AI Toolkit handlers retain their native validation, success, failure, and requirements;
- the engine wraps Handler effects in Effect `Semaphore` permits and runs them as scoped child
  fibers;
- per-run concurrency is finite, and outer global, Agent, or Tool-group semaphores may impose
  stricter limits;
- a Run or Tool may require sequential execution;
- calls start in declaration order where scheduling permits;
- progress and completion may be observed in actual completion order;
- final Tool results commit in original declaration order;
- the next model request never sees a partial Tool batch.

### Completion

Every started call reaches exactly one in-memory terminal classification:

- success;
- typed failure in the Effect error channel;
- infrastructure failure;
- denied;
- interrupted.

The durable runtime adds unknown outcome.

### Provider-executed Tools

A provider-hosted built-in Tool does not run an application Handler. Its complete Effect AI call
and result parts retain `providerExecuted: true`; the runtime emits declaration and terminal
events with that provenance, but no `ToolCallStarted` event. These calls still consume Tool Call
and Turn budgets. When another Turn is required, provider results remain assistant content rather
than becoming application Tool output messages. Their authorization, recovery, and source-trust
limits belong to the explicit provider capability that enabled them.

A response containing only provider-executed calls may finish with `stop` and final text in the
same Turn, because no application Handler remains unresolved. A response containing any
application Tool Call still requires the Tool-compatible finish path and another Turn.

## 6. Steering and follow-ups

Steering and follow-up are typed Run commands delivered through Effect queues.

- Steering is drained at Run start and after a complete assistant/Tool Turn, before the next model
  request.
- Steering never cancels an in-flight response or skips its active Tool Calls.
- Follow-up is drained only when there are no pending Tool Calls or steering messages and the Run
  would otherwise complete.
- Queue drain policy is `one` or `all`, with `one` as the initial default.
- Durable input delivery uses claimed `joining` and `joined` Submissions at the same safe
  boundaries.

## 7. Interruption and abort

### Ephemeral interruption

Interrupting the consumer fiber closes the Run Scope:

- stop starting new work;
- interrupt the Effect AI Response stream;
- interrupt active Tool/Subagent fibers;
- run finalizers;
- emit no false success;
- preserve any already materialized result available to the caller.

### Durable abort

Durable abort is a persisted intent. Interrupting a local `await` or observation fiber only detaches
that caller. It does not abort accepted work. A separate runtime operation requests durable abort.

This distinction must be visible in naming and documentation.

## 8. Retry policy

Retries are classified and local:

- Model transient failure may retry before a canonical terminal response.
- Context overflow invokes compaction policy and at most the configured retry count.
- Tool handler failures do not automatically retry.
- Tool infrastructure retry is opt-in and must respect idempotency/durability annotations.
- Output encoding failure never retries as an external Tool call.
- Defects are not retried by default.

Effect's `retry` operator is an implementation mechanism, not the product policy. Retrying an
effectful Tool without durable evidence is forbidden.

## 9. Context and compaction

Context preparation:

1. Project canonical/ephemeral Conversation Messages.
2. Apply stable system instructions.
3. Apply ordered context transforms.
4. Calculate window/budget.
5. Compact if policy requires.
6. Produce normalized Model Input.

Compaction appends or emits a summary/branch representation and preserves the source history.
Summary generation is a separately metered Model purpose. Failed compaction leaves the original
history authoritative.

## 10. Event interface

Every semantic Run Event carries:

- event version;
- Run ID;
- Conversation ID;
- monotonic sequence;
- timestamp from Effect Clock;
- Agent ID;
- optional Turn ID;
- optional Tool Call ID;
- typed payload.

`ToolCallDeclared` carries the complete JSON parameters and whether execution belongs to the
provider. Tool progress and terminal events repeat that execution provenance so transports and
UIs do not infer an application Handler where none ran.

Terminal events are exactly one of:

- `RunCompleted`;
- `RunFailed`;
- `RunInterrupted`;
- `RunSuspended`.

Raw provider chunks are never mixed into the stable event union.

## 11. Backpressure

Local `stream` uses a bounded queue:

- default strategy: suspend producer at semantic event seams;
- text delta coalescing MAY reduce volume;
- semantic terminal events are never dropped;
- disconnecting/interrupting the sole ephemeral consumer interrupts the Run unless execution was
  explicitly detached.

Durable transports observe from the journal/projection and do not own execution liveness.

## 12. Runtime invariants

- **RUN-001:** `run` and `stream` share one interpreter.
- **RUN-002:** A Run settles its state exactly once, and any complete observed event trace contains
  at most one terminal event. An interrupted observer is not guaranteed to receive that event.
- **RUN-003:** A Tool Handler never sees undecoded parameters.
- **RUN-004:** Truncated Tool arguments never execute.
- **RUN-005:** The next Model Request sees a complete Tool Batch or none.
- **RUN-006:** Canonical/materialized Tool result order follows declaration order.
- **RUN-007:** Every owned child fiber belongs to the Run Scope.
- **RUN-008:** Interruption propagates to all attached children.
- **RUN-009:** Concurrency is bounded.
- **RUN-010:** Slow detached observers cannot determine durable liveness.
- **RUN-011:** Budget exhaustion cannot masquerade as success.
- **RUN-012:** Provider SDK types do not enter Conversation records; Effect AI Prompt and Response
  values remain the model-facing boundary.
- **RUN-013:** No retry policy can blindly repeat an uncertain external effect.
- **RUN-014:** Steering is delivered only before a model request and never mutates an in-flight
  response or Tool Batch.
- **RUN-015:** Follow-up input is delivered only when the Agent would otherwise stop.
