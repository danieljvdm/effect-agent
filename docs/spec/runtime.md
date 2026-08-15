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

Stop Policy evaluation (step 16) enforces the finite Agent Policy at the Turn seam, before the
next model request. Turn and Tool Call limits bound the work itself: work that would exceed them
never starts. How exhaustion resolves is policy-selected via `onExhaustion`:

- `"fail"` fails the Run typed (`AgentPolicyError`) at the seam, exactly the pre-RUN-018
  behavior.
- `"final-answer"` (the default) gives the model one constrained settlement opportunity. An
  over-budget declared Tool batch never executes a handler and is never durably declared as a
  pending batch; every open call settles synthetically as a model-visible failed result carrying
  the encoded policy failure, exempt from repeated-failure folding. Every subsequent model
  request forbids tool use (`toolChoice: "none"`), and Turn exhaustion admits exactly one grace
  Turn past `maxTurns` under the same constraint. A Run that settles this way completes with
  `finishReason: "budget-exhausted"` (RUN-011), and a model that declares a Tool Call under the
  constraint fails the Run typed (`ModelProtocolError`, RUN-020).

Duration, token, cost, and hierarchical budget-hook bounds are hard rails regardless of
`onExhaustion`. Repeated-failure enforcement is Run-level: each completed Turn's terminal Tool
Call outcomes fold into one consecutive-failure counter in declaration order, any terminal Tool
Call success resets it, and reaching `repeatedFailureLimit` fails the Run with the typed policy
failure (`limit: "repeated-failures"`). A `repeatedFailureLimit` of `0` disables the bound.
Budget-rejected synthetic settlements neither advance nor reset that counter.

Note on durable Attempts: the batch-resume seam counts Tool Calls from the resumed batch onward,
so `maxToolCalls` is enforced per Attempt under the durable coordinator. This is existing,
documented behavior; cumulative cross-Attempt accounting would require persisted counters and is
deliberately out of scope for RUN-018.

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

When a declared Tool Call settles, its call-level terminal classification is:

- success;
- failure returned as a terminal Tool value;
- typed failure in the Effect error channel;
- infrastructure failure;
- denied;
- interrupted.

The DN and DC assemblies add unknown outcome. Approval suspension and unresolved Tool Calls are
nonterminal and may remain unresolved indefinitely; the runtime does not replay an ordinary
unresolved call merely to force settlement. Denial happens during preflight, before any application
Handler starts. Provider-executed calls likewise have no application-handler attempt.

An in-memory application-handler attempt therefore starts only for a native call that passed
preflight. Its complete lifecycle classification set is:

- success — terminal;
- failure — terminal, including returned failures, typed or infrastructure Causes, and
  post-terminal anomalies;
- interruption — terminal for that in-memory attempt and preserved as interruption;
- waiting — nonterminal and potentially indefinite.

Only success and failure receive a bounded terminal telemetry outcome. Interruption produces no
terminal outcome log. Denied and provider-executed calls remain solely call-level classifications:
neither creates an application-handler attempt.

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

Official prior history remains the exact prefix of the newly materialized Run history. Evaluated
instructions and the current decoded input append after that prefix; a new Run must never prepend,
rewrite, or reorder already-official messages.

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

`RunFailed` covers expected failures. The engine keeps defects as defects: a defect fails the
event stream with its full Cause and is never converted into a typed failure or a successful
stream end. Host boundaries that forward Run Events to a UI or transport may opt in to the
exported `withTerminalDefectEvent` combinator, whose contract is:

- typed failures and interruptions pass through untouched (their terminal event was already
  emitted — nothing is duplicated);
- a cause carrying a defect first appends one bounded terminal
  `RunFailed { errorTag: "Defect" }` — a bounded string rendering, never the raw defect value —
  then rethrows the original cause unchanged;
- identity fields come from the last event already streamed; a defect before the first event is
  rethrown without an event, because the helper never fabricates Run identities.

## 11. Backpressure

Local `stream` uses a bounded queue:

- default strategy: suspend producer at semantic event seams;
- text delta coalescing MAY reduce volume;
- semantic terminal events are never dropped;
- disconnecting/interrupting the sole ephemeral consumer interrupts the Run unless execution was
  explicitly detached.

Durable transports observe from the journal/projection and do not own execution liveness.

## 12. S1 Subagent execution seam

This section documents the S1 surface added for attached ephemeral Subagents
([Subagent specification §4.3, §10.1](./subagents.md)). The engine owns the one
interpreter and exposes delegation through execution options and two
engine-provided services; it implements no second child loop and no
delegation policy.

### Execution options

`RunOptions` accepts preallocated identity and non-model-visible lineage:

- `conversationId` reuses an existing ephemeral Conversation identity;
- `runId` is used instead of `IdGenerator` when supplied, so a delegating
  handler can know the intended child identity;
- `parentLink` carries the core `SubagentParentLink` for a delegated child
  Run. It never enters the model prompt or event payloads; the engine uses it
  only to fix the Run's delegation depth (`parentLink.depth` for a child, `0`
  when absent), and future durable work persists it as child lineage.

### Run event sink

`RunEventSink` is an engine-owned `Context.Service` provided locally to Tool
handlers so a delegation handler can emit the seven Subagent lifecycle events
into the parent Run's semantic stream. `emit` accepts a pre-base payload (the
core event minus `eventVersion`, `runId`, `conversationId`, `agentId`,
`sequence`, `timestamp`, and `turnId`); the engine stamps those fields through
the same `eventBase` path as every other event, so the base identity and the
emitting batch's Turn are authoritative and the Run sequence stays monotonic.
Each Tool batch owns one sink backed by an unbounded queue drained by the
Run's own stream — consistent with the Run's existing buffering, the Run
stream is the only consumer, so no external observer can backpressure the
batch. Sink events surface inside the batch, and the batch settles (including
by failure) only after already-emitted events have surfaced. Emission after
the batch settled, or outside any Tool batch, fails closed with the typed
`RunEventSinkClosedError`.

### AgentSpawner

`AgentSpawner` is the engine-owned service contract through which a declared
delegation Tool runs an Attached Child on the same interpreter. The engine
provides it locally to every Run, bound to a narrow immutable parent value
(`agentId`, `conversationId`, `runId`) and the Run's delegation depth (`0`
for a root Run); it exposes neither the engine's mutable Run state nor a root
Layer Context. `spawn(binding, input, delegation, options?)` allocates a
fresh child `ConversationId`/`RunId` through `IdGenerator` (no Conversation
reuse), constructs the immutable Parent Link at `depth + 1`, and starts the
child eagerly inside the caller-provided `Scope`, so parent interruption
reaches the child and its finalizers (SUB-011). It returns the child identity
and Parent Link plus the `DetachedRun` observation surface (`await`,
`events`, `observe`) with the child's full `E`/`R` visible. Depth exposure is
the seam the delegation preflight uses to reject nested delegation (SUB-029);
the engine itself enforces no delegation policy.

Both services are excluded from `AgentRuntimeRequirements` because the
interpreter supplies them itself; an application Layer must not provide them.

## 12.1 Code Mode programmatic invocation seam

This section documents the engine surface for Code Mode programmatic Tool invocation
([capability specification §9.1](./capabilities.md), ADR-0017). The engine owns a broker seam in
the same pattern as `AgentSpawner` and `DurableStep`: provided locally by the interpreter, bound
per outer Tool Call, and excluded from `AgentRuntimeRequirements`. The live native Toolkit
handlers, engine policy context, and parent Tool Call identity are capabilities bound when the
per-outer-call broker service is constructed; per-call input from generated code is data only —
namespace, method, and encoded arguments. The broker allocates each call's sequence index from
its own monotonic per-pass state: generated code never supplies the authoritative index, and a
transport-carried index is validated against the broker's state, failing typed on mismatch. A
caller inside business execution can never substitute handlers or policy.

A programmatic call shares the existing per-call execution path — Tool lookup, parameter
handling, approval preflight, scoped handler execution, typed failure handling, success and
failure encoding, and per-call telemetry. It executes under the parent Tool Call's already-held
scheduling permit and never acquires Tool Batch permits of its own: the batch semaphore is
created per batch, so re-entrant acquisition would deadlock at `toolConcurrency: 1`, and a
second batch path would let inner calls escape the declared concurrency bound. Calls are
strictly sequential — a host call issued while another call from the same pass is unsettled
fails with a typed concurrency error. Each call's identity derives from the outer `ToolCallId`
plus the broker-owned zero-based sequence index; neither the model nor generated code can choose
or forge it.

Two behaviors are specific to the broker path. Tool-call and duration budgets are consumed and
checked before every inner invocation, so budget exhaustion prevents the next call mid-pass;
direct model-declared calls keep their Turn-boundary accounting unchanged. Result size bounds
and redaction at the sandbox boundary are broker-owned; no such stage is added to the direct
path. An inner call that would require approval fails with a typed policy failure and never
suspends in the ephemeral slice. Per-Tool authorization remains application- and handler-owned;
the engine contributes approval policy, scheduling, budgets, encoding, and telemetry only.

## 13. Runtime invariants

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
- **RUN-011:** Budget exhaustion cannot masquerade as success. A Run that settles through the
  final-answer resolution carries `finishReason: "budget-exhausted"` — never `"model-stop"` — on
  the live terminal event and on the durable `SubmissionSettled` record.
- **RUN-012:** Provider SDK types do not enter Conversation records; Effect AI Prompt and Response
  values remain the model-facing boundary.
- **RUN-013:** No retry policy can blindly repeat an uncertain external effect.
- **RUN-014:** Steering is delivered only before a model request and never mutates an in-flight
  response or Tool Batch.
- **RUN-015:** Follow-up input is delivered only when the Agent would otherwise stop.
- **RUN-016:** Programmatic Tool calls execute only through the engine-owned broker seam, under
  the parent Tool Call's already-held permit, strictly sequentially, with identities derived
  from the outer `ToolCallId` and a broker-owned zero-based index that generated code cannot
  supply; a concurrent host call fails typed.
- **RUN-017:** Every programmatic Tool call consumes the Run's Tool-call and duration budgets
  before its handler is invoked; exhaustion prevents the next call mid-pass while direct calls
  keep their Turn-boundary accounting.
- **RUN-018:** Under `onExhaustion: "final-answer"` (the default), an over-budget declared Tool
  batch never executes a handler and is never durably declared: every open call settles
  synthetically as a model-visible failed result exempt from repeated-failure folding,
  subsequent model requests forbid tool use, and the Run completes with
  `finishReason: "budget-exhausted"`. Under `"fail"` the Run fails typed before the batch
  starts.
- **RUN-019:** Turn exhaustion under `"final-answer"` admits exactly one grace Turn past
  `maxTurns`, with tool use forbidden; the pending batch at the final permitted Turn executes
  normally, and no second grace Turn exists. Under `"fail"` the Run fails typed at the seam.
- **RUN-020:** Final-answer Turns are fail-closed: a model that declares any Tool Call under a
  `toolChoice: "none"` request fails the Run typed.
