# Runtime engine

Status: **Draft**

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
5. Materialize the Effect AI Prompt and model request, including the model-visible final-output
   contract (RUN-028).
6. Stream Effect AI Response parts.
7. Reduce them into one complete assistant response.
8. Reject malformed or incomplete provider sequences.
9. If the assistant response is final, decode Agent output.
10. Otherwise validate the complete Tool Call batch.
11. Apply approval policy.
12. Re-evaluate optional host Tool authorization at the action boundary.
13. Apply scheduling policy and execute Tool Handlers.
14. Encode outcomes and construct deterministic Tool result messages.
15. Advance the Conversation.
16. Drain steering commands.
17. Evaluate Stop Policy.
18. If otherwise complete, drain follow-ups.
19. Begin another Turn or emit one terminal Run event.

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

A Run may carry tightening-only allowances (`toolCallAllowance`, `turnAllowance`, RUN-021): the
effective limit is `min(policy bound, max(1, floor(allowance)))`, so an allowance can never widen
the Definition's ceiling, and the `onExhaustion` resolution keys off the effective limits. This is
the budget-extension seam: an orchestrator grants a delegated child more budget by re-invoking the
delegation with a larger allowance below the child Definition's policy.

Duration, cost, and hierarchical budget-hook bounds are hard limits regardless of
`onExhaustion`. The token dimension originally shipped as a hard limit and now participates in
the `onExhaustion` resolution with a one-shot bound (RUN-025; at most one grace call).
Repeated-failure enforcement is Run-level: each completed Turn's terminal Tool
Call outcomes fold into one consecutive-failure counter in declaration order, any terminal Tool
Call success resets it, and reaching `repeatedFailureLimit` fails the Run with the typed policy
failure (`limit: "repeated-failures"`). A `repeatedFailureLimit` of `0` disables the bound.
Budget-rejected synthetic settlements neither advance nor reset that counter.

`maxDuration` bounds wall clock for one logical Run, not one process Attempt and not cumulative
worker-active time. In the durable assemblies the clock starts when the Submission's initial
`UserInputRecorded` record becomes canonical: admission and queue delay precede the Run clock,
while process loss, recovery gaps, approval suspension, unknown-outcome suspension, and
`waitingForChild` suspension do not reset or pause it. The coordinator derives one absolute
deadline from that canonical timestamp and supplies it to every replacement Attempt; the engine
accepts only a deadline that preserves or tightens its fresh policy allowance (RUN-030).
An ordinary replacement Attempt whose deadline is already expired fails before subscribing to
unresolved Tool or model execution; deadline interruption then handles only future expiry.
If the deadline expires while attached children are suspended, the coordinator still completes
the mandatory joins of children whose Settlements are already canonical before failing the
parent. The coordinator supplies the exact still-open child Call IDs, the engine verifies they
are every and only the resumed delegation calls, and duration interruption is restored around
the continuation after those joins. That recovery cleanup authorizes no new child, ordinary
Tool, or model execution and cannot turn the expired Run into success, including when the
deadline expires during the post-join continuation (SUB-019).

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

### Host action authorization

`RunOptions.toolAuthorization`, when present, runs for every still-executable model-declared call
in an application Tool batch after complete-batch validation and approval, but before durable
preparation, semaphore acquisition, or any Handler. Every decision in the batch completes in
declaration order before
the first Handler may start. The request carries canonical `ConversationId`, `RunId`, `TurnId`,
Turn number, the Agent-Schema encoded Run input, the selected call's stable ID/name/encoded
parameters/execution class. A durable host receives the exact admitted canonical Submission input.
Policy remains host-owned; the library assigns no mutation meaning to Tool names. Programmatic
`ToolBroker` calls are outside this hook.

A durable batch resume reconstructs input authority from the canonical Submission and calls from
the pending response record, then invokes the same hook again with the same Run, Turn, input, and
call identity. Already-settled calls are omitted because no Handler can start for them. A denied
decision emits `ToolCallFailed` and fails the Run with `AgentToolAuthorizationDenied` before the
denied Attempt starts any Handler or creates a new side effect. A fresh denial precedes
preparation; a resumed denial may retain the prior Attempt's prepared record and historical effects
but writes no new preparation. DN/DC then record the ordinary bounded failed Submission settlement,
which is terminal and cannot select the call for another retry. A hook failure remains typed in the
Run error channel and is likewise fail-closed.

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

The DN and DC assemblies add Unknown Outcome. Approval suspension and unresolved Tool Calls are
nonterminal and may remain unresolved indefinitely; the runtime does not replay an ordinary
unresolved call merely to force settlement. Denial happens during preflight, before any application
Handler starts. Provider-executed calls likewise have no application-handler attempt.

An in-memory application-handler attempt therefore starts only for a native call that passed
preflight. Its complete lifecycle classification set is:

- A successful attempt is terminal.
- A failed attempt is terminal. Failures include returned failures, typed or infrastructure
  Causes, and post-terminal anomalies.
- An interrupted attempt is terminal and remains classified as interrupted.
- A waiting attempt is nonterminal and may last indefinitely.

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
- A provider context-length rejection is classified into a typed overflow. With a configured
  `contextTokenLimit` the engine compacts (§9) and issues at most one framework-level retry
  (transport ambiguity may still duplicate the external model execution);
  otherwise, or on a second overflow, the Run fails with `ContextOverflowError` rather than an
  opaque provider error.
- Tool handler failures do not automatically retry.
- Tool infrastructure retry is opt-in and must respect idempotency/durability annotations.
- Output encoding failure never retries as an external Tool call.
- Defects are not retried by default.

Effect's `retry` operator is an implementation mechanism, not the product policy. Retrying an
effectful Tool without durable evidence is forbidden.

## 9. Context and compaction

Context preparation:

1. Reconstruct canonical/ephemeral Conversation Messages and append stable instructions/input.
2. On a durable resume, replace the re-evaluated history prefix with the canonical run-journal
   projection.
3. Apply the optional host `RunContextPreparation` service to the resulting immutable source.
4. Calculate window/budget over that model-visible result.
5. Compact with the engine-native policy if required.
6. Append derived run status and the final-output contract and produce normalized Model Input.

Official prior history remains the exact prefix of the newly materialized Run history. Evaluated
instructions and the current decoded input append after that prefix; a new Run must never prepend,
rewrite, or reorder already-official messages.

`RunContextPreparation` is a generic Effect service, not a Conversation store. Its optional prompt
hook receives stable Run/Turn identities, the immutable source `Prompt`, and the rendered
output-contract text when available; it returns only the prompt for the next model call. Its
optional `toolAuthorization` hook is the closed durable-host form of the action boundary in §5. An
absent hook is the original pass-through behavior. `DurableAgentRuntime.layerWithContext` exposes
the service in its Layer requirement; `DurableAgentRuntime.layer` explicitly supplies
`RunContextPreparationPassthrough` for compatibility. Coordinators always apply journal
reconstruction before a prompt hook and rebuild action-authorization authority from canonical
records before a resumed Handler, so an ownership retry or a new host incarnation cannot bypass
either boundary. The returned prompt is never assigned to official history and cannot enter
canonical records.

### Window and budget calculation

Step 4 is implemented from policy plus observed usage. The engine tracks the most recent model
call's provider-reported input and output tokens as the live-context estimate, and estimates the
next call's context as that value plus a chars/4 approximation of parts appended since (the whole
prompt on the first call). Cache-read and cache-write input tokens are accounted distinctly from
uncached input and forwarded through the budget hook. `AgentPolicy.contextTokenLimit`, when
present, bounds one call's live context; `tokenBudget` remains the cumulative runaway stop; spend
belongs to `costBudgetMicrousd`.

### Tool result bounds

Every application Tool result, including MCP results, is bounded once at the settle boundary by
`AgentPolicy.toolResultBounds` (default 50 KiB) before it enters records or prompts, so both
carry the same value. An oversized encoded result becomes the canonical `TruncatedToolResult`
envelope preserving head, tail, and original byte size. Provider-executed results are exempt.

### Run-status message

With `AgentPolicy.runStatus: "appended"` (the default), each outgoing model request ends with a
derived run-status message showing Turns, Tool Calls, tokens against budget, last-call
context, and elapsed time, with a wrap-up warning once any tracked dimension reaches 80%. The
message is derived at prompt-assembly time and is never persisted as canonical history.

### Compaction

Step 5 runs at the pre-Turn seam, synchronously, when the estimated next context exceeds
`contextTokenLimit`, per `AgentPolicy.compaction`:

1. **Prune** (`clear-tool-results`): application Tool results older than the protected
   `keepRecentTokens` tail are replaced with `"[tool result cleared by compaction]"`, preserving
   message structure and call/result pairing.
2. **Summarize**, if still over and the mode allows. One metered model call on the Run's bound
   model summarizes the goal, constraints, progress, decisions, next steps, and critical context.
   The call's usage counts like any other. The rebuilt prompt is the
   instruction prefix, the summary message, and the kept tail.

Cut points never split an assistant Tool call from its result, and prepared-unsettled Tool
records are always in the kept tail. In the DN and DC assemblies each compaction appends a
canonical `CompactionCreated` record (`kind`, `coversThrough`, optional `summary`) inside the
epoch-fenced log it covers. The run-journal projection folds it, and covered records render as the
summary or with cleared Tool results. The projection ignores an invalid range and keeps the full
history authoritative. The session selects `coversThrough` itself, walks its own records with the
shared estimator, and limits coverage to prior Runs. The owning Run's records are never covered,
because its first response record carries the evaluated
instructions and input. The engine's in-memory rebuild is therefore a view that may cover more
than the record; the record is canonical. A threshold compaction with no prior-Run records to
cover commits no record and applies only in-view. Compaction appends or emits a summary representation and
preserves the source history; failed compaction leaves the original history authoritative.
Host-supplied, digest-bound compaction artifacts remain available through the capabilities layer
([capabilities §6](./capabilities.md)). They shape each model request through
`RunContextPreparation`; unlike `CompactionCreated`, the supplied artifact itself is not canonical
and is recomputed after restart from the canonical source.

### Budget warnings and the token soft landing

Crossing 80% of a configured budget dimension emits a `BudgetWarning` Run Event once per
dimension (RUN-025). Turn and Tool Call exhaustion resolve through the Stop Policy's
`onExhaustion` machinery in section 3 and RUN-018 through RUN-020. Token exhaustion is a
post-response check and joins the same resolution. Under the default `"final-answer"`, a breaching response that
already carries a decodable final answer at a stop completes the Run directly with that answer
and no extra call, and otherwise the Run takes at most one constrained grace Turn
(`toolChoice: "none"`; its usage is consumed once and exempt from re-triggering breach). Either
way the Run settles as `RunCompleted` with `finishReason: "budget-exhausted"` and
`exhausted: "tokens"`. A grace-Turn response that declares Tool calls fails typed
(`ModelProtocolError`, RUN-020); under `onExhaustion: "fail"` token breach keeps the fail-fast
contract with `AgentPolicyError`. A `maxDuration` breach always fails because a grace call would
extend wall clock past the contract.

After these steps the engine appends the model-visible final-output contract to the produced
Model Input (RUN-028): one framework-owned system message carrying the JSON Schema derived from
the Agent's output Schema, inserted immediately after the request's last system message and never
entered into official history. Context transforms receive the exact contract text
(`RunContextRequest.outputContract`) so a limit-targeting adapter can reserve its overhead, and
no transform can remove or alter it. A Definition whose output Schema cannot render to JSON
Schema produces no contract. The field is absent and the request is unchanged, with one Turn-1
diagnostic per Attempt.

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

An Agent Definition may declare an application run-disposition Schema plus a pure selector from
decoded output. At an ordinary completion seam the engine selects, Schema-encodes, and JSON-checks
that value before adding it to `RunCompleted.runDisposition`; `undefined` means absent. Invalid
selection fails typed with `AgentRunDispositionError`. A final-answer budget completion never
evaluates or carries the selector result. Reducers fail closed if a budget-completed event carries
a disposition or if an event carries one without a Definition-owned Schema. No runtime path parses
output prose or infers disposition from Tool events. When the application selector throws, the
typed error retains the original value in its Schema-safe diagnostic `cause`; the terminal event
uses a fixed non-sensitive message and never serializes that foreign cause. The public
`AgentResultSchema` independently rejects a disposition on `finishReason: "budget-exhausted"`, so
untrusted serialized results cannot bypass the event-reducer invariant.

Raw provider chunks are never mixed into the stable event union.

`RunFailed` covers expected failures. The engine keeps defects as defects: a defect fails the
event stream with its full Cause and is never converted into a typed failure or a successful
stream end. Host boundaries that forward Run Events to a UI or transport may opt in to the
exported `withTerminalDefectEvent` combinator, whose contract is:

- typed failures and interruptions pass through untouched because their terminal event was
  already emitted;
- a cause carrying a defect first appends one bounded terminal
  `RunFailed { errorTag: "Defect" }` with a bounded string rendering rather than the raw defect
  value, then rethrows the original cause unchanged;
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

This section documents the S1 APIs for attached ephemeral Subagents
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
Run's own stream. Consistent with the Run's existing buffering, the Run
stream is the only consumer, so no external observer can backpressure the
batch. Sink events appear inside the batch, and the batch settles, including on failure, only
after already-emitted events have surfaced in the Run stream. Emission after
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
and Parent Link plus the `DetachedRun` observation API (`await`,
`events`, `observe`) with the child's full `E`/`R` visible. Depth exposure is
the seam the delegation preflight uses to reject nested delegation (SUB-029);
the engine itself enforces no delegation policy.

Both services are excluded from `AgentRuntimeRequirements` because the
interpreter supplies them itself; an application Layer must not provide them.

## 12.1 Code Mode programmatic invocation seam

This section documents the engine API for Code Mode programmatic Tool invocation
([capability specification §9.1](./capabilities.md)). The engine owns a broker seam in
the same pattern as `AgentSpawner` and `DurableStep`: provided locally by the interpreter, bound
per outer Tool Call, and excluded from `AgentRuntimeRequirements`. The live native Toolkit
handlers, engine policy context, and parent Tool Call identity are capabilities bound when the
per-outer-call broker service is constructed. Per-call input from generated code contains only
the namespace, method, and encoded arguments. The broker allocates each call's sequence index from
its own monotonic per-pass state: generated code never supplies the authoritative index, and a
transport-carried index is validated against the broker's state, failing typed on mismatch. A
caller inside business execution can never substitute handlers or policy.

A programmatic call shares the existing per-call execution path for Tool lookup, parameter
handling, approval preflight, scoped handler execution, typed failure handling, success and
failure encoding, and per-call telemetry. It executes under the parent Tool Call's already-held
scheduling permit and never acquires Tool Batch permits of its own: the batch semaphore is
created per batch, so re-entrant acquisition would deadlock at `toolConcurrency: 1`, and a
second batch path would let inner calls escape the declared concurrency bound. Calls are
strictly sequential. A host call issued while another call from the same pass is unsettled
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
- **RUN-011:** Budget exhaustion cannot masquerade as success, and its dimension survives
  durably typed. A Run that settles through final-answer resolution after Turn, Tool Call, or
  token exhaustion carries `finishReason: "budget-exhausted"` and the `exhausted` dimension
  marker, never `"model-stop"`, on the live terminal event and on the durable
  `SubmissionSettled` record; `onExhaustion: "fail"` fails typed before any successful stop,
  and a Run failed by `AgentPolicyError` settles with the typed `limit` preserved as the
  durable record's `policyLimit`. Consumers never reconstruct either dimension from message
  text. The metadata is family-bound fail-closed: `exhausted` decodes only alongside
  `finishReason: "budget-exhausted"` on a `completed` settlement, `policyLimit` only on a
  `failed` settlement whose recorded failure projection is the `AgentPolicyError` it names, and
  histories persisted before the dimensions became durable decode with the metadata absent.
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
- **RUN-021:** Per-Run allowances are tightening-only: the effective Turn/Tool-Call limit is the
  minimum of the Agent Policy bound and the normalized allowance, never more, and the
  `onExhaustion` resolution applies at the effective limit.
- **RUN-022:** Every application Tool result, including MCP results, is bounded by policy
  `toolResultBounds` once at the settle seam before entering records or prompts; an oversized
  result becomes the canonical `TruncatedToolResult` envelope preserving head, tail, and
  original byte size, and provider-executed results are exempt.
- **RUN-023:** The engine accounts cache-read and cache-write input tokens distinctly from
  uncached input and tracks the most recent call's input/output tokens as the live-context
  estimate, both visible through the budget hook.
- **RUN-024:** With policy `runStatus: "appended"`, every outgoing model request carries a
  derived run-status message showing Turns, Tool Calls, tokens, and elapsed time; the
  message is never persisted as canonical history.
- **RUN-025:** Crossing 80% of a configured budget emits `BudgetWarning` once per dimension.
  The token dimension participates in the `onExhaustion` resolution: under `"final-answer"`, a
  token-breaching response that already carries decodable
  final output settles directly, and otherwise the Run takes at most one constrained grace Turn
  (`toolChoice: "none"`, its usage consumed once and exempt from re-triggering breach),
  completing with `finishReason: "budget-exhausted"` and `exhausted: "tokens"`; under `"fail"`
  token breach stays fatal. Duration and cost remain hard limits in both modes.
- **RUN-026:** When the estimated next model-call context exceeds policy `contextTokenLimit`,
  including the reserved size of the model-visible output contract the engine appends after
  preparation under RUN-028, the engine compacts at the pre-Turn boundary. It prunes old Tool
  results and then summarizes through one metered model call. Compaction never splits an assistant
  Tool call from its result and always keeps prepared-unsettled Tool records. In the DN and DC
  assemblies, compaction appends a canonical `CompactionCreated` record that projections fold, and source
  history is never erased.
- **RUN-027:** A provider context-length rejection is classified typed; with compaction
  configured the engine compacts and issues at most one framework-level retry (transport
  ambiguity may still duplicate the external model execution); otherwise, or on a second
  overflow, the Run fails with `ContextOverflowError`.
- **RUN-028:** Every model request of a Run whose Agent Definition declares an output Schema
  carries a model-visible representation of that Schema derived by Effect AI's JSON-Schema
  derivation, applied after context preparation and never entered into official history; a
  Definition whose output Schema cannot be derived runs with the documented fallback and a
  diagnostic, never a silent difference.
- **RUN-029:** An Agent Definition may declare an application-owned run-disposition Schema and
  decoded-output selector. Only an ordinary completed Run may Schema-validate, emit, and durably
  persist the selected value. Failed, interrupted, aborted, incomplete, run-less, and
  budget-exhausted Runs carry none; invalid values fail typed, and consumers never infer a
  disposition from prose or Tool output.
- **RUN-030:** `maxDuration` is one wall-clock allowance per logical Run. DN and DC derive its
  absolute deadline from the first canonical input record and preserve that deadline across
  Attempt replacement and every durable suspension; admission and queue delay are excluded, and
  no Run option may widen the Definition's fresh duration allowance. Already-settled attached
  children still join as mandatory recovery cleanup before the expired parent fails. The engine
  verifies the coordinator's exact open delegation Call IDs and restores duration interruption
  before continuation, without authorizing a new model, ordinary Tool, or child execution.
- **RUN-031:** Optional host Tool authorization runs for each model-declared call in the complete
  still-executable application batch before durable preparation or any Handler. Fresh and
  durable-resumed calls present the same canonical Run/Turn/input authority and stable call
  descriptor; denial fails typed and settles accepted work terminally without the denied Attempt
  starting a Handler or creating a new side effect. Fresh denial writes no prepared record, and
  resumed denial writes no new preparation.
