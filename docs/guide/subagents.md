---
title: Subagents
description: Build a parent that delegates to a child agent, with runnable code for tools, model bindings, budgets, and results.
---

# Subagents

A parent calls a child agent through a declared tool. Here, a trip coordinator asks a researcher
for activities, then turns its shortlist into an itinerary.

```text
Coordinator
  delegate_research_activities
    prepareInput
    Researcher
      search_activities
    projectResult
  return itinerary
```

The parent sees the shortlist. The child's tool history and research notes stay in its thread.

## Start with the child contract

Ordinary delegation needs only a target. Its toolkit remains explicit; parent tools are never
inherited.

```ts
const Research = Subagent.make("research", {
  target: Agent.make("researcher", {
    input: ResearchRequest,
    output: ResearchFindings,
    instructions: "Check opening hours. Cite URLs.",
    toolkit: ResearchTools,
  }),
});

const ResearchLive = SubagentRuntime.layer(Research, ResearchModel).pipe(
  Layer.provide(ResearchToolsLive),
);
```

The tool parameters use the child's input Schema. The default result is
`{ output: ResearchFindings, budgetExhausted: boolean }`. Identity input mapping works on decoded
values, including transformed Schemas. Expected child failures become a bounded
`SubagentExecutionFailure` with the error tag, without exposing the raw child error.

Set `parameters` and `prepareInput` when the parent's request differs from the child's input.
Set `success` and `projectResult` when only part of the child output should be exposed.
Set `failure` and `mapChildFailure` for application-specific errors. These customization points
are independent. Missing mappings validate the default value against the selected Schema and
fail with `SubagentProjectionFailure` if it does not fit.

## Define the child

The child is an ordinary agent. Give it a narrow task and the tools that task needs.
Save these files in the same directory. `tools.ts` uses sample data so the only external service
needed for this example is the model provider.

::: code-group

<<< @/snippets/travel-planner/researcher.ts{ts twoslash}

<<< @/snippets/travel-planner/tools.ts{ts twoslash}

:::

`Researcher` can call `search_activities`. The parent will receive a different toolkit containing
only the delegation tool.

## Expose the child as a tool {#define-a-delegation}

In `delegation.ts`, `Subagent.make` connects what the parent requests, what the child receives,
and what the parent gets back. Use an application name such as `research`. The definition's
tool annotation determines delegation behavior; names never confer authority or authorize replay.

Replace `Subagent.define(name, options)` with `Subagent.make(name, options)`. The deprecated
constructor remains an alias. Keep existing names, including `delegate_` names, when upgrading:
the constructor migration preserves their durable identities.

<<< @/snippets/travel-planner/delegation.ts{ts twoslash}

This example inherits parameters and input mapping from `Researcher`. A custom `prepareInput`
also receives bounded parent metadata, but never the parent's transcript.
`projectResult` drops `researchNotes` and exposes
whether the child finished because its budget ran out.

For a successful call, the parent receives a tool result shaped like this:

```json
{
  "activities": ["Riverside walk", "Food market"],
  "partial": false
}
```

The returned activities depend on the model's selection. Both projections return Effects, so they
can use services and retain typed failures.

## Give the parent the delegation tool

Save the parent as `coordinator.ts`. Add `Research.tool` to its toolkit. The parent model decides
when to invoke it, just as it decides when to call any other tool.

<<< @/snippets/travel-planner/coordinator.ts{ts twoslash}

One delegation counts as one parent tool call. The child's calls to `search_activities` consume its
own allowance. The parent waits for the projected result before continuing its model loop.

## Bind models and run

`SubagentRuntime.layer` receives the child's model Layer directly and implements the delegation
tool. Supply the child's tool handlers through `Layer.provide`.

::: code-group

<<< @/snippets/travel-planner/delegation-live.ts{ts twoslash}

<<< @/snippets/travel-planner/delegation-main.ts{ts twoslash}

:::

The model Layer on `AgentRuntime.run` supplies the parent. The model passed to
`SubagentRuntime.layer` supplies the child. They use the same model here; change either independently.

`SubagentReservationsMemoryLive` tracks the parent's child reservations. `ThreadHistory.layerTransient`
keeps this example ephemeral. `RunContextPreparationPassthrough` disables additional context
loading. Children inherit the parent's history and context services. The provider client and
HTTP Layer serve both model bindings.

## Start and manage a background worker

An authorized durable host can run a declaration in the background. A worker identifies its
reusable child Thread; a Receipt identifies one accepted input in that Thread. Programmatic
starts and follow-ups require an explicit `IdempotencyKey`:

```ts
const { worker, receipt } =
  yield *
  Subagent.start(Research, request, {
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("research:first-request"),
  });
const status = yield * Subagent.inspect(Research, worker, receipt);
const settled = yield * Subagent.await(Research, worker, receipt);
const next =
  yield *
  Subagent.followUp(Research, worker, nextRequest, {
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("research:follow-up"),
  });
```

Import `IdempotencyKey` from `@effect-agent/core/Receipt`. These operations require the host's
authorized `SubagentHost` facet. The host resolves the declaration's exact registered target;
worker values carry identity, not permission. An unavailable host fails closed.

`inspect` returns `Pending` or a `Settled` result for the requested Receipt. Successful settlement
decodes that input's saved parameters and child output before applying `projectResult`.
Interrupting or timing out `await` stops only the waiter. Use `Subagent.cancel(Research, worker,
receipt)` to request cancellation of that input. A `JoinedToHost` conflict remains explicit and
never redirects cancellation to the host input. `Subagent.list(Research, { limit: 20 })` returns a
bounded page of visible workers.

Opt in to native model tools separately:

```ts
const ResearchBackground = Subagent.background(Research, {
  start: true,
  followUp: true,
  inspect: true,
  summary: true,
  list: true,
  cancel: true,
});
```

Use `ResearchBackground.toolkit` and provide `ResearchBackground.layer` for its handlers. The
selected names are `research_start`, `research_follow_up`, `research_inspect`, `research_summary`,
`research_list`, and `research_cancel`; `Research.tool` remains the attached delegation. There is no model wait
tool. Start and follow-up tools derive stable keys from the host-bound Tool Call identity and
require the platform Crypto service. Projection services are supplied to the handler Layer.

Custom `prepareInput` receives `context.source` as `"tool"` or `"programmatic"`. Only the tool
variant contains `context.toolCallId` and `context.parent.runId`.

## Independently fund background Runs

Background workers normally reserve against their source's subtree. A host may separately
fund a root's reusable worker by supplying `WorkerBudgetAuthorizer` from
`@effect-agent/thread/WorkerHost` and allowing the exact source, destination, and allowance.
The default denies this permission. Request it from author-owned code:

```ts
const start = Subagent.start(Research, request, {
  idempotencyKey,
  budgetScope: "worker-run",
});
const tools = Subagent.background(Research, { start: true, budgetScope: "worker-run" });
```

The model cannot select the funding scope. The host checks it before every native input admission.
This mode resolves the worker's own Definition policy without inheriting its source's execution
ceiling. Declared delegation allocations bound the worker's own work and its descendants. Tokens
and cost have no cumulative ceiling unless explicitly configured. Keep finite turn, tool-call,
duration, concurrency, and result bounds, and authorize the exact allocation at the host.

The first admission freezes the scope with the worker's immutable source, grant, and depth.
A later native logical Run receives the same configured allowance. Input joining an active Run
shares that Run's usage and deadline; a Receipt does not create budget credit. Retried delivery,
replacement Attempts, owner eviction, and compaction retain the same Run journal. Changing history
or application task identifiers never resets an active allowance.

Independent funding is available only to root-created workers. Root, worker, and attached scout
still have depths zero, one, and two. Set the worker grant's `childLifetimes` to `["attached"]`
and `maxDepth` to `2` to allow scouts without another background generation. Reserve enough
`descendantInvocations` and allocation beyond the worker's own full ceiling for those scouts.
Scouts share the immediate worker Run's remaining allocation. Host worker-count, pending-input,
input-retention, concurrency, and lifetime limits still apply across the source Thread.
Set `WorkerHostConfig.maxActiveWorkersPerSource` to bound concurrent background workers separately
from the root's Tool execution concurrency; omission retains the prior concurrency ceiling.

When each source has an authorized concurrency preference, provide `WorkerConcurrencyResolver`
from `@effect-agent/thread/WorkerHost` through an Effect Layer. It receives the immutable source,
worker, principal, and explicitly selected canonical owner submission. Return `Option.some({
maxActiveWorkersPerSource })` to narrow the fixed host ceiling, or `Option.none()` to retain it.
The runtime resolves this limit inside the source reservation CAS loop, including retries after
competing appends. Counted workers have at least one input awaiting canonical completion; queued
inputs count, and steering an active worker needs no additional slot. An idle worker must acquire
a slot before a later input. Lowering the ceiling, including to zero, does not cancel incumbents
or reject replay of an established reservation. Temporarily unavailable authority must return
`WorkerError` with reason `unavailable`; it must not silently choose a fallback. This operational
limit never changes an established worker policy, delegation depth, retained-worker limit, or
Run allowance.

### Resolve policies from captured input

Supply `WorkerPolicyResolver` from `@effect-agent/thread/WorkerHost` when immutable application
input captures an execution policy separately from a finite, versioned Agent Definition. Provide
its implementation through an Effect Layer and retain the Layer's construction dependencies.
The default returns `Option.none()`, preserving registered policy inheritance and overrides.
Returning `Option.some(policy)` selects a complete policy without reapplying static overrides.
Missing authority for an opted-in definition must fail explicitly, rather than returning the
legacy fallback or loading mutable settings. Use `WorkerError` with reason `unavailable` when
the same captured evidence can become available on retry.

`Subagent.start` prepares and encodes input before the caller-bound host resolves its target
policy. Both source start and destination admission validate that exact initial input; destination
validation also runs before replay returns an existing reservation. Explicit declaration limits
still narrow the resolved policy. Construct a declaration per invocation from the same immutable
capture when its allocation includes the worker's own ceiling plus fixed attached-scout reserves.
Reuse the exact registered target Definition, grant, and reporting projection; this does not
require a dynamic registration graph or another compiled model Tool.

The initial admission stores the effective policy in the existing immutable worker origin.
For `RetainedWorker`, a resolver may affirm `origin.policy`; returning a different policy fails.
Later inputs, joined receipts, retries, and replacement Attempts never select a new worker policy.
Application follow-up preparation must retain the original authorized capture and change only
the intended task input. Receiving a new payload does not authorize changing its capture.

Root source resolution receives the exact explicitly selected owner Submission and its registered
binding, when retained. It never selects the latest input. Programmatic callers can pass
`sourceSubmissionId` to `durableRuntime.workerHost`; `WorkerHostAuthorizer` receives that locator
for authorization. Worker and attached source policies continue to come from stored lineage.
Inspection, listing, observation, and cancellation do not require resolving a source policy.
Keep retained binding versions available; a policy resolver cannot repair ambiguous historical
definition identities or reconstruct a missing capture.

A root conversation can admit a new registered Agent ID after an application upgrade. When
an explicit owner Submission selects that Agent, worker creation and reporting use its exact
retained binding, while the original `ThreadCreated` record stays unchanged. An unregistered
Agent/digest pair is rejected. Existing workers retain their original lineage and reporting
binding when the upgraded root sends follow-ups; worker and attached child Agent IDs cannot
be replaced this way. Without an explicit owner Submission, programmatic hosts continue to
use the thread's original Agent.

Captured source reporting uses the initial owner binding and stores its existing reporting intent
in the worker origin. A later input from another source revision does not replace that projection;
terminal preparation validates the original owner retained by the first worker input reservation.

## Bound child work

Children inherit omitted policy fields from their parent's resolved policy. Explicit child fields
override those defaults, then delegation ceilings clamp turns, calls, duration, tokens, and cost.
Use a partial `policy` object for selective overrides. A complete `AgentPolicy.make(...)` value
already includes its defaults, so those fields count as explicit.

When delegation `policy` is omitted, the parent's limits also set a shared delegation pool.
Children reserve slices of that pool, not a fresh copy per invocation. Explicit `SubagentPolicy`
retains the per-child limits below and derives aggregate caps by multiplying by `maxChildren`.
Use `parentCaps` to set another aggregate pool, and share identical caps across all delegations
in the parent Run. This pool accounts for child work; it is separate from the parent's own
model and tool-call counters. A global spending quota still needs a host-owned usage budget.

Ephemeral settlement refunds reported unused allocation. Durable settlement conservatively
charges the reservation when usage is unavailable. Durable admission rejects a reservation
that exceeds the shared pool, including its child-count and concurrency limits.

Parent, delegation, and child limits apply at different points:

| Setting in this example            | Meaning                                                      |
| ---------------------------------- | ------------------------------------------------------------ |
| Parent `maxToolCalls: 2`           | At most two ordinary delegation calls before finalization    |
| Delegation `maxChildren: 2`        | At most two child invocations in this parent run             |
| Delegation `maxConcurrency: 2`     | At most two children executing at once                       |
| Delegation `maxToolCalls: 4`       | Four tool calls reserved for each child                      |
| Child `maxToolCalls: 8`            | The child's definition ceiling; a delegation cannot raise it |
| Delegation `maxResultBytes: 4_096` | Maximum encoded result returned to the parent                |

To let the parent request a smaller allowance, change `delegation.ts`:

```diff
+parameters: Schema.Struct({
+  city: Schema.String,
+  focus: Schema.String,
+  maxCalls: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
+}),
+prepareInput: ({ city, focus }) => Effect.succeed({ city, focus }),
+toolCallAllowance: {
+  default: 1,
+  fromParameters: ({ maxCalls }) => maxCalls,
+},
```

A request for two calls gets two. A request for twenty gets four, the reservation ceiling in this
example. If the child returns `partial: true`, the parent can delegate again with a larger request
and forward its findings through the input. That starts a new child thread; it does not top
up the first child. See [delegation budgets](../concepts/budgets#delegation-budgets).

## Let the parent handle a failed child {#handle-failures}

The example fails the parent tool batch if the child fails. To make expected failures available to
the parent model as result data, change one option in `delegation.ts`:

```diff
-failureMode: "error",
+failureMode: "return",
```

The optional `mapChildFailure` maps a child run failure to the declared `ResearchFailed` Schema. With return
mode, the parent can receive this result and choose another approach:

```json
{ "_tag": "ResearchFailed", "reason": "AgentPolicyError" }
```

Expected delegation failures, such as denied admission or an invalid result projection, also
become result data. Suspension and durability failures stay in the error channel. Defects and
interruption retain their Effect meaning.

## Limit the child's authority {#limit-authority}

The example gives the child `TravelTools` and gives the parent only `Research.tool`. Adding a tool
to the parent does not add it to the child.

To require approval before establishing the child, add this to `Subagent.make`:

```diff
 failureMode: "error",
+needsApproval: true,
```

Supply an [approval handler](./tools#approval) for the request. This approves starting the child;
its individual actions still need their own authorization. A narrower grant hides child tools
outside its allowlist and rejects attempts to invoke them, including through the programmatic
broker. It does not reject the whole child Toolkit.

## Keep the child attached {#keep-children-attached}

```text
parent invokes child → reserve allowance → run child → project result → settle parent tool
```

Ephemeral children share the parent's Scope. Durable children have separate threads and
attempts; a waiting parent releases its worker permit.

For durable execution, register the exact target Definition with its model and version declarations.
The handler resolves its digests from that registration. Missing, ambiguous, or different
Definitions fail before child admission; an explicit `durable.targetDigests` override must
match the registration. Use the
[Node](../platforms/node) or [Cloudflare](../platforms/cloudflare) runtime setup to supply durable
storage and registrations instead of the ephemeral assembly above.

Registration accepts the same Definition and model Layer without `Agent.withModel`:

```ts
{ agent: Research.target, model: ResearchModel, definitions: childVersions }
```

Existing bindings still work, but `SubagentRuntime.layer` rejects one whose Definition is not
the delegation's exact target. Durable workers continue to require matching identity and version
digests. Change the declared versions when changing a definition, model, or tool contract.

Recovery rejoins the same child and restores its resolved policy, allowance, shared reservation,
and committed usage. These values are recorded before child admission. Uncertain admission
never starts a replacement child. Parent abort joins the child's terminal outcome before settling
the parent; it cannot undo external effects. See [child recovery](../concepts/durability#attached-subagents).

## Bound nested delegation

Nested declarations are allowed. The default `maxDepth: 1` keeps further launch tools hidden.
Set a root-relative `maxDepth` such as `2` on the participating declarations to permit a child
and grandchild, and include the permitted tool names across that subtree in the grant. Each Run
still exposes only tools from its own Toolkit. Effective names, depth, and child lifetimes
intersect at every level; a descendant cannot restore removed authority.

`grant.childLifetimes` controls children that the resulting worker may launch. For example,
a root may start a background builder whose grant permits only `["attached"]`; the builder can
then attach scouts but cannot start background grandchildren. Omitting the field permits both
lifetimes, subject to depth and budget. Inspection and cancellation tools remain usable at the
depth ceiling when their names are allowed.

Reserve descendant slots explicitly with `SubagentPolicy.descendantInvocations`; omission
reserves zero. The allocation covers the child's own execution plus its descendants. Its own
resolved policy for nested and background launches remains bounded by the parent and child
policies, while the allocation may be larger to leave a remainder. Descendants reserve only that remainder after the child's full
own ceiling is deducted, across turns, calls, duration, tokens, cost, and result bytes.
`maxChildren` includes the held descendant slots. Ephemeral subtrees also hold their possible
concurrent child slots up front and charge the whole started subtree allocation at settlement.

A top-level attached delegation's explicit pool remains separate from its parent's own Run
counters. At inherited depth one and deeper, attached and background descendants share the
reserved subtree allowance. Background roots remain bounded by the source Thread's host policy.
A declaration with sufficient depth but no remaining slots or allocation fails before starting
another child. Handoff remains unsupported.

## Continue work in the background

`Subagent.start` returns a continuing worker identity and the Receipt for its first accepted
input. `Subagent.followUp` submits more declared parameters to that same Thread. Both are
Effects; acceptance does not wait for the child to finish. Programmatic calls require an
explicit, stable `IdempotencyKey`. A model tool derives its key from its actual invocation.

```ts
const Research = Subagent.make("research", { target: researcher });
const tools = Subagent.background(Research, {
  start: true,
  followUp: true,
  inspect: true,
  list: true,
  cancel: true,
});
// Add tools.toolkit to the parent Definition and tools.layer to its services.
```

The host chooses which native tools to expose. Each retains this declaration's parameter and
result Schemas. Programmatic code acquires a separately authorized facet with
`durableRuntime.workerHost({ sourceThreadId, principal })` and provides it as `SubagentHost`.
The source Thread must already exist. No fabricated Run or Tool Call ID is needed.

For native tools, the durable runtime provides `SubagentHost.forTool` through Effect context.
The interpreter supplies the actual Agent, Thread, Run, and Tool Call identity; the runtime
refuses a binding from another Run. The reference defaults to an unavailable host and is not
a `RunOptions` callback.

Keep the two references distinct: a worker identifies its continuing Thread, while a Receipt
identifies one input. Neither grants access. Encode/decode worker references with
`Subagent.Worker(Research)`. `inspect(Research, worker)` returns the same summary as discovery;
passing a third Receipt argument inspects that exact input. `await` takes the declaration,
worker, and exact Receipt and can be interrupted without cancelling work. `cancel` targets only that
Receipt and preserves `JoinedToHost` if it joined another input's Run. Cancellation does not
close the worker or cancel an entire work tree.

Inputs can join an active Run at a safe boundary or start a later Run. Callers use the same
operation for both. Parent completion or abort leaves background work running; attached
children retain their existing cancellation and join semantics. Worker provenance, authority,
and reservations survive later coordinator Runs and host reconstruction. The admission ledger
atomically prevents replacing an ordinary Thread lane with a worker lane or changing its origin.

`Subagent.list(Research, { limit, after })` returns a bounded page. Read canonical history with
`Subagent.observe(Research, worker, { after })`: this is a finite Stream through the tail captured
at acquisition, using bounded storage pages. Pass its last `sequence` as the next cursor.
Observation acquires no execution permit and does not cancel work when interrupted.

`WorkerHostAuthorizer` separates context, read, send, and control access and denies by default.
`WorkerHostConfig` bounds retained workers, inputs per worker, pending inputs, and lifetime across
coordinator Runs (defaults: 32 workers, 64 inputs, 8 pending, 24 hours). Started allocations are
not refunded. Execution concurrency is a separate host setting; waiting attached parents release
their permits. Configure sufficient host capacity for conversational work and the chosen child
concurrency. Idle workers own no execution resources.

## Deliver completion reports

Declare the conversion from the child's projected outcome to coordinator input on the existing
coordinator registration:

```ts
const report = Subagent.reporting(Research, {
  input: CoordinatorInput,
  prepare: (outcome) =>
    Effect.succeed({
      _tag: "ResearchFinished",
      runId: outcome.runId,
      summary: outcome.outcome === "completed" ? outcome.result : outcome.failure.classification,
    }),
});

const registration = {
  agent: coordinator,
  definitions: coordinatorVersions,
  reporting: [report],
};
```

The Schema must be the coordinator Definition's exact input Schema. Registration captures the
projection's required services separately from per-Attempt services. Declare an expected mapper
failure with the optional `failure` Schema. Change the existing registration versions when changing
report behavior; recovery never substitutes another source binding or target Definition.

Launch intent pins reporting before acceptance. Each actual child Run has one logical report,
even when several steering Receipts join it; an input cancelled before any Run starts has no Run
report. The declaration's result projection and mapper produce a frozen `PreparedInput` before
delivery insertion. They should be deterministic and free of external side effects: a crash before
the canonical preparation decision commits can rerun them. Delivery retries never reproject a
committed decision. Expected failure, defect, invalid output, or preparation timeout records a
bounded refusal without replacing the child's outcome. Preparation has its own Scope and a
5-second default timeout, configurable up to 30 seconds in `WorkerHostConfig`.

For a receiving coordinator that is itself a background worker, express the report in its incoming
declaration's Parameters Schema and wrap it with
`Subagent.reportingToWorker(report, receivingDeclaration)`. This explicitly maps parameters into
Agent input and charges the additional input to the original ancestor allocation. It cannot reuse
old parameters or obtain a fresh budget. An attached destination has no independent continuing
input lifetime; delivery to it is refused. An attached scout returns directly through its waiting
parent's tool result.

Report preparation decisions appear in authorized canonical worker history. Retained delivery
records expose pending, accepted, processed, parked, and refused states through the host-owned
`MessageDeliveryStore`. A child's completion and its report's processing remain separate facts.

## Send messages through fixed peer routes

Peers are independent Agent Threads. Their input Schema belongs to the receiving Definition:

```ts
const Advisor = Messaging.peer("advisor", { target: advisor });
const send = Messaging.sendTool(Advisor);
// Programmatic: Messaging.send(Advisor, input, { idempotencyKey })
```

Provide the caller-bound `MessagingHost` returned by
`durableRuntime.messagingHost({ sourceThreadId, principal })` for programmatic operations.
The interpreter provides native tools with the actual caller facet. `sendTool`, `replyTool`,
`inboxTool`, `inspectTool`, and `retryTool` each derive a native Tool, Toolkit, and handler Layer;
install only the operations the host wants to expose.
The runtime provides `MessagingHost.forTool` through Effect context with the same per-Run
identity check and unavailable default as worker tools.

`PeerRoutes` maps a source, fixed peer name, and registered target to a destination Thread.
`PeerAuthorizer` separately authorizes context, read, send, and control and returns a stable
delivery principal. Both deny by default. Reply authorization receives the recorded sender
address and the original `reply` operation. Incoming messages confer no reverse send grant or
worker management grant. Use `Subagent.followUp` for worker input; a peer route cannot bypass
worker admission and budget ownership.

The runtime retains authenticated sender and return-address metadata separately from application
input, backed by a canonical source proof. `Messaging.inbox` returns bounded provenance from
authorized sender Threads. `Messaging.reply` requires one of those actual inbound references
and checks its sender against the declared peer. A send's optional `inReplyTo` is correlation
only. Models cannot choose arbitrary destination Threads, principals, or return addresses.

`Messaging.send` and `reply` return a retained message status. `pending` means outbound work is
stored; `accepted` includes the destination Receipt; `processed` includes its Settlement. Use
`Messaging.inspect` to read status. Automatic delivery retries preserve the exact destination,
input, principal, code version, and admission identity, including after a lost admission reply.
`Messaging.retry` renews a parked delivery's finite retry budget after control authorization;
conclusively refused and processed deliveries cannot be rewound.

Default delivery limits are eight automatic attempts, a 30-second attempt timeout, exponential
backoff from 1 to 60 seconds, and a 24-hour peer deadline. Exhaustion parks work; rejection remains
inspectable. `PeerMessageCapacity` bounds canonical send intents across all peers and principals
in a source Thread (default 256, maximum 1,000), including preparations whose insertion failed.
The delivery store separately bounds pending and retained rows. Neither history nor deduplication
evidence is automatically deleted. Effect spans identify preparation, admission, and driver
operations; persisted failures contain bounded codes rather than raw application errors.

Node's scoped delivery pump and Cloudflare's persisted alarms rediscover stored obligations even
after both Runs settle and wake hints are lost. During an active Cloudflare maintenance pass,
message delivery continues alongside source execution, so a message can reach its destination
before the source Run finishes. Progress still needs a functioning host and
available capacity. The [canonical Cloudflare application](https://github.com/danieljvdm/effect-agent/tree/main/examples/travel-planner)
provides the runnable application entrypoint.
