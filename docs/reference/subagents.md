---
title: Subagent policies and recovery
description: Configure subagent projections, budgets, authority, nested delegation, and durable worker recovery.
---

# Subagent policies and recovery

Start with the [subagent overview](../guide/subagents) to choose a lifecycle, then follow the
[ephemeral attached](../guide/subagents/ephemeral-attached),
[durable attached](../guide/subagents/durable-attached), or
[background worker](../guide/subagents/background) guide. This reference covers advanced configuration
shared by those guides.

## Pass the Agent directly

For the default input and result contract, the declaration is optional:

```ts twoslash
import { Subagent } from "effect-agent";
import { HotelResearcher } from "./background-updates.ts";

// Before: an explicit declaration, with the default name and mappings.
const declaration = Subagent.make("hotel-researcher", { target: HotelResearcher });
const before = Subagent.background(declaration, { start: true, reportToParent: true });

// After: the same generated hotel-researcher_start Tool and exact target.
const after = Subagent.background(HotelResearcher, { start: true, reportToParent: true });
```

Use `Subagent.make` when you need another name, input/result projections, grants, or budgets.
The host registers the original Agent definition; background configuration does not derive a
replacement Agent. Existing explicit declarations and custom completion reporting remain supported.

## Input and result mappings

`Subagent.make("research", { target: researcher })` is enough to declare delegation.
`researcher` is an ordinary Agent Definition with input and output Schemas and an explicit toolkit.

The tool parameters use the child's input Schema. The default result is
`{ output: ChildOutput, budgetExhausted: boolean }`. Identity input mapping works on decoded
values, including transformed Schemas. Expected child failures become a bounded
`SubagentExecutionFailure` with the error tag, without exposing the raw child error.

Set `parameters` and `prepareInput` when the parent's request differs from the child's input.
Set `success` and `projectResult` when only part of the child output should be exposed.
Set `failure` and `mapChildFailure` for application-specific errors. These customization points
are independent. Missing mappings validate the default value against the selected Schema and
fail with `SubagentProjectionFailure` if it does not fit.

Child terminal events and the `projectResult` context expose `usage` and `delegatedUsage`.
Durable joins preserve verified totals across recovery; see [usage accounting](../guide/run-agents.md#provider-usage-and-cost-evidence).
For live child deltas or pricing, pass `child.budget` or `child.estimateCostMicrousd` to
`SubagentRuntime.layer`.

Custom `prepareInput` receives `context.source` as `"tool"` or `"programmatic"`.
Only the tool variant contains `context.toolCallId` and `context.parent.runId`.
A custom mapper receives bounded parent metadata, never the parent's transcript.

## Upgrade the constructor

Replace `Subagent.define(name, options)` with `Subagent.make(name, options)`. The deprecated
constructor remains an alias. Keep existing names, including `delegate_` names, when upgrading:
the constructor migration preserves their durable identities.

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

In the [attached walkthrough](../guide/subagents/ephemeral-attached), parent, delegation, and child limits apply at different points:

| Setting in this example            | Meaning                                                      |
| ---------------------------------- | ------------------------------------------------------------ |
| Parent `maxToolCalls: 2`           | At most two ordinary delegation calls before finalization    |
| Delegation `maxChildren: 2`        | At most two child invocations in this parent run             |
| Delegation `maxConcurrency: 2`     | At most two children executing at once                       |
| Delegation `maxToolCalls: 4`       | Four tool calls reserved for each child                      |
| Child `maxToolCalls: 8`            | The child's definition ceiling; a delegation cannot raise it |
| Delegation `maxResultBytes: 4_096` | Maximum encoded result returned to the parent                |

In the [attached example](../guide/subagents/ephemeral-attached), change `delegation.ts` to let the parent request a smaller allowance:

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

The [attached example](../guide/subagents/ephemeral-attached) fails the parent tool batch if the child fails. To make expected failures available to
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

The [attached example](../guide/subagents/ephemeral-attached) gives the child `TravelTools` and gives the parent only `Research.tool`. Adding a tool
to the parent does not add it to the child.

To require approval before establishing the child, add this to `Subagent.make`:

```diff
 failureMode: "error",
+needsApproval: true,
```

Supply an [approval handler](../guide/tools#approval) for the request. This approves starting the child;
its individual actions still need their own authorization. A narrower grant hides child tools
outside its allowlist and rejects attempts to invoke them, including through the programmatic
broker. It does not reject the whole child Toolkit.

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

## Start workers from application code

The [background guide](../guide/subagents/background) shows model-facing tools. Application code can also call
`Subagent.start`, `followUp`, `inspect`, `await`, and `cancel` with an authorized `SubagentHost`.

A programmatic start needs an **idempotency key**: a stable identifier for one intended input.
If delivery is retried, reuse the same key and parameters so the host can recognize that input.
Use a new key for a new input. Native model tools derive their keys automatically.

```ts twoslash
import * as Subagent from "@effect-agent/capabilities/Subagent";
import { IdempotencyKey } from "@effect-agent/core/Receipt";
import { Effect, Schema } from "effect";

import { Research } from "./delegation.ts";

const program = Effect.gen(function* () {
  const { worker, receipt } = yield* Subagent.start(
    Research,
    { city: "Lisbon", focus: "food and walking" },
    { idempotencyKey: Schema.decodeSync(IdempotencyKey)("lisbon-research-1") },
  );
  return yield* Subagent.inspect(Research, worker, receipt);
});
```

Provide the facet obtained from
`durableRuntime.workerHost({ sourceThreadId, principal })` as `SubagentHost`. The source thread
must already exist. Programmatic `Subagent.await` waits for an exact receipt; interruption or
timeout stops only that waiter, leaving the worker running.

## Background delivery and recovery

`Subagent.start` returns a continuing worker identity and the Receipt for its first accepted
input. `Subagent.followUp` submits more declared parameters to that same Thread. Both are
Effects; acceptance does not wait for the child to finish. Programmatic calls require an
explicit, stable `IdempotencyKey`. A model tool derives its key from its actual invocation.

If another delivery attempt owns the claim, `start` and `followUp` can fail with
`WorkerError` reason `delivery-pending`. This confirms that the exact input is durably retained;
it does not confirm destination acceptance or that the child started. The host's existing
delivery recovery owns progress. Preserve the same parameters and idempotency key when
reconciling instead of launching replacement work. Conclusive refusal retains its specific
reason, while a recorded retry failure or a storage exception still reports `storage`.
These operations add no waiting or polling for a competing claim.

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
Successful receipt inspection decodes that input's saved parameters and child output before
applying `projectResult`. The optional `summary: true` background tool exposes a worker summary;
projection services must be supplied to the handler Layer. Native start and follow-up tools
require the platform Crypto service.

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

## Completion report guarantees

Use `Subagent.background(Research, { start: true, reportToParent: true })` for a standard
`WorkerCompletion` message, as shown in the [background walkthrough](../guide/subagents/background).
Its `report` has the same typed projected success or bounded failure as `Subagent.WorkerReport`;
`budgetExhausted` also preserves exhaustion when an application projection omits it.
The admitted parent input stays available to instructions and policy. The model receives the
completion as a user message, so child output never becomes trusted instructions.
Canonical `UserInputRecorded.messageAdmission` distinguishes framework completions from peer
provenance through the `InputMessage` Schema. Inspect `WorkerCompletion` with its Schema before
reading a completion; use `MessageAdmission` for peer messages.

For an application-specific format, pass a `Subagent.reporting(Research, { input, prepare })`
descriptor as `reportToParent`. Its input Schema must be the parent's exact input Schema.
The existing registration `reporting` array remains supported for stored custom-report intents;
do not register the same descriptor twice. Projection services are captured separately from
per-Attempt services. Declare expected mapper failures with its optional `failure` Schema.
Change registration versions when changing projection behavior. Automatic discovery pins its
mode and target in the registration digest; recovery never substitutes another source or target.

Application-driven starts with standard reports must acquire the host facet with the exact
`sourceSubmissionId` whose application input supplies parent context. Model tool calls already
carry that identity. No current or latest input is guessed for a programmatic caller.

Launch intent pins reporting before acceptance. Each actual child Run has one logical report,
even when several steering Receipts join it; an input cancelled before any Run starts has no Run
report. The declaration's result projection and optional mapper produce a frozen `PreparedInput` before
delivery insertion. They should be deterministic and free of external side effects: a crash before
the canonical preparation decision commits can rerun them. Delivery retries never reproject a
committed decision. Expected failure, defect, invalid output, or preparation timeout records a
bounded refusal without replacing the child's outcome. Preparation has its own Scope and a
5-second default timeout, configurable up to 30 seconds in `WorkerHostConfig`.

A standard report to a parent that is itself a background worker retains that parent's
original application input and declaration parameters. Its additional run is charged to the
original ancestor allocation, with the same grants, lifetime, and resource ceilings. No extra
nested-worker adapter is required. For **custom** report inputs, use
`Subagent.reportingToWorker(report, receivingDeclaration)` to convert the receiving declaration's
Parameters into its Agent input. A custom input cannot pretend to be the old parameters or
obtain a fresh budget. An attached destination has no independent continuing input lifetime;
delivery to it is refused. An attached scout returns through its waiting parent's tool result.

Report preparation decisions appear in authorized canonical worker history. Retained delivery
records expose pending, accepted, processed, parked, and refused states through the host-owned
`MessageDeliveryStore`. A child's completion and its report's processing remain separate facts.

## Update delivery guarantees

`Agent.make(name, { updates: schema, ... })` declares intentional intermediate output independently of
final output. Objects and tagged unions work, including transformed Schemas. The framework adds
one native `emit_update` Tool with `{ value: schema }` parameters. Its expected refusal is returned
as a typed tool result, so the Agent can continue toward completion. The name is reserved when
updates are declared; inherited tool grants still apply.

Application tool code can use `AgentUpdates.emit(agent, value, { idempotencyKey })` instead.
Declare `AgentUpdates.Emitter` as a dependency of that Tool. The key identifies one intended
update within the Run: equal retries reuse it, and changed payloads fail with `conflict`.
The emitter is available during an active tool batch and closes with its Scope.
Native calls derive their keys from the actual Run and Tool Call.

Observe the same definition at the top level:

<<< @/snippets/travel-planner/observe-updates.ts{ts twoslash}

`AgentUpdates.decode` and `observe` decode through the Agent's update Schema and preserve its
required decoding services. Runtime events contain encoded values; they never assert encoded
payloads into application types. Durable canonical history retains `AgentUpdateEmitted` records,
including the exact source definition versions. Authorized `Subagent.observe` readers can decode
those records and their updates with the same APIs.

With `reportToParent: true`, acceptance commits the finding and a frozen parent delivery envelope
before acknowledging it. The child continues without waiting for destination admission, parent
processing, or a user decision. The parent receives a `WorkerUpdate` as untrusted user-message
content, retaining its original application input for instructions and policy. Custom
`reportToParent` mappings keep their existing terminal-completion behavior; they are optional.

Each worker Thread orders update deliveries and completion reports across its Runs. A successor
waits until the predecessor has a destination Receipt or a conclusive refusal. A parked predecessor
without a Receipt blocks later delivery until recovery; a refusal remains observable instead of
silently discarding the finding. Waiting for a predecessor does not spend admission attempts.
Destination admission and processing remain separate states, and no external side effect is
promised to execute exactly once.

The runtime defaults to 32 updates and 16 KiB of total encoded update payload per Run. `RunOptions.updates`
configures these limits. Durable acceptance rechecks canonical counts after a restart. The delivery
store separately bounds update retention and pending work per worker (defaults: 256 retained and
32 pending). `maxRetainedUpdatesPerOwner` and `maxPendingUpdatesPerOwner` configure that partition;
updates cannot consume the ordinary capacity used for terminal reports. Capacity refusal happens
before a new update is accepted. A parent that is itself a worker has separate update input quotas:
`WorkerHostLimits.maxUpdateInputsPerWorker` defaults to 256 and
`maxPendingUpdateInputsPerWorker` defaults to 32. Temporary pending or active-worker limits retry
delivery; permanent retention or budget exhaustion refuses it. Parent admissions still obey
inherited budgets, grants, lifetime, and host limits.

Retained findings and pending delivery survive parent completion or abort, dropped wake hints,
restart, and eviction. Recovery inserts missing outbox rows from canonical envelopes and retries
the same logical delivery without rebuilding its payload. A lost acknowledgement can therefore
leave a retained finding even when the caller did not see success. Reuse its key when reconciling.
If ownership is lost before the native Tool result is recorded, the Tool call remains unresolved
under the ordinary recovery rules. Recovery delivers the retained update without replaying the
Tool; explicit cancellation can then settle the worker and deliver its separate aborted report.
Accepted updates remain distinct from the eventual completed, failed, or aborted outcome.
