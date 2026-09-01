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
const Research = Subagent.define("delegate_research", {
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

In `delegation.ts`, `Subagent.define` connects what the parent requests, what the child receives,
and what the parent gets back. Delegation tool names must start with `delegate_`.

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

To require approval before establishing the child, add this to `Subagent.define`:

```diff
 failureMode: "error",
+needsApproval: true,
```

Supply an [approval handler](./tools#approval) for the request. This approves starting the child;
its individual actions still need their own authorization. A narrower grant rejects a child whose
toolkit exceeds it. Define a smaller child toolkit to reduce authority.

## Keep the child attached {#keep-children-attached}

```text
parent invokes child → reserve allowance → run child → project result → settle parent tool
```

Ephemeral children share the parent's Scope. Durable children have separate threads and
attempts; a waiting parent releases its worker permit.

For durable execution, the handler Layer also needs the registered child's exact digests:

```diff
SubagentRuntime.layer(Research, ResearchModel, {
   mapChildFailure: (error) => ResearchFailed.make({ reason: error._tag }),
+  durable: { targetDigests },
 })
```

Obtain `targetDigests` from the child's matching host registration. Use the
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

Nested delegation, handoff, and detached children are currently unsupported.
