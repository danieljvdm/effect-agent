---
title: Tools and Layers
description: Native Effect AI Tools under bounded, deterministic runtime scheduling.
---

# Tools and Layers

<StatusCallout status="available" phase="P1–P2" title="Native Tools, handler Layers, bounded scheduling, approval seams, and overrides exist today." />

Effect Agent does not define its own Tool abstraction. Define Tools and Toolkits with Effect AI;
the engine supplies Agent-specific scheduling, policy, and Conversation boundaries around their
native handlers.

## One Tool system

```ts
const Search = Tool.make("search", {
  parameters: SearchQuery,
  success: SearchResult,
  failure: SearchUnavailable,
  failureMode: "error",
  dependencies: [SearchIndex],
});

const Tools = Toolkit.make(Search);

const ToolsLive = Tools.toLayer({
  search: (query) => Effect.flatMap(SearchIndex, (_) => _.search(query)),
});
```

Effect AI owns parameter, success, and failure Schemas; approval; handler requirements; failure
mode; and preliminary results. A model-generated Tool Call is decoded through that same Tool.

## Batch execution

Before any handler in a Tool batch begins, the engine verifies the complete assistant response,
resolves every Tool name, decodes every parameter value, checks budgets, and obtains approval.

Then it:

1. acquires finite Effect `Semaphore` permits;
2. executes handlers as scoped child fibers;
3. exposes live progress in real completion order;
4. presents results to the next Model Turn in declaration order;
5. never exposes a partial Tool batch to the Model.

Parallel work stays fast; canonical history stays deterministic.

## Failure remains failure

The default `failureMode: "error"` keeps a typed Tool failure in the Effect error channel. The
runtime does not convert errors into plausible-looking model content.

Use `failureMode: "return"` only when the application deliberately wants a declared failure to
become a model-visible Tool result. Legitimate absence is usually a successful value such as
`Option.none` or an empty result collection.

## Scheduling overrides

Agent policy supplies the finite upper bound. A Run-level override can only make that bound
stricter:

```ts
const options = {
  scheduling: toRunSchedulingHook(
    { mode: "sequential" },
    (toolName) => toolName === "mutate_account",
  ),
};
```

Unbounded execution is never the default. Mutating Tools should opt into sequential behavior when
their effects are not independent.

## Approval

Effect AI's `needsApproval` marks the Tool. The capabilities package adapts the native approval
request to a typed Effect service with stable Run identity, normalized resource targets, bounded
preview, expiration, audit, and a deny/unresolved decision.

Approval occurs after parameters decode and before the handler starts. Model prose cannot grant
approval.

## What is not durable yet

<StatusCallout status="planned" phase="P5" title="Prepared calls, unknown outcomes, and durable Steps are target behavior.">

Today, a process crash loses an active ephemeral Tool Call. The future runtime will persist a
prepared boundary before ordinary external effects and refuse to replay an ambiguous unresolved
call. See [Durable execution](../future/durable-execution).

</StatusCallout>
