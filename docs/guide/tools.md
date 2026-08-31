---
title: Tools & layers
description: Define Effect AI tools and run them with bounded, deterministic scheduling.
---

# Tools & layers

Define tools and toolkits with Effect AI. Effect Agent runs their native handlers under its
scheduling, policy, and thread rules.

## Define tools once {#one-tool-system}

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

The tool declaration owns parameter, success, and failure schemas, approval, dependencies,
failure mode, and preliminary results. The runtime decodes every model-generated tool call through
that declaration.

## Run batches deterministically {#batch-execution}

The runtime validates the complete model response before starting any handler. It resolves tool
names, decodes parameters, checks budgets, and obtains approvals for the whole batch.

It then runs scoped child fibers behind a finite Effect `Semaphore`. Live progress follows actual
completion order. Canonical history and the next model turn use declaration order. The model never
sees a partial batch.

## Keep tool failures typed {#failure-remains-failure}

The default `failureMode: "error"` keeps a declared tool failure in the Effect error channel. Use
`failureMode: "return"` when the model should receive that declared failure as a tool result.

Represent an expected empty result as success with `Option.none` or an empty collection.

## Reduce concurrency per run {#scheduling-overrides}

The agent policy sets the maximum concurrency. A run override can only reduce it.

```ts
const options = {
  scheduling: toRunSchedulingHook(
    { mode: "sequential" },
    (toolName) => toolName === "mutate_account",
  ),
};
```

Use sequential execution for mutating tools whose effects depend on order. Every other batch still
has a finite concurrency limit.

## Approve before execution {#approval}

Effect AI's `needsApproval` marks a tool for approval. The capabilities package turns its native
request into a typed Effect service with stable run identity, normalized resource targets, a
bounded preview, expiration, audit, and a deny or unresolved decision.

Approval occurs after parameter decoding and before the handler starts. The model cannot approve
a tool call.

## Authorize tool calls

Use `RunToolAuthorization` to decide whether a model-declared application tool call may execute.
This policy permits only the `search` tool:

```ts twoslash
import { RunToolAuthorization } from "@effect-agent/engine";
import { Effect, Layer } from "effect";

export const searchOnly = RunToolAuthorization.of({
  authorize: ({ call }) =>
    Effect.succeed(
      call.toolName === "search"
        ? { _tag: "allowed" }
        : { _tag: "denied", reason: "Only search is permitted." },
    ),
});

export const SearchOnlyLive = Layer.succeed(RunToolAuthorization, searchOnly);
```

Pass `searchOnly` as the `toolAuthorization` option to `AgentRuntime.run`, `stream`, or `start`.
For durable execution, install `SearchOnlyLive` in the
[Node host](../platforms/node#configure-runtime-services),
[Cloudflare application](../platforms/cloudflare#configure-runtime-services), or
[custom runtime](./run-agents#assemble-a-custom-durable-runtime).

The policy receives run identity, encoded input, and the proposed call's name, ID, parameters,
and execution classification. Decode unknown input and parameters with the application's schemas
when checking resource access. Keep denial reasons safe to log.

The runtime checks each executable model-declared call after approval and before any handler in
the batch starts. A denial fails with `AgentToolAuthorizationDenied`. Recovery checks calls that still need
execution; it reuses recorded results without executing or authorizing them again.

Omitting the hook allows calls without this additional host check. Durable hosts use
`RunToolAuthorization.allowAll` by default. Install a policy before granting tools access to
protected resources. Authenticate callers and authorize runtime operations as described in
[operations](./operations#authorization-and-isolation).

This hook does not authorize provider-executed calls or [Code Mode](./code-mode)'s inner programmatic calls.
The Code Mode broker restricts inner calls to its allowlisted toolkit; enforce resource access
inside those handlers.

## Handle uncertain external effects {#durability}

Process loss ends an active tool call in an ephemeral run. Durable hosts persist a prepared
boundary before ordinary external effects. If the runtime cannot determine whether the effect
happened, it records an Unknown Outcome and waits for an explicit resolution. It never replays the
call automatically. See [Persistence & durability](../concepts/durability).

## Delegate to an agent

`Subagent.define` exposes a child agent as a tool with explicit input and result projections.
The [Subagents guide](./subagents) covers definition, model binding, budgets, authority, failure
handling, and durable child recovery.

## Browse web pages

Use `WebCapture.make`, `WebCapture.makeScrape`, or `WebCapture.makeExtract` to expose authorized page
capture as Effect AI Tools. The [browser guide](./browser) shows how to supply capture and crawl
adapters, take screenshots, and open scoped interactive passes, including Live View and handoff.

### Capture and crawl

Choose a Worker binding or a Node-safe REST adapter in [capture and crawl](./browser#capture-and-crawl).
Structured extraction requires explicit Workers AI authorization and accounting.

### Interact with a browser

The [interactive browser walkthrough](./browser#interact-with-a-browser) covers Layer setup,
network policies, bounded actions, and session cleanup.

## Execute code

[Code Mode](./code-mode) lets an agent write bounded JavaScript that calls an allowlisted set of
read-only Tools through an isolated executor. [Sandbox execution](./sandbox) covers structured
process requests and the trusted local adapter.
