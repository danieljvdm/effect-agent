---
title: Tools & layers
description: Define Effect AI tools and run them with bounded, deterministic scheduling.
---

# Tools & layers

Define tools and toolkits with Effect AI. Effect Agent runs their native handlers under its
scheduling, policy, and conversation rules.

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

This hook does not authorize provider-executed calls or Code Mode's inner programmatic calls.
The Code Mode broker restricts inner calls to its allowlisted toolkit; enforce resource access
inside those handlers.

## Handle uncertain external effects {#durability}

Process loss ends an active tool call in an ephemeral run. Durable hosts persist a prepared
boundary before ordinary external effects. If the runtime cannot determine whether the effect
happened, it records an Unknown Outcome and waits for an explicit resolution. It never replays the
call automatically. See [Persistence & durability](../concepts/durability).

## Delegate to an agent

Use `Subagent.define` from `@effect-agent/capabilities` to expose a child agent as a tool:

1. Supply the child definition as `target`, schemas for delegation parameters, success and failure,
   and a finite `SubagentPolicy`.
2. Use `prepareInput` to select what the child receives and `projectResult` to select what returns
   to the parent. Both return Effects with the declared failure type. The child transcript stays
   private unless the result projection exposes it.
3. Add the returned `delegation.tool` to the parent's toolkit. Provide
   `SubagentRuntime.layer(delegation, childBinding, { mapChildFailure })` with an explicit
   `Agent.withModel` binding. Map every expected child failure to the declared failure schema.

See the [Travel Planner delegation example](https://github.com/danieljvdm/effect-agent/blob/main/packages/testing/src/fixtures/travel-planner/subagents.ts)
for a complete definition, projections, handlers, and parent agent.

The default `failureMode: "error"` fails the parent tool batch on an expected child failure.
Use `"return"` to let the parent model receive and act on that failure. Suspension and durability
failures remain in the error channel. Surface `projectResult`'s `budgetExhausted` flag when the
parent needs to distinguish partial output. See [delegation budgets](../concepts/budgets#delegation-budgets).

The default grant permits the target's declared tools at depth one. A grant that excludes a target
tool is rejected; it does not remove that tool from the child. Define a narrower child toolkit
when needed. Nested delegation, handoff, and detached children are unsupported.
`needsApproval` approves establishing this child only. It never authorizes the child's actions,
siblings, or retries. Apply [current tool and resource policy](./operations#tools-delegation-and-external-resources)
to each action.

Ephemeral children share the parent's Scope. Durable children require fixed child definition
digests in the handler layer's `durable` option and matching host registrations. They run in
separate conversations and survive lost attempts. See [child recovery and abort](../concepts/durability#attached-subagents).

## Browse web pages

Use the sandbox `PageCapture`, `PageScreenshot`, and `PageCrawl` services for capture and crawl tasks.
`WebCapture.make` and `WebCapture.makeExtract` from `@effect-agent/capabilities` expose `PageCapture`
as tools. Install the [Cloudflare browser adapter](../reference/packages#effect-agent-platform-cloudflare)
for the service you need; REST capture and crawl also run on Node.

### Capture and crawl

REST adapters require account credentials and `HttpClient`. REST crawl bounds same-host Markdown
collection and polling, and cleans up its job within Scope. Quick Actions use a browser binding;
structured extraction also requires explicit Workers AI authorization and accounting.

Quick Actions require compatibility date `2026-03-24` or later.
Use remote mode for local `wrangler dev`; Quick Actions have no local implementation.

### Interact with a browser

An interactive browser handle owns one scoped browser pass. It supports navigation, bounded
reads, clicks, fills, screenshots, scrolling, and early closure. Handles cannot be persisted or
transferred.

| Network policy | Boundary                                                                      |
| -------------- | ----------------------------------------------------------------------------- |
| `ExactHosts`   | HTTPS page-request host checks, not private-network isolation                 |
| `PublicWeb`    | Requires connection-time public-network enforcement                           |
| `Unrestricted` | Allows credential-free HTTP/HTTPS without host or private-network containment |

Cloudflare rejects `PublicWeb` before acquisition with `InteractiveBrowserUnsupportedError`.
Use host isolation for untrusted pages and viewers.

The interactive adapter needs `BrowserRunInteractiveBinding` and
`BrowserRunSessionLifecycle.layer({ accountId, apiToken })`, backed by `HttpClient`
and a Browser Rendering Write token. Browser sessions last for one scoped pass.

`browserRunInteractiveLayer` supplies browser actions. `browserRunInteractiveHostLayer` adds
viewport resizing, Live View, handoff, and cleanup by session identity.
The host must authorize viewer access, resizing, and handoff.

Click and fill require exactly one matching element. `isBrowserRunUndispatchedActionError`
identifies selector failures before dispatch; callers can correct those selectors.
Other action failures invalidate the handle. Never automatically retry a mutation whose outcome
is unknown. Interruption cannot reliably cancel an action already sent to Puppeteer.

`readText().text` contains JSON with page text, selector counts, and at most 64 controls.
Control diagnostics omit field values and HTML. Results, including PNG screenshots, obey the
pass byte limit. Logs omit URLs, selectors, labels, field values, credentials, and provider errors.

Set the initial `viewport` on `BrowserRunInteractiveBinding.layer` or call
`session.resizeViewport`. Width and height accept integers in `1..2048`;
`deviceScaleFactor` accepts `1..2`, defaults to `1`, and must satisfy
`max(width, height) * deviceScaleFactor <= 2048`. Mobile, touch, and orientation options are
unsupported. Resizing consumes no agent action but remains subject to the pass deadline and lock.

Session closure waits up to ten seconds to confirm whole-browser termination or exact-session absence.
A pending close or transport/authentication failure is not proof of cleanup.
`BrowserRunCleanupError` reports a sanitized reason. Correct authorization or configuration
failures before retrying.

See the [interactive browser API comments](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/src/interactive-browser.ts)
for action timing and lifecycle details. Browser passes do not provide durable execution or reconnection.
