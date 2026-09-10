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

## Discover tools progressively {#progressive-discovery}

A large registered catalogue can contain hundreds of tools even when a request needs only two.
`ToolDiscovery.make` adds an ordinary `discover_tools` tool. Start with common tools pinned, then
expose matching schemas after discovery. All tools retain their native Effect AI definitions and
handlers; omitting selection configuration and discovery preserves eager exposure.

```ts twoslash
import { Agent, ToolDiscovery, ToolExposure } from "effect-agent";
import { Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

const GetRecord = Tool.make("get_record", {
  description: "Read one record by its ID.",
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.String,
})
  .annotate(ToolExposure.ToolNamespace, "records")
  .annotate(ToolExposure.PinnedTool, true);

const SearchRecords = Tool.make("search_records", {
  description: "Search records by title.",
  parameters: Schema.Struct({ title: Schema.String }),
  success: Schema.Array(Schema.String),
}).annotate(ToolExposure.ToolNamespace, "records");

const discovery = ToolDiscovery.make({
  maxResults: 8,
  maxResultBytes: 32_768,
  namespaceDescriptions: { records: "Record lookup and title search" },
});

export const agent = Agent.make("record-assistant", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Use discover_tools to find missing tools. Return the answer as a JSON string.",
  toolkit: Toolkit.make(GetRecord, SearchRecords, discovery.tool),
  toolExposure: { initialToolNames: [], maxTools: 16, maxSchemaBytes: 65_536 },
});

export const Handlers = Layer.merge(
  Toolkit.make(GetRecord, SearchRecords).toLayer({
    get_record: ({ id }) => Effect.succeed(`Record ${id}`),
    search_records: ({ title }) => Effect.succeed([`Matching record: ${title}`]),
  }),
  discovery.handlers,
);
```

The first request exposes `get_record` and `discover_tools`. A call such as
`discover_tools({ query: "search", namespace: "records" })` returns metadata and encoded parameter
and success schemas; `search_records` becomes callable on the next turn. Provide handlers for the
full registered toolkit as before: hidden schemas do not remove requirements from `R`.

Default search matches every whitespace-separated query term, ignoring case, against names,
descriptions, methods and namespace hints, with deterministic catalogue-ID ordering. Namespaces
come from `ToolNamespace` annotations or Code Mode's allowlist, never from parsing a tool name.
Namespace hints appear only on eligible matches. Queries are bounded to 512 characters and exact
namespace filters to 128. The default returns at most eight matches and 32 KiB of complete encoded
JSON; limits can rise to 64 matches and 256 KiB. Oversized documentation fails with
`ToolDiscoveryError` rather than returning broken schemas. Provider-defined tools are not ordinary
callable schemas and cannot be documented by this capability.

### Supply application search

The optional Effect callback receives only the eligible catalogue, already filtered by the exact
namespace. Return ranked `Descriptor.id` values. Every ID is validated before limiting results;
unknown or duplicate IDs fail closed. Native tools and each Code Mode alias have separate IDs.

```ts twoslash
import * as ToolDiscovery from "effect-agent/ToolDiscovery";
import { Context, Effect, Schema } from "effect";

class SearchUnavailable extends Schema.TaggedError<SearchUnavailable>()("SearchUnavailable", {
  message: Schema.String,
}) {}

class SearchIndex extends Context.Service<
  SearchIndex,
  {
    readonly rank: (
      query: string,
      catalogue: ReadonlyArray<ToolDiscovery.Descriptor>,
    ) => Effect.Effect<ReadonlyArray<string>, SearchUnavailable>;
  }
>()("SearchIndex") {}

export const discovery = ToolDiscovery.make({
  failure: SearchUnavailable,
  search: (request, catalogue) =>
    Effect.flatMap(SearchIndex, (index) => index.rank(request.query, catalogue)),
});
```

Provide `SearchIndex` when building `discovery.handlers`. Its requirements remain in the Layer's
`R`, and declared failures remain in the tool's `E` alongside `ToolDiscoveryError`. Search runs in
a fresh Scope per invocation; failure, defect, timeout and interruption close acquired resources.

An existing ordinary readonly search tool can use the same contract: annotate it with
`ToolExposure.DiscoveryTool` and return a decoded `toolNames` array containing registered native
names. The runtime validates that selection before recording it. Discovery tools must use the
`ToolExecutionClass` annotation from `effect-agent/DurableStep` with value `"readonly"`;
uncertain and orchestration tools have different durable settlement paths and are refused.

### Select without search {#tool-selection}

Host context and workflow state can use the same mechanism directly:

```ts twoslash
import { RunToolVisibility, Selection } from "effect-agent/ToolExposure";
import type { RunOptions } from "effect-agent/RunOptions";
import { Effect, Layer } from "effect";

export const options: RunOptions = {
  toolSelection: Selection.make({ toolNames: ["search_records"] }),
};

export const VisibilityLive = Layer.succeed(RunToolVisibility, {
  visible: ({ toolNames }) => Effect.succeed(toolNames.filter((name) => name !== "delete_record")),
});
```

Pass these options to `AgentRuntime.run`, `stream`, or `start`. A context preparation hook may
return `toolSelection` beside its `prompt` to replace the set before a new model request. Provide
`VisibilityLive` around an ephemeral run or when constructing a durable runtime. The optional
`RunToolVisibility` service defaults to no filter; durable hosts capture that choice, including
absence, so worker callers cannot replace it. Resolve policy dependencies and setup failures in
the host Layer, where their types remain visible. The policy operation returns eligible names;
an empty list denies all tools.

Visibility controls eligibility. Exposure controls which eligible native schemas the model sees.
Authorization, approval, budgets and resource checks still decide whether an action may execute.
Visibility and inherited grants are applied before custom search receives any names or docs.
Guessed native calls outside the original request exposure fail before handlers start. Code Mode
also filters its sandbox method surface and denies hidden inner calls; resource authorization
still belongs inside those handlers.

### Selection lifetime and recovery

Selections last for one run and **replace** the non-pinned set; they do not accumulate. An empty
successful selection clears it. If a batch contains several successful selections, the last in
declaration order wins, regardless of completion order. No selection takes effect midway through
a batch. Failed results retain the previous selection; ordinary tool error behavior still applies.

During working turns, eligible tools with explicit `PinnedTool` annotations stay exposed across
selections. Host visibility and inherited grants may hide these common tools without failing the
run. Discovery, required completion and context rollover tools are mandatory: excluding one causes
a typed refusal. Exposed pins count toward the limits and never override eligibility. Optional
completion is available in the final answer turn only when eligible; otherwise the model finishes
with text. That turn may expose only the completion tool. The default exposure limits are 64 tools and 256 KiB of aggregate
UTF-8 JSON declarations (names, descriptions and parameter schemas, including the current model's
schema transformation). Exceeding a limit fails with `ModelProtocolError`; there is no silent
eviction beyond replacement.

The runtime records the actual request exposure with each canonical model response and each
successful selection with its tool settlement, before result truncation. `ToolCallSucceeded`
also carries `toolSelection`. Durable recovery, compaction and checkpoints restore this metadata
without searching again for a committed result. A crash before a result is committed follows the
ordinary readonly recovery contract. Resumed calls retain their original exposure and recheck
current eligibility before unfinished handlers run; already settled siblings remain canonical.
Custom durable hooks must stage request snapshots through `noteToolExposure`. Version custom
search semantics in your registration definitions as with other handler changes.

This provider-neutral API changes the native toolkit sent on subsequent calls. It does not use
provider-specific deferred-tool references or promise a latency win: extra discovery rounds and
provider prompt caching can outweigh smaller schemas. Measure common, uncommon and composed
tasks against eager exposure before claiming a performance improvement.

## Run batches deterministically {#batch-execution}

The runtime validates the complete model response before starting any handler. It resolves tool
names, decodes parameters, checks budgets, and obtains approvals for the whole batch.

It bounds both active call streams and handler execution by the resolved concurrency, using scoped
child fibers and a finite Effect `Semaphore`. Pending calls do not allocate waiting stream fibers.
Live progress follows actual completion order. Canonical history and the next model turn use
declaration order. The model never sees a partial batch.

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

Durable hosts provide `RunToolScheduling` from `@effect-agent/engine/RunOptions` when constructing
the runtime. Its `toolRequiresSequential` predicate inserts barriers around those tools while
independent neighboring calls run concurrently. The runtime captures this host choice across
replacement attempts; a worker's ambient reference cannot replace it. Ephemeral runs use the same
reference unless `RunOptions.scheduling` is explicitly supplied.

## Approve before execution {#approval}

Effect AI's `needsApproval` marks a tool for approval. The capabilities package turns its native
request into a typed Effect service with stable run identity, normalized resource targets, a
bounded preview, expiration, audit, and a deny or unresolved decision.

Approval occurs after parameter decoding and before the handler starts. The model cannot approve
a tool call.

## Authorize tool calls

Use `RunToolAuthorization` to decide whether a native or programmatic application tool call may execute.
Code Mode invokes the same policy for each inner call before reserving its budget or starting its
handler. The request includes `programmatic.parentToolCallId` and `programmatic.sequenceIndex`;
allowing the outer execution Tool does not grant permission to its inner Tools.
This policy permits only the `search` tool:

```ts twoslash
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
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

Provide `SearchOnlyLive` to `AgentRuntime.run`, `stream`, or `start`. A per-run
`toolAuthorization` option overrides the provided policy and retains its own typed failures
and service requirements. For durable execution, install `SearchOnlyLive` in the
[Node host](../platforms/node#configure-runtime-services),
[Cloudflare application](../platforms/cloudflare#configure-runtime-services), or
[custom runtime](./run-agents#assemble-a-custom-durable-runtime).

The policy receives run identity, encoded input, and the proposed call's name, ID, parameters,
and execution classification. Decode unknown input and parameters with the application's schemas
when checking resource access. Keep denial reasons safe to log.

The runtime checks each executable model-declared call after approval and before any handler in
the batch starts. A denial fails with `AgentToolAuthorizationDenied`. Recovery checks calls that still need
execution; it reuses recorded results without executing or authorizing them again.

Omitting both the service and per-run hook allows calls without this additional host check. Durable hosts use
`RunToolAuthorization.allowAll` by default. Install a policy before granting tools access to
protected resources. Authenticate callers and authorize runtime operations as described in
[operations](./operations#authorization-and-isolation).

This hook does not authorize provider-executed calls. A denied [Code Mode](./code-mode) inner call
returns a catchable `ProgrammaticToolAuthorizationDenied` outcome without consuming execution budget;
other independent calls may already have completed. The broker also restricts calls to the eligible
allowlist. Keep resource access checks inside handlers as appropriate for the application.

## Handle uncertain external effects {#durability}

Process loss ends an active tool call in an ephemeral run. Durable hosts persist a prepared
boundary before ordinary external effects. If the runtime cannot determine whether the effect
happened, it records an Unknown Outcome and waits for an explicit resolution. It never replays the
call automatically. See [Persistence & durability](../concepts/durability).

## Connect MCP servers {#mcp}

`McpClient.layer` provides `McpConnector` over real transports. `McpHttpTransport.make` speaks
Streamable HTTP and needs `HttpClient`; `McpStdioTransport.make` runs a local server process and
needs `ChildProcessSpawner`, which `NodeServices.layer` supplies on Node.js. Both requirements
stay in the Layer's `R`.

```ts
import * as McpClient from "@effect-agent/capabilities/McpClient";
import { connectMcp, McpConnectionRequest } from "@effect-agent/capabilities/Mcp";
import { McpHttpTransport } from "@effect-agent/capabilities/McpClient";
import { FetchHttpClient } from "effect/unstable/http";
import { Effect, Layer } from "effect";

const McpLive = McpClient.layer([
  McpHttpTransport.make({ serverId: "docs", url: "https://mcp.example.com/mcp" }),
]).pipe(Layer.provide(FetchHttpClient.layer));

const program = Effect.gen(function* () {
  const connection = yield* connectMcp(
    McpConnectionRequest.make({
      serverId: "docs",
      maxToolCount: 16,
      maxToolDescriptionBytes: 1_024,
      maxDiscoveryBytes: 65_536,
      connectTimeoutMillis: 5_000,
    }),
  );
  // Merge `connection.toolkit` into the agent's toolkit and provide
  // `connection.handlers` with the application's other tool handlers.
  return connection;
});
```

`connectMcp` negotiates a protocol revision, lists tools within the request bounds, and returns
dynamic Effect AI tools whose handlers forward `tools/call`. Provide the returned `handlers` Layer
wherever the agent runs. The connection lives in the caller's Scope; closing it ends the session
or stops the process. Server-initiated requests such as sampling and elicitation are declined, and
event streams are not resumed after a disconnect. A stdio `env` is added to the inherited
environment.

Remote tools stay ordinary tools: they are `uncertain` by default, need approval and authorization
like any other tool, and receive an Unknown Outcome after process loss. Set `trustToolAnnotations`
on a transport to let the server's `readOnlyHint` and `idempotentHint` choose the execution class.
A tool with `isError` fails the call with `McpToolCallFailed`. Set `expectedToolkitSchemaDigest`
on the request to reject a server whose tools changed since the agent was authored.

Remote servers are untrusted input. Bound their tool descriptions and results with the request
limits and `toolResultBounds`, supply credentials through the transport headers or `HttpClient`,
and keep local server commands under application control.

## Delegate to an agent

`Subagent.make` exposes a child agent as a tool with explicit input and result projections.
The [Subagents guide](./subagents) covers definition, model binding, budgets, authority, failure
handling, and durable child recovery.

## Search the web {#web-search}

`WebSearch.tool` is an ordinary Effect AI tool with a stable `{ query }` input and a result
containing `text`, `sources`, and search-model token `usage`. Its handler uses a separately
supplied LanguageModel and a native hosted search tool. The calling agent can use a different
model or provider. Include `WebSearch.tool` in its toolkit, then provide this handler Layer:

```ts twoslash
import * as WebSearch from "effect-agent/WebSearch";
import * as Gateway from "@effect-agent/platform-cloudflare/CloudflareAiGateway";
import { OpenAiClient, OpenAiLanguageModel, OpenAiTool } from "@effect/ai-openai";
import { Layer, Redacted } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

const gateway = {
  accountId: "your-account",
  gatewayId: "your-gateway",
  apiToken: Redacted.make("your-cloudflare-token"),
};

const SearchLive = WebSearch.layer({
  tool: OpenAiTool.WebSearch({ search_context_size: "medium" }),
  timeoutMillis: 30_000,
  maxOutputBytes: 32 * 1024,
}).pipe(
  Layer.provide(
    OpenAiLanguageModel.model("openai/gpt-4.1-mini", {
      max_output_tokens: 2_048,
      store: false,
    }),
  ),
  Gateway.provide(OpenAiClient.layer, { ...gateway, protocol: "responses" }),
  Layer.provide(FetchHttpClient.layer),
);
```

Load real gateway credentials from your host configuration or secret store. For Anthropic,
select `AnthropicTool.WebSearch_20250305({ maxUses: 3 })`, provide an `AnthropicLanguageModel`
Layer, and use `Gateway.provide(AnthropicClient.layer, { ...gateway, provider: "anthropic" })`.
Direct provider clients work too. The compiling
[provider examples](https://github.com/danieljvdm/effect-agent/tree/main/examples/providers#cloudflare-ai-gateway) show both backends.

Each invocation makes one model request, without handler retries. The host fixes the backend,
native search options, deadline (1–300,000 ms), and encoded result limit (1–1,048,576 bytes).
Queries are bounded to 8,192 characters and results to 64 source citations. A missing completed
search, provider error, invalid result, or exceeded limit returns `WebSearchFailure`. Defects
and interruption propagate; timeout interrupts the in-flight request. Search results and source
URLs remain untrusted, and a citation grants no permission to fetch it. No provider payload or
credential is included in the tool result. Model-call telemetry remains upstream Effect AI's;
the wrapper adds the `WebSearch.search` span without logging queries or responses itself.

Search is separately billed. Returned token counts use `null` when unavailable and are **not**
added to the parent Run's model usage or spending limit. Configure provider output limits and
host billing controls. For search within the primary model call and its normal Run accounting,
include the native `OpenAiTool.WebSearch` or `AnthropicTool.WebSearch_20250305` directly in the
agent's toolkit instead. Both work with [Gateway client configuration](../platforms/cloudflare#ai-gateway).
The ordinary WebSearch tool remains uncertain for recovery: an unresolved call is not replayed
automatically after ownership loss.

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
