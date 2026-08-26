# Capability specification

Status: Draft

This document specifies the capabilities surrounding the core agent interpreter. A
capability is an Effect service with an explicit contract, error channel, resource
lifetime, and events. Capabilities may be omitted from a runtime. The engine
must detect absence explicitly; it must not silently substitute weaker behavior.

## 1. Capability matrix

| Capability                 |      Status | Required for ephemeral core |     Required for DN/DC assembly |
| -------------------------- | ----------: | --------------------------: | ------------------------------: |
| Tools and toolkits         | Implemented |                         Yes |                             Yes |
| Sessions and conversations | Implemented |                          No |                             Yes |
| Steering and follow-up     | Implemented |                          No |                              No |
| Approval                   | Implemented |                          No |          For configured actions |
| Compaction                 | Implemented |                          No | Yes for unbounded conversations |
| Skills                     |    Deferred |                          No |                              No |
| MCP client                 | Implemented |                          No |                              No |
| Sandbox                    | Implemented |                          No |     For untrusted commands/code |
| Code Mode                  | Implemented |                          No |                              No |
| Page capture               | Implemented |                          No |                              No |
| Page crawl                 | Implemented |                          No |                              No |
| Interactive browser        | Implemented |                          No |                              No |
| Subagents                  | Implemented |                          No |                              No |
| Persistent agent state     | Implemented |                          No |                              No |
| Durable steps              | Implemented |                          No |                              No |

"Required for DN/DC assembly" means the DN or DC host assembly must supply the
service when the related behavior is enabled. It does not mean every deployment
enables every capability.

## 2. Tool execution

The engine owns durable Tool scheduling. Effect AI emits Tool Call Response parts,
and the engine invokes the original Effect AI Toolkit handlers.

Every model-declared Tool Call passes through this pipeline:

1. Reduce Effect AI Response parts into a complete Tool Call.
2. Resolve the Tool in the Effect AI Toolkit.
3. Let Effect AI decode input with the Tool's Effect Schema.
4. Evaluate Effect AI approval policy.
5. Re-evaluate optional host authorization over the exact Run/Turn/input and call descriptor.
6. Commit durable preparation for non-readonly calls.
7. Acquire the required Effect `Semaphore` permits.
8. Execute the Effect AI Toolkit handler within a scoped child Fiber; release permits on every
   exit.
9. Encode success or a declared tool-domain failure.
10. Redact and bound the result for model context and persisted events.
11. Commit the result in deterministic order.

The framework distinguishes:

- `ToolSuccess`: a successful declared output;
- `ToolRejected`: policy or approval denied execution;
- `ToolInvalidInput`: schema decoding failed;
- `ToolFailure`: the handler failed in the typed Effect error channel;
- `ToolDefect`: the handler died, was interrupted unexpectedly, or violated its
  output schema;
- `ToolUnknownOutcome`: an external side effect may have happened but no canonical
  result was recorded.

The tool call ID is stable across logging, approval, host authorization, execution, and recovery.
The host, not the framework, classifies which Tools mutate and whether the originating Run input
still carries current authority. A recovered durable batch passes through the same authorization
step again before any Handler.

## 3. Sessions and conversations

A `SessionStore` manages named session records. A session may point to one or more
conversations but is not itself the canonical conversation history.

```ts
export interface SessionStore {
  readonly create: (input: SessionCreate) => Effect.Effect<Session, SessionStoreError>;

  readonly get: (id: SessionId) => Effect.Effect<Option.Option<Session>, SessionStoreError>;

  readonly update: (
    id: SessionId,
    patch: SessionPatch,
    expectedRevision: Revision,
  ) => Effect.Effect<Session, SessionConflict | SessionStoreError>;
}
```

Session metadata is schema-versioned and must use optimistic concurrency. Mutable
session metadata must never be used as the sole source of truth for accepted work.

## 4. Steering and follow-up

Steering adds input before the next model request at a defined safe point. Follow-up
adds input only when the Agent would otherwise stop.

The initial contract is:

- steering is accepted only while a run is active;
- the current model response and active Tool batch finish normally;
- accepted steering becomes canonical before the next model request;
- follow-ups enter the same admission path as ordinary submissions;
- per-conversation FIFO ordering applies;
- steering and follow-up return receipts when durable admission is enabled.

Steering is not an out-of-band mutable string. It is observable input with an ID,
timestamp, author, and admission result.

## 5. Approval

Effect AI Tool `needsApproval` defines whether approval is needed. The runtime's
approval resolver is a first-class Effect service rather than an untracked callback.

```ts
export interface Approval {
  readonly request: (request: ApprovalRequest) => Effect.Effect<ApprovalDecision, ApprovalError>;
}
```

An approval request includes:

- run, conversation, tool, and tool-call identity;
- a human-readable action summary;
- normalized resource targets;
- risk classification;
- redacted input preview;
- expiration time;
- whether denial is terminal or recoverable.

An approval provider may be interactive, policy-based, remote, or test-controlled.
Approval decisions are canonical audit events. A timeout is a denial unless the
configured policy explicitly says otherwise.

On the durable runtime, an unresolved approval is a **durable suspension**: the
canonical approval request record is the safe boundary (durability §8), ownership ends, and the
lane consumes no worker permit. Durable suspension itself has no implicit timeout. Suspension
is operational ledger state with no canonical "suspended" record; the resuming
Attempt appends the canonical decision before honoring it and replays the declared Tool batch
without re-invoking the model. An immediate policy decision commits atomically with its request.
Denial remains terminal per the ephemeral policy default.

## 6. Compaction

Compaction converts a prefix of conversation history into a smaller context
representation. It does not rewrite or delete the canonical log. Two forms exist
(RUN-026).

**Engine-native compaction** is the operational path ([runtime §9](./runtime.md)): when the
estimated next model-call context exceeds `AgentPolicy.contextTokenLimit`, the engine prunes old
Tool results and, if needed, summarizes through one metered model call at the pre-Turn seam. On
the DN and DC assemblies (their shared session coordinator) each compaction appends a canonical
`CompactionCreated { runId, turn, kind: "clear-tool-results" | "summarize", coversThrough,
summary? }` record inside the same epoch-fenced log it covers; the run-journal projection folds
covered records into the summary or the cleared-result marker, and an invalid range is ignored
fail-safe. No source digest is carried: the record is appended by the fenced owner into the log
it covers, and re-verifying a digest would re-read the whole covered range on every wake.

**Host-supplied compaction artifacts** cross a trust boundary and stay digest-bound. The
`ContextCompactor` capability returns a versioned `CompactionArtifact` with an explicit inclusive
source range:

```ts
interface CompactionResult {
  readonly coversFrom: ConversationSequence;
  readonly coversThrough: ConversationSequence;
  readonly summary: ModelContextMessage;
  readonly retainedFacts: ReadonlyArray<RetainedFact>;
  readonly tokenEstimate: number;
  readonly sourceDigest: Digest;
  readonly compactorVersion: string;
}
```

`contextCompactorRunContextLayer` adapts that capability to the generic engine
`RunContextPreparation` service. The adapter captures `ContextCompactor` and `Crypto.Crypto` at
Layer acquisition, projects the native Effect AI prompt to a deterministic `ConversationSnapshot`
(prompt index is source sequence and timestamps are fixed), verifies the returned artifact with
`applyCompaction`, and substitutes only the covered messages in the model-visible prompt. Native
uncovered messages, parts, and provider options pass through unchanged. A prose summary may use
the system, user, or assistant role; a tool-role prose summary fails typed because it cannot form a
valid native `ToolMessage` without inventing a Tool result. Coverage that would split a native Tool
call/result or approval request/response correlation also fails typed.

The adapter maps expected capability/schema/digest failures to `RunContextPreparationError` with
the original failure as its live `cause`. Defects remain defects. Durable settlement stores only
the existing bounded `{ errorTag, message }` projection, never the cause object.

Requirements:

- input coverage is an explicit sequence range;
- for host-supplied artifacts, the source digest binds the result to that exact range;
- summaries are versioned;
- failed compaction cannot corrupt or advance conversation state;
- replay may reuse a valid compaction artifact or committed compaction record;
- operators may rebuild compactions from the canonical log;
- secrets excluded by policy must not enter the summary.

Compaction quality is evaluated separately from persistence correctness.

## 7. Skills

A skill is a versioned package of instructions, schemas, assets, and optional
capability requirements. Loading a skill produces immutable agent context for a run
or turn.

Each skill manifest declares:

- stable ID and semantic version;
- human-readable description;
- instruction entry point;
- required tools and services;
- optional assets;
- compatibility range for the framework;
- integrity digest;
- trust classification.

Skill discovery and skill activation are distinct. Discovery may list metadata;
activation reads content and must pass authorization. Loaded skill versions are
recorded in run metadata for reproducibility.

The first implementation supports local, trusted, read-only skill directories.
Remote installation and executable skill hooks are out of scope.

## 8. MCP

Use Effect AI's MCP protocol/schema/server primitives where they fit. MCP remains an
external capability, not the framework's internal Tool abstraction.

An MCP client adapter:

- connects within a Scope;
- discovers server capabilities;
- maps MCP tools to Effect AI dynamic Tools;
- binds advertised input schemas and object-shaped output schemas to the native Effect AI Toolkit;
- maps content and structured data without losing provider detail;
- validates inputs through Effect AI Tool Schema boundaries;
- enforces timeouts, size limits, and authorization;
- records server identity and advertised version;
- turns connection loss into typed failures;
- never treats remote tool execution as exactly once unless the remote protocol and
  tool explicitly support idempotency.

MCP resources and prompts may later map to separate framework capabilities. They
must not be disguised as tools solely to reduce interface count.

## 9. Sandbox

The sandbox capability executes commands or code with explicit limits:

```ts
interface Sandbox {
  readonly execute: (request: SandboxRequest) => Stream.Stream<SandboxEvent, SandboxError>;
}
```

The request declares:

- image/runtime identity;
- command and arguments as separate fields;
- working directory;
- environment allowlist;
- filesystem mounts and access modes;
- network policy;
- CPU, memory, output, and wall-clock limits;
- secret handles rather than raw secret values;
- artifact collection rules.

The result records exit status, bounded output, resource use, artifacts, and the
sandbox implementation identity. A local process runner may satisfy the interface
for trusted development, but it must identify itself as `unisolated`; it is not a
security sandbox. An adapter rejects a runtime identity it cannot honor instead of
echoing it as an observed runtime. Event artifact metadata and failure diagnostics
are bounded at the adapter boundary.

## 9.1 Code Mode and the CodeExecutor port

Code Mode is distinct from a general code interpreter: one native Effect AI Tool accepts bounded
JavaScript source written by the model, executes it in one isolated pass, and lets the program
call an explicit allowlist of existing Effect AI Tools through typed sandbox globals.
The generated program is one
async function expression invoked by a fixed harness entrypoint. The handler never invokes a
second model.

The builder lives in `@effect-agent/capabilities` and follows the Delegation pattern: an explicit
record of selected Tools plus namespace mapping at construction, returning an ordinary Effect AI
Tool and a handler Layer, with no ambient registry. Construction fails closed on a non-`readonly`
Tool, an approval-requiring Tool, a sanitized-name collision, and any parameter or success Schema
the declaration deriver cannot render. The outer Tool is annotated `readonly`; unless the author
separately adds them to the model-facing Toolkit, the model sees only the Code Mode Tool.

Model-facing TypeScript declarations are documentation derived from the encoded side of the
selected Tools' Effect Schemas, which define the JSON that crosses the sandbox boundary. Runtime
validation always uses the original Schemas, before the original handler starts and again when
its result crosses back. A failed inner call rejects inside the program with a Schema-encoded
envelope carrying the existing framework error tags; raw Effects, Layers, services, database
clients, credentials, and Causes never cross into the sandbox.

The `CodeExecutor` port is a sibling of the command-shaped `Sandbox` service in
`@effect-agent/sandbox` (the documented `capabilities -> sandbox` edge):

```ts
interface CodeExecutor {
  readonly execute: (
    request: CodeExecutionRequest,
  ) => Effect.Effect<CodeExecutionResult, CodeExecutionError, Scope.Scope | CodeExecutionHost>;
}
```

`CodeExecutionHost` is an Effect service in the requirement channel, provided per pass at the
pass edge; its live host-call bindings are scoped resources and are never persisted. Requests,
results, limits, and expected errors are Effect Schemas. The request bounds source bytes, CPU
and wall-clock time, captured output, the final result, and host calls (a maximum call count
plus per-call argument and result byte bounds). Implementations reuse the
`SandboxImplementation` posture idiom (CAP-010) and reject limits or policies they cannot
enforce; an `unisolated` executor is never a security boundary. Interruption closes the workload
and every transport or resource owned by the pass.

The final result, captured logs, and thrown values share one model-visible output boundary with a
single aggregate byte budget and redaction policy. Intermediate Tool results never leave the
pass implicitly. They do not pass through telemetry, canonical records, or declarations. In deployment class
`E`, inner calls produce no Canonical Records: the Conversation Log carries only the outer Tool
Call and its bounded final result, with inner-call evidence in telemetry counts and host-Tool
audit metadata. Code Mode claims deployment class `E` only; the `DN` and `DC` assemblies make
no Code Mode claim until this specification says otherwise.

## 9.2 Page capture and the PageCapture port

Page capture renders one page — a navigated https URL or supplied HTML — in a managed headless
browser and returns exactly one bounded output: rendered HTML, Markdown, discovered links,
schema-shaped structured data, or grouped selector scrape results. The port is a stateless sibling
of `Sandbox` and `CodeExecutor`
in `@effect-agent/sandbox`: a browser is intrinsically an egress device, so it carries its own
explicit capture contract instead of widening the sandbox network policy that every existing
adapter rejects typed.

```ts
interface PageCapture {
  readonly capture: (
    request: PageCaptureRequest,
  ) => Effect.Effect<PageCaptureResult, PageCaptureError>;
}
```

Requests, results, limits, and expected errors are Effect Schemas. Navigation targets must be
absolute HTTPS URLs without embedded credentials. Returned links are data, never navigation
authority, and must be absolute credential-free HTTP or HTTPS URLs. The request selects the
engine (`chromium`, or the lightweight `kitesurf` where an adapter supports it), bounds the
response in UTF-8 bytes, and may constrain navigation readiness, viewport, and request egress.
Structured requests accept only object-shaped JSON Schema documents whose root and every nested
node use the explicitly supported, type-checked JSON Schema vocabulary. The document is limited
to 64 KiB of encoded data, depth 32, 4,096 total nodes, and 256 entries per collection.
Malformed keywords, unsupported keywords, cycles, and over-budget documents fail at the request
Schema boundary.
Selector scrape requests contain 1 to 64 bounded selectors. Results preserve provider group and
element order and are limited to 4,096 aggregate elements, 64 bounded attributes per element,
bounded text and HTML, and finite geometry. Malformed or over-budget scrape results fail typed;
adapters never truncate them into an apparently successful result. The response byte budget still
applies to the complete provider response before decoding.
Adapters reuse the `SandboxImplementation` posture idiom (CAP-010), reject any feature or engine
they cannot honor, and surface platform rate and quota refusals as one typed failure carrying
the platform's own backoff hint. Foreign browser or transport failures retain their original live
cause; remote error bodies and provider envelopes retain only a bounded host-only cause. Public
failure messages and cleanup logs use fixed operation or status descriptions and never include
foreign exception text, response bodies, or provider diagnostics. Capture resource use records
browser time and, when an adapter
performs separately authorized model inference, its provider and model-call count. Everything a
capture returns is untrusted, attacker-influenced content
([security §9](./security-operations.md)).

The model-facing builders live in `@effect-agent/capabilities` and follow the Delegation
pattern: `WebCapture.make` exposes a fixed action set over an immutable construction-time https
host allowlist (deny-by-default; a `*.example.com` wildcard matches the apex and its subdomains).
The same allowlist is projected into the browser's request policy, covering initial navigation,
redirect destinations, and every rendered-page subrequest. `WebCapture.makeExtract` derives the
platform-side JSON response format from one Effect Schema and decodes the untrusted result
through that exact Schema; its handler Layer visibly requires both `PageCapture` and the
Schema's decoding services, and the Tool invocation keeps those decoding services visible in
its own requirement channel. `WebCapture.makeScrape` exposes only the target URL and bounded
selector list while preserving the portable grouped result contract.

All three builders return ordinary Tools with `failureMode: "return"` and execution class
`uncertain`: page JavaScript can mutate remote state, so captures are neither safely replayable
nor eligible for readonly-only Code Mode. Platform-side model inference is never implicit; the
host must authorize and account for its provider before extraction starts. Authorization and
accounting refusals fail as `PageCaptureInferencePolicyError`, identifying the provider and which
policy step failed while retaining host diagnostics only as the live cause. Construction fails
closed on an empty or malformed host pattern, an invalid response byte budget, an empty action
set, and an extraction Schema the deriver cannot express. Stateful browser sessions, screenshots,
PDFs, snapshot bundles, crawling, and accessibility trees remain outside this one-page port.
Screenshots and crawling use separate sibling contracts with their own binary and scoped-stream
semantics. Page capture claims deployment class `E` only.

Raw CDP browser execution is not a supported capability. The pinned Cloudflare browser client has
no session egress guardrail, and the later provider guardrail accepts hostnames rather than this
framework's exact HTTPS origins. Page request interception is not a containment boundary for
arbitrary Target, Runtime, Fetch, and Network commands. A future connector therefore requires
either a provider-enforced exact-origin boundary or an explicitly accepted, separately named
unrestricted authority; it must not be hidden inside Code Mode or `InteractiveBrowser`.

## 9.3 Page screenshots

`PageScreenshot` is a sibling stateless port for one caller-owned PNG. Its request fixes one
credential-free HTTPS target, navigation and subrequest policy, viewport, full-page choice,
engine, and byte limit before execution. `capture` returns one bounded `Uint8Array` with the
fixed `image/png` media type. The framework never writes those bytes to canonical records, model
text, logs, telemetry, or result metadata. The caller decides any later storage or handoff.

The native Browser Run adapter validates MIME, rejects an honest over-budget Content-Length before
acquiring a reader, and uses incremental byte counting as the authority. It cancels bodies on all
paths. Kitesurf fails typed because the native binding cannot select it.

## 9.4 Page crawls

`PageCrawl` is a Schema-first, scoped `Stream` port for one bounded rendered-Markdown crawl. Its
request fixes one credential-free HTTPS starting URL, a non-empty set of content-signal purposes,
and page-count, depth, per-page byte, aggregate byte, and wall-clock limits. Every returned record
must remain on the starting URL's exact WHATWG host; redirects reported in record metadata are
checked against the same host. The stream preserves Cloudflare's documented per-record states so a
consumer can distinguish completed, disallowed, skipped, errored, queued, and cancelled pages.

Provider job identity, polling, cursor pagination, cancellation, and retention are adapter-private.
The caller cannot reattach to or persist a crawl job. Pagination is lazy: the adapter requests the
next cursor only after downstream demand consumes the current page. It accepts the two cursor
shapes exposed by Cloudflare's official surfaces, normalizes each to one comparison representation,
and rejects malformed or repeated cursors before another request. All response bodies are read
incrementally under explicit transport bounds, while record Markdown is checked at the first
per-page or aggregate byte violation before emission.

The crawl and its remote job belong to the consuming `Scope`. If interruption wins before creation
returns a job identity, the adapter performs no deletion. Once creation returns an identity, the
adapter installs its finalizer immediately. A known-running job receives exactly one cancellation
attempt on typed failure, defect, deadline, interruption, or early consumer close; completed and
other terminal jobs are never cancelled. Cancellation failure is cleanup uncertainty: it emits one
fixed bounded warning without provider detail and never defects, masks, or changes the primary
`Exit`. The adapter performs no automatic retry and claims deployment class `E` only.

## 9.5 Interactive browser

`InteractiveBrowser` is a separate, provider-neutral, scoped service for one browser pass. The
pass owns exactly one browser, context, and page. Its handle supports navigation, bounded text
reads, field filling, element clicks, PNG screenshots, viewport scrolling, and explicit closure.
Its immutable policy fixes an exact HTTPS host allowlist, one page, action-count, elapsed-time,
and per-result byte limits before acquisition. Text counts UTF-8 bytes; screenshots count PNG
bytes. The same policy governs initial navigation, redirects, and every page subrequest;
violations fail closed.

`screenshot(BrowserScreenshotRequest)` captures the current page without navigation or a new
browser. The request selects viewport or full-page capture with `fullPage`; the result reuses
`PageScreenshotResult`. Bytes belong to the caller and never enter framework records, model
context, logs, or telemetry. `scroll(BrowserScrollRequest)` scrolls the current viewport by
`deltaX` and `deltaY` CSS pixels, each an integer from -100,000 to 100,000, and returns the observed
page URL. Screenshot and scroll operations consume the same action budget and use the same
deadline and concurrency gate as navigation, reads, fills, and clicks.

The returned handle is ephemeral and is not a Schema value, durable record, reconnect token, or
model-facing Tool. A handle permits one operation at a time; concurrent calls fail immediately
with a typed busy error. Capacity refusal, browser expiry/remote closure, navigation failure, and
malformed adapter observations remain typed failures. Browser JavaScript and navigation can mutate
remote state, so operations are uncertain: no automatic retry or replay is permitted.

Adapters acquire browser, context, and page resources in `Scope`, register finalizers immediately,
and close them in reverse order on every `Exit`. `yield* handle.close` invalidates the handle before
teardown and remains available after an interrupted action, policy failure, or exhausted budget.
It shares one teardown attempt with Scope finalization; repeated close calls return that attempt's
outcome without repeating remote closure. Explicit close reports a typed cleanup failure. Scope
cleanup emits fixed warnings and must not mask the primary result. An action that finishes after
closure cannot return success. No daemon fiber, module-global registry, persistence, or generic
Puppeteer wrapper is allowed. Features the provider cannot enforce fail as typed unsupported.

The Cloudflare adapter uses the explicit Browser Run binding and `@cloudflare/puppeteer` 1.1.0.
Cloudflare's `keep_alive` controls inactivity only; the application deadline remains authoritative.
Cloudflare launch capacity refusal and disconnected/expired sessions map to typed capacity or
expiry errors, retaining provider details only as host-side causes.
Cloudflare Live View, handoff, and cleanup by private provider identity belong to the separate
host API in `@effect-agent/platform-cloudflare`, not to `BrowserHandle`. The host owns permissions,
controller arbitration, and any durable action receipts. This capability adds no durable browser
state or automatic recovery of uncertain actions.

## 10. Subagents

The proposed Subagent capability is specified in [subagents.md](./subagents.md). It uses declared
Effect AI Tools for attached delegation: the parent retains conversational control, each invocation
owns a fresh child Conversation, child context and authority are explicit, and the parent joins one
Schema-validated result.

An ephemeral child uses structured concurrency and belongs to its parent Scope. A durable child is
a separate accepted Submission with immutable parent linkage and independent Attempt ownership.
The full proposed authority model is not yet implemented.

## 11. Persistent agent state

Persistent state is separate from conversation history. An agent state store uses
Effect Schema for value validation and optimistic revision checks.

State writes may be:

- run-local and discarded on settlement;
- committed transactionally with a canonical event;
- external and compensatable;
- external and uncertain.

The API must force the caller to select the write class. "Memory" is not a single
untyped key-value bag.

## 12. Observability integration

Every capability operation inherits tracing context and emits:

- operation name and version;
- run/conversation identity;
- duration and outcome;
- retry count;
- bounded, redacted attributes;
- relevant resource identity;
- structured failure category.

Model text, tool input/output, skill content, and sandbox output are sensitive by
default and are excluded from ordinary span attributes.

## 13. Requirements

- **CAP-001**: Every optional capability is represented by an Effect service.
- **CAP-002**: Missing capabilities fail during runtime construction or preflight,
  not midway through an unrelated operation.
- **CAP-003**: Application Tools use Effect AI definitions and handlers; the engine owns their
  durable scheduling rather than a provider SDK.
- **CAP-004**: All tool inputs and outputs cross Effect Schema boundaries.
- **CAP-005**: Tool call result commits are deterministic.
- **CAP-006**: Approval requests and decisions are canonical audit events.
- **CAP-007**: Compaction never mutates canonical history.
- **CAP-008**: Loaded skill identity and version are recorded for a run.
- **CAP-009**: Effect AI MCP integration preserves remote identity and does not imply exactly-once
  execution.
- **CAP-010**: Unisolated process execution is never labeled a security sandbox.
- **CAP-011**: When enabled, Subagents use only declared delegation Tools; attached ephemeral
  children use structured concurrency and belong to the parent Scope.
- **CAP-012**: Persistent agent state is distinct from conversation history.
- **CAP-013**: Capability telemetry is bounded and redacted by default.
- **CAP-014**: A Code Mode Tool is an ordinary Effect AI Tool over an explicit construction-time
  allowlist; construction fails closed on non-`readonly` Tools, approval-requiring Tools, name
  collisions, and non-renderable declaration Schemas.
- **CAP-015**: The `CodeExecutor` port is Schema-first and scoped: adapters report their
  isolation posture honestly, reject limits they cannot enforce, and release every pass-owned
  resource on success, failure, timeout, and interruption.
- **CAP-016**: Code Mode applies one byte budget and redaction policy to all model-visible output,
  including the final result, captured logs, and thrown values. Intermediate results never
  leave a pass implicitly.
- **CAP-017**: Budget snapshots are cache-aware and context-aware: `UsageTotals` and
  `UsageDelta` carry cache-read and cache-write input tokens distinctly (with `inputTokens`
  remaining the total), and totals expose the most recent call's input/output tokens as the
  live-context estimate.
- **CAP-018**: Page capture is deny-by-default: its immutable target-host allowlist governs
  credential-free HTTPS navigation, redirects, and subrequests; returned links are validated
  credential-free HTTP(S) data; its action set, engine, and byte budget are fixed at capability
  construction; structured extraction accepts only a bounded object JSON Schema and exposes its
  decoder requirements; selector scrape preserves bounded grouped results and rejects malformed or
  over-budget provider output without truncation; browser execution remains `uncertain`; model
  inference requires explicit host authorization and accounting; and every result is treated as
  untrusted input.
- **CAP-019**: Page screenshots use a separate Schema-first port with fixed PNG output and a
  caller-owned byte lifetime; adapters enforce MIME and byte bounds and never persist or project
  screenshot bytes.
- **CAP-020**: Page crawls use a scoped, exact-host Stream with immutable page-count, depth,
  per-page, aggregate, and deadline limits; adapters keep job identity private, paginate lazily,
  never retry unresolved work, and cancel a known-running job exactly once when its Scope exits.
- **CAP-021**: Interactive browser actions share one scoped page, immutable host policy, action
  budget, deadline, and single-operation gate. Screenshots return bounded caller-owned PNG bytes;
  scrolls accept bounded pixel deltas. Explicit closure invalidates the handle and shares teardown
  with Scope finalization. Provider identities and operator controls stay outside the generic handle.
