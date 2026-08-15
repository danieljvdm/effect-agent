# Observability

Effect Agent emits logical measurements and leaves exporter selection to the host. Tool
instrumentation belongs to `@effect-agent/engine`; exporter lifecycle at Cloudflare native
delivery boundaries belongs to `@effect-agent/platform-cloudflare`. Neither package depends on
Sentry or another telemetry vendor.

## Tool execution

Each in-memory application-handler attempt creates one canonical internal OpenTelemetry span:

```text
execute_tool <tool-name>
```

The span covers `ToolCallStarted` through settlement of the handler result stream. The complete
application-handler attempt lifecycle set is success, failure, interruption, and nonterminal
waiting. Success, failure, and interruption terminate that in-memory attempt; waiting may remain
unresolved indefinitely. Denied and provider-executed calls are call-level classifications only
and never create an application-handler attempt. Success/failure attempts emit one bounded outcome only after that
stream settles: a Tool failure returned as a value (`failureMode: "return"`) closes the span with a
failed status just like a failure in the Effect error channel, while an error or duplicate result
after an apparent terminal success overrides the span/log outcome to failure. Interrupted attempts
emit no terminal outcome log. Recovery is
at-least-once and can create another attempt—and therefore another span—without changing the
canonical event stream.
The terminal Run event and Turn trace result remain provisional until the handler stream settles;
a failure after a provisional terminal value therefore commits one `ToolCallFailed`, never
`ToolCallSucceeded`, before its original Cause propagates.
Exporter-visible failure detail is one fresh bounded framework marker object per handler attempt.
Only that exact identity is recognized, so an independently constructed same-class handler failure
is ordinary Tool failure. The original typed error, defect, or interruption is restored unchanged
after the span closes.
Effect AI's Toolkit implementation annotations land on a manually constructed, non-exported local
span whose parent is the canonical Tool span. The local span carries
`Tracer.DisablePropagation=true`, so explicit host-owned spans created by the Tool handler filter
past it and export as children of `execute_tool`; tracing remains enabled for application code.
There is no exported `AgentRuntime.toolkit.handle` span, and Toolkit parameters cannot contaminate
the canonical span.

| Attribute                           | Meaning                                              |
| ----------------------------------- | ---------------------------------------------------- |
| `gen_ai.operation.name`             | Always `execute_tool`                                |
| `gen_ai.tool.name`                  | Effect AI Tool name                                  |
| `gen_ai.tool.type`                  | Always `function`                                    |
| `gen_ai.tool.call.id`               | Validated provider/runtime Tool Call correlation     |
| `gen_ai.agent.name`                 | Agent Definition ID                                  |
| `gen_ai.conversation.id`            | Framework-owned conversation/session correlation     |
| `effect_agent.tool.execution_class` | `readonly`, `idempotent`, or fail-closed `uncertain` |
| `effect_agent.tool.outcome`         | Bounded terminal `success` or `failure`              |
| `agentId`, `conversationId`         | Backward-compatible Agent/conversation correlation   |
| `runId`, `turnId`                   | Framework Run/Turn correlation                       |
| `toolCallId`, `toolName`            | Backward-compatible validated/name correlation       |

Provider/model Tool Call IDs are untrusted input. The engine accepts only 1–128 ASCII characters
matching `[A-Za-z0-9][A-Za-z0-9._:-]*` and validates before Turn correlation, canonical event
emission, or handler scheduling. An invalid ID fails the Run with a typed `ModelProtocolError` and
never invokes the Tool. The semantic and compatibility telemetry fields therefore contain the same
already-validated identifier used by the framework.

This follows OpenTelemetry's
[`gen_ai.conversation.id` guidance](https://github.com/open-telemetry/semantic-conventions-genai/blob/30182acd5ed78ab5f619041eaec5e95a4eb83a48/docs/registry/attributes/gen-ai.md):
use a readily available conversation identifier and never synthesize one from trace IDs, hashes,
or request content.

Successful attempts emit one info log (`agent tool execution completed`); failed attempts emit one
warning (`agent tool execution failed`). Both carry the same bounded identity, execution-class,
and outcome fields. The existing start log remains debug-only. Provider-executed Tools have no
application handler and therefore do not produce this span or these logs.
If a handler stream fails after producing a provisional terminal value, the engine emits one
`ToolCallFailed`, never `ToolCallSucceeded`, before preserving the original handler Cause for the
batch failure.
If a host Logger or Tracer defects while creating, annotating, or closing the canonical span or
emitting its terminal log, the engine reports that telemetry Cause through Effect's
`ErrorReporter` boundary and preserves the Tool event and exit. Span creation falls back to a
non-exported local span; span-close failure never reruns a handler that already completed.
A defective reporter is isolated as well: observability cannot make a completed side effect appear
eligible for recovery. The terminal Run event and Turn trace result are committed and emitted
before terminal telemetry runs. External interruption is never recovered by this derivative
boundary, but it therefore cannot erase completed Tool state; only non-interrupt telemetry Causes
are reported and suppressed. Internally, an emission boundary returns the singleton terminal event
from its first pull, then starts telemetry from the next pull or structured stream finalization if
the owner closes after that event. A synchronous phase gate gives the action one owner and never
retries an in-flight interrupted attempt. Once the terminal outcome annotation exists, it remains
authoritative for canonical span status over downstream cancellation or telemetry interruption.
The authority is module-private identity state, not the mutable public outcome attribute.
`AgentRuntime.stream` builds this isolation policy once at its composition boundary as an internal
`ToolSpanTelemetry` capability derived from the host's ambient Tracer. Tool execution consumes that
capability; it does not select, decorate, or locally provide a Tracer implementation. Each
execution of a reusable Tool stream gets its own local tracer-isolation/defect state, and a Toolkit
effect invoked without a current span simply runs unchanged with its typed error channel intact.

Tool parameters, results, prompts, source code, commands, conversation content, and failure
messages are never attached to these spans or logs. There is no implicit content-capture mode.
Applications that deliberately add content telemetry own its classification, redaction,
authorization, and retention policy.

## Cloudflare exporter lifecycle

`makeConversationObjectClass` caches one `ManagedRuntime` per Durable Object incarnation. Supply
the host telemetry Layer as its second argument:

```ts
import {
  CloudflareRuntimeTelemetry,
  makeConversationObjectClass,
} from "@effect-agent/platform-cloudflare";
import { Effect, Layer } from "effect";
import { OtlpExporter } from "effect/unstable/observability";

declare const HostObservability: Layer.Layer<OtlpExporter.Flusher>;

const Telemetry = Layer.effect(
  CloudflareRuntimeTelemetry,
  Effect.map(OtlpExporter.Flusher, ({ flush }) => CloudflareRuntimeTelemetry.of({ flush })),
).pipe(Layer.provideMerge(HostObservability));

export class ConversationObject extends makeConversationObjectClass(
  {
    namespaceBinding: "CONVERSATIONS",
    deploymentId: "production",
    producerPrefix: "conversation",
    telemetryFlushTimeout: 2_000,
  },
  Telemetry,
) {}
```

`HostObservability` is where the application installs its Effect Logger, Tracer, Metric, and
exporter layers. `OtlpExporter.Flusher` is one implementation; any vendor or custom exporter can
provide the same content-free `CloudflareRuntimeTelemetry` service. The Layer may require
`DurableObjectContext` or `ConversationObjectNamespace` to derive configuration from the Object
environment, may provide additional services alongside the required telemetry capability, and a
typed acquisition failure remains part of constructor-gate failure. Those additional outputs and
defaulted Effect Logger/Tracer/Metric overrides remain present in the cached `ManagedRuntime`.
Omitting
the second argument installs `CloudflareRuntimeTelemetry.layerNoop` at this Worker composition
edge. `CloudflareDurableRuntime.layer` itself requires the telemetry service explicitly; it does
not choose or hide a provider in runtime options.

`flush` has the typed error channel `CloudflareTelemetryExportError`. A custom adapter maps its
foreign exporter failure with `CloudflareTelemetryExportError.make({ cause })`, retaining the
original value for host diagnostics without placing it on entrypoint spans or changing delivery
results.

Every native encoded RPC, cross-Object port call, wake, and alarm attempt gets one owner-side server
span. The native RPC/alarm Promise never awaits export. Before `ctx.waitUntil`, one per-incarnation
coordinator synchronously reserves the delivery into a shared pending, trailing, or queued batch.
Each batch retains at most 64 delivery settlement Promises, waits for those retained deliveries to
settle, and only its first owner registers its shared always-fulfilled background Promise. Further
arrivals are lossy-coalesced into the requested export without retaining their Promise and produce
one bounded `reservation_limit` diagnostic per capped batch. Cycles remain capped at a first
attempt plus one trailing attempt; deliveries arriving while that trailing attempt runs share one
queued cycle. There is at most one active exporter, one coalesced trailing batch, and one queued
cycle—never concurrent export attempts, per-delivery background registrations, or unbounded
delivery retention. `telemetryFlushTimeout` is a cooperative budget:
effect-agent interrupts an interruptible exporter when the budget expires, but does not claim a
hard deadline for exporter code that masks interruption. Cloudflare owns that remaining background
lifetime and may cancel it when the Object is evicted.

Expected exporter failure, timeout, defect, interruption, and `waitUntil` registration diagnostics
contain only a framework-owned `effect_agent.cloudflare.telemetry.failure_kind` classification.
Foreign exporter causes, arbitrary defects, fiber IDs, and caught platform causes are never passed
to the configured Logger. `CloudflareTelemetryExportError.cause` remains available only when the
host explicitly inspects the typed failure at its own diagnostic boundary. Failures stay rejected
through the coordinator. Only the final always-fulfilled
`waitUntil` Promise bridge isolates background telemetry failure from the original RPC result or
alarm retry signal. Budget expiry logs a bounded warning and remains a typed `TimeoutError` through
the same boundary. None of these diagnostics is attached to the already-closed native span. Native rejection spans likewise receive only a bounded failure
marker before the original Cause is restored. The cached runtime is not disposed per event:
Durable Objects have no guaranteed shutdown callback, and storage/runtime/exporter scopes must
remain available for later events in the same incarnation.

A synchronous `ctx.waitUntil` registration failure is also derivative: effect-agent emits only the
bounded `wait_until_registration` classification, cancels that unowned batch ticket, then returns
the already-running native delivery Promise unchanged rather than creating an uncertain RPC
outcome. The caught platform Cause is not made available to the automatic Logger. The registration
failure means Cloudflare has not accepted ownership of that background work, so the gated exporter is not invoked. The diagnostic uses the
already-built runtime's synchronous Logger contract; logger defects are captured without starting a
fiber. Even a broken diagnostic sink does not change the delivery result or alarm retry signal.

There must be one native-delivery flush owner. If an effect-cf or host native-entry integration already
flushes the same exporter, either disable that integration's delivery flush and provide it via
`CloudflareRuntimeTelemetry`, or keep the existing owner and provide the Effect observability Layer
merged with `CloudflareRuntimeTelemetry.layerNoop`. Do not install both delivery flush paths.

## Migrating an application wrapper

Applications upgrading from `@effect-agent/*` `0.0.1-beta.5` can remove loopback executor
wrappers that duplicate `execute_tool` names, execution classes, or outcomes after upgrading the
engine. Cloudflare applications can also remove per-RPC manual exporter flushes after choosing the
single lifecycle owner described above. Keep application-specific scope fields on a parent span or
as ambient log annotations if they are still useful.

If an application adopted an earlier preview of this API, move `telemetry: Telemetry` out of
`CloudflareDurableRuntimeOptions` and pass the same Layer as the second
`makeConversationObjectClass(options, Telemetry)` argument. Acquisition requirements and typed
errors then remain visible at the Worker composition boundary.

Verify the migration by searching for `gen_ai.operation.name=execute_tool`, checking both bounded
outcomes, and forcing one failed alarm delivery. The downstream emergency bridge that motivated
this API is [Kommunikasie PR #353](https://github.com/reve-ai/kommunikasie/pull/353).
