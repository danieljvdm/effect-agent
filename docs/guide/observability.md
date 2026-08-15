# Observability

Effect Agent emits logical measurements and leaves exporter selection to the host. Tool
instrumentation belongs to `@effect-agent/engine`; Cloudflare native runtime and exporter lifecycle belongs to `effect-cf`. Neither package depends on
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

Provider/model Tool Call IDs are untrusted input. The engine accepts only values whose entire string
matches `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}` and validates before Turn correlation, canonical event
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

`makeConversationObjectClass` is implemented with `effect-cf`'s
`DurableObject.make`. effect-cf owns one cached `ManagedRuntime` per Durable Object
incarnation, native RPC method execution, per-event Layer scopes, Object state/environment
services, `DurableObjectState.waitUntil`, and optional post-RPC OTLP flushing.

Supply a host observability Layer as the factory's second argument:

```ts
import { makeConversationObjectClass } from "@effect-agent/platform-cloudflare";
import { CloudflareOtlp } from "effect-cf";

const Observability = CloudflareOtlp.layerDurableObject({
  className: "ConversationObject",
  signals: ["logs", "traces"],
  resource: { serviceName: "effect-agent" },
});

export class ConversationObject extends makeConversationObjectClass(
  {
    namespaceBinding: "CONVERSATIONS",
    deploymentId: "production",
    producerPrefix: "conversation",
  },
  Observability,
) {}
```

The Layer is built inside each native event Scope. It may provide Effect Logger, Tracer, Metric,
and `OtlpExporter.Flusher` services; require effect-cf's `DurableObjectState` or
`WorkerEnvironment`; consume Effect Agent's `DurableObjectContext` or
`ConversationObjectNamespace`; and fail acquisition typed. Omitting the second argument installs
no telemetry service and selects no vendor.

After every native RPC method, effect-cf schedules the optional flusher with
`DurableObjectState.waitUntil`, on both handler success and failure. The RPC result does not await
export. effect-cf isolates exporter and scheduling failures and emits content-free diagnostics;
Effect Agent does not add a second flush path, batching coordinator, timeout, or waitUntil bridge.
Cross-Object port calls and wake are native RPC methods on this same boundary.

### Raw alarm limitation

In effect-cf 0.25.2, the raw `alarm` handler runs in the same event-scoped runtime, but it does not
receive the automatic post-exit flusher scheduling that native RPC methods receive. This is an
upstream lifecycle gap. Effect Agent deliberately does not recreate a private alarm-only
`waitUntil` owner: alarm rejection must continue to reach workerd for retry, and the common fix
belongs in effect-cf. Durable correctness never depends on telemetry export.

## Migrating an application wrapper

Applications upgrading from `@effect-agent/*` `0.0.1-beta.5` can remove loopback executor
wrappers that duplicate `execute_tool` names, execution classes, or outcomes. Cloudflare
applications can remove manual per-RPC exporter flushing and provide their Effect observability
Layer directly as the second `makeConversationObjectClass` argument.

If an application adopted an earlier preview of this PR, remove
`CloudflareRuntimeTelemetry`, `CloudflareTelemetryExportError`, and
`telemetryFlushTimeout`; those APIs were part of the superseded duplicate lifecycle
implementation and are not exported.

Verify the migration by searching for `gen_ai.operation.name=execute_tool`, checking both bounded
outcomes, confirming one effect-cf flush after a native RPC, and preserving failed-alarm retry
behavior. The downstream emergency bridge that motivated this API is
[Kommunikasie PR #353](https://github.com/reve-ai/kommunikasie/pull/353).
