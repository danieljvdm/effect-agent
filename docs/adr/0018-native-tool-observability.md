# ADR-0018: Native Tool observability and Cloudflare exporter lifecycle

- Status: Superseded by [ADR-0019](0019-effect-cf-cloudflare-lifecycle.md) for Cloudflare lifecycle
- Date: 2026-08-14
- Decision owners: Project owner
- Related decisions: D-001, D-019, D-032, D-033, D-036
- Builds on: [ADR-0001](0001-effect-native-core.md),
  [ADR-0014](0014-cloudflare-conversation-objects.md),
  [ADR-0015](0015-hardening-shape.md)

## Context

> Historical note (2026-08-15): the engine Tool-observability decision remains in force. The
> private Cloudflare runtime/flush design below was implemented on this branch and then removed
> after `effect-cf@0.25.2` gained native RPC telemetry flushing. ADR-0019 records the corrected
> ownership boundary without rewriting this accepted decision as though the first design had not
> happened.

A downstream `0.0.1-beta.5` Cloudflare deployment could observe successful repository Tools in
canonical events but could not search for them in exported logs or spans. Its emergency bridge
wrapped an application loopback RPC executor with content-free `execute_tool` telemetry and
manually flushed exporters after native RPC delivery. That restored visibility, but duplicated
canonical Tool names, execution classes, outcomes, and platform lifecycle outside the framework.

The engine already had a logical span around the full application-handler lifecycle. Ordinary
span failure inference was insufficient because Effect AI can represent a Tool failure as a
terminal value. The Cloudflare package also built a private cached `ManagedRuntime` without a
host observability Layer or a native-delivery export boundary.

## Decision

The existing engine handler lifecycle is the canonical OpenTelemetry Tool span. It is named
`execute_tool {toolName}`, follows GenAI `execute_tool` attributes, retains framework correlation,
and records the declared execution class plus a bounded success/failure outcome. Because the
framework already owns the Conversation ID, the span/log records it as `gen_ai.conversation.id`
and backward-compatible `conversationId`; it never derives identity from content. When an
in-memory handler attempt's complete lifecycle set is success, failure, interruption, and
nonterminal waiting. Success, failure, and interruption terminate that in-memory attempt; waiting
may remain unresolved indefinitely. Denied and provider-executed calls remain solely call-level
classifications and create no application-handler attempt. Success/failure attempts emit one
outcome after the handler stream settles, while interrupted attempts emit no terminal outcome log.
Value-level returned failures are failure, and any post-terminal error or
duplicate result overrides an apparent success to failure. At-least-once recovery can start another
attempt with its own telemetry. Framework terminal logs contain only bounded identity/outcome
fields. No Tool input, output, prompt,
conversation content, command, source, or failure message is exported.
Provider/model Tool Call IDs are untrusted and their entire value must match
`[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`. The engine validates before Turn correlation, canonical event
emission, or application handler scheduling; rejection is a typed protocol failure. Span/log
correlation therefore contains only the already-validated identifier.
The terminal Tool Run event and in-memory trace result are not committed until the handler stream
settles. A stream that fails after a provisional terminal value commits one `ToolCallFailed`, never
the provisional success, before preserving the original Cause for the batch. Terminal state is
committed and emitted before derivative telemetry, so telemetry interruption cannot make completed
Tool work eligible for replay. An internal emission boundary returns the singleton terminal event
on the first pull, then gives telemetry to the next pull or structured stream finalization on early
downstream closure. A synchronous phase gate permits one attempt and does not retry an in-flight
interrupted action. Once the terminal outcome annotation exists, it determines the canonical span
status even if downstream cancellation or telemetry interruption determines the channel Exit. The
status source is module-private identity-authenticated state rather than the publicly mutable
attribute map.
Span-facing failures use one fresh bounded marker object per handler attempt and recognize only that
exact identity; a handler cannot forge the control path by returning the same error class. The
original Cause is restored unchanged outside the measured boundary. Effect AI's parameter
annotations land on a manually constructed,
non-exported local span whose parent is the canonical span. Its `Tracer.DisablePropagation`
annotation makes explicit host-owned handler spans filter past it and attach to the canonical span,
while no duplicate framework handler span or raw handler Cause is exported by framework-owned
instrumentation. Logger/Tracer defects during canonical span creation,
annotation, logging, or closure are reported through Effect's owned `ErrorReporter` boundary
without changing the Tool event or exit. Creation falls back to a non-exported local span; closure
cannot rerun a completed handler; a defective reporter is isolated too. External interruption of
terminal telemetry or the reporter remains interruption instead of being recovered.
The engine composition boundary derives an internal `ToolSpanTelemetry` capability from the host's
ambient Tracer. Individual Tool execution consumes that capability and never selects, decorates, or
locally provides a Tracer implementation. Reusable stream executions allocate independent local
isolation/defect state, and absence of a current span is not converted into a defect.
Provider-executed calls remain outside that handler capability. Their progress/terminal events and
`TurnCompleted` are staged until the complete model response validates all call/result
correlations, so a malformed duplicate or post-finish part cannot append a provisional Tool
success.

`@effect-agent/platform-cloudflare` exposes a host-owned `CloudflareRuntimeTelemetry` service.
the `makeConversationObjectClass` native-entrypoint runtime requires it explicitly, and the Worker passes its provider as the
second `makeConversationObjectClass` argument rather than hiding it in runtime options. The host
Layer may require the Durable Object context or namespace and fail acquisition typed. It is
provided outside the complete Object application Layer so its Effect Logger/Tracer/Metric
configuration and any additional Layer outputs are present in the cached runtime. Every native
RPC, port call, wake, and alarm
attempt closes an owner-delivery span, then requests host export through `ctx.waitUntil`. Native
delivery never awaits export.
`telemetryFlushTimeout` is a cooperative budget for interruptible exporters, not a hard bound on
code that masks interruption; Cloudflare owns that remaining background lifetime. The service
exposes the concrete typed `CloudflareTelemetryExportError`, retaining a foreign exporter cause for
host diagnostics. Before calling `waitUntil`, a per-incarnation coordinator synchronously reserves
each delivery into a shared pending, trailing, or queued batch. A batch waits for all deliveries
reserved into it to settle before exporting, and only its first owner registers the shared
always-fulfilled background Promise. Cycles remain capped at a first attempt plus one trailing
attempt, with one queued cycle while trailing runs. This prevents concurrent attempts, per-delivery
background registrations, and unbounded exporter waiters under a stalled export. Each batch also
retains at most 64 delivery settlements; excess arrivals are lossy-coalesced into its requested
export and produce one bounded `reservation_limit` diagnostic for that batch. Expected exporter
failures, defects, and interruption log only a bounded framework-owned failure classification and
remain failed/interrupted until the final always-fulfilled `waitUntil` Promise bridge. Foreign
exporter causes, arbitrary defects, and fiber IDs are never passed to the automatic Logger; the
typed export error retains its cause for explicit host-controlled inspection. Timeout, exporter
failure, or synchronous `waitUntil` registration failure never changes the original result or alarm
rejection. Registration failure is logged synchronously with only the bounded
`wait_until_registration` classification before the exact delivery Promise is returned; its
arbitrary platform Cause is not sent to the configured Logger, and the unowned exporter
continuation is not invoked.
Native rejection spans are sanitized before the original Cause is restored. The runtime remains
cached for the Object incarnation; it is not disposed per delivery. Hosts configure one
native-delivery flush owner and do not duplicate this path with effect-cf or another native
boundary.

Telemetry remains derivative. No Run Event, canonical record, wire schema, persistence mutation, or
replay decision changes.

## Rejected alternatives

- **Application executor wrappers:** they cannot reliably own engine Tool identity or terminal
  value classification and force every downstream application to duplicate framework semantics.
- **A Sentry- or OTLP-specific platform API:** exporter choice belongs to the host. A one-method
  flush service is enough for Effect OTLP and vendor adapters.
- **Payload capture by default:** arguments/results are sensitive and unbounded. The framework
  ships no implicit capture switch.
- **Disposing `ManagedRuntime` after each event:** it would tear down storage, PubSub, and exporter
  scopes that must survive across Object inputs, and Cloudflare exposes no reliable final
  shutdown callback.
- **Awaiting a bounded flush in the native delivery Promise:** interruption-masking exporter code
  defeats the apparent hard bound and can delay RPC completion or suppress an alarm retry signal.
  `ctx.waitUntil` preserves span-before-flush ordering without making exporter completion part of
  native delivery.
- **A semaphore or per-delivery background Promise around each flush:** it bounds exporter
  concurrency but leaves an unbounded waiter/registration queue under a stalled exporter. Shared
  pending/trailing/queued batch tickets bound both dimensions.

## Consequences

Downstream applications can remove canonical Tool wrappers and native-RPC flush duplication.
Operators get searchable Tool identity and bounded outcomes without tenant content. Export no
longer adds latency to native Cloudflare delivery; a background flush can still be cancelled by
Object eviction, as expected. In the DC assembly tested with `@effect-agent/storage-cloudflare`,
durability belongs to canonical state, not observability.

## Validation

> The Cloudflare test files named below documented the superseded implementation and were removed
> with it. Current Cloudflare lifecycle validation is recorded in ADR-0019.

- `packages/engine/test/agent-runtime.test.ts` — success, returned failure, thrown typed failure,
  post-terminal failed-event/trace deferral, span create/close/context defects, single primitive
  evaluation, composed marker/residual Causes, host-owned handler-span parenting, status,
  attributes, logging, and all-exported-span content/Cause exclusion.
- `packages/platform-cloudflare/test/telemetry.test.ts` — every public native edge, span-before-
  background-flush ordering, non-blocking RPC/alarm exits, concurrent delivery, typed exporter
  failure, and content-free attributes/events/links.
- `packages/platform-cloudflare/test/restart/telemetry-lifecycle-unit.test.ts` — virtual-time proof
  of the cooperative timeout, uninterruptible-exporter behavior, same-turn/active/trailing/final-
  settlement coordinator cases, bounded shared waitUntil registration count/identity, bounded
  Cause-free diagnostics, delayed-start causal completion, and original-plus-residual restoration.
