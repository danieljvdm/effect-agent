# ADR-0019: Use effect-cf as the Cloudflare native lifecycle owner

- Status: Accepted
- Date: 2026-08-15
- Decision owners: Project owner
- Related decisions: D-014, D-032, D-036
- Supersedes: the Cloudflare lifecycle portion of
  [ADR-0018](0018-native-tool-observability.md)
- Builds on: [ADR-0014](0014-cloudflare-conversation-objects.md)

## Context

ADR-0018 correctly placed canonical application Tool telemetry in the engine, but also assigned
native Cloudflare runtime and exporter lifecycle to `@effect-agent/platform-cloudflare`. The
result was a private `CloudflareRuntimeTelemetry` service, a 432-line flush coordinator, a custom
cached `ManagedRuntime` boundary, reservation limits, pending/trailing/queued batches, bespoke
`waitUntil` failure handling, and a large lifecycle-only fixture matrix.

Before that implementation was finalized, `effect-cf@0.25.2` added post-handler telemetry
scheduling for native Durable Object RPC methods. Its `DurableObject.make` boundary already owns
one cached runtime per Object incarnation, Effect services for Object state and environment,
event-scoped Layers, native RPC methods, `DurableObjectState.waitUntil`, optional
`OtlpExporter.Flusher`, and content-free flush-failure diagnostics. Keeping both implementations
would create two lifecycle owners and duplicate export attempts.

## Decision

`@effect-agent/platform-cloudflare` builds its Conversation Object class with
`effect-cf`'s `DurableObject.make`.

Effect Agent continues to own:

- the engine's canonical `execute_tool` span and bounded terminal log;
- safe Tool identity, execution class, correlation, and terminal outcome;
- Conversation-specific native RPC handler effects;
- the existing storage, routed-port, wake, and multiplexed-alarm services behind Effect Layers.

effect-cf owns:

- the cached `ManagedRuntime` for the Durable Object incarnation;
- native RPC method installation and execution;
- event-scoped Layer acquisition and finalization;
- Durable Object state/environment services;
- `waitUntil` and post-RPC OTLP flush scheduling on success and failure;
- content-free flush and scheduling-failure diagnostics.

`makeConversationObjectClass(options, observability?)` accepts an optional event Layer as its
second argument. A host may provide Logger, Tracer, Metric, and `OtlpExporter.Flusher` services
there. Omitting the Layer adds no framework telemetry service and no no-op exporter. The Effect
Agent storage/config Layer is acquired inside `effect-cf`'s `blockConcurrencyWhile` service so
migration, compatibility checks, and defensive alarm inspection retain their existing constructor
gate. Recovery remains outside that local-only gate.

`effect-cf@0.25.2` schedules the flusher after native RPC methods but not after its raw alarm hook.
That is an upstream lifecycle gap. Effect Agent keeps the raw alarm handler in the same
event-scoped runtime but does not recreate a private flush coordinator or a second `waitUntil`
boundary. A future effect-cf release should close the gap at the common native entrypoint owner.
Alarm failure propagation and workerd retry behavior remain unchanged.

No canonical record, wire Schema, persistence mutation, settlement rule, or replay decision
changes.

## Rejected alternatives

- **Retain `CloudflareRuntimeTelemetry` beside effect-cf:** two lifecycle owners can flush the same
  exporter and force hosts to understand an implementation conflict that the platform adapter
  should eliminate.
- **Keep the private coordinator but disable it by convention:** dead code and its lifecycle test
  matrix would still drift from effect-cf and invite accidental reactivation.
- **Patch only raw alarms in Effect Agent:** this would establish split ownership at the exact
  boundary being reconciled. The missing common post-exit hook belongs upstream.
- **Keep the bespoke `ManagedRuntime`:** it duplicates effect-cf's entrypoint runtime, event Scope,
  Object services, and native method installation.

## Consequences

The Cloudflare platform change becomes a small adapter around the same durable services instead of
a second runtime framework. Hosts provide ordinary Effect observability Layers and remove manual
per-RPC flushing. Raw-alarm spans do not receive a guaranteed immediate post-exit flush until
effect-cf closes its gap; this limitation is explicit and does not affect durable correctness.

## Validation

- `packages/engine/test/agent-runtime.test.ts` pins canonical Tool spans, outcome status, bounded
  logs, interruption, defect isolation, and content exclusion.
- `packages/platform-cloudflare/test/observability.test.ts` proves that one native Conversation
  Object RPC schedules exactly one event-scoped OTLP flush through effect-cf.
- The existing platform-cloudflare workerd, eviction, alarm, cross-Object, and Miniflare restart
  suites prove the effect-cf class boundary preserves DC behavior.
- effect-cf commit
  [`56c1b2d`](https://github.com/danieljvdm/effect-cf/commit/56c1b2dfa30cbcc2336f28a8d8f7a48032f565df)
  owns the native RPC flush behavior and its success/failure/redaction tests.
