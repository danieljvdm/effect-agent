import { Context, Effect, Layer, Schema } from "effect";

/** A host exporter failed during one coalesced background export attempt. */
export class CloudflareTelemetryExportError extends Schema.TaggedError<CloudflareTelemetryExportError>()(
  "CloudflareTelemetryExportError",
  {
    /** The original exporter failure, retained for host-side diagnostics. */
    cause: Schema.Defect(),
  },
) {}

/**
 * Host-owned exporter lifecycle for one Cloudflare Conversation Object incarnation.
 *
 * The platform package creates measurements but deliberately does not select an exporter or
 * vendor. Native entrypoint instrumentation consumes this service at the Worker composition edge;
 * applications pass its provider as the second `makeConversationObjectClass` argument together
 * with their Effect Logger, Tracer, and Metric layers. That provider may read `DurableObjectContext` or
 * `ConversationObjectNamespace`, and its typed acquisition error remains visible in construction.
 * `flush` must attempt to export every configured signal. After the native span closes, the
 * Conversation Object reserves it into a shared post-settlement batch before `ctx.waitUntil` under
 * a cooperative timeout. Only each pending/trailing/queued batch's first owner registers its
 * shared Promise. Cycles remain capped at one first exporter attempt plus at most one trailing
 * attempt, with one queued cycle while trailing runs; exporter completion never holds endpoint
 * delivery open and exporter failure never replaces endpoint behavior.
 */
export class CloudflareRuntimeTelemetry extends Context.Service<
  CloudflareRuntimeTelemetry,
  {
    readonly flush: Effect.Effect<void, CloudflareTelemetryExportError>;
  }
>()("@effect-agent/platform-cloudflare/CloudflareRuntimeTelemetry") {
  /** Content-free default selected by `makeConversationObjectClass` when no host Layer is passed. */
  static readonly layerNoop = Layer.succeed(CloudflareRuntimeTelemetry)({
    flush: Effect.void,
  });
}
