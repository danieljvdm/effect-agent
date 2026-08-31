import { ThreadPortTransport, portTransportFailure } from "@effect-agent/storage-cloudflare";
import { Effect, Layer } from "effect";

import { ThreadObjectNamespace } from "./bindings.ts";

/**
 * `ThreadPortTransport` over native Durable Object JS RPC (decision D-P6-3): one
 * `portCall(envelope)` on the stub of the Object that owns the addressed Thread
 * (`namespace.idFromName(threadId)` — the identity rule, plan §1.2). The envelopes are
 * already Schema-encoded JSON, so the RPC boundary carries only structured-cloneable values;
 * the protocol module stays transport-agnostic and fetch-with-JSON remains the documented
 * fallback carrier.
 *
 * Every delivery problem — stub construction, RPC rejection, overload, deploy-in-progress —
 * surfaces as `PortTransportError` (preserving the platform stub's own `retryable` signal
 * when present) and NEVER as a fabricated answer: on `resolveAdmission` the routing layer
 * turns exactly this error into `AdmissionIndeterminate` (SUB-031).
 */
export const threadPortTransportLayer: Layer.Layer<
  ThreadPortTransport,
  never,
  ThreadObjectNamespace
> = Layer.effect(ThreadPortTransport)(
  Effect.gen(function* () {
    const { namespace } = yield* ThreadObjectNamespace;
    return ThreadPortTransport.of({
      call: (threadId, request) =>
        Effect.tryPromise({
          try: () => namespace.get(namespace.idFromName(threadId)).portCall(request),
          catch: (cause) => portTransportFailure(threadId, cause),
        }).pipe(
          Effect.withSpan("CloudflarePortTransport.call", {
            attributes: { threadId },
          }),
        ),
    });
  }),
);
