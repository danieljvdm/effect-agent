import { ConversationPortTransport, portTransportFailure } from "@effect-agent/storage-cloudflare";
import { Effect, Layer } from "effect";

import { ConversationObjectNamespace } from "./bindings.ts";

/**
 * `ConversationPortTransport` over native Durable Object JS RPC (decision D-P6-3): one
 * `portCall(envelope)` on the stub of the Object that owns the addressed Conversation
 * (`namespace.idFromName(conversationId)` — the identity rule, plan §1.2). The envelopes are
 * already Schema-encoded JSON, so the RPC boundary carries only structured-cloneable values;
 * the protocol module stays transport-agnostic and fetch-with-JSON remains the documented
 * fallback carrier.
 *
 * Every delivery problem — stub construction, RPC rejection, overload, deploy-in-progress —
 * surfaces as `PortTransportError` (preserving the platform stub's own `retryable` signal
 * when present) and NEVER as a fabricated answer: on `resolveAdmission` the routing layer
 * turns exactly this error into `AdmissionIndeterminate` (SUB-031).
 */
export const conversationPortTransportLayer: Layer.Layer<
  ConversationPortTransport,
  never,
  ConversationObjectNamespace
> = Layer.effect(ConversationPortTransport)(
  Effect.gen(function* () {
    const { namespace } = yield* ConversationObjectNamespace;
    return ConversationPortTransport.of({
      call: (conversationId, request) =>
        Effect.tryPromise({
          try: () => namespace.get(namespace.idFromName(conversationId)).portCall(request),
          catch: (cause) => portTransportFailure(conversationId, cause),
        }).pipe(
          Effect.withSpan("CloudflarePortTransport.call", {
            attributes: { conversationId },
          }),
        ),
    });
  }),
);
