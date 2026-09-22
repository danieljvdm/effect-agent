import {
  ThreadPortTransport,
  portTransportFailure,
} from "@effect-agent/storage-cloudflare/port-routing";
import { Effect, Layer } from "effect";
import type { ThreadId } from "effect-agent/identifiers";
import { RpcTracing } from "effect-cf";

import { callThreadObject, ThreadObjectNamespace } from "../CloudflareBindings.ts";

/**
 * `ThreadPortTransport` over native Durable Object JS RPC (decision D-P6-3): one
 * `portCall(envelope)` on the stub of the Object that owns the addressed Thread
 * (`namespace.idFromName(threadId)` — the identity rule, plan §1.2). The envelopes are
 * already Schema-encoded JSON, so the RPC boundary carries only structured-cloneable values;
 * the protocol module stays transport-agnostic and fetch-with-JSON remains the documented
 * fallback carrier.
 * The namespace's optional `rpcTracing` setting uses the same transient trailing context
 * as host calls; it never changes the encoded port envelope.
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
    const namespace = yield* ThreadObjectNamespace;
    const { rpcTracing } = namespace;

    return ThreadPortTransport.of({
      call: Effect.fn(
        function* (threadId: ThreadId, request: unknown) {
          const traceArgs =
            rpcTracing === undefined ? [] : yield* RpcTracing.withRpcTraceContext([]);

          return yield* callThreadObject(
            threadId,
            (target) => target.portCall(request, ...traceArgs),
            (cause) => portTransportFailure(threadId, cause),
          ).pipe(Effect.provideService(ThreadObjectNamespace, namespace));
        },
        (effect, threadId) =>
          rpcTracing === undefined
            ? Effect.withSpan(effect, "CloudflarePortTransport.call", {
                attributes: { threadId },
              })
            : RpcTracing.withRpcClientSpan(effect, rpcTracing, "portCall"),
      ),
    });
  }),
);
