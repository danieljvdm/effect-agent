import type { ThreadId } from "@effect-agent/core";
import type { ProducerId } from "@effect-agent/thread";
import { Context, Effect, Layer, Predicate, Schema } from "effect";

/**
 * Cloudflare platform bindings as Effect services (DEPLOY-010: "Cloudflare platform bindings
 * are supplied as Effect services/Layers"). Application code never reads `env` or touches a
 * `DurableObjectState` directly — the Thread Object class constructs these Layers once
 * per incarnation and everything downstream consumes the services.
 */

/** A Cloudflare platform binding was missing or carried the wrong shape (DEPLOY-003/010). */
export class CloudflareBindingError extends Schema.TaggedError<CloudflareBindingError>()(
  "CloudflareBindingError",
  {
    binding: Schema.String,
    message: Schema.String,
  },
) {}

/**
 * The RPC surface one Thread Durable Object exposes to Workers and to sibling
 * Thread Objects. `ThreadObject.make` implements it; the Worker-side client
 * and the cross-Object transport call it through `DurableObjectNamespace` stubs. Every
 * `encoded` value is a Schema-encoded envelope (`client.ts` wire schemas for host entry
 * points, `@effect-agent/storage-cloudflare` port envelopes for `portCall`), so the RPC
 * boundary carries only structured-cloneable JSON. The optional trailing trace context is
 * transient native RPC metadata, stripped by an opted-in effect-cf receiver before decoding
 * the host envelope. It never enters durable state.
 */
export interface ThreadObjectRpc extends Rpc.DurableObjectBranded {
  /** Admission-limits gate + `DurableAgentRuntime.submit`; answers a `SubmitResponse`. */
  submitEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  /** Wake-hinted, poll-guaranteed settlement wait; answers an `AwaitSettlementResponse`. */
  awaitSettlementEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  /** Event-driven durable progress wait; answers a `ProgressObserved` host response. */
  awaitProgressEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  /** Best-effort cancellation for one in-flight progress wait. */
  cancelProgressEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  /** One bounded page of canonical records; answers an `ObservePageResponse`. */
  observePage(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  /** Durable abort intent; answers an `AbortResponse`. */
  abortEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  /** Durable approval decision (plan §2.6); answers a `ResolveApprovalResponse`. */
  resolveApprovalEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  /** Authorized DUR-017 Unknown-Outcome resolution; answers a `ResolveUnknownResponse`. */
  resolveUnknownEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  /** Owner-side cross-Object port endpoint (WP2 envelopes, executed on LOCAL facets). */
  portCall(encoded: unknown): Promise<unknown>;
  /** Droppable liveness hint from another Object: arms an immediate alarm. */
  wake(): Promise<void>;
}

/**
 * The `DurableObjectNamespace` binding that addresses Thread Objects. The Object
 * identity rule is `namespace.idFromName(threadId)` (plan §1.2): Thread IDs are
 * globally unique, so the mapping is total and deterministic and no directory service exists.
 */
export class ThreadObjectNamespace extends Context.Service<
  ThreadObjectNamespace,
  {
    readonly namespace: DurableObjectNamespace<ThreadObjectRpc>;
    /** Stable binding name for opted-in native RPC tracing; absent by default. */
    readonly rpcTracing?: string;
  }
>()("@effect-agent/platform-cloudflare/ThreadObjectNamespace") {
  static layer(
    namespace: DurableObjectNamespace<ThreadObjectRpc>,
  ): Layer.Layer<ThreadObjectNamespace> {
    return Layer.succeed(ThreadObjectNamespace)({ namespace });
  }
}

/**
 * Narrow one `env` member to a `DurableObjectNamespace`. `env` is an untyped platform value,
 * and a namespace binding is a host object no Schema can decode, so this is the documented
 * narrowest-boundary check (structural probe for the namespace surface the transport uses);
 * a missing or misshaped binding fails typed before any Layer is built.
 */
export const threadNamespaceFromEnv = Effect.fn("threadNamespaceFromEnv")(function* (
  env: unknown,
  binding: string,
): Effect.fn.Return<DurableObjectNamespace<ThreadObjectRpc>, CloudflareBindingError> {
  if (!Predicate.isObjectKeyword(env)) {
    return yield* CloudflareBindingError.make({
      binding,
      message: "The Worker environment is not an object; no bindings are available.",
    });
  }
  const candidate = yield* Effect.try({
    try: () => {
      const value: unknown = Reflect.get(env, binding);
      if (!Predicate.isObjectKeyword(value)) return undefined;
      const idFromName: unknown = Reflect.get(value, "idFromName");
      const get: unknown = Reflect.get(value, "get");
      return typeof idFromName === "function" && typeof get === "function" ? value : undefined;
    },
    catch: () =>
      CloudflareBindingError.make({
        binding,
        message: `env.${binding} could not be inspected as a DurableObjectNamespace binding.`,
      }),
  });
  if (candidate !== undefined) {
    // The structural probe above is the entire runtime contract this package relies on;
    // the assertion records that `idFromName`/`get` name a DurableObjectNamespace.
    return candidate as unknown as DurableObjectNamespace<ThreadObjectRpc>;
  }
  return yield* CloudflareBindingError.make({
    binding,
    message:
      `env.${binding} is not a DurableObjectNamespace binding; declare the Thread ` +
      "Object class under this binding in the Worker configuration.",
  });
});

/**
 * Build the namespace from Worker `env` (fails typed). Enable `rpcTracing` only when the
 * receiver also opts into the effect-cf native RPC trace-context contract.
 */
export const threadNamespaceLayer = (
  env: unknown,
  binding: string,
  options: { readonly rpcTracing?: boolean } = {},
): Layer.Layer<ThreadObjectNamespace, CloudflareBindingError> =>
  Layer.effect(ThreadObjectNamespace)(
    Effect.map(threadNamespaceFromEnv(env, binding), (namespace) => ({
      namespace,
      ...(options.rpcTracing === true ? { rpcTracing: binding } : {}),
    })),
  );

/**
 * The live Durable Object execution context of THIS incarnation. Only Layer construction and
 * the alarm service consume it; important state never lives on it (`ctx.storage` is truth,
 * everything in memory is a cache — deployment spec §11).
 */
export class DurableObjectContext extends Context.Service<
  DurableObjectContext,
  {
    readonly ctx: DurableObjectState;
    readonly env: unknown;
  }
>()("@effect-agent/platform-cloudflare/DurableObjectContext") {
  static layer(ctx: DurableObjectState, env: unknown): Layer.Layer<DurableObjectContext> {
    return Layer.succeed(DurableObjectContext)({ ctx, env });
  }
}

/**
 * The Thread identity this Object serializes and the producer identity its Attempts
 * write with (`{producerPrefix}:{threadId}`, plan §1.4). Derived once per incarnation
 * from `ctx.id.name` — the Object identity rule guarantees the name IS the Thread ID.
 */
export class ThreadObjectIdentity extends Context.Service<
  ThreadObjectIdentity,
  {
    readonly threadId: ThreadId;
    readonly producerId: ProducerId;
  }
>()("@effect-agent/platform-cloudflare/ThreadObjectIdentity") {}
