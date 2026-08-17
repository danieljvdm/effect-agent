import type { ConversationId } from "@effect-agent/core";
import type { ProducerId } from "@effect-agent/session";
import { Context, Effect, Layer, Schema } from "effect";

/**
 * Cloudflare platform bindings as Effect services (DEPLOY-010: "Cloudflare platform bindings
 * are supplied as Effect services/Layers"). Application code never reads `env` or touches a
 * `DurableObjectState` directly — the Conversation Object class constructs these Layers once
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
 * The RPC surface one Conversation Durable Object exposes to Workers and to sibling
 * Conversation Objects. `makeConversationObjectClass` implements it; the Worker-side client
 * and the cross-Object transport call it through `DurableObjectNamespace` stubs. Every
 * `encoded` value is a Schema-encoded envelope (`client.ts` wire schemas for host entry
 * points, `@effect-agent/storage-cloudflare` port envelopes for `portCall`), so the RPC
 * boundary carries only structured-cloneable JSON.
 */
export interface ConversationObjectRpc extends Rpc.DurableObjectBranded {
  /** Admission-limits gate + `DurableAgentRuntime.submit`; answers a `SubmitResponse`. */
  submitEncoded(encoded: unknown): Promise<unknown>;
  /** Wake-hinted, poll-guaranteed settlement wait; answers an `AwaitSettlementResponse`. */
  awaitSettlementEncoded(encoded: unknown): Promise<unknown>;
  /** Event-driven durable progress wait; answers a `ProgressObserved` host response. */
  awaitProgressEncoded(encoded: unknown): Promise<unknown>;
  /** Best-effort cancellation for one in-flight progress wait. */
  cancelProgressEncoded(encoded: unknown): Promise<unknown>;
  /** One bounded page of canonical records; answers an `ObservePageResponse`. */
  observePage(encoded: unknown): Promise<unknown>;
  /** Durable abort intent; answers an `AbortResponse`. */
  abortEncoded(encoded: unknown): Promise<unknown>;
  /** Durable approval decision (plan §2.6); answers a `ResolveApprovalResponse`. */
  resolveApprovalEncoded(encoded: unknown): Promise<unknown>;
  /** Authorized DUR-017 Unknown-Outcome resolution; answers a `ResolveUnknownResponse`. */
  resolveUnknownEncoded(encoded: unknown): Promise<unknown>;
  /** Owner-side cross-Object port endpoint (WP2 envelopes, executed on LOCAL facets). */
  portCall(encoded: unknown): Promise<unknown>;
  /** Droppable liveness hint from another Object: arms an immediate alarm. */
  wake(): Promise<void>;
}

/**
 * The `DurableObjectNamespace` binding that addresses Conversation Objects. The Object
 * identity rule is `namespace.idFromName(conversationId)` (plan §1.2): Conversation IDs are
 * globally unique, so the mapping is total and deterministic and no directory service exists.
 */
export class ConversationObjectNamespace extends Context.Service<
  ConversationObjectNamespace,
  {
    readonly namespace: DurableObjectNamespace<ConversationObjectRpc>;
  }
>()("@effect-agent/platform-cloudflare/ConversationObjectNamespace") {
  static layer(
    namespace: DurableObjectNamespace<ConversationObjectRpc>,
  ): Layer.Layer<ConversationObjectNamespace> {
    return Layer.succeed(ConversationObjectNamespace)({ namespace });
  }
}

/**
 * Narrow one `env` member to a `DurableObjectNamespace`. `env` is an untyped platform value,
 * and a namespace binding is a host object no Schema can decode, so this is the documented
 * narrowest-boundary check (structural probe for the namespace surface the transport uses);
 * a missing or misshaped binding fails typed before any Layer is built.
 */
export const conversationNamespaceFromEnv = (
  env: unknown,
  binding: string,
): Effect.Effect<DurableObjectNamespace<ConversationObjectRpc>, CloudflareBindingError> =>
  Effect.suspend(() => {
    if (typeof env !== "object" || env === null) {
      return Effect.fail(
        CloudflareBindingError.make({
          binding,
          message: "The Worker environment is not an object; no bindings are available.",
        }),
      );
    }
    const candidate: unknown = (env as Record<string, unknown>)[binding];
    if (
      typeof candidate === "object" &&
      candidate !== null &&
      "idFromName" in candidate &&
      typeof candidate.idFromName === "function" &&
      "get" in candidate &&
      typeof candidate.get === "function"
    ) {
      // The structural probe above is the entire runtime contract this package relies on;
      // the assertion records that `idFromName`/`get` name a DurableObjectNamespace.
      return Effect.succeed(candidate as DurableObjectNamespace<ConversationObjectRpc>);
    }
    return Effect.fail(
      CloudflareBindingError.make({
        binding,
        message:
          `env.${binding} is not a DurableObjectNamespace binding; declare the Conversation ` +
          "Object class under this binding in the Worker configuration.",
      }),
    );
  });

/** `ConversationObjectNamespace` built from the untyped Worker `env` (fails typed). */
export const conversationNamespaceLayer = (
  env: unknown,
  binding: string,
): Layer.Layer<ConversationObjectNamespace, CloudflareBindingError> =>
  Layer.effect(ConversationObjectNamespace)(
    Effect.map(conversationNamespaceFromEnv(env, binding), (namespace) => ({ namespace })),
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
 * The Conversation identity this Object serializes and the producer identity its Attempts
 * write with (`{producerPrefix}:{conversationId}`, plan §1.4). Derived once per incarnation
 * from `ctx.id.name` — the Object identity rule guarantees the name IS the Conversation ID.
 */
export class ConversationObjectIdentity extends Context.Service<
  ConversationObjectIdentity,
  {
    readonly conversationId: ConversationId;
    readonly producerId: ProducerId;
  }
>()("@effect-agent/platform-cloudflare/ConversationObjectIdentity") {}
