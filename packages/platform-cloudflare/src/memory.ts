import type { MemoryLookup, MemoryReader, MemoryWrite } from "@effect-agent/core";
import {
  MemoryAccess,
  MemoryDocument,
  MemoryMutationFailpoint,
  MemoryNamespace,
  MemoryNamespaceAddress,
  MemoryRecallLimits,
  MemoryStorageError,
  MemoryWriter,
  type MemoryIndexSearch,
  type SemanticCandidateLimits,
  type SemanticMemoryProfile,
} from "@effect-agent/core";
import type {
  DoMemoryStorageLimits,
  MemoryOwnerAuthorizer,
} from "@effect-agent/storage-cloudflare";
import {
  decodeMemoryWire,
  defaultDoMemoryStorageLimits,
  defaultMemoryRpcLimits,
  doMemoryStoreLayerWithFailpoints,
  encodeMemoryWire,
  handleMemoryOwnerRequest,
  MemoryOwnerIdentity,
  MemoryOwnerRequest,
  MemoryOwnerResponse,
  MemoryRpcError,
  MemoryRpcLimits,
  type MemoryOwnerFailure,
} from "@effect-agent/storage-cloudflare";
import { Clock, Context, Effect, Layer, Schema } from "effect";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectState,
  type WorkerEnvironment,
} from "effect-cf";

export interface MemoryObjectRpc extends Rpc.DurableObjectBranded {
  memory(encoded: string): Promise<string>;
}

export class MemoryObjectNamespace extends Context.Service<
  MemoryObjectNamespace,
  {
    readonly namespace: DurableObjectNamespace<MemoryObjectRpc>;
  }
>()("@effect-agent/platform-cloudflare/MemoryObjectNamespace") {}

/** Namespace version and identity are already canonicalized by MemoryNamespace. */
export const memoryObjectName = (namespace: MemoryNamespace.Any): string => namespace.address;

/**
 * Effect-native, host-bound memory client. Revalidate the entire admitted lookup once,
 * then supply its result to recallMemory. No retries or per-source splitting occur here.
 * Interrupted callers stop waiting; the owner has its own deadline. A timed-out write
 * may have committed: reconcile by sending the identical operation ID and command.
 */
export const makeCloudflareMemoryClient = Effect.fn("makeCloudflareMemoryClient")(function* <
  Namespace extends MemoryNamespace.Any,
>(
  access: MemoryAccess<Namespace>,
  principal: string,
  rpcLimits: MemoryRpcLimits = defaultMemoryRpcLimits,
) {
  const validated = yield* Schema.decodeUnknownEffect(MemoryRpcLimits)(rpcLimits).pipe(
    Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
  );
  const bound = yield* Schema.decodeUnknownEffect(MemoryAccess.Wire)(access).pipe(
    Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
  );
  const { namespace } = yield* MemoryObjectNamespace;
  const call = Effect.fn("CloudflareMemoryClient.call")(function* (request: MemoryOwnerRequest) {
    const decoded = yield* Schema.decodeUnknownEffect(MemoryOwnerRequest)(request).pipe(
      Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
    );
    const encoded = yield* encodeMemoryWire(MemoryOwnerRequest, decoded, validated.maxRequestBytes);
    const raw = yield* Effect.tryPromise({
      try: () =>
        namespace.get(namespace.idFromName(memoryObjectName(bound.namespace))).memory(encoded),
      catch: () => MemoryRpcError.make({ reason: "unavailable" }),
    });
    const response = yield* decodeMemoryWire(MemoryOwnerResponse, raw, validated.maxResponseBytes);
    if (response._tag === "Failed") return yield* response.failure;
    if (
      !MemoryNamespace.equals(response.access.namespace, bound.namespace) ||
      response.access.scope !== bound.scope
    )
      return yield* MemoryRpcError.make({ reason: "protocol" });
    return response;
  });
  const withinDeadline = <A, E, R>(effect: Effect.Effect<A, E, R>, timeoutMillis: number) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: timeoutMillis,
        orElse: () => Effect.fail(MemoryRpcError.make({ reason: "timeout" })),
      }),
    );
  const revalidate = Effect.fn("CloudflareMemoryClient.revalidate")(function* (
    lookup: MemoryLookup,
    limits: MemoryRecallLimits,
  ) {
    limits = yield* Schema.decodeUnknownEffect(MemoryRecallLimits)(limits).pipe(
      Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
    );
    const timeoutMillis = Math.min(validated.timeoutMillis, limits.timeoutMillis);
    return yield* Effect.gen(function* () {
      const response = yield* call({
        _tag: "Revalidate",
        version: 1,
        access: bound,
        principal,
        lookup,
        limits,
        deadlineMillis: (yield* Clock.currentTimeMillis) + timeoutMillis,
      });
      if (response._tag !== "Lookup") return yield* MemoryRpcError.make({ reason: "protocol" });
      return response.lookup;
    }).pipe((effect) => withinDeadline(effect, timeoutMillis));
  });
  const change = Effect.fn("CloudflareMemoryClient.change")(function* (
    write: MemoryWrite<Namespace>,
  ) {
    if (!MemoryNamespace.equals(write.key.namespace, bound.namespace))
      return yield* MemoryRpcError.make({ reason: "denied" });
    return yield* Effect.gen(function* () {
      const response = yield* call({
        _tag: "Change",
        version: 1,
        access: bound,
        principal,
        write,
        deadlineMillis: (yield* Clock.currentTimeMillis) + validated.timeoutMillis,
      });
      if (response._tag !== "Changed" || response.document.key.id !== write.key.id)
        return yield* MemoryRpcError.make({ reason: "protocol" });
      return yield* MemoryDocument.restore(access.namespace, response.document);
    }).pipe((effect) => withinDeadline(effect, validated.timeoutMillis));
  });
  const revalidateSemantic = Effect.fn("CloudflareMemoryClient.revalidateSemantic")(function* (
    found: MemoryIndexSearch<Namespace>,
    profile: SemanticMemoryProfile,
    limits: SemanticCandidateLimits,
  ) {
    return yield* Effect.gen(function* () {
      const response = yield* call({
        _tag: "RevalidateSemantic",
        version: 1,
        access: bound,
        principal,
        found,
        profile,
        limits,
        deadlineMillis: (yield* Clock.currentTimeMillis) + validated.timeoutMillis,
      });
      if (response._tag !== "Semantic") return yield* MemoryRpcError.make({ reason: "protocol" });
      return response.result;
    }).pipe((effect) => withinDeadline(effect, validated.timeoutMillis));
  });
  return { revalidate, revalidateSemantic, change };
});

/**
 * Optional activity-processor destination. Keeps domain write errors intact; transport,
 * authorization and deadline failures become the existing MemoryStorageError contract.
 * Receipts remain authoritative, including after caller interruption or lost replies.
 */
export const cloudflareMemoryWriterLayer = (
  access: MemoryAccess,
  principal: string,
  limits: MemoryRpcLimits = defaultMemoryRpcLimits,
) =>
  Layer.effect(
    MemoryWriter,
    Effect.gen(function* () {
      const client = yield* makeCloudflareMemoryClient(access, principal, limits);
      return MemoryWriter.fromAdapter({
        change: (write) =>
          client.change(write).pipe(
            Effect.catchTag("MemoryRpcError", (error) =>
              Effect.fail(
                MemoryStorageError.make({
                  operation: `memory RPC ${error.reason}`,
                  reason:
                    error.reason === "unavailable" || error.reason === "timeout"
                      ? "unavailable"
                      : "invalid-input",
                }),
              ),
            ),
            Effect.catchTag(["MemoryRecallError", "MemoryIndexError", "SemanticMemoryError"], () =>
              Effect.fail(
                MemoryStorageError.make({ operation: "memory RPC response", reason: "corrupt" }),
              ),
            ),
          ),
      });
    }),
  );

type OwnerServices = MemoryReader | MemoryWriter | MemoryOwnerAuthorizer | MemoryOwnerIdentity;
export interface MemoryObjectInstance extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<Record<never, never>, OwnerServices>
> {
  memory(encoded: string): Promise<string>;
}
export interface MemoryObjectClass {
  new (ctx: globalThis.DurableObjectState, env: Cloudflare.Env): MemoryObjectInstance;
}

/**
 * Dedicated SQLite owner, independent of Thread lifetimes. The host binds authorization
 * after restoring its namespace definition from MemoryOwnerIdentity. Do not retain
 * cleanup-scoped resources in the host Layer; it lives for the DO incarnation.
 */
export const makeMemoryObjectClass = <E>(
  host: Layer.Layer<
    MemoryOwnerAuthorizer,
    E,
    MemoryOwnerIdentity | DurableObjectState.DurableObjectState | WorkerEnvironment
  >,
  options: {
    readonly storageLimits?: DoMemoryStorageLimits;
    readonly rpcLimits?: MemoryRpcLimits;
    readonly failpoints?: Layer.Layer<
      MemoryMutationFailpoint,
      never,
      DurableObjectState.DurableObjectState
    >;
  } = {},
): MemoryObjectClass => {
  const identity = Layer.effect(
    MemoryOwnerIdentity,
    Effect.gen(function* () {
      const state = yield* DurableObjectState.DurableObjectState;
      const address = yield* Schema.decodeUnknownEffect(MemoryNamespaceAddress)(
        state.raw.id.name,
      ).pipe(Effect.mapError(() => MemoryRpcError.make({ reason: "denied" })));
      return { namespace: MemoryNamespace.Any.make({ address }) };
    }),
  );
  const store = Layer.unwrap(
    Effect.map(DurableObjectState.DurableObjectState, (state) =>
      doMemoryStoreLayerWithFailpoints(
        state.raw.storage,
        options.storageLimits ?? defaultDoMemoryStorageLimits,
      ),
    ),
  ).pipe(Layer.provide(options.failpoints ?? MemoryMutationFailpoint.layer));
  const application = Layer.merge(store, host).pipe(Layer.provideMerge(identity));
  const runtime: Layer.Layer<
    OwnerServices,
    E | MemoryOwnerFailure,
    DurableObjectState.DurableObjectState | WorkerEnvironment
  > = Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* DurableObjectState.DurableObjectState;
      const scope = yield* Effect.scope;
      yield* Schema.decodeUnknownEffect(MemoryRpcLimits)(
        options.rpcLimits ?? defaultMemoryRpcLimits,
      ).pipe(Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })));
      return yield* state.blockConcurrencyWhile(Layer.buildWithScope(application, scope));
    }),
  );
  return EffectCfDurableObject.make(runtime, {
    rpc: { memory: (encoded: string) => handleMemoryOwnerRequest(encoded, options.rpcLimits) },
  });
};
