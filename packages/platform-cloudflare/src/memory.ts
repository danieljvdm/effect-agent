import {
  type MemoryLookup,
  type MemoryReader,
  type MemoryWrite,
  Memory,
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
import {
  type DoMemoryStorageLimits,
  type MemoryOwnerAuthorizer,
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
import { Principal } from "@effect-agent/thread";
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
 * Effect-native, host-bound memory client. Recall revalidates the entire admitted lookup
 * in one RPC and renders it locally. No retries or per-source splitting occur here.
 * Interrupted callers stop waiting; the owner has its own deadline. A timed-out write
 * may have committed: reconcile by sending the identical operation ID and command.
 */
const makeMemoryClient = Effect.fn("CloudflareMemoryClient.make")(function* <
  Namespace extends MemoryNamespace.Any,
>(
  access: MemoryAccess<Namespace>,
  principal: Principal,
  rpcLimits: MemoryRpcLimits = defaultMemoryRpcLimits,
) {
  const validated = yield* Schema.decodeUnknownEffect(MemoryRpcLimits)(rpcLimits).pipe(
    Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
  );

  const bound = yield* Schema.decodeUnknownEffect(MemoryAccess.Wire)(access).pipe(
    Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
  );

  principal = yield* Schema.decodeUnknownEffect(Principal)(principal).pipe(
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

  /**
   * Revalidate in one owner RPC, then render whole passages within the caller's budget.
   * The bound source is essential: unavailable/stale results and matches that cannot fit
   * fail instead of silently producing empty context. No-match remains successful.
   * The single outcome has sourceId "memory". No embedding or candidate search is performed.
   * Use revalidate with Memory.recall for multiple readers sharing one output budget.
   */
  const recall = Effect.fn("CloudflareMemoryClient.recall")(function* (
    lookup: MemoryLookup,
    limits: MemoryRecallLimits,
    estimateTokens?: (text: string) => number,
  ) {
    return yield* Memory.recall(
      [{ id: "memory", essential: true, read: revalidate(lookup, limits) }],
      limits,
      estimateTokens,
    );
  });

  return { recall, revalidate, revalidateSemantic, change };
});

export const CloudflareMemoryClient = {
  /** Bind access and principal using the MemoryObjectNamespace supplied by the application. */
  make: makeMemoryClient,
  /** Use a resolved Worker or Durable Object binding without manual service provisioning. */
  fromBinding: Effect.fn("CloudflareMemoryClient.fromBinding")(function* <
    Namespace extends MemoryNamespace.Any,
  >(
    binding: DurableObjectNamespace<MemoryObjectRpc>,
    options: {
      readonly access: MemoryAccess<Namespace>;
      readonly principal: Principal;
      readonly rpcLimits?: MemoryRpcLimits;
    },
  ) {
    return yield* makeMemoryClient(options.access, options.principal, options.rpcLimits).pipe(
      Effect.provideService(MemoryObjectNamespace, { namespace: binding }),
    );
  }),
};

/**
 * Optional activity-processor destination. Keeps domain write errors intact; transport,
 * authorization and deadline failures become the existing MemoryStorageError contract.
 * Receipts remain authoritative, including after caller interruption or lost replies.
 */
export const cloudflareMemoryWriterLayer = (
  access: MemoryAccess,
  principal: Principal,
  limits: MemoryRpcLimits = defaultMemoryRpcLimits,
) =>
  Layer.effect(
    MemoryWriter,
    Effect.gen(function* () {
      const client = yield* CloudflareMemoryClient.make(access, principal, limits);

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
const makeMemoryObject = <E>(
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

  const rpc = { memory: (encoded: string) => handleMemoryOwnerRequest(encoded, options.rpcLimits) };

  return EffectCfDurableObject.make<
    OwnerServices,
    E | MemoryOwnerFailure,
    never,
    never,
    typeof rpc
  >(runtime, { rpc });
};

export const MemoryObject = {
  /** Build the SQLite Durable Object class with the application's owner authorization Layer. */
  make: makeMemoryObject,
};
