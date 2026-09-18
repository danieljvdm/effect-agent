import { type DoMemoryStorageLimits } from "@effect-agent/storage-cloudflare/do-memory-store";
import {
  type MemoryOwnerIdentity,
  type MemoryOwnerAuthorizer,
  type MemoryRpcLimits,
  defaultMemoryRpcLimits,
  type MemoryOwnerFailure,
} from "@effect-agent/storage-cloudflare/memory-protocol";
import { Effect, Layer } from "effect";
import type * as MemoryNamespace from "effect-agent/memory-namespace";
import { type MemoryAccess } from "effect-agent/memory-revalidation";
import type { MemoryMutationFailpoint } from "effect-agent/memory-store";
import { type Principal } from "effect-agent/submission-ledger";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectState,
  WorkerEnvironment,
} from "effect-cf";

import { DurableObjectContext } from "./CloudflareHostBindings.ts";
import { effectCfRpcLayer } from "./internal/effect-cf-rpc.ts";
import * as Host from "./MemoryObjectHost.ts";
export * from "./MemoryObjectHost.ts";

export const CloudflareMemoryClient = {
  make: <Namespace extends MemoryNamespace.Any>(
    access: MemoryAccess<Namespace>,
    principal: Principal,
    limits = defaultMemoryRpcLimits,
  ) =>
    Host.CloudflareMemoryClient.make(access, principal, limits).pipe(
      Effect.provide(effectCfRpcLayer),
    ),
  fromBinding: <Namespace extends MemoryNamespace.Any>(
    binding: DurableObjectNamespace<Host.MemoryObjectRpc>,
    options: {
      readonly access: MemoryAccess<Namespace>;
      readonly principal: Principal;
      readonly rpcLimits?: MemoryRpcLimits;
    },
  ) =>
    Host.CloudflareMemoryClient.fromBinding(binding, options).pipe(
      Effect.provide(effectCfRpcLayer),
    ),
};

export const cloudflareMemoryWriterLayer = (
  access: MemoryAccess,
  principal: Principal,
  limits = defaultMemoryRpcLimits,
) =>
  Host.cloudflareMemoryWriterLayer(access, principal, limits).pipe(Layer.provide(effectCfRpcLayer));

export interface MemoryObjectInstance extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<Record<never, never>, Host.Services>
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
  const application = Host.makeRuntime(host, options).pipe(
    Layer.provideMerge(
      Layer.effect(DurableObjectContext)(
        Effect.gen(function* () {
          const state = yield* DurableObjectState.DurableObjectState;

          return { ctx: state.raw, env: yield* WorkerEnvironment };
        }),
      ),
    ),
    Layer.provideMerge(effectCfRpcLayer),
  );

  const runtime: Layer.Layer<
    Host.Services,
    E | MemoryOwnerFailure,
    DurableObjectState.DurableObjectState | WorkerEnvironment
  > = Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* DurableObjectState.DurableObjectState;
      const scope = yield* Effect.scope;

      return yield* state.blockConcurrencyWhile(Layer.buildWithScope(application, scope));
    }),
  );

  const rpc = Host.rpc(options.rpcLimits);

  return EffectCfDurableObject.make<
    Host.Services,
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
