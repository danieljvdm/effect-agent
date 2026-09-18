import * as Host from "@effect-agent/platform-cloudflare/memory-object-host";
import { type DoMemoryStorageLimits } from "@effect-agent/storage-cloudflare/do-memory-store";
import {
  type MemoryOwnerAuthorizer,
  type MemoryOwnerIdentity,
  type MemoryRpcLimits,
} from "@effect-agent/storage-cloudflare/memory-protocol";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Effect, Layer } from "effect";
import { type MemoryMutationFailpoint } from "effect-agent/memory-store";

import {
  acquire,
  platformLayer,
  type HostServices,
  type Constructor,
  type NativeHandlers,
} from "./internal/runtime.ts";

export {
  CloudflareMemoryClient,
  cloudflareMemoryWriterLayer,
  memoryObjectName,
} from "@effect-agent/platform-cloudflare/memory-object-host";

/** Compose the existing Memory owner behind an Alchemy Durable Object declaration. */
export const make = <E>(
  host: Layer.Layer<MemoryOwnerAuthorizer, E, MemoryOwnerIdentity | HostServices>,
  options: {
    readonly rpcLimits?: MemoryRpcLimits;
    readonly storageLimits?: DoMemoryStorageLimits;
    readonly failpoints?: Layer.Layer<MemoryMutationFailpoint, never, HostServices>;
  } = {},
): Constructor<Rpc> =>
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;

    return Effect.gen(function* () {
      const { invoke } = yield* acquire(
        Host.makeRuntime(host, options).pipe(Layer.provideMerge(platformLayer)),
      );

      const rpc = Host.rpc(options.rpcLimits);

      return { memory: (encoded: string) => invoke(rpc.memory(encoded)) };
    }).pipe(Effect.provideService(WorkerEnvironment, env), Effect.orDie);
  });

export type Rpc = NativeHandlers<ReturnType<typeof Host.rpc>>;
