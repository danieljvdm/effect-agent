import {
  threadNamespaceLayer,
  type DurableObjectContext,
  type ThreadObjectNamespace,
} from "@effect-agent/platform-cloudflare/cloudflare-host-bindings";
import * as Host from "@effect-agent/platform-cloudflare/thread-object-host";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Effect, Layer, type Scope } from "effect";

import {
  acquire,
  platformLayer,
  type EventOptions,
  type HostServices,
  type Constructor,
  type NativeHandlers,
} from "./internal/runtime.ts";

export {
  layer,
  layerConfig,
  layerHostConfig,
  layerInHost,
  handleRpc,
  submit,
  portCall,
  type Services,
  type InitializationError,
  type BootstrapServices,
} from "@effect-agent/platform-cloudflare/thread-object-host";

export interface Options<A = never, EventServices = never, EventError = never>
  extends
    Host.RuntimeOptions,
    EventOptions<
      Host.Services | Host.BootstrapServices | DurableObjectContext | ThreadObjectNamespace | A,
      EventServices,
      EventError
    > {
  /** Alchemy's resource/binding identifier; keep it stable for existing stored Threads. */
  readonly namespaceBinding: string;
}

/**
 * Supply this constructor to `Cloudflare.DurableObject.make`. Alchemy owns the native
 * class, initialization gate, per-instance Scope and per-event tracing. Construction
 * failures reject native initialization after its Scope is closed; alarm failures reject
 * the invocation so Cloudflare retries. The engine's durable records remain authoritative.
 */
export const make = <A, E, EventServices = never, EventError = never>(
  application: Layer.Layer<
    Host.Services | A,
    E,
    Host.BootstrapServices | HostServices | DurableObjectContext | ThreadObjectNamespace
  >,
  options: Options<A, EventServices, EventError>,
): Constructor<Rpc> =>
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;

    return Effect.gen(function* () {
      const namespace = Layer.unwrap(
        Effect.map(WorkerEnvironment, (env) => threadNamespaceLayer(env, options.namespaceBinding)),
      );

      const runtime = Host.makeRuntime(application, options).pipe(
        Layer.provideMerge(platformLayer),
        Layer.provideMerge(namespace),
      );

      const { services, invoke } = yield* acquire(runtime, options);

      yield* Host.initialize.pipe(Effect.provide(services));

      return {
        submitEncoded: (encoded: unknown) => invoke(Host.rpc.submitEncoded(encoded)),
        submissionStatusEncoded: (encoded: unknown) =>
          invoke(Host.rpc.submissionStatusEncoded(encoded)),
        awaitSettlementEncoded: (encoded: unknown) =>
          invoke(Host.rpc.awaitSettlementEncoded(encoded)),
        awaitProgressEncoded: (encoded: unknown) => invoke(Host.rpc.awaitProgressEncoded(encoded)),
        cancelProgressEncoded: (encoded: unknown) =>
          invoke(Host.rpc.cancelProgressEncoded(encoded)),
        observePage: (encoded: unknown) => invoke(Host.rpc.observePage(encoded)),
        abortEncoded: (encoded: unknown) => invoke(Host.rpc.abortEncoded(encoded)),
        resolveApprovalEncoded: (encoded: unknown) =>
          invoke(Host.rpc.resolveApprovalEncoded(encoded)),
        resolveUnknownEncoded: (encoded: unknown) =>
          invoke(Host.rpc.resolveUnknownEncoded(encoded)),
        portCall: (encoded: unknown) => invoke(Host.rpc.portCall(encoded)),
        wake: () => invoke(Host.rpc.wake()),
        explainEncoded: (encoded: unknown) =>
          invoke(Host.administrativeRpc.explainEncoded(encoded)),
        verifyEncoded: (encoded: unknown) => invoke(Host.administrativeRpc.verifyEncoded(encoded)),
        retryEncoded: (encoded: unknown) => invoke(Host.administrativeRpc.retryEncoded(encoded)),
        obligationsEncoded: (encoded: unknown) =>
          invoke(Host.administrativeRpc.obligationsEncoded(encoded)),
        alarm: () => invoke(Host.alarm).pipe(Effect.orDie),
      };
    }).pipe(Effect.provideService(WorkerEnvironment, env), Effect.orDie);
  });

export type Rpc = NativeHandlers<typeof Host.rpc & typeof Host.administrativeRpc> & {
  alarm: () => Effect.Effect<void, never, Scope.Scope>;
};
