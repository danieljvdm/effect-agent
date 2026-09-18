import { Effect, Layer } from "effect";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectState as EffectCfDurableObjectState,
  WorkerEnvironment,
} from "effect-cf";

import { type MaintenancePassFailure } from "./Alarm.ts";
import {
  DurableObjectContext,
  ThreadObjectNamespace,
  threadNamespaceFromEnv,
  type CloudflareBindingError,
} from "./CloudflareHostBindings.ts";
import { effectCfRpcLayer } from "./internal/effect-cf-rpc.ts";
import {
  layerConfig,
  type CloudflareDurableRuntimeInitializationError,
  type CloudflareDurableRuntimeOptions,
  type CloudflareDurableRuntimeServices,
  type CloudflareBootstrapServices,
} from "./internal/layers.ts";
import * as Host from "./ThreadObjectHost.ts";
export * from "./ThreadObjectHost.ts";

/** Construction options for one deployed Thread Object class. */
export interface Options<
  ApplicationServices = never,
  EventServices = never,
  EventLayerError = never,
> extends CloudflareDurableRuntimeOptions {
  /** Accept transient native RPC tracing through effect-cf; disabled by default. */
  readonly rpcTracing?: boolean;
  /**
   * Name of the Worker `env` binding carrying THIS class's `DurableObjectNamespace` — the
   * Object's route back to sibling Thread Objects for the WP2 cross-Object port calls
   * and remote wakes (DEPLOY-010: the binding enters through a Layer, never ambiently).
   */
  readonly namespaceBinding: string;
  /** Acquired and finalized per native event, with access to the complete application runtime. */
  readonly eventLayer?: Layer.Layer<
    EventServices,
    EventLayerError,
    | RuntimeServices
    | ApplicationServices
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
  >;
}

type EndpointServices =
  | CloudflareDurableRuntimeServices
  | CloudflareBootstrapServices
  | DurableObjectContext;
type RuntimeServices = EndpointServices | ThreadObjectNamespace;
type ThreadObjectInitializationError =
  | CloudflareDurableRuntimeInitializationError
  | CloudflareBindingError
  | MaintenancePassFailure;

/**
 * Adapter from effect-cf's native Durable Object services to Effect Agent's existing platform
 * ports. effect-cf owns the cached ManagedRuntime and supplies these values once per Object
 * incarnation; the durable runtime continues to depend only on the narrow services below.
 */
const effectCfPlatformLayer = (
  namespaceBinding: string,
  rpcTracing = false,
): Layer.Layer<
  DurableObjectContext | ThreadObjectNamespace,
  CloudflareBindingError,
  EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
> => {
  const context = Layer.effect(DurableObjectContext)(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const env = yield* WorkerEnvironment;

      return DurableObjectContext.of({ ctx: state.raw, env });
    }),
  );

  const namespace = Layer.effect(ThreadObjectNamespace)(
    Effect.gen(function* () {
      const env = yield* WorkerEnvironment;
      const binding = yield* threadNamespaceFromEnv(env, namespaceBinding);

      return ThreadObjectNamespace.of({
        get: (threadId) => binding.get(binding.idFromName(threadId)),
        ...(rpcTracing === true ? { rpcTracing: namespaceBinding } : {}),
      });
    }),
  );

  return Layer.merge(context, namespace);
};

/** The public endpoints and effect-cf invocation hook of one Thread Object instance. */
export interface Instance<EventServices = never> extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<Record<never, never>, RuntimeServices | EventServices>
> {
  submitEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  submissionStatusEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  awaitSettlementEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  awaitProgressEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  cancelProgressEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  observePage(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  abortEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  resolveApprovalEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  resolveUnknownEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  explainEncoded(encoded: unknown): Promise<unknown>;
  verifyEncoded(encoded: unknown): Promise<unknown>;
  retryEncoded(encoded: unknown): Promise<unknown>;
  obligationsEncoded(encoded: unknown): Promise<unknown>;
  portCall(encoded: unknown): Promise<unknown>;
  wake(): Promise<void>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

/** The constructor shape workerd instantiates for each Thread Object. */
export interface Class<EventServices = never> {
  new (ctx: DurableObjectState, env: Cloudflare.Env): Instance<EventServices>;
}

/**
 * Export a composed application Layer as a native Durable Object class.
 * Bootstrap services are provided to the whole graph before it acquires, so application Layers
 * can yield effect-cf's WorkerEnvironment and DurableObjectState, derived identity, and Crypto.
 * Effect Config reads scalar Worker vars and secrets through effect-cf's environment provider;
 * WorkerEnvironment exposes resource bindings without a separate config Layer.
 * Application dependencies remain visible until Layer.provide satisfies them. effect-cf owns the
 * cached ManagedRuntime, native RPC methods, event scopes, and telemetry flushing.
 * Initialization is local and bounded inside the constructor gate. Cloudflare eviction does not
 * guarantee finalizers; put resources requiring timely release in scoped operations or eventLayer.
 */
export const make = <
  ApplicationServices,
  ApplicationError,
  EventServices = never,
  EventLayerError = never,
>(
  applicationLayer: Layer.Layer<
    CloudflareDurableRuntimeServices | ApplicationServices,
    ApplicationError,
    | CloudflareBootstrapServices
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
    | DurableObjectContext
    | ThreadObjectNamespace
  >,
  options: Options<ApplicationServices, EventServices, EventLayerError>,
): Class<ApplicationServices | EventServices> => {
  const application = applicationLayer.pipe(
    Layer.provideMerge(layerConfig(options)),
    Layer.provideMerge(effectCfPlatformLayer(options.namespaceBinding, options.rpcTracing)),
    Layer.provideMerge(effectCfRpcLayer),
  );

  // The storage/config Layer must acquire inside Cloudflare's constructor gate. effect-cf owns
  // the ManagedRuntime, while this effectContext ensures its first Layer build enters the gate
  // before migration, compatibility checks, or alarm inspection touch Object storage.
  const runtime: Layer.Layer<
    RuntimeServices | ApplicationServices,
    ThreadObjectInitializationError | ApplicationError,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
  > = Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const scope = yield* Effect.scope;

      return yield* state.blockConcurrencyWhile(
        Effect.gen(function* () {
          const services = yield* Layer.buildWithScope(application, scope);

          yield* Host.initialize.pipe(Effect.provide(services));

          return services;
        }),
      );
    }),
  );

  const rpc = {
    ...Host.rpc,
    explainEncoded: (encoded: unknown) => Host.administrativeRpc.explainEncoded(encoded),
    verifyEncoded: (encoded: unknown) => Host.administrativeRpc.verifyEncoded(encoded),
    retryEncoded: (encoded: unknown) => Host.administrativeRpc.retryEncoded(encoded),
    obligationsEncoded: (encoded: unknown) => Host.administrativeRpc.obligationsEncoded(encoded),
  } satisfies EffectCfDurableObject.DurableObjectRpc<
    RuntimeServices | ApplicationServices | EventServices
  >;

  type NativeOptions = EffectCfDurableObject.DurableObjectOptions<
    RuntimeServices | ApplicationServices,
    EventServices,
    EventLayerError,
    typeof rpc
  >;

  const EffectCfThreadObject = EffectCfDurableObject.make<
    RuntimeServices | ApplicationServices,
    ThreadObjectInitializationError | ApplicationError,
    EventServices,
    EventLayerError,
    typeof rpc
  >(runtime, {
    ...(options.rpcTracing === true ? { rpcTracing: { service: options.namespaceBinding } } : {}),
    ...(options.eventLayer === undefined ? {} : { eventLayer: options.eventLayer }),
    // Force the gated runtime Layer when Cloudflare loads this Object incarnation. Recovery stays
    // in each bounded pass so cross-Object initialization cannot deadlock.
    initialize: Effect.void,
    rpc,
    alarm: () => Host.alarm,
    // This host owns the raw alarm and supplies event services through options.eventLayer.
    // Upstream's conditional alarm-registration check cannot reduce over generic application
    // services. Options and the rpc satisfies check above retain their Effect requirements.
  } as NativeOptions);

  // effect-cf's class type keeps `alarm` optional even when the handler option is present. This
  // concrete override reflects this factory's stronger contract while delegating execution to
  // the effect-cf runtime unchanged.
  class ThreadObject extends EffectCfThreadObject {
    override alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void {
      return super.alarm?.(alarmInfo);
    }
  }

  return ThreadObject;
};
