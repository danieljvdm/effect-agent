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

/** Services available to custom RPCs, including the current invocation's resources. */
export type HandlerServices<A = never, EventServices = never> =
  | Host.Services
  | Host.BootstrapServices
  | DurableObjectContext
  | ThreadObjectNamespace
  | HostServices
  | Scope.Scope
  | A
  | EventServices;

/** Constraint for an inferred registry; retain the concrete registry type for its RPC contract. */
export type Handlers<A = never, EventServices = never> = Record<
  string,
  (...args: never[]) => Effect.Effect<unknown, unknown, HandlerServices<A, EventServices>>
>;

const nativeReservedRpcNames = [
  "alarm",
  "__proto__",
  "constructor",
  "toString",
  "toLocaleString",
  "valueOf",
  "hasOwnProperty",
  "isPrototypeOf",
  "propertyIsEnumerable",
  "__defineGetter__",
  "__defineSetter__",
  "__lookupGetter__",
  "__lookupSetter__",
  "then",
  "catch",
  "finally",
  "dup",
  "id",
  "name",
  "ctx",
  "env",
  "fetch",
  "connect",
  "webSocketMessage",
  "webSocketClose",
  "webSocketError",
] as const;

type ReservedRpcName = keyof Rpc | (typeof nativeReservedRpcNames)[number];

type CustomNames<Custom> = {
  readonly [Key in keyof Custom]: Key extends string
    ? Key extends ReservedRpcName
      ? never
      : Custom[Key]
    : never;
};

const reservedRpcNames = new Set<string>([
  ...Object.keys(Host.rpc),
  ...Object.keys(Host.administrativeRpc),
  ...Object.getOwnPropertyNames(Object.prototype),
  ...nativeReservedRpcNames,
]);

/**
 * Supply this constructor to `Cloudflare.DurableObject.make`. Alchemy owns the native
 * class, initialization gate, per-instance Scope and per-event tracing. Construction
 * failures reject native initialization after its Scope is closed; alarm failures reject
 * the invocation so Cloudflare retries. The engine's durable records remain authoritative.
 *
 * Optional custom RPCs share this application's services and acquire `eventLayer` for
 * every invocation. Their Effect failures reject native RPC; encode expected application
 * outcomes explicitly when callers need a wire-level error contract. Framework methods,
 * native lifecycle methods, and reserved RPC names cannot be replaced.
 */
export const make = <
  A,
  E,
  EventServices = never,
  EventError = never,
  Custom extends Handlers<NoInfer<A>, NoInfer<EventServices>> = {},
>(
  application: Layer.Layer<
    Host.Services | A,
    E,
    Host.BootstrapServices | HostServices | DurableObjectContext | ThreadObjectNamespace
  >,
  options: Options<A, EventServices, EventError>,
  handlers?: Custom & CustomNames<Custom>,
): Constructor<Rpc<Custom>> =>
  Effect.gen(function* () {
    // Check before building application services or mutating durable storage. JS callers
    // receive the same protection as TypeScript users against shadowing host operations.
    for (const name of Reflect.ownKeys(handlers ?? {})) {
      if (typeof name !== "string" || reservedRpcNames.has(name))
        return yield* Effect.die(new Error(`Custom RPC name '${String(name)}' is reserved`));
      const descriptor = Object.getOwnPropertyDescriptor(handlers, name);

      if (!descriptor?.enumerable || typeof descriptor.value !== "function")
        return yield* Effect.die(
          new Error(`Custom RPC '${name}' must be an enumerable own method`),
        );
    }
    const customEntries = Object.entries(handlers ?? {});
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

      const wrap =
        <Value, Failure, Requirements>(
          handler: (...args: never[]) => Effect.Effect<Value, Failure, Requirements>,
        ) =>
        (...args: never[]) =>
          invoke(Effect.suspend(() => handler(...args)));

      // The registry's keys and argument tuples are unchanged; only each Effect's
      // invocation environment is supplied. This assertion crosses no wire boundary.
      const custom = Object.fromEntries(
        customEntries.map(([name, handler]) => [name, wrap(handler)]),
      ) as NativeHandlers<Custom>;

      return {
        ...custom,
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

export type Rpc<Custom = {}> = NativeHandlers<typeof Host.rpc & typeof Host.administrativeRpc> & {
  alarm: () => Effect.Effect<void, never, Scope.Scope>;
} & NativeHandlers<Custom>;
