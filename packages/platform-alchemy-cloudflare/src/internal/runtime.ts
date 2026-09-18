import { DurableObjectContext } from "@effect-agent/platform-cloudflare/cloudflare-host-bindings";
import { DurableObjectState } from "alchemy/Cloudflare/Workers/DurableObjectState";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Context, Effect, Layer, Scope } from "effect";

import * as Rpc from "../Rpc.ts";

export type HostServices = DurableObjectState | WorkerEnvironment;

/** Raw host objects cross only the platform boundary; persisted values keep their Schemas. */
export const platformLayer = Layer.effect(DurableObjectContext)(
  Effect.gen(function* () {
    const state = yield* DurableObjectState;
    // Alchemy imports Workers types as a module; the shared host uses their ambient
    // declarations. Both describe this native object, but recursively comparing the
    // duplicate RPC types overwhelms the compiler. No persisted value crosses here.
    const ctx = state.raw as unknown as DurableObjectContext["Service"]["ctx"];

    return { ctx, env: yield* WorkerEnvironment };
  }),
);

export interface EventOptions<Services, EventServices = never, EventError = never> {
  /** Acquired per invocation, never during construction. Alchemy also provides native telemetry. */
  readonly eventLayer?: Layer.Layer<EventServices, EventError, Services | HostServices>;
}

/**
 * Run inside Alchemy's inner constructor Effect. The patched bridge owns the incarnation
 * Scope and constructor gate; it closes that Scope on initialization failure. Handlers
 * reuse the application services while retaining their invocation's Scope and memo map.
 */
export const acquire = <Services, Error, Requirements, EventServices = never, EventError = never>(
  application: Layer.Layer<Services, Error, Requirements>,
  options: EventOptions<Services, EventServices, EventError> = {},
) =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;

    const built = yield* Layer.buildWithScope(
      application.pipe(Layer.provideMerge(Rpc.layer)),
      scope,
    );

    const hosts = yield* Effect.context<HostServices>().pipe(
      Effect.map(Context.pick(DurableObjectState, WorkerEnvironment)),
    );

    const services = Context.merge(hosts, built);

    const invoke = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      Effect.gen(function* () {
        const eventScope = yield* Effect.scope;
        const ambient = yield* Effect.context<never>();

        const eventMemo =
          Context.getOrUndefined(ambient, Layer.CurrentMemoMap) ?? Layer.makeMemoMapUnsafe();

        return yield* effect.pipe(
          Effect.provide(options.eventLayer ?? Layer.empty),
          Effect.provide(
            services.pipe(
              Context.add(Scope.Scope, eventScope),
              Context.add(Layer.CurrentMemoMap, eventMemo),
            ),
          ),
          Rpc.withScope,
          Effect.orDie,
        );
      });

    return { services, invoke };
  });

/** Native handlers use the bridge's event Scope; application errors stay in encoded envelopes. */
export type NativeHandlers<Handlers> = {
  [Key in keyof Handlers]: Handlers[Key] extends (
    ...args: infer Args
  ) => Effect.Effect<infer Value, infer _Error, infer _Requirements>
    ? (...args: Args) => Effect.Effect<Value, never, Scope.Scope>
    : never;
};

export type Constructor<Rpc> = Effect.Effect<
  Effect.Effect<Rpc, never, DurableObjectState | Scope.Scope>,
  never,
  WorkerEnvironment
>;
