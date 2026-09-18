import { Context, Data, Effect } from "effect";

/** A host could not acquire the native RPC target for this invocation. */
export class RpcTargetError extends Data.TaggedError("RpcTargetError")<{
  readonly cause: unknown;
}> {}

/** Native RPC policy supplied by the host, never retained across incoming events. */
export interface Strategy {
  readonly get: <A extends object, Owner extends object>(
    owner: Owner,
    address: string,
    create: () => A,
  ) => Effect.Effect<A, RpcTargetError>;
  readonly invalidate: (target: object) => Effect.Effect<void>;
  readonly traceArguments: Effect.Effect<ReadonlyArray<unknown>>;
  readonly withClientSpan: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    service: string,
    method: string,
  ) => Effect.Effect<A, E, R>;
}

/**
 * Hosts install their event-scoped target and tracing policy. The default invokes the
 * native binding directly and sends no private tracing metadata to another runtime.
 */
export const RpcStrategy = Context.Reference<Strategy>(
  "@effect-agent/platform-cloudflare/RpcStrategy",
  {
    defaultValue: () => ({
      get: (_owner, _address, create) =>
        Effect.try({ try: create, catch: (cause) => new RpcTargetError({ cause }) }),
      invalidate: () => Effect.void,
      traceArguments: Effect.succeed([]),
      withClientSpan: (effect, service, method) =>
        Effect.withSpan(effect, "Cloudflare.rpc", { attributes: { service, method } }),
    }),
  },
);

export const get = <A extends object, Owner extends object>(
  owner: Owner,
  address: string,
  create: () => A,
) => Effect.flatMap(RpcStrategy, (strategy) => strategy.get(owner, address, create));

export const invalidate = (target: object) =>
  Effect.flatMap(RpcStrategy, (strategy) => strategy.invalidate(target));
