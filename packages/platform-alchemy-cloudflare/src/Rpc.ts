import {
  RpcStrategy,
  RpcTargetError,
  type Strategy,
} from "@effect-agent/platform-cloudflare/cloudflare-rpc";
import { Context, Effect, Layer, Option, type Scope } from "effect";

interface Targets {
  readonly get: Strategy["get"];
  readonly invalidate: Strategy["invalidate"];
}

class CurrentTargets extends Context.Service<CurrentTargets, Targets>()(
  "@effect-agent/platform-alchemy-cloudflare/CurrentTargets",
) {}

/** A client captures this policy once; target ownership is resolved anew on each invocation. */
export const layer = Layer.succeed(RpcStrategy, {
  get: (owner, address, create) =>
    Effect.flatMap(Effect.serviceOption(CurrentTargets), (targets) =>
      Option.isSome(targets)
        ? targets.value.get(owner, address, create)
        : Effect.try({ try: create, catch: (cause) => new RpcTargetError({ cause }) }),
    ),
  invalidate: (target) =>
    Effect.flatMap(Effect.serviceOption(CurrentTargets), (targets) =>
      Option.isSome(targets) ? targets.value.invalidate(target) : Effect.void,
    ),
  traceArguments: Effect.succeed([]),
  withClientSpan: (effect, service, method) =>
    Effect.withSpan(effect, "Cloudflare.rpc", {
      attributes: { service, method },
    }),
});

/**
 * Share native RPC targets only inside the current Worker/DO event. Keeping a target
 * across incoming events captures another request's channel; repeatedly reacquiring one
 * after a callback can extend Cloudflare's subrequest chain. Alchemy owns the event Scope.
 */
export const withScope = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | Scope.Scope> =>
  Effect.gen(function* () {
    const existing = yield* Effect.serviceOption(CurrentTargets);

    if (Option.isSome(existing)) return yield* effect;

    let active = true;
    let owners = new WeakMap<object, Map<string, object>>();

    let locations = new WeakMap<
      object,
      { readonly entries: Map<string, object>; readonly address: string }
    >();

    const targets: Targets = {
      get: (owner, address, create) =>
        Effect.try({
          try: () => {
            if (!active) return create();
            let entries = owners.get(owner);

            if (entries === undefined) {
              entries = new Map();
              owners.set(owner, entries);
            }
            const cached = entries.get(address);

            // The owner/address pair identifies one native target type; no wire value is cast.
            if (cached !== undefined) return cached as ReturnType<typeof create>;
            const target = create();

            entries.set(address, target);
            locations.set(target, { entries, address });

            return target;
          },
          catch: (cause) => new RpcTargetError({ cause }),
        }),
      invalidate: (target) =>
        Effect.sync(() => {
          const location = locations.get(target);

          if (location?.entries.get(location.address) === target)
            location.entries.delete(location.address);
          locations.delete(target);
        }),
    };

    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        active = false;
        owners = new WeakMap();
        locations = new WeakMap();
      }),
    );

    return yield* effect.pipe(Effect.provideService(CurrentTargets, targets));
  });
