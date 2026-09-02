import { Context, Effect, Layer, Ref } from "effect";

import { DoStorageFailpoint, type DoStorageFailpointHandler } from "./do-storage-failpoint.ts";
import type { DoStorageFailpointLocation } from "./errors.ts";

const noFailpoint: DoStorageFailpointHandler = () => Effect.void;

/** Test-only control for replacing the active Durable Object failpoint handler. */
export class DoStorageFailpointTestControl extends Context.Service<
  DoStorageFailpointTestControl,
  {
    readonly clear: Effect.Effect<void>;
    readonly setHandler: (handler: DoStorageFailpointHandler) => Effect.Effect<void>;
  }
>()("@effect-agent/storage-cloudflare/DoStorageFailpointTestControl") {
  /** Reusable test Layer with a control service backed by the same handler Ref. */
  static readonly layer = Layer.effectContext(
    Effect.gen(function* () {
      const handler = yield* Ref.make<DoStorageFailpointHandler>(noFailpoint);

      return Context.make(
        DoStorageFailpoint,
        DoStorageFailpoint.of({
          hit: (location) => Ref.get(handler).pipe(Effect.flatMap((current) => current(location))),
        }),
      ).pipe(
        Context.add(
          DoStorageFailpointTestControl,
          DoStorageFailpointTestControl.of({
            clear: Ref.set(handler, noFailpoint),
            setHandler: (next) => Ref.set(handler, next),
          }),
        ),
      );
    }),
  );
}

/**
 * The DC-specific eviction failpoint mode: instead of failing typed, an armed hit evicts the
 * Durable Object through an injected `evict` thunk — in production-shaped harnesses that thunk
 * is `() => ctx.abort()`, the platform's real failure mode. `ctx.abort()` never returns (it
 * throws while destroying the in-memory instance and every in-flight implicit or explicit
 * storage transaction rolls back), so an armed hit ends the current Attempt exactly like an
 * unannounced platform eviction; DO storage — the only correctness-critical state — survives
 * for the next incarnation, which the persisted alarm wakes without any incoming request.
 *
 * The handles stay injected: this package never imports `cloudflare:workers`, so the harness
 * that owns a `DurableObjectState` supplies the thunk.
 */
export const evictionFailpointHandler =
  (options: {
    readonly isArmed: (location: DoStorageFailpointLocation) => Effect.Effect<boolean>;
    /** Kills the incarnation — e.g. `() => ctx.abort()`. Typed `void` because the platform
     * declares `abort` as returning, but it throws while destroying the instance. */
    readonly evict: (location: DoStorageFailpointLocation) => void;
  }): DoStorageFailpointHandler =>
  (location) =>
    options.isArmed(location).pipe(
      Effect.flatMap((armed) =>
        armed
          ? // `ctx.abort()` throws while destroying the instance; that throw surfaces as a
            // defect in the (already dying) incarnation. The defensive throw below keeps the
            // guarantee — nothing after an armed hit may observe in-memory state — even if a
            // harness supplies an evict thunk that returns.
            Effect.sync((): never => {
              options.evict(location);
              throw new Error(
                `Durable Object eviction did not interrupt execution at ${location}.`,
              );
            })
          : Effect.void,
      ),
    );
