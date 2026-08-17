import { Context, Deferred, Effect, Layer, Ref, type Scope } from "effect";

/** Cancellation tombstones are bounded hints, never durable authority. */
const MAX_CANCELLATION_TOMBSTONES = 1_024;

type ActiveRegistration = ReadonlySet<Deferred.Deferred<void>>;
type Registration = ActiveRegistration | "cancelled";
type Registrations = ReadonlyMap<string, Registration>;

/**
 * Per-incarnation cancellation registry for long-lived progress RPCs. The public runtime owns
 * the actual wake registration; this host-only registry lets an interrupted Worker Effect ask
 * the Object to interrupt its scoped wait before the Worker execution context itself ends.
 */
export class ProgressWaitRegistry extends Context.Service<
  ProgressWaitRegistry,
  {
    /** Register a Scope-owned cancellation signal, observing any early cancel tombstone. */
    readonly subscribe: (
      waiterId: string,
    ) => Effect.Effect<Effect.Effect<void>, never, Scope.Scope>;
    /** Cancel a registered waiter, or remember a bounded early cancellation. */
    readonly cancel: (waiterId: string) => Effect.Effect<void>;
  }
>()("@effect-agent/platform-cloudflare/ProgressWaitRegistry") {
  static readonly layer: Layer.Layer<ProgressWaitRegistry> = Layer.effect(
    ProgressWaitRegistry,
    Effect.gen(function* () {
      const registrations = yield* Ref.make<Registrations>(new Map());

      const remove = (waiterId: string, deferred: Deferred.Deferred<void>) =>
        Ref.update(registrations, (current) => {
          const existing = current.get(waiterId);
          if (existing === undefined || existing === "cancelled" || !existing.has(deferred)) {
            return current;
          }
          const next = new Map(current);
          const active = new Set(existing);
          active.delete(deferred);
          if (active.size === 0) {
            next.delete(waiterId);
          } else {
            next.set(waiterId, active);
          }
          return next;
        });

      const subscribe = Effect.fn("ProgressWaitRegistry.subscribe")(
        (waiterId: string): Effect.Effect<Effect.Effect<void>, never, Scope.Scope> =>
          Effect.gen(function* () {
            const deferred = yield* Deferred.make<void>();
            yield* Effect.addFinalizer(() => remove(waiterId, deferred));
            const cancelled = yield* Ref.modify(registrations, (current) => {
              const existing = current.get(waiterId);
              const next = new Map(current);
              if (existing === "cancelled") {
                return [true, current] as const;
              }
              const active = new Set(existing ?? []);
              active.add(deferred);
              next.set(waiterId, active);
              return [false, next] as const;
            });
            return { cancelled, deferred };
          }).pipe(
            Effect.map(({ cancelled, deferred }) =>
              cancelled ? Effect.void : Deferred.await(deferred),
            ),
          ),
      );

      const cancel = Effect.fn("ProgressWaitRegistry.cancel")(function* (waiterId: string) {
        const waiters = yield* Ref.modify(registrations, (current) => {
          const existing = current.get(waiterId);
          const next = new Map(current);
          if (existing === undefined) {
            next.set(waiterId, "cancelled");
            let tombstones = 0;
            for (const registration of next.values()) {
              if (registration === "cancelled") tombstones += 1;
            }
            if (tombstones > MAX_CANCELLATION_TOMBSTONES) {
              for (const [id, registration] of next) {
                if (registration !== "cancelled") continue;
                next.delete(id);
                break;
              }
            }
            return [[], next] as const;
          }
          if (existing === "cancelled") return [[], current] as const;
          next.delete(waiterId);
          return [[...existing], next] as const;
        });
        yield* Effect.forEach(waiters, (waiter) => Deferred.succeed(waiter, undefined), {
          discard: true,
        });
      });

      return ProgressWaitRegistry.of({ subscribe, cancel });
    }),
  );
}
