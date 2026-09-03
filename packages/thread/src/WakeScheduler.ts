import { type ThreadId } from "@effect-agent/core/Identifiers";
import type { Scope } from "effect";
import { Context, Deferred, Effect, Layer, Ref, Stream } from "effect";

/**
 * Incarnation-local, thread-keyed wake registrations. A registration is installed before
 * its returned Effect can be awaited, which lets a caller subscribe, check durable authority,
 * and then park without a lost-wakeup window.
 */
export interface WakeSubscriptionHub {
  /** Register one Scope-owned waiter and return its one-shot wake Effect. */
  readonly subscribe: (
    threadId: ThreadId,
  ) => Effect.Effect<Effect.Effect<void>, never, Scope.Scope>;
  /** Wake every currently registered waiter for exactly this Thread. */
  readonly notify: (threadId: ThreadId) => Effect.Effect<void>;
}

interface WakeRegistration {
  readonly id: number;
  readonly deferred: Deferred.Deferred<void>;
}

type WakeRegistrations = ReadonlyMap<ThreadId, ReadonlyMap<number, Deferred.Deferred<void>>>;

/**
 * Build the shared per-incarnation registration primitive used by host WakeSchedulers. The hub
 * contains hints only: eviction may discard it, while a reconnecting caller re-subscribes and
 * rechecks canonical storage. Scope finalizers remove cancelled waiters, and notify broadcasts
 * instead of consuming a signal that belongs to another waiter.
 */
export const makeWakeSubscriptionHub: Effect.Effect<WakeSubscriptionHub> = Effect.gen(function* () {
  const registrations = yield* Ref.make<WakeRegistrations>(new Map());
  const nextId = yield* Ref.make(0);

  const remove = (threadId: ThreadId, id: number) =>
    Ref.update(registrations, (current) => {
      const existing = current.get(threadId);

      if (existing === undefined || !existing.has(id)) return current;
      const next = new Map(current);
      const thread = new Map(existing);

      thread.delete(id);
      if (thread.size === 0) {
        next.delete(threadId);
      } else {
        next.set(threadId, thread);
      }

      return next;
    });

  const subscribe = Effect.fn("WakeSubscriptionHub.subscribe")(
    (threadId: ThreadId): Effect.Effect<Effect.Effect<void>, never, Scope.Scope> =>
      Effect.gen(function* () {
        const deferred = yield* Deferred.make<void>();
        const id = yield* Ref.getAndUpdate(nextId, (current) => current + 1);
        const registration: WakeRegistration = { id, deferred };

        yield* Effect.addFinalizer(() => remove(threadId, registration.id));
        yield* Ref.update(registrations, (current) => {
          const next = new Map(current);
          const thread = new Map(current.get(threadId) ?? []);

          thread.set(registration.id, registration.deferred);
          next.set(threadId, thread);

          return next;
        });

        return registration.deferred;
      }).pipe(Effect.map((deferred) => Deferred.await(deferred))),
  );

  const notify = Effect.fn("WakeSubscriptionHub.notify")(function* (threadId: ThreadId) {
    const waiters = yield* Ref.modify(registrations, (current) => {
      const thread = current.get(threadId);

      if (thread === undefined) return [[], current] as const;
      const next = new Map(current);

      next.delete(threadId);

      return [[...thread.values()], next] as const;
    });

    yield* Effect.forEach(waiters, (waiter) => Deferred.succeed(waiter, undefined), {
      discard: true,
    });
  });

  return { subscribe, notify };
});

/**
 * Liveness hint channel for durable schedulers. `notify` announces that a Thread lane may
 * have claimable, canonical, or settled work; `wakes` is the all-lanes subscription surface for
 * workers, while `subscribe` installs an efficient one-Thread wait registration.
 *
 * Correctness must NEVER depend on delivery: notifications may be dropped, coalesced, duplicated,
 * or observed by no subscriber (a notify before any subscription is simply lost). Every consumer
 * must pair a wake with durable authority: workers use periodic SubmissionLedger scans, while a
 * public progress waiter subscribes before its canonical read and reconnects/rechecks after an
 * incarnation loss (persistence §14).
 *
 * Each run of `wakes` creates its own subscription whose resources live in that run's Scope and
 * are released when the consuming stream ends or is interrupted. Implementations must not spawn
 * daemon fibers.
 */
export class WakeScheduler extends Context.Service<
  WakeScheduler,
  {
    /** Best-effort hint that `threadId` may have claimable or settled work. Never fails. */
    readonly notify: (threadId: ThreadId) => Effect.Effect<void>;
    /** Scope-owned one-shot registration for exactly one Thread. */
    readonly subscribe: (
      threadId: ThreadId,
    ) => Effect.Effect<Effect.Effect<void>, never, Scope.Scope>;
    /** Scope-owned subscription to wake hints, starting at subscription time. */
    readonly wakes: Stream.Stream<ThreadId>;
  }
>()("@effect-agent/thread/WakeScheduler") {
  /**
   * Discards every notification and never wakes anyone. Valid because WakeScheduler is only a
   * liveness hint: consumers relying on ledger scans stay correct, merely slower.
   */
  static readonly layerNoop: Layer.Layer<WakeScheduler> = Layer.succeed(WakeScheduler, {
    notify: () => Effect.void,
    subscribe: () => Effect.succeed(Effect.never),
    wakes: Stream.never,
  });
}
