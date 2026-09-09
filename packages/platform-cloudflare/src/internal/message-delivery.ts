import {
  MessageDeliveryDriver,
  MessageDeliveryError,
  MessageDeliveryStore,
} from "@effect-agent/thread/MessageDelivery";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { Clock, Context, Deferred, Effect, Layer, Option, Ref, Semaphore } from "effect";

import { DurableAlarmError, ThreadMessageDelivery, ThreadMutationGate } from "../Alarm.ts";
import { ThreadObjectIdentity } from "../CloudflareBindings.ts";
import { CloudflareDurableRuntimeConfig } from "../CloudflareConfig.ts";

/** Every externally requested write prearms the owning Object's maintenance generation. */
export const guardedMessageDeliveryStoreLayer = Layer.effect(
  MessageDeliveryStore,
  Effect.gen(function* () {
    const store = yield* MessageDeliveryStore;
    const mutations = yield* ThreadMutationGate;
    const wakes = yield* WakeScheduler;
    const { threadId } = yield* ThreadObjectIdentity;
    // Reconstructed as unknown on every incarnation. The gate prevents a racing read from
    // caching an empty deadline across a write; SQL remains the recovery authority.
    const deadline = yield* Ref.make<number | null | undefined>(undefined);
    const cacheGate = yield* Semaphore.make(1);

    const local = <A, E>(owner: string, body: Effect.Effect<A, E>) =>
      owner === threadId
        ? body
        : Effect.fail(
            MessageDeliveryError.make({ reason: "validation", operation: "message owner" }),
          );

    const mutate = <A, E>(body: Effect.Effect<A, E>) =>
      mutations
        .withMutation(cacheGate.withPermit(Ref.set(deadline, undefined).pipe(Effect.andThen(body))))
        .pipe(
          Effect.catchTag("DurableAlarmError", () =>
            MessageDeliveryError.make({ reason: "storage", operation: "prearm message delivery" }),
          ),
        );

    const nextDeadline = cacheGate.withPermit(
      Effect.gen(function* () {
        const cached = yield* Ref.get(deadline);

        if (cached !== undefined) return cached;
        const current = yield* store.nextDeadline(threadId);

        yield* Ref.set(deadline, current);

        return current;
      }),
    );

    return MessageDeliveryStore.of({
      insert: (record) =>
        local(
          record.key.ownerThreadId,
          mutate(store.insert(record)).pipe(Effect.tap(() => wakes.notify(threadId))),
        ),
      get: (key) => local(key.ownerThreadId, store.get(key)),
      list: (request) => local(request.ownerThreadId, store.list(request)),
      change: (key, change) => local(key.ownerThreadId, mutate(store.change(key, change))),
      due: (nowMillis, limit, owner = threadId) =>
        local(owner, store.due(nowMillis, limit, threadId)),
      nextDeadline: (owner = threadId) => local(owner, nextDeadline),
    });
  }),
);

/** Message progress shares the native alarm slot without blocking source runtime work. */
export const threadMessageDeliveryLayer = Layer.effectContext(
  Effect.gen(function* () {
    const driver = yield* MessageDeliveryDriver;
    const store = yield* MessageDeliveryStore;
    const wakes = yield* WakeScheduler;
    const config = yield* CloudflareDurableRuntimeConfig;
    const { threadId } = yield* ThreadObjectIdentity;

    const failure = (operation: string) => () =>
      DurableAlarmError.make({
        operation,
        message: "Durable message recovery remains pending",
      });

    const drain = Effect.gen(function* () {
      const deadline = yield* store.nextDeadline(threadId);

      if (deadline !== null && deadline <= (yield* Clock.currentTimeMillis)) {
        yield* driver.runDue(threadId);
      }
    }).pipe(Effect.mapError(failure("drain message delivery")));

    return Context.make(ThreadMessageDelivery, {
      drain,
      drainUntil: Effect.fn("ThreadMessageDelivery.drainUntil")(function* (
        finished: Deferred.Deferred<void>,
      ) {
        // Always finish one wave; source completion prevents starting subsequent waves.
        do {
          yield* Effect.scoped(
            Effect.gen(function* () {
              // Subscribe before the durable read so an insertion during a wave is retained
              // as a hint for the next one. The scan interval covers dropped notifications.
              const notified = yield* wakes.subscribe(threadId);

              yield* drain;

              const deadline = yield* store
                .nextDeadline(threadId)
                .pipe(Effect.mapError(failure("read message deadline")));

              // The index includes unfinished waves, lease expiry, retry and settlement polls.
              // Yield at least one millisecond for an already-due deadline instead of spinning.
              const delay =
                deadline === null
                  ? config.wakeScanInterval
                  : Math.min(
                      config.wakeScanInterval,
                      Math.max(1, deadline - (yield* Clock.currentTimeMillis)),
                    );

              yield* Effect.raceFirst(
                Deferred.await(finished),
                Effect.raceFirst(notified, Effect.sleep(delay)),
              );
            }),
          );
        } while (!(yield* Deferred.isDone(finished)));
      }),
      pendingDeadline: store
        .nextDeadline(threadId)
        .pipe(Effect.map(Option.fromNullishOr), Effect.mapError(failure("read message deadline"))),
    });
  }),
);
