import { type ThreadId } from "@effect-agent/core/Identifiers";
import {
  MessageDeliveryDriver,
  MessageDeliveryError,
  MessageDeliveryStore,
} from "@effect-agent/thread/MessageDelivery";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { Clock, Context, Deferred, Effect, Layer, Option, Ref, Semaphore, Stream } from "effect";

import { DurableAlarmError, ThreadMessageDelivery, ThreadMutationGate } from "../Alarm.ts";
import { ThreadObjectPlacement } from "../CloudflareBindings.ts";
import { CloudflareDurableRuntimeConfig } from "../CloudflareConfig.ts";

/** Every externally requested write prearms the owning Object's maintenance generation. */
export const guardedMessageDeliveryStoreLayer = Layer.effect(
  MessageDeliveryStore,
  Effect.gen(function* () {
    const store = yield* MessageDeliveryStore;
    const mutations = yield* ThreadMutationGate;
    const wakes = yield* WakeScheduler;
    const { ownsThread } = yield* ThreadObjectPlacement;
    // Reconstructed as unknown on every incarnation. The gate prevents a racing read from
    // caching an empty deadline across a write; SQL remains the recovery authority.
    const deadline = yield* Ref.make<number | null | undefined>(undefined);
    const cacheGate = yield* Semaphore.make(1);

    const local = <A, E>(
      owner: ThreadId | undefined,
      body: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | MessageDeliveryError> =>
      owner === undefined || ownsThread(owner)
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
        const current = yield* store.nextDeadline();

        yield* Ref.set(deadline, current);

        return current;
      }),
    );

    return MessageDeliveryStore.of({
      limits: store.limits,
      maxStoredValueBytes: store.maxStoredValueBytes,
      insert: (record) =>
        local(
          record.key.ownerThreadId,
          mutate(store.insert(record)).pipe(
            Effect.tap(() => wakes.notify(record.key.ownerThreadId)),
          ),
        ),
      get: (key) => local(key.ownerThreadId, store.get(key)),
      list: (request) => local(request.ownerThreadId, store.list(request)),
      change: (key, change) => local(key.ownerThreadId, mutate(store.change(key, change))),
      // Only the trusted physical-owner pump omits an owner. All returned keys still
      // pass placement validation before the driver may dispatch any of the wave.
      due: (nowMillis, limit, owner) =>
        local(owner, store.due(nowMillis, limit, owner)).pipe(
          Effect.flatMap((keys) =>
            keys.every((key) => ownsThread(key.ownerThreadId))
              ? Effect.succeed(keys)
              : Effect.fail(
                  MessageDeliveryError.make({ reason: "validation", operation: "message owner" }),
                ),
          ),
        ),
      nextDeadline: (owner) =>
        local(owner, owner === undefined ? nextDeadline : store.nextDeadline(owner)),
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

    const failure = (operation: string) => () =>
      DurableAlarmError.make({
        operation,
        message: "Durable message recovery remains pending",
      });

    const drain = Effect.gen(function* () {
      const deadline = yield* store.nextDeadline();

      if (deadline !== null && deadline <= (yield* Clock.currentTimeMillis)) {
        yield* driver.runDue();
      }
    }).pipe(Effect.mapError(failure("drain message delivery")));

    return Context.make(ThreadMessageDelivery, {
      drain,
      drainUntil: Effect.fn("ThreadMessageDelivery.drainUntil")(function* (
        finished: Deferred.Deferred<void>,
      ) {
        // One scoped subscription covers every logical lane in this physical owner.
        // Acquire it before reading durable deadlines; hints remain droppable.
        const notified = (yield* Stream.toPull(wakes.wakes)).pipe(Effect.catch(() => Effect.never));

        // Always finish one wave; source completion prevents starting subsequent waves.
        yield* Effect.gen(function* () {
          yield* drain;

          const deadline = yield* store
            .nextDeadline()
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
        }).pipe(Effect.repeat({ until: () => Deferred.isDone(finished) }));
      }, Effect.scoped),
      pendingDeadline: store
        .nextDeadline()
        .pipe(Effect.map(Option.fromNullishOr), Effect.mapError(failure("read message deadline"))),
    });
  }),
);
