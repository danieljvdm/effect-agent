import {
  Clock,
  Context,
  DateTime,
  Effect,
  Fiber,
  Layer,
  Option,
  Ref,
  Semaphore,
  Stream,
} from "effect";
import { type ThreadId } from "effect-agent/identifiers";
import {
  MessageDeliveryDriver,
  MessageDeliveryError,
  MessageDeliveryStore,
} from "effect-agent/message-delivery";
import { WakeScheduler } from "effect-agent/wake-scheduler";

import { DurableAlarmError, ThreadMessageDelivery, ThreadMutationGate } from "../Alarm.ts";
import { ThreadObjectPlacement } from "../CloudflareBindings.ts";

/** Every write prearms its owner; the delivery due index owns its recovery deadline. */
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
        .withMutation(
          cacheGate.withPermit(Ref.set(deadline, undefined).pipe(Effect.andThen(body))),
          // A foreign receipt changing does not make the source ledger actionable. Keep the
          // prearm and producer gate so eviction and a racing pass cannot lose delivery work.
          { invalidatesRecovery: false },
        )
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
          Effect.filterOrFail(
            (keys) => keys.every((key) => ownsThread(key.ownerThreadId)),
            () => MessageDeliveryError.make({ reason: "validation", operation: "message owner" }),
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

    const failure = (operation: string) => () =>
      DurableAlarmError.make({
        operation,
        message: "Durable message recovery remains pending",
      });

    const wakes = yield* WakeScheduler;

    const prepare = Effect.gen(function* () {
      const deadline = yield* store.nextDeadline();

      if (deadline === null || deadline > (yield* Clock.currentTimeMillis))
        return { timeoutMillis: 1, run: Effect.void };
      // The assembled driver has four permits: a wave is one parallel attempt window.
      const keys = yield* store.due(yield* Clock.currentTimeMillis, 4);
      const records = yield* Effect.forEach(keys, (key) => store.get(key));

      return {
        timeoutMillis: Math.max(
          1,
          ...records.map((record) => record?.policy.attemptTimeoutMillis ?? 1),
        ),
        run: Effect.forEach(keys, (key) => driver.process(key), {
          concurrency: 4,
          discard: true,
        }).pipe(Effect.mapError(failure("dispatch message delivery"))),
      };
    }).pipe(Effect.mapError(failure("prepare message delivery")));

    return Context.make(ThreadMessageDelivery, {
      drainUntil: (dispatchClosed, dispatchUntil) =>
        Effect.gen(function* () {
          // Subscribe before the initial scan. Only admission of new waves stops;
          // the enclosing event owns these resources until its actual teardown.
          const notified = (yield* Stream.toPull(wakes.wakes)).pipe(
            Effect.catch(() => Effect.never),
          );

          const done = yield* Effect.forkScoped(dispatchClosed);

          const select = (initial = false) =>
            Effect.gen(function* () {
              const wave = yield* prepare;
              const now = yield* Clock.currentTimeMillis;

              // Recheck after local preparation: a late selection cannot start another wave once
              // dispatch closes. Always grant the initial opportunity to a caught-up alarm.
              if (
                (!initial && done.pollUnsafe() !== undefined) ||
                now + wave.timeoutMillis > DateTime.toEpochMillis(dispatchUntil)
              )
                return;
              yield* wave.run;
            });

          yield* select(true);
          while (done.pollUnsafe() === undefined) {
            const wake = yield* Effect.raceFirst(
              notified.pipe(Effect.map(Option.some)),
              Fiber.join(done).pipe(Effect.as(Option.none())),
            );

            if (Option.isNone(wake)) return;
            yield* Effect.forEach(wake.value, () => select(), { discard: true });
          }
          // No deadline sleeps: the driver finishes its one active parallel wave, including
          // timeout/backoff commits; retained retries belong to a future physical alarm.
        }),
      pendingDeadline: store
        .nextDeadline()
        .pipe(Effect.map(Option.fromNullishOr), Effect.mapError(failure("read message deadline"))),
    });
  }),
);
