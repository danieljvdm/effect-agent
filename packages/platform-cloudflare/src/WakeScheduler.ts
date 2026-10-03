import { Effect, Layer, PubSub, Schema, Stream } from "effect";
import { type ThreadId } from "effect-agent/identifiers";
import { makeWakeSubscriptionHub, WakeScheduler } from "effect-agent/wake-scheduler";

import { DurableAlarmService } from "./Alarm.ts";
import {
  callThreadObject,
  ThreadObjectPlacement,
  ThreadObjectNamespace,
} from "./CloudflareBindings.ts";
import { safeCauseMessage } from "./internal/boundary.ts";

/**
 * Bounded in-memory wake buffer for same-incarnation workers. Wake
 * hints are droppable by contract (consumers pair hints with durable authority), so
 * a full buffer slides out the oldest hint and an eviction simply loses the buffer — the
 * poll interval and the persisted alarm keep liveness (persistence §14).
 */
const WAKE_BUFFER_CAPACITY = 1_024;

/** A remote wake stub call failed; always swallowed and logged (hints are droppable). */
class RemoteWakeDropped extends Schema.TaggedError<RemoteWakeDropped>()("RemoteWakeDropped", {
  threadId: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/**
 * The DC `WakeScheduler` (plan §1.4):
 *
 * - `notify(local)` wakes progress and settlement registrations, publishes a worker hint, and
 *   schedules the earliest alarm. Progress hints skip settlement registrations only.
 * - `notify(remote)` → fire-and-forget `wake()` on the owning Object's stub with every error
 *   swallowed and logged: hints are droppable, and the target's own alarm/scan pairing (the
 *   maintenance pass re-polls unsettled children) guarantees liveness without this call.
 * - `wakes` → the bounded sliding PubSub only. Thread waiters use Scope-owned one-shot hubs;
 *   the settlement poll interval and persisted alarm retain liveness after hint loss.
 */
export const cloudflareWakeSchedulerLayer: Layer.Layer<
  WakeScheduler,
  never,
  DurableAlarmService | ThreadObjectPlacement | ThreadObjectNamespace
> = Layer.effect(WakeScheduler)(
  Effect.gen(function* () {
    const alarm = yield* DurableAlarmService;
    const placement = yield* ThreadObjectPlacement;
    const namespace = yield* ThreadObjectNamespace;
    const hints = yield* PubSub.sliding<ThreadId>(WAKE_BUFFER_CAPACITY);
    const progress = yield* makeWakeSubscriptionHub;
    const settlements = yield* makeWakeSubscriptionHub;

    yield* Effect.addFinalizer(() => PubSub.shutdown(hints));

    const notifyLocal = (threadId: ThreadId, kind?: "progress") =>
      (kind === "progress"
        ? progress.notify(threadId)
        : settlements.notify(threadId).pipe(Effect.andThen(progress.notify(threadId)))
      ).pipe(
        Effect.andThen(PubSub.publish(hints, threadId)),
        Effect.andThen(alarm.scheduleNow),
        Effect.catch((error) =>
          // `notify` never fails by contract; a failed alarm write degrades to "hint lost"
          // and the pass re-arm (or the next entry point's pre-arm) restores the invariant.
          Effect.logWarning("CloudflareWakeScheduler: local alarm wake failed", error),
        ),
        Effect.asVoid,
      );

    const notifyRemote = (threadId: ThreadId) =>
      callThreadObject(
        threadId,
        (target) => target.wake(),
        (cause) =>
          RemoteWakeDropped.make({
            threadId,
            message: safeCauseMessage(cause, "The remote wake failed without a diagnostic"),
            cause,
          }),
      ).pipe(
        Effect.provideService(ThreadObjectNamespace, namespace),
        Effect.catch((error) =>
          Effect.logWarning(`CloudflareWakeScheduler: remote wake of ${threadId} dropped`, error),
        ),
        Effect.asVoid,
      );

    return WakeScheduler.of({
      notify: (threadId, kind) =>
        placement.ownsThread(threadId) ? notifyLocal(threadId, kind) : notifyRemote(threadId),
      subscribe: (threadId, kind) =>
        (kind === "settlement" ? settlements : progress).subscribe(threadId),
      wakes: Stream.fromPubSub(hints),
    });
  }),
);
