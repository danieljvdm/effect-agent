import { type ThreadId } from "@effect-agent/core/Identifiers";
import { makeWakeSubscriptionHub, WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { Effect, Layer, PubSub, Schema, Stream } from "effect";

import { DurableAlarmService } from "./Alarm.ts";
import { ThreadObjectIdentity, ThreadObjectNamespace } from "./CloudflareBindings.ts";
import { safeCauseMessage } from "./internal/boundary.ts";

/**
 * Bounded in-memory wake buffer for same-incarnation `awaitSettlement` subscribers. Wake
 * hints are droppable by contract (consumers pair every subscription with ledger polls), so
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
 * - `notify(local)` → `setAlarm(now)` — durable, cheap, coalescing (the alarm slot keeps the
 *   earliest deadline) — plus a same-incarnation PubSub hint for `awaitSettlement` waiters.
 * - `notify(remote)` → fire-and-forget `wake()` on the owning Object's stub with every error
 *   swallowed and logged: hints are droppable, and the target's own alarm/scan pairing (the
 *   maintenance pass re-polls unsettled children) guarantees liveness without this call.
 * - `wakes` → the bounded sliding PubSub only. No important state lives in memory: the
 *   subscription accelerates waiters within one incarnation and the settlement poll interval
 *   is the correctness path.
 */
export const cloudflareWakeSchedulerLayer: Layer.Layer<
  WakeScheduler,
  never,
  DurableAlarmService | ThreadObjectIdentity | ThreadObjectNamespace
> = Layer.effect(WakeScheduler)(
  Effect.gen(function* () {
    const alarm = yield* DurableAlarmService;
    const identity = yield* ThreadObjectIdentity;
    const { namespace } = yield* ThreadObjectNamespace;
    const hints = yield* PubSub.sliding<ThreadId>(WAKE_BUFFER_CAPACITY);
    const progress = yield* makeWakeSubscriptionHub;

    yield* Effect.addFinalizer(() => PubSub.shutdown(hints));

    const notifyLocal = (threadId: ThreadId) =>
      progress.notify(threadId).pipe(
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
      Effect.tryPromise({
        try: () => namespace.get(namespace.idFromName(threadId)).wake(),
        catch: (cause) =>
          RemoteWakeDropped.make({
            threadId,
            message: safeCauseMessage(cause, "The remote wake failed without a diagnostic"),
            cause,
          }),
      }).pipe(
        Effect.catch((error) =>
          Effect.logWarning(`CloudflareWakeScheduler: remote wake of ${threadId} dropped`, error),
        ),
        Effect.asVoid,
      );

    return WakeScheduler.of({
      notify: (threadId) =>
        threadId === identity.threadId ? notifyLocal(threadId) : notifyRemote(threadId),
      subscribe: progress.subscribe,
      wakes: Stream.fromPubSub(hints),
    });
  }),
);
