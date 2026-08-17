import type { ConversationId } from "@effect-agent/core";
import { makeWakeSubscriptionHub, WakeScheduler } from "@effect-agent/session";
import { Effect, Layer, PubSub, Schema, Stream } from "effect";

import { DurableAlarmService } from "./alarm.ts";
import { ConversationObjectIdentity, ConversationObjectNamespace } from "./bindings.ts";

/**
 * Bounded in-memory wake buffer for same-incarnation `awaitSettlement` subscribers. Wake
 * hints are droppable by contract (consumers pair every subscription with ledger polls), so
 * a full buffer slides out the oldest hint and an eviction simply loses the buffer — the
 * poll interval and the persisted alarm keep liveness (persistence §14).
 */
const WAKE_BUFFER_CAPACITY = 1_024;

/** A remote wake stub call failed; always swallowed and logged (hints are droppable). */
class RemoteWakeDropped extends Schema.TaggedError<RemoteWakeDropped>()("RemoteWakeDropped", {
  conversationId: Schema.String,
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
  DurableAlarmService | ConversationObjectIdentity | ConversationObjectNamespace
> = Layer.effect(WakeScheduler)(
  Effect.gen(function* () {
    const alarm = yield* DurableAlarmService;
    const identity = yield* ConversationObjectIdentity;
    const { namespace } = yield* ConversationObjectNamespace;
    const hints = yield* PubSub.sliding<ConversationId>(WAKE_BUFFER_CAPACITY);
    const progress = yield* makeWakeSubscriptionHub;
    yield* Effect.addFinalizer(() => PubSub.shutdown(hints));

    const notifyLocal = (conversationId: ConversationId) =>
      progress.notify(conversationId).pipe(
        Effect.andThen(PubSub.publish(hints, conversationId)),
        Effect.andThen(alarm.scheduleNow),
        Effect.catch((error) =>
          // `notify` never fails by contract; a failed alarm write degrades to "hint lost"
          // and the pass re-arm (or the next entry point's pre-arm) restores the invariant.
          Effect.logWarning("CloudflareWakeScheduler: local alarm wake failed", error),
        ),
        Effect.asVoid,
      );

    const notifyRemote = (conversationId: ConversationId) =>
      Effect.tryPromise({
        try: () => namespace.get(namespace.idFromName(conversationId)).wake(),
        catch: (cause) =>
          RemoteWakeDropped.make({
            conversationId,
            message: cause instanceof Error ? cause.message : String(cause),
            cause,
          }),
      }).pipe(
        Effect.catch((error) =>
          Effect.logWarning(
            `CloudflareWakeScheduler: remote wake of ${conversationId} dropped`,
            error,
          ),
        ),
        Effect.asVoid,
      );

    return WakeScheduler.of({
      notify: (conversationId) =>
        conversationId === identity.conversationId
          ? notifyLocal(conversationId)
          : notifyRemote(conversationId),
      subscribe: progress.subscribe,
      wakes: Stream.fromPubSub(hints),
    });
  }),
);
