import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { submissionLedgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  threadStoreLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { Clock, Context, DateTime, Duration, Effect, Fiber, Layer, Option, Stream } from "effect";
import { type ResolvedBinding } from "effect-agent/agent-registration";
import { DurableAgentRuntime, DurableRuntimeConfig } from "effect-agent/durable-agent-runtime";
import { type PersistedJson } from "effect-agent/records";
import { SubmissionLedger, type SubmissionLookupByKey } from "effect-agent/submission-ledger";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { WakeScheduler } from "effect-agent/wake-scheduler";

import {
  DurableAlarmError,
  ThreadHostMaintenance,
  ThreadMaintenance,
  ThreadMaintenanceActivity,
} from "../src/Alarm.ts";
import {
  DurableObjectContext,
  ThreadObjectIdentity,
  ThreadObjectPlacement,
} from "../src/CloudflareBindings.ts";
import { CloudflareDurableRuntimeConfig } from "../src/CloudflareConfig.ts";
import { cloudflareWakeSchedulerLayer } from "../src/WakeScheduler.ts";

/** Only the history read is held; admission, control state and real SQLite remain available. */
export const recoveryReadHolds = new Map<string, Effect.Effect<void>>();

/** A source-owned obligation for only the new session, independent of old reply debt. */
export const recoveryReplies = new Map<
  string,
  {
    readonly lookup: SubmissionLookupByKey;
    readonly published: Array<PersistedJson>;
  }
>();

const replyHost = Layer.effectContext(
  Effect.gen(function* () {
    const { threadId } = yield* ThreadObjectIdentity;
    const ledger = yield* SubmissionLedger;
    const store = yield* ThreadStore;
    const wakes = yield* WakeScheduler;

    const failure = (cause: unknown) =>
      DurableAlarmError.make({
        operation: "publish fixture reply",
        message: "Reply publication failed",
        cause,
      });

    // Match a host that stops admitting waves once dispatchClosed fires. A native completion
    // after that signal would remain due until another alarm; the regression must detect it.
    const dispatchTimeoutMillis = 1_000;

    const pendingDeadline = Effect.gen(function* () {
      const reply = recoveryReplies.get(threadId);

      if (reply === undefined || reply.published.length > 0) return Option.none<number>();
      const row = yield* ledger.lookup(reply.lookup);

      return Option.isSome(row) && row.value.state === "settled"
        ? Option.some(yield* Clock.currentTimeMillis)
        : Option.none<number>();
    }).pipe(Effect.mapError(failure));

    const flush = Effect.gen(function* () {
      const reply = recoveryReplies.get(threadId);

      if (reply === undefined) return;

      const records = yield* Stream.runCollect(
        store.read(
          ThreadRead.make({
            threadId: reply.lookup.threadId,
            limit: 100,
          }),
        ),
      );

      const completed = records.find((record) => record.record.payload._tag === "RunCompleted")
        ?.record.payload;

      if (completed?._tag !== "RunCompleted")
        return yield* failure("settled reply has no completion");
      reply.published.push(completed.output);
    }).pipe(Effect.mapError(failure));

    return Context.make(ThreadHostMaintenance, {
      dispatchTimeoutMillis,
      pendingDeadline,
      drainUntil: (dispatchClosed, dispatchUntil) =>
        Effect.gen(function* () {
          const activity = yield* ThreadMaintenanceActivity;
          const hinted = yield* Stream.toPull(wakes.wakes);
          const checked = yield* activity.subscribeChanges;

          const notified = Effect.raceFirst(hinted, checked).pipe(
            Effect.asVoid,
            Effect.catch(() => Effect.never),
          );

          const done = yield* Effect.forkScoped(dispatchClosed);

          const select = Effect.fnUntraced(function* (initial = false) {
            const deadline = yield* pendingDeadline;
            const now = yield* Clock.currentTimeMillis;

            if (
              Option.isNone(deadline) ||
              deadline.value > now ||
              (!initial && done.pollUnsafe() !== undefined) ||
              now + dispatchTimeoutMillis > DateTime.toEpochMillis(dispatchUntil)
            )
              return;
            yield* flush;
          });

          yield* activity.run(activity.ready.pipe(Effect.andThen(select(true))));
          while (done.pollUnsafe() === undefined) {
            const changed = yield* Effect.raceFirst(
              notified.pipe(Effect.as(true)),
              Fiber.join(done).pipe(Effect.as(false)),
            );

            if (!changed) return;
            yield* activity.run(select());
          }
        }),
    });
  }),
);

/** A multi-Thread physical owner exercised through its real workerd alarm entry point. */
export const recoveryTestLayer = (bindings: ReadonlyArray<ResolvedBinding>, host = replyHost) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const { ctx } = yield* DurableObjectContext;
      const { threadId: owner } = yield* ThreadObjectIdentity;
      const config = yield* CloudflareDurableRuntimeConfig;
      const runtimeConfig = yield* DurableRuntimeConfig;

      const observedStore = Layer.effect(
        ThreadStore,
        Effect.map(ThreadStore, (store) =>
          ThreadStore.of({
            ...store,
            read: (request) =>
              Stream.unwrap(
                Effect.suspend(() =>
                  (recoveryReadHolds.get(request.threadId) ?? Effect.void).pipe(
                    Effect.as(store.read(request)),
                  ),
                ),
              ),
          }),
        ),
      ).pipe(Layer.provide(threadStoreLayer));

      const ports = Layer.mergeAll(observedStore, submissionLedgerLayer).pipe(
        Layer.provide(
          storageConfigLayer({
            storage: ctx.storage,
            ownershipLeaseDuration: config.ownershipLeaseDuration,
          }),
        ),
        Layer.provide(DoStorageFailpoint.layer),
      );

      return Layer.fresh(ThreadMaintenance.layer).pipe(
        Layer.provide(host),
        Layer.provideMerge(DurableAgentRuntime.layerWithBindings(bindings)),
        Layer.provideMerge(ports),
        Layer.provide(
          Layer.fresh(cloudflareWakeSchedulerLayer).pipe(
            Layer.provide(
              Layer.succeed(ThreadObjectPlacement, {
                ownsThread: (threadId) => threadId === owner || threadId.startsWith(`${owner}-`),
              }),
            ),
          ),
        ),
        Layer.provide(
          Layer.succeed(DurableRuntimeConfig, {
            ...runtimeConfig,
            recoveryTimeout: Duration.seconds(2),
          }),
        ),
      );
    }),
  );
