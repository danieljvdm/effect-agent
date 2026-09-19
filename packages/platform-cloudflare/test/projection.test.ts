import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import type { Receipt } from "effect-agent/receipt";
import { ProducerEpoch, type PersistedJson } from "effect-agent/records";
import {
  AbortCommand,
  AbortIntentRequest,
  ApprovalDecisionCommand,
  SubmissionLedger,
  SubmissionLookupById,
  type AbortIntent,
} from "effect-agent/submission-ledger";
import type { ThreadProjectionMaintenance } from "effect-agent/thread-projection-maintenance";
import {
  FencedAppendRequest,
  ThreadMaterialization,
  ThreadRead,
  ThreadStore,
  ThreadTailRequest,
} from "effect-agent/thread-store";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import {
  DurableAlarmError,
  ThreadHostMaintenance,
  ThreadMaintenance,
  ThreadMaintenanceActivity,
  ThreadMaintenanceFailpoint,
} from "../src/Alarm.ts";
import type { DurableObjectContext, ThreadObjectNamespace } from "../src/CloudflareBindings.ts";
import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import * as ThreadObject from "../src/ThreadObject.ts";
import {
  BOOK_TOOL_CALL_ID,
  approvalDefinition,
  plannerDefinition,
  maintenanceClocks,
  modelRequestHolds,
  submitOptions,
  decodeThreadId,
  armMaintenancePause,
  awaitMaintenancePause,
  releaseMaintenancePause,
} from "./fixtures.ts";
import {
  allSettled,
  anyInState,
  drainAlarmsUntil,
  laneRows,
  readCanonical,
  runClient,
  scheduledAlarm,
  stubFor,
} from "./harness.ts";
import {
  hostMaintenanceControls,
  ProjectionIndex,
  projectionLayer,
  projectionDefinition,
  projectionControls,
  projectionResources,
  projectionConstructions,
  projectionLookups,
  projectionLiveBatches,
} from "./projection-fixture.ts";

const namespace = "PROJECTIONS";
const stub = (thread: string) => env.PROJECTIONS.get(env.PROJECTIONS.idFromName(thread));

const alarm = (thread: string) =>
  runInDurableObject(stub(thread), (instance) => Promise.resolve(instance.alarm()));

const watermark = (thread: string) =>
  runInDurableObject(stub(thread), (instance) =>
    instance[DurableObject.RunSymbol](Effect.flatMap(ProjectionIndex, (index) => index.watermark)),
  );

const quiesce = (thread: string) =>
  drainAlarmsUntil(thread, async () => (await scheduledAlarm(thread, namespace)) === null, {
    namespace,
  });

const submit = (
  thread: string,
  definition:
    | typeof projectionDefinition
    | typeof plannerDefinition
    | typeof approvalDefinition = projectionDefinition,
) =>
  runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition },
        { question: "index", ref: thread },
        submitOptions(thread, thread),
      ),
    ),
    namespace,
  );

const withThread = (
  test: (thread: string, now: number, advance: (millis: number) => Promise<void>) => Promise<void>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const thread = `projection-${crypto.randomUUID()}`;
      const now = Date.now() + 86_400_000;

      yield* TestClock.setTime(now);
      maintenanceClocks.set(thread, yield* Clock.Clock);
      const testClock = yield* TestClock.testClockWith(Effect.succeed);

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          hostMaintenanceControls.delete(thread);
          projectionControls.delete(thread);
          projectionResources.delete(thread);
          projectionConstructions.delete(thread);
          projectionLookups.delete(thread);
          projectionLiveBatches.delete(thread);
          maintenanceClocks.delete(thread);
          releaseMaintenancePause(thread);
        }),
      );
      yield* Effect.promise(() =>
        test(thread, now, (millis) => Effect.runPromise(testClock.adjust(millis))),
      );
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

const prepareAppend = (thread: string, count: number) =>
  runInDurableObject(stub(thread), (instance) =>
    instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const threadId = decodeThreadId(thread);

        yield* store.materialize(
          ThreadMaterialization.make({ threadId, producerEpoch: ProducerEpoch.make(0) }),
        );
        const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

        return yield* Schema.decodeUnknownEffect(FencedAppendRequest)({
          threadId,
          producerEpoch: tail.producerEpoch,
          expectedTailSequence: tail.tailSequence,
          expectedTailDigest: tail.tailDigest,
          batch: {
            batchId: `projection-batch-${tail.tailSequence}`,
            producerId: "projection-test",
            records: Array.from({ length: count }, (_, index) => ({
              recordId: `projection-record-${tail.tailSequence + index + 1}`,
              family: "thread",
              schemaVersion: 1,
              deploymentId: "projection-test",
              createdAt: "2026-09-08T00:00:00.000Z",
              payload: { _tag: "UserInputRecorded", kind: "user", input: `retained-${index}` },
            })),
          },
        });
      }),
    ),
  );

const append = (thread: string, request: FencedAppendRequest) =>
  runInDurableObject(stub(thread), (instance) =>
    instance[DurableObject.RunSymbol](
      Effect.flatMap(ThreadStore, (store) => store.append(request)),
    ),
  );

describe("live Thread projection and alarm backfill", () => {
  it(
    "shares one raw-source index and SQL owner with Tools throughout an active Run",
    () =>
      withThread(async (thread) => {
        await submit(thread);
        await drainAlarmsUntil(thread, allSettled(thread, namespace), { namespace });
        const lookups = projectionLookups.get(thread);

        expect(lookups).toHaveLength(3);
        expect(new Set(lookups).size).toBe(3);
        expect(projectionConstructions.get(thread)).toBe(1);
        await runInDurableObject(stub(thread), (instance) =>
          instance[DurableObject.RunSymbol](
            Effect.gen(function* () {
              expect((yield* ProjectionIndex).ownerSql).toBe(yield* SqlClient);
            }),
          ),
        );
        const records = await readCanonical(thread, namespace);

        expect(
          records
            .filter((record) => record.record.payload._tag === "ToolCallSettled")
            .every(
              (record) =>
                record.record.payload._tag !== "ToolCallSettled" ||
                !record.record.payload.isFailure,
            ),
        ).toBe(true);
        await quiesce(thread);
      }),
    30_000,
  );

  it(
    "applies all 256 committed records before returning, replays idempotently, and rejects stale producers",
    () =>
      withThread(async (thread) => {
        const request = await prepareAppend(thread, 256);
        const result = await append(thread, request);

        expect(result.lastSequence).toBe(256);
        expect(await watermark(thread)).toBe(256);
        expect((await append(thread, request)).replayed).toBe(true);
        expect(await watermark(thread)).toBe(256);
        const calls = projectionLiveBatches.get(thread)?.length;

        await runInDurableObject(stub(thread), (instance) =>
          instance[DurableObject.RunSymbol](
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              yield* store.materialize(
                ThreadMaterialization.make({
                  threadId: decodeThreadId(thread),
                  producerEpoch: ProducerEpoch.make(1),
                }),
              );
              const exit = yield* store.append(request).pipe(Effect.exit);

              expect(Exit.isFailure(exit) && Cause.findErrorOption(exit.cause)).toMatchObject({
                _tag: "Some",
                value: { _tag: "FenceRejected" },
              });
            }),
          ),
        );
        expect(projectionLiveBatches.get(thread)?.length).toBe(calls);
        await quiesce(thread);
      }),
    30_000,
  );

  it.each(["failure", "defect"] as const)(
    "keeps a canonical commit authoritative after live %s",
    (failure) =>
      withThread(async (thread) => {
        const request = await prepareAppend(thread, 9);

        projectionControls.set(thread, { operation: "live", failure });
        expect((await append(thread, request)).lastSequence).toBe(9);
        expect(await watermark(thread)).toBe(0);
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
        projectionControls.delete(thread);
        await alarm(thread);
        expect(await watermark(thread)).toBe(4);
        await quiesce(thread);
        expect(await watermark(thread)).toBe(9);
      }),
  );

  it.each(["before", "after"] as const)(
    "resumes bounded backfill after eviction %s its atomic commit",
    (stage) =>
      withThread(async (thread) => {
        const request = await prepareAppend(thread, 9);

        projectionControls.set(thread, { skipLive: true });
        await append(thread, request);
        projectionControls.set(thread, { operation: "drain", failure: "eviction", stage });
        await alarm(thread).catch(() => undefined);
        expect(await watermark(thread)).toBe(stage === "before" ? 0 : 4);
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
        await quiesce(thread);
        expect(await watermark(thread)).toBe(9);
        expect(projectionConstructions.get(thread)).toBe(2);
        await runInDurableObject(stub(thread), async (_, state) => {
          const rows = state.storage.sql
            .exec<{ count: number }>("SELECT count(*) AS count FROM test_projection_rows")
            .one();

          expect(rows.count).toBe(9);
        });
      }),
  );

  it.each(["failure", "defect", "timeout"] as const)(
    "runs canonical work before reporting a backfill %s and releases resources",
    (failure) =>
      withThread(async (thread, _now, advance) => {
        projectionControls.set(thread, { skipLive: true });
        await submit(thread, plannerDefinition);
        projectionControls.set(thread, { skipLive: true, operation: "drain", failure });
        await expect(alarm(thread)).rejects.toBeDefined();
        expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
        const resources = projectionResources.get(thread);

        expect(resources?.released).toBe(resources?.acquired);
        projectionControls.delete(thread);
        await advance(100);
        await quiesce(thread);
      }),
  );

  it.each(["failure", "timeout"] as const)(
    "runs native work before reporting an unrelated host setup %s",
    (failure) =>
      withThread(async (thread, _now, advance) => {
        await submit(thread, plannerDefinition);
        let markEntered!: () => void;

        const entered = new Promise<void>((resolve) => {
          markEntered = resolve;
        });

        hostMaintenanceControls.set(thread, {
          dispatchTimeoutMillis: 1_000,
          drainUntil: () =>
            Effect.suspend(() => {
              markEntered();

              return failure === "timeout"
                ? Effect.never
                : DurableAlarmError.make({
                    operation: "test host setup",
                    message: "outbox unavailable",
                  });
            }),
          pendingDeadline: Effect.succeed(Option.some(0)),
        });

        const rejected = alarm(thread).then(
          () => undefined,
          (cause: unknown) => cause,
        );

        if (failure === "timeout") {
          await entered;
          await advance(1_000);
        }
        expect(String(await rejected)).toContain(
          failure === "timeout" ? "initial maintenance before its allowance" : "outbox unavailable",
        );
        expect(await allSettled(thread, namespace)()).toBe(true);
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
        hostMaintenanceControls.delete(thread);
        await advance(100);
        await quiesce(thread);
      }),
  );

  it("preserves a completed interruption while a sibling dispatch remains blocked", () =>
    withThread(async (thread, _now, advance) => {
      projectionControls.set(thread, { skipLive: true });
      await submit(thread, plannerDefinition);
      projectionControls.set(thread, {
        skipLive: true,
        operation: "drain",
        failure: "interruption",
      });
      let release!: () => void;

      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      hostMaintenanceControls.set(thread, {
        dispatchTimeoutMillis: 1_000,
        drainUntil: (_closed, _until, activity) =>
          activity.run(activity.ready.pipe(Effect.andThen(Effect.promise(() => held)))),
        pendingDeadline: Effect.succeed(Option.some(0)),
      });
      try {
        await expect(alarm(thread)).rejects.toBeDefined();
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
        const resources = projectionResources.get(thread);

        expect(resources?.released).toBe(resources?.acquired);
      } finally {
        hostMaintenanceControls.delete(thread);
        projectionControls.delete(thread);
        release();
      }
      await advance(100);
      await quiesce(thread);
      expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
    }));

  // Incident regression: https://reve-r6.sentry.io/issues/KOMMUNIKASIE-API-AA
  it("keeps host abort and fresh reply work available for native dispatch during retirement", async () => {
    const thread = `recovery-retirement-${crypto.randomUUID()}`;
    const liveClock = Effect.runSync(Clock.Clock);
    const nowMillis = () => Date.now() + 86_400_000;
    const nowNanos = () => BigInt(nowMillis()) * 1_000_000n;

    maintenanceClocks.set(thread, {
      currentTimeMillisUnsafe: nowMillis,
      currentTimeMillis: Effect.sync(nowMillis),
      currentTimeNanosUnsafe: nowNanos,
      currentTimeNanos: Effect.sync(nowNanos),
      monotonicTimeNanosUnsafe: () => liveClock.monotonicTimeNanosUnsafe(),
      monotonicTimeNanos: liveClock.monotonicTimeNanos,
      sleep: (duration) => liveClock.sleep(duration),
    });
    const advance = (millis: number) => new Promise<void>((resolve) => setTimeout(resolve, millis));
    const old = `${thread}-old`;
    const fresh = `${thread}-fresh`;

    const run = <A, E>(
      body: Effect.Effect<
        A,
        E,
        ThreadMaintenance | DurableAgentRuntime | SubmissionLedger | ThreadStore | WakeScheduler
      >,
    ) => runInDurableObject(stubFor(thread), (instance) => instance[DurableObject.RunSymbol](body));

    const controls = await run(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const maintenance = yield* ThreadMaintenance;
        const ledger = yield* SubmissionLedger;
        const store = yield* ThreadStore;
        const releaseModel = yield* Deferred.make<void>();
        const releaseCleanup = yield* Deferred.make<void>();
        let oldActive = 0;
        let oldEntered = 0;
        let oldCompleted = false;
        let freshEntered = 0;
        let cleanupActive = false;
        let closed = false;
        let command: AbortCommand | undefined;
        let abort: AbortIntent | undefined;
        let freshReceipt: Receipt | undefined;
        const published: Array<PersistedJson> = [];

        modelRequestHolds.set(
          old,
          Effect.acquireUseRelease(
            Effect.sync(() => {
              oldActive++;
              oldEntered++;
            }),
            () =>
              Deferred.await(releaseModel).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    oldCompleted = true;
                  }),
                ),
              ),
            () =>
              Effect.sync(() => {
                oldActive--;
              }),
          ),
        );
        modelRequestHolds.set(
          fresh,
          Effect.sync(() => {
            freshEntered++;
          }),
        );
        hostMaintenanceControls.set(thread, {
          // Covers the fixture's phase waits and the real native abort poll. Success must
          // still precede cleanup release; no assertion depends on exhausting this allowance.
          dispatchTimeoutMillis: 5_000,
          pendingDeadline: Effect.sync(() =>
            cleanupActive ||
            (command !== undefined && abort === undefined) ||
            (freshReceipt !== undefined && published.length === 0)
              ? Option.some(0)
              : Option.none(),
          ),
          drainUntil: (dispatchClosed, _dispatchUntil, activity) =>
            Effect.gen(function* () {
              const wakes = yield* WakeScheduler;
              const hinted = yield* Stream.toPull(wakes.wakes);
              const checked = yield* Stream.toPull(activity.changes);
              const notified = Effect.raceFirst(hinted, checked);

              const done = yield* Effect.forkScoped(
                dispatchClosed.pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      closed = true;
                    }),
                  ),
                ),
              );

              const control = Effect.gen(function* () {
                if (command !== undefined && abort === undefined) {
                  abort = yield* maintenance.withMutation(runtime.abort(command));
                }
                if (freshReceipt === undefined || published.length > 0) return;
                const state = yield* runtime.submissionStatus(freshReceipt);

                if (state._tag !== "settled") return;

                const records = yield* Stream.runCollect(
                  store.read(
                    ThreadRead.make({
                      threadId: decodeThreadId(fresh),
                      limit: 100,
                    }),
                  ),
                );

                const completed = records.find(
                  ({ record }) => record.payload._tag === "RunCompleted",
                )?.record.payload;

                if (completed?._tag === "RunCompleted") published.push(completed.output);
              }).pipe(
                Effect.mapError((cause) =>
                  DurableAlarmError.make({
                    operation: "fixture host control",
                    message: "Host control failed",
                    cause,
                  }),
                ),
              );

              yield* ThreadMaintenanceActivity.all(activity, [
                (child) =>
                  Effect.gen(function* () {
                    yield* child.run(child.ready.pipe(Effect.andThen(control)));
                    while (done.pollUnsafe() === undefined) {
                      const changed = yield* Effect.raceFirst(
                        notified.pipe(
                          Effect.as(true),
                          Effect.catch(() => Effect.never),
                        ),
                        Fiber.join(done).pipe(Effect.as(false)),
                      );

                      if (!changed || done.pollUnsafe() !== undefined) return;
                      yield* child.run(control);
                    }
                  }),
                (child) =>
                  child.run(
                    child.ready.pipe(
                      Effect.andThen(
                        Effect.acquireUseRelease(
                          Effect.sync(() => {
                            cleanupActive = true;
                          }),
                          () => Deferred.await(releaseCleanup),
                          () =>
                            Effect.sync(() => {
                              cleanupActive = false;
                            }),
                        ),
                      ),
                    ),
                  ),
              ]);
            }),
        });

        return {
          get oldEntered() {
            return oldEntered;
          },
          get oldActive() {
            return oldActive;
          },
          get oldCompleted() {
            return oldCompleted;
          },
          get freshEntered() {
            return freshEntered;
          },
          get cleanupActive() {
            return cleanupActive;
          },
          get closed() {
            return closed;
          },
          get abort() {
            return abort;
          },
          published,
          clear: (value: AbortCommand) =>
            Effect.sync(() => {
              command = value;
            }),
          replyTo: (receipt: Receipt) =>
            Effect.sync(() => {
              freshReceipt = receipt;
            }),
          release: Deferred.succeed(releaseCleanup, undefined).pipe(
            Effect.andThen(Deferred.succeed(releaseModel, undefined)),
          ),
          receipt: (receipt: Receipt) =>
            ledger.lookup(SubmissionLookupById.make({ submissionId: receipt.submissionId })),
        };
      }),
    );

    const admit = (target: string, key: string) =>
      run(
        Effect.gen(function* () {
          const maintenance = yield* ThreadMaintenance;
          const runtime = yield* DurableAgentRuntime;

          return yield* maintenance.withMutation(
            runtime.submitRegistered(
              { definition: plannerDefinition },
              { question: "retirement", ref: target },
              submitOptions(target, key),
            ),
          );
        }),
      );

    await admit(thread, "bootstrap");
    let retired = false;

    const running = runDurableObjectAlarm(stubFor(thread)).finally(() => {
      retired = true;
    });

    try {
      for (let count = 0; count < 10 && !(await allSettled(thread)()); count++) await advance(100);
      expect(await allSettled(thread)()).toBe(true);
      expect(controls.cleanupActive).toBe(true);
      await advance(100);
      const oldReceipt = await admit(old, "old-model");

      for (let count = 0; count < 5 && controls.oldEntered === 0; count++) await advance(100);
      expect(controls.oldEntered).toBe(1);
      expect(controls.oldActive).toBe(1);

      const command = AbortCommand.make({
        submissionId: oldReceipt.submissionId,
        author: "fixture-owner",
        reason: "retire the previous session",
      });

      await run(controls.clear(command));
      const freshReceipt = await admit(fresh, "fresh-session");

      await run(controls.replyTo(freshReceipt));
      await run(WakeScheduler.use((wakes) => wakes.notify(decodeThreadId(thread))));
      expect(await run(controls.receipt(freshReceipt))).toMatchObject({
        _tag: "Some",
        // Readiness is durable even if the concurrently running scheduler already claimed it.
        value: { readyAt: expect.anything() },
      });
      for (let count = 0; count < 5 && controls.published.length === 0; count++) await advance(100);
      expect({
        closed: controls.closed,
        aborted: controls.abort !== undefined,
        model: controls.freshEntered,
        replies: controls.published,
      }).toEqual({
        closed: false,
        aborted: true,
        model: 1,
        replies: [{ answer: "done" }],
      });
      expect(controls.oldCompleted).toBe(false);
      expect(controls.oldActive).toBe(0);
      expect(controls.cleanupActive).toBe(true);
      expect(retired).toBe(false);
      expect(
        await run(
          ThreadStore.use((store) =>
            Stream.runCollect(
              store.read(ThreadRead.make({ threadId: decodeThreadId(fresh), limit: 100 })),
            ).pipe(Effect.map((records) => records.map(({ record }) => record.payload._tag))),
          ),
        ),
      ).toContain("ModelResponseRecorded");
      expect(
        await run(
          SubmissionLedger.use((ledger) =>
            ledger.readAbortIntent(
              AbortIntentRequest.make({ submissionId: oldReceipt.submissionId }),
            ),
          ),
        ),
      ).toMatchObject({
        ...command,
        requestedAt: controls.abort?.requestedAt,
        canonicalRecordId: expect.any(String),
      });
      expect(
        await run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(oldReceipt))),
      ).toMatchObject({
        _tag: "settled",
        settlement: { outcome: "aborted" },
      });
      expect(await admit(old, "old-model")).toEqual(oldReceipt);
      expect(await admit(fresh, "fresh-session")).toEqual(freshReceipt);
    } finally {
      await run(controls.release);
      modelRequestHolds.delete(old);
      modelRequestHolds.delete(fresh);
      for (let count = 0; count < 20 && !retired; count++) await advance(100);
      await running;
      hostMaintenanceControls.delete(thread);
      maintenanceClocks.delete(thread);
    }
  });

  // Regression: https://github.com/danieljvdm/effect-agent/commit/0fe79ac5
  it.each(["host", "projection"] as const)(
    "executes admitted work while the same %s delivery remains in flight",
    (held) =>
      withThread(async (thread, _now, advance) => {
        let entered!: () => void;
        let release!: () => void;

        const started = new Promise<void>((resolve) => {
          entered = resolve;
        });

        const response = new Promise<void>((resolve) => {
          release = resolve;
        });

        let wakes = 0;
        let released = false;
        let retired = false;

        projectionControls.set(thread, { skipLive: true });
        await submit(thread, plannerDefinition);
        if (held === "projection") {
          projectionControls.set(thread, {
            skipLive: true,
            operation: "drain",
            entered: () => entered(),
            release: response,
          });
        }
        hostMaintenanceControls.set(thread, {
          pendingDeadline: Effect.succeed(held === "host" ? Option.some(0) : Option.none()),
          dispatchTimeoutMillis: 1_000,
          drainUntil: (_closed, _until, activity) =>
            Effect.gen(function* () {
              const scheduler = yield* WakeScheduler;
              const notified = yield* Stream.toPull(scheduler.wakes);

              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  released = true;
                }),
              );
              yield* notified.pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    wakes++;
                  }),
                ),
                Effect.forever,
                Effect.forkScoped,
              );
              yield* activity.run(
                activity.ready.pipe(
                  Effect.andThen(
                    Effect.gen(function* () {
                      if (held !== "host") return;
                      entered();
                      yield* Effect.promise(() => response);
                    }),
                  ),
                ),
              );
            }),
        });

        const running = alarm(thread).then(
          () => {
            retired = true;

            return undefined;
          },
          (cause: unknown) => {
            retired = true;

            return cause;
          },
        );

        try {
          await started;
          for (
            let attempt = 0;
            attempt < 200 && !(await allSettled(thread, namespace)());
            attempt++
          ) {
            await Promise.resolve();
          }
          expect(await allSettled(thread, namespace)()).toBe(true);
          expect(retired).toBe(false);
          const previousWakes = wakes;

          await runClient(
            Effect.flatMap(CloudflareThreadClient, (client) =>
              client.submit(
                { definition: plannerDefinition },
                { question: "new input", ref: thread },
                submitOptions(thread, "next-input"),
              ),
            ),
            namespace,
          );
          expect(wakes).toBeGreaterThan(previousWakes);
          expect(released).toBe(false);
          await advance(100);
          expect(retired).toBe(false);
          expect((await laneRows(thread, namespace)).map((row) => row.state)).toEqual([
            "settled",
            "settled",
          ]);
          await advance(1_000);
          expect(retired).toBe(true);
          expect(released).toBe(true);
          expect((await laneRows(thread, namespace)).map((row) => row.state)).toEqual([
            "settled",
            "settled",
          ]);
          expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
          const outcome = await running;

          expect(held === "host" ? String(outcome) : outcome).toEqual(
            held === "host"
              ? expect.stringContaining("host wave exceeded its allowance")
              : undefined,
          );
        } finally {
          hostMaintenanceControls.delete(thread);
          projectionControls.delete(thread);
          release();
          await running;
        }
        await alarm(thread);
        expect(await allSettled(thread, namespace)()).toBe(true);
        await quiesce(thread);
      }),
  );

  // Regression: https://github.com/danieljvdm/effect-agent/commit/0fe79ac5
  it.each(["maintenance:checkpoint:before", "maintenance:checkpoint:after"] as const)(
    "recovers native progress and pending projection after eviction at %s",
    (location) =>
      withThread(async (thread) => {
        projectionControls.set(thread, { skipLive: true });
        await submit(thread, plannerDefinition);

        const outcome = await runInDurableObject(stub(thread), (instance, state) =>
          instance[DurableObject.RunSymbol](
            ThreadMaintenance.use((maintenance) => maintenance.pass).pipe(
              Effect.provide(Layer.fresh(ThreadMaintenance.layer)),
              Effect.provideService(ThreadMaintenanceFailpoint, {
                hit: (at) =>
                  at === location
                    ? Effect.sync(() => state.abort("native checkpoint eviction"))
                    : Effect.void,
              }),
              Effect.provideService(ThreadHostMaintenance, {
                dispatchTimeoutMillis: 1_000,
                pendingDeadline: Effect.succeed(Option.none()),
                drainUntil: () => Effect.never,
              }),
              Effect.exit,
            ),
          ),
        ).catch((cause) => Exit.die(cause));

        expect(
          Exit.isFailure(outcome) ? Cause.pretty(outcome.cause) : "unexpected success",
        ).toContain("native checkpoint eviction");
        expect(await allSettled(thread, namespace)()).toBe(true);
        const canonical = await readCanonical(thread, namespace);

        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
        projectionControls.delete(thread);
        await quiesce(thread);
        expect(await readCanonical(thread, namespace)).toEqual(canonical);
        expect(await watermark(thread)).toBe(canonical.at(-1)!.sequence);
        expect(await allSettled(thread, namespace)()).toBe(true);
      }),
  );

  it("does not let a future projection deadline gate approval publication or execution", () =>
    withThread(async (thread, now) => {
      const receipt = await submit(thread, approvalDefinition);

      await drainAlarmsUntil(thread, anyInState(thread, "suspended", namespace), { namespace });
      await quiesce(thread);
      projectionControls.set(thread, { skipLive: true, retryAt: now + 25 });
      await runClient(
        Effect.flatMap(CloudflareThreadClient, (client) =>
          client.resolveApproval(
            decodeThreadId(thread),
            ApprovalDecisionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: BOOK_TOOL_CALL_ID,
              decision: "approved",
              resolver: "projection-test",
              reason: "approved",
            }),
          ),
        ),
        namespace,
      );
      await alarm(thread);
      expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
      expect(await watermark(thread)).toBeLessThan(
        (await readCanonical(thread, namespace)).at(-1)!.sequence,
      );
      expect(await scheduledAlarm(thread, namespace)).toBeLessThanOrEqual(now + 25);
      projectionControls.delete(thread);
      await quiesce(thread);
    }));

  it("waits for an in-flight predecessor before committing and serving the next lookup", () =>
    withThread(async (thread) => {
      const firstRequest = await prepareAppend(thread, 1);
      let enter!: () => void;
      let release!: () => void;

      const entered = new Promise<void>((resolve) => {
        enter = resolve;
      });

      const released = new Promise<void>((resolve) => {
        release = resolve;
      });

      projectionControls.set(thread, { operation: "live", entered: enter, release: released });
      const first = append(thread, firstRequest);

      await entered;
      // The predecessor has committed its source, but its derived cursor is still at zero.
      const nextRequest = await prepareAppend(thread, 1);

      projectionControls.delete(thread);
      armMaintenancePause(thread, "maintenance:mutation:armed");

      const next = runInDurableObject(stub(thread), (instance) =>
        instance[DurableObject.RunSymbol](
          Effect.gen(function* () {
            yield* (yield* ThreadStore).append(nextRequest);

            return yield* (yield* ProjectionIndex).lookup;
          }).pipe(Effect.exit),
        ),
      );

      await awaitMaintenancePause(thread, "maintenance:mutation:armed");
      releaseMaintenancePause(thread);
      try {
        // This read crosses the Object event boundary while the second producer is active.
        expect((await readCanonical(thread, namespace)).at(-1)?.sequence).toBe(1);
      } finally {
        release();
        await first;
      }
      const result = await next;

      expect(Exit.isSuccess(result) && result.value).toBe(2);
      await quiesce(thread);
    }));

  it("retains a producer racing projection-only acknowledgement", () =>
    withThread(async (thread) => {
      await alarm(thread);
      const request = await prepareAppend(thread, 1);

      armMaintenancePause(thread, "maintenance:finish:before");
      const running = alarm(thread);

      await awaitMaintenancePause(thread, "maintenance:finish:before");
      try {
        projectionControls.set(thread, { skipLive: true });
        await append(thread, request);
      } finally {
        projectionControls.delete(thread);
        releaseMaintenancePause(thread);
        await running;
      }
      expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      await quiesce(thread);
      expect(await watermark(thread)).toBe(1);
    }));
});

it("exposes index services and preserves distinct publication and projection E/R", () => {
  class Destination extends Context.Service<Destination, {}>()("test/ProjectionDestination") {}
  class SetupError extends Schema.TaggedError<SetupError>()("ProjectionSetupError", {}) {}

  const projection = projectionLayer.pipe(
    Layer.provide(
      Layer.effect(SqlClient)(
        Effect.gen(function* () {
          yield* Destination;

          return yield* SetupError.make({});
        }),
      ),
    ),
  );

  const runtime = ThreadObject.layer([], { projection }).pipe(
    Layer.provide(ThreadObject.layerConfig({ deploymentId: "test", producerPrefix: "test" })),
  );

  expectTypeOf<
    Extract<Layer.Success<typeof runtime>, ProjectionIndex>
  >().toEqualTypeOf<ProjectionIndex>();
  expectTypeOf<Extract<Layer.Services<typeof runtime>, Destination>>().toEqualTypeOf<Destination>();
  expectTypeOf<Extract<Layer.Error<typeof runtime>, SetupError>>().toEqualTypeOf<SetupError>();
  expectTypeOf<
    Extract<Layer.Success<typeof runtime>, ThreadProjectionMaintenance>
  >().toEqualTypeOf<ThreadProjectionMaintenance>();
  expectTypeOf<Exclude<Layer.Services<typeof runtime>, Destination>>().toEqualTypeOf<
    DurableObjectContext | ThreadObjectNamespace
  >();

  const activity: ThreadMaintenanceActivity = {
    run: (wave) => wave,
    ready: Effect.void,
    changes: Stream.empty,
  };

  const pumps = ThreadMaintenanceActivity.all(activity, [
    () => Destination.use(() => SetupError.make({})),
  ]);

  expectTypeOf<Effect.Error<typeof pumps>>().toEqualTypeOf<SetupError>();
  expectTypeOf<Effect.Services<typeof pumps>>().toEqualTypeOf<Destination>();
});
