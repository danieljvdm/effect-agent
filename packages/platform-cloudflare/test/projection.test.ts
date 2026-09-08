import { ProducerEpoch } from "@effect-agent/thread/Records";
import { ApprovalDecisionCommand } from "@effect-agent/thread/SubmissionLedger";
import type { ThreadProjectionMaintenance } from "@effect-agent/thread/ThreadProjectionMaintenance";
import {
  FencedAppendRequest,
  ThreadMaterialization,
  ThreadStore,
  ThreadTailRequest,
} from "@effect-agent/thread/ThreadStore";
import { env, runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Context, Effect, Exit, Layer, Schema } from "effect";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import type { DurableObjectContext, ThreadObjectNamespace } from "../src/CloudflareBindings.ts";
import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import * as ThreadObject from "../src/ThreadObject.ts";
import {
  BOOK_TOOL_CALL_ID,
  approvalDefinition,
  plannerDefinition,
  maintenanceClocks,
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
} from "./harness.ts";
import {
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

const withThread = (test: (thread: string, now: number) => Promise<void>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const thread = `projection-${crypto.randomUUID()}`;
      const now = Date.now() + 86_400_000;

      yield* TestClock.setTime(now);
      maintenanceClocks.set(thread, yield* Clock.Clock);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          projectionControls.delete(thread);
          projectionResources.delete(thread);
          projectionConstructions.delete(thread);
          projectionLookups.delete(thread);
          projectionLiveBatches.delete(thread);
          maintenanceClocks.delete(thread);
          releaseMaintenancePause(thread);
        }),
      );
      yield* Effect.promise(() => test(thread, now));
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
      withThread(async (thread) => {
        projectionControls.set(thread, { skipLive: true });
        await submit(thread, plannerDefinition);
        projectionControls.set(thread, { skipLive: true, operation: "drain", failure });
        await expect(alarm(thread)).rejects.toBeDefined();
        expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
        const resources = projectionResources.get(thread);

        expect(resources?.released).toBe(resources?.acquired);
        projectionControls.delete(thread);
        await quiesce(thread);
      }),
  );

  it("preserves interruption and recovers through the prearmed alarm", () =>
    withThread(async (thread) => {
      projectionControls.set(thread, { skipLive: true });
      await submit(thread, plannerDefinition);
      projectionControls.set(thread, {
        skipLive: true,
        operation: "drain",
        failure: "interruption",
      });
      await expect(alarm(thread)).rejects.toBeDefined();
      expect((await laneRows(thread, namespace))[0]?.state).not.toBe("settled");
      expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      const resources = projectionResources.get(thread);

      expect(resources?.released).toBe(resources?.acquired);
      projectionControls.delete(thread);
      await quiesce(thread);
      expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
    }));

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
});
