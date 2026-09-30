import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Clock, Effect, Option, Schema } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ThreadId } from "effect-agent/identifiers";
import { ApprovalDecisionCommand, SubmissionLedger } from "effect-agent/submission-ledger";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it, vi } from "vite-plus/test";

import { ThreadMaintenance } from "../src/Alarm.ts";
import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import * as DueQueue from "../src/internal/due-queue.ts";
import {
  approvalDefinition,
  bookDefinition,
  bookToolHolds,
  plannerDefinition,
  submitOptions,
  maintenanceClocks,
  modelRequestHolds,
  armMaintenancePause,
  awaitMaintenancePause,
  releaseMaintenancePause,
  armStorageEviction,
  armedEvictionsRemaining,
  BOOK_TOOL_CALL_ID,
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
  PublicationCursor,
  PUBLICATION_KEY,
  SOURCE_KEY,
  publicationControls,
  publicationResources,
  failedLifecycleThreads,
  lifecycleBatches,
  lifecyclePublicationControls,
} from "./publication-fixture.ts";

const namespace = "PUBLICATIONS";
const stub = (thread: string) => stubFor(thread, namespace);

const alarm = (thread: string) =>
  runInDurableObject(stub(thread), (instance) => Promise.resolve(instance.alarm()));

const cursor = (thread: string) =>
  runInDurableObject(stub(thread), async (_, state) =>
    Schema.decodeUnknownSync(PublicationCursor)(await state.storage.get(PUBLICATION_KEY)),
  );

const lifecycleRows = (thread: string) =>
  runInDurableObject(stub(thread), (_, state) =>
    Schema.decodeUnknownSync(
      Schema.Array(
        Schema.Struct({
          id: Schema.String,
          ordinal: Schema.Number,
          fingerprint: Schema.String,
          payload_json: Schema.NullOr(Schema.String),
        }),
      ),
    )(
      state.storage.sql
        .exec(
          "SELECT id, ordinal, fingerprint, payload_json FROM effect_agent_lifecycle_publications ORDER BY ordinal",
        )
        .toArray(),
    ),
  );

const generation = (thread: string) =>
  runInDurableObject(stub(thread), async (_, state) =>
    Schema.decodeUnknownSync(
      Schema.Struct({ dirty: Schema.BigIntFromString, processed: Schema.BigIntFromString }),
    )(await state.storage.get("effect-agent:thread-maintenance:v1")),
  );

const submit = (
  thread: string,
  definition:
    | typeof plannerDefinition
    | typeof approvalDefinition
    | typeof bookDefinition = plannerDefinition,
) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition },
        { question: "publication", ref: thread },
        submitOptions(thread, thread),
      );
    }),
    namespace,
  );

const mutate = (thread: string, source: number) =>
  runInDurableObject(stub(thread), (instance, state) =>
    instance[DurableObject.RunSymbol](
      ThreadMaintenance.use((maintenance) =>
        maintenance.withMutation(Effect.promise(() => state.storage.put(SOURCE_KEY, source))),
      ),
    ),
  );

const quiesce = (thread: string) =>
  drainAlarmsUntil(thread, async () => (await scheduledAlarm(thread, namespace)) === null, {
    namespace,
  });

const withThread = (
  test: (thread: string, now: number, advance: (millis: number) => Promise<void>) => Promise<void>,
  lifecycle = false,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const thread = `${lifecycle ? "lifecycle-publication" : "publication"}-${crypto.randomUUID()}`;
      const now = Date.now() + 86_400_000;

      yield* TestClock.setTime(now);
      maintenanceClocks.set(thread, yield* Clock.Clock);
      const clock = yield* TestClock.testClockWith(Effect.succeed);

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          failedLifecycleThreads.delete(thread);
          lifecycleBatches.delete(thread);
          modelRequestHolds.delete(thread);
          bookToolHolds.delete(thread);
          lifecyclePublicationControls.delete(thread);
          publicationControls.delete(thread);
          publicationResources.delete(thread);
          maintenanceClocks.delete(thread);
          releaseMaintenancePause(thread);
        }),
      );
      yield* Effect.promise(() =>
        test(thread, now, (millis) => Effect.runPromise(clock.adjust(millis))),
      );
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

const latch = () => {
  let resolve!: () => void;

  const promise = new Promise<void>((done) => {
    resolve = done;
  });

  return { promise, resolve: () => resolve() };
};

describe("durable host publication", () => {
  // Regression: 405916b0 cleared concurrent publication after the first eight-fact batch.
  // A single-admission Run cannot expose start progress hidden behind queued readiness facts.
  it("publishes start progress behind a queued backlog while the provider remains held", () =>
    withThread(async (thread) => {
      await submit(thread);
      await quiesce(thread);
      lifecycleBatches.delete(thread);
      const entered = latch();
      const release = latch();

      modelRequestHolds.set(
        thread,
        Effect.sync(entered.resolve).pipe(Effect.andThen(Effect.promise(() => release.promise))),
      );
      for (let index = 0; index < 10; index++)
        await runClient(
          CloudflareThreadClient.use((client) =>
            client.submit(
              { definition: plannerDefinition },
              { question: "backlog", ref: thread },
              submitOptions(thread, `backlog-${index}`),
            ),
          ),
          namespace,
        );
      const running = alarm(thread);

      try {
        await entered.promise;
        await vi.waitFor(
          () => {
            const batches = lifecycleBatches.get(thread) ?? [];

            expect(batches.flat().some((p) => p.fact._tag === "RunStarted")).toBe(true);
            expect(batches.every((batch) => batch.length <= 8)).toBe(true);
            expect(batches.flat().some((p) => p.fact._tag === "SubmissionSettled")).toBe(false);
          },
          { timeout: 2_000 },
        );
      } finally {
        release.resolve();
        await running;
      }
      await quiesce(thread);
    }, true));

  // Regression: #713 deferred the entire start prefix until native execution settled.
  // a87f948f then rescheduled empty start waves for undrainable canonical Tool intent.
  // Hold the Tool and delay acknowledgement to expose that suffix before the deadline check.
  it("publishes start progress, then stays dormant during a held tool until settlement", () =>
    withThread(async (thread, _now, advance) => {
      const entered = latch();
      const release = latch();
      const acknowledge = latch();

      bookToolHolds.set(
        thread,
        Effect.sync(entered.resolve).pipe(Effect.andThen(Effect.promise(() => release.promise))),
      );
      lifecyclePublicationControls.set(thread, { release: acknowledge.promise });
      await submit(thread, bookDefinition);
      const running = alarm(thread);
      const interrupted = running.catch(() => undefined);

      const startLane = () =>
        runInDurableObject(stub(thread), (_, state) =>
          DueQueue.make(state.storage)
            .read()
            .find((row) => row.id === DueQueue.LifecycleStart),
        );

      let dormantObserved = false;

      try {
        await entered.promise;
        acknowledge.resolve();

        const prepared = await runInDurableObject(stub(thread), (_, state) =>
          Schema.decodeUnknownSync(Schema.Struct({ prepared: Schema.Natural }))(
            state.storage.sql
              .exec(
                "SELECT COUNT(*) AS prepared FROM effect_agent_canonical_records WHERE json_extract(record_json, '$.payload._tag') = 'ToolCallPrepared'",
              )
              .one(),
          ),
        );

        expect(prepared.prepared).toBe(1);
        await vi.waitFor(
          () => {
            expect((lifecycleBatches.get(thread) ?? []).flat().map((p) => p.fact._tag)).toEqual([
              "SubmissionReady",
              "UserInputRecorded",
              "RunStarted",
            ]);
          },
          { timeout: 500 },
        );
        await vi.waitFor(async () => {
          expect((await lifecycleRows(thread)).every((row) => row.payload_json === null)).toBe(
            true,
          );
        });
        await vi.waitFor(async () => expect((await startLane())?.dueAt).toBeNull());
        dormantObserved = true;
        const dormant = await startLane();

        await advance(15_000);
        expect(await startLane()).toEqual(dormant);
        expect(await allSettled(thread, namespace)()).toBe(false);
        expect((lifecycleBatches.get(thread) ?? []).flat()).toHaveLength(3);
      } finally {
        acknowledge.resolve();
        release.resolve();
        if (dormantObserved) await running;
        else {
          // A broken immediate continuation must not strand cleanup at the frozen event clock.
          await runInDurableObject(stub(thread), (_, state) =>
            state.abort("failed start-publication dormancy assertion"),
          ).catch(() => undefined);
          await interrupted;
        }
      }
      await quiesce(thread);
      const batches = lifecycleBatches.get(thread) ?? [];

      expect(batches.flat().map((p) => p.fact._tag)).toEqual([
        "SubmissionReady",
        "UserInputRecorded",
        "RunStarted",
        "SubmissionSettled",
      ]);
      expect(batches.at(-1)?.map((p) => p.fact._tag)).toEqual(["SubmissionSettled"]);
      expect((await lifecycleRows(thread)).every((row) => row.payload_json === null)).toBe(true);
    }, true));

  // Regression: c68edc7a made host publication an execution prerequisite.
  it("runs routed and alarm attempts with publication debt, then publishes bounded ordered batches", () =>
    withThread(async (thread, _now, advance) => {
      let providerCalls = 0;

      modelRequestHolds.set(
        thread,
        Effect.sync(() => {
          providerCalls++;
        }),
      );
      failedLifecycleThreads.add(thread);
      const receipt = await submit(thread);
      const threadId = Schema.decodeSync(ThreadId)(thread);

      const result = await runInDurableObject(stub(thread), (instance) =>
        instance[DurableObject.RunSymbol](
          DurableAgentRuntime.use((runtime) => runtime.processThreadHead(threadId)),
        ),
      );

      expect(providerCalls).toBe(1);
      expect(Option.isSome(result)).toBe(true);
      expect((await laneRows(thread, namespace))[0]?.submission_id).toBe(receipt.submissionId);
      expect(lifecycleBatches.get(thread) ?? []).toEqual([]);

      await runClient(
        CloudflareThreadClient.use((client) =>
          client.submit(
            { definition: plannerDefinition },
            { question: "follow-up", ref: thread },
            submitOptions(thread, "follow-up"),
          ),
        ),
        namespace,
      );
      await alarm(thread).catch(() => undefined);
      expect(providerCalls).toBe(2);
      expect(await allSettled(thread, namespace)()).toBe(true);

      // Regression: bce45cdd loaded the entire owner backlog before persisting a retry.
      await runClient(
        CloudflareThreadClient.use((client) =>
          client.submit(
            { definition: plannerDefinition },
            { question: "backlog", ref: thread },
            submitOptions(thread, "backlog"),
          ),
        ),
        namespace,
      );
      await alarm(thread).catch(() => undefined);
      expect(providerCalls).toBe(3);
      expect(lifecycleBatches.get(thread)).toHaveLength(1);
      lifecycleBatches.delete(thread);

      const before = await lifecycleRows(thread);

      expect(before.length).toBeGreaterThan(8);
      expect(before.every((row) => row.payload_json !== null)).toBe(true);
      failedLifecycleThreads.delete(thread);
      await advance(11_000);
      await quiesce(thread);
      const batches = lifecycleBatches.get(thread) ?? [];

      expect(batches.every((batch) => batch.length <= 8)).toBe(true);
      expect(batches.flatMap((batch) => batch.map((fact) => fact.id))).toEqual(
        before.map((row) => row.id),
      );
      expect(batches.flatMap((batch) => batch.map((fact) => fact.ordinal))).toEqual(
        before.map((row) => row.ordinal),
      );
      expect(batches[0]?.slice(0, 3).map((fact) => fact.fact._tag)).toEqual([
        "SubmissionReady",
        "UserInputRecorded",
        "RunStarted",
      ]);
      expect(await lifecycleRows(thread)).toEqual(
        before.map((row) => ({ ...row, payload_json: null })),
      );
    }, true));

  it("rebuilt maintenance observes an in-flight native ledger producer through the exported gate", () =>
    withThread(async (thread) => {
      const receipt = await submit(thread, approvalDefinition);

      await drainAlarmsUntil(thread, anyInState(thread, "suspended", namespace), { namespace });
      await quiesce(thread);
      const before = await cursor(thread);

      armMaintenancePause(thread, "maintenance:mutation:armed");

      // Bypass ingress maintenance: only the source port's ORIGINAL producer gate is active.
      const producer = runInDurableObject(stub(thread), (instance) =>
        instance[DurableObject.RunSymbol](
          SubmissionLedger.use((ledger) =>
            ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: receipt.submissionId,
                toolCallId: BOOK_TOOL_CALL_ID,
                decision: "approved",
                resolver: "publication-composition-test",
                reason: "share native producer activity",
              }),
            ),
          ),
        ),
      );

      await awaitMaintenancePause(thread, "maintenance:mutation:armed");
      try {
        await alarm(thread);
        expect((await cursor(thread)).generation).toBe(before.generation);
        const state = await generation(thread);

        expect(state.dirty).toBeGreaterThan(state.processed);
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      } finally {
        releaseMaintenancePause(thread);
        await producer;
      }
      await quiesce(thread);
      expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
      expect((await cursor(thread)).decisions).toEqual(["approved"]);
    }));

  it("keeps a producer racing an empty publication drain armed", () =>
    withThread(async (thread) => {
      await alarm(thread);
      const entered = latch();
      const release = latch();

      publicationControls.set(thread, { entered: entered.resolve, release: release.promise });
      const first = mutate(thread, 1);

      await entered.promise;
      publicationControls.delete(thread);
      try {
        await mutate(thread, 2);
        // The older drain now writes a stale cursor after the newer publication acknowledged.
      } finally {
        release.resolve();
        await first;
      }
      expect((await cursor(thread)).source).toBe(1);
      expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      await quiesce(thread);
      expect((await cursor(thread)).source).toBe(2);
    }));

  it("does not erase a new producer when finishing a publication-only pass", () =>
    withThread(async (thread) => {
      await alarm(thread);
      armMaintenancePause(thread, "maintenance:finish:before");
      const running = alarm(thread);

      await awaitMaintenancePause(thread, "maintenance:finish:before");
      try {
        await mutate(thread, 1);
      } finally {
        releaseMaintenancePause(thread);
        await running;
      }
      expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      await quiesce(thread);
      expect((await cursor(thread)).source).toBe(1);
    }));

  it("recertifies a runtime-owned append after eviction between source commit and invalidation", () =>
    withThread(async (thread, _now, advance) => {
      await submit(thread);
      armStorageEviction(thread, "append:after");
      await alarm(thread).catch(() => undefined);
      expect(armedEvictionsRemaining(thread)).toBe(0);
      // A new incarnation must see a generation newer than the scan prepared before runtime work.
      const state = await generation(thread);
      const before = await cursor(thread);

      expect(state.dirty).toBeGreaterThan(BigInt(before.generation));
      expect(state.dirty).toBeGreaterThan(state.processed);
      expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      await advance(1_000);
      await drainAlarmsUntil(thread, allSettled(thread, namespace), { namespace });
      await quiesce(thread);
      expect((await cursor(thread)).tail).toBe(
        (await readCanonical(thread, namespace)).at(-1)?.sequence,
      );
    }));

  it("returns the committed submission when immediate publication fails and repairs it by alarm", () =>
    withThread(async (thread) => {
      publicationControls.set(thread, { failure: "failure" });
      const receipt = await submit(thread);

      // Regression: bce45cdd skipped the required custom host-publication hook.
      expect(publicationResources.get(thread)?.acquired ?? 0).toBeGreaterThan(0);
      expect((await laneRows(thread, namespace))[0]?.submission_id).toBe(receipt.submissionId);
      expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      publicationControls.delete(thread);
      await quiesce(thread);
      expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
      expect((await cursor(thread)).tail).toBe(
        (await readCanonical(thread, namespace)).at(-1)?.sequence,
      );
    }));

  it.each(["failure"] as const)(
    "backs off repeated publication %s across eviction without losing native work",
    (failure) =>
      withThread(async (thread, now, advance) => {
        await submit(thread);
        publicationControls.set(thread, { failure });
        let current = now;

        // Preserve the earned retry deadline across real Object retirement.
        for (const [minimum, maximum] of [
          [5, 10],
          [10, 20],
        ] as const) {
          await expect(alarm(thread)).rejects.toBeDefined();
          const resources = publicationResources.get(thread);

          expect(resources?.acquired).toBeGreaterThan(0);
          expect(resources?.released).toBe(resources?.acquired);
          const scheduled = await scheduledAlarm(thread, namespace);

          const deadline = await runInDurableObject(
            stub(thread),
            (_instance, state) =>
              DueQueue.make(state.storage)
                .read()
                .find((row) => row.id === DueQueue.Publication)!.dueAt,
          );

          expect(deadline).toBeGreaterThanOrEqual(current + minimum);
          expect(deadline).toBeLessThanOrEqual(current + maximum);
          await runInDurableObject(stub(thread), (_instance, state) => {
            state.abort("publication retry restart");
          }).catch(() => undefined);
          await runInDurableObject(stub(thread), (instance) =>
            instance[DurableObject.RunSymbol](
              ThreadMaintenance.use((maintenance) => maintenance.ensureAlarm),
            ),
          );
          expect(await scheduledAlarm(thread, namespace)).toBe(scheduled);
          const state = await generation(thread);

          expect(state.dirty).toBeGreaterThan(state.processed);
          await advance(deadline! - current);
          current = deadline!;
        }
        publicationControls.delete(thread);
        await runDurableObjectAlarm(stub(thread));
        await drainAlarmsUntil(
          thread,
          async () => {
            const next = await scheduledAlarm(thread, namespace);

            if (next === null) return true;
            // Native recovery and publication now retain independent retry deadlines.
            await advance(Math.max(0, next - current));
            current = Math.max(current, next);

            return false;
          },
          { namespace },
        );
        expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
      }),
  );
});
