import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Clock, Effect, Option, Schema } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ThreadId } from "effect-agent/identifiers";
import { ApprovalDecisionCommand, SubmissionLedger } from "effect-agent/submission-ledger";
import { ThreadStore } from "effect-agent/thread-store";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { ThreadMaintenance } from "../src/Alarm.ts";
import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import {
  approvalDefinition,
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
} from "./publication-fixture.ts";

const namespace = "PUBLICATIONS";
const stub = (thread: string) => stubFor(thread, namespace);

const alarm = (thread: string) =>
  runInDurableObject(stub(thread), (instance) => Promise.resolve(instance.alarm()));

const cursor = (thread: string) =>
  runInDurableObject(stub(thread), async (_, state) =>
    Schema.decodeUnknownSync(PublicationCursor)(await state.storage.get(PUBLICATION_KEY)),
  );

const generation = (thread: string) =>
  runInDurableObject(stub(thread), async (_, state) =>
    Schema.decodeUnknownSync(
      Schema.Struct({ dirty: Schema.BigIntFromString, processed: Schema.BigIntFromString }),
    )(await state.storage.get("effect-agent:thread-maintenance:v1")),
  );

const submit = (
  thread: string,
  definition: typeof plannerDefinition | typeof approvalDefinition = plannerDefinition,
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
          modelRequestHolds.delete(thread);
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
  it("gates the actual routed runtime on retained lifecycle debt before invoking a provider", () =>
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
      const foreignId = Schema.decodeSync(ThreadId)("foreign-owner");

      const result = await runInDurableObject(stub(thread), (instance) =>
        instance[DurableObject.RunSymbol](
          DurableAgentRuntime.use((runtime) => runtime.processThreadHead(threadId)),
        ),
      );

      expect(providerCalls).toBe(0);
      expect(Option.isNone(result)).toBe(true);
      expect((await laneRows(thread, namespace))[0]?.submission_id).toBe(receipt.submissionId);

      const foreignDeadline = await runInDurableObject(stub(thread), (instance) =>
        instance[DurableObject.RunSymbol](
          ThreadStore.use((store) =>
            store.lifecyclePublications === undefined
              ? Effect.die("Missing lifecycle storage")
              : store.lifecyclePublications.pendingDeadlineFor(foreignId).pipe(Effect.flip),
          ),
        ),
      );

      expect(foreignDeadline).toMatchObject({
        _tag: "LifecyclePublicationError",
        reason: "unavailable",
      });
      failedLifecycleThreads.delete(thread);
      await advance(11_000);
      await drainAlarmsUntil(thread, allSettled(thread, namespace), { namespace });
      expect(providerCalls).toBe(1);
      expect((await laneRows(thread, namespace))[0]?.submission_id).toBe(receipt.submissionId);
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
          const deadline = await scheduledAlarm(thread, namespace);

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
          expect(await scheduledAlarm(thread, namespace)).toBe(deadline);
          const state = await generation(thread);

          expect(state.dirty).toBeGreaterThan(state.processed);
          await advance(deadline! - current);
          current = deadline!;
        }
        publicationControls.delete(thread);
        await runDurableObjectAlarm(stub(thread));
        await quiesce(thread);
        expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
      }),
  );
});
