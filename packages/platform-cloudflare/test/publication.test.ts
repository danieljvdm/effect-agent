import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { ApprovalDecisionCommand, SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import { ThreadStore } from "@effect-agent/thread/ThreadStore";
import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Context, Effect, Exit, Layer, Schema } from "effect";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import type { DurableAlarmError, ThreadMutationGate } from "../src/Alarm.ts";
import { ThreadMaintenance, ThreadPublication } from "../src/Alarm.ts";
import {
  DurableObjectContext,
  ThreadObjectIdentity,
  ThreadObjectNamespace,
} from "../src/CloudflareBindings.ts";
import type { CloudflarePlatformConfigError } from "../src/CloudflareConfig.ts";
import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import * as ThreadObject from "../src/ThreadObject.ts";
import {
  approvalDefinition,
  plannerDefinition,
  submitOptions,
  maintenanceClocks,
  armMaintenancePause,
  awaitMaintenancePause,
  releaseMaintenancePause,
  armStorageEviction,
  armedEvictionsRemaining,
  BOOK_TOOL_CALL_ID,
  decodeThreadId,
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
  publicationPreparations,
  publicationResources,
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
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const thread = `publication-${crypto.randomUUID()}`;
      const now = Date.now() + 86_400_000;

      yield* TestClock.setTime(now);
      maintenanceClocks.set(thread, yield* Clock.Clock);
      const clock = yield* TestClock.testClockWith(Effect.succeed);

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          publicationControls.delete(thread);
          publicationPreparations.delete(thread);
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
  it("drains committed intent before runtime recovery and preserves the earliest retry deadline", () =>
    withThread(async (thread, now) => {
      const receipt = await submit(thread, approvalDefinition);

      await drainAlarmsUntil(thread, anyInState(thread, "suspended", namespace), { namespace });
      await quiesce(thread);
      publicationControls.set(thread, { retryAt: now + 25 });
      await runClient(
        CloudflareThreadClient.use((client) =>
          client.resolveApproval(
            decodeThreadId(thread),
            ApprovalDecisionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: BOOK_TOOL_CALL_ID,
              decision: "approved",
              resolver: "publication-test",
              reason: "approved",
            }),
          ),
        ),
        namespace,
      );
      await alarm(thread);
      expect((await laneRows(thread, namespace))[0]?.state).toBe("input-applied");
      expect(await scheduledAlarm(thread, namespace)).toBe(now + 25);
      publicationControls.delete(thread);
      const entered = latch();
      const release = latch();

      publicationControls.set(thread, { entered: entered.resolve, release: release.promise });
      const running = alarm(thread);

      try {
        await entered.promise;
        expect((await laneRows(thread, namespace))[0]?.state).toBe("input-applied");
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      } finally {
        publicationControls.delete(thread);
        release.resolve();
        await running;
      }
      await quiesce(thread);
      expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
      expect((await cursor(thread)).decisions).toEqual(["approved"]);
      expect((await cursor(thread)).tail).toBe(
        (await readCanonical(thread, namespace)).at(-1)?.sequence,
      );
    }));

  it("does not certify a generation while a producer is still in flight", () =>
    withThread(async (thread) => {
      await alarm(thread);
      const before = await cursor(thread);

      armMaintenancePause(thread, "maintenance:mutation:armed");
      const mutation = mutate(thread, 1);

      await awaitMaintenancePause(thread, "maintenance:mutation:armed");
      try {
        await alarm(thread);
        expect((await cursor(thread)).generation).toBe(before.generation);
        const state = await generation(thread);

        expect(state.dirty).toBeGreaterThan(state.processed);
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
      } finally {
        releaseMaintenancePause(thread);
        await mutation;
      }
      await quiesce(thread);
      expect((await cursor(thread)).source).toBe(1);
    }));

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

  it.each(["failure", "defect", "interruption", "timeout"] as const)(
    "retains a prearmed native alarm across publication %s",
    (failure) =>
      withThread(async (thread) => {
        await submit(thread);
        publicationControls.set(thread, { failure });
        await expect(alarm(thread)).rejects.toBeDefined();
        const resources = publicationResources.get(thread);

        expect(resources?.acquired).toBeGreaterThan(0);
        expect(resources?.released).toBe(resources?.acquired);
        expect(await scheduledAlarm(thread, namespace)).not.toBeNull();
        const state = await generation(thread);

        expect(state.dirty).toBeGreaterThan(state.processed);
        publicationControls.delete(thread);
        await runDurableObjectAlarm(stub(thread));
        await quiesce(thread);
        expect((await laneRows(thread, namespace))[0]?.state).toBe("settled");
      }),
  );
});

class PublicationSetupError extends Schema.TaggedError<PublicationSetupError>()(
  "PublicationSetupError",
  {},
) {}
class Destination extends Context.Service<Destination, { readonly enabled: boolean }>()(
  "test/PublicationDestination",
) {}

it("preserves publication setup E/R/Scope and releases resources on typed initialization failure", async () => {
  const lifecycle: Array<string> = [];

  const publication = Layer.effect(ThreadPublication)(
    Effect.gen(function* () {
      yield* Destination;
      yield* ThreadStore;
      yield* SubmissionLedger;
      yield* SqlClient;
      yield* ThreadObjectIdentity;
      yield* DurableObjectContext;
      yield* Effect.acquireRelease(
        Effect.sync(() => lifecycle.push("acquired")),
        () => Effect.sync(() => lifecycle.push("released")),
      );

      return yield* PublicationSetupError.make({});
    }),
  );

  const runtime = ThreadObject.layer([], { publication }).pipe(
    Layer.provide(
      ThreadObject.layerConfig({ deploymentId: "publication", producerPrefix: "publication" }),
    ),
  );

  expectTypeOf<Layer.Error<typeof runtime>>().toEqualTypeOf<
    ThreadObject.InitializationError | PublicationSetupError
  >();
  expectTypeOf<Layer.Services<typeof runtime>>().toEqualTypeOf<
    Destination | DurableObjectContext | ThreadObjectNamespace
  >();
  expectTypeOf<ThreadPublication["Service"]["drain"]>().toEqualTypeOf<
    Effect.Effect<void, DurableAlarmError>
  >();
  await runInDurableObject(stub("publication-scope"), (_, state) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const exit = yield* Layer.build(
          runtime.pipe(
            Layer.provide([
              Layer.succeed(Destination, { enabled: true }),
              DurableObjectContext.layer(state, env),
              ThreadObjectNamespace.layer(env.PUBLICATIONS),
            ]),
          ),
        ).pipe(Effect.scoped, Effect.exit);

        expect(Exit.isFailure(exit) && Cause.findErrorOption(exit.cause)).toMatchObject({
          _tag: "Some",
          value: { _tag: "PublicationSetupError" },
        });
        expect(lifecycle).toEqual(["acquired", "released"]);
      }),
    ),
  );
});

it("provides the native gate to fresh maintenance and runtime Layers without new requirements", () => {
  const rebuilt = Layer.fresh(ThreadMaintenance.layer).pipe(
    Layer.provideMerge(DurableAgentRuntime.layerWithBindings([])),
    Layer.provideMerge(ThreadObject.layer([])),
  );

  expectTypeOf<
    Extract<ThreadObject.Services, ThreadMutationGate>
  >().toEqualTypeOf<ThreadMutationGate>();
  expectTypeOf<Layer.Services<typeof rebuilt>>().toEqualTypeOf<
    ThreadObject.BootstrapServices | DurableObjectContext | ThreadObjectNamespace
  >();
  expectTypeOf<Layer.Error<typeof rebuilt>>().toEqualTypeOf<
    Exclude<ThreadObject.InitializationError, CloudflarePlatformConfigError>
  >();
});
