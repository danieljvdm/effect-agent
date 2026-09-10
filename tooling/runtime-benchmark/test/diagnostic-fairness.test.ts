import { ThreadId } from "@effect-agent/core/Identifiers";
import { NodeDurableHost } from "@effect-agent/platform-node/NodeDurableHost";
import { ScriptedModel } from "@effect-agent/testing/ScriptedModel";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Cause, Deferred, Effect, Exit, Fiber, FileSystem, Layer, Path, Schema } from "effect";
import { Agent } from "effect-agent";
import { Model, Toolkit } from "effect/unstable/ai";
import { expect, expectTypeOf, it } from "vite-plus/test";

import { BenchmarkError } from "../src/contracts.js";
import {
  DiagnosticProgress,
  DiagnosticResult,
  type DiagnosticMark,
} from "../src/diagnostic-contracts.js";
import { fairnessCases, runFairnessCase } from "../src/diagnostic-fairness.js";

const silent = DiagnosticProgress.of({ phase: () => Effect.void, mark: () => Effect.void });

it.each(fairnessCases)(
  "checks public workers, independent lanes and finalizers in $name",
  async (workload) => {
    const marks: Array<DiagnosticMark> = [];
    const phases: Array<string> = [];

    const result = await Effect.runPromise(
      runFairnessCase(workload).pipe(
        Effect.provideService(DiagnosticProgress, {
          phase: (phase) =>
            Effect.sync(() => {
              phases.push(phase);
            }),
          mark: (mark) =>
            Effect.sync(() => {
              marks.push(mark);
            }),
        }),
        Effect.provide(NodeServices.layer),
        Effect.timeout("15 seconds"),
      ),
    );

    const counters = Object.fromEntries(result.counters.map(({ name, value }) => [name, value]));

    expect(Schema.is(DiagnosticResult)(result)).toBe(true);
    expect(phases).toEqual(["setup", "operation", "verification"]);
    expect(marks.length).toBeLessThanOrEqual(512);
    expect(counters).toMatchObject({
      attemptsOpened: 5,
      attemptFinalizers: 5,
      toolsOpened: 16,
      toolFinalizers: 16,
      providers: 9,
      streamFinalizers: 9,
      modelsOpened: 5,
      modelFinalizers: 5,
    });
    expect(counters.maxActiveAttempts).toBeLessThanOrEqual(workload.parameters.workerConcurrency!);
    expect(counters.maxActiveTools).toBeLessThanOrEqual(workload.parameters.workerConcurrency! * 2);
    for (const label of ["busy0", "busy1", "busy2", "busy3", "short"]) {
      expect(counters[`${label}.maxActiveAttempts`]).toBe(1);
      expect(marks.filter(({ name }) => name === `request.${label}`)).toHaveLength(1);
      expect(marks.filter(({ name }) => name === `attempt.open.${label}`)).toHaveLength(1);
      expect(marks.filter(({ name }) => name === `settlement.observed.${label}`)).toHaveLength(1);
      expect(
        result.metrics.find(({ name }) => name === `${label}.requestToAttempt`)?.value,
      ).toBeGreaterThanOrEqual(0);
    }
    if (workload.parameters.warmArrival === 1) {
      expect(counters.shortAdmissionActiveAttempts).toBeGreaterThan(0);
      expect(counters.shortAdmissionActiveTools).toBeGreaterThan(0);
    } else {
      expect(counters.shortAdmissionActiveAttempts).toBe(0);
      expect(counters.shortAdmissionActiveTools).toBe(0);
    }
  },
  20_000,
);

it("rejects altered workloads before acquiring host resources", async () => {
  const marks: Array<DiagnosticMark> = [];

  const result = await Effect.runPromiseExit(
    runFairnessCase({
      ...fairnessCases[0]!,
      parameters: { workerConcurrency: 999 },
    }).pipe(
      Effect.provideService(DiagnosticProgress, {
        ...silent,
        mark: (mark) =>
          Effect.sync(() => {
            marks.push(mark);
          }),
      }),
      Effect.provide(NodeServices.layer),
    ),
  );

  expect(Exit.isFailure(result)).toBe(true);
  expect(marks).toEqual([]);
});

const assertReleased = (marks: ReadonlyArray<string>) => {
  for (const resource of ["attempt", "tool", "model"]) {
    const opened = marks
      .filter((name) => name.startsWith(`${resource}.open.`))
      .map((name) => name.slice(`${resource}.open.`.length))
      .sort();

    const closed = marks
      .filter((name) => name.startsWith(`${resource}.closed.`))
      .map((name) => name.slice(`${resource}.closed.`.length))
      .sort();

    expect(opened.length).toBeGreaterThan(0);
    expect(closed).toEqual(opened);
  }
};

it.each(["typed hook failure converted to defect", "defect"] as const)(
  "preserves the original %s and releases active resources",
  async (mode) => {
    const marks: Array<string> = [];
    const injected = BenchmarkError.make({ message: `original ${mode}` });

    const result = await Effect.runPromiseExit(
      runFairnessCase(fairnessCases[0]!).pipe(
        Effect.provideService(DiagnosticProgress, {
          ...silent,
          mark: ({ name }) =>
            Effect.gen(function* () {
              marks.push(name);
              if (name !== "tool.open.busy0.0") return;

              // The diagnostic Tool has no typed failure channel: its boundary deliberately
              // uses orDie. Verify that conversion retains the original hook error object.
              return yield* mode === "typed hook failure converted to defect"
                ? Effect.fail(injected)
                : Effect.die(injected);
            }),
        }),
        Effect.provide(NodeServices.layer),
        Effect.timeout("10 seconds"),
      ),
    );

    const cause = Exit.isFailure(result) ? result.cause : Cause.empty;
    const defects = cause.reasons.filter(Cause.isDieReason).map(({ defect }) => defect);

    expect(defects[0]).toBe(injected);
    expect(Cause.hasFails(cause)).toBe(false);
    assertReleased(marks);
  },
  15_000,
);

it("preserves a typed verification failure after closing worker resources", async () => {
  const marks: Array<string> = [];
  const injected = BenchmarkError.make({ message: "original verification failure" });

  const result = await Effect.runPromiseExit(
    runFairnessCase(fairnessCases[0]!).pipe(
      Effect.provideService(DiagnosticProgress, {
        phase: (phase) => (phase === "verification" ? Effect.fail(injected) : Effect.void),
        mark: ({ name }) =>
          Effect.sync(() => {
            marks.push(name);
          }),
      }),
      Effect.provide(NodeServices.layer),
      Effect.timeout("10 seconds"),
    ),
  );

  const cause = Exit.isFailure(result) ? result.cause : Cause.empty;
  const errors = cause.reasons.filter(Cause.isFailReason).map(({ error }) => error);
  const original = errors[0];

  expect(errors).toHaveLength(1);
  expect(Schema.is(BenchmarkError)(original) ? original.cause : undefined).toBe(injected);
  expect(Cause.hasDies(cause)).toBe(false);
  expect(Cause.hasInterrupts(cause)).toBe(false);
  assertReleased(marks);
}, 15_000);

it("an enclosing timeout interrupts active work and returns that exact timeout after finalizers", async () => {
  const marks: Array<string> = [];
  const deadline = new Cause.TimeoutError();

  const result = await Effect.runPromiseExit(
    runFairnessCase(fairnessCases[0]!).pipe(
      Effect.provideService(DiagnosticProgress, {
        ...silent,
        mark: ({ name }) =>
          Effect.gen(function* () {
            marks.push(name);
            if (name === "tool.open.busy0.0") return yield* Effect.never;
          }),
      }),
      Effect.provide(NodeServices.layer),
      Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.fail(deadline) }),
      // A hung finalizer must fail with a different TimeoutError, never satisfy this assertion.
      Effect.timeout("10 seconds"),
    ),
  );

  const cause = Exit.isFailure(result) ? result.cause : Cause.empty;
  const errors = cause.reasons.filter(Cause.isFailReason).map(({ error }) => error);

  expect(errors).toHaveLength(1);
  expect(errors[0]).toBe(deadline);
  expect(Cause.isTimeoutError(errors[0])).toBe(true);
  expect(Cause.hasDies(cause)).toBe(false);
  assertReleased(marks);
}, 15_000);

it("joins scoped worker cancellation and finalizes the interrupted tool", async () => {
  const marks: Array<string> = [];

  await Effect.runPromise(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();

      const fiber = yield* runFairnessCase(fairnessCases[0]!).pipe(
        Effect.provideService(DiagnosticProgress, {
          ...silent,
          mark: ({ name }) =>
            Effect.gen(function* () {
              marks.push(name);
              if (name === "tool.open.busy0.0") {
                yield* Deferred.succeed(entered, undefined);

                return yield* Effect.never;
              }
            }),
        }),
        Effect.forkChild,
      );

      yield* Deferred.await(entered).pipe(Effect.timeout("5 seconds"));
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
      expect(Exit.isFailure(exit) && Cause.hasFails(exit.cause)).toBe(false);
      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), Effect.timeout("10 seconds")),
  );
  assertReleased(marks);
}, 15_000);

it("preserves FIFO for two queued inputs joining one public-host Run", async () => {
  let attempts = 0;
  let finalized = 0;
  let requests = 0;

  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "fairness-fifo-" });

      const definition = Agent.make("fairness-fifo", {
        input: Schema.String,
        output: Schema.String,
        toolkit: Toolkit.empty,
        instructions: "Answer both inputs.",
        policy: { maxTurns: 2, maxDuration: "10 seconds" },
      });

      const model = Layer.mergeAll(
        ScriptedModel.layer([
          {
            _tag: "Stream",
            termination: { _tag: "Complete" },
            parts: [
              { type: "text-start", id: "answer" },
              { type: "text-delta", id: "answer", delta: '"done"' },
              { type: "text-end", id: "answer" },
              { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
            ],
            assertRequest: (request) =>
              Effect.sync(() => {
                requests++;

                const users = request.prompt.content
                  .filter((message) => message.role === "user")
                  .flatMap((message) => message.content)
                  .filter((part) => part.type === "text")
                  .map((part) => part.text);

                expect(users.filter((text) => text === '"first"' || text === '"second"')).toEqual([
                  '"first"',
                  '"second"',
                ]);
              }),
          },
        ]),
        Layer.succeed(Model.ProviderName, "scripted"),
        Layer.succeed(Model.ModelName, "fifo"),
      );

      const agent = Agent.withModel(definition, model);

      const definitions = DefinitionDigestInput.make({
        agent: "fifo-v1",
        model: "fifo-v1",
        tools: [],
      });

      const digests = yield* digestDefinitions(definitions).pipe(Effect.provide(NodeCrypto.layer));

      const layer = NodeDurableHost.layerRegistered(
        [
          {
            agent,
            definitions,
            attemptLayer: () =>
              Layer.effectDiscard(
                Effect.acquireRelease(
                  Effect.sync(() => {
                    attempts++;
                  }),
                  () =>
                    Effect.sync(() => {
                      finalized++;
                    }),
                ),
              ),
          },
        ],
        {
          filename: path.join(directory, "fifo.sqlite"),
          deploymentId: "fifo",
          producerId: "fifo",
          workerConcurrency: 1,
        },
      );

      yield* Effect.gen(function* () {
        const host = yield* NodeDurableHost;
        const store = yield* ThreadStore;
        const threadId = ThreadId.make("fifo-thread");

        const receipts = yield* Effect.forEach(["first", "second"], (input) =>
          host.submit(agent, input, {
            threadId,
            principal: Principal.make("fifo"),
            idempotencyKey: IdempotencyKey.make(input),
            definitions: digests,
          }),
        );

        expect(receipts[1]!.queueSequence).toBe(receipts[0]!.queueSequence + 1);
        const worker = yield* host.runResolvedWorkers.pipe(Effect.forkScoped);

        const settlements = yield* Effect.raceFirst(
          Effect.forEach(receipts, host.awaitSettlement, { concurrency: 2 }),
          Fiber.join(worker).pipe(Effect.andThen(Effect.die("FIFO worker exited early"))),
        );

        yield* Fiber.interrupt(worker);
        const log = yield* store.export(ThreadExportRequest.make({ threadId }));
        const payloads = log.records.map(({ record }) => record.payload);

        expect(settlements.map(({ outcome }) => outcome)).toEqual(["completed", "completed"]);
        expect(
          payloads
            .filter((payload) => payload._tag === "UserInputRecorded")
            .map(({ submissionId }) => submissionId),
        ).toEqual(receipts.map(({ submissionId }) => submissionId));
        expect(payloads.filter((payload) => payload._tag === "RunStarted")).toHaveLength(1);
        expect(payloads.filter((payload) => payload._tag === "SubmissionSettled")).toHaveLength(2);
      }).pipe(Effect.provide(layer), Effect.scoped);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), Effect.timeout("10 seconds")),
  );
  expect({ attempts, finalized, requests }).toEqual({ attempts: 1, finalized: 1, requests: 1 });
}, 15_000);

it("keeps platform services and typed fixture failure visible", () => {
  expectTypeOf<Effect.Services<ReturnType<typeof runFairnessCase>>>().toEqualTypeOf<
    DiagnosticProgress | FileSystem.FileSystem | Path.Path
  >();
  expectTypeOf<Effect.Error<ReturnType<typeof runFairnessCase>>>().toEqualTypeOf<BenchmarkError>();
});
