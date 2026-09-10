import { Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { expect, expectTypeOf, it } from "vite-plus/test";

import { BenchmarkError } from "../src/contracts.ts";
import {
  DiagnosticProgress,
  DiagnosticResult,
  MAX_DIAGNOSTIC_MARKS,
  type DiagnosticMark,
} from "../src/diagnostic-contracts.ts";
import { policyCases, runPolicyCase } from "../src/diagnostic-policy.ts";

it.each(policyCases)("validates the public policy workload $name", async (workload) => {
  const marks: Array<DiagnosticMark> = [];
  const phases: Array<string> = [];

  const result = await Effect.runPromise(
    runPolicyCase(workload).pipe(
      Effect.provide(
        Layer.succeed(DiagnosticProgress, {
          phase: (phase) =>
            Effect.sync(() => {
              phases.push(phase);
            }),
          mark: (mark) =>
            Effect.sync(() => {
              marks.push(mark);
            }),
        }),
      ),
    ),
  );

  const rounds = workload.parameters.rounds!;

  expect(Schema.is(DiagnosticResult)(result)).toBe(true);
  expect(phases).toEqual(["setup", "operation", "verification"]);
  expect(Object.fromEntries(result.counters.map(({ name, value }) => [name, value]))).toMatchObject(
    {
      providers: rounds + 1,
      streamFinalizers: rounds + 1,
      approvals: rounds * 8,
      authorizations: rounds * 8,
      authorizationFinalizers: rounds * 8,
      handlers: rounds * 8,
      handlerFinalizers: rounds * 8,
      modelsOpened: rounds + 1,
      modelsReady: rounds + 1,
      modelsClosed: rounds + 1,
    },
  );
  expect(marks.length).toBeLessThanOrEqual(MAX_DIAGNOSTIC_MARKS);
  expect(marks.findIndex(({ name }) => name === "authorize:end:7")).toBeLessThan(
    marks.findIndex(({ name }) => name === "handler:start:0"),
  );
  expect(marks.filter(({ name }) => name.startsWith("provider:")).length).toBe(rounds + 1);
});

it("rejects unknown policy cases", async () => {
  const failure = await Effect.runPromise(
    runPolicyCase({ name: "unknown", family: "policy", parameters: {} }).pipe(
      Effect.provideService(DiagnosticProgress, {
        phase: () => Effect.void,
        mark: () => Effect.void,
      }),
      Effect.flip,
    ),
  );

  expect(failure).toBeInstanceOf(BenchmarkError);
});

it.each(["authorize:start:0", "model:open:1"])(
  "closes the active model on interruption at %s without starting handlers",
  async (boundary) => {
    const marks: Array<string> = [];

    await Effect.runPromise(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();

        const workload = policyCases.find(
          (workload) =>
            workload.parameters.authorizationMs === 20 &&
            workload.parameters.modelAcquisitionMs === 20,
        )!;

        const fiber = yield* Effect.forkChild(
          runPolicyCase(workload).pipe(
            Effect.provideService(DiagnosticProgress, {
              phase: () => Effect.void,
              mark: ({ name }) =>
                Effect.gen(function* () {
                  marks.push(name);
                  if (name === boundary) yield* Deferred.succeed(entered, undefined);
                }),
            }),
          ),
        );

        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
      }).pipe(Effect.scoped, Effect.timeout("5 seconds")),
    );
    expect(marks).toContain("model:close:1");
    expect(marks.some((name) => name.startsWith("handler:start:"))).toBe(false);
  },
);

it("closes the acquired model when phase recording fails", async () => {
  const marks: Array<string> = [];

  const result = await Effect.runPromiseExit(
    runPolicyCase(policyCases[0]!).pipe(
      Effect.provideService(DiagnosticProgress, {
        phase: () => Effect.void,
        mark: ({ name }) =>
          Effect.gen(function* () {
            marks.push(name);
            if (name === "model:open:1")
              return yield* BenchmarkError.make({ message: "evidence failed" });
          }),
      }),
    ),
  );

  expect(Exit.isFailure(result)).toBe(true);
  expect(marks).toContain("model:close:1");
  expect(marks.some((name) => name.startsWith("provider:"))).toBe(false);
});

it("exposes diagnostic progress and typed fixture failure", () => {
  expectTypeOf<
    Effect.Services<ReturnType<typeof runPolicyCase>>
  >().toEqualTypeOf<DiagnosticProgress>();
  expectTypeOf<Effect.Error<ReturnType<typeof runPolicyCase>>>().toEqualTypeOf<BenchmarkError>();
});
