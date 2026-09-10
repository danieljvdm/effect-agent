import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { expect, it } from "vite-plus/test";

import { BenchmarkError } from "../src/contracts.ts";
import {
  capabilityCases,
  DiagnosticFault,
  runCapabilityCase,
} from "../src/diagnostic-capabilities.ts";
import {
  DiagnosticProgress,
  DiagnosticResult,
  type DiagnosticMark,
  type DiagnosticPhase,
} from "../src/diagnostic-contracts.ts";

const services = Layer.merge(NodeServices.layer, NodeCrypto.layer);
const silent = DiagnosticProgress.of({ phase: () => Effect.void, mark: () => Effect.void });

it.each(capabilityCases)(
  "validates completed work and owned finalizers in $name",
  async (workload) => {
    const phases: Array<DiagnosticPhase> = [];
    const marks: Array<DiagnosticMark> = [];

    const result = await Effect.runPromise(
      runCapabilityCase(workload).pipe(
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
        Effect.provide(services),
      ),
    );

    expect(Schema.is(DiagnosticResult)(result)).toBe(true);
    expect(phases).toEqual(["setup", "operation", "verification"]);
    expect(new Set(result.counters.map(({ name }) => name)).size).toBe(result.counters.length);
    expect(marks.length).toBeLessThanOrEqual(512);
    expect(marks.every((mark) => mark.elapsedMs >= 0)).toBe(true);
  },
  30_000,
);

it("rejects a changed fixture inventory before starting work", async () => {
  const workload = capabilityCases[0]!;

  const result = await Effect.runPromiseExit(
    runCapabilityCase({ ...workload, parameters: { prefix: 1_000_000 } }).pipe(
      Effect.provideService(DiagnosticProgress, silent),
      Effect.provide(services),
    ),
  );

  expect(Exit.isFailure(result)).toBe(true);
  expect(Exit.isFailure(result) ? Cause.pretty(result.cause) : "").toContain("Unknown or modified");
});

it("propagates a failed progress boundary without reporting a successful sample", async () => {
  const result = await Effect.runPromiseExit(
    runCapabilityCase(capabilityCases[0]!).pipe(
      Effect.provideService(DiagnosticProgress, {
        ...silent,
        phase: (phase) =>
          phase === "operation"
            ? Effect.fail(BenchmarkError.make({ message: "progress write failed" }))
            : Effect.void,
      }),
      Effect.provide(services),
    ),
  );

  expect(Exit.isFailure(result)).toBe(true);
  expect(Exit.isFailure(result) ? Cause.pretty(result.cause) : "").toContain(
    "progress write failed",
  );
});

it("allows interruption at the operation boundary and a subsequent fresh sample", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();

      const fiber = yield* runCapabilityCase(capabilityCases[0]!).pipe(
        Effect.provideService(DiagnosticProgress, {
          ...silent,
          phase: (phase) =>
            phase === "operation"
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.void,
        }),
        Effect.forkChild,
      );

      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      const interrupted = yield* Fiber.await(fiber);

      expect(Exit.isFailure(interrupted)).toBe(true);

      const next = yield* runCapabilityCase(capabilityCases[0]!).pipe(
        Effect.provideService(DiagnosticProgress, silent),
      );

      expect(next.counters.find(({ name }) => name === "committedMessages")?.value).toBe(256);
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

const ownedCases = [
  {
    name: "memory-run-recall",
    stop: "memory.readerIO.complete",
    finalized: "memory.reader.finalized",
    count: 1,
  },
  {
    name: "remembering-run-on",
    stop: "remember.foreground.complete",
    finalized: "remember.extract.finalized.previous",
    count: 1,
  },
  {
    name: "mcp-in-process-http-on",
    stop: "mcp.call.begin.0",
    finalized: "mcp.session.closed",
    count: 1,
  },
  {
    name: "subagent-run-on",
    stop: "subagent.queued.ready",
    finalized: "subagent.release.end.",
    count: 2,
  },
] as const;

it.each(ownedCases)(
  "closes acquired resources when $name is interrupted",
  async (sample) => {
    const marks: Array<DiagnosticMark> = [];

    await Effect.runPromise(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const workload = capabilityCases.find(({ name }) => name === sample.name)!;

        const fiber = yield* runCapabilityCase(workload).pipe(
          Effect.provideService(DiagnosticProgress, {
            phase: () => Effect.void,
            mark: (mark) =>
              Effect.gen(function* () {
                marks.push(mark);
                if (mark.name === sample.stop) {
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

        expect(Exit.isFailure(exit)).toBe(true);
      }).pipe(Effect.scoped, Effect.provide(services)),
    );
    expect(marks.filter((mark) => mark.name.startsWith(sample.finalized))).toHaveLength(
      sample.count,
    );
  },
  10_000,
);

it.each(
  ownedCases.flatMap((sample) =>
    ["failure", "defect", "timeout"].map((mode) => ({ ...sample, mode })),
  ),
)(
  "closes acquired resources on $mode in $name",
  async (sample) => {
    const marks: Array<DiagnosticMark> = [];
    const workload = capabilityCases.find(({ name }) => name === sample.name)!;

    const exit = await Effect.runPromiseExit(
      runCapabilityCase(workload).pipe(
        Effect.provideService(DiagnosticProgress, {
          phase: () => Effect.void,
          mark: (mark) =>
            Effect.gen(function* () {
              marks.push(mark);
              if (mark.name !== sample.stop) return;
              if (sample.mode === "defect") return yield* Effect.die("diagnostic injected defect");
              if (sample.mode === "timeout")
                return yield* Effect.never.pipe(
                  Effect.timeout("10 millis"),
                  Effect.mapError(() =>
                    BenchmarkError.make({ message: "diagnostic injected timeout" }),
                  ),
                );

              return yield* BenchmarkError.make({ message: "diagnostic injected failure" });
            }),
        }),
        Effect.provide(services),
        Effect.timeout("5 seconds"),
      ),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    expect(marks.some((mark) => mark.name === sample.stop)).toBe(true);
    expect(marks.filter((mark) => mark.name.startsWith(sample.finalized))).toHaveLength(
      sample.count,
    );
  },
  10_000,
);

it.each(["mcp-malformed-discovery", "mcp-call-failure"] as const)(
  "fails closed and ends the real HTTP session for %s",
  async (fault) => {
    const marks: Array<DiagnosticMark> = [];
    const workload = capabilityCases.find(({ name }) => name === "mcp-in-process-http-on")!;

    const exit = await Effect.runPromiseExit(
      runCapabilityCase(workload).pipe(
        Effect.provideService(DiagnosticFault, fault),
        Effect.provideService(DiagnosticProgress, {
          phase: () => Effect.void,
          mark: (mark) =>
            Effect.sync(() => {
              marks.push(mark);
            }),
        }),
        Effect.provide(services),
        Effect.timeout("5 seconds"),
      ),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    expect(marks.filter(({ name }) => name === "mcp.session.closed")).toHaveLength(1);
  },
);
