import { assert, expectTypeOf, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Response, Tool, Toolkit } from "effect/unstable/ai";

import { CheckoutSpan } from "../src/checkout-contract.ts";
import {
  CheckoutTelemetry,
  CheckoutTelemetryStore,
  instrumentModels,
  makeTelemetry,
  measured,
} from "../src/checkout-telemetry.ts";

expectTypeOf<Effect.Error<ReturnType<typeof makeTelemetry>>>().toEqualTypeOf<never>();
expectTypeOf<
  Effect.Services<ReturnType<typeof makeTelemetry>>
>().toEqualTypeOf<CheckoutTelemetryStore>();

type MeasuredModel = ReturnType<
  typeof instrumentModels<string, "expected", LanguageModel.LanguageModel>
>;
type MeasuredOperation = ReturnType<
  typeof measured<string, "expected", LanguageModel.LanguageModel>
>;

expectTypeOf<Effect.Error<MeasuredModel>>().toEqualTypeOf<"expected">();
expectTypeOf<Effect.Services<MeasuredModel>>().toEqualTypeOf<LanguageModel.LanguageModel>();
expectTypeOf<Effect.Error<MeasuredOperation>>().toEqualTypeOf<"expected">();
expectTypeOf<Effect.Services<MeasuredOperation>>().toEqualTypeOf<LanguageModel.LanguageModel>();

it.effect(
  "retains running spans and monotonic duration on success, failure, defect, timeout and interruption",
  () =>
    Effect.gen(function* () {
      for (const outcome of ["success", "failure", "defect", "timeout", "interruption"] as const) {
        const recorded: Array<typeof CheckoutSpan.Type> = [];

        const telemetry = yield* makeTelemetry().pipe(
          Effect.provideService(CheckoutTelemetryStore, {
            allocateRequestUnsafe: () => 3,
            recordUnsafe: (span) => recorded.push(CheckoutSpan.make(span)),
          }),
        );

        const entered = yield* Deferred.make<void>();

        const work = Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          yield* Effect.sleep("1 second");
          if (outcome === "failure") return yield* Effect.fail("expected");
          if (outcome === "defect") return yield* Effect.die("defect");
          if (outcome === "timeout" || outcome === "interruption") return yield* Effect.never;

          return "result";
        });

        const fiber = yield* measured("browser", "click", work).pipe(
          Effect.provideService(CheckoutTelemetry, telemetry),
          outcome === "timeout" ? Effect.timeout("2 seconds") : (effect) => effect,
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        assert.strictEqual(recorded.length, 1);
        assert.strictEqual(recorded[0]?.outcome, "running");
        yield* TestClock.adjust("2 seconds");
        if (outcome === "interruption") yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        assert.strictEqual(Exit.isSuccess(exit), outcome === "success");
        assert.strictEqual(recorded.length, 2);
        assert.strictEqual(recorded[1]?.id, recorded[0]?.id);
        assert.strictEqual(
          recorded[1]?.outcome,
          outcome === "success"
            ? "completed"
            : outcome === "timeout" || outcome === "interruption"
              ? "interrupted"
              : outcome,
        );
        assert.strictEqual(
          recorded[1]?.elapsedMillis,
          outcome === "timeout" || outcome === "interruption" ? 2_000 : 1_000,
        );
      }
    }),
);

it.effect(
  "correlates observation-only native model turns, tokens and browser work without changing responses",
  () =>
    Effect.gen(function* () {
      const recorded: Array<typeof CheckoutSpan.Type> = [];

      const telemetry = yield* makeTelemetry().pipe(
        Effect.provideService(CheckoutTelemetryStore, {
          allocateRequestUnsafe: () => 7,
          recordUnsafe: (span) => recorded.push(span),
        }),
      );

      const toolkit = Toolkit.make(
        Tool.make("observe", { parameters: Tool.EmptyParams, success: Schema.String }),
      );

      const model = yield* LanguageModel.make({
        generateText: () => Effect.die("The buyer uses streaming"),
        streamText: () =>
          Stream.fromEffect(Effect.sleep("2 seconds")).pipe(
            Stream.flatMap(() =>
              Stream.fromArray([
                {
                  type: "response-metadata" as const,
                  id: "reply",
                  modelId: "resolved-version",
                  timestamp: undefined,
                  request: undefined,
                },
                Response.makePart("tool-call", {
                  id: "call",
                  name: "observe",
                  params: {},
                  providerExecuted: false,
                }),
                Response.makePart("finish", {
                  reason: "tool-calls",
                  usage: new Response.Usage({
                    inputTokens: {
                      total: 10,
                      uncached: 10,
                      cacheRead: undefined,
                      cacheWrite: undefined,
                    },
                    outputTokens: { total: 3, text: 3, reasoning: undefined },
                  }),
                  response: undefined,
                }),
              ]),
            ),
          ),
      });

      const program = instrumentModels(
        Effect.gen(function* () {
          yield* LanguageModel.streamText({
            prompt: "Observe",
            toolkit,
            disableToolCallResolution: true,
          }).pipe(Stream.runDrain);
          yield* measured("observation", "observe", Effect.sleep("1 second"));
        }),
        "requested-version",
      ).pipe(
        Effect.provideService(LanguageModel.LanguageModel, model),
        Effect.provideService(CheckoutTelemetry, telemetry),
        Effect.provide(toolkit.toLayer({ observe: () => Effect.succeed("unused") })),
      );

      const fiber = yield* program.pipe(Effect.forkChild);

      yield* TestClock.adjust("3 seconds");
      yield* Fiber.join(fiber);
      const completed = recorded.filter((span) => span.outcome === "completed");

      assert.strictEqual(completed.length, 2);
      assert.deepStrictEqual(
        completed.map((span) => [span.phase, span.request, span.turn, span.elapsedMillis]),
        [
          ["model", 7, 1, 2_000],
          ["observation", 7, 1, 1_000],
        ],
      );
      assert.deepStrictEqual(completed[0]?.tools, ["observe"]);
      assert.strictEqual(completed[0]?.inputTokens, 10);
      assert.strictEqual(completed[0]?.outputTokens, 3);
      assert.strictEqual(completed[0]?.resolvedModel, "resolved-version");
    }),
);

it.effect("keeps overlapping requests distinct when both arrive before their first span", () =>
  Effect.gen(function* () {
    const persisted = new Map<string, typeof CheckoutSpan.Type>();

    const store = CheckoutTelemetryStore.of({
      allocateRequestUnsafe: () => persisted.size + 1,
      recordUnsafe: (span) => {
        persisted.set(span.id, span);
      },
    });

    const first = yield* makeTelemetry().pipe(Effect.provideService(CheckoutTelemetryStore, store));

    const second = yield* makeTelemetry().pipe(
      Effect.provideService(CheckoutTelemetryStore, store),
    );

    const finishFirst = first.begin("resume", "agent.run");
    const finishSecond = second.begin("approval", "approve");

    finishSecond(Exit.void);
    const finishBrowser = first.begin("browser", "click");

    finishBrowser(Exit.void);
    finishFirst(Exit.void);
    const spans = [...persisted.values()];

    assert.strictEqual(spans.length, 3);
    assert.notStrictEqual(spans[0]?.request, spans[1]?.request);
    assert.strictEqual(spans[0]?.request, spans[2]?.request);
    assert.deepStrictEqual(
      spans.map((span) => span.outcome),
      ["completed", "completed", "completed"],
    );
  }),
);
