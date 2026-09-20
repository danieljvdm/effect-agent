import { expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import type { AiError } from "effect/unstable/ai";
import { DecisionModel, LanguageModel } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

import type { CheckoutError } from "../src/checkout-contract.ts";
import { ControllerInput, type Operation } from "../src/checkout-indexed-contract.ts";
import { chooseIndexed, decisionDefinition } from "../src/checkout-indexed.ts";

const input = ControllerInput.make({
  goal: "Buy the requested item",
  approvalGranted: false,
  observation: { url: "https://shop.test", frames: [], controls: [] },
  history: [],
});

const targets = new Map<Operation, Array<{ label: string; controls: []; value: string }>>([
  ["CLICK", [{ label: "Continue", controls: [], value: "" }]],
  [
    "SELECT",
    [
      { label: "Small", controls: [], value: "S" },
      { label: "Medium", controls: [], value: "M" },
    ],
  ],
]);

it("omits empty and singleton target classifiers without preselecting their operation", () => {
  const definition = decisionDefinition(targets);

  expect(Object.keys(definition.decisions)).toEqual(["operation", "SELECT"]);
  expect(Object.keys(definition.decisions.operation!.criteria)).not.toContain("TYPE");
  expect(Object.keys(decisionDefinition(new Map()).decisions)).toEqual(["operation"]);
});

it.effect(
  "uses one native request and dispatches only the selected head, resolving a singleton afterward",
  () =>
    Effect.gen(function* () {
      let calls = 0;

      const native = yield* DecisionModel.make({
        decide: (request) => {
          calls++;
          expect(Object.keys(request.decisions)).toEqual(["operation", "SELECT"]);

          return Effect.succeed({
            answers: Object.fromEntries(
              Object.entries(request.decisions).map(([name, decision]) => {
                if (decision._tag !== "Classify") throw new Error("Unexpected decision kind");
                const label = name === "operation" ? "CLICK" : "1";

                return [
                  name,
                  {
                    _tag: "Classify" as const,
                    label,
                    probabilities: Object.fromEntries(
                      Object.keys(decision.criteria).map((key) => [key, key === label ? 1 : 0]),
                    ),
                  },
                ];
              }),
            ),
            usage: { inputTokens: 10, outputTokens: 2 },
          });
        },
      });

      const selected = yield* chooseIndexed(input, targets, "jev").pipe(
        Effect.provideService(DecisionModel.DecisionModel, native),
        Effect.provide(
          Layer.succeed(
            LanguageModel.LanguageModel,
            yield* LanguageModel.make({
              generateText: () => Effect.die("Unexpected text model"),
              streamText: () => {
                throw new Error("Unexpected stream");
              },
            }),
          ),
        ),
      );

      expect(calls).toBe(1);
      expect(selected.operation).toBe("CLICK");
      expect(selected.target?.label).toBe("Continue");
      expect(selected.tokens).toBe(12);
    }),
);

it.effect("retains native invalid-output errors instead of renormalizing probabilities", () =>
  Effect.gen(function* () {
    const native = yield* DecisionModel.make({
      decide: () =>
        Effect.succeed({
          answers: {
            operation: {
              _tag: "Classify",
              label: "DONE",
              probabilities: { WAIT: 0, APPROVAL: 0, HUMAN: 0, DONE: 0.8, BLOCKED: 0 },
            },
          },
          usage: { inputTokens: undefined, outputTokens: undefined },
        }),
    });

    const result = yield* DecisionModel.decide(decisionDefinition(new Map()), { input }).pipe(
      Effect.provideService(DecisionModel.DecisionModel, native),
      Effect.flip,
    );

    expect(result._tag).toBe("AiError");
    expect(result.reason._tag).toBe("InvalidOutputError");
  }),
);

it("retains native model requirements and errors", () => {
  expectTypeOf<Effect.Error<ReturnType<typeof chooseIndexed>>>().toEqualTypeOf<
    AiError.AiError | CheckoutError
  >();
  expectTypeOf<Effect.Services<ReturnType<typeof chooseIndexed>>>().toEqualTypeOf<
    DecisionModel.DecisionModel | LanguageModel.LanguageModel
  >();
});
