import {
  BrowserSessionError,
  type BrowserSession,
} from "@effect-agent/platform-cloudflare/browser-session";
import { expect, it } from "@effect/vitest";
import { Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import type { AiError } from "effect/unstable/ai";
import { DecisionModel, LanguageModel } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

import { CheckoutOwner } from "../src/checkout-agent.ts";
import type { CheckoutError } from "../src/checkout-contract.ts";
import { observeIndexed } from "../src/checkout-indexed-browser.ts";
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
        Effect.provideService(
          LanguageModel.LanguageModel,
          yield* LanguageModel.make({
            generateText: () => Effect.die("Unexpected text model"),
            streamText: () => {
              throw new Error("Unexpected stream");
            },
          }),
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

it.effect("bounds read-only recovery without replaying input or swallowing interruption", () =>
  Effect.gen(function* () {
    let reads = 0;
    let writes = 0;

    const session: Pick<BrowserSession, "run"> = {
      run: (authorize) =>
        authorize.pipe(
          Effect.andThen(
            Effect.suspend(() => {
              reads++;

              return BrowserSessionError.make({
                reason: "provider",
                dispatch: "possibly-dispatched",
                cleanup: "not-requested",
              });
            }),
          ),
        ),
    };

    const owner = CheckoutOwner.of({
      authorize: Effect.void,
      observe: () => Effect.void,
      observeIndexed: () => Effect.void,
      record: () =>
        Effect.sync(() => {
          writes++;
        }),
      approval: Effect.die("unused"),
      human: Effect.die("unused"),
    });

    const fiber = yield* observeIndexed(session, ["https://shop.test"]).pipe(
      Effect.provideService(CheckoutOwner, owner),
      Effect.result,
      Effect.forkChild,
    );

    yield* TestClock.adjust("2 seconds");
    const result = yield* Fiber.join(fiber);

    expect(result._tag).toBe("Failure");
    expect(reads).toBe(3);
    expect(writes).toBe(0);
    reads = 0;

    const interrupted = yield* observeIndexed(session, ["https://shop.test"]).pipe(
      Effect.provideService(CheckoutOwner, owner),
      Effect.forkChild,
    );

    yield* Effect.yieldNow;
    yield* Fiber.interrupt(interrupted);
    expect(reads).toBe(1);
    expect(writes).toBe(0);
  }),
);
