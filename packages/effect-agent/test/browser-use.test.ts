import { assert, expectTypeOf, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { AiError, DecisionModel } from "effect/unstable/ai";

import {
  type Observation,
  type BrowserUseError,
  selectTargets,
} from "../src/capabilities/BrowserUse.ts";

expectTypeOf<Effect.Error<ReturnType<typeof selectTargets>>>().toEqualTypeOf<BrowserUseError>();
expectTypeOf<
  Effect.Services<ReturnType<typeof selectTargets>>
>().toEqualTypeOf<DecisionModel.DecisionModel>();

const observation: typeof Observation.Type = {
  text: "New task: Title, Save, Cancel",
  controls: [
    { ref: "title", kind: "input", name: "Title", value: "", options: [] },
    { ref: "save", kind: "button", name: "Save", value: "", options: [] },
    { ref: "cancel", kind: "button", name: "Cancel", value: "", options: [] },
  ],
};

it.effect(
  "resolves a whole batch to action-compatible observed controls using one native decision",
  () =>
    Effect.gen(function* () {
      let calls = 0;

      const model = Layer.effect(
        DecisionModel.DecisionModel,
        DecisionModel.make({
          decide: (request) =>
            Effect.sync(() => {
              calls++;
              assert.deepStrictEqual(Object.keys(request.decisions), ["element_0", "element_1"]);
              const first = request.decisions.element_0;
              const second = request.decisions.element_1;

              assert.strictEqual(first?._tag, "Classify");
              if (first?._tag === "Classify")
                assert.deepStrictEqual(Object.keys(first.criteria), ["__none__", "title"]);
              if (second?._tag === "Classify")
                assert.deepStrictEqual(Object.keys(second.criteria), [
                  "__none__",
                  "save",
                  "cancel",
                ]);

              return {
                answers: {
                  element_0: {
                    _tag: "Classify",
                    label: "title",
                    probabilities: { title: 0.95, __none__: 0.05 },
                  },
                  element_1: {
                    _tag: "Classify",
                    label: "save",
                    probabilities: { save: 0.8, cancel: 0.1, __none__: 0.1 },
                  },
                },
                usage: { inputTokens: 80, outputTokens: 4 },
              };
            }),
        }),
      );

      const selected = yield* selectTargets(observation, [
        { kind: "fill", target: "Task title field", value: "Ship demo" },
        { kind: "click", target: "Save this task" },
      ]).pipe(Effect.provide(model));

      assert.strictEqual(calls, 1);
      assert.deepStrictEqual(selected.actions, [
        { kind: "fill", ref: "title", value: "Ship demo" },
        { kind: "click", ref: "save" },
      ]);
      assert.deepStrictEqual(
        selected.choices.map((choice) => choice.probability),
        [0.95, 0.8],
      );
      assert.strictEqual(selected.usage.inputTokens, 80);
      assert.strictEqual(selected.usage.outputTokens, 4);
    }),
);

it.effect(
  "rejects abstention, ambiguous matches, invented controls, invalid distributions, and incompatible actions",
  () =>
    Effect.gen(function* () {
      for (const [label, probabilities] of [
        ["__none__", { save: 0.05, cancel: 0.05, __none__: 0.9 }],
        ["save", { save: 0.55, cancel: 0.35, __none__: 0.1 }],
        ["invisible", { save: 0.8, cancel: 0.1, __none__: 0.1 }],
        ["save", { save: 0.8, cancel: 0.1, __none__: 0.09 }],
      ] as const) {
        const model = Layer.effect(
          DecisionModel.DecisionModel,
          DecisionModel.make({
            decide: () =>
              Effect.succeed({
                answers: { element_0: { _tag: "Classify", label, probabilities } },
                usage: { inputTokens: undefined, outputTokens: undefined },
              }),
          }),
        );

        const result = yield* selectTargets(observation, [{ kind: "click", target: "Save" }]).pipe(
          Effect.provide(model),
          Effect.result,
        );

        assert.strictEqual(result._tag, "Failure", label);
      }

      const unavailable = yield* selectTargets(observation, [
        { kind: "select", target: "Priority", value: "High" },
      ]).pipe(
        Effect.provide(
          Layer.effect(
            DecisionModel.DecisionModel,
            DecisionModel.make({ decide: () => Effect.die("Must reject before provider I/O") }),
          ),
        ),
        Effect.result,
      );

      assert.strictEqual(unavailable._tag, "Failure");
    }),
);

it.effect(
  "bounds decision I/O and preserves failure, defect and interruption without returning dispatchable actions",
  () =>
    Effect.gen(function* () {
      for (const outcome of ["failure", "defect", "timeout", "interruption"] as const) {
        const started = yield* Deferred.make<void>();
        let finalized = false;

        const pending = Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Effect.sync(() => {
              finalized = true;
            }),
          ),
        );

        const model = Layer.effect(
          DecisionModel.DecisionModel,
          DecisionModel.make({
            decide: () =>
              outcome === "failure"
                ? Effect.fail(
                    new AiError.AiError({
                      module: "test",
                      method: "decide",
                      reason: new AiError.InvalidOutputError({ description: "Invalid decision" }),
                    }),
                  )
                : outcome === "defect"
                  ? Effect.die("provider defect")
                  : pending,
          }),
        );

        const fiber = yield* Effect.forkChild(
          selectTargets(observation, [{ kind: "click", target: "Save" }]).pipe(
            Effect.provide(model),
          ),
        );

        if (outcome === "timeout" || outcome === "interruption") {
          yield* Deferred.await(started);
          if (outcome === "timeout") yield* TestClock.adjust("15 seconds");
          else yield* Fiber.interrupt(fiber);
        }
        const exit = yield* Fiber.await(fiber);

        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          assert.strictEqual(Cause.hasInterrupts(exit.cause), outcome === "interruption");
          assert.strictEqual(Cause.hasDies(exit.cause), outcome === "defect");
        }
        if (outcome === "timeout" || outcome === "interruption") assert.isTrue(finalized);
      }
    }),
);
