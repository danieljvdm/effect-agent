import { Effect, Layer } from "effect";
import * as BrowserUse from "effect-agent/browser-use";
import { DecisionModel, Toolkit } from "effect/unstable/ai";

import { Browser, completionTools, completionLayer } from "./browser.ts";
import { LabError } from "./contract.ts";
import { Trace } from "./telemetry.ts";

export const groundedSingleTools = Toolkit.merge(completionTools, BrowserUse.groundedSingleTools);
export const groundedBatchTools = Toolkit.merge(completionTools, BrowserUse.groundedBatchTools);

/** Translate the shared selection failure into the lab's API error. */
export const selectTargets = (
  observation: typeof BrowserUse.Observation.Type,
  targets: ReadonlyArray<BrowserUse.TargetAction>,
) =>
  BrowserUse.selectTargets(observation, targets).pipe(
    Effect.mapError((error) => new LabError({ code: error.code, message: error.message })),
  );

/** The lab decorates library selection with its report spans; it owns no matching logic. */
export const makeGroundedLayers = Effect.fnUntraced(function* (
  initial: typeof BrowserUse.Observation.Type,
) {
  const browser = yield* Browser;
  const trace = yield* Trace;

  const selector = Layer.effect(
    BrowserUse.TargetSelector,
    Effect.gen(function* () {
      const model = yield* DecisionModel.DecisionModel;

      return BrowserUse.TargetSelector.of({
        select: (observation, targets) =>
          trace.measure(
            "decision",
            `Jev · select ${targets.length} control${targets.length === 1 ? "" : "s"}`,
            BrowserUse.selectTargets(observation, targets).pipe(
              Effect.provideService(DecisionModel.DecisionModel, model),
            ),
            ({ usage, choices }) => ({
              model: "jev-latest",
              choices,
              ...(usage.inputTokens === undefined ? {} : { inputTokens: usage.inputTokens }),
              ...(usage.outputTokens === undefined ? {} : { outputTokens: usage.outputTokens }),
            }),
          ),
      });
    }),
  );

  return {
    single: Layer.merge(
      BrowserUse.groundedSingleLayer(initial).pipe(Layer.provide([browser.actionsLayer, selector])),
      completionLayer,
    ),
    batched: Layer.merge(
      BrowserUse.groundedBatchLayer(initial).pipe(Layer.provide([browser.actionsLayer, selector])),
      completionLayer,
    ),
  };
});
