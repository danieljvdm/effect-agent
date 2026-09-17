import type { DecisionModel, DecisionSchema } from "@effect-agent/ai-decision";
import { Effect } from "effect";
import type { AiError } from "effect/unstable/ai";

import type { Hook, Request } from "../engine/ToolSelector.ts";
import { readDecisionConfig } from "./DecisionToolSelectorConfig.ts";
import { rankToolRelevance } from "./internal/tool-relevance.ts";

export { DecisionConfig, defaultDecisionConfig } from "./DecisionToolSelectorConfig.ts";

export interface DecisionOptions<
  StateError = never,
  StateRequirements = never,
  ObserverError = never,
  ObserverRequirements = never,
> {
  /** Explicitly project authorized model-visible state. Never serializes the entire prompt implicitly. */
  readonly state: (
    request: Request,
  ) => Effect.Effect<DecisionSchema.Content, StateError, StateRequirements>;
  /** Observe separately billed usage; failures remain typed and stop selection. Contains no state/answers. */
  readonly onEvaluation?:
    | ((
        result: Pick<DecisionSchema.EvaluateResponse, "provider" | "model" | "usage">,
      ) => Effect.Effect<void, ObserverError, ObserverRequirements>)
    | undefined;
}

/**
 * Read DecisionConfig and construct a selector using any DecisionModel. Provide configuration
 * to this construction Effect; the returned hook retains the validated settings for its lifetime.
 * One evaluation batches independent probability questions for all eligible candidates.
 * Choices are categorical; their normalized probabilities are not independent relevance scores.
 * Empty catalogues skip model I/O. Equal scores retain catalogue-ID order. The engine validates
 * all selected IDs, limits distinct native Tools, and retains its ordinary pins and authorization.
 */
export const fromDecisionModel = Effect.fnUntraced(function* <
  SE = never,
  SR = never,
  OE = never,
  OR = never,
>(
  options: DecisionOptions<SE, SR, OE, OR>,
): Effect.fn.Return<
  Hook<SE | OE | AiError.AiError, SR | OR | DecisionModel.DecisionModel>,
  AiError.AiError
> {
  const bounds = yield* readDecisionConfig;

  return {
    maxTools: bounds.maxTools,
    maxCandidates: bounds.maxCandidates,
    maxCatalogueBytes: bounds.maxCatalogueBytes,
    select: Effect.fn("ToolSelector.decisions")(function* (request) {
      if (request.catalogue.length === 0) return bounds.onNoMatch === "clear" ? [] : undefined;
      const state = yield* options.state(request);

      const result = yield* rankToolRelevance({
        state,
        prompt: bounds.prompt,
        criteria: bounds.criteria,
        catalogue: request.catalogue,
        minimumRelevance: bounds.minimumRelevance,
        maxStateBytes: bounds.maxStateBytes,
        module: "ToolSelector",
      });

      if (options.onEvaluation !== undefined) yield* options.onEvaluation(result.evaluation);
      if (result.ids.length === 0 && bounds.onNoMatch === "keep") return undefined;

      return result.ids;
    }),
  };
});
