import { DecisionModel, DecisionQuery, DecisionSchema } from "@effect-agent/ai-decision";
import { Effect, Schema } from "effect";
import { AiError } from "effect/unstable/ai";

import { utf8ByteLength } from "../core/internal/utf8.ts";
import type { Hook, Request } from "../engine/ToolSelector.ts";
import { readDecisionConfig } from "./DecisionToolSelectorConfig.ts";

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

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(DecisionSchema.Content))(
        state,
      ).pipe(Effect.mapError(() => invalidState("Invalid Tool selector state")));

      if (utf8ByteLength(encoded) > bounds.maxStateBytes)
        return yield* invalidState("Tool selector state exceeds its byte bound");
      const model = yield* DecisionModel.DecisionModel;

      const questions: Record<string, DecisionSchema.ProbabilityQuestion> = Object.fromEntries(
        request.catalogue.map((candidate, index) => [
          `candidate_${index}`,
          DecisionQuery.probability({
            instructions: {
              question: bounds.prompt,
              tool: {
                name: candidate.name,
                description: candidate.description ?? "",
                namespace: candidate.namespace ?? "",
                method: candidate.method ?? "",
              },
            },
            ...(bounds.criteria === undefined ? {} : { criteria: bounds.criteria }),
          }),
        ]),
      );

      const result = yield* model.evaluate({ state, questions });

      if (options.onEvaluation !== undefined)
        yield* options.onEvaluation({
          provider: result.provider,
          model: result.model,
          usage: result.usage,
        });
      const ranked: Array<{ id: string; relevance: number }> = [];

      for (const [index, candidate] of request.catalogue.entries()) {
        const answer = result.answers[`candidate_${index}`];

        if (answer === undefined)
          return yield* new AiError.AiError({
            module: "ToolSelector",
            method: "select",
            reason: new AiError.InvalidOutputError({ description: "Missing candidate relevance" }),
          });
        if (answer.probability >= bounds.minimumRelevance)
          ranked.push({ id: candidate.id, relevance: answer.probability });
      }
      if (ranked.length === 0 && bounds.onNoMatch === "keep") return undefined;

      return ranked
        .toSorted((a, b) => b.relevance - a.relevance || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
        .map((candidate) => candidate.id);
    }),
  };
});

const invalidState = (description: string) =>
  new AiError.AiError({
    module: "ToolSelector",
    method: "select",
    reason: new AiError.InvalidRequestError({ description }),
  });
