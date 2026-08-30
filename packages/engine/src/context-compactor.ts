import type { CompactionPolicy } from "@effect-agent/core";
import { Context, Effect, Layer, Schema, Stream } from "effect";
import { type LanguageModel, type Model, Prompt } from "effect/unstable/ai";

import {
  buildCompactedView,
  choosePruneBound,
  chooseSummarizeCut,
  collectCoveredMessages,
  COMPACTION_INSTRUCTION,
  estimatePromptTokens,
  renderForSummary,
  type ContextCompactionState,
} from "./compaction.ts";

/** A proposed view change. Source indices are exclusive prefix bounds, never record sequences. */
export const CompactionDecision = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("clear-tool-results"), through: Schema.Natural }),
  Schema.Struct({
    kind: Schema.Literal("summarize"),
    through: Schema.Natural,
    summary: Schema.NonEmptyString.check(
      Schema.isMaxLength(8 * 1024 * 1024),
      Schema.isPattern(/\S/),
    ),
  }),
]);
export type CompactionDecision = typeof CompactionDecision.Type;

/** Expected strategy or decision-validation failure. Causes stay in the live Effect only. */
export class CompactionError extends Schema.TaggedError<CompactionError>()("CompactionError", {
  message: Schema.String.check(Schema.isMaxLength(4_096)),
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** Upstream Model.captureRequirements returns this closed Layer. */
export type CompactionModelLayer = Layer.Layer<
  LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName
>;

/**
 * One bounded pass over an immutable source snapshot. A harness owns state, metering, and
 * application of decisions. The interpreter permits at most one prune, one summary decision,
 * and one call to summarize per Turn, shared across pressure and overflow passes.
 * All model work must use summarize so it is metered.
 * Callback failures and requirements pass through unchanged; strategy dependencies belong to
 * its construction Layer. Protected messages and Tool pairs cannot be removed by a decision.
 */
export interface CompactionRequest<E, R> {
  readonly source: Prompt.Prompt;
  readonly state: Readonly<ContextCompactionState>;
  readonly policy: CompactionPolicy;
  readonly targetTokens: number | undefined;
  readonly forceSummarize: boolean;
  readonly allowSummarize: boolean;
  readonly summarize: (
    prompt: Prompt.Prompt,
    model?: CompactionModelLayer,
  ) => Effect.Effect<string, E, R>;
}

export interface ContextCompaction {
  /** Non-negative finite integer estimate, used for admission and derived prompt overhead too. */
  readonly estimate: (messages: ReadonlyArray<Prompt.Message>) => number;
  /** Emit decisions sequentially; the consumer commits each before pulling the next. */
  readonly compact: <E, R>(
    request: CompactionRequest<E, R>,
  ) => Stream.Stream<CompactionDecision, E | CompactionError, R>;
}

const defaultCompactor = (model?: CompactionModelLayer): ContextCompaction => ({
  estimate: estimatePromptTokens,
  compact: <E, R>(request: CompactionRequest<E, R>) =>
    Stream.suspend(() => {
      const { source, policy, targetTokens, forceSummarize, allowSummarize } = request;
      const state = { ...request.state };
      const keepRecentTokens =
        targetTokens === undefined
          ? policy.keepRecentTokens
          : Math.max(1, Math.min(policy.keepRecentTokens, targetTokens));
      const decisions: Array<CompactionDecision> = [];
      if (!forceSummarize && policy.mode !== "summarize") {
        const through = choosePruneBound(source.content, state, keepRecentTokens);
        if (through > state.clearedThrough) {
          state.clearedThrough = through;
          decisions.push({ kind: "clear-tool-results", through });
          if (
            targetTokens !== undefined &&
            estimatePromptTokens(buildCompactedView(source.content, state)) <= targetTokens
          ) {
            return Stream.fromIterable(decisions);
          }
        }
      }
      const prune = Stream.fromIterable(decisions);
      if ((!allowSummarize || policy.mode === "prune") && !forceSummarize) return prune;
      return prune.pipe(
        Stream.concat(
          Stream.unwrap(
            Effect.gen(function* () {
              const through = chooseSummarizeCut(source.content, state, keepRecentTokens);
              const covered = collectCoveredMessages(source.content, state, through);
              if (covered.length === 0) return Stream.empty;
              const transcript = renderForSummary(covered, state.summary);
              const prompt = Prompt.fromMessages([
                Prompt.userMessage({
                  content: [
                    Prompt.textPart({
                      text: `${COMPACTION_INSTRUCTION}\n\n<transcript>\n${transcript}\n</transcript>`,
                    }),
                  ],
                }),
              ]);
              const text = yield* request.summarize(prompt, model);
              return Stream.succeed({
                kind: "summarize",
                through,
                summary: text.trim(),
              } satisfies CompactionDecision);
            }),
          ),
        ),
      );
    }),
});

/**
 * The sole interpreter compaction port. Runs without an installed service provide layer at their
 * composition boundary. Decorators yield this service during construction and receive their
 * underlying implementation through Layer.provide. Direct harnesses use the same Layer contract.
 */
export class ContextCompactor extends Context.Service<ContextCompactor, ContextCompaction>()(
  "@effect-agent/engine/ContextCompactor",
) {
  static readonly layer = Layer.succeed(ContextCompactor, defaultCompactor());

  /** Use the bounded default algorithm with a separate upstream Effect AI Model. */
  static readonly layerWithModel = <Provider, Requirements>(
    model: Model.Model<Provider, LanguageModel.LanguageModel, Requirements>,
  ): Layer.Layer<ContextCompactor, never, Requirements> =>
    Layer.effect(
      ContextCompactor,
      Effect.map(model.captureRequirements, (captured) => defaultCompactor(captured)),
    );
}
