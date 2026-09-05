import { type CompactionPolicy } from "@effect-agent/core/AgentPolicy";
import { type RunId, type ThreadId } from "@effect-agent/core/Identifiers";
import { Context, Effect, Layer, Schema, Stream } from "effect";
import { type LanguageModel, type Model, Prompt } from "effect/unstable/ai";

import { ContextHandoff, type ContextRolloverRequest } from "./ContextWindow.ts";
import {
  buildCompactedView,
  buildRolloverHandoff,
  choosePruneBound,
  chooseSummarizeCut,
  collectCoveredMessages,
  estimatePromptTokens,
  renderForSummary,
  SUMMARY_MAX_LENGTH,
  SUMMARY_REQUEST_PREFIX,
  SUMMARY_REQUEST_SUFFIX,
  type ContextCompactionState,
} from "./internal/compaction.ts";

/**
 * A proposed view change. Source indices are exclusive prefix bounds, never record sequences.
 * Summaries contain at most 65,536 characters; the interpreter rejects oversized decisions
 * before committing or changing coverage.
 */
export const CompactionDecision = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("rollover"),
    through: Schema.Natural,
    handoff: Schema.optionalKey(ContextHandoff),
  }),
  Schema.Struct({ kind: Schema.Literal("clear-tool-results"), through: Schema.Natural }),
  Schema.Struct({
    kind: Schema.Literal("summarize"),
    through: Schema.Natural,
    summary: Schema.NonEmptyString.check(
      Schema.isMaxLength(SUMMARY_MAX_LENGTH),
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
 * application of decisions. The interpreter permits at most one prune followed by one replacement
 * (summary or rollover), and one call to summarize per Turn, shared across every trigger.
 * All model work must use summarize so it is metered.
 * Callback failures and requirements pass through unchanged; strategy dependencies belong to
 * its construction Layer. Protected messages cannot be removed and Tool pairs cannot be split by a decision.
 */
export interface CompactionRequest<E, R> {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turn: number;
  readonly source: Prompt.Prompt;
  readonly state: Readonly<ContextCompactionState>;
  readonly policy: CompactionPolicy;
  readonly targetTokens: number | undefined;
  readonly trigger: "pressure" | "overflow" | "requested";
  /** Budget admission may prohibit a separate model call while still allowing a rollover. */
  readonly modelCallAllowed: boolean;
  /** Present for a successful, singleton context rollover Tool; through excludes later steering. */
  readonly requested?: (ContextRolloverRequest & { readonly through: number }) | undefined;
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
      const { source, policy, targetTokens, trigger, modelCallAllowed } = request;

      if (request.requested !== undefined) {
        return Stream.succeed({ kind: "rollover", ...request.requested });
      }
      const forceSummarize = trigger === "overflow";
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

      if ((!modelCallAllowed || policy.mode === "prune") && !forceSummarize) return prune;

      return prune.pipe(
        Stream.concat(
          Stream.unwrap(
            Effect.gen(function* () {
              const through = chooseSummarizeCut(source.content, state, keepRecentTokens);
              const covered = collectCoveredMessages(source.content, state, through);

              if (covered.length === 0) return Stream.empty;

              const transcript = renderForSummary(
                covered,
                state.replacement?.kind === "summarize"
                  ? state.replacement.summary
                  : state.replacement?.handoff,
              );

              if (transcript === undefined) {
                return yield* CompactionError.make({
                  message: `Previous compaction summary exceeds ${SUMMARY_MAX_LENGTH} characters`,
                });
              }

              const prompt = Prompt.fromMessages([
                Prompt.userMessage({
                  content: [
                    Prompt.textPart({
                      text: `${SUMMARY_REQUEST_PREFIX}${transcript}${SUMMARY_REQUEST_SUFFIX}`,
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

  /**
   * Start fresh windows without a summarizer call. The engine retains instructions/input and
   * commits each boundary; a bounded emergency handoff preserves user inputs and unseen results.
   * Applications provide notes/history tools alongside this strategy.
   */
  static readonly layerRollover = Layer.succeed(ContextCompactor, {
    estimate: estimatePromptTokens,
    compact: <E, R>(request: CompactionRequest<E, R>) =>
      Stream.suspend(() => {
        if (request.requested !== undefined) {
          return Stream.succeed({
            kind: "rollover",
            ...request.requested,
          } satisfies CompactionDecision);
        }

        // Trailing user steering may not be canonical until the next response. Keep it verbatim.
        const through =
          request.source.content.findLastIndex(
            (message) => message.role === "assistant" || message.role === "tool",
          ) + 1;

        if (collectCoveredMessages(request.source.content, request.state, through).length === 0) {
          return Stream.empty;
        }

        const handoff = buildRolloverHandoff(
          request.source.content,
          request.state,
          request.targetTokens,
          through,
        );

        if (handoff === undefined)
          return Stream.fail(
            CompactionError.make({
              message: "Insufficient context capacity for an automatic rollover handoff",
            }),
          );

        return Stream.succeed({ kind: "rollover", through, handoff } satisfies CompactionDecision);
      }),
  });

  /** Use the bounded default algorithm with a separate upstream Effect AI Model. */
  static readonly layerWithModel = <Provider, Requirements>(
    model: Model.Model<Provider, LanguageModel.LanguageModel, Requirements>,
  ): Layer.Layer<ContextCompactor, never, Requirements> =>
    Layer.effect(
      ContextCompactor,
      Effect.map(model.captureRequirements, (captured) => defaultCompactor(captured)),
    );
}
