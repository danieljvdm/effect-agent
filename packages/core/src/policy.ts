import { Duration, Schema } from "effect";

import { ToolResultBounds } from "./tool-result.ts";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

const CompactionMode = Schema.Literals(["prune", "summarize", "prune-then-summarize"]);

/** Inputs normalized and validated by `CompactionPolicy.make`. */
export type CompactionPolicyInput = Readonly<{
  /** Estimated tokens of recent history preserved verbatim across a compaction; defaults to `20_000`. */
  readonly keepRecentTokens?: number;
  /** Compaction strategy order; defaults to `"prune-then-summarize"`. */
  readonly mode?: typeof CompactionMode.Type;
}>;

/** How the engine reduces model context when `contextTokenLimit` is crossed. */
export class CompactionPolicy extends Schema.Class<CompactionPolicy>("CompactionPolicy")({
  keepRecentTokens: PositiveInt,
  mode: CompactionMode,
}) {
  /** Normalize and validate compaction bounds, filling documented defaults. */
  static override make(input: CompactionPolicyInput = {}): CompactionPolicy {
    return super.make({
      keepRecentTokens: input.keepRecentTokens ?? 20_000,
      mode: input.mode ?? "prune-then-summarize",
    });
  }
}
const FinitePositiveDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && Duration.isPositive(duration),
    { expected: "a finite positive duration" },
  ),
);

/**
 * Resolution when `maxTurns`, `maxToolCalls`, or `tokenBudget` is exhausted:
 * `"final-answer"` gives the model one constrained opportunity to settle with
 * output (the Run completes with `finishReason: "budget-exhausted"` and the
 * exhausted-dimension marker), while `"fail"` fails the Run typed. Turn and
 * Tool Call bounds resolve per ADR-0019; the token dimension participates per
 * ADR-0018's dated extension with a one-shot bound (at most one grace call).
 * Duration, cost, and repeated-failure bounds are hard rails regardless of
 * this setting.
 */
const OnExhaustion = Schema.Literals(["final-answer", "fail"]);

const AgentPolicyFields = Schema.Struct({
  maxTurns: PositiveInt,
  maxToolCalls: PositiveInt,
  maxDuration: FinitePositiveDuration,
  toolConcurrency: PositiveInt,
  repeatedFailureLimit: NonNegativeInt,
  onExhaustion: OnExhaustion,
  tokenBudget: Schema.optionalKey(PositiveInt),
  costBudgetMicrousd: Schema.optionalKey(NonNegativeInt),
  /** Per-model-call live-context bound; crossing it triggers compaction rather than failure. */
  contextTokenLimit: Schema.optionalKey(PositiveInt),
  toolResultBounds: ToolResultBounds,
  runStatus: Schema.Literals(["appended", "off"]),
  compaction: CompactionPolicy,
});
type AgentPolicyFields = typeof AgentPolicyFields.Type;

/** Inputs normalized and validated by `AgentPolicy.make`. */
export type AgentPolicyInput = Readonly<
  Omit<
    AgentPolicyFields,
    | "maxDuration"
    | "repeatedFailureLimit"
    | "onExhaustion"
    | "toolResultBounds"
    | "runStatus"
    | "compaction"
  > & {
    /** Finite, positive wall-clock duration accepted in any Effect Duration input form. */
    readonly maxDuration: Duration.Input;
    /** Non-negative repeated-failure bound; defaults to `3`. */
    readonly repeatedFailureLimit?: number;
    /** Turn/Tool-Call/token exhaustion resolution; defaults to `"final-answer"`. */
    readonly onExhaustion?: typeof OnExhaustion.Type;
    /** Byte bound for each encoded Tool result; defaults to `50 KiB`. */
    readonly toolResultBounds?: ToolResultBounds;
    /** Whether a derived run-status message is appended to each model call; defaults to `"appended"`. */
    readonly runStatus?: AgentPolicyFields["runStatus"];
    /** Compaction strategy applied when `contextTokenLimit` is crossed; defaults documented on `CompactionPolicy`. */
    readonly compaction?: CompactionPolicy;
  }
>;

/** Schema class for finite agent run limits. */
export class AgentPolicy extends Schema.Class<AgentPolicy>("AgentPolicy")(AgentPolicyFields) {
  /** Normalize and validate finite policy bounds, throwing on invalid input. */
  static override make(input: AgentPolicyInput): AgentPolicy {
    return super.make({
      ...input,
      maxDuration: Duration.fromInputUnsafe(input.maxDuration),
      repeatedFailureLimit: input.repeatedFailureLimit ?? 3,
      onExhaustion: input.onExhaustion ?? "final-answer",
      toolResultBounds: input.toolResultBounds ?? ToolResultBounds.make({ maxBytes: 50 * 1024 }),
      runStatus: input.runStatus ?? "appended",
      compaction: input.compaction ?? CompactionPolicy.make(),
    });
  }
}
