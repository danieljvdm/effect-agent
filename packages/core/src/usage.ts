import { Effect, Schema } from "effect";

const UsageIdentity = Schema.NonEmptyString.check(Schema.isMaxLength(256));

const hasAdditiveTotal = (total: number, components: ReadonlyArray<number>): boolean => {
  const sum = components.reduce((accumulator, component) => accumulator + component, 0);
  return Number.isSafeInteger(sum) && total === sum;
};

const InputTokenUsageFields = Schema.Struct({
  total: Schema.Natural,
  uncached: Schema.Natural,
  cacheRead: Schema.Natural,
  cacheWrite: Schema.Natural,
}).check(
  Schema.makeFilter(
    (usage) => hasAdditiveTotal(usage.total, [usage.uncached, usage.cacheRead, usage.cacheWrite]),
    { title: "Input token total equals uncached, cache-read, and cache-write components" },
  ),
);

/** Provider-reported input-token components for one completed model call. */
export class InputTokenUsage extends Schema.Class<InputTokenUsage>(
  "@effect-agent/core/InputTokenUsage",
)(InputTokenUsageFields) {}

const OutputTokenUsageFields = Schema.Struct({
  total: Schema.Natural,
  text: Schema.Natural,
  reasoning: Schema.Natural,
}).check(
  Schema.makeFilter((usage) => hasAdditiveTotal(usage.total, [usage.text, usage.reasoning]), {
    title: "Output token total equals text and reasoning components",
  }),
);

/** Provider-reported output-token components for one completed model call. */
export class OutputTokenUsage extends Schema.Class<OutputTokenUsage>(
  "@effect-agent/core/OutputTokenUsage",
)(OutputTokenUsageFields) {}

/** Canonical, provider-independent accounting for one completed model call. */
export class ModelCallUsage extends Schema.Class<ModelCallUsage>(
  "@effect-agent/core/ModelCallUsage",
)({
  provider: UsageIdentity,
  model: UsageIdentity,
  serviceTier: Schema.optionalKey(UsageIdentity),
  pricingVersion: Schema.optionalKey(UsageIdentity),
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  costMicrousd: Schema.Natural,
}) {}

/** Settlement-sized aggregate for calls sharing one pricing identity. */
export class ModelUsageGroup extends Schema.Class<ModelUsageGroup>(
  "@effect-agent/core/ModelUsageGroup",
)({
  provider: UsageIdentity,
  model: UsageIdentity,
  serviceTier: Schema.optionalKey(UsageIdentity),
  pricingVersion: Schema.optionalKey(UsageIdentity),
  modelCalls: Schema.Natural,
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  costMicrousd: Schema.Natural,
}) {}

/** Settlement aggregate included with a terminal Run settlement. */
export class RunUsageSummary extends Schema.Class<RunUsageSummary>(
  "@effect-agent/core/RunUsageSummary",
)({
  modelCalls: Schema.Natural,
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  costMicrousd: Schema.Natural,
  byModel: Schema.Array(ModelUsageGroup),
}) {}

interface MutableUsageGroup {
  readonly provider: string;
  readonly model: string;
  readonly serviceTier?: string | undefined;
  readonly pricingVersion?: string | undefined;
  modelCalls: number;
  inputTokens: {
    total: number;
    uncached: number;
    cacheRead: number;
    cacheWrite: number;
  };
  outputTokens: { total: number; text: number; reasoning: number };
  costMicrousd: number;
}

const emptyInputTokens = () => ({ total: 0, uncached: 0, cacheRead: 0, cacheWrite: 0 });
const emptyOutputTokens = () => ({ total: 0, text: 0, reasoning: 0 });

/** Canonical usage could not be aggregated without exceeding safe-integer accounting. */
export class UsageAggregationError extends Schema.TaggedError<UsageAggregationError>()(
  "UsageAggregationError",
  {
    field: Schema.NonEmptyString,
    message: Schema.String,
  },
) {}

const checkedAdd = (
  field: string,
  left: number,
  right: number,
): Effect.Effect<number, UsageAggregationError> => {
  const total = left + right;
  return Number.isSafeInteger(left) &&
    left >= 0 &&
    Number.isSafeInteger(right) &&
    right >= 0 &&
    Number.isSafeInteger(total)
    ? Effect.succeed(total)
    : Effect.fail(
        UsageAggregationError.make({
          field,
          message: `Canonical usage aggregation exceeded the safe-integer range at ${field}`,
        }),
      );
};

/** Deterministically aggregate canonical per-call usage without making cached tokens free. */
export const summarizeModelUsage = Effect.fn("summarizeModelUsage")(function* (
  calls: ReadonlyArray<ModelCallUsage>,
): Effect.fn.Return<RunUsageSummary, UsageAggregationError> {
  const inputTokens = emptyInputTokens();
  const outputTokens = emptyOutputTokens();
  const groups = new Map<string, MutableUsageGroup>();
  let modelCalls = 0;
  let costMicrousd = 0;

  for (const call of calls) {
    modelCalls = yield* checkedAdd("modelCalls", modelCalls, 1);
    inputTokens.total = yield* checkedAdd(
      "inputTokens.total",
      inputTokens.total,
      call.inputTokens.total,
    );
    inputTokens.uncached = yield* checkedAdd(
      "inputTokens.uncached",
      inputTokens.uncached,
      call.inputTokens.uncached,
    );
    inputTokens.cacheRead = yield* checkedAdd(
      "inputTokens.cacheRead",
      inputTokens.cacheRead,
      call.inputTokens.cacheRead,
    );
    inputTokens.cacheWrite = yield* checkedAdd(
      "inputTokens.cacheWrite",
      inputTokens.cacheWrite,
      call.inputTokens.cacheWrite,
    );
    outputTokens.total = yield* checkedAdd(
      "outputTokens.total",
      outputTokens.total,
      call.outputTokens.total,
    );
    outputTokens.text = yield* checkedAdd(
      "outputTokens.text",
      outputTokens.text,
      call.outputTokens.text,
    );
    outputTokens.reasoning = yield* checkedAdd(
      "outputTokens.reasoning",
      outputTokens.reasoning,
      call.outputTokens.reasoning,
    );
    costMicrousd = yield* checkedAdd("costMicrousd", costMicrousd, call.costMicrousd);

    const key = JSON.stringify([
      call.provider,
      call.model,
      call.serviceTier ?? null,
      call.pricingVersion ?? null,
    ]);
    let group = groups.get(key);
    if (group === undefined) {
      group = {
        provider: call.provider,
        model: call.model,
        ...(call.serviceTier === undefined ? {} : { serviceTier: call.serviceTier }),
        ...(call.pricingVersion === undefined ? {} : { pricingVersion: call.pricingVersion }),
        modelCalls: 0,
        inputTokens: emptyInputTokens(),
        outputTokens: emptyOutputTokens(),
        costMicrousd: 0,
      };
      groups.set(key, group);
    }
    group.modelCalls = yield* checkedAdd("byModel.modelCalls", group.modelCalls, 1);
    group.inputTokens.total = yield* checkedAdd(
      "byModel.inputTokens.total",
      group.inputTokens.total,
      call.inputTokens.total,
    );
    group.inputTokens.uncached = yield* checkedAdd(
      "byModel.inputTokens.uncached",
      group.inputTokens.uncached,
      call.inputTokens.uncached,
    );
    group.inputTokens.cacheRead = yield* checkedAdd(
      "byModel.inputTokens.cacheRead",
      group.inputTokens.cacheRead,
      call.inputTokens.cacheRead,
    );
    group.inputTokens.cacheWrite = yield* checkedAdd(
      "byModel.inputTokens.cacheWrite",
      group.inputTokens.cacheWrite,
      call.inputTokens.cacheWrite,
    );
    group.outputTokens.total = yield* checkedAdd(
      "byModel.outputTokens.total",
      group.outputTokens.total,
      call.outputTokens.total,
    );
    group.outputTokens.text = yield* checkedAdd(
      "byModel.outputTokens.text",
      group.outputTokens.text,
      call.outputTokens.text,
    );
    group.outputTokens.reasoning = yield* checkedAdd(
      "byModel.outputTokens.reasoning",
      group.outputTokens.reasoning,
      call.outputTokens.reasoning,
    );
    group.costMicrousd = yield* checkedAdd(
      "byModel.costMicrousd",
      group.costMicrousd,
      call.costMicrousd,
    );
  }

  return RunUsageSummary.make({
    modelCalls,
    inputTokens: InputTokenUsage.make(inputTokens),
    outputTokens: OutputTokenUsage.make(outputTokens),
    costMicrousd,
    byModel: [...groups.values()].map((group) =>
      ModelUsageGroup.make({
        provider: group.provider,
        model: group.model,
        ...(group.serviceTier === undefined ? {} : { serviceTier: group.serviceTier }),
        ...(group.pricingVersion === undefined ? {} : { pricingVersion: group.pricingVersion }),
        modelCalls: group.modelCalls,
        inputTokens: InputTokenUsage.make(group.inputTokens),
        outputTokens: OutputTokenUsage.make(group.outputTokens),
        costMicrousd: group.costMicrousd,
      }),
    ),
  });
});
