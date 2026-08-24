import { Schema } from "effect";

const UsageIdentity = Schema.NonEmptyString.check(Schema.isMaxLength(256));

/** Provider-reported input-token components for one completed model call. */
export class InputTokenUsage extends Schema.Class<InputTokenUsage>(
  "@effect-agent/core/InputTokenUsage",
)({
  total: Schema.Natural,
  uncached: Schema.Natural,
  cacheRead: Schema.Natural,
  cacheWrite: Schema.Natural,
}) {}

/** Provider-reported output-token components for one completed model call. */
export class OutputTokenUsage extends Schema.Class<OutputTokenUsage>(
  "@effect-agent/core/OutputTokenUsage",
)({
  total: Schema.Natural,
  text: Schema.Natural,
  reasoning: Schema.Natural,
}) {}

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

/** Deterministically aggregate canonical per-call usage without making cached tokens free. */
export const summarizeModelUsage = (calls: ReadonlyArray<ModelCallUsage>): RunUsageSummary => {
  const inputTokens = emptyInputTokens();
  const outputTokens = emptyOutputTokens();
  const groups = new Map<string, MutableUsageGroup>();
  let costMicrousd = 0;

  for (const call of calls) {
    inputTokens.total += call.inputTokens.total;
    inputTokens.uncached += call.inputTokens.uncached;
    inputTokens.cacheRead += call.inputTokens.cacheRead;
    inputTokens.cacheWrite += call.inputTokens.cacheWrite;
    outputTokens.total += call.outputTokens.total;
    outputTokens.text += call.outputTokens.text;
    outputTokens.reasoning += call.outputTokens.reasoning;
    costMicrousd += call.costMicrousd;

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
    group.modelCalls += 1;
    group.inputTokens.total += call.inputTokens.total;
    group.inputTokens.uncached += call.inputTokens.uncached;
    group.inputTokens.cacheRead += call.inputTokens.cacheRead;
    group.inputTokens.cacheWrite += call.inputTokens.cacheWrite;
    group.outputTokens.total += call.outputTokens.total;
    group.outputTokens.text += call.outputTokens.text;
    group.outputTokens.reasoning += call.outputTokens.reasoning;
    group.costMicrousd += call.costMicrousd;
  }

  return RunUsageSummary.make({
    modelCalls: calls.length,
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
};
