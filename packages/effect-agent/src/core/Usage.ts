import { Effect, Schema } from "effect";

import { RunId } from "./Identifiers.ts";

const UsageIdentity = Schema.NonEmptyString.check(Schema.isMaxLength(256));

/** Bounded provider response identity. Request configuration is not response evidence. */
export class ModelResponseIdentity extends Schema.Class<ModelResponseIdentity>(
  "@effect-agent/core/ModelResponseIdentity",
)({
  id: Schema.optionalKey(UsageIdentity),
  model: Schema.optionalKey(UsageIdentity),
}) {}

/** Missing legacy status means unknown, never a free call or a complete report. */
export const UsageCompleteness = Schema.Literals(["complete", "partial", "unknown"]);

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
  /** Configured binding identity. See response.model for provider-reported identity. */
  model: UsageIdentity,
  serviceTier: Schema.optionalKey(UsageIdentity),
  pricingVersion: Schema.optionalKey(UsageIdentity),
  response: Schema.optionalKey(ModelResponseIdentity),
  purpose: Schema.optionalKey(Schema.Literals(["turn", "summary"])),
  usageStatus: Schema.optionalKey(UsageCompleteness),
  pricingStatus: Schema.optionalKey(Schema.Literals(["estimated", "unknown"])),
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  costMicrousd: Schema.Natural,
}) {}

/**
 * Observed model usage and estimated cost, without per-model attribution.
 * Numeric zero with unknown coverage never establishes free execution. Missing legacy
 * statuses mean unknown. Unobserved calls are excluded from the numeric call/token totals.
 */
export class RunTotals extends Schema.Class<RunTotals>("@effect-agent/core/RunTotals")({
  modelCalls: Schema.Natural,
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  costMicrousd: Schema.Natural,
  usageStatus: Schema.optionalKey(UsageCompleteness),
  pricingStatus: Schema.optionalKey(UsageCompleteness),
  unobservedModelCalls: Schema.optionalKey(Schema.Natural),
}) {}

/** Own Run usage and disjoint attached-descendant usage. Add each component once. */
export class RunUsageReport extends Schema.Class<RunUsageReport>(
  "@effect-agent/core/RunUsageReport",
)({
  usage: RunTotals,
  delegatedUsage: RunTotals,
}) {}

/** A verified child report retained across durable parent Attempts. */
export class ChildRunUsage extends Schema.Class<ChildRunUsage>("@effect-agent/core/ChildRunUsage")({
  runId: RunId,
  report: RunUsageReport,
}) {}

/** Known absence of observed work. */
export const emptyRunTotals = (): RunTotals =>
  RunTotals.make({
    modelCalls: 0,
    inputTokens: 0,
    outputTokens: 0,
    costMicrousd: 0,
    usageStatus: "complete",
    pricingStatus: "complete",
    unobservedModelCalls: 0,
  });

/** No accounting evidence, including legacy child reports. This is not known zero spend. */
export const unknownRunTotals = (): RunTotals =>
  RunTotals.make({
    ...emptyRunTotals(),
    usageStatus: "unknown",
    pricingStatus: "unknown",
  });

/** Settlement-sized aggregate for calls sharing one pricing identity. */
export class ModelUsageGroup extends Schema.Class<ModelUsageGroup>(
  "@effect-agent/core/ModelUsageGroup",
)({
  provider: UsageIdentity,
  model: UsageIdentity,
  /** Actual returned model; absent when only the configured binding is known. */
  responseModel: Schema.optionalKey(UsageIdentity),
  serviceTier: Schema.optionalKey(UsageIdentity),
  pricingVersion: Schema.optionalKey(UsageIdentity),
  modelCalls: Schema.Natural.check(Schema.isGreaterThan(0)),
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  costMicrousd: Schema.Natural,
}) {}

const RunUsageSummaryFields = Schema.Struct({
  modelCalls: Schema.Natural,
  inputTokens: InputTokenUsage,
  outputTokens: OutputTokenUsage,
  costMicrousd: Schema.Natural,
  byModel: Schema.Array(ModelUsageGroup),
  /** Coverage of recorded calls only; interruptions can make the Run less complete. */
  usageStatus: Schema.optionalKey(UsageCompleteness),
  pricingStatus: Schema.optionalKey(UsageCompleteness),
  /** Observed invocations without retained accounting, excluded from numeric call/token totals. */
  unobservedModelCalls: Schema.optionalKey(Schema.Natural),
}).check(
  Schema.makeFilter(
    (summary) => {
      const identities = summary.byModel.map((group) =>
        JSON.stringify([
          group.provider,
          group.model,
          group.responseModel ?? null,
          group.serviceTier ?? null,
          group.pricingVersion ?? null,
        ]),
      );

      return (
        new Set(identities).size === identities.length &&
        hasAdditiveTotal(
          summary.modelCalls,
          summary.byModel.map((group) => group.modelCalls),
        ) &&
        hasAdditiveTotal(
          summary.inputTokens.total,
          summary.byModel.map((group) => group.inputTokens.total),
        ) &&
        hasAdditiveTotal(
          summary.inputTokens.uncached,
          summary.byModel.map((group) => group.inputTokens.uncached),
        ) &&
        hasAdditiveTotal(
          summary.inputTokens.cacheRead,
          summary.byModel.map((group) => group.inputTokens.cacheRead),
        ) &&
        hasAdditiveTotal(
          summary.inputTokens.cacheWrite,
          summary.byModel.map((group) => group.inputTokens.cacheWrite),
        ) &&
        hasAdditiveTotal(
          summary.outputTokens.total,
          summary.byModel.map((group) => group.outputTokens.total),
        ) &&
        hasAdditiveTotal(
          summary.outputTokens.text,
          summary.byModel.map((group) => group.outputTokens.text),
        ) &&
        hasAdditiveTotal(
          summary.outputTokens.reasoning,
          summary.byModel.map((group) => group.outputTokens.reasoning),
        ) &&
        hasAdditiveTotal(
          summary.costMicrousd,
          summary.byModel.map((group) => group.costMicrousd),
        )
      );
    },
    {
      title:
        "Run usage totals equal unique per-model pricing groups within safe-integer accounting",
    },
  ),
);

/** Settlement aggregate included with a terminal Run settlement. */
export class RunUsageSummary extends Schema.Class<RunUsageSummary>(
  "@effect-agent/core/RunUsageSummary",
)(RunUsageSummaryFields) {}

interface MutableUsageGroup {
  readonly provider: string;
  readonly model: string;
  readonly responseModel?: string | undefined;
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

/** Combine disjoint totals, retaining uncertainty and rejecting numeric overflow. */
export const sumRunTotals = Effect.fn("sumRunTotals")(function* (
  contributions: ReadonlyArray<RunTotals>,
): Effect.fn.Return<RunTotals, UsageAggregationError> {
  let modelCalls = 0;
  let inputTokens = 0;
  let outputTokens = 0;
  let costMicrousd = 0;
  let unobservedModelCalls = 0;
  const usageStatuses: Array<typeof UsageCompleteness.Type> = [];
  const pricingStatuses: Array<typeof UsageCompleteness.Type> = [];

  for (const contribution of contributions) {
    const value = yield* Schema.decodeEffect(RunTotals)(contribution).pipe(
      Effect.mapError(
        () => new UsageAggregationError({ field: "totals", message: "Invalid Run totals" }),
      ),
    );

    modelCalls = yield* checkedAdd("modelCalls", modelCalls, value.modelCalls);
    inputTokens = yield* checkedAdd("inputTokens", inputTokens, value.inputTokens);
    outputTokens = yield* checkedAdd("outputTokens", outputTokens, value.outputTokens);
    costMicrousd = yield* checkedAdd("costMicrousd", costMicrousd, value.costMicrousd);
    unobservedModelCalls = yield* checkedAdd(
      "unobservedModelCalls",
      unobservedModelCalls,
      value.unobservedModelCalls ?? 0,
    );
    // Known empty contributions are identities; unknown empty contributions are evidence gaps.
    if (
      value.modelCalls > 0 ||
      (value.unobservedModelCalls ?? 0) > 0 ||
      value.usageStatus !== "complete"
    ) {
      usageStatuses.push(value.usageStatus ?? "unknown");
    }
    if (
      value.modelCalls > 0 ||
      (value.unobservedModelCalls ?? 0) > 0 ||
      value.pricingStatus !== "complete"
    ) {
      pricingStatuses.push(value.pricingStatus ?? "unknown");
    }
  }

  const coverage = (statuses: ReadonlyArray<typeof UsageCompleteness.Type>) =>
    statuses.every((status) => status === "complete")
      ? ("complete" as const)
      : statuses.every((status) => status === "unknown")
        ? ("unknown" as const)
        : ("partial" as const);

  return RunTotals.make({
    modelCalls,
    inputTokens,
    outputTokens,
    costMicrousd,
    unobservedModelCalls,
    usageStatus: coverage(usageStatuses),
    pricingStatus: coverage(pricingStatuses),
  });
});

/** Flatten a canonical settlement summary without discarding its coverage markers. */
export const runTotalsFromSummary = (summary: RunUsageSummary): RunTotals =>
  RunTotals.make({
    modelCalls: summary.modelCalls,
    inputTokens: summary.inputTokens.total,
    outputTokens: summary.outputTokens.total,
    costMicrousd: summary.costMicrousd,
    usageStatus: summary.usageStatus ?? "unknown",
    pricingStatus: summary.pricingStatus ?? "unknown",
    unobservedModelCalls: summary.unobservedModelCalls ?? 0,
  });

/**
 * Deterministically aggregate canonical per-call usage without making cached tokens free.
 * A validated seed retains earlier call totals and pricing groups without retaining those calls.
 * Empty contributions do not change completeness; the seed is never mutated.
 */
export const summarizeModelUsage = Effect.fn("summarizeModelUsage")(function* (
  calls: ReadonlyArray<ModelCallUsage>,
  seed?: RunUsageSummary,
): Effect.fn.Return<RunUsageSummary, UsageAggregationError> {
  const initial =
    seed === undefined
      ? undefined
      : yield* Schema.decodeEffect(RunUsageSummary)(seed).pipe(
          Effect.mapError(
            () =>
              new UsageAggregationError({
                field: "seed",
                message: "Canonical usage seed does not satisfy the RunUsageSummary Schema",
              }),
          ),
        );

  const inputTokens = initial === undefined ? emptyInputTokens() : { ...initial.inputTokens };
  const outputTokens = initial === undefined ? emptyOutputTokens() : { ...initial.outputTokens };
  const groups = new Map<string, MutableUsageGroup>();
  let modelCalls = initial?.modelCalls ?? 0;
  let costMicrousd = initial?.costMicrousd ?? 0;
  const hasSeedCalls = modelCalls > 0;

  let allUsageUnknown =
    !hasSeedCalls || initial?.usageStatus === undefined || initial.usageStatus === "unknown";

  let allUsageComplete = !hasSeedCalls || initial?.usageStatus === "complete";

  let allPricingUnknown =
    !hasSeedCalls || initial?.pricingStatus === undefined || initial.pricingStatus === "unknown";

  let allPricingComplete = !hasSeedCalls || initial?.pricingStatus === "complete";

  for (const group of initial?.byModel ?? []) {
    const key = JSON.stringify([
      group.provider,
      group.model,
      group.responseModel ?? null,
      group.serviceTier ?? null,
      group.pricingVersion ?? null,
    ]);

    groups.set(key, {
      ...group,
      inputTokens: { ...group.inputTokens },
      outputTokens: { ...group.outputTokens },
    });
  }

  for (const call of calls) {
    allUsageUnknown &&= call.usageStatus === undefined || call.usageStatus === "unknown";
    allUsageComplete &&= call.usageStatus === "complete";
    allPricingUnknown &&= call.pricingStatus !== "estimated";
    allPricingComplete &&= call.pricingStatus === "estimated";
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
      call.response?.model ?? null,
      call.serviceTier ?? null,
      call.pricingVersion ?? null,
    ]);

    let group = groups.get(key);

    if (group === undefined) {
      group = {
        provider: call.provider,
        model: call.model,
        ...(call.response?.model === undefined ? {} : { responseModel: call.response.model }),
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
    usageStatus: allUsageUnknown ? "unknown" : allUsageComplete ? "complete" : "partial",
    pricingStatus: allPricingUnknown ? "unknown" : allPricingComplete ? "complete" : "partial",
    ...(initial?.unobservedModelCalls === undefined
      ? {}
      : { unobservedModelCalls: initial.unobservedModelCalls }),
    byModel: [...groups.values()].map((group) =>
      ModelUsageGroup.make({
        provider: group.provider,
        model: group.model,
        ...(group.responseModel === undefined ? {} : { responseModel: group.responseModel }),
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
