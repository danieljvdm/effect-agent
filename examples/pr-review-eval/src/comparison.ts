import { Effect, Schema } from "effect";

import {
  EvalCase,
  EvalComparison,
  EvalComparisonPayload,
  EvalConfigurationError,
  EvalFrozenCaseIdentity,
  EvalFrozenRun,
  EvalFrozenRunPayload,
  EvalSuite,
  EvalVariantConfiguration,
} from "./contracts.ts";
import { digestEvalOracle, digestText, validateEvalSuite } from "./corpus.ts";

export const configurationIdentity = (configuration: EvalVariantConfiguration): string =>
  JSON.stringify(Schema.encodeSync(EvalVariantConfiguration)(configuration));

const digestCases = (cases: ReadonlyArray<EvalCase>) =>
  digestText(Schema.encodeSync(Schema.fromJsonString(Schema.Array(EvalCase)))(cases));

/** Freeze one descriptive baseline run; paired comparison and rescore rules remain separate. */
export const freezeEvalRun = Effect.fn("PrReviewEval.freezeEvalRun")(function* (
  suite: EvalSuite,
  configuration: EvalVariantConfiguration,
  id: string,
  trials: number,
) {
  yield* validateEvalSuite(suite);
  if (
    suite.comparison !== undefined ||
    configuration.strategy !== "baseline" ||
    configuration.effective?.reviewerRevision === undefined ||
    configuration.guidanceDigest === undefined ||
    configuration.costLimitMicrousd !== 999_999 ||
    configuration.effective.discoveryLimitMicrousd !== 999_999 ||
    configuration.effective.verificationReserveMicrousd !== 0
  ) {
    return yield* EvalConfigurationError.make({
      message:
        "A baseline freeze requires one exact baseline configuration and no paired comparison",
    });
  }
  const groups = new Map<string, string>();
  const cases: Array<EvalCase> = [];

  for (const evalCase of suite.cases) {
    if (
      evalCase.repository === undefined ||
      evalCase.oracleVersion === undefined ||
      evalCase.split === undefined ||
      evalCase.relatedGroup === undefined
    ) {
      return yield* EvalConfigurationError.make({
        message: `Case ${evalCase.id} requires a frozen repository, oracleVersion, split, and relatedGroup`,
      });
    }
    if (groups.has(evalCase.relatedGroup) && groups.get(evalCase.relatedGroup) !== evalCase.split) {
      return yield* EvalConfigurationError.make({
        message: `Related group ${evalCase.relatedGroup} crosses development and heldout splits`,
      });
    }
    groups.set(evalCase.relatedGroup, evalCase.split);
    cases.push(EvalCase.make({ ...evalCase, oracleDigest: yield* digestEvalOracle(evalCase) }));
  }
  const casesDigest = yield* digestCases(cases);

  const payload = yield* Schema.decodeUnknownEffect(EvalFrozenRunPayload)({
    version: 1,
    id,
    configuration,
    casesDigest,
    trials,
    plannedRuns: cases.length * trials,
    maximumCostMicrousd: cases.length * trials * 999_999,
    order: "case-then-trial-v1",
  }).pipe(
    Effect.mapError(() =>
      EvalConfigurationError.make({
        message:
          "A baseline freeze requires 1-20 trials, at most 120 runs, and bounded total liability",
      }),
    ),
  );

  const digest = yield* digestText(
    Schema.encodeSync(Schema.fromJsonString(EvalFrozenRunPayload))(payload),
  );

  return EvalSuite.make({
    version: 1,
    cases,
    frozenRun: EvalFrozenRun.make({ ...payload, digest }),
  });
});

export const validateFrozenRun = Effect.fn("PrReviewEval.validateFrozenRun")(function* (
  suite: EvalSuite,
) {
  if (suite.frozenRun === undefined) {
    return yield* EvalConfigurationError.make({
      message: "Freeze the baseline run before execution",
    });
  }
  const frozen = suite.frozenRun;
  const rebuilt = yield* freezeEvalRun(suite, frozen.configuration, frozen.id, frozen.trials);

  if (
    (yield* digestCases(suite.cases)) !== frozen.casesDigest ||
    rebuilt.frozenRun === undefined ||
    Schema.encodeSync(Schema.fromJsonString(EvalFrozenRun))(rebuilt.frozenRun) !==
      Schema.encodeSync(Schema.fromJsonString(EvalFrozenRun))(frozen)
  ) {
    return yield* EvalConfigurationError.make({
      message: "Frozen baseline identity changed; create an explicit new freeze",
    });
  }

  return frozen;
});

/** No inference. The returned suite is the complete, bounded comparison manifest. */
export const freezeEvalSuite = Effect.fn("PrReviewEval.freezeEvalSuite")(function* (
  suite: EvalSuite,
  configurations: ReadonlyArray<EvalVariantConfiguration>,
  id: string,
) {
  yield* validateEvalSuite(suite);
  if (suite.frozenRun !== undefined)
    return yield* EvalConfigurationError.make({
      message: "A baseline frozen run cannot be used as a paired comparison",
    });
  const strategies = configurations.map((configuration) => configuration.strategy).sort();

  if (
    strategies.join(",") !== "baseline,verified" ||
    new Set(configurations.map((configuration) => configuration.id)).size !== 2
  ) {
    return yield* EvalConfigurationError.make({
      message: "Freeze exactly one baseline and one verified configuration with distinct IDs",
    });
  }
  for (const configuration of configurations) {
    if (
      configuration.effective?.reviewerRevision === undefined ||
      configuration.guidanceDigest === undefined ||
      configuration.costLimitMicrousd !== 999_999
    ) {
      return yield* EvalConfigurationError.make({
        message:
          "A comparison requires exact reviewer revision, prompts, guidance, pricing, limits, cache policy, and the 999999 microdollar total",
      });
    }
    const effective = configuration.effective;
    const verified = configuration.strategy === "verified";

    if (
      effective.discoveryLimitMicrousd !== (verified ? 699_999 : 999_999) ||
      effective.verificationReserveMicrousd !== (verified ? 300_000 : 0)
    ) {
      return yield* EvalConfigurationError.make({
        message: "Comparison stage allocation does not match the frozen baseline/verified policy",
      });
    }
  }

  const commonConfigurations = configurations.map((configuration) => {
    const {
      id: _id,
      strategy: _strategy,
      reviewerProfile: _profile,
      effective,
      ...shared
    } = configuration;

    if (effective === undefined) return "";

    const {
      discoveryLimitMicrousd: _discovery,
      verificationReserveMicrousd: _reserve,
      ...common
    } = effective;

    return JSON.stringify({ ...shared, effective: common });
  });

  if (new Set(commonConfigurations).size !== 1) {
    return yield* EvalConfigurationError.make({
      message:
        "Paired strategies must share reviewer revision, prompts, guidance, model, pricing, limits, and cache policy",
    });
  }
  const plannedRuns = suite.cases.length * 2 * 3;

  if (
    suite.cases.filter((evalCase) => evalCase.kind !== "unadjudicated").length > 20 ||
    plannedRuns > 120
  ) {
    return yield* EvalConfigurationError.make({
      message:
        "Freeze at most 20 adjudicated cases and 120 total trials, including operational replays",
    });
  }
  const groups = new Map<string, string>();
  const frozenCases: Array<EvalCase> = [];
  const identities: Array<EvalFrozenCaseIdentity> = [];

  for (const evalCase of [...suite.cases].sort((left, right) => left.id.localeCompare(right.id))) {
    if (
      evalCase.repository === undefined ||
      evalCase.oracleVersion === undefined ||
      evalCase.split === undefined ||
      evalCase.relatedGroup === undefined
    ) {
      return yield* EvalConfigurationError.make({
        message: `Case ${evalCase.id} requires a frozen repository, oracleVersion, split, and relatedGroup before comparison`,
      });
    }
    if (groups.has(evalCase.relatedGroup) && groups.get(evalCase.relatedGroup) !== evalCase.split) {
      return yield* EvalConfigurationError.make({
        message: `Related group ${evalCase.relatedGroup} crosses development and heldout splits`,
      });
    }
    groups.set(evalCase.relatedGroup, evalCase.split);
    const oracleDigest = yield* digestEvalOracle(evalCase);

    frozenCases.push(EvalCase.make({ ...evalCase, oracleDigest }));
    identities.push(
      EvalFrozenCaseIdentity.make({
        id: evalCase.id,
        inputDigest: evalCase.inputDigest,
        repositoryDigest: evalCase.repository.digest,
        oracleVersion: evalCase.oracleVersion,
        oracleDigest,
        split: evalCase.split,
        relatedGroup: evalCase.relatedGroup,
      }),
    );
  }
  if (
    !frozenCases.some(
      (evalCase) => evalCase.kind !== "unadjudicated" && evalCase.split === "development",
    ) ||
    !frozenCases.some(
      (evalCase) => evalCase.kind !== "unadjudicated" && evalCase.split === "heldout",
    )
  ) {
    return yield* EvalConfigurationError.make({
      message: "Freeze adjudicated development and heldout cases before comparison",
    });
  }

  const payload = yield* Schema.decodeUnknownEffect(EvalComparisonPayload)({
    version: 1,
    id,
    cases: identities,
    configurations: [...configurations].sort((left, right) =>
      (left.strategy ?? "").localeCompare(right.strategy ?? ""),
    ),
    trials: 3,
    plannedRuns,
    maximumCostMicrousd: plannedRuns * 999_999,
    order: "alternating-pairs-v1",
  }).pipe(
    Effect.mapError(() =>
      EvalConfigurationError.make({ message: "Invalid comparison identity or run budget" }),
    ),
  );

  const digest = yield* digestText(
    Schema.encodeSync(Schema.fromJsonString(EvalComparisonPayload))(payload),
  );

  return EvalSuite.make({
    version: 1,
    cases: frozenCases,
    comparison: EvalComparison.make({ ...payload, digest }),
  });
});

export const validateFrozenComparison = Effect.fn("PrReviewEval.validateFrozenComparison")(
  function* (suite: EvalSuite) {
    if (suite.comparison === undefined) {
      return yield* EvalConfigurationError.make({
        message: "Freeze the comparison before paid runs; use the freeze command",
      });
    }

    const rebuilt = yield* freezeEvalSuite(
      suite,
      suite.comparison.configurations,
      suite.comparison.id,
    );

    if (
      rebuilt.comparison === undefined ||
      JSON.stringify(Schema.encodeSync(EvalComparison)(rebuilt.comparison)) !==
        JSON.stringify(Schema.encodeSync(EvalComparison)(suite.comparison))
    ) {
      return yield* EvalConfigurationError.make({
        message: "Frozen comparison identity changed; create an explicit new freeze",
      });
    }

    return suite.comparison;
  },
);
