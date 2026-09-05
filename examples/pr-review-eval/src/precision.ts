import { Effect, Schema } from "effect";

export const PrecisionSampleCount = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: 1_000_000 }),
);

export const PrecisionProbability = Schema.Finite.check(
  Schema.isBetween({ minimum: 0.000001, maximum: 0.999999 }),
);

export class PrecisionInput extends Schema.Class<PrecisionInput>("PrecisionInput")({
  samples: PrecisionSampleCount,
  confidence: PrecisionProbability,
  maxHarmRate: PrecisionProbability,
}) {}

const Probability = Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 }));
const MinimumCount = Schema.Int.check(Schema.isGreaterThan(0));

export class PrecisionResult extends Schema.Class<PrecisionResult>("PrecisionResult")({
  input: PrecisionInput,
  median: Schema.Struct({
    maximumConfidenceApproximation: Probability,
    logNoncoverageProbability: Schema.Finite,
    minimumIndependentSamples: MinimumCount,
  }),
  zeroHarms: Schema.Struct({
    assumedEventCount: Schema.Literal(0),
    upperRateApproximation: Probability,
    minimumIndependentSamples: MinimumCount,
  }),
  assumptions: Schema.Array(Schema.String),
}) {}

export class PrecisionError extends Schema.TaggedError<PrecisionError>()("PrecisionError", {
  message: Schema.String,
}) {}

// Validated probabilities are positive normal IEEE doubles with denominators at
// most 2^72. Scaling to 256 or 512 fractional bits therefore loses no input bits.
const scaledProbability = (value: number, bits: bigint): bigint =>
  BigInt(value * 2 ** Number(bits));

const compareZeroEventTail = (
  samples: number,
  maxHarmRate: number,
  confidence: number,
  bits: bigint,
): boolean | undefined => {
  const scale = 1n << bits;
  const target = scale - scaledProbability(confidence, bits);
  let baseLow = scale - scaledProbability(maxHarmRate, bits);
  let baseHigh = baseLow;
  let low = scale;
  let high = scale;
  let remaining = samples;

  // Directed rounding encloses the exact rational power in a fixed-size interval.
  // Each multiplication has at most 1025 product bits, independent of sample N.
  while (remaining > 0) {
    if (remaining % 2 === 1) {
      low = (low * baseLow) >> bits;
      high = (high * baseHigh + scale - 1n) >> bits;
    }
    remaining = Math.floor(remaining / 2);
    if (remaining > 0) {
      baseLow = (baseLow * baseLow) >> bits;
      baseHigh = (baseHigh * baseHigh + scale - 1n) >> bits;
    }
  }

  if (high <= target) return true;
  if (low > target) return false;

  return undefined;
};

const zeroEventTailAtMost = Effect.fn("PrReviewEval.zeroEventTailAtMost")(function* (
  samples: number,
  input: PrecisionInput,
) {
  const comparison =
    compareZeroEventTail(samples, input.maxHarmRate, input.confidence, 256n) ??
    compareZeroEventTail(samples, input.maxHarmRate, input.confidence, 512n);

  if (comparison === undefined)
    return yield* PrecisionError.make({
      message:
        "The event-rate threshold is numerically ambiguous at bounded precision; no minimum count is certified",
    });

  return comparison;
});

/**
 * Planning calculations for a fixed number of independent observations; no data
 * or study decisions are consumed. Median coverage is exact for an iid continuous
 * population and conservative with ties. The event bound assumes iid Bernoulli
 * trials with zero events and a prespecified event definition and sample size.
 *
 * Derivations: P(all observations on either side of a median) = 2 * (1/2)^N;
 * P(zero events at rate p) = (1-p)^N. See the NIST references in the example guide.
 */
export const calculatePrecision = Effect.fn("PrReviewEval.calculatePrecision")(function* (
  input: unknown,
) {
  const decoded = yield* Schema.decodeUnknownEffect(PrecisionInput)(input).pipe(
    Effect.mapError((cause) => PrecisionError.make({ message: cause.message })),
  );

  const logAlpha = Math.log1p(-decoded.confidence);
  const logNoHarm = Math.log1p(-decoded.maxHarmRate);
  const logMedianTail = (1 - decoded.samples) * Math.LN2;
  let minimumMedian = Math.max(1, Math.ceil(1 - logAlpha / Math.LN2));

  // Required N is at most 21: these dyadic confidences are exactly representable.
  if (minimumMedian > 1 && 1 - 2 ** (2 - minimumMedian) >= decoded.confidence) minimumMedian -= 1;
  else if (1 - 2 ** (1 - minimumMedian) < decoded.confidence) minimumMedian += 1;

  let minimumHarms = Math.max(1, Math.ceil(logAlpha / logNoHarm));

  if (minimumHarms > 1 && (yield* zeroEventTailAtMost(minimumHarms - 1, decoded)))
    minimumHarms -= 1;
  else if (!(yield* zeroEventTailAtMost(minimumHarms, decoded))) minimumHarms += 1;

  if (
    !(yield* zeroEventTailAtMost(minimumHarms, decoded)) ||
    (minimumHarms > 1 && (yield* zeroEventTailAtMost(minimumHarms - 1, decoded)))
  )
    return yield* PrecisionError.make({
      message: "The numerical sample-count estimate could not be certified as minimal",
    });

  return PrecisionResult.make({
    input: decoded,
    median: {
      maximumConfidenceApproximation: 1 - 2 ** (1 - decoded.samples),
      logNoncoverageProbability: logMedianTail,
      minimumIndependentSamples: minimumMedian,
    },
    zeroHarms: {
      assumedEventCount: 0,
      upperRateApproximation: -Math.expm1(logAlpha / decoded.samples),
      minimumIndependentSamples: minimumHarms,
    },
    assumptions: [
      "N counts genuinely independent units from a common population. Correlated findings, repeated reviews of one case, and related cases cannot automatically be counted as independent population samples.",
      "Median min/max coverage is two-sided, exact for iid continuous observations and conservative with ties. No observations or interval endpoints are supplied.",
      "The one-sided event-rate bound assumes exactly zero harms in a prespecified fixed N of iid Bernoulli trials with a common event probability. This command does not assert that zero harms were observed.",
      "Probabilities use floating-point approximations. Median noncoverage is exactly 2^(1-N); its finite logarithm is retained even when displayed confidence rounds to one. Minimum event counts are certified for the represented numeric inputs with bounded directed arithmetic.",
      "N alone cannot predict interval width or statistical power. These calculations are not observed results, study eligibility, or evidence of quality, speed, affordability, or population performance.",
    ],
  });
});

export const renderPrecision = (result: PrecisionResult): string => {
  const maximum = result.median.maximumConfidenceApproximation;
  const percentage = maximum === 1 ? "<100% (rounded approximation: 1)" : `${maximum * 100}%`;

  return [
    `Independent units: ${result.input.samples}; requested confidence: ${result.input.confidence * 100}%.`,
    `Two-sided median [minimum, maximum] confidence: ${percentage}; exact noncoverage: 2^(${1 - result.input.samples}).`,
    `Minimum independent N for that median confidence: ${result.median.minimumIndependentSamples}.`,
    `Assuming zero harms: one-sided upper event-rate bound approximately ${result.zeroHarms.upperRateApproximation}.`,
    `Minimum independent N for an upper bound <= ${result.input.maxHarmRate} with zero harms: ${result.zeroHarms.minimumIndependentSamples}.`,
    ...result.assumptions,
  ].join("\n");
};
