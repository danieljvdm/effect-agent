import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import type { PrecisionError } from "../src/precision.ts";
import { calculatePrecision, PrecisionResult, renderPrecision } from "../src/precision.ts";

describe("sample-count precision", () => {
  it.effect("distinguishes three repeats from 95% median coverage and a 5% zero-harm bound", () =>
    Effect.gen(function* () {
      const calculation = calculatePrecision({ samples: 3, confidence: 0.95, maxHarmRate: 0.05 });

      expectTypeOf<Effect.Error<typeof calculation>>().toEqualTypeOf<PrecisionError>();
      expectTypeOf<Effect.Services<typeof calculation>>().toEqualTypeOf<never>();
      const result = yield* calculation;

      expect(result.median.maximumConfidenceApproximation).toBe(0.75);
      expect(result.median.minimumIndependentSamples).toBe(6);
      expect(result.zeroHarms.upperRateApproximation).toBeCloseTo(0.6315968501359612, 14);
      expect(result.zeroHarms.minimumIndependentSamples).toBe(59);
      const six = yield* calculatePrecision({ samples: 6, confidence: 0.95, maxHarmRate: 0.05 });

      expect(six.median.maximumConfidenceApproximation).toBe(0.96875);

      const fiftyEight = yield* calculatePrecision({
        samples: 58,
        confidence: 0.95,
        maxHarmRate: 0.05,
      });

      const fiftyNine = yield* calculatePrecision({
        samples: 59,
        confidence: 0.95,
        maxHarmRate: 0.05,
      });

      expect(fiftyEight.zeroHarms.upperRateApproximation).toBeGreaterThan(0.05);
      expect(fiftyNine.zeroHarms.upperRateApproximation).toBeLessThan(0.05);
    }),
  );

  it.effect.each([
    { confidence: 0.75, maxHarmRate: 0.5, median: 3, harms: 2 },
    { confidence: 0.75 + Number.EPSILON, maxHarmRate: 0.5, median: 4, harms: 3 },
    { confidence: 0.875, maxHarmRate: 0.5, median: 4, harms: 3 },
    { confidence: 0.875, maxHarmRate: 0.5 - Number.EPSILON, median: 4, harms: 4 },
    { confidence: 0.9375, maxHarmRate: 0.5, median: 5, harms: 4 },
    { confidence: 0.96875, maxHarmRate: 0.5, median: 6, harms: 5 },
    { confidence: 0.0199, maxHarmRate: 0.01, median: 2, harms: 3 },
    { confidence: 0.000001999999, maxHarmRate: 0.000001, median: 2, harms: 3 },
  ])("keeps the inclusive exact threshold at $confidence / $maxHarmRate", (example) =>
    Effect.gen(function* () {
      const result = yield* calculatePrecision({ samples: 3, ...example });

      expect(result.median.minimumIndependentSamples).toBe(example.median);
      expect(result.zeroHarms.minimumIndependentSamples).toBe(example.harms);
    }),
  );

  it.effect(
    "retains a finite tail when confidence rounds to one and does not clamp required N",
    () =>
      Effect.gen(function* () {
        const result = yield* calculatePrecision({
          samples: 1_000_000,
          confidence: 0.999999,
          maxHarmRate: 0.000001,
        });

        expect(result.median.maximumConfidenceApproximation).toBe(1);
        expect(Number.isFinite(result.median.logNoncoverageProbability)).toBe(true);
        expect(result.median.logNoncoverageProbability).toBeLessThan(-690_000);
        expect(result.median.minimumIndependentSamples).toBe(21);
        expect(result.zeroHarms.minimumIndependentSamples).toBeGreaterThan(13_000_000);
        expect(result.zeroHarms.minimumIndependentSamples).toBeLessThan(14_000_000);
        expect(renderPrecision(result)).toContain("<100%");
        expect(renderPrecision(result)).toContain("2^(-999999)");
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(PrecisionResult))(result);

        expect(
          yield* Schema.decodeUnknownEffect(Schema.fromJsonString(PrecisionResult))(encoded),
        ).toEqual(result);
      }),
  );

  it.effect("certifies near-threshold minima against bounded exact rational powers", () =>
    Effect.gen(function* () {
      // Independent test oracle: full rational exponentiation, only for N<=64.
      // All supported double inputs scale exactly to this integer denominator.
      const scale = 1n << 72n;
      let checked = 0;

      for (const rate of [0.000001, 0.001, 0.01, 0.05, 0.1, 0.2, 0.5, 0.75]) {
        for (const count of [1, 2, 3, 5, 8, 12]) {
          const boundary = 1 - (1 - rate) ** count;

          for (const confidence of [
            boundary - Number.EPSILON,
            boundary,
            boundary + Number.EPSILON,
          ]) {
            if (confidence < 0.000001 || confidence > 0.999999) continue;
            const numerator = scale - BigInt(rate * 2 ** 72);
            const alphaNumerator = scale - BigInt(confidence * 2 ** 72);
            let exactMinimum = 1;

            while (
              exactMinimum < 64 &&
              numerator ** BigInt(exactMinimum) > alphaNumerator * scale ** BigInt(exactMinimum - 1)
            )
              exactMinimum += 1;

            expect(exactMinimum).toBeLessThan(64);

            const result = yield* calculatePrecision({
              samples: count,
              confidence,
              maxHarmRate: rate,
            });

            expect(result.zeroHarms.minimumIndependentSamples).toBe(exactMinimum);
            checked += 1;
          }
        }
      }
      expect(checked).toBeGreaterThan(100);
    }),
  );

  it.effect("handles one independent unit without implying median coverage", () =>
    Effect.gen(function* () {
      const result = yield* calculatePrecision({ samples: 1, confidence: 0.95, maxHarmRate: 0.95 });

      expect(result.median.maximumConfidenceApproximation).toBe(0);
      expect(result.zeroHarms.upperRateApproximation).toBe(0.95);
      expect(result.zeroHarms.minimumIndependentSamples).toBe(1);
    }),
  );

  it.effect("rejects invalid or unsupported counts, confidences and harm rates", () =>
    Effect.gen(function* () {
      const valid = { samples: 3, confidence: 0.95, maxHarmRate: 0.05 };

      const invalid = [
        ...[0, -1, 1.5, 1_000_001, Infinity, NaN, "3"].map((samples) => ({ ...valid, samples })),
        ...[0, 1, -0.1, 0.0000001, Infinity, NaN, "0.95"].map((confidence) => ({
          ...valid,
          confidence,
        })),
        ...[0, 1, -0.1, 0.0000001, Infinity, NaN, "0.05"].map((maxHarmRate) => ({
          ...valid,
          maxHarmRate,
        })),
        {},
        null,
      ];

      for (const input of invalid) {
        expect((yield* calculatePrecision(input).pipe(Effect.flip))._tag).toBe("PrecisionError");
      }
    }),
  );
});
