import {
  type ReviewContextError,
  ReviewLineMatches,
  ReviewRepository,
} from "@effect-agent/pr-review/ReviewRepository";
import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Effect } from "effect";

const input = {
  path: "src/provider.ts",
  revision: "head",
  literal: "getUsage(",
  startLine: 1,
} as const;

describe("bounded literal source lookup", () => {
  it.effect("locates a late definition literally and paginates distinct matching lines", () =>
    Effect.gen(function* () {
      const source = `${"unrelated\n".repeat(3_152)}const getUsage(value) = value; getUsage(value)\nGETUSAGE(value)\n`;
      const result = yield* ReviewLineMatches.fromText(input, source);

      expect(result).toEqual(
        ReviewLineMatches.make({
          path: input.path,
          revision: "head",
          lines: [3_153],
          truncated: false,
        }),
      );
      expect(JSON.stringify(result)).not.toContain("getUsage");

      const repeated = "getUsage(x); getUsage(y)\n".repeat(21);
      const first = yield* ReviewLineMatches.fromText(input, repeated);
      const rest = yield* ReviewLineMatches.fromText({ ...input, startLine: 21 }, repeated);
      const absent = yield* ReviewLineMatches.fromText({ ...input, literal: "getUsage.*" }, source);

      expect(first.lines).toEqual([
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      ]);
      expect(first.truncated).toBe(true);
      expect((yield* ReviewLineMatches.fromText(input, "getUsage(x)\n".repeat(20))).truncated).toBe(
        false,
      );
      expect(rest.lines).toEqual([21]);
      expect(rest.truncated).toBe(false);
      expect(absent.lines).toEqual([]);
      expect(absent.truncated).toBe(false);
      expect((yield* ReviewLineMatches.fromText(input, "")).lines).toEqual([]);
    }),
  );

  it.effect("enforces UTF-8 bytes and readable lines at their exact boundaries", () =>
    Effect.gen(function* () {
      const twoMegabytes = "é".repeat(1_000_000);

      expect(
        (yield* ReviewLineMatches.fromText({ ...input, literal: "é" }, twoMegabytes)).lines,
      ).toEqual([1]);
      for (const source of [twoMegabytes + "x", "x".repeat(2_000_001)]) {
        const error = yield* ReviewLineMatches.fromText(input, source).pipe(Effect.flip);

        expect(error.message).toContain("2,000,000-byte UTF-8");
      }

      const lastLine = yield* ReviewLineMatches.fromText(
        { ...input, startLine: 1_000_000 },
        `${"\n".repeat(999_999)}getUsage(x)\n`,
      );

      expect(lastLine.lines).toEqual([1_000_000]);

      const tooManyLines = yield* ReviewLineMatches.fromText(
        input,
        `getUsage(x)\n${"\n".repeat(999_999)}last`,
      ).pipe(Effect.flip);

      expect(tooManyLines.message).toContain("1,000,000-line");
    }),
  );

  it.effect.each([
    { literal: "" },
    { literal: "getUsage(\n" },
    { literal: "getUsage(\r" },
    { literal: "x".repeat(201) },
    { startLine: 0 },
    { startLine: 1_000_001 },
    { startLine: 2 },
  ])("rejects invalid or out-of-range lookup without a false empty result: %j", (override) =>
    Effect.gen(function* () {
      const error = yield* ReviewLineMatches.fromText(
        { ...input, ...override },
        "getUsage(x)",
      ).pipe(Effect.flip);

      expect(error._tag).toBe("ReviewContextError");
    }),
  );

  it("keeps source lookup failures and requirements visible", () => {
    const local = ReviewLineMatches.fromText(input, "");

    const fromHost = Effect.gen(function* () {
      const repository = yield* ReviewRepository;

      return yield* repository.findInFile(input);
    });

    expectTypeOf(local).toEqualTypeOf<Effect.Effect<ReviewLineMatches, ReviewContextError>>();
    expectTypeOf(fromHost).toEqualTypeOf<
      Effect.Effect<ReviewLineMatches, ReviewContextError, ReviewRepository>
    >();
  });
});
