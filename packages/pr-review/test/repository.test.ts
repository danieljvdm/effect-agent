import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import type { ReviewContextError } from "../src/index.ts";
import { ReviewLineMatches, ReviewRepository } from "../src/index.ts";

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

      expect(result).toMatchObject({
        path: input.path,
        revision: "head",
        lines: [3_153],
        truncated: false,
        context: {
          path: input.path,
          revision: "head",
          startLine: 3_143,
          totalLines: 3_154,
          content: `${"unrelated\n".repeat(10)}const getUsage(value) = value; getUsage(value)\nGETUSAGE(value)`,
        },
      });

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
      expect(absent.context).toBeUndefined();
      expect((yield* ReviewLineMatches.fromText(input, "")).lines).toEqual([]);
    }),
  );

  it.effect(
    "returns complete lines around the first match and preserves locations outside that window",
    () =>
      Effect.gen(function* () {
        const source = Array.from({ length: 90 }, (_, index) =>
          index === 19 || index === 79
            ? `getUsage(${String(index + 1)})`
            : `line ${String(index + 1)}`,
        );

        const result = yield* ReviewLineMatches.fromText(input, `${source.join("\n")}\n`);

        expect(result.lines).toEqual([20, 80]);
        expect(result.truncated).toBe(false);
        expect(result.context).toMatchObject({
          startLine: 10,
          totalLines: 90,
          content: source.slice(9, 60).join("\n"),
        });

        const first = yield* ReviewLineMatches.fromText(input, "getUsage(1)\n\n");

        expect(first.context).toMatchObject({
          startLine: 1,
          totalLines: 2,
          content: "getUsage(1)\n",
        });

        const legacy = yield* Schema.decodeUnknownEffect(ReviewLineMatches)({
          path: input.path,
          revision: input.revision,
          lines: [20],
          truncated: false,
        });

        expect(legacy.context).toBeUndefined();
      }),
  );

  it.effect.each(["界", "\u0001"])(
    "bounds the encoded response while retaining whole matching lines: %s",
    (character) =>
      Effect.gen(function* () {
        const source = Array.from({ length: 90 }, (_, index) =>
          index === 19 ? "getUsage(value)" : character.repeat(400),
        );

        const result = yield* ReviewLineMatches.fromText(input, source.join("\n"));
        const encoded = yield* Schema.encodeEffect(ReviewLineMatches)(result);

        expect(new TextEncoder().encode(JSON.stringify(encoded)).byteLength).toBeLessThanOrEqual(
          8_192,
        );
        expect(result.lines).toEqual([20]);
        expect(result.truncated).toBe(false);
        expect(result.context).toBeDefined();
        if (result.context !== undefined) {
          const delivered = result.context.content.split("\n");

          expect(delivered.length).toBeLessThanOrEqual(51);
          expect(delivered).toContain("getUsage(value)");
          expect(delivered).toEqual(
            source.slice(
              result.context.startLine - 1,
              result.context.startLine - 1 + delivered.length,
            ),
          );
        }
      }),
  );

  it.effect("keeps locations for oversized matches and useful context beside oversized lines", () =>
    Effect.gen(function* () {
      const huge = "x".repeat(1_500_000);

      const oversized = yield* ReviewLineMatches.fromText(
        input,
        `getUsage(${huge})\ngetUsage(2)\n`,
      );

      expect(oversized.lines).toEqual([1, 2]);
      expect(oversized.context).toBeUndefined();
      expect(oversized.truncated).toBe(false);

      const neighboring = yield* ReviewLineMatches.fromText(input, `${huge}\ngetUsage(2)\nlast\n`);

      expect(neighboring.context).toMatchObject({ startLine: 2, content: "getUsage(2)" });
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
