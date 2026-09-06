import { ReviewRepository } from "@effect-agent/pr-review/ReviewRepository";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Result, Schema } from "effect";

import { EvalInputDigest, EvalRepositoryFile, EvalRepositorySnapshot } from "../src/contracts.ts";
import { repositoryLayer } from "../src/repository.ts";

describe("frozen review source", () => {
  it.effect(
    "searches literal source with the same page and snippet bounds as the live adapter",
    () =>
      Effect.gen(function* () {
        const files = Array.from({ length: 21 }, (_, index) =>
          EvalRepositoryFile.make({
            path: `src/file-${String(index).padStart(2, "0")}.ts`,
            revision: "head",
            content:
              index === 0
                ? Array.from({ length: 6 }, () => `prefix needle.* ${"x".repeat(250)}`).join("\n")
                : "needleABC does not match\nprefix needle.* match",
          }),
        );

        const snapshot = EvalRepositorySnapshot.make({
          version: 1,
          digest: Schema.decodeSync(EvalInputDigest)("0".repeat(64)),
          files: [
            ...files.toReversed(),
            EvalRepositoryFile.make({
              path: "other/file.ts",
              revision: "head",
              content: "needle.*",
            }),
            EvalRepositoryFile.make({
              path: "src/file-00.ts",
              revision: "base",
              content: "base needle.*",
            }),
          ],
        });

        yield* Effect.gen(function* () {
          const repository = yield* ReviewRepository;
          const input = { query: "needle.*", path: "src/", revision: "head", cursor: 0 } as const;
          const first = yield* repository.searchCode(input);

          expect(first.nextCursor).toBe(20);
          expect(first.truncated).toBe(true);
          expect(first.unreadablePaths).toEqual([]);
          expect(first.matches).toHaveLength(24);
          expect(first.matches.slice(0, 5).map((match) => match.line)).toEqual([1, 2, 3, 4, 5]);
          expect(first.matches[0]).toEqual({
            path: "src/file-00.ts",
            line: 1,
            content: `needle.* ${"x".repeat(191)}`,
          });
          expect(first.matches.at(-1)?.path).toBe("src/file-19.ts");

          const last = yield* repository.searchCode({ ...input, cursor: 20 });

          expect(last).toEqual({
            matches: [{ path: "src/file-20.ts", line: 2, content: "needle.* match" }],
            truncated: false,
            unreadablePaths: [],
          });
          const base = yield* repository.searchCode({ ...input, revision: "base" });

          expect(base.matches).toEqual([{ path: "src/file-00.ts", line: 1, content: "needle.*" }]);
          expect(base.nextCursor).toBeUndefined();
          expect((yield* repository.searchCode({ ...input, query: "NEEDLE.*" })).matches).toEqual(
            [],
          );
        }).pipe(Effect.provide(repositoryLayer(snapshot)));
      }),
  );

  it.effect("does not claim source absence when the case has no frozen snapshot", () =>
    Effect.gen(function* () {
      const repository = yield* ReviewRepository;

      const result = yield* repository
        .searchCode({ query: "caller", path: "", revision: "head", cursor: 0 })
        .pipe(Effect.result);

      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) expect(result.failure._tag).toBe("ReviewContextError");
    }).pipe(Effect.provide(repositoryLayer(undefined))),
  );
});
