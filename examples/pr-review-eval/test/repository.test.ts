import { ReviewRepository } from "@effect-agent/pr-review";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Effect } from "effect";

import { EvalInputDigest, EvalRepositoryFile, EvalRepositorySnapshot } from "../src/contracts.ts";
import { digestRepositorySnapshot } from "../src/corpus.ts";
import { repositoryLayer } from "../src/repository.ts";

layer(NodeCrypto.layer)("frozen source lookup", (it) => {
  it.effect("locates and reads the exact frozen revision, without searching unfrozen source", () =>
    Effect.gen(function* () {
      const files = [
        EvalRepositoryFile.make({
          path: "vendor/provider.ts",
          revision: "base",
          content: "const usage = 0;\n",
        }),
        EvalRepositoryFile.make({
          path: "vendor/provider.ts",
          revision: "head",
          content: `${"unrelated\n".repeat(3_152)}const usage = 1;\n`,
        }),
      ];

      const unsigned = EvalRepositorySnapshot.make({
        version: 1,
        files,
        digest: EvalInputDigest.make("0".repeat(64)),
      });

      const snapshot = EvalRepositorySnapshot.make({
        ...unsigned,
        digest: yield* digestRepositorySnapshot(unsigned),
      });

      yield* Effect.gen(function* () {
        const repository = yield* ReviewRepository;

        const input = {
          path: "vendor/provider.ts",
          revision: "head",
          literal: "usage",
          startLine: 1,
        } as const;

        const base = yield* repository.findInFile({ ...input, revision: "base" });
        const head = yield* repository.findInFile(input);
        const absent = yield* repository.findInFile({ ...input, literal: "Usage" });

        const missing = yield* repository
          .findInFile({ ...input, path: "unfrozen.ts" })
          .pipe(Effect.flip);

        const source = yield* repository.readFile({ ...input, startLine: 3_153, lineCount: 1 });

        expect(base).toMatchObject({ revision: "base", lines: [1], truncated: false });
        expect(head).toMatchObject({ revision: "head", lines: [3_153], truncated: false });
        expect(absent.lines).toEqual([]);
        expect(missing.message).toBe("No frozen head source exists for unfrozen.ts");
        expect(source.content).toBe("const usage = 1;");
      }).pipe(Effect.provide(repositoryLayer(snapshot)));
    }),
  );
});
