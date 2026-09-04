import {
  ReviewContextError,
  ReviewFileList,
  ReviewLineMatches,
  ReviewRepository,
  ReviewSource,
} from "@effect-agent/pr-review/ReviewRepository";
import { Effect, Layer } from "effect";

import { type EvalRepositorySnapshot } from "./contracts.ts";

const missing = (path: string, revision: "base" | "head") =>
  ReviewContextError.make({ message: `No frozen ${revision} source exists for ${path}` });

/** Bind one immutable, digest-checked eval snapshot to the reviewer source tools. */
export const repositoryLayer = (snapshot: EvalRepositorySnapshot | undefined) =>
  Layer.succeed(
    ReviewRepository,
    ReviewRepository.of({
      readFile: Effect.fn("EvalRepository.readFile")(function* (
        input: Parameters<ReviewRepository["Service"]["readFile"]>[0],
      ) {
        const { path, revision } = input;

        const file = snapshot?.files.find(
          (candidate) => candidate.path === path && candidate.revision === revision,
        );

        if (file === undefined) return yield* missing(path, revision);

        return yield* ReviewSource.fromText(input, file.content);
      }),
      findFiles: ({ query, revision }) => {
        const paths = [
          ...new Set(
            (snapshot?.files ?? [])
              .filter((file) => file.revision === revision && file.path.includes(query))
              .map((file) => file.path),
          ),
        ].sort();

        return Effect.succeed(
          ReviewFileList.make({ paths: paths.slice(0, 100), truncated: paths.length > 100 }),
        );
      },
      findInFile: Effect.fn("EvalRepository.findInFile")(function* (
        input: Parameters<ReviewRepository["Service"]["findInFile"]>[0],
      ) {
        const file = snapshot?.files.find(
          (candidate) => candidate.path === input.path && candidate.revision === input.revision,
        );

        if (file === undefined) return yield* missing(input.path, input.revision);

        return yield* ReviewLineMatches.fromText(input, file.content);
      }),
    }),
  );
