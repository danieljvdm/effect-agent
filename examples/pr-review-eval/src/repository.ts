import {
  ReviewContextError,
  ReviewFileList,
  ReviewFileMatches,
  ReviewRepository,
  ReviewSource,
} from "@effect-agent/pr-review";
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
        const files = snapshot?.files ?? [];
        const fileIndex = files.findIndex(
          (candidate) => candidate.path === path && candidate.revision === revision,
        );
        const file = files[fileIndex];
        const access =
          file === undefined
            ? Effect.fail(missing(path, revision))
            : ReviewSource.fromText(input, file.content);
        const log = (outcome: "success" | "failure") =>
          Effect.logDebug("Eval repository source access").pipe(
            Effect.annotateLogs({
              evalRepositoryOperation: "read_file",
              evalRepositoryFileIndex: fileIndex,
              evalRepositoryRevision: revision,
              evalRepositoryStartLine: input.startLine,
              evalRepositoryLineCount: input.lineCount,
              evalRepositoryOutcome: outcome,
            }),
          );
        return yield* access.pipe(
          Effect.tap(() => log("success")),
          Effect.tapError(() => log("failure")),
        );
      }),
      searchFile: Effect.fn("EvalRepository.searchFile")(function* (
        input: Parameters<ReviewRepository["Service"]["searchFile"]>[0],
      ) {
        const files = snapshot?.files ?? [];
        const fileIndex = files.findIndex(
          (candidate) => candidate.path === input.path && candidate.revision === input.revision,
        );
        const file = files[fileIndex];
        const access =
          file === undefined
            ? Effect.fail(missing(input.path, input.revision))
            : ReviewFileMatches.fromText(input, file.content);
        const log = (outcome: "success" | "failure") =>
          Effect.logDebug("Eval repository source access").pipe(
            Effect.annotateLogs({
              evalRepositoryOperation: "search_file",
              evalRepositoryFileIndex: fileIndex,
              evalRepositoryRevision: input.revision,
              evalRepositoryStartLine: input.startLine,
              evalRepositoryQueryLength: input.query.length,
              evalRepositoryOutcome: outcome,
            }),
          );
        return yield* access.pipe(
          Effect.tap(() => log("success")),
          Effect.tapError(() => log("failure")),
        );
      }),
      findFiles: ({ query, revision }) => {
        const paths = [
          ...new Set(
            (snapshot?.files ?? [])
              .filter((file) => file.revision === revision && file.path.includes(query))
              .map((file) => file.path),
          ),
        ].sort();
        const result = ReviewFileList.make({
          paths: paths.slice(0, 100),
          truncated: paths.length > 100,
        });
        return Effect.logDebug("Eval repository source access").pipe(
          Effect.annotateLogs({
            evalRepositoryOperation: "find_files",
            evalRepositoryRevision: revision,
            evalRepositoryQueryLength: query.length,
            evalRepositoryMatchCount: result.paths.length,
            evalRepositoryTruncated: result.truncated,
            evalRepositoryOutcome: "success",
          }),
          Effect.as(result),
        );
      },
    }),
  );
