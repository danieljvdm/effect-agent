import {
  ReviewContextError,
  ReviewFileList,
  ReviewRepository,
  ReviewSearchMatch,
  ReviewSearchResult,
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
      searchCode: Effect.fn("EvalRepository.searchCode")(function* (
        input: Parameters<ReviewRepository["Service"]["searchCode"]>[0],
      ) {
        if (snapshot === undefined) {
          return yield* ReviewContextError.make({
            message: "No frozen repository snapshot is available for this case.",
          });
        }

        const files = snapshot.files
          .filter(
            (file) =>
              file.revision === input.revision &&
              file.path.length <= 512 &&
              file.path.includes(input.path),
          )
          .sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));

        const page = files.slice(input.cursor, input.cursor + 20);
        const matches: Array<ReviewSearchMatch> = [];
        let truncated = false;

        for (const file of page) {
          let matchedLines = 0;

          for (const [lineIndex, line] of file.content.split("\n").entries()) {
            const position = line.indexOf(input.query);

            if (position < 0) continue;
            if (matchedLines === 5) {
              truncated = true;
              break;
            }
            matches.push(
              ReviewSearchMatch.make({
                path: file.path,
                line: lineIndex + 1,
                content: line.slice(position, position + 200),
              }),
            );
            matchedLines += 1;
          }
        }
        const nextCursor = input.cursor + page.length;

        return ReviewSearchResult.make({
          matches,
          ...(nextCursor < files.length ? { nextCursor } : {}),
          truncated,
          // Every authorized frozen entry already contains its complete decoded source.
          unreadablePaths: [],
        });
      }),
    }),
  );
