import { normalizeWorkspacePath, type WorkspacePath } from "@effect-agent/example-pr-work-orders";
import { Effect } from "effect";

import { PublisherVerificationFailure } from "./contracts.ts";

const reject = (detail: string) =>
  PublisherVerificationFailure.make({ reason: "path-not-allowed", detail });

export const completeModifiedPaths = Effect.fn("completeModifiedPaths")(function* (
  patch: string,
): Effect.fn.Return<ReadonlyArray<WorkspacePath>, PublisherVerificationFailure> {
  if (patch.length === 0 || patch.length > 1_000_000) {
    return yield* reject("publisher requires one bounded non-empty patch");
  }
  const paths = new Set<WorkspacePath>();
  let current:
    | {
        source: string;
        destination: string;
        sawSourceHeader: boolean;
        sawDestinationHeader: boolean;
        sawHunk: boolean;
      }
    | undefined;
  let files = 0;
  const flush = Effect.fn("completeModifiedPaths.flush")(function* () {
    if (current === undefined) return;
    if (
      !current.sawSourceHeader ||
      !current.sawDestinationHeader ||
      !current.sawHunk ||
      current.source !== current.destination
    ) {
      return yield* reject("publisher accepts only complete same-path text modifications");
    }
    paths.add(
      yield* normalizeWorkspacePath(current.source).pipe(
        Effect.mapError(() => reject("patch contains a non-normalized repository path")),
      ),
    );
    files += 1;
    current = undefined;
  });
  for (const line of patch.split("\n")) {
    if (
      line.startsWith("rename ") ||
      line.startsWith("copy ") ||
      line.startsWith("new file mode ") ||
      line.startsWith("deleted file mode ") ||
      line.startsWith("old mode ") ||
      line.startsWith("new mode ") ||
      line.startsWith("Binary files ") ||
      line === "GIT binary patch"
    ) {
      return yield* reject(
        "renames, copies, mode changes, additions, deletions, and binary patches are unsupported",
      );
    }
    if (line.startsWith("diff --git ")) {
      yield* flush();
      const matched = /^diff --git a\/(.+) b\/(.+)$/.exec(line);
      if (matched?.[1] === undefined || matched[2] === undefined || matched[1] !== matched[2]) {
        return yield* reject("publisher could not prove the diff header path pair");
      }
      current = {
        source: matched[1],
        destination: matched[2],
        sawSourceHeader: false,
        sawDestinationHeader: false,
        sawHunk: false,
      };
      continue;
    }
    if (line.startsWith("@@ ")) {
      if (current === undefined || !current.sawSourceHeader || !current.sawDestinationHeader) {
        return yield* reject("publisher found a patch hunk before its complete path headers");
      }
      current.sawHunk = true;
      continue;
    }
    if (current?.sawHunk === true) continue;
    if (line.startsWith("--- ")) {
      if (
        current === undefined ||
        current.sawSourceHeader ||
        current.sawDestinationHeader ||
        line === "--- /dev/null"
      ) {
        return yield* reject("publisher could not prove the patch source path");
      }
      const source = line.slice(6).split("\t", 1)[0];
      if (source === undefined || source !== current.source) {
        return yield* reject("patch source header differs from its diff header");
      }
      current.sawSourceHeader = true;
      continue;
    }
    if (line.startsWith("+++ ")) {
      if (
        current === undefined ||
        !current.sawSourceHeader ||
        current.sawDestinationHeader ||
        line === "+++ /dev/null"
      ) {
        return yield* reject("publisher could not prove the patch destination path");
      }
      const destination = line.slice(6).split("\t", 1)[0];
      if (destination === undefined || destination !== current.destination) {
        return yield* reject("patch destination header differs from its diff header");
      }
      current.sawDestinationHeader = true;
    }
  }
  yield* flush();
  if (files === 0 || paths.size !== files) {
    return yield* reject("publisher could not prove one complete path pair per modified file");
  }
  return [...paths].sort();
});
