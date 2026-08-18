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
        hunk:
          | {
              oldRemaining: number;
              newRemaining: number;
              allowNoNewlineMarker: boolean;
            }
          | undefined;
      }
    | undefined;
  let files = 0;
  const flush = Effect.fn("completeModifiedPaths.flush")(function* () {
    if (current === undefined) return;
    if (
      !current.sawSourceHeader ||
      !current.sawDestinationHeader ||
      !current.sawHunk ||
      (current.hunk !== undefined &&
        (current.hunk.oldRemaining !== 0 || current.hunk.newRemaining !== 0)) ||
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
        hunk: undefined,
      };
      continue;
    }
    if (current?.hunk !== undefined) {
      const hunk = current.hunk;
      if (hunk.oldRemaining === 0 && hunk.newRemaining === 0) {
        if (line === "\\ No newline at end of file") {
          if (!hunk.allowNoNewlineMarker) {
            return yield* reject("publisher found an unbound no-newline marker");
          }
          current.hunk = undefined;
          continue;
        }
        current.hunk = undefined;
      } else {
        if (line === "\\ No newline at end of file") {
          if (!hunk.allowNoNewlineMarker) {
            return yield* reject("publisher found an unbound no-newline marker");
          }
          hunk.allowNoNewlineMarker = false;
          continue;
        }
        if (line.startsWith(" ")) {
          hunk.oldRemaining -= 1;
          hunk.newRemaining -= 1;
        } else if (line.startsWith("-")) {
          hunk.oldRemaining -= 1;
        } else if (line.startsWith("+")) {
          hunk.newRemaining -= 1;
        } else {
          return yield* reject("publisher found malformed or incomplete patch hunk content");
        }
        if (hunk.oldRemaining < 0 || hunk.newRemaining < 0) {
          return yield* reject("patch hunk content exceeds its declared line counts");
        }
        hunk.allowNoNewlineMarker = true;
        continue;
      }
    }
    if (line.startsWith("@@ ")) {
      if (current === undefined || !current.sawSourceHeader || !current.sawDestinationHeader) {
        return yield* reject("publisher found a patch hunk before its complete path headers");
      }
      const matched = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$/.exec(line);
      const oldCount = Number(matched?.[2] ?? (matched === null ? Number.NaN : 1));
      const newCount = Number(matched?.[4] ?? (matched === null ? Number.NaN : 1));
      if (
        !Number.isSafeInteger(oldCount) ||
        !Number.isSafeInteger(newCount) ||
        oldCount < 0 ||
        newCount < 0 ||
        oldCount > 1_000_000 ||
        newCount > 1_000_000 ||
        (oldCount === 0 && newCount === 0)
      ) {
        return yield* reject("publisher could not prove the patch hunk line counts");
      }
      current.sawHunk = true;
      current.hunk = {
        oldRemaining: oldCount,
        newRemaining: newCount,
        allowNoNewlineMarker: false,
      };
      continue;
    }
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
      continue;
    }
    if (current?.sawHunk === true && line.length > 0) {
      return yield* reject("publisher found unproven trailing patch structure after a hunk");
    }
  }
  yield* flush();
  if (files === 0 || paths.size !== files) {
    return yield* reject("publisher could not prove one complete path pair per modified file");
  }
  return [...paths].sort();
});
