import { Effect, Layer } from "effect";

import { PullRequestMetadata, PullRequestSource, ReviewInputViolation } from "./source.ts";

// ---------------------------------------------------------------------------
// Configured ignore globs, applied at the source port. Ignored files are
// removed from the reviewer's entire observation surface — the changeset
// list, diffs, and head reads — so the model never spends budget on them and
// can never anchor a finding to them. Filtering fails closed: reading an
// ignored path is a ReviewInputViolation, exactly like a path outside the
// changeset.
// ---------------------------------------------------------------------------

const REGEX_SPECIALS = /[.+^${}()|[\]\\]/g;

// Placeholders for the directory-crossing wildcard while single-segment
// wildcards are rewritten; NUL/SOH cannot appear in a valid repository path.
const CROSSING_SLASH = "\u0000";
const CROSSING = "\u0001";

/**
 * The supported glob vocabulary is deliberately minimal: `**` crosses
 * directory separators, `*` and `?` stay within one path segment, everything
 * else is literal. Every string compiles — there is no invalid pattern.
 */
const globToRegExpSource = (pattern: string): string =>
  pattern
    .replace(REGEX_SPECIALS, String.raw`\$&`)
    .replaceAll("**/", CROSSING_SLASH)
    .replaceAll("**", CROSSING)
    .replaceAll("*", "[^/]*")
    .replaceAll("?", "[^/]")
    .replaceAll(CROSSING_SLASH, "(?:.*/)?")
    .replaceAll(CROSSING, ".*");

/** Compile ignore globs into one predicate over repository-relative paths. */
export const compileIgnoreGlobs = (
  patterns: ReadonlyArray<string>,
): ((path: string) => boolean) => {
  if (patterns.length === 0) return () => false;
  const expressions = patterns.map((pattern) => new RegExp(`^(?:${globToRegExpSource(pattern)})$`));
  return (path) => expressions.some((expression) => expression.test(path));
};

/**
 * Decorate the ambient PullRequestSource with configured ignore globs. The
 * resulting Layer requires the undecorated source, so callers provide their
 * real adapter beneath it. Metadata's changed-file total is reduced by the
 * ignored count: from the reviewer's perspective the ignored files do not
 * exist, and truncation reporting stays about the reviewer's own bound.
 */
export const ignoringPullRequestSourceLayer = (
  patterns: ReadonlyArray<string>,
): Layer.Layer<PullRequestSource, never, PullRequestSource> =>
  Layer.effect(PullRequestSource)(
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      const ignored = compileIgnoreGlobs(patterns);
      const changedFiles = source.changedFiles.pipe(
        Effect.map((files) => files.filter((file) => !ignored(file.path))),
      );
      const anchorFiles = source.anchorFiles.pipe(
        Effect.map((files) => files.filter((file) => !ignored(file.path))),
      );
      const metadata = Effect.gen(function* () {
        const [meta, files] = yield* Effect.all([source.metadata, source.anchorFiles]);
        const ignoredCount = files.filter((file) => ignored(file.path)).length;
        return PullRequestMetadata.make({
          ...meta,
          totalChangedFiles: Math.max(0, meta.totalChangedFiles - ignoredCount),
        });
      });
      return PullRequestSource.of({
        metadata,
        changedFiles,
        anchorFiles,
        readFile: (path) =>
          ignored(path)
            ? Effect.fail(
                ReviewInputViolation.make({
                  input: path,
                  reason: "Path is excluded from this review by configuration.",
                }),
              )
            : source.readFile(path),
      });
    }),
  );
