import { Context, Effect, Layer, Schema } from "effect";

import { ChangedFile } from "./diff.ts";

// ---------------------------------------------------------------------------
// The pull-request source port: everything the review tools may observe about
// one pull request. The live adapter speaks the GitHub REST API; the fixture
// adapter serves an in-memory pull request so ordinary gates and dry runs
// need no network or credential.
// ---------------------------------------------------------------------------

/** Reading a file head version larger than this is refused, never truncated silently. */
export const MAX_FILE_CHARS = 200_000;

/** The changeset surface is bounded; larger pull requests fail typed. */
export const MAX_CHANGED_FILES = 300;

/** Pull-request identity and framing shown to the agent as its mission. */
export class PullRequestMetadata extends Schema.Class<PullRequestMetadata>(
  "@effect-agent/example-pr-review/PullRequestMetadata",
)({
  /** `owner/name`, exactly as GitHub renders it. */
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  number: Schema.Int.check(Schema.isGreaterThan(0)),
  title: Schema.String.check(Schema.isMaxLength(400)),
  /** Author-provided description; empty when the author left none. */
  body: Schema.String.check(Schema.isMaxLength(20_000)),
  baseRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  headRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  headSha: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  /** GitHub's own changed-file total; may exceed what `changedFiles` returns. */
  totalChangedFiles: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

/** The upstream source failed: API error, network fault, or malformed payload. */
export class PullRequestSourceFailure extends Schema.TaggedErrorClass<PullRequestSourceFailure>()(
  "PullRequestSourceFailure",
  {
    operation: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `Pull-request source operation '${this.operation}' failed: ${this.reason}`;
  }
}

/** A model-supplied path or range was invalid; always fail-closed (SEC-007). */
export class ReviewInputViolation extends Schema.TaggedErrorClass<ReviewInputViolation>()(
  "ReviewInputViolation",
  {
    input: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `Rejected review input '${this.input}': ${this.reason}`;
  }
}

const BACKSLASH = String.fromCharCode(92);

/**
 * Normalize and validate one model-supplied repository-relative path.
 * Absolute paths, drive letters, backslashes, empty segments, `.` and `..`
 * segments are all violations — never silently fixed. The changeset list is
 * the real allowlist; this check is defense in depth for URL construction.
 */
export const normalizeRepoRelativePath = (
  path: string,
): Effect.Effect<string, ReviewInputViolation> => {
  const fail = (reason: string) => Effect.fail(ReviewInputViolation.make({ input: path, reason }));
  if (path.length === 0 || path.length > 512) {
    return fail("Path length is out of bounds.");
  }
  if (path.includes(BACKSLASH)) {
    return fail("Path contains a forbidden backslash.");
  }
  if (path.startsWith("/") || /^[A-Za-z]:/.test(path)) {
    return fail("Path must be repository-relative, not absolute.");
  }
  const segments = path.split("/");
  for (const segment of segments) {
    if (segment === "" || segment === "." || segment === "..") {
      return fail("Path segments must not be empty, '.', or '..'.");
    }
  }
  return Effect.succeed(segments.join("/"));
};

/** Read-only view of one pull request; the only repository access tools get. */
export class PullRequestSource extends Context.Service<
  PullRequestSource,
  {
    readonly metadata: Effect.Effect<PullRequestMetadata, PullRequestSourceFailure>;
    readonly changedFiles: Effect.Effect<ReadonlyArray<ChangedFile>, PullRequestSourceFailure>;
    /**
     * The head-version content of one CHANGED file. Paths outside the
     * changeset are violations: the reviewer reads the change, not the tree.
     */
    readonly readFile: (
      path: string,
    ) => Effect.Effect<string, PullRequestSourceFailure | ReviewInputViolation>;
  }
>()("@effect-agent/example-pr-review/PullRequestSource") {}

/** One fixture file: its changeset entry plus optional head content. */
export class FixtureFile extends Schema.Class<FixtureFile>(
  "@effect-agent/example-pr-review/FixtureFile",
)({
  file: ChangedFile,
  headContent: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(MAX_FILE_CHARS))),
}) {}

/** A complete in-memory pull request for tests, dry runs, and live smokes. */
export class FixturePullRequest extends Schema.Class<FixturePullRequest>(
  "@effect-agent/example-pr-review/FixturePullRequest",
)({
  metadata: PullRequestMetadata,
  files: Schema.Array(FixtureFile).check(Schema.isMaxLength(MAX_CHANGED_FILES)),
}) {}

const requireChanged = (
  fixture: FixturePullRequest,
  path: string,
): Effect.Effect<FixtureFile, ReviewInputViolation> => {
  const entry = fixture.files.find((candidate) => candidate.file.path === path);
  return entry === undefined
    ? Effect.fail(
        ReviewInputViolation.make({
          input: path,
          reason: "Path is not part of this pull request's changeset.",
        }),
      )
    : Effect.succeed(entry);
};

/** Deterministic `PullRequestSource` over one fixture pull request. */
export const fixturePullRequestSourceLayer = (
  fixture: FixturePullRequest,
): Layer.Layer<PullRequestSource> =>
  Layer.succeed(PullRequestSource)(
    PullRequestSource.of({
      metadata: Effect.succeed(fixture.metadata),
      changedFiles: Effect.succeed(fixture.files.map((entry) => entry.file)),
      readFile: (path) =>
        Effect.gen(function* () {
          const relative = yield* normalizeRepoRelativePath(path);
          const entry = yield* requireChanged(fixture, relative);
          if (entry.headContent === undefined) {
            return yield* ReviewInputViolation.make({
              input: relative,
              reason: "No head content is available for this file.",
            });
          }
          return entry.headContent;
        }),
    }),
  );
