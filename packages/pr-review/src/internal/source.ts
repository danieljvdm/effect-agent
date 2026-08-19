import { Context, Effect, Schema } from "effect";

import type { ChangedFile } from "./diff.ts";

// ---------------------------------------------------------------------------
// The pull-request source port: everything the review tools may observe about
// one pull request. The live adapter speaks the GitHub REST API; the fixture
// adapter (testing entry) serves an in-memory pull request so ordinary gates
// and dry runs need no network or credential.
// ---------------------------------------------------------------------------

/** Reading a file head version larger than this is refused, never truncated silently. */
export const MAX_FILE_CHARS = 200_000;

/** The changeset surface is bounded; larger pull requests fail typed. */
export const MAX_CHANGED_FILES = 300;

/** Pull-request identity and framing shown to the agent as its mission. */
export class PullRequestMetadata extends Schema.Class<PullRequestMetadata>(
  "@effect-agent/pr-review/PullRequestMetadata",
)({
  /** `owner/name`, exactly as GitHub renders it. */
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  number: Schema.Int.check(Schema.isGreaterThan(0)),
  title: Schema.String.check(Schema.isMaxLength(400)),
  /** Author-provided description; empty when the author left none. */
  body: Schema.String.check(Schema.isMaxLength(20_000)),
  baseRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  /** Exact base commit used to validate persisted incremental-review lineage. */
  baseSha: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(64))),
  headRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  headSha: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  /** GitHub's own changed-file total; may exceed what `changedFiles` returns. */
  totalChangedFiles: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

/** The upstream source failed: API error, network fault, or malformed payload. */
export class PullRequestSourceFailure extends Schema.TaggedError<PullRequestSourceFailure>()(
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
export class ReviewInputViolation extends Schema.TaggedError<ReviewInputViolation>()(
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
    /** Files exposed to the model for this run (full PR or selected delta). */
    readonly changedFiles: Effect.Effect<ReadonlyArray<ChangedFile>, PullRequestSourceFailure>;
    /** Full current PR diff used only for host-side anchor/state validation. */
    readonly anchorFiles: Effect.Effect<ReadonlyArray<ChangedFile>, PullRequestSourceFailure>;
    /**
     * The head-version content of one CHANGED file. Paths outside the
     * changeset are violations: the reviewer reads the change, not the tree.
     */
    readonly readFile: (
      path: string,
    ) => Effect.Effect<string, PullRequestSourceFailure | ReviewInputViolation>;
  }
>()("@effect-agent/pr-review/PullRequestSource") {}
