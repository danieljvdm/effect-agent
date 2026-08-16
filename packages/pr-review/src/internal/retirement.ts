import { Context, DateTime, Effect, Option, Schema } from "effect";

import {
  ReviewStateAuthenticator,
  type ReviewState,
  type StoredReviewFinding,
} from "./review-state.ts";

// ---------------------------------------------------------------------------
// Review retirement. GitHub mutations stay behind ReviewRetirementHost; this
// module owns only deterministic identity matching, body rewriting, and the
// fail-open orchestration that turns stale reviews into quiet history.
// ---------------------------------------------------------------------------

const PositiveLine = Schema.Int.check(Schema.isGreaterThan(0));

/** One previously posted review as observed through the retirement host. */
export class RetirableReview extends Schema.Class<RetirableReview>(
  "@effect-agent/pr-review/RetirableReview",
)({
  reviewId: Schema.Int.check(Schema.isGreaterThan(0)),
  body: Schema.String.check(Schema.isMaxLength(60_000)),
  commitSha: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  authorNodeId: Schema.NullOr(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  submittedAt: Schema.NullOr(Schema.DateTimeUtc),
}) {}

/** One inline comment attached to a previously posted review. */
export class RetirableReviewComment extends Schema.Class<RetirableReviewComment>(
  "@effect-agent/pr-review/RetirableReviewComment",
)({
  nodeId: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  path: Schema.NonEmptyString.check(Schema.isMaxLength(500)),
  startLine: Schema.NullOr(PositiveLine),
  endLine: Schema.NullOr(PositiveLine),
  body: Schema.String.check(Schema.isMaxLength(65_536)),
}) {}

/** A GitHub retirement read or mutation failed. */
export class ReviewRetirementFailure extends Schema.TaggedError<ReviewRetirementFailure>()(
  "ReviewRetirementFailure",
  {
    operation: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `Review retirement operation '${this.operation}' failed: ${this.reason}`;
  }
}

/**
 * Host-side GitHub operations used by retirement. Domain code never reaches
 * into REST or GraphQL directly, and deterministic tests substitute this port.
 */
export class ReviewRetirementHost extends Context.Service<
  ReviewRetirementHost,
  {
    readonly listReviews: Effect.Effect<ReadonlyArray<RetirableReview>, ReviewRetirementFailure>;
    readonly listComments: (
      reviewId: number,
    ) => Effect.Effect<ReadonlyArray<RetirableReviewComment>, ReviewRetirementFailure>;
    readonly updateBody: (
      reviewId: number,
      body: string,
    ) => Effect.Effect<void, ReviewRetirementFailure>;
    readonly minimizeComment: (nodeId: string) => Effect.Effect<void, ReviewRetirementFailure>;
  }
>()("@effect-agent/pr-review/ReviewRetirementHost") {}

/** Observable cosmetic work completed by one fail-open retirement pass. */
export class ReviewRetirementReport extends Schema.Class<ReviewRetirementReport>(
  "@effect-agent/pr-review/ReviewRetirementReport",
)({
  reviewsRetired: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  findingsResolved: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  commentsMinimized: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  failures: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

export interface ReviewRetirementInput {
  readonly currentReviewId: number;
  readonly currentReviewUrl: string;
  readonly currentAuthorNodeId: string;
  readonly currentSubmittedAt: DateTime.Utc;
  readonly currentState: ReviewState;
}

export interface ReviewRetirementDecision {
  readonly body: string;
  readonly resolvedFindings: ReadonlyArray<StoredReviewFinding>;
  readonly priorFindingCount: number;
}

const findingIdentity = (finding: {
  readonly path: string;
  readonly startLine: number;
  readonly endLine: number;
  readonly title: string;
}): string =>
  `${finding.path}\u0000${finding.startLine}\u0000${finding.endLine}\u0000${finding.title}`;

const REVIEW_METADATA_PATTERN = /<!-- effect-agent-pr-review metadata\n[\s\S]*?\n-->/g;
const FINGERPRINT_PATTERN = /<!-- effect-agent-pr-review fingerprint=sha256:[0-9a-f]{64} -->/g;
const STATE_PATTERN =
  /<!-- effect-agent-pr-review state-v1:[A-Za-z0-9+/]+={0,2}\.[0-9a-f]{64} -->/g;
const RETIRED_ORIGINAL_PATTERN =
  /<!-- effect-agent-pr-review retired-original:start -->\n([\s\S]*?)\n<!-- effect-agent-pr-review retired-original:end -->/;
const MACHINE_COMMENT_PATTERN = new RegExp(
  `${REVIEW_METADATA_PATTERN.source}|${FINGERPRINT_PATTERN.source}|${STATE_PATTERN.source}`,
  "g",
);
const VERDICT_CALLOUT_PATTERN =
  /^(?:> \[!(?:CAUTION|IMPORTANT)\]\n> [^\n]*(?:\n> [^\n]*)*|> (?:ℹ️|✅)[^\n]*)\n*/;
// Accepts both the pre-category first line (`**[⚠️ important] Title**`) and
// the current one carrying an optional category chip (`… · security]`), so
// retirement keeps matching inline comments posted by older package versions.
const INLINE_FINDING_TITLE_PATTERN =
  /^\*\*\[(?:🛑 blocking|⚠️ important|💅 nit)(?: · [a-z-]+)?\] ([^\n]+)\*\*$/;
const MAX_REVIEW_BODY_CHARS = 60_000;

/** The host-authored metadata marker is the authority gate for any edit. */
export const hasReviewMetadataMarker = (body: string): boolean =>
  /<!-- effect-agent-pr-review metadata\n/.test(body);

const machineComments = (body: string): ReadonlyArray<string> =>
  Array.from(body.matchAll(MACHINE_COMMENT_PATTERN), (match) => match[0]);

const originalVisibleBody = (body: string): string => {
  const retired = RETIRED_ORIGINAL_PATTERN.exec(body)?.[1];
  if (retired !== undefined) return retired;
  return body.replace(MACHINE_COMMENT_PATTERN, "").trim().replace(VERDICT_CALLOUT_PATTERN, "");
};

const findingLocation = (finding: StoredReviewFinding): string =>
  `${finding.path}:${finding.startLine}${
    finding.endLine === finding.startLine ? "" : `-${finding.endLine}`
  }`;

const renderRetiredBody = (input: {
  readonly priorBody: string;
  readonly priorState: ReviewState;
  readonly currentState: ReviewState;
  readonly currentReviewUrl: string;
  readonly resolvedFindings: ReadonlyArray<StoredReviewFinding>;
}): string => {
  const shortSha = input.currentState.reviewedHeadSha.slice(0, 7);
  const comments = machineComments(input.priorBody);
  const original = originalVisibleBody(input.priorBody);
  const resolved =
    input.resolvedFindings.length === 0
      ? []
      : [
          "### Findings resolved by later review",
          "",
          ...input.resolvedFindings.map(
            (finding) =>
              `- \`${findingLocation(finding)}\` ~~${finding.title}~~ · resolved at \`${shortSha}\``,
          ),
          "",
        ];
  const prefix = [
    `> ℹ️ Superseded — ${input.resolvedFindings.length} of ${input.priorState.unresolvedFindings.length} findings resolved at \`${shortSha}\`; see [the latest review](${input.currentReviewUrl}).`,
    "",
    "<details>",
    "<summary>Previous review details</summary>",
    "",
    ...resolved,
    "<!-- effect-agent-pr-review retired-original:start -->",
  ];
  const suffix = [
    "<!-- effect-agent-pr-review retired-original:end -->",
    "",
    "</details>",
    ...(comments.length === 0 ? [] : ["", ...comments]),
  ];
  const render = (visible: string) => [...prefix, visible, ...suffix].join("\n");
  if (render(original).length <= MAX_REVIEW_BODY_CHARS) return render(original);
  const truncationNotice = "\n\n_Original review content truncated during retirement._";
  const budget = Math.max(0, MAX_REVIEW_BODY_CHARS - render(truncationNotice).length);
  return render(`${original.slice(0, budget)}${truncationNotice}`);
};

/** Compute one prior review's resolved subset and deterministic retired body. */
export const decideReviewRetirement = (input: {
  readonly priorBody: string;
  readonly priorState: ReviewState;
  readonly currentState: ReviewState;
  readonly currentReviewUrl: string;
}): ReviewRetirementDecision => {
  const current = new Set(input.currentState.unresolvedFindings.map(findingIdentity));
  const resolvedFindings = input.priorState.unresolvedFindings.filter(
    (finding) => !current.has(findingIdentity(finding)),
  );
  return {
    body: renderRetiredBody({ ...input, resolvedFindings }),
    resolvedFindings,
    priorFindingCount: input.priorState.unresolvedFindings.length,
  };
};

const inlineCommentIdentity = (comment: RetirableReviewComment): string | undefined => {
  if (comment.startLine === null || comment.endLine === null) return undefined;
  const firstLine = comment.body.split("\n", 1)[0] ?? "";
  const title = INLINE_FINDING_TITLE_PATTERN.exec(firstLine)?.[1];
  return title === undefined
    ? undefined
    : findingIdentity({
        path: comment.path,
        startLine: comment.startLine,
        endLine: comment.endLine,
        title,
      });
};

const failOpen = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  fallback: A,
  message: string,
): Effect.Effect<A, never, R> =>
  effect.pipe(
    Effect.catch((error) =>
      Effect.logWarning(`${message}: ${String(error)}`).pipe(Effect.as(fallback)),
    ),
  );

const isStrictlyOlderReview = (review: RetirableReview, input: ReviewRetirementInput): boolean => {
  if (review.submittedAt === null) return false;
  const submittedAt = DateTime.toEpochMillis(review.submittedAt);
  const currentSubmittedAt = DateTime.toEpochMillis(input.currentSubmittedAt);
  return (
    submittedAt < currentSubmittedAt ||
    (submittedAt === currentSubmittedAt && review.reviewId < input.currentReviewId)
  );
};

/**
 * Retire every marker-bearing prior review against the newest posted state.
 * Every lookup, edit, and minimization is isolated: retirement is cosmetic
 * and can never change the run or check outcome.
 */
export const retireStaleReviews = Effect.fn("retireStaleReviews")(function* (
  input: ReviewRetirementInput,
) {
  const host = yield* ReviewRetirementHost;
  const authenticator = yield* ReviewStateAuthenticator;
  if (authenticator.status !== "available") {
    yield* Effect.logWarning(
      "Skipping stale-review retirement because authenticated review state is unavailable.",
    );
    return ReviewRetirementReport.make({
      reviewsRetired: 0,
      findingsResolved: 0,
      commentsMinimized: 0,
      failures: 0,
    });
  }

  let failures = 0;
  let reviewsRetired = 0;
  let findingsResolved = 0;
  let commentsMinimized = 0;
  const reviews = yield* failOpen(host.listReviews, undefined, "Could not list prior reviews");
  if (reviews === undefined) {
    return ReviewRetirementReport.make({
      reviewsRetired,
      findingsResolved,
      commentsMinimized,
      failures: 1,
    });
  }

  for (const review of reviews) {
    if (
      review.authorNodeId !== input.currentAuthorNodeId ||
      !isStrictlyOlderReview(review, input) ||
      !hasReviewMetadataMarker(review.body)
    ) {
      continue;
    }
    const priorState = yield* failOpen(
      authenticator.extract(review.body),
      Option.none<ReviewState>(),
      `Could not authenticate prior review ${review.reviewId}`,
    );
    if (Option.isNone(priorState)) continue;

    const decision = decideReviewRetirement({
      priorBody: review.body,
      priorState: priorState.value,
      currentState: input.currentState,
      currentReviewUrl: input.currentReviewUrl,
    });
    const updated = yield* failOpen(
      host.updateBody(review.reviewId, decision.body).pipe(Effect.as(true)),
      false,
      `Could not retire prior review ${review.reviewId}`,
    );
    if (updated) {
      reviewsRetired += 1;
      findingsResolved += decision.resolvedFindings.length;
    } else {
      failures += 1;
    }

    if (decision.resolvedFindings.length === 0) continue;
    const comments = yield* failOpen(
      host.listComments(review.reviewId),
      undefined,
      `Could not list inline comments for prior review ${review.reviewId}`,
    );
    if (comments === undefined) {
      failures += 1;
      continue;
    }
    const resolved = new Set(decision.resolvedFindings.map(findingIdentity));
    for (const comment of comments) {
      const identity = inlineCommentIdentity(comment);
      if (identity === undefined || !resolved.has(identity)) continue;
      const minimized = yield* failOpen(
        host.minimizeComment(comment.nodeId).pipe(Effect.as(true)),
        false,
        `Could not minimize resolved inline comment ${comment.nodeId}`,
      );
      if (minimized) commentsMinimized += 1;
      else failures += 1;
    }
  }

  return ReviewRetirementReport.make({
    reviewsRetired,
    findingsResolved,
    commentsMinimized,
    failures,
  });
});
