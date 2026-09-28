import { Option, Schema } from "effect";

export type ReviewMode = "auto" | "incremental" | "full" | "reconcile";

const ReviewDismissal = Schema.Struct({
  reviewUrl: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(2_048))),
  reviewId: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  ),
  reason: Schema.NonEmptyString.check(Schema.isMaxLength(1_000)),
});

/** A dismissal names one review and requires an explanation on subsequent lines. */
export const dismissalFromCommand = (command: string) => {
  const match =
    /^@effect-agent[ \t]+dismiss[ \t]+(?:(https:\/\/\S+#pullrequestreview-([1-9][0-9]*))|([1-9][0-9]*))[ \t]*\r?\n([\s\S]*)$/i.exec(
      command.trimStart(),
    );

  return match === null
    ? undefined
    : Option.getOrUndefined(
        Schema.decodeOption(ReviewDismissal)({
          ...(match[1] === undefined ? {} : { reviewUrl: match[1] }),
          reviewId: Number(match[2] ?? match[3]),
          reason: match[4]?.trim(),
        }),
      );
};

/** Read the first nonblank line of a trusted comment; later lines may contain explanation. */
export const reviewModeFromCommand = (command: string): "incremental" | "full" | undefined => {
  const firstLine = command.trimStart().split(/[\r\n]/, 1)[0];

  switch (
    firstLine
      ?.trim()
      .replace(/[ \t]+/g, " ")
      .toLowerCase()
  ) {
    case "@effect-agent review":
      return "incremental";
    case "@effect-agent review full":
      return "full";
    default:
      return undefined;
  }
};

export interface ReviewHistoryItem {
  readonly id: number;
  readonly authorLogin: string;
  readonly authorType: string;
  readonly body: string;
  readonly commitId: string | undefined;
  readonly submittedAt: string | undefined;
  readonly state: "APPROVED" | "CHANGES_REQUESTED" | "COMMENTED" | "DISMISSED" | "PENDING";
}

export type ReviewSelection =
  | {
      readonly _tag: "reconcile";
      readonly reason: "head-already-reviewed";
    }
  | {
      readonly _tag: "skip";
      readonly reason:
        | "head-already-reviewed"
        | "head-review-incomplete"
        | "head-not-reviewed"
        | "automatic-reviews-paused"
        | "incremental-baseline-unavailable";
    }
  | {
      readonly _tag: "pause";
      readonly reason: "automatic-reviews-paused";
      readonly automaticReviewLimit: number;
      readonly automaticAttempts: number;
      readonly lastCompletedRevision: string | undefined;
    }
  | {
      readonly _tag: "review";
      readonly scope: "full" | "incremental";
      readonly baseRevision: string | undefined;
      readonly automatic: boolean;
      readonly automaticReviewsRemaining: number;
      readonly reason: string;
    };

const ATTEMPT_MARKER_PATTERN =
  /(?:^|\n)<!-- effect-agent-review:v(2|3) automatic=(true|false) completed=(true|false) -->\s*$/;

const PAUSE_MARKER_PATTERN = /(?:^|\n)<!-- effect-agent-review-pause:v1 limit=([0-9]+) -->\s*$/;

export const reviewMarker = (automatic: boolean, completed = true): string =>
  `<!-- effect-agent-review:v3 automatic=${String(automatic)} completed=${String(completed)} -->`;

export const reviewPauseMarker = (automaticReviewLimit: number): string =>
  `<!-- effect-agent-review-pause:v1 limit=${String(automaticReviewLimit)} -->`;

const markerKind = (
  body: string,
):
  | {
      readonly _tag: "attempt";
      readonly version: 2 | 3;
      readonly automatic: boolean;
      readonly completed: boolean;
    }
  | { readonly _tag: "pause"; readonly automaticReviewLimit: string }
  | undefined => {
  const attempt = ATTEMPT_MARKER_PATTERN.exec(body);

  if (attempt !== null) {
    return {
      _tag: "attempt",
      version: attempt[1] === "3" ? 3 : 2,
      automatic: attempt[2] === "true",
      completed: attempt[3] === "true",
    };
  }
  const pause = PAUSE_MARKER_PATTERN.exec(body);

  return pause === null ? undefined : { _tag: "pause", automaticReviewLimit: pause[1] ?? "" };
};

const trustedHistory = (input: {
  readonly reviewAuthor: string;
  readonly history: ReadonlyArray<ReviewHistoryItem>;
}) => {
  const author = input.reviewAuthor.toLowerCase();

  // Preserve GitHub's chronological list order, including undated entries:
  // https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
  return input.history.flatMap((item) => {
    const marker = markerKind(item.body);

    return marker !== undefined &&
      item.authorType === "Bot" &&
      item.authorLogin.toLowerCase() === author &&
      item.commitId !== undefined
      ? [{ item, marker }]
      : [];
  });
};

/** Select only this channel's terminal, bot-authored change requests. */
export const unresolvedChangeRequests = (input: {
  readonly reviewAuthor: string;
  readonly history: ReadonlyArray<ReviewHistoryItem>;
}): ReadonlyArray<ReviewHistoryItem> =>
  trustedHistory(input).flatMap(({ item, marker }) =>
    marker._tag === "attempt" && item.state === "CHANGES_REQUESTED" ? [item] : [],
  );

export const unresolvedChangeRequestCount = (input: {
  readonly reviewAuthor: string;
  readonly history: ReadonlyArray<ReviewHistoryItem>;
}): number => unresolvedChangeRequests(input).length;

/** Select scope or an explicit status refresh from trusted GitHub reviews. */
export const selectReview = (input: {
  readonly mode: ReviewMode;
  readonly currentHead: string;
  readonly reviewAuthor: string;
  readonly automaticReviewLimit: number;
  readonly history: ReadonlyArray<ReviewHistoryItem>;
}): ReviewSelection => {
  const trusted = trustedHistory(input);

  const attempts = trusted.flatMap(({ item, marker }) =>
    marker._tag === "attempt" ? [{ item, marker }] : [],
  );

  const automaticAttempts = attempts.filter(({ marker }) => marker.automatic).length;

  const automaticReviewsRemaining = Math.max(
    0,
    input.automaticReviewLimit - automaticAttempts - (input.mode === "auto" ? 1 : 0),
  );

  if (input.mode === "full") {
    return {
      _tag: "review",
      scope: "full",
      baseRevision: undefined,
      automatic: false,
      automaticReviewsRemaining,
      reason: "manual full review",
    };
  }

  const currentHeadAttempts = attempts.filter(({ item }) => item.commitId === input.currentHead);
  const latestHeadAttempt = currentHeadAttempts.at(-1);

  if (latestHeadAttempt?.marker.version === 3 && latestHeadAttempt.marker.completed) {
    return {
      _tag: input.mode === "incremental" || input.mode === "reconcile" ? "reconcile" : "skip",
      reason: "head-already-reviewed",
    };
  }

  // A human disposition changes feedback, never supplies missing review coverage.
  if (input.mode === "reconcile") {
    return {
      _tag: "skip",
      reason: latestHeadAttempt === undefined ? "head-not-reviewed" : "head-review-incomplete",
    };
  }

  if (
    currentHeadAttempts.some(({ marker }) => marker.version === 3 && marker.completed) ||
    (input.mode === "auto" &&
      currentHeadAttempts.length > 0 &&
      automaticAttempts >= input.automaticReviewLimit)
  ) {
    return { _tag: "skip", reason: "head-review-incomplete" };
  }

  if (input.mode === "auto" && automaticAttempts >= input.automaticReviewLimit) {
    if (input.automaticReviewLimit === 0) {
      return { _tag: "skip", reason: "automatic-reviews-paused" };
    }

    const pausePublished = trusted.some(
      ({ marker }) =>
        marker._tag === "pause" &&
        marker.automaticReviewLimit === String(input.automaticReviewLimit),
    );

    if (pausePublished) return { _tag: "skip", reason: "automatic-reviews-paused" };

    return {
      _tag: "pause",
      reason: "automatic-reviews-paused",
      automaticReviewLimit: input.automaticReviewLimit,
      automaticAttempts,
      lastCompletedRevision: attempts
        .filter(({ marker }) => marker.version === 3 && marker.completed)
        .at(-1)?.item.commitId,
    };
  }

  const latest = attempts
    .filter(({ marker }) => marker.version === 3 && marker.completed)
    .at(-1)?.item;

  if (latest?.commitId === undefined) {
    if (input.mode === "incremental") {
      return { _tag: "skip", reason: "incremental-baseline-unavailable" };
    }

    return {
      _tag: "review",
      scope: "full",
      baseRevision: undefined,
      automatic: input.mode === "auto",
      automaticReviewsRemaining,
      reason: "no prior review baseline",
    };
  }

  return {
    _tag: "review",
    scope: "incremental",
    baseRevision: latest.commitId,
    automatic: input.mode === "auto",
    automaticReviewsRemaining,
    reason: input.mode === "auto" ? "automatic incremental review" : "manual incremental review",
  };
};
