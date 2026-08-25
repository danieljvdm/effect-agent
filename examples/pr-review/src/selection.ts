export type ReviewMode = "auto" | "incremental" | "full";

export interface ReviewHistoryItem {
  readonly id: number;
  readonly authorLogin: string;
  readonly authorType: string;
  readonly body: string;
  readonly commitId: string | undefined;
  readonly submittedAt: string | undefined;
}

export type ReviewSelection =
  | {
      readonly _tag: "skip";
      readonly reason: "head-already-reviewed" | "automatic-reviews-paused";
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
  /(?:^|\n)<!-- effect-agent-review:v2 automatic=(true|false) completed=(true|false) -->\s*$/;
const PAUSE_MARKER_PATTERN = /(?:^|\n)<!-- effect-agent-review-pause:v1 limit=([0-9]+) -->\s*$/;

export const reviewMarker = (automatic: boolean, completed = true): string =>
  `<!-- effect-agent-review:v2 automatic=${String(automatic)} completed=${String(completed)} -->`;

export const reviewPauseMarker = (automaticReviewLimit: number): string =>
  `<!-- effect-agent-review-pause:v1 limit=${String(automaticReviewLimit)} -->`;

const markerKind = (
  body: string,
):
  | { readonly _tag: "attempt"; readonly automatic: boolean; readonly completed: boolean }
  | { readonly _tag: "pause"; readonly automaticReviewLimit: string }
  | undefined => {
  const attempt = ATTEMPT_MARKER_PATTERN.exec(body);
  if (attempt !== null) {
    return {
      _tag: "attempt",
      automatic: attempt[1] === "true",
      completed: attempt[2] === "true",
    };
  }
  const pause = PAUSE_MARKER_PATTERN.exec(body);
  return pause === null ? undefined : { _tag: "pause", automaticReviewLimit: pause[1] ?? "" };
};

/** Select scope from trusted GitHub reviews without persisting model context. */
export const selectReview = (input: {
  readonly mode: ReviewMode;
  readonly currentHead: string;
  readonly reviewAuthor: string;
  readonly automaticReviewLimit: number;
  readonly history: ReadonlyArray<ReviewHistoryItem>;
}): ReviewSelection => {
  const author = input.reviewAuthor.toLowerCase();
  const trusted = input.history
    .flatMap((item) => {
      const marker = markerKind(item.body);
      return marker !== undefined &&
        item.authorType === "Bot" &&
        item.authorLogin.toLowerCase() === author &&
        item.commitId !== undefined
        ? [{ item, marker }]
        : [];
    })
    .sort((left, right) => {
      const byTime = (left.item.submittedAt ?? "").localeCompare(right.item.submittedAt ?? "");
      return byTime === 0 ? left.item.id - right.item.id : byTime;
    });
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

  if (
    attempts.some(
      ({ item, marker }) =>
        item.commitId === input.currentHead && (input.mode === "auto" || marker.completed),
    )
  ) {
    return { _tag: "skip", reason: "head-already-reviewed" };
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
      lastCompletedRevision: attempts.filter(({ marker }) => marker.completed).at(-1)?.item
        .commitId,
    };
  }

  const latest = attempts.filter(({ marker }) => marker.completed).at(-1)?.item;
  if (latest?.commitId === undefined) {
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
    reason: input.mode === "auto" ? "one automatic follow-up" : "manual incremental review",
  };
};
