export type ReviewMode = "auto" | "incremental" | "full";

const MAX_AUTOMATIC_REVIEWS = 2;

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
      readonly _tag: "review";
      readonly scope: "full" | "incremental";
      readonly baseRevision: string | undefined;
      readonly automatic: boolean;
      readonly automaticReviewsRemaining: number;
      readonly reason: string;
    };

const MARKER_PATTERN =
  /(?:^|\n)<!-- effect-agent-review:v2 automatic=(true|false) completed=(true|false) -->\s*$/;

export const reviewMarker = (automatic: boolean, completed = true): string =>
  `<!-- effect-agent-review:v2 automatic=${String(automatic)} completed=${String(completed)} -->`;

const markerKind = (
  body: string,
): { readonly automatic: boolean; readonly completed: boolean } | undefined => {
  const match = MARKER_PATTERN.exec(body);
  return match === null
    ? undefined
    : { automatic: match[1] === "true", completed: match[2] === "true" };
};

/** Select scope from trusted GitHub reviews without persisting model context. */
export const selectReview = (input: {
  readonly mode: ReviewMode;
  readonly currentHead: string;
  readonly reviewAuthor: string;
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
  const automaticAttempts = trusted.filter(({ marker }) => marker.automatic).length;
  const automaticReviewsRemaining = Math.max(
    0,
    MAX_AUTOMATIC_REVIEWS - automaticAttempts - (input.mode === "auto" ? 1 : 0),
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
    trusted.some(
      ({ item, marker }) =>
        item.commitId === input.currentHead && (input.mode === "auto" || marker.completed),
    )
  ) {
    return { _tag: "skip", reason: "head-already-reviewed" };
  }

  if (input.mode === "auto" && automaticAttempts >= MAX_AUTOMATIC_REVIEWS) {
    return { _tag: "skip", reason: "automatic-reviews-paused" };
  }

  const latest = trusted.filter(({ marker }) => marker.completed).at(-1)?.item;
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
