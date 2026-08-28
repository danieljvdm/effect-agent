export type ReviewMode = "auto" | "incremental" | "full";

/** Parse a trusted collaborator command without admitting prefixes or trailing prose. */
export const reviewModeFromCommand = (command: string): "incremental" | "full" | undefined => {
  switch (command.trim()) {
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
      readonly _tag: "skip";
      readonly reason:
        | "head-already-reviewed"
        | "head-review-incomplete"
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
  return input.history
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
};

/** Count trusted change requests that GitHub still considers unresolved. */
export const unresolvedChangeRequestCount = (input: {
  readonly reviewAuthor: string;
  readonly history: ReadonlyArray<ReviewHistoryItem>;
}): number =>
  trustedHistory(input).filter(
    ({ item, marker }) => marker._tag === "attempt" && item.state === "CHANGES_REQUESTED",
  ).length;

/** Select scope from trusted GitHub reviews without persisting model context. */
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
  if (currentHeadAttempts.some(({ marker }) => marker.version === 3 && marker.completed)) {
    return { _tag: "skip", reason: "head-already-reviewed" };
  }
  if (input.mode === "auto" && currentHeadAttempts.length > 0) {
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
    reason: input.mode === "auto" ? "one automatic follow-up" : "manual incremental review",
  };
};
