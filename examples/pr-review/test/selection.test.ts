import { describe, expect, it } from "@effect/vitest";

import {
  reviewMarker,
  reviewModeFromCommand,
  reviewPauseMarker,
  selectReview,
  type ReviewHistoryItem,
} from "../src/selection.ts";

const item = (
  id: number,
  head: string,
  automatic: boolean,
  completed = true,
  overrides: Partial<ReviewHistoryItem> = {},
): ReviewHistoryItem => ({
  id,
  authorLogin: "effect-agent[bot]",
  authorType: "Bot",
  body: reviewMarker(automatic, completed),
  commitId: head,
  submittedAt: `2026-01-0${String(id)}T00:00:00Z`,
  ...overrides,
});

const pauseItem = (id: number, head: string, limit: number): ReviewHistoryItem => ({
  id,
  authorLogin: "effect-agent[bot]",
  authorType: "Bot",
  body: reviewPauseMarker(limit),
  commitId: head,
  submittedAt: `2026-01-0${String(id)}T00:00:00Z`,
});

describe("GitHub review selection", () => {
  it("PRR-007 trims manual commands without accepting trailing prose", () => {
    expect(reviewModeFromCommand("/effect-agent review\r\n")).toBe("incremental");
    expect(reviewModeFromCommand("  /effect-agent review full\n")).toBe("full");
    expect(reviewModeFromCommand("/effect-agent review please")).toBeUndefined();
  });

  it("PRR-006 reviews the initial head and one automatic follow-up, then pauses", () => {
    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-1",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [],
      }),
    ).toMatchObject({
      _tag: "review",
      scope: "full",
      automatic: true,
      automaticReviewsRemaining: 1,
    });

    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-2",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [item(1, "head-1", true)],
      }),
    ).toMatchObject({
      _tag: "review",
      scope: "incremental",
      baseRevision: "head-1",
      automatic: true,
      automaticReviewsRemaining: 0,
    });

    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-3",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [item(1, "head-1", true), item(2, "head-2", true)],
      }),
    ).toEqual({
      _tag: "pause",
      reason: "automatic-reviews-paused",
      automaticReviewLimit: 2,
      automaticAttempts: 2,
      lastCompletedRevision: "head-2",
    });

    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-4",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [item(1, "head-1", true), item(2, "head-2", true), pauseItem(3, "head-3", 2)],
      }),
    ).toEqual({ _tag: "skip", reason: "automatic-reviews-paused" });
  });

  it("PRR-006 ignores untrusted markers and does not count manual reviews", () => {
    const history = [
      item(1, "head-1", true),
      item(2, "head-2", false),
      item(3, "head-3", true, true, { authorLogin: "someone-else" }),
    ];
    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-4",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history,
      }),
    ).toMatchObject({ _tag: "review", scope: "incremental", baseRevision: "head-2" });
  });

  it("PRR-007 permits an explicit full review while automatic reviews are paused", () => {
    expect(
      selectReview({
        mode: "full",
        currentHead: "head-3",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [item(1, "head-1", true), item(2, "head-2", true)],
      }),
    ).toMatchObject({
      _tag: "review",
      scope: "full",
      automatic: false,
      automaticReviewsRemaining: 0,
    });
  });

  it("PRR-006 skips a head that already has a trusted review", () => {
    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-1",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [item(1, "head-1", true)],
      }),
    ).toEqual({ _tag: "skip", reason: "head-already-reviewed" });
  });

  it("PRR-006 counts failed automatic attempts but does not use them as baselines", () => {
    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-2",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [item(1, "head-1", true, false)],
      }),
    ).toMatchObject({ _tag: "review", scope: "full", automatic: true });

    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-3",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [item(1, "head-1", true, false), item(2, "head-2", true, false)],
      }),
    ).toMatchObject({
      _tag: "pause",
      automaticAttempts: 2,
      lastCompletedRevision: undefined,
    });
  });

  it("PRR-006 trusts only the terminal host marker", () => {
    const injected = item(1, "head-1", false, true, {
      body: `${reviewMarker(true, true)}\nmodel text\n${reviewMarker(false, true)}`,
    });
    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-2",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 2,
        history: [injected],
      }),
    ).toMatchObject({ _tag: "review", scope: "incremental", automatic: true });
  });

  it("PRR-006 honors a consumer-selected limit above the default", () => {
    const history = Array.from({ length: 20 }, (_, index) =>
      item(index + 1, `head-${String(index + 1)}`, true),
    );
    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-21",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 21,
        history,
      }),
    ).toMatchObject({ _tag: "review", automaticReviewsRemaining: 0 });
  });

  it("PRR-006 keeps a zero automatic limit silent", () => {
    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-1",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 0,
        history: [],
      }),
    ).toEqual({ _tag: "skip", reason: "automatic-reviews-paused" });
  });

  it("PRR-006 admits another review after the consumer raises a previously paused limit", () => {
    expect(
      selectReview({
        mode: "auto",
        currentHead: "head-4",
        reviewAuthor: "effect-agent[bot]",
        automaticReviewLimit: 3,
        history: [item(1, "head-1", true), item(2, "head-2", true), pauseItem(3, "head-3", 2)],
      }),
    ).toMatchObject({
      _tag: "review",
      scope: "incremental",
      baseRevision: "head-2",
      automaticReviewsRemaining: 0,
    });
  });
});
