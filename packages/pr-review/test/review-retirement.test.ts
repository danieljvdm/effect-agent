import { describe, expect, it } from "@effect/vitest";
import { DateTime, Effect, Exit, Option, Redacted, Ref } from "effect";

import {
  decideReviewRetirement,
  parseGitHubSubmittedAt,
  RetirableReview,
  RetirableReviewComment,
  ReviewRetirementFailure,
  ReviewRetirementHost,
  ReviewState,
  ReviewStateAuthenticator,
  retireStaleReviews,
  StoredReviewFinding,
  webCryptoReviewStateAuthenticatorLayer,
} from "../src/index.ts";

const BASE_SHA = "1".repeat(40);
const PRIOR_HEAD_SHA = "2".repeat(40);
const CURRENT_HEAD_SHA = "3".repeat(40);
const PROFILE_FINGERPRINT = "a".repeat(64);
const SCOPE_FINGERPRINT = "b".repeat(64);
const STATE_SECRET = Redacted.make("review-retirement-test-secret");
const BOT_NODE_ID = "BOT_effect-agent-reviewer";
const CURRENT_SUBMITTED_AT = DateTime.makeUnsafe("2026-08-15T20:10:00Z");

const resolvedFinding = StoredReviewFinding.make({
  path: "src/resolved.ts",
  startLine: 4,
  endLine: 6,
  severity: "blocking",
  title: "Release the scoped resource",
  body: "The corrected code now releases the resource.",
});

const unresolvedFinding = StoredReviewFinding.make({
  path: "src/unresolved.ts",
  startLine: 9,
  endLine: 9,
  severity: "important",
  title: "Preserve the typed failure",
  body: "This finding remains active in the newest review.",
});

const makeState = (
  reviewedHeadSha: string,
  unresolvedFindings: ReadonlyArray<StoredReviewFinding>,
) =>
  ReviewState.make({
    version: 1,
    repository: "acme/widgets",
    pullRequestNumber: 42,
    baseRef: "main",
    baseSha: BASE_SHA,
    headRef: "fix/resources",
    reviewedHeadSha,
    profileFingerprint: PROFILE_FINGERPRINT,
    acceptedScopeFingerprint: SCOPE_FINGERPRINT,
    reviewedPathCount: 2,
    unresolvedFindings,
    unresolvedConcerns: [],
    lastReviewMode: "incremental",
  });

const priorState = makeState(PRIOR_HEAD_SHA, [resolvedFinding, unresolvedFinding]);
const currentState = makeState(CURRENT_HEAD_SHA, [unresolvedFinding]);

const renderPriorBody = Effect.gen(function* () {
  const authenticator = yield* ReviewStateAuthenticator;
  const stateMarker = yield* authenticator.render(priorState);
  const metadata = [
    "<!-- effect-agent-pr-review metadata",
    `reviewed-head: ${PRIOR_HEAD_SHA}`,
    "base-ref: main",
    "head-ref: fix/resources",
    "files-visible: 2 of 2",
    "review-mode: incremental",
    "Findings were written against the head commit above; if commits have landed",
    "since, treat file and line callouts as potentially stale and re-diff first.",
    "-->",
  ].join("\n");
  const fingerprint = `<!-- effect-agent-pr-review fingerprint=sha256:${"c".repeat(64)} -->`;
  const body = [
    "> [!CAUTION]",
    "> 1 blocking finding — do not merge before addressing it.",
    "",
    "The original review summary remains available for historical context.",
    "",
    metadata,
    fingerprint,
    stateMarker,
  ].join("\n");
  return { body, metadata, fingerprint, stateMarker };
});

const machineComments = (body: string): ReadonlyArray<string> =>
  Array.from(
    body.matchAll(
      /<!-- effect-agent-pr-review metadata\n[\s\S]*?\n-->|<!-- effect-agent-pr-review fingerprint=sha256:[0-9a-f]{64} -->|<!-- effect-agent-pr-review state-v1:[A-Za-z0-9+/]+={0,2}\.[0-9a-f]{64} -->/g,
    ),
    (match) => match[0],
  );

const resolvedComment = RetirableReviewComment.make({
  nodeId: "PRRC_resolved",
  path: resolvedFinding.path,
  startLine: resolvedFinding.startLine,
  endLine: resolvedFinding.endLine,
  // The current first-line format, carrying a category chip.
  body: `**[🛑 blocking · resources] ${resolvedFinding.title}**\n\n${resolvedFinding.body}`,
});

const legacyResolvedComment = RetirableReviewComment.make({
  nodeId: "PRRC_resolved_legacy",
  path: resolvedFinding.path,
  startLine: resolvedFinding.startLine,
  endLine: resolvedFinding.endLine,
  // The pre-category first line posted by older package versions.
  body: `**[🛑 blocking] ${resolvedFinding.title}**\n\n${resolvedFinding.body}`,
});

const unresolvedComment = RetirableReviewComment.make({
  nodeId: "PRRC_unresolved",
  path: unresolvedFinding.path,
  startLine: unresolvedFinding.startLine,
  endLine: unresolvedFinding.endLine,
  body: `**[⚠️ important] ${unresolvedFinding.title}**\n\n${unresolvedFinding.body}`,
});

describe("review retirement", () => {
  it("canonicalizes valid GitHub submission times and rejects malformed values", () => {
    const utc = parseGitHubSubmittedAt("2026-08-15T20:00:00Z");
    const offset = parseGitHubSubmittedAt("2026-08-15T13:00:00-07:00");

    if (utc === null || offset === null) throw new Error("Expected valid test timestamps");
    expect(DateTime.toEpochMillis(utc)).toBe(DateTime.toEpochMillis(offset));
    expect(parseGitHubSubmittedAt("not-a-timestamp")).toBeNull();
    expect(parseGitHubSubmittedAt(null)).toBeNull();
  });

  it.effect("computes the resolved subset and keeps edited machine state byte-identical", () =>
    Effect.gen(function* () {
      const prior = yield* renderPriorBody;
      const decision = decideReviewRetirement({
        priorBody: prior.body,
        priorState,
        currentState,
        currentReviewUrl: "https://github.com/acme/widgets/pull/42#pullrequestreview-2",
      });

      expect(decision.resolvedFindings).toEqual([resolvedFinding]);
      expect(decision.priorFindingCount).toBe(2);
      expect(decision.body).toContain("Superseded — 1 of 2 findings resolved at `3333333`");
      expect(decision.body).toContain(`~~${resolvedFinding.title}~~ · resolved at \`3333333\``);
      expect(decision.body).toContain("<details>");
      expect(decision.body).not.toContain("[!CAUTION]");
      expect(decision.body).toContain(prior.metadata);
      expect(decision.body).toContain(prior.fingerprint);
      expect(decision.body).toContain(prior.stateMarker);
      expect(machineComments(decision.body)).toEqual(machineComments(prior.body));

      const authenticator = yield* ReviewStateAuthenticator;
      const roundTripped = yield* authenticator.extract(decision.body);
      expect(Option.getOrUndefined(roundTripped)).toEqual(priorState);
      expect(decision.body.endsWith(prior.stateMarker)).toBe(true);
    }).pipe(Effect.provide(webCryptoReviewStateAuthenticatorLayer(STATE_SECRET))),
  );

  it.effect(
    "touches only older same-actor marker reviews and minimizes only resolved inline comments",
    () =>
      Effect.gen(function* () {
        const prior = yield* renderPriorBody;
        const updates = yield* Ref.make<ReadonlyArray<readonly [number, string]>>([]);
        const minimized = yield* Ref.make<ReadonlyArray<string>>([]);
        const host = ReviewRetirementHost.of({
          listReviews: Effect.succeed([
            RetirableReview.make({
              reviewId: 1,
              body: prior.body,
              commitSha: PRIOR_HEAD_SHA,
              authorNodeId: BOT_NODE_ID,
              submittedAt: DateTime.makeUnsafe("2026-08-15T20:00:00Z"),
            }),
            RetirableReview.make({
              reviewId: 6,
              body: prior.body,
              commitSha: PRIOR_HEAD_SHA,
              authorNodeId: BOT_NODE_ID,
              submittedAt: null,
            }),
            RetirableReview.make({
              reviewId: 7,
              body: "A human-authored review without the package metadata marker.",
              commitSha: PRIOR_HEAD_SHA,
              authorNodeId: BOT_NODE_ID,
              submittedAt: DateTime.makeUnsafe("2026-08-15T20:01:00Z"),
            }),
            RetirableReview.make({
              reviewId: 8,
              body: prior.body,
              commitSha: PRIOR_HEAD_SHA,
              authorNodeId: "USER_copied-signed-state",
              submittedAt: DateTime.makeUnsafe("2026-08-15T20:02:00Z"),
            }),
            RetirableReview.make({
              reviewId: 9,
              body: prior.body,
              commitSha: CURRENT_HEAD_SHA,
              authorNodeId: BOT_NODE_ID,
              submittedAt: CURRENT_SUBMITTED_AT,
            }),
            RetirableReview.make({
              reviewId: 10,
              body: prior.body,
              commitSha: CURRENT_HEAD_SHA,
              authorNodeId: BOT_NODE_ID,
              submittedAt: DateTime.makeUnsafe("2026-08-15T20:11:00Z"),
            }),
          ]),
          listComments: () =>
            Effect.succeed([resolvedComment, legacyResolvedComment, unresolvedComment]),
          updateBody: (reviewId, body) =>
            Ref.update(updates, (entries) => [...entries, [reviewId, body] as const]),
          minimizeComment: (nodeId) => Ref.update(minimized, (nodeIds) => [...nodeIds, nodeId]),
        });

        const report = yield* retireStaleReviews({
          currentReviewId: 9,
          currentReviewUrl: "https://github.com/acme/widgets/pull/42#pullrequestreview-9",
          currentAuthorNodeId: BOT_NODE_ID,
          currentSubmittedAt: CURRENT_SUBMITTED_AT,
          currentState,
        }).pipe(Effect.provideService(ReviewRetirementHost, host));

        expect(report).toMatchObject({
          reviewsRetired: 1,
          findingsResolved: 1,
          commentsMinimized: 2,
          failures: 0,
        });
        expect((yield* Ref.get(updates)).map(([reviewId]) => reviewId)).toEqual([1]);
        // Both first-line formats matched: the categorized current format and
        // the pre-category one posted by older package versions.
        expect(yield* Ref.get(minimized)).toEqual([
          resolvedComment.nodeId,
          legacyResolvedComment.nodeId,
        ]);
      }).pipe(Effect.provide(webCryptoReviewStateAuthenticatorLayer(STATE_SECRET))),
  );

  it.effect("keeps review editing and comment minimization failures fail-open", () =>
    Effect.gen(function* () {
      const prior = yield* renderPriorBody;
      const failed = (operation: string) =>
        Effect.fail(ReviewRetirementFailure.make({ operation, reason: "scripted failure" }));
      const host = ReviewRetirementHost.of({
        listReviews: Effect.succeed([
          RetirableReview.make({
            reviewId: 1,
            body: prior.body,
            commitSha: PRIOR_HEAD_SHA,
            authorNodeId: BOT_NODE_ID,
            submittedAt: DateTime.makeUnsafe("2026-08-15T20:00:00Z"),
          }),
        ]),
        listComments: () => Effect.succeed([resolvedComment]),
        updateBody: () => failed("updateReview"),
        minimizeComment: () => failed("minimizeComment"),
      });

      const report = yield* retireStaleReviews({
        currentReviewId: 9,
        currentReviewUrl: "https://github.com/acme/widgets/pull/42#pullrequestreview-9",
        currentAuthorNodeId: BOT_NODE_ID,
        currentSubmittedAt: CURRENT_SUBMITTED_AT,
        currentState,
      }).pipe(Effect.provideService(ReviewRetirementHost, host));

      expect(report).toMatchObject({
        reviewsRetired: 0,
        findingsResolved: 0,
        commentsMinimized: 0,
        failures: 2,
      });
    }).pipe(Effect.provide(webCryptoReviewStateAuthenticatorLayer(STATE_SECRET))),
  );

  it.effect("does not downgrade retirement defects into cosmetic failures", () =>
    Effect.gen(function* () {
      const host = ReviewRetirementHost.of({
        listReviews: Effect.die("scripted retirement defect"),
        listComments: () => Effect.die("Unexpected comment listing"),
        updateBody: () => Effect.die("Unexpected review edit"),
        minimizeComment: () => Effect.die("Unexpected comment minimization"),
      });

      const exit = yield* retireStaleReviews({
        currentReviewId: 9,
        currentReviewUrl: "https://github.com/acme/widgets/pull/42#pullrequestreview-9",
        currentAuthorNodeId: BOT_NODE_ID,
        currentSubmittedAt: CURRENT_SUBMITTED_AT,
        currentState,
      }).pipe(Effect.provideService(ReviewRetirementHost, host), Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(webCryptoReviewStateAuthenticatorLayer(STATE_SECRET))),
  );
});
