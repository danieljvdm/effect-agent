import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Redacted } from "effect";

import {
  ChangedFile,
  PullRequestMetadata,
  planReviewUnits,
  ReviewHeadComparison,
  ReviewState,
  ReviewStateAuthenticator,
  selectReviewRange,
  StoredReviewFinding,
  webCryptoReviewStateAuthenticatorLayer,
} from "../src/index.ts";

const BASE_SHA = "1".repeat(40);
const REVIEWED_HEAD_SHA = "2".repeat(40);
const CURRENT_HEAD_SHA = "3".repeat(40);
const PROFILE_FINGERPRINT = "a".repeat(64);
const SCOPE_FINGERPRINT = "b".repeat(64);

const acceptedFile = ChangedFile.make({
  path: "src/accepted.ts",
  status: "modified",
  additions: 1,
  deletions: 1,
  patch: "@@ -1 +1 @@\n-old\n+accepted",
});

const correctiveFile = ChangedFile.make({
  path: "src/corrective.ts",
  status: "modified",
  additions: 1,
  deletions: 1,
  patch: "@@ -1 +1 @@\n-before\n+after",
});

const metadata = PullRequestMetadata.make({
  repository: "acme/widgets",
  number: 30,
  title: "Correct the review finding",
  body: "",
  baseRef: "main",
  baseSha: BASE_SHA,
  headRef: "fix/review",
  headSha: CURRENT_HEAD_SHA,
  totalChangedFiles: 2,
});

const unresolvedFinding = StoredReviewFinding.make({
  path: "src/accepted.ts",
  startLine: 1,
  endLine: 1,
  severity: "important",
  title: "Still requires attention",
  body: "This unresolved finding must remain active while another file changes.",
});

const priorState = ReviewState.make({
  version: 2,
  repository: metadata.repository,
  pullRequestNumber: metadata.number,
  baseRef: metadata.baseRef,
  baseSha: BASE_SHA,
  headRef: metadata.headRef,
  reviewedHeadSha: REVIEWED_HEAD_SHA,
  profileFingerprint: PROFILE_FINGERPRINT,
  acceptedScopeFingerprint: SCOPE_FINGERPRINT,
  reviewedPathCount: 1,
  unresolvedFindings: [unresolvedFinding],
  unresolvedConcerns: [],
  unreviewedPaths: [],
  settled: true,
  lastReviewMode: "full",
});

const comparison = ReviewHeadComparison.make({
  status: "ahead",
  baseSha: REVIEWED_HEAD_SHA,
  headSha: CURRENT_HEAD_SHA,
  mergeBaseSha: REVIEWED_HEAD_SHA,
  files: [correctiveFile],
  truncated: false,
});

const select = (overrides: Partial<Parameters<typeof selectReviewRange>[0]> = {}) =>
  selectReviewRange({
    requestedMode: "incremental",
    current: metadata,
    fullFiles: [acceptedFile, correctiveFile],
    profileFingerprint: PROFILE_FINGERPRINT,
    priorState,
    comparison,
    ...overrides,
  });

describe("review state", () => {
  it.effect("authenticates only a terminal schema-validated review-body marker", () =>
    Effect.gen(function* () {
      const secret = Redacted.make("stable-state-secret");
      const authenticate = (body: string, key = secret) =>
        Effect.gen(function* () {
          const authenticator = yield* ReviewStateAuthenticator;
          return yield* authenticator.extract(body);
        }).pipe(Effect.provide(webCryptoReviewStateAuthenticatorLayer(key)));
      const marker = yield* Effect.gen(function* () {
        const authenticator = yield* ReviewStateAuthenticator;
        return yield* authenticator.render(priorState);
      }).pipe(Effect.provide(webCryptoReviewStateAuthenticatorLayer(secret)));
      expect(Option.getOrUndefined(yield* authenticate(`review body\n${marker}`))).toEqual(
        priorState,
      );
      expect(
        Option.getOrUndefined(yield* authenticate(`review body\n${marker}\nhost footer`)),
      ).toBeUndefined();
      expect(
        Option.getOrUndefined(
          yield* authenticate(`review body\n${marker}`, Redacted.make("wrong-secret")),
        ),
      ).toBeUndefined();
      expect(
        Option.getOrUndefined(
          yield* authenticate("<!-- effect-agent-pr-review state-v1:not-base64 -->"),
        ),
      ).toBeUndefined();
    }),
  );

  it.effect("keeps WebCrypto failures typed and bounds the authenticated marker", () =>
    Effect.gen(function* () {
      const sign = (state: ReviewState, key: string) =>
        Effect.gen(function* () {
          const authenticator = yield* ReviewStateAuthenticator;
          return yield* authenticator.render(state);
        }).pipe(
          Effect.provide(webCryptoReviewStateAuthenticatorLayer(Redacted.make(key))),
          Effect.flip,
        );
      const authenticationFailure = yield* sign(priorState, "");
      expect(authenticationFailure._tag).toBe("ReviewStateAuthenticationFailure");

      const oversized = ReviewState.make({
        ...priorState,
        unresolvedFindings: Array.from({ length: 20 }, (_, index) =>
          StoredReviewFinding.make({
            path: `src/${index}-${"a".repeat(480)}.ts`,
            startLine: 1,
            endLine: 1,
            severity: "blocking",
            title: `Finding ${index} ${"t".repeat(100)}`,
            body: "b".repeat(800),
          }),
        ),
      });
      const markerFailure = yield* sign(oversized, "stable-state-secret");
      expect(markerFailure._tag).toBe("ReviewStateMarkerTooLarge");
    }),
  );

  it("reviews only the corrective delta and preserves unchanged prior scope", () => {
    const selection = select();
    expect(selection.mode).toBe("incremental");
    expect(selection.baselineSha).toBe(REVIEWED_HEAD_SHA);
    expect(selection.files.map((file) => file.path)).toEqual(["src/corrective.ts"]);
    expect(selection.files.map((file) => file.path)).not.toContain("src/accepted.ts");
    expect(selection.priorState?.unresolvedFindings).toEqual([unresolvedFinding]);
    expect(selection.reason).toContain(REVIEWED_HEAD_SHA.slice(0, 7));
  });

  it("falls back to the full diff when state is missing or incompatible", () => {
    const missing = select({ priorState: undefined, comparison: undefined });
    expect(missing.mode).toBe("full");
    expect(missing.reason).toContain("no compatible stored review state");

    const wrongProfile = select({ profileFingerprint: "c".repeat(64) });
    expect(wrongProfile.mode).toBe("full");
    expect(wrongProfile.reason).toContain("profile or model configuration changed");

    const changedBase = select({
      current: PullRequestMetadata.make({ ...metadata, baseSha: "4".repeat(40) }),
    });
    expect(changedBase.mode).toBe("full");
    expect(changedBase.reason).toContain("base changed");
  });

  it("falls back to the full diff for unsafe or incomplete head comparisons", () => {
    const diverged = select({
      comparison: ReviewHeadComparison.make({ ...comparison, status: "diverged" }),
    });
    expect(diverged.mode).toBe("full");
    expect(diverged.reason).toContain("not an ancestor");

    const truncated = select({
      comparison: ReviewHeadComparison.make({ ...comparison, truncated: true }),
    });
    expect(truncated.mode).toBe("full");
    expect(truncated.reason).toContain("file bound");

    const unavailable = select({ comparison: undefined });
    expect(unavailable.mode).toBe("full");
    expect(unavailable.reason).toContain("comparison was unavailable");
  });

  it("keeps an ancestor base advance incremental and includes overlapping PR context", () => {
    const nextBase = "4".repeat(40);
    const baseComparison = ReviewHeadComparison.make({
      status: "ahead",
      baseSha: BASE_SHA,
      headSha: nextBase,
      mergeBaseSha: BASE_SHA,
      files: [acceptedFile],
      truncated: false,
    });
    const selection = select({
      current: PullRequestMetadata.make({ ...metadata, baseSha: nextBase }),
      baseComparison,
    });
    expect(selection.mode).toBe("incremental");
    expect(selection.files.map((file) => file.path)).toEqual([
      "src/accepted.ts",
      "src/corrective.ts",
    ]);
    expect(selection.reason).toContain("base advanced");

    const rewrittenBase = select({
      current: PullRequestMetadata.make({ ...metadata, baseSha: nextBase }),
      baseComparison: ReviewHeadComparison.make({
        ...baseComparison,
        status: "diverged",
        mergeBaseSha: "5".repeat(40),
      }),
    });
    expect(rewrittenBase.mode).toBe("full");
    expect(rewrittenBase.reason).toContain("changed materially");
  });

  it("retries carried unreviewed paths inside the incremental scope", () => {
    const carryingState = ReviewState.make({
      ...priorState,
      unreviewedPaths: ["src/accepted.ts", "src/reverted.ts"],
      settled: false,
    });
    const selection = select({ priorState: carryingState });

    expect(selection.mode).toBe("incremental");
    // The delta plus the carried path still present in the pull request; a
    // carried path reverted out of the PR has nothing left to review.
    expect(selection.files.map((file) => file.path)).toEqual([
      "src/accepted.ts",
      "src/corrective.ts",
    ]);
    // Carried paths count as affected: their stored findings are re-derived
    // by the fresh re-review instead of being carried blindly.
    expect(selection.affectedPaths).toContain("src/accepted.ts");
    expect(selection.reason).toContain("retrying 1 carried unreviewed path(s)");
  });

  it("forces fresh full-diff discovery only when final mode is explicit", () => {
    const selection = select({ requestedMode: "final" });
    expect(selection.mode).toBe("full");
    expect(selection.files.map((file) => file.path)).toEqual([
      "src/accepted.ts",
      "src/corrective.ts",
    ]);
    expect(selection.reason).toBe("explicit final full-diff audit requested");
    expect(selection.priorState).toBe(undefined);
    const plan = planReviewUnits(selection.files, { totalChangedFiles: 2 });
    expect(plan.discoveryPasses.filter((pass) => pass.perspective === "general")).toHaveLength(
      plan.units.length,
    );
    expect([...new Set(plan.discoveryPasses.flatMap((pass) => pass.paths))].sort()).toEqual([
      "src/accepted.ts",
      "src/corrective.ts",
    ]);
  });
});
