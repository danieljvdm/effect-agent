import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Redacted, Schema } from "effect";

import {
  adjudicationIdentity,
  ChangedFile,
  concernIdentity,
  findingIdentity,
  PullRequestMetadata,
  planReviewUnits,
  ReviewHeadComparison,
  ReviewState,
  ReviewStateAuthenticator,
  ReviewTreeComparison,
  selectReviewRange,
  StoredAdjudication,
  StoredReviewConcern,
  StoredReviewFinding,
  StoredUnreviewedPass,
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
  version: 1,
  repository: metadata.repository,
  pullRequestNumber: metadata.number,
  baseRef: metadata.baseRef,
  baseSha: BASE_SHA,
  headRef: metadata.headRef,
  reviewedHeadSha: REVIEWED_HEAD_SHA,
  profileFingerprint: PROFILE_FINGERPRINT,
  settledScopeFingerprint: SCOPE_FINGERPRINT,
  reviewedPathCount: 1,
  unresolvedFindings: [unresolvedFinding],
  unresolvedConcerns: [],
  unreviewedPaths: [],
  unreviewedPasses: [],
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
  it("keeps anchored findings and unanchored concerns in disjoint identity namespaces", () => {
    const finding = {
      path: "src/a.ts",
      startLine: 1,
      endLine: 1,
      title: "Blocker",
    };
    const delimiterMimic = `${finding.path}\u0000${finding.startLine}\u0000${finding.endLine}\u0000${finding.title}`;
    const unanchored = StoredAdjudication.make({
      title: delimiterMimic,
      disposition: "refuted",
      actor: "dan",
    });
    const anchored = StoredAdjudication.make({
      ...finding,
      disposition: "refuted",
      actor: "dan",
    });

    expect(adjudicationIdentity(anchored)).toBe(findingIdentity(finding));
    expect(adjudicationIdentity(unanchored)).toBe(concernIdentity(unanchored));
    expect(adjudicationIdentity(unanchored)).not.toBe(findingIdentity(finding));
  });

  it("rejects adjudications with partial locations at the persisted-state boundary", () => {
    const common = {
      title: "Still requires attention",
      disposition: "accepted-risk",
      actor: "dan",
    } as const;
    for (const partial of [
      { path: "src/accepted.ts" },
      { startLine: 1 },
      { endLine: 1 },
      { path: "src/accepted.ts", startLine: 1 },
      { path: "src/accepted.ts", endLine: 1 },
      { startLine: 1, endLine: 1 },
    ]) {
      expect(Schema.decodeUnknownExit(StoredAdjudication)({ ...common, ...partial })._tag).toBe(
        "Failure",
      );
    }
    expect(Schema.decodeUnknownExit(StoredAdjudication)(common)._tag).toBe("Success");
    expect(
      Schema.decodeUnknownExit(StoredAdjudication)({
        ...common,
        path: "src/accepted.ts",
        startLine: 1,
        endLine: 1,
      })._tag,
    ).toBe("Success");
  });

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
          yield* authenticate("<!-- effect-agent-pr-review state-v2:not-base64 -->"),
        ),
      ).toBeUndefined();
    }),
  );

  it.effect("round-trips adjudications and still decodes markers without the field", () =>
    Effect.gen(function* () {
      const secret = Redacted.make("stable-state-secret");
      const roundTrip = (state: ReviewState) =>
        Effect.gen(function* () {
          const authenticator = yield* ReviewStateAuthenticator;
          const marker = yield* authenticator.render(state);
          return yield* authenticator.extract(`review body\n${marker}`);
        }).pipe(Effect.provide(webCryptoReviewStateAuthenticatorLayer(secret)));
      const adjudicated = ReviewState.make({
        ...priorState,
        adjudications: [
          StoredAdjudication.make({
            path: "src/accepted.ts",
            startLine: 1,
            endLine: 1,
            title: "Still requires attention",
            disposition: "accepted-risk",
            reason: "Known cost, accepted for launch",
            actor: "dan",
          }),
          StoredAdjudication.make({
            title: "No rollout note",
            disposition: "refuted",
            actor: "dan",
          }),
        ],
      });
      expect(Option.getOrUndefined(yield* roundTrip(adjudicated))).toEqual(adjudicated);
      // A marker signed before the field existed carries no `adjudications`
      // key and must keep decoding unchanged.
      const legacy = Option.getOrUndefined(yield* roundTrip(priorState));
      expect(legacy).toEqual(priorState);
      expect(legacy?.adjudications).toBeUndefined();
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

  it("falls back to the full diff for legacy pathless concerns", () => {
    const legacyState = ReviewState.make({
      ...priorState,
      unresolvedConcerns: [
        StoredReviewConcern.make({
          severity: "blocking",
          title: "Legacy concern",
          body: "This state predates affected-path tracking.",
        }),
      ],
    });
    const selection = select({ priorState: legacyState });

    expect(selection.mode).toBe("full");
    expect(selection.reason).toContain("concerns predate affected-path tracking");
  });

  it("reopens every current evidence path when a concern path changes", () => {
    const concernState = ReviewState.make({
      ...priorState,
      unresolvedConcerns: [
        StoredReviewConcern.make({
          evidencePaths: ["src/accepted.ts", "src/corrective.ts"],
          severity: "blocking",
          title: "Cross-file cutover is incomplete",
          body: "Both files are needed to reassess the cutover.",
        }),
      ],
    });
    const selection = select({ priorState: concernState });

    expect(selection.mode).toBe("incremental");
    expect(selection.files.map((file) => file.path)).toEqual([
      "src/accepted.ts",
      "src/corrective.ts",
    ]);
    expect(selection.affectedPaths).toEqual(["src/accepted.ts", "src/corrective.ts"]);
    expect(selection.reason).toContain("reopening 1 related concern path(s) for context");
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
    // Paths without a recorded failed stage receive fresh discovery.
    expect(selection.affectedPaths).toContain("src/accepted.ts");
    expect(selection.retryPaths).toEqual([]);
    expect(selection.reason).toContain("retrying 1 carried unreviewed path(s)");
  });

  it("retries an unchanged leftover without invalidating its stored findings", () => {
    const carryingState = ReviewState.make({
      ...priorState,
      unreviewedPaths: ["src/accepted.ts"],
      unreviewedPasses: [
        StoredUnreviewedPass.make({ stage: "specialist", paths: ["src/accepted.ts"] }),
      ],
      settled: false,
    });
    const selection = select({ priorState: carryingState });

    expect(selection.mode).toBe("incremental");
    expect(selection.files.map((file) => file.path)).toEqual([
      "src/accepted.ts",
      "src/corrective.ts",
    ]);
    expect(selection.affectedPaths).not.toContain("src/accepted.ts");
    expect(selection.retryPasses).toEqual([
      StoredUnreviewedPass.make({ stage: "specialist", paths: ["src/accepted.ts"] }),
    ]);
    expect(selection.retryPaths).toEqual(["src/accepted.ts"]);
    expect(selection.retryStages).toEqual(["specialist"]);
    expect(selection.reason).toContain("by recorded failed stage");
  });

  it("preserves different failed stages for different unchanged paths", () => {
    const queuedFile = ChangedFile.make({
      path: "src/queued.ts",
      status: "modified",
      additions: 1,
      deletions: 1,
      patch: "@@ -1 +1 @@\n-old\n+queued",
    });
    const carryingState = ReviewState.make({
      ...priorState,
      unreviewedPaths: ["src/accepted.ts", "src/queued.ts"],
      unreviewedPasses: [
        StoredUnreviewedPass.make({ stage: "discovery", paths: ["src/accepted.ts"] }),
        StoredUnreviewedPass.make({ stage: "specialist", paths: ["src/queued.ts"] }),
      ],
      settled: false,
    });
    const selection = select({
      current: PullRequestMetadata.make({ ...metadata, totalChangedFiles: 3 }),
      fullFiles: [acceptedFile, correctiveFile, queuedFile],
      priorState: carryingState,
    });

    expect(selection.retryPasses).toEqual([
      StoredUnreviewedPass.make({ stage: "discovery", paths: ["src/accepted.ts"] }),
      StoredUnreviewedPass.make({ stage: "specialist", paths: ["src/queued.ts"] }),
    ]);
  });

  it("reviews only content-changed PR paths after a rewritten head", () => {
    const rewritten = ReviewHeadComparison.make({
      status: "diverged",
      baseSha: REVIEWED_HEAD_SHA,
      headSha: CURRENT_HEAD_SHA,
      mergeBaseSha: "5".repeat(40),
      files: [acceptedFile, correctiveFile],
      truncated: false,
    });
    const contentComparison = ReviewTreeComparison.make({
      baseSha: REVIEWED_HEAD_SHA,
      headSha: CURRENT_HEAD_SHA,
      changedPaths: [correctiveFile.path],
      truncated: false,
    });
    const selection = select({ comparison: rewritten, contentComparison });

    expect(selection.mode).toBe("incremental");
    expect(selection.files).toEqual([correctiveFile]);
    expect(selection.files[0]?.patch).toBe(correctiveFile.patch);
    expect(selection.reason).toContain("rewritten history");
    expect(selection.affectedPaths).toEqual(["src/corrective.ts"]);
  });

  it("hydrates renamed snapshot paths from the current PR record", () => {
    const renamedFile = ChangedFile.make({
      path: "src/current.ts",
      previousPath: "src/previous.ts",
      status: "renamed",
      additions: 2,
      deletions: 1,
      patch: "@@ -1 +1,2 @@\n-old\n+new\n+line",
    });
    const selection = select({
      comparison: ReviewHeadComparison.make({
        ...comparison,
        status: "diverged",
        mergeBaseSha: "5".repeat(40),
      }),
      fullFiles: [renamedFile, correctiveFile],
      contentComparison: ReviewTreeComparison.make({
        baseSha: REVIEWED_HEAD_SHA,
        headSha: CURRENT_HEAD_SHA,
        changedPaths: ["src/previous.ts"],
        truncated: false,
      }),
    });

    expect(selection.mode).toBe("incremental");
    expect(selection.files).toEqual([renamedFile]);
    expect(selection.files[0]?.previousPath).toBe("src/previous.ts");
    expect(selection.files[0]?.patch).toBe(renamedFile.patch);
  });

  it("falls back to the full PR for truncated or failed tree snapshots", () => {
    const rewritten = ReviewHeadComparison.make({
      ...comparison,
      status: "diverged",
      mergeBaseSha: "5".repeat(40),
    });
    const truncated = select({
      comparison: rewritten,
      contentComparison: ReviewTreeComparison.make({
        baseSha: REVIEWED_HEAD_SHA,
        headSha: CURRENT_HEAD_SHA,
        changedPaths: [],
        truncated: true,
      }),
    });
    expect(truncated.mode).toBe("full");
    expect(truncated.files).toEqual([acceptedFile, correctiveFile]);
    expect(truncated.reason).toContain("tree snapshot comparison was truncated");

    const failed = select({
      comparison: rewritten,
      contentComparison: undefined,
      contentComparisonFailure: "ResponseError: status 404",
    });
    expect(failed.mode).toBe("full");
    expect(failed.files).toEqual([acceptedFile, correctiveFile]);
    expect(failed.reason).toContain("tree snapshot comparison failed");
    expect(failed.reason).toContain("status 404");
  });

  it("keeps a carried unreviewed path in scope across a rename", () => {
    const renamedFile = ChangedFile.make({
      path: "src/renamed.ts",
      previousPath: "src/accepted.ts",
      status: "renamed",
      additions: 0,
      deletions: 0,
      patch: "@@ -1 +1 @@\n-old\n+accepted",
    });
    const carryingState = ReviewState.make({
      ...priorState,
      unreviewedPaths: ["src/accepted.ts"],
      settled: false,
    });
    const selection = select({
      priorState: carryingState,
      fullFiles: [renamedFile, correctiveFile],
    });

    expect(selection.mode).toBe("incremental");
    expect(selection.files.map((file) => file.path)).toEqual([
      "src/corrective.ts",
      "src/renamed.ts",
    ]);
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
