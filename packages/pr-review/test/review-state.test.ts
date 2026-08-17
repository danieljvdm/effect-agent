import { expect, layer } from "@effect/vitest";
import { Effect, Option, Redacted } from "effect";

import * as publicApi from "../src/index.ts";
import {
  ChangedFile,
  PullRequestMetadata,
  ReviewHeadComparison,
  reviewSelectionAuthorityLayer,
  ReviewState,
  ReviewStateAuthenticator,
  StoredReviewFinding,
  webCryptoReviewStateAuthenticatorLayer,
} from "../src/index.ts";
import { selectedReviewRangeFor, selectReviewRange } from "../src/internal/review-state.ts";

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
  acceptedScopeFingerprint: SCOPE_FINGERPRINT,
  reviewedPathCount: 1,
  unresolvedFindings: [unresolvedFinding],
  unresolvedConcerns: [],
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

layer(reviewSelectionAuthorityLayer)("review state", (it) => {
  it("exposes selection issuance only through explicit host authority", () => {
    expect("selectReviewRange" in publicApi).toBe(false);
    expect("selectedReviewRangeFor" in publicApi).toBe(false);
    expect("sealedReviewSelection" in publicApi).toBe(false);
    expect("ReviewSelectionAuthority" in publicApi).toBe(true);
    expect("reviewSelectionAuthorityLayer" in publicApi).toBe(true);
  });

  it.effect(
    "invalidates a sealed selection when an aliased source array changes after selection",
    () =>
      Effect.gen(function* () {
        const fullFiles = [acceptedFile, correctiveFile];
        const selection = yield* selectReviewRange({
          requestedMode: "final",
          current: metadata,
          fullFiles,
          profileFingerprint: PROFILE_FINGERPRINT,
          priorState: undefined,
          comparison: undefined,
        });

        expect(
          (yield* selectedReviewRangeFor(selection, metadata))?.files.map((file) => file.path),
        ).toEqual(["src/accepted.ts", "src/corrective.ts"]);
        fullFiles[0] = correctiveFile;

        expect(yield* selectedReviewRangeFor(selection, metadata)).toBeUndefined();
      }),
  );

  it.effect("does not share selection authority across host compositions", () =>
    Effect.gen(function* () {
      const selection = yield* selectReviewRange({
        requestedMode: "final",
        current: metadata,
        fullFiles: [acceptedFile, correctiveFile],
        profileFingerprint: PROFILE_FINGERPRINT,
        priorState: undefined,
        comparison: undefined,
      }).pipe(Effect.provide(reviewSelectionAuthorityLayer));

      const resolved = yield* selectedReviewRangeFor(selection, metadata).pipe(
        Effect.provide(reviewSelectionAuthorityLayer),
      );
      expect(resolved).toBeUndefined();
    }),
  );

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

  it.effect("reviews only the corrective delta and preserves unchanged prior scope", () =>
    Effect.gen(function* () {
      const selection = yield* select();
      expect(selection.mode).toBe("incremental");
      expect(selection.baselineSha).toBe(REVIEWED_HEAD_SHA);
      expect(selection.files.map((file) => file.path)).toEqual(["src/corrective.ts"]);
      expect(selection.files.map((file) => file.path)).not.toContain("src/accepted.ts");
      expect(selection.priorState?.unresolvedFindings).toEqual([unresolvedFinding]);
      expect(selection.reason).toContain(REVIEWED_HEAD_SHA.slice(0, 7));
    }),
  );

  it.effect("falls back to the full diff when state is missing or incompatible", () =>
    Effect.gen(function* () {
      const missing = yield* select({ priorState: undefined, comparison: undefined });
      expect(missing.mode).toBe("full");
      expect(missing.reason).toContain("no compatible stored review state");

      const wrongProfile = yield* select({ profileFingerprint: "c".repeat(64) });
      expect(wrongProfile.mode).toBe("full");
      expect(wrongProfile.reason).toContain("profile or model configuration changed");

      const changedBase = yield* select({
        current: PullRequestMetadata.make({ ...metadata, baseSha: "4".repeat(40) }),
      });
      expect(changedBase.mode).toBe("full");
      expect(changedBase.reason).toContain("base changed");
    }),
  );

  it.effect("falls back to the full diff for unsafe or incomplete head comparisons", () =>
    Effect.gen(function* () {
      const diverged = yield* select({
        comparison: ReviewHeadComparison.make({ ...comparison, status: "diverged" }),
      });
      expect(diverged.mode).toBe("full");
      expect(diverged.reason).toContain("not an ancestor");

      const truncated = yield* select({
        comparison: ReviewHeadComparison.make({ ...comparison, truncated: true }),
      });
      expect(truncated.mode).toBe("full");
      expect(truncated.reason).toContain("file bound");

      const unavailable = yield* select({ comparison: undefined });
      expect(unavailable.mode).toBe("full");
      expect(unavailable.reason).toContain("comparison was unavailable");
    }),
  );

  it.effect("keeps an ancestor base advance incremental and includes overlapping PR context", () =>
    Effect.gen(function* () {
      const nextBase = "4".repeat(40);
      const baseComparison = ReviewHeadComparison.make({
        status: "ahead",
        baseSha: BASE_SHA,
        headSha: nextBase,
        mergeBaseSha: BASE_SHA,
        files: [acceptedFile],
        truncated: false,
      });
      const selection = yield* select({
        current: PullRequestMetadata.make({ ...metadata, baseSha: nextBase }),
        baseComparison,
      });
      expect(selection.mode).toBe("incremental");
      expect(selection.files.map((file) => file.path)).toEqual([
        "src/accepted.ts",
        "src/corrective.ts",
      ]);
      expect(selection.reason).toContain("base advanced");

      const rewrittenBase = yield* select({
        current: PullRequestMetadata.make({ ...metadata, baseSha: nextBase }),
        baseComparison: ReviewHeadComparison.make({
          ...baseComparison,
          status: "diverged",
          mergeBaseSha: "5".repeat(40),
        }),
      });
      expect(rewrittenBase.mode).toBe("full");
      expect(rewrittenBase.reason).toContain("changed materially");
    }),
  );

  it.effect("performs a bounded full-diff audit only when final mode is explicit", () =>
    Effect.gen(function* () {
      const selection = yield* select({ requestedMode: "final" });
      expect(selection.mode).toBe("full");
      expect(selection.files.map((file) => file.path)).toEqual([
        "src/accepted.ts",
        "src/corrective.ts",
      ]);
      expect(selection.reason).toBe("explicit final full-diff audit requested");
      expect(selection.priorState).toBe(undefined);
    }),
  );
});
