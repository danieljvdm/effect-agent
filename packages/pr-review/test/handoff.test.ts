import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Ref } from "effect";

import {
  ChangedFile,
  CodeReview,
  computeReviewHandoffDigest,
  makeReviewHandoff,
  PrReview,
  PullRequestMetadata,
  ReviewFinding,
  ReviewHandoffAuthenticator,
  type ReviewPublicationPlan,
  webCryptoReviewHandoffAuthenticatorLayer,
} from "../src/index.ts";
import {
  FixtureFile,
  FixturePullRequest,
  collectingReviewPublisherLayer,
  fixturePullRequestSourceLayer,
  makeOfflineReviewerModel,
} from "../src/testing.ts";

const HEAD_SHA = "a".repeat(40);
const file = ChangedFile.make({
  path: "src/value.ts",
  status: "modified",
  additions: 1,
  deletions: 1,
  patch: "@@ -1 +1 @@\n-export const value = 1;\n+export const value = 2;",
});
const fixture = FixturePullRequest.make({
  metadata: PullRequestMetadata.make({
    repository: "acme/widgets",
    number: 42,
    title: "Correct the exported value",
    body: "",
    baseRef: "main",
    baseSha: "b".repeat(40),
    headRef: "fix/value",
    headSha: HEAD_SHA,
    totalChangedFiles: 1,
  }),
  files: [
    FixtureFile.make({
      file,
      headContent: "export const value = 2;",
    }),
  ],
});
const important = ReviewFinding.make({
  path: "src/value.ts",
  startLine: 1,
  endLine: 1,
  severity: "important",
  category: "correctness",
  title: "The exported value is still wrong",
  body: "This public constant must be 3.",
  suggestion: "export const value = 3;",
});

describe("review remediation handoff", () => {
  it.effect("builds an immutable exact-head handoff and rejects tampering", () =>
    Effect.gen(function* () {
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: file.path,
        readPath: file.path,
        review: CodeReview.make({
          summary: "One important defect remains.",
          verdict: "comment",
          findings: [important],
        }),
      });
      const reviewer = PrReview.make({ model: scripted.model });
      const source = fixturePullRequestSourceLayer(fixture);
      const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
      const outcome = yield* reviewer
        .run()
        .pipe(Effect.provide(Layer.merge(source, collectingReviewPublisherLayer(published))));
      const snapshot = yield* reviewer.snapshot.pipe(Effect.provide(source));
      const profileFingerprint = yield* reviewer.profileFingerprint.pipe(Effect.provide(source));
      const handoff = yield* makeReviewHandoff({
        outcome,
        metadata: snapshot.metadata,
        files: snapshot.files,
        profileFingerprint,
      });

      expect(Object.isFrozen(handoff)).toBe(true);
      expect(Object.isFrozen(handoff.findings)).toBe(true);
      expect(handoff.reviewedHeadSha).toBe(HEAD_SHA);
      expect(handoff.findings).toHaveLength(1);
      expect(handoff.findings[0]?.suggestion).toBe(important.suggestion);
      expect(handoff.findings[0]?.id).toMatch(/^[0-9a-f]{64}$/);
      expect(yield* computeReviewHandoffDigest(handoff)).toMatch(/^[0-9a-f]{64}$/);
      const invalidAnchor = yield* makeReviewHandoff({
        outcome,
        metadata: snapshot.metadata,
        files: [],
        profileFingerprint,
      }).pipe(Effect.flip);
      expect(invalidAnchor._tag).toBe("ReviewHandoffBuildFailure");

      const authenticated = yield* Effect.gen(function* () {
        const authenticator = yield* ReviewHandoffAuthenticator;
        const envelope = yield* authenticator.sign(handoff);
        const verified = yield* authenticator.verify(envelope);
        const failure = yield* authenticator
          .verify({ ...envelope, signature: "0".repeat(64) })
          .pipe(Effect.flip);
        return { envelope, verified, failure };
      }).pipe(
        Effect.provide(
          webCryptoReviewHandoffAuthenticatorLayer(Redacted.make("handoff-test-secret")),
        ),
      );
      expect(authenticated.verified).toEqual(handoff);
      expect(authenticated.failure._tag).toBe("ReviewHandoffAuthenticationFailure");
      expect(authenticated.envelope.handoff.findings[0]?.id).toBe(handoff.findings[0]?.id);
    }),
  );
});
