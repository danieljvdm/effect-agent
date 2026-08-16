import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";

import {
  ChangedFile,
  CodeReview,
  computeChangesetFingerprint,
  extractFingerprint,
  planPublication,
  PrReview,
  PullRequestMetadata,
  renderFingerprintMarker,
  ReviewFinding,
} from "../src/index.ts";
import {
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  makeOfflineReviewerModel,
} from "../src/testing.ts";

const fileA = ChangedFile.make({
  path: "src/a.ts",
  status: "modified",
  additions: 1,
  deletions: 0,
  patch: "@@ -1,1 +1,2 @@\n const a = 1;\n+const b = 2;",
});
const fileB = ChangedFile.make({
  path: "src/b.ts",
  status: "added",
  additions: 1,
  deletions: 0,
  patch: "@@ -0,0 +1,1 @@\n+export const b = 2;",
});

describe("fingerprint marker", () => {
  it("round-trips and extracts the LAST marker in a body", () => {
    const first = "1".repeat(64);
    const second = "2".repeat(64);
    const body = [
      "A review summary.",
      renderFingerprintMarker(first),
      "An amended paragraph.",
      renderFingerprintMarker(second),
    ].join("\n");
    expect(extractFingerprint(body)).toBe(second);
    expect(extractFingerprint("no marker here")).toBeUndefined();
  });
});

describe("computeChangesetFingerprint", () => {
  it.effect("ignores provider file ordering but not content or signature", () =>
    Effect.gen(function* () {
      const forward = yield* computeChangesetFingerprint([fileA, fileB], "sig");
      const reversed = yield* computeChangesetFingerprint([fileB, fileA], "sig");
      expect(forward).toBe(reversed);
      expect(forward).toMatch(/^[0-9a-f]{64}$/);

      const editedPatch = ChangedFile.make({ ...fileA, patch: `${fileA.patch}\n+const c = 3;` });
      expect(yield* computeChangesetFingerprint([editedPatch, fileB], "sig")).not.toBe(forward);
      expect(yield* computeChangesetFingerprint([fileA, fileB], "other-sig")).not.toBe(forward);
      expect(yield* computeChangesetFingerprint([fileA], "sig")).not.toBe(forward);

      const recovered = ChangedFile.make({
        path: "db/snapshot.json",
        status: "added",
        additions: 1,
        deletions: 0,
        reviewHeadContent: '{"version":1}',
      });
      const editedRecovered = ChangedFile.make({
        ...recovered,
        reviewHeadContent: '{"version":2}',
      });
      expect(yield* computeChangesetFingerprint([editedRecovered], "sig")).not.toBe(
        yield* computeChangesetFingerprint([recovered], "sig"),
      );
    }),
  );
});

describe("plan fingerprint embedding", () => {
  const review = CodeReview.make({
    summary: "Looks fine.",
    verdict: "approve",
    findings: [],
  });
  const SHA = "abcdefabcdefabcdefabcdefabcdefabcdefabcd";

  it("embeds the marker and keeps it under the body cap", () => {
    const fingerprint = "f".repeat(64);
    const plan = planPublication(review, [fileA], {
      applyVerdict: false,
      headSha: SHA,
      totalChangedFiles: 1,
      fingerprint,
    });
    expect(extractFingerprint(plan.body)).toBe(fingerprint);

    // Even a body at the cap keeps the marker intact at its tail.
    const huge = CodeReview.make({
      summary: "x".repeat(4_000),
      verdict: "comment",
      findings: Array.from({ length: 20 }, (_, index) =>
        ReviewFinding.make({
          path: "src/a.ts",
          startLine: 99,
          endLine: 99,
          severity: "nit",
          title: `Demoted ${index + 1}`,
          body: "y".repeat(2_000),
        }),
      ),
    });
    const hugePlan = planPublication(huge, [fileA], {
      applyVerdict: false,
      headSha: SHA,
      totalChangedFiles: 1,
      fingerprint,
    });
    expect(hugePlan.body.length).toBeLessThanOrEqual(60_000);
    expect(extractFingerprint(hugePlan.body)).toBe(fingerprint);
  });

  it("embeds no marker without a fingerprint", () => {
    const plan = planPublication(review, [fileA], {
      applyVerdict: false,
      headSha: SHA,
      totalChangedFiles: 1,
    });
    expect(extractFingerprint(plan.body)).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// The factory's fingerprint: stable for one configuration, different across
// configurations, and blind to ignored files.
// ---------------------------------------------------------------------------

describe("reviewer fingerprint", () => {
  const fixtureWith = (files: ReadonlyArray<ChangedFile>) =>
    FixturePullRequest.make({
      metadata: PullRequestMetadata.make({
        repository: "acme/widgets",
        number: 3,
        title: "Change things",
        body: "",
        baseRef: "main",
        headRef: "change/things",
        headSha: "1234567890abcdef1234567890abcdef12345678",
        totalChangedFiles: files.length,
      }),
      files: files.map((file) => FixtureFile.make({ file })),
    });

  it.effect("is stable per configuration and configuration-sensitive", () =>
    Effect.gen(function* () {
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: "src/a.ts",
        readPath: "src/a.ts",
        review: CodeReview.make({ summary: "ok", verdict: "approve", findings: [] }),
      });
      const source = fixturePullRequestSourceLayer(fixtureWith([fileA, fileB]));

      const plain = PrReview.make({ model: scripted.model });
      const first = yield* plain.fingerprint.pipe(Effect.provide(source));
      const second = yield* plain.fingerprint.pipe(Effect.provide(source));
      expect(first).toBe(second);

      const guided = PrReview.make({ model: scripted.model, guidance: "Focus on security." });
      expect(yield* guided.fingerprint.pipe(Effect.provide(source))).not.toBe(first);

      const fanOut = PrReview.makeFanOut({ model: scripted.model });
      expect(yield* fanOut.fingerprint.pipe(Effect.provide(source))).not.toBe(first);
    }),
  );

  it.effect("does not change when only an ignored file changes", () =>
    Effect.gen(function* () {
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: "src/a.ts",
        readPath: "src/a.ts",
        review: CodeReview.make({ summary: "ok", verdict: "approve", findings: [] }),
      });
      const lockV1 = ChangedFile.make({
        path: "bun.lock",
        status: "modified",
        additions: 1,
        deletions: 1,
        patch: "@@ -1,1 +1,1 @@\n-v1\n+v2",
      });
      const lockV2 = ChangedFile.make({ ...lockV1, patch: "@@ -1,1 +1,1 @@\n-v1\n+v3" });
      const reviewer = PrReview.make({ model: scripted.model, ignore: ["**/*.lock"] });

      const before = yield* reviewer.fingerprint.pipe(
        Effect.provide(fixturePullRequestSourceLayer(fixtureWith([fileA, lockV1]))),
      );
      const after = yield* reviewer.fingerprint.pipe(
        Effect.provide(fixturePullRequestSourceLayer(fixtureWith([fileA, lockV2]))),
      );
      expect(before).toBe(after);
    }),
  );
});
