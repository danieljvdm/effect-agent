import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  ConfigProvider,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Redacted,
  Ref,
  Schema,
} from "effect";
import { PlatformError, SystemError } from "effect/PlatformError";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  GuidanceFileUnreadable,
  InvalidMaxDurationInput,
  MAX_GUIDANCE_FILE_CHARS,
  resolveActionInputs,
  resolveGuidance,
  ReviewGateFailed,
  runReviewAction,
  type ReviewActionResult,
} from "../src/action.ts";
import {
  ChangedFile,
  CodeReview,
  InvalidEffortInput,
  planPublication,
  PublishedReview,
  PullRequestMetadata,
  ReviewCoverage,
  ReviewAssurance,
  ReviewFinding,
  ReviewInputCoverage,
  ReviewRunOutcome,
  ReviewState,
  StoredReviewFinding,
  webCryptoReviewStateAuthenticatorLayer,
  type ReviewVerdict,
} from "../src/index.ts";
import { staticPriorReviews } from "../src/testing.ts";

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Effect to fail");
  }
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed failure in the Cause");
  }
  return failure.value;
};

// ---------------------------------------------------------------------------
// Input parsing.
// ---------------------------------------------------------------------------

const withEnv = (env: Record<string, string>) =>
  Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnvRecord(env)));

describe("resolveActionInputs", () => {
  it.effect("defaults every input", () =>
    Effect.gen(function* () {
      const inputs = yield* resolveActionInputs().pipe(withEnv({}));
      expect(inputs).toEqual({
        provider: "openai",
        model: undefined,
        effort: undefined,
        post: true,
        applyVerdict: false,
        fanOut: true,
        guidance: undefined,
        guidanceFile: undefined,
        ignore: [],
        maxFindings: undefined,
        maxDurationMinutes: undefined,
        skipUnchanged: true,
        retireStaleReviews: true,
        progressComment: true,
        reviewMode: "incremental",
      });
    }),
  );

  it.effect("parses every configured input", () =>
    Effect.gen(function* () {
      const inputs = yield* resolveActionInputs().pipe(
        withEnv({
          PR_REVIEW_PROVIDER: "anthropic",
          PR_REVIEW_MODEL: "claude-sonnet-5",
          PR_REVIEW_EFFORT: "xhigh",
          PR_REVIEW_POST: "false",
          PR_REVIEW_APPLY_VERDICT: "true",
          PR_REVIEW_FAN_OUT: "false",
          PR_REVIEW_GUIDANCE: "Flag naked Promises.",
          PR_REVIEW_GUIDANCE_FILE: ".github/review-guidance.md",
          PR_REVIEW_IGNORE: " **/*.lock , dist/** ,",
          PR_REVIEW_MAX_FINDINGS: "7",
          PR_REVIEW_MAX_DURATION_MINUTES: "12",
          PR_REVIEW_SKIP_UNCHANGED: "false",
          PR_REVIEW_RETIRE_STALE_REVIEWS: "false",
          PR_REVIEW_PROGRESS_COMMENT: "false",
          PR_REVIEW_MODE: "final",
        }),
      );
      expect(inputs).toEqual({
        provider: "anthropic",
        model: "claude-sonnet-5",
        effort: 0.75,
        post: false,
        applyVerdict: true,
        fanOut: false,
        guidance: "Flag naked Promises.",
        guidanceFile: ".github/review-guidance.md",
        ignore: ["**/*.lock", "dist/**"],
        maxFindings: 7,
        maxDurationMinutes: 12,
        skipUnchanged: false,
        retireStaleReviews: false,
        progressComment: false,
        reviewMode: "final",
      });
    }),
  );

  it.effect("accepts a numeric effort position", () =>
    Effect.gen(function* () {
      const inputs = yield* resolveActionInputs().pipe(withEnv({ PR_REVIEW_EFFORT: "0.6" }));
      expect(inputs.effort).toBe(0.6);
    }),
  );

  it.effect("fails typed on an effort that is neither a name nor a position", () =>
    Effect.gen(function* () {
      const exit = yield* resolveActionInputs().pipe(
        withEnv({ PR_REVIEW_EFFORT: "extreme" }),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      expect(Schema.is(InvalidEffortInput)(failure)).toBe(true);
    }),
  );

  it.effect("fails typed on a non-positive max duration", () =>
    Effect.gen(function* () {
      const exit = yield* resolveActionInputs().pipe(
        withEnv({ PR_REVIEW_MAX_DURATION_MINUTES: "0" }),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      expect(Schema.is(InvalidMaxDurationInput)(failure)).toBe(true);
    }),
  );
});

// ---------------------------------------------------------------------------
// The action harness around one reviewer run: skips, outputs, and the gate.
// The reviewer itself is a stub — the harness is what's under test.
// ---------------------------------------------------------------------------

const EVENT_PATH = "/tmp/github-event.json";
const OUTPUT_PATH = "/tmp/github-output";

/** Typed stand-in for a reviewer-run failure in the progress-settle test. */
class ModelUnavailable extends Schema.TaggedError<ModelUnavailable>()("ModelUnavailable", {}) {}

const fakeOutcome = (
  verdict: ReviewVerdict,
  options: {
    readonly blocking?: boolean;
    readonly incomplete?: boolean;
    readonly assuranceIncomplete?: boolean;
    readonly state?: ReviewState;
  } = {},
): ReviewRunOutcome => {
  const findings = options.blocking
    ? [
        ReviewFinding.make({
          path: "src/a.ts",
          startLine: 1,
          endLine: 1,
          severity: "blocking",
          title: "Unsafe behavior",
          body: "This must be corrected before merge.",
        }),
      ]
    : [];
  const review = CodeReview.make({ summary: "Stubbed review.", verdict, findings });
  return ReviewRunOutcome.make({
    review,
    activeFindings: findings,
    activeConcerns: review.concerns ?? [],
    coverage: ReviewCoverage.make({
      status: options.incomplete || options.assuranceIncomplete ? "incomplete" : "complete",
      requiredPaths: [],
      reviewedPaths: [],
      unreviewedPaths: [],
      failedUnits: [],
      reasons:
        options.incomplete || options.assuranceIncomplete
          ? ["configured review work did not settle"]
          : [],
    }),
    inputCoverage: ReviewInputCoverage.make({
      status: options.incomplete ? "incomplete" : "complete",
      requiredPaths: [],
      assignedPaths: [],
      partialPaths: [],
      unassignedPaths: [],
      undiffablePaths: [],
      reasons: options.incomplete ? ["review input was not completely assigned"] : [],
    }),
    assurance: ReviewAssurance.make({
      status: options.incomplete || options.assuranceIncomplete ? "incomplete" : "settled",
      requiredGeneralDiscoveryPasses: 1,
      completedGeneralDiscoveryPasses: options.incomplete || options.assuranceIncomplete ? 0 : 1,
      requiredSpecialistPasses: 0,
      completedSpecialistPasses: 0,
      requiredVerificationPasses: 0,
      completedVerificationPasses: 0,
      discoveredCandidates: 0,
      confirmedCandidates: 0,
      rejectedCandidates: 0,
      unsettledCandidates: 0,
      discardedInvalidFindings: 0,
      failedPasses: [],
      reasons:
        options.incomplete || options.assuranceIncomplete
          ? ["review discovery did not settle"]
          : [],
    }),
    unreviewedPaths: options.incomplete || options.assuranceIncomplete ? ["src/a.ts"] : [],
    plan: planPublication(review, [], {
      applyVerdict: false,
      headSha: "abcdefabcdefabcdefabcdefabcdefabcdefabcd",
      totalChangedFiles: 0,
    }),
    ...(options.state === undefined
      ? {}
      : {
          state: options.state,
          published: PublishedReview.make({
            reviewId: 12,
            url: "memory://review/12",
            event: "COMMENT",
            inlineComments: 0,
            authorNodeId: "BOT_action-reviewer",
            submittedAt: DateTime.makeUnsafe("2026-08-15T20:00:00Z"),
          }),
        }),
    turns: 1,
  });
};

/** Environment harness: event payload + captured GITHUB_OUTPUT writes. */
const actionHarness = (eventJson: string | undefined) =>
  Effect.gen(function* () {
    const written = yield* Ref.make("");
    const requests = yield* Ref.make<ReadonlyArray<string>>([]);
    const fs = FileSystem.makeNoop({
      readFileString: (path) =>
        eventJson !== undefined && path === EVENT_PATH
          ? Effect.succeed(eventJson)
          : Effect.die(new Error(`Unexpected read: ${path}`)),
      writeFileString: (path, data) =>
        path === OUTPUT_PATH
          ? Ref.update(written, (previous) => previous + data)
          : Effect.die(new Error(`Unexpected write: ${path}`)),
    });
    const env: Record<string, string> = {
      GITHUB_OUTPUT: OUTPUT_PATH,
      GITHUB_REPOSITORY: "acme/widgets",
      ...(eventJson !== undefined ? { GITHUB_EVENT_PATH: EVENT_PATH } : {}),
    };
    const httpClient = HttpClient.make((request, url) =>
      Ref.update(requests, (previous) => [...previous, `${request.method} ${url.href}`]).pipe(
        Effect.as(
          HttpClientResponse.fromWeb(
            request,
            new Response("[]", { status: 200, headers: { "content-type": "application/json" } }),
          ),
        ),
      ),
    );
    const layer = Layer.mergeAll(
      Layer.succeed(FileSystem.FileSystem)(fs),
      Layer.succeed(HttpClient.HttpClient)(httpClient),
      ConfigProvider.layer(ConfigProvider.fromEnvRecord(env)),
      webCryptoReviewStateAuthenticatorLayer(Redacted.make("test-review-state-secret")),
    );
    return { requests, written, layer };
  });

describe("runReviewAction", () => {
  it.effect("skips a non-pull-request event without invoking the reviewer", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(JSON.stringify({}));
      const invoked = yield* Ref.make(0);
      const result = yield* runReviewAction({
        run: () =>
          Ref.update(invoked, (count) => count + 1).pipe(Effect.map(() => fakeOutcome("comment"))),
      }).pipe(Effect.provide(harness.layer));
      expect(result._tag).toBe("Skipped");
      expect(yield* Ref.get(invoked)).toBe(0);
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("skipped=true");
      expect(outputs).toContain("skip-reason=the triggering event carries no pull request");
    }),
  );

  it.effect("skips a draft pull request", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5, draft: true },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const result = yield* runReviewAction({
        run: () => Effect.succeed(fakeOutcome("comment")),
      }).pipe(Effect.provide(harness.layer));
      expect(result._tag).toBe("Skipped");
      expect(yield* Ref.get(harness.written)).toContain("skip-reason=the pull request is a draft");
    }),
  );

  it.effect("completes a run and writes the step outputs", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const result: ReviewActionResult = yield* runReviewAction(
        { run: () => Effect.succeed(fakeOutcome("comment")) },
        { post: false },
      ).pipe(Effect.provide(harness.layer));
      expect(result._tag).toBe("Completed");
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("skipped=false");
      expect(outputs).toContain("verdict=comment");
      expect(outputs).toContain("inline-comments=0");
      expect(outputs).toContain("conclusion=success");
      expect(outputs).toContain("input-coverage=complete");
      expect(outputs).toContain("review-assurance=settled");
      expect(outputs).toContain("coverage=complete");
    }),
  );

  it.effect(
    "starts retirement only for an enabled state-bearing post with a complete receipt",
    () =>
      Effect.gen(function* () {
        const harness = yield* actionHarness(
          JSON.stringify({
            pull_request: { number: 5 },
            repository: { full_name: "acme/widgets" },
          }),
        );
        const state = ReviewState.make({
          version: 2,
          repository: "acme/widgets",
          pullRequestNumber: 5,
          baseRef: "main",
          baseSha: "1".repeat(40),
          headRef: "fix/review",
          reviewedHeadSha: "2".repeat(40),
          profileFingerprint: "a".repeat(64),
          acceptedScopeFingerprint: "b".repeat(64),
          reviewedPathCount: 0,
          unresolvedFindings: [],
          unresolvedConcerns: [],
          unreviewedPaths: [],
          settled: true,
          lastReviewMode: "full",
        });

        yield* runReviewAction({
          run: () => Effect.succeed(fakeOutcome("comment", { state })),
        }).pipe(Effect.provide(harness.layer));
        expect(yield* Ref.get(harness.requests)).toEqual([
          "GET https://api.github.com/repos/acme/widgets/pulls/5/reviews?per_page=100&page=1",
        ]);

        yield* runReviewAction(
          { run: () => Effect.succeed(fakeOutcome("comment", { state })) },
          { retireStaleReviews: false },
        ).pipe(Effect.provide(harness.layer));
        expect((yield* Ref.get(harness.requests)).length).toBe(1);

        const incompleteReceipt = fakeOutcome("comment", { state });
        const completeReceipt = incompleteReceipt.published;
        if (completeReceipt === undefined) {
          throw new Error("Expected the state-bearing fixture to include a publication receipt");
        }
        yield* runReviewAction({
          run: () =>
            Effect.succeed(
              ReviewRunOutcome.make({
                ...incompleteReceipt,
                published: PublishedReview.make({
                  reviewId: completeReceipt.reviewId,
                  url: completeReceipt.url,
                  event: completeReceipt.event,
                  inlineComments: completeReceipt.inlineComments,
                  submittedAt: completeReceipt.submittedAt,
                  authorNodeId: null,
                }),
              }),
            ),
        }).pipe(Effect.provide(harness.layer));
        expect((yield* Ref.get(harness.requests)).length).toBe(1);
      }),
  );

  it.effect("posts progress comments only when enabled and posting", () =>
    Effect.gen(function* () {
      const event = JSON.stringify({
        pull_request: { number: 5 },
        repository: { full_name: "acme/widgets" },
      });
      const isProgressRequest = (request: string) => request.includes("/issues/");

      // Default off: existing harnesses keep their exact mutation surface.
      const defaultHarness = yield* actionHarness(event);
      yield* runReviewAction(
        { run: () => Effect.succeed(fakeOutcome("comment")) },
        { post: false },
      ).pipe(Effect.provide(defaultHarness.layer));
      expect((yield* Ref.get(defaultHarness.requests)).some(isProgressRequest)).toBe(false);

      // Enabled but dry-run: progress never posts what the run itself won't.
      const dryRunHarness = yield* actionHarness(event);
      yield* runReviewAction(
        { run: () => Effect.succeed(fakeOutcome("comment")) },
        { post: false, progressComment: true },
      ).pipe(Effect.provide(dryRunHarness.layer));
      expect((yield* Ref.get(dryRunHarness.requests)).some(isProgressRequest)).toBe(false);

      // Enabled and posting: the sticky comment is begun before the run.
      const enabledHarness = yield* actionHarness(event);
      yield* runReviewAction(
        { run: () => Effect.succeed(fakeOutcome("comment")) },
        { progressComment: true },
      ).pipe(Effect.provide(enabledHarness.layer));
      const requests = yield* Ref.get(enabledHarness.requests);
      expect(requests).toContain(
        "GET https://api.github.com/repos/acme/widgets/issues/5/comments?per_page=100&page=1",
      );
      expect(requests).toContain(
        "POST https://api.github.com/repos/acme/widgets/issues/5/comments",
      );
    }),
  );

  it.effect("settles the progress comment when the reviewer run fails", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const exit = yield* runReviewAction(
        { run: () => Effect.fail(ModelUnavailable.make({})) },
        { progressComment: true },
      ).pipe(Effect.provide(harness.layer), Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
      const requests = yield* Ref.get(harness.requests);
      const progressWrites = requests.filter((request) =>
        request.startsWith("POST https://api.github.com/repos/acme/widgets/issues/"),
      );
      // One write attempt from begin and one from the failure settle.
      expect(progressWrites.length).toBe(2);
    }),
  );

  it.effect("fails the check for a blocking finding regardless of model verdict", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const exit = yield* runReviewAction(
        { run: () => Effect.succeed(fakeOutcome("comment", { blocking: true })) },
        { post: false },
      ).pipe(Effect.provide(harness.layer), Effect.exit);
      const failure = failureFrom(exit);
      expect(Schema.is(ReviewGateFailed)(failure)).toBe(true);
      if (Schema.is(ReviewGateFailed)(failure)) {
        expect(failure.conclusion).toBe("blocking");
      }
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("verdict=comment");
      expect(outputs).toContain("conclusion=blocking");
    }),
  );

  it.effect(
    "concludes blocking, not incomplete, when code findings and machinery gaps coexist",
    () =>
      Effect.gen(function* () {
        const harness = yield* actionHarness(
          JSON.stringify({
            pull_request: { number: 5 },
            repository: { full_name: "acme/widgets" },
          }),
        );
        const exit = yield* runReviewAction(
          {
            run: () =>
              Effect.succeed(fakeOutcome("comment", { blocking: true, assuranceIncomplete: true })),
          },
          { post: false },
        ).pipe(Effect.provide(harness.layer), Effect.exit);
        const failure = failureFrom(exit);
        expect(Schema.is(ReviewGateFailed)(failure)).toBe(true);
        if (Schema.is(ReviewGateFailed)(failure)) {
          expect(failure.conclusion).toBe("blocking");
          expect(failure.reasons[0]).toContain("blocking finding");
          expect(failure.reasons.join("; ")).toContain("not a code defect");
        }
      }),
  );

  it.effect("fails the check when required coverage is incomplete", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const exit = yield* runReviewAction(
        { run: () => Effect.succeed(fakeOutcome("approve", { incomplete: true })) },
        { post: false },
      ).pipe(Effect.provide(harness.layer), Effect.exit);
      const failure = failureFrom(exit);
      expect(Schema.is(ReviewGateFailed)(failure)).toBe(true);
      if (Schema.is(ReviewGateFailed)(failure)) {
        expect(failure.conclusion).toBe("incomplete");
      }
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("verdict=approve");
      expect(outputs).toContain("conclusion=incomplete");
      expect(outputs).toContain("coverage=incomplete");
    }),
  );

  it.effect("fails the check when input is covered but review assurance is unsettled", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const exit = yield* runReviewAction(
        {
          run: () => Effect.succeed(fakeOutcome("approve", { assuranceIncomplete: true })),
        },
        { post: false },
      ).pipe(Effect.provide(harness.layer), Effect.exit);
      const failure = failureFrom(exit);
      expect(Schema.is(ReviewGateFailed)(failure)).toBe(true);
      if (Schema.is(ReviewGateFailed)(failure)) {
        expect(failure.conclusion).toBe("incomplete");
        expect(failure.reasons).toContain("review discovery did not settle");
        // The failure explicitly tells a coding agent this is reviewer-side
        // uncertainty carried forward, never an invitation to expand the patch.
        expect(failure.reasons[0]).toContain("not a code defect");
        expect(failure.reasons[0]).toContain("carried forward and retried");
      }
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("input-coverage=complete");
      expect(outputs).toContain("review-assurance=incomplete");
      expect(outputs).toContain("conclusion=incomplete");
    }),
  );

  it.effect("reruns a fingerprint-only harness instead of claiming unauthenticated assurance", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const fingerprint = "f".repeat(64);
      const invoked = yield* Ref.make(0);
      const result = yield* runReviewAction(
        {
          run: () =>
            Ref.update(invoked, (count) => count + 1).pipe(
              Effect.map(() => fakeOutcome("comment")),
            ),
          fingerprint: Effect.succeed(fingerprint),
        },
        {
          post: false,
          priorReviews: staticPriorReviews(Option.some(fingerprint)),
        },
      ).pipe(Effect.provide(harness.layer));
      expect(result._tag).toBe("Completed");
      expect(yield* Ref.get(invoked)).toBe(1);
      expect(yield* Ref.get(harness.written)).not.toContain("skipped=true");
    }),
  );

  it.effect("preserves a blocking conclusion when the reviewed head is unchanged", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const headSha = "1".repeat(40);
      const baseSha = "2".repeat(40);
      const profileFingerprint = "a".repeat(64);
      const metadata = PullRequestMetadata.make({
        repository: "acme/widgets",
        number: 5,
        title: "Review target",
        body: "",
        baseRef: "main",
        baseSha,
        headRef: "fix/review",
        headSha,
        totalChangedFiles: 0,
      });
      const state = ReviewState.make({
        version: 2,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha,
        headRef: metadata.headRef,
        reviewedHeadSha: headSha,
        profileFingerprint,
        acceptedScopeFingerprint: "b".repeat(64),
        reviewedPathCount: 0,
        unresolvedFindings: [
          StoredReviewFinding.make({
            path: "src/a.ts",
            startLine: 1,
            endLine: 1,
            severity: "blocking",
            title: "Unsafe behavior",
            body: "This remains unresolved.",
          }),
        ],
        unresolvedConcerns: [],
        unreviewedPaths: [],
        settled: true,
        lastReviewMode: "full",
      });
      const invoked = yield* Ref.make(0);
      const exit = yield* runReviewAction(
        {
          run: () =>
            Ref.update(invoked, (count) => count + 1).pipe(
              Effect.map(() => fakeOutcome("comment")),
            ),
          profileFingerprint: Effect.succeed(profileFingerprint),
          snapshot: Effect.succeed({ metadata, files: [] }),
        },
        {
          post: false,
          priorReviews: staticPriorReviews(Option.none(), { state: Option.some(state) }),
        },
      ).pipe(Effect.provide(harness.layer), Effect.exit);
      const failure = failureFrom(exit);
      expect(Schema.is(ReviewGateFailed)(failure)).toBe(true);
      expect(yield* Ref.get(invoked)).toBe(0);
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("skipped=true");
      expect(outputs).toContain("conclusion=blocking");
    }),
  );

  it.effect("skips a patch-equivalent rebase without requiring head ancestry", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const reviewedHeadSha = "1".repeat(40);
      const currentHeadSha = "2".repeat(40);
      const profileFingerprint = "a".repeat(64);
      const patchFingerprint = "b".repeat(64);
      const metadata = PullRequestMetadata.make({
        repository: "acme/widgets",
        number: 5,
        title: "Review target",
        body: "",
        baseRef: "main",
        baseSha: "3".repeat(40),
        headRef: "fix/review",
        headSha: currentHeadSha,
        totalChangedFiles: 1,
      });
      const file = ChangedFile.make({
        path: "src/a.ts",
        status: "modified",
        additions: 1,
        deletions: 1,
        patch: "@@ -8 +8 @@\n-before\n+after",
      });
      const state = ReviewState.make({
        version: 2,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha: "4".repeat(40),
        headRef: metadata.headRef,
        reviewedHeadSha,
        profileFingerprint,
        acceptedScopeFingerprint: patchFingerprint,
        reviewedPathCount: 1,
        unresolvedFindings: [],
        unresolvedConcerns: [],
        unreviewedPaths: [],
        settled: true,
        lastReviewMode: "full",
      });
      const invoked = yield* Ref.make(0);
      const result = yield* runReviewAction(
        {
          run: () =>
            Ref.update(invoked, (count) => count + 1).pipe(
              Effect.map(() => fakeOutcome("comment")),
            ),
          fingerprint: Effect.succeed(patchFingerprint),
          profileFingerprint: Effect.succeed(profileFingerprint),
          snapshot: Effect.succeed({ metadata, files: [file] }),
        },
        {
          post: false,
          priorReviews: staticPriorReviews(Option.none(), { state: Option.some(state) }),
        },
      ).pipe(Effect.provide(harness.layer));

      expect(result._tag).toBe("Skipped");
      expect(yield* Ref.get(invoked)).toBe(0);
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("effective pull-request patch is unchanged");
      expect(outputs).toContain("conclusion=success");
    }),
  );

  it.effect("re-reviews an unchanged patch when the stored state carries unreviewed scope", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const profileFingerprint = "a".repeat(64);
      const patchFingerprint = "b".repeat(64);
      const metadata = PullRequestMetadata.make({
        repository: "acme/widgets",
        number: 5,
        title: "Review target",
        body: "",
        baseRef: "main",
        baseSha: "3".repeat(40),
        headRef: "fix/review",
        headSha: "2".repeat(40),
        totalChangedFiles: 1,
      });
      const file = ChangedFile.make({
        path: "src/a.ts",
        status: "modified",
        additions: 1,
        deletions: 1,
        patch: "@@ -8 +8 @@\n-before\n+after",
      });
      const state = ReviewState.make({
        version: 2,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha: metadata.baseSha ?? "3".repeat(40),
        headRef: metadata.headRef,
        reviewedHeadSha: metadata.headSha,
        profileFingerprint,
        acceptedScopeFingerprint: patchFingerprint,
        reviewedPathCount: 1,
        unresolvedFindings: [],
        unresolvedConcerns: [],
        // A prior pass failed on src/a.ts; skipping would abandon the retry.
        unreviewedPaths: ["src/a.ts"],
        settled: false,
        lastReviewMode: "incremental",
      });
      const invoked = yield* Ref.make(0);
      const result = yield* runReviewAction(
        {
          run: () =>
            Ref.update(invoked, (count) => count + 1).pipe(
              Effect.map(() => fakeOutcome("comment")),
            ),
          fingerprint: Effect.succeed(patchFingerprint),
          profileFingerprint: Effect.succeed(profileFingerprint),
          snapshot: Effect.succeed({ metadata, files: [file] }),
        },
        {
          post: false,
          priorReviews: staticPriorReviews(Option.none(), { state: Option.some(state) }),
        },
      ).pipe(Effect.provide(harness.layer));

      expect(result._tag).toBe("Completed");
      expect(yield* Ref.get(invoked)).toBe(1);
      expect(yield* Ref.get(harness.written)).toContain("skipped=false");
    }),
  );

  it.effect("reviews a rebased head when the effective patch changed", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const profileFingerprint = "a".repeat(64);
      const metadata = PullRequestMetadata.make({
        repository: "acme/widgets",
        number: 5,
        title: "Review target",
        body: "",
        baseRef: "main",
        baseSha: "3".repeat(40),
        headRef: "fix/review",
        headSha: "2".repeat(40),
        totalChangedFiles: 0,
      });
      const state = ReviewState.make({
        version: 2,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha: "4".repeat(40),
        headRef: metadata.headRef,
        reviewedHeadSha: "1".repeat(40),
        profileFingerprint,
        acceptedScopeFingerprint: "b".repeat(64),
        reviewedPathCount: 0,
        unresolvedFindings: [],
        unresolvedConcerns: [],
        unreviewedPaths: [],
        settled: true,
        lastReviewMode: "full",
      });
      const invoked = yield* Ref.make(0);
      const result = yield* runReviewAction(
        {
          run: () =>
            Ref.update(invoked, (count) => count + 1).pipe(
              Effect.map(() => fakeOutcome("comment")),
            ),
          fingerprint: Effect.succeed("c".repeat(64)),
          profileFingerprint: Effect.succeed(profileFingerprint),
          snapshot: Effect.succeed({ metadata, files: [] }),
        },
        {
          post: false,
          priorReviews: staticPriorReviews(Option.none(), { state: Option.some(state) }),
        },
      ).pipe(Effect.provide(harness.layer));

      expect(result._tag).toBe("Completed");
      expect(yield* Ref.get(invoked)).toBe(1);
      expect(yield* Ref.get(harness.written)).toContain("skipped=false");
    }),
  );
});

// ---------------------------------------------------------------------------
// The committed review-profile file.
// ---------------------------------------------------------------------------

const fileInfo = (size: number): FileSystem.File.Info => ({
  type: "File",
  mtime: Option.none(),
  atime: Option.none(),
  birthtime: Option.none(),
  dev: 0,
  ino: Option.none(),
  mode: 0o644,
  nlink: Option.none(),
  uid: Option.none(),
  gid: Option.none(),
  rdev: Option.none(),
  size: FileSystem.Size(size),
  blksize: Option.none(),
  blocks: Option.none(),
});

describe("resolveGuidance", () => {
  const PROFILE_PATH = ".github/review-guidance.md";
  const withProfileFs = (content: string | undefined) =>
    Layer.succeed(FileSystem.FileSystem)(
      FileSystem.makeNoop({
        stat: (path) =>
          content !== undefined && path === PROFILE_PATH
            ? Effect.succeed(fileInfo(content.length))
            : Effect.fail(
                new PlatformError(
                  new SystemError({
                    _tag: "NotFound",
                    module: "FileSystem",
                    method: "stat",
                  }),
                ),
              ),
        readFileString: (path) =>
          content !== undefined && path === PROFILE_PATH
            ? Effect.succeed(content)
            : Effect.fail(
                new PlatformError(
                  new SystemError({
                    _tag: "NotFound",
                    module: "FileSystem",
                    method: "readFileString",
                  }),
                ),
              ),
      }),
    );

  it.effect("passes inline guidance through when no file is configured", () =>
    Effect.gen(function* () {
      expect(yield* resolveGuidance({ guidance: "inline", guidanceFile: undefined })).toBe(
        "inline",
      );
      expect(yield* resolveGuidance({ guidance: undefined, guidanceFile: undefined })).toBe(
        undefined,
      );
    }).pipe(Effect.provide(withProfileFs(undefined))),
  );

  it.effect("reads the profile and appends any inline guidance", () =>
    Effect.gen(function* () {
      const alone = yield* resolveGuidance({
        guidance: undefined,
        guidanceFile: PROFILE_PATH,
      }).pipe(Effect.provide(withProfileFs("Deep architecture profile.\n")));
      expect(alone).toBe("Deep architecture profile.");

      const combined = yield* resolveGuidance({
        guidance: "Also: be brief.",
        guidanceFile: PROFILE_PATH,
      }).pipe(Effect.provide(withProfileFs("Deep architecture profile.")));
      expect(combined).toBe("Deep architecture profile.\nAlso: be brief.");
    }),
  );

  it.effect("fails typed when the configured profile cannot be read", () =>
    Effect.gen(function* () {
      const exit = yield* resolveGuidance({
        guidance: undefined,
        guidanceFile: PROFILE_PATH,
      }).pipe(Effect.provide(withProfileFs(undefined)), Effect.exit);
      const failure = failureFrom(exit);
      expect(Schema.is(GuidanceFileUnreadable)(failure)).toBe(true);
    }),
  );
});

// (Bound check for the committed review profile: refused, never truncated.)
describe("resolveGuidance bound", () => {
  it.effect("refuses an oversized guidance file typed", () =>
    Effect.gen(function* () {
      const oversized = "x".repeat(MAX_GUIDANCE_FILE_CHARS + 1);
      const fs = Layer.succeed(FileSystem.FileSystem)(
        FileSystem.makeNoop({
          stat: () => Effect.succeed(fileInfo(oversized.length)),
          readFileString: () => Effect.succeed(oversized),
        }),
      );
      const exit = yield* resolveGuidance({
        guidance: undefined,
        guidanceFile: "huge.md",
      }).pipe(Effect.provide(fs), Effect.exit);
      const failure = failureFrom(exit);
      expect(Schema.is(GuidanceFileUnreadable)(failure)).toBe(true);
      expect(String(failure)).toContain("guidance bound");
    }),
  );
});
