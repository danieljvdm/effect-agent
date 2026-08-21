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
  ReviewAssurance,
  ReviewExecutionContext,
  ReviewFinding,
  ReviewHeadComparison,
  ReviewInputCoverage,
  ReviewRunOutcome,
  ReviewState,
  PullRequestSource,
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
    /** The only gap is a binary: settled assurance, one undiffable path. */
    readonly undiffableOnly?: boolean;
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
    inputCoverage: ReviewInputCoverage.make({
      status: options.incomplete || options.undiffableOnly ? "incomplete" : "complete",
      requiredPaths: [],
      assignedPaths: [],
      partialPaths: [],
      unassignedPaths: [],
      undiffablePaths: options.undiffableOnly ? ["assets/logo.png"] : [],
      reasons: options.undiffableOnly
        ? ["required paths have no reviewable diff or bounded text (1): assets/logo.png"]
        : options.incomplete
          ? ["review input was not completely assigned"]
          : [],
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
    unreviewedPaths: options.undiffableOnly
      ? ["assets/logo.png"]
      : options.incomplete || options.assuranceIncomplete
        ? ["src/a.ts"]
        : [],
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
const actionHarness = (
  eventJson: string | undefined,
  options: {
    readonly changedFiles?: ReadonlyArray<{
      readonly filename: string;
      readonly status: string;
      readonly additions: number;
      readonly deletions: number;
      readonly patch?: string | undefined;
    }>;
  } = {},
) =>
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
            new Response(
              url.pathname.endsWith("/files")
                ? JSON.stringify(options.changedFiles ?? [])
                : options.changedFiles !== undefined &&
                    url.pathname === "/repos/acme/widgets/pulls/5"
                  ? JSON.stringify({
                      number: 5,
                      title: "Review target",
                      body: "",
                      changed_files: options.changedFiles.length,
                      base: { ref: "main", sha: "1".repeat(40) },
                      head: { ref: "fix/review", sha: "2".repeat(40) },
                    })
                  : "[]",
              { status: 200, headers: { "content-type": "application/json" } },
            ),
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
    }),
  );

  it.effect("preserves the full source for a custom reviewer without a selection profile", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
        {
          changedFiles: [
            {
              filename: "src/full-source.ts",
              status: "modified",
              additions: 1,
              deletions: 1,
              patch: "@@ -1 +1 @@\n-before\n+after",
            },
          ],
        },
      );
      const reviewedPaths = yield* Ref.make<ReadonlyArray<string>>([]);

      const result = yield* runReviewAction(
        {
          run: () =>
            Effect.gen(function* () {
              const source = yield* PullRequestSource;
              const files = yield* source.changedFiles;
              yield* Ref.set(
                reviewedPaths,
                files.map((file) => file.path),
              );
              return fakeOutcome("comment");
            }),
        },
        { post: false },
      ).pipe(Effect.provide(harness.layer));

      expect(result._tag).toBe("Completed");
      expect(yield* Ref.get(reviewedPaths)).toEqual(["src/full-source.ts"]);
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
          version: 1,
          repository: "acme/widgets",
          pullRequestNumber: 5,
          baseRef: "main",
          baseSha: "1".repeat(40),
          headRef: "fix/review",
          reviewedHeadSha: "2".repeat(40),
          profileFingerprint: "a".repeat(64),
          settledScopeFingerprint: "b".repeat(64),
          reviewedPathCount: 0,
          unresolvedFindings: [],
          unresolvedConcerns: [],
          unreviewedPaths: [],
          unreviewedPasses: [],
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

  it.effect("names removal or ignore globs — not retry — for an undiffable-only gap", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const exit = yield* runReviewAction(
        { run: () => Effect.succeed(fakeOutcome("approve", { undiffableOnly: true })) },
        { post: false },
      ).pipe(Effect.provide(harness.layer), Effect.exit);
      const failure = failureFrom(exit);
      expect(Schema.is(ReviewGateFailed)(failure)).toBe(true);
      if (Schema.is(ReviewGateFailed)(failure)) {
        expect(failure.conclusion).toBe("incomplete");
        // A retry can never settle a binary: the gate must instruct the one
        // real remedy (remove the file or ignore-glob it) and must not promise
        // an automatic retry that cannot succeed.
        const joined = failure.reasons.join("\n");
        expect(joined).not.toContain("retried automatically");
        expect(joined).toContain("remove them from the pull request or exclude them");
        expect(joined).toContain("assets/logo.png");
      }
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
        version: 1,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha,
        headRef: metadata.headRef,
        reviewedHeadSha: headSha,
        profileFingerprint,
        settledScopeFingerprint: "b".repeat(64),
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
        unreviewedPasses: [],
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
          fingerprint: Effect.succeed("b".repeat(64)),
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
        version: 1,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha: "4".repeat(40),
        headRef: metadata.headRef,
        reviewedHeadSha,
        profileFingerprint,
        settledScopeFingerprint: patchFingerprint,
        reviewedPathCount: 1,
        unresolvedFindings: [],
        unresolvedConcerns: [],
        unreviewedPaths: [],
        unreviewedPasses: [],
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
        version: 1,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha: metadata.baseSha ?? "3".repeat(40),
        headRef: metadata.headRef,
        reviewedHeadSha: metadata.headSha,
        profileFingerprint,
        settledScopeFingerprint: patchFingerprint,
        reviewedPathCount: 1,
        unresolvedFindings: [],
        unresolvedConcerns: [],
        // A prior pass failed on src/a.ts; skipping would abandon the retry.
        unreviewedPaths: ["src/a.ts"],
        unreviewedPasses: [],
        settled: false,
        lastReviewMode: "incremental",
      });
      const reviewedScopes = yield* Ref.make<ReadonlyArray<ReadonlyArray<string>>>([]);
      const result = yield* runReviewAction(
        {
          // Record the exact selected scope the harness provides, so this
          // test proves the carried path is actually RE-REVIEWED — not merely
          // that the skip was declined.
          run: () =>
            Effect.gen(function* () {
              const selection = Option.getOrUndefined(
                yield* Effect.serviceOption(ReviewExecutionContext),
              );
              yield* Ref.update(reviewedScopes, (previous) => [
                ...previous,
                selection?.files.map((selected) => selected.path) ?? [],
              ]);
              return fakeOutcome("comment");
            }),
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
      expect(yield* Ref.get(reviewedScopes)).toEqual([["src/a.ts"]]);
      expect(yield* Ref.get(harness.written)).toContain("skipped=false");
    }),
  );

  it.effect("reviews only content-changed PR paths after a rewritten head", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const profileFingerprint = "a".repeat(64);
      const reviewedHeadSha = "1".repeat(40);
      const currentHeadSha = "2".repeat(40);
      const metadata = PullRequestMetadata.make({
        repository: "acme/widgets",
        number: 5,
        title: "Review target",
        body: "",
        baseRef: "main",
        baseSha: "3".repeat(40),
        headRef: "fix/review",
        headSha: currentHeadSha,
        totalChangedFiles: 2,
      });
      const changed = ChangedFile.make({
        path: "src/fix.ts",
        status: "modified",
        additions: 1,
        deletions: 1,
        patch: "@@ -1 +1 @@\n-before\n+after",
      });
      const untouched = ChangedFile.make({
        path: "src/untouched.ts",
        status: "modified",
        additions: 1,
        deletions: 0,
        patch: "@@ -1 +1 @@\n-old\n+kept",
      });
      const state = ReviewState.make({
        version: 1,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha: metadata.baseSha ?? "3".repeat(40),
        headRef: metadata.headRef,
        reviewedHeadSha,
        profileFingerprint,
        settledScopeFingerprint: "b".repeat(64),
        reviewedPathCount: 2,
        unresolvedFindings: [],
        unresolvedConcerns: [],
        unreviewedPaths: [],
        unreviewedPasses: [],
        settled: true,
        lastReviewMode: "full",
      });
      const reviewedScopes = yield* Ref.make<ReadonlyArray<ReadonlyArray<string>>>([]);
      const result = yield* runReviewAction(
        {
          run: () =>
            Effect.gen(function* () {
              const selection = Option.getOrUndefined(
                yield* Effect.serviceOption(ReviewExecutionContext),
              );
              yield* Ref.update(reviewedScopes, (previous) => [
                ...previous,
                selection?.files.map((selected) => selected.path) ?? [],
              ]);
              return fakeOutcome("comment");
            }),
          fingerprint: Effect.succeed("c".repeat(64)),
          profileFingerprint: Effect.succeed(profileFingerprint),
          snapshot: Effect.succeed({ metadata, files: [changed, untouched] }),
        },
        {
          post: false,
          priorReviews: staticPriorReviews(Option.none(), {
            state: Option.some(state),
            comparison: ReviewHeadComparison.make({
              status: "diverged",
              baseSha: reviewedHeadSha,
              headSha: currentHeadSha,
              mergeBaseSha: "4".repeat(40),
              files: [changed, untouched],
              truncated: false,
            }),
            treeComparison: ReviewHeadComparison.make({
              status: "ahead",
              baseSha: reviewedHeadSha,
              headSha: currentHeadSha,
              mergeBaseSha: reviewedHeadSha,
              files: [changed],
              truncated: false,
            }),
          }),
        },
      ).pipe(Effect.provide(harness.layer));

      expect(result._tag).toBe("Completed");
      expect(yield* Ref.get(reviewedScopes)).toEqual([["src/fix.ts"]]);
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
        version: 1,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha: "4".repeat(40),
        headRef: metadata.headRef,
        reviewedHeadSha: "1".repeat(40),
        profileFingerprint,
        settledScopeFingerprint: "b".repeat(64),
        reviewedPathCount: 0,
        unresolvedFindings: [],
        unresolvedConcerns: [],
        unreviewedPaths: [],
        unreviewedPasses: [],
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
