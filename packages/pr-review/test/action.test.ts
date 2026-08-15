import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  ConfigProvider,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
} from "effect";
import { PlatformError, SystemError } from "effect/PlatformError";

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
  CodeReview,
  InvalidEffortInput,
  planPublication,
  PullRequestMetadata,
  ReviewCoverage,
  ReviewFinding,
  ReviewRunOutcome,
  ReviewState,
  StoredReviewFinding,
  type ReviewVerdict,
} from "../src/index.ts";
import { staticPriorReviewsLayer } from "../src/testing.ts";

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
        fanOut: false,
        guidance: undefined,
        guidanceFile: undefined,
        ignore: [],
        maxFindings: undefined,
        maxDurationMinutes: undefined,
        failOn: "never",
        skipUnchanged: true,
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
          PR_REVIEW_FAN_OUT: "true",
          PR_REVIEW_GUIDANCE: "Flag naked Promises.",
          PR_REVIEW_GUIDANCE_FILE: ".github/review-guidance.md",
          PR_REVIEW_IGNORE: " **/*.lock , dist/** ,",
          PR_REVIEW_MAX_FINDINGS: "7",
          PR_REVIEW_MAX_DURATION_MINUTES: "12",
          PR_REVIEW_FAIL_ON: "request-changes",
          PR_REVIEW_SKIP_UNCHANGED: "false",
          PR_REVIEW_MODE: "final",
        }),
      );
      expect(inputs).toEqual({
        provider: "anthropic",
        model: "claude-sonnet-5",
        effort: 0.75,
        post: false,
        applyVerdict: true,
        fanOut: true,
        guidance: "Flag naked Promises.",
        guidanceFile: ".github/review-guidance.md",
        ignore: ["**/*.lock", "dist/**"],
        maxFindings: 7,
        maxDurationMinutes: 12,
        failOn: "request-changes",
        skipUnchanged: false,
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

const fakeOutcome = (
  verdict: ReviewVerdict,
  options: { readonly blocking?: boolean; readonly incomplete?: boolean } = {},
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
      status: options.incomplete ? "incomplete" : "complete",
      requiredPaths: [],
      reviewedPaths: [],
      unreviewedPaths: [],
      failedUnits: [],
      reasons: options.incomplete ? ["review unit unit-001 did not complete"] : [],
    }),
    plan: planPublication(review, [], {
      applyVerdict: false,
      headSha: "abcdefabcdefabcdefabcdefabcdefabcdefabcd",
      totalChangedFiles: 0,
    }),
    turns: 1,
  });
};

/** Environment harness: event payload + captured GITHUB_OUTPUT writes. */
const actionHarness = (eventJson: string | undefined) =>
  Effect.gen(function* () {
    const written = yield* Ref.make("");
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
    const layer = Layer.merge(
      Layer.succeed(FileSystem.FileSystem)(fs),
      ConfigProvider.layer(ConfigProvider.fromEnvRecord(env)),
    );
    return { written, layer };
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
      expect(outputs).toContain("coverage=complete");
    }),
  );

  it.effect("fails the check for a blocking finding regardless of model verdict or fail-on", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const exit = yield* runReviewAction(
        { run: () => Effect.succeed(fakeOutcome("comment", { blocking: true })) },
        { post: false, failOn: "never" },
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
        { post: false },
      ).pipe(
        Effect.provide(
          Layer.merge(
            harness.layer,
            staticPriorReviewsLayer(Option.none(), { state: Option.some(state) }),
          ),
        ),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      expect(Schema.is(ReviewGateFailed)(failure)).toBe(true);
      expect(yield* Ref.get(invoked)).toBe(0);
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("skipped=true");
      expect(outputs).toContain("conclusion=blocking");
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
