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

import {
  resolveActionInputs,
  ReviewGateFailed,
  runReviewAction,
  type ReviewActionResult,
} from "../src/action.ts";
import { CodeReview, planPublication, ReviewRunOutcome, type ReviewVerdict } from "../src/index.ts";

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
        post: true,
        applyVerdict: false,
        fanOut: false,
        guidance: undefined,
        ignore: [],
        maxFindings: undefined,
        failOn: "never",
      });
    }),
  );

  it.effect("parses every configured input", () =>
    Effect.gen(function* () {
      const inputs = yield* resolveActionInputs().pipe(
        withEnv({
          PR_REVIEW_PROVIDER: "anthropic",
          PR_REVIEW_MODEL: "claude-sonnet-5",
          PR_REVIEW_POST: "false",
          PR_REVIEW_APPLY_VERDICT: "true",
          PR_REVIEW_FAN_OUT: "true",
          PR_REVIEW_GUIDANCE: "Flag naked Promises.",
          PR_REVIEW_IGNORE: " **/*.lock , dist/** ,",
          PR_REVIEW_MAX_FINDINGS: "7",
          PR_REVIEW_FAIL_ON: "request-changes",
        }),
      );
      expect(inputs).toEqual({
        provider: "anthropic",
        model: "claude-sonnet-5",
        post: false,
        applyVerdict: true,
        fanOut: true,
        guidance: "Flag naked Promises.",
        ignore: ["**/*.lock", "dist/**"],
        maxFindings: 7,
        failOn: "request-changes",
      });
    }),
  );
});

// ---------------------------------------------------------------------------
// The action harness around one reviewer run: skips, outputs, and the gate.
// The reviewer itself is a stub — the harness is what's under test.
// ---------------------------------------------------------------------------

const EVENT_PATH = "/tmp/github-event.json";
const OUTPUT_PATH = "/tmp/github-output";

const fakeOutcome = (verdict: ReviewVerdict): ReviewRunOutcome => {
  const review = CodeReview.make({ summary: "Stubbed review.", verdict, findings: [] });
  return ReviewRunOutcome.make({
    review,
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
      const result = yield* runReviewAction(() =>
        Ref.update(invoked, (count) => count + 1).pipe(Effect.map(() => fakeOutcome("comment"))),
      ).pipe(Effect.provide(harness.layer));
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
      const result = yield* runReviewAction(() => Effect.succeed(fakeOutcome("comment"))).pipe(
        Effect.provide(harness.layer),
      );
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
        () => Effect.succeed(fakeOutcome("comment")),
        { post: false },
      ).pipe(Effect.provide(harness.layer));
      expect(result._tag).toBe("Completed");
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("skipped=false");
      expect(outputs).toContain("verdict=comment");
      expect(outputs).toContain("inline-comments=0");
    }),
  );

  it.effect("fails the job on the configured verdict gate — after writing outputs", () =>
    Effect.gen(function* () {
      const harness = yield* actionHarness(
        JSON.stringify({
          pull_request: { number: 5 },
          repository: { full_name: "acme/widgets" },
        }),
      );
      const exit = yield* runReviewAction(() => Effect.succeed(fakeOutcome("request-changes")), {
        post: false,
        failOn: "request-changes",
      }).pipe(Effect.provide(harness.layer), Effect.exit);
      const failure = failureFrom(exit);
      expect(Schema.is(ReviewGateFailed)(failure)).toBe(true);
      const outputs = yield* Ref.get(harness.written);
      expect(outputs).toContain("verdict=request-changes");
    }),
  );
});
