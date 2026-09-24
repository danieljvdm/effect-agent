import {
  ReviewChange,
  ReviewOutcome,
  ReviewReport,
  ReviewRequest,
} from "@effect-agent/pr-review/review";
import { OpenAiClient } from "@effect/ai-openai";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  ConfigProvider,
  Deferred,
  Effect,
  FileSystem,
  Layer,
  Option,
  PlatformError,
  Redacted,
  Ref,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  decodeObservationLines,
  digestReviewRequest,
  EvalCase,
  EvalCaseId,
  EvalDefectId,
  EvalExpectedDefect,
  EvalEvidence,
  EvalSuite,
  EvalVariantConfiguration,
  EvalVariantId,
  makeCurrentOpenAiVariant,
  runEvalSuite,
  writeObservations,
  type EvalVariant,
} from "../src/index.ts";

const patch = `@@ -1 +1 @@
-export const read = (value?: string) => value?.length ?? 0;
+export const read = (value?: string) => value.length;`;

const request = ReviewRequest.make({
  title: "Remove optional handling",
  description: "",
  baseRevision: "base",
  headRevision: "head",
  changes: [ReviewChange.make({ path: "src/read.ts", patch })],
  unreviewedPaths: [],
});

const caseId = Schema.decodeSync(EvalCaseId)("optional-read");
const defectId = Schema.decodeSync(EvalDefectId)("undefined-dereference");

const makeSuite = Effect.fn("PrReviewEvalTest.makeSuite")(function* (
  input: ReviewRequest = request,
) {
  const inputDigest = yield* digestReviewRequest(input);

  return EvalSuite.make({
    version: 1,
    cases: [
      EvalCase.make({
        version: 1,
        id: caseId,
        kind: "known-defects",
        provenance: "Synthetic fixture for the eval runner contract.",
        inputDigest,
        request: input,
        expectedDefects: [
          EvalExpectedDefect.make({
            id: defectId,
            severity: "blocking",
            invariant: "The changed function dereferences an optional string.",
            evidence: [
              EvalEvidence.make({
                path: "src/read.ts",
                line: 1,
                description: "value.length executes when value is undefined.",
              }),
            ],
          }),
        ],
      }),
    ],
  });
});

const configuration = (id: string) =>
  EvalVariantConfiguration.make({
    id,
    reviewerProfile: "scripted-v1",
    provider: "openai",
    model: "scripted-eval",
    reasoningEffort: "medium",
    compaction: "prune",
    contextTokenLimit: 128_000,
    maxOutputTokens: 8_000,
    strictJsonSchema: true,
    store: false,
    maxCostMicrousd: 2_500_000,
    budgetPolicy: "input-size-v1",
  });

const successfulOutcome = ReviewOutcome.make({
  report: ReviewReport.make({ summary: "No findings.", findings: [] }),
  turns: 1,
  usage: {
    inputTokens: 10,
    uncachedInputTokens: 10,
    cachedInputTokens: 0,
    cacheWriteInputTokens: 0,
    outputTokens: 2,
  },
});

describe("PR-review model eval", () => {
  it.effect("records actionable AI error categories without provider payloads or credentials", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();

      const variant = yield* makeCurrentOpenAiVariant({
        id: Schema.decodeSync(EvalVariantId)("provider-failure"),
      }).pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromEnv({ env: { PR_REVIEW_MODEL: "gpt-6-astra" } }),
        ),
      );

      const privateText = "private-source-and-provider-payload";

      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({ error: { type: "invalid_request_error", message: privateText } }),
              { status: 400, headers: { "content-type": "application/json" } },
            ),
          ),
        ),
      );

      const observations = yield* runEvalSuite(suite, [variant], {
        trials: 1,
        concurrency: 1,
        caseIds: [],
      }).pipe(
        Stream.runCollect,
        Effect.provide(
          OpenAiClient.layer({ apiKey: Redacted.make("private-api-key") }).pipe(
            Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
          ),
        ),
      );

      expect(observations[0]?.result).toEqual({
        _tag: "Failed",
        errorTag: "AiError/InvalidRequestError",
        message: "AI failure; retryable=false",
        estimatedCostMicrousd: 0,
      });
      expect(JSON.stringify(observations)).not.toContain(privateText);
      expect(JSON.stringify(observations)).not.toContain("private-api-key");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("flushes out-of-order completions and cancels active work after a write failure", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-write-failure-" });
      const output = `${directory}/observations.jsonl`;
      const blocked = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      const saved = yield* Deferred.make<void>();
      const calls = yield* Ref.make(0);
      const writes = yield* Ref.make(0);
      const opened = yield* Ref.make<Option.Option<FileSystem.File>>(Option.none());

      const variant: EvalVariant<Scope.Scope> = {
        configuration: configuration("write-failure"),
        review: () =>
          Effect.gen(function* () {
            const call = yield* Ref.updateAndGet(calls, (count) => count + 1);

            if (call === 1) {
              return yield* Effect.acquireRelease(Deferred.succeed(blocked, undefined), () =>
                Deferred.succeed(released, undefined),
              ).pipe(Effect.andThen(Effect.never));
            }
            yield* Deferred.await(call === 2 ? blocked : saved);

            return successfulOutcome;
          }),
      };

      const outputFailure = PlatformError.systemError({
        _tag: "Unknown",
        module: "FileSystem",
        method: "writeAll",
        description: "Injected output failure",
      });

      const result = yield* writeObservations(
        output,
        runEvalSuite(suite, [variant], { trials: 6, concurrency: 2, caseIds: [] }),
      ).pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          open: Effect.fn(function* (path, options) {
            const file = yield* fs.open(path, options);

            yield* Ref.set(opened, Option.some(file));

            return {
              ...file,
              writeAll: (bytes: Uint8Array) =>
                Ref.updateAndGet(writes, (count) => count + 1).pipe(
                  Effect.flatMap((count) =>
                    count === 2 ? Effect.fail(outputFailure) : file.writeAll(bytes),
                  ),
                ),
              sync: file.sync.pipe(Effect.tap(() => Deferred.succeed(saved, undefined))),
            };
          }),
        }),
        Effect.result,
      );

      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "EvalDataError", operation: "write observations" },
      });
      expect(yield* Deferred.isDone(released)).toBe(true);
      const decoded = yield* decodeObservationLines(yield* fs.readFileString(output));

      expect(decoded.map((observation) => observation.trial)).toEqual([2]);
      const handle = yield* Ref.get(opened);

      expect(Option.isSome(handle)).toBe(true);
      if (Option.isSome(handle)) {
        expect((yield* Effect.result(handle.value.stat))._tag).toBe("Failure");
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
