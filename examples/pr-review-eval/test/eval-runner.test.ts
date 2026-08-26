import { fileURLToPath } from "node:url";

import {
  makeReviewer,
  ReviewChange,
  ReviewOutcome,
  ReviewReport,
  ReviewRequest,
} from "@effect-agent/pr-review";
import { ScriptedModel } from "@effect-agent/testing";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Fiber,
  FileSystem,
  Option,
  PlatformError,
  Ref,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { Model } from "effect/unstable/ai";

import {
  decodeObservationLines,
  digestReviewRequest,
  EvalCase,
  EvalCaseId,
  EvalDefectId,
  EvalExpectedDefect,
  EvalEvidence,
  EvalInputDigest,
  EvalReviewerFailure,
  EvalSuite,
  EvalVariantConfiguration,
  EvalVariantId,
  digestGuidance,
  loadEvalSuite,
  makeCurrentOpenAiVariant,
  runEvalSuite,
  validateEvalSuite,
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

const makeSuite = Effect.fn("PrReviewEvalTest.makeSuite")(function* () {
  const inputDigest = yield* digestReviewRequest(request);
  return EvalSuite.make({
    version: 1,
    cases: [
      EvalCase.make({
        version: 1,
        id: caseId,
        kind: "known-defects",
        provenance: "Synthetic fixture for the eval runner contract.",
        inputDigest,
        request,
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
    maxOutputTokens: 8_000,
    strictJsonSchema: true,
    store: false,
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
  it.effect("replays the real reviewer and round-trips its JSONL observation", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const model = Model.make(
        "scripted",
        "eval",
        ScriptedModel.layer([
          {
            _tag: "Stream",
            parts: [
              { type: "text-start", id: "review" },
              {
                type: "text-delta",
                id: "review",
                delta:
                  '{"summary":"One blocker.","findings":[{"path":"src/read.ts","line":1,"severity":"blocking","category":"correctness","title":"Optional value is dereferenced","body":"Calling read without a value now throws."}]}',
              },
              { type: "text-end", id: "review" },
              {
                type: "finish",
                reason: "stop",
                usage: {
                  inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
                  outputTokens: { total: 4 },
                },
              },
            ],
            termination: { _tag: "Complete" },
          },
        ]),
      );
      const reviewer = makeReviewer({ model });
      const variant: EvalVariant<never> = {
        configuration: configuration("scripted"),
        review: (input) =>
          reviewer.review(input).pipe(
            Effect.mapError((error) =>
              EvalReviewerFailure.make({
                errorTag: error._tag,
                message: "Scripted reviewer failed",
              }),
            ),
          ),
      };
      const observations = yield* runEvalSuite(suite, [variant], {
        trials: 1,
        concurrency: 1,
        caseIds: [],
      }).pipe(Stream.runCollect);
      expect(observations).toHaveLength(1);
      expect(observations[0]?.runnerVersion).toBe("0.1.1");
      expect(observations[0]?.result._tag).toBe("Succeeded");
      if (observations[0]?.result._tag === "Succeeded") {
        expect(observations[0].result.outcome.report.findings[0]?.title).toBe(
          "Optional value is dereferenced",
        );
      }

      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-eval-" });
      const output = `${directory}/observations.jsonl`;
      expect(yield* writeObservations(output, Stream.fromIterable(observations))).toBe(1);
      const decoded = yield* decodeObservationLines(yield* fs.readFileString(output));
      expect(decoded).toEqual(observations);
      const historical = (yield* fs.readFileString(output)).replace(
        '"runnerVersion":"0.1.1"',
        '"runnerVersion":"0.0.9"',
      );
      expect((yield* decodeObservationLines(historical))[0]?.runnerVersion).toBe("0.0.9");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("derives output-affecting guidance identity inside the live variant", () =>
    Effect.gen(function* () {
      const variant = yield* makeCurrentOpenAiVariant({
        id: Schema.decodeSync(EvalVariantId)("candidate-guidance-v1"),
        model: "gpt-5.6-sol",
        reasoningEffort: "medium",
        guidance: "  Keep the public error channel typed.  ",
      });
      expect(variant.configuration.id).toBe("candidate-guidance-v1");
      expect(variant.configuration.reviewerProfile).toBe("single-pass-v1");
      expect(variant.configuration.guidanceDigest).toBe(
        yield* digestGuidance("Keep the public error channel typed."),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("selects cases and records elapsed time from the Effect clock", () =>
    Effect.gen(function* () {
      const originalSuite = yield* makeSuite();
      const original = originalSuite.cases[0];
      expect(original).toBeDefined();
      if (original === undefined) return;

      const selectedId = Schema.decodeSync(EvalCaseId)("selected-case");
      const suite = EvalSuite.make({
        version: 1,
        cases: [original, EvalCase.make({ ...original, id: selectedId })],
      });
      const variant: EvalVariant<never> = {
        configuration: configuration("timed"),
        review: () => TestClock.adjust(5).pipe(Effect.as(successfulOutcome)),
      };
      const observations = yield* runEvalSuite(suite, [variant], {
        trials: 1,
        concurrency: 1,
        caseIds: [selectedId],
      }).pipe(Stream.runCollect);

      expect(observations).toHaveLength(1);
      expect(observations[0]?.caseId).toBe(selectedId);
      expect(observations[0]?.elapsedMillis).toBe(5);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "persists completed trials before interruption and closes model and file resources",
    () =>
      Effect.gen(function* () {
        const suite = yield* makeSuite();
        const acquired = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const calls = yield* Ref.make(0);
        const opened = yield* Ref.make<Option.Option<FileSystem.File>>(Option.none());
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-interrupted-" });
        const output = `${directory}/observations.jsonl`;
        const variant: EvalVariant<Scope.Scope> = {
          configuration: configuration("interruptible"),
          review: () =>
            Effect.gen(function* () {
              if ((yield* Ref.updateAndGet(calls, (count) => count + 1)) === 1) {
                return successfulOutcome;
              }
              return yield* Effect.acquireRelease(Deferred.succeed(acquired, undefined), () =>
                Deferred.succeed(released, undefined),
              ).pipe(Effect.andThen(Effect.never));
            }),
        };
        const fiber = yield* writeObservations(
          output,
          runEvalSuite(suite, [variant], { trials: 3, concurrency: 1, caseIds: [] }),
        ).pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            open: (path, options) =>
              fs.open(path, options).pipe(Effect.tap((file) => Ref.set(opened, Option.some(file)))),
          }),
          Effect.forkChild,
        );

        yield* Deferred.await(acquired);
        const persisted = yield* fs.readFileString(output);
        const decoded = yield* decodeObservationLines(persisted);
        expect(decoded.map((observation) => observation.trial)).toEqual([1]);
        expect(decoded[0]?.result._tag).toBe("Succeeded");
        yield* Fiber.interrupt(fiber);

        expect(yield* Deferred.isDone(released)).toBe(true);
        expect(yield* fs.readFileString(output)).toBe(persisted);
        expect(yield* Ref.get(calls)).toBe(2);
        const handle = yield* Ref.get(opened);
        expect(Option.isSome(handle)).toBe(true);
        if (Option.isSome(handle)) {
          expect((yield* Effect.result(handle.value.stat))._tag).toBe("Failure");
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses an existing output before starting any model call", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-existing-" });
      const output = `${directory}/observations.jsonl`;
      yield* fs.writeFileString(output, "existing evidence\n");
      const calls = yield* Ref.make(0);
      const variant: EvalVariant<never> = {
        configuration: configuration("existing"),
        review: () => Ref.update(calls, (count) => count + 1).pipe(Effect.as(successfulOutcome)),
      };

      const result = yield* writeObservations(
        output,
        runEvalSuite(suite, [variant], { trials: 1, concurrency: 1, caseIds: [] }),
      ).pipe(Effect.result);

      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "EvalDataError", operation: "open observations" },
      });
      expect(yield* Ref.get(calls)).toBe(0);
      expect(yield* fs.readFileString(output)).toBe("existing evidence\n");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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

  it.effect("bounds concurrent trials and records typed reviewer failures", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const active = yield* Ref.make(0);
      const maximum = yield* Ref.make(0);
      const gate = yield* Deferred.make<void>();
      const successful: EvalVariant<never> = {
        configuration: configuration("successful"),
        review: () =>
          Effect.gen(function* () {
            const count = yield* Ref.updateAndGet(active, (value) => value + 1);
            yield* Ref.update(maximum, (value) => Math.max(value, count));
            if (count === 2) yield* Deferred.succeed(gate, undefined);
            yield* Deferred.await(gate);
            yield* Ref.update(active, (value) => value - 1);
            return successfulOutcome;
          }),
      };
      const failing: EvalVariant<never> = {
        configuration: configuration("failing"),
        review: () =>
          EvalReviewerFailure.make({
            errorTag: "RateLimitError",
            message: "Provider declined the trial",
          }),
      };
      const observations = yield* runEvalSuite(suite, [successful, failing], {
        trials: 4,
        concurrency: 2,
        caseIds: [],
      }).pipe(Stream.runCollect);
      expect(yield* Ref.get(maximum)).toBe(2);
      expect(observations).toHaveLength(8);
      expect(
        observations.filter((observation) => observation.result._tag === "Failed"),
      ).toHaveLength(4);
      expect(
        observations
          .filter((observation) => observation.result._tag === "Failed")
          .every(
            (observation) =>
              observation.result._tag === "Failed" &&
              observation.result.errorTag === "RateLimitError",
          ),
      ).toBe(true);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects corrupt corpus identity before a model can run", () =>
    Effect.gen(function* () {
      const suite = yield* makeSuite();
      const original = suite.cases[0];
      expect(original).toBeDefined();
      if (original === undefined) return;

      const wrongDigest = Schema.decodeSync(EvalInputDigest)("0".repeat(64));
      const corrupt = EvalSuite.make({
        version: 1,
        cases: [EvalCase.make({ ...original, inputDigest: wrongDigest })],
      });
      const result = yield* Effect.result(validateEvalSuite(corrupt));
      expect(result._tag).toBe("Failure");

      const encoded = Schema.encodeSync(EvalSuite)(suite);
      expect(Schema.decodeSync(EvalSuite)(encoded)).toEqual(suite);
      const changedDigest = yield* digestReviewRequest(
        ReviewRequest.make({ ...request, headRevision: "changed-head" }),
      );
      expect(changedDigest).not.toBe(original.inputDigest);
      const persisted = yield* loadEvalSuite(
        fileURLToPath(new URL("../fixtures/smoke-suite.json", import.meta.url)),
      );
      expect(persisted.cases).toHaveLength(2);
      const duplicateCases = { ...encoded, cases: [encoded.cases[0], encoded.cases[0]] };
      const duplicateDefects = {
        ...encoded,
        cases: [
          {
            ...encoded.cases[0],
            expectedDefects: [
              encoded.cases[0]?.expectedDefects[0],
              encoded.cases[0]?.expectedDefects[0],
            ],
          },
        ],
      };
      const cleanWithDefect = {
        ...encoded,
        cases: [{ ...encoded.cases[0], kind: "clean-control" }],
      };
      const unknownEvidencePath = {
        ...encoded,
        cases: [
          {
            ...encoded.cases[0],
            expectedDefects: [
              {
                ...encoded.cases[0]?.expectedDefects[0],
                evidence: [
                  {
                    ...encoded.cases[0]?.expectedDefects[0]?.evidence[0],
                    path: "src/not-admitted.ts",
                  },
                ],
              },
            ],
          },
        ],
      };
      const malformedRevision = {
        ...encoded,
        cases: [
          {
            ...encoded.cases[0],
            request: { ...encoded.cases[0]?.request, headRevision: "" },
          },
        ],
      };
      for (const invalid of [
        duplicateCases,
        duplicateDefects,
        cleanWithDefect,
        unknownEvidencePath,
        malformedRevision,
      ]) {
        expect(Option.isNone(Schema.decodeUnknownOption(EvalSuite)(invalid))).toBe(true);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
