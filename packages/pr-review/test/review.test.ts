import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Ref, Result, Schema, Stream, Struct } from "effect";
import { TestClock } from "effect/testing";
import {
  AiError,
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Tool,
} from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

import {
  isCommentableLine,
  ReviewChange,
  ReviewFinding,
  ReviewRequest,
  ReviewContextError,
  ReviewFileList,
  ReviewRepository,
  ReviewSource,
  type ReviewVerificationError,
  makeReviewer,
} from "../src/index.ts";

const patch = `@@ -1,3 +1,4 @@
 unchanged
-old
+new
 tail
+added`;

const request = ReviewRequest.make({
  title: "Change",
  description: "",
  baseRevision: "base",
  headRevision: "head",
  changes: [ReviewChange.make({ path: "src/index.ts", patch })],
  unreviewedPaths: [],
});

const usage = {
  inputTokens: { total: 10, uncached: 7, cacheRead: 2, cacheWrite: 1 },
  outputTokens: { total: 4 },
};
const response = (value: object): Stream.Stream<Response.StreamPartEncoded> =>
  Stream.fromIterable([
    {
      type: "tool-call",
      id: "review",
      name: "submit_review",
      params: value,
    },
    { type: "finish", reason: "tool-calls", usage },
  ]);
const scriptedModel = (
  respond: (
    prompt: Prompt.Prompt,
    tools: ReadonlyArray<Tool.Any>,
  ) => Stream.Stream<Response.StreamPartEncoded, AiError.AiError>,
) =>
  Model.make(
    "scripted",
    "review",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt, tools }) => respond(prompt, tools),
      }),
    ),
  );
const emptyRepository = ReviewRepository.of({
  readFile: () => Effect.fail(ReviewContextError.make({ message: "Source unavailable" })),
  findFiles: () => Effect.succeed(ReviewFileList.make({ paths: [], truncated: false })),
});
const blocker = ReviewFinding.make({
  path: "src/index.ts",
  line: 2,
  severity: "blocking",
  category: "reliability",
  title: "Dropped acknowledgment",
  body: "A committed operation loses its acknowledgment; preserve the result until acknowledgment completes.",
});
const otherBlocker = ReviewFinding.make({
  ...blocker,
  category: "security",
  body: "The owner check is bypassed when the cached record is returned; check ownership before returning it.",
});
const unchangedDependency = ReviewFinding.make({
  ...otherBlocker,
  path: "src/unchanged-dependency.ts",
});
const externalTrigger = "A supported caller commits one operation for its owning request.";
const changedBehavior = {
  before: "The base caller completes the supported operation and returns its result.",
  after: "The changed branch drops the result before acknowledgment or skips the ownership check.",
  repairSafety: "Retain results through acknowledgment and return them only to the owning caller.",
};
const confirmed = (finding: ReviewFinding) => ({
  _tag: "confirmed",
  finding: {
    ...changedBehavior,
    impact: "The changed behavior loses required work or bypasses the ownership check.",
    ...finding,
  },
});
const discovered = (finding: ReviewFinding) => ({
  path: finding.path,
  ...(finding.line === undefined ? {} : { line: finding.line }),
  externalTrigger,
  headFailure: `${changedBehavior.after} ${finding.body}`,
  governingContract: "The supported operation must return its result only to its owner.",
});

describe("review output boundary", () => {
  it.effect(
    "PRR-002 investigates independently, verifies every cause, and charges every call on fresh repeats",
    () =>
      Effect.gen(function* () {
        const requests = yield* Ref.make<ReadonlyArray<Prompt.Prompt>>([]);
        const reads = yield* Ref.make(0);
        const model = scriptedModel((prompt) =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* Ref.update(requests, (values) => [...values, prompt]);
              const text = JSON.stringify(prompt);
              if (text.includes("Verify the supplied candidate defects")) {
                const handoff = prompt.content
                  .flatMap((message) =>
                    message.role === "user" && typeof message.content !== "string"
                      ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
                      : [],
                  )
                  .at(-1);
                expect(handoff).toBe(
                  JSON.stringify({
                    request,
                    candidates: [discovered(blocker), discovered(unchangedDependency)],
                  }),
                );
                expect(text).toContain(externalTrigger);
                expect(text).toContain(changedBehavior.after);
                return response({
                  decisions: [confirmed(blocker), confirmed(otherBlocker)],
                  additionalFindings: [],
                });
              }
              if (!text.includes('"role":"tool"')) {
                return Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: "tool-call",
                    id: "source",
                    name: "read_file",
                    params: { path: "src/index.ts", revision: "head", startLine: 1, lineCount: 4 },
                  },
                  { type: "finish", reason: "tool-calls", usage },
                ]);
              }
              return response({
                findings: [
                  discovered(
                    text.includes("Independently audit the change") ? unchangedDependency : blocker,
                  ),
                ],
              });
            }),
          ),
        );
        const reviewer = makeReviewer({
          model,
          estimateCostMicrousd: () => Effect.succeed(123),
        });
        const repository = ReviewRepository.of({
          ...emptyRepository,
          readFile: (input) =>
            Ref.update(reads, (value) => value + 1).pipe(
              Effect.as(
                ReviewSource.make({
                  path: input.path,
                  revision: input.revision,
                  startLine: 1,
                  totalLines: 4,
                  content: "unchanged\nnew\ntail\nadded",
                }),
              ),
            ),
        });
        const outcomes = yield* Effect.forEach([0, 1], () =>
          reviewer.review(request).pipe(Effect.provideService(ReviewRepository, repository)),
        );
        const prompts = yield* Ref.get(requests);
        expect(prompts).toHaveLength(10);
        expect(yield* Ref.get(reads)).toBe(4);
        expect(JSON.stringify(prompts[0])).not.toContain("<run-status>");
        expect(outcomes.map((outcome) => outcome.report.findings)).toEqual([
          [blocker, otherBlocker],
          [blocker, otherBlocker],
        ]);
        expect(outcomes[0]).toMatchObject({
          turns: 5,
          usage: {
            inputTokens: 50,
            uncachedInputTokens: 35,
            cachedInputTokens: 10,
            cacheWriteInputTokens: 5,
            outputTokens: 20,
            estimatedCostMicrousd: 615,
          },
        });
      }),
  );

  it.effect("PRR-002 recovers from repeated bounded source lookup failures", () =>
    Effect.gen(function* () {
      const reads = yield* Ref.make<
        ReadonlyArray<{
          readonly path: string;
          readonly startLine: number;
          readonly lineCount: number;
        }>
      >([]);
      const failedInputs = [
        { path: "src/missing.ts", revision: "head", startLine: 1, lineCount: 4 },
        { path: "src/index.ts", revision: "head", startLine: 99, lineCount: 4 },
        { path: "src/index.ts", revision: "head", startLine: 1, lineCount: 200 },
      ] as const;
      const correctedInput = {
        path: "src/index.ts",
        revision: "head",
        startLine: 1,
        lineCount: 4,
      } as const;
      const model = scriptedModel((prompt) => {
        const text = JSON.stringify(prompt);
        if (text.includes("Verify the supplied candidate defects")) {
          return response({ decisions: [], additionalFindings: [] });
        }
        if (text.includes("Independently audit the change")) {
          return response({ findings: [] });
        }
        const sourceResults = prompt.content
          .filter((message) => message.role === "tool")
          .flatMap((message) => message.content)
          .flatMap((part) =>
            part.type === "tool-result" && part.name === "read_file" ? [part] : [],
          );
        if (sourceResults.length > 0 && sourceResults.length <= failedInputs.length) {
          expect(sourceResults.at(-1)).toMatchObject({
            isFailure: true,
            result: { _tag: "ReviewContextError", message: "Source range unavailable" },
          });
        }
        if (sourceResults.length <= failedInputs.length) {
          return Stream.fromIterable<Response.StreamPartEncoded>([
            {
              type: "tool-call",
              id: `source-${String(sourceResults.length)}`,
              name: "read_file",
              params: failedInputs[sourceResults.length] ?? correctedInput,
            },
            { type: "finish", reason: "tool-calls", usage },
          ]);
        }
        expect(sourceResults.map((result) => result.isFailure)).toEqual([true, true, true, false]);
        expect(sourceResults[3]).toMatchObject({
          result: {
            path: correctedInput.path,
            revision: correctedInput.revision,
            startLine: correctedInput.startLine,
            totalLines: 4,
            content: "unchanged\nnew\ntail\nadded",
          },
        });
        return response({ findings: [] });
      });
      const repository = ReviewRepository.of({
        ...emptyRepository,
        readFile: (input) =>
          Effect.gen(function* () {
            yield* Ref.update(reads, (values) => [...values, input]);
            if (
              input.path !== correctedInput.path ||
              input.startLine !== correctedInput.startLine ||
              input.lineCount !== correctedInput.lineCount
            ) {
              return yield* ReviewContextError.make({ message: "Source range unavailable" });
            }
            return ReviewSource.make({
              path: input.path,
              revision: input.revision,
              startLine: input.startLine,
              totalLines: 4,
              content: "unchanged\nnew\ntail\nadded",
            });
          }),
      });

      const outcome = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, repository));

      expect(yield* Ref.get(reads)).toEqual([...failedInputs, correctedInput]);
      expect(outcome).toMatchObject({
        turns: 7,
        report: { findings: [] },
      });
    }),
  );

  it.effect("PRR-002 reviews a full long-path input without echoing paths in completion", () =>
    Effect.gen(function* () {
      const pathAt = (index: number) =>
        `src/${String(index).padStart(3, "0")}/${`${"segment".repeat(16)}/`.repeat(4)}${"x".repeat(52)}`;
      const wideRequest = ReviewRequest.make({
        ...request,
        changes: Array.from({ length: 100 }, (_, index) =>
          ReviewChange.make({ path: pathAt(index), patch }),
        ),
      });
      const model = scriptedModel((prompt) => {
        const text = JSON.stringify(prompt);
        const verifying = text.includes("Verify the supplied candidate defects");
        expect(text.indexOf(pathAt(0)) < text.indexOf(pathAt(99))).toBe(
          !text.includes("Independently audit the change"),
        );
        const params = verifying ? { decisions: [], additionalFindings: [] } : { findings: [] };
        // ASCII bytes are a conservative bound on output tokens for this response.
        expect(JSON.stringify(params).length).toBeLessThan(8_000);
        return response(params);
      });
      const result = yield* makeReviewer({ model })
        .review(wideRequest)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
      expect(result.report.findings).toEqual([]);
    }),
  );

  it.effect.each([
    { decisions: undefined, expectedError: "AiError" },
    { decisions: [], expectedError: "ReviewVerificationError" },
    {
      decisions: [confirmed(blocker), confirmed(otherBlocker)],
      expectedError: "ReviewVerificationError",
    },
    {
      decisions: [{ _tag: "duplicate", duplicateOf: 0 }],
      expectedError: "ReviewVerificationError",
    },
  ])(
    "PRR-002 refuses an omitted or unsupported verification decision %#",
    ({ decisions, expectedError }) =>
      Effect.gen(function* () {
        const model = scriptedModel((prompt) =>
          response(
            JSON.stringify(prompt).includes("Verify the supplied candidate defects")
              ? { decisions, additionalFindings: [] }
              : { findings: [discovered(blocker)] },
          ),
        );
        const result = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);
        expect(Result.isFailure(result) && result.failure._tag).toBe(expectedError);
      }),
  );

  it.effect("PRR-002 supports empty native verification without inventing an all-clear", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const model = scriptedModel((prompt, tools) => {
        const verifying = JSON.stringify(prompt).includes("Verify the supplied candidate defects");
        if (verifying) {
          expect(
            tools
              .filter((tool) => tool.name === "submit_review")
              .map((tool) => Tool.getJsonSchema(tool, { transformer: toCodecOpenAI })),
          ).toMatchObject([{ type: "object", properties: { decisions: { type: "array" } } }]);
        }
        return Stream.unwrap(
          Ref.update(calls, (n) => n + 1).pipe(
            Effect.as(
              response(
                verifying
                  ? { decisions: [], additionalFindings: [] }
                  : { findings: [], summary: "Safe to merge" },
              ),
            ),
          ),
        );
      });
      const outcome = yield* makeReviewer({ model })
        .review(ReviewRequest.make({ ...request, scope: "incremental" }))
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
      expect(yield* Ref.get(calls)).toBe(3);
      expect(outcome.report.findings).toEqual([]);
      expect(outcome.report.summary).toContain("does not resolve earlier findings");
      expect(outcome.report.summary).not.toContain("Safe to merge");
    }),
  );

  it.effect(
    "PRR-002 retains source evidence beyond a prose guideline without dropping a blocker",
    () =>
      Effect.gen(function* () {
        const evidence = `${changedBehavior.after} ${"Source context. ".repeat(60)}`;
        const model = scriptedModel((prompt, tools) =>
          Stream.unwrap(
            Effect.gen(function* () {
              if (JSON.stringify(prompt).includes("Verify the supplied candidate defects")) {
                expect(JSON.stringify(prompt)).toContain(evidence);
                return response({
                  decisions: [
                    {
                      _tag: "confirmed",
                      finding: { ...confirmed(blocker).finding, after: evidence },
                    },
                  ],
                  additionalFindings: [],
                });
              }
              const completion = tools.find((tool) => tool.name === "submit_review");
              expect(completion).toBeDefined();
              if (completion === undefined) return response({ findings: [] });
              const nativeSchema = JSON.stringify(
                Tool.getJsonSchema(completion, { transformer: toCodecOpenAI }),
              );
              expect(nativeSchema).toContain('"externalTrigger"');
              expect(nativeSchema).toContain('"headFailure"');
              expect(nativeSchema).toContain('"governingContract"');
              expect(nativeSchema).not.toContain('"changedCausalEdge"');
              expect(nativeSchema).not.toContain('"baseBehavior"');
              expect(nativeSchema).not.toContain('"severity"');
              expect(nativeSchema).not.toContain('"category"');
              expect(nativeSchema).not.toContain('"title"');
              expect(nativeSchema).not.toContain('"body"');
              expect(nativeSchema).not.toContain('"repairSafety"');
              const wireCodec = toCodecOpenAI(Schema.toEncoded(completion.parametersSchema)).codec;
              const parameters = {
                findings: [{ ...discovered(blocker), headFailure: evidence }],
              };
              yield* Schema.decodeUnknownEffect(wireCodec)(parameters).pipe(Effect.orDie);
              return response(parameters);
            }),
          ),
        );
        const result = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));
        expect(result.report.findings).toEqual([blocker]);
      }),
  );

  it.effect.each([true, false])(
    "PRR-002 audits an empty candidate set and validates added findings: changed path=%s",
    (changedPath) =>
      Effect.gen(function* () {
        const additional = [
          blocker,
          ReviewFinding.make({
            ...otherBlocker,
            path: changedPath ? otherBlocker.path : "src/unchanged.ts",
          }),
        ];
        const model = scriptedModel((prompt) =>
          response(
            JSON.stringify(prompt).includes("Verify the supplied candidate defects")
              ? {
                  decisions: [],
                  additionalFindings: additional.map((finding) => ({
                    ...changedBehavior,
                    impact: finding.body,
                    ...finding,
                  })),
                }
              : { findings: [] },
          ),
        );
        const result = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);
        if (changedPath) {
          expect(Result.isSuccess(result) && result.success.report.findings).toEqual(additional);
        } else {
          expect(Result.isFailure(result) && result.failure._tag).toBe("ReviewVerificationError");
        }
      }),
  );

  it.effect.each(["changed-caller", "pre-existing", "unchanged-anchor"] as const)(
    "PRR-002 hands off unchanged callees and requires final changed-path proof: %s",
    (mode) =>
      Effect.gen(function* () {
        const model = scriptedModel((prompt) => {
          if (!JSON.stringify(prompt).includes("Verify the supplied candidate defects")) {
            return response({ findings: [discovered(unchangedDependency)] });
          }
          const handoff = prompt.content
            .flatMap((message) =>
              message.role === "user" && typeof message.content !== "string"
                ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
                : [],
            )
            .at(-1);
          expect(handoff).toBe(
            JSON.stringify({ request, candidates: [discovered(unchangedDependency)] }),
          );
          return response({
            decisions:
              mode === "pre-existing"
                ? [
                    {
                      _tag: "rejected",
                      evidence: "The exact trigger reaches the same terminal failure at base.",
                    },
                  ]
                : [confirmed(mode === "changed-caller" ? otherBlocker : unchangedDependency)],
            additionalFindings: [],
          });
        });
        const result = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);
        if (mode === "changed-caller") {
          expect(Result.isSuccess(result) && result.success.report.findings).toEqual([
            otherBlocker,
          ]);
        } else if (mode === "pre-existing") {
          expect(Result.isSuccess(result) && result.success.report.findings).toEqual([]);
        } else {
          expect(Result.isFailure(result) && result.failure._tag).toBe("ReviewVerificationError");
        }
      }),
  );

  it.effect.each(["within-bound", "overflow", "anchor-duplicate"] as const)(
    "PRR-002 counts distinct findings after anchor validation: %s",
    (mode) =>
      Effect.gen(function* () {
        const candidates = Array.from({ length: 24 }, (_, index) =>
          ReviewFinding.make({
            ...(mode === "anchor-duplicate" && index === 0
              ? Struct.omit(blocker, ["line"])
              : blocker),
            title: `Independent cause ${String(index)}`,
            body: `Trigger and source evidence for cause ${String(index)}. `.padEnd(2_000, "."),
          }),
        );
        const model = scriptedModel((prompt) => {
          const text = JSON.stringify(prompt);
          return response(
            text.includes("Verify the supplied candidate defects")
              ? {
                  decisions: candidates.map(confirmed),
                  additionalFindings:
                    mode === "overflow"
                      ? [{ ...changedBehavior, impact: otherBlocker.body, ...otherBlocker }]
                      : mode === "anchor-duplicate"
                        ? candidates.slice(0, 1).map((finding) => ({
                            ...changedBehavior,
                            impact: "The required operation loses its acknowledgment.",
                            ...finding,
                            line: 999,
                          }))
                        : [],
                }
              : {
                  findings: text.includes("Independently audit the change")
                    ? candidates.slice(12).map(discovered)
                    : candidates.slice(0, 12).map(discovered),
                },
          );
        });
        const result = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);
        if (mode === "overflow") {
          expect(Result.isFailure(result) && result.failure._tag).toBe("ReviewVerificationError");
        } else {
          expect(Result.isSuccess(result) && result.success.report.findings).toEqual(candidates);
        }
      }),
  );

  it.effect("PRR-003 shares exact source ranges and rejects unavailable or oversized ranges", () =>
    Effect.gen(function* () {
      const input = {
        path: "src/index.ts",
        revision: "head",
        startLine: 1,
        lineCount: 200,
      } as const;
      const source = yield* ReviewSource.fromText(input, "first\nlast\n");
      expect(source).toMatchObject({ totalLines: 2, content: "first\nlast" });
      const empty = yield* ReviewSource.fromText(input, "");
      expect(empty).toMatchObject({ totalLines: 0, content: "" });
      const finalBlank = yield* ReviewSource.fromText({ ...input, startLine: 2 }, "first\n\n");
      expect(finalBlank).toMatchObject({ totalLines: 2, startLine: 2, content: "" });
      for (const read of [
        ReviewSource.fromText({ ...input, startLine: 3 }, "first\nlast\n"),
        ReviewSource.fromText({ ...input, lineCount: 201 }, "first\nlast\n"),
        ReviewSource.fromText(input, "x".repeat(20_001)),
      ]) {
        const result = yield* Effect.result(read);
        expect(Result.isFailure(result) && result.failure._tag).toBe("ReviewContextError");
      }
    }),
  );

  it.effect.each(["failure", "defect"] as const)(
    "PRR-002 closes the other investigation on %s",
    (mode) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const finalized = yield* Ref.make(false);
        const model = scriptedModel((prompt) => {
          if (JSON.stringify(prompt).includes("Trace each changed behavior from its entry point")) {
            return Stream.unwrap(
              Deferred.await(started).pipe(
                Effect.as(
                  mode === "defect"
                    ? Stream.die("model defect")
                    : Stream.fail(
                        AiError.AiError.make({
                          module: "test",
                          method: "streamText",
                          reason: AiError.UnknownError.make({ description: "provider failed" }),
                        }),
                      ),
                ),
              ),
            );
          }
          return Stream.unwrap(
            Deferred.succeed(started, undefined).pipe(
              Effect.as(Stream.never.pipe(Stream.ensuring(Ref.set(finalized, true)))),
            ),
          );
        });
        const result = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.exit);
        expect(Exit.isFailure(result)).toBe(true);
        expect(yield* Ref.get(finalized)).toBe(true);
      }),
  );

  it.effect.each(["timeout", "interruption"] as const)(
    "PRR-002 closes both investigations on %s",
    (mode) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const active = yield* Ref.make(0);
        const finalized = yield* Ref.make(0);
        const model = scriptedModel(() =>
          Stream.unwrap(
            Effect.gen(function* () {
              if ((yield* Ref.updateAndGet(active, (n) => n + 1)) === 2)
                yield* Deferred.succeed(started, undefined);
              return Stream.never.pipe(Stream.ensuring(Ref.update(finalized, (n) => n + 1)));
            }),
          ),
        );
        const fiber = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.forkChild);
        yield* Deferred.await(started);
        if (mode === "timeout") yield* TestClock.adjust("6 minutes");
        else yield* Fiber.interrupt(fiber);
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
        expect(yield* Ref.get(finalized)).toBe(2);
      }),
  );

  it("PRR-004 accepts only RIGHT-side patch lines", () => {
    expect([0, 1, 2, 3, 4, 5].filter((line) => isCommentableLine(patch, line))).toEqual([
      1, 2, 3, 4,
    ]);
  });
});

// Compile-time E/R proof: adding host pricing does not add an error or service requirement.
type Equal<Left, Right> =
  (<T>() => T extends Left ? 1 : 2) extends <T>() => T extends Right ? 1 : 2 ? true : false;
type Assert<T extends true> = T;
type EffectError<Value> =
  Value extends Effect.Effect<unknown, infer Error, unknown> ? Error : never;
type EffectRequirements<Value> =
  Value extends Effect.Effect<unknown, unknown, infer Requirements> ? Requirements : never;

declare const typedModel: Model.Model<"typed", LanguageModel.LanguageModel, never>;
const typedReviews = (model: typeof typedModel) => ({
  unpriced: makeReviewer({ model }).review(request),
  priced: makeReviewer({
    model,
    estimateCostMicrousd: () => Effect.succeed(1),
  }).review(request),
});
type TypedReviews = ReturnType<typeof typedReviews>;
const pricingTypeProofs: readonly [
  Assert<Equal<EffectError<TypedReviews["priced"]>, EffectError<TypedReviews["unpriced"]>>>,
  Assert<
    Equal<EffectRequirements<TypedReviews["priced"]>, EffectRequirements<TypedReviews["unpriced"]>>
  >,
  Assert<Equal<EffectRequirements<TypedReviews["priced"]>, ReviewRepository>>,
  Assert<
    Equal<
      Extract<EffectError<TypedReviews["priced"]>, ReviewVerificationError>,
      ReviewVerificationError
    >
  >,
] = [true, true, true, true];
void pricingTypeProofs;
