import {
  isCommentableLine,
  makeReviewer,
  ReviewChange,
  ReviewCostSnapshot,
  ReviewFinding,
  ReviewFollowUp,
  ReviewResolution,
  type ReviewOutcome,
  ReviewRequest,
  ReviewUsage,
  type ReviewVerificationError,
} from "@effect-agent/pr-review/Review";
import {
  ReviewContextError,
  ReviewFileList,
  ReviewRepository,
  ReviewSource,
} from "@effect-agent/pr-review/ReviewRepository";
import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Ref,
  Result,
  Schema,
  Stream,
  Struct,
} from "effect";
import { TestClock } from "effect/testing";
import {
  type AiError,
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Tool,
} from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

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

const followUp = ReviewFollowUp.make({
  id: "prior-review",
  description: "The earlier review blocks because omitted input is retained without a bound.",
});

const resolution = ReviewResolution.make({
  id: followUp.id,
  evidence:
    "src/index.ts now charges every candidate before retaining it, rejecting input over the aggregate bound.",
});

const usage = {
  inputTokens: { total: 10, uncached: 7, cacheRead: 2, cacheWrite: 1 },
  outputTokens: { total: 4 },
};

const response = (
  value: object,
  responseUsage: typeof usage = usage,
): Stream.Stream<Response.StreamPartEncoded> =>
  Stream.fromIterable([
    { type: "tool-call", id: "review", name: "submit_review", params: value },
    { type: "finish", reason: "tool-calls", usage: responseUsage },
  ]);

const toolResponse = (
  calls: ReadonlyArray<{ readonly name: string; readonly params: object }>,
  responseUsage: typeof usage = usage,
): Stream.Stream<Response.StreamPartEncoded> =>
  Stream.fromIterable([
    ...calls.map((call, index) => ({ type: "tool-call" as const, id: `call-${index}`, ...call })),
    { type: "finish", reason: "tool-calls", usage: responseUsage },
  ]);

const scriptedModel = (
  respond: (
    prompt: Prompt.Prompt,
    tools: ReadonlyArray<Tool.Any>,
    toolChoice: LanguageModel.ToolChoice<string>,
  ) => Stream.Stream<Response.StreamPartEncoded, AiError.AiError>,
) =>
  Model.make(
    "scripted",
    "review",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt, tools, toolChoice }) => respond(prompt, tools, toolChoice),
      }),
    ),
  );

const emptyRepository = ReviewRepository.of({
  readFile: () => Effect.fail(ReviewContextError.make({ message: "Source unavailable" })),
  findFiles: () => Effect.succeed(ReviewFileList.make({ paths: [], truncated: false })),
  searchCode: () => Effect.fail(ReviewContextError.make({ message: "Source unavailable" })),
});

const largeRequest = (count: number, patchCharacters = 70_014) =>
  ReviewRequest.make({
    ...request,
    changes: Array.from({ length: count }, (_, index) =>
      ReviewChange.make({
        path: `src/part-${String(index)}.ts`,
        patch: "@@ -0,0 +1 @@\n+".padEnd(patchCharacters, "x"),
      }),
    ),
  });

const costControl = (calls: Ref.Ref<number>) => ({
  snapshot: Ref.get(calls).pipe(
    Effect.map((modelCalls) =>
      ReviewCostSnapshot.make({
        stopped: false,
        modelCalls,
        usage: ReviewUsage.make({
          inputTokens: modelCalls * 10,
          uncachedInputTokens: modelCalls * 7,
          cachedInputTokens: modelCalls * 2,
          cacheWriteInputTokens: modelCalls,
          outputTokens: modelCalls * 4,
        }),
      }),
    ),
  ),
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
  title: "Ownership check bypassed",
  body: "The cached return bypasses the owner check; check ownership before returning the record.",
});

const importantFinding = ReviewFinding.make({
  ...otherBlocker,
  severity: "important",
  title: "Nonblocking fallback error",
  body: "The fallback reports the wrong optional status; return the status produced by the supported fallback.",
});

const nitFinding = ReviewFinding.make({
  ...otherBlocker,
  severity: "nit",
  title: "Minor diagnostic mismatch",
  body: "The diagnostic names the wrong optional phase; use the phase that produced the message.",
});

const submittedFinding = (
  finding: ReviewFinding,
  priority: 0 | 1 | 2 | 3,
): Omit<ReviewFinding, "severity"> & { readonly priority: 0 | 1 | 2 | 3 } => ({
  ...Struct.omit(finding, ["severity"]),
  priority,
});

const sourceResults = (prompt: Prompt.Prompt) =>
  prompt.content
    .filter((message) => message.role === "tool")
    .flatMap((message) => message.content)
    .flatMap((part) => (part.type === "tool-result" && part.name === "read_file" ? [part] : []));

const completionResult = (prompt: Prompt.Prompt) =>
  prompt.content
    .flatMap((message) => (message.role === "tool" ? message.content : []))
    .findLast((part) => part.type === "tool-result" && part.name === "submit_review");

const reviewInput = (prompt: Prompt.Prompt): string =>
  prompt.content
    .flatMap((message) =>
      message.role === "user" && typeof message.content !== "string"
        ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
        : [],
    )
    .at(0) ?? "";

class ResearchContext extends Context.Service<ResearchContext, { readonly summary: string }>()(
  "test/ReviewResearchContext",
) {}

describe("review output boundary", () => {
  it.effect("retains shared model requirements and uses the host cost snapshot", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      let parentCalls = 0;

      const child = Model.make(
        "scripted",
        "research",
        Layer.effect(
          LanguageModel.LanguageModel,
          Effect.gen(function* () {
            const configuration = yield* ResearchContext;

            return yield* LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Ref.update(calls, (count) => count + 1).pipe(
                    Effect.as(
                      toolResponse([
                        {
                          name: "finish_research",
                          params: {
                            summary: configuration.summary,
                            incomplete: false,
                          },
                        },
                      ]),
                    ),
                  ),
                ),
            });
          }),
        ),
      );

      const review = makeReviewer({
        model: Model.make(
          "scripted",
          "review",
          Layer.effect(
            LanguageModel.LanguageModel,
            Effect.andThen(ResearchContext, LanguageModel.LanguageModel),
          ).pipe(
            Layer.provide(
              scriptedModel(() =>
                Stream.unwrap(
                  Ref.update(calls, (count) => count + 1).pipe(
                    Effect.as(
                      ++parentCalls === 1
                        ? toolResponse([
                            {
                              name: "delegate_research",
                              params: {
                                question: "Check the caller.",
                                paths: ["src/index.ts"],
                              },
                            },
                          ])
                        : response({}),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
        research: { model: child },
        costControl: {
          snapshot: costControl(calls).snapshot.pipe(
            Effect.map((snapshot) =>
              ReviewCostSnapshot.make({
                ...snapshot,
                usage: ReviewUsage.make({
                  ...snapshot.usage,
                  estimatedCostMicrousd: 999,
                  reservedCostMicrousd: 321,
                }),
              }),
            ),
          ),
        },
      }).review(request);

      expectTypeOf<Effect.Services<typeof review>>().toEqualTypeOf<
        ReviewRepository | ResearchContext
      >();
      expectTypeOf<Effect.Error<typeof review>>().not.toBeAny();

      const outcome = yield* review.pipe(
        Effect.provideService(ReviewRepository, emptyRepository),
        Effect.provideService(ResearchContext, { summary: "Verified the caller contract." }),
      );

      expect(outcome.turns).toBe(3);
      expect(outcome.usage).toMatchObject({
        inputTokens: 30,
        estimatedCostMicrousd: 999,
        reservedCostMicrousd: 321,
      });
      expect(outcome.research?.completed).toBe(1);
      expect(outcome.incomplete).toBeUndefined();
    }),
  );

  it.effect.each(["complete", "incomplete", "failed", "exhausted"] as const)(
    "retains child findings and enforces child completion: %s",
    (mode) =>
      Effect.gen(function* () {
        let parentCalls = 0;
        let childCalls = 0;

        const child = scriptedModel((prompt, tools, toolChoice) => {
          childCalls += 1;
          if (childCalls === 1) {
            expect(tools.map(({ name }) => name).sort()).toEqual([
              "find_files",
              "finish_research",
              "read_file",
              "record_finding",
              "search_code",
            ]);
            expect(JSON.parse(reviewInput(prompt))).toMatchObject({
              question: "Check the acknowledgment contract.",
              baseRevision: "base",
              headRevision: "head",
              changes: [{ path: "src/index.ts", patch }],
              savedFindings: [],
            });
            expect(JSON.stringify(prompt.content)).toContain("Preserve acknowledgments.");

            return toolResponse([
              { name: "record_finding", params: submittedFinding(blocker, 1) },
              ...(mode === "exhausted"
                ? Array.from({ length: 1 }, () => ({
                    name: "find_files",
                    params: { query: "index", revision: "head" },
                  }))
                : []),
            ]);
          }
          if (mode === "exhausted") {
            expect(childCalls).toBeLessThanOrEqual(7);
            if (toolChoice === "required")
              return toolResponse(
                Array.from({ length: 2 }, (_, index) => ({
                  name: "find_files",
                  params: { query: `caller-${childCalls}-${index}`, revision: "head" },
                })),
              );
            expect(toolChoice).toEqual({ tool: "finish_research" });
          }

          return toolResponse([
            {
              name: "finish_research",
              params: {
                summary: mode === "failed" ? 123 : "The source contract was checked.",
                incomplete: mode === "incomplete",
              },
            },
          ]);
        });

        const review = makeReviewer({
          model: scriptedModel((prompt) => {
            parentCalls += 1;
            if (parentCalls === 1)
              return toolResponse([
                {
                  name: "delegate_research",
                  params: {
                    question: "Check the acknowledgment contract.",
                    paths: ["src/index.ts"],
                  },
                },
              ]);
            if (parentCalls === 2) return toolResponse([{ name: "review_status", params: {} }]);
            expect(JSON.stringify(prompt.content)).toContain(blocker.title);

            return response({ resolutions: [resolution] });
          }),
          research: { model: child },
          guidance: "Preserve acknowledgments.",
          estimateCostMicrousd: () => Effect.succeed(123),
        }).review(ReviewRequest.make({ ...request, followUps: [followUp] }));

        expectTypeOf<Effect.Services<typeof review>>().toEqualTypeOf<ReviewRepository>();
        expectTypeOf<Effect.Error<typeof review>>().not.toBeAny();
        expectTypeOf<
          Extract<Effect.Error<typeof review>, AiError.AiError | ReviewVerificationError>
        >().toEqualTypeOf<AiError.AiError | ReviewVerificationError>();

        const outcome = yield* review.pipe(
          Effect.provideService(ReviewRepository, emptyRepository),
        );

        expect(outcome.report.findings).toEqual([blocker]);
        expect(outcome.research).toEqual({
          delegations: 1,
          started: 1,
          completed: mode === "failed" ? 0 : 1,
          failed: mode === "failed" ? 1 : 0,
          interrupted: 0,
          incomplete: mode === "incomplete" || mode === "exhausted" ? 1 : 0,
        });
        expect(outcome.incomplete).toBe(mode === "complete" ? undefined : true);
        expect(outcome.resolutions).toEqual(mode === "complete" ? [resolution] : undefined);
        const accountedCalls = parentCalls + childCalls - (mode === "failed" ? 1 : 0);

        expect(outcome.turns).toBe(accountedCalls);
        expect(outcome.usage.inputTokens).toBe(accountedCalls * 10);
        expect(outcome.usage.estimatedCostMicrousd).toBe(accountedCalls * 123);
      }),
  );

  it.effect.each(["unknown", "duplicate", "oversized"] as const)(
    "rejects invalid research scope before starting a child: %s",
    (mode) =>
      Effect.gen(function* () {
        let parentCalls = 0;
        let childCalls = 0;
        const input = mode === "oversized" ? largeRequest(1, 32_001) : request;
        const path = input.changes[0]!.path;

        const outcome = yield* makeReviewer({
          model: scriptedModel((prompt) => {
            parentCalls += 1;
            if (parentCalls > 2) {
              expect(completionResult(prompt)).toMatchObject({ isFailure: true });

              return Stream.empty;
            }

            return parentCalls === 1
              ? toolResponse([
                  {
                    name: "delegate_research",
                    params: {
                      question: "Check the bounds.",
                      paths:
                        mode === "unknown"
                          ? ["src/not-admitted.ts"]
                          : mode === "duplicate"
                            ? [path, path]
                            : [path],
                    },
                  },
                ])
              : response({});
          }),
          research: {
            model: scriptedModel(() => {
              childCalls += 1;

              return toolResponse([
                { name: "finish_research", params: { summary: "Done.", incomplete: false } },
              ]);
            }),
          },
        })
          .review(input)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(childCalls).toBe(0);
        expect(outcome.incomplete).toBe(true);
        expect(outcome.research).toEqual({
          delegations: 1,
          started: 0,
          completed: 0,
          failed: 0,
          interrupted: 0,
          incomplete: 0,
        });
      }),
  );

  it.effect.each([1, 2] as const)(
    "bounds research concurrency to %i and total children to two",
    (concurrency) =>
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const firstStarted = yield* Deferred.make<void>();
        const bothStarted = yield* Deferred.make<void>();
        const active = yield* Ref.make(0);
        const started = yield* Ref.make(0);
        let parentCalls = 0;

        const child = scriptedModel(() =>
          Stream.fromEffect(
            Effect.gen(function* () {
              yield* Ref.update(active, (count) => count + 1);
              const count = yield* Ref.updateAndGet(started, (current) => current + 1);

              yield* Deferred.succeed(count === 1 ? firstStarted : bothStarted, undefined);
              yield* Deferred.await(release);
            }),
          ).pipe(
            Stream.flatMap(() =>
              toolResponse([
                { name: "finish_research", params: { summary: "Checked.", incomplete: false } },
              ]),
            ),
            Stream.ensuring(Ref.update(active, (count) => count - 1)),
          ),
        );

        const fiber = yield* makeReviewer({
          model: scriptedModel((prompt) => {
            if (++parentCalls === 1)
              return toolResponse(
                Array.from({ length: 3 }, (_, index) => ({
                  name: "delegate_research",
                  params: { question: `Check contract ${index}.`, paths: ["src/index.ts"] },
                })),
              );
            expect(JSON.stringify(prompt.content)).toContain("SubagentBudgetExhausted");

            return response({});
          }),
          research: { model: child, concurrency },
        })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.forkChild);

        yield* Deferred.await(firstStarted);
        if (concurrency === 2) yield* Deferred.await(bothStarted);
        expect(yield* Ref.get(active)).toBe(concurrency);
        yield* Deferred.succeed(release, undefined);
        const outcome = yield* Fiber.join(fiber);

        expect(yield* Ref.get(active)).toBe(0);
        expect(yield* Ref.get(started)).toBe(2);
        expect(outcome.research).toEqual({
          delegations: 3,
          started: 2,
          completed: 2,
          failed: 0,
          interrupted: 0,
          incomplete: 0,
        });
        expect(outcome.incomplete).toBe(true);
      }),
  );

  it.effect("child research cannot establish parent diff coverage", () =>
    Effect.gen(function* () {
      let parentCalls = 0;

      const input = ReviewRequest.make({
        ...request,
        changes: [...request.changes, ...largeRequest(1).changes],
      });

      const outcome = yield* makeReviewer({
        model: scriptedModel((prompt) => {
          parentCalls += 1;
          if (parentCalls > 2) {
            expect(completionResult(prompt)).toMatchObject({ isFailure: true });

            return Stream.empty;
          }

          return parentCalls === 1
            ? toolResponse([
                {
                  name: "delegate_research",
                  params: { question: "Check this contract.", paths: ["src/index.ts"] },
                },
              ])
            : response({});
        }),
        research: {
          model: scriptedModel(() =>
            toolResponse([
              { name: "finish_research", params: { summary: "Checked.", incomplete: false } },
            ]),
          ),
        },
      })
        .review(input)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.research?.completed).toBe(1);
      expect(outcome.pendingPaths).toEqual(input.changes.map(({ path }) => path));
      expect(outcome.incomplete).toBe(true);
    }),
  );

  it.effect(
    "child context pressure uses its own compactor without clearing queued parent reads",
    () =>
      Effect.gen(function* () {
        let parentCalls = 0;
        let childCalls = 0;

        const input = ReviewRequest.make({
          ...request,
          changes: [...request.changes, ...largeRequest(1, 40_000).changes],
        });

        const outcome = yield* makeReviewer({
          model: scriptedModel((prompt) => {
            parentCalls += 1;
            if (parentCalls > 3) {
              expect(completionResult(prompt)).toMatchObject({ isFailure: true });

              return Stream.empty;
            }
            if (parentCalls === 1)
              return toolResponse([
                { name: "read_diff", params: { offset: 0 } },
                {
                  name: "delegate_research",
                  params: { question: "Trace both callers.", paths: ["src/index.ts"] },
                },
              ]);
            if (parentCalls === 2) return toolResponse([{ name: "review_status", params: {} }]);

            const status = prompt.content
              .flatMap((message) => (message.role === "tool" ? message.content : []))
              .find((part) => part.type === "tool-result" && part.name === "review_status");

            expect(status).toMatchObject({
              result: {
                pending: [{ path: "src/part-0.ts", offset: 32_000 }],
                pendingCount: 1,
              },
            });

            return response({});
          }),
          compaction: "rollover",
          research: {
            model: scriptedModel((prompt) => {
              childCalls += 1;
              if (childCalls <= 4)
                return toolResponse(
                  [
                    {
                      name: "read_file",
                      params: {
                        path: `src/context-${childCalls}.ts`,
                        revision: "head",
                        startLine: 1,
                        lineCount: 1,
                      },
                    },
                  ],
                  {
                    inputTokens: {
                      total: childCalls === 4 ? 30_000 : 10_000,
                      uncached: childCalls === 4 ? 30_000 : 10_000,
                      cacheRead: 0,
                      cacheWrite: 0,
                    },
                    outputTokens: { total: 4 },
                  },
                );
              const text = JSON.stringify(prompt.content);

              expect(text).not.toContain("A fresh context window has started.");
              expect(text).not.toContain("a".repeat(20_000));
              expect(text).toContain("b".repeat(20_000));

              return toolResponse([
                {
                  name: "finish_research",
                  params: { summary: "Both callers checked.", incomplete: false },
                },
              ]);
            }),
          },
        })
          .review(input)
          .pipe(
            Effect.provideService(ReviewRepository, {
              ...emptyRepository,
              readFile: (input) =>
                Effect.succeed(
                  ReviewSource.make({
                    ...input,
                    totalLines: 1,
                    content: (input.path === "src/context-1.ts" ? "a" : "b").repeat(20_000),
                  }),
                ),
            }),
          );

        expect(childCalls).toBe(5);
        expect(outcome.research?.completed).toBe(1);
        expect(outcome.compactions).toEqual([]);
        expect(outcome.pendingPaths).toEqual(["src/part-0.ts"]);
      }),
  );

  it.effect(
    "concurrent research retains the same findings regardless of child completion order",
    () =>
      Effect.gen(function* () {
        const initial = Array.from({ length: 24 }, (_, index) =>
          ReviewFinding.make({
            ...importantFinding,
            title: `Z cause ${String(index).padStart(2, "0")}`,
          }),
        );

        const alpha = ReviewFinding.make({ ...importantFinding, title: "Alpha cause" });
        const beta = ReviewFinding.make({ ...importantFinding, title: "Beta cause" });
        const reports: Array<ReadonlyArray<ReviewFinding>> = [];

        for (const first of ["Alpha", "Beta"]) {
          const firstRecorded = yield* Deferred.make<void>();
          let parentCalls = 0;

          const outcome = yield* makeReviewer({
            model: scriptedModel(() => {
              parentCalls += 1;
              if (parentCalls === 1)
                return toolResponse(
                  initial.map((finding) => ({
                    name: "record_finding",
                    params: submittedFinding(finding, 2),
                  })),
                );
              if (parentCalls === 2)
                return toolResponse(
                  ["Alpha", "Beta"].map((question) => ({
                    name: "delegate_research",
                    params: { question, paths: ["src/index.ts"] },
                  })),
                );

              return response({});
            }),
            research: {
              model: scriptedModel((prompt) => {
                const question = JSON.parse(reviewInput(prompt)).question;
                const recorded = prompt.content.some((message) => message.role === "tool");

                return Stream.fromEffect(
                  recorded || question === first
                    ? recorded
                      ? Deferred.succeed(firstRecorded, undefined).pipe(Effect.asVoid)
                      : Effect.void
                    : Deferred.await(firstRecorded),
                ).pipe(
                  Stream.flatMap(() =>
                    toolResponse(
                      recorded
                        ? [
                            {
                              name: "finish_research",
                              params: { summary: "Checked.", incomplete: false },
                            },
                          ]
                        : [
                            {
                              name: "record_finding",
                              params: submittedFinding(question === "Alpha" ? alpha : beta, 2),
                            },
                          ],
                    ),
                  ),
                );
              }),
            },
          })
            .review(request)
            .pipe(Effect.provideService(ReviewRepository, emptyRepository));

          reports.push(outcome.report.findings);
          expect(outcome.report.findings).toEqual([alpha, beta, ...initial.slice(0, 22)]);
          expect(outcome.research?.completed).toBe(2);
          expect(outcome.incomplete).toBe(true);
        }
        expect(reports[0]).toEqual(reports[1]);
      }),
  );

  it.effect.each(["interrupt", "defect", "timeout"] as const)(
    "closes attached child streams on %s",
    (mode) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const finalized = yield* Ref.make(0);
        let parentCalls = 0;
        let childCalls = 0;

        const fiber = yield* makeReviewer({
          model: scriptedModel(() =>
            ++parentCalls === 1
              ? toolResponse([
                  {
                    name: "delegate_research",
                    params: { question: "Check cleanup.", paths: ["src/index.ts"] },
                  },
                ])
              : response({}),
          ),
          research: {
            model: scriptedModel(() =>
              (++childCalls === 1
                ? toolResponse([{ name: "record_finding", params: submittedFinding(blocker, 1) }])
                : Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
                    Stream.flatMap(() =>
                      mode === "defect" ? Stream.die("child defect") : Stream.never,
                    ),
                  )
              ).pipe(Stream.ensuring(Ref.update(finalized, (count) => count + 1))),
            ),
          },
        })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.forkChild);

        yield* Deferred.await(started);
        if (mode === "interrupt") yield* Fiber.interrupt(fiber);
        if (mode === "timeout") {
          yield* TestClock.adjust("61 seconds");
          const outcome = yield* Fiber.join(fiber);

          expect(outcome.report.findings).toEqual([blocker]);
          expect(outcome.incomplete).toBe(true);
          expect(outcome.research?.failed).toBe(1);
        } else {
          expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
        }
        expect(yield* Ref.get(finalized)).toBe(2);
      }),
  );

  it.effect.each([16_000, 128_000])("accepts a %i-token working context", (contextTokenLimit) =>
    Effect.gen(function* () {
      const outcome = yield* makeReviewer({
        model: scriptedModel(() => response({})),
        contextTokenLimit,
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.compactions).toEqual([]);
      expect(outcome.incomplete).toBeUndefined();
    }),
  );

  it.effect.each([15_999, 128_001, 32_000.5, Number.NaN, Number.POSITIVE_INFINITY])(
    "rejects invalid working context %s before invoking the model",
    (contextTokenLimit) =>
      Effect.gen(function* () {
        let calls = 0;

        const error = yield* makeReviewer({
          contextTokenLimit,
          model: scriptedModel(() => {
            calls += 1;

            return response({});
          }),
        })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.flip);

        expect(error._tag).toBe("ReviewVerificationError");
        expect(calls).toBe(0);
      }),
  );

  it.effect.each(["complete", "incomplete", "excluded", "cost", "omitted"] as const)(
    "returns only explicit resolutions after complete coverage: %s",
    (mode) =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const control = costControl(calls);

        const review = makeReviewer({
          model: scriptedModel((prompt) => {
            expect(reviewInput(prompt)).toContain(followUp.description);

            return Stream.unwrap(
              Ref.update(calls, (n) => n + 1).pipe(
                Effect.as(
                  response({
                    ...(mode === "omitted" ? {} : { resolutions: [resolution] }),
                    ...(mode === "incomplete"
                      ? {
                          blockedOn:
                            "The caller source at src/caller.ts is unavailable, so its acknowledgment contract cannot be checked.",
                        }
                      : {}),
                  }),
                ),
              ),
            );
          }),
          costControl: {
            snapshot: control.snapshot.pipe(
              Effect.map((snapshot) =>
                ReviewCostSnapshot.make({
                  ...snapshot,
                  stopped: mode === "cost" && snapshot.modelCalls > 0,
                }),
              ),
            ),
          },
        }).review(
          ReviewRequest.make({
            ...request,
            followUps: [followUp],
            unreviewedPaths: mode === "excluded" ? ["src/other.ts"] : [],
          }),
        );

        expectTypeOf<Effect.Services<typeof review>>().toEqualTypeOf<ReviewRepository>();
        expectTypeOf<
          Extract<Effect.Error<typeof review>, ReviewVerificationError>
        >().toEqualTypeOf<ReviewVerificationError>();

        const outcome = yield* review.pipe(
          Effect.provideService(ReviewRepository, emptyRepository),
        );

        expect(outcome.resolutions ?? []).toEqual(mode === "complete" ? [resolution] : []);
      }),
  );

  it.effect.each(["unknown", "duplicate"] as const)("rejects invalid resolutions: %s", (mode) =>
    Effect.gen(function* () {
      const outcome = yield* makeReviewer({
        model: scriptedModel(() =>
          response({
            resolutions:
              mode === "unknown" ? [{ ...resolution, id: "forged" }] : [resolution, resolution],
          }),
        ),
      })
        .review(ReviewRequest.make({ ...request, followUps: [followUp] }))
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.incomplete).toBe(true);
      expect(outcome.resolutions).toBeUndefined();
      expect(outcome.report.findings).toEqual([]);
    }),
  );

  it.effect(
    "preserves findings and identifies genuinely missing evidence after complete diff coverage",
    () =>
      Effect.gen(function* () {
        let calls = 0;

        const blockedOn =
          "src/caller.ts at head is unavailable, so the downstream acknowledgment behavior cannot be verified.";

        const model = scriptedModel((prompt) => {
          calls += 1;

          if (calls === 1)
            return toolResponse([
              { name: "record_finding", params: submittedFinding(blocker, 1) },
              {
                name: "read_file",
                params: { path: "src/caller.ts", revision: "head", startLine: 1, lineCount: 20 },
              },
            ]);
          expect(sourceResults(prompt)).toMatchObject([
            { isFailure: true, result: { _tag: "ReviewContextError" } },
          ]);

          return response({ blockedOn, resolutions: [resolution] });
        });

        const outcome = yield* makeReviewer({ model })
          .review(ReviewRequest.make({ ...request, followUps: [followUp] }))
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(outcome.report.findings).toEqual([blocker]);
        expect(outcome.incomplete).toBe(true);
        expect(outcome.exhausted).toBeUndefined();
        expect(outcome.pendingPaths).toBeUndefined();
        expect(outcome.blockedOn).toBe(blockedOn);
        expect(outcome.report.summary).toContain(blockedOn);
        expect(outcome.resolutions).toBeUndefined();
      }),
  );

  it.effect("PRR-002 completes one native review and keeps independent same-line blockers", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);

      const model = scriptedModel((prompt, tools, toolChoice) => {
        expect(toolChoice).toBe("required");
        expect(tools.map((tool) => tool.name)).toEqual([
          "read_file",
          "find_files",
          "search_code",
          "record_finding",
          "new_context",
          "read_diff",
          "review_status",
          "submit_review",
        ]);
        for (const tool of tools)
          expect(Tool.getJsonSchema(tool, { transformer: toCodecOpenAI })).toMatchObject({
            type: "object",
          });
        const text = reviewInput(prompt);

        expect(text).toContain(patch);
        expect(text.split(patch)).toHaveLength(2);
        expect(text).not.toContain("__new hunk__");
        expect(text).not.toContain("__old hunk__");

        const completion = tools.find((tool) => tool.name === "submit_review");

        expect(completion).toBeDefined();
        if (completion !== undefined) {
          const schema = JSON.stringify(
            Tool.getJsonSchema(completion, { transformer: toCodecOpenAI }),
          );

          expect(schema).toContain('"resolutions"');
          expect(schema).toContain('"blockedOn"');
          expect(schema).not.toContain('"incomplete"');
          expect(schema).not.toContain('"findings"');
          expect(schema).not.toContain('"priority"');
        }

        const recording = tools.find((tool) => tool.name === "record_finding");

        expect(recording).toBeDefined();
        if (recording !== undefined) {
          const schema = JSON.stringify(
            Tool.getJsonSchema(recording, { transformer: toCodecOpenAI }),
          );

          expect(schema).toContain('"priority"');
          expect(schema).not.toContain('"severity"');
          expect(schema.indexOf('"body"')).toBeLessThan(schema.indexOf('"priority"'));
        }

        return Stream.unwrap(
          Ref.updateAndGet(calls, (count) => count + 1).pipe(
            Effect.map((call) =>
              call === 1
                ? toolResponse(
                    [
                      submittedFinding(blocker, 0),
                      submittedFinding(otherBlocker, 1),
                      submittedFinding(importantFinding, 2),
                      submittedFinding(nitFinding, 3),
                    ].map((params) => ({ name: "record_finding", params })),
                  )
                : response({}),
            ),
          ),
        );
      });

      const review = makeReviewer({
        model,
        guidance: "Preserve acknowledgments.",
        estimateCostMicrousd: () => Effect.succeed(123),
      }).review(request);

      expectTypeOf<Effect.Services<typeof review>>().toEqualTypeOf<ReviewRepository>();
      expectTypeOf<Effect.Error<typeof review>>().not.toBeAny();
      expectTypeOf<
        Extract<Effect.Error<typeof review>, AiError.AiError | ReviewVerificationError>
      >().toEqualTypeOf<AiError.AiError | ReviewVerificationError>();
      const outcome = yield* review.pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(yield* Ref.get(calls)).toBe(2);
      expect(outcome.report.findings).toEqual([
        blocker,
        otherBlocker,
        importantFinding,
        nitFinding,
      ]);
      expect(outcome).toMatchObject({
        turns: 2,
        usage: {
          inputTokens: 20,
          uncachedInputTokens: 14,
          cachedInputTokens: 4,
          cacheWriteInputTokens: 2,
          outputTokens: 8,
          estimatedCostMicrousd: 246,
        },
      });
      expect(outcome.exhausted).toBeUndefined();
    }),
  );

  it.effect(
    "PRR-002 reads immutable base and head source and recovers from a bounded failure",
    () =>
      Effect.gen(function* () {
        const reads = yield* Ref.make<
          ReadonlyArray<Parameters<typeof emptyRepository.readFile>[0]>
        >([]);

        const failedInputs = [
          {
            path: "src/missing.ts",
            revision: "base",
            startLine: 1,
            lineCount: 4,
          },
          {
            path: "src/index.ts",
            revision: "base",
            startLine: 99,
            lineCount: 4,
          },
          {
            path: "src/index.ts",
            revision: "head",
            startLine: 99,
            lineCount: 4,
          },
        ] as const;

        const base = {
          path: "src/index.ts",
          revision: "base",
          startLine: 1,
          lineCount: 4,
        } as const;

        const head = { ...base, revision: "head" } as const;

        const recoveryUsage = {
          inputTokens: { total: 40_000, uncached: 40_000, cacheRead: 0, cacheWrite: 0 },
          outputTokens: { total: 4 },
        };

        const model = scriptedModel((prompt) => {
          const results = sourceResults(prompt);

          if (results.length < failedInputs.length) {
            if (results.length > 0) {
              expect(results.at(-1)).toMatchObject({
                isFailure: true,
                result: { _tag: "ReviewContextError", message: "Source range unavailable" },
              });
            }

            return Stream.fromIterable([
              {
                type: "tool-call",
                id: `failed-${String(results.length)}`,
                name: "read_file",
                params: failedInputs[results.length] ?? base,
              },
              { type: "finish", reason: "tool-calls", usage: recoveryUsage },
            ]);
          }
          if (results.length === failedInputs.length) {
            expect(results.map((result) => result.isFailure)).toEqual([true, true, true]);

            return Stream.fromIterable([
              { type: "tool-call", id: "base", name: "read_file", params: base },
              { type: "finish", reason: "tool-calls", usage: recoveryUsage },
            ]);
          }
          if (results.length === failedInputs.length + 1) {
            expect(results.at(-1)).toMatchObject({
              isFailure: false,
              result: { revision: "base", content: "old" },
            });

            return Stream.fromIterable([
              { type: "tool-call", id: "head", name: "read_file", params: head },
              { type: "finish", reason: "tool-calls", usage: recoveryUsage },
            ]);
          }
          expect(results.at(-1)).toMatchObject({
            isFailure: false,
            result: { revision: "head", content: "new" },
          });

          return response({}, recoveryUsage);
        });

        const repository = ReviewRepository.of({
          ...emptyRepository,
          readFile: (input) =>
            Effect.gen(function* () {
              yield* Ref.update(reads, (current) => [...current, input]);
              if (input.path !== "src/index.ts" || input.startLine !== 1) {
                return yield* ReviewContextError.make({ message: "Source range unavailable" });
              }

              return ReviewSource.make({
                path: input.path,
                revision: input.revision,
                startLine: input.startLine,
                totalLines: 1,
                content: input.revision === "base" ? "old" : "new",
              });
            }),
        });

        const outcome = yield* makeReviewer({
          model,
          compaction: "prune",
          contextTokenLimit: 128_000,
        })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, repository));

        expect(yield* Ref.get(reads)).toEqual([...failedInputs, base, head]);
        expect(outcome).toMatchObject({
          turns: 6,
          report: { findings: [] },
          usage: {
            inputTokens: 240_000,
            uncachedInputTokens: 240_000,
            cachedInputTokens: 0,
            cacheWriteInputTokens: 0,
            outputTokens: 24,
          },
        });
      }),
  );

  it.effect(
    "reserves a completion turn and preserves findings and usage when research runs out",
    () =>
      Effect.gen(function* () {
        const reads = yield* Ref.make(0);
        const calls = yield* Ref.make(0);

        const model = scriptedModel((prompt, _tools, toolChoice) =>
          Stream.unwrap(
            Ref.updateAndGet(calls, (count) => count + 1).pipe(
              Effect.map((call) => {
                expect(JSON.stringify(prompt.content)).toContain("<run-status>");
                if (call <= 2) {
                  expect(toolChoice).toBe("required");

                  return Stream.fromIterable([
                    {
                      type: "tool-call",
                      id: `read-${String(call)}`,
                      name: "read_file",
                      params: {
                        path: "src/index.ts",
                        revision: "head",
                        startLine: 1,
                        lineCount: 1,
                      },
                    },
                    ...(call === 1
                      ? [
                          {
                            type: "tool-call" as const,
                            id: "recorded",
                            name: "record_finding",
                            params: submittedFinding(blocker, 1),
                          },
                        ]
                      : []),
                    {
                      type: "finish",
                      reason: "tool-calls",
                      usage: {
                        inputTokens: {
                          total: 90_000,
                          uncached: 90_000,
                          cacheRead: 0,
                          cacheWrite: 0,
                        },
                        outputTokens: { total: 20_000 },
                      },
                    },
                  ]);
                }
                expect(call).toBe(3);
                expect(toolChoice).toEqual({ tool: "submit_review" });

                return response(
                  {},
                  {
                    inputTokens: {
                      total: 90_000,
                      uncached: 10_000,
                      cacheRead: 80_000,
                      cacheWrite: 0,
                    },
                    outputTokens: { total: 1_000 },
                  },
                );
              }),
            ),
          ),
        );

        const outcome = yield* makeReviewer({
          model,
          compaction: "prune",
          contextTokenLimit: 128_000,
          estimateCostMicrousd: () => Effect.succeed(123),
        })
          .review(request)
          .pipe(
            Effect.provideService(ReviewRepository, {
              ...emptyRepository,
              readFile: () =>
                Ref.update(reads, (count) => count + 1).pipe(
                  Effect.as(
                    ReviewSource.make({
                      path: "src/index.ts",
                      revision: "head",
                      startLine: 1,
                      totalLines: 1,
                      content: "new",
                    }),
                  ),
                ),
            }),
          );

        expect(yield* Ref.get(reads)).toBe(2);
        expect(yield* Ref.get(calls)).toBe(3);
        expect(outcome).toMatchObject({
          turns: 3,
          exhausted: "tokens",
          report: { findings: [blocker] },
          usage: {
            inputTokens: 270_000,
            uncachedInputTokens: 190_000,
            cachedInputTokens: 80_000,
            outputTokens: 41_000,
            estimatedCostMicrousd: 369,
          },
        });
        expect(outcome.report.summary).toContain("remaining change has not been verified");
      }),
  );

  it.effect("PRR-002 rejects a finding that does not name a causative changed path", () =>
    Effect.gen(function* () {
      let calls = 0;

      const model = scriptedModel((prompt) => {
        calls += 1;
        if (calls === 1)
          return toolResponse([
            {
              name: "record_finding",
              params: submittedFinding(
                ReviewFinding.make({ ...blocker, path: "src/unchanged.ts" }),
                0,
              ),
            },
          ]);

        const failure = prompt.content
          .flatMap((message) => (message.role === "tool" ? message.content : []))
          .find((part) => part.type === "tool-result" && part.name === "record_finding");

        expect(failure).toMatchObject({
          isFailure: true,
          result: { _tag: "ReviewVerificationError" },
        });

        return response({});
      });

      const result = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(result.report.findings).toEqual([]);
      expect(result.incomplete).toBeUndefined();
    }),
  );

  it.effect("PRR-002 demotes invalid anchors and removes only exact duplicates", () =>
    Effect.gen(function* () {
      const topLevel = ReviewFinding.make(Struct.omit(otherBlocker, ["line"]));
      const invalid = ReviewFinding.make({ ...otherBlocker, line: 999 });
      let calls = 0;

      const model = scriptedModel(() => {
        calls += 1;

        return calls === 1
          ? toolResponse(
              [
                submittedFinding(blocker, 0),
                submittedFinding(invalid, 1),
                submittedFinding(topLevel, 1),
                submittedFinding(invalid, 1),
                submittedFinding(blocker, 0),
              ].map((params) => ({ name: "record_finding", params })),
            )
          : response({});
      });

      const outcome = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.report.findings).toEqual([blocker, topLevel]);
    }),
  );

  it.effect.each([24, 25])("PRR-002 enforces the recorded finding bound: %s", (count) =>
    Effect.gen(function* () {
      const findings = Array.from({ length: count }, (_, index) =>
        ReviewFinding.make({
          ...Struct.omit(blocker, ["line"]),
          title: `Independent cause ${String(index)}`,
        }),
      );

      let calls = 0;

      const result = yield* makeReviewer({
        model: scriptedModel(() => {
          calls += 1;

          return calls === 1
            ? toolResponse(
                findings.map((finding) => ({
                  name: "record_finding",
                  params: submittedFinding(finding, 1),
                })),
              )
            : response({});
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(result.report.findings).toEqual(findings.slice(0, 24));
      expect(result.incomplete).toBe(count === 25 ? true : undefined);
    }),
  );

  it.effect.each([
    { summary: "safe" },
    { incomplete: true },
    { blockedOn: "" },
    { blockedOn: "x".repeat(2_001) },
  ])("PRR-002 rejects malformed native completion without retrying: %j", (submission) =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);

      const model = scriptedModel(() =>
        Stream.unwrap(
          Ref.update(calls, (count) => count + 1).pipe(Effect.as(response(submission))),
        ),
      );

      const result = yield* makeReviewer({ model })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);

      expect(Result.isFailure(result) && result.failure._tag).toBe("AiError");
      expect(yield* Ref.get(calls)).toBe(1);
    }),
  );

  it.effect.each(["resolution", "rewritten-findings"] as const)(
    "preserves recorded findings when completion submits invalid %s",
    (failure) =>
      Effect.gen(function* () {
        let calls = 0;

        const model = scriptedModel(() => {
          calls += 1;

          return calls === 1
            ? Stream.fromIterable([
                {
                  type: "tool-call",
                  id: "saved",
                  name: "record_finding",
                  params: submittedFinding(blocker, 1),
                },
                { type: "finish", reason: "tool-calls", usage },
              ])
            : response(
                failure === "resolution"
                  ? { resolutions: [{ ...resolution, id: "forged" }] }
                  : {
                      findings: [
                        submittedFinding(
                          ReviewFinding.make({
                            ...blocker,
                            title: "Preserve the missing acknowledgment",
                          }),
                          1,
                        ),
                      ],
                    },
              );
        });

        const outcome = yield* makeReviewer({ model })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(calls).toBe(2);
        expect(outcome.incomplete).toBe(true);
        expect(outcome.exhausted).toBeUndefined();
        expect(outcome.report.findings).toEqual([blocker]);
        expect(outcome.report.summary).toContain("remaining change has not been verified");
      }),
  );

  it.effect("PRR-002 retains headers, mode metadata, and complete deletion hunks", () =>
    Effect.gen(function* () {
      const deletionPatch = `diff --git a/src/deleted.ts b/src/deleted.ts
old mode 100644
new mode 100755
--- a/src/deleted.ts
+++ b/src/deleted.ts
@@ -5,2 +5,1 @@
 keep
-removed`;

      const modeOnlyPatch = `diff --git a/tool.sh b/tool.sh
old mode 100644
new mode 100755`;

      const formattedRequest = ReviewRequest.make({
        ...request,
        changes: [
          ReviewChange.make({ path: "src/deleted.ts", patch: deletionPatch }),
          ReviewChange.make({ path: "tool.sh", patch: modeOnlyPatch }),
        ],
      });

      const model = scriptedModel((prompt) => {
        const text = reviewInput(prompt);

        expect(text).toContain("old mode 100644");
        expect(text).toContain("new mode 100755");
        expect(text).toContain(deletionPatch);
        expect(text).toContain(modeOnlyPatch);

        return response({});
      });

      yield* makeReviewer({ model })
        .review(formattedRequest)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));
    }),
  );

  it.effect("PRR-002 keeps incremental all-clear wording conservative", () =>
    Effect.gen(function* () {
      const outcome = yield* makeReviewer({
        model: scriptedModel(() => response({})),
      })
        .review(ReviewRequest.make({ ...request, scope: "incremental" }))
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.report.summary).toContain(
        "Earlier findings remain open unless explicitly verified",
      );
      expect(outcome.report.summary).not.toContain("safe to merge");
    }),
  );

  it.effect.each(["prune", "rollover"] as const)(
    "finishes all 50 files after refusing an early blocked submission with %s compaction",
    (compaction) =>
      Effect.gen(function* () {
        const input = ReviewRequest.make({
          ...request,
          changes: [...request.changes, ...largeRequest(49, 6_000).changes],
          followUps: [followUp],
        });

        const exactDiff = input.changes
          .map(({ path, patch }) => `Changed file: ${JSON.stringify(path)}\n${patch}\n\n`)
          .join("");

        const delivered = new Map<number, string>();
        let calls = 0;
        let refused = false;

        const outcome = yield* makeReviewer({
          compaction,
          contextTokenLimit: 32_000,
          model: scriptedModel((prompt) => {
            calls += 1;
            expect(calls).toBeLessThan(30);
            if (calls === 1)
              return toolResponse([
                { name: "record_finding", params: submittedFinding(blocker, 1) },
              ]);
            if (calls === 2)
              return response({
                blockedOn: "An external caller implementation is unavailable.",
                resolutions: [resolution],
              });
            if (calls === 3) {
              const completion = completionResult(prompt);

              expect(completion).toMatchObject({
                isFailure: true,
                result: {
                  _tag: "ReviewVerificationError",
                  message: expect.stringContaining('read_diff({"offset":0})'),
                },
              });
              refused = true;
            }

            const results = prompt.content.flatMap((message) =>
              message.role === "tool" ? message.content : [],
            );

            for (const part of results) {
              if (part.type !== "tool-result" || part.name !== "read_diff" || part.isFailure)
                continue;

              const page = Schema.decodeUnknownOption(
                Schema.Struct({ offset: Schema.Natural, content: Schema.String }),
              )(part.result);

              if (page._tag === "Some") delivered.set(page.value.offset, page.value.content);
            }

            const nextOffset = [...delivered]
              .sort((a, b) => a[0] - b[0])
              .reduce(
                (offset, [start, content]) => (start === offset ? offset + content.length : offset),
                0,
              );

            if (nextOffset === exactDiff.length) return response({ resolutions: [resolution] });

            return toolResponse(
              [{ name: "read_diff", params: { offset: nextOffset } }],
              calls === 5
                ? {
                    inputTokens: { total: 30_000, uncached: 30_000, cacheRead: 0, cacheWrite: 0 },
                    outputTokens: { total: 4 },
                  }
                : usage,
            );
          }),
        })
          .review(input)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(refused).toBe(true);
        expect(
          [...delivered]
            .sort((a, b) => a[0] - b[0])
            .map(([, content]) => content)
            .join(""),
        ).toBe(exactDiff);
        expect(outcome.report.findings).toEqual([blocker]);
        expect(outcome.pendingPaths).toBeUndefined();
        expect(outcome.blockedOn).toBeUndefined();
        expect(outcome.incomplete).toBeUndefined();
        expect(outcome.exhausted).toBeUndefined();
        expect(outcome.resolutions).toEqual([resolution]);
        expect(
          outcome.compactions?.some(
            ({ kind }) => kind === (compaction === "rollover" ? "rollover" : "clear-tool-results"),
          ),
        ).toBe(true);
      }),
  );

  it.effect(
    "bounds repeated completion refusals and preserves an unpriced attempt without findings",
    () =>
      Effect.gen(function* () {
        let calls = 0;
        const input = largeRequest(50, 6_000);

        const outcome = yield* makeReviewer({
          model: scriptedModel((prompt) => {
            calls += 1;
            if (calls > 1) expect(completionResult(prompt)).toMatchObject({ isFailure: true });

            return response({
              blockedOn: "An external definition is unavailable.",
              resolutions: [resolution],
            });
          }),
        })
          .review(ReviewRequest.make({ ...input, followUps: [followUp] }))
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(calls).toBe(129);
        expect(outcome.turns).toBe(129);
        expect(outcome.exhausted).toBe("turns");
        expect(outcome.incomplete).toBe(true);
        expect(outcome.pendingPaths).toEqual(input.changes.map(({ path }) => path));
        expect(outcome.report.findings).toEqual([]);
        expect(outcome.resolutions).toBeUndefined();
        expect(outcome.blockedOn).toBeUndefined();
        expect(outcome.usage.inputTokens).toBe(129 * 10);
      }),
  );

  it.effect.each(["prune", "rollover"] as const)(
    "navigates a 120-file diff through %s without dropping unseen evidence",
    (compaction) =>
      Effect.gen(function* () {
        const changes = Array.from({ length: 120 }, (_, index) =>
          ReviewChange.make({
            path: `src/part-${index}.ts`,
            patch: `@@ -1,151 +1,151 @@\n-old${index}\n+new${index}\n${" unchanged code with context\n".repeat(150)}`,
          }),
        );

        const input = ReviewRequest.make({ ...request, changes });

        const expected = changes
          .map(({ path, patch }) => `Changed file: ${JSON.stringify(path)}\n${patch}\n\n`)
          .join("");

        expect(expected.length).toBeGreaterThan(256_000);
        const calls = yield* Ref.make(0);
        const retained = new Map<number, string>();
        let sawCompaction = false;
        let recorded = false;

        const findings = [0, 59, 119].map((index) =>
          submittedFinding(
            ReviewFinding.make({ ...blocker, path: `src/part-${index}.ts`, line: 1 }),
            1,
          ),
        );

        const model = scriptedModel((prompt) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const call = yield* Ref.updateAndGet(calls, (n) => n + 1);
              const initial = reviewInput(prompt);

              expect(initial).toContain('"path":"src/part-119.ts"');
              expect(initial).not.toContain(changes[0]?.patch);

              const pages = prompt.content
                .flatMap((m) => (m.role === "tool" ? m.content : []))
                .flatMap((part) => {
                  if (part.type !== "tool-result" || part.name !== "read_diff" || part.isFailure)
                    return [];

                  const page = Schema.decodeUnknownOption(
                    Schema.Struct({
                      offset: Schema.Natural,
                      content: Schema.String,
                      nextOffset: Schema.NullOr(Schema.Natural),
                    }),
                  )(part.result);

                  return page._tag === "Some" ? [page.value] : [];
                });

              sawCompaction ||= retained.size > pages.length;

              for (const page of pages) retained.set(page.offset, page.content);

              const nextOffset = [...retained]
                .sort((a, b) => a[0] - b[0])
                .reduce(
                  (offset, [start, content]) =>
                    start === offset ? offset + content.length : offset,
                  0,
                );

              if (nextOffset === expected.length) {
                if (recorded) return response({});
                recorded = true;

                return toolResponse(findings.map((params) => ({ name: "record_finding", params })));
              }

              return toolResponse(
                [{ name: "read_diff", params: { offset: nextOffset } }],
                call === 8
                  ? {
                      inputTokens: {
                        total: 124_000,
                        uncached: 124_000,
                        cacheRead: 0,
                        cacheWrite: 0,
                      },
                      outputTokens: { total: 4 },
                    }
                  : usage,
              );
            }),
          ),
        );

        const review = makeReviewer({
          model,
          compaction,
          contextTokenLimit: 128_000,
          costControl: costControl(calls),
        }).review(input);

        expectTypeOf<Effect.Services<typeof review>>().toEqualTypeOf<ReviewRepository>();
        expectTypeOf<Effect.Error<typeof review>>().not.toBeAny();

        const outcome = yield* review.pipe(
          Effect.provideService(ReviewRepository, emptyRepository),
        );

        expect(
          [...retained]
            .sort((a, b) => a[0] - b[0])
            .map(([, text]) => text)
            .join(""),
        ).toBe(expected);
        expect(outcome.report.findings.map(({ path }) => path)).toEqual([
          "src/part-0.ts",
          "src/part-59.ts",
          "src/part-119.ts",
        ]);
        expect(sawCompaction).toBe(true);
        expect(
          outcome.compactions?.some(
            ({ kind }) => kind === (compaction === "rollover" ? "rollover" : "clear-tool-results"),
          ),
        ).toBe(true);
        expect(outcome.pendingPaths).toBeUndefined();
        expect(outcome.incomplete).toBeUndefined();
        expect(outcome.turns).toBeLessThan(25);
      }),
  );

  it.effect.each([false, true])(
    "default 48k rollover preserves notes and findings while requiring unseen pages again: reread=%s",
    (reread) =>
      Effect.gen(function* () {
        const input = ReviewRequest.make({
          ...request,
          changes: [...request.changes, ...largeRequest(1, 50_000).changes],
          followUps: [followUp],
        });

        const exactDiff = input.changes
          .map(({ path, patch }) => `Changed file: ${JSON.stringify(path)}\n${patch}\n\n`)
          .join("");

        let calls = 0;
        let initialInstructions = "";

        const unresolvedNotes =
          "Unresolved: verify the acknowledgment owner check after reading the remaining patch.";

        const resolvedNotes =
          "Verified the owner check and remaining patch; recorded the independent acknowledgment defect.";

        const model = scriptedModel((prompt) => {
          calls += 1;

          const instructions = JSON.stringify(
            prompt.content.filter((message) => message.role === "system"),
          );

          if (calls === 1) initialInstructions = instructions;
          expect(instructions).toBe(initialInstructions);
          expect(instructions).toContain("Keep the exact source contracts.");
          expect(reviewInput(prompt)).toContain('"path":"src/part-0.ts"');

          if (calls === 1)
            return toolResponse(
              [
                { name: "record_finding", params: submittedFinding(blocker, 1) },
                { name: "read_diff", params: { offset: 0 } },
                {
                  name: "review_status",
                  params: { notes: { text: unresolvedNotes, expectedRevision: 0 } },
                },
              ],
              {
                inputTokens: { total: 46_000, uncached: 46_000, cacheRead: 0, cacheWrite: 0 },
                outputTokens: { total: 4 },
              },
            );

          if (calls === 2) {
            expect(JSON.stringify(prompt.content)).toContain("A fresh context window has started.");
            expect(
              prompt.content.flatMap((message) => (message.role === "tool" ? message.content : [])),
            ).toEqual([]);

            return toolResponse([{ name: "review_status", params: {} }]);
          }

          const results = prompt.content.flatMap((message) =>
            message.role === "tool" ? message.content : [],
          );

          if (!reread && calls > 3) {
            expect(completionResult(prompt)).toMatchObject({
              isFailure: true,
              result: { _tag: "ReviewVerificationError" },
            });

            return Stream.empty;
          }

          if (calls === 3) {
            const status = results.find(
              (part) => part.type === "tool-result" && part.name === "review_status",
            );

            expect(status).toMatchObject({
              result: {
                findings: [blocker],
                pendingCount: 2,
                pending: [{ path: "src/index.ts", offset: 0 }, { path: "src/part-0.ts" }],
                notes: { text: unresolvedNotes, revision: 1 },
              },
            });

            return reread
              ? toolResponse([{ name: "read_diff", params: { offset: 0 } }])
              : response({ resolutions: [resolution] });
          }

          if (calls === 6) {
            const status = results.findLast(
              (part) => part.type === "tool-result" && part.name === "review_status",
            );

            expect(status).toMatchObject({
              result: {
                notes: { text: resolvedNotes, revision: 2 },
                pendingCount: 0,
                findings: [blocker],
              },
            });

            return response({ resolutions: [resolution] });
          }

          const page = results.findLast(
            (part) => part.type === "tool-result" && part.name === "read_diff",
          );

          expect(page).toMatchObject({
            result: {
              content: exactDiff.slice(calls === 4 ? 0 : 32_000, calls === 4 ? 32_000 : undefined),
            },
          });

          return calls === 4
            ? toolResponse([{ name: "read_diff", params: { offset: 32_000 } }])
            : toolResponse([
                {
                  name: "review_status",
                  params: { notes: { text: resolvedNotes, expectedRevision: 1 } },
                },
              ]);
        });

        const outcome = yield* makeReviewer({
          model,
          guidance: "Keep the exact source contracts.",
          estimateCostMicrousd: () => Effect.succeed(123),
        })
          .review(input)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(outcome.report.findings).toEqual([blocker]);
        expect(outcome.compactions).toEqual([
          {
            kind: "rollover",
            turn: 2,
            tokensBeforeEstimate: expect.any(Number),
            tokensAfterEstimate: expect.any(Number),
          },
        ]);
        expect(outcome.compactions?.[0]?.tokensAfterEstimate).toBeLessThan(
          outcome.compactions?.[0]?.tokensBeforeEstimate ?? 0,
        );
        expect(outcome.pendingPaths).toEqual(
          reread ? undefined : input.changes.map(({ path }) => path),
        );
        expect(outcome.incomplete).toBe(reread ? undefined : true);
        expect(outcome.resolutions).toEqual(reread ? [resolution] : undefined);
        expect(outcome.turns).toBe(reread ? 6 : 3);
        expect(outcome.notesUpdates).toBe(reread ? 2 : 1);
        expect(outcome.usage.inputTokens).toBe(46_000 + (outcome.turns - 1) * 10);
        expect(outcome.usage.outputTokens).toBe(outcome.turns * 4);
        expect(outcome.usage.estimatedCostMicrousd).toBe(outcome.turns * 123);
      }),
  );

  it.effect("conflicting concurrent note updates preserve the accepted revision", () =>
    Effect.gen(function* () {
      let calls = 0;
      let acceptedText = "";

      const proposals = [
        "Check the owner boundary.".padEnd(4_000, "."),
        "Check caller acknowledgment cleanup.",
      ];

      const outcome = yield* makeReviewer({
        model: scriptedModel((prompt) => {
          calls += 1;
          if (calls === 1) return toolResponse([{ name: "review_status", params: {} }]);

          const statuses = prompt.content
            .flatMap((message) => (message.role === "tool" ? message.content : []))
            .flatMap((part) =>
              part.type === "tool-result" && part.name === "review_status" ? [part] : [],
            );

          if (calls === 2) {
            expect(statuses.at(-1)).toMatchObject({ result: { notes: { text: "", revision: 0 } } });

            return toolResponse(
              proposals.map((text) => ({
                name: "review_status",
                params: { notes: { text, expectedRevision: 0 } },
              })),
            );
          }
          if (calls === 3) {
            const updates = statuses.slice(-2);
            const failures = updates.filter((part) => part.isFailure);
            const successes = updates.filter((part) => !part.isFailure);

            expect(failures).toHaveLength(1);
            expect(successes).toHaveLength(1);
            expect(failures[0]).toMatchObject({ result: { _tag: "ReviewVerificationError" } });

            const accepted = Schema.decodeUnknownSync(
              Schema.Struct({
                notes: Schema.Struct({ text: Schema.String, revision: Schema.Literal(1) }),
              }),
            )(successes[0]?.result);

            acceptedText = accepted.notes.text;
            expect(proposals).toContain(acceptedText);

            return toolResponse([{ name: "review_status", params: {} }]);
          }
          if (calls === 4) {
            expect(statuses.at(-1)).toMatchObject({
              result: { notes: { text: acceptedText, revision: 1 } },
            });

            return toolResponse([
              { name: "review_status", params: { notes: { text: "", expectedRevision: 1 } } },
            ]);
          }
          expect(statuses.at(-1)).toMatchObject({ result: { notes: { text: "", revision: 2 } } });

          return response({});
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(calls).toBe(5);
      expect(outcome.notesUpdates).toBe(2);
      expect(outcome.incomplete).toBeUndefined();
      expect(outcome.report.findings).toEqual([]);
    }),
  );

  it.effect("rejects oversized notes at the native tool schema boundary", () =>
    Effect.gen(function* () {
      let calls = 0;

      const result = yield* makeReviewer({
        model: scriptedModel(() => {
          calls += 1;

          return toolResponse([
            {
              name: "review_status",
              params: { notes: { text: "x".repeat(4_001), expectedRevision: 0 } },
            },
          ]);
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.result);

      expect(Result.isFailure(result) && result.failure._tag).toBe("AiError");
      expect(calls).toBe(1);
    }),
  );

  it.effect("cannot claim completion after duplicate, out-of-order, or failed diff reads", () =>
    Effect.gen(function* () {
      let call = 0;
      const input = largeRequest(3, 40_000);

      const outcome = yield* makeReviewer({
        model: scriptedModel((prompt) => {
          call += 1;

          if (call > 2) {
            expect(completionResult(prompt)).toMatchObject({ isFailure: true });

            return Stream.empty;
          }

          return call === 1
            ? toolResponse([
                { name: "read_diff", params: { offset: 64_000 } },
                { name: "read_diff", params: { offset: 0 } },
                { name: "read_diff", params: { offset: 0 } },
                { name: "read_diff", params: { offset: 9_000_000 } },
              ])
            : response({ resolutions: [resolution] });
        }),
      })
        .review(ReviewRequest.make({ ...input, followUps: [followUp] }))
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.incomplete).toBe(true);
      expect(outcome.pendingPaths).toEqual(input.changes.map(({ path }) => path));
      expect(outcome.resolutions).toBeUndefined();
    }),
  );

  it.effect("recovers saved findings and unread ranges after native context rollover", () =>
    Effect.gen(function* () {
      let call = 0;

      const input = ReviewRequest.make({
        ...request,
        changes: [ReviewChange.make({ path: "src/index.ts", patch }), ...largeRequest(2).changes],
      });

      const outcome = yield* makeReviewer({
        model: scriptedModel((prompt) => {
          call += 1;
          if (call > 4) {
            expect(completionResult(prompt)).toMatchObject({ isFailure: true });

            return Stream.empty;
          }
          if (call === 1)
            return toolResponse([
              { name: "read_diff", params: { offset: 0 } },
              { name: "record_finding", params: submittedFinding(blocker, 1) },
            ]);
          if (call === 2)
            return toolResponse([
              {
                name: "new_context",
                params: { handoff: "Continue remaining diff and recover findings." },
              },
            ]);
          if (call === 3) {
            expect(reviewInput(prompt)).toContain('"path":"src/part-1.ts"');

            return toolResponse([{ name: "review_status", params: {} }]);
          }

          const status = prompt.content
            .flatMap((m) => (m.role === "tool" ? m.content : []))
            .find((part) => part.type === "tool-result" && part.name === "review_status");

          expect(status).toMatchObject({ result: { pendingCount: 2, findings: [blocker] } });

          return response({});
        }),
      })
        .review(input)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.report.findings).toEqual([blocker]);
      expect(outcome.pendingPaths).toEqual(["src/part-0.ts", "src/part-1.ts"]);
      expect(outcome.incomplete).toBe(true);
    }),
  );

  it.effect("retains a late blocker over early nits and cannot hide finding overflow", () =>
    Effect.gen(function* () {
      let call = 0;

      const outcome = yield* makeReviewer({
        model: scriptedModel(() => {
          call += 1;
          if (call === 1)
            return toolResponse(
              Array.from({ length: 24 }, (_, index) => ({
                name: "record_finding",
                params: submittedFinding(
                  ReviewFinding.make({ ...nitFinding, title: `Minor ${index}` }),
                  3,
                ),
              })),
            );
          if (call === 2)
            return toolResponse([{ name: "record_finding", params: submittedFinding(blocker, 1) }]);

          return response({});
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.report.findings).toHaveLength(24);
      expect(outcome.report.findings[0]).toEqual(blocker);
      expect(outcome.incomplete).toBe(true);
    }),
  );

  it.effect("keeps one deadline through navigation and closes model streams", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const finalized = yield* Ref.make(0);
      const firstStarted = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const input = largeRequest(9);

      const model = scriptedModel(() =>
        Stream.unwrap(
          Effect.gen(function* () {
            const call = yield* Ref.updateAndGet(calls, (n) => n + 1);

            yield* Deferred.succeed(call === 1 ? firstStarted : secondStarted, undefined);

            return Stream.fromEffect(Effect.sleep("4 minutes")).pipe(
              Stream.flatMap(() => toolResponse([{ name: "read_diff", params: { offset: 0 } }])),
              Stream.ensuring(Ref.update(finalized, (n) => n + 1)),
            );
          }),
        ),
      );

      const fiber = yield* makeReviewer({ model, costControl: costControl(calls) })
        .review(input)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.forkChild);

      yield* Deferred.await(firstStarted);
      yield* TestClock.adjust("4 minutes");
      yield* Deferred.await(secondStarted);
      yield* TestClock.adjust("1 minute");
      const outcome = yield* Fiber.join(fiber);

      expect(yield* Ref.get(finalized)).toBe(2);
      expect(outcome.incomplete).toBe(true);
      expect(outcome.pendingPaths).toEqual(input.changes.map(({ path }) => path));
    }),
  );

  it.effect.each(["interrupt", "defect"] as const)("closes navigation streams on %s", (failure) =>
    Effect.gen(function* () {
      const finalized = yield* Ref.make(0);
      const calls = yield* Ref.make(0);
      const started = yield* Deferred.make<void>();

      const model = scriptedModel(() =>
        Stream.unwrap(
          Effect.gen(function* () {
            const call = yield* Ref.updateAndGet(calls, (n) => n + 1);

            const stream =
              call === 1
                ? toolResponse([{ name: "read_diff", params: { offset: 0 } }])
                : Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
                    Stream.flatMap(() =>
                      failure === "interrupt" ? Stream.never : Stream.die("defect"),
                    ),
                  );

            return stream.pipe(Stream.ensuring(Ref.update(finalized, (n) => n + 1)));
          }),
        ),
      );

      const fiber = yield* makeReviewer({ model, costControl: costControl(calls) })
        .review(largeRequest(9))
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.forkChild);

      yield* Deferred.await(started);
      if (failure === "interrupt") yield* Fiber.interrupt(fiber);
      expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
      expect(yield* Ref.get(finalized)).toBe(2);
    }),
  );

  it.effect("PRR-003 shares exact source range bounds", () =>
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

  it("PRR-004 accepts only RIGHT-side patch lines", () => {
    expect([0, 1, 2, 3, 4, 5].filter((line) => isCommentableLine(patch, line))).toEqual([
      1, 2, 3, 4,
    ]);
  });
});

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
  controlled: makeReviewer({
    model,
    costControl: {
      snapshot: Effect.succeed(
        ReviewCostSnapshot.make({
          stopped: false,
          modelCalls: 0,
          usage: ReviewUsage.make({
            inputTokens: 0,
            uncachedInputTokens: 0,
            cachedInputTokens: 0,
            cacheWriteInputTokens: 0,
            outputTokens: 0,
          }),
        }),
      ),
    },
  }).review(request),
});

type TypedReviews = ReturnType<typeof typedReviews>;

const pricingTypeProofs: readonly [
  Assert<Equal<EffectError<TypedReviews["controlled"]>, EffectError<TypedReviews["unpriced"]>>>,
  Assert<Equal<EffectRequirements<TypedReviews["controlled"]>, ReviewRepository>>,
  Assert<Equal<EffectError<TypedReviews["priced"]>, EffectError<TypedReviews["unpriced"]>>>,
  Assert<
    Equal<EffectRequirements<TypedReviews["priced"]>, EffectRequirements<TypedReviews["unpriced"]>>
  >,
  Assert<Equal<EffectRequirements<TypedReviews["priced"]>, ReviewRepository>>,
  Assert<Equal<Effect.Success<TypedReviews["priced"]>, ReviewOutcome>>,
  Assert<
    Equal<
      Extract<EffectError<TypedReviews["priced"]>, ReviewVerificationError>,
      ReviewVerificationError
    >
  >,
] = [true, true, true, true, true, true, true];

void pricingTypeProofs;
