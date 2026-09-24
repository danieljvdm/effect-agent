import {
  makeReviewer,
  ReviewChange,
  ReviewCostSnapshot,
  ReviewFinding,
  ReviewFollowUp,
  ReviewResolution,
  ReviewRequest,
  ReviewUsage,
} from "@effect-agent/pr-review/review";
import {
  ReviewContextError,
  ReviewFileList,
  ReviewRepository,
} from "@effect-agent/pr-review/review-repository";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Logger, Ref, Schema, Stream, Struct } from "effect";
import { TestClock } from "effect/testing";
import {
  type Tool,
  type AiError,
  LanguageModel,
  Model,
  type Prompt,
  type Response,
} from "effect/unstable/ai";

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

describe("review output boundary", () => {
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

  it.effect("closes attached child streams on interruption", () =>
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
                  Stream.flatMap(() => Stream.never),
                )
            ).pipe(Stream.ensuring(Ref.update(finalized, (count) => count + 1))),
          ),
        },
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository), Effect.forkChild);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      {
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
      }
      expect(yield* Ref.get(finalized)).toBe(2);
    }),
  );

  it.effect.each([false, true])(
    "default 48k rollover preserves notes and findings while requiring unseen pages again: reread=%s",
    (reread) =>
      Effect.gen(function* () {
        const logs: Array<unknown> = [];

        const input = ReviewRequest.make({
          ...request,
          changes: [...request.changes, ...largeRequest(1, 50_000).changes],
          followUps: [followUp],
        });

        const exactDiff = input.changes
          .map(({ path, patch }) => `Changed file: ${JSON.stringify(path)}\n${patch}\n\n`)
          .join("");

        let calls = 0;

        const unresolvedNotes =
          "Unresolved: verify the acknowledgment owner check after reading the remaining patch.";

        const resolvedNotes =
          "Verified the owner check and remaining patch; recorded the independent acknowledgment defect.";

        const model = scriptedModel((prompt) => {
          calls += 1;

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
          .pipe(
            Effect.provideService(ReviewRepository, emptyRepository),
            Effect.provide(
              Logger.layer([Logger.make<unknown, void>(({ message }) => logs.push(message))]),
            ),
          );

        expect(outcome.report.findings).toEqual([blocker]);
        expect(JSON.stringify(logs)).not.toContain(unresolvedNotes);
        expect(outcome.pendingPaths).toEqual(
          reread ? undefined : input.changes.map(({ path }) => path),
        );
        expect(outcome.incomplete).toBe(reread ? undefined : true);
        expect(outcome.resolutions).toEqual(reread ? [resolution] : undefined);
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
      expect(outcome.incomplete).toBeUndefined();
      expect(outcome.report.findings).toEqual([]);
    }),
  );

  it.effect("cannot claim completion after duplicate, out-of-order, or failed diff reads", () =>
    Effect.gen(function* () {
      let call = 0;
      const input = largeRequest(3, 40_000);

      const outcome = yield* makeReviewer({
        model: scriptedModel((prompt) => {
          call += 1;

          if (call > 3) {
            expect(completionResult(prompt)).toMatchObject({ isFailure: true });

            return Stream.empty;
          }

          if (call === 2) return toolResponse([{ name: "read_diff", params: { offset: 0 } }]);

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
});
