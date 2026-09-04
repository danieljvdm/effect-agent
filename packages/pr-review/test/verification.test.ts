import {
  makeReviewer,
  ReviewCandidate,
  ReviewChange,
  ReviewCostSnapshot,
  type ReviewDiagnostics,
  ReviewDiagnosticsSink,
  ReviewFinding,
  ReviewFollowUp,
  ReviewRequest,
  type ReviewStage,
  ReviewUsage,
  reviewRequestDigest,
} from "@effect-agent/pr-review/Review";
import {
  ReviewContextError,
  ReviewFileList,
  ReviewLineMatches,
  ReviewRepository,
  ReviewSource,
} from "@effect-agent/pr-review/ReviewRepository";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, type Prompt, type Response, type Tool } from "effect/unstable/ai";

const patch = "@@ -1,2 +1,2 @@\n context\n-old\n+new";

const request = ReviewRequest.make({
  title: "Change",
  description: "",
  baseRevision: "base-immutable",
  headRevision: "head-immutable",
  changes: [ReviewChange.make({ path: "src/change.ts", patch })],
  unreviewedPaths: [],
});

const finding = ReviewFinding.make({
  path: "src/change.ts",
  line: 2,
  severity: "blocking",
  category: "resources",
  title: "The acquired handle leaks",
  body: "When cleanup precedes acquisition, the acquired handle remains open.",
});

const submitted = (entry: ReviewFinding) => ({
  path: entry.path,
  line: entry.line,
  category: entry.category,
  title: entry.title,
  body: entry.body,
  priority: entry.severity === "blocking" ? 1 : entry.severity === "important" ? 2 : 3,
});

const usage = {
  inputTokens: { total: 10, uncached: 7, cacheRead: 2, cacheWrite: 1 },
  outputTokens: { total: 4 },
};

const toolResponse = (name: string, params: object): Stream.Stream<Response.StreamPartEncoded> =>
  Stream.fromIterable([
    { type: "tool-call", id: name, name, params },
    { type: "finish", reason: "tool-calls", usage },
  ]);

const discoveryResponse = (
  findings: ReadonlyArray<ReviewFinding> = [finding],
  incomplete = false,
) => toolResponse("submit_review", { findings: findings.map(submitted), incomplete });

const inputText = (prompt: Prompt.Prompt) =>
  prompt.content
    .flatMap((message) =>
      message.role === "user" && typeof message.content !== "string"
        ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
        : [],
    )
    .at(0) ?? "";

const originalCandidates = (prompt: Prompt.Prompt) =>
  Schema.decodeUnknownSync(Schema.Array(ReviewCandidate))(
    JSON.parse(inputText(prompt).split("\n\nOriginal candidates:\n")[1] ?? "[]"),
  );

const diffEvidence = {
  kind: "diff",
  revision: request.headRevision,
  path: finding.path,
  startLine: 2,
  endLine: 2,
} as const;

const decision = (
  candidate: ReviewCandidate,
  disposition = "supported",
  evidence: ReadonlyArray<object> = [diffEvidence],
) => ({
  id: candidate.id,
  disposition,
  reason: "The exact supplied operation establishes the claim.",
  evidence,
});

const emptyRepository = ReviewRepository.of({
  readFile: () => Effect.fail(ReviewContextError.make({ message: "Unavailable" })),
  findFiles: () => Effect.succeed(ReviewFileList.make({ paths: [], truncated: false })),
  findInFile: () => Effect.fail(ReviewContextError.make({ message: "Unavailable" })),
});

const scriptedModel = (
  respond: (
    prompt: Prompt.Prompt,
    tools: ReadonlyArray<Tool.Any>,
  ) => Stream.Stream<Response.StreamPartEncoded>,
  close: Effect.Effect<void> = Effect.void,
) =>
  Model.make(
    "scripted",
    "verification",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        yield* Effect.acquireRelease(Effect.void, () => close);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: ({ prompt, tools }) => respond(prompt, tools),
        });
      }),
    ),
  );

const isVerifier = (tools: ReadonlyArray<Tool.Any>) =>
  tools.some(({ name }) => name === "submit_verification");

const verificationFeedback = (prompt: Prompt.Prompt) =>
  prompt.content.flatMap((message) =>
    message.role === "tool"
      ? message.content.flatMap((part) =>
          part.type === "tool-result" && part.name === "submit_verification" && part.isFailure
            ? [
                Schema.decodeUnknownSync(
                  Schema.Struct({
                    _tag: Schema.Literal("ReviewVerificationError"),
                    message: Schema.String,
                  }),
                )(part.result).message,
              ]
            : [],
        )
      : [],
  );

const testLayer = Layer.merge(NodeCrypto.layer, ReviewDiagnosticsSink.layerNoop);

layer(testLayer)("mandatory candidate verification", (it) => {
  it.effect.each([
    { kind: "source", startLine: 154 },
    { kind: "diff", startLine: 157 },
  ] as const)(
    "retains canonical revisions for a $kind selector at $startLine",
    ({ kind, startLine }) =>
      Effect.gen(function* () {
        const path = ".github/workflows/ci.yml";
        const baseRevision = "13acbbcf7b099334bb44082c0849de5833337793";
        const headRevision = "7734374b3dc0df3cedd290d562ee410047dae42c";

        const input = ReviewRequest.make({
          ...request,
          baseRevision,
          headRevision,
          changes: [
            ReviewChange.make({
              path,
              patch:
                "@@ -151,14 +151,14 @@\n" + Array.from({ length: 14 }, () => " context").join("\n"),
            }),
          ],
        });

        const entry = ReviewFinding.make({ ...finding, path, line: 157 });
        let verifierCalls = 0;

        const outcome = yield* makeReviewer({
          strategy: "verified",
          model: scriptedModel((prompt, tools) => {
            if (!isVerifier(tools)) return discoveryResponse([entry]);
            verifierCalls += 1;
            if (kind === "source" && verifierCalls === 1)
              return toolResponse("read_file", {
                path,
                revision: "head",
                startLine: 130,
                lineCount: 35,
              });
            expect(verifierCalls).toBe(kind === "source" ? 2 : 1);

            return toolResponse("submit_verification", {
              decisions: originalCandidates(prompt).map((candidate) =>
                decision(candidate, "supported", [
                  {
                    kind,
                    path,
                    revision: "head",
                    startLine,
                    endLine: 164,
                  },
                ]),
              ),
            });
          }),
        })
          .review(input)
          .pipe(
            Effect.provideService(ReviewRepository, {
              ...emptyRepository,
              readFile: (read) =>
                ReviewSource.fromText(read, Array.from({ length: 164 }, () => "source").join("\n")),
            }),
          );

        expect(outcome.incomplete).toBeUndefined();
        expect(outcome.report.findings).toEqual([entry]);
        expect(outcome.diagnostics?.candidates[0]?.evidence).toEqual([
          {
            kind,
            path,
            revision: headRevision,
            startLine,
            endLine: 164,
          },
        ]);
        expect(outcome.diagnostics?.requestDigest).toBe(yield* reviewRequestDigest(input));
        expect(outcome.turns).toBe(kind === "source" ? 3 : 2);
      }),
  );

  it.effect.each([
    {
      name: "swapped literal revisions",
      base: "head",
      head: "base",
      refs: [
        { revision: "head", line: 10 },
        { revision: "base", line: 20 },
      ],
      expected: ["head", "base"],
    },
    {
      name: "equal literal revisions",
      base: "head",
      head: "head",
      refs: [
        { revision: "head", line: 20 },
        { revision: "base", line: 20 },
      ],
      expected: ["head", "head"],
    },
    {
      name: "base selector on base-only lines",
      base: "base-id",
      head: "head-id",
      refs: [{ revision: "base", line: 10 }],
      expected: ["base-id"],
    },
    {
      name: "head selector on base-only lines",
      base: "base-id",
      head: "head-id",
      refs: [{ revision: "head", line: 10 }],
      expected: [],
    },
    {
      name: "base selector on head-only lines",
      base: "base-id",
      head: "head-id",
      refs: [{ revision: "base", line: 20 }],
      expected: [],
    },
    {
      name: "unrecognized selector spelling",
      base: "base-id",
      head: "head-id",
      refs: [{ revision: "HEAD", line: 20 }],
      expected: [],
    },
  ] as const)(
    "preserves revision identity and diff sides: $name",
    ({ base, head, refs, expected }) =>
      Effect.gen(function* () {
        const input = ReviewRequest.make({
          ...request,
          baseRevision: base,
          headRevision: head,
          changes: [
            ReviewChange.make({
              path: finding.path,
              patch: "@@ -10,2 +20,2 @@\n-old\n+new\n context",
            }),
          ],
        });

        const entry = ReviewFinding.make({ ...finding, line: 20 });
        let verifierCalls = 0;

        const outcome = yield* makeReviewer({
          strategy: "verified",
          model: scriptedModel((prompt, tools) => {
            if (!isVerifier(tools)) return discoveryResponse([entry]);
            verifierCalls += 1;
            if (verifierCalls > 1) {
              expect(expected).toEqual([]);
              expect(verifierCalls).toBe(2);
              expect(verificationFeedback(prompt).at(-1)).toContain("evidence 1");
            }

            return toolResponse("submit_verification", {
              decisions: originalCandidates(prompt).map((candidate) =>
                decision(
                  candidate,
                  verifierCalls === 1 ? "supported" : "unresolved",
                  verifierCalls === 1
                    ? refs.map(({ revision, line }) => ({
                        ...diffEvidence,
                        revision,
                        startLine: line,
                        endLine: line,
                      }))
                    : [],
                ),
              ),
            });
          }),
        })
          .review(input)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(outcome.incomplete).toBe(expected.length > 0 ? undefined : true);
        expect(
          outcome.diagnostics?.candidates[0]?.evidence.map(({ revision }) => revision),
        ).toEqual(expected);
        expect(verifierCalls).toBe(expected.length > 0 ? 1 : 2);
      }),
  );

  it.effect.each([
    "contiguous",
    "overlapping",
    "contained",
    "gap",
    "wrong-revision",
    "unavailable",
    "truncated",
    "eof",
    "search-only",
    "discovery-only",
  ] as const)("validates cited source coverage across delivered reads: %s", (mode) =>
    Effect.gen(function* () {
      let calls = 0;
      let discoveryCalls = 0;
      const accepted = mode === "contiguous" || mode === "overlapping" || mode === "contained";

      const source = Array.from({ length: mode === "eof" ? 5 : 8 }, (_, index) =>
        mode === "truncated" && index >= 3 ? "界".repeat(3_600) : `Line ${String(index + 1)}`,
      ).join("\n");

      const outcome = yield* makeReviewer({
        strategy: "verified",
        model: scriptedModel((prompt, tools) => {
          if (!isVerifier(tools)) {
            discoveryCalls += 1;
            if (mode === "discovery-only" && discoveryCalls === 1)
              return toolResponse("read_file", {
                path: "src/source.ts",
                revision: "head",
                startLine: 1,
                lineCount: 8,
              });

            return discoveryResponse();
          }
          calls += 1;
          if (calls === 1 && mode !== "discovery-only") {
            if (mode === "search-only")
              return toolResponse("find_in_file", {
                path: "src/source.ts",
                revision: "head",
                literal: "Line",
                startLine: 1,
              });

            const firstEnd =
              mode === "overlapping" ? 5 : mode === "contained" ? 8 : mode === "contiguous" ? 4 : 3;

            const secondStart = mode === "contiguous" || mode === "gap" ? 5 : 4;

            return Stream.fromIterable<Response.StreamPartEncoded>([
              {
                type: "tool-call",
                id: "later-range",
                name: "read_file",
                params: {
                  path: "src/source.ts",
                  revision: mode === "wrong-revision" ? "base" : "head",
                  startLine: secondStart,
                  lineCount: 9 - secondStart,
                },
              },
              {
                type: "tool-call",
                id: "earlier-range",
                name: "read_file",
                params: {
                  path: "src/source.ts",
                  revision: "head",
                  startLine: 1,
                  lineCount: firstEnd,
                },
              },
              { type: "finish", reason: "tool-calls", usage },
            ]);
          }
          if (calls > (mode === "discovery-only" ? 1 : 2)) {
            expect(accepted).toBe(false);
            expect(calls).toBe(mode === "discovery-only" ? 2 : 3);
            expect(verificationFeedback(prompt).at(-1)).toContain("evidence 1");

            return toolResponse("submit_verification", {
              decisions: originalCandidates(prompt).map((candidate) =>
                decision(candidate, "unresolved", []),
              ),
            });
          }

          return toolResponse("submit_verification", {
            decisions: originalCandidates(prompt).map((candidate) =>
              decision(candidate, "supported", [
                {
                  kind: "source",
                  revision: "head",
                  path: "src/source.ts",
                  startLine: 2,
                  endLine: 7,
                },
              ]),
            ),
          });
        }),
      })
        .review(request)
        .pipe(
          Effect.provideService(ReviewRepository, {
            ...emptyRepository,
            readFile: (input) =>
              mode === "unavailable" && input.startLine > 1
                ? Effect.fail(ReviewContextError.make({ message: "Unavailable" }))
                : ReviewSource.fromText(input, source),
            findInFile: (input) => ReviewLineMatches.fromText(input, source),
          }),
        );

      expect(calls).toBe(accepted || mode === "discovery-only" ? 2 : 3);
      expect(outcome.diagnostics?.candidates[0]?.disposition).toBe(
        accepted ? "supported" : "unresolved",
      );
      expect(outcome.incomplete).toBe(accepted ? undefined : true);
    }),
  );

  it.effect("repairs an invalid citation in the same verifier context and cost ledger", () =>
    Effect.gen(function* () {
      let calls = 0;
      let verifierCalls = 0;
      const stages = yield* Ref.make<ReadonlyArray<ReviewStage>>([]);
      const streamsClosed = yield* Ref.make(0);
      const modelsClosed = yield* Ref.make(0);
      const observed = yield* Ref.make<ReadonlyArray<ReviewDiagnostics>>([]);

      const outcome = yield* makeReviewer({
        strategy: "verified",
        costControl: {
          beginStage: (stage) => Ref.update(stages, (current) => [...current, stage]),
          snapshot: Effect.sync(() =>
            ReviewCostSnapshot.make({
              stopped: false,
              modelCalls: calls,
              usage: ReviewUsage.make({
                inputTokens: calls * 10,
                uncachedInputTokens: calls * 7,
                cachedInputTokens: calls * 2,
                cacheWriteInputTokens: calls,
                outputTokens: calls * 4,
                estimatedCostMicrousd: calls * 100,
              }),
            }),
          ),
        },
        model: scriptedModel(
          (prompt, tools) => {
            calls += 1;
            let response: Stream.Stream<Response.StreamPartEncoded>;

            if (!isVerifier(tools)) response = discoveryResponse();
            else {
              verifierCalls += 1;
              if (verifierCalls === 1)
                response = Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: "tool-call",
                    id: "before-gap",
                    name: "read_file",
                    params: { path: "src/source.ts", revision: "head", startLine: 1, lineCount: 3 },
                  },
                  {
                    type: "tool-call",
                    id: "after-gap",
                    name: "read_file",
                    params: { path: "src/source.ts", revision: "head", startLine: 5, lineCount: 4 },
                  },
                  { type: "finish", reason: "tool-calls", usage },
                ]);
              else if (verifierCalls === 3) {
                expect(verificationFeedback(prompt).at(-1)).toContain("evidence 1");
                response = toolResponse("read_file", {
                  path: "src/source.ts",
                  revision: "head",
                  startLine: 4,
                  lineCount: 1,
                });
              } else {
                expect([2, 4]).toContain(verifierCalls);
                response = toolResponse("submit_verification", {
                  decisions: originalCandidates(prompt).map((candidate) =>
                    decision(candidate, "supported", [
                      {
                        kind: "source",
                        revision: request.headRevision,
                        path: "src/source.ts",
                        startLine: 1,
                        endLine: 8,
                      },
                    ]),
                  ),
                });
              }
            }

            return response.pipe(Stream.ensuring(Ref.update(streamsClosed, (count) => count + 1)));
          },
          Ref.update(modelsClosed, (count) => count + 1),
        ),
      })
        .review(request)
        .pipe(
          Effect.provideService(ReviewDiagnosticsSink, {
            record: (entry) => Ref.update(observed, (entries) => [...entries, entry]),
          }),
          Effect.provideService(ReviewRepository, {
            ...emptyRepository,
            readFile: (input) =>
              ReviewSource.fromText(input, "one\ntwo\nthree\nfour\nfive\nsix\nseven\neight"),
          }),
        );

      expect(outcome.incomplete).toBeUndefined();
      expect(outcome.diagnostics?.verification).toBe("complete");
      expect(outcome.diagnostics?.candidates[0]?.disposition).toBe("supported");
      expect(outcome.turns).toBe(5);
      expect(outcome.usage.estimatedCostMicrousd).toBe(500);
      expect(outcome.diagnostics?.stages.at(-1)).toMatchObject({ modelCalls: 4, toolCalls: 5 });
      expect(yield* Ref.get(stages)).toEqual(["discovery", "verification"]);
      expect(yield* Ref.get(streamsClosed)).toBe(5);
      expect(yield* Ref.get(modelsClosed)).toBe(2);
      expect(yield* Ref.get(observed)).toHaveLength(1);
    }),
  );
  it.effect.each(["界", "\u0001"] as const)(
    "does not accept evidence hidden by the engine's encoded byte bound: %s",
    (character) =>
      Effect.gen(function* () {
        const source = Array.from({ length: 180 }, () => character.repeat(100)).join("\n");

        for (const strategy of ["baseline", "verified"] as const) {
          let reads = 0;

          const outcome = yield* makeReviewer({
            strategy,
            model: scriptedModel((prompt, tools) => {
              if (strategy === "verified" && !isVerifier(tools)) return discoveryResponse();
              reads += 1;
              if (reads === 1)
                return toolResponse("read_file", {
                  path: "src/source.ts",
                  revision: "head",
                  startLine: 1,
                  lineCount: 200,
                });
              if (strategy === "baseline") return discoveryResponse([]);
              if (reads > 2) {
                expect(reads).toBe(3);
                expect(verificationFeedback(prompt).at(-1)).toContain("evidence 1");

                return toolResponse("submit_verification", {
                  decisions: originalCandidates(prompt).map((candidate) =>
                    decision(candidate, "unresolved", []),
                  ),
                });
              }

              return toolResponse("submit_verification", {
                decisions: originalCandidates(prompt).map((candidate) =>
                  decision(candidate, "supported", [
                    {
                      kind: "source",
                      revision: request.headRevision,
                      path: "src/source.ts",
                      startLine: 90,
                      endLine: 90,
                    },
                  ]),
                ),
              });
            }),
          })
            .review(request)
            .pipe(
              Effect.provideService(ReviewRepository, {
                ...emptyRepository,
                readFile: (input) => ReviewSource.fromText(input, source),
              }),
            );

          expect(outcome.diagnostics?.activity[0]).toMatchObject(
            strategy === "baseline"
              ? { outcome: "truncated", truncated: true }
              : { outcome: "oversized", truncated: false },
          );
          if (strategy === "verified") {
            expect(outcome.diagnostics?.candidates[0]?.disposition).toBe("unresolved");
            expect(outcome.incomplete).toBe(true);
          }
        }
      }),
  );
  it.effect(
    "records bounded source outcomes without retaining source, search queries, or raw failures",
    () =>
      Effect.gen(function* () {
        let calls = 0;

        const outcome = yield* makeReviewer({
          model: scriptedModel(() => {
            calls += 1;
            if (calls > 1) return discoveryResponse([]);

            return Stream.fromIterable<Response.StreamPartEncoded>([
              ...["short.ts", "large.ts", "missing.ts"].map((path): Response.StreamPartEncoded => ({
                type: "tool-call",
                id: path,
                name: "read_file",
                params: { path, revision: "base", startLine: 1, lineCount: 20 },
              })),
              {
                type: "tool-call",
                id: "search",
                name: "find_files",
                params: { query: "private search term", revision: "head" },
              },
              {
                type: "tool-call",
                id: "literal-search",
                name: "find_in_file",
                params: {
                  path: "short.ts",
                  literal: "private literal",
                  revision: "base",
                  startLine: 1,
                },
              },
              { type: "finish", reason: "tool-calls", usage },
            ]);
          }),
        })
          .review(request)
          .pipe(
            Effect.provideService(ReviewRepository, {
              readFile: (input) =>
                input.path === "missing.ts"
                  ? Effect.fail(ReviewContextError.make({ message: "private failure" }))
                  : ReviewSource.fromText(
                      input,
                      input.path === "large.ts"
                        ? "x".repeat(20_001)
                        : "private source text\nsecond line\n",
                    ),
              findFiles: () =>
                Effect.succeed(ReviewFileList.make({ paths: ["one.ts"], truncated: true })),
              findInFile: (input) =>
                ReviewLineMatches.fromText(input, "private literal and private source"),
            }),
          );

        expect(outcome.diagnostics?.activity).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              stage: "discovery",
              batch: 0,
              revision: request.baseRevision,
              path: "short.ts",
              requestedStartLine: 1,
              requestedEndLine: 20,
              returnedStartLine: 1,
              returnedEndLine: 2,
              outcome: "eof",
              truncated: false,
            }),
            expect.objectContaining({ path: "large.ts", outcome: "oversized", truncated: false }),
            expect.objectContaining({ path: "missing.ts", outcome: "unavailable" }),
            expect.objectContaining({
              operation: "find_files",
              revision: request.headRevision,
              outcome: "truncated",
              truncated: true,
              returnedPaths: 1,
            }),
            expect.objectContaining({
              operation: "find_in_file",
              revision: request.baseRevision,
              path: "short.ts",
              requestedStartLine: 1,
              returnedMatches: 1,
              outcome: "success",
              truncated: false,
            }),
          ]),
        );
        expect(outcome.diagnostics?.droppedActivityCount).toBe(0);
        expect(JSON.stringify(outcome.diagnostics)).not.toContain("private");
        expect(outcome.diagnostics?.patchesSupplied).toBe(1);
      }),
  );

  it.effect(
    "verifies the exact union of early and completion-only findings in a fresh context",
    () =>
      Effect.gen(function* () {
        const refuted = ReviewFinding.make({ ...finding, title: "The wrapper has a second leak" });
        const unresolved = ReviewFinding.make({ ...finding, title: "An unproven leak" });
        let discoveryCalls = 0;
        let verifierCalls = 0;
        const closed = yield* Ref.make(0);

        const outcome = yield* makeReviewer({
          strategy: "verified",
          model: scriptedModel(
            (prompt, tools) => {
              if (!isVerifier(tools)) {
                discoveryCalls += 1;

                return discoveryCalls === 1
                  ? toolResponse("record_finding", submitted(finding))
                  : discoveryResponse([refuted, unresolved]);
              }
              verifierCalls += 1;
              expect(prompt.content.filter(({ role }) => role === "assistant")).toHaveLength(
                verifierCalls === 1 ? 0 : 1,
              );
              expect(tools.map(({ name }) => name)).toEqual([
                "read_file",
                "find_files",
                "find_in_file",
                "submit_verification",
              ]);
              const candidates = originalCandidates(prompt);

              expect(candidates.map(({ finding }) => finding)).toEqual([
                finding,
                refuted,
                unresolved,
              ]);
              if (verifierCalls === 1)
                return toolResponse("read_file", {
                  path: "src/cleanup.ts",
                  revision: "head",
                  startLine: 1,
                  lineCount: 20,
                });

              return toolResponse("submit_verification", {
                decisions: candidates.map((candidate, index) =>
                  decision(
                    candidate,
                    index === 0 ? "supported" : index === 1 ? "refuted" : "unresolved",
                    index === 0
                      ? [
                          {
                            kind: "source",
                            revision: request.headRevision,
                            path: "src/cleanup.ts",
                            startLine: 1,
                            endLine: 2,
                          },
                        ]
                      : index === 1
                        ? [diffEvidence]
                        : [],
                  ),
                ),
              });
            },
            Ref.update(closed, (count) => count + 1),
          ),
        })
          .review(request)
          .pipe(
            Effect.provideService(ReviewRepository, {
              ...emptyRepository,
              readFile: (input) => ReviewSource.fromText(input, "original effect\nlate cleanup\n"),
            }),
          );

        expect(outcome.report.findings).toEqual([finding, unresolved]);
        expect(outcome.incomplete).toBe(true);
        expect(
          outcome.diagnostics?.candidates.map(({ disposition, publication }) => [
            disposition,
            publication,
          ]),
        ).toEqual([
          ["supported", "published"],
          ["refuted", "suppressed"],
          ["unresolved", "unverified"],
        ]);
        expect(outcome.diagnostics?.activity).toMatchObject([
          {
            stage: "verification",
            revision: request.headRevision,
            outcome: "eof",
            returnedEndLine: 2,
          },
        ]);
        expect(
          outcome.diagnostics?.stages.map(({ stage, modelCalls }) => [stage, modelCalls]),
        ).toEqual([
          ["discovery", 2],
          ["verification", 2],
        ]);
        expect(yield* Ref.get(closed)).toBe(2);
      }),
  );

  it.effect.each([
    "missing",
    "unknown",
    "duplicate",
    "empty-evidence",
    "wrong-revision",
    "outside-diff",
    "unread-source",
    "reversed-range",
  ] as const)("leaves malformed or unsupported decisions unresolved: %s", (mode) =>
    Effect.gen(function* () {
      let submissions = 0;

      const outcome = yield* makeReviewer({
        strategy: "verified",
        model: scriptedModel((prompt, tools) => {
          if (!isVerifier(tools)) return discoveryResponse();
          submissions += 1;
          const candidate = originalCandidates(prompt)[0]!;

          if (submissions > 1) {
            expect(submissions).toBe(2);
            expect(verificationFeedback(prompt)).toHaveLength(1);
            expect(verificationFeedback(prompt)[0]!.length).toBeLessThan(500);

            return toolResponse("submit_verification", {
              decisions: [decision(candidate, "unresolved", [])],
            });
          }
          const valid = decision(candidate);

          const decisions =
            mode === "missing"
              ? []
              : mode === "unknown"
                ? [{ ...valid, id: "another-request:1" }]
                : mode === "duplicate"
                  ? [valid, valid]
                  : [
                      {
                        ...valid,
                        evidence:
                          mode === "empty-evidence"
                            ? []
                            : [
                                {
                                  ...diffEvidence,
                                  revision: "head",
                                  ...(mode === "wrong-revision"
                                    ? { revision: "head-from-another-request" }
                                    : {}),
                                  ...(mode === "outside-diff" ? { endLine: 3 } : {}),
                                  ...(mode === "unread-source" ? { kind: "source" } : {}),
                                  ...(mode === "reversed-range"
                                    ? { startLine: 2, endLine: 1 }
                                    : {}),
                                },
                              ],
                      },
                    ];

          return toolResponse("submit_verification", { decisions });
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.report.findings).toEqual([finding]);
      expect(outcome.incomplete).toBe(true);
      expect(outcome.diagnostics?.verification).toBe("incomplete");
      expect(submissions).toBe(2);
      expect(outcome.diagnostics?.candidates[0]).toMatchObject({
        disposition: "unresolved",
        publication: "unverified",
      });
    }),
  );

  it.effect("accepts patch-only refutation without treating it as a prior-review resolution", () =>
    Effect.gen(function* () {
      const outcome = yield* makeReviewer({
        strategy: "verified",
        model: scriptedModel((prompt, tools) =>
          !isVerifier(tools)
            ? discoveryResponse()
            : toolResponse("submit_verification", {
                decisions: originalCandidates(prompt).map((candidate) =>
                  decision(candidate, "refuted"),
                ),
              }),
        ),
      })
        .review(
          ReviewRequest.make({
            ...request,
            followUps: [
              ReviewFollowUp.make({ id: "prior", description: "A prior blocker remains." }),
            ],
          }),
        )
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.report.findings).toEqual([]);
      expect(outcome.resolutions).toBeUndefined();
      expect(outcome.incomplete).toBeUndefined();
      expect(outcome.diagnostics?.candidates[0]?.finding).toEqual(finding);
      expect(outcome.diagnostics?.verification).toBe("complete");
    }),
  );

  it.effect.each([false, true])(
    "skips zero-candidate verification and retains explicit prior resolution semantics: %s",
    (incomplete) =>
      Effect.gen(function* () {
        let calls = 0;

        const outcome = yield* makeReviewer({
          strategy: "verified",
          model: scriptedModel((_prompt, tools) => {
            calls += 1;
            expect(isVerifier(tools)).toBe(false);

            return toolResponse("submit_review", {
              findings: [],
              incomplete,
              resolutions: [
                { id: "prior", evidence: "The head guards the original unsupported transition." },
              ],
            });
          }),
        })
          .review(
            ReviewRequest.make({
              ...request,
              followUps: [
                ReviewFollowUp.make({ id: "prior", description: "Verify the old transition." }),
              ],
            }),
          )
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(calls).toBe(1);
        expect(outcome.diagnostics?.verification).toBe("skipped");
        expect(outcome.incomplete).toBe(incomplete ? true : undefined);
        expect(outcome.resolutions?.length ?? 0).toBe(incomplete ? 0 : 1);
      }),
  );

  it.effect.each([
    { strategy: "baseline", invalidPosition: "first" },
    { strategy: "baseline", invalidPosition: "last" },
    { strategy: "verified", invalidPosition: "first" },
    { strategy: "verified", invalidPosition: "last" },
  ] as const)(
    "keeps valid final candidates with $strategy and an invalid finding $invalidPosition",
    ({ strategy, invalidPosition }) =>
      Effect.gen(function* () {
        const invalid = ReviewFinding.make({ ...finding, path: "unknown.ts" });
        let calls = 0;

        const outcome = yield* makeReviewer({
          strategy,
          model: scriptedModel((prompt, tools) => {
            calls += 1;

            return !isVerifier(tools)
              ? discoveryResponse(
                  invalidPosition === "first" ? [invalid, finding] : [finding, invalid],
                )
              : toolResponse("submit_verification", {
                  decisions: originalCandidates(prompt).map((candidate) => decision(candidate)),
                });
          }),
        })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(outcome.report.findings).toEqual([finding]);
        expect(outcome.incomplete).toBe(true);
        expect(calls).toBe(strategy === "verified" ? 2 : 1);
        expect(outcome.diagnostics).toMatchObject({
          discovery: "incomplete",
          verification: strategy === "verified" ? "complete" : "not-requested",
        });
        expect(outcome.diagnostics?.stages[0]).toMatchObject({
          completion: "incomplete",
          stopReason: "invalid-submission",
          declaredAssessment: "complete",
        });
      }),
  );

  it.effect.each(["union", "record"] as const)(
    "retains the first 24 candidates and makes overflow sticky: %s",
    (mode) =>
      Effect.gen(function* () {
        const findings = Array.from({ length: 25 }, (_, index) =>
          ReviewFinding.make({ ...finding, title: `Independent cause ${String(index)}` }),
        );

        let calls = 0;

        const outcome = yield* makeReviewer({
          strategy: "verified",
          model: scriptedModel((prompt, tools) => {
            if (isVerifier(tools))
              return toolResponse("submit_verification", {
                decisions: originalCandidates(prompt).map((candidate) =>
                  decision(candidate, "refuted"),
                ),
              });
            calls += 1;
            if (calls === 1)
              return Stream.fromIterable<Response.StreamPartEncoded>([
                ...findings
                  .slice(0, mode === "record" ? 25 : 23)
                  .map((entry, index): Response.StreamPartEncoded => ({
                    type: "tool-call",
                    id: `record-${String(index)}`,
                    name: "record_finding",
                    params: submitted(entry),
                  })),
                { type: "finish", reason: "tool-calls", usage },
              ]);

            return discoveryResponse(mode === "record" ? [] : findings.slice(23));
          }),
        })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(outcome.report.findings).toEqual([]);
        expect(outcome.diagnostics?.candidates.map(({ finding }) => finding)).toEqual(
          findings.slice(0, 24),
        );
        expect(outcome.diagnostics?.droppedCandidateCount).toBe(1);
        expect(outcome.incomplete).toBe(true);
        expect(outcome.diagnostics?.verification).toBe("complete");
      }),
  );

  it.effect("repeated anchors do not consume candidate capacity or verifier decisions", () =>
    Effect.gen(function* () {
      const findings = [
        finding,
        ReviewFinding.make({ ...finding, path: "src/other.ts" }),
        ReviewFinding.make({ ...finding, severity: "important" }),
        ReviewFinding.make({ ...finding, category: "reliability" }),
        ReviewFinding.make({ ...finding, title: "A separate handle leaks" }),
        ReviewFinding.make({ ...finding, body: "Cancellation skips cleanup of the handle." }),
        ...Array.from({ length: 18 }, (_, index) =>
          ReviewFinding.make({ ...finding, title: `Independent cause ${String(index)}` }),
        ),
      ];

      const input = ReviewRequest.make({
        ...request,
        changes: [...request.changes, ReviewChange.make({ path: "src/other.ts", patch })],
      });

      const digest = yield* reviewRequestDigest(input);
      let discoveryCalls = 0;
      let verifierCalls = 0;

      const outcome = yield* makeReviewer({
        strategy: "verified",
        model: scriptedModel((prompt, tools) => {
          if (isVerifier(tools)) {
            verifierCalls += 1;
            const candidates = originalCandidates(prompt);

            expect(candidates.map(({ finding }) => finding)).toEqual(findings);
            expect(candidates.map(({ id }) => id)).toEqual(
              findings.map((_, index) => `${digest}:${String(index + 1)}`),
            );

            return toolResponse("submit_verification", {
              decisions: candidates.map((candidate) =>
                decision(candidate, "supported", [
                  { ...diffEvidence, path: candidate.finding.path },
                ]),
              ),
            });
          }
          discoveryCalls += 1;
          if (discoveryCalls === 1)
            return Stream.fromIterable<Response.StreamPartEncoded>([
              ...findings.map((entry, index): Response.StreamPartEncoded => ({
                type: "tool-call",
                id: `record-${String(index)}`,
                name: "record_finding",
                params: submitted(entry),
              })),
              { type: "finish", reason: "tool-calls", usage },
            ]);
          if (discoveryCalls === 2)
            return toolResponse(
              "record_finding",
              submitted(ReviewFinding.make({ ...finding, line: 1 })),
            );

          return discoveryResponse(
            findings.map((entry) => ReviewFinding.make({ ...entry, line: 1 })),
          );
        }),
      })
        .review(input)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(discoveryCalls).toBe(3);
      expect(verifierCalls).toBe(1);
      expect(outcome.report.findings).toEqual(findings);
      expect(outcome.incomplete).toBeUndefined();
      expect(outcome.diagnostics).toMatchObject({
        discovery: "complete",
        verification: "complete",
        droppedCandidateCount: 0,
      });
    }),
  );

  it.effect.each(["discovery", "verification"] as const)(
    "preserves candidates after a handled failure in %s",
    (failedStage) =>
      Effect.gen(function* () {
        let calls = 0;
        let verifierCalls = 0;

        const outcome = yield* makeReviewer({
          strategy: "verified",
          model: scriptedModel((prompt, tools) => {
            if (isVerifier(tools)) {
              verifierCalls += 1;

              return failedStage === "verification"
                ? toolResponse("submit_verification", { malformed: true })
                : toolResponse("submit_verification", {
                    decisions: originalCandidates(prompt).map((candidate) => decision(candidate)),
                  });
            }
            calls += 1;

            return failedStage === "verification"
              ? discoveryResponse()
              : calls === 1
                ? toolResponse("record_finding", submitted(finding))
                : toolResponse("submit_review", { malformed: true });
          }),
        })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(verifierCalls).toBe(1);
        expect(outcome.report.findings).toEqual([finding]);
        expect(outcome.incomplete).toBe(true);
        expect(outcome.diagnostics?.candidates[0]?.disposition).toBe(
          failedStage === "verification" ? "unresolved" : "supported",
        );
      }),
  );

  it.effect.each(["defect", "interrupt"] as const)(
    "propagates verifier %s during citation repair after cleanup, retaining available evidence",
    (mode) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const streamsClosed = yield* Ref.make(0);
        const modelsClosed = yield* Ref.make(0);
        const observed = yield* Ref.make<ReviewDiagnostics | undefined>(undefined);
        let calls = 0;
        let verifierCalls = 0;

        const program = makeReviewer({
          strategy: "verified",
          model: scriptedModel(
            (prompt, tools) => {
              calls += 1;
              if (isVerifier(tools)) verifierCalls += 1;

              return (
                isVerifier(tools)
                  ? verifierCalls === 1
                    ? toolResponse("submit_verification", {
                        decisions: originalCandidates(prompt).map((candidate) =>
                          decision(candidate, "supported", [
                            diffEvidence,
                            { ...diffEvidence, kind: "source" },
                          ]),
                        ),
                      })
                    : Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
                        Stream.flatMap(() =>
                          mode === "defect" ? Stream.die("private defect") : Stream.never,
                        ),
                      )
                  : discoveryResponse()
              ).pipe(Stream.ensuring(Ref.update(streamsClosed, (count) => count + 1)));
            },
            Ref.update(modelsClosed, (count) => count + 1),
          ),
        })
          .review(request)
          .pipe(
            Effect.provideService(ReviewDiagnosticsSink, {
              record: (value) => Ref.set(observed, value),
            }),
            Effect.provideService(ReviewRepository, emptyRepository),
          );

        const fiber = yield* Effect.forkChild(program);

        yield* Deferred.await(started);
        if (mode === "interrupt") yield* Fiber.interrupt(fiber);
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
        expect(calls).toBe(3);
        expect(yield* Ref.get(streamsClosed)).toBe(3);
        expect(yield* Ref.get(modelsClosed)).toBe(2);
        const diagnostics = yield* Ref.get(observed);

        expect(diagnostics?.candidates[0]).toMatchObject({
          finding,
          disposition: "unresolved",
          evidence: [diffEvidence],
        });
        expect(diagnostics?.verification).toBe("failed");
        expect(JSON.stringify(diagnostics)).not.toContain("private defect");
      }),
  );

  it.effect("retains available evidence after a failed attempt to repair a submission", () =>
    Effect.gen(function* () {
      let verifierCalls = 0;

      const outcome = yield* makeReviewer({
        strategy: "verified",
        model: scriptedModel((prompt, tools) => {
          if (!isVerifier(tools)) return discoveryResponse();
          verifierCalls += 1;
          if (verifierCalls === 1)
            return toolResponse("submit_verification", {
              decisions: originalCandidates(prompt).map((candidate) =>
                decision(candidate, "supported", [
                  { ...diffEvidence, revision: "head" },
                  { ...diffEvidence, revision: "head", kind: "source" },
                ]),
              ),
            });
          expect(verifierCalls).toBe(2);
          expect(verificationFeedback(prompt).at(-1)).toContain("evidence 2");

          return toolResponse("submit_verification", { malformed: true });
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(verifierCalls).toBe(2);
      expect(outcome.incomplete).toBe(true);
      expect(outcome.diagnostics?.verification).toBe("failed");
      expect(outcome.diagnostics?.candidates[0]).toMatchObject({
        finding,
        disposition: "unresolved",
        evidence: [diffEvidence],
      });
      expect(outcome.diagnostics?.stages.at(-1)?.stopReason).toBe("invalid-decisions");
    }),
  );

  it.effect.each(["defect", "interrupt"] as const)(
    "never enters verification after a discovery %s",
    (mode) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const observed = yield* Ref.make<ReviewDiagnostics | undefined>(undefined);
        const closed = yield* Ref.make(0);
        let calls = 0;

        const fiber = yield* makeReviewer({
          strategy: "verified",
          model: scriptedModel((_prompt, tools) => {
            expect(isVerifier(tools)).toBe(false);
            calls += 1;

            return calls === 1
              ? toolResponse("record_finding", submitted(finding))
              : Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
                  Stream.flatMap(() =>
                    mode === "defect" ? Stream.die("discovery defect") : Stream.never,
                  ),
                  Stream.ensuring(Ref.update(closed, (count) => count + 1)),
                );
          }),
        })
          .review(request)
          .pipe(
            Effect.provideService(ReviewDiagnosticsSink, {
              record: (value) => Ref.set(observed, value),
            }),
            Effect.provideService(ReviewRepository, emptyRepository),
            Effect.forkChild,
          );

        yield* Deferred.await(started);
        if (mode === "interrupt") yield* Fiber.interrupt(fiber);
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
        expect(calls).toBe(2);
        expect(yield* Ref.get(closed)).toBe(1);
        expect((yield* Ref.get(observed))?.candidates[0]?.finding).toEqual(finding);
      }),
  );

  it.effect.each([90_000, 200_000])(
    "shares candidates across batches and respects the verifier context bound: %s",
    (patchSize) =>
      Effect.gen(function* () {
        const perBatch = patchSize === 90_000 ? 2 : 1;

        const changes = Array.from({ length: 3 * perBatch }, (_, index) =>
          ReviewChange.make({
            path: `src/${String(index)}.ts`,
            patch: "@@ -0,0 +1 @@\n+".padEnd(patchSize, "x"),
          }),
        );

        const calls = yield* Ref.make(0);
        let discovered = 0;

        const outcome = yield* makeReviewer({
          strategy: "verified",
          costControl: {
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
          },
          model: scriptedModel((prompt, tools) =>
            Stream.unwrap(
              Ref.update(calls, (count) => count + 1).pipe(
                Effect.map(() => {
                  if (isVerifier(tools))
                    return toolResponse("submit_verification", {
                      decisions: originalCandidates(prompt).map((candidate) =>
                        decision(candidate, "supported", [
                          {
                            ...diffEvidence,
                            path: candidate.finding.path,
                            startLine: 1,
                            endLine: 1,
                          },
                        ]),
                      ),
                    });
                  const path = changes[discovered * perBatch]!.path;

                  discovered += 1;

                  return discoveryResponse(
                    [ReviewFinding.make({ ...finding, path, line: 1 })],
                    discovered === 1,
                  );
                }),
              ),
            ),
          ),
        })
          .review(ReviewRequest.make({ ...request, changes }))
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(outcome.diagnostics?.candidates.map(({ batch }) => batch)).toEqual([0, 1, 2]);
        expect(outcome.diagnostics?.patchesSupplied).toBe(3 * perBatch);
        expect(outcome.diagnostics?.verification).toBe(perBatch === 2 ? "complete" : "failed");
        expect(outcome.pendingPaths).toBeUndefined();
        expect(outcome.incomplete).toBe(true);
        expect(outcome.diagnostics?.discovery).toBe("incomplete");
        expect(outcome.resolutions).toBeUndefined();
        expect(outcome.turns).toBe(perBatch === 2 ? 4 : 3);
        if (perBatch === 1) expect(outcome.exhausted).toBe("tokens");
        expect(
          outcome.diagnostics?.stages.reduce((count, stage) => count + stage.modelCalls, 0),
        ).toBe(outcome.turns);
      }),
  );

  it.effect("does not renew the tool allowance for verification", () =>
    Effect.gen(function* () {
      let calls = 0;

      const outcome = yield* makeReviewer({
        strategy: "verified",
        model: scriptedModel((_prompt, tools) => {
          calls += 1;
          expect(isVerifier(tools)).toBe(false);
          if (calls > 1) return discoveryResponse();

          return Stream.fromIterable<Response.StreamPartEncoded>([
            {
              type: "tool-call",
              id: "candidate",
              name: "record_finding",
              params: submitted(finding),
            },
            ...Array.from({ length: 62 }, (_, index): Response.StreamPartEncoded => ({
              type: "tool-call",
              id: `search-${String(index)}`,
              name: "find_files",
              params: { revision: "head", query: "guard" },
            })),
            { type: "finish", reason: "tool-calls", usage },
          ]);
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(calls).toBe(2);
      expect(outcome.diagnostics?.candidates[0]?.disposition).toBe("unresolved");
      expect(outcome.diagnostics?.stages.at(-1)).toMatchObject({
        stage: "verification",
        modelCalls: 0,
        stopReason: "tool-calls",
      });
      expect(outcome.incomplete).toBe(true);
    }),
  );

  it.effect("does not renew the turn allowance for verification", () =>
    Effect.gen(function* () {
      let calls = 0;

      const outcome = yield* makeReviewer({
        strategy: "verified",
        model: scriptedModel((_prompt, tools) => {
          calls += 1;
          expect(isVerifier(tools)).toBe(false);

          return calls < 8
            ? toolResponse("find_files", { revision: "head", query: "guard" })
            : discoveryResponse();
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(calls).toBe(8);
      expect(outcome.incomplete).toBe(true);
      expect(outcome.diagnostics?.candidates[0]?.disposition).toBe("unresolved");
      expect(outcome.diagnostics?.stages.at(-1)).toMatchObject({
        stage: "verification",
        modelCalls: 0,
        stopReason: "turns",
      });
    }),
  );

  it.effect("shares the absolute deadline and cleans up a timed-out citation repair", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const verifying = yield* Deferred.make<void>();
      const closed = yield* Ref.make(0);
      let verifierCalls = 0;

      const program = makeReviewer({
        strategy: "verified",
        model: scriptedModel((prompt, tools) => {
          const verifier = isVerifier(tools);

          if (verifier && ++verifierCalls === 1)
            return toolResponse("submit_verification", {
              decisions: originalCandidates(prompt).map((candidate) =>
                decision(candidate, "supported", [
                  diffEvidence,
                  { ...diffEvidence, kind: "source" },
                ]),
              ),
            }).pipe(Stream.ensuring(Ref.update(closed, (count) => count + 1)));
          if (verifier) expect(verificationFeedback(prompt).at(-1)).toContain("evidence 2");

          return Stream.fromEffect(
            Deferred.succeed(verifier ? verifying : started, undefined),
          ).pipe(
            Stream.flatMap(() => Stream.fromEffect(Effect.sleep("4 minutes"))),
            Stream.flatMap(() => discoveryResponse()),
            Stream.ensuring(Ref.update(closed, (count) => count + 1)),
          );
        }),
      })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      const fiber = yield* Effect.forkChild(program);

      yield* Deferred.await(started);
      yield* TestClock.adjust("4 minutes");
      yield* Deferred.await(verifying);
      yield* TestClock.adjust("1 minute");
      const outcome = yield* Fiber.join(fiber);

      expect(outcome.incomplete).toBe(true);
      expect(outcome.diagnostics?.candidates[0]?.disposition).toBe("unresolved");
      expect(outcome.diagnostics?.candidates[0]?.evidence).toEqual([diffEvidence]);
      expect(verifierCalls).toBe(2);
      expect(yield* Ref.get(closed)).toBe(3);
      expect(outcome.diagnostics?.stages.at(-1)).toMatchObject({
        stage: "verification",
        completion: "failed",
        stopReason: "deadline",
      });
      expect(outcome.diagnostics?.stages.at(-1)?.incompleteReason).toBeUndefined();
    }),
  );

  it.effect.each([false, true])(
    "distinguishes discovery allowance exhaustion from global closure: %s",
    (globalStopped) =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const stage = yield* Ref.make<ReviewStage>("discovery");
        const transitioned = yield* Ref.make<ReadonlyArray<ReviewStage>>([]);

        const control = {
          beginStage: (next: ReviewStage) =>
            Ref.set(stage, next).pipe(
              Effect.andThen(Ref.update(transitioned, (current) => [...current, next])),
            ),
          snapshot: Effect.gen(function* () {
            const modelCalls = yield* Ref.get(calls);
            const current = yield* Ref.get(stage);

            return ReviewCostSnapshot.make({
              stage: current,
              globalStopped: globalStopped && modelCalls > 0,
              stopped: modelCalls > 0 && (globalStopped || current === "discovery"),
              modelCalls,
              usage: ReviewUsage.make({
                inputTokens: modelCalls * 10,
                uncachedInputTokens: modelCalls * 7,
                cachedInputTokens: modelCalls * 2,
                cacheWriteInputTokens: modelCalls,
                outputTokens: modelCalls * 4,
              }),
            });
          }),
        };

        const outcome = yield* makeReviewer({
          strategy: "verified",
          costControl: control,
          model: scriptedModel((prompt, tools) =>
            Stream.unwrap(
              Ref.update(calls, (count) => count + 1).pipe(
                Effect.as(
                  !isVerifier(tools)
                    ? discoveryResponse()
                    : toolResponse("submit_verification", {
                        decisions: originalCandidates(prompt).map((candidate) =>
                          decision(candidate),
                        ),
                      }),
                ),
              ),
            ),
          ),
        })
          .review(request)
          .pipe(Effect.provideService(ReviewRepository, emptyRepository));

        expect(yield* Ref.get(calls)).toBe(globalStopped ? 1 : 2);
        expect(yield* Ref.get(transitioned)).toEqual(
          globalStopped ? ["discovery"] : ["discovery", "verification"],
        );
        expect(outcome.exhausted).toBe("cost");
        expect(outcome.incomplete).toBe(true);
        expect(outcome.diagnostics?.candidates[0]?.disposition).toBe(
          globalStopped ? "unresolved" : "supported",
        );
      }),
  );

  it.effect("binds baseline and verified candidate identities to every exact request field", () =>
    Effect.gen(function* () {
      const digest = yield* reviewRequestDigest(request);
      const same = yield* reviewRequestDigest(ReviewRequest.make({ ...request }));

      expect(same).toBe(digest);
      for (const changed of [
        ReviewRequest.make({ ...request, description: "Different supported operation" }),
        ReviewRequest.make({ ...request, headRevision: "another-head" }),
        ReviewRequest.make({
          ...request,
          changes: [ReviewChange.make({ path: finding.path, patch: `${patch}\n+another` })],
        }),
        ReviewRequest.make({ ...request, unreviewedPaths: ["excluded.ts"] }),
        ReviewRequest.make({
          ...request,
          followUps: [ReviewFollowUp.make({ id: "prior", description: "Old blocker" })],
        }),
      ])
        expect(yield* reviewRequestDigest(changed)).not.toBe(digest);

      const outcome = yield* makeReviewer({ model: scriptedModel(() => discoveryResponse()) })
        .review(request)
        .pipe(Effect.provideService(ReviewRepository, emptyRepository));

      expect(outcome.diagnostics?.candidates[0]).toMatchObject({
        id: `${digest}:1`,
        requestDigest: digest,
        disposition: "not-requested",
        publication: "published",
      });
    }),
  );
});
