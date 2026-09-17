import { DecisionModel } from "@effect-agent/ai-decision";
import { describe, expect, it } from "@effect/vitest";
import type { Layer } from "effect";
import { Cause, Context, Effect, Encoding, Exit, Fiber, Queue, Ref, Schema, Stream } from "effect";
import * as CodeMode from "effect-agent/code-mode";
import { ToolExecutionClass } from "effect-agent/durable-step";
import * as ToolDiscovery from "effect-agent/tool-discovery";
import {
  AdditionalToolCatalog,
  DiscoveryTool,
  PinnedTool,
  ToolNamespace,
  CurrentToolCatalog,
  type CatalogEntry,
} from "effect-agent/tool-exposure";
import { TestClock } from "effect/testing";
import { AiError, Tool, type Toolkit } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

const Search = Tool.make("search_pages", {
  description: "Read web pages about a topic",
  parameters: Schema.Struct({ count: Schema.NumberFromString }),
  success: Schema.Struct({ found: Schema.NumberFromString }),
})
  .annotate(ToolNamespace, "web")
  .annotate(ToolExecutionClass, "readonly");

const Query = Tool.make("query_records", {
  description: "Query stored account records",
  parameters: Schema.Struct({ account: Schema.String }),
  success: Schema.Array(Schema.String),
})
  .annotate(ToolNamespace, "records")
  .annotate(ToolExecutionClass, "readonly");

const native = (tool: Tool.Any, namespace?: string): CatalogEntry => ({
  kind: "native",
  tool,
  nativeToolName: tool.name,
  ...(namespace === undefined ? {} : { namespace }),
});

const invoke = <NativeTool extends Tool.Any, BuildError, Requirements>(
  definition: {
    readonly toolkit: Toolkit.Toolkit<{ readonly discover_tools: NativeTool }>;
    readonly handlers: Layer.Layer<
      Tool.HandlersFor<{ readonly discover_tools: NativeTool }>,
      BuildError,
      Requirements
    >;
  },
  entries: ReadonlyArray<CatalogEntry>,
  parameters: Tool.ParametersEncoded<NativeTool>,
) =>
  Effect.gen(function* () {
    const toolkit = yield* definition.toolkit;
    const stream = yield* toolkit.handle("discover_tools", parameters);
    const results = yield* Stream.runCollect(stream);
    const result = results[0];

    if (result === undefined) return yield* Effect.die("Missing discovery result");

    return yield* Schema.decodeUnknownEffect(ToolDiscovery.Result)(result.result);
  }).pipe(
    Effect.provide(definition.handlers),
    Effect.provideService(CurrentToolCatalog, { entries }),
  );

describe("ToolDiscovery", () => {
  it.effect(
    "searches native Schema description annotations and rejects provider-only documentation",
    () =>
      Effect.gen(function* () {
        const Annotated = Tool.make("annotated", {
          parameters: Schema.Struct({ key: Schema.String }).annotate({
            description: "Locate special archives",
          }),
          success: Schema.String,
        });

        const result = yield* invoke(ToolDiscovery.make(), [native(Annotated)], {
          query: "special archives",
        });

        expect(result.matches[0]?.description).toBe("Locate special archives");

        const Provider = Tool.providerDefined({
          id: "test.provider",
          customName: "provider_search",
          providerName: "provider_search",
          args: Tool.EmptyParams,
        })({});

        const error = yield* invoke(ToolDiscovery.make(), [native(Provider)], {
          query: "provider",
        }).pipe(Effect.flip);

        expect(error).toMatchObject({ _tag: "ToolDiscoveryError", reason: "invalid-schema" });
      }),
  );
  it.effect("returns deterministic matches and encoded schemas from the visible catalogue", () =>
    Effect.gen(function* () {
      const definition = ToolDiscovery.make({
        namespaceDescriptions: { web: "Public research", secret: "Confidential operations" },
      });

      const result = yield* invoke(definition, [native(Query, "records"), native(Search, "web")], {
        query: "PUBLIC pages",
      });

      expect(result.toolNames).toEqual(["search_pages"]);
      expect(result.matches).toMatchObject([
        {
          id: "native:search_pages",
          kind: "native",
          namespace: "web",
          namespaceDescription: "Public research",
          parameters: { properties: { count: { type: "string" } } },
          success: { properties: { found: { type: "string" } } },
        },
      ]);
      expect(JSON.stringify(result)).not.toContain("Confidential");
      expect(definition.tool.description).not.toContain("secret");
      expect(Context.get(definition.tool.annotations, DiscoveryTool)).toBe(true);
      expect(Context.get(definition.tool.annotations, PinnedTool)).toBe(true);
      expect(Object.keys(definition.toolkit.tools)).toEqual(["discover_tools"]);

      const sorted = yield* invoke(definition, [native(Search, "web"), native(Query, "records")], {
        query: "r",
      });

      expect(sorted.matches.map((match) => match.id)).toEqual([
        "native:query_records",
        "native:search_pages",
      ]);
      const none = yield* invoke(definition, [native(Search, "web")], { query: "missing" });

      expect(none).toEqual({ toolNames: [], matches: [] });

      const prototypeNamespace = yield* invoke(definition, [native(Query, "constructor")], {
        query: "constructor",
      });

      expect(prototypeNamespace.toolNames).toEqual(["query_records"]);
      expect(prototypeNamespace.matches[0]?.namespaceDescription).toBeUndefined();
    }),
  );

  it.effect("keeps direct registration and Code Mode aliases distinct", () =>
    Effect.gen(function* () {
      const codeMode = CodeMode.make("execute", {
        description: "Execute a program",
        includeDeclarations: false,
        tools: { web: { search: Search, lookup: Search } },
      });

      const additional = Context.get(codeMode.tool.annotations, AdditionalToolCatalog);

      expect(Object.isFrozen(additional)).toBe(true);
      expect(additional.every(Object.isFrozen)).toBe(true);

      const entries: ReadonlyArray<CatalogEntry> = [
        native(Search, "web"),
        ...additional.map((entry) => ({
          ...entry,
          kind: "code-mode" as const,
          nativeToolName: "execute",
        })),
      ];

      const definition = ToolDiscovery.make();
      const result = yield* invoke(definition, entries, { query: "pages" });

      expect(result.matches.map((match) => match.id)).toEqual([
        "code-mode:execute:web.lookup",
        "code-mode:execute:web.search",
        "native:search_pages",
      ]);
      expect(result.toolNames).toEqual(["execute", "search_pages"]);

      const onlyAlias = ToolDiscovery.make({
        search: (_request, catalogue) =>
          Effect.succeed(
            catalogue
              .filter((entry) => entry.id === "code-mode:execute:web.lookup")
              .map((entry) => entry.id),
          ),
      });

      const selected = yield* invoke(onlyAlias, entries, { query: "pages" });

      expect(selected.toolNames).toEqual(["execute"]);
      expect(selected.matches.map((match) => match.method)).toEqual(["lookup"]);
    }),
  );

  it.effect("filters namespaces before custom search and validates every returned identity", () =>
    Effect.gen(function* () {
      const observed = yield* Ref.make<ReadonlyArray<string>>([]);

      const definition = ToolDiscovery.make({
        search: (_request, catalogue) =>
          Ref.set(
            observed,
            catalogue.map((entry) => entry.id),
          ).pipe(Effect.as(catalogue.map((entry) => entry.id))),
      });

      const result = yield* invoke(definition, [native(Search, "web"), native(Query, "records")], {
        query: "ignored",
        namespace: "web",
      });

      expect(yield* Ref.get(observed)).toEqual(["native:search_pages"]);
      expect(result.toolNames).toEqual(["search_pages"]);
      for (const ids of [["native:secret"], ["native:search_pages", "native:secret"]]) {
        const invalid = ToolDiscovery.make({ maxResults: 1, search: () => Effect.succeed(ids) });

        const error = yield* invoke(invalid, [native(Search, "web")], { query: "pages" }).pipe(
          Effect.flip,
        );

        expect(error).toMatchObject({ _tag: "ToolDiscoveryError", reason: "unknown-match" });
      }

      const duplicates = ToolDiscovery.make({
        search: () => Effect.succeed(["native:search_pages", "native:search_pages"]),
      });

      const error = yield* invoke(duplicates, [native(Search, "web")], { query: "pages" }).pipe(
        Effect.flip,
      );

      expect(error).toMatchObject({ reason: "invalid-matches" });
    }),
  );

  it.effect("enforces the complete encoded UTF-8 result bound", () =>
    Effect.gen(function* () {
      const Unicode = Tool.make("unicode", {
        description: "Read café 東京 😀",
        parameters: Schema.Struct({ key: Schema.String }),
        success: Schema.String,
      });

      const entries = [native(Unicode)];
      const result = yield* invoke(ToolDiscovery.make(), entries, { query: "read" });

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ToolDiscovery.Result))(
        result,
      );

      const bytes = Encoding.encodeHex(encoded).length / 2;

      expect(bytes).toBeGreaterThan(encoded.length);
      expect(
        yield* invoke(ToolDiscovery.make({ maxResultBytes: bytes }), entries, { query: "read" }),
      ).toEqual(result);

      const bounded = yield* invoke(ToolDiscovery.make({ maxResultBytes: bytes - 1 }), entries, {
        query: "read",
      });

      expect(bounded).toMatchObject({
        toolNames: [],
        matches: [],
        notice: expect.stringMatching(/increase maxResultBytes/),
      });

      const minimum = yield* invoke(ToolDiscovery.make({ maxResultBytes: 256 }), entries, {
        query: "read",
      });

      expect(minimum).toEqual(bounded);

      const boundedJson = yield* Schema.encodeEffect(Schema.fromJsonString(ToolDiscovery.Result))(
        minimum,
      );

      expect(Encoding.encodeHex(boundedJson).length / 2).toBeLessThanOrEqual(256);
    }),
  );

  it.effect(
    "skips oversized candidates and keeps complete later matches in custom rank order (#496)",
    () =>
      Effect.gen(function* () {
        const Oversized = Tool.make("oversized", {
          parameters: Schema.Struct({
            key: Schema.String.annotate({ description: "東京".repeat(400) }),
          }),
          success: Schema.String,
        });

        const ranked = ["native:oversized", "native:search_pages", "native:query_records"];

        const definition = ToolDiscovery.make({
          maxResults: 3,
          maxResultBytes: 2_000,
          search: () => Effect.succeed(ranked),
        });

        const entries = [native(Query), native(Oversized), native(Search)];

        const complete = yield* invoke(ToolDiscovery.make(), [native(Search), native(Query)], {
          query: "r",
        });

        const result = yield* invoke(definition, entries, { query: "ignored" });

        expect(result.toolNames).toEqual(["search_pages", "query_records"]);
        expect(result.matches).toEqual([complete.matches[1], complete.matches[0]]);
        expect(result.notice).toMatch(/narrow.*search/i);
        expect(yield* invoke(definition, entries.toReversed(), { query: "ignored" })).toEqual(
          result,
        );

        // The exact budget includes the notice, JSON escaping, schemas and toolNames.
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ToolDiscovery.Result))(
          result,
        );

        const bytes = Encoding.encodeHex(encoded).length / 2;

        expect(bytes).toBeLessThanOrEqual(2_000);
        expect(
          yield* invoke(
            ToolDiscovery.make({ maxResultBytes: bytes, search: () => Effect.succeed(ranked) }),
            entries,
            { query: "ignored" },
          ),
        ).toEqual(result);

        const smaller = yield* invoke(
          ToolDiscovery.make({ maxResultBytes: bytes - 1, search: () => Effect.succeed(ranked) }),
          entries,
          { query: "ignored" },
        );

        expect(smaller.toolNames).toEqual(["search_pages"]);
        expect(smaller.matches).toEqual([complete.matches[1]]);

        const limited = yield* invoke(
          ToolDiscovery.make({
            maxResults: 1,
            maxResultBytes: 2_000,
            search: () => Effect.succeed(ranked),
          }),
          entries,
          { query: "ignored" },
        );

        expect(limited).toMatchObject({ toolNames: [], matches: [], notice: expect.any(String) });
      }),
  );

  it.effect("retains catalogue and selected schema failures under tight byte budgets", () =>
    Effect.gen(function* () {
      const definition = ToolDiscovery.make({
        maxResultBytes: 256,
        search: (_request, catalogue) => Effect.succeed(catalogue.map((entry) => entry.id)),
      });

      const duplicate = yield* invoke(definition, [native(Search), native(Search)], {
        query: "read",
      }).pipe(Effect.flip);

      expect(duplicate).toMatchObject({ reason: "invalid-catalogue" });

      const Invalid = Tool.make("unsupported", {
        success: Schema.Struct({ [Symbol.for("opaque")]: Schema.String }),
      });

      const invalid = yield* invoke(definition, [native(Search), native(Invalid)], {
        query: "read",
      }).pipe(Effect.flip);

      expect(invalid).toMatchObject({ reason: "invalid-schema" });
    }),
  );

  it("rejects invalid construction bounds and namespace descriptions", () => {
    for (const maxResults of [0, 65, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => ToolDiscovery.make({ maxResults })).toThrow(/maxResults/);
    }
    for (const maxResultBytes of [0, 255, 256 * 1024 + 1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => ToolDiscovery.make({ maxResultBytes })).toThrow(/maxResultBytes/);
    }
    expect(() => ToolDiscovery.make({ namespaceDescriptions: { web: "x".repeat(513) } })).toThrow(
      /namespaceDescriptions/,
    );
  });

  it.effect("preserves custom failures and closes search resources on success and failure", () =>
    Effect.gen(function* () {
      const finalized = yield* Ref.make(0);

      for (const shouldFail of [false, true]) {
        const definition = ToolDiscovery.make({
          failure: SearchError,
          search: (_request, catalogue) =>
            Effect.gen(function* () {
              yield* Effect.addFinalizer(() => Ref.update(finalized, (count) => count + 1));
              if (shouldFail) return yield* SearchError.make({ reason: "unavailable" });

              return catalogue.map((entry) => entry.id);
            }),
        });

        const exit = yield* invoke(definition, [native(Search, "web")], { query: "pages" }).pipe(
          Effect.exit,
        );

        if (shouldFail) {
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) expect(Cause.hasFails(exit.cause)).toBe(true);
        } else {
          expect(Exit.isSuccess(exit)).toBe(true);
        }
      }
      expect(yield* Ref.get(finalized)).toBe(2);

      const failure = yield* invoke(
        ToolDiscovery.make({
          failure: SearchError,
          search: () => SearchError.make({ reason: "unavailable" }),
        }),
        [],
        { query: "pages" },
      ).pipe(Effect.flip);

      expect(failure).toEqual(SearchError.make({ reason: "unavailable" }));
    }),
  );

  it.effect("retains defects and finalizes interrupted and timed-out searches", () =>
    Effect.gen(function* () {
      const finalized = yield* Ref.make(0);
      const started = yield* Queue.unbounded<void>();

      const waiting = ToolDiscovery.make({
        search: () =>
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() => Ref.update(finalized, (count) => count + 1));
            yield* Queue.offer(started, undefined);

            return yield* Effect.never;
          }),
      });

      const fiber = yield* invoke(waiting, [], { query: "pages" }).pipe(Effect.forkChild);

      yield* Queue.take(started);
      yield* Fiber.interrupt(fiber);

      const timed = yield* invoke(waiting, [], { query: "pages" }).pipe(
        Effect.timeout("1 second"),
        Effect.forkChild,
      );

      yield* Queue.take(started);
      yield* TestClock.adjust("1 second");
      expect(Exit.isFailure(yield* Fiber.await(timed))).toBe(true);
      expect(yield* Ref.get(finalized)).toBe(2);

      const defect = yield* invoke(ToolDiscovery.make({ search: () => Effect.die("broken") }), [], {
        query: "pages",
      }).pipe(Effect.exit);

      expect(Exit.isFailure(defect)).toBe(true);
      if (Exit.isFailure(defect)) expect(Cause.hasDies(defect.cause)).toBe(true);
    }),
  );
});

class SearchError extends Schema.TaggedError<SearchError>()("SearchError", {
  reason: Schema.String,
}) {}
class SearchIndex extends Context.Service<SearchIndex, string>()(
  "tool-discovery-test/SearchIndex",
) {}

const typed = ToolDiscovery.make({
  failure: SearchError,
  search: (_request, catalogue) =>
    Effect.gen(function* () {
      yield* SearchIndex;
      yield* Effect.addFinalizer(() => Effect.void);
      if (catalogue.length === 0) return yield* SearchError.make({ reason: "empty" });

      return catalogue.map((entry) => entry.id);
    }),
});

it("retains search errors and Layer requirements without capturing the engine catalogue", () => {
  expectTypeOf<Layer.Services<typeof typed.handlers>>().toEqualTypeOf<SearchIndex>();
  expectTypeOf<
    Extract<Tool.HandlerError<typeof typed.tool>, SearchError>
  >().toEqualTypeOf<SearchError>();
  expectTypeOf<Tool.HandlerServices<typeof typed.tool>>().toEqualTypeOf<CurrentToolCatalog>();
});

const evidence = {
  provider: "test",
  model: "relevance",
  usage: { inputTokens: 40, outputTokens: 2 },
};

describe("decision-model discovery", () => {
  it.effect("ranks with a custom relevance prompt and rubric within the namespace", () =>
    Effect.gen(function* () {
      let observed = 0;

      const definition = ToolDiscovery.fromDecisionModel({
        prompt: "Would this tool find the billing evidence requested by the query?",
        criteria: { true: "Finds the requested billing evidence", false: "Unrelated capability" },
        minimumRelevance: 0.8,
        maxResults: 1,
        onEvaluation: (result) =>
          Effect.sync(() => {
            expect(result).toEqual(evidence);
            observed++;
          }),
      });

      const Archive = Tool.make("archived_statements", {
        description: "Historical billing documents",
        success: Schema.String,
      });

      const model = yield* DecisionModel.make({
        evaluate: (request) =>
          Effect.sync(() => {
            expect(request.state).toEqual({
              query: "uncover recent charges",
              namespace: "records",
            });
            expect(Object.keys(request.questions)).toEqual(["candidate_0", "candidate_1"]);
            expect(JSON.stringify(request.questions)).not.toContain("search_pages");
            expect(JSON.stringify(request.questions)).not.toContain("discover_tools");
            expect(request.questions.candidate_1).toEqual({
              type: "probability",
              instructions: {
                question: "Would this tool find the billing evidence requested by the query?",
                tool: {
                  name: "query_records",
                  description: "Query stored account records",
                  namespace: "records",
                  method: "",
                },
              },
              criteria: {
                true: "Finds the requested billing evidence",
                false: "Unrelated capability",
              },
            });

            return {
              ...evidence,
              answers: {
                candidate_0: { type: "probability", probability: 0.2 },
                candidate_1: { type: "probability", probability: 0.95 },
              },
            };
          }),
      });

      const result = yield* invoke(
        definition,
        [
          native(Query, "records"),
          native(Search, "web"),
          native(Archive, "records"),
          native(definition.tool, "records"),
        ],
        { query: "uncover recent charges", namespace: "records" },
      ).pipe(Effect.provideService(DecisionModel.DecisionModel, model));

      expect(result.toolNames).toEqual(["query_records"]);
      expect(result.matches[0]?.parameters).toMatchObject({
        properties: { account: { type: "string" } },
      });
      expect(observed).toBe(1);
    }),
  );

  it.effect("breaks ties by catalogue ID and activates an aliased native owner once", () =>
    Effect.gen(function* () {
      const code = CodeMode.make("execute", {
        description: "Execute research tools",
        includeDeclarations: false,
        tools: { web: { search: Search, lookup: Search } },
      });

      const aliases = Context.get(code.tool.annotations, AdditionalToolCatalog).map((entry) => ({
        ...entry,
        kind: "code-mode" as const,
        nativeToolName: "execute",
      }));

      const model = yield* DecisionModel.make({
        evaluate: () =>
          Effect.succeed({
            ...evidence,
            answers: {
              candidate_0: { type: "probability", probability: 0.9 },
              candidate_1: { type: "probability", probability: 0.9 },
              candidate_2: { type: "probability", probability: 0.9 },
            },
          }),
      });

      const result = yield* invoke(
        ToolDiscovery.fromDecisionModel({ minimumRelevance: 0.8, maxResults: 2 }),
        [native(Search, "web"), ...aliases],
        { query: "investigate a subject" },
      ).pipe(Effect.provideService(DecisionModel.DecisionModel, model));

      expect(result.matches.map((match) => match.id)).toEqual([
        "code-mode:execute:web.lookup",
        "code-mode:execute:web.search",
      ]);
      expect(result.toolNames).toEqual(["execute"]);
    }),
  );

  it.effect("returns no matches below the cutoff and skips evaluation for an empty catalogue", () =>
    Effect.gen(function* () {
      let evaluations = 0;
      const definition = ToolDiscovery.fromDecisionModel({ minimumRelevance: 0.8 });

      const model = yield* DecisionModel.make({
        evaluate: () =>
          Effect.sync(() => {
            evaluations++;

            return {
              ...evidence,
              answers: { candidate_0: { type: "probability", probability: 0.1 } },
            };
          }),
      });

      for (const entries of [[native(Search)], [native(definition.tool)], []]) {
        const result = yield* invoke(definition, entries, { query: "pages" }).pipe(
          Effect.provideService(DecisionModel.DecisionModel, model),
        );

        expect(result).toEqual({ toolNames: [], matches: [] });
      }
      expect(evaluations).toBe(1);
    }),
  );

  it.effect("refuses oversized catalogues before paid evaluation", () =>
    Effect.gen(function* () {
      let evaluations = 0;

      const model = yield* DecisionModel.make({
        evaluate: () =>
          Effect.sync(() => {
            evaluations++;

            return {};
          }),
      });

      for (const limits of [{ maxCandidates: 1 }, { maxCatalogueBytes: 1 }]) {
        const failure = yield* invoke(
          ToolDiscovery.fromDecisionModel({ minimumRelevance: 0.8, ...limits }),
          [native(Search), native(Query)],
          { query: "pages" },
        ).pipe(Effect.provideService(DecisionModel.DecisionModel, model), Effect.flip);

        expect(failure).toMatchObject({ _tag: "AiError", reason: { _tag: "InvalidRequestError" } });
      }
      expect(evaluations).toBe(0);
    }),
  );

  it("validates semantic bounds during construction", () => {
    for (const minimumRelevance of [-0.1, 1.1, Number.NaN])
      expect(() => ToolDiscovery.fromDecisionModel({ minimumRelevance })).toThrow(
        /minimumRelevance/,
      );
    for (const maxCandidates of [0, 1025])
      expect(() =>
        ToolDiscovery.fromDecisionModel({ minimumRelevance: 0.5, maxCandidates }),
      ).toThrow(/maxCandidates/);
    for (const maxCatalogueBytes of [0, 1_048_577])
      expect(() =>
        ToolDiscovery.fromDecisionModel({ minimumRelevance: 0.5, maxCatalogueBytes }),
      ).toThrow(/maxCatalogueBytes/);
  });

  it.effect(
    "preserves provider/observer failures, defects and cancellation while closing evaluator resources",
    () =>
      Effect.gen(function* () {
        const finalized = yield* Ref.make(0);
        const started = yield* Queue.unbounded<void>();

        const providerError = AiError.AiError.make({
          module: "test",
          method: "evaluate",
          reason: AiError.InvalidRequestError.make({ description: "offline" }),
        });

        const observerError = SearchError.make({ reason: "audit unavailable" });

        for (const outcome of [
          "success",
          "provider",
          "observer",
          "defect",
          "timeout",
          "interrupt",
        ] as const) {
          const model = yield* DecisionModel.make({
            evaluate: () =>
              Effect.gen(function* () {
                yield* Effect.addFinalizer(() => Ref.update(finalized, (count) => count + 1));
                yield* Queue.offer(started, undefined);
                if (outcome === "provider") return yield* providerError;
                if (outcome === "defect") return yield* Effect.die("broken provider");
                if (outcome === "timeout" || outcome === "interrupt") return yield* Effect.never;

                return {
                  ...evidence,
                  answers: { candidate_0: { type: "probability", probability: 0.9 } },
                };
              }),
          });

          const definition = ToolDiscovery.fromDecisionModel({
            minimumRelevance: 0.8,
            failure: SearchError,
            onEvaluation: () => (outcome === "observer" ? Effect.fail(observerError) : Effect.void),
          });

          const operation = invoke(definition, [native(Search)], { query: "investigate" }).pipe(
            Effect.provideService(DecisionModel.DecisionModel, model),
          );

          const fiber = yield* (
            outcome === "timeout" ? operation.pipe(Effect.timeout("1 second")) : operation
          ).pipe(Effect.forkChild);

          yield* Queue.take(started);
          if (outcome === "timeout") yield* TestClock.adjust("1 second");
          if (outcome === "interrupt") yield* Fiber.interrupt(fiber);
          const exit = yield* Fiber.await(fiber);

          if (outcome === "success") expect(Exit.isSuccess(exit)).toBe(true);
          else {
            expect(Exit.isFailure(exit)).toBe(true);
            if (Exit.isFailure(exit)) {
              if (outcome === "defect") expect(Cause.hasDies(exit.cause)).toBe(true);
              if (outcome === "interrupt") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
              if (outcome === "provider") expect(Cause.squash(exit.cause)).toBe(providerError);
              if (outcome === "observer") expect(Cause.squash(exit.cause)).toBe(observerError);
            }
          }
        }
        expect(yield* Ref.get(finalized)).toBe(6);
      }),
  );
});

const semanticTyped = ToolDiscovery.fromDecisionModel({
  prompt: { task: "Find the tools required by the discovery query." },
  criteria: { true: "Required", false: "Unrelated" },
  minimumRelevance: 0.5,
  failure: SearchError,
  onEvaluation: () =>
    Effect.gen(function* () {
      yield* SearchIndex;
      yield* Effect.addFinalizer(() => Effect.void);

      return yield* SearchError.make({ reason: "audit" });
    }),
});

it("tracks the decision provider and observer requirements in the handler Layer", () => {
  expectTypeOf<Layer.Services<typeof semanticTyped.handlers>>().toEqualTypeOf<
    DecisionModel.DecisionModel | SearchIndex
  >();
  expectTypeOf<Tool.HandlerError<typeof semanticTyped.tool>>().toEqualTypeOf<
    AiError.AiError | SearchError | ToolDiscovery.ToolDiscoveryError
  >();
  expectTypeOf<
    Tool.HandlerServices<typeof semanticTyped.tool>
  >().toEqualTypeOf<CurrentToolCatalog>();
});
