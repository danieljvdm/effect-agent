import * as CodeMode from "@effect-agent/capabilities/CodeMode";
import * as ToolDiscovery from "@effect-agent/capabilities/ToolDiscovery";
import {
  AdditionalToolCatalog,
  DiscoveryTool,
  PinnedTool,
  ToolNamespace,
} from "@effect-agent/core/ToolExposure";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { CurrentToolCatalog, type CatalogEntry } from "@effect-agent/engine/ToolExposure";
import { describe, expect, it } from "@effect/vitest";
import type { Layer } from "effect";
import { Cause, Context, Effect, Encoding, Exit, Fiber, Queue, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Tool, type Toolkit } from "effect/unstable/ai";
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

      const error = yield* invoke(ToolDiscovery.make({ maxResultBytes: bytes - 1 }), entries, {
        query: "read",
      }).pipe(Effect.flip);

      expect(error).toMatchObject({ reason: "limit-exceeded" });
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
