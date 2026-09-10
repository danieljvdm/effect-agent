import * as WebSearch from "@effect-agent/capabilities/WebSearch";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { AiError, LanguageModel, Tool, type Response } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

const hostedSearch = Tool.providerDefined({
  id: "test.web_search",
  customName: "HostedSearch",
  providerName: "web_search",
  args: Tool.EmptyParams,
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
})({});

const searchParts = (
  text = "An answer",
  url = "https://example.com/source",
): Array<Response.PartEncoded> => [
  {
    type: "tool-call",
    id: "search-1",
    name: "HostedSearch",
    params: { query: "news" },
    providerExecuted: true,
  },
  {
    type: "tool-result",
    id: "search-1",
    name: "HostedSearch",
    result: { status: "completed" },
    isFailure: false,
    providerExecuted: true,
  },
  { type: "text", text },
  { type: "source", sourceType: "url", id: "source-1", url, title: "Example" },
];

const modelLayer = (generate: Effect.Effect<Array<Response.PartEncoded>, AiError.AiError>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => generate,
      streamText: () => Stream.empty,
    }),
  );

const invoke = (options: Partial<Omit<WebSearch.Options, "tool">> = {}) =>
  Effect.gen(function* () {
    const tools = yield* WebSearch.toolkit;

    return yield* (yield* tools.handle("WebSearch", { query: "news" }, "call-1")).pipe(
      Stream.runCollect,
    );
  }).pipe(Effect.provide(WebSearch.layer({ tool: hostedSearch, ...options })));

describe("WebSearch", () => {
  it.effect(
    "keeps the search model requirement visible and returns bounded, provider-neutral evidence",
    () =>
      Effect.gen(function* () {
        expectTypeOf(WebSearch.layer({ tool: hostedSearch })).toEqualTypeOf<
          Layer.Layer<Tool.Handler<"WebSearch">, never, LanguageModel.LanguageModel>
        >();
        expectTypeOf(invoke()).toEqualTypeOf<
          Effect.Effect<
            Array<Tool.HandlerResult<typeof WebSearch.tool>>,
            AiError.AiError,
            LanguageModel.LanguageModel
          >
        >();

        const results = yield* invoke().pipe(
          Effect.provide(modelLayer(Effect.succeed(searchParts()))),
        );

        expect(results).toHaveLength(1);
        expect(results[0]).toMatchObject({
          isFailure: false,
          result: {
            text: "An answer",
            sources: [{ url: "https://example.com/source", title: "Example" }],
            usage: { inputTokens: null, outputTokens: null },
          },
        });
        expect(JSON.stringify(results)).not.toContain("HostedSearch");
      }),
  );

  it.effect(
    "returns typed failures for missing search, failed search, unsafe sources, and UTF-8 overflow",
    () =>
      Effect.gen(function* () {
        const cases: ReadonlyArray<{
          parts: Array<Response.PartEncoded>;
          reason: WebSearch.Failure["reason"];
          maxOutputBytes?: number;
        }> = [
          { parts: [{ type: "text", text: "I did not search" }], reason: "search-not-executed" },
          {
            parts: [
              {
                type: "tool-result",
                id: "search-1",
                name: "HostedSearch",
                result: { status: "failed" },
                isFailure: false,
              },
            ],
            reason: "provider",
          },
          {
            parts: searchParts("answer", "https://secret:password@example.com/"),
            reason: "invalid-response",
          },
          { parts: searchParts("🌍".repeat(100)), reason: "output-limit", maxOutputBytes: 350 },
          { parts: searchParts(""), reason: "invalid-response" },
        ];

        for (const test of cases) {
          const results = yield* invoke(
            test.maxOutputBytes === undefined ? {} : { maxOutputBytes: test.maxOutputBytes },
          ).pipe(Effect.provide(modelLayer(Effect.succeed(test.parts))));

          expect(results[0]).toMatchObject({
            isFailure: true,
            result: { _tag: "WebSearchFailure", reason: test.reason },
          });
        }
      }),
  );

  it.effect("keeps upstream diagnostics out of tool failures and preserves defects", () =>
    Effect.gen(function* () {
      const failure = AiError.make({
        module: "test",
        method: "search",
        reason: new AiError.InvalidOutputError({ description: "secret credential" }),
      });

      const results = yield* invoke().pipe(Effect.provide(modelLayer(Effect.fail(failure))));

      expect(results[0]).toMatchObject({ isFailure: true, result: { reason: "provider" } });
      expect(JSON.stringify(results)).not.toContain("secret credential");

      const exit = yield* invoke().pipe(
        Effect.provide(modelLayer(Effect.die("search defect"))),
        Effect.exit,
      );

      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
    }),
  );

  it.effect("interrupts timed-out requests and runs request finalizers without retrying", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const finalized = yield* Ref.make(0);

      const never = Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Ref.update(finalized, (n) => n + 1)),
      );

      const fiber = yield* invoke({ timeoutMillis: 100 }).pipe(
        Effect.provide(modelLayer(never)),
        Effect.forkChild,
      );

      yield* Deferred.await(started);
      yield* TestClock.adjust(100);
      const results = yield* Fiber.join(fiber);

      expect(results[0]).toMatchObject({ isFailure: true, result: { reason: "timeout" } });
      expect(yield* Ref.get(finalized)).toBe(1);
    }),
  );

  it.effect("propagates parent interruption and releases the in-flight search", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const finalized = yield* Ref.make(false);

      const pending = Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Ref.set(finalized, true)),
      );

      const fiber = yield* invoke().pipe(Effect.provide(modelLayer(pending)), Effect.forkChild);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
      expect(yield* Ref.get(finalized)).toBe(true);
    }),
  );
});
