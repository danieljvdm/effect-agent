import type { Layer } from "effect";
import { Context, Effect, Schema, SchemaGetter, Stream } from "effect";
import * as WebSearch from "effect-agent/web-search";
import type { AiError, LanguageModel } from "effect/unstable/ai";
import { Tool } from "effect/unstable/ai";
import type { expectTypeOf as ExpectTypeOf } from "vite-plus/test";

const hostedSearch = Tool.providerDefined({
  id: "test.web_search",
  customName: "HostedSearch",
  providerName: "web_search",
  args: Tool.EmptyParams,
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
})({});

const invoke = (options: Partial<Omit<WebSearch.Options, "tool">> = {}) =>
  Effect.gen(function* () {
    const tools = yield* WebSearch.toolkit;

    return yield* (yield* tools.handle("WebSearch", { query: "news" }, "call-1")).pipe(
      Stream.runCollect,
    );
  }).pipe(Effect.provide(WebSearch.layer({ tool: hostedSearch, ...options })));

export const verifySearchProviderAndEncoderRequirements = () => {
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

  class SearchEncoding extends Context.Service<SearchEncoding, string>()("test/SearchEncoding") {}

  const encodedSearch = Tool.providerDefined({
    id: "test.web_search",
    customName: "EncodedSearch",
    providerName: "web_search",
    args: Tool.EmptyParams,
    parameters: Schema.Struct({
      query: Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.passthrough(),
          encode: SchemaGetter.transformEffect((query) => Effect.as(SearchEncoding, query)),
        }),
      ),
    }),
    success: Schema.Struct({ status: Schema.String }),
  })({});

  expectTypeOf(WebSearch.layer({ tool: encodedSearch })).toEqualTypeOf<
    Layer.Layer<Tool.Handler<"WebSearch">, never, LanguageModel.LanguageModel | SearchEncoding>
  >();
};

declare const expectTypeOf: typeof ExpectTypeOf;
