import * as WebSearch from "@effect-agent/capabilities/WebSearch";
import * as Gateway from "@effect-agent/platform-cloudflare/CloudflareAiGateway";
import { AnthropicClient, AnthropicLanguageModel, AnthropicTool } from "@effect/ai-anthropic";
import { OpenAiClient, OpenAiLanguageModel, OpenAiTool } from "@effect/ai-openai";
import { Config, Effect, Layer, Redacted, Schema, Stream } from "effect";
import { LanguageModel, Toolkit, type Model, type Tool } from "effect/unstable/ai";
import type { HttpClientRequest } from "effect/unstable/http";
import { FetchHttpClient, Headers, HttpClient, HttpClientResponse } from "effect/unstable/http";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import { anthropicGatewaySearch, openAiGatewaySearch } from "../src/cloudflare-ai-gateway.ts";

const config = {
  accountId: "account",
  gatewayId: "research",
  apiToken: Redacted.make("gateway-secret"),
};

const answer = "Current information from the web.";
const citation = { url: "https://example.com/news", title: "News" };

const openAiResponse = {
  id: "resp-1",
  object: "response",
  created_at: 1,
  model: "gpt-4.1-mini",
  status: "completed",
  output: [
    {
      type: "web_search_call",
      id: "search-1",
      status: "completed",
      action: { type: "search", query: "news", sources: [] },
    },
    {
      type: "message",
      id: "message-1",
      role: "assistant",
      status: "completed",
      content: [
        {
          type: "output_text",
          text: answer,
          annotations: [{ type: "url_citation", ...citation, start_index: 0, end_index: 7 }],
        },
      ],
    },
  ],
  usage: {
    input_tokens: 10,
    output_tokens: 5,
    total_tokens: 15,
    input_tokens_details: { cached_tokens: 0 },
    output_tokens_details: { reasoning_tokens: 0 },
  },
};

const anthropicResponse = {
  id: "msg-1",
  type: "message",
  role: "assistant",
  model: "claude-haiku-4-5",
  content: [
    {
      type: "server_tool_use",
      id: "srvtoolu_search1",
      name: "web_search",
      input: { query: "news" },
    },
    {
      type: "web_search_tool_result",
      tool_use_id: "srvtoolu_search1",
      content: [
        {
          type: "web_search_result",
          ...citation,
          encrypted_content: "opaque-provider-data",
          page_age: null,
        },
      ],
    },
    { type: "text", text: answer, citations: null },
  ],
  stop_reason: "end_turn",
  stop_sequence: null,
  usage: {
    input_tokens: 10,
    output_tokens: 5,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    cache_creation: null,
    inference_geo: null,
    service_tier: null,
  },
};

const decodeBody = (request: HttpClientRequest.HttpClientRequest) => {
  if (request.body._tag !== "Uint8Array") throw new Error("Expected a JSON request");

  return Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(
    new TextDecoder().decode(request.body.body),
  );
};

const runSearch = Effect.gen(function* () {
  const tools = yield* WebSearch.toolkit;

  return yield* (yield* tools.handle("WebSearch", { query: "latest news" })).pipe(
    Stream.runCollect,
  );
});

describe("Cloudflare AI Gateway with upstream Effect clients", () => {
  it("runs the same WebSearch tool against OpenAI REST and Anthropic provider-native search", async () => {
    expectTypeOf(openAiGatewaySearch(config)).toEqualTypeOf<
      Layer.Layer<Tool.Handler<"WebSearch">, never, HttpClient.HttpClient>
    >();
    expectTypeOf(anthropicGatewaySearch(config)).toEqualTypeOf<
      Layer.Layer<Tool.Handler<"WebSearch">, never, HttpClient.HttpClient>
    >();

    const configuredModel = AnthropicLanguageModel.model("claude-haiku-4-5").pipe(
      Gateway.provide(
        (options) =>
          Layer.unwrap(
            Config.redacted("ANTHROPIC_API_KEY").pipe(
              Effect.map((apiKey) => AnthropicClient.layer({ ...options, apiKey })),
            ),
          ),
        { ...config, provider: "anthropic" },
      ),
    );

    expectTypeOf(configuredModel).toEqualTypeOf<
      Layer.Layer<
        LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName,
        Config.ConfigError,
        HttpClient.HttpClient
      >
    >();
    for (const backend of ["openai", "anthropic"] as const) {
      const requests: Array<HttpClientRequest.HttpClientRequest> = [];

      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          requests.push(request);

          return HttpClientResponse.fromWeb(
            request,
            Response.json(backend === "openai" ? openAiResponse : anthropicResponse),
          );
        }),
      );

      const result = await Effect.runPromise(
        runSearch.pipe(
          Effect.provide(
            backend === "openai" ? openAiGatewaySearch(config) : anthropicGatewaySearch(config),
          ),
          Effect.provideService(HttpClient.HttpClient, http),
        ),
      );

      expect(result[0]).toMatchObject({
        isFailure: false,
        result: { text: answer, sources: [citation], usage: { inputTokens: 10, outputTokens: 5 } },
      });
      expect(JSON.stringify(result)).not.toContain("opaque-provider-data");
      expect(requests).toHaveLength(1);
      const request = requests[0]!;

      expect(request.url).toBe(
        backend === "openai"
          ? "https://api.cloudflare.com/client/v4/accounts/account/ai/v1/responses"
          : "https://gateway.ai.cloudflare.com/v1/account/research/anthropic/v1/messages?beta=true",
      );
      expect(request.headers[backend === "openai" ? "authorization" : "cf-aig-authorization"]).toBe(
        "Bearer gateway-secret",
      );
      expect(request.headers["x-api-key"]).toBeUndefined();
      expect(decodeBody(request)).toMatchObject(
        backend === "openai"
          ? { model: "openai/gpt-4.1-mini", tools: [{ type: "web_search" }] }
          : {
              model: "claude-haiku-4-5",
              tools: [{ type: "web_search_20250305", name: "web_search", max_uses: 3 }],
            },
      );
      expect(JSON.stringify(request.headers)).not.toContain("gateway-secret");
    }
  });

  it("keeps BYOK credentials separate and supports embeddings through the same OpenAI client", async () => {
    const options = Gateway.provider({ ...config, provider: "openai" });
    let captured: HttpClientRequest.HttpClientRequest | undefined;

    const http = HttpClient.make((request) =>
      Effect.sync(() => {
        captured = request;

        return HttpClientResponse.fromWeb(
          request,
          Response.json({
            object: "list",
            model: "text-embedding-3-small",
            data: [{ object: "embedding", index: 0, embedding: [0.2, 0.4] }],
            usage: { prompt_tokens: 2, total_tokens: 2 },
          }),
        );
      }),
    );

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* OpenAiClient.OpenAiClient;

        return yield* client.createEmbedding({
          model: "text-embedding-3-small",
          input: "searchable text",
        });
      }).pipe(
        Effect.provide(OpenAiClient.layer({ ...options, apiKey: Redacted.make("openai-secret") })),
        Effect.provideService(HttpClient.HttpClient, http),
      ),
    );

    expect(result.data[0]?.embedding).toEqual([0.2, 0.4]);
    expect(captured?.url).toBe(
      "https://gateway.ai.cloudflare.com/v1/account/research/openai/embeddings",
    );
    expect(captured?.headers.authorization).toBe("Bearer openai-secret");
    expect(captured?.headers["cf-aig-authorization"]).toBe("Bearer gateway-secret");
    expect(JSON.stringify(captured?.headers)).not.toContain("secret");
  });

  it("uses the correct Anthropic REST prefix without an upstream provider key", async () => {
    const options = Gateway.rest({ ...config, protocol: "messages" });
    let captured: HttpClientRequest.HttpClientRequest | undefined;

    const http = HttpClient.make((request) =>
      Effect.sync(() => {
        captured = request;

        return HttpClientResponse.fromWeb(request, Response.json(anthropicResponse));
      }),
    );

    const result = await Effect.runPromise(
      LanguageModel.generateText({
        prompt: "news",
        toolkit: Toolkit.make(AnthropicTool.WebSearch_20250305({})),
        toolChoice: "required",
      }).pipe(
        Effect.provide(
          AnthropicLanguageModel.model("anthropic/claude-haiku-4.5").pipe(
            Layer.provide(AnthropicClient.layer(options)),
          ),
        ),
        Effect.provideService(HttpClient.HttpClient, http),
      ),
    );

    expect(result.text).toBe(answer);
    expect(captured?.url).toBe(
      "https://api.cloudflare.com/client/v4/accounts/account/ai/v1/messages?beta=true",
    );
    expect(captured?.headers.authorization).toBe("Bearer gateway-secret");
    expect(captured?.headers["cf-aig-gateway-id"]).toBe("research");
    expect(captured?.headers["x-api-key"]).toBeUndefined();
  });

  it("preserves upstream HTTP errors without exposing Gateway tokens", async () => {
    const http = HttpClient.make((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          Response.json(
            { error: { message: "Rate limited", type: "rate_limit_error" } },
            { status: 429 },
          ),
        ),
      ),
    );

    for (const options of [
      Gateway.provider({ ...config, provider: "openai" }),
      Gateway.rest({ ...config, protocol: "responses" }),
    ]) {
      const error = await Effect.runPromise(
        LanguageModel.generateText({ prompt: "news" }).pipe(
          Effect.provide(
            OpenAiLanguageModel.model("gpt-4.1-mini").pipe(
              Layer.provide(OpenAiClient.layer(options)),
            ),
          ),
          Effect.provideService(HttpClient.HttpClient, http),
          Effect.provideService(Headers.CurrentRedactedNames, []),
          Effect.flip,
        ),
      );

      expect(error._tag).toBe("AiError");
      expect(error.reason._tag).toBe("RateLimitError");
      expect(JSON.stringify(error)).not.toContain("gateway-secret");
    }
  });

  it("leaves native hosted tools and streaming available to the primary model", async () => {
    const events = [
      { type: "response.created", response: { ...openAiResponse, output: [] } },
      {
        type: "response.output_text.delta",
        item_id: "message-1",
        output_index: 0,
        content_index: 0,
        delta: answer,
        logprobs: [],
      },
      { type: "response.completed", response: openAiResponse },
    ];

    let captured: HttpClientRequest.HttpClientRequest | undefined;

    const http = HttpClient.make((request) =>
      Effect.sync(() => {
        captured = request;

        return HttpClientResponse.fromWeb(
          request,
          new Response(
            events
              .map(
                (event, sequence_number) =>
                  `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
              )
              .join(""),
            { headers: { "content-type": "text/event-stream" } },
          ),
        );
      }),
    );

    const parts = await Effect.runPromise(
      LanguageModel.streamText({
        prompt: "news",
        toolkit: Toolkit.make(OpenAiTool.WebSearch({})),
      }).pipe(
        Stream.runCollect,
        Effect.provide(
          OpenAiLanguageModel.model("gpt-4.1-mini").pipe(
            Gateway.provide(OpenAiClient.layer, { ...config, provider: "openai" }),
          ),
        ),
        Effect.provideService(HttpClient.HttpClient, http),
      ),
    );

    expect(parts.some((part) => part.type === "text-delta" && part.delta === answer)).toBe(true);
    expect(decodeBody(captured!)).toMatchObject({ stream: true, tools: [{ type: "web_search" }] });
  });

  it("fails closed on endpoint escapes and disables fetch redirects while retaining host options", async () => {
    const options = Gateway.provider({ ...config, provider: "parallel" });
    let calls = 0;

    const http = HttpClient.make((request) =>
      Effect.sync(() => {
        calls++;

        return HttpClientResponse.fromWeb(request, Response.json({}));
      }),
    );

    for (const url of [
      "https://example.com/",
      `${options.apiUrl}/../../../another/parallel/search`,
      `${options.apiUrl}-other/search`,
    ]) {
      const exit = await Effect.runPromise(
        options.transformClient(http).get(url).pipe(Effect.exit),
      );

      expect(exit._tag).toBe("Failure");
    }
    expect(calls).toBe(0);
    let init: RequestInit | undefined;

    await Effect.runPromise(
      Effect.gen(function* () {
        const base = yield* HttpClient.HttpClient;

        yield* options.transformClient(base).post(`${options.apiUrl}/v1beta/search`);
      }).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.RequestInit, {
          cache: "no-store",
          redirect: "follow",
        }),
        Effect.provideService(FetchHttpClient.Fetch, async (_url, options) => {
          init = options;

          return Response.json({});
        }),
      ),
    );
    expect(init).toMatchObject({ redirect: "error", cache: "no-store" });
    expect(() => Gateway.provider({ ...config, provider: "../openai" })).toThrow(/RegExp/);
  });
});
