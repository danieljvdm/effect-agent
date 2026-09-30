import { OpenAiClient, OpenAiLanguageModel, OpenAiTool } from "@effect/ai-openai";
import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Schema, Stream } from "effect";
import { Agent, AgentRuntime, InMemory, ThreadHistory, WebSearch } from "effect-agent";
import { compileBindingContracts } from "effect-agent/agent-registration";
import * as ToolDiscovery from "effect-agent/tool-discovery";
import { LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse, HttpServerResponse } from "effect/unstable/http";

const Request = Schema.Struct({
  input: Schema.Array(Schema.Json),
  tools: Schema.Array(Schema.Json),
});

// Requested hosted-search seam: substitute HTTP only; run the real Responses encoder,
// stream decoder, discovery, runtime and prompt history. Live calls cannot force these windows.
it.effect("answers with one hosted search and carries sources through the next call", () =>
  Effect.gen(function* () {
    const requests: Array<typeof Request.Type> = [];
    const text = '"Sunny. Source: https://weather.example/today"';

    const citation = {
      type: "url_citation",
      url: "https://weather.example/today",
      title: "Weather",
      start_index: 1,
      end_index: 6,
    };

    const search = {
      type: "web_search_call",
      id: "ws_1",
      status: "completed",
      action: { type: "search", queries: ["weather today"] },
    };

    const message = {
      type: "message",
      id: "msg_1",
      role: "assistant",
      status: "completed",
      content: [{ type: "output_text", text, annotations: [citation] }],
    };

    const client = HttpClient.make((request) =>
      Effect.gen(function* () {
        if (request.body._tag !== "Uint8Array") return yield* Effect.die("Expected JSON");
        requests.push(
          yield* Schema.decodeEffect(Schema.fromJsonString(Request))(
            new TextDecoder().decode(request.body.body),
          ).pipe(Effect.orDie),
        );
        const index = requests.length;

        const items =
          index === 1
            ? [
                search,
                {
                  ...search,
                  id: "ws_page",
                  action: { type: "open_page", url: "https://weather.example/today" },
                },
                message,
              ]
            : index === 2
              ? [
                  { ...search, id: "ws_2" },
                  { ...message, id: "msg_2" },
                  {
                    type: "function_call",
                    id: "fc_2",
                    call_id: "lookup_2",
                    name: "lookup",
                    arguments: "{}",
                    status: "completed",
                  },
                ]
              : [{ ...message, id: "msg_3" }];

        const response = {
          id: `resp_${index}`,
          object: "response",
          model: "gpt-6.1-sol",
          created_at: 0,
          output: items,
          usage: {
            input_tokens: 10,
            output_tokens: 5,
            total_tokens: 15,
            input_tokens_details: { cached_tokens: 0 },
            output_tokens_details: { reasoning_tokens: 0 },
          },
        };

        const events = [
          { type: "response.created", response: { ...response, output: [] } },
          ...items.flatMap((item, output_index) => [
            {
              type: "response.output_item.added",
              output_index,
              item: { ...item, status: "in_progress" },
            },
            ...(item.type === "message"
              ? [
                  {
                    type: "response.output_text.delta",
                    item_id: item.id,
                    output_index,
                    content_index: 0,
                    delta: text,
                  },
                  {
                    type: "response.output_text.annotation.added",
                    item_id: item.id,
                    output_index,
                    content_index: 0,
                    annotation_index: 0,
                    annotation: citation,
                  },
                ]
              : []),
            { type: "response.output_item.done", output_index, item },
          ]),
          { type: "response.completed", response },
        ];

        return HttpClientResponse.fromWeb(
          request,
          HttpServerResponse.toWeb(
            HttpServerResponse.text(
              events
                .map(
                  (event, sequence_number) =>
                    `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
                )
                .join(""),
              { contentType: "text/event-stream" },
            ),
          ),
        );
      }),
    );

    const provider = yield* OpenAiClient.make({ apiUrl: "https://provider.invalid/v1" }).pipe(
      Effect.provideService(HttpClient.HttpClient, client),
    );

    const model = Model.make(
      "openai",
      "gpt-6.1-sol",
      OpenAiLanguageModel.layer({ model: "gpt-6.1-sol", config: { store: false } }),
    ).pipe(Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, provider)));

    const definition = Agent.make("native-search", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Search and answer as JSON.",
      toolkit: Toolkit.merge(
        WebSearch.native({ tool: OpenAiTool.WebSearch({ search_context_size: "low" }) }),
        Toolkit.make(Tool.make("lookup", { success: Schema.String })),
      ),
      toolExposure: { initialToolNames: ["OpenAiWebSearch", "lookup"], maxSchemaBytes: 1024 },
    });

    const agent = Agent.withModel(definition, model);
    let localCalls = 0;

    const handlers = definition.toolkit.toLayer({
      lookup: () =>
        Effect.sync(() => {
          localCalls++;

          return "confirmed";
        }),
    });

    const first = yield* AgentRuntime.run(agent, "weather today", {
      estimateCostMicrousd: (_usage, request) => {
        expect(request).toMatchObject({ webSearchCalls: requests.length === 1 ? 1 : 0 });

        return Effect.succeed(1);
      },
    }).pipe(Effect.provide(handlers));

    expect(first.output).toContain("weather.example");
    expect(first.usage).toMatchObject({ modelCalls: 1, webSearchCalls: 1 });
    const history = yield* (yield* ThreadHistory.ThreadHistory).load(first.threadId);

    expect(JSON.stringify(history)).toContain("ws_1");
    expect(JSON.stringify(history)).toContain("url_citation");

    const second = yield* AgentRuntime.run(agent, "summarize those sources", {
      threadId: first.threadId,
    }).pipe(Effect.provide(handlers));

    expect(second.usage).toMatchObject({ modelCalls: 2, webSearchCalls: 1 });
    expect(localCalls).toBe(1);
    expect(requests).toHaveLength(3);
    // Upstream store:false omits hosted call/results and re-encodes text annotations.
    expect(JSON.stringify(requests[2]!.input)).not.toContain("web_search_call");
    expect(JSON.stringify(requests[2]!.input)).toContain("url_citation");
  }).pipe(Effect.provide(InMemory.layer)),
);

it.effect("compiles hosted tools without local schemas and pins their configuration", () =>
  Effect.gen(function* () {
    const definition = (size: "low" | "high") =>
      Agent.make("hosted-contract", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Search.",
        toolkit: Toolkit.make(OpenAiTool.WebSearchPreview({ search_context_size: size })),
      });

    const declarations = { agent: "v1", model: "v1", tools: "v1" };
    const low = yield* compileBindingContracts(definition("low"), declarations);
    const high = yield* compileBindingContracts(definition("high"), declarations);

    expect(low.digests.replay?.tools.OpenAiWebSearchPreview).not.toBe(
      high.digests.replay?.tools.OpenAiWebSearchPreview,
    );
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect("discovers and activates a hosted declaration without an application schema", () =>
  Effect.gen(function* () {
    const discovery = ToolDiscovery.make();
    let calls = 0;

    const model = Model.make(
      "scripted",
      "discovery",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) => {
            calls++;
            if (calls === 1)
              return Stream.fromIterable([
                {
                  type: "tool-call",
                  id: "discover",
                  name: "discover_tools",
                  params: { query: "OpenAiWebSearchPreview" },
                  providerExecuted: false,
                },
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: { inputTokens: {}, outputTokens: {} },
                },
              ]);
            expect(request.tools.map((tool) => tool.name)).toContain("OpenAiWebSearchPreview");
            expect(JSON.stringify(request.prompt)).toContain('"providerName":"web_search_preview"');

            return Stream.fromIterable([
              { type: "text-start", id: "answer" },
              { type: "text-delta", id: "answer", delta: '"done"' },
              { type: "text-end", id: "answer" },
              { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
            ]);
          },
        }),
      ),
    );

    const definition = Agent.make("hosted-discovery", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Discover.",
      toolkit: Toolkit.merge(discovery.toolkit, Toolkit.make(OpenAiTool.WebSearchPreview({}))),
      toolExposure: { initialToolNames: [], maxSchemaBytes: 4096 },
    });

    yield* AgentRuntime.run(Agent.withModel(definition, model), "search").pipe(
      Effect.provide(discovery.handlers),
    );
    expect(calls).toBe(2);
  }).pipe(Effect.provide(InMemory.layer)),
);
