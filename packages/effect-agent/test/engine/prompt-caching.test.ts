import { AnthropicClient, AnthropicLanguageModel } from "@effect/ai-anthropic";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { Agent, AgentRuntime, InMemory, ThreadHistory } from "effect-agent";
import { ContextCompactor } from "effect-agent/context-compactor";
import { Prompt, ResponseIdTracker, Tool, Toolkit } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse, HttpServerResponse } from "effect/unstable/http";

const Request = Schema.Struct({
  input: Schema.Array(Schema.Json),
  tools: Schema.optionalKey(Schema.Array(Schema.Json)),
  previous_response_id: Schema.optionalKey(Schema.String),
});

type Request = typeof Request.Type;

const isSystem = Schema.is(
  Schema.Struct({
    role: Schema.Literals(["system", "developer"]),
    content: Schema.Array(
      Schema.Struct({
        type: Schema.Literal("input_text"),
        text: Schema.String,
        prompt_cache_breakpoint: Schema.optionalKey(Schema.Json),
      }),
    ),
  }),
);

const systemText = (request: Request) =>
  request.input.filter(isSystem).flatMap((message) => message.content.map((part) => part.text));

// Substitute only HTTP: prompt construction, provider serialization, streaming decoding,
// tool execution and history all run through their production implementations.
const captureOpenAi = Effect.fn(function* (callTool: (call: number) => boolean = () => false) {
  const requests: Array<Request> = [];

  const client = HttpClient.make((request) =>
    Effect.gen(function* () {
      if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request body");
      requests.push(
        yield* Schema.decodeEffect(Schema.fromJsonString(Request))(
          new TextDecoder().decode(request.body.body),
        ).pipe(Effect.orDie),
      );
      const call = requests.length;
      const tool = callTool(call);

      const item = tool
        ? {
            type: "function_call",
            id: `item-${call}`,
            call_id: `call-${call}`,
            name: "lookup",
            arguments: '{"query":"question"}',
            status: "completed",
          }
        : {
            type: "message",
            id: `item-${call}`,
            role: "assistant",
            status: "completed",
            content: [{ type: "output_text", text: '"done"', annotations: [] }],
          };

      const response = {
        id: `response-${call}`,
        object: "response",
        model: "gpt-5.6",
        created_at: 0,
        output: [item],
      };

      const events = [
        { type: "response.created", response: { ...response, output: [] } },
        {
          type: "response.output_item.added",
          output_index: 0,
          item: tool
            ? { ...item, arguments: "", status: "in_progress" }
            : { ...item, status: "in_progress", content: [] },
        },
        ...(tool
          ? []
          : [
              {
                type: "response.output_text.delta",
                item_id: item.id,
                output_index: 0,
                content_index: 0,
                delta: '"done"',
              },
            ]),
        { type: "response.output_item.done", output_index: 0, item },
        { type: "response.completed", response },
      ];

      return HttpClientResponse.fromWeb(
        request,
        HttpServerResponse.toWeb(
          HttpServerResponse.text(
            events
              .map(
                (event, sequence_number) =>
                  `event: ${event.type}\ndata: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
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

  return {
    requests,
    model: OpenAiLanguageModel.model("gpt-5.6").pipe(
      Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, provider)),
    ),
  };
});

const instructions = Prompt.fromMessages([
  Prompt.systemMessage({
    content: "Answer the user's question.",
    options: { openai: { promptCacheBreakpoint: { mode: "explicit" } } },
  }),
]);

const policy = { maxTurns: 4, maxToolCalls: 3, maxDuration: "10 seconds" } as const;

const lookup = Toolkit.make(
  Tool.make("lookup", {
    parameters: Schema.Struct({ query: Schema.String }),
    success: Schema.String,
  }),
);

// #651: successful answers and reported usage do not prove a reusable prompt prefix.
// These assertions compare real wire prefixes, without simulating the provider cache.
describe("prompt caching: https://github.com/danieljvdm/effect-agent/issues/651", () => {
  it.effect.each(["retained", "prepared", "conversation-only"] as const)(
    "preserves user-ending prefixes within the context limit across three runs with %s history",
    (historyMode) =>
      Effect.gen(function* () {
        const { model, requests } = yield* captureOpenAi();
        const instructionText = "Answer the user's question. ".repeat(120);

        const agent = Agent.withModel(
          Agent.make("cache-runs", {
            input: Schema.String,
            output: Schema.String,
            instructions: Prompt.fromMessages([
              Prompt.systemMessage({
                content: instructionText,
                options: { openai: { promptCacheBreakpoint: { mode: "explicit" } } },
              }),
            ]),
            toolkit: Toolkit.empty,
            policy: { ...policy, contextTokenLimit: 2_000 },
          }),
          model,
        );

        const context =
          historyMode === "prepared"
            ? {
                prepare: ({ source }: { readonly source: Prompt.Prompt }) =>
                  Effect.succeed({ prompt: source }),
              }
            : undefined;

        const first = yield* AgentRuntime.run(agent, "First question", { context });
        const history = yield* ThreadHistory.ThreadHistory;
        let stored = yield* history.load(first.threadId);

        for (const question of ["Second question", "Third question"]) {
          const result = yield* AgentRuntime.run(
            agent,
            question,
            historyMode !== "conversation-only"
              ? { threadId: first.threadId, context }
              : {
                  history: Prompt.fromMessages(
                    stored.content.filter((message) => message.role !== "system"),
                  ),
                },
          );

          stored = yield* history.load(result.threadId);
        }
        expect(requests).toHaveLength(3);
        for (const request of requests) {
          expect(request.input[0]).toEqual({
            role: "developer",
            content: [
              {
                type: "input_text",
                text: instructionText,
                prompt_cache_breakpoint: { mode: "explicit" },
              },
            ],
          });
          expect(systemText(request)).toEqual([
            instructionText,
            expect.stringContaining("Final output contract:"),
          ]);
        }
        for (let index = 1; index < requests.length; index++) {
          const previous = requests[index - 1]!.input;

          expect(requests[index]!.input.slice(0, previous.length)).toEqual(previous);
        }
        expect(stored.content.filter((message) => message.role === "user")).toHaveLength(3);
        expect(stored.content.filter((message) => message.role === "assistant")).toHaveLength(3);
        // Projection must not rewrite persisted instructions or add derived contracts to history.
        expect(stored.content.filter((message) => message.role === "system")).toHaveLength(
          historyMode === "conversation-only" ? 1 : 3,
        );
        expect(JSON.stringify(stored)).not.toContain("Final output contract:");
      }).pipe(Effect.provide(Layer.merge(InMemory.layer, ContextCompactor.layerRollover))),
  );

  it.effect("preserves tool-result prefixes and tool schemas across consecutive tool rounds", () =>
    Effect.gen(function* () {
      const { model, requests } = yield* captureOpenAi((call) => call < 3);
      let executions = 0;

      const agent = Agent.withModel(
        Agent.make("cache-tools", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Look up the answer.",
          toolkit: lookup,
          policy,
        }),
        model,
      );

      const result = yield* AgentRuntime.run(agent, "Question").pipe(
        Effect.provide(
          lookup.toLayer({
            lookup: () => Effect.sync(() => `result-${++executions}`),
          }),
        ),
      );

      expect(result.output).toBe("done");
      expect(executions).toBe(2);
      expect(requests).toHaveLength(3);
      for (let index = 1; index < requests.length; index++) {
        const previous = requests[index - 1]!;

        expect(requests[index]!.input.slice(0, previous.input.length)).toEqual(previous.input);
        expect(requests[index]!.tools).toEqual(previous.tools);
        expect(requests[index]!.input.at(-1)).toMatchObject({
          type: "function_call_output",
          call_id: `call-${index}`,
          output: `result-${index}`,
        });
      }
    }).pipe(Effect.provide(InMemory.layer)),
  );

  it.effect.each(["user", "tool"] as const)(
    "keeps the implicit %s cache boundary before appended run status",
    (boundary) =>
      Effect.gen(function* () {
        const { model, requests } = yield* captureOpenAi(
          (call) => boundary === "tool" && call === 1,
        );

        const agent = Agent.withModel(
          Agent.make("cache-run-status", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Answer using the available evidence.",
            toolkit: lookup,
            policy: { ...policy, runStatus: "appended", contextTokenLimit: 2_000 },
          }),
          model,
        );

        const first = yield* AgentRuntime.run(agent, "First question").pipe(
          Effect.provide(lookup.toLayer({ lookup: () => Effect.succeed("Durable evidence") })),
        );

        yield* AgentRuntime.run(agent, "Follow-up question", { threadId: first.threadId }).pipe(
          Effect.provide(lookup.toLayer({ lookup: () => Effect.succeed("Durable evidence") })),
        );
        const previous = requests[boundary === "tool" ? 1 : 0]!;
        const durableEnd = previous.input.at(-2);

        expect(previous.input.at(-1)).toMatchObject({
          role: "developer",
          content: [{ type: "input_text", text: expect.stringContaining("<run-status>") }],
        });
        if (boundary === "tool") {
          expect(durableEnd).toMatchObject({
            type: "function_call_output",
            call_id: "call-1",
            output: "Durable evidence",
          });
        } else {
          expect(durableEnd).toMatchObject({
            role: "user",
            content: [{ type: "input_text", text: '"First question"' }],
          });
        }
        for (let index = 1; index < requests.length; index++) {
          const prefix = requests[index - 1]!.input.slice(0, -1);

          expect(requests[index]!.input.slice(0, prefix.length)).toEqual(prefix);
        }
        const history = yield* ThreadHistory.ThreadHistory;
        const stored = yield* history.load(first.threadId);

        expect(JSON.stringify(stored)).not.toContain("<run-status>");
        expect(JSON.stringify(stored)).not.toContain("promptCacheBreakpoint");
      }).pipe(Effect.provide(InMemory.layer)),
  );

  it.effect.each(["none", "status", "references", "prepared"] as const)(
    "reuses native response IDs only without discarded context: %s",
    (transient) =>
      Effect.gen(function* () {
        const { model, requests } = yield* captureOpenAi((call) => call === 1);

        const agent = Agent.withModel(
          Agent.make("cache-response-id", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Answer from evidence.",
            toolkit: lookup,
            policy: { ...policy, runStatus: transient === "status" ? "appended" : "off" },
          }),
          model,
        );

        yield* AgentRuntime.run(agent, "Original question", {
          context:
            transient === "prepared"
              ? {
                  prepare: ({ source }) =>
                    Effect.succeed({
                      prompt: Prompt.concat(source, Prompt.make("Ephemeral reference")),
                    }),
                }
              : undefined,
          transientContext:
            transient === "references"
              ? { load: () => Effect.succeed("Ephemeral reference") }
              : undefined,
        }).pipe(Effect.provide(lookup.toLayer({ lookup: () => Effect.succeed("Evidence") })));
        expect(requests).toHaveLength(2);
        if (transient === "none") {
          expect(requests[1]!.previous_response_id).toBe("response-1");
        } else {
          // A previous response retains its discarded suffix on the provider.
          // Local omission alone cannot remove that suffix from a continuation.
          expect(requests[1]!.previous_response_id).toBeUndefined();
          expect(JSON.stringify(requests[1]!.input)).toContain("Original question");
        }
      }).pipe(
        Effect.provideServiceEffect(ResponseIdTracker.ResponseIdTracker, ResponseIdTracker.make),
        Effect.provide(InMemory.layer),
      ),
  );

  it.effect(
    "preserves distinct instructions, latest precedence, native options and source history",
    () =>
      Effect.gen(function* () {
        const { model, requests } = yield* captureOpenAi();

        const history = Prompt.fromMessages([
          Prompt.systemMessage({ content: "Repeat" }),
          Prompt.userMessage({ content: [Prompt.textPart({ text: "Old question" })] }),
          Prompt.systemMessage({ content: "Different" }),
          Prompt.assistantMessage({ content: [Prompt.textPart({ text: "Old answer" })] }),
        ]);

        const agent = Agent.withModel(
          Agent.make("cache-options", {
            input: Schema.String,
            output: Schema.String,
            instructions: Prompt.fromMessages([
              Prompt.systemMessage({ content: "Repeat" }),
              Prompt.systemMessage({
                content: "Repeat",
                options: { openai: { promptCacheBreakpoint: { mode: "explicit" } } },
              }),
            ]),
            toolkit: Toolkit.empty,
            policy,
          }),
          model,
        );

        const result = yield* AgentRuntime.run(agent, "New question", {
          history,
          context: {
            prepare: ({ source }) =>
              Effect.succeed({
                prompt: Prompt.concat(
                  Prompt.fromMessages([Prompt.systemMessage({ content: "Application policy" })]),
                  source,
                ),
              }),
          },
        });

        const request = requests[0]!;

        expect(systemText(request)).toEqual([
          "Application policy",
          "Different",
          "Repeat",
          "Repeat",
          expect.stringContaining("Final output contract:"),
        ]);
        expect(request.input.slice(0, 5).every(isSystem)).toBe(true);
        expect(request.input[2]).not.toEqual(request.input[3]);
        expect(request.input[3]).toMatchObject({
          content: [{ prompt_cache_breakpoint: { mode: "explicit" } }],
        });
        expect(request.input.slice(5)).toMatchObject([
          { role: "user", content: [{ text: "Old question" }] },
          { role: "assistant", content: [{ text: "Old answer" }] },
          { role: "user", content: [{ text: '"New question"' }] },
        ]);
        const retained = yield* ThreadHistory.ThreadHistory;
        const stored = yield* retained.load(result.threadId);

        expect(stored.content.slice(0, history.content.length)).toEqual(history.content);
        expect(stored.content.filter((message) => message.role === "system")).toHaveLength(4);
      }).pipe(Effect.provide(InMemory.layer)),
  );

  it.effect("re-evaluates changed instructions without erasing earlier distinct instructions", () =>
    Effect.gen(function* () {
      const { model, requests } = yield* captureOpenAi();

      const agent = Agent.withModel(
        Agent.make("cache-dynamic", {
          input: Schema.String,
          output: Schema.String,
          instructions: (input) => `Answer in ${input}.`,
          toolkit: Toolkit.empty,
          policy,
        }),
        model,
      );

      const first = yield* AgentRuntime.run(agent, "French");

      yield* AgentRuntime.run(agent, "German", { threadId: first.threadId });
      expect(systemText(requests[1]!)).toEqual([
        "Answer in French.",
        "Answer in German.",
        expect.stringContaining("Final output contract:"),
      ]);
      expect(requests[1]!.input.slice(0, 3).every(isSystem)).toBe(true);
      expect(requests[1]!.input.slice(0, requests[0]!.input.length)).not.toEqual(
        requests[0]!.input,
      );
    }).pipe(Effect.provide(InMemory.layer)),
  );

  it.effect("starts a reusable prefix after compaction without moving canonical coverage", () =>
    Effect.gen(function* () {
      const { model, requests } = yield* captureOpenAi((call) => call === 1);

      const agent = Agent.withModel(
        Agent.make("cache-compaction", {
          input: Schema.String,
          output: Schema.String,
          instructions,
          toolkit: lookup,
          policy: { ...policy, contextTokenLimit: 2_000 },
        }),
        model,
      );

      const result = yield* AgentRuntime.run(agent, "Current question", {
        history: Prompt.fromMessages([
          Prompt.userMessage({ content: [Prompt.textPart({ text: "Previous task" })] }),
          Prompt.assistantMessage({
            content: [Prompt.textPart({ text: "OLD-CONTEXT ".repeat(5_000) })],
          }),
        ]),
      }).pipe(Effect.provide(lookup.toLayer({ lookup: () => Effect.succeed("fresh evidence") })));

      expect(result.output).toBe("done");
      expect(requests).toHaveLength(2);
      expect(requests[0]!.input.slice(0, 2).every(isSystem)).toBe(true);
      expect(JSON.stringify(requests[0])).not.toContain("OLD-CONTEXT");
      expect(JSON.stringify(requests[0])).toContain("Current question");
      expect(requests[1]!.input.slice(0, requests[0]!.input.length)).toEqual(requests[0]!.input);
      const history = yield* ThreadHistory.ThreadHistory;

      expect(JSON.stringify(yield* history.load(result.threadId))).toContain("OLD-CONTEXT");
    }).pipe(Effect.provide(Layer.merge(InMemory.layer, ContextCompactor.layerRollover))),
  );

  it.effect.each(["off", "appended"] as const)(
    "keeps Anthropic instructions and cache markers together with run status %s",
    (runStatus) =>
      Effect.gen(function* () {
        const Body = Schema.Struct({
          system: Schema.Array(Schema.Json),
          messages: Schema.Array(Schema.Json),
        });

        const requests: Array<typeof Body.Type> = [];

        const client = HttpClient.make((request) =>
          Effect.gen(function* () {
            if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request body");
            requests.push(
              yield* Schema.decodeEffect(Schema.fromJsonString(Body))(
                new TextDecoder().decode(request.body.body),
              ).pipe(Effect.orDie),
            );

            const events = [
              {
                type: "message_start",
                message: {
                  id: "message",
                  type: "message",
                  role: "assistant",
                  model: "claude-sonnet-4-5",
                  content: [],
                  stop_reason: null,
                  stop_sequence: null,
                  usage: {
                    input_tokens: 100,
                    output_tokens: 0,
                    cache_creation: null,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                    inference_geo: null,
                    server_tool_use: null,
                    service_tier: null,
                  },
                },
              },
              { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
              {
                type: "content_block_delta",
                index: 0,
                delta: { type: "text_delta", text: '"done"' },
              },
              { type: "content_block_stop", index: 0 },
              {
                type: "message_delta",
                delta: { stop_reason: "end_turn", stop_sequence: null },
                usage: {
                  output_tokens: 1,
                  input_tokens: 100,
                  cache_creation_input_tokens: 0,
                  cache_read_input_tokens: 0,
                  server_tool_use: null,
                },
              },
              { type: "message_stop" },
            ];

            return HttpClientResponse.fromWeb(
              request,
              HttpServerResponse.toWeb(
                HttpServerResponse.text(
                  events
                    .map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`)
                    .join(""),
                  { contentType: "text/event-stream" },
                ),
              ),
            );
          }),
        );

        const provider = yield* AnthropicClient.make({ apiUrl: "https://provider.invalid" }).pipe(
          Effect.provideService(HttpClient.HttpClient, client),
        );

        const model = AnthropicLanguageModel.model("claude-sonnet-4-5").pipe(
          Layer.provide(Layer.succeed(AnthropicClient.AnthropicClient, provider)),
        );

        const agent = Agent.withModel(
          Agent.make("cache-anthropic", {
            input: Schema.String,
            output: Schema.String,
            toolkit: Toolkit.empty,
            policy: { ...policy, runStatus },
            instructions: Prompt.fromMessages([
              Prompt.systemMessage({
                content: "Author instructions",
                options: { anthropic: { cacheControl: { type: "ephemeral" } } },
              }),
            ]),
          }),
          model,
        );

        const context = {
          prepare: ({ source }: { readonly source: Prompt.Prompt }) =>
            Effect.succeed({
              prompt: Prompt.concat(
                Prompt.fromMessages([Prompt.systemMessage({ content: "Application policy" })]),
                source,
              ),
            }),
        };

        const first = yield* AgentRuntime.run(agent, "First question", { context });

        yield* AgentRuntime.run(agent, "Second question", { context, threadId: first.threadId });
        expect(requests).toHaveLength(2);
        expect(requests[0]!.system).toMatchObject([
          { type: "text", text: "Application policy" },
          { type: "text", text: "Author instructions", cache_control: { type: "ephemeral" } },
          { type: "text", text: expect.stringContaining("Final output contract:") },
        ]);
        expect(requests[1]!.system).toEqual(requests[0]!.system);
        if (runStatus === "appended") {
          // Anthropic combines adjacent user messages into one content array.
          expect(requests[0]!.messages).toMatchObject([
            {
              role: "user",
              content: [
                { type: "text", text: '"First question"' },
                { type: "text", text: expect.stringContaining("<run-status>") },
              ],
            },
          ]);
          expect(requests[1]!.messages[0]).toMatchObject({
            role: "user",
            content: [{ type: "text", text: '"First question"' }],
          });
          expect(requests[1]!.messages.at(-1)).toMatchObject({
            role: "user",
            content: [
              { type: "text", text: '"Second question"' },
              { type: "text", text: expect.stringContaining("<run-status>") },
            ],
          });
        } else {
          expect(requests[1]!.messages.slice(0, requests[0]!.messages.length)).toEqual(
            requests[0]!.messages,
          );
        }
      }).pipe(Effect.provide(InMemory.layer)),
  );
});
