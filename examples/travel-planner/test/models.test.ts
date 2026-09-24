import { OpenAiClient, OpenAiLanguageModel, OpenAiTool } from "@effect/ai-openai";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Redacted, Schema, Stream } from "effect";
import { LanguageModel, Tool, Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { expect } from "vite-plus/test";

import { defaultPlannerSettings } from "../src/domain.ts";
import { credentialSourceLayer } from "../src/server/credentials.ts";
import { credentialClient, liveModel } from "../src/server/models.ts";
import { PlannerAttempt, ProgressStore } from "../src/server/progress.ts";
import { PublicOutputLive } from "../src/server/public-output.ts";

const answer = (model: string) => {
  const item = {
    type: "message",
    id: "answer",
    role: "assistant",
    status: "completed",
    content: [{ type: "output_text", text: "A useful answer.", annotations: [] }],
  };

  const response = { id: "response", object: "response", model, created_at: 0, output: [item] };

  const events = [
    { type: "response.created", response: { ...response, output: [] } },
    {
      type: "response.output_item.added",
      output_index: 0,
      item: { ...item, status: "in_progress", content: [] },
    },
    {
      type: "response.output_text.delta",
      item_id: "answer",
      output_index: 0,
      content_index: 0,
      delta: "A useful answer.",
    },
    { type: "response.output_item.done", output_index: 0, item },
    { type: "response.completed", response },
  ];

  return new Response(
    events
      .map((event, sequence_number) => `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`)
      .join(""),
    { headers: { "content-type": "text/event-stream" } },
  );
};

it.effect(
  "keeps streamed native search citations without requesting the incompatible source inventory",
  () =>
    Effect.gen(function* () {
      const SearchRequest = Schema.Struct({
        model: Schema.String,
        include: Schema.Array(Schema.String),
        stream: Schema.optionalKey(Schema.Boolean),
        tools: Schema.Array(Schema.Struct({ type: Schema.String })),
      });

      const requests: Array<typeof SearchRequest.Type> = [];
      const text = "Check the forecast before running.";

      const citation = {
        type: "url_citation",
        url: "https://www.weather.gov/",
        title: "National Weather Service",
        start_index: 0,
        end_index: text.length,
      };

      const action = { type: "search", query: "Mill Valley trail weather" };

      const fetch: typeof globalThis.fetch = async (_url, init) => {
        const request = Schema.decodeSync(Schema.fromJsonString(SearchRequest))(
          await new Response(init?.body).text(),
        );

        requests.push(request);

        const search = {
          type: "web_search_call",
          id: "search-weather",
          status: "completed",
          action: {
            ...action,
            // OpenAI includes the full inventory only when requested, including live feeds.
            ...(request.include.includes("web_search_call.action.sources")
              ? {
                  sources: [
                    { type: "url", url: citation.url },
                    { type: "api", name: "oai-weather" },
                  ],
                }
              : {}),
          },
        };

        const message = {
          type: "message",
          id: "answer-weather",
          role: "assistant",
          status: "completed",
          content: [{ type: "output_text", text, annotations: [citation] }],
        };

        const response = {
          id: "response-weather",
          object: "response",
          model: request.model,
          created_at: 0,
          output: [search, message],
        };

        const events = [
          { type: "response.created", response: { ...response, output: [] } },
          {
            type: "response.output_item.added",
            output_index: 0,
            item: { ...search, status: "in_progress" },
          },
          { type: "response.output_item.done", output_index: 0, item: search },
          {
            type: "response.output_item.added",
            output_index: 1,
            item: { ...message, status: "in_progress", content: [] },
          },
          {
            type: "response.output_text.delta",
            item_id: message.id,
            output_index: 1,
            content_index: 0,
            delta: text,
          },
          {
            type: "response.output_text.annotation.added",
            item_id: message.id,
            output_index: 1,
            content_index: 0,
            annotation_index: 0,
            annotation: citation,
          },
          { type: "response.output_item.done", output_index: 1, item: message },
          { type: "response.completed", response },
        ];

        return new Response(
          events
            .map(
              (event, sequence_number) =>
                `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
            )
            .join(""),
          { headers: { "content-type": "text/event-stream" } },
        );
      };

      const client = credentialClient(Effect.succeed(Redacted.make("fake-api-key")));

      const model = OpenAiLanguageModel.model("gpt-6-luna", {
        store: false,
      }).pipe(
        Layer.provide(Layer.effect(OpenAiClient.OpenAiClient, client)),
        Layer.provide(FetchHttpClient.layer),
      );

      const parts = yield* Effect.gen(function* () {
        const options = {
          prompt: "Check trail conditions near Mill Valley.",
          toolkit: Toolkit.make(OpenAiTool.WebSearch({ search_context_size: "low" })),
        };

        return yield* LanguageModel.streamText(options).pipe(Stream.runCollect);
      }).pipe(Effect.provide(model), Effect.provideService(FetchHttpClient.Fetch, fetch));

      expect(requests).toHaveLength(1);
      expect(requests[0]?.include ?? []).not.toContain("web_search_call.action.sources");
      expect(parts.filter((part) => part.type === "tool-call")).toMatchObject([
        { name: "OpenAiWebSearch", params: { action }, providerExecuted: true },
      ]);
      expect(parts.filter((part) => part.type === "tool-result")).toMatchObject([
        { name: "OpenAiWebSearch", result: { action, status: "completed" }, isFailure: false },
      ]);
      expect(parts.filter((part) => part.type === "source")).toMatchObject([
        { sourceType: "url", url: new URL(citation.url), title: citation.title },
      ]);
      expect(
        parts.flatMap((part) => (part.type === "text-delta" ? [part.delta] : [])).join(""),
      ).toBe(text);
    }),
);

it.effect("streams only deliver-response message arguments through the real SDK SSE decoder", () =>
  Effect.gen(function* () {
    const store = yield* ProgressStore;
    const progress = yield* store.begin("submission", "attempt");
    const writes: string[] = [];
    const frames: string[] = [];

    const chunks = [
      '{"content":{"notes":"CARD_SECRET"},"message":"A ',
      '\\"quiet\\" stay\\',
      "nTahoe ",
      "\\uD83",
      "D\\uDE80",
      '."}',
    ];

    const responseArguments = chunks.join("");

    const item = {
      type: "function_call",
      id: "response-item",
      call_id: "response-call",
      name: "deliver_response",
      arguments: responseArguments,
      status: "completed",
    };

    const events = [
      {
        type: "response.output_item.added",
        output_index: 0,
        item: {
          type: "function_call",
          id: "save-item",
          call_id: "save-call",
          name: "save_trip",
          arguments: "",
          status: "in_progress",
        },
      },
      {
        type: "response.function_call_arguments.delta",
        output_index: 0,
        item_id: "save-item",
        delta: '{"message":"TOOL_SECRET","notes":["NOTE_SECRET"]}',
      },
      {
        type: "response.reasoning_summary_text.delta",
        item_id: "reasoning",
        output_index: 1,
        summary_index: 0,
        delta: "REASONING_SECRET",
      },
      {
        type: "response.output_item.added",
        output_index: 2,
        item: { ...item, arguments: "", status: "in_progress" },
      },
      ...chunks.map((delta) => ({
        type: "response.function_call_arguments.delta",
        output_index: 2,
        item_id: "response-item",
        delta,
      })),
      {
        type: "response.function_call_arguments.delta",
        output_index: 0,
        item_id: "save-item",
        delta: '{"message":"UNRELATED_TOOL_SECRET"}',
      },
      {
        type: "response.output_item.added",
        output_index: 3,
        item: {
          ...item,
          id: "duplicate-item",
          call_id: "duplicate-call",
          arguments: "",
          status: "in_progress",
        },
      },
      {
        type: "response.function_call_arguments.delta",
        output_index: 3,
        item_id: "duplicate-item",
        delta: '{"message":"DUPLICATE_SECRET","content":null}',
      },
      {
        type: "response.function_call_arguments.done",
        output_index: 2,
        item_id: "response-item",
        arguments: responseArguments,
        name: "deliver_response",
      },
      { type: "response.output_item.done", output_index: 2, item },
      {
        type: "response.completed",
        response: {
          id: "response",
          object: "response",
          model: "gpt-6-luna",
          created_at: 0,
          output: [item],
        },
      },
    ].map((event, sequence_number) => ({ ...event, sequence_number }));

    const fetch: typeof globalThis.fetch = async () =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            const encoder = new TextEncoder();

            for (const event of events) {
              const packet = encoder.encode(`data: ${JSON.stringify(event)}\n\n`);

              // Split transport chunks independently of argument boundaries.
              controller.enqueue(packet.slice(0, 13));
              controller.enqueue(packet.slice(13));
            }
            controller.close();
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      );

    const client = yield* OpenAiClient.make({ apiKey: Redacted.make("fake-api-key") }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
    );

    const native = yield* LanguageModel.LanguageModel.pipe(
      Effect.provide(
        OpenAiLanguageModel.model("gpt-6-luna").pipe(
          Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, client)),
        ),
      ),
    );

    const observed = yield* LanguageModel.LanguageModel.pipe(
      Effect.provide(PublicOutputLive),
      Effect.provideService(LanguageModel.LanguageModel, native),
      Effect.provideService(PlannerAttempt, {
        billingOwner: Effect.succeed("fixture"),
        settings: Effect.succeed(defaultPlannerSettings),
        progress: {
          ...progress,
          text: (delta) =>
            progress.text(delta).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  writes.push(delta);
                }),
              ),
            ),
        },
      }),
    );

    const responseTools = Toolkit.make(
      Tool.make("deliver_response", {
        parameters: Schema.Struct({ message: Schema.String, content: Schema.Unknown }),
        success: Schema.Void,
      }),
    );

    const stream = observed.streamText({
      prompt: "Research a stay",
      disableToolCallResolution: true,
      toolkit: responseTools,
    });

    yield* Stream.runCollect(
      stream.pipe(
        Stream.tap(() =>
          store.read.pipe(
            Effect.map((frame) => {
              frames.push(frame.text);
            }),
          ),
        ),
      ),
    ).pipe(Effect.provide(responseTools.toLayer({ deliver_response: () => Effect.void })));
    expect(writes.join("")).toBe('A "quiet" stay\nTahoe 🚀.');
    expect(frames).toContain("A ");
    expect(frames.at(-1)).toBe('A "quiet" stay\nTahoe 🚀.');
    expect(JSON.stringify(frames)).not.toContain("SECRET");
  }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect(
  "bills the verified account, observes key rotation/removal between calls, and never falls back to a host key",
  () =>
    Effect.gen(function* () {
      const encryption = new Uint8Array(32);

      const key = yield* Effect.promise(() =>
        crypto.subtle.importKey("raw", encryption, "AES-GCM", false, ["encrypt"]),
      );

      const seal = (owner: string, secret: string) =>
        Effect.gen(function* () {
          const iv = crypto.getRandomValues(new Uint8Array(12));

          const encrypted = yield* Effect.promise(() =>
            crypto.subtle.encrypt(
              {
                name: "AES-GCM",
                iv,
                additionalData: new TextEncoder().encode(`travel-planner:openai:v1:${owner}`),
              },
              key,
              new TextEncoder().encode(secret),
            ),
          );

          return JSON.stringify({
            version: 1,
            iv: btoa(String.fromCharCode(...iv)),
            ciphertext: btoa(String.fromCharCode(...new Uint8Array(encrypted))),
            lastFour: secret.slice(-4),
            updatedAt: "2026-09-10T00:00:00Z",
          });
        });

      const alice = "alice-account";
      const bob = "bob-account";

      const records = new Map([
        [alice, yield* seal(alice, "sk-private-alice-1111")],
        [bob, yield* seal(bob, "sk-private-bob-2222")],
      ]);

      const credentials = credentialSourceLayer({
        BYOK_ENCRYPTION_KEY: btoa(String.fromCharCode(...encryption)),
        ACCOUNT_THREADS: {
          getByName: (owner) => ({
            demoAccessAllowed: async () => false,
            modelCredential: async () => {
              return records.get(owner) ?? "null";
            },
          }),
        },
      });

      const live = yield* liveModel;
      const requests: string[] = [];

      const fetch: typeof globalThis.fetch = async (_url, init) => {
        requests.push(new Headers(init?.headers).get("authorization") ?? "");

        return answer("gpt-6-luna");
      };

      const store = yield* ProgressStore;
      const progress = yield* store.begin("byok-submission", "byok-attempt");
      const ask = Stream.runDrain(LanguageModel.streamText({ prompt: "A fixture request" }));

      yield* Effect.gen(function* () {
        yield* ask;
        records.set(alice, yield* seal(alice, "sk-private-rotated-3333"));
        yield* ask;
        records.delete(alice);
        const failure = yield* Effect.flip(ask);

        expect(failure.message).toContain("Connect your OpenAI API key");
      }).pipe(
        Effect.provide(
          live.selectable.pipe(
            Layer.provide(credentials),
            Layer.provide(
              Layer.succeed(PlannerAttempt, {
                billingOwner: Effect.succeed(alice),
                settings: Effect.succeed(defaultPlannerSettings),
                progress,
              }),
            ),
          ),
        ),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
      );
      yield* ask.pipe(
        Effect.provide(
          live.selectable.pipe(
            Layer.provide(credentials),
            Layer.provide(
              Layer.succeed(PlannerAttempt, {
                billingOwner: Effect.succeed(bob),
                settings: Effect.succeed(defaultPlannerSettings),
                progress,
              }),
            ),
          ),
        ),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
      );
      expect(requests).toEqual([
        "Bearer sk-private-alice-1111",
        "Bearer sk-private-rotated-3333",
        "Bearer sk-private-bob-2222",
      ]);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ProgressStore.layer,
          ConfigProvider.layer(
            ConfigProvider.fromEnvRecord({ OPENAI_API_KEY: "sk-must-never-use-host-key" }),
          ),
        ),
      ),
    ),
);
