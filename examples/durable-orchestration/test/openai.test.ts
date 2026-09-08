import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { NodeFileSystem } from "@effect/platform-node";
import { ConfigProvider, Effect, FileSystem, References } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { expect, it } from "vite-plus/test";

import { rootThread } from "../src/agents.ts";
import { nodeDemonstration, nodeHost } from "../src/node.ts";

it.each(["unauthorized", "no-tools"] as const)(
  "uses the OpenAI HTTP boundary and reports %s without persisting the key",
  async (scenario) => {
    const model = scenario === "unauthorized" ? "gpt-4.1-mini" : "gpt-4.1-mini-2025-04-14";
    const apiKey = "sk-fixture-secret-never-persist";
    const requests: Array<{ url: string; authorization: string | null; body: unknown }> = [];

    const fetch: typeof globalThis.fetch = async (input, init) => {
      const request = new Request(input, init);

      requests.push({
        url: request.url,
        authorization: request.headers.get("authorization"),
        body: JSON.parse(await request.text()),
      });
      if (scenario === "unauthorized")
        return new Response(
          JSON.stringify({
            error: {
              message: "Fixture key rejected",
              type: "invalid_request_error",
              code: "invalid_api_key",
              param: null,
            },
          }),
          { status: 401, headers: { "content-type": "application/json" } },
        );

      const response = {
        id: "resp_fixture",
        model: "gpt-4.1-mini-2025-04-14",
        created_at: 1,
        output: [],
      };

      const item = {
        type: "message",
        id: "msg_fixture",
        role: "assistant",
        status: "completed",
        content: [
          { type: "output_text", text: '{"answer":"I did not call any tools."}', annotations: [] },
        ],
      };

      const events = [
        { type: "response.created", response },
        { type: "response.output_item.added", output_index: 0, item: { ...item, content: [] } },
        {
          type: "response.output_text.delta",
          item_id: item.id,
          output_index: 0,
          content_index: 0,
          delta: item.content[0]?.text,
        },
        { type: "response.output_item.done", output_index: 0, item },
        { type: "response.completed", response: { ...response, output: [item] } },
      ];

      return new Response(
        events
          .map(
            (event, sequence_number) =>
              `event: ${event.type}\ndata: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
          )
          .join(""),
        { headers: { "content-type": "text/event-stream" } },
      );
    };

    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "openai-orchestration-" });

        return yield* Effect.gen(function* () {
          const result = yield* nodeDemonstration.pipe(Effect.result);
          const store = yield* ThreadStore;
          const history = yield* store.export(ThreadExportRequest.make({ threadId: rootThread }));

          return { result, history };
        }).pipe(Effect.provide(nodeHost(`${directory}/runtime.sqlite`)));
      }).pipe(
        Effect.scoped,
        Effect.provide(NodeFileSystem.layer),
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromUnknown({
            OPENAI_API_KEY: apiKey,
            OPENAI_MODEL: scenario === "unauthorized" ? undefined : model,
          }),
        ),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
        Effect.provideService(References.MinimumLogLevel, "None"),
      ),
    );

    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      url: "https://api.openai.com/v1/responses",
      authorization: `Bearer ${apiKey}`,
      body: {
        model,
        store: false,
        stream: true,
        tools: expect.arrayContaining([
          expect.objectContaining({ name: "build_a_start" }),
          expect.objectContaining({ name: "build_b_start" }),
        ]),
      },
    });
    expect(outcome.result).toMatchObject({
      _tag: "Failure",
      failure: {
        _tag: "DemonstrationFailed",
        message: expect.stringContaining(
          scenario === "unauthorized" ? "Fixture key rejected" : "did not start both builders",
        ),
      },
    });
    expect(JSON.stringify(outcome.history)).not.toContain(apiKey);
  },
);
