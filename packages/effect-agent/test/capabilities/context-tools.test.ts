import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
import { ContextWindow, ContextWindowStatus } from "effect-agent/context-window";
import { ThreadId, RunId } from "effect-agent/identifiers";
import { LanguageModel, Toolkit } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import * as ContextTools from "../../src/capabilities/ContextTools.ts";
import * as MemoryNotes from "../../src/capabilities/MemoryNotes.ts";

const status = ContextWindowStatus.make({
  threadId: ThreadId.make("current-thread"),
  runId: RunId.make("current-run"),
  windowId: "initial",
  estimatedTokens: 40,
  contextTokenLimit: 100,
  remainingTokens: 60,
});

describe("context window tools", () => {
  // https://linear.app/reve-ai/issue/KOM-125 — native OpenAI encoding rejected empty Struct parameters.
  it.effect("sends native no-argument context and notes tools through OpenAI preparation", () =>
    Effect.gen(function* () {
      const toolkit = Toolkit.make(ContextTools.GetContextRemaining, MemoryNotes.ReadNotes);

      const expectedParameters = {
        type: "object",
        properties: {},
        additionalProperties: false,
      };

      let requests = 0;

      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          requests++;
          expect(request.body._tag).toBe("Uint8Array");
          if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request body");

          const body = Schema.decodeSync(
            Schema.fromJsonString(
              Schema.Struct({
                tools: Schema.Array(
                  Schema.Struct({
                    type: Schema.Literal("function"),
                    name: Schema.String,
                    parameters: Schema.Json,
                  }),
                ),
              }),
            ),
          )(new globalThis.TextDecoder().decode(request.body.body));

          expect(body.tools.map(({ name, parameters }) => ({ name, parameters }))).toEqual([
            { name: "get_context_remaining", parameters: expectedParameters },
            { name: "read_notes", parameters: expectedParameters },
          ]);

          return HttpClientResponse.fromWeb(
            request,
            new globalThis.Response(
              JSON.stringify({
                id: "response-context-tools",
                object: "response",
                created_at: 0,
                model: "gpt-6-sol",
                output: [
                  {
                    type: "function_call",
                    id: "call-item-context",
                    call_id: "call-context",
                    name: "get_context_remaining",
                    arguments: "{}",
                  },
                  {
                    type: "function_call",
                    id: "call-item-notes",
                    call_id: "call-notes",
                    name: "read_notes",
                    arguments: "{}",
                  },
                ],
              }),
              { headers: { "content-type": "application/json" } },
            ),
          );
        }),
      );

      const openai = yield* OpenAiClient.make({ apiUrl: "https://provider.invalid/v1" }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      );

      const result = yield* Effect.gen(function* () {
        const tools = yield* toolkit;

        return yield* LanguageModel.generateText({
          prompt: "Read context and notes.",
          toolkit: tools,
        });
      }).pipe(
        Effect.provide(
          Layer.merge(
            toolkit.toLayer({
              get_context_remaining: () => Effect.succeed(status),
              read_notes: () =>
                Effect.succeed(MemoryNotes.NotesSnapshot.make({ revision: null, text: "" })),
            }),
            OpenAiLanguageModel.model("gpt-6-sol"),
          ),
        ),
        Effect.provideService(OpenAiClient.OpenAiClient, openai),
        Effect.provideService(ContextWindow, { status: Effect.succeed(status) }),
      );

      expect(requests).toBe(1);
      expect(result.toolResults).toMatchObject([
        { name: "get_context_remaining", isFailure: false, result: status },
        { name: "read_notes", isFailure: false, result: { revision: null, text: "" } },
      ]);
    }),
  );
});
