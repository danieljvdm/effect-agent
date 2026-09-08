import { ThreadId, RunId } from "@effect-agent/core/Identifiers";
import {
  ContextHistory,
  ContextHistoryError,
  ContextHistoryHit,
  ContextHistoryPage,
  type ContextHistoryRead,
  type ContextHistorySearch,
} from "@effect-agent/engine/ContextHistory";
import {
  ContextRolloverTool,
  ContextWindow,
  ContextWindowStatus,
} from "@effect-agent/engine/ContextWindow";
import { getToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Tool, Toolkit } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import * as ContextTools from "../src/ContextTools.ts";
import * as MemoryNotes from "../src/MemoryNotes.ts";

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

      for (const tool of Object.values(toolkit.tools)) {
        expect(Schema.decodeUnknownSync(tool.parametersSchema)({})).toEqual({});
        expect(Schema.encodeSync(tool.parametersSchema)({})).toEqual({});
        expect(
          Tool.getJsonSchema(tool.setParameters(Schema.toEncoded(tool.parametersSchema)), {
            transformer: toCodecOpenAI,
          }),
        ).toEqual(expectedParameters);
      }

      let requests = 0;

      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          requests++;
          expect(request.body._tag).toBe("Uint8Array");
          if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request body");

          const body = Schema.decodeUnknownSync(
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
                model: "gpt-5.6-terra",
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
            OpenAiLanguageModel.model("gpt-5.6-terra"),
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

  it.effect("rejects archive requests above the tools' result bounds before reading", () =>
    Effect.gen(function* () {
      const tools = yield* ContextTools.toolkit;

      const searchFailure = yield* tools
        .handle("search_context_windows", { query: "saved", limit: 4 }, "search")
        .pipe(Effect.flip);

      const readFailure = yield* tools
        .handle("read_context_window", { recordId: "evidence", maxChars: 5_001 }, "read")
        .pipe(Effect.flip);

      const cursorFailure = yield* tools
        .handle("search_context_windows", { query: "saved", beforeRecordId: "" }, "cursor")
        .pipe(Effect.flip);

      expect(searchFailure).toMatchObject({ reason: { _tag: "ToolParameterValidationError" } });
      expect(readFailure).toMatchObject({ reason: { _tag: "ToolParameterValidationError" } });
      expect(cursorFailure).toMatchObject({ reason: { _tag: "ToolParameterValidationError" } });
    }).pipe(
      Effect.provide(ContextTools.layer),
      Effect.provideService(ContextWindow, { status: Effect.die("Unexpected status read") }),
      Effect.provideService(ContextHistory, {
        search: () => Effect.die("Unexpected archive search"),
        read: () => Effect.die("Unexpected archive read"),
      }),
    ),
  );

  it.effect("returns a designated rollover request without performing a hidden mutation", () =>
    Effect.gen(function* () {
      const tools = yield* ContextTools.toolkit;

      const results = yield* tools
        .handle("new_context", { handoff: "Continue from the saved notes." }, "rotate")
        .pipe(Effect.flatMap(Stream.runCollect));

      expect(results).toMatchObject([
        { isFailure: false, result: { handoff: "Continue from the saved notes." } },
      ]);
      expect(Context.get(ContextTools.NewContext.annotations, ContextRolloverTool)).toBe(true);
      expect(getToolExecutionClass(ContextTools.NewContext)).toBe("idempotent");
    }).pipe(Effect.provide(ContextTools.layer)),
  );

  it.effect("binds archive queries to the current Thread on every invocation", () =>
    Effect.gen(function* () {
      const current = yield* Ref.make(status);
      const searches: Array<ContextHistorySearch> = [];
      const reads: Array<ContextHistoryRead> = [];
      const tools = yield* ContextTools.toolkit;

      const archive = ContextHistory.of({
        search: (request) =>
          Effect.sync(() => {
            searches.push(request);

            return [
              ContextHistoryHit.make({ recordId: "evidence", windowId: "old", text: "saved" }),
            ];
          }),
        read: (request) =>
          Effect.sync(() => {
            reads.push(request);

            return ContextHistoryPage.make({
              recordId: request.recordId,
              windowId: "old",
              text: "saved evidence",
              nextOffset: null,
            });
          }),
      });

      const run = Effect.gen(function* () {
        const untrustedSearch = { query: "saved", threadId: "another-thread" };

        yield* tools
          .handle("search_context_windows", untrustedSearch, "search")
          .pipe(Effect.flatMap(Stream.runCollect));

        yield* Ref.set(
          current,
          ContextWindowStatus.make({
            ...status,
            threadId: ThreadId.make("next-thread"),
            estimatedTokens: 55,
            remainingTokens: 45,
          }),
        );

        const untrustedContinuation = {
          query: "saved",
          limit: 1,
          beforeRecordId: "evidence",
          threadId: "another-thread",
        };

        yield* tools
          .handle("search_context_windows", untrustedContinuation, "older")
          .pipe(Effect.flatMap(Stream.runCollect));

        const remaining = yield* tools
          .handle("get_context_remaining", {}, "status")
          .pipe(Effect.flatMap(Stream.runCollect));

        const untrustedRead = { recordId: "evidence", threadId: "another-thread" };

        const page = yield* tools
          .handle("read_context_window", untrustedRead, "read")
          .pipe(Effect.flatMap(Stream.runCollect));

        expect(remaining).toMatchObject([{ result: { estimatedTokens: 55, remainingTokens: 45 } }]);
        expect(page).toMatchObject([{ result: { text: "saved evidence", nextOffset: null } }]);
      });

      yield* run.pipe(
        Effect.provideService(ContextWindow, { status: Ref.get(current) }),
        Effect.provideService(ContextHistory, archive),
      );
      expect(searches).toMatchObject([
        { threadId: "current-thread", query: "saved", limit: 3 },
        { threadId: "next-thread", query: "saved", limit: 1, beforeRecordId: "evidence" },
      ]);
      expect(searches[0]?.beforeRecordId).toBeUndefined();
      expect(reads).toMatchObject([
        { threadId: "next-thread", recordId: "evidence", offset: 0, maxChars: 5_000 },
      ]);
    }).pipe(Effect.provide(ContextTools.layer)),
  );

  it.effect("returns an unavailable archive as a typed tool failure", () =>
    Effect.gen(function* () {
      const tools = yield* ContextTools.toolkit;

      const failure = ContextHistoryError.make({
        reason: "unavailable",
        message: "Archive offline",
      });

      const results = yield* tools
        .handle("search_context_windows", { query: "saved" }, "search")
        .pipe(Effect.flatMap(Stream.runCollect));

      expect(results).toMatchObject([{ isFailure: true, result: failure }]);
    }).pipe(
      Effect.provide(ContextTools.layer),
      Effect.provideService(ContextWindow, { status: Effect.succeed(status) }),
      Effect.provideService(ContextHistory, {
        search: () =>
          Effect.fail(
            ContextHistoryError.make({ reason: "unavailable", message: "Archive offline" }),
          ),
        read: () =>
          Effect.fail(
            ContextHistoryError.make({ reason: "unavailable", message: "Archive offline" }),
          ),
      }),
    ),
  );
});
