import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { ContextBudgetError } from "effect-agent/agent-error";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { ModelCallContext } from "effect-agent/context-window";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import { type RunEvent } from "effect-agent/run-event";
import { type RunDurabilityHook } from "effect-agent/run-options";
import { ToolResultBounds, TruncatedToolResult } from "effect-agent/tool-result";
import {
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";
import { HttpClient, HttpClientResponse, HttpServerResponse } from "effect/unstable/http";

import { RunContextPreparationPassthrough } from "../../src/engine/RunOptions.ts";
import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => Schema.decodeSync(ThreadId)(`thread-1-${++threadSequence}`)),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-1")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-1")),
});

type FinishUsage = Extract<Response.StreamPartEncoded, { readonly type: "finish" }>["usage"];

const emptyUsage: FinishUsage = { inputTokens: {}, outputTokens: {} };

const emptyPolicyUsage = {
  committedTurns: 0,
  toolCalls: 0,
  programmaticToolCalls: 0,
  consecutiveToolFailures: 0,
  finalizationUsed: false,
};

const emptyResumeUsage = {
  ...emptyPolicyUsage,
  modelCalls: 0,
  inputTokens: 0,
  outputTokens: 0,
  lastInputTokens: 0,
  lastOutputTokens: 0,
  costMicrousd: 0,
};

const finalParts = (
  text: string,
  usage: FinishUsage = emptyUsage,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCallParts = (
  id: string,
  name: string,
  params: Record<string, unknown>,
  usage: FinishUsage = emptyUsage,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "tool-call", id, name, params, providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage },
];

interface CapturedRequest {
  readonly prompt: Prompt.Prompt;
  readonly toolCount: number;
  readonly toolChoice: unknown;
}

/** Scripted multi-call model: one parts script per model request, with request capture. */
const scriptedModel = (script: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>) => {
  const requests: Array<CapturedRequest> = [];

  const model = Model.make(
    "scripted",
    "context-economics",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          const index = Math.min(requests.length, script.length - 1);

          requests.push({
            prompt: request.prompt,
            toolCount: request.tools.length,
            toolChoice: request.toolChoice,
          });

          return Stream.fromIterable(script[index] ?? []);
        },
      }),
    ),
  );

  return { model, requests };
};

const toolResultValues = (prompt: Prompt.Prompt): ReadonlyArray<unknown> =>
  prompt.content.flatMap((message) =>
    typeof message.content === "string"
      ? []
      : message.content.flatMap((part) =>
          part.type === "tool-result" ? [part.result as unknown] : [],
        ),
  );

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Effect to fail");
  }
  const failure = Cause.findErrorOption(exit.cause);

  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed failure in the Cause");
  }

  return failure.value;
};

const EmitTool = Tool.make("emit", {
  parameters: Schema.Struct({}),
  success: Schema.Struct({ data: Schema.String }),
});

const emitToolkit = Toolkit.make(EmitTool);

const answerOutput = Schema.Struct({ answer: Schema.String });

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("context economics — bounding, tracking, status, exhaustion", (it) => {
  // ---------------------------------------------------------------- RUN-022

  it.effect(
    "RUN-022: bounds an oversized application Tool result into the TruncatedToolResult envelope for prompt and events",
    () =>
      Effect.gen(function* () {
        const bigData = "x".repeat(3_000);

        const definition = Agent.make("bounds-oversized", {
          input: Schema.Struct({ question: Schema.String }),
          output: answerOutput,
          instructions: "Use the tool once, then answer.",
          toolkit: emitToolkit,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            toolResultBounds: ToolResultBounds.make({ maxBytes: 1_024 }),
          }),
        });

        const { model, requests } = scriptedModel([
          toolCallParts("emit-1", "emit", {}),
          finalParts('{"answer":"done"}'),
        ]);

        const toolLayer = emitToolkit.toLayer({
          emit: () => Effect.succeed({ data: bigData }),
        });

        const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);

        yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "big",
        }).pipe(
          Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
          Stream.runDrain,
          Effect.provide(toolLayer),
        );

        expect(requests).toHaveLength(2);
        const second = requests[1];

        if (second === undefined) throw new Error("expected a second model request");
        const results = toolResultValues(second.prompt);

        expect(results).toHaveLength(1);
        const envelope = Schema.decodeUnknownSync(TruncatedToolResult)(results[0]);
        const originalEncoded = JSON.stringify({ data: bigData });

        expect(envelope.originalBytes).toBe(originalEncoded.length);
        expect(originalEncoded.startsWith(envelope.head)).toBe(true);
        expect(originalEncoded.endsWith(envelope.tail)).toBe(true);
        expect(JSON.stringify(results[0]).length).toBeLessThanOrEqual(1_024);

        // The success event carries the same bounded value as the prompt.
        const succeeded = (yield* Ref.get(events)).find(
          (event) => event._tag === "ToolCallSucceeded",
        );

        expect(succeeded).toBeDefined();
        if (succeeded === undefined || succeeded._tag !== "ToolCallSucceeded") {
          throw new Error("expected ToolCallSucceeded");
        }
        expect(succeeded.result).toEqual(results[0]);
      }),
  );

  it.effect(
    "reserves grace before provider execution and never grants it again after interruption",
    () =>
      Effect.gen(function* () {
        const definition = Agent.make("resume-grace", {
          input: Schema.String,
          output: answerOutput,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });

        const { model, requests } = scriptedModel([finalParts('{"answer":"never"}')]);
        let reserved = false;

        const durability: RunDurabilityHook = {
          commitResponse: () => Effect.void,
          prepareToolCalls: () => Effect.void,
          commitCompaction: () => Effect.void,
          noteTurnUsage: () => Effect.void,
          step: { lookup: () => Effect.succeed(Option.none()), commit: () => Effect.void },
          reservePolicyUsage: (usage) =>
            Effect.sync(() => {
              reserved = usage.finalizationUsed;
            }).pipe(Effect.andThen(Effect.interrupt)),
        };

        const seed = { ...emptyResumeUsage, committedTurns: 1, modelCalls: 1 };

        const interrupted = yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
          resumeUsage: seed,
          durability,
        }).pipe(Effect.exit);

        expect(Exit.isFailure(interrupted) && Cause.hasInterrupts(interrupted.cause)).toBe(true);
        expect(reserved).toBe(true);

        const replacement = yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
          resumeUsage: { ...seed, finalizationUsed: reserved },
        }).pipe(Effect.exit);

        expect(failureFrom(replacement)).toMatchObject({
          _tag: "AgentPolicyError",
          limit: "turns",
        });
        expect(requests).toHaveLength(0);
      }),
  );

  // Regression: https://github.com/danieljvdm/effect-agent/commit/2259fc0
  // KOM-125: a transformed Class retains provider definitions that Schema.toEncoded removes.
  it.effect("admits original Tool schemas exactly as native OpenAI serializes them", () =>
    Effect.gen(function* () {
      class CredentialLookup extends Schema.Class<CredentialLookup>("CredentialLookupEncoded")({
        bindingId: Schema.String.annotate({
          description: "Reserved credential metadata. ".repeat(200),
        }),
      }) {}

      const lookup = Tool.make("credential_lookup", {
        parameters: CredentialLookup,
        success: Schema.String,
      });

      const toolkit = Toolkit.make(lookup);

      const definition = Agent.make("original-tool-provider-schema", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Return a JSON string.",
        toolkit,
        policy: { maxTurns: 2, maxToolCalls: 2, maxDuration: "30 seconds", runStatus: "off" },
      });

      const run = Effect.fn(function* (capacity: number) {
        const admittedSchemas: Array<unknown> = [];
        const wireSchemas: Array<unknown> = [];

        const client = HttpClient.make((request) =>
          Effect.gen(function* () {
            if (request.body._tag !== "Uint8Array") throw new Error("Expected JSON request body");

            const body = yield* HttpClientResponse.fromWeb(
              request,
              HttpServerResponse.toWeb(HttpServerResponse.uint8Array(request.body.body)),
            ).json.pipe(
              Effect.flatMap(
                Schema.decodeUnknownEffect(
                  Schema.Struct({
                    tools: Schema.Array(
                      Schema.Struct({ name: Schema.String, parameters: Schema.Json }),
                    ),
                  }),
                ),
              ),
              Effect.orDie,
            );

            expect(body.tools.map(({ name }) => name)).toEqual(["credential_lookup"]);
            wireSchemas.push(body.tools[0]?.parameters);

            const message = {
              type: "message",
              id: "message-schema",
              role: "assistant",
              status: "completed",
              content: [{ type: "output_text", text: '"done"', annotations: [] }],
            };

            const response = {
              id: "response-schema",
              object: "response",
              model: "gpt-6-sol",
              created_at: 0,
              output: [message],
            };

            const events = [
              { type: "response.created", response: { ...response, output: [] } },
              {
                type: "response.output_item.added",
                output_index: 0,
                item: { ...message, status: "in_progress", content: [] },
              },
              {
                type: "response.output_text.delta",
                item_id: message.id,
                output_index: 0,
                content_index: 0,
                delta: '"done"',
              },
              { type: "response.output_item.done", output_index: 0, item: message },
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

        const model = OpenAiLanguageModel.model("gpt-6-sol").pipe(
          Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, provider)),
        );

        const transformer: LanguageModel.CodecTransformer = (schema) => {
          const transformed = toCodecOpenAI(schema);

          admittedSchemas.push(transformed.jsonSchema);

          return transformed;
        };

        const exit = yield* AgentRuntime.run(Agent.withModel(definition, model), "input", {
          context: {
            prepare: (request) =>
              Effect.succeed({
                prompt: request.source,
                modelCall: {
                  model,
                  toolSchemaTransformer: transformer,
                  context: ModelCallContext.make({
                    contextCapacity: capacity,
                    outputReserveTokens: 100,
                    uncountedOverheadTokens: 0,
                  }),
                },
              }),
          },
        }).pipe(
          Effect.provide(toolkit.toLayer({ credential_lookup: () => Effect.succeed("unused") })),
          Effect.exit,
        );

        return { exit, admittedSchemas, wireSchemas };
      });

      const fitting = yield* run(20_000);

      expect(Exit.isSuccess(fitting.exit)).toBe(true);
      expect(fitting.wireSchemas).toHaveLength(1);
      expect(fitting.admittedSchemas).toEqual(fitting.wireSchemas);
      expect(fitting.wireSchemas[0]).toEqual(toCodecOpenAI(CredentialLookup).jsonSchema);
      const tooSmall = yield* run(2_400);

      expect(failureFrom(tooSmall.exit)).toBeInstanceOf(ContextBudgetError);
      expect(tooSmall.wireSchemas).toHaveLength(0);
    }),
  );
});
