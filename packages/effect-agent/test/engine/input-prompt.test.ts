import { expect, layer } from "@effect/vitest";
import { Context, Effect, Layer, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { ContextCompactor, type CompactionDecision } from "effect-agent/context-compactor";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import { LanguageModel, Model, Prompt, Toolkit, type Response } from "effect/unstable/ai";

import { RunContextPreparationPassthrough } from "../../src/engine/RunOptions.ts";
import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() =>
    Schema.decodeSync(ThreadId)(`input-prompt-thread-${++threadSequence}`),
  ),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("input-prompt-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("input-prompt-turn")),
});

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "5 seconds",
  toolConcurrency: 1,
  runStatus: "off",
});

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const captureModel = (capture: (prompt: Prompt.Prompt) => string) =>
  Model.make(
    "scripted",
    "input-prompt-model",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt }) => Stream.fromIterable(finalParts(capture(prompt))),
      }),
    ),
  );

class InputPromptService extends Context.Service<InputPromptService, { readonly prefix: string }>()(
  "@effect-agent/engine/test/InputPromptService",
) {}

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("Agent input prompts", (it) => {
  it.effect("projects decoded native content before context preparation and summary requests", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];
      const sentinel = "HOST-ONLY-ENGINE-SENTINEL";

      const file = Prompt.filePart({
        mediaType: "image/png",
        data: new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]),
      });

      const projected = Prompt.fromMessages([
        Prompt.userMessage({ content: [Prompt.textPart({ text: "public question:2" }), file] }),
      ]);

      const agent = Agent.withModel(
        Agent.make("native-input-prompt", {
          input: Schema.Struct({
            question: Schema.String,
            count: Schema.NumberFromString,
            hostOnly: Schema.String,
          }),
          output: Schema.String,
          instructions: () =>
            Effect.sync(() => {
              return "Answer the public question.";
            }),
          inputPrompt: (input) =>
            Effect.map(InputPromptService, ({ prefix }) => {
              expect(input.count).toBe(2);
              expect(input.hostOnly).toBe(sentinel);
              expect(`${prefix}${input.question}:${input.count}`).toBe("public question:2");

              return projected;
            }),
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({ ...policy, contextTokenLimit: 5_000 }),
        }),
        captureModel((prompt) => {
          requests.push(prompt);
          expect(JSON.stringify(prompt)).not.toContain(sentinel);
          expect(prompt.content).toContainEqual(projected.content[0]);

          return requests.length === 1 ? "Prior thread summary." : '"done"';
        }),
      );

      const compactor = Layer.succeed(
        ContextCompactor,
        ContextCompactor.of({
          estimate: (messages) => JSON.stringify(messages).length,
          compact: (request) =>
            Stream.fromEffect(
              Effect.sync(() => {
                expect(JSON.stringify(request.source)).not.toContain(sentinel);
                expect(JSON.stringify(request.source)).toContain("context-added");
              }).pipe(
                Effect.andThen(request.summarize(request.source)),
                Effect.map((summary): CompactionDecision => ({
                  kind: "summarize",
                  through: 1,
                  summary,
                })),
              ),
            ),
        }),
      );

      const result = yield* AgentRuntime.run(
        agent,
        { question: "question", count: "2", hostOnly: sentinel },
        {
          history: Prompt.make("old history ".repeat(2_000)),
          context: {
            prepare: ({ source }) =>
              Effect.sync(() => {
                expect(JSON.stringify(source)).not.toContain(sentinel);

                return { prompt: Prompt.concat(source, Prompt.make("context-added")) };
              }),
          },
        },
      ).pipe(
        Effect.provideService(InputPromptService, { prefix: "public " }),
        Effect.provide(compactor),
      );

      expect(result.output).toBe("done");
      expect(requests).toHaveLength(2);
    }),
  );
});
