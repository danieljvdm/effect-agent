import { Agent, AgentPolicy, ThreadId, IdGenerator, RunId, TurnId } from "@effect-agent/core";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Prompt, Toolkit, type Response } from "effect/unstable/ai";

import { AgentRuntime, ContextCompactor, type CompactionDecision } from "../src/index.ts";
import { ThreadHistory } from "../src/thread-history.ts";

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("input-prompt-thread")),
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

class InputPromptFailure extends Schema.TaggedError<InputPromptFailure>()(
  "InputPromptFailure",
  {},
) {}

const testLayer = Layer.merge(identifiers, ThreadHistory.layerTransient);

layer(testLayer)("Agent input prompts", (it) => {
  it.effect("projects decoded native content before context preparation and summary requests", () =>
    Effect.gen(function* () {
      const order: Array<string> = [];
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
              order.push("instructions");
              return "Answer the public question.";
            }),
          inputPrompt: (input) =>
            Effect.map(InputPromptService, ({ prefix }) => {
              order.push("input");
              expect(input.count).toBe(2);
              expect(input.hostOnly).toBe(sentinel);
              expect(`${prefix}${input.question}:${input.count}`).toBe("public question:2");
              return projected;
            }),
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({ ...policy, contextTokenLimit: 5_000 }),
        }),
        captureModel((prompt) => {
          order.push(requests.length === 0 ? "summary" : "model");
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
                order.push("compact");
                expect(JSON.stringify(request.source)).not.toContain(sentinel);
                expect(JSON.stringify(request.source)).toContain("context-added");
              }).pipe(
                Effect.andThen(request.summarize(request.source)),
                Effect.map(
                  (summary): CompactionDecision => ({ kind: "summarize", through: 1, summary }),
                ),
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
                order.push("context");
                expect(JSON.stringify(source)).not.toContain(sentinel);
                expect(source.content.slice(1)).toEqual([
                  Prompt.systemMessage({ content: "Answer the public question." }),
                  ...projected.content,
                ]);
                return { prompt: Prompt.concat(source, Prompt.make("context-added")) };
              }),
          },
        },
      ).pipe(
        Effect.provideService(InputPromptService, { prefix: "public " }),
        Effect.provide(compactor),
      );
      expect(result.output).toBe("done");
      expect(order).toEqual(["instructions", "input", "context", "compact", "summary", "model"]);
      expect(requests).toHaveLength(2);
    }),
  );

  it.effect("preserves default JSON rendering and permits an empty input prompt", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];
      const model = captureModel((prompt) => {
        requests.push(prompt);
        return '"done"';
      });
      const options = {
        input: Schema.Struct({ value: Schema.NumberFromString }),
        output: Schema.String,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy,
      };
      yield* AgentRuntime.run(Agent.withModel(Agent.make("default-input", options), model), {
        value: "2",
      });
      yield* AgentRuntime.run(
        Agent.withModel(Agent.make("empty-input", { ...options, inputPrompt: () => [] }), model),
        { value: "2" },
      );
      expect(requests[0]?.content).toContainEqual(
        Prompt.userMessage({ content: [Prompt.textPart({ text: '{"value":"2"}' })] }),
      );
      expect(requests[1]?.content.filter((message) => message.role === "user")).toEqual([]);
    }),
  );

  it.effect.each(["failure", "defect", "timeout", "interruption"] as const)(
    "does not call the model and releases projection resources on %s",
    (mode) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const finalized = yield* Ref.make(false);
        const requests: Array<Prompt.Prompt> = [];
        const defect = new Error("input projection defect");
        const agent = Agent.withModel(
          Agent.make(`input-${mode}`, {
            input: Schema.String,
            output: Schema.String,
            instructions: "Answer.",
            inputPrompt: () =>
              Effect.gen(function* () {
                yield* Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
                  Ref.set(finalized, true),
                );
                if (mode === "failure") return yield* new InputPromptFailure();
                if (mode === "defect") return yield* Effect.die(defect);
                return yield* Effect.never;
              }),
            toolkit: Toolkit.empty,
            policy,
          }),
          captureModel((prompt) => {
            requests.push(prompt);
            return '"unreachable"';
          }),
        );
        const fiber = yield* AgentRuntime.run(agent, "HOST-ONLY-FAILURE-SENTINEL").pipe(
          Effect.scoped,
          Effect.forkChild,
        );
        yield* Deferred.await(entered);
        if (mode === "timeout") yield* TestClock.adjust("5 seconds");
        if (mode === "interruption") yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        if (mode === "failure" || mode === "timeout") {
          const failure = Cause.findErrorOption(exit.cause);
          expect(Option.isSome(failure)).toBe(true);
          if (Option.isSome(failure)) {
            expect(failure.value).toMatchObject(
              mode === "failure"
                ? { _tag: "InputPromptFailure" }
                : { _tag: "AgentPolicyError", limit: "duration" },
            );
          }
        } else if (mode === "defect") {
          expect(Cause.squash(exit.cause)).toBe(defect);
        } else {
          expect(Exit.hasInterrupts(exit)).toBe(true);
        }
        expect(yield* Ref.get(finalized)).toBe(true);
        expect(requests).toEqual([]);
      }),
  );
});
