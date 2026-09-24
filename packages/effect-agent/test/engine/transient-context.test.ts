import { expect, layer } from "@effect/vitest";
import { Context, Effect, Layer, Ref, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy, CompactionPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { ContextCompactor } from "effect-agent/context-compactor";
import { IdGenerator } from "effect-agent/id-generator";
import { RunId, ThreadId, TurnId } from "effect-agent/identifiers";
import { RunContextPreparation } from "effect-agent/run-options";
import { AiError, LanguageModel, Model, Prompt, type Response, Toolkit } from "effect/unstable/ai";

import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() =>
    Schema.decodeSync(ThreadId)(`transient-thread-${++threadSequence}`),
  ),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("transient-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("transient-turn")),
});

const testLayer = Layer.mergeAll(identifiers, ThreadHistory.layer, ContextCompactor.layer);

const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const makeModel = (requests: Array<Prompt.Prompt>) =>
  Model.make(
    "scripted",
    "transient-context",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          requests.push(request.prompt);

          return Stream.fromIterable(finalParts);
        },
      }),
    ),
  );

const makeAgent = (requests: Array<Prompt.Prompt>, contextTokenLimit?: number) =>
  Agent.withModel(
    Agent.make("transient-context", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Answer the question.",
      toolkit: Toolkit.empty,
      policy: AgentPolicy.make({
        maxTurns: 1,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
        ...(contextTokenLimit === undefined ? {} : { contextTokenLimit }),
      }),
    }),
    makeModel(requests),
  );

class TransientContextFailure extends Schema.TaggedError<TransientContextFailure>()(
  "TransientContextFailure",
  { message: Schema.String },
) {}

class TransientContextDependency extends Context.Service<
  TransientContextDependency,
  { readonly value: string }
>()("@effect-agent/engine/test/TransientContextDependency") {}

layer(testLayer)("transient model context", (it) => {
  it.effect("reuses one transient snapshot for a same-Turn overflow retry", () =>
    Effect.gen(function* () {
      const requests: Array<Prompt.Prompt> = [];

      const model = Model.make(
        "scripted",
        "transient-overflow-retry",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              requests.push(request.prompt);

              return requests.length === 1
                ? Stream.fail(
                    AiError.AiError.make({
                      module: "test",
                      method: "streamText",
                      reason: AiError.UnknownError.make({
                        description: "context_length_exceeded",
                      }),
                    }),
                  )
                : Stream.fromIterable(finalParts);
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("transient-overflow-retry", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer the question.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 100_000,
            compaction: CompactionPolicy.make({ keepRecentTokens: 300 }),
          }),
        }),
        model,
      );

      const loads = yield* Ref.make(0);

      const result = yield* AgentRuntime.run(agent, "question").pipe(
        Effect.provideService(RunContextPreparation, {
          transientContext: {
            load: () =>
              Ref.getAndUpdate(loads, (count) => count + 1).pipe(
                Effect.map((count) => Prompt.make(`snapshot ${count + 1}`)),
              ),
          },
        }),
      );

      expect(result.output).toBe("done");
      expect(yield* Ref.get(loads)).toBe(1);
      expect(requests).toHaveLength(2);
      for (const request of requests) {
        expect(JSON.stringify(request.content)).toContain("snapshot 1");
        expect(JSON.stringify(request.content)).not.toContain("snapshot 2");
      }
    }),
  );
});

export const verifyPreservesAPerRunOverridesTypedErrorAndServiceRequirement = () => {
  const requests: Array<Prompt.Prompt> = [];

  const program = AgentRuntime.run(makeAgent(requests), "question", {
    transientContext: {
      load: () =>
        Effect.gen(function* () {
          const dependency = yield* TransientContextDependency;

          return yield* TransientContextFailure.make({ message: dependency.value });
        }),
    },
  });

  const dependencyRequired: TransientContextDependency extends Effect.Services<typeof program>
    ? true
    : false = true;

  const failurePreserved: TransientContextFailure extends Effect.Error<typeof program>
    ? true
    : false = true;

  void (dependencyRequired && failurePreserved);
};
