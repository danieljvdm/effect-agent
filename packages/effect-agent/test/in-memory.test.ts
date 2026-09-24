import { describe, expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response, Toolkit } from "effect/unstable/ai";
import type { expectTypeOf as ExpectTypeOf } from "vite-plus/test";

import * as Agent from "../src/core/Agent.ts";
import { ThreadId } from "../src/core/Identifiers.ts";
import * as AgentRuntime from "../src/engine/AgentRuntime.ts";
import { ThreadHistory } from "../src/engine/ThreadHistory.ts";
import * as InMemory from "../src/InMemory.ts";

const answer: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const scriptedModel = (
  name: string,
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  observe: (options: LanguageModel.ProviderOptions) => void = () => {},
  beforeTurn: Effect.Effect<void> = Effect.void,
) =>
  Model.make(
    "test",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* beforeTurn;
                observe(options);
                const index = yield* Ref.getAndUpdate(turn, (value) => value + 1);

                return Stream.fromIterable(turns[index] ?? answer);
              }),
            ),
        });
      }),
    ),
  );

const child = Agent.make("child", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer the question.",
  toolkit: Toolkit.empty,
});

describe("in-memory assembly", () => {
  it.effect(
    "rejects stale concurrent history without erasing the winning conversation or retrying",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        const threadId = ThreadId.make("concurrent");
        let modelCalls = 0;

        const slow = scriptedModel(
          "slow",
          [answer],
          () => {
            modelCalls++;
          },
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(resume))),
        );

        const fast = scriptedModel("fast", [answer], () => {
          modelCalls++;
        });

        yield* Effect.gen(function* () {
          const first = yield* AgentRuntime.run(Agent.withModel(child, slow), "First request", {
            threadId,
          }).pipe(Effect.forkChild);

          yield* Deferred.await(entered);
          yield* AgentRuntime.run(Agent.withModel(child, fast), "Second request", { threadId });
          yield* Deferred.succeed(resume, undefined);
          const failed = yield* Fiber.join(first).pipe(Effect.flip);

          expect(failed).toMatchObject({ _tag: "ThreadHistoryError", reason: "conflict" });
          expect(modelCalls).toBe(2);
          const history = yield* ThreadHistory;
          const stored = JSON.stringify(yield* history.load(threadId));

          expect(stored).toContain("First request");
          expect(stored).toContain("Second request");
        }).pipe(Effect.provide(InMemory.layer));
      }),
  );
});

class ProviderClient extends Context.Service<ProviderClient, string>()(
  "in-memory/ProviderClient",
) {}

export const verifyProviderRequirementsAndTypedFailures = () => {
  const model = Model.make(
    "test",
    "required-client",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        yield* ProviderClient;

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.fromIterable(answer),
        });
      }),
    ),
  );

  const program = AgentRuntime.run(Agent.withModel(child, model), "question").pipe(
    Effect.provide(InMemory.layer),
  );

  expectTypeOf<Effect.Services<typeof program>>().toEqualTypeOf<ProviderClient>();
  expectTypeOf<Effect.Error<typeof program>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof child>
  >();
};

declare const expectTypeOf: typeof ExpectTypeOf;
