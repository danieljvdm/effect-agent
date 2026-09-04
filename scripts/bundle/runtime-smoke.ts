import { ScriptedModel } from "@effect-agent/testing/ScriptedModel";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import { Console, Effect, Layer, Ref, Schema } from "effect";
import { Agent, AgentError, AgentRuntime, IdGenerator } from "effect-agent";
import * as DirectAgent from "effect-agent/Agent";
import { AgentInputError } from "effect-agent/AgentError";
import * as DirectRuntime from "effect-agent/AgentRuntime";
import { RunId, ThreadId, TurnId } from "effect-agent/Identifiers";
import { IdGenerator as DirectIdGenerator } from "effect-agent/IdGenerator";
import { ThreadHistory } from "effect-agent/ThreadHistory";
import { Model, Toolkit } from "effect/unstable/ai";

import { loadRuntime } from "./lazy-module.ts";

class BundleSmokeError extends Schema.TaggedError<BundleSmokeError>()("BundleSmokeError", {
  message: Schema.String,
}) {}

const check = Effect.fn("bundleSmoke.check")(function* (condition: boolean, message: string) {
  if (!condition) {
    return yield* new BundleSmokeError({ message });
  }
});

const agent = Agent.make("bundle-runtime-smoke", {
  input: Schema.NonEmptyString,
  output: Schema.Struct({ answer: Schema.Literal("bundled") }),
  instructions: "Return the scripted answer.",
  toolkit: Toolkit.empty,
});

const identifiers = Layer.succeed(DirectIdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("bundle-thread")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("bundle-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("bundle-turn")),
});

/** Execute the minified published SDK, including its deferred runtime import. */
export const program = Effect.gen(function* () {
  yield* check(Agent.make === DirectAgent.make, "Agent.make root/direct identity changed");
  yield* check(AgentRuntime.run === DirectRuntime.run, "AgentRuntime.run identity changed");
  yield* check(AgentError.AgentInputError === AgentInputError, "Schema class identity changed");
  yield* check(IdGenerator.IdGenerator === DirectIdGenerator, "Service identity changed");

  const deferred = yield* Effect.promise(loadRuntime);

  yield* check(deferred.run === AgentRuntime.run, "Deferred runtime identity changed");

  const finalized = yield* Ref.make(0);

  yield* Effect.gen(function* () {
    const result = yield* deferred.run(agent, "Exercise the bundled SDK.");

    yield* check(result.output.answer === "bundled", "Structured output did not decode");
    yield* check(result.turns === 1, "Scripted run did not finish in one turn");
    yield* check(result.threadId === "bundle-thread", "Provided identity service was not used");

    const invalid = yield* AgentRuntime.run(agent, "").pipe(
      Effect.as(false),
      Effect.catchTag("AgentInputError", (error) =>
        Effect.succeed(error instanceof AgentInputError && error.message.length > 0),
      ),
    );

    yield* check(invalid, "Invalid input did not produce the typed AgentInputError");

    const scripted = yield* ScriptedModel;

    yield* scripted.assertExhausted;
    const requests = yield* scripted.requests;

    yield* check(requests.length === 1, "Invalid input reached the model");
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        identifiers,
        ThreadHistory.layerTransient,
        Layer.succeed(Model.ProviderName, "scripted"),
        Layer.succeed(Model.ModelName, "bundle-smoke"),
        ScriptedModel.layer([
          {
            _tag: "Stream",
            parts: [
              { type: "text-start", id: "answer" },
              { type: "text-delta", id: "answer", delta: '{"answer":"bundled"}' },
              { type: "text-end", id: "answer" },
              { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
            ],
            termination: { _tag: "Complete" },
            onStreamFinalize: Ref.update(finalized, (count) => count + 1),
          },
        ]),
      ),
    ),
    Effect.scoped,
  );

  yield* check((yield* Ref.get(finalized)) === 1, "Model stream finalizer did not run once");
  yield* Console.log("Bundled runtime smoke passed");
});

if (import.meta.main) {
  NodeRuntime.runMain(program);
}
