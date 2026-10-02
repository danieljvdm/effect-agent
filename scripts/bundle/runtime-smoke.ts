import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import {
  Agent,
  AgentError,
  AgentRuntime,
  InMemory,
  IdGenerator,
  Subagent,
  Thread,
  ThreadHistory,
  PersistentHistory,
} from "@yielded/agent";
import { ScriptedModel } from "@yielded/agent-testing/scripted-model";
import * as DirectAgent from "@yielded/agent/agent";
import { AgentInputError } from "@yielded/agent/agent-error";
import * as DirectRuntime from "@yielded/agent/agent-runtime";
import { IdGenerator as DirectIdGenerator } from "@yielded/agent/id-generator";
import { RunId, ThreadId, TurnId } from "@yielded/agent/identifiers";
import * as DirectInMemory from "@yielded/agent/in-memory";
import { layer as persistentHistoryLayer } from "@yielded/agent/persistent-history";
import * as DirectSubagent from "@yielded/agent/subagent";
import * as DirectThread from "@yielded/agent/thread";
import { layer as historyLayer } from "@yielded/agent/thread-history";
import { Console, Effect, Layer, Ref, Schema } from "effect";
import { Model, Toolkit } from "effect/ai";

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
  yield* check(InMemory.layer === DirectInMemory.layer, "InMemory.layer identity changed");
  yield* check(Subagent.layer === DirectSubagent.layer, "Subagent.layer identity changed");
  yield* check(ThreadHistory.layer === historyLayer, "History layer identity changed");
  yield* check(Thread.Store === DirectThread.Store, "Thread store identity changed");
  yield* check(
    PersistentHistory.layer === persistentHistoryLayer,
    "Persistent history identity changed",
  );

  const ids = yield* DirectIdGenerator;
  const defaultThread = yield* ids.nextThreadId;
  const anotherThread = yield* ids.nextThreadId;

  yield* check(
    defaultThread.startsWith("thread-") && defaultThread !== anotherThread,
    "Default IDs failed",
  );

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
        InMemory.layer,
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
