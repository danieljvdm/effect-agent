import { Agent, AgentPolicy, ThreadId } from "@effect-agent/core";
import {
  AgentRuntime,
  ThreadHistory,
  RunContextPreparationPassthrough,
} from "@effect-agent/engine";
import { layer as sqliteStore } from "@effect-agent/storage-sqlite";
import { ScriptedModel, type ScriptedTurnInput } from "@effect-agent/testing";
import { PersistentHistory } from "@effect-agent/thread/history";
import { Console, Effect, Layer, Schema } from "effect";
import { Model, Prompt, Toolkit } from "effect/unstable/ai";
import { Command, Flag } from "effect/unstable/cli";

const sqliteHistory = (filename: string) =>
  PersistentHistory.layer.pipe(Layer.provide(sqliteStore({ filename })));

const threadId = Schema.decodeSync(ThreadId)("persistent-history-example");
const definition = Agent.make("history-example", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer using the thread history.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

// The example runs offline. Replace this Model with an upstream provider Model in an application.
const binding = (answer: string) => {
  const turn: ScriptedTurnInput = {
    _tag: "Stream",
    parts: [
      { type: "text-start", id: "answer" },
      {
        type: "text-delta",
        id: "answer",
        delta: Schema.encodeSync(Schema.fromJsonString(Schema.String))(answer),
      },
      { type: "text-end", id: "answer" },
      { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
    ],
    termination: { _tag: "Complete" },
  };
  return Agent.withModel(
    definition,
    Model.make("scripted", "history-example", ScriptedModel.layer([turn])),
  );
};

/** Two inputs share one SQLite history; each call closes its connection before the next opens. */
export const writeHistory = Effect.fn("example.writeHistory")(function* (filename: string) {
  const first = yield* AgentRuntime.run(binding("I'll remember Kyoto."), "I'm visiting Kyoto.", {
    threadId,
  }).pipe(Effect.provide([sqliteHistory(filename), RunContextPreparationPassthrough]));
  const second = yield* AgentRuntime.run(
    binding("You're visiting Kyoto."),
    "Which city am I visiting?",
    { threadId },
  ).pipe(Effect.provide([sqliteHistory(filename), RunContextPreparationPassthrough]));
  return [first.output, second.output];
});

/** No model or Agent is needed to reconstruct the retained Prompt after the process restarts. */
export const readHistory = Effect.fn("example.readHistory")((filename: string) =>
  Effect.flatMap(ThreadHistory, (history) => history.load(threadId)).pipe(
    Effect.provide(sqliteHistory(filename)),
  ),
);

const history = Command.make("history").pipe(
  Command.withDescription(
    "Retain two offline Agent Runs in SQLite, then read them in a new process",
  ),
  Command.withSharedFlags({
    database: Flag.string("database").pipe(
      Flag.withDescription("SQLite history file"),
      Flag.withSchema(Schema.NonEmptyString),
    ),
  }),
);
const seed = Command.make(
  "seed",
  {},
  Effect.fn("history.seed")(function* () {
    const { database } = yield* history;
    const replies = yield* writeHistory(database);
    yield* Console.log(replies.join("\n"));
  }),
).pipe(Command.withDescription("Append two successful Runs to the example Thread"));
const show = Command.make(
  "show",
  {},
  Effect.fn("history.show")(function* () {
    const { database } = yield* history;
    const prompt = yield* readHistory(database);
    yield* Console.log(yield* Schema.encodeEffect(Schema.fromJsonString(Prompt.Prompt))(prompt));
  }),
).pipe(Command.withDescription("Reconstruct and print the canonical Prompt as JSON"));

export const historyCommand = history.pipe(Command.withSubcommands([seed, show]));
