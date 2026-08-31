import { Agent, AgentPolicy, ConversationId, IdGenerator, type RunEvent } from "@effect-agent/core";
import { AgentRuntime, ConversationHistory } from "@effect-agent/engine";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationStore,
  DeploymentId,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
  replayConversation,
} from "@effect-agent/session";
import { PersistentHistory } from "@effect-agent/session/history";
import { MemoryConversationStoreLive } from "@effect-agent/storage-memory";
import { layer as sqliteStore, SqliteStorageFailpointError } from "@effect-agent/storage-sqlite";
import { ScriptedModel, type ScriptedTurnInput } from "@effect-agent/testing";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Array,
  Cause,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Ref,
  Schema,
  SchemaGetter,
  SchemaIssue,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { Model, Prompt, Tool, Toolkit } from "effect/unstable/ai";

const conversationId = Schema.decodeSync(ConversationId)("retained-history");
const options = { conversationId };
const policy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 3,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});
const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ name: Schema.String }),
  success: Schema.String,
});
const toolkit = Toolkit.make(Lookup);
const definition = Agent.define("retained-history", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Use the retained conversation.",
  toolkit,
  policy,
});
const agent = (turns: ReadonlyArray<ScriptedTurnInput>) =>
  Agent.withModel(definition, Model.make("scripted", "history", ScriptedModel.layer(turns)));
const answer = (text: string): ScriptedTurnInput => ({
  _tag: "Stream",
  parts: [
    { type: "text-start", id: "answer" },
    {
      type: "text-delta",
      id: "answer",
      delta: Schema.encodeSync(Schema.fromJsonString(Schema.String))(text),
    },
    { type: "text-end", id: "answer" },
    { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
  ],
  termination: { _tag: "Complete" },
});
const lookup: ScriptedTurnInput = {
  _tag: "Stream",
  parts: [
    { type: "reasoning-start", id: "reason" },
    {
      type: "reasoning-delta",
      id: "reason",
      delta: "Find the city.",
      metadata: { test: { signature: "kept" } },
    },
    { type: "reasoning-end", id: "reason" },
    { type: "tool-call", id: "lookup-1", name: "lookup", params: { name: "Dan" } },
    { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
  ],
  termination: { _tag: "Complete" },
};
const services = Layer.mergeAll(
  IdGenerator.layer,
  NodeCrypto.layer,
  toolkit.toLayer({ lookup: () => Effect.succeed("Kyoto") }),
);
const memory = PersistentHistory.layer.pipe(
  Layer.provideMerge(MemoryConversationStoreLive),
  Layer.provideMerge(services),
);
const sqliteLayer = (options: Parameters<typeof sqliteStore>[0]) =>
  PersistentHistory.layer.pipe(Layer.provideMerge(sqliteStore(options)));
const loadHistory = (id: ConversationId) =>
  Effect.flatMap(ConversationHistory, (history) => history.load(id));
const exported = Effect.flatMap(ConversationStore, (store) =>
  store.export(ConversationExportRequest.make({ conversationId })),
);
const withDatabase = <A, E, R>(use: (filename: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "retained-history-" });
      return yield* use(`${directory}/history.sqlite`);
    }),
  ).pipe(Effect.provide(Layer.merge(NodeFileSystem.layer, services)));

describe("persistent conversations", () => {
  it.effect("rejects competing history ownership before model execution", () =>
    Effect.gen(function* () {
      yield* AgentRuntime.run(agent([answer("retained")]), "prior", options);
      const before = yield* exported;
      const calls = yield* Ref.make(0);
      const binding = agent([
        { ...answer("not retained"), onStreamStart: Ref.update(calls, (n) => n + 1) },
      ]);
      const rejected = yield* AgentRuntime.run(binding, "new", {
        ...options,
        history: Prompt.empty,
      }).pipe(Effect.flip);
      expect(rejected).toMatchObject({ _tag: "ConversationHistoryError", reason: "incompatible" });
      expect(yield* Ref.get(calls)).toBe(0);
      expect(yield* exported).toEqual(before);
    }).pipe(Effect.provide(memory)),
  );

  it.effect("does not commit or publish completion when final result decoding fails", () =>
    Effect.gen(function* () {
      const decodes = yield* Ref.make(0);
      const output = Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.transformOrFail((value) =>
            Ref.updateAndGet(decodes, (n) => n + 1).pipe(
              Effect.flatMap((count) =>
                count === 1
                  ? Effect.succeed(value)
                  : Effect.fail(
                      new SchemaIssue.InvalidValue({ message: "Result decoder refused the value" }),
                    ),
              ),
            ),
          ),
          encode: SchemaGetter.transform((value) => value),
        }),
      );
      const binding = Agent.withModel(
        Agent.define("history-result-codec", {
          input: Schema.String,
          output,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy,
        }),
        Model.make("scripted", "result-codec", ScriptedModel.layer([answer("reply")])),
      );
      const started = yield* AgentRuntime.start(binding, "request", options);
      expect((yield* started.await.pipe(Effect.flip))._tag).toBe("AgentOutputError");
      expect(yield* Ref.get(decodes)).toBe(2);
      expect((yield* started.events).some((event) => event._tag === "RunCompleted")).toBe(false);
      expect((yield* exported).records).toEqual([]);
    }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "run, start, and stream commit after model finalizers and before completion is visible",
    () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        const finalized = yield* Ref.make(0);
        const commits = yield* Ref.make(0);
        const history = PersistentHistory.layer.pipe(
          Layer.provide(
            Layer.succeed(ConversationStore, {
              ...store,
              append: Effect.fn(function* (request: FencedAppendRequest) {
                expect(yield* Ref.get(finalized)).toBe((yield* Ref.get(commits)) + 1);
                const result = yield* store.append(request);
                yield* Ref.update(commits, (n) => n + 1);
                return result;
              }),
            }),
          ),
        );
        const binding = Agent.withModel(
          definition,
          Model.make(
            "scripted",
            "scoped-history",
            Layer.merge(
              ScriptedModel.layer([answer("retained")]),
              Layer.effectDiscard(Effect.addFinalizer(() => Ref.update(finalized, (n) => n + 1))),
            ),
          ),
        );
        yield* AgentRuntime.run(binding, "run", options).pipe(Effect.provide(history));
        expect(yield* Ref.get(commits)).toBe(1);
        const started = yield* AgentRuntime.start(binding, "start", options).pipe(
          Effect.provide(history),
        );
        yield* started.observe.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              if (event._tag === "RunCompleted") expect(yield* Ref.get(commits)).toBe(2);
            }),
          ),
        );
        expect((yield* started.await).output).toBe("retained");
        yield* AgentRuntime.stream(binding, "stream", options).pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              if (event._tag === "RunCompleted") expect(yield* Ref.get(commits)).toBe(3);
            }),
          ),
          Effect.provide(history),
        );
        expect((yield* exported).records).toHaveLength(9);
        expect(yield* Ref.get(finalized)).toBe(3);
      }).pipe(Effect.provide(MemoryConversationStoreLive.pipe(Layer.provideMerge(services)))),
  );

  it.effect("retains the engine's encoded values without repeating Schema transformations", () =>
    Effect.gen(function* () {
      const decodes = yield* Ref.make(0);
      const seenInput = yield* Ref.make("");
      const input = Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.transformOrFail((value) =>
            Ref.updateAndGet(decodes, (n) => n + 1).pipe(
              Effect.map((count) => `${value}:${count}`),
            ),
          ),
          encode: SchemaGetter.transform((value) => value),
        }),
      );
      const output = Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.transform((value) => value),
          encode: SchemaGetter.transform((value) => `reencoded:${value}`),
        }),
      );
      const transformed = Agent.define("history-codecs", {
        input,
        output,
        inputPrompt: (value) => Ref.set(seenInput, value).pipe(Effect.as(value)),
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy,
      });
      const binding = Agent.withModel(
        transformed,
        Model.make("scripted", "codecs", ScriptedModel.layer([answer("reply")])),
      );
      const result = yield* AgentRuntime.run(binding, "request", options);
      const log = yield* exported;
      const projection = replayConversation(conversationId, log.records);
      expect(yield* Ref.get(decodes)).toBe(1);
      expect(yield* Ref.get(seenInput)).toBe("request:1");
      expect(projection.inputs).toEqual(["request:1"]);
      expect(projection.modelOutputs).toEqual(["reply"]);
      expect(result.output).toBe("reply");
      const refused = yield* AgentRuntime.run(agent([]), "x".repeat(1_048_577), options).pipe(
        Effect.flip,
      );
      expect(refused._tag).toBe("ConversationHistoryError");
      expect(yield* exported).toEqual(log);
    }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "retains native Tool exchanges in the input without treating them as executed Turns",
    () =>
      Effect.gen(function* () {
        const examples = Agent.define("history-examples", {
          input: Schema.String,
          inputPrompt: (input) => [
            { role: "user", content: "Example question" },
            Prompt.makeMessage("assistant", {
              content: [
                Prompt.makePart("tool-call", {
                  id: "example-lookup",
                  name: "lookup",
                  params: { name: "example" },
                  providerExecuted: false,
                }),
              ],
            }),
            Prompt.makeMessage("tool", {
              content: [
                Prompt.makePart("tool-result", {
                  id: "example-lookup",
                  name: "lookup",
                  result: "Example answer",
                  isFailure: false,
                  providerExecuted: false,
                }),
              ],
            }),
            { role: "user", content: input },
          ],
          output: Schema.String,
          instructions: "Follow the example.",
          toolkit: Toolkit.empty,
          policy,
        });
        const bound = Agent.withModel(
          examples,
          Model.make("scripted", "examples", ScriptedModel.layer([answer("actual answer")])),
        );
        const result = yield* AgentRuntime.run(bound, "Actual question", options);
        const log = yield* exported;
        expect(result.turns).toBe(1);
        expect(
          log.records.flatMap(({ record }) =>
            record.payload._tag === "ModelCompleted" ? [record.payload.output] : [],
          ),
        ).toEqual(["actual answer"]);
        expect((yield* loadHistory(conversationId)).content.map((message) => message.role)).toEqual(
          ["system", "user", "assistant", "tool", "user", "assistant"],
        );
      }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "runs two SQLite inputs across closed connections and reconstructs native history",
    () =>
      withDatabase((filename) =>
        Effect.gen(function* () {
          const first = yield* AgentRuntime.run(
            agent([lookup, answer("Kyoto")]),
            "Find my city",
            options,
          ).pipe(Effect.provide(sqliteLayer({ filename })));
          const before = yield* loadHistory(conversationId).pipe(
            Effect.provide(sqliteLayer({ filename })),
          );
          const second = yield* AgentRuntime.run(
            agent([
              {
                ...answer("Welcome back to Kyoto"),
                assertRequest: (request) => {
                  expect(request.prompt.content.slice(0, before.content.length)).toEqual(
                    before.content,
                  );
                  expect(before.content.map((message) => message.role)).toEqual([
                    "system",
                    "user",
                    "assistant",
                    "tool",
                    "assistant",
                  ]);
                  expect(JSON.stringify(before)).toContain("Find the city.");
                  expect(JSON.stringify(before)).toContain("signature");
                },
              },
            ]),
            "Where was I?",
            options,
          ).pipe(Effect.provide(sqliteLayer({ filename })));
          const restored = yield* Effect.gen(function* () {
            return {
              prompt: yield* loadHistory(conversationId),
              log: yield* exported,
            };
          }).pipe(Effect.provide(sqliteLayer({ filename, verifyOnOpen: true })));
          expect(first.output).toBe("Kyoto");
          expect(second.output).toBe("Welcome back to Kyoto");
          expect(restored.prompt.content.map((message) => message.role)).toEqual([
            "system",
            "user",
            "assistant",
            "tool",
            "assistant",
            "system",
            "user",
            "assistant",
          ]);
          const projection = replayConversation(
            conversationId,
            restored.log.records,
            restored.log.tailDigest,
          );
          expect(projection.inputs).toEqual(["Find my city", "Where was I?"]);
          expect(projection.modelOutputs).toEqual(["Kyoto", "Welcome back to Kyoto"]);
          expect(projection.completedRuns).toEqual([first.runId, second.runId]);
          expect(projection.settlements).toEqual([]);
          expect(
            restored.log.records
              .filter(({ record }) => record.payload._tag === "UserInputRecorded")
              .every(({ record }) => !("submissionId" in record.payload)),
          ).toBe(true);
          expect(new Set(restored.log.records.map((entry) => entry.batchId)).size).toBe(2);
        }),
      ),
  );

  it.effect(
    "keeps the last fitting Run loadable and refuses overflow before external effects",
    () =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        const producerEpoch = Schema.decodeSync(ProducerEpoch)(0);
        yield* store.materialize(
          ConversationMaterialization.make({ conversationId, producerEpoch }),
        );
        const createdAt = yield* DateTime.now;
        let tailSequence = Schema.decodeSync(CanonicalSequence)(0);
        let tailDigest = EMPTY_TAIL_DIGEST;
        // Seed through the store's public append contract without thousands of model calls.
        for (let start = 0; start < 65_532; start += 256) {
          const records = Array.makeBy(Math.min(256, 65_532 - start), (offset) =>
            RecordEnvelope.make({
              recordId: Schema.decodeSync(RecordId)(`seed:${start + offset}`),
              family: "conversation",
              schemaVersion: 1,
              createdAt,
              deploymentId: Schema.decodeSync(DeploymentId)("history-limit-test"),
              payload: RepairAnnotated.make({ reason: "history seed", details: {} }),
            }),
          );
          const batch = yield* CanonicalBatch.makeEffect({
            batchId: Schema.decodeSync(BatchId)(`seed:${start}`),
            producerId: Schema.decodeSync(ProducerId)("history-limit-test"),
            records,
          });
          const appended = yield* store.append(
            FencedAppendRequest.make({
              conversationId,
              producerEpoch,
              batch,
              expectedTailSequence: tailSequence,
              expectedTailDigest: tailDigest,
            }),
          );
          tailSequence = appended.lastSequence;
          tailDigest = appended.tailDigest;
        }
        const modelCalls = yield* Ref.make(0);
        const toolCalls = yield* Ref.make(0);
        const binding = agent(
          [lookup, answer("retained")].map((turn) => ({
            ...turn,
            onStreamStart: Ref.update(modelCalls, (n) => n + 1),
          })),
        );
        const run = (input: string) =>
          AgentRuntime.run(binding, input, options).pipe(
            Effect.provide(
              toolkit.toLayer({
                lookup: () => Ref.update(toolCalls, (n) => n + 1).pipe(Effect.as("Kyoto")),
              }),
            ),
          );
        expect((yield* run("last fitting Run")).output).toBe("retained");
        const before = yield* exported;
        const prompt = yield* loadHistory(conversationId);
        expect(before.records).toHaveLength(65_535);
        expect(prompt.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
          "tool",
          "assistant",
        ]);
        expect(yield* run("overflow").pipe(Effect.flip)).toMatchObject({
          _tag: "ConversationHistoryError",
          message: expect.stringContaining("65536"),
        });
        expect(yield* Ref.get(modelCalls)).toBe(2);
        expect(yield* Ref.get(toolCalls)).toBe(1);
        expect(yield* exported).toEqual(before);
        expect(yield* loadHistory(conversationId)).toEqual(prompt);
      }).pipe(Effect.provide(memory)),
    30_000,
  );

  it.effect(
    "rejects a concurrent stale writer without rerunning it or appending partial history",
    () =>
      Effect.gen(function* () {
        const firstStarted = yield* Deferred.make<void>();
        const secondStarted = yield* Deferred.make<void>();
        const releaseFirst = yield* Deferred.make<void>();
        const releaseSecond = yield* Deferred.make<void>();
        const calls = yield* Ref.make(0);
        const first = yield* AgentRuntime.run(
          agent([
            {
              ...answer("winner"),
              onStreamStart: Ref.update(calls, (n) => n + 1).pipe(
                Effect.andThen(Deferred.succeed(firstStarted, undefined)),
                Effect.andThen(Deferred.await(releaseFirst)),
              ),
            },
          ]),
          "first",
          options,
        ).pipe(Effect.forkChild);
        yield* Deferred.await(firstStarted);
        const second = yield* AgentRuntime.run(
          agent([
            {
              ...answer("loser"),
              onStreamStart: Ref.update(calls, (n) => n + 1).pipe(
                Effect.andThen(Deferred.succeed(secondStarted, undefined)),
                Effect.andThen(Deferred.await(releaseSecond)),
              ),
            },
          ]),
          "second",
          options,
        ).pipe(Effect.forkChild);
        yield* Deferred.await(secondStarted);
        yield* Deferred.succeed(releaseFirst, undefined);
        const winner = yield* Fiber.join(first);
        yield* Deferred.succeed(releaseSecond, undefined);
        const loser = yield* Fiber.await(second);
        expect(Exit.isFailure(loser) && Cause.findErrorOption(loser.cause)).toMatchObject({
          value: { _tag: "ConversationHistoryError", reason: "conflict" },
        });
        const log = yield* exported;
        expect(replayConversation(conversationId, log.records).completedRuns).toEqual([
          winner.runId,
        ]);
        expect(yield* Ref.get(calls)).toBe(2);
        expect(JSON.stringify(yield* loadHistory(conversationId))).not.toContain("loser");
      }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "honors a newer producer epoch and refuses subsequent history Runs before model execution",
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const running = yield* AgentRuntime.run(
          agent([
            {
              ...answer("stale"),
              onStreamStart: Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
              ),
            },
          ]),
          "input",
          options,
        ).pipe(Effect.forkChild);
        yield* Deferred.await(started);
        const store = yield* ConversationStore;
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId,
            producerEpoch: Schema.decodeSync(ProducerEpoch)(1),
          }),
        );
        yield* Deferred.succeed(release, undefined);
        const stale = yield* Fiber.await(running);
        expect(Exit.isFailure(stale) && Cause.findErrorOption(stale.cause)).toMatchObject({
          value: { _tag: "ConversationHistoryError", reason: "fenced" },
        });
        const next = yield* AgentRuntime.run(agent([]), "next", options).pipe(Effect.flip);
        expect(next).toMatchObject({ _tag: "ConversationHistoryError", reason: "fenced" });
        expect((yield* exported).records).toEqual([]);
      }).pipe(Effect.provide(memory)),
  );

  for (const ending of ["failure", "defect", "timeout", "interruption"] as const) {
    it.effect(`retains no partial Run after ${ending} and closes model resources`, () =>
      Effect.gen(function* () {
        yield* AgentRuntime.run(agent([answer("retained")]), "prior", options);
        const before = yield* exported;
        const started = yield* Deferred.make<void>();
        const finalized = yield* Ref.make(0);
        const finalize = Ref.update(finalized, (n) => n + 1);
        const interruptedTurn: ScriptedTurnInput = {
          _tag: "Stream",
          parts: [],
          termination:
            ending === "failure"
              ? { _tag: "Fail", description: "provider failed" }
              : { _tag: "Hang" },
          onStreamStart: Deferred.succeed(started, undefined).pipe(
            Effect.andThen(ending === "defect" ? Effect.die("provider defect") : Effect.void),
          ),
          onStreamFinalize: finalize,
        };
        const fiber = yield* AgentRuntime.run(
          agent([{ ...lookup, onStreamFinalize: finalize }, interruptedTurn]),
          "not retained",
          options,
        ).pipe(Effect.forkChild);
        yield* Deferred.await(started);
        if (ending === "timeout") yield* TestClock.adjust("31 seconds");
        if (ending === "interruption") yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          if (ending === "failure")
            expect(Cause.findErrorOption(exit.cause)).toMatchObject({ value: { _tag: "AiError" } });
          if (ending === "timeout")
            expect(Cause.findErrorOption(exit.cause)).toMatchObject({
              value: { _tag: "AgentPolicyError", limit: "duration" },
            });
          if (ending === "defect") expect(Cause.hasDies(exit.cause)).toBe(true);
          if (ending === "interruption") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
        }
        expect(yield* Ref.get(finalized)).toBe(2);
        expect(yield* exported).toEqual(before);
      }).pipe(Effect.provide(memory)),
    );
  }

  for (const entrypoint of ["run", "start", "stream"] as const) {
    for (const location of ["append:before", "append:after"] as const) {
      it.effect(
        `${entrypoint}: reopening after ${location} sees either the whole Run or none of it`,
        () =>
          withDatabase((filename) =>
            Effect.gen(function* () {
              const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
              const binding = agent([lookup, answer("Kyoto")]);
              const execution =
                entrypoint === "run"
                  ? AgentRuntime.run(binding, "city", options)
                  : entrypoint === "stream"
                    ? AgentRuntime.stream(binding, "city", options).pipe(
                        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
                        Stream.runDrain,
                      )
                    : Effect.gen(function* () {
                        const started = yield* AgentRuntime.start(binding, "city", options);
                        return yield* started.await.pipe(
                          Effect.onExit(() =>
                            started.events.pipe(Effect.flatMap((all) => Ref.set(events, all))),
                          ),
                        );
                      });
              const failure = yield* execution.pipe(
                Effect.provide(
                  sqliteLayer({
                    filename,
                    failpoint: (hit) =>
                      hit === location
                        ? Effect.fail(SqliteStorageFailpointError.make({ location }))
                        : Effect.void,
                  }),
                ),
                Effect.flip,
              );
              expect(failure).toMatchObject({
                _tag: "ConversationHistoryError",
                reason: "storage",
              });
              if (entrypoint !== "run") {
                const observed = yield* Ref.get(events);
                expect(observed.some((event) => event._tag === "RunCompleted")).toBe(false);
                expect(observed.at(-1)).toMatchObject({
                  _tag: "RunFailed",
                  errorTag: "ConversationHistoryError",
                });
              }
              const log = yield* exported.pipe(
                Effect.provide(sqliteLayer({ filename, verifyOnOpen: true })),
              );
              const projection = replayConversation(conversationId, log.records, log.tailDigest);
              expect(projection.inputs).toEqual(location === "append:after" ? ["city"] : []);
              expect(projection.modelOutputs).toEqual(location === "append:after" ? ["Kyoto"] : []);
              expect(projection.completedRuns).toHaveLength(location === "append:after" ? 1 : 0);
            }),
          ),
      );
    }
  }
});
