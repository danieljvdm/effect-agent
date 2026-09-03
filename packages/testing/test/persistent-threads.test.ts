import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { type RunEvent } from "@effect-agent/core/RunEvent";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { RunContextPreparationPassthrough } from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { SqliteStorageFailpointError } from "@effect-agent/storage-sqlite/SqliteStorageError";
import { layer as sqliteStore } from "@effect-agent/storage-sqlite/SqliteThreadStore";
import { ScriptedModel, type ScriptedTurnInput } from "@effect-agent/testing/ScriptedModel";
import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import { PersistentHistory } from "@effect-agent/thread/PersistentHistory";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  DeploymentId,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
} from "@effect-agent/thread/Records";
import { replayThread } from "@effect-agent/thread/ThreadProjection";
import {
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadStore,
  FencedAppendRequest,
} from "@effect-agent/thread/ThreadStore";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Array,
  Cause,
  Context,
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

const threadId = Schema.decodeSync(ThreadId)("retained-history");
const options = { threadId };

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

const definition = Agent.make("retained-history", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Use the retained thread.",
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
  RunContextPreparationPassthrough,
  IdGenerator.layer,
  NodeCrypto.layer,
  toolkit.toLayer({ lookup: () => Effect.succeed("Kyoto") }),
);

const memory = PersistentHistory.layer.pipe(
  Layer.provideMerge(MemoryThreadStoreLive),
  Layer.provideMerge(services),
);

const sqliteLayer = (options: Parameters<typeof sqliteStore>[0]) =>
  PersistentHistory.layer.pipe(Layer.provideMerge(sqliteStore(options)));

const loadHistory = (id: ThreadId) => Effect.flatMap(ThreadHistory, (history) => history.load(id));

const exported = Effect.flatMap(ThreadStore, (store) =>
  store.export(ThreadExportRequest.make({ threadId })),
);

const withDatabase = <A, E, R>(use: (filename: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "retained-history-" });

      return yield* use(`${directory}/history.sqlite`);
    }),
  ).pipe(Effect.provide(Layer.merge(NodeFileSystem.layer, services)));

describe("persistent threads", () => {
  it.effect("rejects competing history ownership before model execution", () =>
    Effect.gen(function* () {
      yield* AgentRuntime.run(agent([answer("retained")]), "prior", options);
      const before = yield* exported;
      const calls = yield* Ref.make(0);

      const binding = agent([
        { ...answer("not retained"), onStreamStart: Ref.update(calls, (n) => n + 1) },
      ]);

      for (const hooks of [
        { history: Prompt.empty },
        { onHistory: () => Effect.die("Competing history callback must not run") },
      ]) {
        const rejected = yield* AgentRuntime.run(binding, "new", {
          ...options,
          ...hooks,
        }).pipe(Effect.flip);

        expect(rejected).toMatchObject({
          _tag: "ThreadHistoryError",
          reason: "incompatible",
        });
      }
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
        Agent.make("history-result-codec", {
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
    "run, start, and stream close run-local resources before commit while sharing an application client",
    () =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const finalized = yield* Ref.make(0);
        const commits = yield* Ref.make(0);
        const clientLifetime = yield* Ref.make<ReadonlyArray<string>>([]);

        class SharedClient extends Context.Service<
          SharedClient,
          { readonly request: Effect.Effect<void> }
        >()("test/persistent-threads/SharedClient") {}

        const clientLayer = Layer.effect(
          SharedClient,
          Effect.acquireRelease(
            Ref.update(clientLifetime, (events) => [...events, "acquired"]).pipe(
              Effect.as({
                request: Ref.update(clientLifetime, (events) => [...events, "request"]),
              }),
            ),
            () => Ref.update(clientLifetime, (events) => [...events, "released"]),
          ),
        );

        const history = PersistentHistory.layer.pipe(
          Layer.provide(
            Layer.succeed(ThreadStore, {
              ...store,
              append: Effect.fn(function* (request: FencedAppendRequest) {
                expect(yield* Ref.get(finalized)).toBe((yield* Ref.get(commits)) + 1);
                expect(yield* Ref.get(clientLifetime)).toEqual([
                  "acquired",
                  ...Array.replicate("request", (yield* Ref.get(commits)) + 1),
                ]);
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
              Layer.unwrap(
                Effect.gen(function* () {
                  const client = yield* SharedClient;

                  return ScriptedModel.layer([
                    { ...answer("retained"), onStreamStart: client.request },
                  ]);
                }),
              ),
              Layer.effectDiscard(Effect.addFinalizer(() => Ref.update(finalized, (n) => n + 1))),
            ),
          ),
        );

        yield* Effect.gen(function* () {
          const application = yield* Layer.build(clientLayer);

          yield* Effect.gen(function* () {
            yield* AgentRuntime.run(binding, "run", options);
            expect(yield* Ref.get(commits)).toBe(1);
            expect(yield* Ref.get(finalized)).toBe(1);
            expect(yield* Ref.get(clientLifetime)).toEqual(["acquired", "request"]);
            const started = yield* AgentRuntime.start(binding, "start", options);

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
            );
          }).pipe(Effect.provideContext(application), Effect.provide(history));
          expect(yield* Ref.get(clientLifetime)).toEqual([
            "acquired",
            "request",
            "request",
            "request",
          ]);
        }).pipe(Effect.scoped);
        expect(yield* Ref.get(clientLifetime)).toEqual([
          "acquired",
          "request",
          "request",
          "request",
          "released",
        ]);
        expect((yield* exported).records).toHaveLength(9);
        expect(yield* Ref.get(finalized)).toBe(3);
      }).pipe(Effect.provide(MemoryThreadStoreLive.pipe(Layer.provideMerge(services)))),
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

      const transformed = Agent.make("history-codecs", {
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
      const projection = replayThread(threadId, log.records);

      expect(yield* Ref.get(decodes)).toBe(1);
      expect(yield* Ref.get(seenInput)).toBe("request:1");
      expect(projection.inputs).toEqual(["request:1"]);
      expect(projection.modelOutputs).toEqual(["reply"]);
      expect(result.output).toBe("reply");

      const refused = yield* AgentRuntime.run(agent([]), "x".repeat(1_048_577), options).pipe(
        Effect.flip,
      );

      expect(refused._tag).toBe("ThreadHistoryError");
      expect(yield* exported).toEqual(log);
    }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "retains native Tool exchanges in the input without treating them as executed Turns",
    () =>
      Effect.gen(function* () {
        const examples = Agent.make("history-examples", {
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
        expect((yield* loadHistory(threadId)).content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
          "tool",
          "user",
          "assistant",
        ]);
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

          const before = yield* loadHistory(threadId).pipe(
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
              prompt: yield* loadHistory(threadId),
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
          const projection = replayThread(threadId, restored.log.records, restored.log.tailDigest);

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
        const store = yield* ThreadStore;
        const producerEpoch = Schema.decodeSync(ProducerEpoch)(0);

        yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch }));
        const createdAt = yield* DateTime.now;
        let tailSequence = Schema.decodeSync(CanonicalSequence)(0);
        let tailDigest = EMPTY_TAIL_DIGEST;

        // Seed through the store's public append contract without thousands of model calls.
        for (let start = 0; start < 65_532; start += 256) {
          const records = Array.makeBy(Math.min(256, 65_532 - start), (offset) =>
            RecordEnvelope.make({
              recordId: Schema.decodeSync(RecordId)(`seed:${start + offset}`),
              family: "thread",
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
              threadId,
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
        const prompt = yield* loadHistory(threadId);

        expect(before.records).toHaveLength(65_535);
        expect(prompt.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
          "tool",
          "assistant",
        ]);
        expect(yield* run("overflow").pipe(Effect.flip)).toMatchObject({
          _tag: "ThreadHistoryError",
          message: expect.stringContaining("65536"),
        });
        expect(yield* Ref.get(modelCalls)).toBe(2);
        expect(yield* Ref.get(toolCalls)).toBe(1);
        expect(yield* exported).toEqual(before);
        expect(yield* loadHistory(threadId)).toEqual(prompt);
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
          value: { _tag: "ThreadHistoryError", reason: "conflict" },
        });
        const log = yield* exported;

        expect(replayThread(threadId, log.records).completedRuns).toEqual([winner.runId]);
        expect(yield* Ref.get(calls)).toBe(2);
        expect(JSON.stringify(yield* loadHistory(threadId))).not.toContain("loser");
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
        const store = yield* ThreadStore;

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId,
            producerEpoch: Schema.decodeSync(ProducerEpoch)(1),
          }),
        );
        yield* Deferred.succeed(release, undefined);
        const stale = yield* Fiber.await(running);

        expect(Exit.isFailure(stale) && Cause.findErrorOption(stale.cause)).toMatchObject({
          value: { _tag: "ThreadHistoryError", reason: "fenced" },
        });
        const next = yield* AgentRuntime.run(agent([]), "next", options).pipe(Effect.flip);

        expect(next).toMatchObject({ _tag: "ThreadHistoryError", reason: "fenced" });
        expect((yield* exported).records).toEqual([]);
      }).pipe(Effect.provide(memory)),
  );

  for (const ending of ["failure", "defect", "timeout", "interruption"] as const) {
    it.effect(`retains no partial Run after ${ending} and closes run-local model streams`, () =>
      Effect.gen(function* () {
        yield* AgentRuntime.run(agent([answer("retained")]), "prior", options);
        const before = yield* exported;
        const started = yield* Deferred.make<void>();
        const finalized = yield* Ref.make(0);
        const modelFinalized = yield* Ref.make(0);
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

        const binding = Agent.withModel(
          definition,
          Model.make(
            "scripted",
            "history-cleanup",
            Layer.merge(
              ScriptedModel.layer([{ ...lookup, onStreamFinalize: finalize }, interruptedTurn]),
              Layer.effectDiscard(
                Effect.addFinalizer(() => Ref.update(modelFinalized, (n) => n + 1)),
              ),
            ),
          ),
        );

        const fiber = yield* AgentRuntime.run(binding, "not retained", options).pipe(
          Effect.forkChild,
        );

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
        expect(yield* Ref.get(modelFinalized)).toBe(1);
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
                _tag: "ThreadHistoryError",
                reason: "storage",
              });
              if (entrypoint !== "run") {
                const observed = yield* Ref.get(events);

                expect(observed.some((event) => event._tag === "RunCompleted")).toBe(false);
                expect(observed.at(-1)).toMatchObject({
                  _tag: "RunFailed",
                  errorTag: "ThreadHistoryError",
                });
              }

              const log = yield* exported.pipe(
                Effect.provide(sqliteLayer({ filename, verifyOnOpen: true })),
              );

              const projection = replayThread(threadId, log.records, log.tailDigest);

              expect(projection.inputs).toEqual(location === "append:after" ? ["city"] : []);
              expect(projection.modelOutputs).toEqual(location === "append:after" ? ["Kyoto"] : []);
              expect(projection.completedRuns).toHaveLength(location === "append:after" ? 1 : 0);
            }),
          ),
      );
    }
  }
});
