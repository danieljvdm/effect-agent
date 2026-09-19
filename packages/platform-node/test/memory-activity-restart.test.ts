import { activityProcessorStoreLayer } from "@effect-agent/storage-sqlite/sqlite-activity-store";
import { layer as sqliteThreadStoreLayer } from "@effect-agent/storage-sqlite/sqlite-thread-store";
import { NodeServices } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import {
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Path,
  Result,
  Schema,
  Stream,
} from "effect";
import { PersistentHistory } from "effect-agent";
import { ActivityProcessorStore } from "effect-agent/activity-store";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { ThreadId } from "effect-agent/identifiers";
import { MemoryReader } from "effect-agent/memory-store";
import { RunContextPreparationPassthrough } from "effect-agent/run-options";
import { memoryReaderLayer } from "effect-agent/sql-memory-store";
import { ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { LanguageModel, Model, type Response, Toolkit } from "effect/unstable/ai";
import { ChildProcess, type ChildProcessSpawner } from "effect/unstable/process";

import {
  DAN_THREAD,
  DIVERGENT_TEXT,
  MemoryActivityMarker,
  MemoryActivityWorkerResult,
  ORIGINAL_TEXT,
  activityKey,
  DanStatement,
  danStatement,
  memoryKey,
  type MemoryActivityWorkerMode,
} from "./memory-activity-fixtures.ts";

const danThreadId = Schema.decodeSync(ThreadId)(DAN_THREAD);

const policy = AgentPolicy.make({
  maxTurns: 1,
  maxToolCalls: 1,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify(text) },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const model = (name: string, answer: string) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.fromIterable(finalParts(answer)),
      }),
    ),
  );

const sourceAgent = Agent.withModel(
  Agent.make("memory-observer-chad", {
    input: DanStatement,
    output: Schema.String,
    instructions: "Acknowledge Dan's statement without changing its status.",
    toolkit: Toolkit.empty,
    policy,
  }),
  model("memory-observer-chad", ORIGINAL_TEXT),
);

const historyLayer = (filename: string) =>
  PersistentHistory.layer.pipe(Layer.provide(sqliteThreadStoreLayer({ filename })));

const activityLayer = (filename: string) =>
  activityProcessorStoreLayer.pipe(
    Layer.provide(SqliteClient.layer({ filename, busyTimeout: 5_000 })),
  );

const readerLayer = (filename: string) =>
  memoryReaderLayer.pipe(Layer.provide(SqliteClient.layer({ filename, busyTimeout: 5_000 })));

const readMemory = (filename: string, key = memoryKey) =>
  Effect.flatMap(MemoryReader, (reader) => reader.get(key)).pipe(
    Effect.provide(readerLayer(filename)),
  );

const inspectActivity = (filename: string) =>
  Effect.flatMap(ActivityProcessorStore, (store) => store.inspect(activityKey)).pipe(
    Effect.provide(activityLayer(filename)),
  );

const exportThread = (filename: string, threadId: ThreadId) =>
  Effect.flatMap(ThreadStore, (store) => store.export(ThreadExportRequest.make({ threadId }))).pipe(
    Effect.provide(sqliteThreadStoreLayer({ filename })),
  );

const spawnWorker = Effect.fn("MemoryActivityTest.spawnWorker")(function* (
  filename: string,
  mode: MemoryActivityWorkerMode,
) {
  const path = yield* Path.Path;

  const entry = yield* path.fromFileUrl(
    new URL("./memory-activity-worker-entry.ts", import.meta.url),
  );

  return yield* ChildProcess.make("node", ["--experimental-transform-types", entry], {
    cwd: path.dirname(entry),
    env: {
      EFFECT_AGENT_MEMORY_ACTIVITY_DB: filename,
      EFFECT_AGENT_MEMORY_ACTIVITY_MODE: mode,
    },
    extendEnv: true,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
});

const decodeFirstLine = <A, I>(
  child: ChildProcessSpawner.ChildProcessHandle,
  schema: Schema.Codec<A, I, never>,
) =>
  child.stdout.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.mapEffect((line) => Schema.decodeEffect(Schema.fromJsonString(schema))(line)),
    Stream.runHead,
    Effect.timeout(Duration.seconds(15)),
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.die("Child exited without a protocol message"),
        onSome: Effect.succeed,
      }),
    ),
  );

const runWorker = (filename: string, mode: MemoryActivityWorkerMode) =>
  Effect.scoped(
    Effect.gen(function* () {
      const child = yield* spawnWorker(filename, mode);

      const stderr = yield* Effect.forkScoped(
        child.stderr.pipe(Stream.decodeText(), Stream.mkString),
      );

      const output = yield* decodeFirstLine(child, MemoryActivityWorkerResult);
      const exitCode = yield* child.exitCode;
      const errorText = yield* Fiber.join(stderr);

      if (Number(exitCode) !== 0) return yield* Effect.die(`Worker failed: ${errorText}`);

      return output;
    }),
  );

it.live(
  "replays pinned memory after SIGKILL without re-extracting or overwriting it",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;

        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "effect-agent-memory-activity-restart-",
        });

        const filename = `${directory}/memory.sqlite`;

        yield* AgentRuntime.run(sourceAgent, danStatement, { threadId: danThreadId }).pipe(
          Effect.provide([
            historyLayer(filename),

            RunContextPreparationPassthrough,
          ]),
        );
        const source = yield* exportThread(filename, danThreadId);

        expect(source.records.map(({ record }) => record.payload._tag)).toEqual([
          "UserInputRecorded",
          "ModelCompleted",
          "RunCompleted",
        ]);
        expect(JSON.stringify(source.records[1])).toContain(ORIGINAL_TEXT);

        const first = yield* spawnWorker(filename, "crash-after-apply");

        const stderr = yield* Effect.forkScoped(
          first.stderr.pipe(Stream.decodeText(), Stream.mkString),
        );

        expect(yield* decodeFirstLine(first, MemoryActivityMarker)).toEqual({
          _tag: "MemoryActivityMarker",
          point: "memory:change:after",
        });
        yield* first.kill({ killSignal: "SIGKILL" });
        const killed = yield* first.exitCode.pipe(Effect.result);

        yield* Fiber.join(stderr);
        expect(Result.isFailure(killed)).toBe(true);

        const crashedProgress = yield* inspectActivity(filename);

        expect(crashedProgress?.throughSequence).toBe(0);
        expect(crashedProgress?.pending?.sequence).toBe(1);
        const pending = crashedProgress?.pending;

        if (pending === null || pending === undefined) {
          return yield* Effect.die("Expected pinned activity after process death");
        }
        const firstDocument = yield* readMemory(filename);

        expect(firstDocument).toMatchObject({
          _tag: "ActiveMemoryDocument",
          generation: 1,
          predecessor: null,
          content: { text: ORIGINAL_TEXT },
        });

        // SIGKILL skips claim release, so the restarted process must wait out the real lease.
        yield* Effect.sleep(Duration.millis(2_500));
        const recovered = yield* runWorker(filename, "recover-divergent");

        expect(recovered.pass).toMatchObject({
          capturedTail: 3,
          throughSequence: 3,
          processed: 3,
          pendingRecords: 0,
        });
        expect(recovered.appliedWorkIds[0]).toBe(pending.workId);
        expect(recovered.extractedUserRecords).toBe(0);
        const progress = yield* inspectActivity(filename);

        expect(progress).toMatchObject({ throughSequence: 3, pending: null });

        const original = yield* readMemory(filename);

        expect(original?._tag).toBe("ActiveMemoryDocument");
        if (original === null || original._tag !== "ActiveMemoryDocument") {
          return yield* Effect.die("Expected active recovered memory");
        }
        expect(original.generation).toBe(1);
        expect(original.content.text).toBe(ORIGINAL_TEXT);
        expect(original.content.text).not.toBe(DIVERGENT_TEXT);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  60_000,
);
