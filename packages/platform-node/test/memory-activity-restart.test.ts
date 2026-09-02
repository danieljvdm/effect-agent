import { recallMemory, revalidateMemoryLookup } from "@effect-agent/capabilities";
import {
  MemoryNamespace,
  MemoryScope,
  Agent,
  AgentPolicy,
  IdGenerator,
  MemoryReader,
  MemoryRecallLimits,
  MemoryWrite,
  MemoryWriter,
  ThreadId,
} from "@effect-agent/core";
import type { ActiveMemoryDocument } from "@effect-agent/core";
import {
  AgentRuntime,
  ThreadHistory,
  RunContextPreparationPassthrough,
} from "@effect-agent/engine";
import {
  activityProcessorStoreLayer,
  layer as sqliteThreadStoreLayer,
  memoryReaderLayer,
  memoryStoreLayer,
} from "@effect-agent/storage-sqlite";
import {
  ActivityProcessorStore,
  PersistentHistory,
  ThreadExportRequest,
  ThreadStore,
  type PreparedActivity,
} from "@effect-agent/thread";
import { NodeServices } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import {
  Schema as NamespaceSchema,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Path,
  Ref,
  Result,
  Schema,
  Stream,
} from "effect";
import { LanguageModel, Model, Prompt, type Response, Toolkit } from "effect/unstable/ai";
import { ChildProcess, type ChildProcessSpawner } from "effect/unstable/process";

import {
  ActivityMemoryOutput,
  CORRECTED_TEXT,
  DAN_THREAD,
  DIVERGENT_TEXT,
  MEMORY_NAMESPACE,
  MEMORY_SCOPE,
  MemoryActivityMarker,
  MemoryActivityWorkerResult,
  ORIGINAL_TEXT,
  TIM_THREAD,
  activityKey,
  DanStatement,
  danStatement,
  memoryKey,
  type MemoryActivityWorkerMode,
} from "./memory-activity-fixtures.ts";

const TestNamespace = MemoryNamespace.define({
  name: "test/memory",
  version: 1,
  identity: NamespaceSchema.String,
});

const danThreadId = Schema.decodeSync(ThreadId)(DAN_THREAD);
const timThreadId = Schema.decodeSync(ThreadId)(TIM_THREAD);
const recallLimits = MemoryRecallLimits.make({
  maxSources: 2,
  maxItems: 8,
  maxBytes: 64_000,
  maxTokens: 64_000,
  timeoutMillis: 5_000,
});
const FIVE_MINUTES_MILLIS = 300_000;
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

const model = (name: string, answer: string, prompts?: Ref.Ref<ReadonlyArray<Prompt.Prompt>>) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt }) =>
          Stream.unwrap(
            (prompts === undefined
              ? Effect.void
              : Ref.update(prompts, (seen) => [...seen, prompt])
            ).pipe(Effect.as(Stream.fromIterable(finalParts(answer)))),
          ),
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

const timDefinition = Agent.make("memory-consumer-tim", {
  input: Schema.Struct({ question: Schema.String, askedAt: Schema.Finite }),
  output: Schema.String,
  instructions: "Answer using supplied references, preserving attribution and uncertainty.",
  toolkit: Toolkit.empty,
  policy,
});

const historyLayer = (filename: string) =>
  PersistentHistory.layer.pipe(Layer.provide(sqliteThreadStoreLayer({ filename })));

const activityLayer = (filename: string) =>
  activityProcessorStoreLayer.pipe(
    Layer.provide(SqliteClient.layer({ filename, busyTimeout: 5_000 })),
  );

const memoryLayer = (filename: string) =>
  memoryStoreLayer.pipe(Layer.provide(SqliteClient.layer({ filename, busyTimeout: 5_000 })));

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
    Stream.mapEffect((line) => Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(line)),
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

const candidateFrom = (document: ActiveMemoryDocument) => ({
  _tag: "Found" as const,
  passages: [
    {
      version: 1 as const,
      source: document.source,
      passageId: "document",
      content: document.content,
    },
  ],
});

const runTim = Effect.fn("MemoryActivityTest.runTim")(function* (
  filename: string,
  question: string,
  askedAt: number,
  candidates: ReturnType<typeof candidateFrom>,
) {
  const prompts = yield* Ref.make<ReadonlyArray<Prompt.Prompt>>([]);
  const recalled = yield* Ref.make<ReadonlyArray<string>>([]);
  const agent = Agent.withModel(timDefinition, model(`tim-${question}`, "acknowledged", prompts));
  yield* AgentRuntime.run(
    agent,
    { question, askedAt },
    {
      threadId: timThreadId,
      transientContext: {
        load: () =>
          recallMemory(
            [
              {
                id: "authoritative-team-memory",
                essential: false,
                read: revalidateMemoryLookup(candidates, {
                  namespace: MEMORY_NAMESPACE,
                  scope: MEMORY_SCOPE,
                }),
              },
            ],
            recallLimits,
          ).pipe(
            Effect.tap((result) => Ref.update(recalled, (texts) => [...texts, result.text])),
            Effect.map((result) =>
              result.text.length === 0
                ? Prompt.empty
                : Prompt.make([{ role: "user", content: result.text }]),
            ),
          ),
      },
    },
  ).pipe(
    Effect.provide([
      historyLayer(filename),
      readerLayer(filename),
      IdGenerator.layer,
      RunContextPreparationPassthrough,
    ]),
  );
  return { prompts: yield* Ref.get(prompts), recalled: yield* Ref.get(recalled) };
});

const originalWriteFrom = Effect.fn("MemoryActivityTest.originalWriteFrom")(function* (
  pending: PreparedActivity,
) {
  const output = yield* Schema.decodeUnknownEffect(ActivityMemoryOutput)(pending.output);
  if (output._tag !== "Remember") return yield* Effect.die("Expected remembered activity");
  return yield* Schema.decodeUnknownEffect(MemoryWrite.Wire)({
    _tag: "Put",
    key: output.key,
    operationId: pending.workId,
    expectedRevision: null,
    locator: output.locator,
    content: {
      ...output.content,
      metadata: {
        ...output.content.metadata,
        sourceRecordDigest: pending.recordDigest,
        sourceWorkId: pending.workId,
      },
    },
    scopes: output.scopes,
  });
});

it.live(
  "replays pinned cross-Thread memory after SIGKILL, then honors correction and withdrawal",
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
            IdGenerator.layer,
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
        expect(original.content.attributions).toEqual([
          {
            originId: source.records[0].record.recordId,
            speaker: "Dan",
            observers: ["Chad"],
            locator: danStatement.locator,
            activityAt: 1_000,
            interpretation: "proposal reported by Dan; not a decision",
          },
        ]);
        expect(original.content).toMatchObject({ recordedAt: 2_000, extractedAt: 3_000 });
        expect(original.content.metadata).toMatchObject({
          sourceThreadId: DAN_THREAD,
          sourceSequence: 1,
          sourceRecordId: source.records[0].record.recordId,
          sourceSchemaVersion: 1,
          sourceRecordDigest: pending.recordDigest,
          sourceWorkId: pending.workId,
        });

        const staleCandidates = candidateFrom(original);
        const askedAt = danStatement.activityAt + FIVE_MINUTES_MILLIS;
        const initialRecall = yield* runTim(
          filename,
          "What did Dan say about Chad?",
          askedAt,
          staleCandidates,
        );
        const initialPrompt = JSON.stringify(initialRecall.prompts[0]);
        const recalledText = initialRecall.recalled[0] ?? "";
        expect(recalledText).toContain(ORIGINAL_TEXT);
        expect(recalledText).toContain('"speaker":"Dan"');
        expect(recalledText).toContain('"observers":["Chad"]');
        expect(recalledText).toContain(danStatement.locator);
        expect(recalledText).toContain('"activityAt":1000');
        expect(recalledText).toContain('"revision":"1"');
        expect(recalledText).toContain('"sourceSchemaVersion":1');
        expect(recalledText).toContain(source.records[0].record.recordId);
        expect(recalledText).toContain("proposal reported by Dan; not a decision");
        expect(recalledText).toContain(pending.recordDigest);
        expect(initialPrompt).toContain(ORIGINAL_TEXT);
        expect(initialPrompt).toContain(String(askedAt));

        const wrongNamespace = yield* revalidateMemoryLookup(staleCandidates, {
          namespace: TestNamespace.make("another-team"),
          scope: MEMORY_SCOPE,
        }).pipe(Effect.provide(readerLayer(filename)));
        const wrongScope = yield* revalidateMemoryLookup(staleCandidates, {
          namespace: MEMORY_NAMESPACE,
          scope: MemoryScope.make("unshared-channel"),
        }).pipe(Effect.provide(readerLayer(filename)));
        expect(wrongNamespace).toEqual({ _tag: "NoMatch" });
        expect(wrongScope).toEqual({ _tag: "NoMatch" });

        const corrected = yield* Effect.flatMap(MemoryWriter, (writer) =>
          writer.change({
            _tag: "Put",
            key: memoryKey,
            operationId: "correct-project-atlas",
            expectedRevision: original.source.revision,
            locator: original.source.locator,
            content: {
              ...original.content,
              text: CORRECTED_TEXT,
              recordedAt: 4_000,
              extractedAt: 4_500,
            },
            scopes: [MEMORY_SCOPE],
          }),
        ).pipe(Effect.provide(memoryLayer(filename)));
        expect(corrected).toMatchObject({
          _tag: "ActiveMemoryDocument",
          generation: 2,
          predecessor: original.source,
        });
        const correctedRecall = yield* runTim(
          filename,
          "Was the Project Atlas statement corrected?",
          askedAt + 1_000,
          staleCandidates,
        );
        expect(correctedRecall.recalled[0]).toContain(CORRECTED_TEXT);
        expect(correctedRecall.recalled[0]).not.toContain(ORIGINAL_TEXT);

        const withdrawn = yield* Effect.flatMap(MemoryWriter, (writer) =>
          writer.change({
            _tag: "Withdraw",
            key: memoryKey,
            operationId: "withdraw-project-atlas",
            expectedRevision: corrected.source.revision,
            reason: "Dan withdrew the source statement",
          }),
        ).pipe(Effect.provide(memoryLayer(filename)));
        expect(withdrawn).toMatchObject({ _tag: "WithdrawnMemoryDocument", generation: 3 });

        const replayed = yield* Effect.flatMap(MemoryWriter, (writer) =>
          Effect.flatMap(originalWriteFrom(pending), writer.change),
        ).pipe(Effect.provide(memoryLayer(filename)));
        expect(replayed).toMatchObject({
          _tag: "ActiveMemoryDocument",
          generation: 1,
          source: { revision: "1" },
        });
        expect(yield* readMemory(filename)).toMatchObject({
          _tag: "WithdrawnMemoryDocument",
          generation: 3,
        });

        const withdrawnRecall = yield* runTim(
          filename,
          "What remains recallable after withdrawal?",
          askedAt + 2_000,
          staleCandidates,
        );
        expect(withdrawnRecall.recalled[0]).toBe("");
        expect(JSON.stringify(withdrawnRecall.prompts[0])).not.toContain(CORRECTED_TEXT);

        const timHistory = yield* Effect.flatMap(ThreadHistory, (history) =>
          history.load(timThreadId),
        ).pipe(Effect.provide(historyLayer(filename)));
        const canonicalTim = JSON.stringify(timHistory);
        expect(canonicalTim).not.toContain(ORIGINAL_TEXT);
        expect(canonicalTim).not.toContain(CORRECTED_TEXT);
        expect(canonicalTim).not.toContain(pending.recordDigest);
        expect((yield* exportThread(filename, timThreadId)).records).toHaveLength(9);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  60_000,
);
