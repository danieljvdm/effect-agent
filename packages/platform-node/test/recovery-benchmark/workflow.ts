import { NewContext } from "@effect-agent/capabilities/ContextTools";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { RunId, ThreadId, ToolCallId } from "@effect-agent/core/Identifiers";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { ledgerLayer } from "@effect-agent/storage-sqlite/SqliteSubmissionLedger";
import { layer as threadLayer } from "@effect-agent/storage-sqlite/SqliteThreadStore";
import { IntegrityReport } from "@effect-agent/thread/Admin";
import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
} from "@effect-agent/thread/DurableAgentRuntime";
import {
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
} from "@effect-agent/thread/DurableFailpoint";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ModelResponseRecorded,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  ThreadCreated,
  ToolCallSettled,
} from "@effect-agent/thread/Records";
import {
  modelResponseRecordId,
  toolCallSettledRecordId,
  turnIdForRun,
} from "@effect-agent/thread/RunJournal";
import { IdempotencyKey, Principal, SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import {
  FencedAppendRequest,
  ThreadCheckpoint,
  ThreadExport,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadRead,
  ThreadStore,
  ThreadTailRequest,
} from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import {
  Array,
  Cause,
  Config,
  Console,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  References,
  Schema,
  Stream,
} from "effect";
import { LanguageModel, Model, Prompt, Tool, Toolkit, type Response } from "effect/unstable/ai";

import {
  environment,
  nowMillis,
  memoryUsage,
  collectGarbage,
  sha256,
} from "./node-measurements.ts";

const BenchmarkConfig = Schema.Struct({
  mode: Schema.Literals(["seed", "run"]),
  count: Schema.Literals([1000, 10000, 100000]),
  root: Schema.NonEmptyString,
  revision: Schema.NonEmptyString,
});

const threadId = ThreadId.make("full-host-scaling");
const deploymentId = DeploymentId.make("recovery-full-run");
const producerId = ProducerId.make("recovery-full-run");

const definitions = DefinitionDigests.make({
  agent: Digest.make("a".repeat(64)),
  model: Digest.make("a".repeat(64)),
  tools: Digest.make("a".repeat(64)),
});

const retainedText = "Retained canonical file evidence. ".repeat(12);

const readFile = Tool.make("read_file", {
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.Struct({ text: Schema.String }),
}).annotate(ToolExecutionClass, "readonly");

const toolkit = Toolkit.make(NewContext, readFile);

const handlers = toolkit.toLayer({
  new_context: Effect.succeed,
  read_file: () => Effect.succeed({ text: "Fixed active suffix evidence." }),
});

const definition = Agent.make("full-host-scaling", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Finish the active request and answer as JSON.",
  toolkit,
  policy: AgentPolicy.make({
    maxTurns: 30,
    maxToolCalls: 30,
    maxDuration: "1 hour",
    toolConcurrency: 1,
  }),
});

const finish: Response.StreamPartEncoded = {
  type: "finish",
  reason: "tool-calls",
  usage: { inputTokens: { total: 100 }, outputTokens: { total: 10 } },
};

const final: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  {
    type: "finish",
    reason: "stop",
    usage: { inputTokens: { total: 100 }, outputTokens: { total: 10 } },
  },
];

const base = (filename: string, failpoint: typeof DurableRuntimeFailpoint.Service) =>
  Layer.mergeAll(
    threadLayer({ filename }),
    ledgerLayer({ filename }),
    WakeScheduler.layerNoop,
    ToolReconciler.uncertain,
    Layer.succeed(DurableRuntimeFailpoint, failpoint),
    RunToolAuthorization.allowAll,
    DurableRuntimeConfig.layer({ deploymentId, producerId }),
  ).pipe(Layer.provideMerge(NodeCrypto.layer));

const sha = (value: Schema.Json) => Digest.make(sha256(JSON.stringify(value)));

const promptJson = (prompt: Prompt.Prompt) =>
  Schema.decodeUnknownSync(PersistedJson)(Schema.encodeSync(Prompt.Prompt)(prompt));

type Phase = "acquire" | "recovery" | "resume" | "completion" | "verification";

const phaseCounts = (): Record<Phase, number> => ({
  acquire: 0,
  recovery: 0,
  resume: 0,
  completion: 0,
  verification: 0,
});

const checkpointBytes = (checkpoint: ThreadCheckpoint) =>
  new TextEncoder().encode(JSON.stringify(Schema.encodeSync(ThreadCheckpoint)(checkpoint)))
    .byteLength;

/** Deliberately full-history checks. Call only outside measured startup and completion intervals. */
const verifyPublicContract = Effect.fn("RecoveryBenchmark.verifyPublicContract")(function* () {
  const store = yield* ThreadStore;
  const runtime = yield* DurableAgentRuntime;
  const exported = yield* store.export(ThreadExportRequest.make({ threadId }));
  const encoded = yield* Schema.encodeEffect(ThreadExport)(exported);
  const decoded = yield* Schema.decodeEffect(ThreadExport)(encoded);
  const integrity = yield* runtime.verify(threadId);

  if (
    !integrity.ok ||
    decoded.records.length !== decoded.tailSequence ||
    integrity.recordCount !== decoded.records.length ||
    integrity.tailSequence !== decoded.tailSequence
  )
    return yield* Effect.die({
      message: "Public export or integrity verification failed",
      integrity,
    });

  return {
    exportSchemaRoundTrip: true,
    exportedRecords: decoded.records.length,
    exportedTailSequence: decoded.tailSequence,
    integrity: yield* Schema.encodeEffect(IntegrityReport)(integrity),
  };
});

/** Opt-in diagnostic workflow; the caller supplies one cohort and seed/run mode through Config. */
export const benchmark = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  const config = yield* Schema.decodeUnknownEffect(BenchmarkConfig)({
    mode: yield* Config.string("EFFECT_AGENT_RECOVERY_BENCHMARK_MODE"),
    count: yield* Config.number("EFFECT_AGENT_RECOVERY_BENCHMARK_COUNT"),
    root: yield* Config.string("EFFECT_AGENT_RECOVERY_BENCHMARK_OUT"),
    revision: yield* Config.string("EFFECT_AGENT_RECOVERY_BENCHMARK_REVISION"),
  });

  const { mode, count, root } = config;

  yield* fs.makeDirectory(root, { recursive: true });
  const sourceHashes: Record<string, string> = {};

  for (const path of [
    "packages/thread/src/DurableAgentRuntime.ts",
    "packages/thread/src/RunJournal.ts",
    "packages/thread/src/internal/journal-checkpoint.ts",
    "packages/thread/src/ThreadStore.ts",
    "packages/thread/src/Records.ts",
    "packages/thread/src/ThreadInvariants.ts",
    "packages/thread/src/DurableFailpoint.ts",
    "packages/core/src/Usage.ts",
    "packages/core/src/RunPolicyUsage.ts",
    "packages/engine/src/internal/agent-runtime.ts",
    "packages/engine/src/RunOptions.ts",
    "packages/storage-sqlite/src/SqliteThreadStore.ts",
    "packages/storage-sqlite/src/SqliteSubmissionLedger.ts",
    "packages/storage-sqlite/src/internal/sqlite-journal.ts",
    "packages/storage-sqlite/src/internal/migrations.ts",
    "packages/storage-sqlite/src/internal/recovery-checkpoint-schema.ts",
    "packages/storage-sqlite/src/SqliteStorageVersion.ts",
    "packages/storage-sqlite/src/SqliteStorageError.ts",
  ])
    sourceHashes[path] = sha256(yield* fs.readFileString(path));

  const packageInfo = yield* Schema.decodeEffect(
    Schema.fromJsonString(Schema.Struct({ version: Schema.String })),
  )(yield* fs.readFileString("packages/platform-node/package.json"));

  const seedFilename = `${root}/full-host-${count}-seed.sqlite`;

  const seed = Effect.scoped(
    Effect.gen(function* () {
      const store = yield* ThreadStore;
      const createdAt = yield* DateTime.now;
      const epoch = ProducerEpoch.make(0);
      let sequence = CanonicalSequence.make(0);
      let tailDigest = EMPTY_TAIL_DIGEST;

      yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch: epoch }));
      const started = nowMillis();

      for (let start = 0; start < count; start += 256) {
        const rows: Array<RecordEnvelope> = [];

        for (let pos = start; pos < Math.min(start + 256, count); pos++) {
          let payload: typeof RecordEnvelope.Type.payload;
          let recordId: RecordId;

          if (pos === 0) {
            payload = ThreadCreated.make({ agentId: definition.id, definitions });
            recordId = RecordId.make("thread-created");
          } else {
            const pair = Math.floor((pos - 1) / 2);
            const runId = RunId.make(`historic-run-${Math.floor(pair / 20)}`);
            const turn = (pair % 20) + 1;
            const callId = ToolCallId.make(`historic-call-${pair}`);

            if (pos % 2 === 1) {
              const messages = promptJson(
                Prompt.make([
                  Prompt.makeMessage("assistant", {
                    content: [
                      Prompt.makePart("tool-call", {
                        id: callId,
                        name: "read_file",
                        params: { path: `file-${pair}.ts` },
                        providerExecuted: false,
                      }),
                    ],
                  }),
                ]),
              );

              payload = ModelResponseRecorded.make({
                runId,
                turnId: turnIdForRun(runId, turn),
                turn,
                messages,
                messagesDigest: sha(messages),
                inputTokens: 100,
                outputTokens: 10,
              });
              recordId = modelResponseRecordId(runId, turn);
            } else {
              payload = ToolCallSettled.make({
                runId,
                toolCallId: callId,
                toolName: "read_file",
                result: PersistedJson.make({ text: retainedText }),
                isFailure: false,
              });
              recordId = toolCallSettledRecordId(runId, turn, callId);
            }
          }
          // Even cohort counts end in one assistant-only message; no pending Tool exists in the active Run.
          if (pos === count - 1) {
            const runId = RunId.make("historic-terminal");

            const messages = promptJson(
              Prompt.make([
                Prompt.makeMessage("assistant", {
                  content: [Prompt.makePart("text", { text: "Historical cohort finished." })],
                }),
              ]),
            );

            payload = ModelResponseRecorded.make({
              runId,
              turnId: turnIdForRun(runId, 1),
              turn: 1,
              messages,
              messagesDigest: sha(messages),
            });
            recordId = modelResponseRecordId(runId, 1);
          }
          rows.push(
            RecordEnvelope.make({
              recordId,
              family: "thread",
              schemaVersion: 1,
              deploymentId,
              createdAt,
              payload,
            }),
          );
        }
        if (!Array.isArrayNonEmpty(rows)) return yield* Effect.die("Empty fixture batch");

        const appended = yield* store.append(
          FencedAppendRequest.make({
            threadId,
            producerEpoch: epoch,
            expectedTailSequence: sequence,
            expectedTailDigest: tailDigest,
            batch: CanonicalBatch.make({
              batchId: BatchId.make(`seed-${start}`),
              producerId,
              records: rows,
            }),
          }),
        );

        sequence = appended.lastSequence;
        tailDigest = appended.tailDigest;
      }
      yield* Console.log(
        JSON.stringify({ phase: "historical-seed", count, elapsedMs: nowMillis() - started }),
      );
      let calls = 0;

      const model = Model.make(
        "scripted",
        "full-host-scaling",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              const call = calls++;

              return Stream.fromIterable<Response.StreamPartEncoded>(
                call === 0
                  ? [
                      {
                        type: "tool-call",
                        id: "rollover-call",
                        name: "new_context",
                        params: { handoff: "Continue the fixed active request." },
                        providerExecuted: false,
                      },
                      finish,
                    ]
                  : [
                      {
                        type: "tool-call",
                        id: `active-call-${call}`,
                        name: "read_file",
                        params: { path: `active-${call}.ts` },
                        providerExecuted: false,
                      },
                      finish,
                    ],
              );
            },
          }),
        ),
      );

      const agent = Agent.withModel(definition, model);
      const runtime = yield* DurableAgentRuntime.pipe(Effect.provide(DurableAgentRuntime.layer));

      const receipt = yield* runtime.submit(agent, "Fixed original active request", {
        threadId,
        principal: Principal.make("scaling-author"),
        idempotencyKey: IdempotencyKey.make("active"),
        definitions,
      });

      const exit = yield* runtime
        .processThread(agent, threadId)
        .pipe(Effect.provide(handlers), Effect.exit);

      if (
        !(Exit.isFailure(exit) && Cause.squash(exit.cause) instanceof DurableRuntimeFailpointError)
      )
        return yield* Effect.die({ message: "seed failed at unexpected boundary", exit });
      const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

      const suffix = yield* store
        .read(
          ThreadRead.make({ threadId, afterSequence: CanonicalSequence.make(count), limit: 256 }),
        )
        .pipe(Stream.runCollect);

      const boundary = suffix.find(({ record }) => record.payload._tag === "CompactionCreated");

      if (
        calls !== 7 ||
        suffix.length !== 18 ||
        boundary?.sequence !== count + 6 ||
        tail.tailSequence - boundary.sequence !== 12
      )
        return yield* Effect.die({
          message: "Active suffix fixture changed",
          calls,
          tail,
          boundary,
        });
      const tags = suffix.map(({ sequence, record }) => ({ sequence, tag: record.payload._tag }));

      const publicContract =
        count === 100000
          ? yield* verifyPublicContract().pipe(Effect.provideService(DurableAgentRuntime, runtime))
          : undefined;

      yield* fs.writeFileString(
        `${root}/full-host-${count}-fixture.json`,
        JSON.stringify(
          {
            historicalRecords: count,
            tailSequence: tail.tailSequence,
            modelCalls: calls,
            suffix: tags,
            receipt,
            publicContract,
          },
          null,
          2,
        ),
      );
      yield* Console.log(
        JSON.stringify({
          phase: "active-fixture-ready",
          historicalRecords: count,
          tailSequence: tail.tailSequence,
          modelCalls: calls,
          suffix: tags,
          publicContract,
        }),
      );
    }),
  );

  const sample = Effect.fn("RecoveryBenchmark.sample")(function* (index: number) {
    const filename = `${root}/full-host-${count}-sample-${index}.sqlite`;

    yield* fs.copyFile(seedFilename, filename);
    let phase: Phase = "acquire";
    const returned = phaseCounts();
    const pages = phaseCounts();
    const inspectTailCalls = phaseCounts();
    const exportCalls = phaseCounts();
    const exportedCanonicalRecords = phaseCounts();
    const checkpointLoadCalls = phaseCounts();
    const checkpointLoadHits = phaseCounts();
    const checkpointLoadedBytes = phaseCounts();
    const checkpointSaveCalls = phaseCounts();
    const checkpointSubmittedBytes = phaseCounts();
    const ledgerLookupCalls = phaseCounts();
    const ledgerSnapshotLoadCalls = phaseCounts();
    const ledgerScanCalls = phaseCounts();
    const ledgerScanRows = phaseCounts();
    let heapAtModel = 0;
    let rssAtModel = 0;
    let heapAtModelBeforeGc = 0;
    let rssAtModelBeforeGc = 0;
    let peakObservedRss = 0;
    let promptMessages = 0;
    let promptBytes = 0;
    let promptEvidence = "";
    let firstModelMs = 0;
    let processStarted = 0;
    let modelCalls = 0;

    collectGarbage();
    const before = memoryUsage();
    const started = nowMillis();

    const program = Effect.gen(function* () {
      const store = yield* ThreadStore;
      const ledger = yield* SubmissionLedger;
      const recoveryCheckpoints = store.recoveryCheckpoints;

      const counted = ThreadStore.of({
        ...store,
        inspectTail: (request) =>
          Effect.suspend(() => {
            inspectTailCalls[phase]++;

            return store.inspectTail(request);
          }),
        export: (request) =>
          Effect.suspend(() => {
            exportCalls[phase]++;
            const requestPhase = phase;

            return store.export(request).pipe(
              Effect.tap((value) =>
                Effect.sync(() => {
                  exportedCanonicalRecords[requestPhase] += value.records.length;
                }),
              ),
            );
          }),
        recoveryCheckpoints:
          recoveryCheckpoints === undefined
            ? undefined
            : {
                load: (request) =>
                  Effect.suspend(() => {
                    const requestPhase = phase;

                    checkpointLoadCalls[requestPhase]++;

                    return recoveryCheckpoints.load(request).pipe(
                      Effect.tap((value) =>
                        Effect.sync(() => {
                          if (Option.isSome(value)) {
                            checkpointLoadHits[requestPhase]++;
                            checkpointLoadedBytes[requestPhase] += checkpointBytes(value.value);
                          }
                        }),
                      ),
                    );
                  }),
                save: (request) =>
                  Effect.suspend(() => {
                    checkpointSaveCalls[phase]++;
                    checkpointSubmittedBytes[phase] += checkpointBytes(request.checkpoint);

                    return recoveryCheckpoints.save(request);
                  }),
              },
        read: (request) =>
          Stream.suspend(() => {
            pages[phase]++;
            peakObservedRss = Math.max(peakObservedRss, memoryUsage().rss);

            return store.read(request).pipe(
              Stream.tap(() =>
                Effect.sync(() => {
                  returned[phase]++;
                }),
              ),
            );
          }),
      });

      const countedLedger = SubmissionLedger.of({
        ...ledger,
        lookup: (request) =>
          Effect.suspend(() => {
            ledgerLookupCalls[phase]++;

            return ledger.lookup(request);
          }),
        loadRecoverySnapshot: (request) =>
          Effect.suspend(() => {
            ledgerSnapshotLoadCalls[phase]++;

            return ledger.loadRecoverySnapshot(request);
          }),
        scanNonterminal: Stream.suspend(() => {
          const requestPhase = phase;

          ledgerScanCalls[requestPhase]++;

          return ledger.scanNonterminal.pipe(
            Stream.tap(() =>
              Effect.sync(() => {
                ledgerScanRows[requestPhase]++;
              }),
            ),
          );
        }),
      });

      const runtime = yield* DurableAgentRuntime.pipe(
        Effect.provide(DurableAgentRuntime.layer),
        Effect.provideService(ThreadStore, counted),
        Effect.provideService(SubmissionLedger, countedLedger),
      );

      const acquiredMs = nowMillis() - started;

      phase = "recovery";
      const recoveryStarted = nowMillis();
      const reports = yield* runtime.runRecovery;
      const recoveryMs = nowMillis() - recoveryStarted;
      const afterRecovery = memoryUsage();

      const model = Model.make(
        "scripted",
        "full-host-scaling",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              modelCalls++;
              firstModelMs = nowMillis() - processStarted;
              promptMessages = request.prompt.content.length;
              promptEvidence = JSON.stringify(Schema.encodeSync(Prompt.Prompt)(request.prompt));
              promptBytes = new TextEncoder().encode(promptEvidence).byteLength;
              const beforeModelGc = memoryUsage();

              heapAtModelBeforeGc = beforeModelGc.heapUsed;
              rssAtModelBeforeGc = beforeModelGc.rss;
              collectGarbage();
              const atModel = memoryUsage();

              heapAtModel = atModel.heapUsed;
              rssAtModel = atModel.rss;
              peakObservedRss = Math.max(peakObservedRss, atModel.rss);
              phase = "completion";

              return Stream.fromIterable(final);
            },
          }),
        ),
      );

      const agent = Agent.withModel(definition, model);

      phase = "resume";
      processStarted = nowMillis();
      const result = yield* runtime.processThread(agent, threadId).pipe(Effect.provide(handlers));
      const processMs = nowMillis() - processStarted;

      collectGarbage();
      const after = memoryUsage();

      if (modelCalls !== 1 || result.length !== 1 || result[0]?.outcome !== "completed")
        return yield* Effect.die({ message: "resume did not complete once", result, modelCalls });
      phase = "verification";

      // A full export/verification between samples would warm unrelated full-history code.
      // Verify only the final sample after its startup/completion metrics are captured.
      const publicContract =
        count === 100000 && index === 2
          ? yield* verifyPublicContract().pipe(
              Effect.provideService(ThreadStore, counted),
              Effect.provideService(DurableAgentRuntime, runtime),
            )
          : undefined;

      return {
        revision: config.revision,
        sourceHashes,
        packageVersion: packageInfo.version,
        measurement:
          "Actual DurableAgentRuntime.runRecovery then processThread, public SQLite adapters; local Node, not Cloudflare",
        environment: environment(),
        historicalRecords: count,
        fixedActiveSuffixRecords: 12,
        sample: index,
        state:
          index === 0
            ? "fresh process/module imports complete, fresh runtime and SQLite open, OS cache warmed by seed"
            : "warm process/JIT and OS cache, fresh runtime and SQLite clone",
        acquiredMs,
        recoveryMs,
        resumeToFirstModelMs: firstModelMs,
        resumedProcessToCompletionMs: processMs,
        throughFirstModelMs: acquiredMs + recoveryMs + firstModelMs,
        canonicalRecordsRead: returned,
        canonicalReadPages: pages,
        inspectTailCalls,
        exportCalls,
        exportedCanonicalRecords,
        recoveryCheckpointOperations: {
          supported: recoveryCheckpoints !== undefined,
          loadCalls: checkpointLoadCalls,
          loadHits: checkpointLoadHits,
          loadedBytes: checkpointLoadedBytes,
          saveCalls: checkpointSaveCalls,
          submittedBytes: checkpointSubmittedBytes,
          byteDefinition:
            "UTF-8 JSON of schema-encoded ThreadCheckpoint; successful loads returned and save requests submitted. Excludes SQLite row overhead and is separate from canonical read records.",
        },
        ledgerOperations: {
          lookupCalls: ledgerLookupCalls,
          recoverySnapshotLoadCalls: ledgerSnapshotLoadCalls,
          scanCalls: ledgerScanCalls,
          scannedRows: ledgerScanRows,
        },
        publicContract,
        publicVerificationRanAfterStartupAndCompletionMetrics: publicContract !== undefined,
        recoveryReports: reports.map((report) => ({
          decision: report.decision._tag,
          disposition: report.disposition,
        })),
        resumedModelCalls: modelCalls,
        promptMessages,
        promptJsonBytes: promptBytes,
        promptEvidence,
        settlements: result.map(({ outcome, submissionId }) => ({ outcome, submissionId })),
        heapBeforeBytes: before.heapUsed,
        residentSetBeforeBytes: before.rss,
        heapAfterRecoveryBeforeGcBytes: afterRecovery.heapUsed,
        residentSetAfterRecoveryBytes: afterRecovery.rss,
        heapAtModelBeforeGcBytes: heapAtModelBeforeGc,
        residentSetAtModelBeforeGcBytes: rssAtModelBeforeGc,
        residentSetAtModelBytes: rssAtModel,
        peakObservedResidentSetBytes: peakObservedRss,
        retainedHeapAtModelBytes: heapAtModel,
        retainedHeapAtModelDeltaBytes: heapAtModel - before.heapUsed,
        retainedHeapAfterCompletionBytes: after.heapUsed,
        retainedHeapAfterCompletionDeltaBytes: after.heapUsed - before.heapUsed,
        residentSetBytes: after.rss,
        seedDatabaseBytes: Number((yield* fs.stat(seedFilename)).size),
      };
    });

    const result = yield* program.pipe(Effect.provide(base(filename, { hit: () => Effect.void })));

    collectGarbage();
    yield* Console.log(
      JSON.stringify({
        ...result,
        retainedHeapAfterScopeBytes: memoryUsage().heapUsed,
        retainedHeapAfterScopeIncludesPublicVerification: result.publicContract !== undefined,
      }),
    );
  }, Effect.scoped);

  let committedResults = 0;

  const program = Effect.gen(function* () {
    if (mode === "seed") {
      yield* seed.pipe(
        Effect.provide(
          base(seedFilename, {
            hit: (location) => {
              if (location === "turn:after-results-append" && ++committedResults === 7)
                return DurableRuntimeFailpointError.make({ location });

              return Effect.void;
            },
          }),
        ),
      );
    } else {
      for (let index = 0; index < 3; index++) yield* sample(index);
    }
  });

  yield* program.pipe(
    Effect.provideService(References.MinimumLogLevel, "Error"),
    Effect.tapCause((cause) =>
      Console.log(
        JSON.stringify({
          phase: "failed",
          mode,
          historicalRecords: count,
          revision: config.revision,
          sourceHashes,
          environment: environment(),
          cause: Cause.pretty(cause),
        }),
      ),
    ),
  );
});
