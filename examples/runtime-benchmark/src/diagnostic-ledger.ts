import process from "node:process";
import { fileURLToPath } from "node:url";

import { AgentId, ThreadId } from "@effect-agent/core/Identifiers";
import { ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import { ScriptedModel } from "@effect-agent/testing/ScriptedModel";
import { digestJson } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  SubmissionSettledRecord,
} from "@effect-agent/thread/Records";
import {
  AdmissionRequest,
  ClaimRequest,
  IdempotencyKey,
  MarkReadyRequest,
  Principal,
  SettlementFinalization,
  SettlementReservation,
  SubmissionLedger,
  submissionSettlementId,
  submissionSettlementRecordId,
} from "@effect-agent/thread/SubmissionLedger";
import type { Settlement } from "@effect-agent/thread/SubmissionLedger";
import type { SubmissionStatus } from "@effect-agent/thread/SubmissionStatus";
import {
  Clock,
  Context,
  DateTime,
  Effect,
  FileSystem,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import { Agent } from "effect-agent";
import { Model, Toolkit } from "effect/unstable/ai";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { CurrentTransformer, type Statement } from "effect/unstable/sql/Statement";

import { BenchmarkError, check } from "./contracts.js";
import { ledgerCases } from "./diagnostic-cases.js";
import {
  DiagnosticProgress,
  type DiagnosticCase,
  type DiagnosticResult,
} from "./diagnostic-contracts.js";
import {
  nodeMonotonicNanos,
  writerOverlap,
  WriterOptions,
  WriterResult,
} from "./diagnostic-writer.js";

export { ledgerCases } from "./diagnostic-cases.js";

/** Test-only scheduling input; real diagnostic cases observe immediately after the go signal. */
export const DiagnosticLedgerWaitForWriterRelease = Context.Reference(
  "runtime-benchmark/DiagnosticLedgerWaitForWriterRelease",
  { defaultValue: () => false },
);

const digest = Digest.make("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const deploymentId = DeploymentId.make("ledger-diagnostic");
const producerId = ProducerId.make("ledger-diagnostic");
const principal = Principal.make("ledger-diagnostic");
const seedThread = ThreadId.make("ledger-seed");

const agent = Agent.make("ledger-diagnostic", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer the question.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 1, maxDuration: "5 seconds" },
});

const host = (filename: string) =>
  NodeDurableAgentRuntime.layer({ filename, deploymentId, producerId, busyTimeout: 5_000 }).pipe(
    Layer.provide(ContextCompactor.layerRollover),
  );

const admit = Effect.fn("diagnostic.ledger.admit")(function* (index: number) {
  const ledger = yield* SubmissionLedger;

  return yield* ledger.admit(
    AdmissionRequest.make({
      threadId: seedThread,
      principal,
      idempotencyKey: IdempotencyKey.make(`seed-${index}`),
      agentId: AgentId.make("ledger-diagnostic"),
      agentDigests: definitions,
      deploymentId,
      inputPayload: "fixture",
      inputDigest: yield* digestJson("fixture"),
    }),
  );
});

/** Public adapter seed, deliberately independent of canonical history size. */
const reserve = Effect.fn("diagnostic.ledger.reserve")(function* (index: number) {
  const ledger = yield* SubmissionLedger;
  const admitted = yield* admit(index);

  yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
  const claim = yield* ledger.claim(ClaimRequest.make({ threadId: seedThread, producerId }));

  if (Option.isNone(claim))
    return yield* BenchmarkError.make({ message: "Ledger seed claim missing" });
  const settlementId = submissionSettlementId(admitted.submissionId);

  const record = RecordEnvelope.make({
    recordId: submissionSettlementRecordId(admitted.submissionId),
    family: "thread",
    schemaVersion: 1,
    createdAt: DateTime.makeUnsafe(1),
    deploymentId,
    payload: yield* Schema.decodeUnknownEffect(SubmissionSettledRecord)({
      _tag: "SubmissionSettled",
      submissionId: admitted.submissionId,
      receiptId: admitted.receiptId,
      settlementId,
      outcome: "aborted",
    }),
  });

  yield* ledger.reserveSettlement(
    SettlementReservation.make({
      submissionId: admitted.submissionId,
      ownershipToken: claim.value.ownershipToken,
      settlementId,
      outcome: "aborted",
      record,
      recordDigest: yield* digestJson(yield* Schema.encodeEffect(RecordEnvelope)(record)),
    }),
  );

  return SettlementFinalization.make({ submissionId: admitted.submissionId, settlementId });
});

const seed = Effect.fn("diagnostic.ledger.seed")(function* (settled: number, unfinished: number) {
  const ledger = yield* SubmissionLedger;
  const progress = yield* DiagnosticProgress;

  for (let index = 0; index < settled; index++) {
    yield* ledger.finalizeSettlement(yield* reserve(index));
    if (index === 0) yield* progress.mark({ name: "seed.firstSettlement.committed", elapsedMs: 0 });
  }
  for (let index = settled; index < settled + unfinished; index++) yield* admit(index);
  yield* check(
    (yield* Stream.runCollect(ledger.scanNonterminal)).length === unfinished,
    "Seed unfinished count mismatch",
  );
});

const makeSeeds = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "diagnostic-ledger-seeds-" });
  const templates = new Map<string, string>();
  let attempts = 0;

  const copy = Effect.fn("diagnostic.ledger.copySeed")(function* (
    settled: number,
    unfinished: number,
    destination: string,
  ) {
    yield* check(
      (settled === 8192 && unfinished === 16) || (settled === 0 && unfinished === 768),
      "Unsupported ledger seed",
    );
    const key = `${settled}-${unfinished}`;
    let source = templates.get(key);
    let seedSetupMs = 0;

    if (source === undefined) {
      // A failed initialization can leave committed rows. Never retry against that file.
      const candidate = `${directory}/${key}-${attempts++}.sqlite`;
      const started = yield* Clock.monotonicTimeNanos;

      yield* seed(settled, unfinished).pipe(Effect.provide(host(candidate)), Effect.scoped);
      yield* check(
        !(yield* fs.exists(`${candidate}-wal`)) && !(yield* fs.exists(`${candidate}-shm`)),
        "Ledger template still owns SQLite sidecars",
      );
      seedSetupMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;
      templates.set(key, candidate);
      source = candidate;
    }
    yield* fs.copyFile(source, destination);

    return seedSetupMs;
  });

  return { copy };
});

/** Provide once around the worker, not around each sample. Only closed database files are reused. */
export class DiagnosticLedgerSeeds extends Context.Service<
  DiagnosticLedgerSeeds,
  Effect.Success<typeof makeSeeds>
>()("runtime-benchmark/DiagnosticLedgerSeeds") {
  static readonly layer = Layer.effect(DiagnosticLedgerSeeds, makeSeeds);
}

type Query = ReturnType<Statement<unknown>["compile"]>;

const inspectQueries = Effect.fn("diagnostic.ledger.inspectQueries")(function* (
  queries: ReadonlyArray<Query>,
  started: bigint,
) {
  const sql = yield* SqlClient;
  const progress = yield* DiagnosticProgress;
  let indexedPlans = 0;
  let searchedPlans = 0;
  let sortedPlans = 0;

  for (const [index, [query, parameters]] of queries.entries()) {
    if (!query.includes("queue_sequence") || !query.includes("LIMIT")) continue;
    const plans = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, parameters);

    for (const [part, { detail }] of plans.entries()) {
      indexedPlans += Number(detail.includes("effect_agent_submissions_nonterminal"));
      searchedPlans += Number(detail.includes("SEARCH"));
      sortedPlans += Number(detail.includes("TEMP B-TREE"));
      for (let offset = 0; offset < detail.length; offset += 64)
        yield* progress.mark({
          name: `plan.${index}.${part}.${offset}.${detail.slice(offset, offset + 64)}`,
          elapsedMs: Number((yield* Clock.monotonicTimeNanos) - started) / 1e6,
        });
    }
  }

  // Statement transformation observes adapter work, not the driver's BEGIN/COMMIT commands.
  return [
    { name: "sqlStatements", value: queries.length },
    {
      name: "sqlWriteStatements",
      value: queries.filter(([query]) => /^\s*(INSERT|UPDATE|DELETE)/i.test(query)).length,
    },
    { name: "indexedPlans", value: indexedPlans },
    { name: "searchedPlans", value: searchedPlans },
    { name: "sortedPlans", value: sortedPlans },
  ];
});

const scanCase = Effect.fn("diagnostic.ledger.scan")(function* (
  workload: DiagnosticCase,
  setupStarted: bigint,
  seedSetupMs: number,
) {
  const progress = yield* DiagnosticProgress;
  const ledger = yield* SubmissionLedger;
  const queries: Array<Query> = [];
  const setupMs = Number((yield* Clock.monotonicTimeNanos) - setupStarted) / 1e6;

  yield* progress.phase("operation");
  const started = yield* Clock.monotonicTimeNanos;

  const rows = yield* Stream.runCollect(ledger.scanNonterminal).pipe(
    Effect.provideService(CurrentTransformer, (statement) =>
      Effect.sync(() => {
        queries.push(statement.compile());

        return statement;
      }),
    ),
  );

  const totalMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;

  yield* progress.phase("verification");
  yield* check(
    rows.length === workload.parameters.unfinished &&
      rows.every(
        (row, index) =>
          row.threadId === seedThread &&
          row.queueSequence === (workload.parameters.settled ?? 0) + index + 1,
      ),
    "Scan lost FIFO rows or included settled rows",
  );

  return {
    totalMs,
    metrics: [
      { name: "scanNonterminal", value: totalMs },
      { name: "setup", value: setupMs },
      { name: "seedSetup", value: seedSetupMs },
    ],
    counters: [
      { name: "unfinished", value: rows.length },
      { name: "settled", value: workload.parameters.settled ?? 0 },
      ...(yield* inspectQueries(queries, started)),
    ],
  } satisfies DiagnosticResult;
});

const settledCase = Effect.fn("diagnostic.ledger.settled")(function* (
  workload: DiagnosticCase,
  filename: string,
  setupStarted: bigint,
) {
  const progress = yield* DiagnosticProgress;
  const ledger = yield* SubmissionLedger;
  const runtime = yield* DurableAgentRuntime;
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const mode = workload.parameters.mode;
  let request: SettlementFinalization;
  let modelCalls = 0;
  let modelFinalizers = 0;

  const model = ScriptedModel.layer([
    {
      _tag: "Stream",
      termination: { _tag: "Complete" },
      parts: [
        { type: "text-start", id: "answer" },
        { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
        { type: "text-end", id: "answer" },
        { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
      ],
      assertRequest: () =>
        Effect.sync(() => {
          modelCalls++;
        }),
      onStreamFinalize: Effect.sync(() => {
        modelFinalizers++;
      }),
    },
  ]).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Layer.succeed(Model.ProviderName, "scripted"),
        Layer.succeed(Model.ModelName, "ledger-diagnostic"),
      ),
    ),
  );

  const receipt =
    mode === 1
      ? undefined
      : yield* Effect.gen(function* () {
          const receipt = yield* runtime.submit({ definition: agent }, "Answer", {
            threadId: ThreadId.make("healthy"),
            principal,
            idempotencyKey: IdempotencyKey.make("healthy"),
            definitions,
          });

          yield* runtime.processThread({ definition: agent, model }, receipt.threadId);

          return receipt;
        });

  if (receipt === undefined) request = yield* reserve(0);
  else
    request = SettlementFinalization.make({
      submissionId: receipt.submissionId,
      settlementId: submissionSettlementId(receipt.submissionId),
    });
  const original = receipt === undefined ? undefined : yield* runtime.awaitSettlement(receipt);

  yield* check(
    original === undefined || original.outcome === "completed",
    "Runtime seed did not settle",
  );
  yield* check(
    modelCalls === (mode === 1 ? 0 : 1) && modelFinalizers === modelCalls,
    "Runtime seed model work or finalization mismatch",
  );
  const holdMs = workload.parameters.holdMs ?? -1;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "diagnostic-ledger-writer-" });
  let writer: ChildProcessSpawner.ChildProcessHandle | undefined;
  let operationStarted = 0n;

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      if (writer === undefined) return;
      yield* check(!(yield* writer.isRunning), "External writer survived its Scope");
      yield* progress.mark({
        name: "writer.scope.closed",
        elapsedMs:
          operationStarted === 0n
            ? 0
            : Number((yield* Clock.monotonicTimeNanos) - operationStarted) / 1e6,
      });
    }).pipe(Effect.orDie),
  );
  if (holdMs >= 0) {
    const options = yield* Schema.decodeUnknownEffect(WriterOptions)({
      filename,
      directory,
      holdMs,
    });

    writer = yield* spawner.spawn(
      ChildProcess.make(
        process.execPath,
        [
          fileURLToPath(
            new URL(
              import.meta.url.endsWith(".ts") ? "./diagnostic-writer.ts" : "./diagnostic-writer.js",
              import.meta.url,
            ),
          ),
        ],
        {
          env: {
            RUNTIME_DIAGNOSTIC_WRITER: yield* Schema.encodeEffect(
              Schema.fromJsonString(WriterOptions),
            )(options),
            NODE_NO_WARNINGS: "1",
          },
          extendEnv: true,
          stdin: "ignore",
          stdout: "ignore",
          stderr: "pipe",
        },
      ),
    );
  }
  if (writer !== undefined) {
    const activeWriter = writer;

    yield* Effect.gen(function* () {
      while (!(yield* fs.exists(`${directory}/ready`))) {
        if (!(yield* activeWriter.isRunning)) {
          const stderr = yield* activeWriter.stderr.pipe(Stream.decodeText(), Stream.mkString);

          return yield* BenchmarkError.make({
            message: `External SQLite writer exited before acquiring lock: ${stderr}`,
          });
        }
        yield* Effect.sleep("1 millis");
      }
    }).pipe(Effect.timeout("5 seconds"));
    yield* progress.mark({ name: "writer.acquired", elapsedMs: 0 });
  }
  const setupMs = Number((yield* Clock.monotonicTimeNanos) - setupStarted) / 1e6;

  yield* progress.phase("operation");
  if (writer !== undefined) yield* fs.writeFileString(`${directory}/go`, "go");
  if (writer !== undefined && (yield* DiagnosticLedgerWaitForWriterRelease)) yield* writer.exitCode;
  const queries: Array<Query> = [];
  const durations: Array<number> = [];
  const observations: Array<Settlement | SubmissionStatus> = [];

  const observerIntervals: Array<{
    readonly startedNanos: bigint;
    readonly finishedNanos: bigint;
  }> = [];

  const started = yield* Clock.monotonicTimeNanos;

  operationStarted = started;

  for (let index = 0; index < (workload.parameters.repeats ?? 1); index++) {
    const callStarted = yield* Clock.monotonicTimeNanos;
    const startedNanos = yield* nodeMonotonicNanos;

    const observation = yield* Effect.gen(function* () {
      return mode === 3 && receipt !== undefined
        ? yield* runtime.submissionStatus(receipt)
        : yield* ledger.finalizeSettlement(request);
    }).pipe(
      Effect.provideService(CurrentTransformer, (statement) =>
        Effect.sync(() => {
          queries.push(statement.compile());

          return statement;
        }),
      ),
    );

    const finishedNanos = yield* nodeMonotonicNanos;

    durations.push(Number((yield* Clock.monotonicTimeNanos) - callStarted) / 1e6);
    observerIntervals.push({ startedNanos, finishedNanos });
    observations.push(observation);
  }
  const totalMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;

  yield* progress.phase("verification");
  for (const observation of observations) {
    if ("_tag" in observation && observation._tag !== "settled")
      return yield* BenchmarkError.make({ message: "Settled runtime observation became pending" });
    const settlement = "_tag" in observation ? observation.settlement : observation;

    yield* check(
      settlement.submissionId === request.submissionId &&
        settlement.settlementId === request.settlementId &&
        settlement.outcome === (mode === 1 ? "aborted" : "completed") &&
        (original === undefined ||
          (settlement.receiptId === original.receiptId &&
            DateTime.toEpochMillis(settlement.settledAt) ===
              DateTime.toEpochMillis(original.settledAt))),
      "Observation changed authoritative settlement",
    );
  }
  let heldMs = 0;
  let lockMs = 0;
  let overlapMs = 0;
  let observationsWithWriterOverlap = 0;
  let observationsStartingWithWriter = 0;
  let observationsFinishedWhileHeld = 0;

  if (writer !== undefined) {
    const stderr = yield* writer.stderr.pipe(Stream.decodeText(), Stream.mkString);

    yield* check((yield* writer.exitCode) === 0, `External SQLite writer failed: ${stderr}`);

    const result = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WriterResult))(
      yield* fs.readFileString(`${directory}/result.json`),
    );

    heldMs = result.heldMs;
    lockMs = result.lockMs;

    // Exact timestamps remain available even when the requested hold window missed the call.
    const rawBoundaries = [
      ["writer.acquired", result.acquiredNanos],
      ["writer.releaseStarted", result.releaseStartedNanos],
      ["writer.released", result.releasedNanos],
      ...observerIntervals.flatMap(
        (interval, index) =>
          [
            [`observer.${index}.started`, interval.startedNanos],
            [`observer.${index}.finished`, interval.finishedNanos],
          ] as const,
      ),
    ] as const;

    for (const [boundary, nanos] of rawBoundaries)
      yield* progress.mark({
        name: `node-hrtime.${boundary}.${nanos}`,
        elapsedMs: Number((yield* Clock.monotonicTimeNanos) - started) / 1e6,
      });
    for (const interval of observerIntervals) {
      const overlap = writerOverlap(result, interval);

      overlapMs += overlap.overlapMs;
      observationsWithWriterOverlap += Number(overlap.overlapMs > 0);
      observationsStartingWithWriter += Number(overlap.heldAtStart);
      observationsFinishedWhileHeld += Number(overlap.finishedWhileHeld);
    }
    yield* check(!(yield* writer.isRunning), "External writer remains running");
    yield* progress.mark({
      name: "writer.closed",
      elapsedMs: Number((yield* Clock.monotonicTimeNanos) - started) / 1e6,
    });
  }
  // A real subsequent write proves release, outside the observation clock.
  yield* admit(1);

  return {
    totalMs,
    metrics: [
      { name: "setup", value: setupMs },
      { name: "writerHold", value: heldMs },
      { name: "writerLock", value: lockMs },
      { name: "writerObserverOverlap", value: overlapMs },
      ...durations.map((value, index) => ({ name: `observation.${index}`, value })),
    ],
    counters: [
      { name: "observations", value: durations.length },
      { name: "writers", value: Number(writer !== undefined) },
      { name: "observationsWithWriterOverlap", value: observationsWithWriterOverlap },
      { name: "observationsStartingWithWriter", value: observationsStartingWithWriter },
      { name: "observationsFinishedWhileHeld", value: observationsFinishedWhileHeld },
      {
        name: "noWriterOverlap",
        value: Number(writer !== undefined && observationsWithWriterOverlap === 0),
      },
      { name: "seedModelCalls", value: modelCalls },
      { name: "seedModelFinalizers", value: modelFinalizers },
      ...(yield* inspectQueries(queries, started)),
    ],
  } satisfies DiagnosticResult;
});

export const runLedgerCase = Effect.fn("diagnostic.runLedgerCase")(function* (
  workload: DiagnosticCase,
) {
  const expected = ledgerCases.find(({ name }) => name === workload.name);

  yield* check(
    expected !== undefined &&
      workload.family === "ledger" &&
      Object.keys(expected.parameters).length === Object.keys(workload.parameters).length &&
      Object.entries(expected.parameters).every(
        ([key, value]) => workload.parameters[key] === value,
      ),
    "Unknown or modified ledger diagnostic case",
  );
  const progress = yield* DiagnosticProgress;
  const fs = yield* FileSystem.FileSystem;
  const seeds = yield* DiagnosticLedgerSeeds;

  yield* progress.phase("setup");
  const setupStarted = yield* Clock.monotonicTimeNanos;

  return yield* Effect.gen(function* () {
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "diagnostic-ledger-sample-" });
    const filename = `${directory}/thread.sqlite`;

    const seedSetupMs =
      workload.parameters.mode === 0
        ? yield* seeds.copy(
            workload.parameters.settled ?? 0,
            workload.parameters.unfinished ?? 0,
            filename,
          )
        : 0;

    return yield* (
      workload.parameters.mode === 0
        ? scanCase(workload, setupStarted, seedSetupMs)
        : settledCase(workload, filename, setupStarted)
    ).pipe(Effect.provide(host(filename)));
  }).pipe(Effect.scoped);
});
