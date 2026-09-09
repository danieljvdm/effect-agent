import { AgentId, RunId, ThreadId } from "@effect-agent/core/Identifiers";
import { ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import { RunContextPreparation } from "@effect-agent/engine/RunOptions";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import {
  ScriptedModel,
  type ScriptedTurnInput,
  type ScriptedStreamPart,
} from "@effect-agent/testing/ScriptedModel";
import { digestJson } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import {
  BatchId,
  CanonicalBatch,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ModelCompleted,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
  RunCompleted,
  SubmissionSettled,
  SubmissionSettledRecord,
  ThreadCreated,
  UserInputRecorded,
} from "@effect-agent/thread/Records";
import { runIdForSubmission } from "@effect-agent/thread/RunJournal";
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
import {
  FencedAppendRequest,
  LoadCheckpointRequest,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadStore,
  ThreadTailRequest,
} from "@effect-agent/thread/ThreadStore";
import {
  Cause,
  Clock,
  DateTime,
  type Duration,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  References,
  Schema,
  Stream,
} from "effect";
import { Agent, AgentRuntime } from "effect-agent";
import { IdGenerator } from "effect-agent/IdGenerator";
import { ThreadHistory } from "effect-agent/ThreadHistory";
import type { PlatformError } from "effect/PlatformError";
import type { LanguageModel } from "effect/unstable/ai";
import { AiError, Model, Prompt, Tool, Toolkit } from "effect/unstable/ai";

import {
  BenchmarkError,
  check,
  type Case,
  type Sample,
  type SamplePhase,
  type SampleProgress,
} from "./contracts.js";
import type { SeedTemplates } from "./seeds.js";

const answerSchema = Schema.Struct({ answer: Schema.String });

const tool = Tool.make("work", {
  parameters: Schema.Struct({ index: Schema.Int }),
  success: Schema.Int,
});

const toolkit = Toolkit.make(tool);

const agent = Agent.make("runtime-benchmark", {
  input: Schema.String,
  output: answerSchema,
  instructions: "Return the requested answer.",
  toolkit,
  policy: { maxTurns: 10, maxToolCalls: 64, maxDuration: "30 seconds", toolConcurrency: 4 },
});

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const threadId = Schema.decodeSync(ThreadId)("benchmark-thread");
const deploymentId = Schema.decodeSync(DeploymentId)("runtime-benchmark-v1");
const producerId = Schema.decodeSync(ProducerId)("runtime-benchmark");
const principal = Schema.decodeSync(Principal)("benchmark");
const now = DateTime.makeUnsafe("2026-09-08T00:00:00.000Z");

const ambientModel = Layer.effectContext(
  Effect.context<LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName>(),
);

const binding = { definition: agent, model: ambientModel };

const finalParts = (answer: string, chunks: number): ReadonlyArray<ScriptedStreamPart> => {
  const text = JSON.stringify({ answer });
  const parts: Array<ScriptedStreamPart> = [{ type: "text-start", id: "answer" }];

  for (let index = 0; index < chunks; index++) {
    parts.push({
      type: "text-delta",
      id: "answer",
      delta: text.slice(
        Math.floor((index * text.length) / chunks),
        Math.floor(((index + 1) * text.length) / chunks),
      ),
    });
  }
  parts.push(
    { type: "text-end", id: "answer" },
    { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
  );

  return parts;
};

const toolParts = (round: number, count: number): ReadonlyArray<ScriptedStreamPart> => [
  ...Array.from({ length: count }, (_, index) => ({
    type: "tool-call" as const,
    id: `call-${round}-${index}`,
    name: "work",
    params: { index },
    providerExecuted: false,
  })),
  { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
];

const retainedRuns = (records: number) => Math.max(0, Math.floor((records - 1) / 3));
const retainedInput = (index: number) => `retained input ${index}`;
const retainedOutput = (index: number) => ({ answer: `retained-${index}` });

/** Match PersistentHistory's input/model/completion triples with native prompt suffixes. */
const seedHistory = Effect.fn("benchmark.seedHistory")(function* (count: number) {
  const store = yield* ThreadStore;

  yield* store.materialize(
    ThreadMaterialization.make({ threadId, producerEpoch: Schema.decodeSync(ProducerEpoch)(0) }),
  );
  for (let start = 0; start < count; start += 256) {
    const records: Array<RecordEnvelope> = [];

    for (let position = start; position < Math.min(start + 256, count); position++) {
      const index = Math.floor((position - 1) / 3);
      const runId = RunId.make(`retained-run-${index}`);
      let payload: RecordEnvelope["payload"];

      if (position === 0) payload = ThreadCreated.make({ agentId: agent.id, definitions });
      else if (index >= retainedRuns(count))
        payload = RepairAnnotated.make({ reason: "fixture padding", details: { position } });
      else if ((position - 1) % 3 === 0)
        payload = UserInputRecorded.make({ kind: "user", runId, input: retainedInput(index) });
      else if ((position - 1) % 3 === 1) {
        const messages = yield* Schema.decodeUnknownEffect(PersistedJson)(
          yield* Schema.encodeEffect(Prompt.Prompt)(
            Prompt.make([
              { role: "user", content: JSON.stringify(retainedInput(index)) },
              { role: "assistant", content: JSON.stringify(retainedOutput(index)) },
            ]),
          ),
        );

        payload = ModelCompleted.make({ runId, output: retainedOutput(index), messages });
      } else payload = RunCompleted.make({ runId, output: retainedOutput(index) });
      records.push(
        RecordEnvelope.make({
          recordId: RecordId.make(`retained-${position}`),
          family: "thread",
          schemaVersion: 1,
          createdAt: now,
          deploymentId,
          payload,
        }),
      );
    }

    const [first, ...rest] = records;

    if (first === undefined)
      return yield* BenchmarkError.make({ message: "Empty retained-history batch" });

    const batch = CanonicalBatch.make({
      batchId: BatchId.make(`retained-${start}`),
      producerId,
      records: [first, ...rest],
    });

    const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

    yield* store.append(
      FencedAppendRequest.make({
        threadId,
        batch,
        expectedTailSequence: tail.tailSequence,
        expectedTailDigest: tail.tailDigest,
        producerEpoch: tail.producerEpoch,
      }),
    );
  }
});

/** Adapter-only settled rows isolate ledger growth from canonical-history growth. */
const seedLedger = Effect.fn("benchmark.seedLedger")(function* (count: number) {
  const ledger = yield* SubmissionLedger;
  const inputDigest = yield* digestJson("fixture");
  const seedThread = Schema.decodeSync(ThreadId)("settled-ledger-fixture");

  for (let index = 0; index < count; index++) {
    const admitted = yield* ledger.admit(
      AdmissionRequest.make({
        threadId: seedThread,
        principal,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)(`seed-${index}`),
        agentId: Schema.decodeSync(AgentId)("runtime-benchmark"),
        agentDigests: definitions,
        deploymentId,
        inputPayload: "fixture",
        inputDigest,
      }),
    );

    yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
    const claim = yield* ledger.claim(ClaimRequest.make({ threadId: seedThread, producerId }));

    if (Option.isNone(claim)) return yield* check(false, "Ledger seed could not claim its row");
    const settlementId = submissionSettlementId(admitted.submissionId);

    const payload = yield* Schema.decodeUnknownEffect(SubmissionSettledRecord)(
      SubmissionSettled.make({
        submissionId: admitted.submissionId,
        settlementId,
        receiptId: admitted.receiptId,
        outcome: "aborted",
      }),
    );

    const record = RecordEnvelope.make({
      recordId: submissionSettlementRecordId(admitted.submissionId),
      family: "thread",
      schemaVersion: 1,
      createdAt: now,
      deploymentId,
      payload,
    });

    const recordDigest = yield* digestJson(yield* Schema.encodeEffect(RecordEnvelope)(record));

    yield* ledger.reserveSettlement(
      SettlementReservation.make({
        submissionId: admitted.submissionId,
        ownershipToken: claim.value.ownershipToken,
        settlementId,
        outcome: "aborted",
        record,
        recordDigest,
      }),
    );
    yield* ledger.finalizeSettlement(
      SettlementFinalization.make({ submissionId: admitted.submissionId, settlementId }),
    );
  }
  const pending = yield* Stream.runCollect(ledger.scanNonterminal);

  yield* check(pending.length === 0, "Settled fixture contains unfinished submissions");
});

/** Each sample owns a fresh script; request capture cannot grow between samples. */
export const runSample = Effect.fn("benchmark.runSample")(function* (
  workload: Case,
  ordinal: number,
  warmup: boolean,
  options: {
    readonly seeds?: SeedTemplates;
    readonly onProgress?: (
      progress: SampleProgress,
    ) => Effect.Effect<void, PlatformError, FileSystem.FileSystem>;
    readonly timeout?: Duration.Input;
  } = {},
) {
  const attemptStarted = yield* Clock.monotonicTimeNanos;
  let phase: SamplePhase = "setup";
  let started = 0n;
  let finished = 0n;
  let entered: bigint | undefined;
  let calls = 0;
  let finalized = 0;
  let toolCalls = 0;
  let activeTools = 0;
  let maxActiveTools = 0;
  let recovering = false;
  let checkpointStarted: bigint | undefined;
  let checkpointCreationMs: number | null = null;
  let retainedPromptMessages = 0;
  const priorText = "h".repeat(workload.historyBytes);
  const expectedRetainedRuns = workload.kind === "ledger" ? 0 : retainedRuns(workload.records);

  const expectedUsers = new Set(
    Array.from({ length: expectedRetainedRuns }, (_, index) =>
      JSON.stringify(retainedInput(index)),
    ),
  );

  const expectedAssistants = new Set(
    Array.from({ length: expectedRetainedRuns }, (_, index) =>
      JSON.stringify(retainedOutput(index)),
    ),
  );

  // outputBytes includes the JSON envelope, keeping total bytes fixed across fragmentation.
  const answer =
    workload.outputBytes === 0
      ? "ok"
      : "x".repeat(workload.outputBytes - JSON.stringify({ answer: "" }).length);

  const expectedBytes = JSON.stringify({ answer }).length;

  const changePhase = Effect.fn("benchmark.samplePhase")(function* (next: SamplePhase) {
    phase = next;
    yield* (
      options.onProgress?.({
        case: workload.name,
        ordinal,
        warmup,
        phase,
        elapsedMs: Number((yield* Clock.monotonicTimeNanos) - attemptStarted) / 1e6,
      }) ?? Effect.void
    );
  });

  // Evidence writes happen outside the measured interval, including before the start clock.
  const markStart = changePhase("operation").pipe(
    Effect.andThen(Clock.monotonicTimeNanos),
    Effect.tap((time) =>
      Effect.sync(() => {
        started = time;
      }),
    ),
  );

  const markFinish = Clock.monotonicTimeNanos.pipe(
    Effect.tap((time) =>
      Effect.sync(() => {
        finished = time;
      }),
    ),
    Effect.andThen(changePhase("verification")),
  );

  const script = (parts: ReadonlyArray<ScriptedStreamPart>): ScriptedTurnInput => ({
    _tag: "Stream",
    parts,
    termination: { _tag: "Complete" },
    assertRequest: (request) =>
      Effect.gen(function* () {
        entered ??= yield* Clock.monotonicTimeNanos;

        const userTexts = request.prompt.content
          .filter((message) => message.role === "user")
          .flatMap((message) => message.content)
          .filter((part) => part.type === "text")
          .map((part) => part.text);

        const assistantTexts = request.prompt.content
          .filter((message) => message.role === "assistant")
          .flatMap((message) => message.content)
          .filter((part) => part.type === "text")
          .map((part) => part.text);

        const retainedUsers = userTexts.filter((text) => text.startsWith('"retained input '));

        const retainedAssistants = assistantTexts.filter((text) =>
          text.startsWith('{"answer":"retained-'),
        );

        retainedPromptMessages = retainedUsers.length + retainedAssistants.length;

        const historyMatches =
          workload.historyBytes === 0 ||
          userTexts.filter((text) => text === priorText).length === 1;

        const retainedMatches = recovering
          ? retainedPromptMessages === 0 &&
            userTexts.some((text) => text.includes("Benchmark continuation"))
          : retainedUsers.length === expectedUsers.size &&
            new Set(retainedUsers).size === expectedUsers.size &&
            retainedUsers.every((text) => expectedUsers.has(text)) &&
            retainedAssistants.length === expectedAssistants.size &&
            new Set(retainedAssistants).size === expectedAssistants.size &&
            retainedAssistants.every((text) => expectedAssistants.has(text));

        if (!historyMatches || !retainedMatches || !userTexts.includes('"Answer"'))
          return yield* AiError.AiError.make({
            module: "runtime-benchmark",
            method: "assertRequest",
            reason: AiError.UnknownError.make({
              description: `Provider prompt lost or duplicated required history/input (history=${historyMatches}, retained=${retainedMatches}, retainedMessages=${retainedPromptMessages}, recovering=${recovering})`,
            }),
          });
        if (calls > 0 && workload.kind === "tools") {
          const results = request.prompt.content
            .filter((message) => message.role === "tool")
            .flatMap((message) => message.content)
            .filter((part) => part.type === "tool-result");

          if (
            results.length !== calls * 8 ||
            results.some(
              (part) => part.isFailure || part.result !== Number(part.id.split("-").at(-1)),
            )
          )
            return yield* AiError.AiError.make({
              module: "runtime-benchmark",
              method: "assertRequest",
              reason: AiError.UnknownError.make({
                description: "Scripted provider did not receive successful previous tool results",
              }),
            });
        }
        calls++;
      }),
    onStreamFinalize: Effect.sync(() => {
      finalized++;
    }),
  });

  const model = (turns: ReadonlyArray<ScriptedTurnInput>) =>
    Layer.mergeAll(
      ScriptedModel.layer(turns),
      Layer.succeed(Model.ProviderName, "scripted"),
      Layer.succeed(Model.ModelName, "runtime-benchmark"),
    );

  const handlers = toolkit.toLayer({
    work: ({ index }) =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          toolCalls++;
          activeTools++;
          maxActiveTools = Math.max(maxActiveTools, activeTools);
        }),
        () => Effect.sleep("2 millis").pipe(Effect.as(index)),
        () =>
          Effect.sync(() => {
            activeTools--;
          }),
      ),
  });

  const inspectScript = Effect.gen(function* () {
    const scripted = yield* ScriptedModel;

    yield* scripted.assertExhausted;
  });

  const verify = (output: unknown) =>
    Schema.decodeUnknownEffect(answerSchema)(output).pipe(
      Effect.flatMap((result) => check(result.answer === answer, "Incorrect final answer")),
    );

  const execute = Effect.gen(function* () {
    yield* changePhase("setup");
    if (["run", "stream", "tools"].includes(workload.kind)) {
      const turns = [
        ...Array.from({ length: workload.rounds }, (_, round) => script(toolParts(round, 8))),
        script(finalParts(answer, workload.chunks)),
      ];

      const history =
        workload.historyBytes === 0
          ? Prompt.empty
          : Prompt.make([{ role: "user", content: priorText }]);

      yield* Effect.gen(function* () {
        yield* markStart;
        if (workload.kind === "stream") {
          let completions = 0;
          let chunks = 0;
          let text = "";

          yield* AgentRuntime.stream(agent, "Answer", {
            history,
            bufferLimits: { maxModelResponseParts: 8_192 },
          }).pipe(
            Stream.runForEach((event) =>
              event._tag === "RunCompleted"
                ? verify(event.output).pipe(
                    Effect.tap(() =>
                      Effect.sync(() => {
                        completions++;
                      }),
                    ),
                  )
                : event._tag === "TextDelta"
                  ? Effect.sync(() => {
                      chunks++;
                      text += event.text;
                    })
                  : Effect.void,
            ),
          );
          yield* check(completions === 1, "Stream did not complete exactly once");
          yield* check(
            chunks === workload.chunks && text === JSON.stringify({ answer }),
            "Stream dropped or duplicated response chunks",
          );
        } else {
          const result = yield* AgentRuntime.run(agent, "Answer", { history });

          yield* verify(result.output);
          yield* check(result.turns === workload.rounds + 1, "Unexpected model turn count");
        }
        yield* markFinish;
        yield* inspectScript;
      }).pipe(
        Effect.provide(
          Layer.mergeAll(model(turns), handlers, IdGenerator.layer, ThreadHistory.layerTransient),
        ),
        Effect.scoped,
      );
      yield* check(calls === workload.rounds + 1, "Unexpected provider call count");
      yield* check(
        toolCalls === workload.rounds * 8 && activeTools === 0 && maxActiveTools <= 4,
        "Tool work or finalizers changed",
      );
      if (workload.rounds > 0)
        yield* check(maxActiveTools > 1, "Independent tools stopped overlapping");
    } else {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "runtime-benchmark-" });
      const filename = `${directory}/thread.sqlite`;

      const host = (crash: boolean, database = filename) =>
        NodeDurableAgentRuntime.layer({
          filename: database,
          deploymentId,
          producerId,
          ...(workload.kind === "recovery"
            ? {
                runContext: Layer.succeed(RunContextPreparation, {
                  hook: {
                    prepare: (request) =>
                      Effect.succeed({
                        prompt: request.source,
                        ...(request.turn === 2 &&
                        !JSON.stringify(request.source).includes("Benchmark continuation")
                          ? {
                              rollover: {
                                handoff: "Benchmark continuation",
                                through: request.source.content.length,
                              },
                            }
                          : {}),
                      }),
                  },
                }),
              }
            : {}),
          runtimeFailpoint: (location) =>
            Effect.gen(function* () {
              if (location === "compaction:after-canonical-append")
                checkpointStarted = yield* Clock.monotonicTimeNanos;
              if (location === "checkpoint:after-save") {
                if (checkpointStarted !== undefined)
                  checkpointCreationMs ??=
                    Number((yield* Clock.monotonicTimeNanos) - checkpointStarted) / 1e6;
                if (crash) return yield* DurableRuntimeFailpointError.make({ location });
              }
            }),
        }).pipe(Layer.provide(ContextCompactor.layerRollover));

      // Setup and seeding never enter the reported warm-operation interval.
      const initialize = Effect.fn("benchmark.initializeSeed")(
        function* (_database: string) {
          if (workload.kind === "ledger") yield* seedLedger(workload.records);
          else yield* seedHistory(workload.records);
        },
        (effect, database) => effect.pipe(Effect.provide(host(false, database)), Effect.scoped),
      );

      if (options.seeds === undefined) yield* initialize(filename);
      else
        yield* options.seeds.copy(
          `${workload.kind === "ledger" ? "ledger" : "history"}-${workload.records}`,
          filename,
          initialize,
        );

      const submit = Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        return yield* runtime.submit({ definition: agent }, "Answer", {
          threadId,
          principal,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("measured"),
          definitions,
        });
      });

      const finish = Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const receipt = yield* submit;

        if (workload.kind === "ledger") {
          const ledger = yield* SubmissionLedger;
          const pending = yield* Stream.runCollect(ledger.scanNonterminal);

          yield* check(
            pending.length === 1 && pending[0]?.submissionId === receipt.submissionId,
            "Ledger scan did not isolate the new submission",
          );
        }
        yield* runtime.processThread(binding, threadId);
        const settlement = yield* runtime.awaitSettlement(receipt);

        yield* markFinish;
        yield* check(settlement.outcome === "completed", "Durable submission did not complete");
        const store = yield* ThreadStore;
        const log = yield* store.export(ThreadExportRequest.make({ threadId }));

        const completed = log.records.filter(
          ({ record }) =>
            record.payload._tag === "RunCompleted" &&
            record.payload.runId === runIdForSubmission(receipt.submissionId),
        );

        yield* check(completed.length === 1, "Expected one canonical RunCompleted");
        yield* check(
          log.records.filter(({ record }) => record.recordId.startsWith("retained-")).length ===
            (workload.kind === "ledger" ? 0 : workload.records),
          "Retained canonical archive changed during the measured submission",
        );
        for (const envelope of completed)
          if (envelope.record.payload._tag === "RunCompleted")
            yield* verify(envelope.record.payload.output);
        yield* inspectScript;
      });

      if (workload.kind === "recovery") {
        yield* changePhase("checkpoint");
        yield* Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          yield* submit;
          const attempt = yield* runtime.processThread(binding, threadId).pipe(Effect.exit);

          const fault = Exit.isFailure(attempt)
            ? Cause.findErrorOption(attempt.cause)
            : Option.none();

          yield* check(
            Option.isSome(fault) &&
              fault.value._tag === "DurableRuntimeFailpointError" &&
              fault.value.location === "checkpoint:after-save",
            `Expected checkpoint fault was not observed: ${Exit.isFailure(attempt) ? Cause.pretty(attempt.cause) : "attempt succeeded"}`,
          );
          const store = yield* ThreadStore;

          const checkpoint =
            store.recoveryCheckpoints === undefined
              ? Option.none()
              : yield* store.recoveryCheckpoints.load(LoadCheckpointRequest.make({ threadId }));

          yield* check(Option.isSome(checkpoint), "Recovery fixture has no persisted checkpoint");
          yield* inspectScript;
        }).pipe(
          Effect.provide(Layer.mergeAll(host(true), model([script(toolParts(0, 1))]), handlers)),
          Effect.scoped,
        );
        yield* check(
          finalized === calls && activeTools === 0,
          "Checkpoint fault did not finalize model/tools",
        );
        calls = 0;
        finalized = 0;
        entered = undefined;
        recovering = true;
        yield* markStart;
        yield* finish.pipe(
          Effect.provide(
            Layer.mergeAll(host(false), model([script(finalParts(answer, 1))]), handlers),
          ),
          Effect.scoped,
        );
        yield* check(toolCalls === 1, "Checkpoint recovery repeated completed tools");
        yield* check(
          checkpointCreationMs !== null && checkpointCreationMs >= 0,
          "Missing inline checkpoint creation interval",
        );
      } else {
        yield* markStart;
        yield* finish.pipe(
          Effect.provide(
            Layer.mergeAll(host(false), model([script(finalParts(answer, 1))]), handlers),
          ),
          Effect.scoped,
        );
      }
      yield* check(calls === 1, "Durable submission made extra model calls");
    }
    yield* check(
      finalized === calls && activeTools === 0,
      "Model/tool resources were not finalized",
    );
    yield* check(entered !== undefined && finished >= started, "Missing timing boundary");
  }).pipe(
    Effect.scoped,
    Effect.timeout(options.timeout ?? "3 minutes"),
    Effect.provideService(References.MinimumLogLevel, "None"),
  );

  const result = yield* execute.pipe(Effect.exit);

  if (finished === 0n) finished = yield* Clock.monotonicTimeNanos;
  const attemptFinished = yield* Clock.monotonicTimeNanos;

  const sample: Sample = {
    case: workload.name,
    ordinal,
    warmup,
    totalMs: started === 0n ? 0 : Number(finished - started) / 1e6,
    attemptMs: Number(attemptFinished - attemptStarted) / 1e6,
    setupMs: Number((started === 0n ? attemptFinished : started) - attemptStarted) / 1e6,
    failurePhase: Exit.isFailure(result) ? phase : null,
    modelEntryMs: entered === undefined || started === 0n ? null : Number(entered - started) / 1e6,
    checkpointCreationMs,
    retainedPromptMessages,
    modelCalls: calls,
    finalizers: finalized,
    toolCalls,
    outputBytes: expectedBytes,
    status: Exit.isSuccess(result) ? "passed" : "failed",
    failure: Exit.isFailure(result) ? Cause.pretty(result.cause) : null,
  };

  return sample;
});
