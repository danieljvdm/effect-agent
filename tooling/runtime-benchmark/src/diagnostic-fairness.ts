import { ThreadId } from "@effect-agent/core/Identifiers";
import { NodeDurableHost } from "@effect-agent/platform-node/NodeDurableHost";
import { ScriptedModel, type ScriptedStreamPart } from "@effect-agent/testing/ScriptedModel";
import { type AgentAttemptContext } from "@effect-agent/thread/AgentRegistration";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { type Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import {
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  SubmissionLedger,
} from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { NodeCrypto } from "@effect/platform-node";
import { Clock, Deferred, Effect, Fiber, FileSystem, Layer, Path, Schema } from "effect";
import { Agent } from "effect-agent";
import { AiError, Model, Tool, Toolkit } from "effect/unstable/ai";

import { BenchmarkError, check } from "./contracts.js";
import { fairnessCases } from "./diagnostic-cases.js";
import {
  DiagnosticProgress,
  type DiagnosticCase,
  type DiagnosticResult,
} from "./diagnostic-contracts.js";

export { fairnessCases } from "./diagnostic-cases.js";

const work = Tool.make("fairness_work", {
  parameters: Schema.Struct({ index: Schema.Natural }),
  success: Schema.Natural,
});

const toolkit = Toolkit.make(work);
const principal = Principal.make("fairness-diagnostic");
const labels = ["busy0", "busy1", "busy2", "busy3", "short"] as const;
const encodeInput = Schema.encodeSync(Schema.fromJsonString(Schema.String));
const FairnessOutput = Schema.Struct({ answer: Schema.String });

const finalParts = (label: string): ReadonlyArray<ScriptedStreamPart> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify({ answer: label }) },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

/**
 * Existing registered Node workers only: one independent Submission per lane, no joining or
 * preemption claim. Request-to-Attempt includes admission, since the public API does not expose
 * the exact ready timestamp. Settlement time is when awaitSettlement observes it (including
 * wake/poll costs), not the canonical commit time. Host acquisition/close and verification are
 * outside totalMs; submitted work, worker startup/stop, and in-memory marks are inside it.
 */
export const runFairnessCase = Effect.fn("diagnostic.fairness")(
  function* (workload: DiagnosticCase) {
    const selected = fairnessCases.find((entry) => entry.name === workload.name);

    yield* check(
      selected !== undefined &&
        workload.family === "fairness" &&
        Object.keys(workload.parameters).length === Object.keys(selected.parameters).length &&
        Object.entries(selected.parameters).every(
          ([key, value]) => workload.parameters[key] === value,
        ),
      "Unknown or modified fairness diagnostic case",
    );
    if (selected === undefined)
      return yield* BenchmarkError.make({ message: "Unknown fairness diagnostic case" });

    const concurrency = selected.parameters.workerConcurrency!;
    const warmArrival = selected.parameters.warmArrival === 1;
    const progress = yield* DiagnosticProgress;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    yield* progress.phase("setup");
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "fairness-diagnostic-" });
    const firstBusyTool = yield* Deferred.make<void>();
    let started = 0n;
    let activeAttempts = 0;
    let maxAttempts = 0;
    let attemptsOpened = 0;
    let attemptsClosed = 0;
    let activeTools = 0;
    let maxTools = 0;
    let toolsOpened = 0;
    let toolsClosed = 0;
    let modelsOpened = 0;
    let modelsClosed = 0;
    let providers = 0;
    let streamsClosed = 0;
    let shortAdmissionActiveAttempts = 0;
    let shortAdmissionActiveTools = 0;

    const stamp = Effect.fn("diagnostic.fairnessMark")(function* (name: string) {
      const elapsedMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;

      yield* progress.mark({ name, elapsedMs });

      return elapsedMs;
    });

    const lanes = labels.map((label) => ({
      label,
      busy: label !== "short",
      threadId: ThreadId.make(label === "short" ? "z-short" : `a-${label}`),
      input: `Run ${label}`,
      encodedInput: encodeInput(`Run ${label}`),
      requestAt: -1,
      acceptedAt: -1,
      attemptAt: -1,
      providerAt: -1,
      observedAt: -1,
      closedAt: -1,
      active: 0,
      maxActive: 0,
      toolCalls: 0,
      claimedSubmissionId: "",
      receipt: undefined as Receipt | undefined,
    }));

    const registrations = lanes.map((lane) => {
      const definition = Agent.make(`fairness-${lane.label}`, {
        input: Schema.String,
        output: FairnessOutput,
        instructions: "Perform the requested work and answer.",
        toolkit,
        policy: { maxTurns: 3, maxToolCalls: 4, maxDuration: "20 seconds", toolConcurrency: 2 },
      });

      const parts: ReadonlyArray<ReadonlyArray<ScriptedStreamPart>> = lane.busy
        ? [
            [
              ...Array.from({ length: 4 }, (_, index) => ({
                type: "tool-call" as const,
                id: `${lane.label}-${index}`,
                name: "fairness_work",
                params: { index },
                providerExecuted: false,
              })),
              {
                type: "finish",
                reason: "tool-calls",
                usage: { inputTokens: {}, outputTokens: {} },
              },
            ],
            finalParts(lane.label),
          ]
        : [finalParts(lane.label)];

      const model = Layer.mergeAll(
        ScriptedModel.layer(
          parts.map((parts, turn) => ({
            _tag: "Stream" as const,
            parts,
            termination: { _tag: "Complete" as const },
            assertRequest: (request) =>
              Effect.gen(function* () {
                const at = yield* stamp(`provider.${lane.label}.${turn}`);

                if (lane.providerAt < 0) lane.providerAt = at;
                providers++;

                const users = request.prompt.content
                  .filter((message) => message.role === "user")
                  .flatMap((message) => message.content)
                  .filter((part) => part.type === "text")
                  .map((part) => part.text);

                const results = request.prompt.content
                  .filter((message) => message.role === "tool")
                  .flatMap((message) => message.content)
                  .filter((part) => part.type === "tool-result");

                yield* check(
                  users.includes(lane.encodedInput) &&
                    results.length === turn * 4 &&
                    results.every(
                      (part, index) =>
                        part.id === `${lane.label}-${index}` &&
                        !part.isFailure &&
                        part.result === index,
                    ),
                  "Fairness provider lost, reordered, or duplicated input/tool results",
                );
              }).pipe(
                Effect.mapError((cause) =>
                  AiError.AiError.make({
                    module: "fairness-diagnostic",
                    method: "assertRequest",
                    reason: AiError.UnknownError.make({ description: cause.message }),
                  }),
                ),
              ),
            onStreamFinalize: Effect.sync(() => {
              streamsClosed++;
            }),
          })),
        ),
        Layer.succeed(Model.ProviderName, "scripted"),
        Layer.succeed(Model.ModelName, `fairness-${lane.label}`),
        Layer.effectDiscard(
          Effect.acquireRelease(
            Effect.sync(() => {
              modelsOpened++;
            }),
            () =>
              Effect.sync(() => {
                modelsClosed++;
              }).pipe(Effect.andThen(stamp(`model.closed.${lane.label}`)), Effect.orDie),
          ).pipe(Effect.tap(() => stamp(`model.open.${lane.label}`).pipe(Effect.orDie))),
        ),
      );

      return {
        agent: Agent.withModel(definition, model),
        definitions: DefinitionDigestInput.make({
          agent: `fairness-${lane.label}-v1`,
          model: "scripted-v1",
          tools: ["work-v1"],
        }),
        attemptLayer: ({ threadId, submissionId }: AgentAttemptContext) =>
          toolkit.toLayer(
            Effect.gen(function* () {
              yield* check(threadId === lane.threadId, "Attempt resolved the wrong lane").pipe(
                Effect.orDie,
              );
              // Acquisition itself cannot block: the release is registered before any hook runs.
              yield* Effect.acquireRelease(
                Effect.sync(() => {
                  lane.active++;
                  lane.maxActive = Math.max(lane.maxActive, lane.active);
                  activeAttempts++;
                  attemptsOpened++;
                  maxAttempts = Math.max(maxAttempts, activeAttempts);
                }),
                () =>
                  Effect.gen(function* () {
                    lane.active--;
                    activeAttempts--;
                    attemptsClosed++;
                    lane.closedAt = yield* stamp(`attempt.closed.${lane.label}`);
                  }).pipe(Effect.orDie),
              );
              lane.attemptAt = yield* stamp(`attempt.open.${lane.label}`).pipe(Effect.orDie);
              lane.claimedSubmissionId = submissionId;
              // A warm worker may claim before host.submit returns its Receipt.
              if (lane.receipt !== undefined)
                yield* check(
                  lane.receipt.submissionId === submissionId,
                  "Attempt identity mismatch",
                ).pipe(Effect.orDie);

              return {
                fairness_work: ({ index }: { index: number }) =>
                  Effect.acquireUseRelease(
                    Effect.sync(() => {
                      lane.toolCalls++;
                      activeTools++;
                      toolsOpened++;
                      maxTools = Math.max(maxTools, activeTools);
                    }),
                    () =>
                      Effect.gen(function* () {
                        yield* stamp(`tool.open.${lane.label}.${index}`);
                        yield* Deferred.succeed(firstBusyTool, undefined);
                        yield* Effect.sleep(100);

                        return index;
                      }),
                    () =>
                      Effect.gen(function* () {
                        activeTools--;
                        toolsClosed++;
                        yield* stamp(`tool.closed.${lane.label}.${index}`);
                      }).pipe(Effect.orDie),
                  ).pipe(Effect.orDie),
              };
            }),
          ),
      };
    });

    const digests = yield* Effect.forEach(registrations, (entry) =>
      digestDefinitions(entry.definitions),
    ).pipe(Effect.provide(NodeCrypto.layer));

    const hostLayer = NodeDurableHost.layerRegistered(registrations, {
      filename: path.join(directory, "fairness.sqlite"),
      deploymentId: "fairness-diagnostic",
      producerId: "fairness-diagnostic",
      workerConcurrency: concurrency,
    });

    const result = yield* Effect.gen(function* () {
      const host = yield* NodeDurableHost;
      const store = yield* ThreadStore;
      const ledger = yield* SubmissionLedger;

      const submit = Effect.fn("diagnostic.fairnessSubmit")(function* (index: number) {
        const lane = lanes[index]!;

        lane.requestAt = yield* stamp(`request.${lane.label}`);
        if (!lane.busy) {
          shortAdmissionActiveAttempts = activeAttempts;
          shortAdmissionActiveTools = activeTools;
        }
        lane.receipt = yield* host.submit(registrations[index]!.agent, lane.input, {
          threadId: lane.threadId,
          principal,
          idempotencyKey: IdempotencyKey.make(`input-${lane.label}`),
          definitions: digests[index]!,
        });
        lane.acceptedAt = yield* stamp(`accepted.${lane.label}`);
      });

      yield* progress.phase("operation");
      started = yield* Clock.monotonicTimeNanos;
      yield* Effect.scoped(
        Effect.gen(function* () {
          // runResolvedWorkers already creates C workers; fork exactly one pool.
          const worker = warmArrival
            ? yield* host.runResolvedWorkers.pipe(Effect.forkScoped)
            : undefined;

          for (let index = 0; index < 4; index++) yield* submit(index);
          if (warmArrival) {
            yield* Deferred.await(firstBusyTool);
            yield* check(activeTools > 0, "Warm short arrival missed the active busy workload");
          }
          yield* submit(4);
          const pool = worker ?? (yield* host.runResolvedWorkers.pipe(Effect.forkScoped));

          yield* Effect.raceFirst(
            Effect.forEach(
              lanes,
              (lane) =>
                Effect.gen(function* () {
                  const settlement = yield* host.awaitSettlement(lane.receipt!);

                  lane.observedAt = yield* stamp(`settlement.observed.${lane.label}`);
                  yield* check(
                    settlement.outcome === "completed",
                    "Fairness lane did not complete",
                  );
                }),
              { concurrency: 5, discard: true },
            ),
            Fiber.join(pool).pipe(
              Effect.andThen(
                Effect.fail(
                  BenchmarkError.make({ message: "Worker pool exited before all settlements" }),
                ),
              ),
            ),
          );
          yield* Fiber.interrupt(pool);
        }),
      );
      const totalMs = Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;

      yield* progress.phase("verification");
      yield* check(
        attemptsOpened === 5 &&
          attemptsClosed === 5 &&
          activeAttempts === 0 &&
          maxAttempts <= concurrency &&
          toolsOpened === 16 &&
          toolsClosed === 16 &&
          activeTools === 0 &&
          maxTools <= concurrency * 2 &&
          providers === 9 &&
          streamsClosed === providers,
        "Fairness work, worker bounds, or resource finalizers changed",
      );
      for (const lane of lanes) {
        const log = yield* store.export(ThreadExportRequest.make({ threadId: lane.threadId }));
        const payloads = log.records.map(({ record }) => record.payload);

        const inputs = payloads.filter((payload) => payload._tag === "UserInputRecorded");
        const starts = payloads.filter((payload) => payload._tag === "RunStarted");
        const completions = payloads.filter((payload) => payload._tag === "RunCompleted");
        const settlements = payloads.filter((payload) => payload._tag === "SubmissionSettled");
        const runStarted = starts[0];
        const runCompleted = completions[0];
        const settlement = settlements[0];

        if (
          starts.length !== 1 ||
          completions.length !== 1 ||
          settlements.length !== 1 ||
          runStarted === undefined ||
          runCompleted === undefined ||
          settlement === undefined
        )
          return yield* BenchmarkError.make({
            message:
              "Fairness lane did not retain exactly one started/completed Run and Settlement",
          });

        const output = yield* Schema.decodeUnknownEffect(FairnessOutput)(runCompleted.output);
        const settledOutput = yield* Schema.decodeUnknownEffect(FairnessOutput)(settlement.result);

        const snapshot = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: lane.receipt!.submissionId }),
        );

        yield* check(
          lane.maxActive === 1 &&
            lane.active === 0 &&
            lane.attemptAt >= lane.requestAt &&
            lane.providerAt >= lane.attemptAt &&
            lane.observedAt >= lane.providerAt &&
            lane.closedAt >= lane.attemptAt &&
            lane.toolCalls === (lane.busy ? 4 : 0) &&
            inputs.length === 1 &&
            inputs[0]?.submissionId === lane.receipt!.submissionId &&
            inputs[0]?.runId === runStarted.runId &&
            runCompleted.runId === runStarted.runId &&
            settlement.runId === runStarted.runId &&
            settlement.outcome === "completed" &&
            output.answer === lane.label &&
            settledOutput.answer === lane.label &&
            lane.claimedSubmissionId === lane.receipt!.submissionId &&
            settlement.submissionId === lane.receipt!.submissionId &&
            snapshot.ownership === undefined &&
            snapshot.reservation?.finalized === true &&
            snapshot.hostSubmissionId === undefined,
          "Fairness lane identity, canonical work, finalization, or timestamp mismatch",
        );
      }

      return {
        totalMs,
        metrics: lanes.flatMap((lane) => [
          { name: `${lane.label}.admission`, value: lane.acceptedAt - lane.requestAt },
          { name: `${lane.label}.requestToAttempt`, value: lane.attemptAt - lane.requestAt },
          { name: `${lane.label}.requestToProvider`, value: lane.providerAt - lane.requestAt },
          {
            name: `${lane.label}.requestToObservedSettlement`,
            value: lane.observedAt - lane.requestAt,
          },
          { name: `${lane.label}.attemptLifetime`, value: lane.closedAt - lane.attemptAt },
          { name: `${lane.label}.settlementObservedAt`, value: lane.observedAt },
        ]),
        counters: [
          { name: "maxActiveAttempts", value: maxAttempts },
          { name: "attemptsOpened", value: attemptsOpened },
          { name: "attemptFinalizers", value: attemptsClosed },
          { name: "maxActiveTools", value: maxTools },
          { name: "toolsOpened", value: toolsOpened },
          { name: "toolFinalizers", value: toolsClosed },
          { name: "providers", value: providers },
          { name: "streamFinalizers", value: streamsClosed },
          { name: "shortAdmissionActiveAttempts", value: shortAdmissionActiveAttempts },
          { name: "shortAdmissionActiveTools", value: shortAdmissionActiveTools },
          ...lanes.map((lane) => ({
            name: `${lane.label}.maxActiveAttempts`,
            value: lane.maxActive,
          })),
          ...lanes.map((lane) => ({
            name: `${lane.label}.queueSequence`,
            value: lane.receipt!.queueSequence,
          })),
        ],
      } satisfies DiagnosticResult;
    }).pipe(Effect.provide(hostLayer), Effect.scoped);

    yield* check(
      modelsOpened === 5 && modelsClosed === modelsOpened,
      "Host model resources did not close",
    );

    return {
      ...result,
      counters: [
        ...result.counters,
        { name: "modelsOpened", value: modelsOpened },
        { name: "modelFinalizers", value: modelsClosed },
      ],
    } satisfies DiagnosticResult;
  },
  Effect.scoped,
  Effect.mapError((cause) => BenchmarkError.make({ message: "Fairness diagnostic failed", cause })),
);
