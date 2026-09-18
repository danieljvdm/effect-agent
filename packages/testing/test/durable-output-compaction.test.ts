import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import {
  CLEARED_TOOL_RESULT,
  CONTEXT_ROLLOVER_PREFIX,
  estimatePromptTokens,
} from "effect-agent/compaction";
import {
  CompactionError,
  CompactionEvaluator,
  ContextCompactor,
  type CompactionRequest,
} from "effect-agent/context-compactor";
import { ContextRolloverRequest, ContextRolloverTool } from "effect-agent/context-window";
import { DurableAgentRuntime, DurableRuntimeConfig } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpointError } from "effect-agent/durable-failpoint";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { ThreadId } from "effect-agent/identifiers";
import * as Output from "effect-agent/output";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "effect-agent/records";
import { projectRunJournal, runIdForSubmission } from "effect-agent/run-journal";
import { RunToolAuthorization, type RunCostEstimator } from "effect-agent/run-options";
import { IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "effect-agent/testing/durable-failpoint-test-control";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Prompt, type Response, Tool, Toolkit } from "effect/unstable/ai";

const digest = Digest.make("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const makeTestLayer = (
  compactor: Layer.Layer<ContextCompactor> = ContextCompactor.layer,
  estimateCostMicrousd?: RunCostEstimator,
) =>
  DurableAgentRuntime.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        MemorySubmissionLedgerLive,
        compactor,
        MemoryThreadStoreLive,
        WakeScheduler.layerNoop,
        ToolReconciler.uncertain,
        RunToolAuthorization.allowAll,
        DurableRuntimeFailpointTestControl.layer,
        DurableRuntimeConfig.layer({
          ...(estimateCostMicrousd === undefined ? {} : { estimateCostMicrousd }),
          deploymentId: DeploymentId.make("output-compaction"),
          producerId: ProducerId.make("output-compaction"),
        }),
      ).pipe(Layer.provideMerge(NodeCrypto.layer)),
    ),
  );

const testLayer = makeTestLayer();

const submitOptions = (id: string) => ({
  threadId: ThreadId.make(id),
  principal: Principal.make("output-compaction"),
  idempotencyKey: IdempotencyKey.make(id),
  definitions,
});

layer(
  makeTestLayer(
    Layer.succeed(ContextCompactor, {
      estimate: estimatePromptTokens,
      compact: (request) => {
        const messageIndex = request.source.content.findIndex(
          (message) =>
            message.role === "tool" &&
            message.content.some(
              (part) =>
                part.type === "tool-result" &&
                part.id === "noise" &&
                part.result !== CLEARED_TOOL_RESULT,
            ),
        );

        return messageIndex < 0
          ? Stream.empty
          : Stream.succeed({
              kind: "clear-tool-results",
              through: messageIndex + 1,
              results: [{ messageIndex, toolCallId: "noise" }],
            });
      },
    }),
  ),
)("durable selective pruning", (it) => {
  for (const barrier of [
    "compaction:before-canonical-append",
    "compaction:after-canonical-append",
  ] as const) {
    it.effect(`replays sparse selection across ${barrier}`, () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const control = yield* DurableRuntimeFailpointTestControl;

        const toolkit = Toolkit.make(
          Tool.make("search", {
            parameters: Schema.Struct({ key: Schema.String }),
            success: Schema.String,
          }),
        );

        const evidence: Record<string, string> = {
          receipt: "RECEIPT-42 " + "r".repeat(1_000),
          noise: "noise " + "n".repeat(5_000),
          newest: "newest " + "x".repeat(1_000),
        };

        let executions = 0;

        const handlers = toolkit.toLayer({
          search: ({ key }) =>
            Effect.sync(() => {
              executions++;

              return evidence[key] ?? "unexpected";
            }),
        });

        const scripted = scriptedModel((call) =>
          call === 0
            ? [
                {
                  type: "tool-call",
                  id: "receipt",
                  name: "search",
                  params: { key: "receipt" },
                  providerExecuted: false,
                },
                {
                  type: "tool-call",
                  id: "noise",
                  name: "search",
                  params: { key: "noise" },
                  providerExecuted: false,
                },
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: { ...usage, inputTokens: { total: 100 } },
                },
              ]
            : call === 1
              ? callParts("newest", "search", { key: "newest" }, 1_800)
              : finalParts("Finished."),
        );

        const agent = Agent.withModel(
          Agent.make("selective-pruning", {
            input: Schema.String,
            output: Output.text(Schema.String),
            instructions: "Retain the receipt.",
            toolkit,
            policy: {
              maxTurns: 5,
              contextTokenLimit: 2_000,
              compaction: { mode: "prune", keepRecentTokens: 1 },
            },
          }),
          scripted.model,
        );

        const receipt = yield* runtime.submit(
          agent,
          "ORIGINAL-INPUT",
          submitOptions(`selective-${barrier}`),
        );

        const process = runtime
          .processThread(agent, receipt.threadId)
          .pipe(Effect.provide(handlers));

        yield* control.setHandler((location) =>
          location === barrier
            ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
            : Effect.void,
        );
        expectCrash(yield* process.pipe(Effect.exit, Effect.ensuring(control.clear)));
        expect(executions).toBe(3);
        const settled = yield* process;

        expect(settled[0]?.outcome).toBe("completed");
        expect(executions).toBe(3);
        expect(results(scripted.prompts.at(-1)!)).toEqual([
          evidence.receipt,
          CLEARED_TOOL_RESULT,
          evidence.newest,
        ]);
        const records = yield* readLog(receipt.threadId);

        const prunes = records.flatMap(({ record }) =>
          record.payload._tag === "CompactionCreated" ? [record.payload] : [],
        );

        const noise = records.find(
          ({ record }) =>
            record.payload._tag === "ToolCallSettled" && record.payload.toolCallId === "noise",
        );

        expect(prunes).toHaveLength(1);
        expect(prunes[0]?.toolResultRecordIds).toEqual([noise?.record.recordId]);
        expect(JSON.stringify(records)).toContain(evidence.noise);
        const replay = yield* projectRunJournal(records, runIdForSubmission(receipt.submissionId));

        expect(results(replay.prompt)).toEqual([
          evidence.receipt,
          CLEARED_TOOL_RESULT,
          evidence.newest,
        ]);
      }),
    );
  }
});

let auxiliaryEvaluations = 0;
let auxiliaryFinalizers = 0;
let auxiliaryOutcome = "success";
let auxiliaryEntered: Deferred.Deferred<void> | undefined;

layer(
  makeTestLayer(
    Layer.succeed(ContextCompactor, {
      estimate: estimatePromptTokens,
      compact: <E, R>(request: CompactionRequest<E, R>) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const evaluator = yield* CompactionEvaluator<E, R>();

            if (evaluator.available) {
              yield* evaluator.evaluate(
                Effect.acquireUseRelease(
                  Effect.void,
                  () =>
                    Effect.gen(function* () {
                      auxiliaryEvaluations++;
                      if (auxiliaryOutcome === "provider-failure")
                        return yield* CompactionError.make({ message: "Selector unavailable" });
                      if (auxiliaryOutcome === "provider-defect")
                        return yield* Effect.die("selector defect");
                      if (auxiliaryOutcome === "provider-interruption")
                        return yield* Effect.interrupt;
                      if (auxiliaryOutcome === "provider-timeout") {
                        if (auxiliaryEntered !== undefined)
                          yield* Deferred.succeed(auxiliaryEntered, undefined);

                        return yield* Effect.never.pipe(
                          Effect.timeoutOrElse({
                            duration: "1 millis",
                            orElse: () => CompactionError.make({ message: "Selector timeout" }),
                          }),
                        );
                      }

                      return {
                        value: undefined,
                        provider: "selector",
                        model: "test-selector",
                        usage: {
                          inputTokens: {
                            total: auxiliaryOutcome === "provider-invalid-usage" ? -1 : 70,
                          },
                          outputTokens: { total: 2 },
                        },
                      };
                    }),
                  () =>
                    Effect.sync(() => {
                      auxiliaryFinalizers++;
                    }),
                ),
                100,
              );
            }
            if (auxiliaryOutcome === "empty") return Stream.empty;
            if (auxiliaryOutcome === "invalid")
              return Stream.succeed({
                kind: "clear-tool-results",
                through: 1,
                results: [{ messageIndex: 0, toolCallId: "missing" }],
              });

            const messageIndex = request.source.content.findIndex(
              (message) =>
                message.role === "tool" &&
                message.content.some(
                  (part) =>
                    part.type === "tool-result" &&
                    part.id === "noise" &&
                    part.result !== CLEARED_TOOL_RESULT,
                ),
            );

            return messageIndex < 0
              ? Stream.empty
              : Stream.succeed({
                  kind: "clear-tool-results",
                  through: messageIndex + 1,
                  results: [{ messageIndex, toolCallId: "noise" }],
                });
          }),
        ),
    }),
    (_usage, request) => Effect.succeed(request.purpose === "compaction" ? 7 : 1),
  ),
)("durable auxiliary compaction accounting", (it) => {
  for (const barrier of [
    "cost-limit",
    "empty",
    "invalid",
    "provider-failure",
    "provider-invalid-usage",
    "provider-defect",
    "provider-interruption",
    "provider-timeout",
    "compaction:before-evaluation-reserve",
    "compaction:after-evaluation-reserve",
    "compaction:before-evaluation-accounting",
    "compaction:after-evaluation-accounting",
    "compaction:before-canonical-append",
    "compaction:after-canonical-append",
  ] as const) {
    it.effect(`preserves paid selection accounting across ${barrier}`, () =>
      Effect.gen(function* () {
        auxiliaryEvaluations = 0;
        auxiliaryFinalizers = 0;
        auxiliaryOutcome = barrier;
        const entered = yield* Deferred.make<void>();

        auxiliaryEntered = entered;
        const runtime = yield* DurableAgentRuntime;
        const control = yield* DurableRuntimeFailpointTestControl;

        const toolkit = Toolkit.make(
          Tool.make("search", {
            parameters: Schema.Struct({ key: Schema.String }),
            success: Schema.String,
          }),
        );

        const evidence: Record<string, string> = {
          receipt: "RECEIPT-42 " + "r".repeat(1_000),
          noise: "noise " + "n".repeat(5_000),
          newest: "newest " + "x".repeat(1_000),
        };

        let executions = 0;

        const handlers = toolkit.toLayer({
          search: ({ key }) =>
            Effect.sync(() => {
              executions++;

              return evidence[key] ?? "unexpected";
            }),
        });

        const scripted = scriptedModel((call) =>
          call === 0
            ? [
                {
                  type: "tool-call",
                  id: "receipt",
                  name: "search",
                  params: { key: "receipt" },
                  providerExecuted: false,
                },
                {
                  type: "tool-call",
                  id: "noise",
                  name: "search",
                  params: { key: "noise" },
                  providerExecuted: false,
                },
                {
                  type: "finish",
                  reason: "tool-calls",
                  usage: { ...usage, inputTokens: { total: 100 } },
                },
              ]
            : call === 1
              ? callParts("newest", "search", { key: "newest" }, 1_800)
              : finalParts("Finished."),
        );

        const agent = Agent.withModel(
          Agent.make("selective-pruning", {
            input: Schema.String,
            output: Output.text(Schema.String),
            instructions: "Retain the receipt.",
            toolkit,
            policy: {
              ...(auxiliaryOutcome === "cost-limit" ? { costBudgetMicrousd: 5 } : {}),
              maxTurns: 5,
              contextTokenLimit: 2_000,
              compaction: { mode: "prune", keepRecentTokens: 1 },
            },
          }),
          scripted.model,
        );

        const receipt = yield* runtime.submit(
          agent,
          "ORIGINAL-INPUT",
          submitOptions(`auxiliary-${barrier}`),
        );

        const process = runtime
          .processThread(agent, receipt.threadId)
          .pipe(Effect.provide(handlers));

        yield* control.setHandler((location) =>
          location === barrier
            ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
            : Effect.void,
        );
        if (!barrier.startsWith("compaction:")) {
          if (barrier === "provider-defect" || barrier === "provider-interruption") {
            expect(Exit.isFailure(yield* Effect.exit(process))).toBe(true);
          }

          const completed = yield* (
            barrier === "provider-timeout"
              ? Effect.gen(function* () {
                  const fiber = yield* Effect.forkChild(process);

                  yield* Deferred.await(entered);
                  yield* TestClock.adjust("1 second");

                  return yield* Fiber.join(fiber);
                })
              : process
          ).pipe(Effect.ensuring(control.clear));

          expect(completed[0]?.outcome).toBe("failed");
          expect(auxiliaryEvaluations).toBe(1);
          expect(auxiliaryFinalizers).toBe(1);
          const records = yield* readLog(receipt.threadId);

          expect(
            records.filter(({ record }) => record.payload._tag === "CompactionCreated"),
          ).toHaveLength(0);
          if (barrier === "cost-limit") {
            expect(completed[0]?.usageSummary?.costMicrousd).toBe(9);
            expect(records.at(-1)?.record.payload).toMatchObject({ policyLimit: "cost" });
          }

          const usageRecorded = !barrier.startsWith("provider-");

          expect(
            records.filter(({ record }) => record.payload._tag === "CompactionEvaluationRecorded"),
          ).toHaveLength(usageRecorded ? 1 : 0);
          expect(
            completed[0]?.usageSummary?.byModel.filter((group) => group.provider === "selector"),
          ).toHaveLength(usageRecorded ? 1 : 0);
          if (!usageRecorded) expect(completed[0]?.usageSummary?.unobservedModelCalls).toBe(1);
          expect(scripted.prompts).toHaveLength(2);

          return;
        }
        expectCrash(yield* process.pipe(Effect.exit, Effect.ensuring(control.clear)));
        expect(executions).toBe(3);
        expect(auxiliaryEvaluations).toBe(
          barrier === "compaction:before-evaluation-reserve" ||
            barrier === "compaction:after-evaluation-reserve"
            ? 0
            : 1,
        );
        const settled = yield* process;

        const unresolved =
          barrier === "compaction:after-evaluation-reserve" ||
          barrier === "compaction:before-evaluation-accounting";

        expect(auxiliaryEvaluations).toBe(
          barrier === "compaction:after-evaluation-reserve" ? 0 : 1,
        );
        expect(settled[0]?.outcome).toBe(unresolved ? "failed" : "completed");
        expect(auxiliaryFinalizers).toBe(auxiliaryEvaluations);
        const accountingRecords = yield* readLog(receipt.threadId);

        const accounting = accountingRecords.filter(
          ({ record }) => record.payload._tag === "CompactionEvaluationRecorded",
        );

        expect(accounting).toHaveLength(unresolved ? 0 : 1);

        const replayAccounting = yield* projectRunJournal(
          accountingRecords,
          runIdForSubmission(receipt.submissionId),
        );

        expect(
          replayAccounting.usage.modelUsage.filter((call) => call.purpose === "compaction"),
        ).toHaveLength(unresolved ? 0 : 1);
        expect(
          settled[0]?.usageSummary?.byModel.filter((group) => group.provider === "selector"),
        ).toMatchObject(
          unresolved
            ? []
            : [{ modelCalls: 1, inputTokens: { total: 70 }, outputTokens: { total: 2 } }],
        );
        if (unresolved) {
          expect(settled[0]?.usageSummary?.unobservedModelCalls).toBe(1);
          expect(accountingRecords.at(-1)?.record.payload).toMatchObject({ policyLimit: "usage" });
          expect(scripted.prompts).toHaveLength(2);

          return;
        }
        expect(executions).toBe(3);
        expect(results(scripted.prompts.at(-1)!)).toEqual([
          evidence.receipt,
          CLEARED_TOOL_RESULT,
          evidence.newest,
        ]);
        const records = yield* readLog(receipt.threadId);

        const prunes = records.flatMap(({ record }) =>
          record.payload._tag === "CompactionCreated" ? [record.payload] : [],
        );

        const noise = records.find(
          ({ record }) =>
            record.payload._tag === "ToolCallSettled" && record.payload.toolCallId === "noise",
        );

        expect(prunes).toHaveLength(1);
        expect(prunes[0]?.toolResultRecordIds).toEqual([noise?.record.recordId]);
        expect(JSON.stringify(records)).toContain(evidence.noise);
        const replay = yield* projectRunJournal(records, runIdForSubmission(receipt.submissionId));

        expect(results(replay.prompt)).toEqual([
          evidence.receipt,
          CLEARED_TOOL_RESULT,
          evidence.newest,
        ]);
      }),
    );
  }
});

const usage = { inputTokens: { total: 1_300 }, outputTokens: { total: 10 } };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const callParts = (
  id: string,
  name: string,
  params: Schema.Json,
  inputTokens = 100,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "tool-call", id, name, params, providerExecuted: false },
  {
    type: "finish",
    reason: "tool-calls",
    usage: { ...usage, inputTokens: { total: inputTokens } },
  },
];

// Model calls and captured requests outlive rebuilt Layers across Attempts.
const scriptedModel = (script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>) => {
  const prompts: Array<Prompt.Prompt> = [];

  const model = Model.make(
    "test",
    "output-compaction",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          const call = prompts.length;

          prompts.push(request.prompt);

          return Stream.fromIterable(script(call));
        },
      }),
    ),
  );

  return { model, prompts };
};

const readLog = Effect.fn("readLog")(function* (threadId: ThreadId) {
  const store = yield* ThreadStore;

  return yield* store.read(ThreadRead.make({ threadId, limit: 1_024 })).pipe(Stream.runCollect);
});

const results = (prompt: Prompt.Prompt) =>
  prompt.content.flatMap((message) =>
    typeof message.content === "string"
      ? []
      : message.content.flatMap((part) => (part.type === "tool-result" ? [part.result] : [])),
  );

const expectCrash = <A, E>(exit: Exit.Exit<A, E>) => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the crash failpoint");
  const failure = Cause.findErrorOption(exit.cause);

  expect(Option.isSome(failure) && Schema.is(DurableRuntimeFailpointError)(failure.value)).toBe(
    true,
  );
};

layer(testLayer)("durable output and current-Run pruning", (it) => {
  for (const text of ['  Committed once.\n"Keep these quotes."  ', ""]) {
    it.effect(
      `replays canonical plain text without another model call (${text === "" ? "empty" : "verbatim"})`,
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const control = yield* DurableRuntimeFailpointTestControl;
          const scripted = scriptedModel(() => finalParts(text));

          const agent = Agent.withModel(
            Agent.make("durable-text", {
              input: Schema.String,
              output: Output.text(Schema.String.check(Schema.isMaxLength(100))),
              instructions: "Reply in plain text.",
              toolkit: Toolkit.empty,
            }),
            scripted.model,
          );

          const receipt = yield* runtime.submit(
            agent,
            "reply",
            submitOptions(`text-${text.length}`),
          );

          yield* control.setHandler((location) =>
            location === "turn:after-canonical-append"
              ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
              : Effect.void,
          );
          expectCrash(
            yield* runtime
              .processThread(agent, receipt.threadId)
              .pipe(Effect.exit, Effect.ensuring(control.clear)),
          );
          const settled = yield* runtime.processThread(agent, receipt.threadId);

          expect(settled).toHaveLength(1);
          expect(settled[0]?.outcome).toBe("completed");
          expect(scripted.prompts).toHaveLength(1);
          const records = yield* readLog(receipt.threadId);

          expect(
            records
              .filter(({ record }) => record.payload._tag === "RunCompleted")
              .map(({ record }) => record.payload),
          ).toEqual([expect.objectContaining({ output: text })]);
        }),
    );
  }

  for (const rollover of [false, true]) {
    for (const barrier of [
      "compaction:before-canonical-append",
      "compaction:after-canonical-append",
    ] as const) {
      it.effect(
        `prunes settled current-Run results across ${barrier}, prior rollover=${rollover}`,
        () =>
          Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;
            const control = yield* DurableRuntimeFailpointTestControl;

            const toolkit = Toolkit.make(
              Tool.make("search", {
                parameters: Schema.Struct({}),
                success: Schema.String,
              }).annotate(ToolExecutionClass, "readonly"),
              Tool.make("new_context", {
                parameters: ContextRolloverRequest,
                success: ContextRolloverRequest,
              })
                .annotate(ToolExecutionClass, "readonly")
                .annotate(ContextRolloverTool, true),
            );

            const evidence = [
              "OLD-EVIDENCE " + "a".repeat(4_000),
              "NEWEST-EVIDENCE " + "b".repeat(4_000),
            ];

            let executions = 0;

            const handlers = toolkit.toLayer({
              search: () => Effect.sync(() => evidence[executions++] ?? "Unexpected replay"),
              new_context: Effect.succeed,
            });

            const scripted = scriptedModel((call) => {
              if (rollover && call === 0)
                return callParts("window", "new_context", {
                  handoff: "Continue the original objective.",
                });
              const search = call - (rollover ? 1 : 0);

              return search < 2
                ? callParts(`search-${search}`, "search", {}, search === 0 ? 100 : 1_800)
                : finalParts("Finished.");
            });

            const agent = Agent.withModel(
              Agent.make("durable-pruning", {
                input: Schema.String,
                output: Output.text(Schema.String),
                instructions: "Complete the original objective.",
                toolkit,
                policy: {
                  maxTurns: 6,
                  maxToolCalls: 5,
                  contextTokenLimit: 2_400,
                  compaction: { mode: "prune", keepRecentTokens: 20_000 },
                },
              }),
              scripted.model,
            );

            const receipt = yield* runtime.submit(
              agent,
              "ORIGINAL-INPUT",
              submitOptions(`prune-${rollover}-${barrier}`),
            );

            const process = runtime
              .processThread(agent, receipt.threadId)
              .pipe(Effect.provide(handlers));

            yield* control.setHandler((location) =>
              location === barrier && executions === 2
                ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                : Effect.void,
            );
            const firstAttempt = yield* process.pipe(Effect.exit, Effect.ensuring(control.clear));

            expectCrash(firstAttempt);
            const before = yield* readLog(receipt.threadId);

            const beforePrunes = before.filter(
              ({ record }) =>
                record.payload._tag === "CompactionCreated" &&
                record.payload.kind === "clear-tool-results",
            );

            expect(beforePrunes).toHaveLength(
              barrier === "compaction:after-canonical-append" ? 1 : 0,
            );
            expect(executions).toBe(2);
            const settled = yield* process;

            expect(settled[0]?.outcome).toBe("completed");
            expect(executions).toBe(2);
            expect(scripted.prompts).toHaveLength(rollover ? 4 : 3);
            const finalPrompt = scripted.prompts.at(-1) ?? Prompt.empty;

            expect(results(finalPrompt)).toEqual([CLEARED_TOOL_RESULT, evidence[1]]);
            expect(JSON.stringify(finalPrompt)).toContain("ORIGINAL-INPUT");
            expect(JSON.stringify(finalPrompt)).toContain("Complete the original objective.");
            expect(
              finalPrompt.content.some(
                (message) =>
                  message.role === "user" &&
                  message.content.some(
                    (part) => part.type === "text" && part.text.startsWith(CONTEXT_ROLLOVER_PREFIX),
                  ),
              ),
            ).toBe(rollover);
            const records = yield* readLog(receipt.threadId);

            const prunes = records.filter(
              ({ record }) =>
                record.payload._tag === "CompactionCreated" &&
                record.payload.kind === "clear-tool-results",
            );

            expect(prunes).toHaveLength(1);

            const oldResult = records.find(
              ({ record }) =>
                record.payload._tag === "ToolCallSettled" && record.payload.result === evidence[0],
            );

            expect(prunes[0]?.record.payload).toMatchObject({ coversThrough: oldResult?.sequence });
            expect(JSON.stringify(records)).toContain(evidence[0]);

            const replay = yield* projectRunJournal(
              records,
              runIdForSubmission(receipt.submissionId),
            );

            expect(results(replay.prompt)).toEqual([CLEARED_TOOL_RESULT, evidence[1]]);
            expect(replay.usage.inputTokens).toBe(rollover ? 3_300 : 3_200);
          }),
      );
    }
  }
});
