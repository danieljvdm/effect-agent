import * as Agent from "@effect-agent/core/Agent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { CLEARED_TOOL_RESULT, CONTEXT_ROLLOVER_PREFIX } from "@effect-agent/engine/Compaction";
import { ContextRolloverRequest, ContextRolloverTool } from "@effect-agent/engine/ContextWindow";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import * as Output from "@effect-agent/engine/Output";
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@effect-agent/thread/Records";
import { projectRunJournal, runIdForSubmission } from "@effect-agent/thread/RunJournal";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { DurableRuntimeFailpointTestControl } from "@effect-agent/thread/testing/DurableFailpointTestControl";
import { ThreadRead, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, type Response, Tool, Toolkit } from "effect/unstable/ai";

const digest = Digest.make("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const testLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      MemorySubmissionLedgerLive,
      MemoryThreadStoreLive,
      WakeScheduler.layerNoop,
      ToolReconciler.uncertain,
      RunToolAuthorization.allowAll,
      DurableRuntimeFailpointTestControl.layer,
      DurableRuntimeConfig.layer({
        deploymentId: DeploymentId.make("output-compaction"),
        producerId: ProducerId.make("output-compaction"),
      }),
    ).pipe(Layer.provideMerge(NodeCrypto.layer)),
  ),
);

const submitOptions = (id: string) => ({
  threadId: ThreadId.make(id),
  principal: Principal.make("output-compaction"),
  idempotencyKey: IdempotencyKey.make(id),
  definitions,
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
