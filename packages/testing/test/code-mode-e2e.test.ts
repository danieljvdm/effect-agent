import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { inProcessCodeExecutorLayer } from "@effect-agent/testing/code-executor-substitute";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Layer, Logger, Ref, References, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as CodeMode from "effect-agent/code-mode";
import { DurableAgentRuntime, DurableRuntimeConfig } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "effect-agent/durable-failpoint";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "effect-agent/records";
import {
  RunContextPreparationPassthrough,
  toolFailureObserverLayer,
  type ToolFailureObservation,
} from "effect-agent/run-options";
import { IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { ThreadHistory } from "effect-agent/thread-history";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

let threadSequence = 0;

const usage = { inputTokens: {}, outputTokens: {} };

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => Schema.decodeSync(ThreadId)(`thread-cm-e2e-${++threadSequence}`)),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-cm-e2e")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-cm-e2e")),
});

// The suite opts out of the injected test services because the in-process
// executor's wall-clock deadline runs on the live Clock (see the substitute
// suite for the rationale).
const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer, { excludeTestServices: true })("Code Mode failure reporting", (it) => {
  it.effect(
    "RUN-036 CAP-016 observes the inner Cause while CodeModeFailure recovers to a completed durable Run",
    () => {
      class LookupFailure extends Schema.TaggedError<LookupFailure>()("LookupFailure", {
        message: Schema.String,
        privateDetail: Schema.String,
      }) {}

      const original = LookupFailure.make({
        message: "Lookup unavailable",
        privateDetail: "CODE_MODE_CAUSE_SECRET",
      });

      const Lookup = Tool.make("lookup", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
        failure: LookupFailure,
      }).annotate(ToolExecutionClass, "readonly");

      const Declared = Tool.make("declared", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
        failure: LookupFailure,
        failureMode: "return",
      }).annotate(ToolExecutionClass, "readonly");

      const codeMode = CodeMode.make("search_workspace", {
        description: "Search the workspace",
        tools: { workspace: { declared: Declared, lookup: Lookup } },
      });

      const definition = Agent.make("code-mode-observer-regression", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Search, then answer.",
        toolkit: Toolkit.make(codeMode.tool),
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 4,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });

      const observations: Array<ToolFailureObservation> = [];
      const logs: Array<unknown> = [];

      const logger = Logger.make<unknown, void>(({ message, cause, fiber }) => {
        logs.push({
          message,
          cause: Cause.pretty(cause),
          annotations: fiber.getRef(References.CurrentLogAnnotations),
        });
      });

      const runtimeLayer = DurableAgentRuntime.layer.pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            MemoryThreadStoreLive,
            MemorySubmissionLedgerLive,
            WakeScheduler.layerNoop,
            DurableRuntimeFailpoint.layer,
            ToolReconciler.uncertain,
            DurableRuntimeConfig.layer({
              deploymentId: DeploymentId.make("observer-regression"),
              producerId: ProducerId.make("observer-regression"),
            }),
            toolFailureObserverLayer({
              observe: (observation) =>
                Effect.sync(() => {
                  observations.push(observation);
                }),
            }),
          ),
        ),
        Layer.provide(NodeCrypto.layer),
      );

      const handlers = codeMode.handlers.pipe(
        Layer.provide([
          Toolkit.make(Declared, Lookup).toLayer({
            declared: () =>
              Effect.fail(
                LookupFailure.make({
                  message: "Expected lookup failure",
                  privateDetail: "CODE_MODE_DECLARED_SECRET",
                }),
              ),
            lookup: () => Effect.fail(original),
          }),
          inProcessCodeExecutorLayer,
        ]),
      );

      return Effect.gen(function* () {
        const turn = yield* Ref.make(0);

        const model = Model.make(
          "scripted",
          "observer-regression",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Ref.getAndUpdate(turn, (n) => n + 1).pipe(
                    Effect.map((n) =>
                      Stream.fromIterable<Response.StreamPartEncoded>(
                        n === 0
                          ? [
                              {
                                type: "tool-call",
                                id: "search-1",
                                name: "search_workspace",
                                params: {
                                  code: `async () => {
                                    try { await workspace.declared({ value: 1 }) } catch {}
                                    return await workspace.lookup({ value: 1 })
                                  }`,
                                },
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : [
                              { type: "text-start", id: "answer" },
                              { type: "text-delta", id: "answer", delta: '"fallback answer"' },
                              { type: "text-end", id: "answer" },
                              { type: "finish", reason: "stop", usage },
                            ],
                      ),
                    ),
                  ),
                ),
            }),
          ),
        );

        const agent = Agent.withModel(definition, model);
        const runtime = yield* DurableAgentRuntime;
        const threadId = ThreadId.make("code-mode-observer");
        const digest = Digest.make("a".repeat(64));

        const receipt = yield* runtime.submit(agent, "search", {
          threadId,
          principal: Principal.make("test"),
          idempotencyKey: IdempotencyKey.make("search"),
          definitions: DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
        });

        yield* runtime.processThread(agent, threadId).pipe(Effect.provide(handlers));
        const settlement = yield* runtime.awaitSettlement(receipt);

        expect(settlement.outcome).toBe("completed");
        expect(observations).toHaveLength(3);
        expect(observations[0]).toMatchObject({
          _tag: "ProgrammaticToolFailure",
          kind: "declared-failure",
          tag: "LookupFailure",
          parentToolCallId: "search-1",
          toolCallId: "search-1#0",
        });
        for (const field of ["cause", "message", "result", "encodedResult"])
          expect(observations[0]).not.toHaveProperty(field);
        expect(observations[1]).toMatchObject({
          _tag: "ProgrammaticToolFailure",
          kind: "handler-error",
          tag: "LookupFailure",
          parentToolCallId: "search-1",
          toolCallId: "search-1#1",
        });
        expect(observations[1]?.cause?.reasons.filter(Cause.isFailReason)[0]?.error).toBe(original);
        expect(observations[1]).not.toHaveProperty("message");
        expect(observations[2]).toMatchObject({
          _tag: "ModelToolFailure",
          kind: "declared-failure",
          tag: "CodeModeFailure",
          toolCallId: "search-1",
        });
        expect(observations[2]).not.toHaveProperty("cause");
        expect(observations[2]).not.toHaveProperty("message");
        const store = yield* ThreadStore;

        const records = yield* store
          .read(ThreadRead.make({ threadId, limit: 1_024 }))
          .pipe(Stream.runCollect);

        const tools = records.filter(
          (envelope) => envelope.record.payload._tag === "ToolCallSettled",
        );

        expect(tools).toHaveLength(1);
        expect(tools[0]?.record.payload).toMatchObject({
          toolCallId: "search-1",
          isFailure: true,
          result: { _tag: "CodeModeFailure" },
        });
        expect(JSON.stringify({ records, logs })).not.toContain("CODE_MODE_CAUSE_SECRET");
        expect(JSON.stringify({ records, logs })).not.toContain("CODE_MODE_DECLARED_SECRET");
        expect(JSON.stringify(records)).not.toContain("search-1#0");
        expect(JSON.stringify(records)).not.toContain("search-1#1");
      }).pipe(Effect.provide([runtimeLayer, Logger.layer([logger])]));
    },
  );
});
