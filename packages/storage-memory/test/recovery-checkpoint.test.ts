import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { DurableWorkerBinding } from "effect-agent/agent-registration";
import { ContextCompactor } from "effect-agent/context-compactor";
import { DurableAgentRuntime, DurableRuntimeConfig } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpointError } from "effect-agent/durable-failpoint";
import { DurableStep, DurableStepError, ToolExecutionClass } from "effect-agent/durable-step";
import { RunId, SubmissionId, ThreadId, ToolCallId } from "effect-agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "effect-agent/records";
import { runIdForSubmission } from "effect-agent/run-journal";
import { RunContextPreparation, RunToolAuthorization } from "effect-agent/run-options";
import {
  ApprovalDecisionCommand,
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  ResolutionCompletedWithResult,
  SubmissionLedger,
  UnknownResolutionCommand,
} from "effect-agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "effect-agent/testing/durable-failpoint-test-control";
import {
  LoadCheckpointRequest,
  ThreadCheckpoint,
  ThreadExportRequest,
  ThreadStore,
} from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import type { Prompt, Response } from "effect/unstable/ai";
import { LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";

import { MemorySubmissionLedgerLive } from "../src/MemorySubmissionLedger.ts";
import { MemoryThreadStoreLive } from "../src/MemoryThreadStore.ts";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const base = Layer.mergeAll(
  MemoryThreadStoreLive,
  MemorySubmissionLedgerLive,
  DurableRuntimeFailpointTestControl.layer,
  WakeScheduler.layerNoop,
  ToolReconciler.uncertain,
  DurableRuntimeConfig.layer({
    deploymentId: DeploymentId.make("checkpoint-test"),
    producerId: ProducerId.make("checkpoint-test"),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

const finish = {
  type: "finish",
  reason: "tool-calls",
  usage: { inputTokens: { total: 100 }, outputTokens: { total: 10 } },
} satisfies Response.StreamPartEncoded;

const final: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { ...finish, reason: "stop" },
];

const scenarios = [
  "cache",
  "corrupt",
  "gap",
  "uncertain",
  "approval",
  "steps",
  "checkpoint-after-interruption",
];

describe("disposable durable recovery checkpoint", () => {
  it.effect(
    "resumes retained older work through canonical history when the checkpoint belongs to a later Run",
    () =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const ledger = yield* SubmissionLedger;
        const failpoints = yield* DurableRuntimeFailpointTestControl;
        const threadId = ThreadId.make("checkpoint-foreign-owner");
        let phase: "prefix" | "unknown" | "later" | "resume" = "prefix";
        let laterTurns = 0;
        let handlerCalls = 0;
        const requests: Array<Prompt.Prompt> = [];

        const tools = Toolkit.make(
          Tool.make("write", { parameters: Tool.EmptyParams, success: Schema.String }),
        );

        const handlers = tools.toLayer({
          write: () =>
            Effect.sync(() => {
              handlerCalls++;

              return "recorded";
            }),
        });

        const model = Model.make(
          "scripted",
          "foreign-checkpoint",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) => {
                requests.push(request.prompt);
                if (phase === "later") laterTurns++;

                const call =
                  phase === "unknown"
                    ? "call-a"
                    : phase === "later" && laterTurns <= 2
                      ? `call-b-${laterTurns}`
                      : undefined;

                return Stream.fromIterable<Response.StreamPartEncoded>(
                  call === undefined
                    ? final
                    : [{ type: "tool-call", id: call, name: "write", params: {} }, finish],
                );
              },
            }),
          ),
        );

        const agent = Agent.withModel(
          Agent.make("foreign-checkpoint", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Keep original instructions.",
            toolkit: tools,
            policy: {
              maxTurns: 10,
              maxToolCalls: 10,
              maxDuration: "30 seconds",
              contextTokenLimit: 20_000,
            },
          }),
          model,
        );

        const binding = yield* DurableWorkerBinding.make(agent, definitions).pipe(
          Effect.provide(handlers),
        );

        const runtime = yield* DurableAgentRuntime.pipe(
          Effect.provide(
            DurableAgentRuntime.layerWithBindings([binding]).pipe(
              Layer.provide(
                Layer.mergeAll(RunToolAuthorization.allowAll, ContextCompactor.layerRollover),
              ),
            ),
          ),
          Effect.provideService(RunContextPreparation, {
            hook: {
              prepare: (request) =>
                Effect.sync(() => {
                  const compactPrefix = phase === "later" && request.turn === 3;

                  if (compactPrefix) {
                    expect(
                      request.source.content.slice(0, 2).map((message) => message.role),
                    ).toEqual(["user", "assistant"]);
                  }

                  return {
                    prompt: request.source,
                    ...(compactPrefix
                      ? { rollover: { handoff: "The initial request completed.", through: 2 } }
                      : {}),
                  };
                }),
            },
          }),
        );

        const submit = (input: string) =>
          runtime.submit(agent, input, {
            threadId,
            principal: Principal.make("test"),
            idempotencyKey: IdempotencyKey.make(input),
            definitions,
          });

        const prefix = yield* submit("settled prefix");

        yield* runtime.processThreadHead(threadId);
        phase = "unknown";
        const older = yield* submit("older unresolved input");

        yield* failpoints.setHandler((location) =>
          location === "tools:after-prepared-append"
            ? DurableRuntimeFailpointError.make({ location })
            : Effect.void,
        );
        expect(Exit.isFailure(yield* Effect.exit(runtime.processThreadHead(threadId)))).toBe(true);
        yield* failpoints.clear;
        yield* runtime.runRecovery();
        expect(
          (yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: older.submissionId }),
          )).submission.state,
        ).toBe("unknown");
        expect(handlerCalls).toBe(0);

        phase = "later";
        const later = yield* submit("later input");
        const laterOutcome = yield* runtime.processThreadHead(threadId);

        expect(Option.isSome(laterOutcome) && laterOutcome.value.submissionId).toBe(
          later.submissionId,
        );
        expect(handlerCalls).toBe(2);

        const checkpoint = yield* store.recoveryCheckpoints!.load(
          LoadCheckpointRequest.make({ threadId }),
        );

        if (Option.isNone(checkpoint)) return yield* Effect.die("missing later-owned checkpoint");

        const retained = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            state: Schema.Struct({
              submissionId: SubmissionId,
              submissionIds: Schema.Array(SubmissionId),
              seed: Schema.Struct({ runId: RunId }),
            }),
          }),
        )(checkpoint.value.state);

        expect(retained.state.submissionId).toBe(later.submissionId);
        expect(retained.state.seed.runId).toBe(runIdForSubmission(later.submissionId));
        expect(retained.state.submissionIds).toContain(older.submissionId);
        const before = yield* store.export(ThreadExportRequest.make({ threadId }));

        yield* ledger.recordUnknownResolution(
          UnknownResolutionCommand.make({
            submissionId: older.submissionId,
            toolCallId: ToolCallId.make("call-a"),
            author: "operator",
            reason: "The service confirmed completion",
            resolution: ResolutionCompletedWithResult.make({
              result: "confirmed older result",
              isFailure: false,
            }),
          }),
        );
        phase = "resume";
        const resumed = yield* runtime.processThreadHead(threadId);

        expect(Option.isSome(resumed) && resumed.value).toMatchObject({
          submissionId: older.submissionId,
          receiptId: older.receiptId,
          outcome: "completed",
        });
        expect(handlerCalls).toBe(2);
        expect(JSON.stringify(requests.at(-1))).toContain("confirmed older result");
        const completed = yield* store.export(ThreadExportRequest.make({ threadId }));

        expect(completed.records.slice(0, before.records.length)).toEqual(before.records);
        expect(
          completed.records.flatMap(({ record: { payload } }) =>
            payload._tag === "ToolCallSettled" ? [payload.toolCallId] : [],
          ),
        ).toEqual(["call-b-1", "call-b-2", "call-a"]);
        expect(
          completed.records.flatMap(({ record: { payload } }) =>
            payload._tag === "SubmissionSettled" ? [payload.submissionId] : [],
          ),
        ).toEqual([prefix.submissionId, later.submissionId, older.submissionId]);
        expect(
          completed.records.filter(
            ({ record: { payload } }) =>
              payload._tag === "RunStarted" &&
              payload.runId === runIdForSubmission(older.submissionId),
          ),
        ).toHaveLength(1);
      }).pipe(Effect.provide(base)),
  );

  it.effect.each(scenarios)("preserves canonical obligations across %s", (scenario) =>
    Effect.gen(function* () {
      const store = yield* ThreadStore;
      const ledger = yield* SubmissionLedger;
      const failpoints = yield* DurableRuntimeFailpointTestControl;
      const requests: Array<Prompt.Prompt> = [];
      let calls = 0;
      let stepEffects = 0;

      const tools = Toolkit.make(
        Tool.make("write", {
          parameters: Tool.EmptyParams,
          success: Schema.String,
          dependencies: [DurableStep],
          failure: DurableStepError,
        }).annotate(ToolExecutionClass, scenario === "steps" ? "idempotent" : "uncertain"),
        Tool.make("approve", {
          parameters: Tool.EmptyParams,
          success: Schema.String,
          needsApproval: true,
        }),
      );

      const handlers = tools.toLayer({
        write: () =>
          Effect.gen(function* () {
            calls++;
            if (scenario !== "steps") return "recorded";
            const steps = yield* DurableStep;

            return yield* steps.do(
              "write-once",
              Schema.String,
              Effect.sync(() => {
                stepEffects++;

                return "recorded";
              }),
            );
          }),
        approve: () =>
          Effect.sync(() => {
            calls++;

            return "approved";
          }),
      });

      const model = Model.make(
        "scripted",
        "checkpoint",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              requests.push(request.prompt);

              return Stream.fromIterable<Response.StreamPartEncoded>(
                requests.length <= 4
                  ? [
                      {
                        type: "tool-call",
                        id:
                          scenario === "steps" && requests.length === 4
                            ? "call-1"
                            : `call-${requests.length}`,
                        name:
                          scenario === "approval" && requests.length === 4 ? "approve" : "write",
                        params: {},
                      },
                      finish,
                    ]
                  : final,
              );
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("checkpoint", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Keep original instructions.",
          toolkit: tools,
          policy: {
            maxTurns: 10,
            maxToolCalls: 10,
            maxDuration: "30 seconds",
            runStatus: "appended",
            contextTokenLimit: 20_000,
          },
        }),
        model,
      );

      const makeRuntime = Effect.gen(function* () {
        const binding = yield* DurableWorkerBinding.make(agent, definitions).pipe(
          Effect.provide(handlers),
        );

        return yield* DurableAgentRuntime.pipe(
          Effect.provide(
            DurableAgentRuntime.layerWithBindings([binding]).pipe(
              Layer.provide(
                Layer.mergeAll(RunToolAuthorization.allowAll, ContextCompactor.layerRollover),
              ),
            ),
          ),
          Effect.provideService(RunContextPreparation, {
            hook: {
              prepare: (request) =>
                Effect.succeed({
                  prompt: request.source,
                  ...(request.turn === 3 &&
                  !JSON.stringify(request.source).includes("Keep the continuation.")
                    ? {
                        rollover: {
                          handoff: "Keep the continuation.",
                          through: request.source.content.length,
                        },
                      }
                    : {}),
                }),
            },
          }),
        );
      });

      const runtime = yield* makeRuntime;

      const receipt = yield* runtime.submit(agent, "Original input", {
        threadId: ThreadId.make(`checkpoint-${scenario}`),
        principal: Principal.make("test"),
        idempotencyKey: IdempotencyKey.make("work"),
        definitions,
      });

      yield* failpoints.setHandler((location) => {
        const atCheckpoint =
          scenario.startsWith("checkpoint-") && location === "checkpoint:after-save";

        const atBatch =
          !scenario.startsWith("checkpoint-") &&
          requests.length === 4 &&
          location ===
            (scenario === "uncertain"
              ? "tools:after-prepared-append"
              : scenario === "approval"
                ? "approval:after-request-append"
                : "turn:after-results-append");

        if (!atCheckpoint && !atBatch) return Effect.void;
        if (scenario.endsWith("interruption")) return Effect.interrupt;

        return DurableRuntimeFailpointError.make({ location });
      });

      const stopped = yield* Effect.exit(
        runtime.processThreadHead(receipt.threadId).pipe(Effect.provide(handlers)),
      );

      expect(Exit.isFailure(stopped)).toBe(true);

      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
      );

      expect(snapshot.ownership).toBeUndefined();

      const checkpoint = yield* store.recoveryCheckpoints!.load(
        LoadCheckpointRequest.make({ threadId: receipt.threadId }),
      );

      expect(Option.isSome(checkpoint)).toBe(true);

      const original = yield* store.export(
        ThreadExportRequest.make({ threadId: receipt.threadId }),
      );

      const requestsBefore = requests.length;
      const callsBefore = calls;

      yield* failpoints.clear;
      const checkpoints = store.recoveryCheckpoints!;

      const observed = ThreadStore.of({
        ...store,
        recoveryCheckpoints: {
          ...checkpoints,
          load: (request) =>
            checkpoints.load(request).pipe(
              Effect.map(
                Option.map((saved) => {
                  if (scenario === "corrupt")
                    return ThreadCheckpoint.make({ ...saved, state: { invalid: true } });

                  return saved;
                }),
              ),
            ),
        },
        read: (request) =>
          store
            .read(request)
            .pipe(
              Stream.filter(
                (entry) =>
                  scenario !== "gap" ||
                  Option.isNone(checkpoint) ||
                  entry.sequence !== checkpoint.value.throughSequence + 1,
              ),
            ),
      });

      const resumed = yield* makeRuntime.pipe(Effect.provideService(ThreadStore, observed));

      if (scenario === "gap") {
        expect((yield* resumed.runRecovery()).blocked).toMatchObject([{ _tag: "RecoveryBlocked" }]);
        expect(requests).toHaveLength(requestsBefore);
        expect(calls).toBe(callsBefore);

        return;
      }
      yield* resumed.runRecovery();
      if (scenario === "approval") {
        yield* resumed.resolveApproval(
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: ToolCallId.make("call-4"),
            decision: "approved",
            resolver: "test",
            reason: "resume checkpointed approval",
          }),
        );
      }

      const outcome = yield* resumed
        .processThreadHead(receipt.threadId)
        .pipe(Effect.provide(handlers));

      if (scenario === "uncertain") {
        expect(Option.isNone(outcome)).toBe(true);
        expect(requests).toHaveLength(requestsBefore);
        expect(calls).toBe(callsBefore);

        const pending = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
        );

        expect(pending.submission.state).toBe("unknown");
      } else {
        expect(Option.isSome(outcome) && outcome.value.outcome).toBe("completed");
        expect(calls).toBe(4);
        if (scenario === "steps") expect(stepEffects).toBe(3);
        expect(requests).toHaveLength(5);
        const prompt = JSON.stringify(requests.at(-1));

        expect(prompt).toContain("Original input");
        expect(prompt).toContain("Keep original instructions.");
        expect(prompt).toContain("Keep the continuation.");
        expect(prompt).toContain("turn 5/10");
        expect(prompt).toContain("tokens 440/");
        if (scenario !== "steps") expect(prompt).not.toContain('"id":"call-1"');
        if (Option.isSome(outcome)) expect(outcome.value.usageSummary?.modelCalls).toBe(5);
      }

      const completed = yield* store.export(
        ThreadExportRequest.make({ threadId: receipt.threadId }),
      );

      expect(completed.records.slice(0, original.records.length)).toEqual(original.records);
      expect(
        completed.records.filter(({ record }) => record.payload._tag === "RunStarted"),
      ).toHaveLength(1);
    }).pipe(Effect.provide(base)),
  );
});
