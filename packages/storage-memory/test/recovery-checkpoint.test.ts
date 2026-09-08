import * as Agent from "@effect-agent/core/Agent";
import { ThreadId, ToolCallId } from "@effect-agent/core/Identifiers";
import { ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import {
  DurableStep,
  DurableStepError,
  ToolExecutionClass,
} from "@effect-agent/engine/DurableStep";
import { RunContextPreparation, RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { DurableWorkerBinding } from "@effect-agent/thread/AgentRegistration";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@effect-agent/thread/Records";
import {
  ApprovalDecisionCommand,
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  SubmissionLedger,
} from "@effect-agent/thread/SubmissionLedger";
import { DurableRuntimeFailpointTestControl } from "@effect-agent/thread/testing/DurableFailpointTestControl";
import {
  LoadCheckpointRequest,
  ThreadCheckpoint,
  ThreadExportRequest,
  ThreadStore,
} from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
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
  "missing",
  "corrupt",
  "incompatible",
  "engineVersion",
  "agentDefinitionDigest",
  "modelDigest",
  "toolDigest",
  "definitions",
  "gap",
  "uncertain",
  "deadline",
  "approval",
  "steps",
  "second-rollover",
  "checkpoint-before-failure",
  "checkpoint-after-failure",
  "checkpoint-before-interruption",
  "checkpoint-after-interruption",
  "checkpoint-before-defect",
  "checkpoint-after-defect",
] as const;

describe("disposable durable recovery checkpoint", () => {
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
        }).annotate(ToolExecutionClass, scenario === "steps" ? "idempotent" : "ordinary"),
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
                requests.length <= (scenario === "second-rollover" ? 5 : 4)
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
                  ...((request.turn === 3 &&
                    !JSON.stringify(request.source).includes("Keep the continuation.")) ||
                  (scenario === "second-rollover" && request.turn === 6)
                    ? {
                        rollover: {
                          handoff:
                            scenario === "second-rollover" && request.turn === 6
                              ? "Second continuation."
                              : "Keep the continuation.",
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
          scenario.startsWith("checkpoint-") &&
          location ===
            (scenario.includes("before") ? "checkpoint:before-save" : "checkpoint:after-save");

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
        if (scenario.endsWith("defect")) return Effect.die("checkpoint crash");

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

      expect(Option.isSome(checkpoint)).toBe(!scenario.includes("checkpoint-before"));

      const original = yield* store.export(
        ThreadExportRequest.make({ threadId: receipt.threadId }),
      );

      const requestsBefore = requests.length;
      const callsBefore = calls;

      yield* failpoints.clear;
      if (scenario === "deadline") yield* TestClock.adjust("31 seconds");
      let readRecords = 0;
      const checkpoints = store.recoveryCheckpoints!;

      const observed = ThreadStore.of({
        ...store,
        recoveryCheckpoints:
          scenario === "missing"
            ? undefined
            : {
                ...checkpoints,
                load: (request) =>
                  checkpoints.load(request).pipe(
                    Effect.map(
                      Option.map((saved) => {
                        if (
                          [
                            "engineVersion",
                            "agentDefinitionDigest",
                            "modelDigest",
                            "toolDigest",
                          ].includes(scenario)
                        ) {
                          const encoded = Schema.encodeSync(ThreadCheckpoint)(saved);

                          return Schema.decodeUnknownSync(ThreadCheckpoint)(
                            Object.fromEntries(
                              Object.entries(encoded).filter(([key]) => key !== scenario),
                            ),
                          );
                        }
                        if (scenario === "corrupt")
                          return ThreadCheckpoint.make({ ...saved, state: { invalid: true } });
                        if (scenario === "incompatible")
                          return ThreadCheckpoint.make({ ...saved, engineVersion: "future" });
                        if (scenario === "definitions")
                          return ThreadCheckpoint.make({
                            ...saved,
                            agentDefinitionDigest: Digest.make("b".repeat(64)),
                          });

                        return saved;
                      }),
                    ),
                  ),
              },
        read: (request) =>
          store.read(request).pipe(
            Stream.filter(
              (entry) =>
                scenario !== "gap" ||
                Option.isNone(checkpoint) ||
                entry.sequence !== checkpoint.value.throughSequence + 1,
            ),
            Stream.tap(() =>
              Effect.sync(() => {
                readRecords++;
              }),
            ),
          ),
      });

      const resumed = yield* makeRuntime.pipe(Effect.provideService(ThreadStore, observed));

      if (scenario === "gap") {
        expect(Exit.isFailure(yield* Effect.exit(resumed.runRecovery))).toBe(true);
        expect(requests).toHaveLength(requestsBefore);
        expect(calls).toBe(callsBefore);

        return;
      }
      yield* resumed.runRecovery;
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
      } else if (scenario === "deadline") {
        expect(Option.isSome(outcome) && outcome.value.outcome).toBe("failed");
        expect(requests).toHaveLength(requestsBefore);
        expect(calls).toBe(callsBefore);
      } else {
        expect(Option.isSome(outcome) && outcome.value.outcome).toBe("completed");
        expect(calls).toBe(scenario === "second-rollover" ? 5 : 4);
        if (scenario === "steps") expect(stepEffects).toBe(3);
        expect(requests).toHaveLength(scenario === "second-rollover" ? 6 : 5);
        const prompt = JSON.stringify(requests.at(-1));

        expect(prompt).toContain("Original input");
        expect(prompt).toContain("Keep original instructions.");
        expect(prompt).toContain(
          scenario === "second-rollover" ? "Second continuation." : "Keep the continuation.",
        );
        expect(prompt).toContain(scenario === "second-rollover" ? "turn 6/10" : "turn 5/10");
        expect(prompt).toContain(scenario === "second-rollover" ? "tokens 550/" : "tokens 440/");
        if (scenario !== "steps") expect(prompt).not.toContain('"id":"call-1"');
        if (Option.isSome(outcome))
          expect(outcome.value.usageSummary?.modelCalls).toBe(
            scenario === "second-rollover" ? 6 : 5,
          );
      }

      const completed = yield* store.export(
        ThreadExportRequest.make({ threadId: receipt.threadId }),
      );

      expect(completed.records.slice(0, original.records.length)).toEqual(original.records);
      expect(
        completed.records.filter(({ record }) => record.payload._tag === "RunStarted"),
      ).toHaveLength(1);
      if (scenario === "cache") expect(readRecords).toBeLessThan(original.records.length * 2);
      if (
        ["engineVersion", "agentDefinitionDigest", "modelDigest", "toolDigest"].includes(scenario)
      )
        expect(readRecords).toBeGreaterThanOrEqual(original.records.length);
    }).pipe(Effect.provide(base)),
  );
});
