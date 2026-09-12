import * as Agent from "@effect-agent/core/Agent";
import { CompactionPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId, ToolCallId } from "@effect-agent/core/Identifiers";
import { RunToolAuthorization, type RunCostEstimateRequest } from "@effect-agent/engine/RunOptions";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpoint } from "@effect-agent/thread/DurableFailpoint";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  SubmissionSettled,
} from "@effect-agent/thread/Records";
import {
  ApprovalDecisionCommand,
  IdempotencyKey,
  Principal,
} from "@effect-agent/thread/SubmissionLedger";
import { ThreadRead, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Schema, Stream } from "effect";
import { AiError, LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

const usage = {
  inputTokens: { total: 10, uncached: 7, cacheRead: 2, cacheWrite: 1 },
  outputTokens: { total: 5, text: 3, reasoning: 2 },
};

const toolkit = Toolkit.make(
  Tool.make("lookup", { parameters: Schema.Struct({}), success: Schema.String }),
);

const definition = Agent.make("provider-usage", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Look up, then answer.",
  toolkit,
  policy: { maxTurns: 3, maxToolCalls: 3, maxDuration: "30 seconds", toolConcurrency: 1 },
});

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

// Reported usage was discarded before protocol validation in the original runtime:
// https://github.com/danieljvdm/effect-agent/commit/9257d75caff1d2bb145effc3d461e39f72f8bbc5
for (const failure of ["open-part", "missing-usage", "continuation", "invalid-estimate"] as const) {
  const reportsUsage = failure !== "missing-usage";
  const retainsUsage = reportsUsage && failure !== "invalid-estimate";

  it.live(`retains ${failure} failed-call usage without recounting the committed call`, () =>
    Effect.gen(function* () {
      const seen: Array<RunCostEstimateRequest> = [];
      let requests = 0;

      const model = Model.make(
        "scripted",
        "requested-alias",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              requests++;

              const parts: Array<Response.StreamPartEncoded> = [
                { type: "response-metadata", id: `response-${requests}`, modelId: "actual-model" },
                ...(requests === 1
                  ? [
                      {
                        type: "tool-call" as const,
                        id: "lookup-1",
                        name: "lookup",
                        params: {},
                        providerExecuted: false,
                      },
                    ]
                  : failure === "continuation"
                    ? []
                    : failure === "invalid-estimate"
                      ? [
                          { type: "text-start" as const, id: "answer" },
                          { type: "text-delta" as const, id: "answer", delta: "done" },
                          { type: "text-end" as const, id: "answer" },
                        ]
                      : [{ type: "text-start" as const, id: "unfinished" }]),
              ];

              if (requests === 1 || reportsUsage)
                parts.push({
                  type: "finish",
                  reason:
                    requests === 1 ? "tool-calls" : failure === "continuation" ? "length" : "stop",
                  usage,
                  metadata: { scripted: { serviceTier: "priority" } },
                });

              return Stream.fromIterable(parts);
            },
          }),
        ),
      );

      const base = Layer.mergeAll(
        MemoryThreadStoreLive,
        MemorySubmissionLedgerLive,
        WakeScheduler.layerNoop,
        ToolReconciler.uncertain,
        DurableRuntimeFailpoint.layer,
        RunToolAuthorization.allowAll,
        toolkit.toLayer({ lookup: () => Effect.succeed("found") }),
        DurableRuntimeConfig.layer({
          deploymentId: Schema.decodeSync(DeploymentId)("usage-test"),
          producerId: Schema.decodeSync(ProducerId)("usage-test"),
          estimateCostMicrousd: (_usage, request) =>
            Effect.sync(() => {
              seen.push(request);

              return {
                costMicrousd: requests === 2 && failure === "invalid-estimate" ? -1 : 25,
                serviceTier: "priority",
                pricingVersion: "test-v1",
                pricingStatus: "estimated" as const,
              };
            }),
        }),
      ).pipe(Layer.provideMerge(NodeCrypto.layer));

      yield* Effect.gen(function* () {
        const store = yield* ThreadStore;
        const runtime = yield* DurableAgentRuntime.pipe(Effect.provide(DurableAgentRuntime.layer));
        const threadId = Schema.decodeSync(ThreadId)("usage-test");
        const agent = Agent.withModel(definition, model);

        yield* runtime.submit(agent, "go", {
          threadId,
          principal: Schema.decodeSync(Principal)("viewer"),
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("input"),
          definitions,
        });
        yield* runtime.processThread(agent, threadId);

        const records = yield* store
          .read(ThreadRead.make({ threadId, limit: 128 }))
          .pipe(Stream.runCollect);

        const responses = records.filter(
          ({ record }) => record.payload._tag === "ModelResponseRecorded",
        );

        const settlement = records
          .map(({ record }) => record.payload)
          .find((payload) => payload._tag === "SubmissionSettled");

        expect(responses).toHaveLength(1);
        expect(settlement?.outcome).toBe("failed");
        expect(settlement?.usageSummary).toMatchObject({
          modelCalls: retainsUsage ? 2 : 1,
          costMicrousd: retainsUsage ? 50 : 25,
          unobservedModelCalls: retainsUsage ? 0 : 1,
          pricingStatus: retainsUsage ? "complete" : "partial",
        });
        expect(settlement?.uncommittedModelUsage?.length).toBe(retainsUsage ? 1 : undefined);
        if (settlement === undefined) throw new Error("expected terminal accounting");

        const committedCalls = responses.flatMap(({ record }) =>
          record.payload._tag === "ModelResponseRecorded" ? (record.payload.modelUsage ?? []) : [],
        );

        const retainedCalls = [...committedCalls, ...(settlement.uncommittedModelUsage ?? [])];

        expect(retainedCalls.map((call) => call.response?.id)).toEqual(
          retainsUsage ? ["response-1", "response-2"] : ["response-1"],
        );

        const {
          runId: _runId,
          usageSummary: _summary,
          uncommittedModelUsage: _usage,
          ...withoutRunAccounting
        } = Schema.encodeSync(SubmissionSettled)(settlement);

        // A retained accounting suffix requires a Run identity and its aggregate summary.
        expect(Schema.decodeExit(SubmissionSettled)(withoutRunAccounting)._tag).toBe("Success");
        expect(
          Schema.decodeExit(SubmissionSettled)({
            ...withoutRunAccounting,
            uncommittedModelUsage: retainedCalls,
          })._tag,
        ).toBe("Failure");
        if (retainsUsage)
          expect(settlement?.uncommittedModelUsage?.[0]).toMatchObject({
            model: "requested-alias",
            response: { id: "response-2", model: "actual-model" },
            purpose: "turn",
            usageStatus: "complete",
            pricingStatus: "estimated",
          });
        expect(seen).toHaveLength(reportsUsage ? 2 : 1);
        expect(seen[0]).toMatchObject({
          model: "requested-alias",
          response: { id: "response-1", model: "actual-model" },
          finishMetadata: { scripted: { serviceTier: "priority" } },
        });
        yield* runtime.processThread(agent, threadId);
        expect(requests).toBe(2);

        const replay = yield* store
          .read(ThreadRead.make({ threadId, limit: 128 }))
          .pipe(Stream.runCollect);

        expect(replay).toEqual(records);
      }).pipe(Effect.provide(base));
    }),
  );
}

// KOM-127 requires restart-preserved totals with missing usage explicitly partial:
// https://linear.app/reve-ai/issue/KOM-127
// Native overflow retry and response-only staging originated in:
// https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a
it.live(
  "retains an unreported overflow across retry, approval suspension, and two runtime replacements",
  () =>
    Effect.gen(function* () {
      const tools = Toolkit.make(
        toolkit.tools.lookup,
        Tool.make("approve", {
          parameters: Schema.Struct({}),
          success: Schema.String,
          needsApproval: true,
        }),
      );

      let requests = 0;

      const model = Model.make(
        "scripted",
        "requested-alias",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              requests++;
              if (requests === 4)
                return Stream.fail(
                  AiError.AiError.make({
                    module: "test",
                    method: "streamText",
                    reason: AiError.UnknownError.make({ description: "context_length_exceeded" }),
                  }),
                );

              const parts: Array<Response.StreamPartEncoded> = [
                { type: "response-metadata", id: `response-${requests}`, modelId: "actual-model" },
              ];

              if (requests <= 2 || requests === 6) {
                parts.push({
                  type: "tool-call",
                  id: requests === 6 ? "approval-1" : `lookup-${requests}`,
                  name: requests === 6 ? "approve" : "lookup",
                  params: {},
                  providerExecuted: false,
                });
              } else {
                parts.push(
                  { type: "text-start", id: "text" },
                  {
                    type: "text-delta",
                    id: "text",
                    delta: requests === 5 ? "Prior lookup findings." : JSON.stringify("done"),
                  },
                  { type: "text-end", id: "text" },
                );
              }
              parts.push({
                type: "finish",
                reason: requests <= 2 || requests === 6 ? "tool-calls" : "stop",
                usage,
              });

              return Stream.fromIterable(parts);
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("recovery-usage", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Look up twice, then obtain approval and answer.",
          toolkit: tools,
          policy: {
            maxTurns: 5,
            maxToolCalls: 5,
            maxDuration: "30 seconds",
            contextTokenLimit: 100_000,
            compaction: CompactionPolicy.make({ mode: "summarize", keepRecentTokens: 1 }),
          },
        }),
        model,
      );

      const base = Layer.mergeAll(
        MemoryThreadStoreLive,
        MemorySubmissionLedgerLive,
        WakeScheduler.layerNoop,
        ToolReconciler.uncertain,
        DurableRuntimeFailpoint.layer,
        RunToolAuthorization.allowAll,
        tools.toLayer({
          lookup: () => Effect.succeed("finding ".repeat(100)),
          approve: () => Effect.succeed("approved"),
        }),
        DurableRuntimeConfig.layer({
          deploymentId: Schema.decodeSync(DeploymentId)("recovery-usage"),
          producerId: Schema.decodeSync(ProducerId)("recovery-usage"),
          estimateCostMicrousd: () =>
            Effect.succeed({
              costMicrousd: 25,
              pricingVersion: "test-v1",
              pricingStatus: "estimated" as const,
            }),
        }),
      ).pipe(Layer.provideMerge(NodeCrypto.layer));

      yield* Effect.gen(function* () {
        const store = yield* ThreadStore;
        const threadId = Schema.decodeSync(ThreadId)("recovery-usage");

        const freshRuntime = DurableAgentRuntime.pipe(
          Effect.provide(Layer.fresh(DurableAgentRuntime.layer)),
        );

        const receipt = yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* freshRuntime;

            // Native summaries cover prior Runs, so first create real canonical history.
            yield* runtime.submit(agent, "look up", {
              threadId,
              principal: Schema.decodeSync(Principal)("viewer"),
              idempotencyKey: Schema.decodeSync(IdempotencyKey)("history"),
              definitions,
            });
            yield* runtime.processThread(agent, threadId);
            expect(requests).toBe(3);

            const receipt = yield* runtime.submit(agent, "go", {
              threadId,
              principal: Schema.decodeSync(Principal)("viewer"),
              idempotencyKey: Schema.decodeSync(IdempotencyKey)("input"),
              definitions,
            });

            yield* runtime.processThread(agent, threadId);
            expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");

            return receipt;
          }),
        );

        expect(requests).toBe(6);

        const suspended = yield* store
          .read(ThreadRead.make({ threadId, limit: 128 }))
          .pipe(Stream.runCollect);

        const retryResponse = suspended
          .map(({ record }) => record.payload)
          .find(
            (payload) =>
              payload._tag === "ModelResponseRecorded" &&
              payload.modelUsage?.some((call) => call.response?.id === "response-6"),
          );

        expect(retryResponse).toMatchObject({
          unobservedModelCalls: 1,
          modelUsage: [
            { response: { id: "response-5" }, purpose: "summary" },
            { response: { id: "response-6" }, purpose: "turn" },
          ],
        });
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* freshRuntime;

            yield* runtime.resolveApproval(
              ApprovalDecisionCommand.make({
                submissionId: receipt.submissionId,
                toolCallId: Schema.decodeSync(ToolCallId)("approval-1"),
                decision: "approved",
                resolver: "usage-test",
                reason: "resume accounting regression",
              }),
            );
            yield* runtime.processThread(agent, threadId);
          }),
        );

        const records = yield* store
          .read(ThreadRead.make({ threadId, limit: 128 }))
          .pipe(Stream.runCollect);

        const settlement = records
          .map(({ record }) => record.payload)
          .find(
            (payload) =>
              payload._tag === "SubmissionSettled" && payload.submissionId === receipt.submissionId,
          );

        expect(settlement).toMatchObject({
          outcome: "completed",
          usageSummary: {
            modelCalls: 3,
            costMicrousd: 75,
            unobservedModelCalls: 1,
            usageStatus: "partial",
            pricingStatus: "partial",
          },
        });
        expect(settlement).not.toHaveProperty("uncommittedModelUsage");
        expect(
          records
            .flatMap(({ record }) =>
              record.payload._tag === "ModelResponseRecorded"
                ? (record.payload.modelUsage ?? [])
                : [],
            )
            .map((call) => call.response?.id),
        ).toEqual([
          "response-1",
          "response-2",
          "response-3",
          "response-5",
          "response-6",
          "response-7",
        ]);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* freshRuntime;

            yield* runtime.processThread(agent, threadId);
          }),
        );
        expect(requests).toBe(7);
        expect(
          yield* store.read(ThreadRead.make({ threadId, limit: 128 })).pipe(Stream.runCollect),
        ).toEqual(records);
      }).pipe(Effect.provide(base));
    }),
);
