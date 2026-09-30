import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/cloudflare-thread-client";
import { runInDurableObject } from "cloudflare:test";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { DurableWorkerBinding } from "effect-agent/agent-registration";
import { ContextCompactor } from "effect-agent/context-compactor";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { RunContextPreparation, RunToolAuthorization } from "effect-agent/run-options";
import { submissionSettlementRecordId } from "effect-agent/submission-ledger";
import { ThreadCheckpoint, ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { DurableObject } from "effect-cf";
import { LanguageModel, Model, Toolkit } from "effect/unstable/ai";
import { describe, expect, it } from "vite-plus/test";

import {
  contextCompactorDefinition,
  contextCompactorProbe,
  contextAuthorizationProbe,
  searchDefinition,
  submitOptions,
  TEST_DIGESTS,
} from "./fixtures.ts";
import {
  allSettled,
  drainAlarmsUntil,
  readCanonical,
  runClient,
  stubFor,
  type TestNamespace,
} from "./harness.ts";

let laneCounter = 0;
const lane = (label: string): string => `cf-context-compactor-${label}-${laneCounter++}`;

const submitAndSettle = async (
  thread: string,
  question: string,
  key: string,
  namespace: TestNamespace,
) => {
  const receipt = await runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition: contextCompactorDefinition },
        { question, ref: thread },
        submitOptions(thread, key),
      );
    }),
    namespace,
  );

  await drainAlarmsUntil(thread, allSettled(thread, namespace), { namespace });

  const settlement = await runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.awaitSettlement(receipt);
    }),
    namespace,
  );

  const records = await readCanonical(thread, namespace);

  const terminal = records.find(
    (envelope) => envelope.record.recordId === submissionSettlementRecordId(receipt.submissionId),
  )?.record.payload;

  if (terminal?._tag !== "SubmissionSettled") {
    throw new Error(`Missing canonical settlement for ${receipt.submissionId}`);
  }

  return { receipt, settlement, records, terminal };
};

const abortIncarnation = (thread: string): Promise<void> =>
  runInDurableObject(stubFor(thread, "CONTEXT_COMPACTOR"), (_instance, state) => {
    state.abort("issue #49 reconstruction probe");
  }).then(
    () => undefined,
    () => undefined,
  );

describe("Cloudflare replaceable compaction", () => {
  // https://github.com/danieljvdm/effect-agent/issues/692
  // Memory IDs do not expose native routed IDs rejecting the fresh-Run context cache.
  it("reuses compacted context with native routed IDs and falls back from older certificates", () => {
    const thread = lane("checkpoint:run:nested:tool-settled");

    return runInDurableObject(stubFor(thread), (instance) =>
      instance[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const store = yield* ThreadStore;
          const prompts: Array<string> = [];
          let rollover = false;
          let legacy = false;
          let retiredThrough = 0;
          let retiredReads = 0;

          const model = Model.make(
            "scripted",
            "routed-checkpoint",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: ({ prompt }) => {
                  prompts.push(JSON.stringify(prompt));

                  return Stream.fromIterable([
                    { type: "text-start", id: "answer" },
                    { type: "text-delta", id: "answer", delta: '"done"' },
                    { type: "text-end", id: "answer" },
                    {
                      type: "finish",
                      reason: "stop",
                      usage: { inputTokens: {}, outputTokens: {} },
                    },
                  ]);
                },
              }),
            ),
          );

          const agent = Agent.withModel(
            Agent.make("routed-checkpoint", {
              input: Schema.String,
              output: Schema.String,
              instructions: "Retain the conversation.",
              toolkit: Toolkit.empty,
              policy: { maxTurns: 1, maxDuration: "30 seconds", contextTokenLimit: 250_000 },
            }),
            model,
          );

          const binding = yield* DurableWorkerBinding.make(agent, TEST_DIGESTS);
          const checkpoints = store.recoveryCheckpoints!;

          const observed = ThreadStore.of({
            ...store,
            read: (request) =>
              store.read(request).pipe(
                Stream.tap((entry) =>
                  Effect.sync(() => {
                    if (entry.sequence <= retiredThrough) retiredReads++;
                  }),
                ),
              ),
            recoveryCheckpoints: {
              ...checkpoints,
              load: (request) =>
                checkpoints.load(request).pipe(
                  Effect.map(
                    Option.map((checkpoint) =>
                      legacy
                        ? ThreadCheckpoint.make({
                            ...checkpoint,
                            engineVersion: "effect-agent/recovery@3",
                          })
                        : checkpoint,
                    ),
                  ),
                ),
            },
          });

          const runtime = yield* DurableAgentRuntime.pipe(
            Effect.provide(
              DurableAgentRuntime.layerWithBindings([binding]).pipe(
                Layer.provide(
                  Layer.mergeAll(RunToolAuthorization.allowAll, ContextCompactor.layerRollover),
                ),
              ),
            ),
            Effect.provideService(ThreadStore, observed),
            Effect.provideService(RunContextPreparation, {
              hook: {
                prepare: (request) =>
                  Effect.succeed({
                    prompt: request.source,
                    ...(rollover ? { rollover: { handoff: "Retained handoff." } } : {}),
                  }),
              },
            }),
          );

          const process = Effect.fnUntraced(function* (input: string, key = input) {
            const receipt = yield* runtime.submit(agent, input, submitOptions(thread, key));

            expect(receipt.submissionId.endsWith(`:${thread}`)).toBe(true);
            const result = yield* runtime.processThreadHead(receipt.threadId);

            expect(Option.isSome(result) && result.value).toMatchObject({
              submissionId: receipt.submissionId,
              outcome: "completed",
            });

            return yield* store.export(ThreadExportRequest.make({ threadId: receipt.threadId }));
          });

          yield* process("retired request");
          rollover = true;

          // The context fits by itself; copying it beside the completed Run's input/response
          // records must not disable the cache by exceeding the persisted JSON byte bound.
          const original = yield* process(
            `begin compacted conversation ${"x".repeat(400_000)}`,
            "begin compacted conversation",
          );

          rollover = false;
          retiredThrough = original.records.length;

          for (const input of ["first fresh request", "second fresh request"]) {
            const completed = yield* process(input);

            expect(prompts.at(-1)).toContain("Retained handoff.");
            expect(prompts.at(-1)).toContain("begin compacted conversation");
            expect(prompts.at(-1)).toContain(input);
            expect(prompts.at(-1)).not.toContain("retired request");
            expect(completed.records.slice(0, original.records.length)).toEqual(original.records);
            expect(retiredReads).toBe(0);
            retiredThrough = completed.records.length;
          }
          expect(prompts.at(-1)).toContain("first fresh request");

          legacy = true;
          yield* process("fresh request with older certificate");
          expect(retiredReads).toBeGreaterThan(0);
          expect(prompts.at(-1)).toContain("second fresh request");
          expect(prompts.at(-1)).toContain("fresh request with older certificate");
          legacy = false;
          rollover = true;
          yield* process("new window request");
          expect(prompts.at(-1)).toContain("new window request");
          expect(prompts.at(-1)).not.toContain("first fresh request");
        }).pipe(Effect.scoped),
      ),
    );
  });

  it("retains independent Tool authorization alongside a compactor after eviction", async () => {
    const thread = lane("authorization");

    for (const incarnation of [1, 2]) {
      await submitAndSettle(thread, "seed", `${thread}-seed-${incarnation}`, "CONTEXT_COMPACTOR");

      const compacted = await submitAndSettle(
        thread,
        "compact",
        `${thread}-compact-${incarnation}`,
        "CONTEXT_COMPACTOR",
      );

      expect(compacted.terminal.result).toEqual({ answer: "compacted" });

      const receipt = await runClient(
        CloudflareThreadClient.use((client) =>
          client.submit(
            { definition: searchDefinition },
            { question: "search", ref: thread },
            submitOptions(thread, `${thread}-denied-${incarnation}`),
          ),
        ),
        "CONTEXT_COMPACTOR",
      );

      await drainAlarmsUntil(thread, allSettled(thread, "CONTEXT_COMPACTOR"), {
        namespace: "CONTEXT_COMPACTOR",
      });

      const settlement = await runClient(
        CloudflareThreadClient.use((client) => client.awaitSettlement(receipt)),
        "CONTEXT_COMPACTOR",
      );

      expect(settlement).toMatchObject({
        outcome: "failed",
        failure: {
          errorTag: "AgentToolAuthorizationDenied",
          message: "host denied Tool execution",
        },
      });
      expect(contextAuthorizationProbe(thread)).toEqual({
        acquisitions: incarnation,
        calls: incarnation,
      });
      expect(contextCompactorProbe(thread).acquisitions).toBe(incarnation);
      const records = await readCanonical(thread, "CONTEXT_COMPACTOR");

      expect(records.some(({ record }) => record.payload._tag === "ToolCallSettled")).toBe(false);
      if (incarnation === 1) await abortIncarnation(thread);
    }
  });
});
