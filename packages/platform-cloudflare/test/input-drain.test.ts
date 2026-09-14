import {
  DurableObjectContext,
  threadNamespaceLayer,
} from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import * as ThreadObject from "@effect-agent/platform-cloudflare/thread-object";
import { env, runInDurableObject } from "cloudflare:test";
import { Effect, Layer, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { LedgerError, SubmissionLedger } from "effect-agent/submission-ledger";
import { ThreadStore, ThreadExportRequest } from "effect-agent/thread-store";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";
import { expect, it } from "vite-plus/test";

import { submitOptions } from "./fixtures.ts";
import { stubFor } from "./harness.ts";

// Regression: https://github.com/danieljvdm/effect-agent/commit/efa46d1378493e60102080798064fa8e73f477b9
// A returned Tool failure was followed by a failed input-drain authority read, leaving
// the completed call unknown and blocking its FIFO successor indefinitely.
it("preserves a returned Tool result across an input-drain failure without replaying its handler", () => {
  const thread = "cf-returned-result-before-drain";

  return runInDurableObject(stubFor(thread), (_instance, state) => {
    let toolCalls = 0;
    let drainFailures = 0;

    const Failed = Tool.make("failed", {
      parameters: Schema.Struct({}),
      success: Schema.String,
      failure: Schema.String,
      failureMode: "return",
    });

    const tools = Toolkit.make(Failed);
    const usage = { inputTokens: {}, outputTokens: {} };

    const model = Model.make(
      "scripted",
      "input-drain",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) =>
            Stream.fromIterable<Response.StreamPartEncoded>(
              JSON.stringify(options.prompt).includes('"tool-result"')
                ? [
                    { type: "text-start", id: "answer" },
                    { type: "text-delta", id: "answer", delta: '"fallback"' },
                    { type: "text-end", id: "answer" },
                    { type: "finish", reason: "stop", usage },
                  ]
                : [
                    {
                      type: "tool-call",
                      id: "returned-failure",
                      name: "failed",
                      params: {},
                      providerExecuted: false,
                    },
                    { type: "finish", reason: "tool-calls", usage },
                  ],
            ),
        }),
      ),
    );

    const agent = Agent.withModel(
      Agent.make("input-drain", {
        input: Schema.Struct({ question: Schema.String, ref: Schema.String }),
        output: Schema.String,
        instructions: "Try the Tool, then answer.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      }),
      model,
    );

    const failingDrain = Layer.effect(
      SubmissionLedger,
      Effect.map(SubmissionLedger, (ledger) =>
        SubmissionLedger.of({
          ...ledger,
          claimJoining: (request) =>
            Effect.suspend(() => {
              if (toolCalls > 0 && drainFailures === 0) {
                drainFailures++;

                return Effect.fail(
                  LedgerError.make({
                    operation: "claimJoining",
                    message: "Input admission authority is unavailable",
                  }),
                );
              }

              return ledger.claimJoining(request);
            }),
        }),
      ),
    );

    const runtimeLayer = DurableAgentRuntime.layer.pipe(
      Layer.provide(failingDrain),
      Layer.provideMerge(ThreadObject.layer([])),
      Layer.provide(
        ThreadObject.layerConfig({ deploymentId: "input-drain", producerPrefix: "input-drain" }),
      ),
      Layer.provide([DurableObjectContext.layer(state, env), threadNamespaceLayer(env, "THREADS")]),
    );

    return Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const store = yield* ThreadStore;

        const receipt = yield* runtime.submit(
          agent,
          { question: "try", ref: thread },
          submitOptions(thread, "first"),
        );

        const process = runtime.processThread(agent, receipt.threadId).pipe(
          Effect.provide(
            tools.toLayer({
              failed: () =>
                Effect.suspend(() => {
                  toolCalls++;

                  return Effect.fail("unavailable");
                }),
            }),
          ),
        );

        const failure = yield* process.pipe(Effect.flip);

        expect(failure).toMatchObject({ _tag: "LedgerError", operation: "claimJoining" });
        expect(drainFailures).toBe(1);

        const before = yield* store.export(
          ThreadExportRequest.make({ threadId: receipt.threadId }),
        );

        expect(
          before.records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
          "a returned Tool outcome must be durable before the next input-drain can fail",
        ).toHaveLength(1);

        const follower = yield* runtime.submit(
          agent,
          { question: "follow up", ref: thread },
          submitOptions(thread, "second"),
        );

        yield* process;
        expect(yield* runtime.submissionStatus(receipt)).toMatchObject({ _tag: "settled" });
        expect(yield* runtime.submissionStatus(follower)).toMatchObject({ _tag: "settled" });
        expect(toolCalls).toBe(1);
        const after = yield* store.export(ThreadExportRequest.make({ threadId: receipt.threadId }));

        expect(
          after.records.filter(({ record }) => record.payload._tag === "ToolCallUnknown"),
        ).toHaveLength(0);
      }).pipe(Effect.provide(runtimeLayer)),
    );
  });
});
