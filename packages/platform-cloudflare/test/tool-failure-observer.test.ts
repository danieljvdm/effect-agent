import { Agent, AgentPolicy, ConversationId } from "@effect-agent/core";
import { toolFailureObserverLayer, type ToolFailureObservation } from "@effect-agent/engine";
import { DurableAgentRuntime } from "@effect-agent/session";
import { env, runInDurableObject } from "cloudflare:test";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";
import { expect, it } from "vite-plus/test";

import {
  CloudflareDurableRuntime,
  DurableObjectContext,
  conversationNamespaceLayer,
} from "../src/index.ts";
import { submitOptions } from "./fixtures.ts";
import { stubFor } from "./harness.ts";

it.each([false, true])(
  "RUN-036 Cloudflare observer option owns installation (configured=%s)",
  (configured) => {
    const conversation = `cf-tool-failure-observer-${configured}`;
    // The platform test API owns this Promise boundary. The Run and observer stay in Effect and
    // observations are asserted inside the Object, never encoded into an RPC or stored record.
    return runInDurableObject(stubFor(conversation), (_instance, state) => {
      const observations: Array<ToolFailureObservation> = [];
      const ambient: Array<ToolFailureObservation> = [];
      const Failed = Tool.make("failed", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: Schema.String,
        failureMode: "return",
      });
      const tools = Toolkit.make(Failed);
      const usage = { inputTokens: {}, outputTokens: {} };
      const runtimeLayer = CloudflareDurableRuntime.layer({
        deploymentId: "observer-test",
        producerPrefix: "observer-test",
        toolFailureObserver: configured
          ? {
              observe: (observation) =>
                Effect.sync(() => {
                  observations.push(observation);
                }),
            }
          : undefined,
      }).pipe(
        Layer.provide([
          DurableObjectContext.layer(state, env),
          conversationNamespaceLayer(env, "CONVERSATIONS"),
          toolFailureObserverLayer({
            observe: (observation) =>
              Effect.sync(() => {
                ambient.push(observation);
              }),
          }),
        ]),
      );
      return Effect.runPromise(
        Effect.gen(function* () {
          const turn = yield* Ref.make(0);
          const model = Model.make(
            "scripted",
            "observer-test",
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
                                  id: "cf-failure",
                                  name: "failed",
                                  params: {},
                                  providerExecuted: false,
                                },
                                { type: "finish", reason: "tool-calls", usage },
                              ]
                            : [
                                { type: "text-start", id: "answer" },
                                { type: "text-delta", id: "answer", delta: '"fallback"' },
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
          const agent = Agent.withModel(
            Agent.make("cloudflare-observer", {
              input: Schema.Struct({ question: Schema.String, ref: Schema.String }),
              output: Schema.String,
              instructions: "Try the Tool, then answer.",
              toolkit: tools,
              policy: AgentPolicy.make({
                maxTurns: 2,
                maxToolCalls: 1,
                maxDuration: "30 seconds",
                toolConcurrency: 1,
              }),
            }),
            model,
          );
          const runtime = yield* DurableAgentRuntime;
          const receipt = yield* runtime.submit(
            agent,
            { question: "try", ref: "observer" },
            submitOptions(conversation, "observer"),
          );
          yield* runtime
            .processConversation(agent, ConversationId.make(conversation))
            .pipe(Effect.provide(tools.toLayer({ failed: () => Effect.fail("unavailable") })));
          expect((yield* runtime.awaitSettlement(receipt)).outcome).toBe("completed");
          expect(observations).toEqual(
            configured
              ? [
                  expect.objectContaining({
                    _tag: "ModelToolFailure",
                    kind: "declared-failure",
                    toolName: "failed",
                    toolCallId: "cf-failure",
                    tag: "UnknownError",
                  }),
                ]
              : [],
          );
          expect(ambient).toEqual([]);
        }).pipe(Effect.provide(runtimeLayer)),
      );
    });
  },
);
