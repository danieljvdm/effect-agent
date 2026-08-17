import { Agent } from "@effect-agent/core";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Layer, Redacted, Ref, Stream } from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import { codeModeAgent } from "./agent.ts";

const usage = { inputTokens: {}, outputTokens: {} };

/**
 * The demonstration program the scripted model "writes": it queries the
 * curated warehouse, filters locally to the high-revenue customers, and
 * returns one small JSON answer. On the live profile the real model writes
 * its own program instead.
 */
const scriptedProgram = `async () => {
  const result = await warehouse.listInvoices({ minimumRevenue: 10000 });
  const top = result.invoices;
  console.log("matched", top.length, "high-revenue customers");
  return {
    topCustomers: top.map((row) => row.customer),
    count: top.length,
  };
}`;

const scriptedAnswer = JSON.stringify({
  answer:
    "The customers above $10k in revenue are Stellar Freight, Vertex Robotics, and Nimbus Analytics.",
});

/**
 * A deterministic scripted model: turn 1 emits one `run_javascript` Tool Call
 * carrying `program`; turn 2 emits `answer`. This is the default so the demo
 * runs and tests without any credential.
 */
const makeScriptedModel = (program: string, answer: string) =>
  Model.make(
    "scripted",
    "warehouse-analyst",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);
        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                Effect.map((value) =>
                  Stream.fromIterable<Response.StreamPartEncoded>(
                    value === 0
                      ? [
                          {
                            type: "tool-call",
                            id: "code-1",
                            name: "run_javascript",
                            params: { code: program },
                            providerExecuted: false,
                          },
                          { type: "finish", reason: "tool-calls", usage },
                        ]
                      : [
                          { type: "text-start", id: "answer" },
                          { type: "text-delta", id: "answer", delta: answer },
                          { type: "text-end", id: "answer" },
                          { type: "finish", reason: "stop", usage },
                        ],
                  ),
                ),
              ),
            ),
        });
      }),
    ),
  );

export const scriptedModel = makeScriptedModel(scriptedProgram, scriptedAnswer);

/** OpenAI live profile, used when an OPENAI_API_KEY secret is present. */
export const liveModel = (apiKey: string) =>
  Model.make(
    "openai",
    "warehouse-analyst",
    OpenAiLanguageModel.model("gpt-5.6-sol").pipe(
      Layer.provide(
        OpenAiClient.layer({ apiKey: Redacted.make(apiKey) }).pipe(
          Layer.provide(FetchHttpClient.layer),
        ),
      ),
    ),
  );

/** The demo agent bound to the deterministic scripted profile. */
export const scriptedAgent = Agent.withModel(codeModeAgent, scriptedModel);

/** The demo agent bound to the live OpenAI profile. */
export const liveAgent = (apiKey: string) => Agent.withModel(codeModeAgent, liveModel(apiKey));
