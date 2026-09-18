import { ThreadObject } from "@effect-agent/platform-alchemy-cloudflare";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { localState, Stack } from "alchemy";
import { providers } from "alchemy/Cloudflare";
import { DurableObject } from "alchemy/Cloudflare/Workers/DurableObject";
import { Worker } from "alchemy/Cloudflare/Workers/Worker";
import { Config, Effect, Layer, Schema } from "effect";
import { Agent } from "effect-agent";
import { DefinitionDigestInput } from "effect-agent/records";
import { Toolkit } from "effect/unstable/ai";
import { HttpServerResponse, FetchHttpClient } from "effect/unstable/http";

export const planner = Agent.make("planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer the question briefly.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 3, maxToolCalls: 1, maxDuration: "30 seconds" },
});

export const definitions = DefinitionDigestInput.make({
  agent: { id: planner.id, revision: 1 },
  model: { provider: "openai", name: "gpt-4.1-mini" },
  tools: [],
});

const application = ThreadObject.layer([
  {
    agent: planner,
    definitions,
    model: OpenAiLanguageModel.model("gpt-4.1-mini"),
  },
]).pipe(
  Layer.provide(
    OpenAiClient.layerConfig({
      apiKey: Config.Redacted("OPENAI_API_KEY"),
    }).pipe(Layer.provide(FetchHttpClient.layer)),
  ),
);

export class Threads extends DurableObject<Threads, ThreadObject.Rpc>()("THREADS") {}

const ThreadsLive = Threads.make(
  ThreadObject.make(application, {
    namespaceBinding: "THREADS",
    deploymentId: "alchemy-planner",
    producerPrefix: "planner",
  }),
);

const worker = Worker(
  "Planner",
  {
    main: import.meta.filename,
    compatibility: { date: "2026-08-18", flags: ["nodejs_compat"] },
    env: { OPENAI_API_KEY: Config.Redacted("OPENAI_API_KEY") },
  },
  Effect.gen(function* () {
    yield* Threads;

    // Add authenticated application routes here; the namespace is private to the Worker.
    return { fetch: Effect.succeed(HttpServerResponse.text("Planner host ready")) };
  }).pipe(Effect.provide(ThreadsLive)),
);

export default Stack("alchemy-planner", { providers: providers(), state: localState() }, worker);
