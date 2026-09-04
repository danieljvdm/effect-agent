import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import * as ThreadObject from "@effect-agent/platform-cloudflare/ThreadObject";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { Context, Deferred, Effect, Layer, Ref, Schema, Stream } from "effect";
import { DurableObject } from "effect-cf";
import {
  LanguageModel,
  Model,
  Tool,
  Toolkit,
  type Response as AiResponse,
} from "effect/unstable/ai";

import { ObjectStatus } from "./heap-contracts.ts";

class Gate extends Context.Service<
  Gate,
  {
    readonly entered: Ref.Ref<boolean>;
    readonly released: Deferred.Deferred<void>;
    readonly modelCalls: Ref.Ref<number>;
    readonly toolCalls: Ref.Ref<number>;
  }
>()("heap-benchmark/Gate") {
  static readonly layer = Layer.effect(
    Gate,
    Effect.gen(function* () {
      return Gate.of({
        entered: yield* Ref.make(false),
        released: yield* Deferred.make<void>(),
        modelCalls: yield* Ref.make(0),
        toolCalls: yield* Ref.make(0),
      });
    }),
  );
}

const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ text: Schema.String }),
  dependencies: [Gate],
});

const Summarize = Tool.make("summarize", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ text: Schema.String }),
  dependencies: [Gate],
});

const tools = Toolkit.make(Lookup, Summarize);

const toolLayer = tools.toLayer({
  lookup: () =>
    Effect.gen(function* () {
      const gate = yield* Gate;

      yield* Ref.update(gate.toolCalls, (n) => n + 1);
      yield* Ref.set(gate.entered, true);
      yield* Deferred.await(gate.released);

      return { text: "lookup ".padEnd(16_384, "x") };
    }),
  summarize: () =>
    Effect.gen(function* () {
      const gate = yield* Gate;

      yield* Ref.update(gate.toolCalls, (n) => n + 1);

      return { text: "summary ".padEnd(16_384, "y") };
    }),
});

const usage = { inputTokens: {}, outputTokens: {} };

const parts = (hasResults: boolean): ReadonlyArray<AiResponse.StreamPartEncoded> =>
  hasResults
    ? [
        { type: "text-start", id: "answer" },
        { type: "text-delta", id: "answer", delta: '{"answer":"synthetic result"}' },
        { type: "text-end", id: "answer" },
        { type: "finish", reason: "stop", usage },
      ]
    : [
        {
          type: "tool-call",
          id: "lookup-1",
          name: "lookup",
          params: { query: "synthetic" },
          providerExecuted: false,
        },
        {
          type: "tool-call",
          id: "summarize-1",
          name: "summarize",
          params: { query: "synthetic" },
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ];

const model = Model.make(
  "synthetic",
  "heap-benchmark",
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const gate = yield* Gate;

      return yield* LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt }) =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* Ref.update(gate.modelCalls, (n) => n + 1);

              return Stream.fromIterable(parts(JSON.stringify(prompt).includes('"tool-result"')));
            }),
          ),
      });
    }),
  ),
);

const definition = Agent.make("heap-benchmark", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ question }) => question,
  toolkit: tools,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "60 seconds",
    toolConcurrency: 2,
  }),
});

const agent = Agent.withModel(definition, model);

const definitions = DefinitionDigestInput.make({
  agent: { id: definition.id, revision: 1 },
  model: { provider: "synthetic", name: "heap-benchmark" },
  tools: [
    { name: "lookup", revision: 1 },
    { name: "summarize", revision: 1 },
  ],
});

const application = ThreadObject.layer([{ agent, definitions }]).pipe(
  Layer.provide(toolLayer),
  Layer.provideMerge(Gate.layer),
);

export class HeapThread extends ThreadObject.make(application, {
  namespaceBinding: "HEAP_THREADS",
  deploymentId: "heap-benchmark",
  producerPrefix: "heap-benchmark",
}) {
  status() {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const gate = yield* Gate;

        return ObjectStatus.make({
          active: (yield* Ref.get(gate.entered)) && !(yield* Deferred.isDone(gate.released)),
          modelCalls: yield* Ref.get(gate.modelCalls),
          toolCalls: yield* Ref.get(gate.toolCalls),
        });
      }),
    );
  }

  release() {
    return this[DurableObject.RunSymbol](
      Effect.flatMap(Gate, (gate) => Deferred.succeed(gate.released, undefined)),
    );
  }

  run() {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const { threadId } = yield* ThreadObjectIdentity;

        const receipt = yield* runtime.submit(
          agent,
          { question: "synthetic input ".padEnd(4096, "q") },
          {
            threadId,
            principal: Principal.make("heap-benchmark"),
            idempotencyKey: IdempotencyKey.make("one-run"),
            definitions: yield* digestDefinitions(definitions),
          },
        );

        yield* runtime.processThreadResolved(threadId);

        return (yield* runtime.awaitSettlement(receipt)).outcome;
      }),
    );
  }
}

interface Env {
  HEAP_THREADS: DurableObjectNamespace<HeapThread>;
  BENCH_TOKEN: string;
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return Effect.runPromise(
      Effect.gen(function* () {
        if (
          !env.BENCH_TOKEN ||
          request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`
        )
          return new Response("Unauthorized", { status: 401 });
        const url = new URL(request.url);

        if (url.pathname === "/ready") return Response.json({ ready: true });

        const id = yield* Schema.decodeUnknownEffect(
          Schema.String.check(Schema.isPattern(/^object-[0-9]+$/)),
        )(url.searchParams.get("id"));

        const object = env.HEAP_THREADS.getByName(id);

        switch (url.pathname) {
          case "/status":
            return Response.json(yield* Effect.promise(() => object.status()));
          case "/release":
            return Response.json(yield* Effect.promise(() => object.release()));
          case "/run":
            return Response.json(yield* Effect.promise(() => object.run()));
          default:
            return new Response("Not found", { status: 404 });
        }
      }),
    );
  },
};
