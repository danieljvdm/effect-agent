import { DecisionModel, type DecisionSchema } from "@effect-agent/ai-decision";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { Console, Effect, Layer, Schema, Stream } from "effect";
import { Agent, AgentRuntime, InMemory, SelectiveCompactor } from "effect-agent";
import { CLEARED_TOOL_RESULT, estimatePromptTokens } from "effect-agent/compaction";
import { ContextCompactor } from "effect-agent/context-compactor";
import type { ModelCallUsage } from "effect-agent/usage";
import { LanguageModel, Model, Prompt, Toolkit } from "effect/unstable/ai";
import { Command, Flag } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";

export const receiptEvidence =
  "The booking receipt is RECEIPT-42. Preserve it for the final answer. " + "r".repeat(800);

export const noiseEvidence =
  "Superseded directory listing; no booking information. " + "unused.txt\n".repeat(600);

export const newestEvidence = "LATEST-OBSERVATION: the booking is confirmed. " + "n".repeat(800);

const round = (id: string, name: string, result: string): ReadonlyArray<Prompt.Message> => [
  Prompt.makeMessage("assistant", {
    content: [Prompt.makePart("tool-call", { id, name, params: {}, providerExecuted: false })],
  }),
  Prompt.makeMessage("tool", {
    content: [
      Prompt.makePart("tool-result", {
        id,
        name,
        result,
        isFailure: false,
        providerExecuted: false,
      }),
    ],
  }),
];

/** Exact values occur only in tool evidence, outside user instructions and the oracle. */
export const fixtureHistory = Prompt.fromMessages([
  Prompt.userMessage({
    content: [
      Prompt.textPart({ text: "Book the trip. Keep the booking receipt for my final report." }),
    ],
  }),
  ...round("receipt", "book_trip", receiptEvidence),
  ...round("noise", "list_files", noiseEvidence),
  ...round("newest", "booking_status", newestEvidence),
]);

const ProbeOutput = Schema.Struct({
  receiptAvailable: Schema.Boolean,
  newestAvailable: Schema.Boolean,
});

/** Deterministic transport for offline wiring checks; these probabilities are not model evidence. */
export const scriptedDecisionLayer = Layer.effect(
  DecisionModel.DecisionModel,
  DecisionModel.make({
    evaluate: (request) =>
      Effect.succeed({
        provider: "scripted",
        model: "selection-fixture",
        usage: { inputTokens: 200, outputTokens: 2 },
        answers: Object.fromEntries(
          Object.entries(request.questions).map(([id, question]) => [
            id,
            {
              type: "probability",
              probability: JSON.stringify(question.instructions).includes("list_files")
                ? 0.01
                : 0.99,
            },
          ]),
        ),
      }),
  }),
);

/** The continuation is a literal probe, deliberately not a generative quality evaluation. */
export const runProbe = Effect.fn("SelectiveSpike.runProbe")(function* (
  strategy: "age" | "selective",
) {
  let outgoing = Prompt.empty;
  let original = Prompt.empty;
  const usage: Array<ModelCallUsage> = [];
  const evaluations: Array<DecisionSchema.EvaluateResponse> = [];

  const observedDecisionLayer = Layer.effect(
    DecisionModel.DecisionModel,
    Effect.gen(function* () {
      const model = yield* DecisionModel.DecisionModel;

      return yield* DecisionModel.make({
        evaluate: (request) =>
          model.evaluate(request).pipe(
            Effect.tap((response) =>
              Effect.sync(() => {
                evaluations.push(response);
              }),
            ),
          ),
      });
    }),
  );

  const probeModel = Model.make(
    "scripted",
    "literal-evidence-probe",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          outgoing = request.prompt;
          const text = JSON.stringify(outgoing);

          return Stream.fromIterable([
            { type: "text-start", id: "probe" },
            {
              type: "text-delta",
              id: "probe",
              delta: JSON.stringify({
                receiptAvailable: text.includes("RECEIPT-42"),
                newestAvailable: text.includes("LATEST-OBSERVATION"),
              }),
            },
            { type: "text-end", id: "probe" },
            {
              type: "finish",
              reason: "stop",
              usage: {
                inputTokens: { total: estimatePromptTokens(outgoing.content) },
                outputTokens: { total: 10 },
              },
            },
          ]);
        },
      }),
    ),
  );

  const agent = Agent.withModel(
    Agent.make("selective-compaction-spike", {
      input: Schema.String,
      output: ProbeOutput,
      instructions:
        "Report whether the original receipt and latest booking observation are available.",
      toolkit: Toolkit.empty,
      policy: {
        maxTurns: 2,
        contextTokenLimit: 1_400,
        tokenBudget: 20_000,
        completionReserveTokens: 1_000,
        compaction: { mode: "prune", keepRecentTokens: 1 },
      },
    }),
    probeModel,
  );

  const compactor =
    strategy === "age"
      ? ContextCompactor.layer
      : SelectiveCompactor.layer().pipe(
          Layer.provide(ContextCompactor.layerRollover),
          Layer.provide(observedDecisionLayer),
        );

  const result = yield* AgentRuntime.run(agent, "Prepare the report from the recorded evidence.", {
    history: fixtureHistory,
    onHistory: (history) =>
      Effect.sync(() => {
        original = history;
      }),
    budget: {
      guard: (effect) => effect,
      consume: (delta) =>
        Effect.sync(() => {
          if (delta.modelUsage !== undefined) usage.push(delta.modelUsage);
        }),
    },
  }).pipe(
    Effect.provide(compactor),
    Effect.provide(InMemory.layer),
    // Admission can fail after a paid evaluation. Keep its evidence visible without retrying.
    Effect.tapError((error) =>
      Console.error(
        JSON.stringify({
          strategy,
          failure: error._tag,
          selectorEvaluations: evaluations,
          selectorUsage: usage.filter((call) => call.purpose === "compaction"),
        }),
      ),
    ),
  );

  return {
    strategy,
    ...result.output,
    retainedHistoryIntact:
      JSON.stringify(original).includes(receiptEvidence) &&
      JSON.stringify(original).includes("unused.txt"),
    outgoingTokensEstimate: estimatePromptTokens(outgoing.content),
    clearedResults: outgoing.content.flatMap((message) =>
      message.role === "tool"
        ? message.content.flatMap((part) =>
            part.type === "tool-result" && part.result === CLEARED_TOOL_RESULT ? [part.id] : [],
          )
        : [],
    ),
    selectorEvaluations: evaluations,
    selectorUsage: usage.filter((call) => call.purpose === "compaction"),
  };
});

const liveDecisionLayer = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layer),
  Layer.provide(TypeSafeClient.Config.layer),
  Layer.provide(FetchHttpClient.layer),
);

export const command = Command.make(
  "compaction-spike",
  {
    live: Flag.Boolean("live").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Send one bounded synthetic selection request to Jev using TYPESAFE_API_KEY. Continuation remains scripted.",
      ),
    ),
  },
  Effect.fn(function* ({ live }) {
    const program = Effect.gen(function* () {
      const age = yield* runProbe("age");
      const selective = yield* runProbe("selective");

      yield* Console.log(
        JSON.stringify(
          {
            selector: live ? "jev-latest" : "scripted; wiring evidence only",
            continuation: "scripted literal probe; not a task-quality or latency benchmark",
            comparisons: [age, selective],
          },
          null,
          2,
        ),
      );
    });

    yield* program.pipe(Effect.provide(live ? liveDecisionLayer : scriptedDecisionLayer));
  }),
).pipe(
  Command.withDescription(
    "Compare native age pruning with experimental selective result pruning on the same synthetic history.",
  ),
);
