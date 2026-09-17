import { DecisionModel } from "@effect-agent/ai-decision";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Cause, Clock, Config, Console, Effect, Exit, FileSystem, Layer, Schema } from "effect";
import { Agent, AgentRuntime, ToolDiscovery, ToolSelector } from "effect-agent";
import { ThreadHistory } from "effect-agent/thread-history";
import { type AiError, Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  catalogue,
  commonTools,
  Handlers,
  Output,
  type Task,
  tasks,
  ToolEvidence,
  tools,
} from "./fixture.ts";
import { type Selection, type Arm, BenchmarkError, instrument, Sample } from "./measurement.ts";

export const settings = {
  max_output_tokens: 2_048,
  reasoning: { effort: "low" },
  store: false,
  service_tier: "default",
  strictJsonSchema: true,
} as const;

const arms: ReadonlyArray<Arm> = ["all-50", "fixed-8-discovery", "jev-8-discovery"];

const runSample = Effect.fn("ToolSelectionBenchmark.sample")(function* (
  arm: Arm,
  task: Task,
  repetition: number,
) {
  const metered = yield* instrument(arm === "all-50" ? 50 : 9);
  const toolCalls: Array<(typeof Sample.Type.toolCalls)[number]> = [];
  const selections: Array<typeof Selection.Type> = [];
  const decision = yield* DecisionModel.DecisionModel;
  let evaluation: typeof Selection.Type.evaluation = null;

  const ranking = ToolSelector.fromDecisionModel({
    maxTools: 8,
    minimumRelevance: 0,
    state: (request) => Schema.decodeUnknownEffect(Schema.String)(request.input),
    onEvaluation: (result) =>
      Effect.sync(() => {
        evaluation = result;
      }),
  });

  const selector: ToolSelector.Hook<Schema.SchemaError | AiError.AiError> = {
    maxTools: 8,
    select: Effect.fn("ToolSelectionBenchmark.select")(function* (request) {
      // Initial shortlist only. Later discovery results retain their normal replacement semantics.
      if (request.turn !== 1) return undefined;
      const started = yield* Clock.currentTimeMillis;
      let selected: ReadonlyArray<string> = [];

      return yield* ranking
        .select({
          ...request,
          catalogue: request.catalogue.filter((candidate) => candidate.name !== "discover_tools"),
        })
        .pipe(
          Effect.provideService(DecisionModel.DecisionModel, decision),
          Effect.tap((ids) =>
            Effect.sync(() => {
              selected = ids?.slice(0, 8) ?? [];
            }),
          ),
          Effect.ensuring(
            Effect.gen(function* () {
              selections.push({
                elapsedMs: (yield* Clock.currentTimeMillis) - started,
                selected,
                evaluation,
              });
            }),
          ),
        );
    }),
  };

  const discovery = ToolDiscovery.make({ maxResults: 8 });
  const withDiscovery = arm !== "all-50";

  const definition = Agent.make("tool-selection-benchmark", {
    input: Schema.String,
    output: Output,
    instructions:
      "Use tools to retrieve fresh records before answering; never invent records. If a needed tool is missing, use discover_tools with a short, distinctive search term when available. Follow linked records when the task requires it. Preserve status strings and verification codes exactly in your answer.",
    toolkit: Toolkit.make(...tools, ...(withDiscovery ? [discovery.tool] : [])),
    toolExposure: {
      initialToolNames: withDiscovery ? [...commonTools] : Object.keys(catalogue),
      maxTools: withDiscovery ? 9 : 50,
      maxSchemaBytes: 65_536,
    },
    policy: {
      maxTurns: 6,
      maxToolCalls: 8,
      maxDuration: "2 minutes",
      toolConcurrency: 1,
      toolResultBounds: { maxBytes: 32_768 },
      onExhaustion: "fail",
    },
  });

  const observeTool = Effect.fnUntraced(function* (name: string, id: string) {
    toolCalls.push({ name, id, at: yield* Clock.currentTimeMillis });
  });

  const handlers = Layer.merge(Handlers, discovery.handlers);
  const agent = Agent.withModel(definition, OpenAiLanguageModel.model("gpt-6-astra", settings));
  const start = yield* Clock.currentTimeMillis;

  const exit = yield* AgentRuntime.run(
    agent,
    task.input,
    arm === "jev-8-discovery" ? { toolSelector: selector } : {},
  ).pipe(
    Effect.provide(Layer.merge(handlers, ThreadHistory.layer)),
    Effect.provideService(ToolEvidence, { record: observeTool }),
    Effect.provideService(OpenAiClient.OpenAiClient, metered.client),
    Effect.timeout("150 seconds"),
    Effect.exit,
  );

  const elapsedMs = (yield* Clock.currentTimeMillis) - start;
  const answer = Exit.isSuccess(exit) ? exit.value.output.answer : null;
  const called = new Set(toolCalls.map((call) => `${call.name}/${call.id}`));

  const success =
    answer !== null &&
    task.evidence.every((value) => answer.includes(value)) &&
    task.requiredCalls.every((value) => called.has(value));

  return Sample.make({
    arm,
    task: task.name,
    repetition,
    elapsedMs,
    success,
    answer,
    failure: Exit.isFailure(exit)
      ? Cause.pretty(exit.cause)
      : success
        ? null
        : "Missing required evidence or tool execution",
    modelCalls: metered.calls,
    toolCalls,
    selections,
  });
});

const Report = Schema.Struct({
  version: Schema.Literal(1),
  startedAt: Schema.Finite,
  sourceCommit: Schema.String,
  dirty: Schema.Boolean,
  runtime: Schema.String,
  platform: Schema.String,
  repetitions: Schema.Natural,
  live: Schema.Boolean,
  model: Schema.Literal("gpt-6-astra"),
  decisionModel: Schema.String,
  samples: Schema.Array(Sample),
});

export const benchmark = Effect.fn("ToolSelectionBenchmark.run")(function* (options: {
  readonly output: string;
  readonly repetitions: number;
  readonly live: boolean;
}) {
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  if (yield* fs.exists(options.output))
    return yield* BenchmarkError.make({
      message: "Output already exists; choose a new path to preserve prior attempts",
    });
  if (Object.keys(catalogue).length !== 50)
    return yield* BenchmarkError.make({ message: "Fixture must contain 50 tools" });

  const sourceCommit = (yield* spawner.string(
    ChildProcess.make("git", ["rev-parse", "HEAD"]),
  )).trim();

  const dirty =
    (yield* spawner.string(ChildProcess.make("git", ["status", "--porcelain"]))).trim() !== "";

  const runtime = (yield* spawner.string(ChildProcess.make("bun", ["--version"]))).trim();
  const platform = (yield* spawner.string(ChildProcess.make("uname", ["-sm"]))).trim();
  const samples: Array<typeof Sample.Type> = [];
  const startedAt = yield* Clock.currentTimeMillis;

  const save = () =>
    fs.writeFileString(
      options.output,
      Schema.encodeSync(Schema.fromJsonString(Report))({
        version: 1,
        startedAt,
        sourceCommit,
        dirty,
        runtime,
        platform,
        repetitions: options.repetitions,
        live: options.live,
        model: "gpt-6-astra",
        decisionModel: "jev-latest",
        samples,
      }),
    );

  yield* save();
  if (!options.live) {
    yield* Console.log(
      `Dry run: ${tasks.length * arms.length * options.repetitions} samples; at most 6 OpenAI calls and 1 JEV call each. ${options.output}`,
    );

    return;
  }

  const providers = Layer.merge(
    OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }),
    TypeSafeDecisionModel.model("jev-latest").pipe(
      Layer.provide(TypeSafeClient.layerConfig({ apiKey: Config.Redacted("TYPESAFEAI_API_KEY") })),
    ),
  ).pipe(Layer.provide(FetchHttpClient.layer));

  yield* Effect.gen(function* () {
    // Latin-square rotation gives each arm every position for every task over three repetitions.
    for (let repetition = 0; repetition < options.repetitions; repetition++) {
      for (const [taskIndex, task] of tasks.entries()) {
        const offset = (repetition + taskIndex) % arms.length;

        for (const arm of [...arms.slice(offset), ...arms.slice(0, offset)]) {
          yield* Console.log(`Running ${task.name} / ${arm} / ${repetition + 1}`);
          const sample = yield* runSample(arm, task, repetition + 1);

          samples.push(sample);
          yield* save();
          yield* Console.log(
            `${sample.success ? "PASS" : "FAIL"} ${Math.round(sample.elapsedMs)}ms; ${sample.modelCalls.length} model calls`,
          );
        }
      }
    }
  }).pipe(Effect.provide(providers));

  if (samples.some((sample) => !sample.success))
    return yield* BenchmarkError.make({
      message: "One or more samples failed; all results retained",
    });
});
