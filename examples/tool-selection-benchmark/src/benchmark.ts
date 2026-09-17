import { DecisionModel } from "@effect-agent/ai-decision";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Cause, Clock, Config, Console, Effect, Exit, FileSystem, Layer, Schema } from "effect";
import { Agent, AgentRuntime, ToolDiscovery, ToolSelector } from "effect-agent";
import { ThreadHistory } from "effect-agent/thread-history";
import { type AiError, Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { runProbe } from "./cache-probe.ts";
import {
  catalogue,
  grade,
  commonTools,
  Handlers,
  Output,
  type Task,
  tasks,
  cacheTasks,
  referenceContext,
  ToolEvidence,
  tools,
} from "./fixture.ts";
import {
  type Selection,
  arms,
  cacheArms,
  informedArms,
  probeArms,
  Arm,
  ContextSize,
  Suite,
  BenchmarkError,
  instrument,
  Journal,
  refuse,
  Sample,
} from "./measurement.ts";
import { encodeTools } from "./stable-tools.ts";

export const settings = {
  max_output_tokens: 2_048,
  reasoning: { effort: "low" },
  store: false,
  service_tier: "default",
  strictJsonSchema: true,
} as const;

const runSample = Effect.fn("ToolSelectionBenchmark.sample")(function* (
  arm: Arm,
  task: Task,
  repetition: number,
  context: typeof ContextSize.Type,
) {
  const toolCalls: Array<(typeof Sample.Type.toolCalls)[number]> = [];
  const selections: Array<typeof Selection.Type> = [];
  const useInitialRanking = arm.includes("jev-8");
  const useSemanticDiscovery = arm === "all-50-discovery" || arm.endsWith("-jev");
  const withDiscovery = arm !== "all-50";
  const allTools = arm === "all-50" || arm === "all-50-discovery";
  const maxCalls = task.name === "chain-4" ? 12 : 6;

  const discovery = useSemanticDiscovery
    ? ToolDiscovery.fromDecisionModel({ maxResults: 8, minimumRelevance: 0.5 })
    : ToolDiscovery.make({ maxResults: 8 });

  const metered = yield* instrument(allTools ? (withDiscovery ? 51 : 50) : 9, {
    maxCalls,
    availabilityNotes: arm.startsWith("informed-"),
    ...(arm.startsWith("stable-") || arm.startsWith("informed-")
      ? { stableTools: yield* encodeTools([...tools, discovery.tool]) }
      : {}),
  });

  const ranking = yield* ToolSelector.fromDecisionModel({
    state: (request) => Schema.decodeUnknownEffect(Schema.String)(request.input),
  }).pipe(Effect.provideService(ToolSelector.DecisionConfig, { maxTools: 8, minimumRelevance: 0 }));

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
          catalogue: request.catalogue.filter(
            (candidate) =>
              candidate.name !== "discover_tools" && !task.withhold?.includes(candidate.name),
          ),
        })
        .pipe(
          Effect.provideService(DecisionModel.DecisionModel, metered.decision),
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
              });
            }),
          ),
        );
    }),
  };

  const definition = Agent.make("tool-selection-benchmark", {
    input: Schema.String,
    output: Output,
    instructions:
      "Use tools to retrieve fresh records before answering; never invent records. If a needed tool is missing, use discover_tools when available, following its description. Follow linked records when the task requires it. Return only the requested field values as strings in values, with no labels or commentary. Preserve codes, dates and status strings exactly. Do not include unrelated fields or record IDs." +
      (context === "reference" ? `\n${referenceContext}` : ""),
    toolkit: Toolkit.make(...tools, ...(withDiscovery ? [discovery.tool] : [])),
    toolExposure: {
      initialToolNames: allTools ? Object.keys(catalogue) : [...commonTools],
      maxTools: allTools ? (withDiscovery ? 51 : 50) : 9,
      maxSchemaBytes: 65_536,
    },
    policy: {
      maxTurns: maxCalls,
      maxToolCalls: task.name === "chain-4" ? 16 : 8,
      maxDuration: "2 minutes",
      toolConcurrency: 1,
      toolResultBounds: { maxBytes: 32_768 },
      onExhaustion: "fail",
    },
  });

  const observeTool = Effect.fnUntraced(function* (name: string, id: string) {
    toolCalls.push({ name, id, at: yield* Clock.currentTimeMillis });
  });

  const handlers = Layer.merge(Handlers, discovery.handlers).pipe(
    Layer.provide(Layer.succeed(DecisionModel.DecisionModel, metered.decision)),
  );

  const agent = Agent.withModel(definition, OpenAiLanguageModel.model("gpt-6-astra", settings));
  const start = yield* Clock.currentTimeMillis;

  const exit = yield* AgentRuntime.run(
    agent,
    task.input,
    useInitialRanking ? { toolSelector: selector } : {},
  ).pipe(
    Effect.provide(Layer.merge(handlers, ThreadHistory.layer)),
    Effect.provideService(ToolEvidence, { record: observeTool }),
    Effect.provideService(OpenAiClient.OpenAiClient, metered.client),
    Effect.timeout("150 seconds"),
    Effect.exit,
  );

  const elapsedMs = (yield* Clock.currentTimeMillis) - start;
  const answer = Exit.isSuccess(exit) ? exit.value.output.values : null;
  const success = answer !== null && grade(task, answer, toolCalls);

  return Sample.make({
    arm,
    task: task.name,
    repetition,
    context,
    forcedMiss: task.withhold !== undefined,
    elapsedMs,
    success,
    answer,
    failure: Exit.isFailure(exit)
      ? Cause.pretty(exit.cause)
      : success
        ? null
        : "Missing required evidence or tool execution",
    modelCalls: metered.calls,
    decisionCalls: metered.decisions,
    toolCalls,
    selections,
  });
});

const Report = Schema.Struct({
  version: Schema.Literal(4),
  startedAt: Schema.Finite,
  sourceCommit: Schema.String,
  dirty: Schema.Boolean,
  runtime: Schema.String,
  platform: Schema.String,
  repetitions: Schema.Natural,
  suite: Suite,
  context: ContextSize,
  arms: Schema.Array(Arm),
  tasks: Schema.Array(Schema.String),
  live: Schema.Boolean,
  model: Schema.Literal("gpt-6-astra"),
  decisionModel: Schema.String,
  samples: Schema.Array(Sample),
});

export const benchmark = Effect.fn("ToolSelectionBenchmark.run")(function* (options: {
  readonly output: string;
  readonly repetitions: number;
  readonly live: boolean;
  readonly suite: typeof Suite.Type;
  readonly context: typeof ContextSize.Type;
}) {
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const journalPath = `${options.output}.events.jsonl`;

  if ((yield* fs.exists(options.output)) || (yield* fs.exists(journalPath)))
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

  const selectedArms =
    options.suite === "probe"
      ? probeArms
      : options.suite === "informed"
        ? informedArms
        : options.suite === "cache"
          ? cacheArms
          : arms;

  const selectedTasks =
    options.suite === "discovery" || options.suite === "probe" ? tasks : cacheTasks;

  const samples: Array<typeof Sample.Type> = [];
  const startedAt = yield* Clock.currentTimeMillis;

  const save = () =>
    fs
      .writeFileString(
        `${options.output}.tmp`,
        Schema.encodeSync(Schema.fromJsonString(Report))({
          version: 4,
          startedAt,
          sourceCommit,
          dirty,
          runtime,
          platform,
          repetitions: options.repetitions,
          suite: options.suite,
          context: options.context,
          arms: selectedArms,
          tasks:
            options.suite === "probe" ? ["cache-probe"] : selectedTasks.map((task) => task.name),
          live: options.live,
          model: "gpt-6-astra",
          decisionModel: "jev-latest",
          samples,
        }),
      )
      .pipe(Effect.andThen(fs.rename(`${options.output}.tmp`, options.output)));

  yield* save();

  const count =
    selectedArms.length *
    options.repetitions *
    (options.suite === "probe" ? 1 : selectedTasks.length);

  if (!options.live) {
    yield* Console.log(
      `Dry run: ${count} ${options.suite}/${options.context} samples; at most ${options.suite === "probe" ? 4 : options.suite === "discovery" ? 6 : 12} OpenAI calls each. ${options.output}`,
    );

    return;
  }

  const providers = Layer.merge(
    OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }),
    TypeSafeDecisionModel.model("jev-latest").pipe(
      Layer.provide(TypeSafeClient.layer),
      Layer.provide(
        Layer.effect(
          TypeSafeClient.Config,
          Config.all({ apiKey: Config.Redacted("TYPESAFEAI_API_KEY") }),
        ),
      ),
    ),
  ).pipe(Layer.provide(FetchHttpClient.layer));

  let currentSample = "";

  const Event = Schema.Struct({
    sample: Schema.String,
    at: Schema.Finite,
    kind: Schema.String,
    payload: Schema.String,
  });

  const journal = Journal.of({
    record: Effect.fnUntraced(function* (kind, payload) {
      const line = Schema.encodeSync(Schema.fromJsonString(Event))({
        sample: currentSample,
        at: yield* Clock.currentTimeMillis,
        kind,
        payload,
      });

      yield* fs
        .writeFileString(journalPath, `${line}\n`, { flag: "a" })
        .pipe(Effect.mapError(() => refuse("Could not checkpoint benchmark evidence")));
    }),
  });

  yield* Effect.gen(function* () {
    // Rotate positions by task and repetition; retain every attempt, including failures.
    for (let repetition = 0; repetition < options.repetitions; repetition++) {
      for (const [taskIndex, task] of (options.suite === "probe"
        ? selectedTasks.slice(0, 1)
        : selectedTasks
      ).entries()) {
        const offset = (repetition + taskIndex) % selectedArms.length;

        for (const arm of [...selectedArms.slice(offset), ...selectedArms.slice(0, offset)]) {
          currentSample = `${options.context}/${options.suite === "probe" ? "cache-probe" : task.name}/${arm}/${repetition + 1}`;
          yield* journal.record(
            "sample-start",
            JSON.stringify({
              task: options.suite === "probe" ? null : task,
              arm,
              context: options.context,
              repetition: repetition + 1,
            }),
          );
          yield* Console.log(`Running ${currentSample}`);

          const sample = yield* options.suite === "probe"
            ? runProbe(arm, repetition + 1, options.context, `${startedAt}/${currentSample}`)
            : runSample(arm, task, repetition + 1, options.context);

          yield* journal.record(
            "sample-complete",
            Schema.encodeSync(Schema.fromJsonString(Sample))(sample),
          );
          samples.push(sample);
          yield* save();
          yield* Console.log(
            `${sample.success ? "PASS" : "FAIL"} ${Math.round(sample.elapsedMs)}ms; ${sample.modelCalls.length} model calls`,
          );
        }
      }
    }
  }).pipe(Effect.provide(Layer.merge(providers, Layer.succeed(Journal, journal))));

  if (samples.some((sample) => !sample.success))
    return yield* BenchmarkError.make({
      message: "One or more samples failed; all results retained",
    });
});
