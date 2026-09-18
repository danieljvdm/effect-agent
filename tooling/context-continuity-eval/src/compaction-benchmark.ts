import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Console, DateTime, Effect, FileSystem, Layer, Option, Ref, Schema } from "effect";
import { CLEARED_TOOL_RESULT } from "effect-agent/compaction";
import { Command, Flag } from "effect/unstable/cli";
import { FetchHttpClient, HttpBody, HttpClientResponse } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  BenchmarkCase,
  benchmarkCases,
  labeledBenchmarkCases,
} from "./compaction-benchmark-cases.ts";
import { makeLiveClient, MAX_OUTPUT_TOKENS } from "./live-model.ts";
import type { RequestAudit } from "./request-audit.ts";
import { RequestAuditSink } from "./request-audit.ts";
import {
  Outcome,
  runCase,
  ScoreSample,
  scoreCase,
  SelectiveEvalError,
  thresholds,
} from "./selective-eval.ts";

const Scores = Schema.Struct({
  revision: Schema.String,
  node: Schema.String,
  dirty: Schema.Boolean,
  startedAt: Schema.String,
  cases: Schema.Array(BenchmarkCase),
  samples: Schema.Array(ScoreSample),
  threshold: Schema.NullOr(Schema.Finite),
});

const Counts = Schema.Struct({
  caseId: Schema.String,
  before: Schema.Natural,
  framingAndConversation: Schema.Natural,
  variants: Schema.Array(
    Schema.Struct({
      threshold: Schema.Finite,
      afterJev: Schema.Natural,
      dropped: Schema.Array(Schema.String),
      missingEvidence: Schema.Array(Schema.String),
    }),
  ),
});

const ResultRow = Schema.Struct({
  caseId: Schema.String,
  variant: Schema.String,
  model: Schema.String,
  outcome: Outcome,
  missingAnswers: Schema.Array(Schema.String),
  unexpectedAnswers: Schema.Array(Schema.String),
  passed: Schema.Boolean,
  inputTokens: Schema.Array(Schema.Natural),
  outputTokens: Schema.Array(Schema.Natural),
  costMicrousd: Schema.Natural,
});

export const droppedIds = (sample: ScoreSample, threshold: number): ReadonlyArray<string> =>
  sample.error !== null
    ? []
    : (sample.state?.results
        .filter((candidate) => {
          const answer = sample.response?.answers[candidate.id];

          return answer?.type === "probability" && answer.probability < threshold;
        })
        .map((candidate) => candidate.toolCallId) ?? []);

export const missingEvidence = (fixture: BenchmarkCase, dropped: ReadonlyArray<string>) =>
  fixture.scenario.required
    .filter((fact) => dropped.includes(fact.toolCallId))
    .map((fact) => fact.value);

const writeJson = Effect.fnUntraced(function* <S extends Schema.Top>(
  path: string,
  schema: S,
  value: S["Type"],
) {
  const fs = yield* FileSystem.FileSystem;
  const json = yield* Schema.encodeEffect(Schema.fromJsonString(schema))(value);

  yield* fs.writeFileString(`${path}.next`, json);
  yield* fs.rename(`${path}.next`, path);
});

const decisionLayer = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layer),
  Layer.provide(TypeSafeClient.Config.layer),
  Layer.provide(FetchHttpClient.layer),
);

const openAiLayer = OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(
  Layer.provide(FetchHttpClient.layer),
);

const TokenCount = Schema.Struct({
  object: Schema.Literal("response.input_tokens"),
  input_tokens: Schema.Natural,
});

const CountPayload = Schema.Struct({
  model: Schema.String,
  input: Schema.Array(Schema.Record(Schema.String, Schema.Json)),
  instructions: Schema.optionalKey(Schema.NullOr(Schema.String)),
  tools: Schema.optionalKey(Schema.Array(Schema.Json)),
  tool_choice: Schema.optionalKey(Schema.Json),
  reasoning: Schema.optionalKey(Schema.Json),
  text: Schema.optionalKey(Schema.Json),
});

const count = Effect.fn("CompactionBenchmark.count")(function* (payload: typeof CountPayload.Type) {
  const client = yield* OpenAiClient.OpenAiClient;

  return yield* client.client
    .post("/responses/input_tokens", {
      body: HttpBody.jsonUnsafe({
        model: payload.model,
        input: payload.input,
        instructions: payload.instructions,
        tools: payload.tools,
        tool_choice: payload.tool_choice,
        reasoning: payload.reasoning,
        text: payload.text,
        truncation: "disabled",
      }),
    })
    .pipe(
      Effect.flatMap(HttpClientResponse.schemaBodyJson(TokenCount)),
      Effect.map((value) => value.input_tokens),
      Effect.timeout("2 minutes"),
    );
});

/** Count a counterfactual request with precisely the bodies selected by the native selector.
 * Actual engine continuation/fallback is measured separately in ResultRow. */
const countProjection = Effect.fn("CompactionBenchmark.countProjection")(function* (
  fixture: BenchmarkCase,
  sample: ScoreSample,
  payload: typeof CountPayload.Type,
  selected: number,
) {
  const input = payload.input;

  if (!Array.isArray(input))
    return yield* new SelectiveEvalError({ message: "Expected native message array" });

  const blank = input.map((item) =>
    item.type === "function_call_output" ? { ...item, output: "" } : item,
  );

  const before = yield* count(payload);
  const framingAndConversation = yield* count({ ...payload, input: blank });
  const variants: Array<(typeof Counts.Type.variants)[number]> = [];

  for (const threshold of [...new Set([0.1, selected])]) {
    const dropped = droppedIds(sample, threshold);

    const projected = input.map((item) =>
      item.type === "function_call_output" &&
      typeof item.call_id === "string" &&
      dropped.includes(item.call_id)
        ? { ...item, output: JSON.stringify(CLEARED_TOOL_RESULT) }
        : item,
    );

    variants.push({
      threshold,
      afterJev: yield* count({ ...payload, input: projected }),
      dropped,
      missingEvidence: missingEvidence(fixture, dropped),
    });
  }

  return Counts.make({ caseId: fixture.scenario.id, before, framingAndConversation, variants });
});

export const command = Command.make(
  "compaction-benchmark",
  {
    corpus: Flag.Literals("corpus", ["stress", "labeled"]).pipe(Flag.withDefault("stress")),
    phase: Flag.Literals("phase", ["validate", "score", "compare"]).pipe(
      Flag.withDefault("validate"),
    ),
    output: Flag.String("output-dir").pipe(
      Flag.withDefault(".context-continuity-eval/compaction-benchmark"),
    ),
    scores: Flag.String("scores").pipe(Flag.optional),
    model: Flag.Literals("model", ["gpt-5.6-luna", "gpt-5.6-sol"]).pipe(
      Flag.withDefault("gpt-5.6-luna"),
    ),
    subset: Flag.Literals("subset", ["all", "large", "small"]).pipe(Flag.withDefault("all")),
  },
  Effect.fn("CompactionBenchmark.command")(function* (options) {
    const corpus = options.corpus === "labeled" ? labeledBenchmarkCases : benchmarkCases;

    const selectorRequests = corpus.reduce(
      (n, c) => n + (c.scenario.split === "calibration" ? 1 : 3),
      0,
    );

    yield* Schema.decodeEffect(Schema.Array(BenchmarkCase))(corpus);
    for (const fixture of corpus) {
      for (const entry of fixture.scenario.history) {
        if (
          entry.kind === "tool" &&
          new TextEncoder().encode(JSON.stringify(entry.result)).length > fixture.maxResultBytes
        )
          return yield* new SelectiveEvalError({
            message: `Fixture exceeds configured result bound: ${fixture.scenario.id}`,
          });
      }
    }
    if (options.phase === "validate") {
      yield* Console.log(
        JSON.stringify({
          cases: corpus.length,
          selectorRequests,
          thresholds,
          maxOpenAiUsd: { "gpt-5.6-luna": 60, "gpt-5.6-sol": 120 },
          jevAllowanceUsd: 1,
          maxInputTokens: 922000,
          corpus: corpus.map((c) => ({
            id: c.scenario.id,
            bytes: c.bytes,
            results: c.scenario.history.filter((e) => e.kind === "tool").length,
          })),
        }),
      );

      return;
    }
    const fs = yield* FileSystem.FileSystem;

    if (yield* fs.exists(options.output))
      return yield* new SelectiveEvalError({
        message: "Use a new output directory; attempts are never overwritten",
      });
    yield* fs.makeDirectory(options.output, { recursive: true });
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const revision = (yield* spawner.string(
      ChildProcess.make("git", ["rev-parse", "HEAD"]),
    )).trim();

    const node = (yield* spawner.string(ChildProcess.make("node", ["--version"]))).trim();

    const dirty =
      (yield* spawner.string(ChildProcess.make("git", ["status", "--porcelain"]))).trim() !== "";

    // Preserve the exact source diff alongside the corpus, since live experiments may precede commit.
    yield* fs.writeFileString(
      `${options.output}/source.patch`,
      yield* spawner.string(ChildProcess.make("git", ["diff", "HEAD"])),
    );
    yield* fs.makeDirectory(`${options.output}/source`, { recursive: true });
    for (const name of [
      "compaction-benchmark.ts",
      "compaction-benchmark-cases.ts",
      "compaction-benchmark-main.ts",
      "live-model.ts",
      "selective-eval.ts",
    ]) {
      yield* fs.copyFile(`src/${name}`, `${options.output}/source/${name}`);
    }
    if (options.phase === "score") {
      const samples: Array<ScoreSample> = [];
      let threshold: number | null = null;
      const startedAt = DateTime.formatIso(yield* DateTime.now);

      const save = () =>
        writeJson(`${options.output}/scores.json`, Scores, {
          revision,
          node,
          dirty,
          startedAt,
          cases: corpus,
          samples,
          threshold,
        });

      yield* save();
      for (const split of ["calibration", "holdout"] as const) {
        for (const fixture of corpus.filter((value) => value.scenario.split === split)) {
          for (let repeat = 0; repeat < (split === "calibration" ? 1 : 3); repeat++) {
            const sample = yield* scoreCase(fixture.scenario, repeat).pipe(
              Effect.provide(decisionLayer),
            );

            samples.push(sample);
            yield* save();
            yield* Console.error(
              JSON.stringify({
                scored: sample.caseId,
                repeat,
                ms: sample.elapsedMs,
                error: sample.error,
                candidates: sample.state?.results.length,
              }),
            );
          }
        }
        if (split === "calibration") {
          // Freeze the highest threshold with zero lost required records on calibration only.
          threshold =
            [...thresholds].reverse().find((candidate) =>
              corpus
                .filter((c) => c.scenario.split === "calibration")
                .every((fixture) => {
                  const sample = samples.find((s) => s.caseId === fixture.scenario.id);

                  return (
                    sample !== undefined &&
                    sample.error === null &&
                    missingEvidence(fixture, droppedIds(sample, candidate)).length === 0
                  );
                }),
            ) ?? 0;
          yield* save();
          yield* Console.error(JSON.stringify({ frozenThreshold: threshold }));
        }
      }

      return;
    }
    if (Option.isNone(options.scores))
      return yield* new SelectiveEvalError({ message: "Comparison requires --scores" });

    const scores = yield* fs
      .readFileString(options.scores.value)
      .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(Scores))));

    if (
      scores.threshold === null ||
      scores.samples.length !== selectorRequests ||
      !Schema.toEquivalence(Schema.Array(BenchmarkCase))(scores.cases, corpus)
    )
      return yield* new SelectiveEvalError({
        message: "Require complete frozen scores and unchanged corpus",
      });
    const selected = scores.threshold;
    const outcomes: Array<typeof ResultRow.Type> = [];
    const counts: Array<typeof Counts.Type> = [];
    const events: Array<RequestAudit> = [];

    const compare = Effect.gen(function* () {
      const phase = yield* Ref.make(0);

      const client = yield* makeLiveClient({
        model: options.model,
        maxCostMicrousd:
          (options.corpus === "labeled" ? 10 : options.model === "gpt-5.6-luna" ? 60 : 120) *
          1_000_000,
        profile: "large-compaction",
        phase,
        maxInputTokens: 922_000,
        maxModelCalls: 300,
      });

      const layer = OpenAiLanguageModel.layer({
        model: options.model,
        config: {
          reasoning: { effort: "low" },
          store: false,
          service_tier: "default",
          max_output_tokens: MAX_OUTPUT_TOKENS,
        },
      }).pipe(Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, client.client)));

      const fixtures = corpus.filter(
        (c) =>
          c.scenario.split === "holdout" &&
          (options.subset === "all" || options.subset === c.scale),
      );

      for (const fixture of fixtures) {
        const sample = scores.samples.find(
          (s) => s.caseId === fixture.scenario.id && s.repeat === 0,
        );

        if (sample === undefined)
          return yield* new SelectiveEvalError({ message: "Missing frozen sample" });

        // Fixed paired order: original first supplies exact pre/post accounting. Cached tokens are billed separately; this is not a cold-cache latency comparison.
        const variants = [
          { name: "original", strategy: "uncompacted", threshold: 0.1 },
          { name: "age", strategy: "age", threshold: 0.1 },
          { name: "summary", strategy: "summary", threshold: 0.1 },
          { name: "jev-default", strategy: "selective-summary", threshold: 0.1 },
          { name: "jev-calibrated", strategy: "selective-summary", threshold: selected },
        ] as const;

        for (const variant of variants) {
          const eventStart = events.length;
          const usageBefore = yield* client.snapshot;

          yield* Ref.set(phase, outcomes.length);

          const outcome = yield* runCase(
            fixture.scenario,
            variant.strategy,
            sample,
            variant.threshold,
            undefined,
            { model: options.model, maxResultBytes: fixture.maxResultBytes },
          ).pipe(Effect.provide(layer));

          const usage = yield* client.snapshot;
          const runEvents = events.slice(eventStart);
          const requests = runEvents.filter((e) => e.kind === "request");
          const responses = runEvents.filter((e) => e.kind === "response");
          const missingAnswers = fixture.expected.filter((v) => !outcome.facts.includes(v));
          const unexpectedAnswers = outcome.facts.filter((v) => !fixture.expected.includes(v));

          const row = ResultRow.make({
            caseId: fixture.scenario.id,
            variant: variant.name,
            model: options.model,
            outcome,
            missingAnswers,
            unexpectedAnswers,
            passed:
              outcome.error === null &&
              missingAnswers.length === 0 &&
              unexpectedAnswers.length === 0 &&
              outcome.historyIntact,
            inputTokens: requests.map((e) => e.inputTokens),
            outputTokens: responses.map((e) => e.outputTokens),
            costMicrousd: usage.estimatedCostMicrousd - usageBefore.estimatedCostMicrousd,
          });

          outcomes.push(row);
          yield* writeJson(`${options.output}/outcomes.json`, Schema.Array(ResultRow), outcomes);
          yield* fs.writeFileString(`${options.output}/usage.json`, JSON.stringify(usage));
          yield* Console.error(
            JSON.stringify({
              case: row.caseId,
              variant: row.variant,
              passed: row.passed,
              error: outcome.error,
              input: row.inputTokens,
              ms: outcome.elapsedMs,
              costUsd: row.costMicrousd / 1_000_000,
            }),
          );
          const failure = yield* client.failure;

          if (failure !== null) return yield* new SelectiveEvalError({ message: failure });
          if (variant.name === "original" && requests[0] !== undefined) {
            const payload = yield* Schema.decodeEffect(Schema.fromJsonString(CountPayload))(
              requests[0].json,
            );

            counts.push(yield* countProjection(fixture, sample, payload, selected));
            yield* writeJson(`${options.output}/counts.json`, Schema.Array(Counts), counts);
          }
          events.length = 0;
        }
      }
    });

    yield* compare.pipe(
      Effect.provide(
        Layer.merge(
          openAiLayer,
          Layer.effect(
            RequestAuditSink,
            Effect.gen(function* () {
              const sink = yield* RequestAuditSink;

              return RequestAuditSink.of({
                write: (event) =>
                  sink.write(event).pipe(
                    Effect.tap(() =>
                      Effect.sync(() => {
                        events.push(event);
                      }),
                    ),
                  ),
              });
            }),
          ).pipe(Layer.provide(RequestAuditSink.file(`${options.output}/requests.jsonl`))),
        ),
      ),
    );
  }),
);
