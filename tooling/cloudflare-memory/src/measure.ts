import { Clock, Config, Console, Effect, FileSystem, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

import { BenchmarkCase, cases, Sample } from "./contracts.ts";

class BenchmarkFailure extends Schema.TaggedError<BenchmarkFailure>()("BenchmarkFailure", {
  reason: Schema.Literals(["transport", "timeout"]),
}) {}

const Observation = Schema.Struct({
  httpMillis: Schema.Finite,
  sample: Schema.NullOr(Sample),
  transportFailure: Schema.NullOr(BenchmarkFailure),
});

const Placement = Schema.Struct({
  callerEgressColo: Schema.String,
  ownerEgressColo: Schema.String,
  ingressColo: Schema.String,
  placementHint: Schema.String,
});

const Distribution = Schema.Struct({
  p50: Schema.NullOr(Schema.Number),
  p95: Schema.NullOr(Schema.Number),
  p99: Schema.NullOr(Schema.Number),
});

const Summary = Schema.Struct({
  http: Distribution,
  validationRpc: Distribution,
  fullRecall: Distribution,
  errors: Schema.Natural,
  timeouts: Schema.Natural,
});

const Cohort = Schema.Struct({
  case: BenchmarkCase,
  state: Schema.Literals(["first-after-inactivity", "warm"]),
  concurrency: Schema.Natural,
  observations: Schema.Array(Observation),
  summary: Summary,
});

const Report = Schema.Struct({
  version: Schema.Literal(1),
  url: Schema.String,
  revision: Schema.String,
  startedAt: Schema.Number,
  samplesPerWarmCohort: Schema.Natural,
  inactivitySeconds: Schema.Natural,
  notes: Schema.Array(Schema.String),
  cohorts: Schema.Array(Cohort),
  placements: Schema.Array(
    Schema.Struct({ case: BenchmarkCase, caller: Schema.String, placement: Placement }),
  ),
});

const distribution = (values: ReadonlyArray<number>) => {
  const sorted = [...values].sort((a, b) => a - b);

  const at = (fraction: number) =>
    sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)] ?? null;

  return { p50: at(0.5), p95: at(0.95), p99: at(0.99) };
};

const summarize = (observations: ReadonlyArray<typeof Observation.Type>) => {
  const valid = observations.flatMap((o) => (o.sample?.status === "ok" ? [o.sample] : []));

  return {
    http: distribution(observations.map((o) => o.httpMillis)),
    validationRpc: distribution(valid.map((s) => s.validationRpcMillis)),
    fullRecall: distribution(valid.map((s) => s.fullRecallMillis)),
    errors: observations.filter((o) => o.transportFailure !== null || o.sample?.status !== "ok")
      .length,
    timeouts: observations.filter(
      (o) => o.transportFailure?.reason === "timeout" || o.sample?.status === "timeout",
    ).length,
  };
};

export const command = Command.make(
  "cloudflare-memory-measure",
  {
    url: Flag.string("url").pipe(
      Flag.withDescription("URL of the temporary authenticated benchmark Worker."),
    ),
    revision: Flag.string("revision").pipe(
      Flag.withDefault("working-tree"),
      Flag.withDescription("Exact source revision descriptor included in the report."),
    ),
    samples: Flag.integer("samples").pipe(
      Flag.withDefault(200),
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 }))),
      Flag.withDescription("Warm samples per case and concurrency, at most 1000."),
    ),
    inactivity: Flag.integer("inactivity-seconds").pipe(
      Flag.withDefault(90),
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 600 }))),
      Flag.withDescription(
        "Idle interval before each first-request cohort; does not prove eviction.",
      ),
    ),
    output: Flag.string("output").pipe(
      Flag.withDefault("/tmp/kom19-cloudflare-memory.json"),
      Flag.withDescription("Path for the schema-encoded report, including every error sample."),
    ),
  },
  Effect.fn("benchmark.measure")(function* (options) {
    const token = yield* Config.redacted("BENCH_TOKEN");
    const http = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk);

    const request = Effect.fn("benchmark.request")(function* <A, I>(
      path: string,
      name: BenchmarkCase,
      caller: string,
      schema: Schema.Codec<A, I, never>,
    ) {
      const response = yield* http.execute(
        HttpClientRequest.get(
          `${options.url.replace(/\/$/, "")}/${path}?case=${name}&caller=${caller}`,
        ).pipe(HttpClientRequest.bearerToken(token)),
      );

      return yield* HttpClientResponse.schemaBodyJson(schema)(response);
    });

    const observe = Effect.fn("benchmark.observe")(function* (name: BenchmarkCase, caller: string) {
      const start = yield* Clock.currentTimeMillis;

      const result = yield* request("sample", name, caller, Sample).pipe(
        Effect.mapError(() => BenchmarkFailure.make({ reason: "transport" })),
        Effect.timeoutOrElse({
          duration: 15_000,
          orElse: () => Effect.fail(BenchmarkFailure.make({ reason: "timeout" })),
        }),
        Effect.result,
      );

      return {
        httpMillis: (yield* Clock.currentTimeMillis) - start,
        sample: result._tag === "Success" ? result.success : null,
        transportFailure: result._tag === "Failure" ? result.failure : null,
      };
    });

    const startedAt = yield* Clock.currentTimeMillis;
    const cohorts: Array<typeof Cohort.Type> = [];

    for (const name of cases) {
      yield* request("seed", name, "a", Schema.Boolean);
      yield* request("sample", name, "b", Sample);
    }
    yield* Console.error(
      `Seeded five isolated memory owners; waiting ${options.inactivity}s before first-after-inactivity samples.`,
    );
    yield* Effect.sleep(options.inactivity * 1000);
    for (const name of cases) {
      const observations = [yield* observe(name, "a")];

      cohorts.push({
        case: name,
        state: "first-after-inactivity",
        concurrency: 1,
        observations,
        summary: summarize(observations),
      });
    }
    for (const concurrency of [1, 4])
      for (const name of cases) {
        const observations = yield* Effect.forEach(
          Array.from({ length: options.samples }, (_, i) => i),
          (i) => observe(name, i % 2 === 0 ? "a" : "b"),
          { concurrency },
        );

        cohorts.push({
          case: name,
          state: "warm",
          concurrency,
          observations,
          summary: summarize(observations),
        });
        yield* Console.error(
          `Measured ${name}, concurrency ${concurrency}, ${observations.length} samples.`,
        );
      }
    const placements: Array<(typeof Report.Type)["placements"][number]> = [];

    for (const name of cases)
      for (const caller of ["a", "b"])
        placements.push({
          case: name,
          caller,
          placement: yield* request("placement", name, caller, Placement),
        });

    const report = {
      version: 1 as const,
      url: options.url,
      revision: options.revision,
      startedAt,
      samplesPerWarmCohort: options.samples,
      inactivitySeconds: options.inactivity,
      cohorts,
      placements,
      notes: [
        "Synthetic 1024-byte source texts; normal cases use 64-byte excerpts; duplicate-heavy uses 128 candidates over 16 sources.",
        "validationRpc measures Thread-to-Memory round trip plus validation, excluding candidate preparation and final rendering; fullRecall adds final rendering. HTTP includes caller activation and ingress.",
        "No embedding or search runs in this source-validation benchmark. Their latency is not measured or inferred.",
        "Workers clocks advance on I/O; local CPU/render intervals may report zero. No subtraction is claimed as measured CPU time.",
        "Placement uses egress colo probes after all samples, not an assertion of physical owner placement; no location hint is requested.",
        "One first-after-inactivity sample per case is retained raw, not evidence of cold-start p95/p99. Inactivity does not prove eviction.",
        "Latency distributions for validation and full recall include successful samples; all transport durations, failures, and timeouts remain in the report.",
      ],
    };

    const json = yield* Schema.encodeEffect(Schema.fromJsonString(Report))(report);
    const fs = yield* FileSystem.FileSystem;

    yield* fs.writeFileString(options.output, `${json}\n`);
    yield* Console.log(json);
  }),
).pipe(
  Command.withDescription(
    "Measure deployed Thread-to-Memory recall. Requires BENCH_TOKEN; never deploys or deletes resources.",
  ),
);

export const measurementHttpLayer = FetchHttpClient.layer;
