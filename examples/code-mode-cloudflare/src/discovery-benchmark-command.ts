import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  Config,
  Console,
  Clock,
  Effect,
  Exit,
  FileSystem,
  Option,
  Path,
  Redacted,
  Schema,
} from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { build } from "esbuild";
import {
  convertV4MiniflareOptions,
  type LegacyWorkerOptions,
  Log,
  LogLevel,
  Miniflare,
} from "miniflare";

import {
  BenchmarkError,
  fixtureVersion,
  grade,
  modes,
  Prices,
  type Request,
  Result,
  Sample,
  seeds,
  settings,
  thresholds,
  workloads,
} from "./discovery-benchmark-contracts.ts";

const workerFile = fileURLToPath(new URL("./discovery-benchmark-worker.ts", import.meta.url).href);
const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");

const adapt = <A>(operation: string, evaluate: () => Promise<A>) =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => BenchmarkError.make({ message: operation, cause }),
  });

/** Bundle once outside timing. The report preserves these exact executable bytes. */
export const bundleWorker = Effect.fn("benchmark.bundle")(function* () {
  const result = yield* adapt("Could not bundle benchmark Worker", () =>
    build({
      entryPoints: [workerFile],
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      logLevel: "silent",
    }),
  );

  const output = result.outputFiles[0];

  if (output === undefined)
    return yield* BenchmarkError.make({ message: "Worker build produced no JavaScript" });

  return output.text;
});

/** The host owns Miniflare; interruption and failure both dispose its actual workerd process. */
export const openRuntime = Effect.fn("benchmark.openRuntime")(function* (
  script: string,
  apiKey: string,
  disposed: () => void = () => {},
  outboundService?: LegacyWorkerOptions["outboundService"],
) {
  const runtime = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        new Miniflare(
          convertV4MiniflareOptions({
            modules: true,
            script,
            modulesRoot: "/",
            compatibilityDate: "2025-05-01",
            compatibilityFlags: ["nodejs_compat", "experimental"],
            workerLoaders: { LOADER: {} },
            bindings: { OPENAI_API_KEY: apiKey },
            log: new Log(LogLevel.ERROR),
            ...(outboundService === undefined ? {} : { outboundService }),
          }),
        ),
      catch: (cause) => BenchmarkError.make({ message: "Could not start workerd", cause }),
    }),
    (runtime) =>
      Effect.promise(() => runtime.dispose()).pipe(Effect.tap(() => Effect.sync(disposed))),
  );

  yield* adapt("workerd did not become ready", () => runtime.ready);

  return runtime;
});

export const runSample = Effect.fn("benchmark.runSample")(function* (
  runtime: Miniflare,
  request: Request,
) {
  const started = yield* Clock.monotonicTimeNanos;

  const response = yield* adapt("Local Worker request failed", () =>
    runtime.dispatchFetch("http://benchmark/sample", {
      method: "POST",
      body: JSON.stringify(request),
      headers: { "content-type": "application/json" },
    }),
  );

  if (!response.ok)
    return yield* BenchmarkError.make({ message: `Local Worker returned HTTP ${response.status}` });
  const body = yield* adapt("Could not read Worker result", () => response.json());

  const result = yield* Schema.decodeUnknownEffect(Result)(body).pipe(
    Effect.mapError((cause) => BenchmarkError.make({ message: "Invalid Worker result", cause })),
  );

  return { result, taskMillis: Number((yield* Clock.monotonicTimeNanos) - started) / 1e6 };
});

const rate = (name: string) =>
  Flag.float(name).pipe(
    Flag.withSchema(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
    Flag.optional,
  );

export const command = Command.make(
  "tool-discovery-benchmark",
  {
    validate: Flag.boolean("validate").pipe(Flag.withDefault(false)),
    live: Flag.boolean("live").pipe(Flag.withDefault(false)),
    model: Flag.string("model").pipe(Flag.withSchema(Schema.NonEmptyString), Flag.optional),
    input: rate("input-usd-per-million"),
    cachedInput: rate("cached-input-usd-per-million"),
    output: rate("output-usd-per-million"),
    maxCost: Flag.float("max-cost-usd").pipe(
      Flag.withDefault(10),
      Flag.withSchema(Schema.Finite.check(Schema.isBetween({ minimum: 0.1, maximum: 10 }))),
    ),
    outputDirectory: Flag.directory("out-dir").pipe(Flag.withDefault(".benchmark-report")),
  },
  Effect.fn("benchmark.command")(function* (options) {
    if (options.validate === options.live)
      return yield* BenchmarkError.make({ message: "Choose exactly one of --validate or --live" });
    if (
      options.live &&
      (Option.isNone(options.model) ||
        Option.isNone(options.input) ||
        Option.isNone(options.cachedInput) ||
        Option.isNone(options.output))
    )
      return yield* BenchmarkError.make({
        message: "Live mode requires explicit --model and all three USD-per-million token prices",
      });
    const model = Option.getOrElse(options.model, () => "scripted");

    const prices = {
      input: Option.getOrElse(options.input, () => 0),
      cachedInput: Option.getOrElse(options.cachedInput, () => 0),
      output: Option.getOrElse(options.output, () => 0),
    };

    if (
      options.live &&
      (prices.input <= 0 || prices.output <= 0 || prices.cachedInput > prices.input)
    )
      return yield* BenchmarkError.make({
        message:
          "Input/output prices must be positive and cached input must not exceed input price",
      });
    const apiKey = options.live ? yield* Config.redacted("OPENAI_API_KEY") : Redacted.make("");
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = path.resolve(options.outputDirectory);

    if (yield* fs.exists(directory))
      return yield* BenchmarkError.make({
        message: "Output directory already exists; preserve it and select a new --out-dir",
      });
    yield* fs.makeDirectory(directory, { recursive: true });
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const sourceCommit = (yield* spawner.string(
      ChildProcess.make("git", ["rev-parse", "HEAD"]),
    )).trim();

    const dirty =
      (yield* spawner.string(ChildProcess.make("git", ["status", "--porcelain"]))).trim().length >
      0;

    const root = (yield* spawner.string(
      ChildProcess.make("git", ["rev-parse", "--show-toplevel"]),
    )).trim();

    const lockHash = sha256(yield* fs.readFileString(path.join(root, "bun.lock")));
    const script = yield* bundleWorker();

    yield* fs.writeFileString(path.join(directory, "worker.mjs"), script);
    const samples: Array<Sample> = [];
    const pilots: Array<Sample> = [];
    let spent = 0;
    let disposed = false;

    let active: {
      cohort: number;
      block: number;
      mode: Request["mode"];
      workload: Request["workload"];
    } | null = null;

    let failure: string | null = null;
    let unresolvedMicrousd = 0;

    const acceptance = () => {
      const comparison = grade(samples);
      const validAttempt = disposed && failure === null;

      return {
        ...comparison,
        validAttempt,
        nativePassed: validAttempt && comparison.nativePassed,
        codeModePassed: validAttempt && comparison.codeModePassed,
      };
    };

    const budget = Math.floor(options.maxCost * 1_000_000);

    const Report = Schema.Struct({
      fixture: Schema.String,
      sourceCommit: Schema.String,
      dirty: Schema.Boolean,
      lockHash: Schema.String,
      bundleHash: Schema.String,
      live: Schema.Boolean,
      model: Schema.String,
      prices: Prices,
      maxCostMicrousd: Schema.Natural,
      spentMicrousd: Schema.Natural,
      settings: Schema.Json,
      thresholds: Schema.Json,
      cacheCondition: Schema.String,
      timing: Schema.String,
      runtime: Schema.String,
      platform: Schema.String,
      architecture: Schema.String,
      active: Schema.NullOr(Schema.Json),
      failure: Schema.NullOr(Schema.String),
      disposed: Schema.Boolean,
      unresolvedMicrousd: Schema.Natural,
      pilots: Schema.Array(Sample),
      samples: Schema.Array(Sample),
      acceptance: Schema.Json,
    });

    const save = Effect.fn("benchmark.save")(function* () {
      const report = {
        fixture: fixtureVersion,
        sourceCommit,
        dirty,
        lockHash,
        bundleHash: sha256(script),
        live: options.live,
        model,
        prices,
        maxCostMicrousd: budget,
        spentMicrousd: spent,
        settings,
        thresholds,
        cacheCondition:
          "Fresh independent threads; reused local workerd host. Provider prefix caching is uncontrolled; inspect actual cached token counts and rotated order.",
        timing:
          "taskMillis uses the runner monotonic clock through decoded result; first-useful times use the Worker clock. No cross-clock subtraction. Local source bundle, not hosted latency or built-package startup.",
        runtime: process.version,
        platform: process.platform,
        architecture: process.arch,
        active,
        failure,
        disposed,
        unresolvedMicrousd,
        pilots,
        samples,
        acceptance: options.live
          ? acceptance()
          : {
              evaluated: false,
              reason: "Scripted validation never establishes latency improvement",
            },
      };

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Report))(report);
      const temporary = path.join(directory, "report.tmp");

      yield* fs.writeFileString(temporary, encoded);
      yield* fs.rename(temporary, path.join(directory, "report.json"));
    });

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        if (active !== null || !disposed) {
          failure ??=
            "Interrupted or incomplete benchmark; active provider usage may be unresolved";
          if (options.live && active !== null) unresolvedMicrousd = Math.max(0, budget - spent);
          yield* save();
        }
      }).pipe(Effect.orDie),
    );
    yield* save();

    const evaluate = Effect.gen(function* () {
      const runtime = yield* openRuntime(script, Redacted.value(apiKey), () => {
        disposed = true;
      });

      const cohorts = options.live ? [-1, 0, 1] : [-1];

      for (const cohort of cohorts) {
        const blocks = cohort === -1 ? [0] : [0, 1, 2, 3, 4];

        for (const block of blocks) {
          const order = [
            ...modes.slice((block + cohort + 3) % 3),
            ...modes.slice(0, (block + cohort + 3) % 3),
          ];

          for (const workload of block % 2 === 0 ? workloads : [...workloads].reverse())
            for (const mode of order) {
              active = { cohort, block, workload, mode };
              yield* save();
              const seed = cohort === -1 ? 97 : (seeds[block] ?? 17);

              const result = yield* runSample(runtime, {
                mode,
                workload,
                seed,
                live: options.live,
                model,
                prices,
                remainingMicrousd: Math.max(0, budget - spent),
              }).pipe(
                Effect.timeout("150 seconds"),
                Effect.mapError((cause) =>
                  BenchmarkError.make({
                    message: "Sample failed or timed out; active sample retained",
                    cause,
                  }),
                ),
              );

              const sample = { ...active, seed, ...result };

              (cohort === -1 ? pilots : samples).push(sample);
              spent += result.result.costMicrousd + result.result.pendingMicrousd;
              active = null;
              yield* save();
              if (!result.result.passed)
                return yield* BenchmarkError.make({
                  message: result.result.failure ?? "Sample failed",
                });
            }
        }
      }
    }).pipe(Effect.scoped, Effect.timeout("25 minutes"), Effect.exit);

    const result = yield* evaluate;

    if (Exit.isFailure(result))
      failure =
        "Benchmark did not complete; inspect the retained active sample, failures and reservations";
    if (options.live && active !== null) unresolvedMicrousd = Math.max(0, budget - spent);
    if (!disposed) failure = "Local runtime cleanup did not complete";
    if (options.live && !grade(samples).complete) failure ??= "Missing successful sample cells";

    const returnedModels = new Set(
      [...pilots, ...samples].flatMap((sample) =>
        sample.result.audits.flatMap((audit) =>
          audit.returnedModel === null ? [] : [audit.returnedModel],
        ),
      ),
    );

    if (
      options.live &&
      (returnedModels.size !== 1 ||
        [...pilots, ...samples].some(
          (sample) =>
            sample.result.audits.length !== sample.result.modelCalls ||
            sample.result.audits.some(
              (audit) =>
                !audit.completed ||
                !audit.returnedModel ||
                (audit.returnedTier !== null && audit.returnedTier !== "default"),
            ),
        ))
    )
      failure ??=
        "Provider identity, tier or completion audit is missing or inconsistent across matched samples";
    yield* save();
    yield* Console.log(
      JSON.stringify({
        report: path.join(directory, "report.json"),
        live: options.live,
        recordedSamples: samples.length + pilots.length,
        successfulSamples: [...samples, ...pilots].filter((sample) => sample.result.passed).length,
        acceptance: options.live ? acceptance() : "offline validation only",
        disposed,
        failure,
      }),
    );
    if (failure !== null) return yield* BenchmarkError.make({ message: failure });
    if (options.live && !grade(samples).nativePassed)
      return yield* BenchmarkError.make({
        message: "Native progressive did not meet the predeclared replicated latency thresholds",
      });
  }),
).pipe(
  Command.withDescription(
    "Compare eager, native discovery and hybrid Code Mode across 120 safe tools in local workerd. --validate makes no network model calls; --live requires explicit model/prices and OPENAI_API_KEY. No latency claim from scripted validation.",
  ),
);
