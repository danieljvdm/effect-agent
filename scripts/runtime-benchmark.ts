import { createHash } from "node:crypto";
import { arch, cpus, platform, release, totalmem } from "node:os";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import {
  Cause,
  Clock,
  Console,
  Effect,
  Exit,
  FileSystem,
  Option,
  Path,
  Schema,
  Stream,
} from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";
import { build, version as esbuildVersion } from "esbuild";

import {
  BenchmarkError,
  casesFor,
  check,
  completeBatch,
  FIXTURE_VERSION,
  Profile,
  summary,
  WorkerOptions,
  WorkerReport,
} from "../examples/runtime-benchmark/src/contracts.ts";
import { writeEvidence } from "../examples/runtime-benchmark/src/evidence.ts";
import { PublishManifest, withPublishManifests } from "./release-publish.ts";

const Revision = Schema.Struct({
  role: Schema.Literals(["base", "head"]),
  revision: Schema.String,
  dirty: Schema.Boolean,
  lockfileSha256: Schema.String,
  builtArtifactsSha256: Schema.String,
  effect: Schema.String,
});

type Revision = typeof Revision.Type;

const Batch = Schema.Struct({
  role: Revision.fields.role,
  cohort: Schema.Natural,
  cold: Schema.Boolean,
  subprocessMs: Schema.Finite,
  exitCode: Schema.Int,
  complete: Schema.Boolean,
  report: Schema.NullOr(WorkerReport),
  failure: Schema.NullOr(Schema.String),
});

type Batch = typeof Batch.Type;

export const PerformanceReport = Schema.Struct({
  fixture: Schema.Literal(FIXTURE_VERSION),
  baselineTag: Schema.NullOr(Schema.String),
  fixtureSha256: Schema.String,
  transpiler: Schema.String,
  profile: Profile,
  environment: Schema.Struct({
    platform: Schema.String,
    release: Schema.String,
    architecture: Schema.String,
    cpu: Schema.String,
    cpuCount: Schema.Natural,
    memoryBytes: Schema.Natural,
    node: Schema.String,
  }),
  settings: Schema.Struct({
    batches: Schema.Natural,
    warmupsPerBatch: Schema.Natural,
    samplesPerBatch: Schema.Natural,
    production: Schema.Literal(true),
    execution: Schema.Literal("unbundled published ESM"),
    timingGate: Schema.Literal("informational"),
  }),
  revisions: Schema.Array(Revision),
  batches: Schema.Array(Batch),
  activeBatch: Schema.NullOr(
    Schema.Struct({
      role: Revision.fields.role,
      cohort: Schema.Natural,
      cold: Schema.Boolean,
    }),
  ),
  failure: Schema.NullOr(Schema.String),
});

type PerformanceReport = typeof PerformanceReport.Type;

const sha256 = (content: string | Uint8Array) => createHash("sha256").update(content).digest("hex");

/** Shared raw-byte budget for stdout and stderr, including the retained log prefix. */
export const MAX_SUBPROCESS_OUTPUT_BYTES = 8 * 1024 * 1024;

export const subprocess = Effect.fn("benchmark.subprocess")(function* (
  executable: string,
  args: ReadonlyArray<string>,
  cwd: string,
  env: Record<string, string> = {},
  logFile?: string,
) {
  const started = yield* Clock.monotonicTimeNanos;
  const fs = yield* FileSystem.FileSystem;
  let outputBytes = 0;

  if (logFile !== undefined) yield* fs.writeFileString(logFile, "");

  const child = yield* ChildProcess.make(executable, args, {
    cwd,
    env,
    extendEnv: true,
    stdout: "pipe",
    stderr: "pipe",
    forceKillAfter: "5 seconds",
  });

  const retain = Effect.fn("benchmark.retainOutput")(function* (chunk: Uint8Array) {
    // Reserve synchronously across both readers. Finish accepted writes even if the
    // other reader overflows, so cancellation cannot erase an already retained prefix.
    const prefix = chunk.subarray(0, MAX_SUBPROCESS_OUTPUT_BYTES - outputBytes);

    outputBytes += prefix.byteLength;
    if (logFile !== undefined && prefix.byteLength > 0)
      yield* fs.writeFile(logFile, prefix, { flag: "a" });
    if (prefix.byteLength !== chunk.byteLength)
      return yield* BenchmarkError.make({
        message: `Child output exceeded ${MAX_SUBPROCESS_OUTPUT_BYTES} bytes across stdout and stderr; retained the bounded prefix`,
      });

    return prefix;
  }, Effect.uninterruptible);

  const [stdout, stderr, exitCode] = yield* Effect.all(
    [
      Stream.mkString(Stream.decodeText(child.stdout.pipe(Stream.mapEffect(retain)))),
      Stream.mkString(Stream.decodeText(child.stderr.pipe(Stream.mapEffect(retain)))),
      child.exitCode,
    ],
    { concurrency: 3 },
  );

  return {
    stdout,
    stderr,
    exitCode,
    subprocessMs: Number((yield* Clock.monotonicTimeNanos) - started) / 1e6,
  };
}, Effect.scoped);

/** Copy only public dist artifacts; external dependencies resolve from this revision's install. */
export const stageCheckout = Effect.fn("benchmark.stageCheckout")(function* (
  root: string,
  role: Revision["role"],
  fixtures: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const stage = yield* fs.makeTempDirectoryScoped({ prefix: `runtime-benchmark-${role}-` });
  const resolved = yield* fs.realPath(root);
  const git = yield* subprocess("git", ["rev-parse", "HEAD"], resolved);

  yield* check(
    git.exitCode === 0 && /^[a-f0-9]{40}$/.test(git.stdout.trim()),
    `Cannot resolve ${role} checkout SHA`,
  );

  const status = yield* subprocess(
    "git",
    ["status", "--porcelain", "--untracked-files=all"],
    resolved,
  );

  yield* check(status.exitCode === 0, `Cannot inspect ${role} checkout state`);

  const effect = yield* Schema.decodeUnknownEffect(
    Schema.fromJsonString(Schema.Struct({ version: Schema.String })),
  )(yield* fs.readFileString(path.join(resolved, "node_modules/effect/package.json")));

  yield* fs.copyFile(path.join(resolved, "package.json"), path.join(stage, "package.json"));
  yield* fs.copy(fixtures, path.join(stage, "fixture"));
  yield* fs.makeDirectory(path.join(stage, "node_modules"), { recursive: true });
  const builtFiles: Array<string> = [];

  // Never link the workspace scope: all framework imports must reach the staged dist graph.
  for (const entry of yield* fs.readDirectory(path.join(resolved, "node_modules"))) {
    if (entry.startsWith(".") || entry === "@effect-agent" || entry === "effect-agent") continue;
    yield* fs.symlink(
      path.join(resolved, "node_modules", entry),
      path.join(stage, "node_modules", entry),
    );
  }
  for (const directory of (yield* fs.readDirectory(path.join(resolved, "packages"))).sort()) {
    if (directory.startsWith(".")) continue;
    const source = path.join(resolved, "packages", directory);

    const manifest = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(PublishManifest))(
      yield* fs.readFileString(path.join(source, "package.json")),
    );

    if (manifest.private === true) continue;
    const destination = path.join(stage, "packages", directory);

    yield* fs.makeDirectory(destination, { recursive: true });
    yield* fs.copyFile(path.join(source, "package.json"), path.join(destination, "package.json"));
    yield* fs.copy(path.join(source, "dist"), path.join(destination, "dist"));
    for (const file of (yield* fs.readDirectory(path.join(destination, "dist"), {
      recursive: true,
    })).sort()) {
      if (file.endsWith(".mjs"))
        builtFiles.push(
          `${manifest.name}/${file}:${sha256(yield* fs.readFile(path.join(destination, "dist", file)))}`,
        );
    }
    const link = path.join(stage, "node_modules", manifest.name);

    yield* fs.makeDirectory(path.dirname(link), { recursive: true });
    yield* fs.symlink(destination, link);
  }

  return {
    stage,
    revision: {
      role,
      revision: git.stdout.trim(),
      dirty: status.stdout.trim() !== "",
      lockfileSha256: sha256(yield* fs.readFile(path.join(resolved, "bun.lock"))),
      builtArtifactsSha256: sha256(builtFiles.join("\n")),
      effect: effect.version,
    } satisfies Revision,
  };
});

export const renderPerformanceReport = (report: PerformanceReport): string => {
  const baseline = report.revisions.find((revision) => revision.role === "base");
  const candidate = report.revisions.find((revision) => revision.role === "head");

  const identical =
    baseline !== undefined &&
    candidate !== undefined &&
    baseline.builtArtifactsSha256 === candidate.builtArtifactsSha256 &&
    baseline.lockfileSha256 === candidate.lockfileSha256;

  const lines = [
    ...(report.baselineTag === null
      ? []
      : [
          `Base release: \`${report.baselineTag}\` (\`${baseline?.revision}\`). Head checkout: \`${candidate?.revision}\`.`,
          "",
        ]),
    "Timing is informational. Operation median [Q1–Q3] in milliseconds; every measured sample and outlier is retained. Model-entry timings remain in raw samples.",
    "Samples share three worker processes per revision in the pr profile; their spread is not a confidence interval or a calibrated regression threshold.",
    ...(identical
      ? [
          "Identical built JavaScript and lockfiles. Timing differences do not establish a code regression; percentage changes are suppressed.",
        ]
      : []),
    "",
    "| Workload | Base | Head | Head/base |",
    "| --- | ---: | ---: | ---: |",
  ];

  const format = (value: ReturnType<typeof summary>) =>
    value.count === 0
      ? "n/a"
      : `${value.median.toFixed(2)} [${value.q1.toFixed(2)}–${value.q3.toFixed(2)}]`;

  for (const workload of casesFor(report.profile)) {
    const samples = (role: Revision["role"]) =>
      report.batches
        .filter(
          (batch) => batch.role === role && !batch.cold && batch.complete && batch.exitCode === 0,
        )
        .flatMap((batch) => batch.report?.samples ?? [])
        .filter(
          (sample) => sample.case === workload.name && !sample.warmup && sample.status === "passed",
        );

    const base = summary(samples("base").map((sample) => sample.totalMs));
    const head = summary(samples("head").map((sample) => sample.totalMs));

    const delta = (baseline: ReturnType<typeof summary>) =>
      identical || baseline.count === 0 || head.count === 0 || baseline.median === 0
        ? "n/a"
        : `${((head.median / baseline.median - 1) * 100).toFixed(1)}%`;

    lines.push(`| ${workload.name} | ${format(base)} | ${format(head)} | ${delta(base)} |`);
  }
  lines.push(
    "",
    "Inline checkpoint construction and save (outside recovery total):",
    "",
    "| Workload | Base | Head |",
    "| --- | ---: | ---: |",
  );
  for (const workload of casesFor(report.profile).filter(
    (workload) => workload.kind === "recovery",
  )) {
    const checkpoints = (role: Revision["role"]) =>
      summary(
        report.batches
          .filter(
            (batch) => batch.role === role && !batch.cold && batch.complete && batch.exitCode === 0,
          )
          .flatMap((batch) => batch.report?.samples ?? [])
          .filter(
            (sample) =>
              sample.case === workload.name && !sample.warmup && sample.status === "passed",
          )
          .flatMap((sample) =>
            sample.checkpointCreationMs === null ? [] : [sample.checkpointCreationMs],
          ),
      );

    lines.push(
      `| ${workload.name} | ${format(checkpoints("base"))} | ${format(checkpoints("head"))} |`,
    );
  }
  lines.push(
    "",
    "Cold subprocess totals include Node startup, imports, one small run, assertions, and process shutdown:",
  );
  for (const role of ["base", "head"] as const)
    lines.push(
      `${role}: ${format(summary(report.batches.filter((batch) => batch.role === role && batch.cold && batch.complete && batch.exitCode === 0).map((batch) => batch.subprocessMs)))}`,
    );

  const failures = report.batches.flatMap(
    (batch) => batch.report?.samples.filter((sample) => sample.status === "failed") ?? [],
  );

  const failedProcesses = report.batches.filter((batch) => batch.exitCode !== 0).length;
  const incomplete = report.batches.filter((batch) => !batch.complete);

  lines.push(
    "",
    `Correctness failures: ${failures.length}; failed subprocesses: ${failedProcesses}.`,
    `Invalid/incomplete batches: ${incomplete.length}; processes recorded: ${report.batches.length}/${report.settings.batches * 4}. Incomplete batches are excluded from comparison summaries.`,
    ...(report.activeBatch === null
      ? []
      : [
          `Interrupted active batch: ${report.activeBatch.role}/${report.activeBatch.cohort}/${report.activeBatch.cold ? "cold" : "warm"}.`,
        ]),
    ...(report.failure === null ? [] : [`Comparison failure: ${report.failure.split("\n")[0]}`]),
    ...incomplete.map(
      (batch) =>
        `${batch.role}/${batch.cohort}/${batch.cold ? "cold" : "warm"}: ${(batch.failure ?? "Missing or invalid worker report").split("\n")[0]}`,
    ),
    `Fixture ${report.fixture} (${report.fixtureSha256}).`,
    `Node ${report.environment.node}; ${report.environment.platform}/${report.environment.architecture}; ${report.environment.cpu}.`,
    `Samples per workload/revision: ${report.settings.samplesPerBatch * report.settings.batches}; warmups: ${report.settings.warmupsPerBatch * report.settings.batches}.`,
  );
  for (const revision of report.revisions)
    lines.push(
      `${revision.role}: ${revision.revision}${revision.dirty ? " (dirty working tree)" : ""}; Effect ${revision.effect}; lock ${revision.lockfileSha256}`,
    );
  lines.push(
    "",
    "Worker wall time includes seed setup, assertions, cleanup, and report writes. Unallocated time includes startup, reporting, shutdown, controller overhead, and any unfinished attempt; these costs are not individually measured:",
    "",
    "| Batch | Process ms | Sample attempts ms | Sample setup ms | Unallocated ms | Last active sample |",
    "| --- | ---: | ---: | ---: | ---: | --- |",
  );
  for (const batch of report.batches) {
    const active = batch.report?.active;
    const attempts = batch.report?.samples.reduce((sum, sample) => sum + sample.attemptMs, 0) ?? 0;
    const setup = batch.report?.samples.reduce((sum, sample) => sum + sample.setupMs, 0) ?? 0;

    const lastActive =
      active === null || active === undefined
        ? "none"
        : `${active.case}:${active.ordinal} ${active.phase} at ${active.elapsedMs.toFixed(2)} ms`;

    lines.push(
      `| ${batch.role}/${batch.cohort}/${batch.cold ? "cold" : "warm"} | ${batch.subprocessMs.toFixed(2)} | ${attempts.toFixed(2)} | ${setup.toFixed(2)} | ${(batch.subprocessMs - attempts).toFixed(2)} | ${lastActive} |`,
    );
  }

  return lines.join("\n") + "\n";
};

export const compareRuntime = Effect.fn("benchmark.compareRuntime")(function* (options: {
  root: string;
  base: string;
  output: string;
  profile: Profile;
  requireClean: boolean;
  baselineTag: string | null;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = path.resolve(options.root);
  const output = path.resolve(options.output);

  yield* fs.makeDirectory(output, { recursive: true });
  yield* check(
    !(yield* fs.exists(path.join(output, "report.json"))),
    "Output already contains a report; choose a new --out-dir to preserve evidence",
  );
  const fixtures = yield* fs.makeTempDirectoryScoped({ prefix: "runtime-benchmark-fixture-" });
  const source = path.join(root, "examples/runtime-benchmark/src");

  yield* Effect.tryPromise({
    try: () =>
      build({
        entryPoints: ["contracts.ts", "fixture.ts", "worker.ts", "evidence.ts", "seeds.ts"].map(
          (file) => path.join(source, file),
        ),
        outdir: fixtures,
        bundle: false,
        platform: "node",
        format: "esm",
        target: "node24",
        sourcemap: false,
        minify: false,
        logLevel: "silent",
      }),
    catch: (cause) => BenchmarkError.make({ message: "Cannot transpile benchmark fixture", cause }),
  });

  const fixtureBytes = yield* Effect.forEach((yield* fs.readDirectory(fixtures)).sort(), (file) =>
    fs
      .readFileString(path.join(fixtures, file))
      .pipe(Effect.map((contents) => `${file}\n${contents}`)),
  );

  yield* fs.copy(fixtures, path.join(output, "fixture"));
  // All compilation and staging finishes before any measurements start.
  const base = yield* stageCheckout(path.resolve(options.base), "base", fixtures);
  const head = yield* stageCheckout(root, "head", fixtures);
  const stages = [base, head];

  if (options.requireClean)
    yield* check(
      stages.every((stage) => !stage.revision.dirty),
      "--require-clean rejected modified checkouts",
    );
  const node = yield* subprocess("node", ["--version"], root);

  yield* check(
    node.exitCode === 0 && node.stdout.trim().startsWith("v24."),
    `${FIXTURE_VERSION} requires Node 24; record runtime changes before comparing across runs`,
  );

  const sizes =
    options.profile === "smoke"
      ? { batches: 1, warmupsPerBatch: 0, samplesPerBatch: 1 }
      : options.profile === "pr"
        ? { batches: 3, warmupsPerBatch: 2, samplesPerBatch: 3 }
        : {
            batches: 3,
            warmupsPerBatch: 5,
            samplesPerBatch: options.profile === "archive" ? 3 : 10,
          };

  const batches: Array<Batch> = [];
  let activeBatch: PerformanceReport["activeBatch"] = null;
  let failure: string | null = null;

  const report: PerformanceReport = {
    fixture: FIXTURE_VERSION,
    baselineTag: options.baselineTag,
    fixtureSha256: sha256(fixtureBytes.join("\n")),
    transpiler: `esbuild ${esbuildVersion} (fixture syntax only; no bundling)`,
    profile: options.profile,
    environment: {
      platform: platform(),
      release: release(),
      architecture: arch(),
      cpu: cpus()[0]?.model ?? "unknown",
      cpuCount: cpus().length,
      memoryBytes: totalmem(),
      node: node.stdout.trim(),
    },
    settings: {
      ...sizes,
      production: true,
      execution: "unbundled published ESM",
      timingGate: "informational",
    },
    revisions: stages.map((stage) => stage.revision),
    batches,
    get activeBatch() {
      return activeBatch;
    },
    get failure() {
      return failure;
    },
  };

  const persist = Effect.gen(function* () {
    yield* writeEvidence(
      path.join(output, "report.json"),
      yield* Schema.encodeEffect(Schema.fromJsonString(PerformanceReport))(report),
    );
    yield* writeEvidence(path.join(output, "report.md"), renderPerformanceReport(report));
  });

  const measure = Effect.gen(function* () {
    yield* persist;
    for (let cohort = 0; cohort < sizes.batches; cohort++) {
      // Alternate base/head, head/base, base/head without reducing per-revision samples.
      // Three cohorts necessarily give one revision the first slot twice. Keep measurements sequential.
      const ordered = cohort % 2 === 0 ? stages : [...stages].reverse();

      for (const cold of [true, false])
        for (const stage of ordered) {
          const name = `${cohort}-${stage.revision.role}-${cold ? "cold" : "warm"}`;
          const outputFile = path.join(output, `${name}.json`);
          const logFile = path.join(output, `${name}.log`);

          activeBatch = { role: stage.revision.role, cohort, cold };
          yield* persist;

          yield* Console.error(`Measuring ${name} (${options.profile})`);

          const workerOptions = {
            cold,
            profile: options.profile,
            warmups: cold ? 0 : sizes.warmupsPerBatch,
            samples: cold ? 1 : sizes.samplesPerBatch,
            output: outputFile,
          };

          yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const childStarted = yield* Clock.monotonicTimeNanos;

              const childExit = yield* restore(
                subprocess(
                  "node",
                  [path.join(stage.stage, "fixture/worker.js")],
                  stage.stage,
                  {
                    NODE_ENV: "production",
                    RUNTIME_BENCHMARK_OPTIONS: Schema.encodeSync(
                      Schema.fromJsonString(WorkerOptions),
                    )(workerOptions),
                  },
                  logFile,
                ).pipe(
                  Effect.timeout(
                    cold
                      ? "30 seconds"
                      : options.profile === "pr" || options.profile === "smoke"
                        ? "5 minutes"
                        : "90 minutes",
                  ),
                ),
              ).pipe(Effect.exit);

              const result = Exit.isSuccess(childExit)
                ? childExit.value
                : {
                    stdout: "",
                    stderr: Cause.pretty(childExit.cause),
                    exitCode: -1,
                    subprocessMs: Number((yield* Clock.monotonicTimeNanos) - childStarted) / 1e6,
                  };

              const decodedReport = (yield* fs.exists(outputFile))
                ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WorkerReport))(
                    yield* fs.readFileString(outputFile),
                  ).pipe(Effect.exit)
                : null;

              const childReport =
                decodedReport !== null && Exit.isSuccess(decodedReport)
                  ? decodedReport.value
                  : null;

              const workMatches = childReport !== null && completeBatch(childReport, workerOptions);

              const environmentMatches =
                childReport !== null &&
                childReport.runtime === report.environment.node &&
                childReport.platform === report.environment.platform &&
                childReport.architecture === report.environment.architecture;

              batches.push({
                role: stage.revision.role,
                cohort,
                cold,
                subprocessMs: result.subprocessMs,
                exitCode: result.exitCode,
                complete: workMatches && environmentMatches,
                report: childReport,
                failure:
                  decodedReport !== null && Exit.isFailure(decodedReport)
                    ? Cause.pretty(decodedReport.cause)
                    : result.exitCode !== 0
                      ? result.stderr
                      : !workMatches
                        ? "Missing, duplicated, failed, or unfinalized workload samples"
                        : !environmentMatches
                          ? "Worker runtime/platform/architecture differs from the controller"
                          : null,
              });
              activeBatch = null;
              yield* persist;
              if (Exit.isFailure(childExit) && Cause.hasInterrupts(childExit.cause))
                return yield* Effect.failCause(childExit.cause);
            }),
          );
        }
    }
  }).pipe(
    Effect.timeout(
      options.profile === "pr" || options.profile === "smoke" ? "19 minutes" : "160 minutes",
    ),
    Effect.onExit((exit) => {
      if (Exit.isFailure(exit)) failure = Cause.pretty(exit.cause);

      return persist;
    }),
  );

  yield* withPublishManifests(base.stage, () => withPublishManifests(head.stage, () => measure));
  yield* Console.log(renderPerformanceReport(report));
  yield* check(
    batches.length === sizes.batches * 4 &&
      batches.every((batch) => batch.exitCode === 0 && batch.complete),
    "Benchmark correctness failed; timings are informational but incomplete work is rejected",
  );

  return report;
}, Effect.scoped);

export const command = Command.make(
  "runtime-benchmark",
  {
    base: Flag.string("base-dir").pipe(
      Flag.withDescription(
        "Exact base checkout, installed with its lockfile and production packages built.",
      ),
    ),
    baselineTag: Flag.string("base-tag").pipe(
      Flag.withSchema(
        Schema.String.check(Schema.isPattern(/^effect-agent@\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/)),
      ),
      Flag.withDescription(
        "Published effect-agent release tag naming the base checkout in CI reports.",
      ),
      Flag.optional,
    ),
    output: Flag.string("out-dir").pipe(
      Flag.withDefault(".performance-report"),
      Flag.withDescription("New artifact directory; existing reports are never overwritten."),
    ),
    profile: Flag.choice("profile", ["smoke", "pr", "extended", "archive"]).pipe(
      Flag.withDefault("pr"),
      Flag.withDescription(
        "Bounded PR cohort, larger local matrix, or manual 100k-record archive profile.",
      ),
    ),
    requireClean: Flag.boolean("require-clean").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Reject modified or untracked files in any checkout (required by CI)."),
    ),
  },
  Effect.fn(function* ({ base, baselineTag, output, profile, requireClean }) {
    const path = yield* Path.Path;

    const root = path.resolve(
      path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
      "..",
    );

    yield* compareRuntime({
      root,
      base,
      baselineTag: Option.getOrNull(baselineTag),
      output,
      profile,
      requireClean,
    });
  }),
).pipe(
  Command.withDescription(
    "Compare public built packages on one runner for Base versus Head; no provider calls.",
  ),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: FIXTURE_VERSION }).pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );
