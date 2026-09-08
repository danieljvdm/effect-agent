import { createHash } from "node:crypto";
import { arch, cpus, platform, release, totalmem } from "node:os";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Clock, Console, Effect, Exit, FileSystem, Path, Schema, Stream } from "effect";
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
  REFERENCE,
  summary,
  WorkerOptions,
  WorkerReport,
} from "../examples/runtime-benchmark/src/contracts.ts";
import { PublishManifest, withPublishManifests } from "./release-publish.ts";

const Revision = Schema.Struct({
  role: Schema.Literals(["base", "head", "reference"]),
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
  fixtureSha256: Schema.String,
  transpiler: Schema.String,
  profile: Profile,
  referenceVersion: Schema.Literal(REFERENCE.version),
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
});

type PerformanceReport = typeof PerformanceReport.Type;

const sha256 = (content: string | Uint8Array) => createHash("sha256").update(content).digest("hex");

const subprocess = Effect.fn("benchmark.subprocess")(function* (
  executable: string,
  args: ReadonlyArray<string>,
  cwd: string,
  env: Record<string, string> = {},
) {
  const started = yield* Clock.monotonicTimeNanos;

  const child = yield* ChildProcess.make(executable, args, {
    cwd,
    env,
    extendEnv: true,
    stdout: "pipe",
    stderr: "pipe",
  });

  const [stdout, stderr, exitCode] = yield* Effect.all(
    [
      Stream.mkString(Stream.decodeText(child.stdout)),
      Stream.mkString(Stream.decodeText(child.stderr)),
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
const stageCheckout = Effect.fn("benchmark.stageCheckout")(function* (
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
  const lines = [
    "Timing is informational. Median [Q1–Q3] in milliseconds; every measured sample and outlier is retained.",
    "",
    "| Workload | Base total | Head total | Reference total | Head/base | Head/reference | Head model entry |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
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
    const reference = summary(samples("reference").map((sample) => sample.totalMs));

    const entry = summary(
      samples("head").flatMap((sample) =>
        sample.modelEntryMs === null ? [] : [sample.modelEntryMs],
      ),
    );

    const delta = (baseline: ReturnType<typeof summary>) =>
      baseline.count === 0 || head.count === 0 || baseline.median === 0
        ? "n/a"
        : `${((head.median / baseline.median - 1) * 100).toFixed(1)}%`;

    lines.push(
      `| ${workload.name} | ${format(base)} | ${format(head)} | ${format(reference)} | ${delta(base)} | ${delta(reference)} | ${format(entry)} |`,
    );
  }
  lines.push(
    "",
    "Inline checkpoint construction and save (outside recovery total):",
    "",
    "| Workload | Base | Head | Reference |",
    "| --- | ---: | ---: | ---: |",
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
      `| ${workload.name} | ${format(checkpoints("base"))} | ${format(checkpoints("head"))} | ${format(checkpoints("reference"))} |`,
    );
  }
  lines.push(
    "",
    "Cold subprocess totals include Node startup, imports, one small run, assertions, and process shutdown:",
  );
  for (const role of ["base", "head", "reference"] as const)
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
    `Invalid/incomplete batches: ${incomplete.length}; processes recorded: ${report.batches.length}/${report.settings.batches * 6}. Incomplete batches are excluded from comparison summaries.`,
    ...incomplete.map(
      (batch) =>
        `${batch.role}/${batch.cohort}/${batch.cold ? "cold" : "warm"}: ${(batch.failure ?? "Missing or invalid worker report").split("\n")[0]}`,
    ),
    `Fixture ${report.fixture} (${report.fixtureSha256}); reference ${report.referenceVersion}.`,
    `Node ${report.environment.node}; ${report.environment.platform}/${report.environment.architecture}; ${report.environment.cpu}.`,
    `Samples per workload/revision: ${report.settings.samplesPerBatch * report.settings.batches}; warmups: ${report.settings.warmupsPerBatch * report.settings.batches}.`,
  );
  for (const revision of report.revisions)
    lines.push(
      `${revision.role}: ${revision.revision}${revision.dirty ? " (dirty working tree)" : ""}; Effect ${revision.effect}; lock ${revision.lockfileSha256}`,
    );

  return lines.join("\n") + "\n";
};

export const compareRuntime = Effect.fn("benchmark.compareRuntime")(function* (options: {
  root: string;
  base: string;
  reference: string;
  output: string;
  profile: Profile;
  requireClean: boolean;
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
        entryPoints: ["contracts.ts", "fixture.ts", "worker.ts"].map((file) =>
          path.join(source, file),
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
  const reference = yield* stageCheckout(path.resolve(options.reference), "reference", fixtures);
  const stages = [base, head, reference];

  yield* check(
    reference.revision.revision === REFERENCE.revision && !reference.revision.dirty,
    `Reference must be clean immutable ${REFERENCE.revision} (${REFERENCE.version})`,
  );
  if (options.requireClean)
    yield* check(
      stages.every((stage) => !stage.revision.dirty),
      "--require-clean rejected modified checkouts",
    );
  const node = yield* subprocess("node", ["--version"], root);

  yield* check(
    node.exitCode === 0 && node.stdout.trim().startsWith("v24."),
    "runtime-v1 requires Node 24; changing the runtime requires a versioned reference reset",
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

  const report: PerformanceReport = {
    fixture: FIXTURE_VERSION,
    fixtureSha256: sha256(fixtureBytes.join("\n")),
    transpiler: `esbuild ${esbuildVersion} (fixture syntax only; no bundling)`,
    profile: options.profile,
    referenceVersion: REFERENCE.version,
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
  };

  const persist = Effect.gen(function* () {
    yield* fs.writeFileString(
      path.join(output, "report.json"),
      yield* Schema.encodeEffect(Schema.fromJsonString(PerformanceReport))(report),
    );
    yield* fs.writeFileString(path.join(output, "report.md"), renderPerformanceReport(report));
  });

  const measure = Effect.gen(function* () {
    yield* persist;
    for (let cohort = 0; cohort < sizes.batches; cohort++) {
      // Rotate who goes first on the same runner; no concurrent builds, tests, or revisions.
      const ordered = [...stages.slice(cohort % 3), ...stages.slice(0, cohort % 3)];

      for (const cold of [true, false])
        for (const stage of ordered) {
          const name = `${cohort}-${stage.revision.role}-${cold ? "cold" : "warm"}`;
          const outputFile = path.join(output, `${name}.json`);

          yield* Console.error(`Measuring ${name} (${options.profile})`);

          const workerOptions = {
            cold,
            profile: options.profile,
            warmups: cold ? 0 : sizes.warmupsPerBatch,
            samples: cold ? 1 : sizes.samplesPerBatch,
            output: outputFile,
          };

          const childStarted = yield* Clock.monotonicTimeNanos;

          const childExit = yield* subprocess(
            "node",
            [path.join(stage.stage, "fixture/worker.js")],
            stage.stage,
            {
              NODE_ENV: "production",
              RUNTIME_BENCHMARK_OPTIONS: Schema.encodeSync(Schema.fromJsonString(WorkerOptions))(
                workerOptions,
              ),
            },
          ).pipe(
            Effect.timeout(
              cold
                ? "30 seconds"
                : options.profile === "pr" || options.profile === "smoke"
                  ? "5 minutes"
                  : "90 minutes",
            ),
            Effect.exit,
          );

          const result = Exit.isSuccess(childExit)
            ? childExit.value
            : {
                stdout: "",
                stderr: Cause.pretty(childExit.cause),
                exitCode: -1,
                subprocessMs: Number((yield* Clock.monotonicTimeNanos) - childStarted) / 1e6,
              };

          yield* fs.writeFileString(
            path.join(output, `${name}.log`),
            result.stdout + result.stderr,
          );

          const decodedReport = (yield* fs.exists(outputFile))
            ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WorkerReport))(
                yield* fs.readFileString(outputFile),
              ).pipe(Effect.exit)
            : null;

          const childReport =
            decodedReport !== null && Exit.isSuccess(decodedReport) ? decodedReport.value : null;

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
          yield* persist;
        }
    }
  }).pipe(Effect.onExit(() => persist));

  yield* withPublishManifests(base.stage, () =>
    withPublishManifests(head.stage, () => withPublishManifests(reference.stage, () => measure)),
  );
  yield* Console.log(renderPerformanceReport(report));
  yield* check(
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
    reference: Flag.string("reference-dir").pipe(
      Flag.withDescription(`Clean built immutable reference ${REFERENCE.revision}.`),
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
  Effect.fn(function* ({ base, reference, output, profile, requireClean }) {
    const path = yield* Path.Path;

    const root = path.resolve(
      path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
      "..",
    );

    yield* compareRuntime({ root, base, reference, output, profile, requireClean });
  }),
).pipe(
  Command.withDescription(
    "Compare public built packages on one runner against PR base and a retained reference; no provider calls.",
  ),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: FIXTURE_VERSION }).pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );
