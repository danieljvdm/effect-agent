import { createHash } from "node:crypto";
import { arch, cpus, platform, release, totalmem } from "node:os";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Clock, Console, Effect, Exit, FileSystem, Path, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { build, version as esbuildVersion } from "esbuild";

import { BenchmarkError, check, summary } from "../examples/runtime-benchmark/src/contracts.ts";
import { diagnosticCases } from "../examples/runtime-benchmark/src/diagnostic-cases.ts";
import {
  completeDiagnosticBatch,
  DIAGNOSTIC_SIZES,
  DIAGNOSTIC_VERSION,
  DiagnosticCase,
  DiagnosticWorkerOptions,
  DiagnosticWorkerReport,
} from "../examples/runtime-benchmark/src/diagnostic-contracts.ts";
import { writeEvidence } from "../examples/runtime-benchmark/src/evidence.ts";
import { withPublishManifests } from "./release-publish.ts";
import { stageCheckout, subprocess } from "./runtime-benchmark.ts";

const Role = Schema.Literals(["base", "head"]);
const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

const Revision = Schema.Struct({
  role: Role,
  revision: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  dirty: Schema.Boolean,
  lockfileSha256: Hash,
  builtArtifactsSha256: Hash,
  effect: Schema.String,
});

const ActiveBatch = Schema.Struct({ role: Role, cohort: Schema.Natural });

const Batch = Schema.Struct({
  ...ActiveBatch.fields,
  subprocessMs: Schema.Finite,
  exitCode: Schema.Int,
  complete: Schema.Boolean,
  report: Schema.NullOr(DiagnosticWorkerReport),
  failure: Schema.NullOr(Schema.String),
});

export const DiagnosticReport = Schema.Struct({
  fixture: Schema.Literal(DIAGNOSTIC_VERSION),
  fixtureSha256: Schema.NullOr(Hash),
  transpiler: Schema.String,
  environment: Schema.Struct({
    node: Schema.String,
    platform: Schema.String,
    release: Schema.String,
    architecture: Schema.String,
    cpu: Schema.String,
    cpuCount: Schema.Natural,
    memoryBytes: Schema.Natural,
  }),
  settings: Schema.Struct({
    cohorts: Schema.Literal(2),
    warmups: Schema.Literal(2),
    samples: Schema.Literal(5),
    production: Schema.Literal(true),
    execution: Schema.Literal("unbundled published ESM"),
    timingGate: Schema.Literal("informational elapsed wall time"),
  }),
  cases: Schema.Array(DiagnosticCase).check(Schema.isMaxLength(64)),
  revisions: Schema.Array(Revision).check(Schema.isMaxLength(2)),
  batches: Schema.Array(Batch).check(Schema.isMaxLength(4)),
  activeBatch: Schema.NullOr(ActiveBatch),
  phase: Schema.Literals(["setup", "comparison", "complete"]),
  failure: Schema.NullOr(Schema.String),
});

export type DiagnosticReport = typeof DiagnosticReport.Type;

export const renderDiagnosticReport = (report: DiagnosticReport): string => {
  const lines = [
    "Manual public-package diagnostics. Elapsed wall milliseconds, median [Q1–Q3]; marks are inclusive. No CPU or provider latency claim.",
    "",
    `Status: ${report.phase}; ${report.batches.filter((batch) => batch.complete && batch.exitCode === 0).length}/4 complete batches.`,
    ...(report.failure === null ? [] : [`Failure: ${report.failure}`]),
    "",
    "| Case / metric | Base | Head |",
    "| --- | ---: | ---: |",
  ];

  for (const workload of report.cases) {
    const samples = (role: "base" | "head") =>
      report.batches
        .filter((batch) => batch.role === role && batch.complete && batch.exitCode === 0)
        .flatMap((batch) => batch.report?.samples ?? [])
        .filter(
          (sample) => sample.case === workload.name && !sample.warmup && sample.status === "passed",
        )
        .flatMap((sample) => (sample.result === null ? [] : [sample.result]));

    const base = samples("base");
    const head = samples("head");

    const names = [
      ...new Set([...base, ...head].flatMap((result) => result.metrics.map(({ name }) => name))),
    ];

    const format = (values: ReadonlyArray<number>) => {
      const stats = summary(values);

      return stats.count === 0
        ? "n/a"
        : `${stats.median.toFixed(2)} [${stats.q1.toFixed(2)}–${stats.q3.toFixed(2)}] (n=${stats.count})`;
    };

    lines.push(
      `| ${workload.name} / total | ${format(base.map(({ totalMs }) => totalMs))} | ${format(head.map(({ totalMs }) => totalMs))} |`,
    );
    for (const name of names) {
      const values = (results: typeof base) =>
        results.flatMap((result) =>
          result.metrics.filter((metric) => metric.name === name).map(({ value }) => value),
        );

      lines.push(
        `| ${workload.name} / ${name} | ${format(values(base))} | ${format(values(head))} |`,
      );
    }
  }

  return lines.join("\n") + "\n";
};

export const compareDiagnostics = Effect.fn("diagnostic.compare")(function* (options: {
  root: string;
  base: string;
  output: string;
  requireClean: boolean;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = path.resolve(options.root);
  const output = path.resolve(options.output);

  yield* fs.makeDirectory(output, { recursive: true });
  yield* check(
    !(yield* fs.exists(path.join(output, "report.json"))),
    "Output already contains a report; choose a new --out-dir",
  );

  const report: { -readonly [K in keyof DiagnosticReport]: DiagnosticReport[K] } = {
    fixture: DIAGNOSTIC_VERSION,
    fixtureSha256: null,
    transpiler: `esbuild ${esbuildVersion} (fixture syntax only; no bundling)`,
    environment: {
      node: "unknown",
      platform: platform(),
      release: release(),
      architecture: arch(),
      cpu: cpus()[0]?.model ?? "unknown",
      cpuCount: cpus().length,
      memoryBytes: totalmem(),
    },
    settings: {
      ...DIAGNOSTIC_SIZES,
      production: true,
      execution: "unbundled published ESM",
      timingGate: "informational elapsed wall time",
    },
    cases: diagnosticCases,
    revisions: [],
    batches: [],
    activeBatch: null,
    phase: "setup",
    failure: null,
  };

  const persist = Effect.gen(function* () {
    yield* writeEvidence(
      path.join(output, "report.json"),
      yield* Schema.encodeEffect(Schema.fromJsonString(DiagnosticReport))(report),
    );
    yield* writeEvidence(path.join(output, "report.md"), renderDiagnosticReport(report));
  });

  yield* Effect.gen(function* () {
    yield* persist;
    yield* check(options.requireClean, "Diagnostics require clean exact-commit checkouts");
    const node = yield* subprocess("node", ["--version"], root);

    report.environment = { ...report.environment, node: node.stdout.trim() };
    yield* check(
      node.exitCode === 0 && report.environment.node.startsWith("v24."),
      "Diagnostics require Node 24",
    );
    const fixtures = yield* fs.makeTempDirectoryScoped({ prefix: "runtime-diagnostic-fixture-" });
    const source = path.join(root, "examples/runtime-benchmark/src");

    yield* Effect.tryPromise({
      try: () =>
        build({
          entryPoints: [
            "contracts.ts",
            "evidence.ts",
            "diagnostic-contracts.ts",
            "diagnostic-cases.ts",
            "diagnostic-worker.ts",
            "diagnostic-policy.ts",
            "diagnostic-capabilities.ts",
            "diagnostic-ledger.ts",
            "diagnostic-writer.ts",
            "diagnostic-fairness.ts",
          ].map((file) => path.join(source, file)),
          outdir: fixtures,
          bundle: false,
          platform: "node",
          format: "esm",
          target: "node24",
          sourcemap: false,
          minify: false,
          logLevel: "silent",
        }),
      catch: (cause) =>
        BenchmarkError.make({ message: "Cannot transpile diagnostic fixtures", cause }),
    });

    const bytes = yield* Effect.forEach((yield* fs.readDirectory(fixtures)).sort(), (file) =>
      fs.readFileString(path.join(fixtures, file)).pipe(Effect.map((text) => `${file}\n${text}`)),
    );

    report.fixtureSha256 = createHash("sha256").update(bytes.join("\n")).digest("hex");
    yield* fs.copy(fixtures, path.join(output, "fixture"));
    const base = yield* stageCheckout(path.resolve(options.base), "base", fixtures);
    const head = yield* stageCheckout(root, "head", fixtures);
    const stages = [base, head];

    report.revisions = yield* Effect.forEach(stages, (stage) =>
      Schema.decodeUnknownEffect(Revision)(stage.revision),
    );
    yield* check(
      report.revisions.every((revision) => !revision.dirty),
      "Diagnostics require clean exact-commit checkouts",
    );

    const measure = Effect.gen(function* () {
      yield* check(
        new Set(report.cases.map(({ name }) => name)).size === report.cases.length,
        "Diagnostic case names must be unique",
      );
      report.phase = "comparison";
      yield* persist;
      for (let cohort = 0; cohort < DIAGNOSTIC_SIZES.cohorts; cohort++)
        for (const stage of cohort === 0 ? stages : [...stages].reverse()) {
          const role = yield* Schema.decodeUnknownEffect(Role)(stage.revision.role);
          const name = `${cohort}-${role}`;
          const filename = path.join(output, `${name}.json`);

          const workerOptions: DiagnosticWorkerOptions = {
            output: filename,
            warmups: DIAGNOSTIC_SIZES.warmups,
            samples: DIAGNOSTIC_SIZES.samples,
            timeoutMs: 120_000,
          };

          report.activeBatch = { cohort, role };
          yield* persist;
          yield* Console.error(`Measuring diagnostics ${name}`);
          yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const started = yield* Clock.monotonicTimeNanos;

              const child = yield* restore(
                subprocess(
                  "node",
                  [path.join(stage.stage, "fixture/diagnostic-worker.js")],
                  stage.stage,
                  {
                    NODE_ENV: "production",
                    RUNTIME_DIAGNOSTIC_OPTIONS: yield* Schema.encodeEffect(
                      Schema.fromJsonString(DiagnosticWorkerOptions),
                    )(workerOptions),
                  },
                  path.join(output, `${name}.log`),
                ).pipe(Effect.timeout("5 minutes")),
              ).pipe(Effect.exit);

              const subprocessMs = Exit.isSuccess(child)
                ? child.value.subprocessMs
                : Number((yield* Clock.monotonicTimeNanos) - started) / 1e6;

              const decoded = yield* Effect.gen(function* () {
                yield* check(
                  yield* fs.exists(filename),
                  "Diagnostic worker did not produce a report",
                );
                const stat = yield* fs.stat(filename);

                yield* check(
                  stat.size <= 16n * 1024n * 1024n,
                  "Diagnostic worker report exceeds 16 MiB",
                );

                return yield* Schema.decodeUnknownEffect(
                  Schema.fromJsonString(DiagnosticWorkerReport),
                )(yield* fs.readFileString(filename));
              }).pipe(Effect.exit);

              const worker = Exit.isSuccess(decoded) ? decoded.value : null;
              const exitCode = Exit.isSuccess(child) ? child.value.exitCode : -1;

              const complete =
                worker !== null &&
                completeDiagnosticBatch(worker, workerOptions, report.cases) &&
                worker.runtime === report.environment.node &&
                worker.platform === report.environment.platform &&
                worker.architecture === report.environment.architecture;

              report.batches = [
                ...report.batches,
                {
                  role,
                  cohort,
                  subprocessMs,
                  exitCode,
                  complete,
                  report: worker,
                  failure: Exit.isFailure(child)
                    ? Cause.pretty(child.cause).slice(0, 8_192)
                    : Exit.isFailure(decoded)
                      ? Cause.pretty(decoded.cause).slice(0, 8_192)
                      : exitCode !== 0
                        ? child.value.stderr.slice(0, 8_192)
                        : !complete
                          ? "Incomplete work or mismatched worker environment"
                          : null,
                },
              ];
              report.activeBatch = null;
              yield* persist;
              if (Exit.isFailure(child) && Cause.hasInterrupts(child.cause))
                return yield* Effect.failCause(child.cause);
            }),
          );
        }
    });

    yield* withPublishManifests(base.stage, () => withPublishManifests(head.stage, () => measure));
    yield* check(
      report.batches.length === 4 &&
        report.batches.every((batch) => batch.complete && batch.exitCode === 0),
      "Diagnostic correctness failed; inspect all retained samples and failures",
    );
    report.phase = "complete";
  }).pipe(
    Effect.timeout("19 minutes"),
    Effect.onExit((exit) => {
      if (Exit.isFailure(exit)) report.failure = Cause.pretty(exit.cause).slice(0, 8_192);

      return persist;
    }),
  );
  yield* Console.log(renderDiagnosticReport(report));

  return report;
}, Effect.scoped);

export const command = Command.make(
  "runtime-diagnostics",
  {
    base: Flag.string("base-dir").pipe(
      Flag.withDescription(
        "Clean exact base checkout with its own installed lockfile and public packages built.",
      ),
    ),
    output: Flag.string("out-dir").pipe(
      Flag.withDefault(".performance-report"),
      Flag.withDescription("New artifact directory; previous reports are never overwritten."),
    ),
    requireClean: Flag.boolean("require-clean").pipe(
      Flag.withDefault(true),
      Flag.withDescription("Required: diagnostics reject modified checkouts."),
    ),
  },
  Effect.fn(function* ({ base, output, requireClean }) {
    const path = yield* Path.Path;

    const root = path.resolve(
      path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
      "..",
    );

    yield* compareDiagnostics({ root, base, output, requireClean });
  }),
).pipe(
  Command.withDescription(
    "Manual deterministic public-package diagnostics; two alternating base/head cohorts, ten measured samples per case. No network model calls.",
  ),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: DIAGNOSTIC_VERSION }).pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );
