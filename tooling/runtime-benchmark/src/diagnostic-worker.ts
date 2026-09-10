import process from "node:process";

import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Clock, Config, Context, Effect, Exit, FileSystem, Layer, Schema } from "effect";

import { BenchmarkError, check } from "./contracts.js";
import { runCapabilityCase } from "./diagnostic-capabilities.js";
import { diagnosticCases } from "./diagnostic-cases.js";
import {
  completeDiagnosticBatch,
  DIAGNOSTIC_VERSION,
  DiagnosticCase,
  DiagnosticMark,
  DiagnosticProgress,
  DiagnosticResult,
  DiagnosticWorkerOptions,
  DiagnosticWorkerReport,
  MAX_DIAGNOSTIC_MARKS,
  type DiagnosticActive,
  type DiagnosticSample,
} from "./diagnostic-contracts.js";
import { runFairnessCase } from "./diagnostic-fairness.js";
import { DiagnosticLedgerSeeds, runLedgerCase } from "./diagnostic-ledger.js";
import { runPolicyCase } from "./diagnostic-policy.js";
import { writeEvidence } from "./evidence.js";

export { diagnosticCases } from "./diagnostic-cases.js";

const runDiagnosticCase = Effect.fn("diagnostic.runCase")(function* (workload: DiagnosticCase) {
  if (workload.family === "policy") return yield* runPolicyCase(workload);
  if (workload.family === "ledger") return yield* runLedgerCase(workload);
  if (workload.family === "fairness") return yield* runFairnessCase(workload);

  return yield* runCapabilityCase(workload);
});

export class DiagnosticRunner extends Context.Service<
  DiagnosticRunner,
  { readonly run: typeof runDiagnosticCase }
>()("runtime-benchmark/DiagnosticRunner") {
  static readonly layer = Layer.succeed(DiagnosticRunner, { run: runDiagnosticCase });
}

export const runDiagnosticWorker = Effect.fn("diagnostic.runWorker")(function* (
  options: DiagnosticWorkerOptions,
) {
  const runner = yield* DiagnosticRunner;
  const samples: Array<DiagnosticSample> = [];
  let active: DiagnosticActive | null = null;
  let failure: string | null = null;

  const report = (): DiagnosticWorkerReport => ({
    fixture: DIAGNOSTIC_VERSION,
    runtime: process.version,
    platform: process.platform,
    architecture: process.arch,
    active,
    failure,
    samples,
  });

  const persist = Effect.gen(function* () {
    yield* writeEvidence(
      options.output,
      yield* Schema.encodeEffect(Schema.fromJsonString(DiagnosticWorkerReport))(report()),
    );
  }).pipe(
    Effect.mapError((cause) =>
      BenchmarkError.make({ message: "Cannot persist diagnostic evidence", cause }),
    ),
  );

  yield* Effect.gen(function* () {
    yield* persist;
    yield* Schema.decodeUnknownEffect(Schema.Array(DiagnosticCase).check(Schema.isMaxLength(64)))(
      diagnosticCases,
    );
    yield* check(
      new Set(diagnosticCases.map(({ name }) => name)).size === diagnosticCases.length,
      "Diagnostic case names must be unique",
    );

    for (let ordinal = 0; ordinal < options.warmups + options.samples; ordinal++) {
      const ordered = ordinal % 2 === 0 ? diagnosticCases : [...diagnosticCases].reverse();

      for (const workload of ordered) {
        const started = yield* Clock.monotonicTimeNanos;
        const marks: Array<DiagnosticMark> = [];

        active = {
          case: workload.name,
          ordinal,
          warmup: ordinal < options.warmups,
          phase: "setup",
          elapsedMs: 0,
          marks,
        };
        yield* persist;

        // Each sample's evidence service owns its bounded mark buffer and phase persistence.
        const progressLayer = Layer.effect(
          DiagnosticProgress,
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem;

            return DiagnosticProgress.of({
              phase: Effect.fn("diagnostic.phase")(function* (phase) {
                active = {
                  case: workload.name,
                  ordinal,
                  warmup: ordinal < options.warmups,
                  phase,
                  elapsedMs: Number((yield* Clock.monotonicTimeNanos) - started) / 1e6,
                  marks,
                };
                yield* persist.pipe(Effect.provideService(FileSystem.FileSystem, fs));
              }),
              mark: Effect.fn("diagnostic.mark")(function* (mark) {
                yield* check(
                  marks.length < MAX_DIAGNOSTIC_MARKS,
                  "Diagnostic phase mark limit exceeded",
                );

                const validated = yield* Schema.decodeUnknownEffect(DiagnosticMark)(mark).pipe(
                  Effect.mapError((cause) =>
                    BenchmarkError.make({ message: "Invalid diagnostic mark", cause }),
                  ),
                );

                marks.push(validated);
              }),
            });
          }),
        );

        yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const result = yield* restore(
              runner
                .run(workload)
                .pipe(
                  Effect.flatMap(Schema.decodeUnknownEffect(DiagnosticResult)),
                  Effect.provide(progressLayer),
                  Effect.timeout(options.timeoutMs),
                ),
            ).pipe(Effect.exit);

            samples.push({
              case: workload.name,
              ordinal,
              warmup: ordinal < options.warmups,
              attemptMs: Number((yield* Clock.monotonicTimeNanos) - started) / 1e6,
              phase: active?.phase ?? "setup",
              result: Exit.isSuccess(result) ? result.value : null,
              marks,
              status: Exit.isSuccess(result) ? "passed" : "failed",
              failure: Exit.isFailure(result) ? Cause.pretty(result.cause).slice(0, 8_192) : null,
            });
            active = null;
            yield* persist;
            if (Exit.isFailure(result) && Cause.hasInterrupts(result.cause))
              return yield* Effect.failCause(result.cause);
          }),
        );
      }
    }
    yield* check(
      completeDiagnosticBatch(report(), options, diagnosticCases),
      "Diagnostic correctness failed; inspect retained samples",
    );
  }).pipe(
    Effect.onExit((exit) => {
      if (Exit.isFailure(exit)) failure = Cause.pretty(exit.cause).slice(0, 8_192);

      return persist;
    }),
  );
});

if (import.meta.main)
  NodeRuntime.runMain(
    Effect.gen(function* () {
      const options = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(DiagnosticWorkerOptions),
      )(yield* Config.string("RUNTIME_DIAGNOSTIC_OPTIONS"));

      yield* runDiagnosticWorker(options);
    }).pipe(
      Effect.provide(
        Layer.merge(DiagnosticRunner.layer, DiagnosticLedgerSeeds.layer).pipe(
          Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
        ),
      ),
    ),
  );
