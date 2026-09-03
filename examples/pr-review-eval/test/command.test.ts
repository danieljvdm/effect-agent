import {
  ReviewDiagnostics,
  ReviewOutcome,
  ReviewReport,
  type ReviewRequest,
} from "@effect-agent/pr-review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { ConfigProvider, Effect, FileSystem, Path, Schema, Sink, Stream } from "effect";
import { Command } from "effect/unstable/cli";
import { ChildProcessSpawner } from "effect/unstable/process";

import { command } from "../src/command.ts";
import {
  EvalSuite,
  EvalQualityReport,
  EvalVariantConfiguration,
  digestReviewRequest,
  freezeEvalRun,
  freezeEvalSuite,
  loadEvalSuite,
  runEvalSuite,
  writeObservations,
} from "../src/index.ts";

// Only Git identity is substituted. The real CLI parses flags, reads/writes the suite,
// rebuilds variants and checks their identity. An empty API-key configuration stops before HTTP.
const cleanCheckout = ChildProcessSpawner.make((command) =>
  Effect.gen(function* () {
    if (command._tag !== "StandardCommand" || command.command !== "git")
      return yield* Effect.die("Unexpected child process");
    const args = command.args.join(" ");

    const output =
      args === "rev-parse --show-toplevel"
        ? "/synthetic-clean-reviewer\n"
        : args === "status --porcelain --untracked-files=all"
          ? ""
          : args === "rev-parse HEAD"
            ? `${"a".repeat(40)}\n`
            : undefined;

    if (output === undefined) return yield* Effect.die(`Unexpected Git identity command: ${args}`);
    const stdout = Stream.make(new TextEncoder().encode(output));

    return ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(42),
      exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
      isRunning: Effect.succeed(false),
      kill: () => Effect.void,
      stdin: Sink.drain,
      stdout,
      stderr: Stream.empty,
      all: stdout,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
      unref: Effect.succeed(Effect.void),
    });
  }),
);

const run = Command.runWith(command, { version: "test", renderErrors: false });

describe("PR-review eval command", () => {
  it.effect(
    "reports the frozen two-trial grid without a trials flag and rejects an explicit mismatch",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped();

        const cases = yield* path.fromFileUrl(
          new URL("../fixtures/verification-corpus.json", import.meta.url),
        );

        const frozenPath = `${directory}/baseline.json`;
        const observationPath = `${directory}/observations.jsonl`;
        const reportPath = `${directory}/report.json`;

        yield* run(["--cases", cases, "freeze-run", "--trials", "2", "--output", frozenPath]);
        const frozen = yield* loadEvalSuite(frozenPath);

        if (frozen.frozenRun === undefined) return yield* Effect.die("Missing baseline freeze");

        const variant = {
          configuration: frozen.frozenRun.configuration,
          review: (request: ReviewRequest) =>
            Effect.gen(function* () {
              const requestDigest = yield* digestReviewRequest(request).pipe(Effect.orDie);

              return ReviewOutcome.make({
                report: ReviewReport.make({ summary: "Synthetic completed review", findings: [] }),
                diagnostics: ReviewDiagnostics.make({
                  strategy: "baseline",
                  requestDigest,
                  discovery: "complete",
                  verification: "not-requested",
                  patchesSupplied: request.changes.length,
                  candidates: [],
                  activity: [],
                  droppedActivityCount: 0,
                  droppedCandidateCount: 0,
                  stages: [],
                }),
                turns: 1,
                usage: {
                  inputTokens: 0,
                  uncachedInputTokens: 0,
                  cachedInputTokens: 0,
                  cacheWriteInputTokens: 0,
                  outputTokens: 0,
                },
              });
            }),
        };

        yield* writeObservations(
          observationPath,
          runEvalSuite(frozen, [variant], {
            trials: 2,
            concurrency: 1,
            caseIds: [],
          }),
        );
        yield* run([
          "--cases",
          frozenPath,
          "report",
          "--observations",
          observationPath,
          "--output",
          reportPath,
        ]);

        const report = yield* fs
          .readFileString(reportPath)
          .pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(EvalQualityReport))),
          );

        expect(report.trialCount).toBe(2);
        expect(report.frozenRunDigest).toBe(frozen.frozenRun.digest);
        expect(report.variants[0]?.resources.attemptedTrials).toBe(frozen.cases.length * 2);
        const rejectedPath = `${directory}/mismatched-report.json`;

        const failure = yield* run([
          "--cases",
          frozenPath,
          "report",
          "--trials",
          "3",
          "--observations",
          observationPath,
          "--output",
          rejectedPath,
        ]).pipe(Effect.flip);

        expect(failure).toMatchObject({ _tag: "EvalReportError" });
        expect(yield* fs.exists(rejectedPath)).toBe(false);
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, cleanCheckout),
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
        Effect.scoped,
        Effect.provide(NodeServices.layer),
      ),
  );

  it.effect.each([
    { freeze: "freeze", model: "gpt-5.6-sol", reasoningEffort: "xhigh", flags: [] },
    {
      freeze: "freeze",
      model: "gpt-5.6-terra",
      reasoningEffort: "high",
      flags: ["--model", "gpt-5.6-terra", "--reasoning-effort", "high"],
    },
    {
      freeze: "freeze-run",
      model: "gpt-5.6-sol",
      reasoningEffort: "xhigh",
      flags: ["--trials", "2"],
    },
    {
      freeze: "freeze-run",
      model: "gpt-5.6-terra",
      reasoningEffort: "high",
      flags: ["--model", "gpt-5.6-terra", "--reasoning-effort", "high", "--trials", "2"],
    },
  ])(
    "$freeze reconstructs $model/$reasoningEffort and the frozen trial count",
    ({ freeze, model, reasoningEffort, flags }) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped();

        const cases = yield* path.fromFileUrl(
          new URL("../fixtures/verification-corpus.json", import.meta.url),
        );

        const frozenPath = `${directory}/frozen.json`;
        const observations = `${directory}/observations.jsonl`;

        yield* run([
          "--cases",
          cases,
          freeze,
          "--variant",
          "recipe",
          "--output",
          frozenPath,
          ...flags,
        ]);
        const frozen = yield* loadEvalSuite(frozenPath);

        const configurations =
          frozen.frozenRun === undefined
            ? frozen.comparison?.configurations
            : [frozen.frozenRun.configuration];

        expect(configurations).toHaveLength(freeze === "freeze" ? 2 : 1);
        if (freeze === "freeze-run") expect(frozen.frozenRun?.trials).toBe(2);
        expect(
          configurations?.map((configuration) => ({
            model: configuration.model,
            reasoningEffort: configuration.reasoningEffort,
            strategy: configuration.strategy,
          })),
        ).toEqual(
          freeze === "freeze"
            ? [
                { model, reasoningEffort, strategy: "baseline" },
                { model, reasoningEffort, strategy: "verified" },
              ]
            : [{ model, reasoningEffort, strategy: "baseline" }],
        );

        const failure = yield* run(["--cases", frozenPath, "run", "--output", observations]).pipe(
          Effect.flip,
        );

        expect(failure).toMatchObject({ _tag: "ConfigError" });
        expect(String(failure)).toContain("OPENAI_API_KEY");
        expect(yield* fs.exists(observations)).toBe(false);
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, cleanCheckout),
        Effect.provide(
          ConfigProvider.layer(ConfigProvider.fromUnknown({ EFFECT_AGENT_LIVE: "1" })),
        ),
        Effect.scoped,
        Effect.provide(NodeServices.layer),
      ),
  );

  it.effect("refuses replay when frozen pricing disagrees with the selected model", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped();

      const cases = yield* path.fromFileUrl(
        new URL("../fixtures/verification-corpus.json", import.meta.url),
      );

      const frozenPath = `${directory}/frozen.json`;

      yield* run(["--cases", cases, "freeze", "--model", "gpt-5.6-terra", "--output", frozenPath]);
      const frozen = yield* loadEvalSuite(frozenPath);

      if (frozen.comparison === undefined) return yield* Effect.die("Missing frozen comparison");

      const changed = yield* freezeEvalSuite(
        frozen,
        frozen.comparison.configurations.map((configuration) =>
          EvalVariantConfiguration.make({ ...configuration, model: "gpt-5.6-sol" }),
        ),
        "different-model",
      );

      const changedPath = `${directory}/changed.json`;
      const observations = `${directory}/observations.jsonl`;

      yield* fs.writeFileString(
        changedPath,
        Schema.encodeSync(Schema.fromJsonString(EvalSuite))(changed),
      );

      const failure = yield* run(["--cases", changedPath, "run", "--output", observations]).pipe(
        Effect.flip,
      );

      expect(failure).toMatchObject({
        _tag: "EvalConfigurationError",
        message:
          "Effective reviewer configuration differs from the freeze; create a new comparison",
      });
      expect(yield* fs.exists(observations)).toBe(false);
    }).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, cleanCheckout),
      Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ EFFECT_AGENT_LIVE: "1" }))),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect(
    "rejects baseline manifest, guidance and grid drift before provider construction or output creation",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped();

        const cases = yield* path.fromFileUrl(
          new URL("../fixtures/verification-corpus.json", import.meta.url),
        );

        const frozenPath = `${directory}/baseline.json`;
        const guidance = `${directory}/guidance.txt`;

        yield* fs.writeFileString(guidance, "Review changed runtime behavior.");
        yield* run([
          "--cases",
          cases,
          "freeze-run",
          "--trials",
          "2",
          "--model",
          "gpt-5.6-terra",
          "--guidance",
          guidance,
          "--output",
          frozenPath,
        ]);
        const frozen = yield* loadEvalSuite(frozenPath);

        if (frozen.frozenRun === undefined || frozen.cases[0] === undefined)
          return yield* Effect.die("Missing baseline freeze");
        for (const flags of [
          [],
          ["--guidance", guidance, "--trials", "1"],
          ["--guidance", guidance, "--case", frozen.cases[0].id],
          ["--guidance", guidance, "--concurrency", "2"],
          ["--guidance", guidance, "--strategy", "verified"],
        ]) {
          const output = `${directory}/must-not-exist.jsonl`;

          expect(
            (yield* run(["--cases", frozenPath, "run", "--output", output, ...flags]).pipe(
              Effect.flip,
            ))._tag,
          ).toBe("EvalConfigurationError");
          expect(yield* fs.exists(output)).toBe(false);
        }

        const drifted = yield* freezeEvalRun(
          frozen,
          EvalVariantConfiguration.make({
            ...frozen.frozenRun.configuration,
            model: "gpt-5.6-sol",
          }),
          "wrong-pricing",
          2,
        );

        const tampered = EvalSuite.make({ ...frozen, cases: [...frozen.cases].reverse() });

        for (const suite of [drifted, tampered]) {
          const input = `${directory}/changed.json`;
          const output = `${directory}/must-not-exist.jsonl`;

          yield* fs.writeFileString(
            input,
            Schema.encodeSync(Schema.fromJsonString(EvalSuite))(suite),
          );
          expect(
            (yield* run(["--cases", input, "run", "--guidance", guidance, "--output", output]).pipe(
              Effect.flip,
            ))._tag,
          ).toBe("EvalConfigurationError");
          expect(yield* fs.exists(output)).toBe(false);
        }
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, cleanCheckout),
        Effect.provide(
          ConfigProvider.layer(ConfigProvider.fromUnknown({ EFFECT_AGENT_LIVE: "1" })),
        ),
        Effect.scoped,
        Effect.provide(NodeServices.layer),
      ),
  );
});
