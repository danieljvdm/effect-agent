import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { expect, it } from "vite-plus/test";

import { RecoveryCheckpointEvidence } from "../src/contracts.ts";
import { pressureInstructions, pressureScenario, pressureToolkit } from "../src/pressure.ts";
import { supervise } from "../src/process-host.ts";

it("keeps the oracle out of the pressure script and removes model-directed rollover", () => {
  expect(Object.keys(pressureToolkit.tools)).not.toContain("new_context");
  expect(pressureInstructions).not.toContain("call new_context");
  expect(pressureScenario(17)[6]?.message).not.toContain(pressureScenario(17)[6]?.receipt?.code);
  expect(pressureScenario(17)[6]?.message).not.toContain("native context-window transition");
});

it("recovers the real SQLite runtime after two SIGKILLs with pressure and cumulative accounting", async () => {
  const { report, checkpoint } = await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      const outputDirectory = yield* fs.makeTempDirectoryScoped({
        prefix: "continuity-kill-test-",
      });

      const report = yield* supervise(
        {
          model: "gpt-6-astra",
          reasoningEffort: "low",
          seed: 17,
          outputDirectory,
          sourceCommit: "a".repeat(40),
          dirtyWorkingTree: false,
          maxCostMicrousd: 10_000_000,
          profile: "pressure-restart-sqlite-v1",
        },
        yield* path.fromFileUrl(new URL("./scripted-worker.ts", import.meta.url)),
      );

      const checkpoint = yield* Schema.decodeEffect(
        Schema.fromJsonString(RecoveryCheckpointEvidence),
      )(yield* fs.readFileString(path.join(outputDirectory, "recovery-checkpoint.json")));

      return { report, checkpoint };
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          NodeServices.layer,
          ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENAI_API_KEY: "test-only" })),
        ),
      ),
    ),
  );

  expect(report.status).toBe("passed");
  expect(checkpoint.status).toBe("present");
  if (checkpoint.status !== "present") throw new Error("Native SQLite checkpoint absent");
  expect(checkpoint.throughSequence).toBeGreaterThanOrEqual(report.windows.at(-1)?.sequence ?? 1);

  expect(report.windows.length).toBeGreaterThanOrEqual(12);
  expect(report.restarts.map((r) => r.killConfirmed)).toEqual([true, true]);
  expect(report.usage.calls).toBe(report.phases.reduce((n, p) => n + p.modelCalls, 0));
}, 60_000);

it.each(["exit", "invalid-barrier", "interruption"] as const)(
  "does not respawn after an unexpected %s",
  async (mode) => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;

        const outputDirectory = yield* fs.makeTempDirectoryScoped({
          prefix: "continuity-supervisor-failure-",
        });

        const entry = path.join(outputDirectory, "fixture.mjs");
        const pidPath = path.join(outputDirectory, "pid");

        yield* fs.writeFileString(
          entry,
          `
      import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
      const options = JSON.parse(readFileSync(process.env.CONTEXT_EVAL_WORKER_OPTIONS, "utf8"));
      appendFileSync(options.outputDirectory + "/launches", "launch\\n");
      writeFileSync(options.outputDirectory + "/pid", String(process.pid));
      ${mode === "exit" ? "process.exitCode = 7;" : mode === "invalid-barrier" ? 'writeFileSync(options.outputDirectory + "/barrier-4.json", "{}"); setInterval(() => {}, 1000);' : "setInterval(() => {}, 1000);"}
    `,
        );

        const run = supervise(
          {
            model: "gpt-6-astra",
            reasoningEffort: "low",
            seed: 17,
            outputDirectory,
            sourceCommit: "a".repeat(40),
            dirtyWorkingTree: false,
            maxCostMicrousd: 10_000_000,
            profile: "pressure-restart-sqlite-v1",
          },
          entry,
        );

        const exit =
          mode === "interruption"
            ? yield* Effect.raceFirst(
                run,
                Effect.gen(function* () {
                  while (!(yield* fs.exists(pidPath))) yield* Effect.sleep("20 millis");

                  return yield* Effect.interrupt;
                }),
              ).pipe(Effect.exit)
            : yield* run.pipe(Effect.exit);

        const pid = Number(yield* fs.readFileString(pidPath));
        let running = true;

        try {
          process.kill(pid, 0);
        } catch {
          running = false;
        }

        return {
          failed: exit._tag === "Failure",
          running,
          launches: yield* fs.readFileString(path.join(outputDirectory, "launches")),
        };
      }).pipe(
        Effect.timeout("10 seconds"),
        Effect.scoped,
        Effect.provide(
          Layer.merge(
            NodeServices.layer,
            ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENAI_API_KEY: "test-only" })),
          ),
        ),
      ),
    );

    expect(result).toEqual({ failed: true, running: false, launches: "launch\n" });
  },
  15_000,
);
