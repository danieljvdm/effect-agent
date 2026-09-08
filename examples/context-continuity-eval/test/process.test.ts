import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, FileSystem, Path } from "effect";
import { expect, it } from "vite-plus/test";

import { pressureInstructions, pressureScenario, pressureToolkit } from "../src/pressure.ts";
import { supervise } from "../src/process-host.ts";

it("keeps the oracle out of the pressure script and removes model-directed rollover", () => {
  expect(Object.keys(pressureToolkit.tools)).not.toContain("new_context");
  expect(pressureInstructions).not.toContain("call new_context");
  expect(pressureScenario(17)[6]?.message).not.toContain(pressureScenario(17)[6]?.receipt?.code);
  expect(pressureScenario(17)[6]?.message).not.toContain("native context-window transition");
});

it("recovers the real SQLite runtime after two SIGKILLs with pressure and cumulative accounting", async () => {
  const report = await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      const outputDirectory = yield* fs.makeTempDirectoryScoped({
        prefix: "continuity-kill-test-",
      });

      return yield* supervise(
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
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      Effect.provide(
        ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENAI_API_KEY: "test-only" })),
      ),
    ),
  );

  expect(report.status).toBe("passed");
  expect(report.windows.length).toBeGreaterThanOrEqual(12);
  expect(report.restarts.map((r) => r.killConfirmed)).toEqual([true, true]);
  expect(report.usage.calls).toBe(report.phases.reduce((n, p) => n + p.modelCalls, 0));
}, 60_000);
