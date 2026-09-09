import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { BenchmarkResult, Mode, Workload } from "./benchmark-contract.ts";

const Sample = Schema.Struct({
  mode: Mode,
  workload: Workload,
  elapsedMs: Schema.Number,
  warmup: Schema.Boolean,
  result: BenchmarkResult,
});

type Sample = typeof Sample.Type;
let runtime: Miniflare;
let fixtureSha256 = "";

beforeAll(async () => {
  const bundled = await build({
    entryPoints: [join(import.meta.dirname, "benchmark-worker.ts")],
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    logLevel: "silent",
  });

  const output = bundled.outputFiles[0];

  if (output === undefined) throw new Error("Missing benchmark bundle");
  fixtureSha256 = createHash("sha256").update(output.text).digest("hex");
  runtime = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: output.text,
      modulesRoot: "/",
      compatibilityDate: "2025-05-01",
      compatibilityFlags: ["nodejs_compat", "no_handle_cross_request_promise_resolution"],
      workerLoaders: { LOADER: {} },
    }),
  );
  await runtime.ready;
}, 120_000);
afterAll(async () => {
  await runtime?.dispose();
});

const run = async (
  mode: Sample["mode"],
  workload: Sample["workload"],
  warmup: boolean,
): Promise<Sample> => {
  const started = performance.now();

  const response = await runtime.dispatchFetch(
    `http://benchmark/?mode=${mode}&workload=${workload}`,
  );

  const payload: unknown = await response.json();
  const elapsedMs = performance.now() - started;

  if (response.status !== 200)
    throw new Error(`Benchmark request failed: ${JSON.stringify(payload)}`);
  const result = Schema.decodeUnknownSync(BenchmarkResult)(payload);

  expect(result.answer).toEqual(
    workload === "writes"
      ? { projectId: "launch", tasksCreated: 3 }
      : { customer: "Acme", unpaidCents: 2_000 },
  );
  expect(result.toolCalls).toBe(workload === "writes" ? 4 : 2);
  expect(result.modelCalls).toBe(mode === "native" && workload === "writes" ? 3 : 2);
  expect(result.peakConcurrency).toBe(mode === "sequential" ? 1 : workload === "writes" ? 3 : 2);
  expect(result.activeAfterRun).toBe(0);

  return { mode, workload, elapsedMs, warmup, result };
};

const modes = ["native", "sequential", "concurrent"] as const;
const workloads = ["reads", "writes"] as const;

it("executes equivalent native, sequential and concurrent workloads in workerd", async () => {
  const samples: Array<Sample> = [];

  for (const workload of workloads)
    for (const mode of modes) samples.push(await run(mode, workload, true));
  expect(samples).toHaveLength(6);
}, 60_000);

it.skipIf(process.env.CODE_MODE_BENCHMARK !== "1")(
  "measures complete replies with matched model and I/O delays",
  async () => {
    const samples: Array<Sample> = [];

    const failures: Array<{
      readonly workload: string;
      readonly mode: string;
      readonly message: string;
    }> = [];

    try {
      for (let iteration = -2; iteration < 20; iteration++) {
        // Rotate execution order to reduce bias from warm caches and host drift.
        const offset = (iteration + 2) % modes.length;
        const order = [...modes.slice(offset), ...modes.slice(0, offset)];

        for (const workload of workloads)
          for (const mode of order)
            try {
              samples.push(await run(mode, workload, iteration < 0));
            } catch (cause) {
              failures.push({ workload, mode, message: String(cause) });
              throw cause;
            }
      }
      expect(samples).toHaveLength(132);
    } finally {
      const summary = workloads.flatMap((workload) =>
        modes.map((mode) => {
          const selected = samples.filter(
            (s) => !s.warmup && s.workload === workload && s.mode === mode,
          );

          const sorted = selected.map((s) => s.elapsedMs).sort((a, b) => a - b);

          return {
            workload,
            mode,
            samples: sorted.length,
            p50Ms: sorted[Math.ceil(sorted.length * 0.5) - 1],
            p95Ms: sorted[Math.ceil(sorted.length * 0.95) - 1],
            modelCalls: selected[0]?.result.modelCalls,
            peakConcurrency: selected[0]?.result.peakConcurrency,
          };
        }),
      );

      const report = {
        environment: { node: process.version, platform: process.platform, arch: process.arch },
        clock: "Node performance.now: before dispatchFetch through fully consumed reply",
        scope:
          "local Miniflare/workerd; scripted model 20ms/request; tool I/O 20ms/call; fresh Dynamic Worker per Code Mode pass",
        revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
        dirty: execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" }).length > 0,
        fixtureSha256,
        failures,
        samples,
        summary,
      };

      await writeFile(
        process.env.CODE_MODE_BENCHMARK_OUT ?? "/tmp/code-mode-benchmark.json",
        JSON.stringify(report, null, 2),
      );
      console.log(JSON.stringify(summary, null, 2));
    }
  },
  120_000,
);
