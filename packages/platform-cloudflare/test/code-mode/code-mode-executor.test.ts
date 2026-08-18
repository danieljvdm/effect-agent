import { join } from "node:path";

import { build } from "esbuild";
import { Miniflare, kCurrentWorker } from "miniflare";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";

/**
 * The Cloudflare Dynamic Worker `CodeExecutor` lane (C4 of ADR-0017,
 * DEPLOY-011). The real adapter runs inside a bundled worker under
 * programmatic Miniflare — a genuine workerd runtime with a real
 * `worker_loaders` binding. The worker exports `CodeModeHostEntrypoint` and
 * self-binds it via `kCurrentWorker` (the production seam is
 * `ctx.exports.CodeModeHostEntrypoint()`), then runs the shared executor
 * conformance suite plus the isolated-only enforcement cases (ambient network
 * denial, synchronous CPU runaway) in-worker and reports failures.
 *
 * Pool-workers cannot host this: it wraps the user worker, so a
 * `kCurrentWorker` self-binding to a named entrypoint is unreachable. The
 * programmatic runtime, where the bundled worker IS the top-level worker,
 * makes the self-binding resolve — the same approach as the restart lane.
 */

const workerEntry = join(import.meta.dirname, "conformance-worker.ts");

let workerScript = "";

const openRuntime = (): Miniflare =>
  new Miniflare({
    modules: true,
    script: workerScript,
    modulesRoot: "/",
    compatibilityDate: "2025-05-01",
    // Deployed consumers cannot set the `experimental` compatibility flag, so
    // the conformance runtime must not either: with it present, a load payload
    // that requires experimental features (e.g. `allowExperimental: true`)
    // passes here while rejecting every pass in production.
    compatibilityFlags: ["nodejs_compat"],
    workerLoaders: { LOADER: {} },
    serviceBindings: {
      CODE_MODE_HOST: { name: kCurrentWorker, entrypoint: "CodeModeHostEntrypoint" },
    },
  });

describe("DEPLOY-011 Cloudflare Dynamic Worker CodeExecutor", () => {
  const cleanups: Array<() => Promise<void>> = [];
  let runtime: Miniflare;

  beforeAll(async () => {
    const bundled = await build({
      entryPoints: [workerEntry],
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
    if (output === undefined) {
      throw new Error("esbuild produced no worker bundle");
    }
    workerScript = output.text;
    runtime = openRuntime();
    cleanups.push(() => runtime.dispose());
  }, 120_000);

  afterAll(async () => {
    for (const cleanup of cleanups.reverse()) {
      await cleanup();
    }
  });

  it("passes the shared executor conformance suite and isolated-only enforcement cases in workerd", async () => {
    const response = await runtime.dispatchFetch("http://placeholder/run");
    const payload = (await response.json()) as { readonly failures: ReadonlyArray<string> };
    expect(response.ok).toBe(true);
    expect(payload.failures).toEqual([]);
  }, 120_000);

  it("runs guest host calls outside the in-flight executor fiber", async () => {
    const response = await runtime.dispatchFetch("http://placeholder/host-call-root-fiber");
    expect(response.ok).toBe(true);
    expect(await response.json()).toMatchObject({
      tag: "success",
      detail: { value: "root" },
    });
  }, 30_000);
});
