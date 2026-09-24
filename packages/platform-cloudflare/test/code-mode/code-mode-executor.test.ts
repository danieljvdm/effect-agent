import { join } from "node:path";

import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";

/**
 * The Cloudflare Dynamic Worker `CodeExecutor` lane (C4 of ADR-0017,
 * DEPLOY-011). The real adapter runs inside a bundled worker under
 * programmatic Miniflare — a genuine workerd runtime with a real
 * `worker_loaders` binding, then checks ambient network denial, hung-program retirement, and host
 * result limits in-worker and reports failures.
 */

const workerEntry = join(import.meta.dirname, "conformance-worker.ts");

let workerScript = "";

const openRuntime = (): Miniflare =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: workerScript,
      modulesRoot: "/",
      compatibilityDate: "2025-05-01",
      // Deployed consumers cannot set the `experimental` compatibility flag, so
      // the conformance runtime must not either: with it present, a load payload
      // that requires experimental features (e.g. `allowExperimental: true`)
      // passes here while rejecting every pass in production.
      compatibilityFlags: [
        "nodejs_compat",
        "enable_ctx_exports",
        "no_handle_cross_request_promise_resolution",
      ],
      workerLoaders: { LOADER: {} },
      durableObjects: {
        CODE_MODE_EXECUTORS: { className: "CodeModeExecutorObject" },
      },
    }),
  );

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

  it("enforces ambient network denial, hung-program retirement, and host result limits in workerd", async () => {
    const response = await runtime.dispatchFetch("http://placeholder/run");
    const payload = (await response.json()) as { readonly failures: ReadonlyArray<string> };

    expect(response.ok).toBe(true);
    expect(payload.failures).toEqual([]);
  }, 120_000);

  // Regression evidence:
  // https://github.com/reve-ai/kommunikasie/actions/runs/32474947060
  // https://github.com/reve-ai/kommunikasie/actions/runs/32483559567
  it("keeps host-call settlement inside each owning Durable Object context", async () => {
    const response = await runtime.dispatchFetch("http://placeholder/durable-object-host-call");

    expect(response.ok).toBe(true);
    expect(await response.json()).toMatchObject({
      outcomes: [
        { tag: "success", value: "executor" },
        { tag: "success", value: "executor" },
      ],
    });
  }, 30_000);
});
