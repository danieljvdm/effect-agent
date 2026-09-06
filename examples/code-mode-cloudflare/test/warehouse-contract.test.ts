import { join } from "node:path";

import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";

describe("warehouse RPC boundary", () => {
  let runtime: Miniflare;

  beforeAll(async () => {
    const bundle = await build({
      entryPoints: [join(import.meta.dirname, "warehouse-worker.ts")],
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      logLevel: "silent",
    });

    const output = bundle.outputFiles[0];

    if (output === undefined) throw new Error("No warehouse worker bundle");
    runtime = new Miniflare(
      convertV4MiniflareOptions({
        modules: true,
        script: output.text,
        modulesRoot: "/",
        compatibilityDate: "2025-05-01",
        durableObjects: { WAREHOUSE: { className: "ForeignWarehouse", useSQLite: true } },
      }),
    );
  });

  afterAll(async () => {
    await runtime?.dispose();
  });

  it("preserves valid JSON query results", async () => {
    const response = await runtime.dispatchFetch("http://warehouse/valid");

    expect(await response.json()).toEqual({
      ok: true,
      columns: ["value"],
      rows: [{ value: 42 }],
      rowCount: 1,
      truncated: false,
    });
  });

  it.each(["non-json", "negative-count", "missing-reason"])(
    "denies malformed %s RPC outcomes",
    async (scenario) => {
      const response = await runtime.dispatchFetch(`http://warehouse/${scenario}`);

      expect(await response.json()).toEqual({
        ok: false,
        columns: [],
        rows: [],
        rowCount: 0,
        truncated: false,
        reason: "warehouse returned a malformed query outcome",
      });
    },
  );

  it("keeps transport failure as a denied query", async () => {
    const response = await runtime.dispatchFetch("http://warehouse/transport");

    expect(await response.json()).toMatchObject({
      ok: false,
      reason: expect.stringContaining("warehouse unavailable:"),
    });
  });
});
