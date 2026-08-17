import { join } from "node:path";

import { Effect, Schema } from "effect";
import { build } from "esbuild";
import { Miniflare, kCurrentWorker } from "miniflare";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";

import { AskResult, WarehouseListOutcome } from "../src/wire.ts";

/**
 * The demo end to end in a real workerd runtime (programmatic Miniflare): a
 * Code Mode Agent answers a question by running one JavaScript program in an
 * isolated Cloudflare Dynamic Worker, and that program queries a real
 * SQLite-backed Durable Object warehouse through the brokered read-only SQL
 * tool. The scripted profile makes the run deterministic and credential-free;
 * the same Worker runs the live OpenAI profile when the secret is set.
 */

const workerEntry = join(import.meta.dirname, "..", "src", "worker.ts");

let workerScript = "";

const openRuntime = (): Miniflare =>
  new Miniflare({
    modules: true,
    script: workerScript,
    modulesRoot: "/",
    compatibilityDate: "2025-05-01",
    compatibilityFlags: ["nodejs_compat", "experimental"],
    durableObjects: {
      WAREHOUSE: { className: "WarehouseObject", useSQLite: true },
    },
    workerLoaders: { LOADER: {} },
    serviceBindings: {
      CODE_MODE_HOST: { name: kCurrentWorker, entrypoint: "CodeModeHostEntrypoint" },
    },
  });

describe("Code Mode over a SQLite Durable Object warehouse", () => {
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
    // A transitive dependency ships a dead-code `import(<computed>)` that
    // single-script Miniflare's static module locator rejects even though it
    // never runs on the demo's path. Neutralize the dynamic-import
    // expressions in the HOST bundle (the fixed dynamic-worker harness uses a
    // static program import, so it carries none), mirroring the repository's
    // Miniflare restart lane.
    const disabled =
      'const __disabledDynamicImport = () => Promise.reject(new Error("dynamic import is disabled in the demo bundle"));\n';
    workerScript = `${disabled}${output.text.replaceAll(/\bimport\s*\(/g, "__disabledDynamicImport(")}`;
    runtime = openRuntime();
    cleanups.push(() => runtime.dispose());
  }, 120_000);

  afterAll(async () => {
    for (const cleanup of cleanups.reverse()) {
      await cleanup();
    }
  });

  it("runs a generated program in a Dynamic Worker that queries the SQLite DO warehouse", async () => {
    const response = await runtime.dispatchFetch("http://demo/ask", {
      method: "POST",
      body: JSON.stringify({ question: "Which customers have more than $10,000 in revenue?" }),
    });
    const rawBody = await response.text();
    expect(response.ok, `unexpected status ${response.status}: ${rawBody}`).toBe(true);
    const result = await Effect.runPromise(
      Schema.decodeUnknownEffect(AskResult)(JSON.parse(rawBody)),
    );

    // The deterministic profile ran (no credential in the test env).
    expect(result.profile).toBe("scripted");
    // The final model answer came back.
    expect(result.answer).toContain("Stellar Freight");

    // Code Mode usage is surfaced explicitly: the tool, the isolated executor,
    // and the actual JavaScript the program was.
    expect(result.codeMode.used).toBe(true);
    expect(result.codeMode.tool).toBe("run_javascript");
    expect(result.codeMode.executor).toBe("cloudflare-dynamic-worker");
    expect(result.codeMode.calls).toBe(1);
    expect(result.codeMode.program).toContain("warehouse.listInvoices");

    // The evidence that matters: the isolated program queried the REAL
    // Durable Object SQLite and returned the computed result. The seed has
    // exactly three customers above $10k, highest revenue first.
    expect(result.codeMode.result).toEqual({
      topCustomers: ["Stellar Freight", "Vertex Robotics", "Nimbus Analytics"],
      count: 3,
    });
    expect(result.codeMode.logs).toContain("matched 3 high-revenue customers");
  }, 120_000);

  it("rejects malformed and out-of-Schema HTTP request bodies", async () => {
    const malformed = await runtime.dispatchFetch("http://demo/ask", {
      method: "POST",
      body: "{",
    });
    expect(malformed.status).toBe(400);

    const wrongShape = await runtime.dispatchFetch("http://demo/ask", {
      method: "POST",
      body: JSON.stringify({ question: 42 }),
    });
    expect(wrongShape.status).toBe(400);
  });

  it("exposes only a curated Schema-decoded invoice operation over DO RPC", async () => {
    const warehouse = await runtime.getDurableObjectNamespace("WAREHOUSE");
    const rawStub = warehouse.get(warehouse.idFromName("acme"));
    const stub = rawStub as unknown as {
      listInvoices: (request: unknown) => Promise<unknown>;
    };

    const listed = await Effect.runPromise(
      Schema.decodeUnknownEffect(WarehouseListOutcome)(
        await stub.listInvoices({ minimumRevenue: 10_000 }),
      ),
    );
    expect(listed).toMatchObject({
      _tag: "WarehouseInvoices",
      invoices: [
        { customer: "Stellar Freight" },
        { customer: "Vertex Robotics" },
        { customer: "Nimbus Analytics" },
      ],
    });

    const inclusive = await Effect.runPromise(
      Schema.decodeUnknownEffect(WarehouseListOutcome)(
        await stub.listInvoices({ minimumRevenue: 12_800 }),
      ),
    );
    expect(inclusive._tag).toBe("WarehouseInvoices");
    if (inclusive._tag !== "WarehouseInvoices") throw new Error("expected invoices");
    expect(
      inclusive.invoices.some(
        (invoice) => invoice.customer === "Nimbus Analytics" && invoice.revenue === 12_800,
      ),
    ).toBe(true);

    const inclusiveInRegion = await Effect.runPromise(
      Schema.decodeUnknownEffect(WarehouseListOutcome)(
        await stub.listInvoices({ minimumRevenue: 12_800, region: "amer" }),
      ),
    );
    expect(inclusiveInRegion._tag).toBe("WarehouseInvoices");
    if (inclusiveInRegion._tag !== "WarehouseInvoices") throw new Error("expected invoices");
    expect(inclusiveInRegion.invoices).toHaveLength(1);
    expect(inclusiveInRegion.invoices[0]?.customer).toBe("Nimbus Analytics");

    const denied = await Effect.runPromise(
      Schema.decodeUnknownEffect(WarehouseListOutcome)(
        await stub.listInvoices({ minimumRevenue: "UPDATE invoice_summary" }),
      ),
    );
    expect(denied).toMatchObject({
      _tag: "WarehouseQueryDenied",
      reason: "invalid-request",
    });

    const legacy = rawStub as unknown as {
      query: (sql: string) => Promise<unknown>;
    };
    let legacyFailure: unknown;
    try {
      await legacy.query("SELECT * FROM invoice_summary");
    } catch (error) {
      legacyFailure = error;
    }
    expect(String(legacyFailure)).toBe(
      'TypeError: The RPC receiver does not implement the method "query".',
    );
  });
});
