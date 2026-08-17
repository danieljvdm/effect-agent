import { join } from "node:path";

import { Effect, Schema } from "effect";
import { build, type OutputFile } from "esbuild";
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
let invalidAnswerWorkerScript = "";
let runtimeFailureWorkerScript = "";
let legacySeedWorkerScript = "";

const executableWorkerScript = (outputFiles: ReadonlyArray<OutputFile> | undefined): string => {
  const output = outputFiles?.[0];
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
  return `${disabled}${output.text.replaceAll(/\bimport\s*\(/g, "__disabledDynamicImport(")}`;
};

const openRuntime = (script = workerScript): Miniflare =>
  new Miniflare({
    modules: true,
    script,
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
    workerScript = executableWorkerScript(bundled.outputFiles);

    const invalidAnswerBundle = await build({
      stdin: {
        contents: `
          import { makeDemoWorker } from "../src/worker.ts";
          export { CodeModeHostEntrypoint, WarehouseObject } from "../src/worker.ts";

          export default makeDemoWorker(() => ({ answer: 42 }));
        `,
        resolveDir: import.meta.dirname,
        sourcefile: "invalid-answer-worker.ts",
        loader: "ts",
      },
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      logLevel: "silent",
    });
    invalidAnswerWorkerScript = executableWorkerScript(invalidAnswerBundle.outputFiles);

    const runtimeFailureBundle = await build({
      stdin: {
        contents: `
          import { Agent } from "@effect-agent/core";
          import { Effect, Layer, Stream } from "effect";
          import { AiError, LanguageModel, Model } from "effect/unstable/ai";
          import { codeModeAgent } from "../src/agent.ts";
          import { makeDemoWorker } from "../src/worker.ts";
          export { CodeModeHostEntrypoint, WarehouseObject } from "../src/worker.ts";

          const failure = AiError.AiError.make({
            module: "code-mode-demo-test",
            method: "streamText",
            reason: AiError.UnknownError.make({ description: "expected model failure" }),
          });
          const failingModel = Model.make(
            "scripted",
            "failing-warehouse-analyst",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.fail(failure),
                streamText: () => Stream.fail(failure),
              }),
            ),
          );

          export default makeDemoWorker(
            undefined,
            Agent.withModel(codeModeAgent, failingModel),
          );
        `,
        resolveDir: import.meta.dirname,
        sourcefile: "runtime-failure-worker.ts",
        loader: "ts",
      },
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      logLevel: "silent",
    });
    runtimeFailureWorkerScript = executableWorkerScript(runtimeFailureBundle.outputFiles);

    const legacySeedBundle = await build({
      stdin: {
        contents: `
          import worker, { CodeModeHostEntrypoint } from "../src/worker.ts";
          import { WarehouseObject as CurrentWarehouseObject } from "../src/warehouse-object.ts";

          export { CodeModeHostEntrypoint };
          export class WarehouseObject extends CurrentWarehouseObject {
            async seedLegacy(): Promise<void> {
              this.ctx.storage.sql.exec(
                "CREATE TABLE IF NOT EXISTS invoice_summary (customer TEXT NOT NULL, region TEXT NOT NULL, revenue INTEGER NOT NULL, created_at TEXT NOT NULL)",
              );
              this.ctx.storage.sql.exec(
                "INSERT INTO invoice_summary (customer, region, revenue, created_at) VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
                "Stellar Freight", "emea", 48200, "2026-07-03",
                "Nimbus Analytics", "amer", 12800, "2026-07-11",
                "Copper Kettle Co", "amer", 730, "2026-07-15",
                "Harbor Lights Ltd", "apac", 9400, "2026-07-21",
                "Vertex Robotics", "emea", 21050, "2026-07-24",
              );
            }
          }

          export default worker;
        `,
        resolveDir: import.meta.dirname,
        sourcefile: "legacy-seed-worker.ts",
        loader: "ts",
      },
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      logLevel: "silent",
    });
    legacySeedWorkerScript = executableWorkerScript(legacySeedBundle.outputFiles);
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
    // Durable Object SQLite and returned the computed result. Atlas has two
    // sub-$10k invoices whose customer-level total crosses the strict threshold.
    expect(result.codeMode.result).toEqual({
      topCustomers: ["Stellar Freight", "Vertex Robotics", "Nimbus Analytics", "Atlas Components"],
      count: 4,
    });
    expect(result.codeMode.logs).toContain("matched 4 high-revenue customers");
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

  it("fails closed when completed output does not match the Answer Schema", async () => {
    const invalidRuntime = openRuntime(invalidAnswerWorkerScript);
    cleanups.push(() => invalidRuntime.dispose());
    const response = await invalidRuntime.dispatchFetch("http://demo/ask", {
      method: "POST",
      body: JSON.stringify({ question: "Return malformed terminal output" }),
    });

    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({
      error: "the completed agent output did not match the Answer Schema",
    });
  });

  it("maps expected Agent runtime failures into the typed demo failure before HTTP translation", async () => {
    const failingRuntime = openRuntime(runtimeFailureWorkerScript);
    cleanups.push(() => failingRuntime.dispose());
    const response = await failingRuntime.dispatchFetch("http://demo/ask", {
      method: "POST",
      body: JSON.stringify({ question: "Trigger the expected model failure" }),
    });

    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({ error: "the agent runtime failed" });
  });

  it("upgrades a pre-change five-row warehouse through idempotent versioned seed migrations", async () => {
    const legacyRuntime = openRuntime(legacySeedWorkerScript);
    cleanups.push(() => legacyRuntime.dispose());
    const warehouse = await legacyRuntime.getDurableObjectNamespace("WAREHOUSE");
    const rawStub = warehouse.get(warehouse.idFromName("legacy-acme"));
    const stub = rawStub as unknown as {
      seedLegacy: () => Promise<void>;
      listInvoices: (request: unknown) => Promise<unknown>;
    };
    await stub.seedLegacy();

    for (let attempt = 0; attempt < 2; attempt++) {
      const migrated = await Effect.runPromise(
        Schema.decodeUnknownEffect(WarehouseListOutcome)(await stub.listInvoices({})),
      );
      expect(migrated._tag).toBe("WarehouseInvoices");
      if (migrated._tag !== "WarehouseInvoices") throw new Error("expected invoices");
      expect(migrated.invoices).toHaveLength(8);
      expect(
        migrated.invoices.filter((invoice) => invoice.customer === "Boundary Foods"),
      ).toHaveLength(1);
      expect(
        migrated.invoices.filter((invoice) => invoice.customer === "Atlas Components"),
      ).toHaveLength(2);
    }
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
        { customer: "Boundary Foods" },
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

    const incomplete = await Effect.runPromise(
      Schema.decodeUnknownEffect(WarehouseListOutcome)(
        await stub.listInvoices({ minimumRevenue: 10_000, maximum: 2 }),
      ),
    );
    expect(incomplete).toMatchObject({
      _tag: "WarehouseQueryDenied",
      reason: "result-limit",
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
