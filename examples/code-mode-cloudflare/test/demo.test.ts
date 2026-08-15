import { join } from "node:path";

import { build } from "esbuild";
import { Miniflare, kCurrentWorker } from "miniflare";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";

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

interface AskResult {
  readonly answer: string;
  readonly codeMode: {
    readonly used: boolean;
    readonly tool: string;
    readonly executor: string;
    readonly calls: number;
    readonly program?: string;
    readonly result?: { readonly topCustomers: ReadonlyArray<string>; readonly count: number };
    readonly logs?: ReadonlyArray<string>;
  };
  readonly profile: string;
}

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
    const result = JSON.parse(rawBody) as AskResult;

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
    expect(result.codeMode.program).toContain("warehouse.query");

    // The evidence that matters: the isolated program queried the REAL
    // Durable Object SQLite and returned the computed result. The seed has
    // exactly three customers above $10k, highest revenue first.
    expect(result.codeMode.result).toEqual({
      topCustomers: ["Stellar Freight", "Vertex Robotics", "Nimbus Analytics"],
      count: 3,
    });
    expect(result.codeMode.logs).toContain("scanned 5 customers, kept 3");
  }, 120_000);

  it("denies a write-attempting program through the read-only allowlist", async () => {
    // The `?probe=write` program attempts an UPDATE inside the isolated
    // Dynamic Worker; the read-only allowlist denies it and the program
    // catches the typed WarehouseQueryDenied envelope.
    const response = await runtime.dispatchFetch("http://demo/ask?probe=write", {
      method: "POST",
      body: JSON.stringify({ question: "try to zero out revenue" }),
    });
    const rawBody = await response.text();
    expect(response.ok, `unexpected status ${response.status}: ${rawBody}`).toBe(true);
    const result = JSON.parse(rawBody) as {
      readonly codeMode: { readonly result?: { readonly writeDenied: boolean } };
    };
    expect(result.codeMode.result?.writeDenied).toBe(true);
  });

  it("rejects a CTE-prefixed write that a leading-keyword denylist would miss", async () => {
    // Direct-to-DO evidence that the allowlist is not bypassable by a
    // statement that merely does not START with a write keyword. The row
    // count is unchanged afterward.
    const warehouse = await runtime.getDurableObjectNamespace("WAREHOUSE");
    const stub = warehouse.get(warehouse.idFromName("acme")) as unknown as {
      query: (
        sql: string,
        parameters: ReadonlyArray<string | number | boolean | null>,
      ) => Promise<{
        readonly ok: boolean;
        readonly rows: ReadonlyArray<Record<string, unknown>>;
        readonly reason?: string;
      }>;
    };

    // Seed and confirm the baseline read works.
    const before = await stub.query("SELECT COUNT(*) AS n FROM invoice_summary", []);
    expect(before.ok).toBe(true);
    expect(before.rows[0]?.n).toBe(5);

    // A denylist keyed on the leading token would let this through; the
    // allowlist rejects it because DELETE appears anywhere in the statement.
    const bypass = await stub.query(
      "WITH doomed AS (SELECT customer FROM invoice_summary) DELETE FROM invoice_summary",
      [],
    );
    expect(bypass.ok).toBe(false);
    expect(bypass.reason).toMatch(/DELETE|read-only/i);

    const after = await stub.query("SELECT COUNT(*) AS n FROM invoice_summary", []);
    expect(after.ok).toBe(true);
    expect(after.rows[0]?.n).toBe(5);
  });
});
