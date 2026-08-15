import { join } from "node:path";

import { build } from "esbuild";
import { Miniflare, kCurrentWorker } from "miniflare";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";

/**
 * The demo end to end in a real workerd runtime (programmatic Miniflare): a
 * Code Mode Agent answers a question by running one JavaScript program in an
 * isolated Cloudflare Dynamic Worker, and that program queries a real
 * SQLite-backed Durable Object invoice database through the brokered read-only SQL
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
      AGENTS: { className: "InvoiceAgentConversationObject", useSQLite: true },
    },
    d1Databases: { DB: "invoices-test" },
    workerLoaders: { LOADER: {} },
    serviceBindings: {
      CODE_MODE_HOST: { name: kCurrentWorker, entrypoint: "CodeModeHostEntrypoint" },
    },
  });

interface AskResult {
  readonly answer: string;
  readonly conversationId: string;
  readonly outcome: string;
  readonly records: ReadonlyArray<string>;
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

describe("durable Code Mode: Conversation Object runs over a D1 database", () => {
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

  it("runs a generated program in a Dynamic Worker that queries the D1 database", async () => {
    const response = await runtime.dispatchFetch("http://demo/ask", {
      method: "POST",
      body: JSON.stringify({ question: "Which customers have more than $10,000 in revenue?" }),
    });
    const rawBody = await response.text();
    expect(response.ok, `unexpected status ${response.status}: ${rawBody}`).toBe(true);
    const result = JSON.parse(rawBody) as AskResult;

    // The deterministic profile ran (no credential in the test env).
    expect(result.outcome, rawBody).toBe("completed");
    expect(result.profile).toBe("scripted");
    // The final model answer came back.
    expect(result.answer).toContain("Stellar Freight");

    // Code Mode usage is surfaced explicitly: the tool, the isolated executor,
    // and the actual JavaScript the program was.
    expect(result.codeMode.used).toBe(true);
    expect(result.codeMode.tool).toBe("run_javascript");
    expect(result.codeMode.executor).toBe("cloudflare-dynamic-worker");
    expect(result.codeMode.calls, rawBody).toBe(1);
    expect(result.codeMode.program).toContain("invoices.query");

    // The evidence that matters: the isolated program queried the REAL
    // Durable Object SQLite and returned the computed result. The seed has
    // exactly three customers above $10k, highest revenue first.
    expect(result.codeMode.result).toEqual({
      topCustomers: ["Stellar Freight", "Vertex Robotics", "Nimbus Analytics"],
      count: 3,
    });
    expect(result.codeMode.logs).toContain("scanned 5 customers, kept 3");

    // Durable evidence: the run settled through the Conversation Object's
    // append-only canonical log — tool prepare/settle records and the
    // settlement itself are all durable facts, not in-memory state.
    expect(result.outcome).toBe("completed");
    expect(result.records).toContain("ModelResponseRecorded");
    expect(result.records).toContain("ToolCallSettled");
    expect(result.records).toContain("SubmissionSettled");
  }, 120_000);

  it("denies a write-attempting program through the read-only allowlist", async () => {
    // The `?probe=write` program attempts an UPDATE inside the isolated
    // Dynamic Worker; the read-only allowlist denies it and the program
    // catches the typed InvoiceQueryDenied envelope.
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

  it("streams the canonical log as NDJSON while the durable run progresses", async () => {
    const response = await runtime.dispatchFetch("http://demo/ask?stream=1", {
      method: "POST",
      body: JSON.stringify({ question: "Which customers have more than $10,000 in revenue?" }),
    });
    expect(response.ok).toBe(true);
    expect(response.headers.get("content-type")).toContain("ndjson");

    // Incremental delivery: read the body with a reader and require at least
    // one non-final record line BEFORE the terminal `done` line arrives — a
    // buffered single-shot response would fail this.
    const reader = (response.body as unknown as ReadableStream<Uint8Array>).getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    let sawRecordBeforeDone = false;
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      buffered += decoder.decode(chunk.value, { stream: true });
      const complete = buffered.split("\n").filter((line) => line.trim().length > 0);
      const hasDone = complete.some((line) => line.includes('"done":true'));
      const hasRecord = complete.some((line) => line.includes('"record":'));
      if (hasRecord && !hasDone) sawRecordBeforeDone = true;
      if (hasDone) break;
    }
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      buffered += decoder.decode(chunk.value, { stream: true });
    }
    expect(sawRecordBeforeDone).toBe(true);
    const lines = buffered
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    // One line per durable record as it committed…
    const tags = lines.filter((line) => typeof line.record === "string").map((l) => l.record);
    expect(tags).toContain("ModelResponseRecorded");
    expect(tags).toContain("ToolCallSettled");
    expect(tags).toContain("SubmissionSettled");
    // …then the full receipt as the final line.
    const final = lines[lines.length - 1];
    expect(final.done).toBe(true);
    expect(final.answer).toContain("Stellar Freight");
    expect(final.codeMode.program).toContain("invoices.query");
  }, 120_000);

  it("rejects a CTE-prefixed write that a leading-keyword denylist would miss", async () => {
    // `WITH … DELETE` does not START with a write keyword, so a naive
    // denylist would let it through. The `?probe=cte` program attempts it in
    // the isolated Dynamic Worker; the allowlist rejects it, and the D1 rows
    // are provably unchanged afterward.
    const response = await runtime.dispatchFetch("http://demo/ask?probe=cte", {
      method: "POST",
      body: JSON.stringify({ question: "try to delete everything" }),
    });
    const rawBody = await response.text();
    expect(response.ok, `unexpected status ${response.status}: ${rawBody}`).toBe(true);
    const result = JSON.parse(rawBody) as {
      readonly codeMode: { readonly result?: { readonly writeDenied: boolean } };
    };
    expect(result.codeMode.result?.writeDenied).toBe(true);

    // The data survived: count straight from the D1 database.
    const db = await runtime.getD1Database("DB");
    const count = await db.prepare("SELECT COUNT(*) AS n FROM invoice_summary").first<{
      n: number;
    }>();
    expect(count?.n).toBe(5);
  });
});
