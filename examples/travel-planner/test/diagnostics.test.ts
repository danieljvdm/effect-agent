import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { diagnosticDetail, RecordedDiagnostics } from "../src/server/diagnostics.ts";

let directory: string;
let script: string;
let runtime: Miniflare;

const start = () =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { DIAGNOSTICS: { className: "Diagnostics", useSQLite: true } },
      resourcePersistencePath: directory,
    }),
  );

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "planner-diagnostics-"));

  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/diagnostics-worker.ts")],
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

  if (!output) throw new Error("Missing fixture bundle");
  script = output.text;
  runtime = start();
});

afterAll(async () => {
  await runtime?.dispose();
  if (directory) await rm(directory, { recursive: true, force: true });
});

const Reply = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
  Schema.Struct({ _tag: Schema.Literal("Failure"), error: Schema.String }),
]);

const call = async (thread: string, input: unknown) => {
  const response = await runtime.dispatchFetch(`http://fixture/${thread}`, {
    method: "POST",
    body: JSON.stringify(input),
  });

  return Schema.decodeUnknownSync(Reply)(await response.json());
};

const list = async (thread: string) => {
  const reply = await call(thread, { action: "list" });

  if (reply._tag === "Failure") throw new Error(reply.error);

  return Schema.decodeUnknownSync(RecordedDiagnostics)(reply.value);
};

it("retains redacted error causes in per-thread SQLite across restart", async () => {
  expect(await call("alice-trip", { action: "append" })).toMatchObject({ _tag: "Success" });
  await runtime.dispose();
  runtime = start();
  const diagnostics = await list("alice-trip");

  expect(diagnostics).toHaveLength(1);
  expect(diagnostics[0]).toMatchObject({
    version: 1,
    toolCallId: "call-test",
    runId: "run-test",
    truncated: false,
  });
  const text = diagnostics[0]?.text;

  expect(text).toContain("Access denied");
  expect(text).toContain("403");
  expect(text).toContain("req-test");
  expect(text).toContain("ray-test");
  expect(text).toContain("stack");
  expect(text).not.toContain("PRIVATE");
  const raw = await call("alice-trip", { action: "raw" });

  expect(JSON.stringify(raw)).not.toContain("PRIVATE");
  expect(await list("bob-trip")).toEqual([]);
  expect(await list("alice-other-trip")).toEqual([]);
});

it("does not replace the original outcome when recording fails before or after its write", async () => {
  for (const mode of ["failure", "defect", "interrupt"]) {
    for (const point of ["append:before", "append:after"]) {
      const thread = `${mode}-${point}`;

      expect(await call(thread, { action: "append", point, mode })).toEqual({
        _tag: "Success",
        value: "original outcome unchanged",
      });
      expect(await list(thread)).toHaveLength(point === "append:after" ? 1 : 0);
    }
  }
  for (const point of ["schema:before", "schema:after"]) {
    expect(await call(point, { action: "list", point })).toMatchObject({ _tag: "Failure" });
    expect(await list(point)).toEqual([]);
  }
  await call("corrupt", { action: "corrupt" });
  const before = await call("corrupt", { action: "raw" });

  expect(await call("corrupt", { action: "list" })).toMatchObject({ _tag: "Failure" });
  expect(await call("corrupt", { action: "raw" })).toEqual(before);
});

it("retains safe nested metadata without evaluating getters or exposing reasoning and credentials", () => {
  let evaluated = false;

  const error = new Error("provider failed", {
    cause: { type: "reasoning", text: "PRIVATE REASONING" },
  });

  Object.defineProperty(error, "dangerous", {
    get() {
      evaluated = true;
      throw new Error("getter");
    },
  });

  const detail = diagnosticDetail({
    error,
    response: {
      headers: { "x-request-id": "req-1", cookie: "PRIVATE COOKIE" },
      body: '{"token":"PRIVATE TOKEN","error":"timeout"}',
      sessionToken: "PRIVATE SESSION",
      assertion: "PRIVATE ASSERTION",
      url: "https://example.com/page?q=hotel&X-Amz-Security-Token=PRIVATE&Policy=PRIVATE",
      usage: { inputTokens: 120 },
    },
  });

  expect(detail.text).toContain("provider failed");
  expect(detail.text).toContain("req-1");
  expect(detail.text).toContain("timeout");
  expect(detail.text).toContain("q=hotel");
  expect(detail.text).toContain('"inputTokens": 120');
  expect(detail.text).not.toContain("PRIVATE");
  expect(evaluated).toBe(false);
  expect(diagnosticDetail({ body: "x".repeat(100_000) })).toMatchObject({ truncated: true });
  expect(diagnosticDetail({ body: "x".repeat(100_000) }).text.length).toBeLessThanOrEqual(65_536);
});
