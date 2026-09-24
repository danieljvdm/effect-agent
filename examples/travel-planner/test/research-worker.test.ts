import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect, Schema } from "effect";
import { SettlementFailureDiagnostic } from "effect-agent/records";
import { ThreadExport } from "effect-agent/thread-store";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import type { PlannerSettings } from "../src/domain.ts";
import { PlannerSnapshot, PlannerWorkerDetail } from "../src/domain.ts";
import { fixtureOwner } from "./fixtures/identity.ts";

const token = "research-worker-fixture";
const settings: PlannerSettings = { model: "gpt-6-astra", reasoningEffort: "high", fast: true };

const RpcExit = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  exit: Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
    Schema.Struct({ _tag: Schema.Literal("Failure"), cause: Schema.Unknown }),
  ]),
});

let runtime: Miniflare;
let directory: string;
let script: string;

const makeRuntime = () =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      bindings: { PLANNER_TOKEN: token },
      durableObjects: { ACCOUNT_THREADS: { className: "TravelPlannerThread", useSQLite: true } },
      r2Buckets: ["APP_BUILDS"],
      resourcePersistencePath: directory,
    }),
  );

beforeAll(async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/research-worker.ts")],
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    alias: { "@tanstack/react-start/server-entry": join(import.meta.dirname, "fixtures/start.ts") },
    banner: {
      js: 'import { createRequire } from "node:module"; const require = createRequire("/fixture.mjs");',
    },
    logLevel: "silent",
  });

  if (!bundle.outputFiles[0]) throw new Error("Missing fixture bundle");
  script = bundle.outputFiles[0].text;
  directory = await mkdtemp(join(tmpdir(), "travel-research-"));
  runtime = makeRuntime();
});
afterAll(async () => {
  await runtime?.dispose();
  if (directory) await rm(directory, { recursive: true, force: true });
});

const rpc = async (tag: string, payload: unknown, email = "research@example.com") => {
  const response = await runtime.dispatchFetch("http://planner/api/rpc", {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "x-test-email": email,
      "content-type": "application/ndjson",
    },
    body: `${JSON.stringify({ _tag: "Request", id: "1", tag, payload, headers: [] })}\n`,
  });

  expect(response.status).toBe(200);

  const result = Schema.decodeSync(Schema.fromJsonString(RpcExit))(
    (await response.text()).trim().split("\n")[0],
  ).exit;

  if (result._tag !== "Success") throw new Error(JSON.stringify(result.cause));

  return result.value;
};

const overview = async (email?: string) =>
  Schema.decodeUnknownSync(PlannerSnapshot)(
    await rpc("GetPlanner", { conversationId: "research" }, email),
  );

const snapshot = async (email?: string) => {
  const main = await overview(email);

  const read = async <A extends { readonly id: string; readonly sourceSequence?: number }>(
    worker: A,
  ) => ({
    ...worker,
    ...Schema.decodeUnknownSync(PlannerWorkerDetail)(
      await rpc(
        "GetPlannerWorker",
        {
          conversationId: "research",
          workerId: worker.id,
          sourceSequence: worker.sourceSequence,
        },
        email,
      ),
    ),
  });

  const workers = [...(main.scouts ?? []), ...(main.editor ? [main.editor] : [])];

  const details = await Effect.runPromise(
    Effect.forEach(workers, (worker) => Effect.promise(() => read(worker)), { concurrency: 3 }),
  );

  return {
    ...main,
    scouts: main.scouts?.map((scout) => ({
      ...scout,
      ...details.find((detail) => detail.id === scout.id),
    })),
    editor: main.editor
      ? { ...main.editor, ...details.find((detail) => detail.id === main.editor?.id) }
      : main.editor,
  };
};

const fixture = async (path: string, parameters: Record<string, string>, method = "GET") =>
  (
    await runtime.dispatchFetch(
      `http://planner/__research/${path}?${new URLSearchParams(parameters)}`,
      { method, headers: { authorization: `Bearer ${token}` } },
    )
  ).json();

const send = (message: string, email?: string) =>
  rpc(
    "SendMessage",
    {
      message,
      settings,
      requestId: crypto.randomUUID(),
      selectedTripId: null,
      conversationId: "research",
    },
    email,
  );

const until = async <A>(read: () => Promise<A>, matches: (value: A) => boolean) => {
  const deadline = performance.now() + 50_000;
  let value = await read();

  while (true) {
    if (matches(value)) return value;
    if (performance.now() >= deadline) break;
    await Effect.runPromise(Effect.sleep("50 millis"));
    value = await read();
  }

  const diagnostic = Schema.is(PlannerSnapshot)(value)
    ? {
        pending: value.pending,
        pendingSubmissionIds: value.pendingSubmissionIds,
        editor: value.editor && { id: value.editor.id, state: value.editor.state },
        scouts: value.scouts?.map(({ id, state, task }) => ({ id, state, task })),
        messages: value.messages.map(({ role, text }) => ({ role, text })),
      }
    : value;

  throw new Error(`Fixture did not settle: ${JSON.stringify(diagnostic).slice(0, 3_000)}`);
};

it("does not treat the retained request on a worker update or completion as fresh delegation authority", async () => {
  const email = "report-denial@example.com";
  const thread = `${fixtureOwner(email)}--research`;

  await send("start report denial", email);

  const active = await until(
    () => snapshot(email),
    (state) =>
      state.pending === 0 &&
      state.scouts?.[0]?.state === "active" &&
      state.activity.some(({ text }) => text.includes("AgentToolAuthorizationDenied")),
  );

  expect(active.scouts).toHaveLength(1);
  await fixture("gate", { name: "Report denial" }, "POST");

  const completed = await until(
    () => snapshot(email),
    (state) => state.pending === 0 && state.scouts?.[0]?.state === "idle",
  );

  const parent = Schema.decodeUnknownSync(ThreadExport)(await fixture("journal", { thread }));

  const denied = parent.records.filter(
    ({ record }) =>
      record.payload._tag === "SubmissionSettled" &&
      record.payload.outcome === "failed" &&
      Schema.decodeOption(SettlementFailureDiagnostic)(record.payload.result).pipe(
        (failure) =>
          failure._tag === "Some" && failure.value.errorTag === "AgentToolAuthorizationDenied",
      ),
  );

  expect(denied).toHaveLength(2);
  expect(completed.scouts).toHaveLength(1);
}, 90_000);
