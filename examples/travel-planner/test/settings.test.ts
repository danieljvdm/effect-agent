import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { Effect } from "effect";
import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, expectTypeOf, it } from "vite-plus/test";

import type { PlannerError } from "../src/domain.ts";
import { PlannerSettings, PlannerSnapshot, defaultPlannerSettings } from "../src/domain.ts";
import type { PlannerSettingsStore } from "../src/server/settings.ts";
import { ownerEmail } from "./fixtures/identity.ts";

const token = "preference-test-token";
const astra: PlannerSettings = { model: "gpt-6-astra", reasoningEffort: "high", fast: true };
const luna: PlannerSettings = { model: "gpt-5.6-luna", reasoningEffort: "none", fast: false };

const RpcExit = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  exit: Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
    Schema.Struct({ _tag: Schema.Literal("Failure"), cause: Schema.Unknown }),
  ]),
});

let directory: string;
let worker: string;
let runtime: Miniflare;

const makeRuntime = () =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: worker,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      bindings: { PLANNER_TOKEN: token },
      r2Buckets: ["APP_BUILDS"],
      durableObjects: { ACCOUNT_THREADS: { className: "TravelPlannerThread", useSQLite: true } },
      resourcePersistencePath: directory,
    }),
  );

beforeAll(async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/settings-worker.ts")],
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

  const output = bundle.outputFiles[0];

  if (output === undefined) throw new Error("No worker bundle");
  worker = output.text;
  directory = await mkdtemp(join(tmpdir(), "planner-settings-test-"));
  runtime = makeRuntime();
});

afterAll(async () => {
  await runtime?.dispose();
  if (directory !== undefined) await rm(directory, { recursive: true, force: true });
});

const headers = (email: string) => ({ authorization: `Bearer ${token}`, "x-test-email": email });

const rpcExit = async (tag: string, payload?: unknown, email = ownerEmail) => {
  const response = await runtime.dispatchFetch("http://planner/api/rpc", {
    method: "POST",
    headers: { ...headers(email), "content-type": "application/ndjson" },
    body: `${JSON.stringify({ _tag: "Request", id: "1", tag, payload: payload ?? null, headers: [] })}\n`,
  });

  const body = await response.text();

  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");

  return Schema.decodeUnknownSync(Schema.fromJsonString(RpcExit))(body.trim().split("\n")[0]).exit;
};

const rpc = async (tag: string, payload?: unknown, email = ownerEmail) => {
  const result = await rpcExit(tag, payload, email);

  if (result._tag === "Failure") throw new Error(JSON.stringify(result.cause));

  return result.value;
};

const get = async (email = ownerEmail) =>
  Schema.decodeUnknownSync(PlannerSettings)(await rpc("GetPlannerSettings", undefined, email));

const save = async (settings: PlannerSettings, email = ownerEmail) =>
  Schema.decodeUnknownSync(PlannerSettings)(await rpc("SavePlannerSettings", settings, email));

const arm = async (point: string, mode = "failure") => {
  const response = await runtime.dispatchFetch(
    `http://planner/__test/preferences?point=${point}&mode=${mode}`,
    { headers: headers(ownerEmail) },
  );

  await response.arrayBuffer();
  expect(response.status).toBe(200);
};

const raw = async (email: string, value?: string) => {
  const response = await runtime.dispatchFetch("http://planner/__test/preferences", {
    method: value === undefined ? "GET" : "PUT",
    headers: headers(email),
    ...(value === undefined ? {} : { body: value }),
  });

  return Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ value: Schema.String })))(
    await response.json(),
  );
};

it("persists one private account preference across devices and restarts without changing trips", async () => {
  const guest = "friend@example.com";

  expect(await get()).toEqual(defaultPlannerSettings);
  expect(await raw(ownerEmail)).toEqual([]);
  expect(await get(guest)).toEqual(defaultPlannerSettings);
  await rpc("SaveTrip", {
    conversationId: "existing-trip",
    tripId: null,
    expectedRevision: null,
    title: "Retained trip",
    destination: "Lisbon",
    summary: "A stored draft",
    startDate: null,
    endDate: null,
    travelers: 1,
    days: [],
    notes: ["Preserve this trip"],
  });

  const before = Schema.decodeUnknownSync(PlannerSnapshot)(
    await rpc("GetPlanner", { conversationId: null }),
  );

  expect(await save(astra)).toEqual(astra);
  expect(await get()).toEqual(astra);
  expect(await get(guest)).toEqual(defaultPlannerSettings);
  expect(await save(luna, guest)).toEqual(luna);
  await runtime.dispose();
  runtime = makeRuntime();
  expect(await get()).toEqual(astra);
  expect(await get(guest)).toEqual(luna);
  expect(await rpc("GetPlanner", { conversationId: null })).toEqual(before);
  // A later device's completed update becomes the account preference everywhere.
  await save(luna);
  expect(await get()).toEqual(luna);
}, 30_000);

it("retains the old row before failed writes and reveals committed writes after a lost reply", async () => {
  const email = "failure@example.com";

  await save(luna, email);
  for (const mode of ["failure", "defect", "interrupt"]) {
    await arm("save:before", mode);
    expect((await rpcExit("SavePlannerSettings", astra, email))._tag).toBe("Failure");
    expect(await get(email)).toEqual(luna);
  }
  await arm("save:after");
  expect((await rpcExit("SavePlannerSettings", astra, email))._tag).toBe("Failure");
  expect(await get(email)).toEqual(astra);
  expect(await save(astra, email)).toEqual(astra);
  expect(await raw(email)).toHaveLength(1);
}, 30_000);

it("fails closed on unsupported or malformed stored preferences without overwriting them", async () => {
  const email = "corrupt@example.com";

  await get(email);
  for (const value of [
    JSON.stringify({ version: 99, settings: astra }),
    JSON.stringify({
      version: 1,
      settings: { model: "gpt-6-astra", reasoningEffort: "none", fast: true },
    }),
    "not-json-PRIVATE_SENTINEL",
  ]) {
    await raw(email, value);
    for (const result of [
      await rpcExit("GetPlannerSettings", undefined, email),
      await rpcExit("SavePlannerSettings", luna, email),
    ]) {
      expect(result._tag).toBe("Failure");
      expect(JSON.stringify(result)).toContain('"code":"storage"');
      expect(JSON.stringify(result)).not.toContain("PRIVATE_SENTINEL");
    }
    expect(await raw(email)).toEqual([{ value }]);
  }
}, 30_000);

it("rejects unauthenticated and invalid updates and keeps storage failures typed", async () => {
  const response = await runtime.dispatchFetch("http://planner/api/rpc", { method: "POST" });

  await response.arrayBuffer();
  expect(response.status).toBe(401);
  const before = await get();

  expect(
    (
      await rpcExit("SavePlannerSettings", {
        model: "gpt-6-astra",
        reasoningEffort: "none",
        fast: false,
      })
    )._tag,
  ).toBe("Failure");
  expect(await get()).toEqual(before);
  expectTypeOf<
    Effect.Error<PlannerSettingsStore["Service"]["get"]>
  >().toEqualTypeOf<PlannerError>();
  expectTypeOf<
    Effect.Services<ReturnType<PlannerSettingsStore["Service"]["save"]>>
  >().toEqualTypeOf<never>();
}, 30_000);
