import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ThreadExport } from "@effect-agent/thread/ThreadStore";
import { Effect, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { AppFile, PlannerSnapshot, Trip, TripApp, type PlannerSettings } from "../src/domain.ts";
import { ownerEmail } from "./fixtures/identity.ts";

const token = "editor-worker-fixture";
const member = "editor-member@example.com";
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
      bindings: { PLANNER_TOKEN: token, APP_DOMAIN: "effect-agent.com" },
      durableObjects: { ACCOUNT_THREADS: { className: "TravelPlannerThread", useSQLite: true } },
      r2Buckets: ["APP_BUILDS"],
      workflows: { SITE_BUILD: { name: "editor-build", className: "FixtureBuild" } },
      resourcePersistencePath: directory,
    }),
  );

beforeAll(async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/editor-worker.ts")],
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

  if (!bundle.outputFiles[0]) throw new Error("Missing editor fixture bundle");
  script = bundle.outputFiles[0].text;
  directory = await mkdtemp(join(tmpdir(), "travel-editor-"));
  runtime = makeRuntime();
});
afterAll(async () => {
  await runtime?.dispose();
  if (directory) await rm(directory, { recursive: true, force: true });
});

const rpc = async (tag: string, payload: unknown, email = member) => {
  const response = await runtime.dispatchFetch("http://planner/api/rpc", {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "x-test-email": email,
      "content-type": "application/ndjson",
    },
    body: `${JSON.stringify({ _tag: "Request", id: "1", tag, payload, headers: [] })}\n`,
  });

  const text = await response.text();

  expect(response.status).toBe(200);

  const result = Schema.decodeUnknownSync(Schema.fromJsonString(RpcExit))(
    text.trim().split("\n")[0],
  ).exit;

  if (result._tag !== "Success") throw new Error(JSON.stringify(result.cause));

  return result.value;
};

const fixture = async (path: string, params: Record<string, string>, method = "GET") => {
  const response = await runtime.dispatchFetch(
    `http://planner/__editor/${path}?${new URLSearchParams(params)}`,
    { method, headers: { authorization: `Bearer ${token}` } },
  );

  if (!response.ok) throw new Error(await response.text());

  return response.json();
};

const snapshot = async (conversationId: string, email = member) =>
  Schema.decodeUnknownSync(PlannerSnapshot)(await rpc("GetPlanner", { conversationId }, email));

const journal = async (thread: string) =>
  Schema.decodeUnknownSync(ThreadExport)(await fixture("journal", { thread }));

const gate = async (name: string, open = false) =>
  Schema.decodeUnknownSync(Schema.Struct({ entered: Schema.Boolean }))(
    await fixture("gate", { name }, open ? "POST" : "GET"),
  );

const until = async <A>(
  read: () => Promise<A>,
  matches: (value: A) => boolean,
  label: string,
  attempts = 240,
) => {
  let value = await read();

  for (let attempt = 0; attempt < attempts; attempt++) {
    if (matches(value)) return value;
    await Effect.runPromise(Effect.sleep("50 millis"));
    value = await read();
  }
  throw new Error(`${label}: ${JSON.stringify(value)}`);
};

const save = async (conversationId: string, email = member) =>
  Schema.decodeUnknownSync(Trip)(
    await rpc(
      "SaveTrip",
      {
        conversationId,
        tripId: null,
        expectedRevision: null,
        title: "Tahoe cabin getaway",
        destination: "Lake Tahoe",
        summary: "A saved fixture trip",
        startDate: null,
        endDate: null,
        travelers: 2,
        days: [],
        notes: [`Private notes for ${email}`],
        places: [],
      },
      email,
    ),
  );

const send = (conversationId: string, tripId: string, message: string, email = member) =>
  rpc(
    "SendMessage",
    { requestId: crypto.randomUUID(), conversationId, selectedTripId: tripId, message, settings },
    email,
  );

const files = async (app: TripApp) =>
  Schema.decodeUnknownSync(Schema.Array(AppFile))(
    await fixture("object", { key: `fixture-source/${app.repoName}/${app.sourceCommit}.json` }),
  );

const appOf = (state: PlannerSnapshot) => {
  if (!state.app) throw new Error(`Missing app: ${JSON.stringify(state)}`);

  return state.app;
};

it("keeps planning available while a scoped durable editor edits source, accepts follow-ups, and resumes after restart", async () => {
  const conversation = "member-editor";
  const other = await save("admin-editor", ownerEmail);

  const adminApp = Schema.decodeUnknownSync(TripApp)(
    await rpc("CreateTripApp", { tripId: other.id }, ownerEmail),
  );

  const adminFiles = await files(adminApp);
  const otherMember = await save("other-member-trip");

  const otherApp = Schema.decodeUnknownSync(TripApp)(
    await rpc("CreateTripApp", { tripId: otherMember.id }),
  );

  const otherFiles = await files(otherApp);
  const trip = await save(conversation);

  expect(
    await send(
      conversation,
      trip.id,
      `start-editor hold:initial parent:initial reject:${otherMember.id}`,
    ),
  ).toEqual({ accepted: true });
  await until(
    () => gate("parent-initial"),
    (value) => value.entered,
    "Parent did not delegate and reach its gate",
  );
  await until(
    () => gate("child-initial"),
    (value) => value.entered,
    "Child did not reach its gate",
  );
  const pending = await snapshot(conversation);

  expect(pending.pending).toBeGreaterThan(0);
  expect(pending.editor?.state).toBe("active");
  const editorId = pending.editor?.id;

  if (!editorId) throw new Error("Expected native worker identity");
  expect(await send(conversation, trip.id, "main input while parent pending")).toEqual({
    accepted: true,
  });
  const queued = await snapshot(conversation);

  expect(queued.queuedMessages).toHaveLength(1);
  expect(queued.queuedMessages?.[0]?.text).toBe("main input while parent pending");
  expect(
    queued.messages.some((message) => message.text === "main input while parent pending"),
  ).toBe(false);
  await gate("parent-initial", true);

  const available = await until(
    () => snapshot(conversation),
    (state) => state.pending === 0 && state.editor?.state === "active",
    "Main planner remained blocked by its editor",
  );

  expect(
    available.messages.some(
      (message) => message.role === "user" && message.text === "main input while parent pending",
    ),
  ).toBe(true);
  expect(available.queuedMessages).toEqual([]);
  expect(
    available.messages.find(
      (message) => message.role === "user" && message.text === "main input while parent pending",
    )?.requestId,
  ).toBe(queued.queuedMessages?.[0]?.requestId);
  expect(await send(conversation, trip.id, "main input while editor active")).toEqual({
    accepted: true,
  });

  const responded = await until(
    () => snapshot(conversation),
    (state) =>
      state.pending === 0 &&
      state.messages.some(
        (message) =>
          message.role === "assistant" && message.text.includes("main input while editor active"),
      ),
    "Main planner did not answer while editor was active",
  );

  expect(responded.editor?.id).toBe(editorId);
  expect(responded.editor?.state).toBe("active");
  expect(
    await send(conversation, trip.id, "follow-editor hold:followup second edit after restart"),
  ).toEqual({ accepted: true });
  await until(
    () => snapshot(conversation),
    (state) =>
      state.pending === 0 && state.editor?.task.includes("second edit after restart") === true,
    "Follow-up was not admitted",
  );
  await gate("child-initial", true);
  await until(
    () => gate("child-followup"),
    (value) => value.entered,
    "Follow-up did not reach the existing editor",
  );
  const active = await snapshot(conversation);

  expect(active.editor?.id).toBe(editorId);
  expect(active.editor?.state).toBe("active");
  // Workerd and every Object are destroyed while the follow-up model is still suspended.
  await runtime.dispose();
  runtime = makeRuntime();
  const reloaded = await snapshot(conversation);

  expect(reloaded.editor?.id).toBe(editorId);
  await gate("child-followup", true);

  const completed = await until(
    () => snapshot(conversation),
    (state) =>
      state.editor?.state === "idle" &&
      state.pending === 0 &&
      state.app !== null &&
      state.app !== undefined,
    "Editor did not recover and finish",
    1000,
  );

  const app = appOf(completed);

  expect((await files(app)).find((file) => file.path === "editor-change.txt")?.content).toBe(
    "hold:followup second edit after restart",
  );
  expect(app.tripId).toBe(trip.id);
  expect(app.id).not.toBe(adminApp.id);
  expect(await files(adminApp)).toEqual(adminFiles);
  expect(await files(otherApp)).toEqual(otherFiles);
  const admin = await snapshot("admin-editor", ownerEmail);

  expect(admin.editor ?? null).toBeNull();
  expect(admin.trips.map((value) => value.id)).not.toContain(trip.id);

  const child = await journal(editorId);

  const childInputs = child.records.flatMap(({ record }) =>
    record.payload._tag === "UserInputRecorded" ? [record.payload.input] : [],
  );

  const Captured = Schema.Struct({
    tripId: Schema.String,
    sourceThreadId: Schema.String,
    settings: Schema.Unknown,
    message: Schema.String,
  });

  const captured = childInputs.map((value) => Schema.decodeUnknownSync(Captured)(value));

  expect(captured.length).toBeGreaterThanOrEqual(2);
  expect(captured.every((value) => value.tripId === trip.id)).toBe(true);
  expect(
    captured.every((value) => JSON.stringify(value.settings) === JSON.stringify(settings)),
  ).toBe(true);
  const sourceThread = captured[0]?.sourceThreadId;

  if (!sourceThread) throw new Error("Missing captured source");
  expect(sourceThread).toMatch(/^account-[a-f0-9-]{36}--member-editor$/);
  expect(sourceThread).not.toBe(editorId);
  const parent = await journal(sourceThread);

  const parentCalls = parent.records.flatMap(({ record }) =>
    record.payload._tag === "ToolCallSettled" ? [record.payload] : [],
  );

  const childCalls = child.records.flatMap(({ record }) =>
    record.payload._tag === "ToolCallSettled" ? [record.payload] : [],
  );

  expect(parentCalls.some((call) => call.toolName === "app_editor_start" && !call.isFailure)).toBe(
    true,
  );
  expect(
    parentCalls.some((call) => call.toolName === "app_editor_follow_up" && !call.isFailure),
  ).toBe(true);
  expect(
    parentCalls.some((call) =>
      ["create_trip_app", "read_trip_app_files", "edit_trip_app"].includes(call.toolName),
    ),
  ).toBe(false);
  expect(childCalls.some((call) => call.toolName === "edit_trip_app" && !call.isFailure)).toBe(
    true,
  );

  const denied = child.records.flatMap(({ record }) => {
    const payload = record.payload;

    if (payload._tag !== "ToolCallPrepared" || payload.toolName !== "edit_trip_app") return [];

    const parameters = Schema.decodeUnknownSync(Schema.Struct({ tripId: Schema.String }))(
      payload.parameters,
    );

    return parameters.tripId === otherMember.id ? [payload] : [];
  })[0];

  expect(denied).toBeDefined();
  expect(
    child.records.flatMap(({ record }) =>
      record.payload._tag === "SubmissionSettled"
        ? [{ runId: record.payload.runId, outcome: record.payload.outcome }]
        : [],
    ),
  ).toContainEqual({ runId: denied?.runId, outcome: "failed" });
  expect(childCalls.some((call) => call.runId === denied?.runId && !call.isFailure)).toBe(false);

  const admissions = parent.records.flatMap(({ record }) =>
    record.payload._tag === "WorkerInputRequested" ? [record.payload.admission] : [],
  );

  expect(new Set(admissions.map((value) => value.origin.worker.threadId))).toEqual(
    new Set([editorId]),
  );
  expect(admissions.length).toBeGreaterThanOrEqual(2);

  const plannerTools = Schema.decodeUnknownSync(Schema.Array(Schema.String))(
    await fixture("object", { key: `fixture-tools/${sourceThread}/planner.json` }),
  );

  const editorTools = Schema.decodeUnknownSync(Schema.Array(Schema.String))(
    await fixture("object", { key: `fixture-tools/${editorId}/editor.json` }),
  );

  expect(plannerTools).not.toContain("edit_trip_app");
  expect(plannerTools).not.toContain("create_trip_app");
  expect(editorTools).toContain("edit_trip_app");
  expect(editorTools).not.toContain("app_editor_start");
}, 90_000);

it("surfaces a failed child without leaving the main planner pending", async () => {
  const conversation = "failed-editor";
  const trip = await save(conversation);

  expect(await send(conversation, trip.id, "start-editor fail-editor")).toEqual({ accepted: true });

  const failed = await until(
    () => snapshot(conversation),
    (state) => state.pending === 0 && state.editor?.state === "failed",
    "Failed editor was not surfaced",
  );

  expect(failed.editor?.activity.some((item) => item.kind === "failure")).toBe(true);
  expect(failed.app ?? null).toBeNull();
  expect(await send(conversation, trip.id, "planner after failed edit")).toEqual({
    accepted: true,
  });
  await until(
    () => snapshot(conversation),
    (state) =>
      state.pending === 0 &&
      state.messages.some((message) =>
        message.text.includes("Planner handled: planner after failed edit"),
      ),
    "Planner stayed blocked after editor failure",
  );
}, 30_000);
