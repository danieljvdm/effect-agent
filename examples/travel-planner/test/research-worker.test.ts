import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { SettlementFailureDiagnostic } from "@effect-agent/thread/Records";
import { ThreadExport } from "@effect-agent/thread/ThreadStore";
import { Effect, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { PlannerSnapshot, Trip, type PlannerSettings } from "../src/domain.ts";
import {
  previousResearchCoordinatorId,
  researchCoordinatorId,
  ScoutInput,
  ScoutReportInput,
  ScoutProgressInput,
} from "../src/research/contracts.ts";

const token = "research-worker-fixture";
const sourceThread = `member-${createHash("sha256").update("research@example.com").digest("hex")}--research`;
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
      durableObjects: { THREADS: { className: "TravelPlannerThread", useSQLite: true } },
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

  const result = Schema.decodeUnknownSync(Schema.fromJsonString(RpcExit))(
    (await response.text()).trim().split("\n")[0],
  ).exit;

  if (result._tag !== "Success") throw new Error(JSON.stringify(result.cause));

  return result.value;
};

const snapshot = async (email?: string) =>
  Schema.decodeUnknownSync(PlannerSnapshot)(
    await rpc("GetPlanner", { conversationId: "research" }, email),
  );

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
  let value = await read();

  for (let attempt = 0; attempt < 1_000; attempt++) {
    if (matches(value)) return value;
    await Effect.runPromise(Effect.sleep("50 millis"));
    value = await read();
  }

  const diagnostic = Schema.is(PlannerSnapshot)(value)
    ? {
        pending: value.pending,
        messages: value.messages.map(({ role, text }) => ({ role, text })),
        scouts: value.scouts?.map(({ id, state, task }) => ({ id, state, task })),
      }
    : value;

  throw new Error(`Fixture did not settle: ${JSON.stringify(diagnostic).slice(0, 3_000)}`);
};

it("upgrades v8 trip history and v9 scouts to the current coordinator across chat and restart", async () => {
  await fixture("seed", { thread: sourceThread }, "POST");
  await until(
    () => snapshot(),
    (state) =>
      state.pending === 0 &&
      state.messages.some(
        (message) => message.text === "Planner handled: Previous trip conversation",
      ),
  );

  const seedTrip = (conversationId: string, destination: string) =>
    rpc("SaveTrip", {
      conversationId,
      tripId: null,
      expectedRevision: null,
      title: destination,
      destination,
      summary: "A saved trip",
      startDate: null,
      endDate: null,
      travelers: 2,
      days: [],
      notes: ["Preserve existing preferences"],
    });

  const trip = Schema.decodeUnknownSync(Trip)(await seedTrip("research", "Lisbon"));
  const otherTrip = Schema.decodeUnknownSync(Trip)(await seedTrip("other-trip", "Tahoe"));

  expect(
    await fixture(
      "seed-research",
      { thread: sourceThread, settings: JSON.stringify(settings) },
      "POST",
    ),
  ).toEqual({ accepted: true });

  const active = await until(
    () => snapshot(),
    (state) =>
      state.pending === 0 &&
      state.scouts?.length === 2 &&
      state.scouts.every((scout) => scout.state === "active"),
  );

  expect(active.messages.some((message) => message.text.includes("What is your budget?"))).toBe(
    true,
  );
  expect(active.scouts?.every((scout) => scout.finding === undefined)).toBe(true);
  const ids = active.scouts?.map((scout) => scout.id).sort();

  expect((await snapshot("other@example.com")).scouts).toEqual([]);
  const followUp = `follow research budget 200 quiet neighborhood referenceTripId=${otherTrip.id}`;

  await send(followUp);

  const updated = await until(
    () => snapshot(),
    (state) =>
      state.pending === 0 &&
      state.scouts?.some((scout) => scout.task.includes("budget 200")) === true,
  );

  expect(updated.messages.at(-1)?.text).toBe(`Planner handled: ${followUp}`);
  expect(updated.trips.find((value) => value.id === trip.id)).toMatchObject({
    revision: 2,
    notes: ["Preserve existing preferences", "Budget 200, quiet neighborhood"],
  });
  expect(updated.trips.find((value) => value.id === otherTrip.id)).toMatchObject(otherTrip);
  expect((await snapshot()).scouts?.map((scout) => scout.id).sort()).toEqual(ids);
  await runtime.dispose();
  runtime = makeRuntime();
  await fixture("gate", { name: "Activities" }, "POST");
  await until(
    () => snapshot(),
    (state) =>
      state.pending === 0 &&
      state.messages.some((message) => message.text === "Research update received."),
  );
  await fixture("gate", { name: "Stays" }, "POST");

  const completed = await until(
    () => snapshot(),
    (state) =>
      state.pending === 0 &&
      state.scouts?.every((scout) => scout.state === "idle") === true &&
      state.messages.some((message) => message.text === "Research update received."),
  );

  expect(completed.scouts?.map((scout) => scout.id).sort()).toEqual(ids);
  expect(completed.scouts?.every((scout) => scout.progress.text !== "")).toBe(true);
  expect(
    completed.messages
      .filter((message) => message.role === "assistant")
      .map((message) => message.text),
  ).toEqual([
    "Where do you want to go?",
    "Planner handled: Previous trip conversation",
    "Research is running. What is your budget?",
    `Planner handled: ${followUp}`,
    "Research update received.",
  ]);
  expect(
    completed.messages.filter((message) => message.role === "user").map((message) => message.text),
  ).toEqual(["Previous trip conversation", "start research", followUp]);
  expect(completed.trips.find((value) => value.id === trip.id)).toMatchObject({
    revision: 3,
    notes: [
      "Preserve existing preferences",
      "Budget 200, quiet neighborhood",
      "Research findings saved after restart",
    ],
  });
  expect(completed.trips.find((value) => value.id === otherTrip.id)).toMatchObject(otherTrip);
  for (const scout of completed.scouts ?? []) {
    const journal = Schema.decodeUnknownSync(ThreadExport)(
      await fixture("journal", { thread: scout.id }),
    );

    const settled = journal.records.findLast(
      ({ record }) => record.payload._tag === "SubmissionSettled",
    )?.record.payload;

    if (settled?._tag !== "SubmissionSettled") throw new Error("Expected settled scout");
    expect(scout.finding).toEqual({ id: settled.settlementId, text: scout.progress.text });
    expect(settled.outcome).toBe("completed");

    const admitted = journal.records.flatMap(({ record }) =>
      record.payload._tag === "UserInputRecorded"
        ? [Schema.decodeUnknownSync(ScoutInput)(record.payload.input)]
        : [],
    );

    expect(
      admitted.every((input) => JSON.stringify(input.settings) === JSON.stringify(settings)),
    ).toBe(true);
    expect(await fixture("tools", { thread: scout.id })).toEqual(
      expect.arrayContaining(["finish_research", "read_travel_page"]),
    );

    const prepared = journal.records.filter(
      ({ record }) => record.payload._tag === "WorkerReportPrepared",
    );

    expect(prepared.length).toBeGreaterThan(0);
    expect(
      journal.records.some(
        ({ record }) =>
          record.payload._tag === "WorkerOriginRecorded" &&
          record.payload.origin.source.agentId === previousResearchCoordinatorId,
      ),
    ).toBe(true);
    expect(
      journal.records.flatMap(({ record }) =>
        record.payload._tag === "WorkerOriginRecorded" ? [record.payload.origin.policy] : [],
      )[0],
    ).toMatchObject({ maxTurns: 8, maxToolCalls: 12 });
  }

  const parent = Schema.decodeUnknownSync(ThreadExport)(
    await fixture("journal", { thread: sourceThread }),
  );

  expect(parent.records[0]?.record.payload).toMatchObject({
    _tag: "ThreadCreated",
    agentId: "travel-planner-v8",
  });

  const reports = parent.records.flatMap(({ record }) => {
    if (record.payload._tag !== "UserInputRecorded") return [];
    const report = Schema.decodeUnknownOption(ScoutReportInput)(record.payload.input);

    return report._tag === "Some" ? [report.value] : [];
  });

  expect(reports.length).toBeGreaterThan(0);
  expect(
    reports.every((report) => JSON.stringify(report.settings) === JSON.stringify(settings)),
  ).toBe(true);
  expect(
    parent.records.some(
      ({ record }) =>
        record.payload._tag === "SubmissionSettled" &&
        record.payload.outcome === "failed" &&
        Schema.decodeUnknownOption(SettlementFailureDiagnostic)(record.payload.result).pipe(
          (diagnostic) =>
            diagnostic._tag === "Some" &&
            diagnostic.value.errorTag === "AgentToolAuthorizationDenied",
        ),
    ),
  ).toBe(true);
}, 90_000);

it.each(["current", "retained"])(
  "delivers a %s sourced milestone before worker completion and accepts a correction on that active worker",
  async (version) => {
    const email = `progress-${version}@example.com`;
    const thread = `member-${createHash("sha256").update(email).digest("hex")}--research`;

    await fixture("gate", { name: "Live progress" }, "DELETE");
    if (version === "retained")
      await fixture("seed-progress", { thread, settings: JSON.stringify(settings) }, "POST");
    else await send("start live progress", email);

    const active = await until(
      () => snapshot(email),
      (state) =>
        state.pending === 0 &&
        state.scouts?.[0]?.state === "active" &&
        state.messages.some(
          (message) => message.text === "Verified milestone received while research continues.",
        ),
    );

    const id = active.scouts?.[0]?.id;

    expect(id).toBeTruthy();
    expect(
      (await snapshot("unrelated@example.com")).messages.some((message) =>
        message.text.includes("milestone"),
      ),
    ).toBe(false);
    const parent = Schema.decodeUnknownSync(ThreadExport)(await fixture("journal", { thread }));

    const updates = parent.records.flatMap(({ record }) =>
      record.payload._tag === "UserInputRecorded" &&
      Schema.is(ScoutProgressInput)(record.payload.input)
        ? [record.payload.input]
        : [],
    );

    expect(updates).toHaveLength(1);
    expect(updates[0]?.settings).toEqual(settings);
    expect(updates[0]?.finding.summary).toContain("availability is unconfirmed");
    await send("steer live progress 50 km", email);

    const steered = await until(
      () => snapshot(email),
      (state) =>
        state.pending === 0 && state.scouts?.[0]?.task.includes("Experienced at 50 km") === true,
    );

    expect(steered.scouts?.[0]?.id).toBe(id);
    expect(steered.scouts?.[0]?.state).toBe("active");
    await fixture("gate", { name: "Live progress" }, "POST");

    const completed = await until(
      () => snapshot(email),
      (state) => state.scouts?.[0]?.state === "idle",
    );

    expect(completed.scouts?.[0]?.finding?.text).toContain("Experienced at 50 km");
  },
  90_000,
);

it("runs six scouts and an editor beyond the old budgets, preserves them across restart, and bounds admission", async () => {
  const email = "expanded@example.com";
  const expandedThread = `member-${createHash("sha256").update(email).digest("hex")}--research`;

  await rpc(
    "SaveTrip",
    {
      conversationId: "research",
      tripId: null,
      expectedRevision: null,
      title: "Lisbon",
      destination: "Lisbon",
      summary: "Independent expanded worker fixture",
      startDate: null,
      endDate: null,
      travelers: 2,
      days: [],
      notes: [],
    },
    email,
  );
  await send("start expanded research", email);

  const active = await until(
    () => snapshot(email),
    (state) =>
      state.pending === 0 &&
      state.scouts?.filter((scout) => scout.state === "active").length === 6 &&
      state.editor?.state === "active",
  );

  expect(active.messages.at(-1)?.text).toBe("Six scouts and the editor are working.");
  if (!active.editor) throw new Error("Expected an active editor");

  const workers = [
    ...(active.scouts?.filter((scout) => scout.state === "active").map((scout) => scout.id) ?? []),
    active.editor.id,
  ];

  await send("overflow expanded research", email);
  await until(
    () => snapshot(email),
    (state) => state.pending === 0,
  );

  const rejected = Schema.decodeUnknownSync(ThreadExport)(
    await fixture("journal", { thread: expandedThread }),
  );

  const failure = rejected.records
    .flatMap(({ record }) =>
      record.payload._tag === "SubmissionSettled" && record.payload.outcome === "failed"
        ? [record.payload.result]
        : [],
    )
    .at(-1);

  expect(failure).toMatchObject({ errorTag: "WorkerError" });
  expect((await snapshot(email)).scouts?.filter((scout) => scout.state === "active")).toHaveLength(
    6,
  );

  await runtime.dispose();
  runtime = makeRuntime();
  for (const name of [
    "Expanded 1",
    "Expanded 2",
    "Expanded 3",
    "Expanded 4",
    "Expanded 5",
    "Expanded 6",
    "Expanded editor",
  ])
    await fixture("gate", { name }, "POST");

  const completed = await until(
    () => snapshot(email),
    (state) =>
      state.pending === 0 &&
      state.editor?.state === "idle" &&
      state.scouts
        ?.filter((scout) => scout.task.includes("expanded allowance"))
        .every((scout) => scout.state === "idle" || scout.state === "failed") === true,
  );

  expect(completed.editor?.id).toBe(active.editor?.id);
  expect(
    completed.scouts
      ?.filter((scout) => scout.task.includes("expanded allowance"))
      .map((scout) => scout.state),
  ).toEqual(["idle", "idle", "idle", "idle", "idle", "idle"]);
  for (const id of workers) {
    const journal = Schema.decodeUnknownSync(ThreadExport)(
      await fixture("journal", { thread: id }),
    );

    const origin = journal.records.flatMap(({ record }) =>
      record.payload._tag === "WorkerOriginRecorded" ? [record.payload.origin] : [],
    )[0];

    expect(origin?.source.agentId).toBe(researchCoordinatorId);
    expect(origin?.policy.contextTokenLimit).toBe(128_000);
    expect(origin?.policy.runStatus).toBe("appended");

    const toolResults = journal.records.filter(
      ({ record }) => record.payload._tag === "ToolCallSettled" && !record.payload.isFailure,
    );

    expect(toolResults.length).toBeGreaterThan(id === active.editor?.id ? 24 : 12);
    expect(
      journal.records.some(
        ({ record }) =>
          record.payload._tag === "SubmissionSettled" && record.payload.outcome === "completed",
      ),
    ).toBe(true);
  }
}, 90_000);
