import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { type Effect, Schema } from "effect";
import type { WorkerEnvironment } from "effect-cf";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, expectTypeOf, it } from "vite-plus/test";

import type { PlannerError } from "../src/domain.ts";
import { AppFile, Trip, TripApp } from "../src/domain.ts";
import type { TripRepository } from "../src/server/trips.ts";
import type { AppRepository } from "../src/trip-app/repository.ts";
import type { createTripApp } from "../src/trip-app/service.ts";
import type { AppSourceStore } from "../src/trip-app/source.ts";

let runtime: Miniflare;
let directory: string;
let script: string;

const start = () =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { FIXTURES: { className: "ServiceFixture", useSQLite: true } },
      resourcePersistencePath: directory,
      r2Buckets: ["APP_BUILDS"],
    }),
  );

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "trip-app-service-"));

  const bundle = await build({
    stdin: {
      resolveDir: import.meta.dirname,
      loader: "ts",
      contents: `
import { DurableObject } from "cloudflare:workers";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { Cause, Effect, Layer } from "effect";
import { WorkerEnvironment } from "effect-cf";
import { PlannerError } from "../src/domain.ts";
import { TripRepository, TripRepositoryLive, TripFailpoint } from "../src/server/trips.ts";
import { AppRepository, AppRepositoryLive } from "../src/trip-app/repository.ts";
import { AppSourceStore } from "../src/trip-app/source.ts";
import { createTripApp, addTripAppMap, editTripApp, readTripAppFiles, restoreTripApp, retryTripAppBuild } from "../src/trip-app/service.ts";
import { recordBuildProgress, settleBuild } from "../src/trip-app/build.ts";
const conversation = "lisbon-conversation";
const draft = { title:"Lisbon with friends", destination:"Lisbon", summary:"Three friends exploring Lisbon", startDate:null, endDate:null, travelers:3, days:[{title:"Arrival",activities:["Walk by the river"]}], notes:["Coming for work"], places:[] };
export class ServiceFixture extends DurableObject {
  trees = new Map(); heads = new Map(); workflows = new Map(); forks=0; commits=0; creates=0; restarts=0; sequence=0;
  async fetch(request) {
    const input = await request.json();
    const source = Layer.succeed(AppSourceStore, {
      fork: ({repoName,files}) => Effect.sync(() => {
        if(this.heads.has(repoName)) return {commitId:this.heads.get(repoName)};
        this.forks++; const commitId=(++this.sequence).toString(16).padStart(40,"0"); this.heads.set(repoName,commitId); this.trees.set(commitId,files); return {commitId};
      }),
      read: ({commitId}) => this.trees.has(commitId) ? Effect.succeed(this.trees.get(commitId)) : Effect.fail(new PlannerError({code:"not-found",message:"Missing source"})),
      commit: ({repoName,parentCommit,files}) => Effect.suspend(() => {
        if(this.heads.get(repoName)!==parentCommit) return Effect.fail(new PlannerError({code:"conflict",message:"Source changed"}));
        if(JSON.stringify(this.trees.get(parentCommit))===JSON.stringify(files)) return Effect.succeed({commitId:parentCommit});
        this.commits++; const commitId=(++this.sequence).toString(16).padStart(40,"0");this.heads.set(repoName,commitId);this.trees.set(commitId,files);return Effect.succeed({commitId});
      }),
    });
    const workflow = {
      createBatch: async (batch) => Promise.all(batch.map(item=>workflow.create(item))),
      create: async ({id,params}) => {
        this.creates++;
        if(input.enqueue==="before") throw new Error("PRIVATE_WORKFLOW_CREDENTIAL");
        if(this.workflows.has(id)) throw new Error("Already exists");
        this.workflows.set(id,{params,status:"queued"});
        if(input.enqueue==="after") throw new Error("Acknowledgement lost");
        return workflow.get(id);
      },
      get: async (id) => {
        const record=this.workflows.get(id);if(!record) throw new Error("Missing workflow");
        return { id, status:async()=>({status:record.status}),restart:async()=>{this.restarts++;record.status="queued";} };
      },
    };
    const sql=SqliteClient.layer({storage:this.ctx.storage});
    const layers=Layer.mergeAll(AppRepositoryLive,TripRepositoryLive).pipe(Layer.provide(sql));
    const fixture=this;
    const program=Effect.gen(function*(){
      const trips=yield* TripRepository; const apps=yield* AppRepository;
      let list=yield* trips.list;
      if(list.length===0){yield* trips.save({...draft,tripId:null,expectedRevision:null},conversation);list=yield* trips.list;}
      const trip=list[0];
      let operation=Effect.void;
      if(input.kind==="create") operation=createTripApp(trip.id);
      if(input.kind==="map") operation=addTripAppMap(trip.id);
      if(input.kind==="retry") operation=retryTripAppBuild(trip.id);
      if(input.kind==="restore") operation=restoreTripApp(trip.id,input.commitId);
      if(input.kind==="edit") operation=editTripApp({tripId:trip.id,expectedCommit:input.commitId,files:input.files,deletePaths:[],label:"Change app"});
      if(input.kind==="settle") {
        const app=yield* apps.get(trip.id);
        const id=app.id+"-"+app.sourceCommit;
        const pending=fixture.workflows.get(id); if(pending)pending.status=input.error ? "errored":"complete";
        operation=settleBuild({owner:"travel-planner-owner-v1",appId:app.id,tripId:trip.id,repoName:app.repoName,commitId:app.sourceCommit,label:input.error?"Failed change":"Built app"},input.error??null);
      }
      if(input.kind==="progress") {
        const app=yield* apps.get(trip.id);
        operation=recordBuildProgress({owner:"travel-planner-owner-v1",appId:app.id,tripId:trip.id,repoName:app.repoName,commitId:input.commitId??app.sourceCommit,label:"Built app"},input.update);
      }
      if(input.kind==="read") operation=readTripAppFiles(trip.id,input.paths);
      const exit=yield* operation.pipe(Effect.exit);
      const app=yield* apps.get(trip.id);
      return {exit:exit._tag==="Success"?{tag:"Success",value:exit.value??null}:{tag:"Failure",error:Cause.pretty(exit.cause)},app,trip:yield* trips.get(trip.id),files:app?(fixture.trees.get(app.sourceCommit)??[]):[],forks:fixture.forks,commits:fixture.commits,creates:fixture.creates,restarts:fixture.restarts,workflows:Array.from(fixture.workflows.values())};
    }).pipe(
      Effect.provide(layers), Effect.provide(source),
      Effect.provideService(ThreadObjectIdentity,{threadId:input.conversation??conversation}),
      Effect.provideService(WorkerEnvironment,{APP_DOMAIN:"apps.example",APP_BUILDS:this.env.APP_BUILDS,SITE_BUILD:workflow}),
      Effect.provideService(TripFailpoint,{hit:(point)=>point!==input.point?Effect.void:input.fault==="defect"?Effect.die("Injected boundary defect"):input.fault==="interrupt"?Effect.interrupt:Effect.fail(new PlannerError({code:"storage",message:"Injected boundary failure"}))}),
    );
    return Response.json(await Effect.runPromise(program));
  }
}
export default {async fetch(request,env){
  const response=await env.FIXTURES.getByName(new URL(request.url).pathname).fetch(request);
  return new Response(await response.arrayBuffer(),response);
}};
`,
    },
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    banner: {
      js: 'import { createRequire } from "node:module"; const require = createRequire("/fixture.mjs");',
    },
    logLevel: "silent",
  });

  const output = bundle.outputFiles[0];

  if (!output) throw new Error("Missing service fixture");
  script = output.text;
  runtime = start();
});
afterAll(async () => {
  await runtime?.dispose();
  if (directory) await rm(directory, { recursive: true, force: true });
});

const Snapshot = Schema.Struct({
  exit: Schema.Struct({
    tag: Schema.Literals(["Success", "Failure"]),
    error: Schema.optionalKey(Schema.String),
    value: Schema.optionalKey(Schema.Unknown),
  }),
  app: Schema.NullOr(TripApp),
  trip: Trip,
  files: Schema.Array(AppFile),
  forks: Schema.Number,
  commits: Schema.Number,
  creates: Schema.Number,
  restarts: Schema.Number,
  workflows: Schema.Array(
    Schema.Struct({
      params: Schema.Struct({
        owner: Schema.String,
        tripId: Schema.String,
        commitId: Schema.String,
      }),
      status: Schema.String,
    }),
  ),
});

const call = async (name: string, kind: string, input: Record<string, unknown> = {}) => {
  const response = await runtime.dispatchFetch(`http://service/${name}`, {
    method: "POST",
    body: JSON.stringify({ kind, ...input }),
  });

  if (!response.ok) throw new Error(await response.text());

  return Schema.decodeUnknownSync(Snapshot)(await response.json());
};

const appOf = (snapshot: typeof Snapshot.Type) => {
  if (snapshot.app === null) throw new Error("Expected an app");

  return snapshot.app;
};

it("creates, builds, edits a real map source, and restores the old source without changing trip facts", async () => {
  const created = await call("loop", "create");

  expect(created.exit.tag).toBe("Success");
  expect(created.app).toMatchObject({ status: "building", activeCommit: null });
  expect(created.app?.url).toMatch(
    /^https:\/\/lisbon-with-friends-[a-f0-9]{12}-trip\.apps\.example$/,
  );
  expect(created.app?.buildProgress?.map(({ phase }) => phase)).toEqual(["queued"]);
  expect(created.workflows[0]?.params).toMatchObject({
    owner: "travel-planner-owner-v1",
    tripId: created.trip.id,
  });
  const first = appOf(await call("loop", "settle"));

  expect(first.buildProgress?.map(({ phase }) => phase)).toEqual(["queued", "ready"]);

  const mapped = await call("loop", "map");

  expect(mapped.exit.tag).toBe("Success");
  expect(mapped.app?.activeCommit).toBe(first.activeCommit);
  expect(mapped.app?.buildProgress?.map(({ phase }) => phase)).toEqual(["queued"]);

  const stale = await call("loop", "progress", {
    commitId: first.sourceCommit,
    update: { phase: "compiling", message: "An old build is still running" },
  });

  expect(stale.app).toEqual(mapped.app);
  expect(
    mapped.files.find((file) => file.path === "packages/web/src/TripMap.tsx")?.content,
  ).toContain("L.map(");
  expect(mapped.files.some((file) => file.path === "packages/web/src/map.css")).toBe(true);
  const built = appOf(await call("loop", "settle"));

  expect(built.versions).toHaveLength(2);
  const restored = await call("loop", "restore", { commitId: first.sourceCommit });

  expect(restored.exit.tag).toBe("Success");
  expect(restored.files).toEqual(created.files);
  expect(restored.app?.sourceCommit).not.toBe(built.sourceCommit);
  expect(restored.app?.activeCommit).toBe(built.activeCommit);
  expect(restored.app?.buildProgress?.map(({ phase }) => phase)).toEqual(["queued"]);
  const completed = await call("loop", "settle");

  expect(completed.app?.activeCommit).toBe(restored.app?.sourceCommit);
  expect(completed.trip).toEqual(created.trip);
  expect((await call("loop", "retry")).app?.status).toBe("ready");
}, 30_000);

it("restores the actual source after a failed change even when the old version is already active", async () => {
  const created = await call("failed-restore", "create");
  const first = appOf(await call("failed-restore", "settle"));

  await call("failed-restore", "map");
  const failed = await call("failed-restore", "settle", { error: "Compilation failed" });

  expect(failed.app).toMatchObject({
    status: "failed",
    activeCommit: first.sourceCommit,
    pendingCommit: null,
  });
  const restored = await call("failed-restore", "restore", { commitId: first.sourceCommit });

  expect(restored.files).toEqual(created.files);
  expect(restored.app?.status).toBe("building");
  expect(restored.app?.sourceCommit).not.toBe(failed.app?.sourceCommit);
});

it("retries saved source after enqueue failures and reconciles acknowledgement loss without another fork", async () => {
  const failed = await call("enqueue", "create", { enqueue: "before" });

  expect(failed.exit.tag).toBe("Failure");
  expect(failed.exit.error).not.toContain("PRIVATE_WORKFLOW_CREDENTIAL");
  expect(failed.app?.status).toBe("building");
  const retried = await call("enqueue", "create");

  expect(retried.exit.tag).toBe("Success");
  expect(retried.forks).toBe(1);
  expect(retried.workflows).toHaveLength(1);
  const lost = await call("lost-ack", "create", { enqueue: "after" });

  expect(lost.exit.tag).toBe("Success");
  const again = await call("lost-ack", "retry");

  expect(again.workflows).toHaveLength(1);
  expect(again.forks).toBe(1);
  await call("lost-ack", "settle", { error: "Failed build" });
  const restarted = await call("lost-ack", "retry");

  expect(restarted.restarts).toBe(1);
  expect(restarted.app?.buildProgress?.map(({ phase }) => phase)).toEqual(["queued"]);
  expect(restarted.app?.error).toBeNull();
  const boundary = await call("after-start", "create", { point: "app-build:after-start" });

  expect(boundary.exit.tag).toBe("Failure");
  expect(boundary.workflows).toHaveLength(1);
  const recovered = await call("after-start", "retry");

  expect(recovered.exit.tag).toBe("Success");
  expect(recovered.workflows).toHaveLength(1);
  expect(recovered.app).toEqual(boundary.app);
});

it("persists progress through save failures, defects, interruption, and a real repository restart", async () => {
  const update = { phase: "installing", message: "Installing app dependencies" };
  let saved: TripApp | undefined;
  let lastName = "";

  for (const fault of ["failure", "defect", "interrupt"]) {
    for (const point of ["app:save:before", "app:save:after"]) {
      const name = `progress-${fault}-${point.endsWith("before") ? "before" : "after"}`;
      const created = appOf(await call(name, "create"));
      const lost = await call(name, "progress", { update, point, fault });

      const appended = {
        ...created,
        revision: created.revision + 1,
        updatedAt: expect.any(String),
        buildProgress: [...(created.buildProgress ?? []), { ...update, at: expect.any(String) }],
      };

      expect(lost.exit.tag).toBe("Failure");
      expect(lost.app).toEqual(point.endsWith("before") ? created : appended);

      const retried = await call(name, "progress", { update });

      expect(retried.exit.tag).toBe("Success");
      expect(retried.app?.revision).toBe(created.revision + 1);
      expect(retried.app?.buildProgress?.map(({ phase }) => phase)).toEqual([
        "queued",
        "installing",
      ]);
      saved = appOf(retried);
      lastName = name;
    }
  }
  await runtime.dispose();
  runtime = start();
  const reloaded = await call(lastName, "snapshot");

  expect(reloaded.app).toEqual(saved);
  expect((await call(lastName, "progress", { update })).app).toEqual(saved);
  const failed = await call(lastName, "settle", { error: "Dependency install failed" });

  expect(failed.app?.buildProgress?.map(({ phase }) => phase)).toEqual([
    "queued",
    "installing",
    "failed",
  ]);
  expect(failed.app?.status).toBe("failed");
  expect(failed.app?.error).toBe("Dependency install failed");
  expect((await call(lastName, "progress", { update })).app).toEqual(failed.app);
}, 30_000);

it("rejects another conversation and stale edits before changing source or scheduling a build", async () => {
  const denied = await call("scope", "create", { conversation: "different-conversation" });

  expect(denied.exit.tag).toBe("Failure");
  expect(denied.app).toBeNull();
  expect(denied.forks).toBe(0);
  const created = await call("scope", "create");
  const deniedMap = await call("scope", "map", { conversation: "different-conversation" });

  expect(deniedMap.exit.tag).toBe("Failure");
  expect(deniedMap.files).toEqual(created.files);

  const stale = await call("scope", "edit", {
    commitId: "f".repeat(40),
    files: [{ path: "extra.txt", content: "changed" }],
  });

  expect(stale.exit.tag).toBe("Failure");
  expect(stale.commits).toBe(0);
  expect(stale.creates).toBe(created.creates);
  expect(stale.trip).toEqual(created.trip);
  const read = await call("scope", "read", { paths: ["packages/web/src/TripMap.tsx"] });

  expect(read.exit.value).toMatchObject({
    commitId: created.app?.sourceCommit,
    files: expect.arrayContaining([
      { path: "packages/web/src/TripMap.tsx", content: expect.any(String) },
    ]),
  });
  expectTypeOf<Effect.Error<ReturnType<typeof createTripApp>>>().toEqualTypeOf<PlannerError>();
  expectTypeOf<Effect.Services<ReturnType<typeof createTripApp>>>().toEqualTypeOf<
    AppRepository | AppSourceStore | TripRepository | ThreadObjectIdentity | WorkerEnvironment
  >();
});

it("retries address publication after durable app creation without forking or changing its URL", async () => {
  for (const fault of ["failure", "defect", "interrupt"]) {
    for (const point of ["app-address:before-put", "app-address:after-put"]) {
      const name = `address-${fault}-${point.endsWith("before-put") ? "before" : "after"}`;
      const interrupted = await call(name, "create", { point, fault });

      expect(interrupted.exit.tag).toBe("Failure");
      expect(interrupted.app?.status).toBe("building");
      expect(interrupted.creates).toBe(0);
      expect(interrupted.forks).toBe(1);
      const retried = await call(name, "retry");

      expect(retried.exit.tag).toBe("Success");
      expect(retried.app).toEqual(interrupted.app);
      expect(retried.forks).toBe(1);
      expect(retried.creates).toBe(1);
    }
  }
});
