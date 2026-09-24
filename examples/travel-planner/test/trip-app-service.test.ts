import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { Trip, TripApp } from "../src/domain.ts";

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
import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import { Cause, Effect, Layer } from "effect";
import { WorkerEnvironment } from "effect-cf";
import { PlannerError } from "../src/domain.ts";
import { TripRepository, TripRepositoryLive } from "../src/server/trips.ts";
import { AppRepository, AppRepositoryLive } from "../src/trip-app/repository.ts";
import { AppSourceStore } from "../src/trip-app/source.ts";
import { AppBuildBucketLive } from "../src/trip-app/bindings.ts";
import { createTripApp, editTripApp, retryTripAppBuild } from "../src/trip-app/service.ts";
const conversation = "account-00000000-0000-0000-0000-000000000001--lisbon-conversation";
const draft = { title:"Lisbon with friends", destination:"Lisbon", summary:"Three friends exploring Lisbon", startDate:null, endDate:null, travelers:3, days:[{title:"Arrival",activities:["Walk by the river"]}], notes:["Coming for work"], places:[] };
export class ServiceFixture extends DurableObject {
  trees = new Map(); heads = new Map(); workflows = new Map(); forks=0; commits=0; creates=0; sequence=0;
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
        return { id, status:async()=>({status:record.status}),restart:async()=>{record.status="queued";} };
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
      if(input.kind==="retry") operation=retryTripAppBuild(trip.id);
      if(input.kind==="edit") operation=editTripApp({tripId:trip.id,expectedCommit:input.commitId,files:input.files,deletePaths:[],label:"Change app"});
      const exit=yield* operation.pipe(Effect.exit);
      const app=yield* apps.get(trip.id);
      return {exit:exit._tag==="Success"?{tag:"Success",value:exit.value??null}:{tag:"Failure",error:Cause.pretty(exit.cause)},app,trip:yield* trips.get(trip.id),forks:fixture.forks,commits:fixture.commits,creates:fixture.creates,workflows:Array.from(fixture.workflows.values())};
    }).pipe(
      Effect.provide(Layer.mergeAll(layers,source,AppBuildBucketLive)),
      Effect.provideService(ThreadObjectIdentity,{threadId:input.conversation??conversation}),
      Effect.provideService(WorkerEnvironment,{APP_DOMAIN:"apps.example",APP_BUILDS:this.env.APP_BUILDS,SITE_BUILD:workflow}),
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
  forks: Schema.Number,
  commits: Schema.Number,
  creates: Schema.Number,
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
});

it("rejects another conversation and stale edits before changing source or scheduling a build", async () => {
  const denied = await call("scope", "create", {
    conversation: "account-00000000-0000-0000-0000-000000000001--different-conversation",
  });

  expect(denied.exit.tag).toBe("Failure");
  expect(denied.app).toBeNull();
  expect(denied.forks).toBe(0);
  const created = await call("scope", "create");

  const stale = await call("scope", "edit", {
    commitId: "f".repeat(40),
    files: [{ path: "extra.txt", content: "changed" }],
  });

  expect(stale.exit.tag).toBe("Failure");
  expect(stale.commits).toBe(0);
  expect(stale.creates).toBe(created.creates);
  expect(stale.trip).toEqual(created.trip);
});
