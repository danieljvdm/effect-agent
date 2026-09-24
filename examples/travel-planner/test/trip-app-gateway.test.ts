import { join } from "node:path";

import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { type TripApp, TripAppData } from "../src/domain.ts";

const app: TripApp = {
  id: "a".repeat(32),
  tripId: "lisbon",
  revision: 1,
  url: `https://lisbon-with-friends-${"a".repeat(12)}-trip.effect-agent.com`,
  repoName: "trip-app-test",
  sourceCommit: "b".repeat(40),
  activeCommit: "b".repeat(40),
  pendingCommit: null,
  status: "ready",
  error: null,
  updatedAt: "2026-09-09T00:00:00Z",
  versions: [],
};

const data: TripAppData = {
  title: "Lisbon with friends",
  destination: "Lisbon",
  summary: "Three travelers",
  startDate: null,
  endDate: null,
  travelers: 3,
  days: [],
  stays: [],
  places: [],
};

const generated = `export default {async fetch(request,env){
 const url=new URL(request.url);
 if(url.pathname==="/api/redirect")return new Response(null,{status:302,headers:{location:"https://untrusted.example","set-cookie":"bad=1"}});
 if(url.pathname==="/api/other")return env.TRIP_DATA.fetch("https://trip-data/api/trip?tripId=other");
 if(url.pathname==="/api/write")return env.TRIP_DATA.fetch("https://trip-data/api/trip",{method:"POST"});
 const trip=await env.TRIP_DATA.fetch("https://trip-data/api/trip");
 if(url.pathname==="/api/trip")return trip;
 let outbound;try{await fetch("https://untrusted.example");outbound="allowed";}catch{outbound="blocked";}
 return Response.json({bindings:Object.keys(env),headers:Object.fromEntries(request.headers),trip:await trip.json(),outbound},{headers:{"set-cookie":"bad=1","x-private-response":"secret"}});
}};`;

let runtime: Miniflare;

beforeAll(async () => {
  const bundle = await build({
    stdin: {
      resolveDir: import.meta.dirname,
      loader: "ts",
      contents: `
import { DurableObject } from "cloudflare:workers";
import { Effect, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";
import { AppBuildBucketLive } from "../src/trip-app/bindings.ts";
import { handleRequest } from "../src/worker.ts";
import { publishTripAppAddress, appAddressKey } from "../src/trip-app/addresses.ts";
import { TripFailpoint } from "../src/server/trips.ts";
import { PlannerError } from "../src/domain.ts";
import { AppCommand } from "../src/trip-app/remote.ts";
import { BuildManifest,buildPrefix } from "../src/trip-app/build.ts";
export { TripData } from "../src/trip-app/gateway.ts";
export class OwnerFixture extends DurableObject {
 async seed(app,data){await this.ctx.storage.put({app,data});}
 async tripApp(encoded){
   const command=Schema.decodeUnknownSync(Schema.fromJsonString(AppCommand))(encoded);
   const app=await this.ctx.storage.get("app");
   if(command._tag==="GetById")return JSON.stringify({_tag:"Success",value:app?.id===command.appId?app:null});
   if(command._tag==="Data"&&app?.tripId===command.tripId)return JSON.stringify({_tag:"Success",value:await this.ctx.storage.get("data")});
   return JSON.stringify({_tag:"Failure",error:{_tag:"PlannerError",code:"not-found",message:"Trip not found"}});
 }
}
export default {async fetch(request,env,ctx){
 const url=new URL(request.url);
 if(url.pathname==="/__seed"){
   const input=await request.json();await env.ACCOUNT_THREADS.getByName(input.owner).seed(input.app,input.data);
   if(input.register!==false)await Effect.runPromise(publishTripAppAddress(input.owner,input.app,"effect-agent.com").pipe(Effect.provide(AppBuildBucketLive),Effect.provideService(WorkerEnvironment,env)));
   if(input.app.activeCommit!==null){
     const prefix=buildPrefix(input.app.id,input.app.activeCommit);
     const files=[{path:"web/index.html",body:"<main>Built trip</main>",contentType:"text/html; charset=utf-8"},{path:"web/assets/style.css",body:"body{color:green}",contentType:"text/css; charset=utf-8"},{path:"server/index.js",body:${JSON.stringify(generated)},contentType:"application/javascript"}];
     const manifest={version:1,appId:input.app.id,commitId:input.app.activeCommit,files:[]};
     for(const file of files){const bytes=new TextEncoder().encode(file.body);const sha256=Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",bytes)),byte=>byte.toString(16).padStart(2,"0")).join("");await env.APP_BUILDS.put(prefix+file.path,file.body,{customMetadata:{sha256},httpMetadata:{contentType:file.contentType}});manifest.files.push({path:file.path,bytes:bytes.byteLength,sha256,contentType:file.contentType});}
     await env.APP_BUILDS.put(prefix+"manifest.json",Schema.encodeSync(Schema.fromJsonString(BuildManifest))(manifest));
   }
   return new Response("seeded");
 }
 if(url.pathname==="/__register") {
   const input=await request.json();
   const result=await Effect.runPromise(publishTripAppAddress(input.owner,input.app,"effect-agent.com").pipe(
     Effect.provide(AppBuildBucketLive),Effect.provideService(WorkerEnvironment,env),
     Effect.provideService(TripFailpoint,{hit:(point)=>point!==input.point?Effect.void:Effect.fail(new PlannerError({code:"storage",message:"Injected failure"}))}),Effect.exit));
   return Response.json({tag:result._tag});
 }
 if(url.pathname==="/__address"){
   const input=await request.json();
   if(input.value!==undefined)await env.APP_BUILDS.put(appAddressKey(input.hostname),JSON.stringify(input.value));
   const value=await env.APP_BUILDS.get(appAddressKey(input.hostname));return Response.json(value?await value.json():null);
 }
 return Effect.runPromise(handleRequest()(request,env,ctx).pipe(Effect.provideService(WorkerEnvironment,env)));
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
    alias: { "@tanstack/react-start/server-entry": join(import.meta.dirname, "fixtures/start.ts") },
    logLevel: "silent",
  });

  const output = bundle.outputFiles[0];

  if (!output) throw new Error("Missing gateway fixture");
  runtime = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: output.text,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { ACCOUNT_THREADS: { className: "OwnerFixture", useSQLite: true } },
      r2Buckets: ["APP_BUILDS"],
      workerLoaders: { APP_LOADER: {} },
      bindings: {
        OPENAI_API_KEY: "PRIVATE_HOST_KEY",
        ACCESS_AUD: "PRIVATE_ACCESS_AUD",
        ACCESS_TEAM_DOMAIN: "https://travel-test.cloudflareaccess.com",
        APP_DOMAIN: "effect-agent.com",
        AI_GATEWAY_KEY: "PRIVATE_GATEWAY_KEY",
      },
    }),
  );
});
afterAll(async () => {
  await runtime?.dispose();
});

const storageOwner = "account-00000000-0000-0000-0000-000000000001";
const memberOwner = "account-00000000-0000-0000-0000-000000000002";

const seed = async (owner: string, value: TripApp = app, register = true) => {
  const response = await runtime.dispatchFetch("https://app.example/__seed", {
    method: "POST",
    body: JSON.stringify({ owner, app: value, data, register }),
  });

  expect(response.status).toBe(200);
  await response.text();
};

const fetchApp = (
  path: string,
  value: TripApp = app,
  headers: Record<string, string> = {},
  method = "GET",
) => runtime.dispatchFetch(`${value.url}${path}`, { method, headers });

it("runs an actual Worker Loader with only scoped TripData and strips incoming and outgoing credentials", async () => {
  await seed(storageOwner);

  const response = await fetchApp("/api/inspect?tripId=other&owner=other-owner", app, {
    authorization: "Bearer PRIVATE_BEARER",
    cookie: "CF_Authorization=PRIVATE_COOKIE",
    "cf-access-jwt-assertion": "PRIVATE_ASSERTION",
    "x-trip-owner": "other-owner",
    "x-api-key": "PRIVATE_API_KEY",
  });

  expect(response.status).toBe(200);

  const inspected = Schema.decodeUnknownSync(
    Schema.Struct({
      bindings: Schema.Array(Schema.String),
      headers: Schema.Record(Schema.String, Schema.String),
      trip: TripAppData,
      outbound: Schema.String,
    }),
  )(await response.json());

  expect(inspected.bindings).toEqual(["TRIP_DATA"]);
  expect(inspected.headers).toEqual({ accept: "application/json" });
  expect(inspected.trip).toEqual(data);
  expect(inspected.outbound).toBe("blocked");
  expect(JSON.stringify(inspected)).not.toContain("PRIVATE_");
  expect(response.headers.get("set-cookie")).toBeNull();
  expect(response.headers.get("x-private-response")).toBeNull();
  for (const path of ["/api/other", "/api/write"]) {
    const denied = await fetchApp(path);

    expect(denied.status).toBe(404);
    await denied.text();
  }
  const redirect = await fetchApp("/api/redirect");

  expect(redirect.status).toBe(502);
  expect(redirect.headers.get("location")).toBeNull();
  expect(redirect.headers.get("set-cookie")).toBeNull();
  await redirect.text();
}, 30_000);

const directory = async (path: string, input: Record<string, unknown>) => {
  const response = await runtime.dispatchFetch(`https://fixture.example${path}`, {
    method: "POST",
    body: JSON.stringify(input),
  });

  expect(response.status).toBe(200);

  return response.json();
};

it("refuses an address whose trip does not match the saved app", async () => {
  const mismatched = {
    ...app,
    id: "8".repeat(32),
    url: `https://mismatched-${"8".repeat(12)}-trip.effect-agent.com`,
  };

  await seed(memberOwner, mismatched);
  const otherHostname = new URL(mismatched.url).hostname;

  await directory("/__address", {
    hostname: otherHostname,
    value: {
      version: 1,
      hostname: otherHostname,
      owner: memberOwner,
      appId: mismatched.id,
      tripId: "other-trip",
    },
  });
  const denied = await fetchApp("/api/trip", mismatched);

  expect(denied.status).toBe(404);
  await denied.text();
});

it("repairs a lost address publication acknowledgement without exposing an unsaved app", async () => {
  let sequence = 100;

  {
    {
      const point = "app-address:after-put";
      const id = (++sequence).toString(16).padStart(32, "0");

      const value = {
        ...app,
        id,
        url: `https://interrupted-${id.slice(-12)}-trip.effect-agent.com`,
      };

      const hostname = new URL(value.url).hostname;

      const interrupted = await directory("/__register", {
        owner: memberOwner,
        app: value,
        point,
      });

      expect(interrupted).toEqual({ tag: "Failure" });
      expect(await directory("/__address", { hostname })).toEqual({
        version: 1,
        owner: memberOwner,
        hostname,
        appId: id,
        tripId: value.tripId,
      });
      // An address without a saved owner record cannot expose data.
      const uncommitted = await fetchApp("/api/trip", value);

      expect(uncommitted.status).toBe(404);
      await uncommitted.text();
      const retried = await directory("/__register", { owner: memberOwner, app: value });

      expect(retried).toEqual({ tag: "Success" });
      expect(await directory("/__address", { hostname })).toMatchObject({
        owner: memberOwner,
        appId: id,
        tripId: value.tripId,
      });
      expect(await directory("/__register", { owner: memberOwner, app: value })).toEqual({
        tag: "Success",
      });
    }
  }
}, 30_000);

it("reserves a colliding address atomically for one owner", async () => {
  const url = "https://same-name-collision-trip.effect-agent.com";

  const candidates = [
    { owner: storageOwner, app: { ...app, id: "6".repeat(32), url } },
    { owner: memberOwner, app: { ...app, id: "7".repeat(32), url } },
  ];

  const results = await Promise.all(
    candidates.map((candidate) => directory("/__register", candidate)),
  );

  expect(results).toContainEqual({ tag: "Success" });
  expect(results).toContainEqual({ tag: "Failure" });

  const winner = results.findIndex((result) =>
    Schema.is(Schema.Struct({ tag: Schema.Literal("Success") }))(result),
  );

  expect(await directory("/__address", { hostname: new URL(url).hostname })).toMatchObject({
    owner: candidates[winner]?.owner,
    appId: candidates[winner]?.app.id,
  });
});
