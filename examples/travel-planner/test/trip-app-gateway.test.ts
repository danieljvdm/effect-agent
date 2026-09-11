import { join } from "node:path";

import { type Effect, Schema } from "effect";
import type { WorkerEnvironment } from "effect-cf";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, expectTypeOf, it } from "vite-plus/test";

import { type PlannerError, type TripApp, TripAppData } from "../src/domain.ts";
import type { publishTripAppAddress, readTripAppAddress } from "../src/trip-app/addresses.ts";
import type { AppBuildBucket } from "../src/trip-app/bucket.ts";
import type { callAppRepository } from "../src/trip-app/remote.ts";

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
import { publishTripAppAddress, appAddressKey, tripAppHostname, appNameFromHost } from "../src/trip-app/addresses.ts";
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
   const input=await request.json();await env.THREADS.getByName(input.owner).seed(input.app,input.data);
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
 if(url.pathname==="/__name") {
   const input=await request.json();const hostname=tripAppHostname(input.title,input.appId,"effect-agent.com");
   return Response.json({hostname,name:appNameFromHost(hostname,"effect-agent.com")});
 }
 if(url.pathname==="/__register") {
   const input=await request.json();
   const result=await Effect.runPromise(publishTripAppAddress(input.owner,input.app,"effect-agent.com").pipe(
     Effect.provide(AppBuildBucketLive),Effect.provideService(WorkerEnvironment,env),
     Effect.provideService(TripFailpoint,{hit:(point)=>point!==input.point?Effect.void:input.fault==="defect"?Effect.die("Injected defect"):input.fault==="interrupt"?Effect.interrupt:Effect.fail(new PlannerError({code:"storage",message:"Injected failure"}))}),Effect.exit));
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
      durableObjects: { THREADS: { className: "OwnerFixture", useSQLite: true } },
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

it("serves public assets without authentication and keeps planner routes protected", async () => {
  expectTypeOf<
    Effect.Services<ReturnType<typeof publishTripAppAddress>>
  >().toEqualTypeOf<AppBuildBucket>();
  expectTypeOf<
    Effect.Error<ReturnType<typeof publishTripAppAddress>>
  >().toEqualTypeOf<PlannerError>();
  expectTypeOf<
    Effect.Services<ReturnType<typeof readTripAppAddress>>
  >().toEqualTypeOf<AppBuildBucket>();
  expectTypeOf<Effect.Error<ReturnType<typeof readTripAppAddress>>>().toEqualTypeOf<PlannerError>();
  expectTypeOf<
    Effect.Services<ReturnType<typeof callAppRepository>>
  >().toEqualTypeOf<WorkerEnvironment>();
  expectTypeOf<Effect.Error<ReturnType<typeof callAppRepository>>>().toEqualTypeOf<PlannerError>();
  await seed(storageOwner);
  const home = await fetchApp("/");

  expect(home.status).toBe(200);
  expect(await home.text()).toBe("<main>Built trip</main>");
  expect(home.headers.get("cache-control")).toBe("private, no-store");
  expect(home.headers.get("content-security-policy")).toContain("connect-src 'self'");
  const style = await fetchApp("/assets/style.css");

  expect(style.headers.get("content-type")).toContain("text/css");
  expect(await style.text()).toBe("body{color:green}");
  const head = await fetchApp("/", app, {}, "HEAD");

  expect(head.status).toBe(200);
  expect(await head.text()).toBe("");
  for (const path of [
    "/",
    "/api/rpc",
    "/api/access",
    "/api/progress",
    "/assets/style.css",
    "/trips/lisbon/1",
  ]) {
    const denied = await runtime.dispatchFetch(`https://travel.effect-agent.com${path}`, {
      redirect: "manual",
    });

    expect(denied.status).toBe(path === "/" || path.startsWith("/assets/") ? 303 : 401);
    await denied.text();
  }
  for (const path of ["/api/rpc", "/api/access", "/api/progress"]) {
    const denied = await fetchApp(path, app, {}, "POST");

    expect(denied.status).toBe(405);
    await denied.text();
  }
  for (const path of ["/server/index.js", "/manifest.json", "/missing.css"]) {
    const missing = await fetchApp(path);

    expect(missing.status).toBe(404);
    await missing.text();
  }
});

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
  const trip = await fetchApp("/api/trip");

  expect(Schema.decodeUnknownSync(TripAppData)(await trip.json())).toEqual(data);
  const redirect = await fetchApp("/api/redirect");

  expect(redirect.status).toBe(502);
  expect(redirect.headers.get("location")).toBeNull();
  expect(redirect.headers.get("set-cookie")).toBeNull();
  await redirect.text();
  const write = await fetchApp("/api/trip", app, {}, "POST");

  expect(write.status).toBe(405);
  await write.text();
}, 30_000);

it("keeps a previous built version available while a new build runs or fails", async () => {
  const unbuilt = {
    ...app,
    id: "c".repeat(32),
    url: `https://new-trip-${"c".repeat(12)}-trip.effect-agent.com`,
    activeCommit: null,
    pendingCommit: app.sourceCommit,
    status: "building" as const,
  };

  await seed(memberOwner, unbuilt);
  const pending = await fetchApp("/", unbuilt);

  expect(pending.status).toBe(202);
  expect(await pending.text()).toContain("being built");
  await seed(memberOwner, {
    ...unbuilt,
    status: "failed",
    pendingCommit: null,
    error: "Build failed",
  });
  const failed = await fetchApp("/", unbuilt);

  expect(failed.status).toBe(503);
  expect(await failed.text()).toContain("needs a build");
  for (const status of ["building", "failed"] as const) {
    await seed(storageOwner, {
      ...app,
      status,
      sourceCommit: "d".repeat(40),
      pendingCommit: status === "building" ? "d".repeat(40) : null,
    });
    const previous = await fetchApp("/");

    expect(previous.status).toBe(200);
    expect(await previous.text()).toBe("<main>Built trip</main>");
    const trip = await fetchApp("/api/trip");

    expect(trip.status).toBe(200);
    expect(await trip.json()).toEqual(data);
  }
});

const directory = async (path: string, input: Record<string, unknown>) => {
  const response = await runtime.dispatchFetch(`https://fixture.example${path}`, {
    method: "POST",
    body: JSON.stringify(input),
  });

  expect(response.status).toBe(200);

  return response.json();
};

it("preserves registered aliases, rejects orphaned legacy sites, and never accepts caller-selected ownership", async () => {
  await seed(storageOwner);
  const alias = { ...app, url: `https://${app.id}-trip.effect-agent.com` };

  expect(await (await fetchApp("/", alias)).text()).toBe("<main>Built trip</main>");

  const legacy = {
    ...app,
    id: "e".repeat(32),
    url: `https://${"e".repeat(32)}-trip.effect-agent.com`,
  };

  await seed(storageOwner, legacy, false);

  const restored = await fetchApp("/api/trip", legacy, {
    "x-trip-owner": memberOwner,
    "x-test-owner": memberOwner,
  });

  expect(restored.status).toBe(404);
  await restored.text();
  expect(await directory("/__address", { hostname: new URL(legacy.url).hostname })).toBeNull();

  const unknown = await runtime.dispatchFetch(
    `https://${"f".repeat(32)}-trip.effect-agent.com/api/trip?owner=${storageOwner}`,
  );

  expect(unknown.status).toBe(404);
  await unknown.text();
  const invalid = await runtime.dispatchFetch("https://nested.name-trip.effect-agent.com/");

  expect(invalid.status).toBe(404);
  await invalid.text();
});

it("refuses conflicting or malformed addresses and validates the saved app scope", async () => {
  await seed(storageOwner);
  const hostname = new URL(app.url).hostname;
  const before = await directory("/__address", { hostname });

  const collision = await directory("/__register", {
    owner: memberOwner,
    app: { ...app, id: "9".repeat(32) },
  });

  expect(collision).toEqual({ tag: "Failure" });
  expect(await directory("/__address", { hostname })).toEqual(before);

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
  await directory("/__address", { hostname: otherHostname, value: { version: 2 } });
  const corrupt = await fetchApp("/", mismatched);

  expect(corrupt.status).toBe(503);
  await corrupt.text();
  expect(await directory("/__address", { hostname: otherHostname })).toEqual({ version: 2 });
});

it("repairs interrupted address publication without changing either owner or legacy alias", async () => {
  let sequence = 100;

  for (const fault of ["failure", "defect", "interrupt"]) {
    for (const point of ["app-address:before-put", "app-address:after-put"]) {
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
        fault,
      });

      expect(interrupted).toEqual({ tag: "Failure" });
      expect(await directory("/__address", { hostname })).toEqual(
        point.endsWith("before-put")
          ? null
          : { version: 1, owner: memberOwner, hostname, appId: id, tripId: value.tripId },
      );
      // An address without a saved owner record cannot expose data.
      const uncommitted = await fetchApp("/api/trip", value);

      expect(uncommitted.status).toBe(404);
      await uncommitted.text();
      const retried = await directory("/__register", { owner: memberOwner, app: value });

      expect(retried).toEqual({ tag: "Success" });
      expect(
        await directory("/__address", { hostname: `${id}-trip.effect-agent.com` }),
      ).toMatchObject({
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

it("creates bounded readable names with stable suffixes and reserves colliding names atomically", async () => {
  expect(
    await directory("/__name", {
      title: "Tahoe Cabin Getaway",
      appId: "123456789abc".padEnd(32, "0"),
    }),
  ).toEqual({
    hostname: "tahoe-cabin-getaway-123456789abc-trip.effect-agent.com",
    name: "tahoe-cabin-getaway-123456789abc",
  });
  expect(
    await directory("/__name", {
      title: "Séjour à Montréal",
      appId: "123456789abd".padEnd(32, "0"),
    }),
  ).toEqual({
    hostname: "sejour-a-montreal-123456789abd-trip.effect-agent.com",
    name: "sejour-a-montreal-123456789abd",
  });

  const long = Schema.decodeUnknownSync(
    Schema.Struct({ hostname: Schema.String, name: Schema.String }),
  )(await directory("/__name", { title: "A very long trip name ".repeat(20), appId: app.id }));

  expect(long.hostname.split(".")[0]?.length).toBeLessThanOrEqual(63);
  expect(long.name).not.toContain("--");

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
