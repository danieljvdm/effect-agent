import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import { type AppBuildRequest, TripApp } from "../src/domain.ts";

const request: AppBuildRequest = {
  owner: "test-owner",
  appId: "a".repeat(32),
  tripId: "lisbon",
  repoName: "trip-app-test",
  commitId: "b".repeat(40),
  label: "Trip app",
};

const initial: TripApp = {
  id: request.appId,
  tripId: request.tripId,
  revision: 1,
  repoName: request.repoName,
  url: "https://app.example",
  sourceCommit: request.commitId,
  activeCommit: "c".repeat(40),
  pendingCommit: request.commitId,
  status: "building",
  error: null,
  updatedAt: "2026-09-09T00:00:00Z",
  versions: [],
};

let runtime: Miniflare;

beforeAll(async () => {
  const bundle = await build({
    stdin: {
      resolveDir: import.meta.dirname,
      loader: "ts",
      contents: `
    import { Cause, Effect, Layer, Schema } from "effect";
    import { WorkerEnvironment } from "effect-cf";
    import { AppBuildRequest, PlannerError, TripApp } from "../src/domain.ts";
    import { AppBuilder, buildTripApp, readBuild, settleBuild } from "../src/trip-app/build.ts";
    import { AppBuildBucketLive } from "../src/trip-app/bindings.ts";
    import { AppSourceStore } from "../src/trip-app/source.ts";
    import { AppRepository } from "../src/trip-app/repository.ts";
    import { TripFailpoint } from "../src/server/trips.ts";
    let app = ${JSON.stringify(initial)};
    let compiles = 0, reads = 0;
    let point = "", mode = "", conflict = "";
    const output = [
      { path: "web/index.html", body: new TextEncoder().encode("<main>Trip</main>") },
      { path: "server/index.js", body: new TextEncoder().encode("export default {}") },
    ];
    const apps = Layer.succeed(AppRepository, {
      get: (tripId) => Effect.succeed(tripId === app.tripId ? app : null), getById: (appId) => Effect.succeed(appId === app.id ? app : null),
      save: (next, expected) => Effect.suspend(() => {
        if (conflict !== "") {
          const selected = conflict;
          conflict = "";
          app = { ...app, revision: app.revision + 1, ...(selected === "newer" ? { sourceCommit: "d".repeat(40), pendingCommit: "d".repeat(40) } : {}) };
          return Effect.fail(new PlannerError({ code: "conflict", message: "Injected concurrent revision" }));
        }
        if (expected !== app.revision) return Effect.fail(new PlannerError({ code: "conflict", message: "Revision changed" }));
        app = next; return Effect.succeed(next);
      }),
    });
    const source = Layer.succeed(AppSourceStore, { read: () => Effect.sync(() => { reads++; return [{ path: "index.ts", content: "export {}" }]; }), fork: () => Effect.die("unused"), commit: () => Effect.die("unused") });
    const builder = Layer.succeed(AppBuilder, { compile: () => Effect.suspend(() => {
      compiles++;
      return Effect.succeed(mode === "changed-output" ? output.map((file) => ({ ...file, body: new Uint8Array([1,2,3]) })) : output);
    }) });
    const failures = Layer.succeed(TripFailpoint, { hit: (candidate) => Effect.suspend(() => {
      if (candidate !== point) return Effect.void;
      point = "";
      return Effect.fail(new PlannerError({code:"storage", message:"Injected acknowledgement loss"}));
    }) });
    const storage = AppBuildBucketLive;
    const services = Layer.mergeAll(apps, source, builder, failures, storage);
    export default { async fetch(http, env) {
      const input = await http.json();
      const request = Schema.decodeUnknownSync(AppBuildRequest)(input.request);
      if (input.kind === "reset") {
        app = Schema.decodeUnknownSync(TripApp)(input.app);
        compiles=0;reads=0;point="";mode="";conflict="";
        const listed = await env.APP_BUILDS.list(); if (listed.objects.length) await env.APP_BUILDS.delete(listed.objects.map((object) => object.key));
      }
      if(input.point !== undefined) point=input.point;
      if(input.mode !== undefined) mode=input.mode;
      if(input.conflict !== undefined) conflict=input.conflict;
      let action = Effect.void;
      if (input.kind === "build") action = buildTripApp(request);
      if (input.kind === "settle") action = settleBuild(request, input.error ?? null);
      const exit = await Effect.runPromise(action.pipe(Effect.provide(services),Effect.provideService(WorkerEnvironment,env),Effect.exit));
      const manifest = await Effect.runPromise(readBuild(request.appId,request.commitId).pipe(Effect.provide(storage),Effect.provideService(WorkerEnvironment,env),Effect.result));
      return Response.json({exit:exit._tag === "Success" ? {tag:"Success"}:{tag:"Failure",error:Cause.pretty(exit.cause)},app,compiles,reads,manifest:manifest._tag === "Success" ? manifest.success : null});
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

  if (!output) throw new Error("Missing build fixture");
  runtime = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: output.text,
      modulesRoot: "/",
      compatibilityDate: "2026-07-01",
      compatibilityFlags: ["nodejs_compat"],
      r2Buckets: ["APP_BUILDS"],
    }),
  );
});
afterAll(async () => {
  await runtime?.dispose();
});

const Result = Schema.Struct({
  exit: Schema.Struct({
    tag: Schema.Literals(["Success", "Failure"]),
    error: Schema.optionalKey(Schema.String),
    value: Schema.optionalKey(Schema.Unknown),
  }),
  app: TripApp,
  compiles: Schema.Number,
  reads: Schema.Number,
  manifest: Schema.NullOr(
    Schema.Struct({
      commitId: Schema.String,
      files: Schema.Array(Schema.Struct({ path: Schema.String, sha256: Schema.String })),
    }),
  ),
});

const call = async (kind: string, extra: Record<string, unknown> = {}) => {
  const response = await runtime.dispatchFetch("http://build/", {
    method: "POST",
    body: JSON.stringify({ kind, request, ...extra }),
  });

  if (!response.ok) throw new Error(await response.text());

  return Schema.decodeUnknownSync(Result)(await response.json());
};

const reset = () => call("reset", { app: initial });

it("publishes the manifest last and reuses a completed build after an acknowledgement loss", async () => {
  await reset();
  const failed = await call("build", { point: "app-build:after-manifest" });

  expect(failed.exit.tag).toBe("Failure");
  expect(failed.app.activeCommit).toBe(initial.activeCommit);
  expect(failed.manifest?.commitId).toBe(request.commitId);
  const retried = await call("build");

  expect(retried.exit.tag).toBe("Success");
  expect(retried.compiles).toBe(1);
  expect(retried.reads).toBe(1);
  expect(retried.app).toMatchObject({
    status: "ready",
    activeCommit: request.commitId,
    pendingCommit: null,
  });
  expect(retried.app.versions).toHaveLength(1);
  expect((await call("build")).app.versions).toHaveLength(1);
}, 30_000);

it("keeps partial builds inactive and rejects conflicting immutable asset bytes", async () => {
  {
    await reset();
    const result = await call("build", { point: "app-build:after-assets" });

    expect(result.exit.tag).toBe("Failure");
    expect(result.manifest).toBeNull();
    expect(result.app.activeCommit).toBe(initial.activeCommit);
  }
  const changed = await call("build", { mode: "changed-output" });

  expect(changed.exit.tag).toBe("Failure");
  expect(changed.manifest).toBeNull();
  expect(changed.app.activeCommit).toBe(initial.activeCommit);
}, 30_000);

it("refuses stale activation when newer source arrives during a CAS conflict", async () => {
  await reset();
  const stale = await call("settle", { conflict: "newer" });

  expect(stale.app).toMatchObject({
    status: "building",
    activeCommit: initial.activeCommit,
    pendingCommit: "d".repeat(40),
  });
});
