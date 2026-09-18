import type {
  WorkflowOperationError,
  WorkflowStep,
  WorkflowStepError,
} from "alchemy/Cloudflare/Workflows";
import type { Effect } from "effect";
import { Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, expectTypeOf, it } from "vite-plus/test";

import {
  type AppBuildRequest,
  type PlannerError,
  TripApp,
  TripAppBuildEvent,
} from "../src/domain.ts";
import type { SiteBuildBinding } from "../src/trip-app/bindings.ts";
import type { AppBuildBucket } from "../src/trip-app/bucket.ts";
import type {
  AppBuilder,
  buildTripApp,
  readBuild,
  recordBuildProgress,
  runSiteBuild,
  settleBuild,
} from "../src/trip-app/build.ts";
import type { AppRepository } from "../src/trip-app/repository.ts";
import type { AppSourceStore } from "../src/trip-app/source.ts";
import { alchemyRuntimeBundle } from "./fixtures/alchemy-bundle.ts";

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
    ...alchemyRuntimeBundle,
    stdin: {
      resolveDir: import.meta.dirname,
      loader: "ts",
      contents: `
    import { DurableObject, RpcTarget, WorkflowEntrypoint } from "cloudflare:workers";
    import { Cause, Deferred, Effect, Fiber, Layer, Schema } from "effect";
    import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
    import { Worker } from "alchemy/Cloudflare/Workers/Worker";
    import { makeWorkflowBridge, wrapWorkflowStep } from "alchemy/Cloudflare/Workflows/WorkflowBridge";
    import { task, WorkflowStep } from "alchemy/Cloudflare/Workflows";
    import { AppBuildBucket } from "../src/trip-app/bucket.ts";
    import { AppBuildRequest, PlannerError, TripApp, TripAppBuildEvent } from "../src/domain.ts";
    import { AppBuilder, AppBuilderLive, buildTripApp, readBuild, recordBuildProgress, runSiteBuild, settleBuild, buildPrefix } from "../src/trip-app/build.ts";
    import { AppBuildBucketLive, AppBuildSandboxLive, SiteBuildBinding, SiteBuildBindingLive } from "../src/trip-app/bindings.ts";
    import { AppSourceStore } from "../src/trip-app/source.ts";
    import { AppRepository } from "../src/trip-app/repository.ts";
    import { TripFailpoint } from "../src/server/trips.ts";
    let app = ${JSON.stringify(initial)};
    let compiles = 0, reads = 0, saves = 0, destroyed = 0, subscriptions = 0, mkdirCalls = 0;
    let point = "", mode = "", conflict = "", fault = "failure";
    const events = [];
    const commands = [];
    const output = [
      { path: "web/index.html", body: new TextEncoder().encode("<main>Trip</main>") },
      { path: "server/index.js", body: new TextEncoder().encode("export default {}") },
    ];
    const apps = Layer.succeed(AppRepository, {
      get: (tripId) => Effect.succeed(tripId === app.tripId ? app : null), getById: (appId) => Effect.succeed(appId === app.id ? app : null),
      save: (next, expected) => Effect.suspend(() => {
        saves++;
        if (conflict !== "") {
          const selected = conflict;
          if (selected !== "always") conflict = "";
          app = { ...app, revision: app.revision + 1, ...(selected === "newer" ? { sourceCommit: "d".repeat(40), pendingCommit: "d".repeat(40) } : {}), ...(selected === "progress" ? {buildProgress:[...(app.buildProgress??[]),{at:"2026-09-09T00:00:00Z",phase:"queued",message:"Concurrent progress"}]} : {}) };
          return Effect.fail(new PlannerError({ code: "conflict", message: "Injected concurrent revision" }));
        }
        if (expected !== app.revision) return Effect.fail(new PlannerError({ code: "conflict", message: "Revision changed" }));
        app = next; return Effect.succeed(next);
      }),
    });
    const source = Layer.succeed(AppSourceStore, { read: () => Effect.sync(() => { reads++; return [{ path: "index.ts", content: "export {}" }]; }), fork: () => Effect.die("unused"), commit: () => Effect.die("unused") });
    const builder = Layer.succeed(AppBuilder, { compile: () => Effect.suspend(() => {
      compiles++;
      if (mode === "compile-fail") return Effect.fail(new PlannerError({ code: "unavailable", message: "Fixture build failed" }));
      if (mode === "compile-defect") return Effect.die("Fixture build defect");
      return Effect.succeed(mode === "changed-output" ? output.map((file) => ({ ...file, body: new Uint8Array([1,2,3]) })) : output);
    }) });
    const failures = Layer.succeed(TripFailpoint, { hit: (candidate) => Effect.suspend(() => {
      events.push(candidate);
      if (candidate !== point) return Effect.void;
      point = "";
      return fault === "defect" ? Effect.die("Fixture mutation defect") : fault === "interrupt" ? Effect.interrupt : Effect.fail(new PlannerError({code:"storage", message:"Injected acknowledgement loss"}));
    }) });
    const storage = AppBuildBucketLive;
    const services = Layer.mergeAll(apps, source, builder, failures, storage);
    const workflow = {
      kind: "workflow",
      make: (env) => Effect.succeed((input) => Schema.decodeUnknownEffect(AppBuildRequest)(input).pipe(
        Effect.flatMap(runSiteBuild), Effect.provide(services), Effect.provideService(WorkerEnvironment, env), Effect.orDie,
      )),
    };
    const failedWorkflow = {
      kind: "workflow",
      make: (env) => Effect.succeed((input) => Schema.decodeUnknownEffect(AppBuildRequest)(input).pipe(
        Effect.flatMap((request) => task("Expected failure", Effect.fail(new PlannerError({code: "unavailable", message: "Fixture compilation failed"})), {retries: {limit: 0, delay: "1 millisecond"}}).pipe(
          Effect.catchTag("WorkflowStepError", (error) => task("Record failed build", settleBuild(request, error.message)).pipe(
            Effect.as({failureTag: error._tag, operation: error.operation}),
          )),
        )),
        Effect.provide(services), Effect.provideService(WorkerEnvironment, env), Effect.orDie,
      )),
    };
    const workflowEntrypoint = Worker("BuildFixture", {main: import.meta.url}, Effect.gen(function* () {
      const worker = yield* Worker;
      yield* worker.export("TestBuild", workflow);
      yield* worker.export("FailedBuild", failedWorkflow);
      return {fetch: Effect.die("Unused workflow ingress")};
    }));
    const bridge = makeWorkflowBridge(WorkflowEntrypoint, {
      entrypoint: workflowEntrypoint, stack: {name: "trip-app-build-test", stage: "test"},
    });
    export class TestBuild extends bridge("TestBuild") {}
    export class FailedBuild extends bridge("FailedBuild") {}
    class Subscription extends RpcTarget {
      delivered = false; resolve;
      async next() {
        if (mode === "hang") return new Promise((resolve) => { this.resolve = resolve; });
        if (this.delivered) return { done: true };
        this.delivered = true;
        return { done: false, value: { type: "terminal", state: "exited", cursor: "1", timestamp: "2026-09-09T00:00:00Z", exit: { code: mode === "command-fail" ? 1 : 0, timedOut: false } } };
      }
      async cancel() { this.resolve?.({done:true}); }
      [Symbol.dispose]() { subscriptions++; this.resolve?.({done:true}); }
    }
    class Process extends RpcTarget {
      async openLogs() { return new Subscription(); }
      async kill() {}
      async status() { return { state: "exited", id: "process", pid: 1, command: ["vp"], startedAt: "2026-09-09T00:00:00Z", endedAt: "2026-09-09T00:00:00Z", exit: { code: 0, timedOut: false } }; }
    }
    export class FakeSandbox extends DurableObject {
      async configure() {}
      async exec(command, options) { commands.push({command,options,phase:app.buildProgress?.at(-1)?.phase??null}); return { id: "process", pid: 1, capability: new Process() }; }
      async mkdir(path) {
        mkdirCalls++;
        if (mode === "capacity-wait" || (mode === "capacity-once" && mkdirCalls === 1))
          throw new Error("Maximum number of running container instances exceeded. PRIVATE_SANDBOX_FAILURE");
        if (mode === "startup-fail") throw new Error("PRIVATE_SANDBOX_FAILURE");
        return { success: true, path, recursive: true, timestamp: "2026-09-09T00:00:00Z" };
      }
      async writeFile(path) { return {success:true,path,timestamp:"2026-09-09T00:00:00Z"}; }
      async listFiles(path) { return { success: true, path, count: 2, timestamp:"2026-09-09T00:00:00Z", files: output.map((file) => ({name:file.path,absolutePath:path+"/"+file.path,relativePath:file.path,type:mode === "symlink" ? "symlink":"file",size:file.body.byteLength,modifiedAt:"2026-09-09T00:00:00Z",mode:"0644",permissions:{readable:true,writable:true,executable:false}})) }; }
      async readFile(path) { const file = output.find((file) => path.endsWith(file.path)); return {success:true,path,content:btoa(String.fromCharCode(...file.body)),encoding:"base64",timestamp:"2026-09-09T00:00:00Z"}; }
      async destroy() { destroyed++; if(mode === "cleanup-fail") throw new Error("PRIVATE_SANDBOX_FAILURE"); }
    }
    export default { async fetch(http, env) {
      const input = await http.json();
      const request = Schema.decodeUnknownSync(AppBuildRequest)(input.request);
      if (input.kind === "reset") {
        app = Schema.decodeUnknownSync(TripApp)(input.app);
        compiles=0;reads=0;saves=0;destroyed=0;subscriptions=0;mkdirCalls=0;events.length=0;commands.length=0;point="";mode="";conflict="";fault="failure";
        const listed = await env.APP_BUILDS.list(); if (listed.objects.length) await env.APP_BUILDS.delete(listed.objects.map((object) => object.key));
      }
      if(input.point !== undefined) point=input.point;
      if(input.mode !== undefined) mode=input.mode;
      if(input.conflict !== undefined) conflict=input.conflict;
      if(input.fault !== undefined) fault=input.fault;
      let action = Effect.void;
      if (input.kind === "build") action = buildTripApp(request);
      if (input.kind === "build-sdk") action = buildTripApp(request).pipe(Effect.provide(AppBuilderLive.pipe(Layer.provide(AppBuildSandboxLive))));
      if (input.kind === "settle") action = settleBuild(request, input.error ?? null);
      if (input.kind === "progress") action = Effect.forEach(Schema.decodeUnknownSync(Schema.Array(TripAppBuildEvent))(input.updates), (event) => recordBuildProgress(request, event), {discard:true});
      if (input.kind === "corrupt") action = Effect.promise(() => env.APP_BUILDS.put(buildPrefix(request.appId, request.commitId)+"manifest.json", input.value));
      if (input.kind === "sdk") action = Effect.flatMap(AppBuilder, (builder) => builder.compile(request, [{path:"index.ts",content:"export {}"}])).pipe(
        Effect.provide(AppBuilderLive.pipe(Layer.provide(AppBuildSandboxLive))),
        ...(mode === "hang" || mode === "capacity-wait" ? [Effect.timeoutOrElse({duration:"100 millis",orElse:()=>Effect.fail(new PlannerError({code:"unavailable",message:"Fixture deadline"}))})] : []),
      );
      if (input.kind === "workflow") action = Effect.flatMap(SiteBuildBinding, (workflow) => workflow.create({params:request,id:input.id})).pipe(Effect.provide(SiteBuildBindingLive));
      if (input.kind === "status") action = Effect.flatMap(SiteBuildBinding, (workflow) => workflow.get(input.id).pipe(Effect.flatMap((instance) => instance.status()))).pipe(Effect.provide(SiteBuildBindingLive));
      if (input.kind === "workflow-rollback") action = Effect.gen(function* () {
        let saved;
        const bridge = wrapWorkflowStep({do: (name, config, callback, rollback) => {
          saved = rollback.rollback;
          return callback({step: {name, count: 1}, attempt: 1, config});
        }});
        const output = yield* task("Successful step", Effect.succeed("step output"), {
          retries: {limit: 0, delay: "1 millisecond"},
          rollback: ({output}) => Effect.acquireUseRelease(
            Effect.sync(() => events.push("rollback-acquired:" + output)),
            () => Effect.sleep("10 millis"),
            () => Effect.sleep("10 millis").pipe(Effect.andThen(Effect.sync(() => events.push("rollback-released")))),
          ),
        }).pipe(Effect.provideService(WorkflowStep, bridge));
        events.push("step-returned");
        yield* Effect.promise(() => saved({error: new Error("Subsequent workflow failure"), output}));
        events.push("rollback-returned");
      });
      if (input.kind === "workflow-interrupt") action = Effect.gen(function* () {
        const entered = yield* Deferred.make();
        const held = Effect.acquireUseRelease(
          Effect.sync(() => events.push("callback-acquired")),
          () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          () => Effect.sleep("25 millis").pipe(Effect.andThen(Effect.sync(() => events.push("callback-released")))),
        );
        const bridge = wrapWorkflowStep({do: (name, config, callback, rollback) => mode === "rollback"
          ? rollback.rollback({error: new Error("Native rollback"), output: undefined})
          : callback({step: {name, count: 1}, attempt: 1, config})});
        const fiber = yield* task("Interrupted task", mode === "rollback" ? Effect.void : held, {
          retries: {limit: 0, delay: "1 millisecond"},
          ...(mode === "rollback" ? {rollback: () => held} : {}),
        }).pipe(Effect.provideService(WorkflowStep, bridge), Effect.forkChild);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        events.push("interrupt-returned");
      });
      if (input.kind === "r2-body") action = Effect.gen(function* () {
        const bucket = yield* AppBuildBucket;
        yield* bucket.put("body-lifecycle", "native R2");
        const object = yield* bucket.get("body-lifecycle");
        if (object === null) return yield* Effect.die("Missing fixture object");
        const before = object.bodyUsed;
        const body = yield* object.text();
        return {before, body, after: object.bodyUsed};
      });
      if (input.failure) action = action.pipe(Effect.provideService(WorkerEnvironment, {...env, SITE_BUILD: env.FAILURE_BUILD}));
      const exit = await Effect.runPromise(action.pipe(Effect.provide(services),Effect.provideService(WorkerEnvironment,env),Effect.exit));
      const manifest = await Effect.runPromise(readBuild(request.appId,request.commitId).pipe(Effect.provide(storage),Effect.provideService(WorkerEnvironment,env),Effect.result));
      return Response.json({exit:exit._tag === "Success" ? {tag:"Success",...(["status","r2-body"].includes(input.kind)?{value:exit.value}:{})}:{tag:"Failure",error:Cause.pretty(exit.cause)},app,compiles,reads,saves,destroyed,subscriptions,mkdirCalls,events,commands,manifest:manifest._tag === "Success" ? manifest.success : null});
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
      durableObjects: { APP_SANDBOX: { className: "FakeSandbox", useSQLite: true } },
      workflows: {
        SITE_BUILD: { name: "trip-app-build-test", className: "TestBuild" },
        FAILURE_BUILD: { name: "trip-app-failed-build-test", className: "FailedBuild" },
      },
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
  saves: Schema.Number,
  destroyed: Schema.Number,
  subscriptions: Schema.Number,
  mkdirCalls: Schema.Number,
  events: Schema.Array(Schema.String),
  commands: Schema.Array(
    Schema.Struct({
      command: Schema.Array(Schema.String),
      options: Schema.Struct({ cwd: Schema.String, timeout: Schema.Number }),
      phase: Schema.NullOr(TripAppBuildEvent.fields.phase),
    }),
  ),
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

const waitForWorkflow = async (id: string, failure = false) => {
  let last = await call("status", { id, failure });

  for (let attempt = 0; attempt < 100; attempt++) {
    const state = Schema.decodeUnknownSync(Schema.Struct({ status: Schema.String }))(
      last.exit.value,
    );

    if (state.status === "complete" || state.status === "errored") break;
    await new Promise((resolve) => setTimeout(resolve, 20));
    last = await call("status", { id, failure });
  }

  return last;
};

const reset = () => call("reset", { app: initial });

const update = (phase: TripAppBuildEvent["phase"], message: string): TripAppBuildEvent => ({
  at: "2026-09-09T00:00:00Z",
  phase,
  message,
});

it("persists the real Sandbox phases before each command and activates only after uploading", async () => {
  await call("reset", { app: { ...initial, buildProgress: [update("queued", "Waiting")] } });
  const built = await call("build-sdk");

  expect(built.exit.tag).toBe("Success");
  expect(built.app.buildProgress?.map(({ phase }) => phase)).toEqual([
    "queued",
    "starting",
    "installing",
    "checking",
    "compiling",
    "uploading",
    "ready",
  ]);
  expect(built.commands.map(({ phase }) => phase)).toEqual(["installing", "checking", "compiling"]);
  expect(built.app.activeCommit).toBe(request.commitId);
  expect(built.destroyed).toBe(1);
  expect(built.manifest?.files).toHaveLength(2);
}, 30_000);

it("deduplicates repeated progress, bounds its history, and rereads conflicts without losing another update", async () => {
  await reset();
  const installing = update("installing", "Installing dependencies");
  const first = await call("progress", { updates: [installing], conflict: "progress" });

  expect(first.exit.tag).toBe("Success");
  expect(first.saves).toBe(2);
  expect(first.app.revision).toBe(initial.revision + 2);
  expect(first.app.buildProgress?.map(({ message }) => message)).toEqual([
    "Concurrent progress",
    "Installing dependencies",
  ]);
  const duplicate = await call("progress", { updates: [installing, installing] });

  expect(duplicate.app).toEqual(first.app);
  expect(duplicate.saves).toBe(first.saves);
  const updates = Array.from({ length: 42 }, (_, index) => update("checking", `Check ${index}`));
  const bounded = await call("progress", { updates });

  expect(bounded.app.buildProgress).toHaveLength(40);
  expect(bounded.app.buildProgress?.[0]?.message).toBe("Check 2");
  expect(bounded.app.buildProgress?.at(-1)?.message).toBe("Check 41");
  const ready = await call("settle");

  expect(ready.app.buildProgress).toHaveLength(40);
  expect(ready.app.buildProgress?.at(-1)?.phase).toBe("ready");
  expect((await call("progress", { updates: [installing] })).app).toEqual(ready.app);
  await reset();
  const exhausted = await call("progress", { updates: [installing], conflict: "always" });

  expect(exhausted.exit.tag).toBe("Failure");
  expect(exhausted.saves).toBe(5);
  expect(exhausted.app.buildProgress).toBeUndefined();
});

it("excludes progress from stale jobs before saving, including a source change during CAS", async () => {
  const updates = [update("compiling", "Building app")];

  for (const mismatch of [
    { appId: "d".repeat(32) },
    { tripId: "different-trip" },
    { repoName: "different-repo" },
    { commitId: "d".repeat(40) },
  ]) {
    await reset();
    const stale = await call("progress", { request: { ...request, ...mismatch }, updates });

    expect(stale.exit.tag).toBe("Success");
    expect(stale.saves).toBe(0);
    expect(stale.app).toEqual(initial);
  }
  for (const app of [
    { ...initial, pendingCommit: "d".repeat(40) },
    { ...initial, sourceCommit: "d".repeat(40) },
    { ...initial, pendingCommit: null },
  ]) {
    await call("reset", { app });
    const stale = await call("progress", { updates });

    expect(stale.saves).toBe(0);
    expect(stale.app).toEqual(app);
  }
  await reset();
  const conflicted = await call("progress", { updates, conflict: "newer" });

  expect(conflicted.exit.tag).toBe("Success");
  expect(conflicted.saves).toBe(1);
  expect(conflicted.app.buildProgress).toBeUndefined();
  expect(conflicted.app.pendingCommit).toBe("d".repeat(40));
});

it("closes the Sandbox when progress cannot be saved and never starts an unreported command", async () => {
  await reset();
  const blocked = await call("sdk", { conflict: "always" });

  expect(blocked.exit.tag).toBe("Failure");
  expect(blocked.saves).toBe(5);
  expect(blocked.destroyed).toBe(1);
  expect(blocked.commands).toEqual([]);
  expect(blocked.app.buildProgress).toBeUndefined();
}, 30_000);

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
  expect(failed.app.buildProgress?.map(({ phase }) => phase)).toEqual(["starting", "uploading"]);
  expect(retried.app.buildProgress?.map(({ phase }) => phase)).toEqual([
    "starting",
    "uploading",
    "starting",
    "ready",
  ]);
  expect(retried.events.indexOf("app-build:after-assets")).toBeLessThan(
    retried.events.indexOf("app-build:before-manifest"),
  );
  expect((await call("build")).app.versions).toHaveLength(1);
}, 30_000);

it("keeps partial builds inactive, rejects conflicting asset bytes, and fails closed on invalid manifests", async () => {
  for (const fault of ["failure", "defect", "interrupt"]) {
    await reset();
    const result = await call("build", { point: "app-build:after-assets", fault });

    expect(result.exit.tag).toBe("Failure");
    expect(result.manifest).toBeNull();
    expect(result.app.activeCommit).toBe(initial.activeCommit);
  }
  const changed = await call("build", { mode: "changed-output" });

  expect(changed.exit.tag).toBe("Failure");
  expect(changed.manifest).toBeNull();
  expect(changed.app.activeCommit).toBe(initial.activeCommit);
  await call("corrupt", { value: '{"version":99,"PRIVATE_TOKEN":"secret"}' });
  const invalid = await call("build", { mode: "" });

  expect(invalid.exit.tag).toBe("Failure");
  expect(invalid.exit.error).not.toContain("PRIVATE_TOKEN");
}, 30_000);

it("re-reads CAS conflicts, refuses stale activation, and preserves the active version on failure", async () => {
  await reset();
  const retried = await call("settle", { conflict: "metadata" });

  expect(retried.saves).toBe(2);
  expect(retried.app.status).toBe("ready");
  await reset();
  const stale = await call("settle", { conflict: "newer" });

  expect(stale.saves).toBe(1);
  expect(stale.app).toMatchObject({
    status: "building",
    activeCommit: initial.activeCommit,
    pendingCommit: "d".repeat(40),
  });
  await reset();
  const failed = await call("settle", { error: "Compilation failed" });

  expect(failed.app).toMatchObject({
    status: "failed",
    activeCommit: initial.activeCommit,
    pendingCommit: null,
    error: "Compilation failed",
  });
  await reset();
  const exhausted = await call("settle", { conflict: "always" });

  expect(exhausted.exit.tag).toBe("Failure");
  expect(exhausted.saves).toBe(5);
  expect(exhausted.app.status).toBe("building");
});

it("uses the actual Sandbox SDK through the Alchemy environment and cleans up success, command failure, timeout, and invalid output", async () => {
  for (const mode of ["", "command-fail", "hang", "symlink", "cleanup-fail"]) {
    await reset();
    const result = await call("sdk", { mode });

    expect(result.exit.tag).toBe(mode === "" ? "Success" : "Failure");
    expect(result.destroyed).toBe(1);
    expect(result.subscriptions).toBeGreaterThanOrEqual(1);
    expect(result.exit.error ?? "").not.toContain("PRIVATE_SANDBOX_FAILURE");
    expect(result.commands[0]).toEqual({
      command: ["vp", "install", "--ignore-scripts"],
      options: { cwd: "/workspace/app", timeout: 240000 },
      phase: "installing",
    });
    expect(result.commands.map(({ command }) => command)).toEqual(
      mode === "command-fail" || mode === "hang"
        ? [["vp", "install", "--ignore-scripts"]]
        : [
            ["vp", "install", "--ignore-scripts"],
            ["vp", "check", "--no-fmt"],
            ["vp", "run", "build"],
          ],
    );
    expect(result.app.buildProgress?.map(({ phase }) => phase)).toEqual(
      mode === "command-fail" || mode === "hang"
        ? ["installing"]
        : ["installing", "checking", "compiling"],
    );
  }
}, 30_000);

it("waits for temporary builder capacity without replaying build commands", async () => {
  await reset();
  const result = await call("sdk", { mode: "capacity-once" });

  expect(result.exit.tag).toBe("Success");
  expect(result.mkdirCalls).toBe(2);
  expect(result.destroyed).toBe(1);
  expect(result.commands).toHaveLength(3);
  expect(result.app.buildProgress?.map(({ phase }) => phase)).toEqual([
    "queued",
    "installing",
    "checking",
    "compiling",
  ]);
  expect(result.app.buildProgress?.[0]?.message).toBe("Waiting for an available app builder");
}, 30_000);

it("does not retry unknown startup failures and releases capacity waiters on interruption", async () => {
  await reset();
  const failed = await call("sdk", { mode: "startup-fail" });

  expect(failed.exit.tag).toBe("Failure");
  expect(failed.exit.error).toContain("mkdir operation failed");
  expect(failed.exit.error).not.toContain("PRIVATE_SANDBOX_FAILURE");
  expect(failed.mkdirCalls).toBe(1);
  expect(failed.commands).toEqual([]);
  expect(failed.destroyed).toBe(1);
  await reset();
  const interrupted = await call("sdk", { mode: "capacity-wait" });

  expect(interrupted.exit.tag).toBe("Failure");
  expect(interrupted.exit.error).toContain("Fixture deadline");
  expect(interrupted.mkdirCalls).toBe(1);
  expect(interrupted.commands).toEqual([]);
  expect(interrupted.destroyed).toBe(1);
  expect(interrupted.app.buildProgress?.map(({ phase }) => phase)).toEqual(["queued"]);
}, 30_000);

it("runs the Alchemy Workflow bridge against real R2 and validates its result", async () => {
  await reset();
  const id = "workflow-build";

  expect((await call("workflow", { id })).exit.tag).toBe("Success");
  const last = await waitForWorkflow(id);

  expect(last.exit.value, JSON.stringify(last.exit.value)).toMatchObject({
    status: "complete",
    output: { commitId: request.commitId },
  });
  expect(last.app.activeCommit).toBe(request.commitId);
  expect(last.manifest?.files).toHaveLength(2);
  expectTypeOf<Effect.Error<ReturnType<typeof buildTripApp>>>().toEqualTypeOf<PlannerError>();
  expectTypeOf<
    Effect.Error<ReturnType<AppBuilder["Service"]["compile"]>>
  >().toEqualTypeOf<PlannerError>();
  expectTypeOf<
    Effect.Services<ReturnType<AppBuilder["Service"]["compile"]>>
  >().toEqualTypeOf<AppRepository>();
  expectTypeOf<Effect.Services<ReturnType<typeof readBuild>>>().toEqualTypeOf<AppBuildBucket>();
  expectTypeOf<Effect.Error<ReturnType<typeof readBuild>>>().toEqualTypeOf<PlannerError>();
  expectTypeOf<Effect.Services<ReturnType<typeof buildTripApp>>>().toEqualTypeOf<
    AppBuildBucket | AppBuilder | AppSourceStore | AppRepository
  >();
  expectTypeOf<Effect.Services<ReturnType<typeof settleBuild>>>().toEqualTypeOf<AppRepository>();
  expectTypeOf<
    Effect.Services<ReturnType<typeof recordBuildProgress>>
  >().toEqualTypeOf<AppRepository>();
  expectTypeOf<
    Effect.Error<ReturnType<typeof recordBuildProgress>>
  >().toEqualTypeOf<PlannerError>();
}, 30_000);

it("recovers a native Workflow step rejection through its typed error and persists the failure step", async () => {
  await reset();
  const id = "workflow-failure";

  expect((await call("workflow", { id, failure: true })).exit.tag).toBe("Success");
  const last = await waitForWorkflow(id, true);

  expect(last.exit.value, JSON.stringify(last.exit.value)).toMatchObject({
    status: "complete",
    output: { failureTag: "WorkflowStepError", operation: "Expected failure" },
  });
  expect(last.app).toMatchObject({
    status: "failed",
    pendingCommit: null,
    activeCommit: initial.activeCommit,
  });
  expect(last.app.error).toContain("Fixture compilation failed");
  expect(last.app.buildProgress?.at(-1)?.phase).toBe("failed");
  expectTypeOf<Effect.Error<ReturnType<typeof runSiteBuild>>>().toEqualTypeOf<WorkflowStepError>();
  expectTypeOf<Effect.Services<ReturnType<typeof runSiteBuild>>>().toEqualTypeOf<
    WorkflowStep | AppBuildBucket | AppBuilder | AppSourceStore | AppRepository
  >();
  expectTypeOf<
    Effect.Error<ReturnType<SiteBuildBinding["Service"]["create"]>>
  >().toEqualTypeOf<WorkflowOperationError>();
});

it("tracks the native R2 body's consumed state after reading it", async () => {
  await reset();
  expect((await call("r2-body")).exit).toEqual({
    tag: "Success",
    value: { before: false, body: "native R2", after: true },
  });
});

it("joins interrupted Workflow callback and rollback finalizers before returning", async () => {
  for (const mode of ["callback", "rollback"]) {
    await reset();
    const result = await call("workflow-interrupt", { mode });

    expect(result.exit.tag).toBe("Success");
    expect(result.events).toEqual(["callback-acquired", "callback-released", "interrupt-returned"]);
  }
});

it("runs a saved native rollback after the successful Workflow task has returned", async () => {
  await reset();
  const result = await call("workflow-rollback");

  expect(result.exit.tag).toBe("Success");
  expect(result.events).toEqual([
    "step-returned",
    "rollback-acquired:step output",
    "rollback-released",
    "rollback-returned",
  ]);
});
