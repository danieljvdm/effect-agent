import { getSandbox, type Sandbox, type SandboxCommand } from "@cloudflare/sandbox";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { task, type WorkflowExport } from "alchemy/Cloudflare/Workflows";
import { Context, DateTime, Duration, Effect, Layer, Option, Schedule, Schema } from "effect";

import {
  AppBuildRequest,
  AppCommit,
  AppFile,
  AppFilePath,
  AppId,
  PlannerError,
  TripApp,
  TripAppBuildEvent,
} from "../domain.ts";
import { plannerEnvironment } from "../server/alchemy.ts";
import { TripFailpoint } from "../server/trips.ts";
import { publishTripAppAddress } from "./addresses.ts";
import { AppBuildBucketLive, AppBuildSandbox, AppBuildSandboxLive } from "./bindings.ts";
import { AppBuildBucket } from "./bucket.ts";
import { appRepositoryForOwner, callAppRepository } from "./remote.ts";
import { AppRepository } from "./repository.ts";
import { AppSourceStore, appSourceLayer } from "./source.ts";

const MAX_FILE = 4 * 1024 * 1024;
const MAX_BUILD = 24 * 1024 * 1024;
const failed = (message: string) => new PlannerError({ code: "unavailable", message });
const badManifest = () => failed("Invalid app build manifest.");

class BuilderCapacity extends Schema.TaggedError<BuilderCapacity>()("BuilderCapacity", {}) {}

const operationFailure = (operation: string) =>
  failed(`The app builder's ${operation} operation failed. Retry the build.`);

// The SDK owns RPC subscriptions and propagates cancellation through its AbortSignal.
const sandboxOperation = <A>(operation: string, run: (signal: AbortSignal) => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: () => operationFailure(operation) });

const outputPath = AppFilePath.check(
  Schema.makeFilter(
    (path) =>
      (path.startsWith("web/") || path === "server/index.js") &&
      path
        .split("/")
        .every(
          (part) => part !== "" && ![".git", "node_modules", "dist"].includes(part.toLowerCase()),
        ),
  ),
);

const digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

const BuildFile = Schema.Struct({
  path: outputPath,
  bytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_FILE)),
  contentType: Schema.String,
  sha256: digest,
});

const completeTree = (files: ReadonlyArray<{ readonly path: string; readonly bytes: number }>) =>
  new Set(files.map((file) => file.path)).size === files.length &&
  files.some((file) => file.path === "web/index.html") &&
  files.some((file) => file.path === "server/index.js") &&
  files.reduce((size, file) => size + file.bytes, 0) <= MAX_BUILD;

export const BuildManifest = Schema.Struct({
  version: Schema.Literal(1),
  appId: AppId,
  commitId: AppCommit,
  files: Schema.Array(BuildFile).check(Schema.isMaxLength(200), Schema.makeFilter(completeTree)),
});

export type BuildManifest = typeof BuildManifest.Type;

export const buildPrefix = (appId: string, commitId: string) =>
  `auth-apps/v1/${appId}/${commitId}/`;

const buildIdentity = Schema.Struct({ appId: AppId, commitId: AppCommit });
const OutputFile = Schema.Struct({ path: outputPath, body: Schema.Uint8Array });

const OutputFiles = Schema.Array(OutputFile).check(
  Schema.isMaxLength(200),
  Schema.makeFilter(
    (files) =>
      files.every((file) => file.body.byteLength <= MAX_FILE) &&
      completeTree(files.map((file) => ({ path: file.path, bytes: file.body.byteLength }))),
  ),
);

const SourceFiles = Schema.Array(AppFile).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(100),
  Schema.makeFilter((files) => {
    const encoder = new TextEncoder();

    return (
      new Set(files.map((file) => file.path)).size === files.length &&
      files.every(
        (file) =>
          file.path
            .split("/")
            .every(
              (part) =>
                part !== "" && ![".git", "node_modules", "dist"].includes(part.toLowerCase()),
            ) && encoder.encode(file.content).byteLength <= 128 * 1024,
      ) &&
      files.reduce((sum, file) => sum + encoder.encode(file.content).byteLength, 0) <=
        2 * 1024 * 1024
    );
  }),
);

export class AppBuilder extends Context.Service<
  AppBuilder,
  {
    /** Persist each phase in the active repository before executing its build command. */
    readonly compile: (
      request: AppBuildRequest,
      files: ReadonlyArray<AppFile>,
    ) => Effect.Effect<ReadonlyArray<typeof OutputFile.Type>, PlannerError, AppRepository>;
  }
>()("trip-app/AppBuilder") {}

const contentType = (path: string) => {
  if (path.endsWith(".html")) return "text/html; charset=utf-8";
  if (path.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (path.endsWith(".css")) return "text/css; charset=utf-8";
  if (path.endsWith(".svg")) return "image/svg+xml";
  if (path.endsWith(".png")) return "image/png";
  if (path.endsWith(".webp")) return "image/webp";
  if (path.endsWith(".jpg") || path.endsWith(".jpeg")) return "image/jpeg";
  if (path.endsWith(".woff2")) return "font/woff2";
  if (path.endsWith(".json")) return "application/json";

  return "application/octet-stream";
};

/** Each attempt owns a fresh container. No host credentials or bindings enter it. */
export const AppBuilderLive = Layer.effect(
  AppBuilder,
  Effect.gen(function* () {
    const namespace = yield* AppBuildSandbox;

    const compile = Effect.fn("AppBuilder.compile")(
      function* (input: AppBuildRequest, source: ReadonlyArray<AppFile>) {
        const request = yield* Schema.decodeEffect(AppBuildRequest)(input).pipe(
          Effect.mapError(() => failed("Invalid app build request.")),
        );

        const files = yield* Schema.decodeEffect(SourceFiles)(source).pipe(
          Effect.mapError(() => failed("Invalid app source.")),
        );

        const prefix = yield* Schema.decodeEffect(
          Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9-]{1,100}$/)),
        )(`${request.appId}-${request.commitId}`).pipe(
          Effect.mapError(() => failed("Invalid build identity.")),
        );

        const name = `${prefix.slice(0, 24)}-${crypto.randomUUID().replaceAll("-", "")}`;

        return yield* Effect.acquireUseRelease(
          Effect.try({
            try: () => getSandbox(namespace, name, { sleepAfter: "5m" }),
            catch: () => operationFailure("get"),
          }),
          (box) =>
            Effect.gen(function* () {
              const root = "/workspace/app";

              const execute = Effect.fn("AppBuilder.execute")(function* (
                command: SandboxCommand,
                label: string,
              ) {
                const process = yield* sandboxOperation("exec", () =>
                  box.exec(command, { cwd: root, timeout: 240_000 }),
                );

                const exit = yield* sandboxOperation("waitForExit", (signal) =>
                  process.waitForExit({ timeout: 240_000, signal }),
                );

                if (exit.code !== 0 || exit.timedOut) {
                  const output = yield* sandboxOperation("output", (signal) =>
                    process.output({ encoding: "utf8", maxBytes: 3000, timeout: 2000, signal }),
                  ).pipe(Effect.option);

                  const details = Option.isSome(output)
                    ? `\n${output.value.stdout}\n${output.value.stderr}`.slice(-3000)
                    : "";

                  return yield* failed(
                    `${label} failed${exit.timedOut ? " (time limit)" : ` (exit ${exit.code})`}.${details}`,
                  );
                }
              });

              // Only the initial, idempotent directory creation can be retried here.
              // Commands and source writes must never be replayed on ambiguous errors.
              const directory = yield* Effect.tryPromise({
                try: () => box.mkdir(root, { recursive: true }),
                catch: (cause) =>
                  cause instanceof Error &&
                  cause.message.includes("Maximum number of running container instances exceeded")
                    ? new BuilderCapacity({})
                    : operationFailure("mkdir"),
              }).pipe(
                Effect.tapError((error) =>
                  error._tag === "BuilderCapacity"
                    ? recordBuildProgress(request, {
                        phase: "queued",
                        message: "Waiting for an available app builder",
                      })
                    : Effect.void,
                ),
                Effect.retry({
                  while: (error) => error._tag === "BuilderCapacity",
                  times: 10,
                  schedule: Schedule.exponential("1 second").pipe(
                    Schedule.modifyDelay(({ duration }) =>
                      Effect.succeed(Duration.min(duration, Duration.seconds(15))),
                    ),
                  ),
                }),
              );

              if (!directory.success) return yield* failed("Could not prepare the app builder.");
              for (const file of files) {
                if (file.path.includes("/")) {
                  const created = yield* sandboxOperation("mkdir", () =>
                    box.mkdir(`${root}/${file.path.slice(0, file.path.lastIndexOf("/"))}`, {
                      recursive: true,
                    }),
                  );

                  if (!created.success)
                    return yield* failed("Could not prepare app source directories.");
                }

                const written = yield* sandboxOperation("writeFile", () =>
                  box.writeFile(`${root}/${file.path}`, file.content),
                );

                if (!written.success) return yield* failed("Could not write app source.");
              }
              yield* recordBuildProgress(request, {
                phase: "installing",
                message: "Installing app dependencies",
              });
              yield* execute(["vp", "install", "--ignore-scripts"], "Dependency install");
              yield* recordBuildProgress(request, {
                phase: "checking",
                message: "Checking app code",
              });
              yield* execute(["vp", "check", "--no-fmt"], "App checks");
              yield* recordBuildProgress(request, {
                phase: "compiling",
                message: "Building the website and API",
              });
              yield* execute(["vp", "run", "build"], "App compilation");

              const listed = yield* sandboxOperation("listFiles", () =>
                box.listFiles(`${root}/dist`, {
                  recursive: true,
                  includeHidden: true,
                }),
              );

              if (!listed.success) return yield* failed("Could not read the app build.");
              const built = listed.files.filter((file) => file.type !== "directory");

              if (
                built.length > 200 ||
                built.some(
                  (file) =>
                    file.type !== "file" ||
                    !Number.isSafeInteger(file.size) ||
                    file.size < 0 ||
                    file.size > MAX_FILE,
                ) ||
                built.reduce((size, file) => size + file.size, 0) > MAX_BUILD
              )
                return yield* failed(
                  "App build output exceeds its limits or contains symbolic links.",
                );
              const output: Array<typeof OutputFile.Type> = [];

              for (const file of built) {
                const path = file.absolutePath.slice(`${root}/dist/`.length);

                if (!file.absolutePath.startsWith(`${root}/dist/`) || !Schema.is(outputPath)(path))
                  return yield* failed("Unexpected app build path.");

                const read = yield* sandboxOperation("readFile", () =>
                  box.readFile(file.absolutePath, { encoding: "base64" }),
                );

                if (!read.success || read.content.length > Math.ceil(MAX_FILE / 3) * 4)
                  return yield* failed("Invalid app build file.");

                const body = yield* Effect.try({
                  try: () =>
                    Uint8Array.from(atob(read.content), (character) => character.charCodeAt(0)),
                  catch: () => failed("Invalid app build encoding."),
                });

                if (body.byteLength !== file.size)
                  return yield* failed("App build output changed during collection.");
                output.push({ path, body });
              }

              return yield* Schema.decodeEffect(OutputFiles)(output).pipe(
                Effect.mapError(() =>
                  failed(
                    "The app must build web/index.html and server/index.js within the output limits.",
                  ),
                ),
              );
            }),
          (box) =>
            sandboxOperation("destroy", () => box.destroy()).pipe(
              Effect.timeoutOrElse({
                duration: "20 seconds",
                orElse: () => Effect.fail(failed("App builder cleanup timed out.")),
              }),
            ),
        ).pipe(
          Effect.catchTag("BuilderCapacity", () =>
            failed("All app builders are busy. Retry the build in a moment."),
          ),
        );
      },
      Effect.scoped,
      Effect.timeoutOrElse({
        duration: "9 minutes",
        orElse: () => Effect.fail(failed("The app build exceeded its time limit.")),
      }),
    );

    return AppBuilder.of({ compile });
  }),
);

/** A build attempt owns its SDK sandbox and destroys it on every exit. */
export const sandboxBuilder = (namespace: DurableObjectNamespace<Sandbox>) =>
  AppBuilderLive.pipe(Layer.provide(Layer.succeed(AppBuildSandbox, namespace)));

/** Manifest bodies are consumed or cancelled in Scope. */
export const readBuild = Effect.fn("readAppBuildManifest")(
  function* (appId: string, commitId: string) {
    const bucket = yield* AppBuildBucket;

    yield* Schema.decodeEffect(buildIdentity)({ appId, commitId }).pipe(
      Effect.mapError(badManifest),
    );
    const found = yield* bucket.get(`${buildPrefix(appId, commitId)}manifest.json`);

    if (found === null) return null;
    const object = found;

    yield* Effect.addFinalizer(() =>
      object.bodyUsed
        ? Effect.void
        : Effect.tryPromise({
            try: () => object.readable?.cancel() ?? Promise.resolve(),
            catch: () => undefined,
          }).pipe(Effect.ignore),
    );
    if (object.size > 64 * 1024) return yield* badManifest();
    const text = yield* object.text();

    const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(BuildManifest))(text).pipe(
      Effect.mapError(badManifest),
    );

    if (manifest.appId !== appId || manifest.commitId !== commitId) return yield* badManifest();

    return manifest;
  },
  Effect.scoped,
  Effect.catchTag("R2Error", () => failed("App build storage is unavailable.")),
);

const sha256 = (body: Uint8Array) =>
  Effect.tryPromise({
    try: () => crypto.subtle.digest("SHA-256", Uint8Array.from(body)),
    catch: () => failed("Could not verify app build content."),
  }).pipe(
    Effect.map((hash) =>
      Array.from(new Uint8Array(hash), (byte) => byte.toString(16).padStart(2, "0")).join(""),
    ),
  );

const verifyAssets = Effect.fn("verifyAppBuildAssets")(function* (manifest: BuildManifest) {
  const bucket = yield* AppBuildBucket;

  yield* Effect.forEach(
    manifest.files,
    (file) =>
      bucket
        .head(`${buildPrefix(manifest.appId, manifest.commitId)}${file.path}`)
        .pipe(
          Effect.flatMap((stored) =>
            stored !== null &&
            stored.size === file.bytes &&
            stored.customMetadata?.sha256 === file.sha256
              ? Effect.void
              : Effect.fail(failed("App build assets are incomplete or invalid.")),
          ),
        ),
    { concurrency: 4, discard: true },
  );
});

/** Progress uses the same append-only CAS and failpoints as app activation. */
export const recordBuildProgress = Effect.fn("recordTripAppBuildProgress")(function* (
  request: AppBuildRequest,
  update: Pick<TripAppBuildEvent, "phase" | "message">,
) {
  const apps = yield* AppRepository;

  const event = yield* Schema.decodeEffect(TripAppBuildEvent)({
    ...update,
    at: DateTime.formatIso(yield* DateTime.now),
  }).pipe(Effect.mapError(() => failed("Invalid app build progress.")));

  for (let attempt = 0; attempt < 5; attempt++) {
    const app = yield* apps.getById(request.appId);

    if (
      app === null ||
      app.tripId !== request.tripId ||
      app.repoName !== request.repoName ||
      app.sourceCommit !== request.commitId ||
      app.pendingCommit !== request.commitId
    )
      return;
    const previous = app.buildProgress?.at(-1);

    if (previous?.phase === event.phase && previous.message === event.message) return;

    const saved = yield* apps
      .save(
        {
          ...app,
          revision: app.revision + 1,
          updatedAt: event.at,
          buildProgress: [...(app.buildProgress ?? []), event].slice(-40),
        },
        app.revision,
      )
      .pipe(Effect.result);

    if (saved._tag === "Success") return;
    if (saved.failure.code !== "conflict") return yield* saved.failure;
  }

  return yield* new PlannerError({
    code: "conflict",
    message: "The app changed while recording build progress.",
  });
});

/** Manifest-last publication; immutable asset writes make interrupted retries safe. */
export const buildTripApp = Effect.fn("buildTripApp")(
  function* (input: AppBuildRequest) {
    const request = yield* Schema.decodeEffect(AppBuildRequest)(input).pipe(
      Effect.mapError(() => failed("Invalid app build request.")),
    );

    const bucket = yield* AppBuildBucket;
    const failpoint = yield* TripFailpoint;

    yield* recordBuildProgress(request, { phase: "starting", message: "Starting the app builder" });
    const cached = yield* readBuild(request.appId, request.commitId);

    if (cached === null) {
      const source = yield* AppSourceStore;
      const builder = yield* AppBuilder;
      const files = yield* source.read({ repoName: request.repoName, commitId: request.commitId });

      const output = yield* builder.compile(request, files).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(OutputFiles)),
        Effect.catchTag("SchemaError", () => failed("Invalid app build output.")),
      );

      const entries = yield* Effect.forEach(output, (file) =>
        sha256(file.body).pipe(
          Effect.map((hash) => ({
            path: file.path,
            bytes: file.body.byteLength,
            contentType: contentType(file.path),
            sha256: hash,
            body: file.body,
          })),
        ),
      );

      const manifest = yield* Schema.decodeEffect(BuildManifest)({
        version: 1,
        appId: request.appId,
        commitId: request.commitId,
        files: entries,
      }).pipe(Effect.mapError(badManifest));

      yield* recordBuildProgress(request, {
        phase: "uploading",
        message: "Saving the built app to Cloudflare",
      });
      yield* failpoint.hit("app-build:before-assets");
      yield* Effect.forEach(
        entries,
        (file) =>
          bucket.put(`${buildPrefix(request.appId, request.commitId)}${file.path}`, file.body, {
            onlyIf: { etagDoesNotMatch: "*" },
            customMetadata: { sha256: file.sha256 },
            httpMetadata: { contentType: file.contentType },
          }),
        { concurrency: 4, discard: true },
      );
      yield* failpoint.hit("app-build:after-assets");
      yield* verifyAssets(manifest);

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(BuildManifest))(
        manifest,
      ).pipe(Effect.mapError(badManifest));

      yield* failpoint.hit("app-build:before-manifest");
      yield* bucket.put(`${buildPrefix(request.appId, request.commitId)}manifest.json`, encoded, {
        onlyIf: { etagDoesNotMatch: "*" },
        httpMetadata: { contentType: "application/json" },
      });
      yield* failpoint.hit("app-build:after-manifest");
      const committed = yield* readBuild(request.appId, request.commitId);

      if (committed === null || JSON.stringify(committed) !== JSON.stringify(manifest))
        return yield* failed("A different build already exists for this source revision.");
    } else yield* verifyAssets(cached);
    yield* settleBuild(request, null);
  },
  Effect.catchTag("R2Error", () => failed("Could not save the app build.")),
  Effect.timeoutOrElse({
    duration: "11 minutes",
    orElse: () => Effect.fail(failed("The app build timed out.")),
  }),
);

/** A conflict requires a fresh read. Only the still-requested source may become active. */
export const settleBuild = Effect.fn("settleTripAppBuild")(function* (
  input: AppBuildRequest,
  error: string | null,
) {
  const request = yield* Schema.decodeEffect(AppBuildRequest)(input).pipe(
    Effect.mapError(() => failed("Invalid app build request.")),
  );

  const apps = yield* AppRepository;

  for (let attempt = 0; attempt < 5; attempt++) {
    const app = yield* apps.getById(request.appId);

    if (
      app === null ||
      app.tripId !== request.tripId ||
      app.repoName !== request.repoName ||
      app.pendingCommit !== request.commitId ||
      app.sourceCommit !== request.commitId
    )
      return;
    const updatedAt = DateTime.formatIso(yield* DateTime.now);

    const saved = yield* apps
      .save(
        {
          ...app,
          revision: app.revision + 1,
          status: error === null ? "ready" : "failed",
          error: error?.slice(0, 4000) ?? null,
          pendingCommit: null,
          activeCommit: error === null ? request.commitId : app.activeCommit,
          updatedAt,
          buildProgress: [
            ...(app.buildProgress ?? []),
            {
              at: updatedAt,
              phase: error === null ? ("ready" as const) : ("failed" as const),
              message:
                error === null ? "Your app is ready" : "Build stopped. Check the build details.",
            },
          ].slice(-40),
          versions:
            error !== null || app.versions.some((version) => version.commitId === request.commitId)
              ? app.versions
              : [
                  ...app.versions,
                  { commitId: request.commitId, label: request.label, createdAt: updatedAt },
                ],
        },
        app.revision,
      )
      .pipe(Effect.result);

    if (saved._tag === "Success") return;
    if (saved.failure.code !== "conflict") return yield* saved.failure;
  }

  return yield* new PlannerError({
    code: "conflict",
    message: "The app changed repeatedly while recording its build. Retry the build.",
  });
});

const BuildHostLive = Layer.mergeAll(
  AppBuildBucketLive,
  AppBuilderLive.pipe(Layer.provide(AppBuildSandboxLive)),
);

export const runSiteBuild = Effect.fn("runSiteBuild")(function* (request: AppBuildRequest) {
  yield* task("Build and activate app", buildTripApp(request), {
    retries: { limit: 2, delay: "15 seconds", backoff: "exponential" },
    timeout: "12 minutes",
  }).pipe(
    Effect.catchTag("WorkflowStepError", (error) =>
      Effect.gen(function* () {
        yield* task("Record build failure", settleBuild(request, error.message.slice(-3800)), {
          retries: { limit: 2, delay: "5 seconds", backoff: "exponential" },
          timeout: "1 minute",
        });

        return yield* error;
      }),
    ),
  );

  return { commitId: request.commitId };
});

/** The existing SiteBuild export is hosted by Alchemy's native Workflow bridge. */
export const siteBuild: WorkflowExport = {
  kind: "workflow",
  make: (environment) =>
    Effect.succeed((input: unknown) =>
      Effect.gen(function* () {
        const request = yield* Schema.decodeUnknownEffect(AppBuildRequest)(input).pipe(
          Effect.mapError(() => failed("Invalid app build request.")),
        );

        const env = yield* plannerEnvironment;

        if (!env.APP_BUILDS || !env.APP_DOMAIN)
          return yield* failed("The app address storage isn't configured.");

        const app = yield* callAppRepository(request.owner, Schema.NullOr(TripApp), {
          _tag: "GetById",
          appId: request.appId,
        });

        if (app === null || app.tripId !== request.tripId || app.repoName !== request.repoName)
          return yield* failed("The app build scope is unavailable.");
        yield* publishTripAppAddress(request.owner, app, env.APP_DOMAIN);
        const apps = Layer.succeed(AppRepository, appRepositoryForOwner(env, request.owner));

        return yield* runSiteBuild(request).pipe(
          Effect.provide(Layer.merge(apps, appSourceLayer(env.ARTIFACTS, env.ARTIFACTS_GIT_BASE))),
        );
      }).pipe(
        Effect.provide(BuildHostLive),
        Effect.provideService(WorkerEnvironment, environment as Cloudflare.Env),
        Effect.orDie,
      ),
    ),
};
