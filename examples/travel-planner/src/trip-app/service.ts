import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { DateTime, Effect, Layer, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { AppCommit, AppId, PlannerError, type AppFile, type TripApp } from "../domain.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import { TripFailpoint } from "../server/trips.ts";
import { publishTripAppAddress, tripAppHostname } from "./addresses.ts";
import { SiteBuildBinding } from "./bindings.ts";
import { AppRepository } from "./repository.ts";
import { requireAppTrip } from "./scope.ts";
import { AppSourceStore, appSourceLayer } from "./source.ts";
import { TRIP_APP_MAP_FILES, TRIP_APP_TEMPLATE_FILES } from "./template.ts";

const files = (source: Readonly<Record<string, string>>): ReadonlyArray<AppFile> =>
  Object.entries(source).map(([path, content]) => ({ path, content }));

const failed = (message: string) => new PlannerError({ code: "unavailable", message });
const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));

export { requireAppTrip } from "./scope.ts";

const requireApp = Effect.fn("requireTripApp")(function* (tripId: string) {
  yield* requireAppTrip(tripId);
  const apps = yield* AppRepository;
  const app = yield* apps.get(tripId);

  if (app === null)
    return yield* new PlannerError({ code: "not-found", message: "Create this trip's app first." });

  return app;
});

const startBuild = Effect.fn("startTripAppBuild")(function* (app: TripApp, label: string) {
  const env = yield* WorkerEnvironment;
  const identity = yield* ThreadObjectIdentity;
  const failpoint = yield* TripFailpoint;

  if (!env.SITE_BUILD || !env.APP_BUILDS || !env.APP_DOMAIN)
    return yield* failed("The app builder isn't configured.");
  yield* publishTripAppAddress(
    env.APP_BUILDS,
    ownerOfThread(identity.threadId),
    app,
    env.APP_DOMAIN,
  );
  const id = `${app.id}-${app.sourceCommit}`;

  yield* failpoint.hit("app-build:before-start");
  yield* Effect.gen(function* () {
    const workflow = yield* SiteBuildBinding;

    const params = {
      owner: ownerOfThread(identity.threadId),
      appId: app.id,
      tripId: app.tripId,
      repoName: app.repoName,
      commitId: app.sourceCommit,
      label,
    };

    yield* workflow.create(params, { id }).pipe(
      Effect.catch((error) =>
        Effect.gen(function* () {
          const existing = yield* workflow.get(id);
          const status = yield* existing.status;

          if (status.status === "errored" || status.status === "terminated")
            yield* existing.restart();
          if (status.status === "unknown") return yield* error;

          return existing;
        }),
      ),
    );
  }).pipe(
    Effect.provide(SiteBuildBinding.layer({ binding: "SITE_BUILD" })),
    Effect.mapError(() =>
      failed("The source is saved, but the build couldn't start. Retry the build."),
    ),
  );
  yield* failpoint.hit("app-build:after-start");

  return app;
});

export const createTripApp = Effect.fn("createTripApp")(function* (tripId: string) {
  const trip = yield* requireAppTrip(tripId);
  const apps = yield* AppRepository;
  const existing = yield* apps.get(tripId);

  if (existing !== null)
    return existing.status === "building" ? yield* startBuild(existing, "Build app") : existing;
  const env = yield* WorkerEnvironment;

  if (!env.APP_DOMAIN || !env.SITE_BUILD || !env.APP_BUILDS)
    return yield* failed("The app builder isn't configured.");
  const identity = yield* ThreadObjectIdentity;

  const digest = yield* Effect.promise(() =>
    crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(`${ownerOfThread(identity.threadId)}:${tripId}`),
    ),
  );

  const id = yield* Schema.decodeUnknownEffect(AppId)(
    Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0"))
      .join("")
      .slice(0, 32),
  ).pipe(Effect.mapError(() => failed("Could not create the app identity.")));

  const source = yield* AppSourceStore;
  const repoName = `trip-app-${id}`;
  const { commitId } = yield* source.fork({ repoName, files: files(TRIP_APP_TEMPLATE_FILES) });
  const queuedAt = yield* now;

  const app = yield* apps.save(
    {
      id,
      tripId,
      revision: 1,
      url: `https://${tripAppHostname(trip.title, id, env.APP_DOMAIN)}`,
      repoName,
      sourceCommit: commitId,
      activeCommit: null,
      pendingCommit: commitId,
      status: "building",
      error: null,
      updatedAt: queuedAt,
      versions: [],
      buildProgress: [{ at: queuedAt, phase: "queued", message: "Waiting for the app builder" }],
    },
    null,
  );

  return yield* startBuild(app, "Initial trip app");
});

export const readTripAppFiles = Effect.fn("readTripAppFiles")(function* (
  tripId: string,
  paths: ReadonlyArray<string>,
) {
  const app = yield* requireApp(tripId);
  const source = yield* AppSourceStore;
  const tree = yield* source.read({ repoName: app.repoName, commitId: app.sourceCommit });
  const selected = tree.filter((file) => paths.includes(file.path));

  if (
    selected.reduce((size, file) => size + new TextEncoder().encode(file.content).byteLength, 0) >
    18 * 1024
  )
    return yield* new PlannerError({
      code: "invalid",
      message: "Read fewer files at once (18 KiB limit).",
    });

  return { commitId: app.sourceCommit, paths: tree.map((file) => file.path), files: selected };
});

/** Commits are immutable. Build completion can activate only the still-pending commit. */
export const editTripApp = Effect.fn("editTripApp")(function* (input: {
  tripId: string;
  expectedCommit: string;
  files: ReadonlyArray<AppFile>;
  deletePaths: ReadonlyArray<string>;
  label: string;
}) {
  const app = yield* requireApp(input.tripId);

  if (app.sourceCommit !== input.expectedCommit)
    return yield* new PlannerError({
      code: "conflict",
      message: "The app source changed. Read its files again before editing.",
    });
  const source = yield* AppSourceStore;
  const current = yield* source.read({ repoName: app.repoName, commitId: app.sourceCommit });
  const changed = new Map(current.map((file) => [file.path, file.content]));

  for (const path of input.deletePaths) changed.delete(path);
  for (const file of input.files) changed.set(file.path, file.content);

  const { commitId } = yield* source.commit({
    repoName: app.repoName,
    parentCommit: app.sourceCommit,
    files: Array.from(changed, ([path, content]) => ({ path, content })),
    message: input.label,
  });

  if (commitId === app.sourceCommit && app.status === "ready") return app;
  const apps = yield* AppRepository;

  const queuedAt = yield* now;

  const queued = yield* apps.save(
    {
      ...app,
      revision: app.revision + 1,
      sourceCommit: commitId,
      pendingCommit: commitId,
      status: "building",
      error: null,
      updatedAt: queuedAt,
      buildProgress: [{ at: queuedAt, phase: "queued", message: "Waiting to build your changes" }],
    },
    app.revision,
  );

  return yield* startBuild(queued, input.label);
});

export const addTripAppMap = Effect.fn("addTripAppMap")(function* (tripId: string) {
  const app = yield* createTripApp(tripId);

  return yield* editTripApp({
    tripId,
    expectedCommit: app.sourceCommit,
    files: files(TRIP_APP_MAP_FILES),
    deletePaths: [],
    label: "Add journey map",
  });
});

export const retryTripAppBuild = Effect.fn("retryTripAppBuild")(function* (tripId: string) {
  const app = yield* requireApp(tripId);

  if (app.status === "ready" && app.activeCommit === app.sourceCommit) return app;
  const apps = yield* AppRepository;
  const queuedAt = yield* now;

  const pending =
    app.status === "building"
      ? app
      : yield* apps.save(
          {
            ...app,
            revision: app.revision + 1,
            status: "building",
            pendingCommit: app.sourceCommit,
            error: null,
            updatedAt: queuedAt,
            buildProgress: [
              { at: queuedAt, phase: "queued", message: "Waiting to retry the build" },
            ],
          },
          app.revision,
        );

  return yield* startBuild(pending, "Build app");
});

export const restoreTripApp = Effect.fn("restoreTripApp")(function* (
  tripId: string,
  commitId: string,
) {
  const app = yield* requireApp(tripId);

  const target = yield* Schema.decodeUnknownEffect(AppCommit)(commitId).pipe(
    Effect.mapError(() => failed("Invalid app version.")),
  );

  if (!app.versions.some((version) => version.commitId === target))
    return yield* new PlannerError({
      code: "invalid",
      message: "Only a successfully built app version can be restored.",
    });
  if (app.activeCommit === target && app.sourceCommit === target && app.pendingCommit === null)
    return app;
  const source = yield* AppSourceStore;
  const restored = yield* source.read({ repoName: app.repoName, commitId: target });
  const current = yield* source.read({ repoName: app.repoName, commitId: app.sourceCommit });
  const paths = new Set(restored.map((file) => file.path));

  return yield* editTripApp({
    tripId,
    expectedCommit: app.sourceCommit,
    files: restored,
    deletePaths: current.filter((file) => !paths.has(file.path)).map((file) => file.path),
    label: `Restore ${target.slice(0, 7)}`,
  });
});

export const AppSourceLive = Layer.unwrap(
  Effect.map(WorkerEnvironment, (env) => appSourceLayer(env.ARTIFACTS, env.ARTIFACTS_GIT_BASE)),
);
