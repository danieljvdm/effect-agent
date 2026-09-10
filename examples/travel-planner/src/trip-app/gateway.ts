import { Effect, Layer, Schema } from "effect";
import { Worker, WorkerEnvironment } from "effect-cf";

import { AppId, PlannerError, TripApp, TripAppData, TripId } from "../domain.ts";
import { storageOwner } from "../server/tenancy.ts";
import { appNameFromHost, publishTripAppAddress, readTripAppAddress } from "./addresses.ts";
import { buildPrefix, readBuild } from "./build.ts";
import { callAppRepository } from "./remote.ts";

const TripScope = Schema.Struct({ owner: Schema.String, tripId: TripId });

const unavailable = () =>
  new PlannerError({ code: "unavailable", message: "The trip app is temporarily unavailable." });

/** Generated Workers get this fixed service binding, never the owner's namespace. */
export class TripData extends Worker.make(Layer.empty, {
  fetch: Effect.gen(function* () {
    const request = yield* Worker.NativeRequest;
    const context = yield* Worker.ExecutionContext;
    const env = yield* WorkerEnvironment;

    if (
      request.method !== "GET" ||
      new URL(request.url).pathname !== "/api/trip" ||
      new URL(request.url).search !== ""
    )
      return new Response("Not found", { status: 404 });
    const scope = yield* Schema.decodeUnknownEffect(TripScope)(context.props);

    const data = yield* callAppRepository(env, scope.owner, TripAppData, {
      _tag: "Data",
      tripId: scope.tripId,
    });

    const body = yield* Schema.encodeEffect(Schema.fromJsonString(TripAppData))(data);

    return new Response(body, {
      headers: { "content-type": "application/json", "cache-control": "no-store" },
    });
  }).pipe(
    Effect.catch(() => Effect.succeed(new Response("Trip data is unavailable.", { status: 503 }))),
  ),
}) {}

const secureHeaders = (headers: Headers) => {
  headers.set("cache-control", "private, no-store");
  headers.set("x-content-type-options", "nosniff");
  headers.set("referrer-policy", "no-referrer");
  headers.set(
    "content-security-policy",
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' https: data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
  );

  return headers;
};

/** Public ingress resolves only trusted host metadata before reading code or trip data. */
export const serveTripApp = Effect.fn("serveTripApp")(function* (
  request: Request,
  env: Cloudflare.Env,
  ctx: ExecutionContext,
) {
  if (!env.APP_BUILDS || !env.APP_LOADER) return yield* unavailable();
  if (request.method !== "GET" && request.method !== "HEAD")
    return new Response("Method not allowed", { status: 405 });

  const hostname = new URL(request.url).hostname;
  const domain = env.APP_DOMAIN ?? "effect-agent.com";
  const name = appNameFromHost(hostname, domain);

  if (name === null) return new Response("Not found", { status: 404 });
  let address = yield* readTripAppAddress(env.APP_BUILDS, hostname);

  // Original administrator apps predate the directory. Only this fixed storage
  // owner can be recovered from a legacy ID; caller identity is never consulted.
  if (address === null && Schema.is(AppId)(name)) {
    const legacy = yield* callAppRepository(env, storageOwner, Schema.NullOr(TripApp), {
      _tag: "GetById",
      appId: name,
    });

    if (legacy !== null && legacy.id === name && legacy.url === `https://${hostname}`) {
      yield* publishTripAppAddress(env.APP_BUILDS, storageOwner, legacy, domain);
      address = yield* readTripAppAddress(env.APP_BUILDS, hostname);
    }
  }
  if (address === null) return new Response("Not found", { status: 404 });
  const { owner, appId } = address;

  const app = yield* callAppRepository(env, owner, Schema.NullOr(TripApp), {
    _tag: "GetById",
    appId,
  });

  if (
    app === null ||
    app.id !== appId ||
    app.tripId !== address.tripId ||
    (app.url !== `https://${hostname}` && hostname !== `${app.id}-trip.${domain}`)
  )
    return new Response("Not found", { status: 404 });
  if (app.activeCommit === null) {
    const building = app.status === "building";

    return new Response(
      `<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1">${building ? '<meta http-equiv="refresh" content="5">' : ""}<title>Your trip app</title><style>body{margin:10vh auto;padding:24px;max-width:600px;font:18px system-ui;color:#30463a;background:#fbfbf7}a{color:inherit}</style></head><body><h1>${building ? "Your trip app is being built" : "This app needs a build"}</h1><p>${building ? "This page will open automatically when it is ready." : "Open your planner to review the build error and retry."}</p><a href="https://travel.effect-agent.com">Back to your planner</a></body></html>`,
      {
        status: building ? 202 : 503,
        headers: secureHeaders(new Headers({ "content-type": "text/html; charset=utf-8" })),
      },
    );
  }
  const commit = app.activeCommit;
  const bucket = env.APP_BUILDS;
  const prefix = buildPrefix(app.id, commit);
  const manifest = yield* readBuild(bucket, app.id, commit);

  if (manifest === null) return yield* unavailable();
  const url = new URL(request.url);

  if (url.pathname === "/api" || url.pathname.startsWith("/api/")) {
    const binding = ctx.exports.TripData({ props: { owner, tripId: app.tripId } });
    const loader = env.APP_LOADER;

    const response = yield* Effect.tryPromise({
      try: async () => {
        const worker = loader.get(`${app.id}:${commit}`, async () => {
          const bundle = await bucket.get(`${prefix}server/index.js`);

          if (bundle === null || bundle.size > 4 * 1024 * 1024)
            throw new Error("App server is missing.");

          return {
            compatibilityDate: "2026-07-01",
            compatibilityFlags: ["nodejs_compat"],
            mainModule: "worker.js",
            modules: { "worker.js": await bundle.text() },
            env: { TRIP_DATA: binding },
            globalOutbound: null,
            limits: { cpuMs: 100, subRequests: 10 },
          };
        });

        // In particular, do not forward Access assertions, cookies, or caller-supplied service headers.
        return worker.getEntrypoint().fetch(
          new Request(request.url, {
            method: request.method,
            redirect: "manual",
            headers: { accept: "application/json" },
          }),
        );
      },
      catch: unavailable,
    });

    const headers = secureHeaders(
      new Headers({ "content-type": response.headers.get("content-type") ?? "application/json" }),
    );

    // Generated code cannot set cookies or navigate the browser by a redirect response.
    if (response.status >= 300 && response.status < 400) {
      yield* Effect.promise(() => response.body?.cancel() ?? Promise.resolve());

      return new Response("App redirects are not supported.", { status: 502, headers });
    }

    if (request.method === "HEAD") {
      yield* Effect.promise(() => response.body?.cancel() ?? Promise.resolve());

      return new Response(null, { status: response.status, headers });
    }

    return new Response(response.body, { status: response.status, headers });
  }
  const path = url.pathname === "/" ? "web/index.html" : `web${url.pathname}`;
  const file = manifest.files.find((entry) => entry.path === path);

  if (!file) return new Response("Not found", { status: 404 });

  const object = yield* Effect.tryPromise({
    try: () => bucket.get(`${prefix}${file.path}`),
    catch: unavailable,
  });

  if (object === null || object.size !== file.bytes) return yield* unavailable();

  if (request.method === "HEAD") yield* Effect.promise(() => object.body.cancel());

  return new Response(request.method === "HEAD" ? null : object.body, {
    headers: secureHeaders(new Headers({ "content-type": file.contentType })),
  });
});
