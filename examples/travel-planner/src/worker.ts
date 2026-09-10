import type { Sandbox } from "@cloudflare/sandbox";
import start from "@tanstack/react-start/server-entry";
import { Effect, Layer, Schema } from "effect";
import { Worker, WorkerEnvironment } from "effect-cf";

import { artifactsLayer } from "./artifacts";
import { Trip, TripId, TripSiteStore, type AppBuildRequest } from "./domain";
import { type AccessAdminEnvironment } from "./server/access-admin";
import { authenticate, type AccessEnvironment } from "./server/access-auth";
import { accessResponse } from "./server/access-http";
import { makeTravelPlannerThread } from "./server/cloudflare";
import { serveProgress } from "./server/progress-http";
import { plannerOwner } from "./server/tenancy";
import { appNameFromHost } from "./trip-app/addresses.ts";
import { serveTripApp } from "./trip-app/gateway.ts";

export { Sandbox } from "@cloudflare/sandbox";
export { SiteBuild } from "./trip-app/build.ts";
export { TripData } from "./trip-app/gateway.ts";

export class PlannerThread extends makeTravelPlannerThread(
  Layer.unwrap(
    Effect.map(WorkerEnvironment, (env) => artifactsLayer(env.ARTIFACTS, env.ARTIFACTS_GIT_BASE)),
  ),
) {}

declare global {
  namespace Cloudflare {
    interface Env extends AccessEnvironment, AccessAdminEnvironment {
      THREADS: DurableObjectNamespace<PlannerThread>;
      ARTIFACTS: Artifacts;
      ARTIFACTS_GIT_BASE: string;
      ASSETS?: Fetcher;
      APP_DOMAIN?: string;
      APP_BUILDS?: R2Bucket;
      APP_LOADER?: WorkerLoader;
      APP_SANDBOX?: DurableObjectNamespace<Sandbox>;
      SITE_BUILD?: Workflow<AppBuildRequest>;
    }
    interface GlobalProps {
      mainModule: typeof import("./worker.ts");
    }
  }
}

const sitePath = Schema.Struct({
  tripId: TripId,
  revision: Schema.String.check(Schema.isPattern(/^[1-9]\d{0,8}$/)),
});

const publishedResponse = Effect.fn("publishedResponse")(
  function* (request: Request, _env: Cloudflare.Env) {
    const parts = new URL(request.url).pathname.split("/");

    if (
      request.method !== "GET" ||
      (parts.length !== 4 && !(parts.length === 5 && parts[4] === "trip.json"))
    ) {
      return new Response("Not found", { status: 404 });
    }

    const path = yield* Schema.decodeUnknownEffect(sitePath)({
      tripId: parts[2],
      revision: parts[3],
    });

    const store = yield* TripSiteStore;
    const document = yield* store.load({ tripId: path.tripId, revision: Number(path.revision) });

    if (document === null) return new Response("This trip hasn't been published.", { status: 404 });
    const isJson = parts.length === 5;

    return new Response(
      isJson
        ? yield* Schema.encodeEffect(Schema.fromJsonString(Trip))(document.trip)
        : document.html,
      {
        headers: {
          "content-type": isJson ? "application/json; charset=utf-8" : "text/html; charset=utf-8",
          "cache-control": "private, max-age=3600, immutable",
          "content-security-policy":
            "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'",
          "x-content-type-options": "nosniff",
          "referrer-policy": "no-referrer",
        },
      },
    );
  },
  (effect, _request, env) =>
    effect.pipe(
      Effect.provide(artifactsLayer(env.ARTIFACTS, env.ARTIFACTS_GIT_BASE)),
      Effect.catchTag("SchemaError", () =>
        Effect.succeed(new Response("Not found", { status: 404 })),
      ),
      Effect.catch(() =>
        Effect.succeed(
          new Response("Trip storage is temporarily unavailable. Please try again.", {
            status: 503,
          }),
        ),
      ),
    ),
);

/** Cloudflare authentication and routing stay in the Effect request scope. */
export const handleRequest = (verify = authenticate) =>
  Effect.fn("Planner.fetch")(function* (
    request: Request,
    env: Cloudflare.Env,
    ctx?: ExecutionContext,
  ) {
    const url = new URL(request.url);
    const appName = appNameFromHost(url.hostname, env.APP_DOMAIN ?? "effect-agent.com");

    if (url.hostname.includes("-trip.") && appName === null)
      return new Response("Not found", { status: 404 });

    if (appName !== null) {
      if (!ctx) return new Response("App runtime is unavailable.", { status: 503 });

      return yield* serveTripApp(request, env, ctx).pipe(
        Effect.catch(() =>
          Effect.succeed(new Response("The trip app is temporarily unavailable.", { status: 503 })),
        ),
      );
    }

    const identity = yield* verify(request, env).pipe(
      Effect.match({
        onSuccess: (session) => ({ _tag: "Granted" as const, session }),
        onFailure: (error) => ({ _tag: "Denied" as const, error }),
      }),
    );

    if (identity._tag === "Denied")
      return new Response(identity.error.message, {
        status: identity.error.code === "unavailable" ? 503 : 401,
        headers: { "cache-control": "no-store" },
      });
    if (url.pathname.startsWith("/trips/")) return yield* publishedResponse(request, env);
    if (
      [
        "/api/rpc",
        "/api/rpc/",
        "/api/access",
        "/api/access/",
        "/api/progress",
        "/api/progress/",
      ].includes(url.pathname)
    ) {
      const origin = request.headers.get("origin");

      if (origin !== null && origin !== url.origin)
        return new Response("Forbidden", { status: 403 });
      if (request.method !== "POST") return new Response("Method not allowed", { status: 405 });

      return yield* Effect.scoped(
        Effect.gen(function* () {
          const reader = yield* Effect.acquireRelease(
            Effect.sync(() => request.body?.getReader()),
            (stream) =>
              Effect.promise(async () => {
                await stream?.cancel();
              }),
          );

          const chunks: Uint8Array[] = [];
          let length = 0;

          if (reader) {
            while (true) {
              const next = yield* Effect.promise(() => reader.read());

              if (next.done) break;
              length += next.value.byteLength;
              if (length > 32 * 1024) return new Response("Request too large", { status: 413 });
              chunks.push(next.value);
            }
          }
          const body = new Uint8Array(length);
          let offset = 0;

          for (const chunk of chunks) {
            body.set(chunk, offset);
            offset += chunk.byteLength;
          }
          const bounded = new Request(request, { method: "POST", body });

          if (url.pathname.startsWith("/api/access"))
            return yield* accessResponse(bounded, env, identity.session);
          if (url.pathname.startsWith("/api/progress"))
            return yield* serveProgress(bounded, env, identity.session);
          const owner = yield* plannerOwner(identity.session.email);

          return yield* Effect.promise(async () => {
            using response = await env.THREADS.getByName(owner).plannerFetch(bounded);
            const headers = new Headers(response.headers);

            headers.set("cache-control", "no-store");

            return new Response(await response.arrayBuffer(), { status: response.status, headers });
          });
        }),
      );
    }
    if ((url.pathname.startsWith("/assets/") || url.pathname === "/favicon.svg") && env.ASSETS) {
      const assets = env.ASSETS;

      return yield* Effect.promise(() => assets.fetch(request));
    }

    return yield* Effect.promise(async () => start.fetch(request));
  });

// Test fixtures may substitute signature verification; production always uses authenticate.
export const makeWorker = (verify = authenticate) => ({
  fetch: (request: Request, env: Cloudflare.Env, ctx?: ExecutionContext) =>
    Effect.runPromise(handleRequest(verify)(request, env, ctx)),
});

export default Worker.make(Layer.empty, {
  fetch: Effect.gen(function* () {
    const request = yield* Worker.NativeRequest;
    const env = yield* WorkerEnvironment;
    const ctx = yield* Worker.ExecutionContext;

    return yield* handleRequest()(request, env, ctx);
  }),
});
