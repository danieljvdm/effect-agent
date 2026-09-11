import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import { LifecycleHooks } from "@yielded/auth/Hooks";
import type { OAuthProtocol } from "@yielded/auth/OAuth";
import type { EmailProofDelivery } from "@yielded/auth/Proofs";
import { layerWebCrypto } from "@yielded/auth/WebCrypto";
import * as Drizzle from "drizzle-orm/effect-sqlite-do";
import { Effect, Layer } from "effect";
import { HttpRouter, HttpServer, HttpServerResponse } from "effect/unstable/http";

import { persistenceLayer } from "./persistence";
import { makeAuth, type AuthConfiguration } from "./server";
import { initializeAuthStorage } from "./storage";

/** The Auth object owns only auth state; planner data never passes through it. */
export const serveAuth = Effect.fn("Auth.fetch")(function* (
  request: Request,
  storage: DurableObjectStorage,
  config: AuthConfiguration,
  delivery: Layer.Layer<EmailProofDelivery>,
  protocol?: Layer.Layer<OAuthProtocol>,
) {
  yield* initializeAuthStorage(storage);
  const { AppAuth, http, security, github } = makeAuth(config);

  const database = Layer.effectContext(
    Effect.gen(function* () {
      const db = yield* Drizzle.makeWithDefaults({ storage });

      return yield* Layer.build(persistenceLayer(AppAuth, db));
    }),
  ).pipe(Layer.provide(SqliteClient.layer({ storage })), Layer.provide(LifecycleHooks.empty));

  const live = AppAuth.layer.pipe(
    Layer.provide([database, security, protocol ?? github, delivery]),
    Layer.provide(layerWebCrypto),
  );

  const authorized = HttpRouter.add(
    "GET",
    "/_internal/session",
    Effect.gen(function* () {
      const auth = yield* AppAuth;
      const session = yield* auth.requireSession();

      return yield* HttpServerResponse.json({
        subjectId: session.subjectId,
        displayName: session.claims.displayName,
      });
    }).pipe(
      Effect.catchTag("AuthenticationRequired", () =>
        Effect.succeed(HttpServerResponse.empty({ status: 401 })),
      ),
      Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 503 }))),
    ),
  ).pipe(http.middleware);

  const routes = Layer.merge(http.routes(), authorized).pipe(
    Layer.provide(live),
    Layer.provide(HttpServer.layerServices),
  );

  const web = yield* Effect.acquireRelease(
    Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
    (web) => Effect.promise(() => web.dispose()),
  );

  const response = yield* Effect.promise(() => web.handler(request));
  const body = yield* Effect.promise(() => response.arrayBuffer());

  return new Response(body, { status: response.status, headers: response.headers });
}, Effect.scoped);
