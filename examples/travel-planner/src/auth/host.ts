import * as SqliteClient from "@effect/sql-sqlite-do/SqliteClient";
import { LifecycleHooks } from "@yielded/auth/Hooks";
import type { OAuthProtocol } from "@yielded/auth/OAuth";
import type { EmailProofDelivery } from "@yielded/auth/Proofs";
import { layerWebCrypto } from "@yielded/auth/WebCrypto";
import * as Drizzle from "drizzle-orm/effect-sqlite-do";
import { Effect, Layer, Option, Schema } from "effect";
import { HttpRouter, HttpServer, HttpServerResponse } from "effect/unstable/http";
import { HttpApiBuilder } from "effect/unstable/httpapi";

import { FundingApi, FundingError } from "../funding-domain";
import { makeFundingStore } from "./funding";
import { makeGithubDiagnostics } from "./oauth-diagnostics";
import { AuthDatabase, persistenceLayer } from "./persistence";
import { makeAuth, type AuthConfiguration } from "./server";
import { initializeAuthStorage } from "./storage";

const rejectedCallback = Schema.decodeOption(
  Schema.fromJsonString(
    Schema.Struct({
      _tag: Schema.Literal("Failure"),
      error: Schema.Struct({ _tag: Schema.Literal("OAuthRejected") }),
    }),
  ),
);

/** The Auth object owns only auth state; planner data never passes through it. */
export const serveAuth = Effect.fn("Auth.fetch")(function* (
  request: Request,
  storage: DurableObjectStorage,
  config: AuthConfiguration,
  delivery: Layer.Layer<EmailProofDelivery>,
  protocol?: Layer.Layer<OAuthProtocol>,
  serverKeyConfigured = false,
) {
  yield* initializeAuthStorage(storage);
  const { AppAuth, http, security, github } = makeAuth(config);
  const diagnostics = yield* makeGithubDiagnostics(AppAuth);

  const database = Layer.effectContext(
    Effect.gen(function* () {
      const db = yield* Drizzle.makeWithDefaults({ storage });

      return yield* Layer.build(
        persistenceLayer(AppAuth).pipe(Layer.provideMerge(Layer.succeed(AuthDatabase, db))),
      );
    }),
  ).pipe(Layer.provide(SqliteClient.layer({ storage })), Layer.provide(LifecycleHooks.empty));

  const live = AppAuth.layer.pipe(
    Layer.provide([
      diagnostics.persistenceLayer.pipe(Layer.provideMerge(database)),
      security,
      protocol ?? github,
      delivery,
      diagnostics.bindingLayer.pipe(Layer.provide(security)),
    ]),
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

  const fundingHandlers = HttpApiBuilder.group(
    FundingApi,
    "funding",
    Effect.fn(function* (handlers) {
      const store = yield* makeFundingStore(storage);

      const actor = Effect.gen(function* () {
        const auth = yield* AppAuth;

        const session = yield* auth
          .requireSession()
          .pipe(Effect.mapError(() => new FundingError({ message: "Sign in to continue." })));

        if (request.headers.get("x-elsewhere-account") !== session.subjectId)
          return yield* new FundingError({ message: "Account changed. Sign in again." });
        if (request.method !== "GET" && request.headers.get("origin") !== config.AUTH_ORIGIN)
          return yield* new FundingError({ message: "Invalid request origin." });

        return session.subjectId;
      });

      return handlers
        .handle("status", () =>
          Effect.gen(function* () {
            const status = yield* store.status(yield* actor);

            return { ...status, configured: serverKeyConfigured };
          }),
        )
        .handle("list", ({ query }) => Effect.flatMap(actor, (id) => store.list(id, query.after)))
        .handle("grant", ({ payload }) => Effect.flatMap(actor, (id) => store.grant(id, payload)))
        .handle("revoke", ({ payload }) =>
          Effect.flatMap(actor, (id) => store.revoke(id, payload)),
        );
    }),
  );

  const fundingRoutes = HttpApiBuilder.layer(FundingApi).pipe(
    Layer.provide(fundingHandlers),
    http.middleware,
  );

  const routes = Layer.mergeAll(
    http.routes(),
    authorized,
    ...(new URL(request.url).pathname.startsWith("/api/funding/") ? [fundingRoutes] : []),
  ).pipe(Layer.provide(live), Layer.provide(HttpServer.layerServices));

  const web = yield* Effect.acquireRelease(
    Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
    (web) => Effect.promise(() => web.dispose()),
  );

  const response = yield* Effect.promise(() => web.handler(request));
  const body = yield* Effect.promise(() => response.arrayBuffer());

  if (
    new URL(request.url).pathname === "/auth/completeSignIn" &&
    response.status === 400 &&
    Option.isSome(rejectedCallback(new TextDecoder().decode(body)))
  )
    yield* diagnostics.report.pipe(
      Effect.timeout("100 millis"),
      Effect.catchCause(() => Effect.void),
    );

  return new Response(body, { status: response.status, headers: response.headers });
}, Effect.scoped);
