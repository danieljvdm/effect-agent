import { makeDurableObjectBridge } from "alchemy/Cloudflare/Bridge";
import { DurableObject as AlchemyDurableObject } from "alchemy/Cloudflare/Workers/DurableObject";
import { DurableObjectState as AlchemyState } from "alchemy/Cloudflare/Workers/DurableObjectState";
import { Request as WorkerRequest } from "alchemy/Cloudflare/Workers/Request";
import { Worker } from "alchemy/Cloudflare/Workers/Worker";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, Schema } from "effect";
import { HttpServerResponse } from "effect/unstable/http";

import { plannerEnvironment, runtimeStack } from "../server/alchemy.ts";
import { AccountError, AccountSession, AccountId } from "./account";
import { emailDeliveryLayer } from "./email-delivery";
import { makeFundingStore } from "./funding";
import { serveAuth } from "./host";
import { AuthConfiguration } from "./server";
import { initializeAuthStorage } from "./storage";

const fetch = Effect.gen(function* () {
  const { raw } = yield* AlchemyState;
  const request = yield* WorkerRequest;
  const env = yield* plannerEnvironment;
  const url = new URL(request.url);

  if (url.pathname.startsWith("/_internal/funding/")) {
    const id = yield* Schema.decodeEffect(AccountId)(
      url.pathname.slice("/_internal/funding/".length),
    );

    yield* initializeAuthStorage(raw.storage);
    const store = yield* makeFundingStore(raw.storage);

    return Response.json(yield* store.status(id));
  }
  const config = yield* Schema.decodeEffect(AuthConfiguration)(env);

  return yield* serveAuth(
    request,
    raw.storage,
    config,
    emailDeliveryLayer(env.AUTH_EMAIL, config.AUTH_EMAIL_FROM).pipe(Layer.orDie),
    undefined,
    Boolean(env.SERVER_OPENAI_KEY),
  );
}).pipe(
  Effect.catch(() =>
    Effect.succeed(
      new Response("Authentication is temporarily unavailable.", {
        status: 503,
        headers: { "cache-control": "no-store" },
      }),
    ),
  ),
  Effect.map(HttpServerResponse.fromWeb),
);

class AuthObject extends AlchemyDurableObject<AuthObject, { readonly fetch: typeof fetch }>()(
  "AUTH",
) {}

const AuthLive = AuthObject.make(Effect.succeed(Effect.succeed({ fetch })));

const entrypoint = Worker(
  "PlannerAuthRuntime",
  { main: import.meta.url },
  Effect.gen(function* () {
    yield* AuthObject;

    return { fetch: Effect.succeed(HttpServerResponse.empty({ status: 404 })) };
  }).pipe(Effect.provide(AuthLive)),
);

const NativeAuth: new (
  ctx: DurableObjectState,
  env: Cloudflare.Env,
) => DurableObject<Cloudflare.Env> = makeDurableObjectBridge(DurableObject, {
  entrypoint,
  stack: runtimeStack,
})("AUTH");

/** Only the planner Worker can reach this object; Alchemy owns its event runtime and scopes. */
export class PlannerAuth extends NativeAuth {}

export const authenticate = Effect.fn("Planner.requireSession")(function* (request: Request) {
  if (!request.headers.get("cookie"))
    return yield* new AccountError({ code: "unauthorized", message: "Sign in to continue." });

  const unavailable = () =>
    new AccountError({
      code: "unavailable",
      message: "Authentication is temporarily unavailable.",
    });

  const url = new URL(request.url);

  url.pathname = "/_internal/session";
  url.search = "";

  const env = yield* plannerEnvironment;

  const response = yield* Effect.tryPromise({
    try: () =>
      env.AUTH.getByName("auth-v1").fetch(
        new Request(url, { headers: { cookie: request.headers.get("cookie") ?? "" } }),
      ),
    catch: unavailable,
  });

  if (response.status === 401)
    return yield* new AccountError({ code: "unauthorized", message: "Sign in to continue." });
  if (!response.ok) return yield* unavailable();

  return yield* Effect.tryPromise({ try: () => response.json(), catch: unavailable }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(AccountSession)),
    Effect.mapError(unavailable),
  );
});
