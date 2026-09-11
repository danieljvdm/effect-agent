import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { AccountError, AccountSession } from "./account";
import { emailDeliveryLayer } from "./email-delivery";
import { serveAuth } from "./host";
import { AuthConfiguration } from "./server";

/** Only the planner Worker can reach this object; it has no public service route. */
export class PlannerAuth extends DurableObject<Cloudflare.Env> {
  fetch(request: Request): Promise<Response> {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const config = yield* Schema.decodeUnknownEffect(AuthConfiguration)(this.env);

        return yield* serveAuth(
          request,
          this.ctx.storage,
          config,
          emailDeliveryLayer(this.env.AUTH_EMAIL, config.AUTH_EMAIL_FROM).pipe(Layer.orDie),
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
      ),
    );
  }
}

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

  const env = yield* WorkerEnvironment;

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
