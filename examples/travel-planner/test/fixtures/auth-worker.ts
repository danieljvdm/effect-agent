import { EmailProofDelivery } from "@yielded/auth/Proofs";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, Redacted } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { handleRequest } from "../../src/worker";
export { TravelPlannerThread } from "./worker";
import { serveAuth } from "../../src/auth/host";

const key = btoa(String.fromCharCode(...new Uint8Array(32).fill(42)))
  .replace(/=/g, "")
  .replace(/\+/g, "-")
  .replace(/\//g, "_");

export const fixtureAuthConfig = {
  AUTH_ORIGIN: "https://planner.test",
  AUTH_BINDING_KEY: key,
  AUTH_PROOF_KEY: key,
  AUTH_TRANSACTION_KEY: key,
  AUTH_GITHUB_CLIENT_ID: "fixture-github",
  AUTH_GITHUB_CLIENT_SECRET: "fixture-github-secret",
  AUTH_EMAIL_FROM: "signin@example.invalid",
};

export class AuthFixture extends DurableObject {
  private deliveries: Array<{ code: string; email: string }> = [];
  async fetch(request: Request) {
    const url = new URL(request.url);

    if (url.pathname === "/_fixture/delivery")
      return Response.json({ ...this.deliveries.at(-1), count: this.deliveries.length });

    const delivery = EmailProofDelivery.layer(
      { vendorId: "fixture", idempotencyMillis: 0 },
      (message) =>
        Effect.sync(() => {
          this.deliveries.push({
            code: Redacted.value(message.secret),
            email: message.recipient.value,
          });

          return { _tag: "Accepted" as const };
        }),
    ).pipe(Layer.orDie);

    return Effect.runPromise(
      serveAuth(request, this.ctx.storage, fixtureAuthConfig, delivery).pipe(
        Effect.catchTag("AuthStorageError", (error) =>
          Effect.succeed(Response.json({ error: String(error.cause) }, { status: 500 })),
        ),
      ),
    );
  }
}

export default {
  fetch: (request: Request, env: Cloudflare.Env, ctx: ExecutionContext) =>
    new URL(request.url).pathname.startsWith("/_fixture/")
      ? env.AUTH.getByName("auth-v1").fetch(request)
      : Effect.runPromise(
          handleRequest()(request, env, ctx).pipe(Effect.provideService(WorkerEnvironment, env)),
        ),
};
