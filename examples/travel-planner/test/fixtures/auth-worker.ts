import { EmailProofDelivery } from "@yielded/auth/Proofs";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, Redacted } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { handleRequest } from "../../src/worker";
export { TravelPlannerThread } from "./worker";
import { serveAuth } from "../../src/auth/host";
import type { GithubRejectionReason } from "../../src/auth/oauth-diagnostics";
import {
  initializeAuthStorage,
  AuthStorageFailpoint,
  AuthStorageError,
} from "../../src/auth/storage";

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
  private deliveryFailure = false;
  private rejections: GithubRejectionReason[] = [];
  private reporterMode: string | null = null;
  async fetch(request: Request) {
    const url = new URL(request.url);

    if (url.pathname === "/_fixture/rejections") return Response.json(this.rejections);
    if (url.pathname === "/_fixture/reporter") {
      this.reporterMode = url.searchParams.get("mode");

      return new Response(null);
    }

    if (url.pathname === "/_fixture/delivery")
      return Response.json({ ...this.deliveries.at(-1), count: this.deliveries.length });
    if (url.pathname === "/_fixture/fail-delivery") {
      this.deliveryFailure = url.searchParams.get("enabled") !== "false";

      return new Response(null);
    }
    if (url.pathname === "/_fixture/expire") {
      this.ctx.storage.sql.exec("update auth_proof_generation set expiresAt = 0");

      return new Response(null);
    }

    const delivery = EmailProofDelivery.layer(
      { vendorId: "fixture", idempotencyMillis: 0 },
      (message) =>
        Effect.sync(() => {
          if (this.deliveryFailure) return { _tag: "Ambiguous" as const };
          this.deliveries.push({
            code: Redacted.value(message.secret),
            email: message.recipient.value,
          });

          return { _tag: "Accepted" as const };
        }),
    ).pipe(Layer.orDie);

    return Effect.runPromise(
      serveAuth(request, this.ctx.storage, fixtureAuthConfig, delivery, undefined, (reason) =>
        Effect.suspend(() => {
          if (this.reporterMode === "defect") return Effect.die("Fixture logger defect");
          if (this.reporterMode === "interrupt") return Effect.interrupt;
          if (this.reporterMode === "timeout") return Effect.never;

          return Effect.sync(() => {
            this.rejections.push(reason);
          });
        }),
      ).pipe(
        Effect.catchTag("AuthStorageError", (error) =>
          Effect.succeed(Response.json({ error: String(error.cause) }, { status: 500 })),
        ),
      ),
    );
  }
}

export class AuthStorageFixture extends DurableObject {
  async fetch(request: Request) {
    const mode = new URL(request.url).searchParams.get("mode");

    if (mode === "unsupported") this.ctx.storage.sql.exec("update auth_format set version = 99");

    const exit = await Effect.runPromiseExit(
      initializeAuthStorage(this.ctx.storage).pipe(
        Effect.provideService(AuthStorageFailpoint, {
          hit: (point) =>
            mode === point
              ? Effect.fail(new AuthStorageError({ cause: "Injected storage fault" }))
              : mode === "defect"
                ? Effect.die("Injected defect")
                : mode === "interrupt"
                  ? Effect.interrupt
                  : mode === "timeout"
                    ? Effect.never
                    : Effect.void,
        }),
        Effect.timeout("20 millis"),
      ),
    );

    return Response.json({
      outcome: exit._tag,
      tables: this.ctx.storage.sql
        .exec(
          "select name from sqlite_master where type='table' and name like 'auth_%' order by name",
        )
        .toArray(),
    });
  }
}

export default {
  fetch: (
    request: Request,
    env: Cloudflare.Env & { STORAGE: DurableObjectNamespace },
    ctx: ExecutionContext,
  ) =>
    new URL(request.url).pathname === "/_fixture/storage"
      ? env.STORAGE.getByName(new URL(request.url).searchParams.get("id") ?? "schema").fetch(
          request,
        )
      : new URL(request.url).pathname.startsWith("/_fixture/")
        ? env.AUTH.getByName("auth-v1").fetch(request)
        : Effect.runPromise(
            handleRequest()(request, env, ctx).pipe(Effect.provideService(WorkerEnvironment, env)),
          ),
};
