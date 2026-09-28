import { BrowserSessions } from "@effect-agent/platform-cloudflare/browser-session";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpRouter, HttpServer } from "effect/unstable/http";
import { HttpApiBuilder } from "effect/unstable/httpapi";

import { LabApi, LabError, ModelApi, modelChoices } from "./contract.ts";
import { connectKitesurf } from "./kitesurf.ts";
import { Control, emptyControl, makeOwner } from "./owner.ts";

export interface Env {
  readonly LAB: DurableObjectNamespace<BrowserLab>;
  readonly BROWSER: BrowserRun;
  readonly ASSETS: Fetcher;
  readonly CLOUDFLARE_ACCOUNT_ID?: string;
  readonly BROWSER_RENDERING_API_TOKEN?: string;
  readonly OPENAI_API_KEY?: string;
  readonly OPENAI_API_URL?: string;
  readonly OPENAI_API_TYPE?: string;
  readonly OPENAI_MODEL?: string;
  readonly WORKERS_AI_API_KEY?: string;
  readonly TYPESAFE_API_KEY?: string;
}

const codec = Schema.fromJsonString(Control);

export class BrowserLab extends DurableObject<Env> {
  private readonly owner;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const hit = (location: string) => this.failpoint(location);

    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS browser_lab (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)",
    );
    this.owner = makeOwner(
      {
        read: () => {
          const row = ctx.storage.sql
            .exec<{ value: string }>("SELECT value FROM browser_lab WHERE id=1")
            .toArray()[0];

          return row === undefined ? emptyControl : Schema.decodeSync(codec)(row.value);
        },
        write: (state, transition) => {
          this.failpoint(`before:${transition}`);
          ctx.storage.sql.exec(
            "INSERT OR REPLACE INTO browser_lab(id,value) VALUES (1,?)",
            Schema.encodeSync(codec)(state),
          );
          this.failpoint(`after:${transition}`);
        },
        alarm: Effect.fnUntraced(function* (at: number | null) {
          const transition = at === null ? "alarm-clear" : "alarm-set";

          hit(`before:${transition}`);
          yield* Effect.tryPromise({
            try: () => (at === null ? ctx.storage.deleteAlarm() : ctx.storage.setAlarm(at)),
            catch: () =>
              new LabError({ code: "storage", message: "Could not schedule browser cleanup." }),
          });
          hit(`after:${transition}`);
        }),
      },
      {
        model: env.OPENAI_MODEL || "gpt-6-luna",
        apiKey: env.OPENAI_API_KEY ?? "",
        ...(env.OPENAI_API_URL ? { apiUrl: env.OPENAI_API_URL } : {}),
        apiType: Schema.decodeUnknownSync(ModelApi)(env.OPENAI_API_TYPE || "responses"),
        browserConfigured: Boolean(env.CLOUDFLARE_ACCOUNT_ID && env.BROWSER_RENDERING_API_TOKEN),
        models: modelChoices.map(({ id, label }) => ({
          model: id,
          label,
          apiKey: id.startsWith("@cf/")
            ? (env.WORKERS_AI_API_KEY ?? "")
            : (env.OPENAI_API_KEY ?? ""),
          apiUrl: id.startsWith("@cf/")
            ? `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/ai/v1`
            : "https://api.openai.com/v1",
          apiType: id.startsWith("@cf/") ? "chat-completions" : "responses",
        })),
        jevApiKey: env.TYPESAFE_API_KEY,
        kitesurf: (retainClose) =>
          connectKitesurf(
            {
              accountId: env.CLOUDFLARE_ACCOUNT_ID ?? "",
              apiToken: Redacted.make(env.BROWSER_RENDERING_API_TOKEN ?? ""),
            },
            retainClose,
          ),
      },
    );
  }

  /** Overridden only by a test subclass; never selected by an HTTP request. */
  protected failpoint(_location: string): void {}

  private get sessions() {
    return BrowserSessions.layer({
      browser: this.env.BROWSER,
      accountId: this.env.CLOUDFLARE_ACCOUNT_ID ?? "",
      apiToken: Redacted.make(this.env.BROWSER_RENDERING_API_TOKEN ?? ""),
    }).pipe(Layer.provide(FetchHttpClient.layer));
  }

  private withBrowser<A>(effect: Effect.Effect<A, LabError, BrowserSessions>) {
    return effect.pipe(
      Effect.provide(this.sessions),
      Effect.catchTag(
        "BrowserRunCleanupError",
        () =>
          new LabError({
            code: "configuration",
            message: "Browser lifecycle credentials are invalid or unavailable.",
          }),
      ),
    );
  }

  fetch(request: Request): Promise<Response> {
    const handlers = HttpApiBuilder.group(LabApi, "lab", (group) =>
      group
        .handle("snapshot", () => this.owner.snapshot())
        .handle("run", ({ payload }) => this.withBrowser(this.owner.run(payload)))
        .handle("stop", () => this.owner.stop())
        .handle("close", () => this.withBrowser(this.owner.close())),
    );

    const routes = HttpApiBuilder.layer(LabApi).pipe(
      Layer.provide(handlers),
      Layer.provide(HttpServer.layerServices),
    );

    return Effect.runPromise(
      Effect.gen(function* () {
        const web = yield* Effect.acquireRelease(
          Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
          (value) => Effect.promise(() => value.dispose()),
        );

        const response = yield* Effect.promise(() => web.handler(request));
        const body = yield* Effect.promise(() => response.arrayBuffer());

        return new Response(body.byteLength === 0 ? null : body, {
          status: response.status,
          headers: { ...Object.fromEntries(response.headers), "cache-control": "no-store" },
        });
      }).pipe(Effect.scoped),
    );
  }

  alarm(): Promise<void> {
    return Effect.runPromise(
      this.withBrowser(this.owner.stop().pipe(Effect.andThen(this.owner.close()))),
    );
  }
}

const errorResponse = (message: string, status: number) =>
  new Response(
    Schema.encodeSync(Schema.fromJsonString(LabError))(
      new LabError({ code: "configuration", message }),
    ),
    { status, headers: { "content-type": "application/json", "cache-control": "no-store" } },
  );

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (!url.pathname.startsWith("/api/")) return env.ASSETS.fetch(request);
    if (
      request.method !== "GET" &&
      request.headers.has("origin") &&
      request.headers.get("origin") !== url.origin
    )
      return errorResponse("Request origin does not match the lab.", 400);

    const session = Schema.decodeUnknownOption(Schema.String.check(Schema.isUUID()))(
      request.headers.get("x-lab-session"),
    );

    if (session._tag === "None") return errorResponse("Invalid lab session.", 400);
    if (
      url.pathname !== "/api/snapshot" &&
      (!env.CLOUDFLARE_ACCOUNT_ID || !env.BROWSER_RENDERING_API_TOKEN)
    )
      return errorResponse(
        "Set CLOUDFLARE_ACCOUNT_ID and BROWSER_RENDERING_API_TOKEN in the Worker.",
        400,
      );

    return env.LAB.get(env.LAB.idFromName(session.value)).fetch(request);
  },
} satisfies ExportedHandler<Env>;
