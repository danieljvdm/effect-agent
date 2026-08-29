import { Context, Effect, Layer, Option, Redacted, Schema, Stream } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";

export class BrowserRunCleanupError extends Schema.TaggedError<BrowserRunCleanupError>()(
  "BrowserRunCleanupError",
  {
    reason: Schema.Literals([
      "configuration",
      "authorization",
      "rate-limited",
      "provider",
      "malformed",
      "timeout",
      "pending",
    ]),
  },
) {}

export interface BrowserRunLifecycleOptions {
  readonly accountId: string;
  readonly apiToken: Redacted.Redacted<string>;
}

const Identity = Schema.String.check(Schema.isUUID());
const Account = Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/));
const Closed = Schema.Struct({ status: Schema.Literals(["closing", "closed"]) });
const Metadata = Schema.Struct({ sessionId: Identity, endTime: Schema.optionalKey(Schema.Finite) });
// This exact provider response is accepted only on a fixed-origin, authenticated,
// exact-session request. An arbitrary 404 or a listing omission is never absence.
const Absent = Schema.Struct({ error: Schema.Literal("Session not found") });

export class BrowserRunSessionLifecycle extends Context.Service<
  BrowserRunSessionLifecycle,
  {
    readonly close: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, BrowserRunCleanupError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserRunSessionLifecycle") {
  static layer(options: BrowserRunLifecycleOptions) {
    return Layer.effect(
      this,
      Effect.gen(function* () {
        if (
          !Schema.is(Account)(options.accountId) ||
          Redacted.value(options.apiToken).length === 0
        ) {
          return yield* new BrowserRunCleanupError({ reason: "configuration" });
        }
        const client = yield* HttpClient.HttpClient;
        const request = Effect.fn("BrowserRunSessionLifecycle.request")(function* (
          method: "GET" | "DELETE",
          sessionId: string,
        ) {
          const path = method === "DELETE" ? "browser" : "session";
          const response = yield* client
            .execute(
              HttpClientRequest.make(method)(
                `https://api.cloudflare.com/client/v4/accounts/${options.accountId}/browser-rendering/devtools/${path}/${sessionId}`,
              ).pipe(HttpClientRequest.bearerToken(options.apiToken)),
            )
            .pipe(
              Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
              Effect.mapError(() => new BrowserRunCleanupError({ reason: "provider" })),
            );
          if (response.status === 401 || response.status === 403)
            return yield* new BrowserRunCleanupError({ reason: "authorization" });
          if (response.status === 429)
            return yield* new BrowserRunCleanupError({ reason: "rate-limited" });
          if (response.status !== 200 && response.status !== 404)
            return yield* new BrowserRunCleanupError({ reason: "provider" });
          if (response.headers["content-type"]?.split(";", 1)[0]?.trim() !== "application/json")
            return yield* new BrowserRunCleanupError({ reason: "malformed" });
          const bytes = yield* Stream.runFoldEffect(
            response.stream,
            () => new Uint8Array(),
            (body, chunk) => {
              if (body.byteLength + chunk.byteLength > 16_384)
                return Effect.fail(new BrowserRunCleanupError({ reason: "malformed" }));
              const combined = new Uint8Array(body.byteLength + chunk.byteLength);
              combined.set(body);
              combined.set(chunk, body.byteLength);
              return Effect.succeed(combined);
            },
          ).pipe(Effect.mapError(() => new BrowserRunCleanupError({ reason: "malformed" })));
          const body = yield* Effect.try({
            try: () => new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes),
            catch: () => new BrowserRunCleanupError({ reason: "malformed" }),
          });
          if (response.status === 404) {
            const absent = Schema.decodeUnknownOption(Schema.fromJsonString(Absent))(body, {
              onExcessProperty: "error",
            });
            if (Option.isSome(absent)) return true;
            return yield* new BrowserRunCleanupError({ reason: "malformed" });
          }
          if (method === "DELETE") {
            const result = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Closed))(
              body,
            ).pipe(Effect.mapError(() => new BrowserRunCleanupError({ reason: "malformed" })));
            return result.status === "closed";
          }
          const result = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Metadata))(
            body,
          ).pipe(Effect.mapError(() => new BrowserRunCleanupError({ reason: "malformed" })));
          if (result.sessionId !== sessionId)
            return yield* new BrowserRunCleanupError({ reason: "malformed" });
          return result.endTime !== undefined && result.endTime > 0;
        });
        const close = Effect.fn("BrowserRunSessionLifecycle.close")(
          function* (sessionId: Redacted.Redacted<string>) {
            const id = yield* Schema.decodeUnknownEffect(Identity)(Redacted.value(sessionId)).pipe(
              Effect.mapError(() => new BrowserRunCleanupError({ reason: "configuration" })),
            );
            if (yield* request("DELETE", id)) return;
            for (let read = 0; read < 2; read++) {
              if (yield* request("GET", id)) return;
            }
            return yield* new BrowserRunCleanupError({ reason: "pending" });
          },
          Effect.timeoutOrElse({
            duration: "10 seconds",
            orElse: () => Effect.fail(new BrowserRunCleanupError({ reason: "timeout" })),
          }),
          Effect.withTracerEnabled(false),
        );
        return { close };
      }),
    );
  }
}
