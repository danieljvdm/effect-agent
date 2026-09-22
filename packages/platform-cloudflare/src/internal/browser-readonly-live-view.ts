import { Context, Effect, Layer, Redacted, Schema, Stream } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";

import { BrowserRunLiveViewRequest, BrowserRunLiveViewResult } from "../InteractiveBrowser.ts";
import { browserFailure, BrowserRunFailure } from "./browser-failure.ts";
import type { BrowserRunLifecycleOptions } from "./browser-session-lifecycle.ts";

const Identity = Schema.Struct({
  sessionId: Schema.String.check(Schema.isUUID()),
  targetId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
});

const Reply = Schema.fromJsonString(
  Schema.Struct({
    id: Identity.fields.targetId,
    options: Schema.Struct({
      mode: Schema.Literal("tab"),
      guardrails: Schema.Struct({ mode: Schema.Literal("readonly") }),
    }),
    devtoolsFrontendUrl: Schema.String,
  }),
);

const operation = "session.readonlyLiveView";

/** Only the REST endpoint can mint provider-enforced read-only connections. */
export class BrowserRunReadonlyLiveView extends Context.Service<
  BrowserRunReadonlyLiveView,
  {
    readonly mint: (
      sessionId: Redacted.Redacted<string>,
      targetId: Redacted.Redacted<string>,
      request: BrowserRunLiveViewRequest,
    ) => Effect.Effect<BrowserRunLiveViewResult, BrowserRunFailure>;
  }
>()("@effect-agent/platform-cloudflare/internal/BrowserRunReadonlyLiveView") {
  static layer(options: BrowserRunLifecycleOptions) {
    return Layer.effect(
      this,
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;

        const mint = Effect.fnUntraced(
          function* (
            sessionId: Redacted.Redacted<string>,
            targetId: Redacted.Redacted<string>,
            request: BrowserRunLiveViewRequest,
          ) {
            const identity = yield* Schema.decodeUnknownEffect(Identity)({
              sessionId: Redacted.value(sessionId),
              targetId: Redacted.value(targetId),
            });

            const input = yield* Schema.decodeUnknownEffect(BrowserRunLiveViewRequest)(request);

            const response = yield* client
              .execute(
                HttpClientRequest.post(
                  `https://api.cloudflare.com/client/v4/accounts/${options.accountId}/browser-rendering/devtools/browser/${identity.sessionId}/live_view`,
                ).pipe(
                  HttpClientRequest.bearerToken(options.apiToken),
                  HttpClientRequest.bodyJsonUnsafe({
                    mode: input.mode,
                    expiresInMs: input.expiresInMs,
                    targetId: identity.targetId,
                    guardrails: { mode: "readonly" },
                  }),
                ),
              )
              .pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }));

            if (response.status !== 200)
              return yield* new BrowserRunFailure({
                operation,
                reason: "provider",
                status: response.status,
              });

            const bytes = yield* Stream.runFoldEffect(
              response.stream,
              () => new Uint8Array(),
              (body, chunk) => {
                if (body.byteLength + chunk.byteLength > 16_384)
                  return Effect.fail(new BrowserRunFailure({ operation, reason: "malformed" }));
                const combined = new Uint8Array(body.byteLength + chunk.byteLength);

                combined.set(body);
                combined.set(chunk, body.byteLength);

                return Effect.succeed(combined);
              },
            );

            const reply = yield* Schema.decodeEffect(Reply)(new TextDecoder().decode(bytes));

            if (reply.id !== identity.targetId)
              return yield* new BrowserRunFailure({ operation, reason: "malformed" });

            return yield* Schema.decodeEffect(BrowserRunLiveViewResult)({
              devtoolsFrontendUrl: Redacted.make(reply.devtoolsFrontendUrl),
            });
          },
          Effect.mapError((cause) => browserFailure(operation, cause)),
          Effect.withTracerEnabled(false),
        );

        return { mint };
      }),
    );
  }
}
