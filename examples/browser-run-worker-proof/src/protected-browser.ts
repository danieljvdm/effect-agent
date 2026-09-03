import {
  BrowserCredentialAccess,
  CardCredential,
  CredentialAccessError,
  CredentialFieldRole,
  CredentialOffer,
  CredentialOfferMetadata,
  CredentialUseResult,
  InteractiveBrowserPolicy,
  ListCredentialOffers,
  LoginCredential,
  ProtectedBrowserError,
  ProtectedBrowserNavigate,
  ProtectedBrowserObservation,
  ProtectedBrowserSession,
  UseCredential,
} from "@effect-agent/sandbox";
import { Crypto, Effect, Layer, Option, Redacted, Schema, Stream } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { BrowserRunWorkerProofResult } from "./contract.ts";

// Ordinary Tools: no readonly/idempotent annotation and no Code Mode bridge. A durable host
// must not replay an unresolved credential mutation. Only opaque refs cross this boundary.
export const protectedTools = Toolkit.make(
  Tool.make("protected_navigate", {
    parameters: ProtectedBrowserNavigate,
    success: Schema.Void,
    failure: ProtectedBrowserError,
    failureMode: "error",
  }),
  Tool.make("protected_observe", {
    parameters: Schema.Struct({}),
    success: ProtectedBrowserObservation,
    failure: ProtectedBrowserError,
    failureMode: "error",
  }),
  Tool.make("credential_offers", {
    parameters: ListCredentialOffers,
    success: Schema.Array(CredentialOffer),
    failure: ProtectedBrowserError,
    failureMode: "error",
  }),
  Tool.make("credential_use", {
    parameters: UseCredential,
    success: CredentialUseResult,
    failure: ProtectedBrowserError,
    failureMode: "error",
  }),
);

export const protectedHandlers = protectedTools.toLayer(
  Effect.gen(function* () {
    const session = yield* ProtectedBrowserSession;

    return {
      protected_navigate: (request) =>
        Effect.flatMap(session.get, (handle) => handle.navigate(request)),
      protected_observe: () => Effect.flatMap(session.get, (handle) => handle.observe),
      credential_offers: (request) =>
        Effect.flatMap(session.get, (handle) => handle.listCredentialOffers(request)),
      credential_use: (request) =>
        Effect.flatMap(session.get, (handle) => handle.useCredential(request)),
    };
  }),
);

class ProtectedProofFailure extends Schema.TaggedError<ProtectedProofFailure>()(
  "ProtectedProofFailure",
  {},
) {}

const final = <A, E, R, E2, R2>(
  response: Effect.Effect<
    Stream.Stream<
      {
        readonly result: A | ProtectedBrowserError;
        readonly preliminary: boolean;
      },
      E,
      R
    >,
    E2,
    R2
  >,
) =>
  response.pipe(
    Effect.flatMap(Stream.runLast),
    Effect.flatMap((last): Effect.Effect<A, ProtectedBrowserError | ProtectedProofFailure> => {
      if (Option.isNone(last) || last.value.preliminary)
        return Effect.fail(new ProtectedProofFailure());

      return Schema.is(ProtectedBrowserError)(last.value.result)
        ? Effect.fail(last.value.result)
        : Effect.succeed(last.value.result);
    }),
  );

const requireProof = (condition: boolean) =>
  condition ? Effect.void : Effect.fail(new ProtectedProofFailure());

/** Dummy-only invocation. In production, authenticate the invocation and consult a real vault. */
export const runProtectedProof = Effect.fn("runProtectedProof")(function* (origin: string) {
  let granted = true;
  const crypto = yield* Crypto.Crypto;
  const principal = Redacted.make(yield* crypto.randomUUIDv4);

  const permitted = (request: {
    readonly caller: Redacted.Redacted<string>;
    readonly target: {
      readonly topOrigin: string;
      readonly frameOrigin: string;
      readonly recipientOrigin: string;
    };
  }) =>
    granted &&
    Redacted.value(request.caller) === Redacted.value(principal) &&
    request.target.topOrigin === origin &&
    request.target.frameOrigin === origin &&
    request.target.recipientOrigin === origin;

  const access = BrowserCredentialAccess.of({
    caller: Effect.succeed(principal),
    list: (request) =>
      permitted(request)
        ? Effect.succeed([
            {
              key: Redacted.make("dummy"),
              metadata: CredentialOfferMetadata.make({ label: "Temporary proof fixture" }),
            },
          ])
        : Effect.fail(new CredentialAccessError({ reason: "denied" })),
    authorize: (request) =>
      Effect.suspend(() =>
        permitted(request)
          ? Effect.void
          : Effect.fail(new CredentialAccessError({ reason: "denied" })),
      ),
    resolve: (request) =>
      Effect.suspend(() =>
        !permitted(request)
          ? Effect.fail(new CredentialAccessError({ reason: "denied" }))
          : Effect.succeed(
              request.kind === "login"
                ? LoginCredential.make({
                    username: Redacted.make("dummy@example.test"),
                    password: Redacted.make("dummy-proof-password"),
                  })
                : CardCredential.make({
                    name: Redacted.make("Dummy Only"),
                    number: Redacted.make("4111111111111111"),
                    expiry: Redacted.make("12/30"),
                    expiryMonth: Redacted.make("12"),
                    expiryYear: Redacted.make("2030"),
                    securityCode: Redacted.make("123"),
                  }),
            ),
      ),
    observation: (request) =>
      Effect.succeed(
        Redacted.value(request.caller) === Redacted.value(principal) &&
          request.topOrigin === origin &&
          request.frameOrigins.every((frame) => frame === origin)
          ? "trust-recipient-no-credential-echo"
          : "deny",
      ),
  });

  const session = ProtectedBrowserSession.layer(
    InteractiveBrowserPolicy.make({
      network: { _tag: "ExactHosts", allowedHosts: [new URL(origin).host] },
      maxActions: 80,
      maxElapsedMillis: 60_000,
      maxReturnedBytes: 16_384,
    }),
  ).pipe(Layer.provide(Layer.succeed(BrowserCredentialAccess)(access)));

  return yield* Effect.gen(function* () {
    const toolkit = yield* protectedTools;

    for (const layout of ["a", "b"]) {
      yield* final(
        toolkit.handle("protected_navigate", { url: `${origin}/protected/login-${layout}` }),
      );
      const observed = yield* final(toolkit.handle("protected_observe", {}));
      const username = observed.controls.find((control) => control.role === "username");
      const password = observed.controls.find((control) => control.role === "password");
      const submit = observed.controls.find((control) => control.role === "submit");

      if (!username || !password || !submit) return yield* new ProtectedProofFailure();

      const offers = yield* final(
        toolkit.handle("credential_offers", { kind: "login", target: username.ref }),
      );

      if (!offers[0]) return yield* new ProtectedProofFailure();

      const result = yield* final(
        toolkit.handle("credential_use", {
          offer: offers[0].ref,
          fields: [
            { ref: username.ref, role: "username" },
            { ref: password.ref, role: "password" },
          ],
          submit: submit.ref,
        }),
      );

      yield* requireProof(result.milestone === "submission-dispatched");
      // Wait using observations, never retry submission. Keep the authenticated private context.
      let authenticated = false;

      for (let attempt = 0; attempt < 30 && !authenticated; attempt++) {
        yield* Effect.sleep("100 millis");

        const next = yield* final(toolkit.handle("protected_observe", {})).pipe(
          Effect.map(Option.some),
          Effect.catch((error) =>
            Schema.is(ProtectedBrowserError)(error) && error.reason === "stale-reference"
              ? Effect.succeed(Option.none())
              : Effect.fail(error),
          ),
        );

        authenticated =
          Option.isSome(next) && next.value.text.includes("Authenticated dummy dashboard");
      }
      yield* requireProof(authenticated);
    }
    yield* final(toolkit.handle("protected_navigate", { url: `${origin}/protected/payment` }));
    const observed = yield* final(toolkit.handle("protected_observe", {}));
    const fields = observed.controls.filter((control) => control.role.startsWith("card-"));

    if (fields.length !== 4 || !fields[0]) return yield* new ProtectedProofFailure();

    const offers = yield* final(
      toolkit.handle("credential_offers", { kind: "card", target: fields[0].ref }),
    );

    if (!offers[0]) return yield* new ProtectedProofFailure();

    const request = UseCredential.make({
      offer: offers[0].ref,
      fields: fields.map((field) => ({
        ref: field.ref,
        role: Schema.decodeUnknownSync(CredentialFieldRole)(field.role),
      })),
    });

    granted = false;
    const denied = yield* final(toolkit.handle("credential_use", request)).pipe(Effect.flip);

    yield* requireProof(
      Schema.is(ProtectedBrowserError)(denied) &&
        denied.reason === "denied" &&
        denied.dispatch === "not-dispatched",
    );
    granted = true;
    const filled = yield* final(toolkit.handle("credential_use", request));

    yield* requireProof(filled.milestone === "filled");
    const after = yield* final(toolkit.handle("protected_observe", {}));

    yield* requireProof(!JSON.stringify(after).includes("4111111111111111"));
    const handle = yield* (yield* ProtectedBrowserSession).get;

    yield* requireProof((yield* handle.close) === "confirmed");

    return Schema.decodeUnknownSync(BrowserRunWorkerProofResult.fields.protectedBrowser)({
      loginLayouts: 2,
      authenticatedContinuation: true,
      revokedOfferRefused: true,
      cardFilled: true,
      closed: true,
    });
  }).pipe(Effect.provide(protectedHandlers.pipe(Layer.provideMerge(session))), Effect.scoped);
});

/** Controlled fixtures only; no vault, submitted values, or real card charge is exposed. */
export const protectedFixture = (request: Request) =>
  Effect.gen(function* () {
    const path = new URL(request.url).pathname;

    const html = (body: string) =>
      new Response(body, {
        headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
      });

    if (path === "/protected/accept" && request.method === "POST") {
      const data = yield* Effect.tryPromise({
        try: () => request.formData(),
        catch: () => new ProtectedProofFailure(),
      });

      if (data.get("u") !== "dummy@example.test" || data.get("p") !== "dummy-proof-password")
        return new Response("Denied", { status: 403 });

      return new Response(null, {
        status: 303,
        headers: {
          location: "/protected/dashboard",
          "set-cookie":
            "dummy-session=authorized; Secure; HttpOnly; SameSite=Strict; Path=/protected/",
        },
      });
    }
    if (path === "/protected/dashboard")
      return html(
        request.headers.get("cookie")?.includes("dummy-session=authorized")
          ? "Authenticated dummy dashboard"
          : "Unauthenticated",
      );
    if (path === "/protected/payment")
      return html('<iframe src="/protected/card-fields"></iframe>');
    if (path === "/protected/card-fields")
      return html(
        '<form><input autocomplete="cc-name"><input autocomplete="cc-number"><input autocomplete="cc-exp"><input autocomplete="cc-csc"><button>Pay</button></form>',
      );
    if (path === "/protected/login-a")
      return html(
        '<form action="/protected/accept" method="post"><label>Account<input name="u" autocomplete="username"></label><label>Password<input name="p" type="password"></label><button>Sign in</button></form>',
      );
    if (path === "/protected/login-b")
      return html(
        '<input form="login" name="u" type="email" autocomplete="username"><form id="login" action="/protected/accept" method="post"><input name="p" type="password"><input type="submit" value="Continue"></form>',
      );

    return new Response("Not found", { status: 404 });
  });
