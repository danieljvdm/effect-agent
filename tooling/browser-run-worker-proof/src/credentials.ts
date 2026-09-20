import {
  BrowserCredentialAccess,
  CardCredential,
  CredentialAccessError,
  CredentialFillError,
  CredentialFillResult,
  FillCredentialRequest,
  LoginCredential,
  type CredentialAccessRequest,
} from "@effect-agent/platform-cloudflare/browser-credentials";
import {
  BrowserSessionError,
  BrowserSessionReference,
  BrowserSessions,
  type BrowserSession,
} from "@effect-agent/platform-cloudflare/browser-session";
import { Effect, Option, Redacted, Schema, Stream } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { BrowserRunWorkerProofResult } from "./contract.ts";

// An ordinary effectful Tool. Only credential identifiers and selectors enter its arguments;
// the invocation's host service resolves material. Never replay an uncertain fill.
const credentialTools = Toolkit.make(
  Tool.make("fill_credential", {
    parameters: FillCredentialRequest,
    success: CredentialFillResult,
    failure: Schema.Union([CredentialFillError, BrowserSessionError]),
    failureMode: "error",
    dependencies: [BrowserCredentialAccess],
  }),
);

class CredentialProofFailure extends Schema.TaggedError<CredentialProofFailure>()(
  "CredentialProofFailure",
  {},
) {}

const requireProof = (condition: boolean) =>
  condition ? Effect.void : Effect.fail(CredentialProofFailure.make({}));

const fillUsingTool = Effect.fnUntraced(function* (
  session: BrowserSession,
  request: FillCredentialRequest,
) {
  return yield* Effect.gen(function* () {
    const toolkit = yield* credentialTools;
    const response = yield* toolkit.handle("fill_credential", request);
    const last = yield* Stream.runLast(response);

    if (
      Option.isNone(last) ||
      last.value.preliminary ||
      !Schema.is(CredentialFillResult)(last.value.result)
    )
      return yield* CredentialProofFailure.make({});

    return last.value.result;
  }).pipe(
    Effect.provide(
      credentialTools.toLayer({ fill_credential: (input) => session.fillCredential(input) }),
    ),
  );
});

const ReferenceJson = Schema.fromJsonString(Schema.toCodecJson(BrowserSessionReference));

/** Dummy-only host owner. Real applications commit the reference and controller in durable storage. */
export const runCredentialProof = Effect.fn("runCredentialProof")(function* (origin: string) {
  const sessions = yield* BrowserSessions;
  let retained: string | undefined;
  let granted = true;

  const permitted = (request: CredentialAccessRequest) =>
    granted &&
    request.credential === "dummy" &&
    request.target.topOrigin === origin &&
    request.target.frameOrigin === origin &&
    request.target.recipientOrigin === origin;

  const authorize = Effect.suspend(() =>
    granted ? Effect.void : Effect.fail(CredentialAccessError.make({ reason: "denied" })),
  );

  const access = BrowserCredentialAccess.of({
    authorize: (request) =>
      Effect.suspend(() =>
        permitted(request)
          ? Effect.void
          : Effect.fail(CredentialAccessError.make({ reason: "denied" })),
      ),
    resolve: (request) =>
      Effect.suspend(() =>
        !permitted(request)
          ? Effect.fail(CredentialAccessError.make({ reason: "denied" }))
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
  });

  const owner = yield* Effect.acquireRelease(
    Effect.gen(function* () {
      const reference = yield* sessions.create({ maxElapsedMillis: 60_000 }, (reference) =>
        Schema.encodeEffect(ReferenceJson)(reference).pipe(
          Effect.tap((encoded) =>
            Effect.sync(() => {
              retained = encoded;
            }),
          ),
          Effect.asVoid,
        ),
      );

      const close = yield* Effect.cached(sessions.close(reference.sessionId));

      return { reference, close };
    }),
    (owner) => owner.close.pipe(Effect.orDie),
  );

  yield* Effect.scoped(
    Effect.gen(function* () {
      const session = yield* sessions.attach(owner.reference);

      for (const layout of ["a", "b"]) {
        yield* session.run(authorize, async (page) => {
          await page.goto(`${origin}/credentials/login-${layout}`);
        });

        const filled = yield* fillUsingTool(
          session,
          FillCredentialRequest.make({
            credential: "dummy",
            kind: "login",
            fields: [
              { selector: '[name="u"]', role: "username" },
              { selector: '[name="p"]', role: "password" },
            ],
          }),
        );

        yield* requireProof(filled.filled === 2);

        const authenticated = yield* session.run(authorize, async (page) => {
          await Promise.all([
            page.waitForNavigation({ waitUntil: "domcontentloaded" }),
            page.click('button, input[type="submit"]'),
          ]);

          return (await page.content()).includes("Authenticated dummy dashboard");
        });

        yield* requireProof(authenticated);
      }
    }).pipe(Effect.provideService(BrowserCredentialAccess, access)),
  );

  const reference = yield* Schema.decodeEffect(ReferenceJson)(retained ?? "");

  yield* Effect.scoped(
    Effect.gen(function* () {
      const session = yield* sessions.attach(reference);

      const retainedPage = yield* session.run(authorize, async (page) =>
        (await page.content()).includes("Authenticated dummy dashboard"),
      );

      yield* requireProof(retainedPage);
      yield* session.run(authorize, async (page) => {
        await page.goto(`${origin}/credentials/payment`);
      });

      const request = FillCredentialRequest.make({
        credential: "dummy",
        kind: "card",
        frame: ["iframe"],
        fields: [
          { selector: '[autocomplete="cc-name"]', role: "card-name" },
          { selector: '[autocomplete="cc-number"]', role: "card-number" },
          { selector: '[autocomplete="cc-exp"]', role: "card-expiry" },
          { selector: '[autocomplete="cc-csc"]', role: "card-security-code" },
        ],
      });

      granted = false;
      const denied = yield* fillUsingTool(session, request).pipe(Effect.flip);

      yield* requireProof(
        Schema.is(CredentialFillError)(denied) &&
          denied.reason === "denied" &&
          denied.dispatch === "not-dispatched" &&
          denied.cleanup === "not-requested" &&
          denied.filled === 0,
      );
      granted = true;
      const filled = yield* fillUsingTool(session, request);

      yield* requireProof(filled.filled === 4);

      const matches = yield* session.run(authorize, async (page) => {
        const frame = page
          .frames()
          .find((frame) => frame.url() === `${origin}/credentials/card-fields`);

        return frame === undefined
          ? false
          : await frame.$$eval(
              "input",
              (fields) =>
                fields.map((field) => field.value).join("|") ===
                "Dummy Only|4111111111111111|12/30|123",
            );
      });

      yield* requireProof(matches);
    }).pipe(Effect.provideService(BrowserCredentialAccess, access)),
  );
  yield* owner.close;

  return BrowserRunWorkerProofResult.fields.browserCredentials.make({
    loginLayouts: 2,
    authenticatedContinuation: true,
    retainedSession: true,
    revokedFillRefused: true,
    cardFilled: true,
    closed: true,
  });
}, Effect.scoped);

/** Controlled fixtures only; no vault, submitted values, or real card charge is exposed. */
export const credentialFixture = Effect.fnUntraced(function* (request: Request) {
  const path = new URL(request.url).pathname;

  const html = (body: string) =>
    new Response(body, {
      headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
    });

  if (path === "/credentials/accept" && request.method === "POST") {
    const data = yield* Effect.tryPromise({
      try: () => request.formData(),
      catch: () => CredentialProofFailure.make({}),
    });

    if (data.get("u") !== "dummy@example.test" || data.get("p") !== "dummy-proof-password")
      return new Response("Denied", { status: 403 });

    return new Response(null, {
      status: 303,
      headers: {
        location: "/credentials/dashboard",
        "set-cookie":
          "dummy-session=authorized; Secure; HttpOnly; SameSite=Strict; Path=/credentials/",
      },
    });
  }
  if (path === "/credentials/dashboard")
    return html(
      request.headers.get("cookie")?.includes("dummy-session=authorized")
        ? "Authenticated dummy dashboard"
        : "Unauthenticated",
    );
  if (path === "/credentials/payment")
    return html('<iframe src="/credentials/card-fields"></iframe>');
  if (path === "/credentials/card-fields")
    return html(
      '<form><input autocomplete="cc-name"><input autocomplete="cc-number"><input autocomplete="cc-exp"><input autocomplete="cc-csc"><button>Pay</button></form>',
    );
  if (path === "/credentials/login-a")
    return html(
      '<form action="/credentials/accept" method="post"><label>Account<input name="u" autocomplete="username"></label><label>Password<input name="p" type="password"></label><button>Sign in</button></form>',
    );
  if (path === "/credentials/login-b")
    return html(
      '<input form="login" name="u" type="email" autocomplete="username"><form id="login" action="/credentials/accept" method="post"><input name="p" type="password"><input type="submit" value="Continue"></form>',
    );

  return new Response("Not found", { status: 404 });
});
