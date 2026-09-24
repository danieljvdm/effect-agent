import { expect, it } from "@effect/vitest";
import { Config, Effect, Layer, Option, Redacted, Schema } from "effect";
import nativePuppeteer from "puppeteer-core";
import browserPuppeteer, {
  type Page,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";
import { type TestContext, vi } from "vite-plus/test";

import {
  BrowserCredentialAccess,
  CredentialAccessError,
  FillCredentialRequest,
  LoginCredential,
} from "../src/BrowserCredentials.ts";
import { BrowserSessionPage, fillCredential } from "../src/internal/browser-credentials.ts";

const secret = "dummy-credential-secret-sentinel";

const login = LoginCredential.make({
  username: Redacted.make("dummy@example.test"),
  password: Redacted.make(secret),
});

const loginRequest = FillCredentialRequest.make({
  credential: "login",
  kind: "login",
  fields: [
    { selector: "#username", role: "username" },
    { selector: "#password", role: "password" },
  ],
});

const access = (overrides: Partial<BrowserCredentialAccess["Service"]> = {}) =>
  BrowserCredentialAccess.of({
    authorize: (request) => {
      const frameOrigin = "https://merchant.test";

      const recipientOrigin = "https://auth.test";

      return request.credential === request.kind &&
        request.target.topOrigin === "https://merchant.test" &&
        request.target.frameOrigin === frameOrigin &&
        request.target.recipientOrigin === recipientOrigin
        ? Effect.void
        : Effect.fail(new CredentialAccessError({ reason: "denied" }));
    },
    resolve: () => Effect.succeed(login),
    ...overrides,
  });

const html = `<form action="https://auth.test/session" onsubmit="event.preventDefault();document.querySelector('main').textContent='Signed in'">
  <input id="username"><input id="password" type="password"><button>Sign in</button></form>
  <main>Login</main>
  <script>window.writes=[];document.addEventListener('input',event=>window.writes.push(event.target.id));</script>`;

const fixture = Effect.fnUntraced(function* (test: TestContext) {
  const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

  if (Option.isNone(executable)) return test.skip();

  const launched = yield* Effect.acquireRelease(
    Effect.promise(() =>
      nativePuppeteer.launch({ executablePath: executable.value, headless: true }),
    ),
    (browser) => Effect.promise(() => browser.close()),
  );

  const browser = yield* Effect.acquireRelease(
    Effect.promise(() => browserPuppeteer.connect({ browserWSEndpoint: launched.wsEndpoint() })),
    (browser) => Effect.promise(() => browser.disconnect()),
  );

  const page = yield* Effect.promise(() => browser.newPage());

  yield* Effect.promise(() => page.setRequestInterception(true));
  page.on("request", (request) => {
    void request
      .respond({
        contentType: "text/html",
        body: html,
      })
      .catch(() => {});
  });
  yield* Effect.promise(() => page.goto("https://merchant.test/login"));

  return BrowserSessionPage.of({ page, commandTimeoutMillis: 30_000 });
});

const read = (page: Page) =>
  Effect.promise(() =>
    page.evaluate(`({
  values: [document.querySelector('#username').value, document.querySelector('#password').value],
  writes: window.writes, text: document.querySelector('main').textContent
})`),
  ).pipe(
    Effect.flatMap(
      Schema.decodeUnknownEffect(
        Schema.Struct({
          values: Schema.Array(Schema.String),
          writes: Schema.Array(Schema.String),
          text: Schema.String,
        }),
      ),
    ),
    Effect.orDie,
  );

for (const mode of ["revoked", "replaced", "form-action"] as const) {
  it.live(
    `rechecks ${mode} authority or targets after resolution before writing`,
    (test) =>
      Effect.gen(function* () {
        const { page } = yield* BrowserSessionPage;
        let allowed = true;

        const authority = access({
          authorize: () =>
            allowed ? Effect.void : Effect.fail(new CredentialAccessError({ reason: "denied" })),
          resolve: () =>
            Effect.gen(function* () {
              if (mode === "revoked") allowed = false;
              else
                yield* Effect.promise(() =>
                  page.evaluate(
                    mode === "replaced"
                      ? "document.querySelector('#password').replaceWith(document.querySelector('#password').cloneNode())"
                      : "document.querySelector('form').action='https://other.test/submit'",
                  ),
                );

              return login;
            }),
        });

        expect(
          yield* fillCredential(loginRequest).pipe(
            Effect.provideService(BrowserCredentialAccess, authority),
            Effect.flip,
          ),
        ).toMatchObject({
          reason: mode === "revoked" ? "denied" : "stale-target",
          dispatch: "not-dispatched",
          filled: 0,
        });
        expect((yield* read(page)).writes).toEqual([]);
      }).pipe(Effect.provide(Layer.effect(BrowserSessionPage, fixture(test)))),
    { timeout: 30_000 },
  );
}

it.live(
  "retains acknowledged partial fill when authority becomes busy before the next field",
  (test) =>
    Effect.gen(function* () {
      const { page } = yield* BrowserSessionPage;

      const authority = access({
        authorize: () =>
          Effect.gen(function* () {
            const writes = (yield* read(page)).writes;

            if (writes.length > 0) return yield* new CredentialAccessError({ reason: "busy" });
          }),
      });

      expect(
        yield* fillCredential(loginRequest).pipe(
          Effect.provideService(BrowserCredentialAccess, authority),
          Effect.flip,
        ),
      ).toMatchObject({ reason: "busy", dispatch: "dispatched", filled: 1 });
      expect(yield* read(page)).toEqual({
        values: ["dummy@example.test", ""],
        writes: ["username"],
        text: "Login",
      });
      expect(page.isClosed()).toBe(false);
    }).pipe(Effect.provide(Layer.effect(BrowserSessionPage, fixture(test)))),
  { timeout: 30_000 },
);

it.live(
  "reports a lost SDK reply without replaying the credential write",
  (test) =>
    Effect.gen(function* () {
      const { page } = yield* BrowserSessionPage;

      const realm = page.mainFrame().isolatedRealm();
      const evaluate = realm.evaluate.bind(realm);

      const spy = vi.spyOn(realm, "evaluate").mockImplementation(async (...args) => {
        const result = await evaluate(...args);

        if (result === "filled") throw new Error(secret);

        return result;
      });

      yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));

      const unknown = yield* fillCredential(loginRequest).pipe(
        Effect.provideService(BrowserCredentialAccess, access()),
        Effect.flip,
      );

      expect(unknown).toMatchObject({
        reason: "provider",
        dispatch: "possibly-dispatched",
        filled: 0,
      });
      expect(JSON.stringify(unknown)).not.toContain(secret);
      expect((yield* read(page)).writes).toEqual(["username"]);
    }).pipe(Effect.provide(Layer.effect(BrowserSessionPage, fixture(test)))),
  { timeout: 30_000 },
);
