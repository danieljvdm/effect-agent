import { expect, it } from "@effect/vitest";
import { Config, Effect, Layer, Option, Redacted, Schema } from "effect";
import nativePuppeteer from "puppeteer-core";
import browserPuppeteer, {
  type Page,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";
import { expectTypeOf, type TestContext, vi } from "vite-plus/test";

import type { CredentialFillError } from "../src/BrowserCredentials.ts";
import {
  BrowserCredentialAccess,
  CardCredential,
  CredentialAccessError,
  CredentialFillResult,
  FillCredentialRequest,
  LoginCredential,
} from "../src/BrowserCredentials.ts";
import { BrowserSessionPage, fillCredential } from "../src/internal/browser-credentials.ts";

const secret = "dummy-credential-secret-sentinel";

const login = LoginCredential.make({
  username: Redacted.make("dummy@example.test"),
  password: Redacted.make(secret),
});

const card = CardCredential.make({
  name: Redacted.make("Dummy Shopper"),
  number: Redacted.make("4111111111111111"),
  expiry: Redacted.make("09/2030"),
  expiryMonth: Redacted.make("09"),
  expiryYear: Redacted.make("2030"),
});

const loginRequest = FillCredentialRequest.make({
  credential: "login",
  kind: "login",
  fields: [
    { selector: "#username", role: "username" },
    { selector: "#password", role: "password" },
  ],
});

const cardRequest = FillCredentialRequest.make({
  credential: "card",
  kind: "card",
  frame: ["#processor"],
  fields: [
    { selector: "#number", role: "card-number" },
    { selector: "#month", role: "card-expiry-month" },
  ],
});

const access = (overrides: Partial<BrowserCredentialAccess["Service"]> = {}) =>
  BrowserCredentialAccess.of({
    authorize: (request) => {
      const frameOrigin =
        request.kind === "login" ? "https://merchant.test" : "https://processor.test";

      const recipientOrigin =
        request.kind === "login" ? "https://auth.test" : "https://processor.test";

      return request.credential === request.kind &&
        request.target.topOrigin === "https://merchant.test" &&
        request.target.frameOrigin === frameOrigin &&
        request.target.recipientOrigin === recipientOrigin
        ? Effect.void
        : Effect.fail(new CredentialAccessError({ reason: "denied" }));
    },
    resolve: (request) => Effect.succeed(request.kind === "login" ? login : card),
    ...overrides,
  });

const html = `<form action="https://auth.test/session" onsubmit="event.preventDefault();document.querySelector('main').textContent='Signed in'">
  <input id="username"><input id="password" type="password"><button>Sign in</button></form>
  <main>Login</main><iframe id="processor" src="https://processor.test/fields"></iframe>
  <script>window.writes=[];document.addEventListener('input',event=>window.writes.push(event.target.id));</script>`;

const processorHtml = `<form action="https://processor.test/pay"><input id="number"><select id="month">
  <option value="01">January</option><option value="09">September</option></select></form>
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
        body: new URL(request.url()).pathname === "/fields" ? processorHtml : html,
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

it("keeps invocation authority and typed failures visible", () => {
  expectTypeOf<ReturnType<typeof fillCredential>>().toEqualTypeOf<
    Effect.Effect<
      CredentialFillResult,
      CredentialFillError,
      BrowserCredentialAccess | BrowserSessionPage
    >
  >();
});

it.live(
  "fills login and a payment frame on the ordinary native page without discovery or submission",
  (test) =>
    Effect.gen(function* () {
      const { page } = yield* BrowserSessionPage;

      const result = yield* fillCredential(loginRequest).pipe(
        Effect.provideService(BrowserCredentialAccess, access()),
      );

      expect(result).toEqual(CredentialFillResult.make({ dispatch: "dispatched", filled: 2 }));
      expect(yield* read(page)).toEqual({
        values: ["dummy@example.test", secret],
        writes: ["username", "password"],
        text: "Login",
      });
      expect(JSON.stringify({ request: loginRequest, result })).not.toContain(secret);
      // Ordinary native reads are intentionally permitted, including a page's credential echoes.
      yield* Effect.promise(() =>
        page.evaluate(
          "document.querySelector('main').textContent=document.querySelector('#password').value",
        ),
      );
      expect((yield* read(page)).text).toBe(secret);
      yield* Effect.promise(() => page.click("button"));
      expect((yield* read(page)).text).toBe("Signed in");
      expect(
        yield* fillCredential(cardRequest).pipe(
          Effect.provideService(BrowserCredentialAccess, access()),
        ),
      ).toMatchObject({ filled: 2 });
      const frame = page.frames().find((frame) => frame.url() === "https://processor.test/fields")!;

      expect(
        yield* Effect.promise(() =>
          frame.evaluate(
            "({number:document.querySelector('#number').value,month:document.querySelector('#month').value,writes:window.writes})",
          ),
        ),
      ).toEqual({ number: "4111111111111111", month: "09", writes: ["number", "month"] });
    }).pipe(Effect.provide(Layer.effect(BrowserSessionPage, fixture(test)))),
  { timeout: 30_000 },
);

for (const mode of ["merchant", "port", "frame", "recipient", "credential-url"] as const) {
  it.live(
    `denies the actual ${mode} before resolving credential material`,
    (test) =>
      Effect.gen(function* () {
        const { page } = yield* BrowserSessionPage;

        if (mode === "merchant" || mode === "port")
          yield* Effect.promise(() =>
            page.goto(
              mode === "merchant" ? "https://other.test/login" : "https://merchant.test:8443/login",
            ),
          );
        else if (mode === "frame")
          yield* Effect.promise(() =>
            page
              .frames()
              .find((frame) => frame.url() === "https://processor.test/fields")!
              .goto("https://other.test/fields"),
          );
        else
          yield* Effect.promise(() =>
            page.evaluate(
              `document.querySelector('form').action='${mode === "recipient" ? "https://other.test/submit" : "https://user:password@auth.test/submit"}'`,
            ),
          );
        let resolved = 0;

        const authority = access({
          resolve: (request) =>
            Effect.sync(() => {
              resolved++;

              return request.kind === "card" ? card : login;
            }),
        });

        const request = mode === "frame" ? cardRequest : loginRequest;

        const error = yield* fillCredential(request).pipe(
          Effect.provideService(BrowserCredentialAccess, authority),
          Effect.flip,
        );

        expect(error).toMatchObject({ reason: "denied", dispatch: "not-dispatched", filled: 0 });
        expect(resolved).toBe(0);
        expect((yield* read(page)).writes).toEqual([]);
      }).pipe(Effect.provide(Layer.effect(BrowserSessionPage, fixture(test)))),
    { timeout: 30_000 },
  );
}

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
  "rejects ambiguous nodes, duplicate mappings and material-kind mismatches before resolving",
  (test) =>
    Effect.gen(function* () {
      const { page } = yield* BrowserSessionPage;
      let resolved = 0;

      const authority = access({
        resolve: () =>
          Effect.sync(() => {
            resolved++;

            return login;
          }),
      });

      const requests = [
        { ...loginRequest, fields: [{ selector: "input", role: "username" as const }] },
        {
          ...loginRequest,
          fields: [
            { selector: "#username", role: "username" as const },
            { selector: "#username", role: "password" as const },
          ],
        },
        {
          ...loginRequest,
          fields: [
            { selector: "#username", role: "username" as const },
            { selector: "#password", role: "username" as const },
          ],
        },
        { ...loginRequest, fields: [{ selector: "#password", role: "card-number" as const }] },
      ];

      for (const request of requests) {
        expect(
          yield* fillCredential(FillCredentialRequest.make(request)).pipe(
            Effect.provideService(BrowserCredentialAccess, authority),
            Effect.flip,
          ),
        ).toMatchObject({ dispatch: "not-dispatched", filled: 0 });
      }
      expect(resolved).toBe(0);
      expect((yield* read(page)).writes).toEqual([]);
    }).pipe(Effect.provide(Layer.effect(BrowserSessionPage, fixture(test)))),
  { timeout: 30_000 },
);

it.live(
  "distinguishes a refused native select from a write rejected after assignment",
  (test) =>
    Effect.gen(function* () {
      const { page } = yield* BrowserSessionPage;
      const frame = page.frames().find((frame) => frame.url() === "https://processor.test/fields")!;

      yield* Effect.promise(() =>
        frame.evaluate("document.querySelector('#month option[value=\"09\"]').disabled=true"),
      );
      const authority = access();

      const refused = yield* fillCredential(cardRequest).pipe(
        Effect.provideService(BrowserCredentialAccess, authority),
        Effect.flip,
      );

      expect(refused).toMatchObject({ reason: "unsupported", dispatch: "dispatched", filled: 1 });
      expect(yield* Effect.promise(() => frame.evaluate("window.writes"))).toEqual(["number"]);

      yield* Effect.promise(() =>
        frame.evaluate("document.querySelector('#number').type='number'"),
      );

      const rejected = yield* fillCredential(
        FillCredentialRequest.make({
          ...cardRequest,
          fields: [{ selector: "#number", role: "card-name" }],
        }),
      ).pipe(Effect.provideService(BrowserCredentialAccess, authority), Effect.flip);

      expect(rejected).toMatchObject({
        reason: "unsupported",
        dispatch: "possibly-dispatched",
        filled: 0,
      });
      expect(
        yield* Effect.promise(() => frame.evaluate("document.querySelector('#number').value")),
      ).toBe("");
    }).pipe(Effect.provide(Layer.effect(BrowserSessionPage, fixture(test)))),
  { timeout: 30_000 },
);

it.live(
  "sanitizes a vault defect and a lost SDK reply without replaying the write",
  (test) =>
    Effect.gen(function* () {
      const { page } = yield* BrowserSessionPage;

      const defect = yield* fillCredential(loginRequest).pipe(
        Effect.provideService(
          BrowserCredentialAccess,
          access({ resolve: () => Effect.die(new Error(secret)) }),
        ),
        Effect.flip,
      );

      expect(defect).toMatchObject({ reason: "provider", dispatch: "not-dispatched", filled: 0 });
      expect(JSON.stringify(defect)).not.toContain(secret);
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

it.live(
  "checks the trusted isolated predicate at each credential write and releases its handle",
  (test) =>
    Effect.gen(function* () {
      const context = yield* fixture(test);

      if (!context) return;
      let disposed = 0;

      const result = yield* fillCredential(loginRequest, async (frame) => {
        const handle = await frame.isolatedRealm().evaluateHandle(() => {
          const original = Reflect.get(globalThis, "document").getElementById("username");

          return (field: unknown, index: number) => index === 0 && field === original;
        });

        const release = handle.dispose.bind(handle);

        handle.dispose = async () => {
          disposed++;
          await release();
        };

        return handle;
      }).pipe(
        Effect.provideService(BrowserSessionPage, context),
        Effect.provideService(BrowserCredentialAccess, access()),
        Effect.flip,
      );

      expect(result.reason).toBe("stale-target");
      expect(result.dispatch).toBe("dispatched");
      expect(result.filled).toBe(1);
      expect(disposed).toBe(1);
      expect((yield* read(context.page)).values).toEqual(["dummy@example.test", ""]);
    }).pipe(Effect.scoped),
);

it.live("releases a credential guard when authority resolution is interrupted", (test) =>
  Effect.gen(function* () {
    const context = yield* fixture(test);

    if (!context) return;
    let disposed = 0;

    const result = yield* fillCredential(loginRequest, async (frame) => {
      const handle = await frame
        .isolatedRealm()
        .evaluateHandle(() => (_field: unknown, _index: number) => true);

      const release = handle.dispose.bind(handle);

      handle.dispose = async () => {
        disposed++;
        await release();
      };

      return handle;
    }).pipe(
      Effect.provideService(BrowserSessionPage, context),
      Effect.provideService(BrowserCredentialAccess, access({ resolve: () => Effect.interrupt })),
      Effect.exit,
    );

    expect(result._tag).toBe("Failure");
    expect(disposed).toBe(1);
    expect((yield* read(context.page)).writes).toEqual([]);
  }).pipe(Effect.scoped),
);
