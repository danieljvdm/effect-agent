import type { Browser, Page } from "@cloudflare/puppeteer";
import nativePuppeteer from "@cloudflare/puppeteer/internal/puppeteer-core.js";
import {
  BrowserCredentialAccess,
  CredentialAccessError,
  CredentialOfferMetadata,
  InteractiveBrowserPolicy,
  ListCredentialOffers,
  LoginCredential,
  CardCredential,
  ProtectedBrowser,
  ProtectedBrowserNavigate,
  ProtectedBrowserClick,
  UseCredential,
} from "@effect-agent/sandbox";
import { BrowserCrypto } from "@effect/platform-browser";
import { expect, it } from "@effect/vitest";
import { Clock, Config, Effect, Layer, Option, Redacted, Schema } from "effect";
import { TestClock } from "effect/testing";

import {
  makeProtectedNativeTransport,
  ProtectedNativeSession,
} from "../src/protected-browser/native.ts";
import {
  BrowserRunProtectedTransport,
  browserRunProtectedLayer,
} from "../src/protected-browser/policy.ts";

class ProbeError extends Schema.TaggedError<ProbeError>()("ProtectedNativeProbeError", {}) {}

const native = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: () => new ProbeError() });

const secret = "dummy-password-sentinel";
const hosts = ["alpha.test", "beta.test", "processor.test"];

const policy = InteractiveBrowserPolicy.make({
  network: { _tag: "ExactHosts", allowedHosts: hosts },
  maxActions: 100,
  maxElapsedMillis: 60_000,
  maxReturnedBytes: 16384,
});

it.live(
  "discovers different native forms, rejects replaced nodes, preserves login, and fills a merchant-bound payment frame",
  (test) =>
    Effect.gen(function* () {
      const executable = yield* Config.option(Config.string("BROWSER_TEST_EXECUTABLE"));

      if (Option.isNone(executable)) return test.skip();

      const browser = yield* Effect.acquireRelease(
        native(() => nativePuppeteer.launch({ executablePath: executable.value, headless: true })),
        (browser) => Effect.promise(() => browser.close()),
      );

      const page = yield* native(() => browser.newPage());
      const submitted: Array<string> = [];

      yield* native(() => page.setRequestInterception(true));
      page.on("request", (request) => {
        const url = new URL(request.url());
        let body = "";
        let status = 200;
        const headers: Record<string, string> = { "content-type": "text/html" };

        if (url.pathname === "/accept") {
          submitted.push(request.postData() ?? "");
          headers["set-cookie"] = "dummy-session=authorized; Secure; HttpOnly; Path=/";
          headers.location = "/dashboard";
          status = 303;
        } else if (url.pathname === "/dashboard") {
          body = request.headers().cookie?.includes("dummy-session=authorized")
            ? '<main>Private dashboard</main><a href="/next">Continue</a>'
            : "Not authenticated";
        } else if (url.pathname === "/next") body = "Useful next page";
        else if (url.pathname === "/standalone")
          body = '<section><input autocomplete="username"><input type="password"></section>'.repeat(
            2,
          );
        else if (url.pathname === "/pay")
          body = '<iframe src="https://processor.test/fields"></iframe>';
        else if (url.pathname === "/fields")
          body =
            '<form><input autocomplete="cc-name"><input autocomplete="cc-number"><input autocomplete="cc-exp"><input autocomplete="cc-csc"><button>Pay</button></form>';
        else
          body =
            url.hostname === "alpha.test"
              ? '<form method="post" action="/accept"><label>Account<input name="u" autocomplete="username"></label><label>Password<input name="p" type="password"></label><button>Sign in</button></form>'
              : '<section><input form="session" type="email" name="u" autocomplete="username"><form id="session" method="post" action="/accept"><input type="password" name="p"><input type="submit" value="Login"></form></section>';
        void request.respond({ status, headers, body }).catch(() => {});
      });
      let resolutions = 0;
      let grants = true;
      let observationTrusted = true;

      const access = BrowserCredentialAccess.of({
        caller: Effect.succeed(Redacted.make("authorized-test-invocation")),
        list: () =>
          Effect.succeed([
            {
              key: Redacted.make("dummy-vault-id"),
              metadata: CredentialOfferMetadata.make({ label: "Dummy only" }),
            },
          ]),
        authorize: (request) =>
          grants &&
          ["https://alpha.test", "https://beta.test"].includes(request.target.topOrigin) &&
          request.target.frameOrigin ===
            (request.kind === "card" ? "https://processor.test" : request.target.topOrigin)
            ? Effect.void
            : Effect.fail(new CredentialAccessError({ reason: "denied" })),
        resolve: (request) =>
          Effect.sync(() => {
            resolutions++;

            return request.kind === "login"
              ? LoginCredential.make({
                  username: Redacted.make("dummy@example.test"),
                  password: Redacted.make(secret),
                })
              : CardCredential.make({
                  name: Redacted.make("Dummy Card"),
                  number: Redacted.make("4111111111111111"),
                  expiry: Redacted.make("12/30"),
                  expiryMonth: Redacted.make("12"),
                  expiryYear: Redacted.make("2030"),
                  securityCode: Redacted.make("123"),
                });
          }),
        observation: () =>
          Effect.succeed(observationTrusted ? "trust-recipient-no-credential-echo" : "deny"),
      });

      // The same pinned SDK ships separate bundled and internal declarations with private brands.
      // This test changes only that declaration identity, not a value or Schema boundary.
      const terminate = yield* Effect.cached(
        native(() => browser.close()).pipe(
          Effect.as("confirmed" as const),
          Effect.catch(() => Effect.succeed("unconfirmed" as const)),
        ),
      );

      const nativeClock = yield* TestClock.make();

      const driver = yield* makeProtectedNativeTransport(policy).pipe(
        Effect.provideService(Clock.Clock, nativeClock),
        Effect.provideService(ProtectedNativeSession, {
          browser: browser as unknown as Browser,
          page: page as unknown as Page,
          close: terminate,
        }),
      );

      const layer = browserRunProtectedLayer().pipe(
        Layer.provide(
          Layer.succeed(BrowserRunProtectedTransport)({ open: () => Effect.succeed(driver) }),
        ),
        Layer.provideMerge(Layer.succeed(BrowserCredentialAccess)(access)),
      );

      let phase = "open";

      yield* Effect.gen(function* () {
        const handle = yield* (yield* ProtectedBrowser).open(policy);

        phase = "native-reference-expiry";
        yield* handle.navigate(ProtectedBrowserNavigate.make({ url: "https://alpha.test/login" }));
        const initial = yield* handle.observe;
        const expiring = initial.controls.find((control) => control.role === "password")!;

        yield* nativeClock.adjust("60 seconds");
        expect(
          yield* handle
            .listCredentialOffers(
              ListCredentialOffers.make({ kind: "login", target: expiring.ref }),
            )
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "stale-reference", dispatch: "not-dispatched" });
        phase = "standalone-fields";
        yield* handle.navigate(
          ProtectedBrowserNavigate.make({ url: "https://alpha.test/standalone" }),
        );
        const standalone = yield* handle.observe;

        expect(standalone.controls).toHaveLength(4);
        for (const control of standalone.controls) {
          expect(control.role).toBe("unsupported");
          expect(
            yield* handle
              .listCredentialOffers(
                ListCredentialOffers.make({ kind: "login", target: control.ref }),
              )
              .pipe(Effect.flip),
          ).toMatchObject({ reason: "unsupported", dispatch: "not-dispatched" });
        }
        expect(
          yield* native(() =>
            page.evaluate("[...document.querySelectorAll('input')].map(el => el.value)"),
          ),
        ).toEqual(["", "", "", ""]);
        for (const attribute of ["name", "autocomplete", "action"]) {
          phase = `oversized-${attribute}`;
          yield* handle.navigate(
            ProtectedBrowserNavigate.make({ url: "https://alpha.test/login" }),
          );
          const observed = yield* handle.observe;
          const field = observed.controls.find((control) => control.role === "password")!;

          const offers = yield* handle.listCredentialOffers(
            ListCredentialOffers.make({ kind: "login", target: field.ref }),
          );

          // Build the hostile attribute in the page, not in the test's CDP request.
          yield* native(() =>
            page.evaluate(
              `document.querySelector('${attribute === "action" ? "form" : "input[type=password]"}').setAttribute('${attribute}', 'x'.repeat(1024 * 1024))`,
            ),
          );
          expect(
            yield* handle
              .useCredential(
                UseCredential.make({
                  offer: offers[0]!.ref,
                  fields: [{ ref: field.ref, role: "password" }],
                }),
              )
              .pipe(Effect.flip),
          ).toMatchObject({ reason: "stale-reference", dispatch: "not-dispatched" });
          // Successful discovery proves the browser omitted the oversized record before host decoding.
          expect(
            (yield* handle.observe).controls.some((control) => control.role === "password"),
          ).toBe(false);
        }
        expect(resolutions).toBe(0);
        for (const host of ["alpha.test", "beta.test"]) {
          phase = `${host}:login`;
          yield* handle.navigate(ProtectedBrowserNavigate.make({ url: `https://${host}/login` }));
          const observation = yield* handle.observe;
          const username = observation.controls.find((control) => control.role === "username")!;
          const password = observation.controls.find((control) => control.role === "password")!;
          const submit = observation.controls.find((control) => control.role === "submit")!;

          expect(username).toBeDefined();
          expect(password).toBeDefined();
          expect(submit).toBeDefined();

          const offers = yield* handle.listCredentialOffers(
            ListCredentialOffers.make({ kind: "login", target: username.ref }),
          );

          const request = UseCredential.make({
            offer: offers[0]!.ref,
            fields: [
              { ref: username.ref, role: "username" },
              { ref: password.ref, role: "password" },
            ],
            submit: submit.ref,
          });

          const result = yield* handle.useCredential(request);

          expect(result.milestone).toBe("submission-dispatched");
          // Read-only polling models what a consumer can do on any asynchronously navigating page.
          let text = "";

          for (let attempt = 0; attempt < 30 && !text.includes("Private dashboard"); attempt++) {
            yield* Effect.sleep("20 millis");

            const observed = yield* handle.observe.pipe(
              Effect.map(Option.some),
              Effect.catch((error) =>
                error.reason === "stale-reference"
                  ? Effect.succeed(Option.none())
                  : Effect.fail(error),
              ),
            );

            if (Option.isSome(observed)) text = observed.value.text;
          }
          expect(page.url()).toBe(`https://${host}/dashboard`);
          const liveContext = yield* driver.context;

          expect(liveContext).toMatchObject({ topOrigin: `https://${host}` });
          expect(text).toContain("Private dashboard");
          expect(text).not.toContain(secret);
          const dashboard = yield* handle.observe;
          const next = dashboard.controls.find((control) => control.role === "link")!;

          yield* handle.click(ProtectedBrowserClick.make({ ref: next.ref }));
        }
        expect(submitted).toHaveLength(2);
        phase = "replaced-node";
        expect(submitted.every((body) => body.includes(secret))).toBe(true);
        yield* handle.navigate(ProtectedBrowserNavigate.make({ url: "https://alpha.test/login" }));
        const observed = yield* handle.observe;
        const field = observed.controls.find((control) => control.role === "password")!;

        const offers = yield* handle.listCredentialOffers(
          ListCredentialOffers.make({ kind: "login", target: field.ref }),
        );

        const before = resolutions;

        yield* native(() =>
          page.evaluate(
            "document.querySelector('input[type=password]').replaceWith(document.querySelector('input[type=password]').cloneNode())",
          ),
        );
        expect(
          yield* handle
            .useCredential(
              UseCredential.make({
                offer: offers[0]!.ref,
                fields: [{ ref: field.ref, role: "password" }],
              }),
            )
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "stale-reference", dispatch: "not-dispatched" });
        expect(resolutions).toBe(before);
        phase = "payment-navigation";
        yield* handle.navigate(ProtectedBrowserNavigate.make({ url: "https://alpha.test/pay" }));
        phase = "payment-discovery";
        const checkout = yield* handle.observe;
        const cardFields = checkout.controls.filter((control) => control.role.startsWith("card-"));

        expect(cardFields).toHaveLength(4);

        const cards = yield* handle.listCredentialOffers(
          ListCredentialOffers.make({ kind: "card", target: cardFields[0]!.ref }),
        );

        const roles = Schema.Literals([
          "card-name",
          "card-number",
          "card-expiry",
          "card-security-code",
        ]);

        const fill = UseCredential.make({
          offer: cards[0]!.ref,
          fields: cardFields.map((field) => ({
            ref: field.ref,
            role: Schema.decodeUnknownSync(roles)(field.role),
          })),
        });

        grants = false;
        expect(yield* handle.useCredential(fill).pipe(Effect.flip)).toMatchObject({
          reason: "denied",
          dispatch: "not-dispatched",
        });
        grants = true;
        expect(yield* handle.useCredential(fill)).toMatchObject({
          milestone: "filled",
          authentication: "unverified",
        });
        expect(JSON.stringify(yield* handle.observe)).not.toContain("4111111111111111");
        // A recipient can echo transformed material long after the fill. Only a current host
        // trust decision authorizes observing that context; substring filters cannot do this.
        observationTrusted = false;

        const processor = page
          .frames()
          .find((frame) => frame.url() === "https://processor.test/fields")!;

        yield* native(() =>
          processor.evaluate(
            "document.body.append(document.createTextNode(btoa(document.querySelector('[autocomplete=cc-number]').value)))",
          ),
        );
        expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({
          reason: "observation-blocked",
          observation: "protected",
        });
        expect(yield* handle.close).toBe("confirmed");
        expect(browser.isConnected()).toBe(false);
      }).pipe(
        Effect.onError(() => Effect.logInfo(`Native proof phase: ${phase}`)),
        Effect.scoped,
        Effect.provide(layer),
      );
    }).pipe(Effect.scoped, Effect.provide(BrowserCrypto.layer)),
  { timeout: 30_000 },
);
