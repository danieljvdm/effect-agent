import type { Browser, Page } from "@cloudflare/puppeteer";
import nativePuppeteer from "@cloudflare/puppeteer/internal/puppeteer-core.js";
import { InteractiveBrowserPolicy } from "@effect-agent/sandbox/InteractiveBrowser";
import {
  BrowserCredentialAccess,
  CredentialAccessError,
  CredentialOfferMetadata,
  CredentialObservationGrant,
  ListCredentialOffers,
  LoginCredential,
  CardCredential,
  ProtectedBrowser,
  ProtectedBrowserNavigate,
  ProtectedBrowserClick,
  UseCredential,
} from "@effect-agent/sandbox/ProtectedBrowser";
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
  ProtectedBrowserDispatch,
} from "../src/protected-browser/policy.ts";

class ProbeError extends Schema.TaggedError<ProbeError>()("ProtectedNativeProbeError", {}) {}

const native = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: () => new ProbeError() });

const secret = "dummy-password-sentinel";
const hosts = ["alpha.test", "beta.test", "processor.test", "incidental.test"];

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
        else if (url.pathname === "/links")
          body =
            '<a id="allowed" href="/allowed">Allowed</a><a href="https://beta.test/private">Denied</a><a href="javascript:void(0)">Script</a><a href="https://u:p@alpha.test/private">Credential URL</a>';
        else if (url.pathname === "/standalone")
          body = '<section><input autocomplete="username"><input type="password"></section>'.repeat(
            2,
          );
        else if (url.pathname === "/pay")
          body =
            '<form onsubmit="event.preventDefault();document.body.append(\'Purchase submitted\')"><button>Pay now</button></form><iframe src="https://processor.test/fields"></iframe><iframe src="https://incidental.test/noise"></iframe>';
        else if (url.pathname === "/noise")
          body = '<main>Untrusted frame text</main><input aria-label="Untrusted frame control">';
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
      let selectedOrigins: ReadonlyArray<string> | undefined;
      let actionOverride: BrowserCredentialAccess["Service"]["authorizeAction"] | undefined;

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
        authorizeAction: (request) =>
          actionOverride?.(request) ??
          (request.action._tag !== "Submit" ||
          (grants &&
            request.action.target.frameOrigin === "https://alpha.test" &&
            request.action.target.recipientOrigin === "https://alpha.test" &&
            request.exposures.some((target) => target.frameOrigin === "https://processor.test"))
            ? Effect.void
            : Effect.fail(new CredentialAccessError({ reason: "denied" }))),
        observation: () =>
          Effect.succeed(
            !observationTrusted
              ? "deny"
              : selectedOrigins === undefined
                ? "trust-recipient-no-credential-echo"
                : CredentialObservationGrant.make({
                    decision: "trust-recipient-no-credential-echo",
                    origins: selectedOrigins,
                  }),
          ),
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
        phase = "link-destination-authorization";
        yield* handle.navigate(ProtectedBrowserNavigate.make({ url: "https://alpha.test/links" }));
        const links = (yield* handle.observe).controls;

        expect(links.map((control) => control.label)).toEqual(["Allowed", "Denied"]);
        expect(links[1]!.target.recipientOrigin).toBe("https://beta.test");
        actionOverride = (request) =>
          Effect.gen(function* () {
            if (
              request.action._tag !== "Click" ||
              request.action.role !== "link" ||
              request.action.url !== "https://alpha.test/allowed"
            )
              return yield* new CredentialAccessError({ reason: "denied" });
            yield* native(() =>
              page.evaluate(
                "document.querySelector('#allowed').href='/changed-during-authorization'",
              ),
            ).pipe(Effect.mapError(() => new CredentialAccessError({ reason: "resolver" })));
          });
        expect(
          yield* handle.click(ProtectedBrowserClick.make({ ref: links[1]!.ref })).pipe(Effect.flip),
        ).toMatchObject({ reason: "denied", dispatch: "not-dispatched" });
        expect(
          yield* handle.click(ProtectedBrowserClick.make({ ref: links[0]!.ref })).pipe(Effect.flip),
        ).toMatchObject({ reason: "stale-reference", dispatch: "not-dispatched" });
        expect(page.url()).toBe("https://alpha.test/links");
        actionOverride = undefined;
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

        const incidentalRef = checkout.controls.find(
          (control) => control.label === "Untrusted frame control",
        )!.ref;

        yield* driver.restrictObservation(["https://alpha.test", "https://processor.test"]);
        expect(yield* driver.target(incidentalRef).pipe(Effect.flip)).toMatchObject({
          reason: "stale-reference",
        });
        yield* driver.restrictObservation(undefined);
        expect(yield* driver.target(incidentalRef).pipe(Effect.flip)).toMatchObject({
          reason: "stale-reference",
        });
        selectedOrigins = ["https://alpha.test", "https://processor.test"];
        const selected = yield* handle.observe;

        expect(JSON.stringify(selected)).not.toContain("4111111111111111");
        expect(JSON.stringify(selected)).not.toContain("Untrusted frame");
        phase = "merchant-submission";
        const merchantSubmit = selected.controls.find((control) => control.label === "Pay now")!;

        grants = false;
        expect(
          yield* handle
            .click(ProtectedBrowserClick.make({ ref: merchantSubmit.ref }))
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "denied", dispatch: "not-dispatched" });
        grants = true;
        yield* handle.click(ProtectedBrowserClick.make({ ref: merchantSubmit.ref }));
        expect((yield* handle.observe).text).toContain("Purchase submitted");
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

it.live(
  "fills native ordinary controls by held refs and rejects stale or credential targets",
  (test) =>
    Effect.gen(function* () {
      const executable = yield* Config.option(Config.string("BROWSER_TEST_EXECUTABLE"));

      if (Option.isNone(executable)) return test.skip();

      const browser = yield* Effect.acquireRelease(
        native(() => nativePuppeteer.launch({ executablePath: executable.value, headless: true })),
        (browser) => Effect.promise(() => browser.close()),
      );

      const page = yield* native(() => browser.newPage());

      yield* native(() => page.setRequestInterception(true));
      page.on("request", (request) => {
        void request
          .respond({
            contentType: "text/html",
            body: `
        <main>Checkout</main>
        <input type="hidden" value="hidden-value">
        <div hidden>hidden-text<input aria-label="Hidden"></div>
        <div style="opacity:0">transparent-text<input aria-label="Transparent"></div>
        <label>Address<input name="address"></label>
        <label>Notes<textarea>private-default-text</textarea></label>
        <label>Region<select><option value="">Choose</option><option value="CA">California</option><option value="NY">New York</option><option disabled value="XX">Disabled</option><optgroup disabled><option value="YY">Disabled group</option></optgroup><option value="a">Duplicate</option><option value="b">Duplicate</option><option value="same">First</option><option value="same">Second</option></select></label>
        <label>Delivery<input type="radio" name="shipping" value="delivery" checked></label>
        <label>Pickup<input type="radio" name="shipping" value="pickup" style="display:none"></label>
        <label>Agree<input type="checkbox"></label>
        <fieldset disabled><label>Disabled<input></label></fieldset>
        <input aria-label="OTP" autocomplete="one-time-code">
        <input aria-label="Password hint" autocomplete="new-password">
        <form><input aria-label="Password" type="password"><input aria-label="Card" autocomplete="cc-number"></form>
        <iframe src="about:blank"></iframe>
        <iframe src="data:text/html,opaque-child"></iframe>
        <script>window.events=[];document.addEventListener('input', e=>events.push(e.type));document.addEventListener('change', e=>events.push(e.type));</script>
      `,
          })
          .catch(() => {});
      });
      yield* native(() => page.goto("https://alpha.test/checkout"));

      const driver = yield* makeProtectedNativeTransport(policy).pipe(
        Effect.provideService(ProtectedNativeSession, {
          browser: browser as unknown as Browser,
          page: page as unknown as Page,
          close: native(() => browser.close()).pipe(
            Effect.as("confirmed" as const),
            Effect.catch(() => Effect.succeed("unconfirmed" as const)),
          ),
        }),
      );

      const initial = yield* driver.discover;

      expect(initial.text).toContain("Checkout");
      for (const hidden of [
        "hidden-value",
        "hidden-text",
        "transparent-text",
        "private-default-text",
        "opaque-child",
      ])
        expect(JSON.stringify(initial)).not.toContain(hidden);
      expect(
        initial.controls.some(
          (control) => control.label === "Hidden" || control.label === "Transparent",
        ),
      ).toBe(false);
      const address = initial.controls.find((control) => control.label === "Address")!;
      const notes = initial.controls.find((control) => control.label === "Notes")!;
      const region = initial.controls.find((control) => control.label === "Region")!;

      expect(address.role).toBe("text");
      expect(notes.role).toBe("text");
      expect(region.role).toBe("select");
      for (const label of ["Disabled", "OTP", "Password hint"])
        expect(initial.controls.find((control) => control.label === label)!.role).toBe(
          "unsupported",
        );
      yield* driver.fill(address.ref, "text", Redacted.make("123 Example Street"));
      yield* driver.fill(notes.ref, "text", Redacted.make("Leave at reception"));
      yield* driver.fill(region.ref, "select", Redacted.make("California"));
      expect(yield* native(() => page.evaluate("document.querySelector('select').value"))).toBe(
        "CA",
      );
      yield* driver.fill(region.ref, "select", Redacted.make("NY"));
      for (const value of ["XX", "YY", "Duplicate", "same", "Second", "Missing"])
        expect(
          yield* driver.fill(region.ref, "select", Redacted.make(value)).pipe(Effect.flip),
        ).toMatchObject({ reason: "unsupported" });
      expect(yield* native(() => page.evaluate("document.querySelector('select').value"))).toBe(
        "NY",
      );
      expect(yield* native(() => page.evaluate("window.events"))).toEqual([
        "input",
        "change",
        "input",
        "change",
        "input",
        "change",
        "input",
        "change",
      ]);
      yield* driver.fill(address.ref, "text", Redacted.make(""));
      expect(
        yield* native(() => page.evaluate("document.querySelector('[name=address]').value")),
      ).toBe("");
      const pickup = initial.controls.find((control) => control.label === "Pickup")!;
      const agree = initial.controls.find((control) => control.label === "Agree")!;

      expect(pickup).toMatchObject({ role: "radio", checked: false });
      yield* driver.click(pickup.ref);
      yield* driver.click(agree.ref);
      const current = yield* driver.discover;

      expect(current.controls.find((control) => control.label === "Delivery")!.checked).toBe(false);
      expect(current.controls.find((control) => control.label === "Pickup")!.checked).toBe(true);
      expect(current.controls.find((control) => control.label === "Agree")!.checked).toBe(true);
      expect(
        yield* driver.fill(address.ref, "text", Redacted.make("old")).pipe(Effect.flip),
      ).toMatchObject({ reason: "stale-reference" });
      const currentAddress = current.controls.find((control) => control.label === "Address")!;

      yield* native(() =>
        page.evaluate(
          "document.querySelector('[name=address]').setAttribute('autocomplete','cc-number')",
        ),
      );
      expect(
        yield* driver.fill(currentAddress.ref, "text", Redacted.make("ordinary")).pipe(Effect.flip),
      ).toMatchObject({ reason: "stale-reference" });
      const currentNotes = current.controls.find((control) => control.label === "Notes")!;

      yield* native(() =>
        page.evaluate(
          "document.querySelector('textarea').replaceWith(document.querySelector('textarea').cloneNode(true))",
        ),
      );
      expect(
        yield* driver.fill(currentNotes.ref, "text", Redacted.make("replaced")).pipe(Effect.flip),
      ).toMatchObject({ reason: "stale-reference" });
      for (const label of ["Password", "Card"])
        expect(
          yield* driver
            .fill(
              current.controls.find((control) => control.label === label)!.ref,
              "text",
              Redacted.make("ordinary"),
            )
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "stale-reference" });
    }).pipe(
      Effect.scoped,
      Effect.provide(BrowserCrypto.layer),
      Effect.provideService(ProtectedBrowserDispatch, { mark: Effect.void }),
    ),
  { timeout: 30_000 },
);
