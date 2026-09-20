import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import type { ServerResponse } from "node:http";
import { createServer } from "node:https";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { BrowserCrypto } from "@effect/platform-browser";
import { expect, it } from "@effect/vitest";
import type { Cause } from "effect";
import { Clock, Config, Effect, ErrorReporter, Layer, Option, Redacted, Schema } from "effect";
import { InteractiveBrowserPolicy } from "effect-agent/interactive-browser";
import {
  BrowserCredentialAccess,
  CredentialAccessError,
  CredentialOfferMetadata,
  CredentialObservationGrant,
  ListCredentialOffers,
  LoginCredential,
  CardCredential,
  ProtectedBrowser,
  ProtectedBrowserFill,
  ProtectedBrowserNavigate,
  ProtectedBrowserClick,
  ProtectedBrowserSession,
  UseCredential,
} from "effect-agent/protected-browser";
import { TestClock } from "effect/testing";
import nativePuppeteer from "puppeteer-core";
import type * as BrowserClient from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";
import type { Browser, Page } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";
import { vi } from "vite-plus/test";

import { BrowserRunHandoffRequest } from "../src/InteractiveBrowser.ts";
import { BrowserRunFailure } from "../src/internal/browser-failure.ts";
import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";
import { browserRunProtectedBindingLayer } from "../src/protected-browser/binding.ts";
import {
  BrowserRunProtectedCheckpoint,
  BrowserRunProtectedHost,
  browserRunProtectedHostLayer,
} from "../src/protected-browser/host.ts";
import {
  makeProtectedNativeTransport,
  ProtectedNativeSession,
} from "../src/protected-browser/native.ts";
import {
  BrowserRunProtectedTransport,
  browserRunProtectedLayer,
  ProtectedBrowserDispatch,
  ProtectedTransportError,
  type ProtectedBrowserTransport,
} from "../src/protected-browser/policy.ts";

class ProbeError extends Schema.TaggedError<ProbeError>()("ProtectedNativeProbeError", {}) {}

const sdk = vi.hoisted(() => ({ connected: vi.fn<(browser: Browser) => void>() }));

// Capture the actual connection for native fixture controls; do not substitute SDK initialization.
vi.mock("puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js", async (original) => {
  const actual = await original<typeof BrowserClient>();

  return {
    ...actual,
    default: {
      ...actual.default,
      connect: async (...args: Parameters<typeof actual.default.connect>) => {
        const browser = await actual.default.connect(...args);

        sdk.connected(browser);

        return browser;
      },
    },
  };
});

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

for (const mode of ["refused", "normalized", "reply-lost", "credential-refused"] as const) {
  it.live(
    `preserves native fill evidence and cleanup for ${mode}`,
    (test) =>
      Effect.gen(function* () {
        const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

        if (Option.isNone(executable)) return test.skip();

        const browser = yield* Effect.acquireRelease(
          native(() =>
            nativePuppeteer.launch({ executablePath: executable.value, headless: true }),
          ),
          (browser) => Effect.promise(() => browser.close()),
        );

        const page = yield* native(() => browser.newPage());

        yield* native(() => page.setRequestInterception(true));
        page.on("request", (request) => {
          void request
            .respond({
              contentType: "text/html",
              body: `<form onsubmit="event.preventDefault();window.submits++">
              <label>Bag size<select name="size"><option value="250">250 g</option><option value="500" label="500 g">private-option-text</option><option value="500 g">Other size</option></select></label>
              <label>Quantity<input name="quantity" type="number" value="1"></label>
              <button>Add to cart</button></form>
              <form><label>Cardholder<input name="cardholder" autocomplete="cc-name"></label>
              <label>Expiry month<select name="month" autocomplete="cc-exp-month"><option value="01" label="09">private-expiry-label</option></select></label></form>
              <script>window.writes=[];window.submits=0;document.addEventListener('input',e=>writes.push(e.target.name));</script>`,
            })
            .catch(() => {});
        });

        const state = native(() =>
          page.evaluate(`({
          size: document.querySelector('[name=size]').value,
          quantity: document.querySelector('[name=quantity]').value,
          month: document.querySelector('[name=month]').value,
          writes: window.writes, submits: window.submits
        })`),
        );

        let closedState: unknown;
        let beforeMark: unknown;
        let allowed = true;

        const access = BrowserCredentialAccess.of({
          caller: Effect.succeed(Redacted.make("native-select-test")),
          list: () =>
            Effect.succeed([
              {
                key: Redacted.make("dummy-card-key"),
                metadata: CredentialOfferMetadata.make({ label: "Dummy card" }),
              },
            ]),
          authorize: () => Effect.void,
          resolve: () =>
            Effect.succeed(
              CardCredential.make({
                name: Redacted.make("Dummy Shopper"),
                number: Redacted.make("4111111111111111"),
                expiry: Redacted.make("09/2030"),
                expiryMonth: Redacted.make("09"),
                expiryYear: Redacted.make("2030"),
              }),
            ),
          authorizeAction: () =>
            allowed ? Effect.void : Effect.fail(new CredentialAccessError({ reason: "denied" })),
          observation: () => Effect.succeed("trust-recipient-no-credential-echo"),
        });

        const driver = yield* makeProtectedNativeTransport(policy).pipe(
          Effect.provideService(ProtectedNativeSession, {
            browser: browser as unknown as Browser,
            page: page as unknown as Page,
            close: Effect.gen(function* () {
              closedState = yield* state.pipe(Effect.orDie);

              return yield* native(() => browser.close()).pipe(
                Effect.as("confirmed" as const),
                Effect.catch(() => Effect.succeed("unconfirmed" as const)),
              );
            }),
          }),
        );

        const transport: ProtectedBrowserTransport = {
          ...driver,
          fill: (ref, role, value) =>
            mode !== "reply-lost"
              ? driver.fill(ref, role, value)
              : Effect.gen(function* () {
                  const dispatch = yield* ProtectedBrowserDispatch;

                  // Lose the native completion at the transport boundary. Also record the DOM at
                  // dispatch marking, so moving that mark after native evaluation cannot pass.
                  yield* driver.fill(ref, role, value).pipe(
                    Effect.provideService(ProtectedBrowserDispatch, {
                      ...dispatch,
                      mark: Effect.gen(function* () {
                        beforeMark = yield* state.pipe(Effect.orDie);
                        yield* dispatch.mark;
                      }),
                    }),
                  );

                  return yield* new ProtectedTransportError({ reason: "provider" });
                }),
        };

        const layer = browserRunProtectedLayer().pipe(
          Layer.provide(
            Layer.succeed(BrowserRunProtectedTransport, {
              open: () => Effect.succeed(transport),
            }),
          ),
          Layer.provideMerge(Layer.succeed(BrowserCredentialAccess, access)),
        );

        yield* Effect.gen(function* () {
          const session = yield* ProtectedBrowserSession;
          const handle = yield* session.get;

          yield* handle.navigate(
            ProtectedBrowserNavigate.make({ url: "https://alpha.test/product" }),
          );
          const initial = yield* handle.observe;
          const bag = initial.controls.find((control) => control.label === "Bag size")!;

          if (mode === "credential-refused") {
            const cardholder = initial.controls.find((control) => control.role === "card-name")!;
            const month = initial.controls.find((control) => control.role === "card-expiry-month")!;

            const offers = yield* handle.listCredentialOffers(
              ListCredentialOffers.make({ kind: "card", target: cardholder.ref }),
            );

            expect(month.options).toBeUndefined();
            expect(JSON.stringify(initial)).not.toContain("private-expiry-label");
            expect(
              yield* handle
                .useCredential(
                  UseCredential.make({
                    offer: offers[0]!.ref,
                    fields: [
                      { ref: cardholder.ref, role: "card-name" },
                      { ref: month.ref, role: "card-expiry-month" },
                    ],
                  }),
                )
                .pipe(Effect.flip),
            ).toMatchObject({
              reason: "unsupported",
              dispatch: "dispatched",
              milestone: "partial-fill",
              observation: "closed",
              cleanup: "confirmed",
            });
            expect(closedState).toEqual({
              size: "250",
              quantity: "1",
              month: "01",
              writes: ["cardholder"],
              submits: 0,
            });
          } else {
            const quantity = initial.controls.find((control) => control.label === "Quantity")!;

            const failure = yield* handle
              .fill(
                ProtectedBrowserFill.make({
                  ref: mode === "normalized" ? quantity.ref : bag.ref,
                  value:
                    mode === "normalized" ? "not-a-number" : mode === "refused" ? "250g" : "250 g",
                }),
              )
              .pipe(Effect.flip);

            expect(failure).toMatchObject({
              reason: mode === "reply-lost" ? "outcome-unknown" : "unsupported",
              dispatch: mode === "refused" ? "not-dispatched" : "possibly-dispatched",
              milestone: "none",
              observation: mode === "refused" ? "before-exposure" : "closed",
              cleanup: mode === "refused" ? "not-requested" : "confirmed",
            });
            if (mode === "refused") {
              expect(yield* state).toEqual({
                size: "250",
                quantity: "1",
                month: "01",
                writes: [],
                submits: 0,
              });
              expect(yield* session.get).toBe(handle);
              const current = yield* handle.observe;
              const selected = current.controls.find((control) => control.label === "Bag size")!;

              expect(current.document).toBe(initial.document);
              expect(selected.options).toEqual([
                { label: "250 g", selected: true, disabled: false },
                { label: "500 g", selected: false, disabled: false },
                { label: "Other size", selected: false, disabled: false },
              ]);
              expect(JSON.stringify(current)).not.toContain("private-option-text");
              yield* handle.fill(
                ProtectedBrowserFill.make({
                  ref: selected.ref,
                  value: selected.options![1]!.label,
                }),
              );
              allowed = false;
              expect(
                yield* handle
                  .fill(
                    ProtectedBrowserFill.make({
                      ref: selected.ref,
                      value: "250",
                    }),
                  )
                  .pipe(Effect.flip),
              ).toMatchObject({ reason: "denied", dispatch: "not-dispatched" });
              expect(yield* state).toEqual({
                size: "500",
                quantity: "1",
                month: "01",
                writes: ["size"],
                submits: 0,
              });
              expect(page.isClosed()).toBe(false);
              expect(yield* handle.close).toBe("confirmed");
            } else {
              expect(closedState).toEqual({
                size: "250",
                quantity: mode === "normalized" ? "" : "1",
                month: "01",
                writes: mode === "reply-lost" ? ["size"] : [],
                submits: 0,
              });
              if (mode === "reply-lost") expect(beforeMark).toMatchObject({ writes: [] });
            }
          }
          expect(browser.isConnected()).toBe(false);
          expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({ reason: "closed" });
        }).pipe(
          Effect.provide(ProtectedBrowserSession.layer(policy)),
          Effect.scoped,
          Effect.provide(layer),
        );
      }).pipe(Effect.scoped, Effect.provide(BrowserCrypto.layer)),
    { timeout: 30_000 },
  );
}

for (const { before, writes, milestone } of [
  { before: "next field", writes: ["username"], milestone: "partial-fill" },
  { before: "submit", writes: ["username", "password"], milestone: "filled" },
] as const) {
  it.live(
    `retains the same native page when authority is busy before ${before}`,
    (test) =>
      Effect.gen(function* () {
        const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

        if (Option.isNone(executable)) return test.skip();

        const browser = yield* Effect.acquireRelease(
          native(() =>
            nativePuppeteer.launch({ executablePath: executable.value, headless: true }),
          ),
          (browser) => Effect.promise(() => browser.close()),
        );

        const page = yield* native(() => browser.newPage());

        yield* native(() => page.setRequestInterception(true));
        page.on("request", (request) => {
          void request
            .respond({
              contentType: "text/html",
              body: `<form onsubmit="event.preventDefault();window.submits++">
              <label>Account<input name="username" autocomplete="username"></label>
              <label>Password<input name="password" type="password"></label>
              <button>Sign in</button></form>
              <label>Address<input name="address"></label>
              <script>
                window.writes=[];window.submits=0;
                document.addEventListener('input', e => window.writes.push(e.target.name));
              </script>`,
            })
            .catch(() => {});
        });
        let pending = false;

        const access = BrowserCredentialAccess.of({
          caller: Effect.succeed(Redacted.make("authorized-test-invocation")),
          list: () =>
            Effect.succeed([
              {
                key: Redacted.make("dummy-vault-id"),
                metadata: CredentialOfferMetadata.make({ label: "Dummy only" }),
              },
            ]),
          authorize: () =>
            Effect.gen(function* () {
              const count = yield* native(() => page.evaluate("window.writes.length")).pipe(
                Effect.mapError(() => new CredentialAccessError({ reason: "resolver" })),
              );

              if (pending && count === writes.length)
                return yield* new CredentialAccessError({ reason: "busy" });
            }),
          resolve: () =>
            Effect.succeed(
              LoginCredential.make({
                username: Redacted.make("dummy@example.test"),
                password: Redacted.make(secret),
              }),
            ),
          authorizeAction: () =>
            pending ? Effect.fail(new CredentialAccessError({ reason: "busy" })) : Effect.void,
          observation: () =>
            pending
              ? Effect.fail(new CredentialAccessError({ reason: "busy" }))
              : Effect.succeed("trust-recipient-no-credential-echo"),
        });

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

        const layer = browserRunProtectedLayer().pipe(
          Layer.provide(
            Layer.succeed(BrowserRunProtectedTransport, {
              open: () => Effect.succeed(driver),
            }),
          ),
          Layer.provideMerge(Layer.succeed(BrowserCredentialAccess, access)),
        );

        yield* Effect.gen(function* () {
          const session = yield* ProtectedBrowserSession;
          const handle = yield* session.get;

          yield* handle.navigate(
            ProtectedBrowserNavigate.make({ url: "https://alpha.test/login" }),
          );
          const initial = yield* handle.observe;
          const username = initial.controls.find((control) => control.role === "username")!;
          const password = initial.controls.find((control) => control.role === "password")!;
          const submit = initial.controls.find((control) => control.role === "submit")!;

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

          pending = true;
          expect(yield* handle.useCredential(request).pipe(Effect.flip)).toMatchObject({
            reason: "busy",
            dispatch: "dispatched",
            milestone,
            observation: "protected",
            cleanup: "not-requested",
          });
          expect(yield* native(() => page.evaluate("window.writes"))).toEqual(writes);
          expect(yield* native(() => page.evaluate("window.submits"))).toBe(0);
          expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({
            reason: "busy",
            dispatch: "not-dispatched",
            cleanup: "not-requested",
          });

          pending = false;
          expect(yield* session.get).toBe(handle);
          expect(yield* handle.useCredential(request).pipe(Effect.flip)).toMatchObject({
            reason: "stale-reference",
            dispatch: "not-dispatched",
          });
          const current = yield* handle.observe;

          expect(current.document).toBe(initial.document);
          expect(current.observation).toBe("approved-after-exposure");
          const address = current.controls.find((control) => control.label === "Address")!;

          yield* handle.fill(ProtectedBrowserFill.make({ ref: address.ref, value: "New address" }));
          expect(yield* native(() => page.evaluate("window.writes"))).toEqual([
            ...writes,
            "address",
          ]);
          expect(yield* native(() => page.evaluate("window.submits"))).toBe(0);
          expect(page.isClosed()).toBe(false);
          expect(yield* handle.close).toBe("confirmed");
          expect(browser.isConnected()).toBe(false);
          expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({ reason: "closed" });
        }).pipe(
          Effect.provide(ProtectedBrowserSession.layer(policy)),
          Effect.scoped,
          Effect.provide(layer),
        );
      }).pipe(Effect.scoped, Effect.provide(BrowserCrypto.layer)),
    { timeout: 30_000 },
  );
}

it.live(
  "fills checkout email, rejects replaced nodes, preserves login, and fills a merchant-bound payment frame",
  (test) =>
    Effect.gen(function* () {
      const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

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
        else if (url.pathname === "/checkout")
          body = `
            <form><label>Receipt email<input name="email" type="email" autocomplete="email"></label>
              <input aria-label="Card number" autocomplete="cc-number">
              <input aria-label="Security code" autocomplete="cc-csc"></form>
            <form><input aria-label="Email-first account" type="email" autocomplete="username"></form>
            <form><input aria-label="Login email" type="email" autocomplete="email">
              <input aria-label="Password" type="password"></form>`;
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

        phase = "checkout-email";
        yield* handle.navigate(
          ProtectedBrowserNavigate.make({ url: "https://alpha.test/checkout" }),
        );
        const checkoutControls = (yield* handle.observe).controls;
        const email = checkoutControls.find((control) => control.label === "Receipt email")!;

        yield* handle.fill(
          ProtectedBrowserFill.make({ ref: email.ref, value: "receipt@example.test" }),
        );
        expect(email.role).toBe("text");
        for (const [label, role] of [
          ["Card number", "card-number"],
          ["Security code", "card-security-code"],
          ["Email-first account", "username"],
          ["Login email", "username"],
          ["Password", "password"],
        ] as const) {
          const control = checkoutControls.find((control) => control.label === label)!;

          expect(control.role).toBe(role);
          expect(
            yield* handle
              .fill(ProtectedBrowserFill.make({ ref: control.ref, value: "ordinary" }))
              .pipe(Effect.flip),
          ).toMatchObject({
            reason: "unsupported",
            dispatch: "not-dispatched",
            observation: "before-exposure",
            cleanup: "not-requested",
          });
        }
        expect(
          yield* handle.listCredentialOffers(
            ListCredentialOffers.make({
              kind: "login",
              target: checkoutControls.find((control) => control.label === "Email-first account")!
                .ref,
            }),
          ),
        ).toHaveLength(1);
        expect(
          yield* native(() =>
            page.evaluate("[...document.querySelectorAll('input')].map(el => el.value)"),
          ),
        ).toEqual(["receipt@example.test", "", "", "", "", ""]);
        expect(resolutions).toBe(0);
        expect((yield* handle.observe).observation).toBe("before-exposure");

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
              Effect.catchIf(
                (error) => error.reason === "stale-reference",
                () => Effect.succeed(Option.none()),
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
      const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

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
        <form><input aria-label="Password" type="password"><input aria-label="Card" autocomplete="cc-number"><select aria-label="Card month" autocomplete="cc-exp-month"><option value="01">private-expiry-label</option></select></form>
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
        "private-expiry-label",
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
      expect(region.options).toEqual(
        expect.arrayContaining([
          { label: "Choose", selected: true, disabled: false },
          { label: "California", selected: false, disabled: false },
          { label: "Disabled group", selected: false, disabled: true },
        ]),
      );
      expect(JSON.stringify(initial)).not.toContain('"CA"');
      expect(
        initial.controls.find((control) => control.label === "Card month")!.options,
      ).toBeUndefined();
      for (const label of ["Disabled", "OTP", "Password hint"])
        expect(initial.controls.find((control) => control.label === label)!.role).toBe(
          "unsupported",
        );
      yield* driver.fill(address.ref, "text", Redacted.make("123 Example Street"));
      yield* driver.fill(notes.ref, "text", Redacted.make("Leave at reception"));
      yield* driver.fill(
        region.ref,
        "select",
        Redacted.make(region.options!.find((option) => option.label === "California")!.label),
      );
      expect(yield* native(() => page.evaluate("document.querySelector('select').value"))).toBe(
        "CA",
      );
      yield* driver.fill(region.ref, "select", Redacted.make("New York"));
      for (const value of [
        "Disabled",
        "Disabled group",
        "Duplicate",
        "same",
        "Second",
        "Missing",
        "NY",
      ])
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
        current.controls.find((control) => control.label === "Region")!.options,
      ).toContainEqual({
        label: "New York",
        selected: true,
        disabled: false,
      });
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
      for (const label of ["Password", "Card", "Card month"])
        expect(
          yield* driver
            .fill(
              current.controls.find((control) => control.label === label)!.ref,
              "text",
              Redacted.make("ordinary"),
            )
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "stale-reference" });
      // The observation budget counts native choices, and never turns a shortened label
      // into a fill argument. Keep this fixture one choice beyond the public 256 limit.
      yield* native(() =>
        page.evaluate(`
        document.body.innerHTML = '<select aria-label="Many choices"></select>';
        for (let index = 0; index < 257; index++) {
          const option = document.createElement('option');
          option.label = index === 0 ? 'x'.repeat(201) : 'Choice ' + index;
          option.value = 'private-option-' + index;
          document.querySelector('select').append(option);
        }
      `),
      );
      const bounded = yield* driver.discover;
      const choices = bounded.controls[0]!.options!;

      expect(bounded.truncated).toBe(true);
      expect(choices).toHaveLength(255);
      expect(choices[0]!.label).toBe("Choice 1");
      expect(choices.at(-1)!.label).toBe("Choice 255");
      expect(JSON.stringify(bounded)).not.toContain("private-option-");
    }).pipe(
      Effect.scoped,
      Effect.provide(BrowserCrypto.layer),
      Effect.provideService(ProtectedBrowserDispatch, {
        mark: Effect.void,
        confirmNoWrite: Effect.void,
      }),
    ),
  { timeout: 30_000 },
);

for (const cacheControl of ["default", "no-store"] as const) {
  it.live(
    `reattaches the same polling page through two human handoffs and form navigation (${cacheControl})`,
    (test) =>
      Effect.gen(function* () {
        const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

        if (Option.isNone(executable)) return test.skip();

        const directory = yield* Effect.acquireRelease(
          native(() => mkdtemp(join(tmpdir(), "protected-reattach-"))),
          (directory) => Effect.promise(() => rm(directory, { recursive: true, force: true })),
        );

        yield* native(() =>
          promisify(execFile)("openssl", [
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=protected-reattach.test",
            "-keyout",
            join(directory, "key.pem"),
            "-out",
            join(directory, "cert.pem"),
          ]),
        );
        const key = yield* native(() => readFile(join(directory, "key.pem")));
        const cert = yield* native(() => readFile(join(directory, "cert.pem")));
        let shop = "";
        let checkout = "";
        let email = "before@example.test";
        let writes = 0;
        let polls = 0;
        const serviceWorker = cacheControl === "no-store";
        let delayedResponse: ServerResponse | undefined;
        let markDelayedStarted = () => {};

        const delayedStarted = new Promise<void>((resolve) => {
          markDelayedStarted = resolve;
        });

        let markDetachedPoll = () => {};

        const detachedPoll = new Promise<void>((resolve) => {
          markDetachedPoll = resolve;
        });

        let detached = false;

        const server = yield* Effect.acquireRelease(
          native(
            () =>
              new Promise<ReturnType<typeof createServer>>((resolve, reject) => {
                const server = createServer({ key, cert }, (request, response) => {
                  const path = request.url ?? "/";

                  if (cacheControl === "no-store") response.setHeader("cache-control", "no-store");
                  if (path === "/poll") {
                    polls++;
                    if (detached) markDetachedPoll();
                    response.end("ready");
                  } else if (path === "/delayed") {
                    delayedResponse = response;
                    markDelayedStarted();
                  } else if (path === "/sw.js") {
                    response.setHeader("content-type", "text/javascript");
                    response.end(`
                      self.addEventListener('install', () => self.skipWaiting());
                      self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
                      self.addEventListener('fetch', event => {
                        if (new URL(event.request.url).pathname === '/worker-response')
                          event.respondWith(new Response('from-service-worker'));
                      });
                    `);
                  } else if (path === "/worker-response") {
                    response.end("from-network");
                  } else if (path === "/processor") {
                    response.setHeader("content-type", "text/html");
                    response.end(`<main>Payment frame</main>
                <form><label>Card number<input name="card" autocomplete="cc-number"></label></form><script>
                window.cardWrites=0;document.querySelector('input').addEventListener('input',()=>window.cardWrites++);
                let n=0; const poll=async()=>{await fetch('/poll');document.body.dataset.polls=String(++n)};
                poll();setInterval(poll,1000);
              </script>`);
                  } else if (path === "/contact" && request.method === "POST") {
                    const chunks: Uint8Array[] = [];

                    request.on("data", (chunk: Uint8Array) => chunks.push(chunk));
                    request.on("end", () => {
                      email =
                        new URLSearchParams(Buffer.concat(chunks).toString()).get("email") ?? "";
                      writes++;
                      response.writeHead(303, { location: "/checkout" }).end();
                    });
                  } else {
                    response.setHeader("content-type", "text/html");
                    response.end(
                      path === "/checkout"
                        ? `<main>Receipt email: ${email}</main><a href="/contact">Edit email</a><button disabled>Pay</button><iframe src="${shop}/processor"></iframe><script>addEventListener('pagehide',event=>sessionStorage.setItem('checkoutCached',String(event.persisted)))</script>`
                        : path === "/contact"
                          ? `<form method="POST"><label>Email<input id="email" name="email"></label><button>Save</button></form>`
                          : `<a href="${checkout}/checkout">Checkout</a>`,
                    );
                  }
                });

                server.once("error", reject);
                server.listen(0, "127.0.0.1", () => resolve(server));
              }),
          ),
          (server) =>
            Effect.promise(
              () =>
                new Promise<void>((resolve) => {
                  server.closeAllConnections();
                  server.close(() => resolve());
                }),
            ),
        );

        const address = server.address();

        if (address === null || typeof address === "string")
          return yield* Effect.die("Missing fixture port");
        shop = `https://shop.test:${address.port}`;
        checkout = `https://checkout.test:${address.port}`;

        const provider = yield* Effect.acquireRelease(
          native(() =>
            nativePuppeteer.launch({
              executablePath: executable.value,
              headless: true,
              args: [
                "--host-resolver-rules=MAP *.test 127.0.0.1",
                "--no-proxy-server",
                "--ignore-certificate-errors",
              ],
            }),
          ),
          (browser) => Effect.promise(() => browser.close()),
        );

        const endpoint = provider.wsEndpoint();

        yield* native(() => provider.disconnect());

        const envelope = Schema.decodeUnknownSync(
          Schema.fromJsonString(
            Schema.Struct({
              id: Schema.Int,
              method: Schema.String,
              sessionId: Schema.optionalKey(Schema.String),
            }),
          ),
        );

        const sockets: WebSocket[] = [];
        const attachments: Browser[] = [];
        const connectionMethods: Array<string[]> = [];
        const methods = new Map<string, number>();
        const protocolErrors: Array<{ method: string; stage: string; code: number }> = [];
        const reports: Array<Cause.Cause<unknown>> = [];
        let handoffs = 0;
        let allocations = 0;
        let active = false;
        let closes = 0;
        let priorCheckoutCached = false;

        const upgrade = async () => {
          const socket = new WebSocket(endpoint);
          const sentMethods: string[] = [];

          sockets.push(socket);
          connectionMethods.push(sentMethods);
          await new Promise<void>((resolve, reject) => {
            socket.addEventListener("open", () => resolve(), { once: true });
            socket.addEventListener("error", () => reject(new ProbeError()), { once: true });
          });
          const events = new EventTarget();
          const pending = new Map<number, string>();

          const reply = Schema.decodeUnknownSync(
            Schema.fromJsonString(
              Schema.Struct({
                id: Schema.optionalKey(Schema.Int),
                error: Schema.optionalKey(Schema.Struct({ code: Schema.Int })),
              }),
            ),
          );

          socket.addEventListener("message", (event) => {
            const packet = reply(event.data);

            if (packet.id !== undefined) {
              const method = pending.get(packet.id);

              pending.delete(packet.id);
              if (packet.error !== undefined && method !== undefined)
                protocolErrors.push({
                  method,
                  stage:
                    method === "Fetch.continueRequest" || method === "Fetch.failRequest"
                      ? "request-callback"
                      : method === "Network.setBypassServiceWorker" || method === "Fetch.enable"
                        ? "interception-setup"
                        : "command-reply",
                  code: packet.error.code,
                });
            }
            events.dispatchEvent(new MessageEvent("message", { data: event.data }));
          });
          socket.addEventListener("close", () => events.dispatchEvent(new Event("close")));

          const transportSocket = {
            accept: () => {},
            addEventListener: events.addEventListener.bind(events),
            removeEventListener: events.removeEventListener.bind(events),
            close: () => socket.close(),
            send(message: string) {
              const packet = envelope(message);

              sentMethods.push(packet.method);
              pending.set(packet.id, packet.method);
              methods.set(packet.method, (methods.get(packet.method) ?? 0) + 1);

              // Only proprietary human-control replies are substituted. The production
              // binding, client, page, contexts, Fetch and reconnect all use real Chromium.
              if (
                packet.method === "Cloudflare.handoff" ||
                packet.method === "Cloudflare.getHandoffState"
              ) {
                if (packet.method === "Cloudflare.handoff") {
                  handoffs++;
                  active = true;
                }

                const result =
                  packet.method === "Cloudflare.handoff"
                    ? { handoffId: `local-handoff-${handoffs}` }
                    : { active, handoffId: `local-handoff-${handoffs}` };

                queueMicrotask(() =>
                  events.dispatchEvent(
                    new MessageEvent("message", {
                      data: JSON.stringify({ id: packet.id, sessionId: packet.sessionId, result }),
                    }),
                  ),
                );
              } else socket.send(message);
            },
          };

          return Object.defineProperties(new Response(null), {
            status: { value: 101 },
            webSocket: { value: transportSocket },
          });
        };

        sdk.connected.mockImplementation((browser) => attachments.push(browser));
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            for (const socket of sockets) socket.close();
            sdk.connected.mockReset();
          }),
        );

        const access = BrowserCredentialAccess.of({
          caller: Effect.succeed(Redacted.make("native-reattach-test")),
          list: () =>
            Effect.succeed([
              {
                key: Redacted.make("dummy-card-key"),
                metadata: CredentialOfferMetadata.make({ label: "Dummy card" }),
              },
            ]),
          authorize: () => Effect.void,
          authorizeAction: () => Effect.void,
          resolve: () =>
            Effect.succeed(
              CardCredential.make({
                name: Redacted.make("Dummy Shopper"),
                number: Redacted.make("4111111111111111"),
                expiry: Redacted.make("09/2030"),
                expiryMonth: Redacted.make("09"),
                expiryYear: Redacted.make("2030"),
              }),
            ),
          observation: () =>
            Effect.succeed(
              CredentialObservationGrant.make({
                decision: "trust-recipient-no-credential-echo",
                origins: [shop, checkout],
              }),
            ),
        });

        const lifecycle = Layer.succeed(BrowserRunSessionLifecycle, {
          close: () =>
            Effect.sync(() => {
              closes++;
            }).pipe(Effect.andThen(Effect.promise(() => provider.close()))),
        });

        const layer = browserRunProtectedHostLayer().pipe(
          Layer.provide(
            browserRunProtectedBindingLayer({
              browser: {
                fetch: async (_input, init) => {
                  if (init?.method !== "POST") return upgrade();
                  allocations++;

                  return Response.json({ sessionId: "00000000-0000-4000-8000-000000000041" });
                },
              },
            }),
          ),
          Layer.provide(Layer.merge(lifecycle, BrowserCrypto.layer)),
          Layer.provideMerge(Layer.succeed(BrowserCredentialAccess, access)),
          Layer.provideMerge(
            ErrorReporter.layer([ErrorReporter.make(({ cause }) => reports.push(cause))]),
          ),
        );

        const request = BrowserRunHandoffRequest.make({
          instructions: "Review only",
          timeout: 60_000,
        });

        const lifetimeClock = yield* TestClock.make();

        const checkpointCodec = Schema.fromJsonString(
          Schema.toCodecJson(BrowserRunProtectedCheckpoint),
        );

        yield* Effect.gen(function* () {
          const host = yield* BrowserRunProtectedHost;

          const first = yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* host.open(
                InteractiveBrowserPolicy.make({
                  network: { _tag: "Unrestricted" },
                  maxActions: 100,
                  maxElapsedMillis: cacheControl === "default" ? 8 * 60 * 60_000 : 120_000,
                  maxReturnedBytes: 16_384,
                }),
              );

              yield* session.handle.navigate(ProtectedBrowserNavigate.make({ url: `${shop}/` }));
              yield* session.handle.navigate(
                ProtectedBrowserNavigate.make({ url: `${checkout}/checkout` }),
              );
              const observed = yield* session.handle.observe;
              const card = observed.controls.find((control) => control.role === "card-number")!;

              const offers = yield* session.handle.listCredentialOffers(
                ListCredentialOffers.make({ kind: "card", target: card.ref }),
              );

              expect(
                yield* session.handle.useCredential(
                  UseCredential.make({
                    offer: offers[0]!.ref,
                    fields: [{ ref: card.ref, role: "card-number" }],
                  }),
                ),
              ).toMatchObject({ dispatch: "dispatched", milestone: "filled" });
              const pages = yield* native(() => attachments[0]!.pages());
              const page = pages.find((page) => page.url() === `${checkout}/checkout`)!;

              yield* host.keepAlive(session.sessionId);
              expect(connectionMethods.at(-1)).toEqual(["Browser.getVersion"]);
              expect(sockets.at(-1)!.readyState).toBeGreaterThanOrEqual(WebSocket.CLOSING);
              expect(attachments).toHaveLength(1);
              expect(attachments[0]!.isConnected()).toBe(true);

              if (serviceWorker) {
                yield* native(() =>
                  page.evaluate(`(async () => {
                    await navigator.serviceWorker.register('/sw.js');
                    await navigator.serviceWorker.ready;
                    if (!navigator.serviceWorker.controller)
                      await new Promise(resolve => navigator.serviceWorker.addEventListener(
                        'controllerchange', () => resolve(), { once: true }));
                  })()`),
                );
              }
              yield* native(() =>
                page.evaluate(
                  "void fetch('/delayed').then(() => { document.body.dataset.delayed = 'complete'; })",
                ),
              );
              yield* native(() => delayedStarted);
              const checkpoint = yield* session.suspend;

              yield* session.detach;

              return Schema.decodeSync(checkpointCodec)(
                Schema.encodeSync(checkpointCodec)(checkpoint),
              );
            }),
          );

          if (cacheControl === "default") yield* lifetimeClock.adjust("2 hours");
          detached = true;
          delayedResponse!.end("released-after-detach");
          yield* native(() => detachedPoll);
          detached = false;
          yield* host.keepAlive(first.sessionId);
          expect(connectionMethods.at(-1)).toEqual(["Browser.getVersion"]);
          expect(sockets.at(-1)!.readyState).toBeGreaterThanOrEqual(WebSocket.CLOSING);
          expect(attachments).toHaveLength(1);

          const human = yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* host.resume(first);

              const page = (yield* native(() => attachments.at(-1)!.pages())).find(
                (page) => page.url() === `${checkout}/checkout`,
              )!;

              yield* native(() =>
                page.waitForFunction("document.body.dataset.delayed === 'complete'"),
              );
              const processor = page.frames().find((frame) => frame.url() === `${shop}/processor`)!;

              expect(yield* native(() => processor.evaluate("window.cardWrites"))).toBe(1);
              if (serviceWorker)
                expect(
                  yield* native(() =>
                    page.evaluate(async () => (await fetch("/worker-response")).text()),
                  ),
                ).toBe("from-service-worker");
              const checkpoint = yield* session.handoff(request);

              yield* session.detach;

              return checkpoint;
            }),
          );

          const viewer = yield* native(() =>
            nativePuppeteer.connect({ browserWSEndpoint: endpoint }),
          );

          active = false;
          yield* native(() => viewer.disconnect());

          const second = yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* host.resume(human);

              yield* session.returnControl;
              yield* session.handle.observe;
              const browser = attachments.at(-1);

              const context = browser
                ?.browserContexts()
                .find((context) => context.id === Redacted.value(first.contextId));

              if (context === undefined) return yield* Effect.die("Missing retained context");
              const pages = yield* native(() => context.pages());
              const page = pages[0];

              if (page === undefined) return yield* Effect.die("Missing retained page");
              yield* native(() => page.goto(`${checkout}/contact`));
              yield* native(() => page.type("#email", "corrected@example.test"));
              yield* native(() => Promise.all([page.waitForNavigation(), page.click("button")]));
              const observed = yield* session.handle.observe;

              expect(observed.text).toContain("corrected@example.test");
              expect(yield* native(() => page.$eval("button", (button) => button.disabled))).toBe(
                true,
              );
              priorCheckoutCached =
                (yield* native(() => page.evaluate("sessionStorage.getItem('checkoutCached')"))) ===
                "true";
              // Keep production caching behavior in the regression: disabling BFCache
              // or its iframe targets must not turn the cacheable case into a false pass.
              expect(priorCheckoutCached).toBe(cacheControl === "default");
              const checkpoint = yield* session.suspend;

              yield* session.detach;

              return checkpoint;
            }),
          );

          yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* host.resume(second);

              const context = attachments
                .at(-1)
                ?.browserContexts()
                .find((context) => context.id === Redacted.value(first.contextId));

              if (context === undefined) return yield* Effect.die("Missing reattached context");
              const pages = yield* native(() => context.pages());

              const processor = pages[0]
                ?.frames()
                .find((frame) => frame.url() === `${shop}/processor`);

              if (processor === undefined)
                return yield* Effect.die("Missing live cross-origin frame");
              expect(
                yield* native(() => processor.evaluate("document.body.dataset.polls")),
              ).toMatch(/^[1-9]\d*$/);
              const checkpoint = yield* session.handoff(request);

              expect(Redacted.value(checkpoint.contextId) === Redacted.value(first.contextId)).toBe(
                true,
              );
              expect(Redacted.value(checkpoint.targetId) === Redacted.value(first.targetId)).toBe(
                true,
              );
              expect(Redacted.value(checkpoint.sessionId) === Redacted.value(first.sessionId)).toBe(
                true,
              );
              expect(checkpoint.protected.startedAt).toBe(first.protected.startedAt);
              expect(checkpoint.protected.policy).toEqual(first.protected.policy);
              expect(new Set(attachments).size).toBe(4);
              expect(handoffs).toBe(2);
              expect(allocations).toBe(1);
              expect(writes).toBe(1);
              expect(polls).toBeGreaterThan(0);
              expect(closes).toBe(0);
              if (serviceWorker) {
                active = false;
                yield* session.returnControl;
                yield* session.handle.observe;
                // A real transport loss must still fence tools; no SDK failure is injected.
                yield* native(
                  () =>
                    new Promise<void>((resolve) => {
                      attachments.at(-1)!.once("disconnected", () => resolve());
                      sockets.at(-1)!.close();
                    }),
                );
                expect(yield* session.handle.observe.pipe(Effect.flip)).toMatchObject({
                  reason: "stale-reference",
                  dispatch: "not-dispatched",
                });
                expect(writes).toBe(1);
              }
              expect(yield* session.close).toBe("confirmed");
              expect(closes).toBe(1);
              expect(attachments.at(-1)!.isConnected()).toBe(false);
              expect(methods.has("Network.setBypassServiceWorker")).toBe(false);
              expect([...methods.keys()].filter((method) => method.startsWith("Fetch."))).toEqual(
                [],
              );
              expect(reports).toEqual([]);
            }),
          );
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              // Method names, stages and numeric CDP error codes only: never wire payloads.
              console.info(
                JSON.stringify({
                  cacheControl,
                  interception: [...methods].filter(
                    ([method]) =>
                      method === "Network.setBypassServiceWorker" || method.startsWith("Fetch."),
                  ),
                  protocolErrors,
                  reports: reports.map((cause) =>
                    cause.reasons.map((reason) =>
                      reason._tag === "Fail" && reason.error instanceof BrowserRunFailure
                        ? {
                            classification: reason._tag,
                            operation: reason.error.operation,
                            reason: reason.error.reason,
                          }
                        : { classification: reason._tag },
                    ),
                  ),
                }),
              );
            }),
          ),
          Effect.provide(layer),
          Effect.provideService(Clock.Clock, lifetimeClock),
        );
      }).pipe(Effect.scoped),
    { timeout: 45_000 },
  );
}
