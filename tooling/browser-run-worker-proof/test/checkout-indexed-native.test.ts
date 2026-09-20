import {
  BrowserSessionError,
  type BrowserSession,
} from "@effect-agent/platform-cloudflare/browser-session";
import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import nativePuppeteer from "puppeteer-core";
import browserPuppeteer from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { CheckoutOwner } from "../src/checkout-agent.ts";
import {
  credentialTarget,
  dispatchIndexed,
  observeIndexed,
  releaseIndexed,
} from "../src/checkout-indexed-browser.ts";

it.live(
  "executes current cross-origin indices and rejects replaced, changed, covered and hidden nodes",
  (test) =>
    Effect.gen(function* () {
      const executable = process.env.BROWSER_TEST_EXECUTABLE;

      if (!executable) return test.skip();

      const launched = yield* Effect.acquireRelease(
        Effect.promise(() =>
          nativePuppeteer.launch({ executablePath: executable, headless: true }),
        ),
        (browser) => Effect.promise(() => browser.close()),
      );

      const browser = yield* Effect.acquireRelease(
        Effect.promise(() =>
          browserPuppeteer.connect({ browserWSEndpoint: launched.wsEndpoint() }),
        ),
        (browser) => Effect.promise(() => browser.disconnect()),
      );

      const page = yield* Effect.promise(() => browser.newPage());

      yield* Effect.promise(() => page.setRequestInterception(true));
      page.on("request", (request) => {
        void request
          .respond({
            contentType: "text/html",
            body: request.url().startsWith("https://processor.test")
              ? '<form><label>Username<input autocomplete="username" id="user"></label><label>Password<input autocomplete="current-password" id="pass"></label><button id="pay" type="button" onclick="document.body.dataset.clicked=\'yes\'">Continue</button></form>'
              : '<button id="main" onclick="document.body.dataset.clicked=\'yes\'">Main</button><iframe src="https://processor.test/fields" style="width:600px;height:400px"></iframe>',
          })
          .catch(() => {});
      });
      yield* Effect.promise(() => page.goto("https://shop.test"));

      const session: Pick<BrowserSession, "run"> = {
        run: (authorize, action) =>
          authorize.pipe(
            Effect.andThen(
              Effect.tryPromise({
                try: () => action(page),
                catch: () =>
                  BrowserSessionError.make({
                    reason: "provider",
                    dispatch: "possibly-dispatched",
                    cleanup: "not-requested",
                  }),
              }),
            ),
          ),
      };

      yield* Effect.addFinalizer(() => releaseIndexed(session));
      const read = observeIndexed(session, ["https://shop.test", "https://processor.test"]);
      const snapshot = yield* read;

      expect(snapshot.observation.frames).toHaveLength(2);
      const control = snapshot.observation.controls.find((c) => c.label === "Continue")!;

      expect(yield* dispatchIndexed(session, snapshot, control, "CLICK", "")).toBe("completed");
      const frame = page.frames().find((f) => f.url().startsWith("https://processor.test"))!;

      expect(yield* Effect.promise(() => frame.evaluate(() => document.body.dataset.clicked))).toBe(
        "yes",
      );

      for (const change of ["replace", "disable", "state", "cover", "hide"] as const) {
        yield* Effect.promise(() => page.goto("https://shop.test"));
        const current = yield* read;
        const main = current.observation.controls.find((c) => c.label === "Main")!;

        yield* Effect.promise(() =>
          page.evaluate((kind) => {
            const el = document.getElementById("main")!;

            if (kind === "replace") el.replaceWith(el.cloneNode(true));
            if (kind === "disable") el.setAttribute("disabled", "");
            if (kind === "state") el.textContent = "Delete everything";
            if (kind === "hide") el.style.display = "none";
            if (kind === "cover") {
              const cover = document.createElement("div");

              cover.style.cssText = "position:fixed;inset:0;z-index:999;background:white";
              document.body.appendChild(cover);
            }
          }, change),
        );
        expect(yield* dispatchIndexed(session, current, main, "CLICK", "")).toBe("stale");
        expect(
          yield* Effect.promise(() => page.evaluate(() => document.body.dataset.clicked)),
        ).toBeUndefined();
      }
      yield* Effect.promise(() => page.goto("https://shop.test"));
      const credentials = yield* read;

      const controls = credentials.observation.controls.filter(
        (c) => c.autocomplete === "username" || c.autocomplete === "current-password",
      );

      const target = credentialTarget(credentials, controls, "account", "login")!;

      const credentialFrame = page
        .frames()
        .find((f) => f.url().startsWith("https://processor.test"))!;

      const guard = yield* Effect.acquireRelease(
        Effect.promise(() => target.guard(credentialFrame)),
        (handle) => Effect.promise(() => handle.dispose()),
      );

      expect(
        yield* Effect.promise(() =>
          guard.evaluate((check) => check(document.getElementById("user")!, 0)),
        ),
      ).toBe(true);
      yield* Effect.promise(() =>
        credentialFrame.evaluate(() => {
          const el = document.getElementById("user")!;

          el.replaceWith(el.cloneNode(true));
        }),
      );
      expect(
        yield* Effect.promise(() =>
          guard.evaluate((check) => check(document.getElementById("user")!, 0)),
        ),
      ).toBe(false);
    }).pipe(
      Effect.scoped,
      Effect.provideService(
        CheckoutOwner,
        CheckoutOwner.of({
          authorize: Effect.void,
          observe: () => Effect.void,
          observeIndexed: () => Effect.void,
          record: () => Effect.void,
          approval: Effect.succeed("approved"),
          human: Effect.succeed("human"),
        }),
      ),
    ),
);
