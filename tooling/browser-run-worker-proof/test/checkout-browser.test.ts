import {
  BrowserSessionError,
  BrowserSessionReference,
  BrowserSessions,
  type BrowserSession,
} from "@effect-agent/platform-cloudflare/browser-session";
import { assert, it } from "@effect/vitest";
import { Config, Effect, Option, Redacted, Stream } from "effect";
import puppeteer from "puppeteer-core";
import browserPuppeteer from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { buyerTools, CheckoutOwner, tools } from "../src/checkout-agent.ts";
import type { BrowserObservation } from "../src/checkout-contract.ts";

it.live(
  "returns the opened payment frame in the click result and copies its path into browser tools",
  (test) =>
    Effect.gen(function* () {
      const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

      if (Option.isNone(executable)) return test.skip();

      const launched = yield* Effect.acquireRelease(
        Effect.promise(() =>
          puppeteer.launch({ executablePath: executable.value, headless: true }),
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
        const url = new URL(request.url());

        const body =
          url.hostname === "shop.test"
            ? `<input name="number" value="merchant"><iframe src="https://other.test/card"></iframe>
            <details><summary>Use another card</summary><section>
              <iframe src="https://pay.test/wrapper"></iframe>
            </section></details>`
            : url.pathname === "/wrapper"
              ? `<article><iframe src="https://pay.test/card"></iframe></article>`
              : `<form><input name="number" value="unfilled"></form>`;

        void request.respond({ contentType: "text/html", body }).catch(() => {});
      });
      yield* Effect.promise(() => page.goto("https://shop.test/payment"));
      const observations: Array<typeof BrowserObservation.Type> = [];

      const reference = BrowserSessionReference.make({
        version: 1,
        sessionId: Redacted.make("00000000-0000-4000-8000-000000000001"),
        contextId: Redacted.make("local-context"),
        targetId: Redacted.make("local-page"),
        expiresAt: 1_900_000_000_000,
        commandTimeoutMillis: 1_000,
      });

      const unused = () => Effect.die("Only native observation and targeting are exercised");

      const session: BrowserSession = {
        reference,
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
        fillCredential: unused,
        handoff: unused,
        getLiveView: unused,
        getHandoffState: unused,
      };

      yield* Effect.gen(function* () {
        const handlers = yield* tools;

        yield* handlers.handle("observe", {}).pipe(Effect.flatMap(Stream.runDrain));
        assert.deepStrictEqual(
          observations[0]?.frames.map((frame) => frame.frame),
          [[]],
        );

        const clicked = yield* handlers
          .handle("click", { frame: [], selector: "summary" })
          .pipe(Effect.flatMap(Stream.runCollect));

        const observed = observations[1];
        const card = observed?.frames.find((frame) => frame.url === "https://pay.test/card");

        assert.deepStrictEqual(clicked.at(-1)?.result, {
          execution: "completed",
          observation: observed,
          readFailure: null,
        });
        assert.strictEqual(observed?.frames.length, 3);
        if (card?.frame === undefined) return yield* Effect.die("Missing observed card frame path");
        assert.strictEqual(card.frame.length, 2);

        const result = yield* handlers
          .handle("type", {
            frame: card.frame,
            selector: "input[name=number]",
            value: "targeted-card",
          })
          .pipe(Effect.flatMap(Stream.runCollect));

        assert.isFalse(result.at(-1)?.isFailure);
        const frame = page.frames().find((frame) => frame.url() === "https://pay.test/card");

        if (frame === undefined) return yield* Effect.die("Card frame detached");
        assert.strictEqual(
          yield* Effect.promise(() => frame.$eval("input", (input) => input.value)),
          "targeted-card",
        );
        assert.strictEqual(
          yield* Effect.promise(() => page.$eval("input", (input) => input.value)),
          "merchant",
        );
      }).pipe(
        Effect.provide(
          buyerTools({
            reference,
            shopOrigin: "https://shop.test",
            processorOrigin: "https://pay.test",
          }),
        ),
        Effect.provideService(
          BrowserSessions,
          BrowserSessions.of({
            create: unused,
            attach: () => Effect.succeed(session),
            keepAlive: unused,
            close: unused,
          }),
        ),
        Effect.provideService(
          CheckoutOwner,
          CheckoutOwner.of({
            authorize: Effect.void,
            observe: (observation) =>
              Effect.sync(() => {
                observations.push(observation);
              }),
            record: () => Effect.void,
            approval: unused(),
            human: unused(),
          }),
        ),
      );
    }),
  { timeout: 30_000 },
);
