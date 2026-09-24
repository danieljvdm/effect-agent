import { createServer } from "node:http";

import {
  BrowserRunInteractiveBinding,
  BrowserRunPageObservation,
  browserRunInteractiveLayer,
  isBrowserRunUndispatchedActionError,
} from "@effect-agent/platform-cloudflare/interactive-browser";
import { expect, it } from "@effect/vitest";
import { Config, Effect, Layer, Logger, Option, Schema } from "effect";
import {
  BrowserClickRequest,
  BrowserFillRequest,
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  InteractiveBrowser,
  InteractiveBrowserPolicy,
} from "effect-agent/interactive-browser";
import nativePuppeteer from "puppeteer-core";
import { vi } from "vite-plus/test";

import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";
import { browserResponse } from "./browser-response.ts";

const sdk = vi.hoisted(() => ({ connect: vi.fn<() => Promise<object>>() }));

vi.mock("puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js", () => ({
  default: { ...sdk, acquire: async () => ({ sessionId: "c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99" }) },
}));

class NativeProbeError extends Schema.TaggedError<NativeProbeError>()("NativeProbeError", {
  cause: Schema.Defect(),
}) {}

const sdkCall = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new NativeProbeError({ cause }) });

// Opt-in local transport proof. No Cloudflare credentials or deployment. The
// Puppeteer version and every adapter callback are the production ones.
it.live(
  "refuses stale DOM authority at final dispatch in real Chromium",
  (context) =>
    Effect.gen(function* () {
      const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

      if (Option.isNone(executable)) return context.skip();
      let cartRequests = 0;

      const html = `<!doctype html><html><body>
    <form id="cart">
      <input id="small" type="radio" name="size" value="private-small-value" required style="display:none"><label for="small">12oz</label>
      <div id="cart-target"><button>Add to Cart</button></div>
    </form>
    <script>
    document.querySelector('#cart').addEventListener('submit', e => { e.preventDefault(); setTimeout(() => fetch('/cart', {method:'POST',body:'private-body'}), 100); });
    </script>
  </body></html>`;

      // Node HTTP is isolated to this scoped test fixture, with all connection and
      // timer ownership here. Production browser code stays platform-independent.
      const timers = new Set<ReturnType<typeof setTimeout>>();

      const server = yield* Effect.acquireRelease(
        sdkCall(
          () =>
            new Promise<ReturnType<typeof createServer>>((resolve, reject) => {
              const server = createServer((request, response) => {
                if (request.url === "/cart") {
                  cartRequests++;

                  const timer = setTimeout(() => {
                    timers.delete(timer);
                    response.writeHead(200, { "content-type": "application/json" }).end("{}");
                  }, 150);

                  timers.add(timer);
                } else {
                  response.setHeader("content-type", "text/html");
                  response.end(html);
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
                for (const timer of timers) clearTimeout(timer);
                server.closeAllConnections();
                server.close(() => resolve());
              }),
          ),
      );

      const address = server.address();

      if (address === null || typeof address === "string")
        return yield* Effect.die("Missing local server address");
      const url = `http://127.0.0.1:${address.port}`;

      const browser = yield* Effect.acquireRelease(
        sdkCall(() => nativePuppeteer.launch({ executablePath: executable.value, headless: true })),
        (browser) => Effect.promise(() => browser.close()),
      );

      const page = yield* sdkCall(() => browser.newPage());
      const requestListenerBaseline = page.listenerCount("request");

      sdk.connect.mockResolvedValue({
        createBrowserContext: async () => ({ newPage: async () => page, close: async () => {} }),
        sessionId: () => "c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99",
        isConnected: () => browser.isConnected(),
        on: () => {},
        off: () => {},
        close: async () => {},
      });

      const unused = async (): Promise<Response> => {
        throw new Error("No Cloudflare requests");
      };

      const logs: Array<ReturnType<typeof Logger.formatStructured.log>> = [];

      const layer = browserRunInteractiveLayer().pipe(
        Layer.provide(
          BrowserRunInteractiveBinding.layer({
            browser: {
              fetch: async (_input, init) => browserResponse(init),
              quickAction: unused,
            },
          }).pipe(
            Layer.provide(Layer.succeed(BrowserRunSessionLifecycle)({ close: () => Effect.void })),
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const handle = yield* (yield* InteractiveBrowser).open(
          InteractiveBrowserPolicy.make({
            network: { _tag: "Unrestricted" },
            maxActions: 30,
            maxElapsedMillis: 30_000,
            maxReturnedBytes: 256 * 1024,
          }),
        );

        yield* handle.navigate(BrowserNavigateRequest.make({ url }));
        const read = handle.readText(BrowserReadTextRequest.make({}));
        const initial = yield* read;
        const observed = yield* Schema.decodeEffect(BrowserRunPageObservation)(initial.text);
        const size = observed.controls.find((c) => c.label === "12oz");
        const cart = observed.controls.find((c) => c.label === "Add to Cart");

        if (size === undefined || cart === undefined)
          return yield* Effect.die("Missing product controls");

        // https://github.com/danieljvdm/effect-agent/commit/a20fb79eb86f5b279460cfbc23021c765b6fa2c5
        yield* sdkCall(() =>
          page.$eval("#cart-target", (element) => {
            element.setAttribute("data-guard-clicks", "0");
            Reflect.apply(Reflect.get(element, "addEventListener"), element, [
              "click",
              () => element.setAttribute("data-guard-clicks", "1"),
            ]);
          }),
        );
        expect(
          isBrowserRunUndispatchedActionError(
            yield* handle
              .click(
                BrowserClickRequest.make({
                  selector: "#cart-target",
                  expectedTarget: {
                    documentId: observed.documentId,
                    nodeId: cart.nodeId,
                    state: cart,
                    scopeSelector: "#cart",
                  },
                }),
              )
              .pipe(Effect.flip),
          ),
        ).toBe(true);
        expect(
          yield* sdkCall(() =>
            page.$eval("#cart-target", (element) => element.getAttribute("data-guard-clicks")),
          ),
        ).toBe("0");

        // https://github.com/danieljvdm/effect-agent/commit/5f83df46d392b1d61e39cb2c74d9eebf36c52415
        for (const update of [
          { property: "checked", value: true, restore: false },
          { property: "type", value: "password", restore: "radio" },
        ]) {
          yield* sdkCall(() =>
            page.$eval(
              "#small",
              (element, update) => Reflect.set(element, update.property, update.value),
              update,
            ),
          );
          expect(
            isBrowserRunUndispatchedActionError(
              yield* handle
                .click(
                  BrowserClickRequest.make({
                    selector: size.selector,
                    expectedTarget: {
                      documentId: observed.documentId,
                      nodeId: size.nodeId,
                      state: size,
                    },
                  }),
                )
                .pipe(Effect.flip),
            ),
          ).toBe(true);
          yield* sdkCall(() =>
            page.$eval(
              "#small",
              (element, update) => Reflect.set(element, update.property, update.restore),
              update,
            ),
          );
        }
        yield* sdkCall(() =>
          page.$eval(cart.selector, (element) => {
            element.replaceWith(element.cloneNode(true));
          }),
        );
        const expectedTarget = { documentId: observed.documentId, nodeId: cart.nodeId };

        expect(
          isBrowserRunUndispatchedActionError(
            yield* handle
              .click(BrowserClickRequest.make({ selector: cart.selector, expectedTarget }))
              .pipe(Effect.flip),
          ),
        ).toBe(true);
        expect(
          isBrowserRunUndispatchedActionError(
            yield* handle
              .fill(
                BrowserFillRequest.make({
                  selector: cart.selector,
                  value: "refused",
                  expectedTarget,
                }),
              )
              .pipe(Effect.flip),
          ),
        ).toBe(true);
        const refreshed = yield* Schema.decodeEffect(BrowserRunPageObservation)((yield* read).text);
        const freshCart = refreshed.controls.find((control) => control.selector === cart.selector);

        if (freshCart === undefined) return yield* Effect.die("Missing replacement control");
        expect(freshCart.nodeId).not.toBe(cart.nodeId);
        // Replace from a synchronous DOM callback at the final validation/dispatch boundary.
        yield* sdkCall(() =>
          page.$eval(freshCart.selector, (element) => {
            const original = Reflect.get(element, "getClientRects");

            Reflect.set(element, "getClientRects", () => {
              const rects = Reflect.apply(original, element, []);

              element.replaceWith(element.cloneNode(true));

              return rects;
            });
          }),
        );
        expect(
          isBrowserRunUndispatchedActionError(
            yield* handle
              .click(
                BrowserClickRequest.make({
                  selector: freshCart.selector,
                  expectedTarget: {
                    documentId: refreshed.documentId,
                    nodeId: freshCart.nodeId,
                    state: freshCart,
                    scopeSelector: "#cart",
                  },
                }),
              )
              .pipe(Effect.flip),
          ),
        ).toBe(true);

        expect(cartRequests).toBe(0);
        expect(page.listenerCount("requestfinished")).toBe(0);
        expect(page.listenerCount("requestfailed")).toBe(0);
        expect(page.listenerCount("request")).toBe(requestListenerBaseline + 1);
        yield* handle.close;
        expect(page.listenerCount("request")).toBe(0);
        expect(logs.every((entry) => entry.cause === undefined)).toBe(true);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.merge(
            layer,
            Logger.layer([
              Logger.map(Logger.formatStructured, (entry) => {
                logs.push(entry);
              }),
            ]),
          ),
        ),
      );
    }).pipe(Effect.scoped),
  30_000,
);
