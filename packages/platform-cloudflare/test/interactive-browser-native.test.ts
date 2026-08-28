import { createServer } from "node:http";

import nativePuppeteer from "@cloudflare/puppeteer/internal/puppeteer-core.js";
import {
  BrowserClickRequest,
  BrowserFillRequest,
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  InteractiveBrowser,
  InteractiveBrowserPolicy,
} from "@effect-agent/sandbox";
import { expect, it } from "@effect/vitest";
import { Config, Effect, Layer, Logger, Option, Schema } from "effect";
import { vi } from "vite-plus/test";

import {
  BrowserRunInteractiveBinding,
  browserRunInteractiveLayer,
  isBrowserRunUndispatchedActionError,
} from "../src/interactive-browser.ts";

const sdk = vi.hoisted(() => ({ launch: vi.fn<() => Promise<object>>() }));
vi.mock("@cloudflare/puppeteer", () => ({ default: sdk }));

class NativeProbeError extends Schema.TaggedError<NativeProbeError>()("NativeProbeError", {
  cause: Schema.Defect(),
}) {}
const sdkCall = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new NativeProbeError({ cause }) });
const Observation = Schema.fromJsonString(
  Schema.Struct({
    pageText: Schema.String,
    controlsTruncated: Schema.Boolean,
    controls: Schema.Array(
      Schema.Struct({
        selector: Schema.String,
        kind: Schema.String,
        label: Schema.optionalKey(Schema.String),
        checked: Schema.optionalKey(Schema.Boolean),
        selected: Schema.optionalKey(Schema.Boolean),
        required: Schema.optionalKey(Schema.Boolean),
        valid: Schema.optionalKey(Schema.Boolean),
        formValid: Schema.optionalKey(Schema.Boolean),
      }),
    ),
  }),
);

// Opt-in local transport proof. No Cloudflare credentials or deployment. The
// Puppeteer version and every adapter callback are the production ones.
it.live(
  "observes native product controls and delayed cart requests in real Chromium",
  (context) =>
    Effect.gen(function* () {
      const executable = yield* Config.option(Config.string("BROWSER_TEST_EXECUTABLE"));
      if (Option.isNone(executable)) return context.skip();
      let cartRequests = 0;
      const html = `<!doctype html><html><body>
    <nav>${Array.from({ length: 80 }, (_, i) => `<a href="#nav${i}">Navigation ${i}</a>`).join("")}</nav>
    <form id="cart">
      <input id="small" type="radio" name="size" value="private-small-value" required style="display:none"><label for="small">12oz</label>
      <input id="large" type="radio" name="size" value="private-large-value" required style="display:none"><label for="large">2lb</label>
      <select id="roast" required><option value="">Choose roast</option><option value="private-roast-value">Light</option><option value="private-dark-value">Dark</option></select>
      <input type="password" value="private-credential"><textarea>private-textarea-default</textarea>
      <button>Add to Cart</button>
    </form>
    <script>document.querySelector('#cart').addEventListener('submit', e => { e.preventDefault(); setTimeout(() => fetch('/cart', {method:'POST',body:'private-body'}), 100); });</script>
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
      sdk.launch.mockResolvedValue({
        createBrowserContext: async () => ({ newPage: async () => page, close: async () => {} }),
        sessionId: () => "native-probe",
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
          BrowserRunInteractiveBinding.layer({ browser: { fetch: unused, quickAction: unused } }),
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
        const observed = yield* Schema.decodeUnknownEffect(Observation)(initial.text);
        expect(observed.controlsTruncated).toBe(true);
        expect(observed.controls).toHaveLength(64);
        const size = observed.controls.find((c) => c.label === "12oz");
        const cart = observed.controls.find((c) => c.label === "Add to Cart");
        expect(size).toMatchObject({
          kind: "label:radio",
          checked: false,
          required: true,
          valid: false,
          formValid: false,
        });
        expect(cart).toBeDefined();
        expect(observed.controls.filter((c) => c.kind === "option")).toMatchObject([
          { label: "Choose roast", selected: true },
          { label: "Light", selected: false },
          { label: "Dark", selected: false },
        ]);
        for (const control of observed.controls) {
          expect(
            yield* sdkCall(() => page.$$eval(control.selector, (elements) => elements.length)),
          ).toBe(1);
          expect(control.selector).not.toContain("private-");
        }
        expect(initial.text).not.toContain("private-small-value");
        expect(initial.text).not.toContain("private-roast-value");
        expect(initial.text).not.toContain("private-credential");
        expect(observed.controls.every((control) => !control.label?.includes("private-"))).toBe(
          true,
        );
        expect(
          isBrowserRunUndispatchedActionError(
            yield* handle.click(BrowserClickRequest.make({ selector: "[" })).pipe(Effect.flip),
          ),
        ).toBe(true);
        expect(
          isBrowserRunUndispatchedActionError(
            yield* handle.click(BrowserClickRequest.make({ selector: "label" })).pipe(Effect.flip),
          ),
        ).toBe(true);
        if (size === undefined || cart === undefined)
          return yield* Effect.die("Missing product controls");
        yield* handle.click(BrowserClickRequest.make({ selector: cart.selector }));
        expect(cartRequests).toBe(0);
        yield* handle.click(BrowserClickRequest.make({ selector: size.selector }));
        yield* handle.fill(
          BrowserFillRequest.make({ selector: "#roast", value: "private-roast-value" }),
        );
        const selected = yield* Schema.decodeUnknownEffect(Observation)((yield* read).text);
        expect(selected.controls.find((c) => c.label === "12oz")).toMatchObject({
          checked: true,
          formValid: true,
        });
        expect(
          selected.controls.find((c) => c.kind === "option" && c.label === "Light"),
        ).toMatchObject({ selected: true });
        yield* handle.click(BrowserClickRequest.make({ selector: cart.selector }));
        expect(cartRequests).toBe(1);
        expect(logs.at(-1)?.annotations).toMatchObject({
          "browser.fetch_xhr_total": 1,
          "browser.fetch_xhr_failed": 0,
          "browser.fetch_xhr_2xx": 1,
          "browser.fetch_xhr_pending": 0,
          "browser.network_settle_timed_out": false,
        });
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
