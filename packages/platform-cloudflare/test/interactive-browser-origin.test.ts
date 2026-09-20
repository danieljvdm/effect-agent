import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import nativePuppeteer from "@cloudflare/puppeteer/internal/puppeteer-core.js";
import {
  BrowserRunInteractiveBinding,
  BrowserRunInteractiveHost,
  BrowserRunOriginOverride,
  BrowserRunSessionLifecycle,
  browserRunInteractiveHostLayer,
} from "@effect-agent/platform-cloudflare/interactive-browser";
import { expect, it } from "@effect/vitest";
import { Config, Effect, Layer, Option, Schema } from "effect";
import { BrowserNavigateRequest, InteractiveBrowserPolicy } from "effect-agent/interactive-browser";
import { FetchHttpClient } from "effect/unstable/http";
import { vi } from "vite-plus/test";

const sdk = vi.hoisted(() => ({ connect: vi.fn<() => Promise<object>>() }));

vi.mock("@cloudflare/puppeteer", () => ({ default: sdk }));

class NativeProbeError extends Schema.TaggedError<NativeProbeError>()("NativeProbeError", {
  cause: Schema.Defect(),
}) {}

const native = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new NativeProbeError({ cause }) });

const server = (
  handler: (request: IncomingMessage, response: ServerResponse) => void,
  onUpgrade?: () => void,
) =>
  Effect.acquireRelease(
    native(
      () =>
        new Promise<ReturnType<typeof createServer>>((resolve, reject) => {
          const value = createServer(handler);

          value.on("upgrade", (_request, socket) => {
            onUpgrade?.();
            socket.destroy();
          });

          value.once("error", reject);
          value.listen(0, "127.0.0.1", () => resolve(value));
        }),
    ),
    (value) =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            value.closeAllConnections();
            value.close(() => resolve());
          }),
      ),
  ).pipe(
    Effect.map((value) => {
      const address = value.address();

      if (address === null || typeof address === "string") throw new Error("Missing server port");

      return `http://127.0.0.1:${address.port}`;
    }),
  );

it.effect.each([
  { productionUrl: "https://public.test/entry", stagingOrigin: "https://public.test" },
  { productionUrl: "http://public.test/entry", stagingOrigin: "https://staging.test" },
  { productionUrl: "https://public.test/entry?query=1", stagingOrigin: "https://staging.test" },
  {
    productionUrl: "https://user:password@public.test/entry",
    stagingOrigin: "https://staging.test",
  },
  { productionUrl: "https://public.test/entry", stagingOrigin: "https://staging.test/nested" },
])("rejects ambiguous or unsafe origin override configuration (%#)", (override) =>
  Effect.gen(function* () {
    expect(
      yield* Schema.decodeUnknownEffect(BrowserRunOriginOverride)(override).pipe(Effect.flip),
    ).toMatchObject({ _tag: "SchemaError" });
  }),
);

it.live(
  "routes one native browser pass without changing location or its request cookie authority",
  (test) =>
    Effect.gen(function* () {
      const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

      if (Option.isNone(executable)) return test.skip();
      let productionRequests = 0;
      let productionSockets = 0;

      const production = yield* server(
        (_request, response) => {
          productionRequests++;
          response.setHeader("content-type", "text/html");
          response.end("<title>Production</title><p>Real production</p>");
        },
        () => {
          productionSockets++;
        },
      );

      const received: Array<{ url: string; method: string; cookie: string; body: string }> = [];
      const staging = "http://localhost";
      const destinations: string[] = [];
      let started: () => void = () => {};
      let closed: () => void = () => {};
      const pendingStarted = new Promise<void>((resolve) => {
        started = resolve;
      });
      const pendingClosed = new Promise<void>((resolve) => {
        closed = resolve;
      });

      const upstream = yield* server((request, response) => {
        const chunks: Buffer[] = [];

        request.on("data", (chunk: Buffer) => chunks.push(chunk));
        request.on("end", () => {
          received.push({
            url: request.url ?? "",
            method: request.method ?? "",
            cookie: request.headers.cookie ?? "",
            body: Buffer.concat(chunks).toString(),
          });
          if (request.url === "/pending") {
            response.writeHead(200, { "content-type": "text/plain" });
            response.write("pending");
            response.once("close", closed);
            started();
          } else if (request.url === "/redirect") {
            response
              .writeHead(302, {
                location: `${staging}/after-redirect`,
                "set-cookie": "redirected=yes; Path=/; SameSite=Lax",
              })
              .end();
          } else if (request.url === "/leave") {
            response.writeHead(302, { location: "https://unrelated.test/" }).end();
          } else if (request.url === "/asset.js") {
            response.setHeader("content-type", "text/javascript");
            response.end("document.body.dataset.asset = 'loaded'");
          } else if (request.url?.startsWith("/echo")) {
            response.setHeader("content-type", "application/json");
            response.end(JSON.stringify({ cookie: request.headers.cookie ?? "" }));
          } else {
            response.setHeader("content-type", "text/html");
            response.setHeader("set-cookie", "session=staging; Path=/; SameSite=Lax");
            response.end(`<!doctype html><title>Staging</title><body>
            <header><button id="contact">Contact Us</button></header>
            <dialog id="notice">This link is outside the replicated filing flow.</dialog>
            <script src="/asset.js"></script>
            <script>document.querySelector('#contact').onclick = () => document.querySelector('#notice').showModal()</script>
          </body>`);
          }
        });
      });

      const browser = yield* Effect.acquireRelease(
        native(() => nativePuppeteer.launch({ executablePath: executable.value, headless: true })),
        (browser) => Effect.promise(() => browser.close()),
      );

      const direct = yield* native(() => browser.newPage());

      yield* native(() => direct.goto(`${production}/entry`));
      expect(yield* native(() => direct.title())).toBe("Production");
      yield* native(() => direct.close());
      const baseline = productionRequests;
      const context = yield* native(() => browser.createBrowserContext());
      const page = yield* native(() => context.newPage());

      sdk.connect.mockResolvedValue({
        createBrowserContext: async () => ({
          newPage: async () => page,
          browser: () => browser,
          close: () => context.close(),
        }),
        sessionId: () => "c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99",
        isConnected: () => browser.isConnected(),
        on: () => {},
        off: () => {},
        close: async () => {},
      });

      const live = browserRunInteractiveHostLayer().pipe(
        Layer.provide(
          BrowserRunInteractiveBinding.layer({
            browser: {
              fetch: async () =>
                Response.json({ sessionId: "c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99" }),
              quickAction: async () => {
                throw new Error("Unused quick action");
              },
            },
            originOverride: { productionUrl: `${production}/entry`, stagingOrigin: staging },
          }).pipe(
            Layer.provide(Layer.succeed(BrowserRunSessionLifecycle)({ close: () => Effect.void })),
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const session = yield* (yield* BrowserRunInteractiveHost).open(
          InteractiveBrowserPolicy.make({
            network: { _tag: "Unrestricted" },
            maxActions: 10,
            maxElapsedMillis: 30_000,
            maxReturnedBytes: 8_192,
          }),
        );

        yield* session.handle.navigate(
          BrowserNavigateRequest.make({ url: `${production}/entry?query=kept` }),
        );
        expect(page.url()).toBe(`${production}/entry?query=kept`);
        expect(
          yield* native(() => page.evaluate(() => Reflect.get(globalThis, "location").origin)),
        ).toBe(production);
        expect(yield* native(() => page.title())).toBe("Staging");
        expect(
          yield* native(() =>
            page.evaluate(() => Reflect.get(globalThis, "document").body.dataset.asset),
          ),
        ).toBe("loaded");
        yield* native(() => page.click("#contact"));
        expect(
          yield* native(() => page.$eval("dialog[open]", (element) => element.textContent)),
        ).toBe("This link is outside the replicated filing flow.");
        expect(page.url()).toBe(`${production}/entry?query=kept`);

        const cookies = yield* native(() =>
          page.evaluate(async () => {
            const options = {
              credentials: "omit",
              method: "POST",
              body: "posted body",
            };

            const omitted = await fetch("/echo?anonymous", options).then((response) =>
              response.json(),
            );

            const admitted = await fetch("/echo?authenticated").then((response) => response.json());

            return { omitted, admitted };
          }),
        );

        expect(cookies).toEqual({
          omitted: { cookie: "" },
          admitted: { cookie: "session=staging" },
        });
        expect(received).toContainEqual({
          url: "/echo?anonymous",
          method: "POST",
          cookie: "",
          body: "posted body",
        });
        yield* session.handle.navigate(
          BrowserNavigateRequest.make({ url: `${production}/redirect` }),
        );
        expect(page.url()).toBe(`${production}/after-redirect`);
        expect(received.find((request) => request.url === "/after-redirect")?.cookie).toContain(
          "redirected=yes",
        );
        expect(productionRequests).toBe(baseline);
        expect(destinations.length).toBeGreaterThan(0);
        expect(destinations.every((origin) => origin === staging)).toBe(true);
        expect(
          yield* native(() =>
            page.evaluate(
              (url) =>
                new Promise<boolean>((resolve) => {
                  const socket = new WebSocket(url);

                  socket.addEventListener("open", () => {
                    socket.close();
                    resolve(false);
                  });
                  socket.addEventListener("error", () => resolve(true));
                }),
              production.replace("http:", "ws:"),
            ),
          ),
        ).toBe(true);
        expect(productionSockets).toBe(0);
        expect(yield* session.detach.pipe(Effect.flip)).toMatchObject({
          _tag: "InteractiveBrowserUnsupportedError",
        });
        expect(
          yield* session.handoff({ instructions: "Test", timeout: 1_000 }).pipe(Effect.flip),
        ).toMatchObject({ _tag: "InteractiveBrowserUnsupportedError" });
        yield* native(() =>
          page.evaluate(() => {
            void fetch("/pending").catch(() => {});
          }),
        );
        yield* native(() => pendingStarted);

        const failure = yield* session.handle
          .navigate(BrowserNavigateRequest.make({ url: `${production}/leave` }))
          .pipe(Effect.flip);

        expect(failure).toMatchObject({ _tag: "InteractiveBrowserActionError" });
        expect(productionRequests).toBe(baseline);
        yield* session.close;
        yield* native(() => pendingClosed).pipe(Effect.timeout("2 seconds"));
      }).pipe(
        Effect.scoped,
        Effect.provide(live),
        Effect.provideService(FetchHttpClient.Fetch, (input, init) => {
          const requested = new URL(input instanceof Request ? input.url : input.toString());

          destinations.push(requested.origin);

          // Keep a default-port staging origin while the real local fixture uses
          // an ephemeral port; this catches retaining the production port.
          return fetch(new URL(requested.pathname + requested.search, upstream), init);
        }),
      );
    }).pipe(Effect.scoped),
  60_000,
);
