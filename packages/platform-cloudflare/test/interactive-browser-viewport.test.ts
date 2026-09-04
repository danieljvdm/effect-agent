import {
  BrowserRunInteractiveBinding,
  BrowserRunInteractiveHost,
  BrowserRunViewport,
  CloudflareInteractiveBrowser,
  browserRunInteractiveHostLayer,
} from "@effect-agent/platform-cloudflare/InteractiveBrowser";
import {
  InteractiveBrowser,
  InteractiveBrowserPolicy,
} from "@effect-agent/sandbox/InteractiveBrowser";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { expectTypeOf, vi } from "vite-plus/test";

import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";

const sdk = vi.hoisted(() => ({ launch: vi.fn<(...args: Array<unknown>) => Promise<object>>() }));

vi.mock("@cloudflare/puppeteer", () => ({ default: sdk }));

const unusedRpc = async (): Promise<Response> => {
  throw new Error("The mocked SDK must not call Browser Run");
};

const browser = { fetch: unusedRpc, quickAction: unusedRpc };

const open = Effect.gen(function* () {
  const host = yield* BrowserRunInteractiveHost;

  return yield* host.open(
    InteractiveBrowserPolicy.make({
      network: { _tag: "ExactHosts", allowedHosts: ["example.com"] },
      maxActions: 1,
      maxElapsedMillis: 5_000,
      maxReturnedBytes: 1_024,
    }),
  );
});

describe("Browser Run viewport boundary", () => {
  it.effect("assembles generic browser access without host controls or an eager launch", () =>
    Effect.gen(function* () {
      sdk.launch.mockClear();

      const live = CloudflareInteractiveBrowser.layer({
        browser,
        accountId: "1234567890abcdef1234567890abcdef",
        apiToken: Redacted.make("token"),
      });

      expectTypeOf<Layer.Success<typeof live>>().toEqualTypeOf<InteractiveBrowser>();
      expectTypeOf<Layer.Services<typeof live>>().toEqualTypeOf<HttpClient.HttpClient>();

      const service = yield* InteractiveBrowser.pipe(
        Effect.provide(live),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die("Unexpected HTTP request")),
        ),
      );

      expect(service.open).toBeTypeOf("function");
      expect(sdk.launch).not.toHaveBeenCalled();
    }),
  );
  it.effect.each([
    { width: 1, height: 1 },
    { width: 2_048, height: 2_048 },
    { width: 1_440, height: 900, deviceScaleFactor: 1 },
    { width: 1_024, height: 1_024, deviceScaleFactor: 2 },
    { width: 1_280, height: 720, deviceScaleFactor: 1.5 },
  ])("round-trips bounded presentation state (%#)", (input) =>
    Effect.gen(function* () {
      const viewport = yield* Schema.decodeUnknownEffect(BrowserRunViewport)(input);

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(BrowserRunViewport))(
        viewport,
      );

      expect(
        yield* Schema.decodeUnknownEffect(Schema.fromJsonString(BrowserRunViewport))(encoded),
      ).toEqual(viewport);
      expect(JSON.parse(encoded)).toEqual(input);
    }),
  );

  it.effect.each([
    { width: 0, height: 600 },
    { width: -1, height: 600 },
    { width: 800.5, height: 600 },
    { width: 800, height: 0 },
    { width: 800, height: 600.5 },
    { width: NaN, height: 600 },
    { width: 800, height: Infinity },
    { width: 2_049, height: 600 },
    { width: 800, height: 2_049 },
    { width: 800, height: 600, deviceScaleFactor: NaN },
    { width: 800, height: 600, deviceScaleFactor: Infinity },
    { width: 800, height: 600, deviceScaleFactor: 0.5 },
    { width: 800, height: 600, deviceScaleFactor: 2.1 },
    { width: 1_025, height: 600, deviceScaleFactor: 2 },
    { width: 800, height: 1_025, deviceScaleFactor: 2 },
    { width: 800, height: 600, isMobile: true },
    { width: 800, height: 600, hasTouch: true },
    { width: 800, height: 600, isLandscape: true },
  ])("rejects unsafe launch and resize requests without calling Puppeteer (%#)", (viewport) =>
    Effect.gen(function* () {
      sdk.launch.mockClear();

      const error = yield* BrowserRunInteractiveBinding.pipe(
        Effect.provide(
          BrowserRunInteractiveBinding.layer({ browser, viewport }).pipe(
            Layer.provide(Layer.succeed(BrowserRunSessionLifecycle)({ close: () => Effect.void })),
          ),
        ),
        Effect.flip,
      );

      expect(error).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      expect(sdk.launch).not.toHaveBeenCalled();

      const setViewport = vi.fn<(...args: Array<unknown>) => Promise<void>>(async () => {});

      mockBrowser(setViewport);
      yield* Effect.gen(function* () {
        const session = yield* open;

        expect(yield* session.resizeViewport(viewport).pipe(Effect.flip)).toMatchObject({
          _tag: "InteractiveBrowserPolicyDeniedError",
        });
      }).pipe(
        Effect.scoped,
        Effect.provide(
          browserRunInteractiveHostLayer().pipe(
            Layer.provide(
              BrowserRunInteractiveBinding.layer({ browser }).pipe(
                Layer.provide(
                  Layer.succeed(BrowserRunSessionLifecycle)({ close: () => Effect.void }),
                ),
              ),
            ),
          ),
        ),
      );
      expect(setViewport).not.toHaveBeenCalled();
    }),
  );

  it.effect.each([
    undefined,
    { width: 1_440, height: 900 },
    { width: 1_024, height: 768, deviceScaleFactor: 2 },
  ])(
    "forwards launch and resize presentation fields without emulation or navigation (%#)",
    (viewport) =>
      Effect.gen(function* () {
        sdk.launch.mockClear();
        const setViewport = vi.fn<(...args: Array<unknown>) => Promise<void>>(async () => {});

        mockBrowser(setViewport);
        const cleanupCalls: Array<string> = [];

        const client = HttpClient.make((request, url) =>
          Effect.sync(() => {
            cleanupCalls.push(`${request.method} ${url.pathname}`);

            return HttpClientResponse.fromWeb(
              request,
              new Response('{"status":"closed"}', {
                headers: { "content-type": "application/json" },
              }),
            );
          }),
        );

        yield* Effect.gen(function* () {
          const session = yield* open;

          yield* session.resizeViewport({ width: 960, height: 720, deviceScaleFactor: 2 });
          yield* session.resizeViewport({ width: 1_440, height: 900 });
        }).pipe(
          Effect.scoped,
          Effect.provide(
            CloudflareInteractiveBrowser.hostLayer({
              browser,
              accountId: "1234567890abcdef1234567890abcdef",
              apiToken: Redacted.make("token"),
              ...(viewport === undefined ? {} : { viewport }),
            }),
          ),
          Effect.provideService(HttpClient.HttpClient, client),
        );
        expect(cleanupCalls).toEqual([
          "DELETE /client/v4/accounts/1234567890abcdef1234567890abcdef/browser-rendering/devtools/browser/c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99",
        ]);
        expect(sdk.launch).toHaveBeenCalledExactlyOnceWith(browser, {
          keep_alive: 10_000,
          ...(viewport === undefined
            ? {}
            : {
                defaultViewport: {
                  ...viewport,
                  deviceScaleFactor: viewport.deviceScaleFactor ?? 1,
                },
              }),
        });
        expect(setViewport.mock.calls).toEqual([
          [{ width: 960, height: 720, deviceScaleFactor: 2 }],
          [{ width: 1_440, height: 900, deviceScaleFactor: 1 }],
        ]);
      }),
  );
});

// Transport substitute deliberately exposes no goto/reload or emulation methods.
const mockBrowser = (setViewport: (viewport: unknown) => Promise<void>) => {
  const page = {
    setViewport,
    url: () => "https://example.com/",
    close: async () => {},
    setBypassServiceWorker: async () => {},
    setRequestInterception: async () => {},
    on: () => {},
    off: () => {},
  };

  sdk.launch.mockResolvedValue({
    createBrowserContext: async () => ({ newPage: async () => page, close: async () => {} }),
    sessionId: () => "c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99",
    isConnected: () => true,
    on: () => {},
    off: () => {},
    close: async () => {},
  });
};
