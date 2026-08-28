import {
  BrowserClickRequest,
  BrowserFillRequest,
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  BrowserScreenshotRequest,
  BrowserScrollRequest,
  InteractiveBrowser,
  InteractiveBrowserError,
  InteractiveBrowserPolicy,
  type BrowserHandle,
  type InteractiveBrowserNetworkPolicy,
} from "@effect-agent/sandbox";
import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Exit, Fiber, Layer, Logger, Redacted, Schema, type Scope } from "effect";
import { TestClock } from "effect/testing";

import {
  BrowserRunInteractiveBinding,
  BrowserRunInteractiveHost,
  BrowserRunLiveViewRequest,
  BrowserRunHandoffRequest,
  BrowserRunViewport,
  browserRunInteractiveHostLayer,
  browserRunInteractiveLayer,
  type BrowserRunCloudflareCommand,
  type BrowserRunInteractiveBrowser,
  type BrowserRunInteractiveCdpSession,
  type BrowserRunInteractiveContext,
  type BrowserRunInteractivePage,
  type BrowserRunInteractiveRequest,
  type BrowserRunInteractiveRequestListener,
  type BrowserRunInteractiveSession,
} from "../src/interactive-browser.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type LayerRequirements<Value> =
  Value extends Layer.Layer<infer _Output, infer _Error, infer Requirements> ? Requirements : never;
type InteractiveLayerRequiresBinding = Equal<
  LayerRequirements<ReturnType<typeof browserRunInteractiveLayer>>,
  BrowserRunInteractiveBinding
>;
type ResizeEffect = Equal<
  ReturnType<BrowserRunInteractiveSession["resizeViewport"]>,
  Effect.Effect<void, InteractiveBrowserError>
>;
type ScopedOpenRequirement = Equal<
  ReturnType<InteractiveBrowser["Service"]["open"]> extends Effect.Effect<
    infer _Value,
    infer _Error,
    infer Requirements
  >
    ? Requirements
    : never,
  Scope.Scope
>;

type CloseTarget =
  | "page"
  | "context"
  | "browser"
  | "request-listener"
  | "disconnect-listener"
  | "cdp";

interface RequestEmissionOptions {
  readonly settlement?: "resolve" | "reject" | "throw";
  readonly urlThrows?: boolean;
}

interface FixtureControls {
  readonly emitRequest: (url: string, options?: RequestEmissionOptions) => void;
  readonly setUrl: (url: unknown) => void;
  readonly disconnect: () => void;
}

interface FixtureOptions {
  readonly initialUrl?: unknown;
  readonly connected?: boolean;
  readonly launchError?: unknown;
  readonly createContextError?: unknown;
  readonly newPageError?: unknown;
  readonly setupError?: unknown;
  readonly interceptionError?: unknown;
  readonly connectionStateError?: unknown;
  readonly urlError?: unknown;
  readonly sessionId?: unknown;
  readonly connectError?: unknown;
  readonly connect?: (
    sessionId: string,
    browser: BrowserRunInteractiveBrowser,
  ) => Promise<BrowserRunInteractiveBrowser>;
  readonly closeErrors?: ReadonlySet<CloseTarget>;
  readonly enableInterception?: (controls: FixtureControls) => Promise<void>;
  readonly launch?: (
    browser: BrowserRunInteractiveBrowser,
  ) => Promise<BrowserRunInteractiveBrowser>;
  readonly createContext?: (
    context: BrowserRunInteractiveContext,
  ) => Promise<BrowserRunInteractiveContext>;
  readonly newPage?: (page: BrowserRunInteractivePage) => Promise<BrowserRunInteractivePage>;
  readonly goto?: (url: string, controls: FixtureControls) => Promise<void>;
  readonly readText?: (selector: string | undefined, maximumBytes: number) => Promise<unknown>;
  readonly fill?: (selector: string, value: string) => Promise<void>;
  readonly click?: (selector: string) => Promise<void>;
  readonly screenshot?: (fullPage: boolean) => Promise<unknown>;
  readonly scroll?: (deltaX: number, deltaY: number) => Promise<void>;
  readonly setViewport?: (viewport: BrowserRunViewport) => Promise<void>;
  readonly cdpSend?: (
    command: BrowserRunCloudflareCommand,
    parameters: unknown,
  ) => Promise<unknown>;
  readonly createCdp?: (
    session: BrowserRunInteractiveCdpSession,
  ) => Promise<BrowserRunInteractiveCdpSession>;
  readonly remoteClose?: (target: "page" | "context" | "browser" | "cdp") => Promise<void>;
}

interface Fixture {
  readonly binding: BrowserRunInteractiveBinding["Service"];
  readonly browser: BrowserRunInteractiveBrowser;
  readonly calls: Array<string>;
  readonly keepAliveMillis: Array<number>;
  readonly controls: FixtureControls;
}

interface Gate<A> {
  readonly promise: Promise<A>;
  readonly started: Promise<void>;
  readonly markStarted: () => void;
  readonly resolve: (value: A) => void;
}

const makeGate = <A>(): Gate<A> => {
  let resolveValue: ((value: A | PromiseLike<A>) => void) | undefined;
  let resolveStarted: (() => void) | undefined;
  const promise = new Promise<A>((resolve) => {
    resolveValue = resolve;
  });
  const started = new Promise<void>((resolve) => {
    resolveStarted = resolve;
  });
  return {
    promise,
    started,
    markStarted: () => {
      if (resolveStarted === undefined) throw new Error("The started gate was not initialized");
      resolveStarted();
    },
    resolve: (value) => {
      if (resolveValue === undefined) throw new Error("The value gate was not initialized");
      resolveValue(value);
    },
  };
};

const makeFixture = (options: FixtureOptions = {}): Fixture => {
  const calls: Array<string> = [];
  const keepAliveMillis: Array<number> = [];
  let currentUrl: unknown = options.initialUrl ?? "https://example.com/";
  let requestListener: BrowserRunInteractiveRequestListener | undefined;
  let disconnectListener: (() => void) | undefined;

  const close = (target: CloseTarget): void => {
    calls.push(`${target}.close`);
    if (options.closeErrors?.has(target) === true) throw new Error(`private-${target}-failure`);
  };

  const controls: FixtureControls = {
    emitRequest: (url, emissionOptions = {}) => {
      if (requestListener === undefined) throw new Error("No request listener is installed");
      const settle = (): Promise<void> => {
        if (emissionOptions.settlement === "throw") {
          throw new Error("private-request-resolution-failure");
        }
        return emissionOptions.settlement === "reject"
          ? Promise.reject(new Error("private-request-resolution-failure"))
          : Promise.resolve();
      };
      const request: BrowserRunInteractiveRequest = {
        url: () => {
          if (emissionOptions.urlThrows === true) throw new Error("private-url-failure");
          return url;
        },
        abort: () => {
          calls.push(`request.abort:${url}`);
          return settle();
        },
        continue: () => {
          calls.push(`request.continue:${url}`);
          return settle();
        },
      };
      requestListener(request);
    },
    setUrl: (url) => {
      currentUrl = url;
    },
    disconnect: () => {
      disconnectListener?.();
    },
  };

  const page: BrowserRunInteractivePage = {
    close: async () => {
      close("page");
      await options.remoteClose?.("page");
    },
    setBypassServiceWorker: async (enabled) => {
      calls.push(`page.bypass:${String(enabled)}`);
      if (options.setupError !== undefined) throw options.setupError;
    },
    setRequestInterception: async (enabled) => {
      calls.push(`page.interception:${String(enabled)}`);
      if (options.setupError !== undefined) throw options.setupError;
      if (options.interceptionError !== undefined) throw options.interceptionError;
      await options.enableInterception?.(controls);
    },
    onRequest: (listener) => {
      calls.push("request-listener.add");
      requestListener = listener;
    },
    offRequest: (listener) => {
      calls.push("request-listener.close");
      if (options.closeErrors?.has("request-listener") === true) {
        throw new Error("private-request-listener-failure");
      }
      if (requestListener === listener) requestListener = undefined;
    },
    goto: async (url) => {
      calls.push(`page.goto:${url}`);
      currentUrl = url;
      await options.goto?.(url, controls);
    },
    url: () => {
      if (options.urlError !== undefined) throw options.urlError;
      return currentUrl;
    },
    readText: async (selector, maximumBytes) => {
      calls.push(`page.readText:${selector ?? "<body>"}:${String(maximumBytes)}`);
      return options.readText === undefined
        ? { _tag: "Text", text: "Example Domain" }
        : await options.readText(selector, maximumBytes);
    },
    fill: async (selector, value) => {
      calls.push(`page.fill:${selector}:${value}`);
      await options.fill?.(selector, value);
    },
    click: async (selector) => {
      calls.push(`page.click:${selector}`);
      await options.click?.(selector);
    },
    screenshot: async (fullPage) => {
      calls.push(`page.screenshot:${String(fullPage)}`);
      return options.screenshot === undefined
        ? new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
        : await options.screenshot(fullPage);
    },
    scroll: async (deltaX, deltaY) => {
      calls.push(`page.scroll:${String(deltaX)}:${String(deltaY)}`);
      await options.scroll?.(deltaX, deltaY);
    },
    createCdpSession: async () => {
      calls.push("page.createCdpSession");
      const session: BrowserRunInteractiveCdpSession = {
        send: async (command, parameters) => {
          calls.push(`cdp.send:${command}`);
          if (options.cdpSend !== undefined) return options.cdpSend(command, parameters);
          if (command === "Cloudflare.getLiveView") {
            return {
              devtoolsFrontendUrl:
                "https://live.browser.run/ui/view?mode=tab&wss=live.browser.run/api/devtools/browser/session/page/target?jwt=secret",
            };
          }
          if (command === "Cloudflare.handoff") return { handoffId: "handoff-id" };
          return { active: true, handoffId: "handoff-id", durationMs: 10 };
        },
        detach: async () => {
          close("cdp");
          await options.remoteClose?.("cdp");
        },
      };
      return options.createCdp === undefined ? session : options.createCdp(session);
    },
    setViewport: async (viewport) => {
      calls.push("page.setViewport");
      await options.setViewport?.(viewport);
    },
  };

  const context: BrowserRunInteractiveContext = {
    newPage: async () => {
      calls.push("context.newPage");
      if (options.newPageError !== undefined) throw options.newPageError;
      return options.newPage === undefined ? page : options.newPage(page);
    },
    close: async () => {
      close("context");
      await options.remoteClose?.("context");
    },
  };

  const browser: BrowserRunInteractiveBrowser = {
    createContext: async () => {
      calls.push("browser.createContext");
      if (options.createContextError !== undefined) throw options.createContextError;
      return options.createContext === undefined ? context : options.createContext(context);
    },
    close: async () => {
      close("browser");
      await options.remoteClose?.("browser");
    },
    sessionId: () => options.sessionId ?? "session-id",
    isConnected: () => {
      if (options.connectionStateError !== undefined) throw options.connectionStateError;
      return options.connected !== false;
    },
    onDisconnected: (listener) => {
      calls.push("disconnect-listener.add");
      disconnectListener = listener;
    },
    offDisconnected: (listener) => {
      calls.push("disconnect-listener.close");
      if (options.closeErrors?.has("disconnect-listener") === true) {
        throw new Error("private-disconnect-listener-failure");
      }
      if (disconnectListener === listener) disconnectListener = undefined;
    },
  };

  const binding: BrowserRunInteractiveBinding["Service"] = {
    launch: async (keepAlive) => {
      calls.push("binding.launch");
      keepAliveMillis.push(keepAlive);
      if (options.launchError !== undefined) throw options.launchError;
      return options.launch === undefined ? browser : options.launch(browser);
    },
    connect: async (sessionId) => {
      calls.push(`binding.connect:${sessionId}`);
      if (options.connectError !== undefined) throw options.connectError;
      return options.connect === undefined ? browser : options.connect(sessionId, browser);
    },
  };

  return { binding, browser, calls, keepAliveMillis, controls };
};

const policy = (
  overrides: Partial<{
    readonly network: InteractiveBrowserNetworkPolicy;
    readonly allowedHosts: ReadonlyArray<string>;
    readonly maxActions: number;
    readonly maxElapsedMillis: number;
    readonly maxReturnedBytes: number;
  }> = {},
) =>
  InteractiveBrowserPolicy.make({
    network: overrides.network ?? {
      _tag: "ExactHosts",
      allowedHosts: overrides.allowedHosts ?? ["example.com"],
    },
    maxActions: overrides.maxActions ?? 8,
    maxElapsedMillis: overrides.maxElapsedMillis ?? 5_000,
    maxReturnedBytes: overrides.maxReturnedBytes ?? 1_024,
  });

const navigate = (url = "https://example.com/page") => BrowserNavigateRequest.make({ url });
const supportedNetworks = [
  { _tag: "ExactHosts", allowedHosts: ["example.com"] },
  { _tag: "Unrestricted" },
] satisfies ReadonlyArray<InteractiveBrowserNetworkPolicy>;
const readText = (selector?: string) =>
  BrowserReadTextRequest.make(selector === undefined ? {} : { selector });
const fill = () => BrowserFillRequest.make({ selector: "#query", value: "effect" });
const click = () => BrowserClickRequest.make({ selector: "button[type=submit]" });

const fixtureLayer = (fixture: Fixture): Layer.Layer<InteractiveBrowser> =>
  browserRunInteractiveLayer().pipe(
    Layer.provide(Layer.succeed(BrowserRunInteractiveBinding)(fixture.binding)),
  );

const fixtureHostLayer = (fixture: Fixture): Layer.Layer<BrowserRunInteractiveHost> =>
  browserRunInteractiveHostLayer().pipe(
    Layer.provide(Layer.succeed(BrowserRunInteractiveBinding)(fixture.binding)),
  );

const withBrowser = <A, E, R>(
  fixture: Fixture,
  use: (handle: BrowserHandle) => Effect.Effect<A, E, R>,
  browserPolicy = policy(),
) =>
  Effect.gen(function* () {
    const browser = yield* InteractiveBrowser;
    const handle = yield* browser.open(browserPolicy);
    return yield* use(handle);
  }).pipe(Effect.scoped, Effect.provide(fixtureLayer(fixture)));

const withHost = <A, E, R>(
  fixture: Fixture,
  use: (host: BrowserRunInteractiveHost["Service"]) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    return yield* use(yield* BrowserRunInteractiveHost);
  }).pipe(Effect.scoped, Effect.provide(fixtureHostLayer(fixture)));

const closedResources = (fixture: Fixture): ReadonlyArray<string> =>
  fixture.calls.filter(
    (call) => call === "page.close" || call === "context.close" || call === "browser.close",
  );

const expectResourcesClosedOnce = (fixture: Fixture): void => {
  expect(fixture.calls.filter((call) => call === "page.close")).toHaveLength(1);
  expect(fixture.calls.filter((call) => call === "context.close")).toHaveLength(1);
  expect(fixture.calls.filter((call) => call === "browser.close")).toHaveLength(1);
  expect(closedResources(fixture)).toEqual(["page.close", "context.close", "browser.close"]);
};

const awaitPromise = <A>(promise: Promise<A>): Effect.Effect<A> => Effect.promise(() => promise);

describe("Browser Run interactive browser adapter", () => {
  it.effect(
    "resizes before and after action exhaustion without spending or restoring actions",
    () =>
      Effect.gen(function* () {
        const viewports: Array<BrowserRunViewport> = [];
        const fixture = makeFixture({
          setViewport: async (viewport) => {
            viewports.push(viewport);
          },
        });
        yield* withHost(fixture, (host) =>
          Effect.gen(function* () {
            const session = yield* host.open(policy({ maxActions: 1 }));
            yield* session.resizeViewport({ width: 1_440, height: 900 });
            yield* session.handle.navigate(navigate());
            yield* session.resizeViewport({ width: 1_024, height: 768, deviceScaleFactor: 2 });
            expect(yield* session.handle.readText(readText()).pipe(Effect.flip)).toMatchObject({
              _tag: "InteractiveBrowserLimitError",
              limit: "actions",
              observed: 2,
            });
          }),
        );
        expect(viewports).toEqual([
          { width: 1_440, height: 900, deviceScaleFactor: 1 },
          { width: 1_024, height: 768, deviceScaleFactor: 2 },
        ]);
        expectResourcesClosedOnce(fixture);
      }),
  );

  it.effect("validates mutated viewport values before provider dispatch or budget admission", () =>
    Effect.gen(function* () {
      const fixture = makeFixture();
      yield* withHost(fixture, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy({ maxActions: 1 }));
          const viewport = BrowserRunViewport.make({ width: 1_440, height: 900 });
          Reflect.set(viewport, "deviceScaleFactor", 2);
          expect(yield* session.resizeViewport(viewport).pipe(Effect.flip)).toMatchObject({
            _tag: "InteractiveBrowserPolicyDeniedError",
          });
          yield* session.handle.navigate(navigate());
        }),
      );
      expect(fixture.calls).not.toContain("page.setViewport");
      expectResourcesClosedOnce(fixture);
    }),
  );

  it.effect.each([
    "off-policy",
    "malformed-url",
    "closed",
    "disconnected",
    "elapsed",
    "scope-closed",
  ])("rejects resize on an unavailable page (%s)", (reason) =>
    Effect.gen(function* () {
      const fixture = makeFixture();
      yield* withHost(fixture, (host) =>
        Effect.gen(function* () {
          const session = yield* reason === "scope-closed"
            ? Effect.scoped(host.open(policy()))
            : host.open(policy());
          if (reason === "closed") yield* session.close;
          if (reason === "disconnected") fixture.controls.disconnect();
          if (reason === "off-policy") fixture.controls.setUrl("https://elsewhere.example/");
          if (reason === "malformed-url") fixture.controls.setUrl(123);
          if (reason === "elapsed") yield* TestClock.adjust(Duration.millis(5_000));
          expect(
            yield* session.resizeViewport({ width: 800, height: 600 }).pipe(Effect.flip),
          ).toMatchObject({
            _tag:
              reason === "off-policy"
                ? "InteractiveBrowserPolicyDeniedError"
                : reason === "malformed-url"
                  ? "InteractiveBrowserProtocolError"
                  : reason === "elapsed"
                    ? "InteractiveBrowserLimitError"
                    : "InteractiveBrowserExpiredError",
          });
        }),
      );
      expect(fixture.calls).not.toContain("page.setViewport");
      expectResourcesClosedOnce(fixture);
    }),
  );

  it.effect.each(["resize", "navigate", "handoff"])(
    "shares the fail-fast lock with %s and releases it on success",
    (operation) =>
      Effect.gen(function* () {
        const gate = makeGate<void>();
        const wait = () => {
          gate.markStarted();
          return gate.promise;
        };
        const fixture = makeFixture({
          setViewport: operation === "resize" ? wait : undefined,
          goto: operation === "navigate" ? wait : undefined,
          cdpSend:
            operation === "handoff"
              ? async () => {
                  await wait();
                  return { active: false };
                }
              : undefined,
        });
        yield* withHost(fixture, (host) =>
          Effect.gen(function* () {
            const session = yield* host.open(policy());
            const resize = session.resizeViewport({ width: 800, height: 600 });
            const pending = yield* (
              operation === "resize"
                ? resize
                : operation === "navigate"
                  ? session.handle.navigate(navigate()).pipe(Effect.asVoid)
                  : session.getHandoffState.pipe(Effect.asVoid)
            ).pipe(Effect.forkChild);
            yield* awaitPromise(gate.started);
            expect(yield* resize.pipe(Effect.flip)).toMatchObject({
              _tag: "InteractiveBrowserBusyError",
            });
            expect(yield* session.handle.readText(readText()).pipe(Effect.flip)).toMatchObject({
              _tag: "InteractiveBrowserBusyError",
            });
            gate.resolve(undefined);
            yield* Fiber.join(pending);
            yield* resize;
            yield* session.handle.readText(readText());
          }),
        );
        expectResourcesClosedOnce(fixture);
      }),
  );

  it.effect.each(["reject", "throw", "remote-closure"])(
    "returns typed provider failures for resize (%s)",
    (failure) =>
      Effect.gen(function* () {
        const cause = new Error(
          failure === "remote-closure" ? "Target closed" : "private-provider-error",
        );
        const fixture = makeFixture({
          setViewport: () => {
            if (failure === "throw") throw cause;
            return Promise.reject(cause);
          },
        });
        yield* withHost(fixture, (host) =>
          Effect.gen(function* () {
            const session = yield* host.open(policy({ maxActions: 1 }));
            const error = yield* session
              .resizeViewport({ width: 800, height: 600 })
              .pipe(Effect.flip);
            expect(error).toMatchObject({
              _tag:
                failure === "remote-closure"
                  ? "InteractiveBrowserExpiredError"
                  : "InteractiveBrowserProtocolError",
            });
            if (failure === "remote-closure") {
              expect(yield* session.handle.navigate(navigate()).pipe(Effect.flip)).toMatchObject({
                _tag: "InteractiveBrowserExpiredError",
              });
            } else {
              expect(error).toHaveProperty("cause", cause);
              expect(error.message).not.toContain("private-provider-error");
              yield* session.handle.navigate(navigate());
            }
          }),
        );
        expectResourcesClosedOnce(fixture);
      }),
  );

  it.effect.each(["timeout", "interruption", "close"])(
    "expires an in-flight resize after %s without replay",
    (ending) =>
      Effect.gen(function* () {
        const gate = makeGate<void>();
        const fixture = makeFixture({
          setViewport: () => {
            gate.markStarted();
            return gate.promise;
          },
        });
        yield* withHost(fixture, (host) =>
          Effect.gen(function* () {
            const session = yield* host.open(policy({ maxElapsedMillis: 100 }));
            const resize = session.resizeViewport({ width: 800, height: 600 });
            const pending = yield* resize.pipe(Effect.forkChild);
            yield* awaitPromise(gate.started);
            if (ending === "timeout") {
              yield* TestClock.adjust(Duration.millis(100));
              expect(yield* Fiber.join(pending).pipe(Effect.flip)).toMatchObject({
                _tag: "InteractiveBrowserLimitError",
                limit: "elapsed",
              });
            } else if (ending === "interruption") {
              yield* Fiber.interrupt(pending);
              expect(Exit.hasInterrupts(yield* Fiber.await(pending))).toBe(true);
            } else {
              yield* session.close;
            }
            gate.resolve(undefined);
            if (ending === "close") {
              expect(yield* Fiber.join(pending).pipe(Effect.flip)).toMatchObject({
                _tag: "InteractiveBrowserExpiredError",
              });
            }
            expect(yield* resize.pipe(Effect.flip)).toMatchObject({
              _tag: "InteractiveBrowserExpiredError",
            });
            expect(yield* session.handle.readText(readText()).pipe(Effect.flip)).toMatchObject({
              _tag: "InteractiveBrowserExpiredError",
            });
          }),
        );
        expect(fixture.calls.filter((call) => call === "page.setViewport")).toHaveLength(1);
        expectResourcesClosedOnce(fixture);
      }),
  );

  it("keeps native binding authority visible in the Layer requirement", () => {
    const proof: InteractiveLayerRequiresBinding = true;
    const scoped: ScopedOpenRequirement = true;
    const resize: ResizeEffect = true;
    expect(proof && scoped && resize).toBe(true);
  });

  it.effect("runs the bounded exact-host flow and closes page, context, then browser", () =>
    Effect.gen(function* () {
      const fixture = makeFixture({
        goto: async (url, controls) => {
          controls.emitRequest(url);
          controls.emitRequest("https://example.com/redirect");
          controls.emitRequest("https://example.com/app.js");
          controls.setUrl("https://example.com/final");
        },
      });

      const result = yield* withBrowser(fixture, (handle) =>
        Effect.gen(function* () {
          const navigation = yield* handle.navigate(navigate());
          const text = yield* handle.readText(readText("main"));
          const filled = yield* handle.fill(fill());
          const clicked = yield* handle.click(click());
          return { navigation, text, filled, clicked };
        }),
      );

      expect(result).toEqual({
        navigation: { url: "https://example.com/final" },
        text: { text: "Example Domain" },
        filled: { url: "https://example.com/final" },
        clicked: { url: "https://example.com/final" },
      });
      expect(fixture.keepAliveMillis).toEqual([10_000]);
      expect(fixture.calls.indexOf("page.bypass:true")).toBeLessThan(
        fixture.calls.indexOf("request-listener.add"),
      );
      expect(fixture.calls.indexOf("request-listener.add")).toBeLessThan(
        fixture.calls.indexOf("page.interception:true"),
      );
      expect(fixture.calls).toContain("request.continue:https://example.com/page");
      expect(
        fixture.calls.filter((call) => call === "request.continue:https://example.com/redirect"),
      ).toHaveLength(1);
      expect(
        fixture.calls.filter((call) => call === "request.continue:https://example.com/app.js"),
      ).toHaveLength(1);
      expectResourcesClosedOnce(fixture);
    }),
  );

  it.effect("snapshots and validates policy before acquisition", () =>
    Effect.gen(function* () {
      const browserPolicy = policy({ maxActions: 1 });
      const fixture = makeFixture();
      yield* withBrowser(
        fixture,
        (handle) =>
          Effect.gen(function* () {
            expect(
              Reflect.set(browserPolicy.network, "allowedHosts", [
                "example.com",
                "widened.invalid",
              ]),
            ).toBe(true);
            expect(Reflect.set(browserPolicy, "network", { _tag: "PublicWeb" })).toBe(true);
            expect(Reflect.set(browserPolicy, "maxActions", 100)).toBe(true);

            const denied = yield* handle
              .navigate(navigate("https://widened.invalid/"))
              .pipe(Effect.flip);
            expect(denied).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });

            yield* handle.navigate(navigate());
            const limit = yield* handle.readText(readText()).pipe(Effect.flip);
            expect(limit).toMatchObject({
              _tag: "InteractiveBrowserLimitError",
              limit: "actions",
              maximum: 1,
              observed: 2,
            });
          }),
        browserPolicy,
      );

      const malformedPolicy = policy();
      expect(Reflect.set(malformedPolicy.network, "allowedHosts", [])).toBe(true);
      const malformed = makeFixture();
      const error = yield* withBrowser(malformed, () => Effect.void, malformedPolicy).pipe(
        Effect.flip,
      );
      expect(error).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      expect(malformed.calls).not.toContain("binding.launch");
    }),
  );

  it.effect("rejects PublicWeb through both public services before any provider operation", () =>
    Effect.gen(function* () {
      const publicWeb = InteractiveBrowserPolicy.make({
        network: { _tag: "PublicWeb" },
        maxActions: 8,
        maxElapsedMillis: 120_000,
        maxReturnedBytes: 1_024,
      });
      const fixture = makeFixture({ launchError: new Error("provider must not be reached") });
      const genericError = yield* withBrowser(fixture, () => Effect.void, publicWeb).pipe(
        Effect.flip,
      );
      const hostError = yield* withHost(fixture, (host) => host.open(publicWeb)).pipe(Effect.flip);
      for (const error of [genericError, hostError]) {
        const encoded = yield* Schema.encodeEffect(InteractiveBrowserError)(error);
        expect(encoded).toEqual({
          _tag: "InteractiveBrowserUnsupportedError",
          implementation: {
            isolation: "isolated",
            identity: "cloudflare-browser-run-interactive",
          },
          feature: "policy",
          message:
            "Cloudflare Browser Run cannot enforce the PublicWeb network policy for all session traffic",
        });
      }
      expect(fixture.calls).toEqual([]);
    }),
  );

  it.effect("installs the request listener before interception can emit a request", () =>
    Effect.gen(function* () {
      const fixture = makeFixture({
        enableInterception: async (controls) => {
          controls.emitRequest("https://example.com/interception-start");
        },
      });
      yield* withBrowser(fixture, () => Effect.void);

      expect(fixture.calls).toContain("request.continue:https://example.com/interception-start");
      expect(fixture.calls.indexOf("request-listener.add")).toBeLessThan(
        fixture.calls.indexOf("page.interception:true"),
      );
    }),
  );

  it.effect(
    "rejects initial navigation, redirects, and subrequests outside the exact host set",
    () =>
      Effect.gen(function* () {
        const initial = makeFixture();
        const initialError = yield* withBrowser(initial, (handle) =>
          handle.navigate(navigate("https://example.com.evil.invalid/")),
        ).pipe(Effect.flip);
        expect(initialError).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
        expect(initial.calls.some((call) => call.startsWith("page.goto:"))).toBe(false);

        for (const deniedUrl of [
          "http://example.com/",
          "https://redirect.invalid/final",
          "https://cdn.invalid/script.js",
          "not a valid URL",
        ]) {
          const intercepted = makeFixture({
            goto: async (_url, controls) => {
              controls.emitRequest(deniedUrl);
              controls.setUrl("https://example.com/final");
            },
          });
          const error = yield* withBrowser(intercepted, (handle) =>
            handle.navigate(navigate()),
          ).pipe(Effect.flip);
          expect(error).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
          expect(intercepted.calls).toContain(`request.abort:${deniedUrl}`);
          expect(intercepted.calls).not.toContain(`request.continue:${deniedUrl}`);
        }

        const port = makeFixture();
        const portError = yield* withBrowser(
          port,
          (handle) => handle.navigate(navigate("https://example.com/")),
          policy({ allowedHosts: ["example.com:8443"] }),
        ).pipe(Effect.flip);
        expect(portError).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      }),
  );

  it.effect("fails typed when an intercepted request cannot be settled", () =>
    Effect.gen(function* () {
      for (const settlement of ["throw", "reject"] satisfies ReadonlyArray<
        RequestEmissionOptions["settlement"]
      >) {
        const fixture = makeFixture({
          goto: async (url, controls) => {
            controls.emitRequest(url, { settlement });
          },
        });
        const error = yield* withBrowser(fixture, (handle) => handle.navigate(navigate())).pipe(
          Effect.flip,
        );
        expect(error).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });
      }
    }),
  );

  it.effect.each(supportedNetworks)(
    "rejects concurrent operations immediately without queueing or consuming an action ($_tag)",
    (network) =>
      Effect.gen(function* () {
        const gate = makeGate<void>();
        const fixture = makeFixture({
          readText: async () => {
            gate.markStarted();
            await gate.promise;
            return { _tag: "Text", text: "done" };
          },
        });

        yield* withBrowser(
          fixture,
          (handle) =>
            Effect.gen(function* () {
              const reading = yield* handle.readText(readText()).pipe(Effect.forkChild);
              yield* awaitPromise(gate.started);
              const busy = yield* handle.click(click()).pipe(Effect.flip);
              expect(busy).toMatchObject({ _tag: "InteractiveBrowserBusyError" });
              expect(fixture.calls.some((call) => call.startsWith("page.click:"))).toBe(false);

              gate.resolve(undefined);
              expect((yield* Fiber.join(reading)).text).toBe("done");
              const limit = yield* handle.navigate(navigate()).pipe(Effect.flip);
              expect(limit).toMatchObject({
                _tag: "InteractiveBrowserLimitError",
                limit: "actions",
                maximum: 1,
                observed: 2,
              });
            }),
          policy({ network, maxActions: 1 }),
        );
      }),
  );

  it.effect.each(supportedNetworks)(
    "bounds text before it crosses the page boundary and rejects malformed observations ($_tag)",
    (network) =>
      Effect.gen(function* () {
        const overLimit = makeFixture({
          readText: async () => ({ _tag: "OverLimit", observed: 9 }),
        });
        const limit = yield* withBrowser(
          overLimit,
          (handle) => handle.readText(readText()),
          policy({ network, maxReturnedBytes: 8 }),
        ).pipe(Effect.flip);
        expect(limit).toMatchObject({
          _tag: "InteractiveBrowserLimitError",
          limit: "returned-bytes",
          maximum: 8,
          observed: 9,
        });

        const multibyte = makeFixture({
          readText: async () => ({ _tag: "Text", text: "éé" }),
        });
        const byteLimit = yield* withBrowser(
          multibyte,
          (handle) => handle.readText(readText()),
          policy({ network, maxReturnedBytes: 3 }),
        ).pipe(Effect.flip);
        expect(byteLimit).toMatchObject({
          _tag: "InteractiveBrowserLimitError",
          limit: "returned-bytes",
          observed: 4,
        });

        for (const observation of [
          { _tag: "Text", text: 42 },
          { _tag: "OverLimit", observed: -1 },
          { _tag: "Unknown" },
        ]) {
          const malformed = makeFixture({ readText: async () => observation });
          const error = yield* withBrowser(
            malformed,
            (handle) => handle.readText(readText()),
            policy({ network }),
          ).pipe(Effect.flip);
          expect(error).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });
        }
      }),
  );

  it.effect.each(supportedNetworks)(
    "enforces elapsed time in flight and expires an uncertain handle ($_tag)",
    (network) =>
      Effect.gen(function* () {
        const gate = makeGate<void>();
        const fixture = makeFixture({
          goto: async () => {
            gate.markStarted();
            await gate.promise;
          },
        });

        yield* withBrowser(
          fixture,
          (handle) =>
            Effect.gen(function* () {
              const navigating = yield* handle.navigate(navigate()).pipe(Effect.forkChild);
              yield* awaitPromise(gate.started);
              yield* TestClock.adjust(Duration.millis(100));
              const elapsed = yield* Fiber.join(navigating).pipe(Effect.flip);
              expect(elapsed).toMatchObject({
                _tag: "InteractiveBrowserLimitError",
                limit: "elapsed",
                maximum: 100,
                observed: 100,
              });
              gate.resolve(undefined);
              const expired = yield* handle.readText(readText()).pipe(Effect.flip);
              expect(expired).toMatchObject({ _tag: "InteractiveBrowserExpiredError" });
            }),
          policy({ network, maxElapsedMillis: 100 }),
        );
        expectResourcesClosedOnce(fixture);
      }),
  );

  it.effect("closes every remote resource that arrives after acquisition has timed out", () =>
    Effect.gen(function* () {
      const browserGate = makeGate<BrowserRunInteractiveBrowser>();
      const browserFixture = makeFixture({
        launch: async () => {
          browserGate.markStarted();
          return browserGate.promise;
        },
      });
      const opening = yield* withBrowser(
        browserFixture,
        () => Effect.void,
        policy({ maxElapsedMillis: 100 }),
      ).pipe(Effect.forkChild);
      yield* awaitPromise(browserGate.started);
      yield* TestClock.adjust(Duration.millis(100));
      const elapsed = yield* Fiber.join(opening).pipe(Effect.flip);
      expect(elapsed).toMatchObject({
        _tag: "InteractiveBrowserLimitError",
        limit: "elapsed",
      });

      browserGate.resolve(browserFixture.browser);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      expect(closedResources(browserFixture)).toEqual(["browser.close"]);

      const contextGate = makeGate<void>();
      const contextFixture = makeFixture({
        createContext: async (context) => {
          contextGate.markStarted();
          await contextGate.promise;
          return context;
        },
      });
      const creatingContext = yield* withBrowser(
        contextFixture,
        () => Effect.void,
        policy({ maxElapsedMillis: 100 }),
      ).pipe(Effect.forkChild);
      yield* awaitPromise(contextGate.started);
      yield* TestClock.adjust(Duration.millis(100));
      expect(yield* Fiber.join(creatingContext).pipe(Effect.flip)).toMatchObject({
        _tag: "InteractiveBrowserLimitError",
        limit: "elapsed",
      });
      contextGate.resolve(undefined);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      expect(contextFixture.calls.filter((call) => call === "context.close")).toHaveLength(1);
      expect(contextFixture.calls.filter((call) => call === "browser.close")).toHaveLength(1);

      const pageGate = makeGate<void>();
      const pageFixture = makeFixture({
        newPage: async (page) => {
          pageGate.markStarted();
          await pageGate.promise;
          return page;
        },
      });
      const creatingPage = yield* withBrowser(
        pageFixture,
        () => Effect.void,
        policy({ maxElapsedMillis: 100 }),
      ).pipe(Effect.forkChild);
      yield* awaitPromise(pageGate.started);
      yield* TestClock.adjust(Duration.millis(100));
      expect(yield* Fiber.join(creatingPage).pipe(Effect.flip)).toMatchObject({
        _tag: "InteractiveBrowserLimitError",
        limit: "elapsed",
      });
      pageGate.resolve(undefined);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      expect(pageFixture.calls.filter((call) => call === "page.close")).toHaveLength(1);
      expect(pageFixture.calls.filter((call) => call === "context.close")).toHaveLength(1);
      expect(pageFixture.calls.filter((call) => call === "browser.close")).toHaveLength(1);
    }),
  );

  it.effect("unwinds each partially acquired open exactly once", () =>
    Effect.gen(function* () {
      const contextFailure = makeFixture({ createContextError: new Error("context failed") });
      expect(yield* withBrowser(contextFailure, () => Effect.void).pipe(Effect.flip)).toMatchObject(
        { _tag: "InteractiveBrowserProtocolError" },
      );
      expect(closedResources(contextFailure)).toEqual(["browser.close"]);

      const pageFailure = makeFixture({ newPageError: new Error("page failed") });
      expect(yield* withBrowser(pageFailure, () => Effect.void).pipe(Effect.flip)).toMatchObject({
        _tag: "InteractiveBrowserProtocolError",
      });
      expect(closedResources(pageFailure)).toEqual(["context.close", "browser.close"]);

      const bypassFailure = makeFixture({ setupError: new Error("bypass failed") });
      expect(yield* withBrowser(bypassFailure, () => Effect.void).pipe(Effect.flip)).toMatchObject({
        _tag: "InteractiveBrowserProtocolError",
      });
      expectResourcesClosedOnce(bypassFailure);

      const interceptionFailure = makeFixture({
        interceptionError: new Error("interception failed"),
      });
      expect(
        yield* withBrowser(interceptionFailure, () => Effect.void).pipe(Effect.flip),
      ).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });
      expect(interceptionFailure.calls).toContain("request-listener.close");
      expectResourcesClosedOnce(interceptionFailure);
    }),
  );

  it.effect("classifies capacity, launch protocol, and disconnected sessions distinctly", () =>
    Effect.gen(function* () {
      const capacity = makeFixture({ launchError: new Error("429 browser time limit") });
      const capacityError = yield* withBrowser(capacity, () => Effect.void).pipe(Effect.flip);
      expect(capacityError).toMatchObject({ _tag: "InteractiveBrowserCapacityError" });

      const protocol = makeFixture({ launchError: new Error("bad handshake") });
      const protocolError = yield* withBrowser(protocol, () => Effect.void).pipe(Effect.flip);
      expect(protocolError).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });

      const unreadableConnection = makeFixture({
        connectionStateError: new Error("bad connection state"),
      });
      const connectionError = yield* withBrowser(unreadableConnection, () => Effect.void).pipe(
        Effect.flip,
      );
      expect(connectionError).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });
      expect(closedResources(unreadableConnection)).toEqual(["browser.close"]);

      const disconnected = makeFixture({ connected: false });
      const expired = yield* withBrowser(disconnected, () => Effect.void).pipe(Effect.flip);
      expect(expired).toMatchObject({ _tag: "InteractiveBrowserExpiredError" });
      expect(closedResources(disconnected)).toEqual(["browser.close"]);
    }),
  );

  it.effect("maps every remote operation rejection without retrying", () =>
    Effect.gen(function* () {
      const closed = makeFixture({
        goto: async () => {
          throw new Error("Target closed");
        },
      });
      const expired = yield* withBrowser(closed, (handle) =>
        Effect.gen(function* () {
          const first = yield* handle.navigate(navigate()).pipe(Effect.flip);
          const second = yield* handle.navigate(navigate()).pipe(Effect.flip);
          return { first, second };
        }),
      );
      expect(expired.first).toMatchObject({ _tag: "InteractiveBrowserExpiredError" });
      expect(expired.second).toMatchObject({ _tag: "InteractiveBrowserExpiredError" });
      expect(closed.calls.filter((call) => call.startsWith("page.goto:")).length).toBe(1);

      const navigation = makeFixture({
        goto: async () => {
          throw new Error("provider rejected navigation");
        },
      });
      const navigationError = yield* withBrowser(navigation, (handle) =>
        handle.navigate(navigate()),
      ).pipe(Effect.flip);
      expect(navigationError).toMatchObject({
        _tag: "InteractiveBrowserActionError",
        operation: "navigate",
      });
      expect(navigation.calls.filter((call) => call.startsWith("page.goto:")).length).toBe(1);

      const reading = makeFixture({
        readText: async () => {
          throw new Error("provider rejected read");
        },
      });
      const readError = yield* withBrowser(reading, (handle) => handle.readText(readText())).pipe(
        Effect.flip,
      );
      expect(readError).toMatchObject({
        _tag: "InteractiveBrowserActionError",
        operation: "read-text",
      });
      expect(reading.calls.filter((call) => call.startsWith("page.readText:")).length).toBe(1);

      const filling = makeFixture({
        fill: async () => {
          throw new Error("provider rejected fill");
        },
      });
      const fillError = yield* withBrowser(filling, (handle) => handle.fill(fill())).pipe(
        Effect.flip,
      );
      expect(fillError).toMatchObject({
        _tag: "InteractiveBrowserActionError",
        operation: "fill",
      });
      expect(filling.calls.filter((call) => call.startsWith("page.fill:")).length).toBe(1);

      const clicking = makeFixture({
        click: async () => {
          throw new Error("provider rejected click");
        },
      });
      const clickError = yield* withBrowser(clicking, (handle) => handle.click(click())).pipe(
        Effect.flip,
      );
      expect(clickError).toMatchObject({
        _tag: "InteractiveBrowserActionError",
        operation: "click",
      });
      expect(clicking.calls.filter((call) => call.startsWith("page.click:")).length).toBe(1);
    }),
  );

  it.effect("validates every post-action URL as malformed or off-policy", () =>
    Effect.gen(function* () {
      const malformed = makeFixture({ initialUrl: 42 });
      const malformedError = yield* withBrowser(malformed, (handle) => handle.click(click())).pipe(
        Effect.flip,
      );
      expect(malformedError).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });

      const unreadable = makeFixture({ urlError: new Error("url failed") });
      const unreadableError = yield* withBrowser(unreadable, (handle) =>
        handle.click(click()),
      ).pipe(Effect.flip);
      expect(unreadableError).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });

      const denied = makeFixture({ initialUrl: "https://off-policy.invalid/" });
      const deniedError = yield* withBrowser(denied, (handle) => handle.fill(fill())).pipe(
        Effect.flip,
      );
      expect(deniedError).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
    }),
  );

  it.effect.each(supportedNetworks)(
    "finalizes acquired resources after success, failure, defect, timeout, and interruption ($_tag)",
    (network) =>
      Effect.gen(function* () {
        const browserPolicy = policy({ network });
        const success = makeFixture();
        yield* withBrowser(success, () => Effect.void, browserPolicy);
        expectResourcesClosedOnce(success);

        const failure = makeFixture({
          goto: async () => {
            throw new Error("navigation failed");
          },
        });
        const failureExit = yield* withBrowser(
          failure,
          (handle) => handle.navigate(navigate("https://denied.invalid/")),
          browserPolicy,
        ).pipe(Effect.exit);
        expect(Exit.isFailure(failureExit)).toBe(true);
        expectResourcesClosedOnce(failure);

        const defect = makeFixture();
        const defectExit = yield* withBrowser(
          defect,
          () => Effect.die("test defect"),
          browserPolicy,
        ).pipe(Effect.exit);
        expect(Exit.isFailure(defectExit)).toBe(true);
        expectResourcesClosedOnce(defect);

        const timeoutReady = makeGate<void>();
        const timeout = makeFixture();
        const timeoutFiber = yield* withBrowser(
          timeout,
          () => Effect.sync(timeoutReady.markStarted).pipe(Effect.andThen(Effect.never)),
          browserPolicy,
        ).pipe(Effect.timeout(Duration.millis(100)), Effect.forkChild);
        yield* awaitPromise(timeoutReady.started);
        yield* TestClock.adjust(Duration.millis(100));
        yield* Fiber.await(timeoutFiber);
        expectResourcesClosedOnce(timeout);

        const interruptReady = makeGate<void>();
        const interrupted = makeFixture();
        const interruptedFiber = yield* withBrowser(
          interrupted,
          () => Effect.sync(interruptReady.markStarted).pipe(Effect.andThen(Effect.never)),
          browserPolicy,
        ).pipe(Effect.forkChild);
        yield* awaitPromise(interruptReady.started);
        yield* Fiber.interrupt(interruptedFiber);
        expectResourcesClosedOnce(interrupted);
      }),
  );

  it.effect("expires a handle before it can be used outside its owning Scope", () =>
    Effect.gen(function* () {
      const fixture = makeFixture();
      const escaped = yield* withBrowser(fixture, Effect.succeed);

      expectResourcesClosedOnce(fixture);
      const error = yield* escaped.click(click()).pipe(Effect.flip);
      expect(error).toMatchObject({ _tag: "InteractiveBrowserExpiredError" });
      expect(fixture.calls.some((call) => call.startsWith("page.click:"))).toBe(false);
    }),
  );

  it.effect("warns with fixed cleanup messages without changing the primary typed failure", () =>
    Effect.gen(function* () {
      const logs: Array<string> = [];
      const logger = Logger.make<unknown, void>(({ cause, message }) => {
        logs.push(`${String(message)} ${String(cause)}`);
      });
      const fixture = makeFixture({
        closeErrors: new Set<CloseTarget>([
          "page",
          "context",
          "browser",
          "request-listener",
          "disconnect-listener",
        ]),
      });
      const error = yield* withBrowser(fixture, (handle) =>
        handle.navigate(navigate("https://denied.invalid/")),
      ).pipe(Effect.flip, Effect.provide(Logger.layer([logger])));

      expect(error).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      expect(closedResources(fixture)).toEqual(["page.close", "context.close", "browser.close"]);
      expect(logs.join("\n")).toContain("Removing the browser request policy failed");
      expect(logs.join("\n")).toContain("Closing the interactive browser page failed");
      expect(logs.join("\n")).toContain("Closing the interactive browser context failed");
      expect(logs.join("\n")).toContain("Removing the browser disconnect listener failed");
      expect(logs.join("\n")).toContain("Closing the interactive browser failed");
      expect(logs.join("\n")).not.toContain("private-");
    }),
  );

  it.effect("screenshots and scrolls the same page within shared action and byte limits", () =>
    Effect.gen(function* () {
      const providerBytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x01]);
      const fixture = makeFixture({ screenshot: async () => providerBytes });
      const result = yield* withBrowser(
        fixture,
        (handle) =>
          Effect.gen(function* () {
            const scrolled = yield* handle.scroll(
              BrowserScrollRequest.make({ deltaX: -10, deltaY: 250 }),
            );
            const screenshot = yield* handle.screenshot(
              BrowserScreenshotRequest.make({ fullPage: false }),
            );
            const limit = yield* handle.readText(readText()).pipe(Effect.flip);
            return { limit, screenshot, scrolled };
          }),
        policy({ maxActions: 2, maxReturnedBytes: 9 }),
      );

      expect(result.scrolled).toEqual({ url: "https://example.com/" });
      expect(result.screenshot.mediaType).toBe("image/png");
      expect(result.screenshot.bytes).toEqual(providerBytes);
      expect(result.screenshot.bytes).not.toBe(providerBytes);
      expect(result.limit).toMatchObject({
        _tag: "InteractiveBrowserLimitError",
        limit: "actions",
        observed: 3,
      });
      expect(fixture.calls).toContain("page.scroll:-10:250");
      expect(fixture.calls).toContain("page.screenshot:false");
    }),
  );

  it.effect("rejects malformed screenshot input, PNG output, and per-result overflow", () =>
    Effect.gen(function* () {
      const malformedRequest = BrowserScreenshotRequest.make({ fullPage: false });
      expect(Reflect.set(malformedRequest, "fullPage", "yes")).toBe(true);
      const inputFixture = makeFixture();
      const inputError = yield* withBrowser(inputFixture, (handle) =>
        handle.screenshot(malformedRequest),
      ).pipe(Effect.flip);
      expect(inputError).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      expect(inputFixture.calls.some((call) => call.startsWith("page.screenshot:"))).toBe(false);

      const malformedPng = makeFixture({ screenshot: async () => new Uint8Array([1, 2, 3]) });
      const protocol = yield* withBrowser(malformedPng, (handle) =>
        handle.screenshot(BrowserScreenshotRequest.make({ fullPage: true })),
      ).pipe(Effect.flip);
      expect(protocol).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });
      expect(protocol).not.toHaveProperty("cause");
      expect(String(protocol)).not.toContain("1,2,3");

      const overflow = makeFixture({
        screenshot: async () =>
          new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x01]),
      });
      const limit = yield* withBrowser(
        overflow,
        (handle) => handle.screenshot(BrowserScreenshotRequest.make({ fullPage: true })),
        policy({ maxReturnedBytes: 8 }),
      ).pipe(Effect.flip);
      expect(limit).toMatchObject({
        _tag: "InteractiveBrowserLimitError",
        limit: "returned-bytes",
        maximum: 8,
        observed: 9,
      });
    }),
  );

  it.effect("invalidates immediately and tears down once during an in-flight action", () =>
    Effect.gen(function* () {
      const gate = makeGate<void>();
      const fixture = makeFixture({
        scroll: async () => {
          gate.markStarted();
          await gate.promise;
        },
      });

      yield* withBrowser(fixture, (handle) =>
        Effect.gen(function* () {
          const scrolling = yield* handle
            .scroll(BrowserScrollRequest.make({ deltaX: 0, deltaY: 1 }))
            .pipe(Effect.forkChild);
          yield* awaitPromise(gate.started);
          yield* handle.close;
          yield* handle.close;
          gate.resolve(undefined);
          const stale = yield* Fiber.join(scrolling).pipe(Effect.flip);
          expect(stale).toMatchObject({ _tag: "InteractiveBrowserExpiredError" });
          const closed = yield* handle.readText(readText()).pipe(Effect.flip);
          expect(closed).toMatchObject({ _tag: "InteractiveBrowserExpiredError" });
        }),
      );
      expectResourcesClosedOnce(fixture);
    }),
  );

  it.effect(
    "explicitly closes after an interrupted action while the owning Scope remains live",
    () =>
      Effect.gen(function* () {
        const gate = makeGate<void>();
        const fixture = makeFixture({
          scroll: async () => {
            gate.markStarted();
            await gate.promise;
          },
        });

        yield* withBrowser(fixture, (handle) =>
          Effect.gen(function* () {
            const scrolling = yield* handle
              .scroll(BrowserScrollRequest.make({ deltaX: 0, deltaY: 1 }))
              .pipe(Effect.forkChild);
            yield* awaitPromise(gate.started);
            yield* Fiber.interrupt(scrolling);
            expect(closedResources(fixture)).toEqual([]);
            yield* handle.close;
            expectResourcesClosedOnce(fixture);
            gate.resolve(undefined);
            yield* Effect.yieldNow;
          }),
        );
        expectResourcesClosedOnce(fixture);
      }),
  );

  it.effect("bounds each explicit cleanup step and continues reverse-order teardown", () =>
    Effect.gen(function* () {
      const pageClose = makeGate<void>();
      const fixture = makeFixture({
        remoteClose: async (target) => {
          if (target === "page") {
            pageClose.markStarted();
            await pageClose.promise;
          }
        },
      });

      yield* withBrowser(fixture, (handle) =>
        Effect.gen(function* () {
          const closing = yield* handle.close.pipe(Effect.forkChild);
          yield* awaitPromise(pageClose.started);
          yield* TestClock.adjust(Duration.seconds(10));
          const error = yield* Fiber.join(closing).pipe(Effect.flip);
          expect(error).toMatchObject({
            _tag: "InteractiveBrowserActionError",
            operation: "close",
          });
        }),
      );
      expectResourcesClosedOnce(fixture);
    }),
  );

  it.effect.each([
    {
      network: {
        _tag: "ExactHosts",
        allowedHosts: ["example.com", "example.org", "cdn.example.net"],
      },
      destination: "https://example.org/",
      resource: "https://cdn.example.net/app.js",
    },
    {
      network: { _tag: "Unrestricted" },
      destination: "http://127.0.0.1:8080/",
      resource: "http://169.254.169.254/resource",
    },
  ] satisfies ReadonlyArray<{
    network: InteractiveBrowserNetworkPolicy;
    destination: string;
    resource: string;
  }>)(
    "keeps navigation, resources, and human handoff on the same page ($network._tag)",
    ({ network, destination, resource }) =>
      Effect.gen(function* () {
        const parameters: Array<unknown> = [];
        const fixture = makeFixture({
          goto: async (url, controls) => {
            controls.emitRequest(url);
            controls.emitRequest(resource);
            controls.emitRequest(destination);
            controls.setUrl(destination);
          },
          cdpSend: async (command, input) => {
            parameters.push(input);
            if (command === "Cloudflare.getLiveView") {
              return {
                devtoolsFrontendUrl:
                  "https://live.browser.run/ui/view?mode=tab&wss=live.browser.run/api/devtools/browser/session/page/target?jwt=secret",
              };
            }
            if (command === "Cloudflare.handoff") return { handoffId: "private-handoff" };
            return { active: false, handoffId: "private-handoff", durationMs: 25 };
          },
        });

        yield* withHost(fixture, (host) =>
          Effect.gen(function* () {
            const session = yield* host.open(
              policy({
                network,
                maxActions: 6,
                maxElapsedMillis: 90_000,
              }),
            );
            expect(Redacted.isRedacted(session.sessionId)).toBe(true);
            expect(Redacted.value(session.sessionId)).toBe("session-id");
            expect((yield* session.handle.navigate(navigate("https://example.com/"))).url).toBe(
              destination,
            );
            yield* session.handle.navigate(navigate(destination));
            const liveView = yield* session.getLiveView(
              BrowserRunLiveViewRequest.make({ mode: "tab", expiresInMs: 60_000 }),
            );
            const handoff = yield* session.handoff(
              BrowserRunHandoffRequest.make({ instructions: "Complete MFA", timeout: 1_000 }),
            );
            fixture.controls.emitRequest(`${destination}after-human`);
            fixture.controls.setUrl(`${destination}after-human`);
            const state = yield* session.getHandoffState;
            expect(Redacted.isRedacted(liveView.devtoolsFrontendUrl)).toBe(true);
            expect(Redacted.value(liveView.devtoolsFrontendUrl)).toContain("jwt=secret");
            expect(Redacted.value(handoff.handoffId)).toBe("private-handoff");
            expect(Redacted.value(state.handoffId!)).toBe("private-handoff");
            expect(state.active).toBe(false);
            const resumed = yield* session.handle.scroll(
              BrowserScrollRequest.make({ deltaX: 0, deltaY: 10 }),
            );
            expect(resumed.url).toBe(`${destination}after-human`);
            const limit = yield* session.handle.readText(readText()).pipe(Effect.flip);
            expect(limit).toMatchObject({
              _tag: "InteractiveBrowserLimitError",
              limit: "actions",
              observed: 7,
            });
            yield* session.close;
            expect(
              yield* session.handle.navigate(navigate(destination)).pipe(Effect.flip),
            ).toMatchObject({ _tag: "InteractiveBrowserExpiredError" });
          }),
        );

        expect(parameters).toEqual([
          { mode: "tab", expiresInMs: 60_000 },
          { instructions: "Complete MFA", timeout: 1_000 },
          {},
        ]);
        expect(fixture.calls.filter((call) => call === "cdp.close")).toHaveLength(3);
        expect(fixture.calls.filter((call) => call === "binding.launch")).toHaveLength(1);
        expect(fixture.calls.filter((call) => call === "context.newPage")).toHaveLength(1);
        expect(
          fixture.calls.filter((call) => call === `request.continue:${resource}`),
        ).toHaveLength(2);
        expectResourcesClosedOnce(fixture);
      }),
  );

  it.effect("rejects unsafe Live View responses and requests beyond the remaining pass", () =>
    Effect.gen(function* () {
      const unsafe =
        "https://operator:secret@live.browser.run/ui/view?mode=tab&wss=live.browser.run/api/devtools/browser/session/page/target";
      const fixture = makeFixture({
        cdpSend: async () => ({ devtoolsFrontendUrl: unsafe }),
      });
      const malformed = yield* withHost(fixture, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy({ maxElapsedMillis: 90_000 }));
          return yield* session
            .getLiveView(BrowserRunLiveViewRequest.make({ mode: "tab", expiresInMs: 60_000 }))
            .pipe(Effect.flip);
        }),
      );
      expect(malformed).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });
      expect(malformed).not.toHaveProperty("cause");
      expect(String(malformed)).not.toContain("operator:secret");
      expect(fixture.calls.filter((call) => call === "cdp.close")).toHaveLength(1);

      const shortExpiry = BrowserRunLiveViewRequest.make({ mode: "tab", expiresInMs: 60_000 });
      expect(Reflect.set(shortExpiry, "expiresInMs", 10_000)).toBe(true);
      const expiryFixture = makeFixture();
      const expiryError = yield* withHost(expiryFixture, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy({ maxElapsedMillis: 90_000 }));
          return yield* session.getLiveView(shortExpiry).pipe(Effect.flip);
        }),
      );
      expect(expiryError).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      expect(expiryFixture.calls.some((call) => call.startsWith("cdp.send:"))).toBe(false);

      const incompleteState = makeFixture({
        cdpSend: async () => ({ active: true }),
      });
      const stateError = yield* withHost(incompleteState, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy());
          return yield* session.getHandoffState.pipe(Effect.flip);
        }),
      );
      expect(stateError).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });
      expect(stateError).not.toHaveProperty("cause");

      const longInstructions = BrowserRunHandoffRequest.make({
        instructions: "Wait",
        timeout: 1_000,
      });
      expect(Reflect.set(longInstructions, "instructions", "x".repeat(1_025))).toBe(true);
      const instructionFixture = makeFixture();
      const instructionError = yield* withHost(instructionFixture, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy());
          return yield* session.handoff(longInstructions).pipe(Effect.flip);
        }),
      );
      expect(instructionError).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      expect(instructionFixture.calls.some((call) => call.startsWith("cdp.send:"))).toBe(false);

      const overlong = makeFixture();
      const denied = yield* withHost(overlong, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy({ maxElapsedMillis: 500 }));
          return yield* session
            .handoff(BrowserRunHandoffRequest.make({ instructions: "Wait", timeout: 1_000 }))
            .pipe(Effect.flip);
        }),
      );
      expect(denied).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      expect(overlong.calls.some((call) => call.startsWith("cdp.send:"))).toBe(false);
    }),
  );

  it.effect("uses one finite cleanup-only connection and closes late acquisition", () =>
    Effect.gen(function* () {
      const success = makeFixture();
      yield* withHost(success, (host) => host.closeSession(Redacted.make("leaked_session")));
      expect(success.calls).toContain("binding.connect:leaked_session");
      expect(success.calls.filter((call) => call === "browser.close")).toHaveLength(1);

      const gate = makeGate<BrowserRunInteractiveBrowser>();
      const late = makeFixture({
        connect: async () => {
          gate.markStarted();
          return gate.promise;
        },
      });
      yield* withHost(late, (host) =>
        Effect.gen(function* () {
          const closing = yield* host
            .closeSession(Redacted.make("late_session"))
            .pipe(Effect.forkChild);
          yield* awaitPromise(gate.started);
          yield* TestClock.adjust(Duration.seconds(10));
          expect(yield* Fiber.join(closing).pipe(Effect.flip)).toMatchObject({
            _tag: "InteractiveBrowserActionError",
            operation: "close",
          });
          gate.resolve(late.browser);
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
        }),
      );
      expect(late.calls.filter((call) => call === "browser.close")).toHaveLength(1);
      expect(late.calls.filter((call) => call.startsWith("binding.connect:"))).toHaveLength(1);
    }),
  );

  it.effect("rejects a concurrent generic action while a host control owns the pass", () =>
    Effect.gen(function* () {
      const gate = makeGate<unknown>();
      const fixture = makeFixture({
        cdpSend: async () => {
          gate.markStarted();
          return gate.promise;
        },
      });

      yield* withHost(fixture, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy());
          const querying = yield* session.getHandoffState.pipe(Effect.forkChild);
          yield* awaitPromise(gate.started);
          const busy = yield* session.handle
            .scroll(BrowserScrollRequest.make({ deltaX: 0, deltaY: 1 }))
            .pipe(Effect.flip);
          expect(busy).toMatchObject({ _tag: "InteractiveBrowserBusyError" });
          expect(fixture.calls.filter((call) => call === "page.createCdpSession")).toHaveLength(1);
          expect(fixture.calls.some((call) => call.startsWith("page.scroll:"))).toBe(false);
          gate.resolve({ active: true, handoffId: "handoff-id", durationMs: 10 });
          expect((yield* Fiber.join(querying)).active).toBe(true);
        }),
      );
      expect(fixture.calls.filter((call) => call === "cdp.close")).toHaveLength(1);
    }),
  );

  it.effect("detaches a CDP session that arrives after the pass deadline", () =>
    Effect.gen(function* () {
      const gate = makeGate<BrowserRunInteractiveCdpSession>();
      const fixture = makeFixture({
        createCdp: async () => {
          gate.markStarted();
          return gate.promise;
        },
      });

      yield* withHost(fixture, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy({ maxElapsedMillis: 100 }));
          const querying = yield* session.getHandoffState.pipe(Effect.forkChild);
          yield* awaitPromise(gate.started);
          yield* TestClock.adjust(Duration.millis(100));
          expect(yield* Fiber.join(querying).pipe(Effect.flip)).toMatchObject({
            _tag: "InteractiveBrowserLimitError",
            limit: "elapsed",
          });
          const cdp: BrowserRunInteractiveCdpSession = {
            send: async () => ({ active: false }),
            detach: async () => {
              fixture.calls.push("late-cdp.detach");
            },
          };
          gate.resolve(cdp);
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
        }),
      );
      expect(fixture.calls.filter((call) => call === "late-cdp.detach")).toHaveLength(1);
    }),
  );
});
