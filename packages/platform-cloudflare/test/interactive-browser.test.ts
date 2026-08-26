import {
  BrowserClickRequest,
  BrowserFillRequest,
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  InteractiveBrowser,
  InteractiveBrowserPolicy,
  type BrowserHandle,
} from "@effect-agent/sandbox";
import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Exit, Fiber, Layer, Logger, type Scope } from "effect";
import { TestClock } from "effect/testing";

import {
  BrowserRunInteractiveBinding,
  browserRunInteractiveLayer,
  type BrowserRunInteractiveBrowser,
  type BrowserRunInteractiveContext,
  type BrowserRunInteractivePage,
  type BrowserRunInteractiveRequest,
  type BrowserRunInteractiveRequestListener,
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

type CloseTarget = "page" | "context" | "browser" | "request-listener" | "disconnect-listener";

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
    close: async () => close("page"),
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
  };

  const context: BrowserRunInteractiveContext = {
    newPage: async () => {
      calls.push("context.newPage");
      if (options.newPageError !== undefined) throw options.newPageError;
      return options.newPage === undefined ? page : options.newPage(page);
    },
    close: async () => close("context"),
  };

  const browser: BrowserRunInteractiveBrowser = {
    createContext: async () => {
      calls.push("browser.createContext");
      if (options.createContextError !== undefined) throw options.createContextError;
      return options.createContext === undefined ? context : options.createContext(context);
    },
    close: async () => close("browser"),
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
  };

  return { binding, browser, calls, keepAliveMillis, controls };
};

const policy = (
  overrides: Partial<{
    readonly allowedHosts: ReadonlyArray<string>;
    readonly maxActions: number;
    readonly maxElapsedMillis: number;
    readonly maxReturnedBytes: number;
  }> = {},
) =>
  InteractiveBrowserPolicy.make({
    allowedHosts: overrides.allowedHosts ?? ["example.com"],
    maxActions: overrides.maxActions ?? 8,
    maxElapsedMillis: overrides.maxElapsedMillis ?? 5_000,
    maxReturnedBytes: overrides.maxReturnedBytes ?? 1_024,
  });

const navigate = (url = "https://example.com/page") => BrowserNavigateRequest.make({ url });
const readText = (selector?: string) =>
  BrowserReadTextRequest.make(selector === undefined ? {} : { selector });
const fill = () => BrowserFillRequest.make({ selector: "#query", value: "effect" });
const click = () => BrowserClickRequest.make({ selector: "button[type=submit]" });

const fixtureLayer = (fixture: Fixture): Layer.Layer<InteractiveBrowser> =>
  browserRunInteractiveLayer().pipe(
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
  it("keeps native binding authority visible in the Layer requirement", () => {
    const proof: InteractiveLayerRequiresBinding = true;
    const scoped: ScopedOpenRequirement = true;
    expect(proof && scoped).toBe(true);
  });

  it.effect("runs the bounded public flow and closes page, context, then browser", () =>
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
              Reflect.set(browserPolicy, "allowedHosts", ["example.com", "widened.invalid"]),
            ).toBe(true);
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
      expect(Reflect.set(malformedPolicy, "allowedHosts", [])).toBe(true);
      const malformed = makeFixture();
      const error = yield* withBrowser(malformed, () => Effect.void, malformedPolicy).pipe(
        Effect.flip,
      );
      expect(error).toMatchObject({ _tag: "InteractiveBrowserPolicyDeniedError" });
      expect(malformed.calls).not.toContain("binding.launch");
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

  it.effect(
    "rejects concurrent operations immediately without queueing or consuming an action",
    () =>
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
          policy({ maxActions: 1 }),
        );
      }),
  );

  it.effect(
    "bounds text before it crosses the page boundary and rejects malformed observations",
    () =>
      Effect.gen(function* () {
        const overLimit = makeFixture({
          readText: async () => ({ _tag: "OverLimit", observed: 9 }),
        });
        const limit = yield* withBrowser(
          overLimit,
          (handle) => handle.readText(readText()),
          policy({ maxReturnedBytes: 8 }),
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
          policy({ maxReturnedBytes: 3 }),
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
          const error = yield* withBrowser(malformed, (handle) => handle.readText(readText())).pipe(
            Effect.flip,
          );
          expect(error).toMatchObject({ _tag: "InteractiveBrowserProtocolError" });
        }
      }),
  );

  it.effect("enforces elapsed time in flight and expires an uncertain handle", () =>
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
        policy({ maxElapsedMillis: 100 }),
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

  it.effect(
    "finalizes acquired resources after success, failure, defect, timeout, and interruption",
    () =>
      Effect.gen(function* () {
        const success = makeFixture();
        yield* withBrowser(success, () => Effect.void);
        expectResourcesClosedOnce(success);

        const failure = makeFixture();
        const failureExit = yield* withBrowser(failure, (handle) =>
          handle.navigate(navigate("https://denied.invalid/")),
        ).pipe(Effect.exit);
        expect(Exit.isFailure(failureExit)).toBe(true);
        expectResourcesClosedOnce(failure);

        const defect = makeFixture();
        const defectExit = yield* withBrowser(defect, () => Effect.die("test defect")).pipe(
          Effect.exit,
        );
        expect(Exit.isFailure(defectExit)).toBe(true);
        expectResourcesClosedOnce(defect);

        const timeoutReady = makeGate<void>();
        const timeout = makeFixture();
        const timeoutFiber = yield* withBrowser(timeout, () =>
          Effect.sync(timeoutReady.markStarted).pipe(Effect.andThen(Effect.never)),
        ).pipe(Effect.timeout(Duration.millis(100)), Effect.forkChild);
        yield* awaitPromise(timeoutReady.started);
        yield* TestClock.adjust(Duration.millis(100));
        yield* Fiber.await(timeoutFiber);
        expectResourcesClosedOnce(timeout);

        const interruptReady = makeGate<void>();
        const interrupted = makeFixture();
        const interruptedFiber = yield* withBrowser(interrupted, () =>
          Effect.sync(interruptReady.markStarted).pipe(Effect.andThen(Effect.never)),
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
});
