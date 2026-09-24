import {
  type BrowserRunViewport,
  BrowserRunInteractiveBinding,
  BrowserRunInteractiveHost,
  BrowserRunInteractiveCheckpoint,
  BrowserRunPageIdentity,
  BrowserRunHandoffRequest,
  browserRunInteractiveHostLayer,
  browserRunInteractiveLayer,
  type BrowserRunCloudflareCommand,
  type BrowserRunInteractiveBrowser,
  type BrowserRunInteractiveCdpSession,
  type BrowserRunInteractiveContext,
  type BrowserRunInteractivePage,
  type BrowserRunInteractiveRequest,
  type BrowserRunInteractiveRequestListener,
} from "@effect-agent/platform-cloudflare/interactive-browser";
import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Fiber, Layer, Redacted, Schema } from "effect";
import {
  type InteractiveBrowserError,
  BrowserClickRequest,
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  BrowserScreenshotRequest,
  BrowserScrollRequest,
  InteractiveBrowser,
  InteractiveBrowserPolicy,
  type BrowserHandle,
  type InteractiveBrowserNetworkPolicy,
} from "effect-agent/interactive-browser";
import { TestClock } from "effect/testing";

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
  readonly missingTarget?: boolean;
  readonly checkpointError?: unknown;
  readonly detachError?: unknown;
  readonly connected?: boolean;
  readonly launchError?: unknown;
  readonly createContextError?: unknown;
  readonly newPageError?: unknown;
  readonly setupError?: unknown;
  readonly interceptionError?: unknown;
  readonly connectionStateError?: unknown;
  readonly urlError?: unknown;
  readonly sessionId?: unknown;
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
  const observation = {
    before: { matchCount: 1 },
    after: { matchCount: 1 },
    afterUnavailable: false,
    network: {
      total: 0,
      status2xx: 0,
      status3xx: 0,
      status4xx: 0,
      status5xx: 0,
      failed: 0,
      pending: 0,
      settleTimedOut: false,
    },
  };

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
    identity: async () => {
      if (options.checkpointError !== undefined) throw options.checkpointError;

      return BrowserRunPageIdentity.make({ contextId: "context-id", targetId: "target-id" });
    },
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
    fill: async (selector, value, _signal, onDispatch, onComplete) => {
      calls.push(`page.fill:${selector}:${value}`);
      onDispatch();
      await options.fill?.(selector, value);
      onComplete?.();

      return observation;
    },
    click: async (selector, _signal, onDispatch, onComplete) => {
      calls.push(`page.click:${selector}`);
      onDispatch();
      await options.click?.(selector);
      onComplete?.();

      return observation;
    },
    selectFile: async (_request, _signal, onDispatch) => {
      onDispatch();

      return observation;
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
    detach: async () => {
      calls.push("browser.detach");
      if (options.detachError !== undefined) throw options.detachError;
    },
    reattach: async (identity) => {
      calls.push(`browser.reattach:${identity.contextId}:${identity.targetId}`);

      return options.missingTarget === true ? undefined : { context, page };
    },
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
    acquire: async (keepAlive) => {
      calls.push("binding.acquire");
      keepAliveMillis.push(keepAlive);
      if (options.launchError !== undefined) throw options.launchError;

      return (options.launch === undefined ? browser : await options.launch(browser)).sessionId();
    },
    connect: async () => browser,
    closeSession: (sessionId) =>
      Effect.sync(() => {
        calls.push(`binding.terminate:${Redacted.value(sessionId)}`);
      }),
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

const readText = (selector?: string) =>
  BrowserReadTextRequest.make(selector === undefined ? {} : { selector });

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

const awaitPromise = <A>(promise: Promise<A>): Effect.Effect<A> => Effect.promise(() => promise);

describe("Browser Run interactive browser adapter", () => {
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
      expect(browserFixture.calls).toContain("binding.terminate:session-id");
      expect(closedResources(browserFixture)).toEqual([]);

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

  it.effect("does not let local teardown exceed the deadline or veto confirmed termination", () =>
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
          yield* Fiber.join(closing);
          expect(fixture.calls).toContain("binding.terminate:session-id");
          pageClose.resolve(undefined);
        }),
      );
      expect(fixture.calls.filter((call) => call === "page.close")).toHaveLength(1);
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

  // https://github.com/danieljvdm/effect-agent/commit/5f83df46d392b1d61e39cb2c74d9eebf36c52415
  it.effect.each([true])(
    "resumes the same checkpoint without replay and preserves the input fence (%s)",
    (pendingInput) => {
      const fixture = makeFixture();
      let staleClose: Effect.Effect<void, InteractiveBrowserError> = Effect.void;
      let staleHandleClose: Effect.Effect<void, InteractiveBrowserError> = Effect.void;

      return Effect.gen(function* () {
        const checkpoint = yield* withHost(fixture, (host) =>
          Effect.gen(function* () {
            const session = yield* host.open(policy({ maxActions: 4 }));

            yield* session.handle.navigate(navigate());
            const checkpoint = yield* session.checkpoint;

            staleClose = session.close;
            staleHandleClose = session.handle.close;

            yield* session.detach;
            expect(yield* session.handle.click(click()).pipe(Effect.flip)).toMatchObject({
              _tag: "InteractiveBrowserExpiredError",
            });

            return checkpoint;
          }),
        );

        expect(fixture.calls.some((call) => call.startsWith("binding.terminate:"))).toBe(false);
        expect(checkpoint.consumedActions).toBe(1);
        yield* withHost(fixture, (host) =>
          Effect.gen(function* () {
            const persisted = yield* Schema.encodeEffect(BrowserRunInteractiveCheckpoint)(
              checkpoint,
            );

            const restored = yield* Schema.decodeEffect(BrowserRunInteractiveCheckpoint)(persisted);
            const session = yield* host.resume(restored, { pendingInput });

            yield* staleClose;
            yield* staleHandleClose;
            expect(fixture.calls.some((call) => call.startsWith("binding.terminate:"))).toBe(false);

            expect((yield* session.checkpoint).startedAt).toBe(checkpoint.startedAt);
            yield* session.handle.readText(readText());
            {
              expect(yield* session.drainInput).toBe("unknown");
              expect(yield* session.handle.click(click()).pipe(Effect.flip)).toMatchObject({
                _tag: "InteractiveBrowserBusyError",
              });
              expect(
                yield* session
                  .handoff(
                    BrowserRunHandoffRequest.make({ instructions: "Continue", timeout: 1_000 }),
                  )
                  .pipe(Effect.flip),
              ).toMatchObject({ _tag: "InteractiveBrowserBusyError" });
              yield* session.handle.screenshot(BrowserScreenshotRequest.make({ fullPage: false }));
            }
          }),
        );
        expect(fixture.calls.filter((call) => call === "binding.acquire")).toHaveLength(1);
        expect(fixture.calls.filter((call) => call === "context.newPage")).toHaveLength(1);
        expect(fixture.calls.filter((call) => call.startsWith("page.goto:"))).toHaveLength(1);
        expect(
          fixture.calls.filter((call) => call === "browser.reattach:context-id:target-id"),
        ).toHaveLength(1);
      });
    },
  );

  // https://github.com/danieljvdm/effect-agent/commit/5f83df46d392b1d61e39cb2c74d9eebf36c52415
  it.effect(
    "does not create or terminate a provider session when the exact checkpoint target is absent",
    () => {
      const fixture = makeFixture({ missingTarget: true });

      return Effect.gen(function* () {
        const checkpoint = yield* withHost(fixture, (host) =>
          Effect.gen(function* () {
            const session = yield* host.open(policy());
            const checkpoint = yield* session.checkpoint;

            yield* session.detach;

            return checkpoint;
          }),
        );

        const error = yield* withHost(fixture, (host) =>
          host.resume(checkpoint, { pendingInput: false }),
        ).pipe(Effect.flip);

        expect(error).toMatchObject({
          _tag: "InteractiveBrowserExpiredError",
          evidence: { session: "lost" },
        });
        expect(fixture.calls.filter((call) => call === "context.newPage")).toHaveLength(1);
        expect(fixture.calls.some((call) => call.startsWith("binding.terminate:"))).toBe(false);
      });
    },
  );
  // https://github.com/danieljvdm/effect-agent/commit/5f83df46d392b1d61e39cb2c74d9eebf36c52415
  it.effect(
    "fences unfinished SDK input after interruption while allowing reads, then drains without replay",
    () => {
      const gate = makeGate<void>();

      const fixture = makeFixture({
        click: async () => {
          gate.markStarted();
          await gate.promise;
        },
      });

      return withHost(fixture, (host) =>
        Effect.gen(function* () {
          const session = yield* host.open(policy());
          const checkpoint = yield* session.checkpoint;

          expect(
            yield* host.resume(checkpoint, { pendingInput: false }).pipe(Effect.flip),
          ).toMatchObject({ _tag: "InteractiveBrowserBusyError" });
          const action = yield* session.handle.click(click()).pipe(Effect.forkChild);

          yield* awaitPromise(gate.started);
          yield* Fiber.interrupt(action);
          expect(yield* session.inputState).toBe("running");
          yield* session.handle.readText(readText());
          expect(yield* session.handle.click(click()).pipe(Effect.flip)).toMatchObject({
            _tag: "InteractiveBrowserBusyError",
          });
          expect(
            yield* session.resizeViewport({ width: 800, height: 600 }).pipe(Effect.flip),
          ).toMatchObject({ _tag: "InteractiveBrowserBusyError" });
          gate.resolve(undefined);
          expect(yield* session.drainInput).toBe("idle");
          yield* session.handle.scroll(BrowserScrollRequest.make({ deltaX: 0, deltaY: 10 }));
          expect(fixture.calls.filter((call) => call.startsWith("page.click:"))).toHaveLength(1);
        }),
      );
    },
  );
  // https://github.com/danieljvdm/effect-agent/commit/5f83df46d392b1d61e39cb2c74d9eebf36c52415
  it.effect("keeps transport-timeout input fenced even after the local promise rejects", () => {
    const fixture = makeFixture({
      click: async () => {
        const timeout = new Error("private timeout details");

        timeout.name = "TimeoutError";
        throw timeout;
      },
    });

    return withHost(fixture, (host) =>
      Effect.gen(function* () {
        const session = yield* host.open(policy());
        const error = yield* session.handle.click(click()).pipe(Effect.flip);

        expect(error).toMatchObject({ evidence: { dispatch: "unknown", session: "attached" } });
        expect(yield* session.drainInput).toBe("unknown");
        yield* session.handle.readText(readText());
        expect(yield* session.handle.click(click()).pipe(Effect.flip)).toMatchObject({
          _tag: "InteractiveBrowserBusyError",
        });
      }),
    );
  });
});
