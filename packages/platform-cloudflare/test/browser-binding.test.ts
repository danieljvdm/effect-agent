import { expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, ErrorReporter, Fiber, Layer, Redacted, Schema } from "effect";
import { TestClock } from "effect/testing";
import { FetchHttpClient } from "effect/unstable/http";

import {
  BrowserSessionError,
  BrowserSessionReference,
  BrowserSessions,
} from "../src/BrowserSession.ts";
import { BrowserRunBinding } from "../src/internal/browser-binding.ts";
import { browserFailure } from "../src/internal/browser-failure.ts";
import { BrowserRunReadonlyLiveView } from "../src/internal/browser-readonly-live-view.ts";
import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";

const identity = {
  sessionId: Redacted.make("00000000-0000-4000-8000-000000000091"),
  contextId: Redacted.make("retained-context"),
  targetId: Redacted.make("retained-page"),
};

const native = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => browserFailure("test.connect", cause) });

const packet = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.Int,
      method: Schema.String,
      sessionId: Schema.optionalKey(Schema.String),
      params: Schema.optionalKey(
        Schema.Struct({
          width: Schema.optionalKey(Schema.Int),
          height: Schema.optionalKey(Schema.Int),
        }),
      ),
    }),
  ),
);

// Real Workers WebSockets and the published browser client. Only the remote CDP
// endpoint is substituted; these tests run inside the existing workerd lane.
const endpoint = Effect.fnUntraced(function* (mode: "success" | "pending" | "pending-version") {
  const pair = yield* Effect.acquireRelease(
    Effect.sync(() => new WebSocketPair()),
    (pair) =>
      Effect.sync(() => {
        pair[0].close();
        pair[1].close();
      }),
  );

  const started = yield* Deferred.make<void>();
  const versionStarted = yield* Deferred.make<void>();
  const closed = yield* Deferred.make<void>();
  const methods: string[] = [];
  const requests: Array<{ method: string; path: string }> = [];
  const viewport = { width: 624, height: 980 };

  const targetInfo = {
    targetId: "retained-page",
    type: "page",
    title: "Viewport fixture",
    url: "https://fixture.test/",
    attached: true,
    canAccessOpener: false,
    browserContextId: "retained-context",
  };

  pair[1].accept();
  pair[1].addEventListener("close", () => pair[1].close());
  pair[0].addEventListener("close", () => Effect.runSync(Deferred.succeed(closed, undefined)));
  pair[1].addEventListener("message", (event) => {
    const message = packet(event.data);

    methods.push(message.method);
    Effect.runSync(Deferred.succeed(started, undefined));
    if (message.method === "Browser.getVersion") {
      Effect.runSync(Deferred.succeed(versionStarted, undefined));
      if (mode === "pending-version") return;
    }
    if (mode === "pending") return;
    if (message.method === "Emulation.setDeviceMetricsOverride") {
      viewport.width = message.params?.width ?? viewport.width;
      viewport.height = message.params?.height ?? viewport.height;
    }
    pair[1].send(
      JSON.stringify({
        id: message.id,
        ...(message.sessionId === undefined ? {} : { sessionId: message.sessionId }),
        result:
          message.method === "Target.getBrowserContexts"
            ? { browserContextIds: ["retained-context"] }
            : message.method === "Browser.getVersion"
              ? { product: "Fixture Chromium", protocolVersion: "1.3" }
              : message.method === "Page.getFrameTree"
                ? {
                    frameTree: {
                      frame: {
                        id: "retained-frame",
                        loaderId: "retained-loader",
                        url: targetInfo.url,
                      },
                    },
                  }
                : {},
      }),
    );
  });

  const browser: Pick<BrowserRun, "fetch"> = {
    fetch: async (input, init) => {
      const request = new Request(input, init);

      requests.push({ method: request.method, path: new URL(request.url).pathname });

      return new Response(null, { status: 101, webSocket: pair[0] });
    },
  };

  return {
    browser,
    socket: pair[0],
    peer: pair[1],
    started,
    versionStarted,
    closed,
    methods,
    requests,
    viewport,
  };
});

it.effect("retires SDK pending callbacks before acknowledging raw attachment closure", () =>
  Effect.gen(function* () {
    const fixture = yield* endpoint("pending-version");

    const binding = yield* BrowserRunBinding.pipe(
      Effect.provide(BrowserRunBinding.layer(fixture.browser)),
    );

    const attachment = binding.connect(Redacted.value(identity.sessionId), "session.connect");
    const browser = yield* native(() => attachment.browser);
    const pending = yield* native(() => browser.version()).pipe(Effect.flip, Effect.forkChild);

    yield* Deferred.await(fixture.versionStarted);
    expect(browser.debugInfo.pendingProtocolErrors.length).toBeGreaterThan(0);
    yield* attachment.retire;
    expect(browser.connected).toBe(false);
    expect(browser.debugInfo.pendingProtocolErrors).toEqual([]);
    expect(yield* Fiber.join(pending)).toMatchObject({ reason: "provider" });
    expect(fixture.socket.readyState).toBe(WebSocket.CLOSED);
    const sent = fixture.methods.length;

    expect(yield* native(() => browser.version()).pipe(Effect.flip)).toMatchObject({
      reason: "provider",
    });
    expect(fixture.methods).toHaveLength(sent);
    expect(fixture.methods).not.toContain("Browser.close");
  }).pipe(Effect.scoped),
);

it.effect.each(["upgrade", "close-failure"] as const)(
  "retires late resume upgrades without SDK dispatch and preserves genuine failures (%s)",
  (mode) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const respond = yield* Deferred.make<void>();
      const reported = yield* Deferred.make<void>();
      const reports: Array<Cause.Cause<unknown>> = [];
      const fixture = yield* endpoint("success");
      const close = fixture.socket.close.bind(fixture.socket);

      // These refusals never hand the fixture socket to the binding; accept it for local release.

      if (mode === "close-failure") {
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            Object.defineProperty(fixture.socket, "close", { configurable: true, value: close });
          }),
        );
        Object.defineProperty(fixture.socket, "close", {
          configurable: true,
          value: () => {
            throw new TypeError("private-late-provider-detail");
          },
        });
      }

      const layer = BrowserSessions.layerNoDeps.pipe(
        Layer.provide(
          BrowserRunReadonlyLiveView.layer({
            accountId: "1234567890abcdef1234567890abcdef",
            apiToken: Redacted.make("fixture-token"),
          }).pipe(Layer.provide(FetchHttpClient.layer)),
        ),
        Layer.provide(
          BrowserRunBinding.layer({
            fetch: async (input, init) => {
              Effect.runSync(Deferred.succeed(started, undefined));
              await Effect.runPromise(Deferred.await(respond));

              if (mode === "upgrade" || mode === "close-failure")
                return fixture.browser.fetch(input, init);

              return new Response("private-late-provider-detail", { status: 503 });
            },
          }),
        ),
        Layer.provide(
          Layer.succeed(BrowserRunSessionLifecycle, {
            close: () => Effect.die("A failed resume must not terminate the retained provider"),
          }),
        ),
      );

      const attempt = yield* Effect.gen(function* () {
        return yield* (yield* BrowserSessions).attach(
          BrowserSessionReference.make({
            ...identity,
            version: 1,
            expiresAt: 120_000,
            commandTimeoutMillis: 30_000,
          }),
        );
      }).pipe(
        Effect.scoped,
        Effect.provide([
          layer,
          ErrorReporter.layer([
            ErrorReporter.make(({ cause }) => {
              reports.push(cause);
              Effect.runSync(Deferred.succeed(reported, undefined));
            }),
          ]),
        ]),
        Effect.forkChild,
      );

      yield* Deferred.await(started);
      yield* Fiber.interrupt(attempt);
      yield* Deferred.succeed(respond, undefined);
      if (mode === "upgrade") {
        yield* Deferred.await(fixture.closed);
        expect(fixture.methods).toEqual([]);
        expect(reports).toEqual([]);

        return;
      }
      yield* Deferred.await(reported);
      expect(reports).toHaveLength(1);
      expect(JSON.stringify(reports)).toContain(
        mode === "close-failure"
          ? '"operation":"session.disconnect"'
          : '"operation":"session.connect"',
      );
      expect(JSON.stringify(reports)).toContain('"name":"TypeError"');
      expect(JSON.stringify(reports)).not.toContain("private-late-provider-detail");
      expect(fixture.methods).toEqual([]);
    }).pipe(Effect.scoped),
);

it.effect.each(["timeout"] as const)(
  "releases a pending resume connection without terminating the retained provider (%s)",
  () =>
    Effect.gen(function* () {
      const fixture = yield* endpoint("pending");
      const reports: Array<Cause.Cause<unknown>> = [];
      let terminations = 0;

      const layer = BrowserSessions.layerNoDeps.pipe(
        Layer.provide(
          BrowserRunReadonlyLiveView.layer({
            accountId: "1234567890abcdef1234567890abcdef",
            apiToken: Redacted.make("fixture-token"),
          }).pipe(Layer.provide(FetchHttpClient.layer)),
        ),
        Layer.provide(BrowserRunBinding.layer(fixture.browser)),
        Layer.provide(
          Layer.succeed(BrowserRunSessionLifecycle, {
            close: () =>
              Effect.sync(() => {
                terminations++;
              }),
          }),
        ),
      );

      const attempt = yield* Effect.gen(function* () {
        return yield* (yield* BrowserSessions).attach(
          BrowserSessionReference.make({
            ...identity,
            version: 1,
            expiresAt: 120_000,
            commandTimeoutMillis: 30_000,
          }),
        );
      }).pipe(
        Effect.scoped,
        Effect.provide([
          layer,
          ErrorReporter.layer([ErrorReporter.make(({ cause }) => reports.push(cause))]),
        ]),
        Effect.forkChild,
      );

      yield* Deferred.await(fixture.started);
      expect(fixture.socket.readyState).toBe(WebSocket.OPEN);
      {
        yield* TestClock.adjust(30_000);
        expect(yield* Fiber.join(attempt).pipe(Effect.flip)).toMatchObject({
          reason: "timeout",
          dispatch: "not-dispatched",
          cleanup: "not-requested",
        });
      }
      expect(fixture.socket.readyState).toBeGreaterThanOrEqual(WebSocket.CLOSING);
      yield* Deferred.await(fixture.closed);
      yield* Effect.yieldNow;
      expect(reports).toHaveLength(1);
      expect(JSON.stringify(reports)).not.toContain('"reason":"provider"');
      expect(terminations).toBe(0);
      expect(fixture.requests).toHaveLength(1);
      expect(fixture.methods).not.toContain("Browser.close");
    }).pipe(Effect.scoped),
);

it.effect.each(["unacknowledged"] as const)(
  "does not qualify a failed resume whose raw retirement is uncertain (%s)",
  () =>
    Effect.gen(function* () {
      const fixture = yield* endpoint("pending");
      const close = fixture.socket.close.bind(fixture.socket);
      const reports: Array<Cause.Cause<unknown>> = [];

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          Object.defineProperty(fixture.socket, "close", { configurable: true, value: close });
        }),
      );
      Object.defineProperty(fixture.socket, "close", {
        configurable: true,
        value: () => {},
      });

      const layer = BrowserSessions.layerNoDeps.pipe(
        Layer.provide(
          BrowserRunReadonlyLiveView.layer({
            accountId: "1234567890abcdef1234567890abcdef",
            apiToken: Redacted.make("fixture-token"),
          }).pipe(Layer.provide(FetchHttpClient.layer)),
        ),
        Layer.provide(BrowserRunBinding.layer(fixture.browser)),
        Layer.provide(
          Layer.succeed(BrowserRunSessionLifecycle, {
            close: () => Effect.die("Failed resume must not DELETE the retained browser"),
          }),
        ),
      );

      const attempt = yield* Effect.gen(function* () {
        return yield* (yield* BrowserSessions).attach(
          BrowserSessionReference.make({
            ...identity,
            version: 1,
            expiresAt: 120_000,
            commandTimeoutMillis: 30_000,
          }),
        );
      }).pipe(
        Effect.scoped,
        Effect.provide([
          layer,
          ErrorReporter.layer([ErrorReporter.make(({ cause }) => reports.push(cause))]),
        ]),
        Effect.forkChild,
      );

      yield* Deferred.await(fixture.started);
      yield* TestClock.adjust(30_000);
      yield* TestClock.adjust(10_000);
      const exit = yield* Fiber.await(attempt);

      expect(exit._tag).toBe("Failure");
      if (exit._tag !== "Failure") return;
      expect(Cause.hasDies(exit.cause)).toBe(true);
      expect(
        exit.cause.reasons.some(
          (reason) =>
            Cause.isFailReason(reason) &&
            Schema.is(BrowserSessionError)(reason.error) &&
            reason.error.reason === "timeout" &&
            reason.error.cleanup === "not-requested",
        ),
      ).toBe(true);
      expect(JSON.stringify(reports)).toContain('"operation":"session.disconnect"');
      expect(JSON.stringify(reports)).not.toContain("private-local-close-detail");
      expect(fixture.methods).toEqual(["Target.getBrowserContexts"]);
      expect(fixture.socket.readyState).toBe(WebSocket.OPEN);
    }).pipe(Effect.scoped),
);
