import { BrowserCrypto } from "@effect/platform-browser";
import { expect, expectTypeOf, it } from "@effect/vitest";
import {
  Cause,
  Clock,
  Deferred,
  Effect,
  ErrorReporter,
  Fiber,
  Layer,
  Redacted,
  Schema,
} from "effect";
import { InteractiveBrowserPolicy } from "effect-agent/interactive-browser";
import { ProtectedBrowserError } from "effect-agent/protected-browser";
import { TestClock } from "effect/testing";

import {
  BrowserRunInteractiveBinding,
  BrowserRunInteractiveCheckpoint,
  BrowserRunInteractiveHost,
  BrowserRunPageIdentity,
  browserRunInteractiveHostLayer,
} from "../src/InteractiveBrowser.ts";
import { BrowserRunBinding } from "../src/internal/browser-binding.ts";
import { browserFailure } from "../src/internal/browser-failure.ts";
import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";
import {
  BrowserRunProtectedBinding,
  browserRunProtectedBindingLayer,
} from "../src/protected-browser/binding.ts";
import {
  BrowserRunProtectedHost,
  browserRunProtectedHostLayer,
} from "../src/protected-browser/host.ts";

const identity = {
  sessionId: Redacted.make("00000000-0000-4000-8000-000000000091"),
  contextId: Redacted.make("retained-context"),
  targetId: Redacted.make("retained-page"),
};

const native = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => browserFailure("test.connect", cause) });

const packet = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ id: Schema.Int, method: Schema.String })),
);

// Real Workers WebSockets and the published browser client. Only the remote CDP
// endpoint is substituted; these tests run inside the existing workerd lane.
const endpoint = Effect.fnUntraced(function* (
  mode:
    | "success"
    | "reject"
    | "pending"
    | "pending-version"
    | "wrong-id"
    | "malformed"
    | "disconnect",
) {
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
    if (mode === "malformed") {
      pair[1].send("private-malformed-provider-detail");

      return;
    }
    if (mode === "disconnect") {
      pair[1].close();

      return;
    }
    pair[1].send(
      JSON.stringify(
        mode === "reject"
          ? { id: message.id, error: { code: -32000, message: "private-provider-detail" } }
          : {
              id: mode === "wrong-id" ? message.id + 1 : message.id,
              result:
                message.method === "Target.getBrowserContexts"
                  ? { browserContextIds: ["retained-context"] }
                  : message.method === "Browser.getVersion"
                    ? { product: "Fixture Chromium", protocolVersion: "1.3" }
                    : {},
            },
      ),
    );
  });

  const browser: Pick<BrowserRun, "fetch"> = {
    fetch: async (input, init) => {
      const request = new Request(input, init);

      requests.push({ method: request.method, path: new URL(request.url).pathname });

      return new Response(null, { status: 101, webSocket: pair[0] });
    },
  };

  return { browser, socket: pair[0], started, versionStarted, closed, methods, requests };
});

const keepAliveHost = (browser: Pick<BrowserRun, "fetch">) =>
  browserRunProtectedHostLayer().pipe(
    Layer.provide(browserRunProtectedBindingLayer({ browser })),
    Layer.provide(BrowserCrypto.layer),
    Layer.provide(
      Layer.succeed(BrowserRunSessionLifecycle, {
        close: () => Effect.die("Keepalive must never terminate the provider"),
      }),
    ),
  );

it.effect(
  "keeps an exact session alive with one browser command and releases its raw attachment",
  () =>
    Effect.gen(function* () {
      const fixture = yield* endpoint("success");

      yield* Effect.gen(function* () {
        const call = (yield* BrowserRunProtectedHost).keepAlive(identity.sessionId);

        expectTypeOf(call).toEqualTypeOf<Effect.Effect<void, ProtectedBrowserError>>();
        yield* call;
      }).pipe(Effect.provide(keepAliveHost(fixture.browser)));
      yield* Deferred.await(fixture.closed);
      expect(fixture.methods).toEqual(["Browser.getVersion"]);
      expect(fixture.requests).toEqual([
        { method: "GET", path: `/v1/devtools/browser/${Redacted.value(identity.sessionId)}` },
      ]);
    }).pipe(Effect.scoped),
);

it.effect.each(["reject", "malformed", "disconnect"] as const)(
  "sanitizes keepalive failure and releases only its connection (%s)",
  (mode) =>
    Effect.gen(function* () {
      const fixture = yield* endpoint(mode);
      const reports: Array<Cause.Cause<unknown>> = [];

      const error = yield* Effect.gen(function* () {
        return yield* (yield* BrowserRunProtectedHost)
          .keepAlive(identity.sessionId)
          .pipe(Effect.flip);
      }).pipe(
        Effect.provide([
          keepAliveHost(fixture.browser),
          ErrorReporter.layer([ErrorReporter.make(({ cause }) => reports.push(cause))]),
        ]),
      );

      expect(error).toMatchObject({
        reason: "provider",
        dispatch: "not-dispatched",
        cleanup: "not-requested",
      });
      expect(ErrorReporter.isIgnored(error)).toBe(true);
      yield* Deferred.await(fixture.closed);
      expect(fixture.methods).toEqual(["Browser.getVersion"]);
      expect(reports).toHaveLength(1);
      expect(JSON.stringify(reports)).toContain('"operation":"protected.keepAlive"');
      expect(JSON.stringify(reports)).toContain(
        `"reason":"${mode === "malformed" ? "malformed" : "provider"}"`,
      );
      expect(JSON.stringify({ error, reports })).not.toContain("private-");
    }).pipe(Effect.scoped),
);

it.effect.each(["timeout", "interruption", "wrong-id"] as const)(
  "does not accept missing or unrelated keepalive replies (%s)",
  (ending) =>
    Effect.gen(function* () {
      const fixture = yield* endpoint(ending === "wrong-id" ? "wrong-id" : "pending");
      const reports: Array<Cause.Cause<unknown>> = [];

      const attempt = yield* Effect.gen(function* () {
        yield* (yield* BrowserRunProtectedHost).keepAlive(identity.sessionId);
      }).pipe(
        Effect.provide([
          keepAliveHost(fixture.browser),
          ErrorReporter.layer([ErrorReporter.make(({ cause }) => reports.push(cause))]),
        ]),
        Effect.forkChild,
      );

      yield* Deferred.await(fixture.started);
      if (ending === "interruption") {
        yield* Fiber.interrupt(attempt);
        const exit = yield* Fiber.await(attempt);

        expect(exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      } else {
        yield* TestClock.adjust(10_000);
        expect(yield* Fiber.join(attempt).pipe(Effect.flip)).toMatchObject({
          reason: "timeout",
          dispatch: "not-dispatched",
          cleanup: "not-requested",
        });
      }
      yield* Deferred.await(fixture.closed);
      expect(fixture.methods).toEqual(["Browser.getVersion"]);
      expect(reports).toHaveLength(ending === "interruption" ? 0 : 1);
      expect(JSON.stringify(reports)).not.toContain('"reason":"provider"');
    }).pipe(Effect.scoped),
);

it.effect.each(["upgrade", "refusal", "defect"] as const)(
  "cleans a late keepalive upgrade and preserves genuine late failures (%s)",
  (ending) =>
    Effect.gen(function* () {
      const fixture = yield* endpoint("success");
      const fetching = yield* Deferred.make<void>();
      const respond = yield* Deferred.make<void>();
      const lateFailure = yield* Deferred.make<void>();
      const reports: Array<Cause.Cause<unknown>> = [];

      const attempt = yield* Effect.gen(function* () {
        yield* (yield* BrowserRunProtectedHost).keepAlive(identity.sessionId);
      }).pipe(
        Effect.provide([
          keepAliveHost({
            fetch: async (input, init) => {
              Effect.runSync(Deferred.succeed(fetching, undefined));
              await Effect.runPromise(Deferred.await(respond));
              if (ending === "defect") throw new TypeError("private-late-keepalive-detail");

              return ending === "refusal"
                ? new Response("private-refusal-detail", { status: 503 })
                : fixture.browser.fetch(input, init);
            },
          }),
          ErrorReporter.layer([
            ErrorReporter.make(({ cause }) => {
              reports.push(cause);
              if (reports.length === 2) Effect.runSync(Deferred.succeed(lateFailure, undefined));
            }),
          ]),
        ]),
        Effect.forkChild,
      );

      yield* Deferred.await(fetching);
      yield* TestClock.adjust(10_000);
      expect(yield* Fiber.join(attempt).pipe(Effect.flip)).toMatchObject({
        reason: "timeout",
        cleanup: "not-requested",
      });
      yield* Deferred.succeed(respond, undefined);
      if (ending === "upgrade") {
        yield* Deferred.await(fixture.closed);
        expect(reports).toHaveLength(1);
      } else {
        yield* Deferred.await(lateFailure);
        expect(JSON.stringify(reports[1])).toContain(
          ending === "refusal" ? '"status":503' : '"name":"TypeError"',
        );
        // This fixture endpoint was never handed to the adapter in these two cases.
        fixture.socket.accept();
      }
      expect(fixture.methods).toEqual([]);
      expect(JSON.stringify(reports)).not.toContain("private-");
    }).pipe(Effect.scoped),
);

it.effect("refuses an invalid keepalive identity before contacting the provider", () =>
  Effect.gen(function* () {
    const error = yield* (yield* BrowserRunProtectedHost)
      .keepAlive(Redacted.make("../not-a-session"))
      .pipe(Effect.flip);

    expect(error).toMatchObject({ reason: "denied", cleanup: "not-requested" });
  }).pipe(
    Effect.provide(
      keepAliveHost({
        fetch: async () => {
          throw new Error("Must not fetch");
        },
      }),
    ),
  ),
);

it.effect("connects through a Workers upgrade and releases only the attachment", () =>
  Effect.gen(function* () {
    const fixture = yield* endpoint("success");

    const binding = yield* BrowserRunBinding.pipe(
      Effect.provide(BrowserRunBinding.layer(fixture.browser)),
    );

    const browser = yield* native(
      () => binding.connect(Redacted.value(identity.sessionId), "protected.connect").browser,
    );

    expect(browser.browserContexts().map((context) => context.id)).toContain("retained-context");
    expect(yield* native(() => browser.version())).toBe("Fixture Chromium");
    yield* native(() => browser.disconnect());
    yield* Deferred.await(fixture.closed);
    expect(fixture.methods).not.toContain("Browser.close");
    expect(fixture.requests).toEqual([
      { method: "GET", path: `/v1/devtools/browser/${Redacted.value(identity.sessionId)}` },
    ]);
  }).pipe(Effect.scoped),
);

it.effect("retires SDK pending callbacks before acknowledging raw attachment closure", () =>
  Effect.gen(function* () {
    const fixture = yield* endpoint("pending-version");

    const binding = yield* BrowserRunBinding.pipe(
      Effect.provide(BrowserRunBinding.layer(fixture.browser)),
    );

    const attachment = binding.connect(Redacted.value(identity.sessionId), "protected.connect");
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

it.effect("releases the raw attachment when client initialization rejects", () =>
  Effect.gen(function* () {
    const fixture = yield* endpoint("reject");

    const binding = yield* BrowserRunBinding.pipe(
      Effect.provide(BrowserRunBinding.layer(fixture.browser)),
    );

    const failure = yield* native(
      () => binding.connect(Redacted.value(identity.sessionId), "protected.connect").browser,
    ).pipe(Effect.flip);

    expect(failure).toMatchObject({ operation: "protected.connect", reason: "provider" });
    expect(ErrorReporter.isIgnored(failure)).toBe(false);
    expect(JSON.stringify(failure)).not.toContain("private-provider-detail");
    expect(fixture.socket.readyState).toBeGreaterThanOrEqual(WebSocket.CLOSING);
    yield* Deferred.await(fixture.closed);
    expect(fixture.methods).not.toContain("Browser.close");
  }).pipe(Effect.scoped),
);

it.effect.each(["refusal", "defect", "upgrade", "close-failure"] as const)(
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
      if (mode === "refusal" || mode === "defect") fixture.socket.accept();
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

      const layer = browserRunProtectedBindingLayer({
        browser: {
          fetch: async (input, init) => {
            Effect.runSync(Deferred.succeed(started, undefined));
            await Effect.runPromise(Deferred.await(respond));
            if (mode === "defect") throw new TypeError("private-late-provider-detail");
            if (mode === "upgrade" || mode === "close-failure")
              return fixture.browser.fetch(input, init);

            return new Response("private-late-provider-detail", { status: 503 });
          },
        },
      }).pipe(
        Layer.provide(BrowserCrypto.layer),
        Layer.provide(
          Layer.succeed(BrowserRunSessionLifecycle, {
            close: () => Effect.die("A failed resume must not terminate the retained provider"),
          }),
        ),
      );

      const attempt = yield* Effect.gen(function* () {
        return yield* (yield* BrowserRunProtectedBinding).open(
          InteractiveBrowserPolicy.make({
            network: { _tag: "Unrestricted" },
            maxActions: 10,
            maxElapsedMillis: 60_000,
            maxReturnedBytes: 16_384,
          }),
          identity,
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
          ? '"operation":"protected.disconnect"'
          : '"operation":"protected.connect"',
      );
      expect(JSON.stringify(reports)).toContain(
        mode === "refusal" ? '"status":503' : '"name":"TypeError"',
      );
      expect(JSON.stringify(reports)).not.toContain("private-late-provider-detail");
      expect(fixture.methods).toEqual([]);
    }).pipe(Effect.scoped),
);

it.effect.each(["timeout", "interruption"] as const)(
  "releases a pending resume connection without terminating the retained provider (%s)",
  (ending) =>
    Effect.gen(function* () {
      const fixture = yield* endpoint("pending");
      const reports: Array<Cause.Cause<unknown>> = [];
      let terminations = 0;

      const layer = browserRunProtectedBindingLayer({ browser: fixture.browser }).pipe(
        Layer.provide(BrowserCrypto.layer),
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
        return yield* (yield* BrowserRunProtectedBinding).open(
          InteractiveBrowserPolicy.make({
            network: { _tag: "Unrestricted" },
            maxActions: 10,
            maxElapsedMillis: 60_000,
            maxReturnedBytes: 16_384,
          }),
          identity,
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
      if (ending === "timeout") {
        yield* TestClock.adjust(30_000);
        expect(yield* Fiber.join(attempt).pipe(Effect.flip)).toMatchObject({
          reason: "timeout",
          dispatch: "not-dispatched",
          cleanup: "not-requested",
        });
      } else {
        yield* Fiber.interrupt(attempt);
        const exit = yield* Fiber.await(attempt);

        expect(exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      }
      expect(fixture.socket.readyState).toBeGreaterThanOrEqual(WebSocket.CLOSING);
      yield* Deferred.await(fixture.closed);
      yield* Effect.yieldNow;
      expect(reports).toHaveLength(ending === "timeout" ? 1 : 0);
      expect(JSON.stringify(reports)).not.toContain('"reason":"provider"');
      expect(terminations).toBe(0);
      expect(fixture.requests).toHaveLength(1);
      expect(fixture.methods).not.toContain("Browser.close");
    }).pipe(Effect.scoped),
);

it.effect.each(["throws", "unacknowledged"] as const)(
  "does not qualify a failed resume whose raw retirement is uncertain (%s)",
  (mode) =>
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
        value: () => {
          if (mode === "throws") throw new TypeError("private-local-close-detail");
        },
      });

      const layer = browserRunProtectedBindingLayer({ browser: fixture.browser }).pipe(
        Layer.provide(BrowserCrypto.layer),
        Layer.provide(
          Layer.succeed(BrowserRunSessionLifecycle, {
            close: () => Effect.die("Failed resume must not DELETE the retained browser"),
          }),
        ),
      );

      const attempt = yield* Effect.gen(function* () {
        return yield* (yield* BrowserRunProtectedBinding).open(
          InteractiveBrowserPolicy.make({
            network: { _tag: "Unrestricted" },
            maxActions: 10,
            maxElapsedMillis: 60_000,
            maxReturnedBytes: 16_384,
          }),
          identity,
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
      yield* TestClock.adjust(1_000);
      const exit = yield* Fiber.await(attempt);

      expect(exit._tag).toBe("Failure");
      if (exit._tag !== "Failure") return;
      expect(Cause.hasDies(exit.cause)).toBe(true);
      expect(
        exit.cause.reasons.some(
          (reason) =>
            Cause.isFailReason(reason) &&
            Schema.is(ProtectedBrowserError)(reason.error) &&
            reason.error.reason === "timeout" &&
            reason.error.cleanup === "not-requested",
        ),
      ).toBe(true);
      expect(JSON.stringify(reports)).toContain('"operation":"protected.disconnect"');
      expect(JSON.stringify(reports)).not.toContain("private-local-close-detail");
      expect(fixture.methods).toEqual(["Target.getBrowserContexts"]);
      expect(fixture.socket.readyState).toBe(WebSocket.OPEN);
    }).pipe(Effect.scoped),
);

it.effect("releases an ordinary retained connection when its initialization times out", () =>
  Effect.gen(function* () {
    const fixture = yield* endpoint("pending");
    const reports: Array<Cause.Cause<unknown>> = [];
    let terminations = 0;

    const layer = browserRunInteractiveHostLayer().pipe(
      Layer.provide(
        BrowserRunInteractiveBinding.layer({
          browser: {
            ...fixture.browser,
            quickAction: async () => {
              throw new Error("No quick action during attachment");
            },
          },
        }).pipe(
          Layer.provide(
            Layer.succeed(BrowserRunSessionLifecycle, {
              close: () =>
                Effect.sync(() => {
                  terminations++;
                }),
            }),
          ),
        ),
      ),
    );

    const checkpoint = BrowserRunInteractiveCheckpoint.make({
      sessionId: identity.sessionId,
      page: BrowserRunPageIdentity.make({
        contextId: Redacted.value(identity.contextId),
        targetId: Redacted.value(identity.targetId),
      }),
      policy: InteractiveBrowserPolicy.make({
        network: { _tag: "Unrestricted" },
        maxActions: 10,
        maxElapsedMillis: 60_000,
        maxReturnedBytes: 16_384,
      }),
      startedAt: yield* Clock.currentTimeMillis,
      consumedActions: 0,
      inputState: "idle",
    });

    const attempt = yield* Effect.gen(function* () {
      return yield* (yield* BrowserRunInteractiveHost).resume(checkpoint, { pendingInput: false });
    }).pipe(
      Effect.scoped,
      Effect.provide([
        layer,
        ErrorReporter.layer([ErrorReporter.make(({ cause }) => reports.push(cause))]),
      ]),
      Effect.forkChild,
    );

    yield* Deferred.await(fixture.started);
    yield* TestClock.adjust(10_000);
    expect(yield* Fiber.join(attempt).pipe(Effect.flip)).toMatchObject({
      _tag: "InteractiveBrowserProtocolError",
    });
    expect(fixture.socket.readyState).toBeGreaterThanOrEqual(WebSocket.CLOSING);
    yield* Deferred.await(fixture.closed);
    yield* Effect.yieldNow;
    expect(reports).toHaveLength(1);
    expect(JSON.stringify(reports)).toContain('"operation":"interactive.resume"');
    expect(terminations).toBe(0);
    expect(fixture.requests).toHaveLength(1);
    expect(fixture.methods).not.toContain("Browser.close");
  }).pipe(Effect.scoped),
);

it.effect.each(["refused", "missing-socket"] as const)(
  "preserves a safe failed-upgrade diagnostic (%s)",
  (mode) =>
    Effect.gen(function* () {
      let cancelled = false;

      const binding = yield* BrowserRunBinding.pipe(
        Effect.provide(
          BrowserRunBinding.layer({
            fetch: async () =>
              mode === "refused"
                ? new Response(
                    new ReadableStream({
                      cancel: () => {
                        cancelled = true;
                        throw new Error("private-cancellation-detail");
                      },
                    }),
                    { status: 403 },
                  )
                : Object.defineProperty(new Response(null), "status", { value: 101 }),
          }),
        ),
      );

      const failure = yield* native(
        () => binding.connect(Redacted.value(identity.sessionId), "protected.connect").browser,
      ).pipe(Effect.flip);

      expect(failure).toMatchObject(
        mode === "refused"
          ? { operation: "protected.connect", reason: "provider", status: 403 }
          : { operation: "protected.connect", reason: "malformed" },
      );
      expect(JSON.stringify(failure)).not.toContain("private-cancellation-detail");
      expect(cancelled).toBe(mode === "refused");
    }),
);
