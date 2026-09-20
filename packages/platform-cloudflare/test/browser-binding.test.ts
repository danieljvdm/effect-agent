import { BrowserCrypto } from "@effect/platform-browser";
import { expect, it } from "@effect/vitest";
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
const endpoint = Effect.fnUntraced(function* (mode: "success" | "reject" | "pending") {
  const pair = yield* Effect.acquireRelease(
    Effect.sync(() => new WebSocketPair()),
    (pair) =>
      Effect.sync(() => {
        pair[0].close();
        pair[1].close();
      }),
  );

  const started = yield* Deferred.make<void>();
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
    if (mode === "pending") return;
    pair[1].send(
      JSON.stringify(
        mode === "reject"
          ? { id: message.id, error: { code: -32000, message: "private-provider-detail" } }
          : {
              id: message.id,
              result:
                message.method === "Target.getBrowserContexts"
                  ? { browserContextIds: ["retained-context"] }
                  : message.method === "Browser.getVersion"
                    ? { product: "Fixture Chromium" }
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

  return { browser, socket: pair[0], started, closed, methods, requests };
});

it.effect("connects through a Workers upgrade and releases only the attachment", () =>
  Effect.gen(function* () {
    const fixture = yield* endpoint("success");

    const binding = yield* BrowserRunBinding.pipe(
      Effect.provide(BrowserRunBinding.layer(fixture.browser)),
    );

    const browser = yield* native(() =>
      binding.connect(Redacted.value(identity.sessionId), "protected.connect"),
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

it.effect("releases the raw attachment when client initialization rejects", () =>
  Effect.gen(function* () {
    const fixture = yield* endpoint("reject");

    const binding = yield* BrowserRunBinding.pipe(
      Effect.provide(BrowserRunBinding.layer(fixture.browser)),
    );

    const failure = yield* native(() =>
      binding.connect(Redacted.value(identity.sessionId), "protected.connect"),
    ).pipe(Effect.flip);

    expect(failure).toMatchObject({ operation: "protected.connect", reason: "provider" });
    expect(ErrorReporter.isIgnored(failure)).toBe(false);
    expect(JSON.stringify(failure)).not.toContain("private-provider-detail");
    expect(fixture.socket.readyState).toBeGreaterThanOrEqual(WebSocket.CLOSING);
    yield* Deferred.await(fixture.closed);
    expect(fixture.methods).not.toContain("Browser.close");
  }).pipe(Effect.scoped),
);

it.effect.each(["refusal", "defect"] as const)(
  "reports a genuine late resume failure after interruption (%s)",
  (mode) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const respond = yield* Deferred.make<void>();
      const reported = yield* Deferred.make<void>();
      const reports: Array<Cause.Cause<unknown>> = [];

      const layer = browserRunProtectedBindingLayer({
        browser: {
          fetch: async () => {
            Effect.runSync(Deferred.succeed(started, undefined));
            await Effect.runPromise(Deferred.await(respond));
            if (mode === "defect") throw new TypeError("private-late-provider-detail");

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
      yield* Deferred.await(reported);
      expect(reports).toHaveLength(1);
      expect(JSON.stringify(reports)).toContain('"operation":"protected.connect"');
      expect(JSON.stringify(reports)).toContain(
        mode === "refusal" ? '"status":503' : '"name":"TypeError"',
      );
      expect(JSON.stringify(reports)).not.toContain("private-late-provider-detail");
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

      const failure = yield* native(() =>
        binding.connect(Redacted.value(identity.sessionId), "protected.connect"),
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
