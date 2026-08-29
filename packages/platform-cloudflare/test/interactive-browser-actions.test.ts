import {
  BrowserClickRequest,
  BrowserFillRequest,
  InteractiveBrowser,
  InteractiveBrowserPolicy,
} from "@effect-agent/sandbox";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Logger } from "effect";
import { afterEach, beforeEach, vi } from "vite-plus/test";

import { BrowserRunSessionLifecycle } from "../src/browser-session-lifecycle.ts";
import {
  BrowserRunInteractiveBinding,
  browserRunInteractiveLayer,
  isBrowserRunUndispatchedActionError,
} from "../src/interactive-browser.ts";

const sdk = vi.hoisted(() => ({ launch: vi.fn<() => Promise<object>>() }));
vi.mock("@cloudflare/puppeteer", () => ({ default: sdk }));

// Node's virtual timers cover the SDK boundary's quiet/deadline windows without
// sleeps. Public handles still run in Effect; only the remote SDK is replaced.
beforeEach(() => vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance"] }));
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

interface Request {
  resourceType: () => string;
  response: () => { status: () => number } | null;
}
const request = (type = "fetch", status = 200): Request => ({
  resourceType: () => type,
  response: () => ({ status: () => status }),
});
const emptyState = { matchCount: 1, kind: "button", formValid: false };
const gate = <A>() => {
  let resolve: (value: A) => void = () => {
    throw new Error("Uninitialized gate");
  };
  const promise = new Promise<A>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
};

const fixture = (
  options: {
    readonly state?: (
      evaluate: (selector: string) => unknown,
      selector: string,
    ) => Promise<unknown>;
    readonly matches?: number;
    readonly action?: () => Promise<void>;
    readonly dispose?: () => Promise<void>;
  } = {},
) => {
  const events: Array<string> = [];
  const logs: Array<ReturnType<typeof Logger.formatStructured.log>> = [];
  const listeners = new Map<string, Set<(request: Request) => void>>();
  const started = gate<void>();
  const action = async () => {
    events.push("dispatch");
    started.resolve();
    await options.action?.();
  };
  const page = {
    evaluate: options.state ?? (async () => emptyState),
    $$: async () =>
      Array.from({ length: options.matches ?? 1 }, () => ({
        click: action,
        evaluate: action,
        dispose: async () => {
          events.push("dispose");
          await options.dispose?.();
        },
      })),
    url: () => "https://example.com/private",
    close: async () => {
      events.push("close");
    },
    setBypassServiceWorker: async () => {},
    setRequestInterception: async () => {},
    on: (event: string, listener: (request: Request) => void) => {
      const existing = listeners.get(event) ?? new Set();
      existing.add(listener);
      listeners.set(event, existing);
    },
    off: (event: string, listener: (request: Request) => void) => {
      listeners.get(event)?.delete(listener);
    },
  };
  sdk.launch.mockResolvedValue({
    createBrowserContext: async () => ({ newPage: async () => page, close: async () => {} }),
    sessionId: () => "actions-test",
    isConnected: () => true,
    on: () => {},
    off: () => {},
    close: async () => {},
  });
  const unused = async (): Promise<Response> => {
    throw new Error("No live Browser Run calls");
  };
  const layer = browserRunInteractiveLayer().pipe(
    Layer.provide(
      BrowserRunInteractiveBinding.layer({ browser: { fetch: unused, quickAction: unused } }).pipe(
        Layer.provide(Layer.succeed(BrowserRunSessionLifecycle)({ close: () => Effect.void })),
      ),
    ),
  );
  return {
    layer: Layer.merge(
      layer,
      Logger.layer([
        Logger.map(Logger.formatStructured, (entry) => {
          logs.push(entry);
          events.push("log");
        }),
      ]),
    ),
    started: started.promise,
    events,
    logs,
    emit: (event: string, value: Request) => {
      // The separately installed policy request listener is not a network observer.
      const callbacks = [...(listeners.get(event) ?? [])];
      for (const callback of event === "request" ? callbacks.slice(1) : callbacks) callback(value);
    },
    observerCount: () =>
      [...listeners].reduce(
        (total, [name, values]) =>
          total + values.size - (name === "request" && values.size > 0 ? 1 : 0),
        0,
      ),
  };
};

const open = Effect.gen(function* () {
  return yield* (yield* InteractiveBrowser).open(
    InteractiveBrowserPolicy.make({
      network: { _tag: "Unrestricted" },
      maxActions: 10,
      maxElapsedMillis: 10_000,
      maxReturnedBytes: 256 * 1024,
    }),
  );
});
const click = BrowserClickRequest.make({ selector: "#private-selector" });
const advance = (millis: number) => Effect.promise(() => vi.advanceTimersByTimeAsync(millis));

describe("Browser Run observed mutations", () => {
  it.effect(
    "classifies a DOM syntax error as definitely undispatched without retaining the selector",
    () => {
      vi.stubGlobal("document", {
        querySelectorAll: () => {
          throw new DOMException("private invalid selector", "SyntaxError");
        },
      });
      const f = fixture({ state: async (evaluate, selector) => evaluate(selector) });
      return Effect.gen(function* () {
        const handle = yield* open;
        const error = yield* handle
          .click(BrowserClickRequest.make({ selector: "[" }))
          .pipe(Effect.flip);
        expect(isBrowserRunUndispatchedActionError(error)).toBe(true);
        expect(error).not.toHaveProperty("cause");
        expect(f.events).not.toContain("dispatch");
        expect(f.logs[0]?.annotations["browser.selector_match_count"]).toBe(0);
      }).pipe(Effect.scoped, Effect.provide(f.layer));
    },
  );

  it.effect("fences dispatch when a preflight query completes after interruption", () => {
    const query = gate<unknown>();
    const entered = gate<void>();
    const f = fixture({
      state: () => {
        entered.resolve();
        return query.promise;
      },
    });
    return Effect.gen(function* () {
      const handle = yield* open;
      const fiber = yield* handle.click(click).pipe(Effect.forkChild);
      yield* Effect.promise(() => entered.promise);
      const interrupt = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild);
      yield* advance(500);
      yield* Fiber.join(interrupt);
      query.resolve(emptyState);
      yield* advance(0);
      expect(f.events).not.toContain("dispatch");
      expect(f.events.filter((e) => e === "dispose")).toHaveLength(1);
      expect(f.logs[0]?.annotations).toMatchObject({
        "browser.action_dispatched": false,
        "browser.action_outcome_unknown": false,
      });
      expect(f.observerCount()).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  });

  it.effect.each([0, 2])(
    "refuses %i matches without dispatch or invalidating the session",
    (count) => {
      let matches = count;
      const f = fixture({ state: async () => ({ matchCount: matches }) });
      return Effect.gen(function* () {
        const handle = yield* open;
        const error = yield* handle.click(click).pipe(Effect.flip);
        expect(isBrowserRunUndispatchedActionError(error)).toBe(true);
        expect(f.events).not.toContain("dispatch");
        expect(f.logs[0]?.annotations["browser.selector_match_count"]).toBe(count);
        matches = 1;
        const fiber = yield* handle.click(click).pipe(Effect.forkChild);
        yield* Effect.promise(() => f.started);
        yield* advance(200);
        yield* Fiber.join(fiber);
        expect(f.events.filter((e) => e === "dispatch")).toHaveLength(1);
        expect(f.observerCount()).toBe(0);
      }).pipe(Effect.scoped, Effect.provide(f.layer));
    },
  );

  it.effect("rechecks uniqueness on the acquired handles and disposes an ambiguous batch", () => {
    const f = fixture({ matches: 2 });
    return Effect.gen(function* () {
      const handle = yield* open;
      expect(
        isBrowserRunUndispatchedActionError(
          yield* handle
            .fill(BrowserFillRequest.make({ selector: "input", value: "private-value" }))
            .pipe(Effect.flip),
        ),
      ).toBe(true);
      expect(f.events.filter((e) => e === "dispose")).toHaveLength(2);
      expect(f.events).not.toContain("dispatch");
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  });

  it.effect(
    "observes delayed fetch/XHR and ignores unrelated requests without leaking request data",
    () => {
      const f = fixture();
      return Effect.gen(function* () {
        const handle = yield* open;
        const fiber = yield* handle.click(click).pipe(Effect.forkChild);
        yield* Effect.promise(() => f.started);
        yield* advance(100);
        f.emit("request", request("image"));
        const requests = [
          request("fetch", 204),
          request("xhr", 302),
          request("fetch", 404),
          request("xhr", 503),
        ];
        for (const req of requests) f.emit("request", req);
        const failed = request();
        f.emit("request", failed);
        f.emit("requestfailed", failed);
        yield* advance(300);
        expect(f.logs).toHaveLength(0);
        for (const req of requests) f.emit("requestfinished", req);
        yield* advance(200);
        yield* Fiber.join(fiber);
        expect(f.logs[0]?.annotations).toMatchObject({
          "browser.fetch_xhr_total": 5,
          "browser.fetch_xhr_2xx": 1,
          "browser.fetch_xhr_3xx": 1,
          "browser.fetch_xhr_4xx": 1,
          "browser.fetch_xhr_5xx": 1,
          "browser.fetch_xhr_failed": 1,
          "browser.fetch_xhr_pending": 0,
          "browser.network_settle_timed_out": false,
        });
        expect(f.logs[0]?.cause).toBeUndefined();
        expect(
          Object.values(f.logs[0]?.annotations ?? {}).every(
            (v) =>
              typeof v === "boolean" || typeof v === "number" || v === "click" || v === "button",
          ),
        ).toBe(true);
        expect(f.observerCount()).toBe(0);
        expect(vi.getTimerCount()).toBe(0);
      }).pipe(Effect.scoped, Effect.provide(f.layer));
    },
  );

  it.effect(
    "caps pending network settlement at two seconds and post-navigation state at 250ms",
    () => {
      let reads = 0;
      const f = fixture({
        state: async () => (++reads === 1 ? emptyState : new Promise(() => {})),
      });
      return Effect.gen(function* () {
        const handle = yield* open;
        const fiber = yield* handle.click(click).pipe(Effect.forkChild);
        yield* Effect.promise(() => f.started);
        f.emit("request", request());
        yield* advance(2_250);
        yield* Fiber.join(fiber);
        expect(f.logs[0]?.annotations).toMatchObject({
          "browser.fetch_xhr_pending": 1,
          "browser.network_settle_timed_out": true,
          "browser.target_after_unavailable": true,
        });
        expect(f.observerCount()).toBe(0);
        expect(vi.getTimerCount()).toBe(0);
      }).pipe(Effect.scoped, Effect.provide(f.layer));
    },
  );

  it.effect("does not turn a destroyed post-action document into an action failure", () => {
    let reads = 0;
    const f = fixture({
      state: async () => {
        if (++reads > 1) throw new Error("private provider exception");
        return emptyState;
      },
    });
    return Effect.gen(function* () {
      const handle = yield* open;
      const fiber = yield* handle.click(click).pipe(Effect.forkChild);
      yield* Effect.promise(() => f.started);
      yield* advance(200);
      yield* Fiber.join(fiber);
      expect(f.logs[0]?.annotations["browser.target_after_unavailable"]).toBe(true);
      expect(f.logs[0]?.cause).toBeUndefined();
      expect(f.observerCount()).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  });

  it.effect(
    "cleans up a failed mutation and refuses subsequent mutations on the uncertain handle",
    () => {
      const f = fixture({
        action: async () => {
          throw new Error("private provider exception");
        },
        dispose: () => new Promise(() => {}),
      });
      return Effect.gen(function* () {
        const handle = yield* open;
        const fiber = yield* handle.click(click).pipe(Effect.flip, Effect.forkChild);
        yield* Effect.promise(() => f.started);
        yield* advance(250);
        expect(isBrowserRunUndispatchedActionError(yield* Fiber.join(fiber))).toBe(false);
        expect(yield* handle.click(click).pipe(Effect.flip)).toMatchObject({
          _tag: "InteractiveBrowserExpiredError",
        });
        expect(f.events.filter((e) => e === "dispatch")).toHaveLength(1);
        expect(f.observerCount()).toBe(0);
      }).pipe(Effect.scoped, Effect.provide(f.layer));
    },
  );

  it.effect(
    "records uncertainty before teardown on abort, bounds cleanup, and never replays a late mutation",
    () => {
      const action = gate<void>();
      const f = fixture({ action: () => action.promise });
      return Effect.gen(function* () {
        const handle = yield* open;
        const fiber = yield* handle.click(click).pipe(Effect.forkChild);
        yield* Effect.promise(() => f.started);
        const interrupt = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild);
        yield* advance(500);
        yield* Fiber.join(interrupt);
        expect(f.observerCount()).toBe(0);
        expect(f.logs[0]?.annotations).toMatchObject({
          "browser.action_dispatched": true,
          "browser.action_outcome_unknown": true,
        });
        expect(f.events).not.toContain("close");
        expect(yield* handle.click(click).pipe(Effect.flip)).toMatchObject({
          _tag: "InteractiveBrowserExpiredError",
        });
        yield* handle.close;
        expect(f.events.indexOf("log")).toBeLessThan(f.events.indexOf("close"));
        action.resolve();
        yield* advance(0);
        expect(f.events.filter((e) => e === "dispatch")).toHaveLength(1);
        expect(f.events.filter((e) => e === "dispose")).toHaveLength(1);
        expect(f.logs).toHaveLength(1);
        expect(vi.getTimerCount()).toBe(0);
      }).pipe(Effect.scoped, Effect.provide(f.layer));
    },
  );
});
