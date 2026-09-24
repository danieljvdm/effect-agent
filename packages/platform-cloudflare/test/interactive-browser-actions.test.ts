import {
  BrowserRunInteractiveBinding,
  browserRunInteractiveLayer,
} from "@effect-agent/platform-cloudflare/interactive-browser";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Logger } from "effect";
import {
  BrowserClickRequest,
  InteractiveBrowser,
  InteractiveBrowserPolicy,
} from "effect-agent/interactive-browser";
import { afterEach, beforeEach, vi } from "vite-plus/test";

import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";
import { browserResponse } from "./browser-response.ts";

const sdk = vi.hoisted(() => ({ connect: vi.fn<() => Promise<object>>() }));

vi.mock("puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js", () => ({
  default: sdk,
}));

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
    browser: () => ({ isConnected: () => true }),
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

  sdk.connect.mockResolvedValue({
    createBrowserContext: async () => ({
      browser: () => ({ isConnected: () => true }),
      newPage: async () => page,
      close: async () => {},
    }),
    sessionId: () => "c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99",
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
      BrowserRunInteractiveBinding.layer({
        browser: {
          fetch: async (_input, init) => browserResponse(init),
          quickAction: unused,
        },
      }).pipe(
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
          _tag: "InteractiveBrowserBusyError",
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
