import { BrowserCrypto } from "@effect/platform-browser";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted } from "effect";
import { InteractiveBrowserPolicy } from "effect-agent/interactive-browser";
import { vi } from "vite-plus/test";

import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";
import {
  BrowserRunProtectedBinding,
  browserRunProtectedBindingLayer,
} from "../src/protected-browser/binding.ts";
import { browserResponse } from "./browser-response.ts";

const provider = vi.hoisted(() => {
  const counters = {
    acquired: 0,
    connected: 0,
    contexts: 0,
    pages: 0,
    disconnected: 0,
    intercepted: 0,
  };

  const page = {
    createCDPSession: async () => ({
      send: async () => ({ targetInfo: { targetId: "existing-page" } }),
      detach: async () => {},
    }),
    browserContext: () => ({ id: "existing-context" }),
    setBypassServiceWorker: async () => {},
    setRequestInterception: async () => {
      counters.intercepted++;
    },
    on: () => {},
    off: () => {},
  };

  const context = {
    id: "existing-context",
    pages: async () => [page],
    newPage: async () => {
      counters.pages++;

      return page;
    },
  };

  const browser = {
    browserContexts: () => [context],
    createBrowserContext: async () => {
      counters.contexts++;

      return context;
    },
    disconnect: async () => {
      counters.disconnected++;
    },
    isConnected: () => true,
    on: () => {},
    off: () => {},
  };

  return { counters, browser, connectFailure: false };
});

vi.mock("puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js", () => ({
  default: {
    connect: async () => {
      provider.counters.connected++;
      if (provider.connectFailure) throw new Error("private-provider-connect-failure");

      return provider.browser;
    },
    acquire: async () => {
      provider.counters.acquired++;

      return { sessionId: "00000000-0000-4000-8000-000000000001" };
    },
  },
}));

const identity = {
  sessionId: Redacted.make("00000000-0000-4000-8000-000000000001"),
  contextId: Redacted.make("existing-context"),
  targetId: Redacted.make("existing-page"),
};

const policy = InteractiveBrowserPolicy.make({
  network: { _tag: "Unrestricted" },
  maxActions: 10,
  maxElapsedMillis: 120_000,
  maxReturnedBytes: 16_384,
});

it.effect.each(["Unrestricted", "ExactHosts"] as const)(
  "reattaches the exact page with interception only for a restricted policy (%s)",
  (network) => {
    const closed: Array<string> = [];
    const before = { ...provider.counters };

    const layer = browserRunProtectedBindingLayer({
      browser: {
        fetch: async (_input, init) => browserResponse(init, Redacted.value(identity.sessionId)),
      },
    }).pipe(
      Layer.provide(BrowserCrypto.layer),
      Layer.provide(
        Layer.succeed(BrowserRunSessionLifecycle, {
          close: (id) =>
            Effect.sync(() => {
              closed.push(Redacted.value(id));
            }),
        }),
      ),
    );

    return Effect.gen(function* () {
      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* (yield* BrowserRunProtectedBinding).open(
            {
              ...policy,
              network:
                network === "Unrestricted"
                  ? { _tag: network }
                  : { _tag: network, allowedHosts: ["shop.test"] },
            },
            identity,
          );

          expect(Redacted.value(session.identity.targetId)).toBe("existing-page");
          expect(Redacted.value(session.identity.contextId)).toBe("existing-context");
          yield* session.detach;
        }),
      );
      expect(closed).toEqual([]);
      expect(provider.counters.acquired).toBe(before.acquired);
      expect(provider.counters.contexts).toBe(before.contexts);
      expect(provider.counters.pages).toBe(before.pages);
      expect(provider.counters.intercepted).toBe(
        before.intercepted + (network === "ExactHosts" ? 1 : 0),
      );
      expect(provider.counters.disconnected).toBe(before.disconnected + 1);
    }).pipe(Effect.provide(layer));
  },
);

it.effect(
  "does not terminate or replace the host-owned session when its saved page cannot be attached",
  () => {
    const closed: Array<string> = [];
    const before = { ...provider.counters };

    const layer = browserRunProtectedBindingLayer({
      browser: {
        fetch: async (_input, init) => browserResponse(init, Redacted.value(identity.sessionId)),
      },
    }).pipe(
      Layer.provide(BrowserCrypto.layer),
      Layer.provide(
        Layer.succeed(BrowserRunSessionLifecycle, {
          close: (id) =>
            Effect.sync(() => {
              closed.push(Redacted.value(id));
            }),
        }),
      ),
    );

    return Effect.gen(function* () {
      const error = yield* (yield* BrowserRunProtectedBinding)
        .open(policy, { ...identity, targetId: Redacted.make("missing-page") })
        .pipe(Effect.scoped, Effect.flip);

      expect(error).toMatchObject({
        reason: "provider",
        dispatch: "not-dispatched",
        cleanup: "not-requested",
      });
      expect(closed).toEqual([]);
      expect(provider.counters.acquired).toBe(before.acquired);
      expect(provider.counters.contexts).toBe(before.contexts);
      expect(provider.counters.pages).toBe(before.pages);
    }).pipe(Effect.provide(layer));
  },
);

it.effect.each(["new", "resume"] as const)(
  "cleans only a newly allocated provider after a failed connection (%s)",
  (mode) =>
    Effect.gen(function* () {
      const closed: string[] = [];
      let allocations = 0;
      const before = provider.counters.connected;

      provider.connectFailure = true;
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          provider.connectFailure = false;
        }),
      );

      const layer = browserRunProtectedBindingLayer({
        browser: {
          fetch: async (_input, init) => {
            if (init?.method === "POST") allocations++;

            return browserResponse(init, Redacted.value(identity.sessionId));
          },
        },
      }).pipe(
        Layer.provide(BrowserCrypto.layer),
        Layer.provide(
          Layer.succeed(BrowserRunSessionLifecycle, {
            close: (id) =>
              Effect.sync(() => {
                closed.push(Redacted.value(id));
              }),
          }),
        ),
      );

      const error = yield* Effect.gen(function* () {
        return yield* (yield* BrowserRunProtectedBinding)
          .open(policy, mode === "resume" ? identity : undefined)
          .pipe(Effect.flip);
      }).pipe(Effect.scoped, Effect.provide(layer));

      expect(error).toMatchObject({
        reason: "provider",
        dispatch: "not-dispatched",
        milestone: "none",
        cleanup: mode === "new" ? "confirmed" : "not-requested",
      });
      expect(closed).toEqual(mode === "new" ? [Redacted.value(identity.sessionId)] : []);
      expect(allocations).toBe(mode === "new" ? 1 : 0);
      expect(provider.counters.connected - before).toBe(1);
    }).pipe(Effect.scoped),
);
