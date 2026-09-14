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

const provider = vi.hoisted(() => {
  const counters = { acquired: 0, contexts: 0, pages: 0, disconnected: 0, intercepted: 0 };

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

  return { counters, browser };
});

vi.mock("@cloudflare/puppeteer", () => ({
  default: {
    connect: async () => provider.browser,
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

it.effect(
  "reattaches exactly the saved context and page, reinstalls interception, and releases without closure",
  () => {
    const closed: Array<string> = [];
    const before = { ...provider.counters };

    const layer = browserRunProtectedBindingLayer({
      browser: { fetch: async () => new Response(null, { status: 503 }) },
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
          const session = yield* (yield* BrowserRunProtectedBinding).open(policy, identity);

          expect(Redacted.value(session.identity.targetId)).toBe("existing-page");
          expect(Redacted.value(session.identity.contextId)).toBe("existing-context");
          yield* session.detach;
        }),
      );
      expect(closed).toEqual([]);
      expect(provider.counters.acquired).toBe(before.acquired);
      expect(provider.counters.contexts).toBe(before.contexts);
      expect(provider.counters.pages).toBe(before.pages);
      expect(provider.counters.intercepted).toBe(before.intercepted + 1);
      expect(provider.counters.disconnected).toBe(before.disconnected + 1);
    }).pipe(Effect.provide(layer));
  },
);

it.effect(
  "terminates the exact saved session when its page is missing instead of creating a replacement",
  () => {
    const closed: Array<string> = [];
    const before = { ...provider.counters };

    const layer = browserRunProtectedBindingLayer({
      browser: { fetch: async () => new Response(null, { status: 503 }) },
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

      expect(error.cleanup).toBe("confirmed");
      expect(closed).toEqual([Redacted.value(identity.sessionId)]);
      expect(provider.counters.acquired).toBe(before.acquired);
      expect(provider.counters.contexts).toBe(before.contexts);
      expect(provider.counters.pages).toBe(before.pages);
    }).pipe(Effect.provide(layer));
  },
);
