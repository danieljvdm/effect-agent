import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Redacted } from "effect";
import { TestClock } from "effect/testing";
import { FetchHttpClient } from "effect/unstable/http";
import { beforeEach, vi } from "vite-plus/test";

import {
  BrowserCredentialAccess,
  FillCredentialRequest,
  LoginCredential,
} from "../src/BrowserCredentials.ts";
import {
  BrowserRunHandoffRequest,
  BrowserSessionOptions,
  BrowserSessions,
} from "../src/BrowserSession.ts";
import { BrowserRunBinding } from "../src/internal/browser-binding.ts";
import { BrowserRunReadonlyLiveView } from "../src/internal/browser-readonly-live-view.ts";
import {
  BrowserRunCleanupError,
  BrowserRunSessionLifecycle,
} from "../src/internal/browser-session-lifecycle.ts";
import { browserResponse } from "./browser-response.ts";

const provider = vi.hoisted(() => ({
  alive: false,
  pages: 0,
  title: "Checkout",
  human: false,
  pageFailure: false,
  malformedHandoff: false,
  lostWriteReply: false,
  writeReply: undefined as ((count: number) => Promise<void>) | undefined,
  credentialWrites: 0,
  disposedFields: 0,
  cleanupPending: false,
  acquired: 0,
  closed: [] as string[],
  retirements: 0,
  delayRetirement: undefined as (() => void) | undefined,
  acknowledgeRetirement: undefined as (() => void) | undefined,
}));

// Substitute only the SDK/provider seam. The real attachment transport, scopes, authority,
// timeout and exact-session cleanup wiring run in workerd; Chromium owns DOM tests separately.
vi.mock("puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js", () => ({
  default: {
    connect: async ({ transport }: { transport: { onclose?: () => void } }) => {
      if (!provider.alive) throw new Error("private-expired-provider");
      let connected = true;

      transport.onclose = () => {
        connected = false;
        provider.retirements++;
      };

      const fields = {
        prepare: () => ({ action: "https://merchant.test/login" }),
        target: () => ({ action: "https://merchant.test/login" }),
        fill: () => {
          provider.credentialWrites++;
          if (provider.lostWriteReply) throw new Error("private-write-reply");

          return provider.writeReply?.(provider.credentialWrites).then(() => "filled") ?? "filled";
        },
      };

      const frame = {
        detached: false,
        url: () => "https://merchant.test/login",
        isolatedRealm: () => ({
          evaluateHandle: async () => ({
            evaluate: async (callback: (...args: unknown[]) => unknown, ...args: unknown[]) =>
              callback(fields, ...args),
            dispose: async () => {
              provider.disposedFields++;
            },
          }),
        }),
      };

      const page = {
        url: frame.url,
        mainFrame: () => frame,
        browserContext: () => ({ id: "owner-context" }),
        title: async () => provider.title,
        goto: async (url: string) => {
          if (!connected) throw new Error("retired connection");
          provider.title = url;

          return null;
        },
        createCDPSession: async () => ({
          send: async (method: string) => {
            if (method === "Target.getTargetInfo")
              return { targetInfo: { targetId: "owner-page" } };
            if (method === "Cloudflare.handoff") {
              provider.human = true;

              return provider.malformedHandoff ? {} : { handoffId: "human-1" };
            }
            if (method === "Cloudflare.getHandoffState")
              return { active: provider.human, handoffId: "human-1" };
            throw new Error("unsupported fixture command");
          },
          detach: async () => {},
        }),
      };

      const context = {
        id: "owner-context",
        pages: async () => [page],
        newPage: async () => {
          if (provider.pageFailure) throw new Error("private-page-setup");
          provider.pages++;

          return page;
        },
      };

      return {
        isConnected: () => connected,
        browserContexts: () => [context],
        createBrowserContext: async () => context,
      };
    },
  },
}));

const id = "00000000-0000-4000-8000-000000000092";

const options = BrowserSessionOptions.make({
  maxElapsedMillis: 60_000,
  commandTimeoutMillis: 1_000,
});

const layer = BrowserSessions.layerNoDeps.pipe(
  Layer.provide(
    BrowserRunReadonlyLiveView.layer({
      accountId: "1234567890abcdef1234567890abcdef",
      apiToken: Redacted.make("fixture-token"),
    }).pipe(Layer.provide(FetchHttpClient.layer)),
  ),
  Layer.provide(
    BrowserRunBinding.layer({
      fetch: async (_input, init) => {
        if (init?.method === "POST") {
          provider.alive = true;
          provider.acquired++;
        }

        const response = browserResponse(init, id);

        if (init?.method !== "POST" && provider.delayRetirement !== undefined) {
          const socket = response.webSocket!;
          const close = socket.close.bind(socket);
          let state = WebSocket.OPEN;

          Object.defineProperty(socket, "readyState", {
            get: () => state,
            configurable: true,
          });
          Object.defineProperty(socket, "close", {
            value: () => {
              state = WebSocket.CLOSING;
              provider.acknowledgeRetirement = () => {
                state = WebSocket.CLOSED;
                close();
              };
              provider.delayRetirement?.();
            },
          });
        }

        return response;
      },
    }),
  ),
  Layer.provide(
    Layer.succeed(BrowserRunSessionLifecycle, {
      close: (sessionId) =>
        Effect.gen(function* () {
          provider.closed.push(Redacted.value(sessionId));
          if (provider.cleanupPending)
            return yield* new BrowserRunCleanupError({ reason: "pending" });
          provider.alive = false;
        }),
    }),
  ),
);

beforeEach(() =>
  Object.assign(provider, {
    alive: false,
    pages: 0,
    title: "Checkout",
    human: false,
    pageFailure: false,
    malformedHandoff: false,
    lostWriteReply: false,
    writeReply: undefined,
    credentialWrites: 0,
    disposedFields: 0,
    cleanupPending: false,
    acquired: 0,
    closed: [],
    retirements: 0,
    delayRetirement: undefined,
    acknowledgeRetirement: undefined,
  }),
);

it.effect("closes a late allocation reply without retaining or connecting it", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const respond = yield* Deferred.make<void>();
    const cleaned = yield* Deferred.make<void>();
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
    let retained = false;

    const lateLayer = BrowserSessions.layerNoDeps.pipe(
      Layer.provide(
        BrowserRunReadonlyLiveView.layer({
          accountId: "1234567890abcdef1234567890abcdef",
          apiToken: Redacted.make("fixture-token"),
        }).pipe(Layer.provide(FetchHttpClient.layer)),
      ),
      Layer.provide(
        BrowserRunBinding.layer({
          fetch: async (_input, init) => {
            await runPromise(Deferred.succeed(started, undefined));
            await runPromise(Deferred.await(respond));

            return browserResponse(init, id);
          },
        }),
      ),
      Layer.provide(
        Layer.succeed(BrowserRunSessionLifecycle, {
          close: (sessionId) =>
            Effect.sync(() => provider.closed.push(Redacted.value(sessionId))).pipe(
              Effect.andThen(Deferred.succeed(cleaned, undefined)),
              Effect.asVoid,
            ),
        }),
      ),
    );

    const fiber = yield* BrowserSessions.use((host) =>
      host.create(options, () =>
        Effect.sync(() => {
          retained = true;
        }),
      ),
    ).pipe(Effect.provide(lateLayer), Effect.forkChild);

    yield* Deferred.await(started);
    yield* Fiber.interrupt(fiber);
    yield* Deferred.succeed(respond, undefined);
    yield* Deferred.await(cleaned);
    expect(provider.closed).toEqual([id]);
    expect(provider.pages).toBe(0);
    expect(retained).toBe(false);
  }),
);

it.effect("closes an uncertain handoff even when the provider reply is malformed", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const reference = yield* host.create(options, () => Effect.void);
    const session = yield* host.attach(reference);

    provider.malformedHandoff = true;
    expect(
      yield* session
        .handoff(
          Effect.void,
          BrowserRunHandoffRequest.make({ instructions: "Review", timeout: 30_000 }),
        )
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "provider", dispatch: "possibly-dispatched", cleanup: "confirmed" });
    expect(provider.closed).toEqual([id]);
    expect(yield* session.run(Effect.void, (page) => page.title()).pipe(Effect.flip)).toMatchObject(
      { reason: "closed" },
    );
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect.each([
  {
    stage: "write reply",
    writes: 2,
    dispatch: "possibly-dispatched",
    filled: 1,
    cleanup: "confirmed",
    budget: 1_000,
  },
  {
    stage: "write reply",
    writes: 2,
    dispatch: "possibly-dispatched",
    filled: 1,
    cleanup: "unconfirmed",
    budget: 1_000,
  },
] as const)(
  "retains credential progress on timeout at $stage ($cleanup cleanup, $budget ms)",
  ({ writes, dispatch, filled, cleanup, budget }) =>
    Effect.gen(function* () {
      const host = yield* BrowserSessions;
      const reference = yield* host.create(options, () => Effect.void);
      const session = yield* host.attach(reference);
      const waiting = yield* Deferred.make<void>();
      const reply = yield* Deferred.make<void>();
      const replied = yield* Deferred.make<void>();
      const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());

      provider.cleanupPending = cleanup === "unconfirmed";
      if (budget < reference.commandTimeoutMillis) yield* TestClock.adjust(60_000 - budget);
      provider.writeReply = async (count) => {
        if (count !== 2) return;
        await runPromise(Deferred.succeed(waiting, undefined));
        await runPromise(Deferred.await(reply));
        await runPromise(Deferred.succeed(replied, undefined));
      };

      const fiber = yield* session
        .fillCredential(
          FillCredentialRequest.make({
            credential: "login-1",
            kind: "login",
            fields: [
              { selector: "#username", role: "username" },
              { selector: "#password", role: "password" },
            ],
          }),
        )
        .pipe(
          Effect.provideService(BrowserCredentialAccess, {
            authorize: () => Effect.void,
            resolve: () =>
              Effect.succeed(
                LoginCredential.make({
                  username: Redacted.make("user"),
                  password: Redacted.make("dummy-password"),
                }),
              ),
          }),
          Effect.flip,
          Effect.forkChild,
        );

      yield* Deferred.await(waiting);
      yield* TestClock.adjust(budget);
      const error = yield* Fiber.join(fiber);

      expect(error).toMatchObject({
        _tag: "CredentialFillError",
        reason: "timeout",
        dispatch,
        filled,
        cleanup,
      });
      expect(provider.closed).toEqual([id]);
      expect(provider.retirements).toBe(2);
      expect(provider.disposedFields).toBe(1);
      expect(
        yield* session.run(Effect.void, (page) => page.title()).pipe(Effect.flip),
      ).toMatchObject({ reason: "closed" });
      {
        yield* Deferred.succeed(reply, undefined);
        yield* Deferred.await(replied);
      }
      expect(provider.credentialWrites).toBe(writes);
      expect(error).toMatchObject({ dispatch, filled, cleanup });
      expect(JSON.stringify(error)).not.toContain("dummy-password");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("preserves a completed command while a delayed disconnect acknowledgment arrives", () =>
  Effect.gen(function* () {
    const host = yield* BrowserSessions;
    const reference = yield* host.create(options, () => Effect.void);
    const closing = yield* Deferred.make<void>();

    provider.delayRetirement = () => Effect.runSync(Deferred.succeed(closing, undefined));
    let completed = false;

    const attempt = yield* Effect.gen(function* () {
      const session = yield* host.attach(reference);

      return yield* session.run(Effect.void, (page) => page.title());
    }).pipe(
      Effect.scoped,
      Effect.tap(() =>
        Effect.sync(() => {
          completed = true;
        }),
      ),
      Effect.forkChild,
    );

    yield* Deferred.await(closing);
    yield* TestClock.adjust(1_500);
    expect(completed).toBe(false);
    expect(provider.retirements).toBe(2);
    provider.acknowledgeRetirement?.();
    expect(yield* Fiber.join(attempt)).toBe("Checkout");
    expect(provider.closed).toEqual([]);
    expect(provider.retirements).toBe(2);
  }).pipe(Effect.provide(layer)),
);
