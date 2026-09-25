import { fileURLToPath } from "node:url";

import { CredentialFillResult } from "@effect-agent/platform-cloudflare/browser-credentials";
import {
  BrowserSessionError,
  BrowserSessionReference,
  BrowserSessions,
  type BrowserSession,
} from "@effect-agent/platform-cloudflare/browser-session";
import { assert, it } from "@effect/vitest";
import { Cause, Effect, Exit, Redacted, Schema, Stream } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

import { buyerTools, CheckoutOwner, tools } from "../src/checkout-agent.ts";
import { browserSessionFailure, RunEvidence, savedAddress } from "../src/checkout-contract.ts";
import { assertPurchase, makeShop, quote, transition } from "../src/checkout-store.ts";
import { describeBrowserRunProofFailure } from "../src/contract.ts";

it("retains safe failure stages and browser session reasons in checkout evidence", () => {
  assert.strictEqual(
    describeBrowserRunProofFailure(502, {
      error: "The Browser Run binding proof failed",
      stage: "handoff",
      cleanupReason: "timeout",
      cleanupStatus: 504,
    }),
    "HTTP 502; stage=handoff; cleanup=timeout (504); invocation was not retried",
  );
  assert.strictEqual(
    describeBrowserRunProofFailure(502, { error: "private provider response" }),
    "HTTP 502; invocation was not retried",
  );
  assert.strictEqual(
    browserSessionFailure(
      Cause.fail(
        BrowserSessionError.make({
          reason: "busy",
          dispatch: "not-dispatched",
          cleanup: "unconfirmed",
        }),
      ),
    ),
    "BrowserSessionError reason=busy dispatch=not-dispatched cleanup=unconfirmed",
  );
});

it.effect("preserves credential acknowledgement through read failure without retrying input", () =>
  Effect.gen(function* () {
    for (const outcome of ["provider"] as const) {
      let fills = 0;
      let reads = 0;
      let released = 0;

      const reference = BrowserSessionReference.make({
        version: 1,
        sessionId: Redacted.make("00000000-0000-4000-8000-000000000001"),
        contextId: Redacted.make("saved-context"),
        targetId: Redacted.make("saved-page"),
        expiresAt: 1_900_000_000_000,
        commandTimeoutMillis: 1_000,
      });

      const fill = CredentialFillResult.make({ dispatch: "dispatched", filled: 2 });
      const unused = () => Effect.die("Only acknowledgement and post-action reading are exercised");

      const session: BrowserSession = {
        reference,
        fillCredential: () =>
          Effect.sync(() => {
            fills++;

            return fill;
          }),
        run: () =>
          Effect.suspend(() => {
            reads++;

            return Effect.fail(
              BrowserSessionError.make({
                reason: outcome,
                dispatch: "possibly-dispatched",
                cleanup: "not-requested",
              }),
            );
          }),
        handoff: unused,
        getLiveView: unused,
        getReadOnlyLiveView: unused,
        getHandoffState: unused,
      };

      const exit = yield* Effect.gen(function* () {
        const handlers = yield* tools;

        return yield* handlers
          .handle("fill_credential", {
            request: {
              credential: "account",
              kind: "login",
              fields: [
                { selector: "input[name=email]", role: "username" },
                { selector: "input[name=password]", role: "password" },
              ],
            },
          })
          .pipe(Effect.flatMap(Stream.runCollect));
      }).pipe(
        Effect.provide(
          buyerTools({
            reference,
            shopOrigin: "https://shop.test",
            processorOrigin: "https://pay.test",
          }),
        ),
        Effect.provideService(
          BrowserSessions,
          BrowserSessions.of({
            create: unused,
            createAttached: unused,
            attach: () =>
              Effect.acquireRelease(Effect.succeed(session), () => Effect.sync(() => released++)),
            keepAlive: unused,
            close: unused,
          }),
        ),
        Effect.provideService(
          CheckoutOwner,
          CheckoutOwner.of({
            authorize: Effect.void,
            observe: unused,
            record: () => Effect.void,
            approval: unused(),
            human: unused(),
          }),
        ),
        Effect.exit,
      );

      assert.strictEqual(fills, 1);
      assert.strictEqual(reads, 1);
      assert.strictEqual(released, 1);
      {
        assert.isTrue(Exit.isSuccess(exit));
        if (Exit.isFailure(exit)) return yield* Effect.die("Expected acknowledged input");
        assert.isFalse(exit.value.at(-1)?.isFailure);
        assert.deepStrictEqual(exit.value.at(-1)?.result, {
          execution: "completed",
          observation: null,
          readFailure:
            "The action completed but its observation failed. Use observe; do not repeat the action.",
          fill,
        });
      }
    }
  }),
);

const checkout = Effect.fnUntraced(function* (scenario: "success" | "correction" | "ambiguous") {
  let state = makeShop({ key: "test", flow: "embedded-card", scenario });

  state = yield* transition(state, {
    _tag: "login",
    email: "alex@example.test",
    password: "dummy-checkout-password",
  });
  state = yield* transition(state, {
    _tag: "cart",
    cart: { product: "everyday-shirt", color: "blue", size: "M", quantity: 1 },
  });
  state = yield* transition(state, { _tag: "address", address: savedAddress });
  state = yield* transition(state, { _tag: "shipping", shipping: "standard" });
  state = yield* transition(state, {
    _tag: "card",
    name: "Alex Example",
    number: "4242424242424242",
    expiry: "12/30",
    cvc: "123",
  });

  return state;
});

it.effect(
  "declines the primary card, requires a new approval, and exposes duplicate purchase dispatch",
  () =>
    Effect.gen(function* () {
      let state = yield* checkout("correction");
      const current = quote(state);

      if (current === null) return yield* Effect.die("Expected a complete quote");
      state = yield* transition(state, { _tag: "approve", quote: current });
      state = yield* transition(state, { _tag: "pay" });
      assert.strictEqual(state.inventory, 4);
      assert.deepStrictEqual(
        state.attempts.map((a) => a.outcome),
        ["declined"],
      );
      state = yield* transition(state, {
        _tag: "card",
        name: "Alex Example",
        number: "5555555555554444",
        expiry: "12/30",
        cvc: "123",
      });
      assert.isNull(state.approval);
      state = yield* transition(state, { _tag: "approve", quote: current });
      state = yield* transition(state, { _tag: "pay" });
      yield* assertPurchase(state);
      assert.strictEqual(state.orders[0]?.payment, "backup");
      state = yield* transition(state, { _tag: "pay" });
      assert.deepStrictEqual(
        state.attempts.map((a) => a.outcome),
        ["declined", "paid", "duplicate"],
      );
      assert.strictEqual(state.orders.length, 1);
      assert.strictEqual(state.inventory, 3);
      assert.strictEqual((yield* assertPurchase(state).pipe(Effect.flip)).stage, "assertion");
    }),
);

it.live(
  "preserves the durable dispatch fence after a workerd failure",
  () =>
    Effect.gen(function* () {
      const bundle = yield* Effect.promise(() =>
        build({
          entryPoints: [fileURLToPath(new URL("../src/checkout-worker.ts", import.meta.url).href)],
          bundle: true,
          write: false,
          format: "esm",
          platform: "browser",
          target: "es2022",
          external: ["cloudflare:*", "node:*"],
          alias: { crypto: "node:crypto" },
          conditions: ["workerd", "worker", "browser"],
        }),
      );

      const script = bundle.outputFiles[0]?.text;

      if (script === undefined) return yield* Effect.die("No worker bundle");

      const runtime = yield* Effect.acquireRelease(
        Effect.sync(
          () =>
            new Miniflare(
              convertV4MiniflareOptions({
                modules: true,
                script,
                modulesRoot: "/",
                compatibilityDate: "2026-08-01",
                compatibilityFlags: ["nodejs_compat"],
                durableObjects: { CHECKOUTS: { className: "CheckoutRun", useSQLite: true } },
                bindings: {
                  CHECKOUT_TOKEN: "runner-secret",
                  OPENAI_API_KEY: "not-used",
                  CHECKOUT_MODEL: "not-used",
                  PROCESSOR_ORIGIN: "https://processor.test",
                  CLOUDFLARE_ACCOUNT_ID: "a".repeat(32),
                  BROWSER_RENDERING_API_TOKEN: "not-used",
                },
                serviceBindings: {
                  BROWSER: () =>
                    new Response("No browser in offline fixture test", { status: 503 }),
                },
              }),
            ),
        ),
        (runtime) => Effect.promise(() => runtime.dispose()),
      );

      const control = (key: string, operation: string, body?: unknown) =>
        Effect.promise(() =>
          runtime.dispatchFetch(`https://shop.test/_control/${key}/${operation}`, {
            method: body === undefined ? "GET" : "POST",
            headers: { authorization: "Bearer runner-secret", "content-type": "application/json" },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          }),
        );

      yield* control("other", "seed", { key: "other", flow: "accelerated", scenario: "success" });
      // A fault after the durable dispatch fence cannot admit a second agent request.
      yield* control("other", "fault", { location: "after:control" });
      assert.strictEqual(
        (yield* control("other", "run", { message: "Buy the shirt" })).status,
        500,
      );
      assert.strictEqual((yield* control("other", "run", { message: "Try again" })).status, 500);
      const fenced = yield* control("other", "evidence");

      const retained = yield* Effect.promise(() => fenced.json()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(RunEvidence)),
      );

      assert.strictEqual(retained.control.requests, 1);
      assert.isTrue(retained.control.running);
      assert.deepStrictEqual(retained.shop.attempts, []);
      assert.strictEqual((yield* control("other", "close", {})).status, 200);
      assert.strictEqual((yield* control("other", "close", {})).status, 200);
    }).pipe(Effect.scoped),
  60_000,
);
