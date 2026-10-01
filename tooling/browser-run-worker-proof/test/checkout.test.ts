import { fileURLToPath } from "node:url";

import { assert, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

import { Evidence } from "../src/proof.ts";

// Retain the real receiver boundary: wrong buyer credentials and duplicate submissions
// must remain observable independently of the agent and its claimed completion.
it.live(
  "authenticates only the test buyer and records duplicate purchase submissions",
  () =>
    Effect.gen(function* () {
      const bundle = yield* Effect.promise(() =>
        build({
          entryPoints: [fileURLToPath(new URL("../src/worker.ts", import.meta.url))],
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

      const runtime = yield* Effect.acquireRelease(
        Effect.sync(
          () =>
            new Miniflare(
              convertV4MiniflareOptions({
                modules: true,
                script: bundle.outputFiles[0]!.text,
                modulesRoot: "/",
                compatibilityDate: "2026-03-24",
                compatibilityFlags: ["nodejs_compat"],
                durableObjects: { CHECKOUTS: { className: "CheckoutRun", useSQLite: true } },
                bindings: { CHECKOUT_TOKEN: "control-secret", CHECKOUT_PASSWORD: "buyer-secret" },
              }),
            ),
        ),
        (runtime) => Effect.promise(() => runtime.dispose()),
      );

      const request = (
        path: string,
        body?: Record<string, string>,
        headers: Record<string, string> = {},
      ) =>
        Effect.promise(() =>
          runtime.dispatchFetch(`https://shop.test${path}`, {
            method: body === undefined ? "GET" : "POST",
            redirect: "manual",
            headers,
            ...(body === undefined ? {} : { body: new URLSearchParams(body) }),
          }),
        );

      assert.strictEqual((yield* request("/evidence")).status, 401);
      assert.strictEqual((yield* request("/shop/pay", {})).status, 401);
      assert.strictEqual(
        (yield* request("/shop/login", { email: "buyer@example.test", password: "wrong" })).status,
        403,
      );

      const login = yield* request("/shop/login", {
        email: "buyer@example.test",
        password: "buyer-secret",
      });

      assert.strictEqual(login.status, 303);
      const cookie = login.headers.get("set-cookie")!.split(";")[0]!;

      assert.isFalse(cookie.includes("buyer-secret"));
      const browserHeaders = { cookie };

      assert.strictEqual(
        (yield* request(
          "/shop/product",
          { color: "blue", size: "M", quantity: "1" },
          browserHeaders,
        )).status,
        303,
      );
      assert.strictEqual((yield* request("/shop/pay", {}, browserHeaders)).status, 503);
      assert.strictEqual((yield* request("/shop/pay", {}, browserHeaders)).status, 409);

      const response = yield* request("/evidence", undefined, {
        authorization: "Bearer control-secret",
      });

      const evidence = yield* Effect.promise(() => response.json()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Evidence)),
      );

      assert.deepStrictEqual(evidence.receipt, {
        buyer: "buyer@example.test",
        product: "everyday-shirt",
        color: "blue",
        size: "M",
        quantity: 1,
        address: "123 Test Street, San Francisco, CA 94107, US",
        shipping: "standard",
        subtotal: 3400,
        shippingCents: 500,
        tax: 312,
        total: 4212,
        currency: "USD",
        paid: true,
      });
      assert.strictEqual(evidence.attempts, 2);
      assert.isFalse(JSON.stringify(evidence).includes("buyer-secret"));
      const orders = yield* request("/shop/orders", undefined, browserHeaders);

      assert.include(yield* Effect.promise(() => orders.text()), "Payment received");
    }).pipe(Effect.scoped),
  60_000,
);
