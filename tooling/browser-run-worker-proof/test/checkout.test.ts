import { fileURLToPath } from "node:url";

import { BrowserCredentialAccess } from "@effect-agent/platform-cloudflare/browser-credentials";
import {
  BrowserSessionReference,
  BrowserSessions,
  type BrowserSession,
  type BrowserSessionError,
} from "@effect-agent/platform-cloudflare/browser-session";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { assert, expectTypeOf, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Schema } from "effect";
import { AgentRuntime, InMemory, type Agent } from "effect-agent";
import type { Tool } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

import { buyer, buyerTools, CheckoutOwner, tools } from "../src/checkout-agent.ts";
import type { CheckoutError } from "../src/checkout-contract.ts";
import { Control, RunEvidence, savedAddress, ShopState } from "../src/checkout-contract.ts";
import {
  assertPurchase,
  makeShop,
  quote,
  returnControl,
  transition,
} from "../src/checkout-store.ts";

const run = AgentRuntime.run(buyer, "Check composition types").pipe(Effect.provide(InMemory.layer));

expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
  Agent.ModelServices | Tool.HandlersFor<typeof tools.tools> | BrowserCredentialAccess
>();
expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<
  AgentRuntime.AgentRuntimeFailure<typeof buyer>
>();
expectTypeOf<Layer.Services<ReturnType<typeof buyerTools>>>().toEqualTypeOf<
  CheckoutOwner | BrowserSessions
>();
expectTypeOf<Layer.Error<ReturnType<typeof buyerTools>>>().toEqualTypeOf<BrowserSessionError>();
expectTypeOf<Effect.Error<ReturnType<typeof transition>>>().toEqualTypeOf<CheckoutError>();

it.effect("the tool layer scopes its saved browser attachment across success and failure", () =>
  Effect.gen(function* () {
    for (const fails of [false, true]) {
      let attached = 0;
      let released = 0;

      const reference = BrowserSessionReference.make({
        version: 1,
        sessionId: Redacted.make("00000000-0000-4000-8000-000000000001"),
        contextId: Redacted.make("saved-context"),
        targetId: Redacted.make("saved-page"),
        expiresAt: 1_900_000_000_000,
        commandTimeoutMillis: 1_000,
      });

      const unused = () => Effect.die("Only attachment lifetime is exercised here");

      const session: BrowserSession = {
        reference,
        run: unused,
        fillCredential: unused,
        handoff: unused,
        getLiveView: unused,
        getHandoffState: unused,
      };

      const result = yield* Effect.gen(function* () {
        yield* tools;
        assert.strictEqual(attached, 1);
        assert.strictEqual(released, 0);
        if (fails) return yield* Effect.fail("consumer failed");
      }).pipe(
        Effect.provide(
          buyerTools({
            reference,
            shopOrigin: "https://shop.example.test",
            processorOrigin: "https://pay.example.test",
          }),
        ),
        Effect.provideService(
          BrowserSessions,
          BrowserSessions.of({
            create: unused,
            attach: (saved) =>
              Effect.acquireRelease(
                Effect.sync(() => {
                  assert.strictEqual(saved, reference);
                  attached++;

                  return session;
                }),
                () => Effect.sync(() => released++),
              ),
            keepAlive: unused,
            close: unused,
          }),
        ),
        Effect.provideService(
          CheckoutOwner,
          CheckoutOwner.of({
            authorize: unused(),
            observe: unused,
            observeIndexed: unused,
            record: unused,
            approval: unused(),
            human: unused(),
          }),
        ),
        Effect.exit,
      );

      assert.strictEqual(result._tag, fails ? "Failure" : "Success");
      assert.strictEqual(released, 1);
    }
  }),
);

it.effect("resumes an inactive handoff without a provider ID only after verified human input", () =>
  Effect.gen(function* () {
    const control = Control.make({
      version: 1,
      controller: "human",
      requests: 1,
      running: false,
      pendingApproval: null,
      handoffId: "recorded-handoff",
      humanReturned: false,
      closed: false,
      failure: null,
    });

    const returned = yield* returnControl(control, { active: false }, true);

    assert.strictEqual(returned.controller, "agent");
    assert.isTrue(returned.humanReturned);
    for (const rejected of [
      returnControl(control, { active: true }, true),
      returnControl(control, { active: false }, false),
      returnControl(control, { active: false, handoffId: Redacted.make("another-handoff") }, true),
      returnControl({ ...control, handoffId: null }, { active: false }, true),
      returnControl({ ...control, handoffId: "dispatching" }, { active: false }, true),
      returnControl({ ...control, running: true }, { active: false }, true),
    ])
      assert.isTrue((yield* Effect.exit(rejected))._tag === "Failure");
  }),
);

it.effect("sends the buyer's runtime toolkit through OpenAI request preparation", () =>
  Effect.gen(function* () {
    let dispatched = false;

    const unused = () =>
      Effect.die("This request-preparation check must not execute a browser tool");

    const result = yield* AgentRuntime.run(buyer, "Inspect the controlled shop").pipe(
      Effect.provide(
        Layer.mergeAll(
          InMemory.layer,
          tools.toLayer({
            observe: unused,
            navigate: unused,
            click: unused,
            type: unused,
            select: unused,
            wait: unused,
            fill_credential: unused,
            request_approval: unused,
            request_human: unused,
          }),
          OpenAiLanguageModel.model("test-model").pipe(
            Layer.provide(
              OpenAiClient.layer({ apiKey: Redacted.make("not-a-real-key") }).pipe(
                Layer.provide(
                  FetchHttpClient.layer.pipe(
                    Layer.provide(
                      Layer.succeed(FetchHttpClient.Fetch, async () => {
                        dispatched = true;

                        return new Response(
                          JSON.stringify({
                            error: {
                              message: "Expected test rejection",
                              type: "invalid_request_error",
                            },
                          }),
                          { status: 400, headers: { "content-type": "application/json" } },
                        );
                      }),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
      Effect.exit,
      Effect.provideService(
        BrowserCredentialAccess,
        BrowserCredentialAccess.of({ authorize: unused, resolve: unused }),
      ),
    );

    assert.isTrue(dispatched, JSON.stringify(result));
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

it.effect("records approval refusal and revokes approval when the total changes", () =>
  Effect.gen(function* () {
    let state = yield* checkout("success");

    state = yield* transition(state, { _tag: "pay" });
    assert.deepStrictEqual(
      state.attempts.map((a) => a.outcome),
      ["not-approved"],
    );
    assert.deepStrictEqual(state.orders, []);
    const current = quote(state);

    if (current === null) return yield* Effect.die("Expected a complete quote");
    state = yield* transition(state, { _tag: "approve", quote: current });
    state = yield* transition(state, { _tag: "shipping", shipping: "express" });

    const rejected = yield* transition(state, { _tag: "approve", quote: current }).pipe(
      Effect.flip,
    );

    assert.strictEqual(rejected.stage, "approval");
    state = yield* transition(state, { _tag: "pay" });
    assert.deepStrictEqual(
      state.attempts.map((a) => a.outcome),
      ["not-approved", "not-approved"],
    );
    assert.strictEqual(quote(state)?.total, 5_292);
    assert.deepStrictEqual(state.orders, []);
  }),
);

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
      const firstAttempt = state.attempts[0];

      if (firstAttempt === undefined) return yield* Effect.die("Expected the declined attempt");
      for (const altered of [
        { ...firstAttempt, payment: "backup" as const },
        { ...firstAttempt, quote: { ...current, total: 1 } },
      ])
        assert.strictEqual(
          (yield* assertPurchase({
            ...state,
            attempts: [altered, ...state.attempts.slice(1)],
          }).pipe(Effect.flip)).stage,
          "assertion",
        );
      // Reopen the schema-encoded durable record before an accidental repeat dispatch.
      state = yield* Schema.decodeEffect(Schema.fromJsonString(ShopState))(
        yield* Schema.encodeEffect(Schema.fromJsonString(ShopState))(state),
      );
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

it.effect("validates address and wallet verification before releasing saved details", () =>
  Effect.gen(function* () {
    let state = makeShop({ key: "wallet", flow: "accelerated", scenario: "success" });

    state = yield* transition(state, {
      _tag: "login",
      email: "alex@example.test",
      password: "dummy-checkout-password",
    });
    assert.strictEqual(
      (yield* transition(state, { _tag: "verify", code: "000000" }).pipe(Effect.flip)).stage,
      "validation",
    );
    assert.strictEqual(
      (yield* transition(state, {
        _tag: "address",
        address: { ...savedAddress, postalCode: "9410" },
      }).pipe(Effect.flip)).stage,
      "validation",
    );
    assert.isFalse(state.walletVerified);
    assert.isNull(state.payment);
    state = yield* transition(state, { _tag: "verify", code: "246810" });
    assert.deepStrictEqual(state.address, savedAddress);
    assert.strictEqual(state.payment, "saved");
  }),
);

it.live(
  "serves cross-origin fixtures and persists authentication, cart, attempts and fault boundaries in workerd",
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

      const dispatch = (path: string, body?: Record<string, string>, cookie?: string) =>
        Effect.promise(() =>
          runtime.dispatchFetch(`https://shop.test${path}`, {
            method: body === undefined ? "GET" : "POST",
            redirect: "manual",
            headers: {
              ...(body === undefined
                ? {}
                : { "content-type": "application/x-www-form-urlencoded" }),
              ...(cookie ? { cookie } : {}),
            },
            ...(body ? { body: new URLSearchParams(body).toString() } : {}),
          }),
        );

      const control = (key: string, operation: string, body?: unknown) =>
        Effect.promise(() =>
          runtime.dispatchFetch(`https://shop.test/_control/${key}/${operation}`, {
            method: body === undefined ? "GET" : "POST",
            headers: { authorization: "Bearer runner-secret", "content-type": "application/json" },
            ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          }),
        );

      assert.strictEqual((yield* dispatch("/_control/run/evidence")).status, 401);
      const absent = yield* control("unseeded", "close", {});

      assert.strictEqual(absent.status, 200);
      assert.isNull(yield* Effect.promise(() => absent.json()));
      assert.strictEqual(
        (yield* control("run", "seed", {
          key: "run",
          flow: "embedded-card",
          scenario: "ambiguous",
        })).status,
        200,
      );
      assert.strictEqual(
        (yield* control("run", "seed", {
          key: "run",
          flow: "embedded-card",
          scenario: "ambiguous",
        })).status,
        409,
      );

      const login = yield* dispatch("/s/run/login", {
        email: "alex@example.test",
        password: "dummy-checkout-password",
      });

      assert.strictEqual(login.status, 303);
      const cookie = login.headers.get("set-cookie")?.split(";")[0];

      assert.isDefined(cookie);
      const cart = { product: "everyday-shirt", color: "blue", size: "M", quantity: "1" };

      assert.strictEqual((yield* dispatch("/s/run/cart", cart)).status, 401);
      yield* dispatch("/s/run/cart", cart, cookie);
      yield* dispatch("/s/run/address", savedAddress, cookie);
      yield* dispatch("/s/run/shipping", { shipping: "standard" }, cookie);
      const payment = yield* dispatch("/s/run/payment");

      assert.include(
        yield* Effect.promise(() => payment.text()),
        "https://processor.test/card?merchant=",
      );
      yield* dispatch("/s/run/pay", {}, cookie);

      let evidence = yield* Effect.promise(() =>
        runtime
          .dispatchFetch("https://shop.test/_control/run/evidence", {
            headers: { authorization: "Bearer runner-secret" },
          })
          .then((r) => r.json()),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Struct({ shop: ShopState }))));

      assert.deepStrictEqual(
        evidence.shop.attempts.map((a) => a.outcome),
        ["invalid"],
      );
      assert.strictEqual(evidence.shop.orders.length, 0);
      yield* control("run", "fault", { location: "before:shop" });
      assert.strictEqual(
        (yield* dispatch("/s/run/cart", { ...cart, quantity: "2" }, cookie)).status,
        500,
      );
      const before = yield* control("run", "evidence");

      evidence = yield* Effect.promise(() => before.json()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Struct({ shop: ShopState }))),
      );
      assert.strictEqual(evidence.shop.cart?.quantity, 1);
      yield* control("run", "fault", { location: "after:shop" });
      assert.strictEqual(
        (yield* dispatch("/s/run/cart", { ...cart, quantity: "2" }, cookie)).status,
        500,
      );
      const after = yield* control("run", "evidence");

      evidence = yield* Effect.promise(() => after.json()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Struct({ shop: ShopState }))),
      );
      assert.strictEqual(evidence.shop.cart?.quantity, 2);
      yield* control("other", "seed", { key: "other", flow: "accelerated", scenario: "success" });
      assert.strictEqual((yield* dispatch("/s/other/cart", cart, cookie)).status, 401);

      for (const side of ["before", "after"]) {
        const key = `span-${side}`;

        yield* control(key, "seed", { key, flow: "accelerated", scenario: "success" });
        yield* control(key, "fault", { location: `${side}:spans` });
        assert.strictEqual((yield* control(key, "run", { message: "Buy the shirt" })).status, 500);
        assert.strictEqual((yield* control(key, "run", { message: "Try again" })).status, 500);
        const response = yield* control(key, "evidence");

        const value = yield* Effect.promise(() => response.json()).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(RunEvidence)),
        );

        assert.strictEqual(value.control.controller, "failed");
        assert.strictEqual(value.control.requests, 0);
        assert.deepStrictEqual(value.shop.attempts, []);
        assert.strictEqual((yield* control(key, "close", {})).status, 200);
      }

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
      assert.isFalse(retained.browserIdentityUnchanged);
      assert.deepStrictEqual(retained.shop.attempts, []);
      assert.notProperty(retained.control, "handoffId");
      assert.strictEqual((yield* control("other", "close", {})).status, 200);
      assert.strictEqual((yield* control("other", "close", {})).status, 200);
      assert.strictEqual(
        (yield* dispatch("/s/other/login", {
          email: "alex@example.test",
          password: "dummy-checkout-password",
        })).status,
        410,
      );
    }).pipe(Effect.scoped),
  60_000,
);
