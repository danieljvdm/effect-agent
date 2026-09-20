import {
  BrowserSessions,
  BrowserSessionReference,
} from "@effect-agent/platform-cloudflare/browser-session";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { DurableObject } from "cloudflare:workers";
import { Cause, Effect, Exit, Layer, Redacted, Schema, Struct } from "effect";
import { AgentRuntime, InMemory } from "effect-agent";
import { FetchHttpClient } from "effect/unstable/http";

import { buyer, buyerTools, CheckoutOwner } from "./checkout-agent.ts";
import {
  AgentOutput,
  AgentRun,
  BrowserObservation,
  Control,
  Decision,
  failure,
  RunEvidence,
  RunKey,
  Seed,
  ShopState,
  Start,
} from "./checkout-contract.ts";
import { shopPage } from "./checkout-pages.ts";
import {
  makeShop,
  quote,
  returnControl,
  sameQuote,
  ShopAction,
  transition,
} from "./checkout-store.ts";

export interface CheckoutEnv {
  CHECKOUTS: DurableObjectNamespace<CheckoutRun>;
  BROWSER: BrowserRun;
  CHECKOUT_TOKEN: string;
  OPENAI_API_KEY: string;
  CHECKOUT_MODEL: string;
  PROCESSOR_ORIGIN: string;
  CLOUDFLARE_ACCOUNT_ID: string;
  BROWSER_RENDERING_API_TOKEN: string;
}

const html = (body: string, status = 200, headers?: HeadersInit) =>
  new Response(body, {
    status,
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      ...headers,
    },
  });

const rpc = <A>(action: () => Promise<A>) =>
  Effect.tryPromise({ try: action, catch: () => failure("storage", "Fixture I/O failed") });

const Reference = Schema.toCodecJson(BrowserSessionReference);
const Observations = Schema.Array(BrowserObservation).check(Schema.isMaxLength(150));
const Outputs = Schema.Array(AgentOutput).check(Schema.isMaxLength(8));
const Runs = Schema.Array(AgentRun).check(Schema.isMaxLength(8));
const Calls = RunEvidence.fields.toolCalls;

/** Consumer-owned SQLite owner; no browser references or Live View capabilities enter model history. */
export class CheckoutRun extends DurableObject<CheckoutEnv> {
  constructor(ctx: DurableObjectState, env: CheckoutEnv) {
    super(ctx, env);
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS checkout_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    );
  }

  // Fault injection is controlled by authenticated test requests and applies once.
  private fault: string | undefined;
  private hit(location: string) {
    if (this.fault === location) {
      this.fault = undefined;
      throw new Error(`checkout failpoint ${location}`);
    }
  }
  private read<
    S extends Schema.Top & { readonly DecodingServices: never; readonly EncodingServices: never },
  >(key: string, schema: S): S["Type"] {
    const value = this.ctx.storage.sql
      .exec<{ value: string }>("SELECT value FROM checkout_state WHERE key=?", key)
      .toArray()[0]?.value;

    if (value === undefined) throw new Error(`Missing checkout ${key}`);

    return Schema.decodeSync(Schema.fromJsonString(schema))(value);
  }
  private exists(key: string) {
    return (
      this.ctx.storage.sql.exec("SELECT key FROM checkout_state WHERE key=?", key).toArray()
        .length > 0
    );
  }
  private write<
    S extends Schema.Top & { readonly DecodingServices: never; readonly EncodingServices: never },
  >(key: string, schema: S, value: S["Type"]) {
    this.hit(`before:${key}`);
    const encoded = Schema.encodeSync(Schema.fromJsonString(schema))(value);

    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO checkout_state(key,value) VALUES (?,?)",
      key,
      encoded,
    );
    this.hit(`after:${key}`);
  }
  private get shop() {
    return this.read("shop", ShopState);
  }
  private get control() {
    return this.read("control", Control);
  }
  private updateControl(value: Partial<Control>) {
    this.write("control", Control, { ...this.control, ...value });
  }
  private get sessions() {
    return BrowserSessions.layer({
      browser: this.env.BROWSER,
      accountId: this.env.CLOUDFLARE_ACCOUNT_ID,
      apiToken: Redacted.make(this.env.BROWSER_RENDERING_API_TOKEN),
    }).pipe(Layer.provide(FetchHttpClient.layer));
  }

  private evidence() {
    return RunEvidence.make({
      shop: this.shop,
      control: Struct.omit(this.control, ["handoffId"]),
      browserIdentityUnchanged:
        this.exists("reference") &&
        this.read("identity", Schema.String) ===
          this.read("reference", Reference).targetId.pipe(Redacted.value),
      observations: this.read("observations", Observations),
      outputs: this.read("outputs", Outputs),
      runs: this.read("runs", Runs),
      toolCalls: this.read("calls", Calls),
    });
  }

  private close = Effect.fnUntraced(function* (this: CheckoutRun) {
    if (this.control.closed) return;
    this.updateControl({ controller: "closed", running: false });
    if (this.exists("reference")) {
      const sessions = yield* BrowserSessions;
      const reference = this.read("reference", Reference);

      yield* sessions.close(reference.sessionId);
    }
    this.updateControl({ closed: true });
    yield* rpc(() => this.ctx.storage.deleteAlarm());
  });

  private run = Effect.fnUntraced(function* (this: CheckoutRun, message: string, origin: string) {
    if (this.control.running || this.control.controller !== "agent")
      return yield* failure(
        "authority",
        "The previous request is unresolved or browser control is paused; do not replay it",
      );
    if (this.control.requests >= 8) return yield* failure("budget", "Request limit exceeded");
    this.updateControl({ running: true, requests: this.control.requests + 1 });
    const sessions = yield* BrowserSessions;

    if (!this.exists("reference")) {
      yield* rpc(() => this.ctx.storage.setAlarm(Date.now() + 30_000));
      yield* sessions.create(
        { maxElapsedMillis: 1_200_000, commandTimeoutMillis: 30_000 },
        (reference) =>
          Effect.sync(() =>
            this.ctx.storage.transactionSync(() => {
              this.write("reference", Reference, reference);
              this.write("identity", Schema.String, Redacted.value(reference.targetId));
            }),
          ),
      );
    }
    const session = yield* sessions.attach(this.read("reference", Reference));

    const owner = Layer.succeed(CheckoutOwner, {
      authorize: Effect.suspend(() =>
        this.control.controller === "agent" && this.control.running
          ? Effect.void
          : failure("authority", "Browser control is paused"),
      ),
      observe: (value) =>
        Effect.sync(() =>
          this.write("observations", Observations, [
            ...this.read("observations", Observations),
            value,
          ]),
        ),
      record: (value) =>
        Effect.sync(() => this.write("calls", Calls, [...this.read("calls", Calls), value])),
      approval: Effect.sync(() => {
        const current = quote(this.shop);

        if (current === null || this.shop.payment === null) return null;
        if (sameQuote(current, this.shop.approval))
          return "The user already approved this exact checkout. Place the order once, then inspect its receipt.";
        this.updateControl({ controller: "approval", pendingApproval: current });

        return "Approval requested. Stop now and wait for the user's separate request.";
      }).pipe(
        Effect.filterOrFail(
          (value) => value !== null,
          () => failure("approval", "Finish checkout information before asking for approval"),
        ),
      ),
      human: Effect.sync(() => {
        this.updateControl({ controller: "human" });

        return "Human takeover requested. Stop now.";
      }),
    });

    const result = yield* AgentRuntime.run(buyer, message).pipe(
      Effect.provide(
        Layer.mergeAll(
          InMemory.layer,
          buyerTools({
            session,
            shopOrigin: origin,
            processorOrigin: this.env.PROCESSOR_ORIGIN,
          }).pipe(Layer.provide(owner)),
          OpenAiLanguageModel.model(this.env.CHECKOUT_MODEL, { max_output_tokens: 4_096 }).pipe(
            Layer.provide(
              OpenAiClient.layer({ apiKey: Redacted.make(this.env.OPENAI_API_KEY) }).pipe(
                Layer.provide(FetchHttpClient.layer),
              ),
            ),
          ),
        ),
      ),
    );

    this.write("runs", Runs, [
      ...this.read("runs", Runs),
      {
        turns: result.turns,
        finishReason: result.finishReason,
        ...(result.exhausted === undefined ? {} : { exhausted: result.exhausted }),
        ...(result.usage === undefined ? {} : { usage: result.usage }),
      },
    ]);
    this.write("outputs", Outputs, [...this.read("outputs", Outputs), result.output]);
    this.updateControl({ running: false });

    return result.output;
  }, Effect.scoped);

  private controlRequest = Effect.fnUntraced(function* (
    this: CheckoutRun,
    request: Request,
    operation: string,
  ) {
    if (
      request.headers.get("authorization") !== `Bearer ${this.env.CHECKOUT_TOKEN}` ||
      !this.env.CHECKOUT_TOKEN
    )
      return new Response("Unauthorized", { status: 401 });

    const body = <S extends Schema.Top & { readonly DecodingServices: never }>(schema: S) =>
      rpc(() => request.json()).pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)));

    if (operation === "seed" && request.method === "POST") {
      if (this.exists("shop")) return new Response("Run already exists", { status: 409 });
      const seed = yield* body(Seed);

      if (new URL(request.url).pathname.split("/")[2] !== seed.key)
        return new Response("Seed key differs from owner", { status: 400 });

      this.ctx.storage.transactionSync(() => {
        this.write("shop", ShopState, makeShop(seed));
        this.write("control", Control, {
          version: 1,
          controller: "agent",
          requests: 0,
          running: false,
          pendingApproval: null,
          handoffId: null,
          humanReturned: false,
          closed: false,
          failure: null,
        });
        this.write("observations", Observations, []);
        this.write("outputs", Outputs, []);
        this.write("runs", Runs, []);
        this.write("calls", Calls, []);
        this.write("cookie", Schema.String, crypto.randomUUID());
      });

      return Response.json({ seeded: true });
    }
    if (!this.exists("shop"))
      return operation === "close" && request.method === "POST"
        ? Response.json(null)
        : new Response("Unknown run", { status: 404 });
    if (operation === "evidence" && request.method === "GET") return Response.json(this.evidence());
    if (request.method !== "POST") return new Response("Method not allowed", { status: 405 });
    if (operation === "close") {
      yield* this.close();

      return Response.json(this.evidence());
    }
    if (operation === "fault") {
      this.fault = (yield* body(Schema.Struct({ location: Schema.String }))).location;

      return Response.json({ armed: true });
    }
    if (operation === "approve") {
      const decision = yield* body(Decision);

      if (
        this.control.running ||
        this.control.controller !== "approval" ||
        !sameQuote(decision.quote, this.control.pendingApproval)
      )
        return yield* failure("approval", "No matching pending approval");
      const next = yield* transition(this.shop, { _tag: "approve", quote: decision.quote });

      this.ctx.storage.transactionSync(() => {
        this.write("shop", ShopState, next);
        this.updateControl({ controller: "agent", pendingApproval: null });
      });

      return Response.json({ approved: true });
    }
    if (operation === "human" || operation === "return") {
      if (
        operation === "return" &&
        this.control.humanReturned &&
        this.control.controller === "agent"
      )
        return Response.json({ returned: true });
      if (this.control.running || this.control.controller !== "human")
        return yield* failure("authority", "Human does not own this browser");
      const sessions = yield* BrowserSessions;
      const session = yield* sessions.attach(this.read("reference", Reference));

      if (operation === "human") {
        if (this.control.handoffId !== null)
          return yield* failure("handoff", "Handoff already dispatched; do not repeat it");
        // Fence before the external dispatch, including a lost reply.
        this.updateControl({ handoffId: "dispatching" });

        const handoff = yield* session.handoff(Effect.void, {
          instructions:
            "Enter 246810 and select Verify and use saved details. Select Done if Live View offers it. Do not place the order.",
          timeout: 300_000,
        });

        this.updateControl({ handoffId: Redacted.value(handoff.handoffId) });
        const view = yield* session.getLiveView(Effect.void, { mode: "tab", expiresInMs: 300_000 });

        return Response.json({ liveView: Redacted.value(view.devtoolsFrontendUrl) });
      }
      const state = yield* session.getHandoffState(Effect.void);

      const returned = yield* returnControl(this.control, state, this.shop.walletVerified);

      this.write("control", Control, returned);

      return Response.json({ returned: true });
    }
    if (operation === "run") {
      const input = yield* body(Start);

      const output = yield* this.run(input.message, new URL(request.url).origin).pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit)
            ? Effect.sync(() =>
                this.updateControl({
                  controller: "failed",
                  failure: this.diagnostic(exit.cause),
                }),
              )
            : Effect.void,
        ),
      );

      return Response.json(output);
    }

    return new Response("Not found", { status: 404 });
  });

  private diagnostic(cause: Cause.Cause<unknown>) {
    let text = Cause.pretty(cause);

    for (const secret of [
      this.env.OPENAI_API_KEY,
      this.env.CHECKOUT_TOKEN,
      this.env.BROWSER_RENDERING_API_TOKEN,
    ])
      if (secret) text = text.replaceAll(secret, "[redacted]");

    return text.slice(0, 2_048);
  }

  private storefront = Effect.fnUntraced(function* (
    this: CheckoutRun,
    request: Request,
    page: string,
  ) {
    if (!this.exists("shop")) return new Response("Unknown shop", { status: 404 });
    const url = new URL(request.url);
    const origin = url.origin;
    const base = `${origin}/s/${this.shop.key}`;

    const render = (name: string, error = "", status = 200) =>
      html(shopPage(this.shop, origin, this.env.PROCESSOR_ORIGIN, name, error), status);

    const redirect = (location: string, extra?: Record<string, string>) =>
      new Response(null, {
        status: 303,
        headers: { location, "cache-control": "no-store", ...extra },
      });

    if (request.method === "GET") return render(page);
    if (request.method !== "POST") return new Response("Method not allowed", { status: 405 });
    if (this.control.closed) return new Response("Run closed", { status: 410 });
    if (
      page !== "login" &&
      !request.headers
        .get("cookie")
        ?.split("; ")
        .includes(`checkout=${this.read("cookie", Schema.String)}`)
    )
      return render("login", "Sign in to continue", 401);
    const form = Object.fromEntries(yield* rpc(() => request.formData()));

    if (page === "wallet") {
      if (form.email !== "alex@example.test")
        return render("cart", "Use your saved wallet email", 422);

      return redirect(`${this.env.PROCESSOR_ORIGIN}/verify?merchant=${encodeURIComponent(base)}`);
    }

    const action = yield* Schema.decodeUnknownEffect(ShopAction)(
      page === "cart"
        ? { _tag: page, cart: { ...form, quantity: Number(form.quantity) } }
        : page === "address"
          ? { _tag: page, address: form }
          : { ...form, _tag: page },
    );

    const next = yield* transition(this.shop, action).pipe(
      Effect.catch((error) =>
        Effect.sync(() => {
          this.write("shop", ShopState, {
            ...this.shop,
            validationErrors: this.shop.validationErrors + 1,
          });

          return error;
        }),
      ),
    );

    if ("_tag" in next) return render(page === "card" ? "payment" : page, next.message, 422);
    this.write("shop", ShopState, next);
    if (page === "login")
      return redirect(`${base}/`, {
        "set-cookie": `checkout=${this.read("cookie", Schema.String)}; Secure; HttpOnly; SameSite=None; Path=/s/${this.shop.key}/`,
      });
    if (page === "card")
      return redirect(`${this.env.PROCESSOR_ORIGIN}/ready?merchant=${encodeURIComponent(base)}`);
    if (page === "pay") {
      const outcome = next.attempts.at(-1)?.outcome;

      if (outcome === "paid")
        return this.shop.scenario === "ambiguous"
          ? render("ambiguous", "", 503)
          : redirect(`${base}/orders`);

      return render(
        outcome === "declined" ? "payment" : "review",
        outcome === "declined"
          ? "Your card was declined. Choose a different payment method."
          : "Order not submitted: approval is missing, invalid, or already used.",
        422,
      );
    }

    return redirect(
      `${base}/${page === "address" || page === "verify" ? "shipping" : page === "shipping" ? "payment" : "cart"}`,
    );
  });

  fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const [, surface, , operation = ""] = url.pathname.split("/");

    return Effect.runPromise(
      (surface === "_control"
        ? this.controlRequest(request, operation)
        : this.storefront(request, operation)
      ).pipe(
        Effect.scoped,
        Effect.provide(this.sessions),
        Effect.catchCause(() =>
          Effect.succeed(
            Response.json(
              { error: "Checkout request failed; inspect run evidence. Do not retry a mutation." },
              { status: 500 },
            ),
          ),
        ),
      ),
    );
  }
  alarm(): Promise<void> {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        if (!this.exists("reference") || this.control.closed) return;
        const reference = this.read("reference", Reference);

        if (reference.expiresAt <= Date.now() || this.control.controller === "failed")
          yield* this.close();
        else {
          const sessions = yield* BrowserSessions;

          yield* sessions.keepAlive(reference.sessionId);
          yield* rpc(() => this.ctx.storage.setAlarm(Date.now() + 30_000));
        }
      }).pipe(Effect.provide(this.sessions)),
    );
  }
}

export default {
  fetch(request: Request, env: CheckoutEnv): Promise<Response> | Response {
    const url = new URL(request.url);

    if (url.pathname === "/health") return Response.json({ fixture: "checkout-v1" });
    const [, surface, key] = url.pathname.split("/");

    if ((surface !== "s" && surface !== "_control") || !Schema.is(RunKey)(key))
      return new Response("Not found", { status: 404 });

    return env.CHECKOUTS.getByName(key).fetch(request);
  },
};
