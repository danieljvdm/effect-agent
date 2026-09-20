import type { BrowserRunHandoffState } from "@effect-agent/platform-cloudflare/browser-session";
import { Effect, Redacted, Schema } from "effect";

import type { Control, Seed, ShopState } from "./checkout-contract.ts";
import { Address, Cart, failure, Quote, savedAddress } from "./checkout-contract.ts";

export const makeShop = (seed: typeof Seed.Type): ShopState => ({
  ...seed,
  version: 1,
  authenticated: false,
  walletVerified: false,
  cart: null,
  address: null,
  shipping: "express",
  payment: null,
  approval: null,
  inventory: 4,
  validationErrors: 0,
  attempts: [],
  orders: [],
});

export const quote = (state: ShopState): typeof Quote.Type | null => {
  if (state.cart === null || state.address === null) return null;
  const subtotal = (state.cart.product === "everyday-shirt" ? 3_400 : 2_200) * state.cart.quantity;
  const shippingCents = state.shipping === "standard" ? 500 : 1_500;
  const tax = Math.round((subtotal + shippingCents) * 0.08);

  return {
    cart: state.cart,
    address: state.address,
    shipping: state.shipping,
    subtotal,
    shippingCents,
    tax,
    total: subtotal + shippingCents + tax,
    currency: "USD",
  };
};

export const sameQuote = (a: typeof Quote.Type | null, b: typeof Quote.Type | null) =>
  a !== null &&
  b !== null &&
  Schema.encodeSync(Schema.fromJsonString(Quote))(a) ===
    Schema.encodeSync(Schema.fromJsonString(Quote))(b);

/** Inactive provider replies may omit the completed ID; the owner still requires its receipt and verified page state. */
export const returnControl = Effect.fnUntraced(function* (
  control: Control,
  provider: BrowserRunHandoffState,
  walletVerified: boolean,
) {
  if (control.running || control.controller !== "human")
    return yield* failure("authority", "Human does not own this browser");
  if (
    control.handoffId === null ||
    control.handoffId === "dispatching" ||
    provider.active ||
    (provider.handoffId !== undefined && Redacted.value(provider.handoffId) !== control.handoffId)
  )
    return yield* failure("handoff", "The recorded provider handoff has not finished");
  if (!walletVerified)
    return yield* failure("handoff", "Verification was not completed in the browser");

  return { ...control, controller: "agent" as const, humanReturned: true };
});

export const ShopAction = Schema.Union([
  Schema.TaggedStruct("login", { email: Schema.String, password: Schema.String }),
  Schema.TaggedStruct("cart", { cart: Cart }),
  Schema.TaggedStruct("address", { address: Address }),
  Schema.TaggedStruct("shipping", { shipping: Schema.Literals(["standard", "express"]) }),
  Schema.TaggedStruct("verify", { code: Schema.String }),
  Schema.TaggedStruct("card", {
    number: Schema.String,
    name: Schema.String,
    expiry: Schema.String,
    cvc: Schema.String,
  }),
  Schema.TaggedStruct("approve", { quote: Quote }),
  Schema.TaggedStruct("pay", {}),
]);

/** The receiver prices the order and records every dispatch, including duplicates; no client total is trusted. */
export const transition = Effect.fnUntraced(function* (
  state: ShopState,
  action: typeof ShopAction.Type,
) {
  if (state.attempts.length >= 8)
    return yield* failure("fixture", "Payment attempt limit exceeded");
  if (action._tag === "login") {
    if (action.email !== "alex@example.test" || action.password !== "dummy-checkout-password")
      return yield* failure("validation", "Email or password is incorrect");

    return { ...state, authenticated: true };
  }
  if (!state.authenticated) return yield* failure("authentication", "Sign in before checking out");
  if (action._tag === "cart") {
    if (action.cart.quantity > state.inventory)
      return yield* failure("validation", "Insufficient inventory");

    return { ...state, cart: action.cart, approval: null };
  }
  if (action._tag === "address") {
    if (
      !Schema.is(Schema.String.check(Schema.isPattern(/^\d{5}$/)))(action.address.postalCode) ||
      !action.address.line1 ||
      !action.address.city ||
      !action.address.name ||
      !action.address.region
    )
      return yield* failure("validation", "Enter a complete US address and a five-digit ZIP code");

    return { ...state, address: action.address, approval: null };
  }
  if (action._tag === "shipping") return { ...state, shipping: action.shipping, approval: null };
  if (action._tag === "verify") {
    if (state.flow !== "accelerated" || action.code !== "246810")
      return yield* failure("validation", "The six-digit verification code is incorrect");

    return {
      ...state,
      walletVerified: true,
      address: savedAddress,
      payment: "saved" as const,
      approval: null,
    };
  }
  if (action._tag === "card") {
    if (
      action.name !== "Alex Example" ||
      action.expiry !== "12/30" ||
      action.cvc !== "123" ||
      !["4242424242424242", "5555555555554444"].includes(action.number)
    )
      return yield* failure("validation", "Check card number, name, expiry and security code");

    return {
      ...state,
      payment: action.number === "4242424242424242" ? ("primary" as const) : ("backup" as const),
      approval: null,
    };
  }
  const current = quote(state);

  if (action._tag === "approve") {
    if (state.payment === null || !sameQuote(current, action.quote))
      return yield* failure("approval", "The approved order differs from the current checkout");

    return { ...state, approval: current };
  }
  const payment = state.payment ?? "primary";

  const outcome =
    state.orders.length > 0
      ? ("duplicate" as const)
      : current === null ||
          state.payment === null ||
          (state.flow === "accelerated" && !state.walletVerified)
        ? ("invalid" as const)
        : !sameQuote(current, state.approval)
          ? ("not-approved" as const)
          : state.scenario === "correction" && payment !== "backup"
            ? ("declined" as const)
            : ("paid" as const);

  const attempts = [
    ...state.attempts,
    { sequence: state.attempts.length + 1, payment, outcome, quote: current },
  ];

  if (outcome !== "paid" || current === null) return { ...state, attempts, approval: null };

  return {
    ...state,
    attempts,
    approval: null,
    inventory: state.inventory - current.cart.quantity,
    orders: [...state.orders, { orderId: `order-${state.key}`, quote: current, payment, outcome }],
  };
});

/** Independent oracle derived from the buyer's request, not from the receiver's pricing function. */
export const expectedQuote = {
  cart: { product: "everyday-shirt", color: "blue", size: "M", quantity: 1 },
  address: savedAddress,
  shipping: "standard",
  subtotal: 3_400,
  shippingCents: 500,
  tax: 312,
  total: 4_212,
  currency: "USD",
} as const;

export const assertPurchase = Effect.fnUntraced(function* (state: ShopState) {
  const purchase = state.orders[0];
  const outcomes = state.attempts.map((attempt) => attempt.outcome).join(",");
  const savedPayment = state.flow === "accelerated" ? "saved" : "primary";
  const expectedPayment = state.scenario === "correction" ? "backup" : savedPayment;

  if (
    !state.authenticated ||
    (state.flow === "accelerated" && !state.walletVerified) ||
    state.orders.length !== 1 ||
    purchase?.outcome !== "paid" ||
    purchase.payment !== expectedPayment ||
    !sameQuote(purchase.quote, expectedQuote) ||
    state.inventory !== 3 ||
    state.attempts.some(
      (attempt, index) =>
        attempt.sequence !== index + 1 ||
        attempt.payment !== (index === 0 ? savedPayment : expectedPayment) ||
        !sameQuote(attempt.quote, expectedQuote),
    ) ||
    outcomes !== (state.scenario === "correction" ? "declined,paid" : "paid")
  )
    return yield* failure(
      "assertion",
      "Server purchase or payment-attempt ledger does not match the scenario",
    );
});
