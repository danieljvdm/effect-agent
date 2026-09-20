import { Schema, Struct } from "effect";
import { RunTotals } from "effect-agent/usage";

export const CheckoutFlow = Schema.Literals(["embedded-card", "accelerated"]);
export const CheckoutScenario = Schema.Literals(["success", "correction", "ambiguous", "handoff"]);
export const RunKey = Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,80}$/));
const Text = Schema.String.check(Schema.isMaxLength(2_048));

export const Address = Schema.Struct({
  name: Text,
  line1: Text,
  city: Text,
  region: Text,
  postalCode: Text,
  country: Schema.Literal("US"),
});

export const savedAddress: typeof Address.Type = {
  name: "Alex Example",
  line1: "123 Test Street",
  city: "San Francisco",
  region: "CA",
  postalCode: "94107",
  country: "US",
};

export const Cart = Schema.Struct({
  product: Schema.Literals(["everyday-shirt", "canvas-bag"]),
  color: Schema.Literals(["blue", "red"]),
  size: Schema.Literals(["S", "M", "L"]),
  quantity: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4 })),
});

export const Quote = Schema.Struct({
  cart: Cart,
  address: Address,
  shipping: Schema.Literals(["standard", "express"]),
  subtotal: Schema.Natural,
  shippingCents: Schema.Natural,
  tax: Schema.Natural,
  total: Schema.Natural,
  currency: Schema.Literal("USD"),
});

export const PaymentAttempt = Schema.Struct({
  sequence: Schema.Natural,
  payment: Schema.Literals(["primary", "backup", "saved"]),
  outcome: Schema.Literals(["declined", "paid", "duplicate", "not-approved", "invalid"]),
  quote: Schema.NullOr(Quote),
});

export const Purchase = Schema.Struct({
  orderId: Schema.String,
  quote: Quote,
  payment: Schema.Literals(["primary", "backup", "saved"]),
  outcome: Schema.Literal("paid"),
});

/** Private fixture state. Payment material and provider session capabilities are never in evidence. */
export const ShopState = Schema.Struct({
  version: Schema.Literal(1),
  key: RunKey,
  flow: CheckoutFlow,
  scenario: CheckoutScenario,
  authenticated: Schema.Boolean,
  walletVerified: Schema.Boolean,
  cart: Schema.NullOr(Cart),
  address: Schema.NullOr(Address),
  shipping: Schema.Literals(["standard", "express"]),
  payment: Schema.NullOr(Schema.Literals(["primary", "backup", "saved"])),
  approval: Schema.NullOr(Quote),
  inventory: Schema.Natural,
  validationErrors: Schema.Natural,
  attempts: Schema.Array(PaymentAttempt).check(Schema.isMaxLength(8)),
  orders: Schema.Array(Purchase).check(Schema.isMaxLength(1)),
});

export type ShopState = typeof ShopState.Type;

export const Control = Schema.Struct({
  version: Schema.Literal(1),
  controller: Schema.Literals(["agent", "approval", "human", "closed", "failed"]),
  requests: Schema.Natural,
  // A process loss with running=true is deliberately not an instruction to replay a browser action.
  running: Schema.Boolean,
  pendingApproval: Schema.NullOr(Quote),
  handoffId: Schema.NullOr(Schema.String),
  humanReturned: Schema.Boolean,
  closed: Schema.Boolean,
  failure: Schema.NullOr(Text),
});

export type Control = typeof Control.Type;

export const AgentOutput = Schema.Struct({
  status: Schema.Literals([
    "approval-required",
    "human-required",
    "complete",
    "uncertain",
    "failed",
  ]),
  message: Text,
});

export const BrowserObservation = Schema.Struct({
  url: Schema.String,
  frames: Schema.Array(Schema.Struct({ url: Schema.String, html: Schema.String })),
});

export const AgentRun = Schema.Struct({
  turns: Schema.Natural,
  finishReason: Schema.Literals(["completed", "model-stop", "budget-exhausted"]),
  exhausted: Schema.optionalKey(Schema.Literals(["tokens", "tool-calls", "turns"])),
  usage: Schema.optionalKey(RunTotals),
});

export const RunEvidence = Schema.Struct({
  shop: ShopState,
  control: Schema.Struct(Struct.omit(Control.fields, ["handoffId"])),
  browserIdentityUnchanged: Schema.Boolean,
  observations: Schema.Array(BrowserObservation).check(Schema.isMaxLength(150)),
  outputs: Schema.Array(AgentOutput).check(Schema.isMaxLength(8)),
  runs: Schema.optionalKey(Schema.Array(AgentRun).check(Schema.isMaxLength(8))),
  toolCalls: Schema.Array(Schema.Struct({ name: Text, outcome: Text })).check(
    Schema.isMaxLength(300),
  ),
});

export const Seed = Schema.Struct({ key: RunKey, flow: CheckoutFlow, scenario: CheckoutScenario });
export const Start = Schema.Struct({ message: Schema.String.check(Schema.isMaxLength(4_096)) });
export const Decision = Schema.Struct({ quote: Quote });

export const CheckoutConcurrency = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 12 }));

export const StartIntervalMillis = Schema.Int.check(
  Schema.isBetween({ minimum: 1_000, maximum: 60_000 }),
);

export const Timings = Schema.Struct({
  deploymentMillis: Schema.optionalKey(Schema.Natural),
  readinessMillis: Schema.optionalKey(Schema.Natural),
  bindingProofMillis: Schema.optionalKey(Schema.Natural),
  matrixMillis: Schema.optionalKey(Schema.Natural),
  retirementMillis: Schema.optionalKey(Schema.Natural),
  totalMillis: Schema.optionalKey(Schema.Natural),
});

export const Report = Schema.Struct({
  version: Schema.Literal(1),
  model: Schema.String,
  sourceCommit: Schema.String,
  dirty: Schema.Boolean,
  repetitions: Schema.Natural,
  profile: Schema.Literals(["automated", "operator"]),
  // Optional additions keep previously retained reports usable for cleanup.
  execution: Schema.optionalKey(
    Schema.Struct({ concurrency: CheckoutConcurrency, startIntervalMillis: StartIntervalMillis }),
  ),
  timings: Schema.optionalKey(Timings),
  bindingProof: Schema.Boolean,
  suiteFailure: Schema.NullOr(Schema.String),
  configuration: Schema.Struct({
    maxTurns: Schema.Natural,
    maxToolCalls: Schema.Natural,
    maxDurationMillis: Schema.Natural,
    tokenBudget: Schema.Natural,
  }),
  results: Schema.Array(
    Schema.Struct({
      key: RunKey,
      flow: CheckoutFlow,
      scenario: CheckoutScenario,
      passed: Schema.Boolean,
      failure: Schema.NullOr(Schema.String),
      evidence: Schema.NullOr(RunEvidence),
      elapsedMillis: Schema.optionalKey(Schema.Natural),
    }),
  ),
  completed: Schema.Natural,
  attempted: Schema.Natural,
  completionRate: Schema.Finite,
  cleanup: Schema.Literals(["pending", "browsers-closed", "confirmed", "failed"]),
  providerCompatibility: Schema.Literal("not-established"),
});

export class CheckoutError extends Schema.TaggedError<CheckoutError>()("CheckoutError", {
  stage: Schema.String,
  message: Schema.String,
}) {}

export const failure = (stage: string, message: string) => CheckoutError.make({ stage, message });

export const policy = {
  maxTurns: 60,
  maxToolCalls: 120,
  maxDurationMillis: 300_000,
  tokenBudget: 500_000,
} as const;
