import { CredentialFieldRole } from "@effect-agent/platform-cloudflare/browser-credentials";
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
  frames: Schema.Array(
    Schema.Struct({
      url: Schema.String,
      // Optional only for reports produced before explicit frame paths were recorded.
      frame: Schema.optionalKey(Schema.Array(Schema.String)),
      html: Schema.String,
    }),
  ),
});

/** A failed follow-up read never changes an acknowledged action into a failed action. */
export const ActionObservation = Schema.Struct({
  execution: Schema.Literal("completed"),
  observation: Schema.NullOr(BrowserObservation),
  readFailure: Schema.NullOr(Text),
});

export const AgentRun = Schema.Struct({
  turns: Schema.Natural,
  finishReason: Schema.Literals(["completed", "model-stop", "budget-exhausted"]),
  exhausted: Schema.optionalKey(Schema.Literals(["tokens", "tool-calls", "turns"])),
  usage: Schema.optionalKey(RunTotals),
});

/** Durations use one request-local monotonic clock; concurrent/nested spans are not additive. */
export const CheckoutSpan = Schema.Struct({
  id: Schema.String,
  request: Schema.Natural,
  turn: Schema.optionalKey(Schema.Natural),
  phase: Schema.Literals([
    "model",
    "decision",
    "text",
    "browser",
    "observation",
    "wait",
    "attach",
    "create",
    "close",
    "approval",
    "resume",
  ]),
  operation: Text,
  offsetMillis: Schema.Finite,
  elapsedMillis: Schema.optionalKey(Schema.Finite),
  outcome: Schema.Literals(["running", "completed", "failure", "defect", "interrupted"]),
  error: Schema.optionalKey(Text),
  requestedModel: Schema.optionalKey(Text),
  resolvedModel: Schema.optionalKey(Text),
  tools: Schema.optionalKey(Schema.Array(Text)),
  inputTokens: Schema.optionalKey(Schema.Natural),
  outputTokens: Schema.optionalKey(Schema.Natural),
  observationBytes: Schema.optionalKey(Schema.Natural),
});

export const CheckoutSpans = Schema.Array(CheckoutSpan).check(Schema.isMaxLength(8_000));

const EvidenceSelector = Schema.NonEmptyString.check(Schema.isMaxLength(2_048));
const SelectorSyntax = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9 .#_*~()>+,:-]*$/));

const fixtureAttributes = new Map([
  [
    "name",
    new Set([
      "name",
      "email",
      "password",
      "number",
      "expiry",
      "cvc",
      "product",
      "quantity",
      "color",
      "size",
      "line1",
      "city",
      "region",
      "postalCode",
      "country",
      "shipping",
      "code",
    ]),
  ],
  ["type", new Set(["text", "email", "password", "number", "hidden", "radio", "submit"])],
  ["title", new Set(["Secure card payment"])],
  [
    "autocomplete",
    new Set([
      "name",
      "username",
      "current-password",
      "email",
      "address-line1",
      "address-level2",
      "address-level1",
      "postal-code",
      "one-time-code",
      "cc-name",
      "cc-number",
      "cc-exp",
      "cc-csc",
    ]),
  ],
]);

const pseudoText = /(:[a-zA-Z-]+\(\s*)(["'])(.*?)\2(\s*\))/g;

// Keep pseudo-selector structure, but never retain its text arguments or arbitrary attribute
// literals. Only the fixture's source-owned attribute tokens above may enter target evidence.
export const evidenceSelector = (selector: string) => {
  if (
    /[\\/%@?&]/.test(selector) ||
    /\b(?:https?|wss?|ftp|file|data|blob|javascript|mailto|tel|about):/i.test(selector) ||
    /\[\s*(?:value|href|src|action|formaction)\b/i.test(selector)
  )
    return null;

  const redacted = selector.replace(pseudoText, "$1$2[redacted]$2$4");
  let allowed = true;

  const syntax = redacted.replace(pseudoText, "$1$4").replace(/\[[^\]]*\]/g, (attribute) => {
    if (/^\[\s*[a-zA-Z-]+\s*\]$/.test(attribute)) return "";

    const parts =
      /^\[\s*(name|type|title|autocomplete)\s*[~|^$*]?=\s*(?:"([^"]*)"|'([^']*)'|([a-zA-Z0-9_-]+))\s*(?:[is]\s*)?\]$/i.exec(
        attribute,
      );

    const value = parts?.[2] ?? parts?.[3] ?? parts?.[4];

    if (
      parts?.[1] === undefined ||
      value === undefined ||
      !fixtureAttributes.get(parts[1].toLowerCase())?.has(value)
    )
      allowed = false;

    return "";
  });

  const literalArgument =
    /(?!:(?:has|is|not|where|nth-(?:last-)?(?:child|of-type))\():[a-zA-Z-]+\(\s*[^)\s]/i.test(
      syntax,
    );

  return allowed &&
    !literalArgument &&
    Schema.is(SelectorSyntax)(syntax) &&
    Schema.is(EvidenceSelector)(redacted)
    ? redacted
    : null;
};

const TargetSelector = Schema.NullOr(EvidenceSelector);
const TargetFrame = Schema.Array(TargetSelector).check(Schema.isMaxLength(8));

const ToolTarget = Schema.Union([
  Schema.Struct({ frame: TargetFrame, selector: TargetSelector }),
  Schema.Struct({
    frame: TargetFrame,
    fields: Schema.Array(
      Schema.Struct({ selector: TargetSelector, role: CredentialFieldRole }),
    ).check(Schema.isMaxLength(8)),
  }),
]);

export const RunEvidence = Schema.Struct({
  shop: ShopState,
  control: Schema.Struct(Struct.omit(Control.fields, ["handoffId"])),
  browserIdentityUnchanged: Schema.Boolean,
  observations: Schema.Array(BrowserObservation).check(Schema.isMaxLength(150)),
  outputs: Schema.Array(AgentOutput).check(Schema.isMaxLength(8)),
  runs: Schema.optionalKey(Schema.Array(AgentRun).check(Schema.isMaxLength(8))),
  spans: Schema.optionalKey(CheckoutSpans),
  toolCalls: Schema.Array(
    Schema.Struct({ name: Text, outcome: Text, target: Schema.optionalKey(ToolTarget) }),
  ).check(Schema.isMaxLength(300)),
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
  measurement: Schema.optionalKey(
    Schema.Struct({
      version: Schema.Literal(1),
      clock: Schema.Literal("monotonic-per-request"),
      queue: Schema.Literal("excluded-from-case-included-in-matrix"),
      caseBoundary: Schema.Literal("before-seed-through-exact-browser-closure"),
      browserProtocolCalls: Schema.Literal("unavailable"),
      cost: Schema.Literal("unpriced"),
      maxOutputTokens: Schema.Natural,
    }),
  ),
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
