import { Schema } from "effect";

export const RunId = Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,24}$/));

/** Source-authored diagnostics only; never publish provider text or session capabilities. */
export const WorkerFailure = Schema.Literals([
  "scrape:PageCaptureNavigationError",
  "scrape:PageCaptureProtocolError",
  "scrape:PageCaptureRateLimitedError",
  "scrape:PageCaptureOutputLimitError",
  "scrape:PageCaptureUnsupportedError",
  "scrape:TimeoutError",
  "scrape:assertion",
  "authority",
  "cleanup",
  "InteractiveBrowserActionError",
  "InteractiveBrowserProtocolError",
  "InteractiveBrowserCapacityError",
  "InteractiveBrowserPolicyDeniedError",
  "BrowserRunCleanupError",
  "AiError",
  "AgentPolicyError",
  "AgentOutputError",
  "ModelProtocolError",
  "worker-failure",
]);

export const Receipt = Schema.Struct({
  buyer: Schema.String,
  product: Schema.String,
  color: Schema.String,
  size: Schema.String,
  quantity: Schema.Natural,
  address: Schema.String,
  shipping: Schema.String,
  subtotal: Schema.Natural,
  shippingCents: Schema.Natural,
  tax: Schema.Natural,
  total: Schema.Natural,
  currency: Schema.String,
  paid: Schema.Boolean,
});

export const Evidence = Schema.Struct({
  started: Schema.Boolean,
  attempts: Schema.Natural,
  receipt: Schema.NullOr(Receipt),
  closed: Schema.Boolean,
  scrapeAttempts: Schema.Natural,
  failure: Schema.NullOr(Schema.String),
});

export class CheckoutError extends Schema.TaggedError<CheckoutError>()("CheckoutError", {
  stage: Schema.String,
  message: Schema.String,
}) {}
