import { Context, Schema, type Effect } from "effect";

import {
  PageCaptureEngine,
  PageCaptureNavigationError,
  PageCaptureProtocolError,
  PageCaptureRateLimitedError,
  PageCaptureTarget,
  PageCaptureUnsupportedError,
  PageNavigationOptions,
  PageResourcePolicy,
  PageViewport,
} from "./page-capture.ts";
import { SandboxImplementation } from "./sandbox.ts";

const MAX_SCREENSHOT_BYTES = 8 * 1024 * 1024;
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

/** The byte limit and full-page choice are fixed before Browser Run starts. */
export class PageScreenshotLimits extends Schema.Class<PageScreenshotLimits>(
  "PageScreenshotLimits",
)({
  maxOutputBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_SCREENSHOT_BYTES)),
}) {}

/** Schema-first request for exactly one PNG screenshot. */
export class PageScreenshotRequest extends Schema.Class<PageScreenshotRequest>(
  "PageScreenshotRequest",
)({
  target: PageCaptureTarget,
  engine: PageCaptureEngine,
  limits: PageScreenshotLimits,
  fullPage: Schema.Boolean,
  navigation: Schema.optionalKey(PageNavigationOptions),
  viewport: Schema.optionalKey(PageViewport),
  resourcePolicy: Schema.optionalKey(PageResourcePolicy),
}) {}

/** The only successful output: bytes are caller-owned and never durable framework data. */
export class PageScreenshotResult extends Schema.Class<PageScreenshotResult>(
  "PageScreenshotResult",
)({
  implementation: SandboxImplementation,
  mediaType: Schema.Literal("image/png"),
  bytes: Schema.Uint8Array.check(Schema.isMaxLength(MAX_SCREENSHOT_BYTES)),
}) {}

/** The response crossed the request's PNG byte limit. */
export class PageScreenshotOutputLimitError extends Schema.TaggedError<PageScreenshotOutputLimitError>()(
  "PageScreenshotOutputLimitError",
  {
    implementation: SandboxImplementation,
    limit: PositiveInt,
    observed: Schema.Natural,
  },
) {}

/** Expected screenshot failures. The shared capture errors keep identical semantics. */
export const PageScreenshotError = Schema.Union([
  PageCaptureRateLimitedError,
  PageCaptureNavigationError,
  PageCaptureUnsupportedError,
  PageCaptureProtocolError,
  PageScreenshotOutputLimitError,
]);

export type PageScreenshotError = typeof PageScreenshotError.Type;

/** Stateless, one-output PNG capture port. It owns neither persistence nor later byte handoff. */
export class PageScreenshot extends Context.Service<
  PageScreenshot,
  {
    readonly capture: (
      request: PageScreenshotRequest,
    ) => Effect.Effect<PageScreenshotResult, PageScreenshotError>;
  }
>()("@effect-agent/sandbox/PageScreenshot") {}

export type PageScreenshotCapture = (
  request: PageScreenshotRequest,
) => Effect.Effect<PageScreenshotResult, PageScreenshotError>;
