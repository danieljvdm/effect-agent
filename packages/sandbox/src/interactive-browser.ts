import { Context, Schema, type Effect, type Scope } from "effect";

import type { PageScreenshotResult } from "./page-screenshot.ts";
import { SandboxImplementation } from "./sandbox.ts";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const BoundedText = Schema.String.check(Schema.isMaxLength(8 * 1024 * 1024));
const BoundedMessage = Schema.String.check(Schema.isMaxLength(8 * 1024));
const Selector = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const FieldValue = Schema.String.check(Schema.isMaxLength(64 * 1024));
const ScrollDelta = Schema.Int.check(Schema.isBetween({ minimum: -100_000, maximum: 100_000 }));
const browserUrl = Reflect.get(globalThis, "URL");

/** Absolute, bounded HTTP(S) navigation URL without embedded credentials. */
export const InteractiveBrowserTargetUrl = Schema.NonEmptyString.check(
  Schema.isMaxLength(8 * 1024),
  Schema.makeFilter(
    (value) => {
      if (typeof browserUrl !== "function") return false;
      try {
        const url: unknown = Reflect.construct(browserUrl, [value]);
        return (
          typeof url === "object" &&
          url !== null &&
          (Reflect.get(url, "protocol") === "http:" || Reflect.get(url, "protocol") === "https:") &&
          Reflect.get(url, "hostname") !== "" &&
          Reflect.get(url, "username") === "" &&
          Reflect.get(url, "password") === ""
        );
      } catch {
        return false;
      }
    },
    { title: "an absolute HTTP or HTTPS URL without embedded credentials" },
  ),
);
export type InteractiveBrowserTargetUrl = typeof InteractiveBrowserTargetUrl.Type;

/** Canonical HTTPS host authority, optionally carrying a non-default port. */
export const InteractiveBrowserHost = Schema.NonEmptyString.check(
  Schema.isMaxLength(255),
  Schema.makeFilter(
    (value) => {
      if (typeof browserUrl !== "function" || value.includes("*")) return false;
      try {
        const url: unknown = Reflect.construct(browserUrl, [`https://${value}/`]);
        return (
          typeof url === "object" &&
          url !== null &&
          Reflect.get(url, "protocol") === "https:" &&
          Reflect.get(url, "username") === "" &&
          Reflect.get(url, "password") === "" &&
          Reflect.get(url, "host") === value
        );
      } catch {
        return false;
      }
    },
    { title: "a canonical credential-free HTTPS host authority" },
  ),
);
export type InteractiveBrowserHost = typeof InteractiveBrowserHost.Type;

/**
 * ExactHosts retains the page-request URL allowlist, not a public-network boundary.
 * PublicWeb requires connection-time public-address enforcement for all session
 * traffic, including human navigation. Adapters that cannot enforce it must fail
 * with InteractiveBrowserUnsupportedError before acquiring a browser.
 * Unrestricted explicitly opts out of URL/host and private-network containment.
 */
export const InteractiveBrowserNetworkPolicy = Schema.Union([
  Schema.TaggedStruct("ExactHosts", {
    allowedHosts: Schema.Array(InteractiveBrowserHost).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(64),
      Schema.isUnique(),
    ),
  }),
  Schema.TaggedStruct("PublicWeb", {}),
  Schema.TaggedStruct("Unrestricted", {}),
]).pipe(Schema.annotate({ parseOptions: { onExcessProperty: "error" } }));
export type InteractiveBrowserNetworkPolicy = typeof InteractiveBrowserNetworkPolicy.Type;

export class InteractiveBrowserPolicy extends Schema.Class<InteractiveBrowserPolicy>(
  "InteractiveBrowserPolicy",
)(
  Schema.Struct({
    network: InteractiveBrowserNetworkPolicy,
    maxActions: PositiveInt.check(Schema.isLessThanOrEqualTo(1_000)),
    maxElapsedMillis: PositiveInt.check(Schema.isLessThanOrEqualTo(10 * 60_000)),
    maxReturnedBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(8 * 1024 * 1024)),
  }).pipe(Schema.annotate({ parseOptions: { onExcessProperty: "error" } })),
) {}

export class BrowserNavigateRequest extends Schema.Class<BrowserNavigateRequest>(
  "BrowserNavigateRequest",
)({ url: InteractiveBrowserTargetUrl }) {}
export class BrowserReadTextRequest extends Schema.Class<BrowserReadTextRequest>(
  "BrowserReadTextRequest",
)({ selector: Schema.optionalKey(Selector) }) {}
export class BrowserFillRequest extends Schema.Class<BrowserFillRequest>("BrowserFillRequest")({
  selector: Selector,
  value: FieldValue,
}) {}
export class BrowserClickRequest extends Schema.Class<BrowserClickRequest>("BrowserClickRequest")({
  selector: Selector,
}) {}
/** Capture the current page without navigating or opening another browser. */
export class BrowserScreenshotRequest extends Schema.Class<BrowserScreenshotRequest>(
  "BrowserScreenshotRequest",
)({ fullPage: Schema.Boolean }) {}
/** Scroll the current viewport by signed CSS pixel deltas. */
export class BrowserScrollRequest extends Schema.Class<BrowserScrollRequest>(
  "BrowserScrollRequest",
)({
  deltaX: ScrollDelta,
  deltaY: ScrollDelta,
}) {}
export class BrowserNavigationResult extends Schema.Class<BrowserNavigationResult>(
  "BrowserNavigationResult",
)({ url: InteractiveBrowserTargetUrl }) {}
export class BrowserTextResult extends Schema.Class<BrowserTextResult>("BrowserTextResult")({
  text: BoundedText,
}) {}
/** Post-action URL observation; no provider session state crosses this boundary. */
export class BrowserActionResult extends Schema.Class<BrowserActionResult>("BrowserActionResult")({
  url: InteractiveBrowserTargetUrl,
}) {}

export class InteractiveBrowserPolicyDeniedError extends Schema.TaggedError<InteractiveBrowserPolicyDeniedError>()(
  "InteractiveBrowserPolicyDeniedError",
  { implementation: SandboxImplementation, message: BoundedMessage },
) {}
export class InteractiveBrowserBusyError extends Schema.TaggedError<InteractiveBrowserBusyError>()(
  "InteractiveBrowserBusyError",
  { implementation: SandboxImplementation, message: BoundedMessage },
) {}
export class InteractiveBrowserCapacityError extends Schema.TaggedError<InteractiveBrowserCapacityError>()(
  "InteractiveBrowserCapacityError",
  { implementation: SandboxImplementation, message: BoundedMessage },
) {}
export class InteractiveBrowserExpiredError extends Schema.TaggedError<InteractiveBrowserExpiredError>()(
  "InteractiveBrowserExpiredError",
  { implementation: SandboxImplementation, message: BoundedMessage },
) {}
export class InteractiveBrowserActionError extends Schema.TaggedError<InteractiveBrowserActionError>()(
  "InteractiveBrowserActionError",
  {
    implementation: SandboxImplementation,
    operation: Schema.Literals([
      "navigate",
      "read-text",
      "fill",
      "click",
      "screenshot",
      "scroll",
      "close",
    ]),
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}
export class InteractiveBrowserProtocolError extends Schema.TaggedError<InteractiveBrowserProtocolError>()(
  "InteractiveBrowserProtocolError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}
export class InteractiveBrowserLimitError extends Schema.TaggedError<InteractiveBrowserLimitError>()(
  "InteractiveBrowserLimitError",
  {
    implementation: SandboxImplementation,
    limit: Schema.Literals(["actions", "elapsed", "returned-bytes"]),
    maximum: PositiveInt,
    observed: Schema.Natural,
    message: BoundedMessage,
  },
) {}
export class InteractiveBrowserUnsupportedError extends Schema.TaggedError<InteractiveBrowserUnsupportedError>()(
  "InteractiveBrowserUnsupportedError",
  {
    implementation: SandboxImplementation,
    feature: Schema.Literals([
      "navigation",
      "read-text",
      "fill",
      "click",
      "screenshot",
      "scroll",
      "policy",
    ]),
    message: BoundedMessage,
  },
) {}
export const InteractiveBrowserError = Schema.Union([
  InteractiveBrowserPolicyDeniedError,
  InteractiveBrowserBusyError,
  InteractiveBrowserCapacityError,
  InteractiveBrowserExpiredError,
  InteractiveBrowserActionError,
  InteractiveBrowserProtocolError,
  InteractiveBrowserLimitError,
  InteractiveBrowserUnsupportedError,
]);
export type InteractiveBrowserError = typeof InteractiveBrowserError.Type;

/** A live handle is intentionally not a Schema value and cannot cross persistence/transport boundaries. */
export interface BrowserHandle {
  readonly navigate: (
    request: BrowserNavigateRequest,
  ) => Effect.Effect<BrowserNavigationResult, InteractiveBrowserError>;
  readonly readText: (
    request: BrowserReadTextRequest,
  ) => Effect.Effect<BrowserTextResult, InteractiveBrowserError>;
  readonly fill: (
    request: BrowserFillRequest,
  ) => Effect.Effect<BrowserActionResult, InteractiveBrowserError>;
  readonly click: (
    request: BrowserClickRequest,
  ) => Effect.Effect<BrowserActionResult, InteractiveBrowserError>;
  /** PNG bytes are caller-owned and bounded by the pass's per-result byte limit. */
  readonly screenshot: (
    request: BrowserScreenshotRequest,
  ) => Effect.Effect<PageScreenshotResult, InteractiveBrowserError>;
  readonly scroll: (
    request: BrowserScrollRequest,
  ) => Effect.Effect<BrowserActionResult, InteractiveBrowserError>;
  /** Invalidate the handle and close its resources early, including after an interrupted action. */
  readonly close: Effect.Effect<void, InteractiveBrowserError>;
}

export class InteractiveBrowser extends Context.Service<
  InteractiveBrowser,
  {
    readonly open: (
      policy: InteractiveBrowserPolicy,
    ) => Effect.Effect<BrowserHandle, InteractiveBrowserError, Scope.Scope>;
  }
>()("@effect-agent/sandbox/InteractiveBrowser") {}
