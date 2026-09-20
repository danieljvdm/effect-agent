import { Context, Option, Schema, type Effect, type Scope } from "effect";

import type { PageScreenshotResult } from "./PageScreenshot.ts";
import { SandboxImplementation } from "./Sandbox.ts";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const BoundedText = Schema.String.check(Schema.isMaxLength(8 * 1024 * 1024));
const BoundedMessage = Schema.String.check(Schema.isMaxLength(8 * 1024));
const Selector = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const FieldValue = Schema.String.check(Schema.isMaxLength(64 * 1024));
const ScrollDelta = Schema.Int.check(Schema.isBetween({ minimum: -100_000, maximum: 100_000 }));
const decodeUrl = Schema.decodeUnknownOption(Schema.URLFromString);

/** Absolute, bounded HTTP(S) navigation URL without embedded credentials. */
export const InteractiveBrowserTargetUrl = Schema.NonEmptyString.check(
  Schema.isMaxLength(8 * 1024),
  Schema.makeFilter(
    (value) => {
      const parsed = decodeUrl(value);

      if (Option.isNone(parsed)) return false;
      const url = parsed.value;

      return (
        (url.protocol === "http:" || url.protocol === "https:") &&
        url.hostname !== "" &&
        url.username === "" &&
        url.password === ""
      );
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
      if (value.includes("*")) return false;
      const parsed = decodeUrl(`https://${value}/`);

      if (Option.isNone(parsed)) return false;
      const url = parsed.value;

      return (
        url.protocol === "https:" &&
        url.username === "" &&
        url.password === "" &&
        url.host === value
      );
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
]);

export type InteractiveBrowserNetworkPolicy = typeof InteractiveBrowserNetworkPolicy.Type;

/** Caller-owned finite per-pass budgets. Elapsed time includes pauses; provider idle limits are separate. */
export class InteractiveBrowserPolicy extends Schema.Class<InteractiveBrowserPolicy>(
  "InteractiveBrowserPolicy",
)(
  Schema.Struct({
    network: InteractiveBrowserNetworkPolicy,
    maxActions: PositiveInt.check(Schema.isLessThanOrEqualTo(1_000)),
    maxElapsedMillis: PositiveInt,
    maxReturnedBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(8 * 1024 * 1024)),
  }),
) {}

export class BrowserNavigateRequest extends Schema.Class<BrowserNavigateRequest>(
  "BrowserNavigateRequest",
)({ url: InteractiveBrowserTargetUrl }) {}

export class BrowserReadTextRequest extends Schema.Class<BrowserReadTextRequest>(
  "BrowserReadTextRequest",
)({ selector: Schema.optionalKey(Selector) }) {}

/** Observed control state, without an input value. Absence is part of the snapshot. */
export const BrowserExpectedTargetState = Schema.Struct({
  kind: Schema.String.check(Schema.isMaxLength(128)),
  inputType: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(64))),
  label: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(200))),
  checked: Schema.optionalKey(Schema.Boolean),
  selected: Schema.optionalKey(Schema.Boolean),
  disabled: Schema.optionalKey(Schema.Boolean),
  required: Schema.optionalKey(Schema.Boolean),
  valid: Schema.optionalKey(Schema.Boolean),
  formValid: Schema.optionalKey(Schema.Boolean),
});

export type BrowserExpectedTargetState = typeof BrowserExpectedTargetState.Type;

/** Exact observed document/node identity. A replacement node is refused before input dispatch. */
export const BrowserExpectedTarget = Schema.Struct({
  documentId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  nodeId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  state: Schema.optionalKey(BrowserExpectedTargetState),
  scopeSelector: Schema.optionalKey(Selector),
});

export type BrowserExpectedTarget = typeof BrowserExpectedTarget.Type;

export class BrowserFillRequest extends Schema.Class<BrowserFillRequest>("BrowserFillRequest")({
  selector: Selector,
  value: FieldValue,
  expectedTarget: Schema.optionalKey(BrowserExpectedTarget),
}) {}

export class BrowserClickRequest extends Schema.Class<BrowserClickRequest>("BrowserClickRequest")({
  selector: Selector,
  expectedTarget: Schema.optionalKey(BrowserExpectedTarget),
}) {}

/** Caller-owned bytes; no host filesystem paths or model-supplied script crosses this port. */
export class BrowserSelectFileRequest extends Schema.Class<BrowserSelectFileRequest>(
  "BrowserSelectFileRequest",
)({
  selector: Selector,
  target: Schema.Literals(["input", "chooser"]),
  fileName: Schema.NonEmptyString.check(
    Schema.isMaxLength(200),
    // oxlint-disable-next-line no-control-regex -- File names cannot contain control characters or paths.
    Schema.isPattern(/^[^/\\\x00-\x1f\x7f]+$/),
  ),
  mediaType: Schema.NonEmptyString.check(
    Schema.isMaxLength(127),
    Schema.isPattern(/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/),
  ),
  bytes: Schema.Uint8Array.check(Schema.isMinLength(1), Schema.isMaxLength(8 * 1024 * 1024)),
}) {}

/** Confirms selection in the input. It does not confirm a website received or submitted the file. */
export class BrowserFileSelectionResult extends Schema.Class<BrowserFileSelectionResult>(
  "BrowserFileSelectionResult",
)({
  url: InteractiveBrowserTargetUrl,
  fileName: BrowserSelectFileRequest.fields.fileName,
  mediaType: BrowserSelectFileRequest.fields.mediaType,
  size: PositiveInt.check(Schema.isLessThanOrEqualTo(8 * 1024 * 1024)),
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

/** Safe execution evidence, independent of provider messages and application receipts.
 * completed proves SDK input completion, never website acceptance or durable settlement.
 */
export const InteractiveBrowserFailureEvidence = Schema.Struct({
  stage: Schema.Literals(["preparation", "input", "observation"]),
  dispatch: Schema.Literals(["not-dispatched", "completed", "unknown", "running"]),
  session: Schema.Literals(["attached", "disconnected", "lost"]),
});

export type InteractiveBrowserFailureEvidence = typeof InteractiveBrowserFailureEvidence.Type;

export class InteractiveBrowserPolicyDeniedError extends Schema.TaggedError<InteractiveBrowserPolicyDeniedError>()(
  "InteractiveBrowserPolicyDeniedError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    evidence: Schema.optionalKey(InteractiveBrowserFailureEvidence),
  },
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
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    evidence: Schema.optionalKey(InteractiveBrowserFailureEvidence),
  },
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
      "select-file",
      "screenshot",
      "scroll",
      "close",
    ]),
    message: BoundedMessage,
    evidence: Schema.optionalKey(InteractiveBrowserFailureEvidence),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

export class InteractiveBrowserProtocolError extends Schema.TaggedError<InteractiveBrowserProtocolError>()(
  "InteractiveBrowserProtocolError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    evidence: Schema.optionalKey(InteractiveBrowserFailureEvidence),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

export class InteractiveBrowserLimitError extends Schema.TaggedError<InteractiveBrowserLimitError>()(
  "InteractiveBrowserLimitError",
  {
    implementation: SandboxImplementation,
    evidence: Schema.optionalKey(InteractiveBrowserFailureEvidence),
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
      "select-file",
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
  readonly selectFile: (
    request: BrowserSelectFileRequest,
  ) => Effect.Effect<BrowserFileSelectionResult, InteractiveBrowserError>;
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
