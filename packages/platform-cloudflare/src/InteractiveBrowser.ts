/// <reference types="@cloudflare/workers-types" />

import {
  Cause,
  Context,
  Clock,
  Duration,
  Effect,
  Layer,
  Redacted,
  Schema,
  Semaphore,
  Scope,
} from "effect";
import {
  BrowserActionResult,
  BrowserExpectedTargetState,
  BrowserSelectFileRequest,
  BrowserFileSelectionResult,
  BrowserScreenshotRequest,
  BrowserScrollRequest,
  BrowserNavigationResult,
  BrowserTextResult,
  InteractiveBrowser,
  InteractiveBrowserActionError,
  InteractiveBrowserBusyError,
  InteractiveBrowserCapacityError,
  InteractiveBrowserExpiredError,
  InteractiveBrowserLimitError,
  InteractiveBrowserPolicy,
  InteractiveBrowserPolicyDeniedError,
  InteractiveBrowserProtocolError,
  InteractiveBrowserTargetUrl,
  InteractiveBrowserUnsupportedError,
  type BrowserHandle,
  type BrowserExpectedTarget,
  type InteractiveBrowserError,
  type InteractiveBrowserFailureEvidence,
  type InteractiveBrowserNetworkPolicy,
} from "effect-agent/interactive-browser";
import { PageScreenshotResult } from "effect-agent/page-screenshot";
import { SandboxImplementation } from "effect-agent/sandbox";
import {
  type Browser,
  type BrowserContext,
  type CDPSession,
  type HTTPRequest,
  type Page,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { BrowserRunBinding } from "./internal/browser-binding.ts";
import {
  BrowserRunFailure,
  reportBrowserCause,
  reportedBrowserError,
} from "./internal/browser-failure.ts";
import { makeFileSelection } from "./internal/browser-file-selection.ts";
import {
  BrowserRunSessionLifecycle,
  type BrowserRunLifecycleOptions,
} from "./internal/browser-session-lifecycle.ts";

export {
  BrowserRunCleanupError,
  BrowserRunSessionLifecycle,
} from "./internal/browser-session-lifecycle.ts";

export const browserRunInteractiveImplementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "cloudflare-browser-run-interactive",
});

const MIN_KEEP_ALIVE_MILLIS = 10_000;
const MAX_KEEP_ALIVE_MILLIS = 600_000;
const MAX_TEXT_LENGTH = 8 * 1024 * 1024;
const MAX_SCREENSHOT_BYTES = 8 * 1024 * 1024;
const MIN_LIVE_VIEW_EXPIRY_MILLIS = 60_000;
const MAX_LIVE_VIEW_EXPIRY_MILLIS = 60 * 60_000;
const MAX_HANDOFF_TIMEOUT_MILLIS = 30 * 60_000;
const MAX_HOST_TEXT_LENGTH = 8 * 1024;
const CLEANUP_STEP_TIMEOUT_MILLIS = 10_000;
const ACTION_NETWORK_QUIET_MILLIS = 200;
const ACTION_NETWORK_SETTLE_MILLIS = 2_000;
const ACTION_POST_STATE_MILLIS = 250;
const MAX_OBSERVED_CONTROLS = 64;
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

const BoundedHostText = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(MAX_HOST_TEXT_LENGTH),
);

const BoundedRemoteText = Schema.String.check(Schema.isMaxLength(MAX_TEXT_LENGTH));

const TextObservation = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Text"), text: BoundedRemoteText }),
  Schema.Struct({ _tag: Schema.Literal("MissingElement") }),
  Schema.Struct({ _tag: Schema.Literal("OverLimit"), observed: Schema.Natural }),
]);

const ActionTargetState = Schema.Struct({
  matchCount: Schema.Natural,
  invalidSelector: Schema.optionalKey(Schema.Boolean),
  kind: Schema.optionalKey(
    Schema.Literals(["button", "checkbox", "radio", "select", "text", "link", "other"]),
  ),
  checked: Schema.optionalKey(Schema.Boolean),
  selected: Schema.optionalKey(Schema.Boolean),
  disabled: Schema.optionalKey(Schema.Boolean),
  required: Schema.optionalKey(Schema.Boolean),
  valid: Schema.optionalKey(Schema.Boolean),
  formValid: Schema.optionalKey(Schema.Boolean),
});

const ActionNetworkState = Schema.Struct({
  total: Schema.Natural,
  status2xx: Schema.Natural,
  status3xx: Schema.Natural,
  status4xx: Schema.Natural,
  status5xx: Schema.Natural,
  failed: Schema.Natural,
  pending: Schema.Natural,
  settleTimedOut: Schema.Boolean,
});

const ActionObservation = Schema.Struct({
  before: ActionTargetState,
  after: Schema.optionalKey(ActionTargetState),
  afterUnavailable: Schema.Boolean,
  network: ActionNetworkState,
  inputMillis: Schema.optionalKey(Schema.Finite),
  observationMillis: Schema.optionalKey(Schema.Finite),
});

export const BrowserRunPageObservation = Schema.fromJsonString(
  Schema.Struct({
    documentId: Schema.NonEmptyString,
    pageText: BoundedRemoteText,
    selectorMatchCount: Schema.Natural,
    controlsTruncated: Schema.Boolean,
    controls: Schema.Array(
      Schema.Struct({
        nodeId: Schema.NonEmptyString,
        selector: BoundedRemoteText,
        ...BrowserExpectedTargetState.fields,
      }),
    ).check(Schema.isMaxLength(MAX_OBSERVED_CONTROLS)),
  }),
);

const PngBytes = Schema.Uint8Array.check(
  Schema.isMaxLength(MAX_SCREENSHOT_BYTES),
  Schema.makeFilter(
    (bytes) =>
      bytes.length >= 8 &&
      bytes[0] === 0x89 &&
      bytes[1] === 0x50 &&
      bytes[2] === 0x4e &&
      bytes[3] === 0x47 &&
      bytes[4] === 0x0d &&
      bytes[5] === 0x0a &&
      bytes[6] === 0x1a &&
      bytes[7] === 0x0a,
    { title: "PNG bytes" },
  ),
);

const BrowserRunSessionId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
  Schema.makeFilter((value) => /^[A-Za-z0-9_-]+$/.test(value), {
    title: "a Browser Run session identifier",
  }),
);

const LiveViewUrl = Schema.String.check(
  Schema.isMaxLength(MAX_HOST_TEXT_LENGTH),
  Schema.makeFilter(
    (value) => {
      try {
        const url = new URL(value);

        return (
          url.protocol === "https:" &&
          url.host === "live.browser.run" &&
          url.username === "" &&
          url.password === "" &&
          url.pathname === "/ui/view" &&
          url.searchParams.get("mode") === "tab" &&
          (url.searchParams.get("wss") ?? "").startsWith("live.browser.run/api/devtools/browser/")
        );
      } catch {
        return false;
      }
    },
    { title: "a Cloudflare Live View HTTPS URL" },
  ),
);

const LiveViewObservation = Schema.Struct({ devtoolsFrontendUrl: LiveViewUrl });
const HandoffObservation = Schema.Struct({ handoffId: BoundedHostText });

const HandoffDuration = Schema.Natural.check(
  Schema.isLessThanOrEqualTo(MAX_HANDOFF_TIMEOUT_MILLIS),
);

const HandoffStateObservation = Schema.Union([
  Schema.Struct({
    active: Schema.Literal(true),
    handoffId: BoundedHostText,
    durationMs: HandoffDuration,
  }),
  Schema.Struct({
    active: Schema.Literal(false),
    handoffId: Schema.optionalKey(BoundedHostText),
    durationMs: Schema.optionalKey(HandoffDuration),
  }),
]);

/**
 * Host presentation state in CSS pixels. Dimensions are integers in 1..2048,
 * density is finite in 1..2 (default 1), and neither scaled dimension may exceed
 * 2048 pixels. Mobile, touch, and orientation emulation are not supported.
 */
export class BrowserRunViewport extends Schema.Class<BrowserRunViewport>("BrowserRunViewport")(
  Schema.Struct({
    width: PositiveInt.check(Schema.isLessThanOrEqualTo(2_048)),
    height: PositiveInt.check(Schema.isLessThanOrEqualTo(2_048)),
    deviceScaleFactor: Schema.optionalKey(
      Schema.Finite.check(Schema.isBetween({ minimum: 1, maximum: 2 })),
    ),
  }).check(
    Schema.makeFilter(
      (viewport) =>
        Math.max(viewport.width, viewport.height) * (viewport.deviceScaleFactor ?? 1) <= 2_048,
      { title: "a viewport with scaled dimensions no larger than 2048 pixels" },
    ),
  ),
) {}

const decodeViewport = (input: BrowserRunViewport) =>
  Schema.decodeEffect(BrowserRunViewport)(input, { onExcessProperty: "error" }).pipe(
    Effect.mapError(() => policyError("The browser viewport is malformed")),
    // Copy only presentation fields, including for Schema class instances.
    Effect.map((viewport) => ({
      width: viewport.width,
      height: viewport.height,
      deviceScaleFactor: viewport.deviceScaleFactor ?? 1,
    })),
  );

/** Host-only request for a redacted Cloudflare Live View URL. */
export class BrowserRunLiveViewRequest extends Schema.Class<BrowserRunLiveViewRequest>(
  "BrowserRunLiveViewRequest",
)({
  mode: Schema.Literal("tab"),
  expiresInMs: PositiveInt.check(
    Schema.isBetween({
      minimum: MIN_LIVE_VIEW_EXPIRY_MILLIS,
      maximum: MAX_LIVE_VIEW_EXPIRY_MILLIS,
    }),
  ),
}) {}

export class BrowserRunLiveViewResult extends Schema.Class<BrowserRunLiveViewResult>(
  "BrowserRunLiveViewResult",
)({ devtoolsFrontendUrl: Schema.Redacted(LiveViewUrl) }) {}

/** Start one bounded handoff; controller ownership remains a consumer concern. */
export class BrowserRunHandoffRequest extends Schema.Class<BrowserRunHandoffRequest>(
  "BrowserRunHandoffRequest",
)({
  instructions: BoundedHostText.check(Schema.isMaxLength(1_024)),
  timeout: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_HANDOFF_TIMEOUT_MILLIS)),
}) {}

export class BrowserRunHandoffResult extends Schema.Class<BrowserRunHandoffResult>(
  "BrowserRunHandoffResult",
)({ handoffId: Schema.Redacted(BoundedHostText) }) {}

export class BrowserRunHandoffState extends Schema.Class<BrowserRunHandoffState>(
  "BrowserRunHandoffState",
)({
  active: Schema.Boolean,
  handoffId: Schema.optionalKey(Schema.Redacted(BoundedHostText)),
  durationMs: Schema.optionalKey(HandoffDuration),
}) {}

type BrowserFailure = typeof InteractiveBrowserError.Type;
type BrowserOperation = InteractiveBrowserActionError["operation"];

interface InteractiveBrowserPolicySnapshot {
  readonly network: Exclude<InteractiveBrowserNetworkPolicy, { readonly _tag: "PublicWeb" }>;
  readonly maxActions: number;
  readonly maxElapsedMillis: number;
  readonly maxReturnedBytes: number;
}

/** One intercepted Puppeteer request, narrowed to the policy-relevant surface. */
export interface BrowserRunInteractiveRequest {
  readonly url: () => string;
  readonly abort: () => Promise<void>;
  readonly continue: () => Promise<void>;
}

export type BrowserRunInteractiveRequestListener = (request: BrowserRunInteractiveRequest) => void;

export type BrowserRunCloudflareCommand =
  | "Cloudflare.getLiveView"
  | "Cloudflare.handoff"
  | "Cloudflare.getHandoffState";

/** Narrow CDP boundary for Cloudflare commands absent from the pinned protocol types. */
export interface BrowserRunInteractiveCdpSession {
  readonly send: (command: BrowserRunCloudflareCommand, parameters: unknown) => Promise<unknown>;
  readonly detach: () => Promise<void>;
}

/** Narrow page boundary used by deterministic tests; SDK values remain in this package. */
export interface BrowserRunInteractivePage {
  readonly identity?: () => Promise<BrowserRunPageIdentity>;
  readonly close: () => Promise<void>;
  readonly setBypassServiceWorker: (enabled: boolean) => Promise<void>;
  readonly setRequestInterception: (enabled: boolean) => Promise<void>;
  readonly onRequest: (listener: BrowserRunInteractiveRequestListener) => void;
  readonly offRequest: (listener: BrowserRunInteractiveRequestListener) => void;
  readonly goto: (url: string) => Promise<void>;
  readonly url: () => unknown;
  readonly readText: (selector: string | undefined, maximumBytes: number) => Promise<unknown>;
  readonly fill: (
    selector: string,
    value: string,
    signal: AbortSignal,
    onDispatch: () => void,
    onComplete?: () => void,
    expectedTarget?: BrowserExpectedTarget,
  ) => Promise<unknown>;
  readonly click: (
    selector: string,
    signal: AbortSignal,
    onDispatch: () => void,
    onComplete?: () => void,
    expectedTarget?: BrowserExpectedTarget,
  ) => Promise<unknown>;
  readonly selectFile: (
    request: BrowserSelectFileRequest,
    signal: AbortSignal,
    onDispatch: () => void,
    onComplete?: () => void,
  ) => Promise<unknown>;
  readonly screenshot: (fullPage: boolean) => Promise<unknown>;
  readonly scroll: (deltaX: number, deltaY: number) => Promise<void>;
  readonly setViewport: (viewport: BrowserRunViewport) => Promise<void>;
  readonly createCdpSession: () => Promise<BrowserRunInteractiveCdpSession>;
}

export interface BrowserRunInteractiveContext {
  readonly newPage: () => Promise<BrowserRunInteractivePage>;
  readonly close: () => Promise<void>;
}

export interface BrowserRunInteractiveBrowser {
  readonly detach?: () => Promise<void>;
  readonly reattach?: (
    identity: BrowserRunPageIdentity,
  ) => Promise<
    | { readonly context: BrowserRunInteractiveContext; readonly page: BrowserRunInteractivePage }
    | undefined
  >;
  readonly createContext: () => Promise<BrowserRunInteractiveContext>;
  readonly close: () => Promise<void>;
  readonly sessionId: () => unknown;
  readonly isConnected: () => boolean;
  readonly onDisconnected: (listener: () => void) => void;
  readonly offDisconnected: (listener: () => void) => void;
}

/** Host-supplied Browser Run allocation and connection transport. */
export class BrowserRunInteractiveBinding extends Context.Service<
  BrowserRunInteractiveBinding,
  {
    readonly acquire: (keepAliveMillis: number) => Promise<unknown>;
    readonly connect: (
      sessionId: string,
      signal?: AbortSignal,
    ) => Promise<BrowserRunInteractiveBrowser>;
    /** Success proves whole-browser termination or exact-session absence. */
    readonly closeSession: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, InteractiveBrowserError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserRunInteractiveBinding") {
  static layer(options: {
    readonly browser: BrowserRun;
    readonly viewport?: BrowserRunViewport;
  }): Layer.Layer<
    BrowserRunInteractiveBinding,
    InteractiveBrowserPolicyDeniedError,
    BrowserRunSessionLifecycle
  > {
    return Layer.effect(BrowserRunInteractiveBinding)(
      Effect.gen(function* () {
        const lifecycle = yield* BrowserRunSessionLifecycle;
        const binding = yield* BrowserRunBinding;

        const viewport =
          options.viewport === undefined ? undefined : yield* decodeViewport(options.viewport);

        return {
          acquire: async (keepAliveMillis: number) =>
            binding.acquire(keepAliveMillis, "interactive.acquire"),
          connect: async (sessionId: string, signal?: AbortSignal) =>
            makeProductionBrowser(
              await binding.connect(sessionId, "interactive.connect", signal).browser,
              sessionId,
              viewport,
            ),
          closeSession: (sessionId: Redacted.Redacted<string>) =>
            lifecycle
              .close(sessionId)
              .pipe(Effect.mapError((cause) => actionError("close", cause))),
        };
      }),
    ).pipe(Layer.provide(BrowserRunBinding.layer(options.browser)));
  }
}

/** Private target identity; never include this checkpoint in model-visible records. */
export class BrowserRunPageIdentity extends Schema.Class<BrowserRunPageIdentity>(
  "BrowserRunPageIdentity",
)({
  contextId: BrowserRunSessionId,
  targetId: BrowserRunSessionId,
}) {}

/** Host-owned recovery data. Persist with the application's input receipts and ownership fence. */
export class BrowserRunInteractiveCheckpoint extends Schema.Class<BrowserRunInteractiveCheckpoint>(
  "BrowserRunInteractiveCheckpoint",
)({
  sessionId: Schema.RedactedFromValue(BrowserRunSessionId),
  page: BrowserRunPageIdentity,
  policy: InteractiveBrowserPolicy,
  startedAt: Schema.Natural,
  consumedActions: Schema.Natural.check(Schema.isLessThanOrEqualTo(1_000)),
  inputState: Schema.Literals(["idle", "running", "unknown"]),
}) {}

export type BrowserRunInputState = "idle" | "running" | "unknown";

export interface BrowserRunInteractiveSession {
  readonly handle: BrowserHandle;
  readonly checkpoint: Effect.Effect<BrowserRunInteractiveCheckpoint, InteractiveBrowserError>;
  /** Invalidate this owner and disconnect locally, retaining the exact remote page until its deadline.
   * Checkpoint and durable input receipts must be persisted first. Never grants another owner authority.
   */
  readonly detach: Effect.Effect<void, InteractiveBrowserError>;
  readonly inputState: Effect.Effect<BrowserRunInputState>;
  /** Wait at most 500 ms for local SDK input. A restart/transport-unknown fence cannot be drained. */
  readonly drainInput: Effect.Effect<BrowserRunInputState>;
  readonly sessionId: Redacted.Redacted<string>;
  /**
   * Resize without charging an agent action or changing emulation modes. Shares
   * the handle's fail-fast lock, page-policy preflight, and elapsed deadline.
   * An interrupted resize fences further input until the SDK settles; no resize is retried.
   * The host must authorize callers. No viewer ownership or durable state is added.
   */
  readonly resizeViewport: (
    viewport: BrowserRunViewport,
  ) => Effect.Effect<void, InteractiveBrowserError>;
  readonly getLiveView: (
    request: BrowserRunLiveViewRequest,
  ) => Effect.Effect<BrowserRunLiveViewResult, InteractiveBrowserError>;
  readonly handoff: (
    request: BrowserRunHandoffRequest,
  ) => Effect.Effect<BrowserRunHandoffResult, InteractiveBrowserError>;
  readonly getHandoffState: Effect.Effect<BrowserRunHandoffState, InteractiveBrowserError>;
  readonly close: Effect.Effect<void, InteractiveBrowserError>;
}

/** Persist the private cleanup identity before connecting or creating a page. */
export interface BrowserRunInteractiveAcquisition {
  readonly sessionId: Redacted.Redacted<string>;
  /** Connect at most once, within the acquisition's original scope and elapsed deadline. */
  readonly connect: Effect.Effect<BrowserRunInteractiveSession, InteractiveBrowserError>;
}

/** Cloudflare host authority kept separate from the provider-neutral browser handle. */
export class BrowserRunInteractiveHost extends Context.Service<
  BrowserRunInteractiveHost,
  {
    readonly cleanupSemantics?: "confirmed-terminal";
    readonly acquire: (
      policy: InteractiveBrowserPolicy,
    ) => Effect.Effect<BrowserRunInteractiveAcquisition, InteractiveBrowserError, Scope.Scope>;
    readonly open: (
      policy: InteractiveBrowserPolicy,
    ) => Effect.Effect<BrowserRunInteractiveSession, InteractiveBrowserError, Scope.Scope>;
    /** Requires exclusive host ownership. Never allocates a replacement page or replays an action.
     * pendingInput must include durable receipts written after the checkpoint. Unknown input keeps
     * mutations and human control fenced; reads remain available. Only confirmed close ends that fence.
     */
    readonly resume: (
      checkpoint: BrowserRunInteractiveCheckpoint,
      options: { readonly pendingInput: boolean },
    ) => Effect.Effect<BrowserRunInteractiveSession, InteractiveBrowserError, Scope.Scope>;
    readonly closeSession: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<void, InteractiveBrowserError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserRunInteractiveHost") {}

const makeProductionRequest = (request: HTTPRequest): BrowserRunInteractiveRequest => ({
  url: () => request.url(),
  abort: () => request.abort("blockedbyclient"),
  continue: () => request.continue(),
});

const makeProductionCdpSession = (session: CDPSession): BrowserRunInteractiveCdpSession => ({
  send: async (command, parameters) => {
    const send = Reflect.get(session, "send");

    return await Reflect.apply(send, session, [command, parameters]);
  },
  detach: () => session.detach(),
});

class BrowserRunActionUndispatched extends Error {
  readonly matchCount: number;

  constructor(matchCount: number) {
    super("The browser action selector did not resolve to exactly one element");
    this.matchCount = matchCount;
  }
}

const readActionTarget = (page: Page, selector: string): Promise<unknown> =>
  page.evaluate((requestedSelector) => {
    const pageDocument = Reflect.get(globalThis, "document");
    let matches;

    try {
      matches = Reflect.apply(Reflect.get(pageDocument, "querySelectorAll"), pageDocument, [
        requestedSelector,
      ]);
    } catch (cause) {
      if (cause instanceof Error && cause.name === "SyntaxError") {
        return { matchCount: 0, invalidSelector: true };
      }
      throw cause;
    }
    if (typeof matches !== "object" || matches === null) throw new Error("Invalid query result");
    const matchCount = Math.min(10_000, Reflect.get(matches, "length"));
    const element = Reflect.get(matches, 0);

    if (element === undefined) return { matchCount };

    const associated = Reflect.get(element, "control") ?? element;
    const tagName = String(Reflect.get(associated, "tagName") ?? "").toLowerCase();
    const inputType = String(Reflect.get(associated, "type") ?? "").toLowerCase();

    const role = String(
      Reflect.apply(Reflect.get(element, "getAttribute"), element, ["role"]) ?? "",
    ).toLowerCase();

    const kind =
      tagName === "button" || role === "button"
        ? "button"
        : inputType === "checkbox" || role === "checkbox" || role === "switch"
          ? "checkbox"
          : inputType === "radio" || role === "radio"
            ? "radio"
            : tagName === "select"
              ? "select"
              : tagName === "input" || tagName === "textarea"
                ? "text"
                : tagName === "a"
                  ? "link"
                  : "other";

    const checked = Reflect.get(associated, "checked");

    const selected =
      tagName === "select"
        ? Reflect.get(associated, "selectedIndex") >= 0
        : Reflect.get(associated, "selected");

    const disabled = Reflect.get(associated, "disabled");
    const required = Reflect.get(associated, "required");
    const validity = Reflect.get(associated, "validity");
    const form = Reflect.get(associated, "form");

    const formMatches =
      form === null || form === undefined ? undefined : Reflect.get(form, "matches");

    const ariaChecked = Reflect.apply(Reflect.get(element, "getAttribute"), element, [
      "aria-checked",
    ]);

    const ariaDisabled = Reflect.apply(Reflect.get(element, "getAttribute"), element, [
      "aria-disabled",
    ]);

    const ariaSelected = Reflect.apply(Reflect.get(element, "getAttribute"), element, [
      "aria-selected",
    ]);

    return {
      matchCount,
      kind,
      ...(typeof checked === "boolean"
        ? { checked }
        : ariaChecked === "true" || ariaChecked === "false"
          ? { checked: ariaChecked === "true" }
          : {}),
      ...(typeof selected === "boolean"
        ? { selected }
        : ariaSelected === "true" || ariaSelected === "false"
          ? { selected: ariaSelected === "true" }
          : {}),
      disabled: typeof disabled === "boolean" ? disabled : ariaDisabled === "true",
      ...(typeof required === "boolean" ? { required } : {}),
      ...(validity !== undefined && typeof Reflect.get(validity, "valid") === "boolean"
        ? { valid: Reflect.get(validity, "valid") }
        : {}),
      ...(typeof formMatches === "function"
        ? { formValid: Reflect.apply(formMatches, form, [":valid"]) }
        : {}),
    };
  }, selector);

// SDK promises cannot be cancelled. Clear our timer and observe late rejections;
// the owning browser Scope is responsible for terminating the remote session.
const boundedBestEffort = async <A>(
  promise: Promise<A>,
  millis: number,
): Promise<A | undefined> => {
  let timer: ReturnType<typeof setTimeout> | number | undefined;

  try {
    return await Promise.race([
      promise.catch(() => undefined),
      new Promise<undefined>((resolve) => {
        timer = setTimeout(resolve, millis);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
};

const readActionTargetAfter = (page: Page, selector: string) =>
  boundedBestEffort(
    readActionTarget(page, selector).then(Schema.decodeUnknownSync(ActionTargetState)),
    ACTION_POST_STATE_MILLIS,
  );

const makeActionRequestTracker = (page: Page, signal: AbortSignal) => {
  const pending = new Set<HTTPRequest>();
  let total = 0;
  let status2xx = 0;
  let status3xx = 0;
  let status4xx = 0;
  let status5xx = 0;
  let failed = 0;
  let lastChange = performance.now();
  let closed = false;
  let wake: (() => void) | undefined;
  let timer: ReturnType<typeof setTimeout> | number | undefined;

  const relevant = (request: HTTPRequest) => {
    const resourceType = request.resourceType();

    return resourceType === "fetch" || resourceType === "xhr";
  };

  const onRequest = (request: HTTPRequest) => {
    if (!relevant(request)) return;
    pending.add(request);
    total++;
    lastChange = performance.now();
  };

  const onFinished = (request: HTTPRequest) => {
    if (!pending.delete(request)) return;
    let status: number | undefined;

    try {
      status = request.response()?.status();
    } catch {
      // Provider response details remain intentionally unavailable to diagnostics.
    }
    if (status !== undefined) {
      if (status >= 200 && status < 300) status2xx++;
      else if (status >= 300 && status < 400) status3xx++;
      else if (status >= 400 && status < 500) status4xx++;
      else if (status >= 500 && status < 600) status5xx++;
    }
    lastChange = performance.now();
  };

  const onFailed = (request: HTTPRequest) => {
    if (!pending.delete(request)) return;
    failed++;
    lastChange = performance.now();
  };

  const close = () => {
    if (closed) return;
    closed = true;
    page.off("request", onRequest);
    page.off("requestfinished", onFinished);
    page.off("requestfailed", onFailed);
    signal.removeEventListener("abort", close);
    clearTimeout(timer);
    wake?.();
  };

  try {
    page.on("request", onRequest);
    page.on("requestfinished", onFinished);
    page.on("requestfailed", onFailed);
    signal.addEventListener("abort", close, { once: true });
    if (signal.aborted) close();
  } catch (cause) {
    close();
    throw cause;
  }

  const wait = async () => {
    const startedAt = performance.now();
    let settleTimedOut = false;

    while (!closed) {
      const now = performance.now();

      if (pending.size === 0 && now - lastChange >= ACTION_NETWORK_QUIET_MILLIS) break;
      if (now - startedAt >= ACTION_NETWORK_SETTLE_MILLIS) {
        settleTimedOut = true;
        break;
      }
      await new Promise<void>((resolve) => {
        wake = resolve;
        timer = setTimeout(() => resolve(), 50);
      });
      wake = undefined;
    }

    return {
      total,
      status2xx,
      status3xx,
      status4xx,
      status5xx,
      failed,
      pending: pending.size,
      settleTimedOut,
    };
  };

  return {
    wait,
    close,
    markActionSettled: () => {
      lastChange = performance.now();
    },
  };
};

const disposeActionHandles = async (handles: ReadonlyArray<{ dispose: () => Promise<void> }>) => {
  await boundedBestEffort(
    Promise.allSettled(handles.map(async (handle) => handle.dispose())),
    ACTION_POST_STATE_MILLIS,
  );
};

/** Runs entirely in the page realm for both observations and the last bound-element guard. */
const observePage = (
  target: object | undefined,
  requestedSelector: string | undefined,
  maximum: number,
  maximumControls: number,
  input?: {
    readonly mode: "click" | "fill";
    readonly expectedTarget: BrowserExpectedTarget;
    readonly value?: string;
  },
) => {
  const pageDocument = Reflect.get(globalThis, "document");

  const matches =
    requestedSelector === undefined
      ? undefined
      : Reflect.apply(Reflect.get(pageDocument, "querySelectorAll"), pageDocument, [
          requestedSelector,
        ]);

  const selectorMatchCount =
    matches === undefined || matches === null
      ? 1
      : Math.min(10_000, Reflect.get(matches, "length"));

  const element =
    target ??
    (matches === undefined || matches === null
      ? Reflect.get(pageDocument, "body")
      : Reflect.get(matches, 0));

  if (target !== undefined && Reflect.get(target, "isConnected") !== true)
    return { _tag: "MissingElement" };

  if (element === null) return { _tag: "MissingElement" };
  if (element === undefined) return { _tag: "MissingElement" };
  const innerText = Reflect.get(element, "innerText");
  const textContent = Reflect.get(element, "textContent");

  const pageText =
    typeof innerText === "string" ? innerText : typeof textContent === "string" ? textContent : "";

  const primaryControlSelector =
    'input,select,textarea,label,button,[role="checkbox"],[role="radio"],[role="option"],[role="switch"],[role="tab"],[role="button"]';

  const optionSelector = "select option";
  const secondaryControlSelector = "a[href]";
  const controlSelector = `${primaryControlSelector},${optionSelector},${secondaryControlSelector}`;

  const selectorFor = (candidate: object) => {
    const parts: Array<string> = [];
    let current: object | null = candidate;

    while (current !== null && current !== undefined) {
      const tagName = String(Reflect.get(current, "tagName") ?? "").toLowerCase();

      if (tagName === "") break;
      const parent: object | null = Reflect.get(current, "parentElement");

      if (parent === null) {
        parts.push(tagName);
        break;
      }
      let sibling = Reflect.get(current, "previousElementSibling");
      let index = 1;

      while (sibling !== null && sibling !== undefined) {
        if (String(Reflect.get(sibling, "tagName") ?? "").toLowerCase() === tagName) index++;
        sibling = Reflect.get(sibling, "previousElementSibling");
      }
      parts.push(`${tagName}:nth-of-type(${index})`);
      current = parent;
    }

    return parts.reverse().join(" > ");
  };

  const visible = (candidate: object) => {
    const hidden = Reflect.get(candidate, "hidden");

    const ariaHidden = Reflect.apply(Reflect.get(candidate, "getAttribute"), candidate, [
      "aria-hidden",
    ]);

    const rects = Reflect.apply(Reflect.get(candidate, "getClientRects"), candidate, []);

    return (
      hidden !== true &&
      ariaHidden !== "true" &&
      typeof rects === "object" &&
      rects !== null &&
      Reflect.get(rects, "length") > 0
    );
  };

  const candidates: Array<object> = [];
  let controlsTruncated = false;

  const consider = (candidate: object) => {
    const tagName = String(Reflect.get(candidate, "tagName") ?? "").toLowerCase();

    // Collapsed native options have no client rects. Their owning
    // select determines visibility; observe text/selection, never value.
    const visibilityTarget =
      tagName === "option"
        ? Reflect.apply(Reflect.get(candidate, "closest"), candidate, ["select"])
        : candidate;

    const actionable = tagName !== "label" || Reflect.get(candidate, "control") !== null;

    if (
      !actionable ||
      typeof visibilityTarget !== "object" ||
      visibilityTarget === null ||
      !visible(visibilityTarget)
    )
      return;
    candidates.push(candidate);
    if (candidates.length > maximumControls) controlsTruncated = true;
  };

  const elementMatches = Reflect.get(element, "matches");

  if (
    typeof elementMatches === "function" &&
    Reflect.apply(elementMatches, element, [controlSelector])
  ) {
    consider(element);
  }

  const considerSelector = (candidateSelector: string) => {
    const descendants = Reflect.apply(Reflect.get(element, "querySelectorAll"), element, [
      candidateSelector,
    ]);

    if (typeof descendants !== "object" || descendants === null) return;
    const descendantCount = Reflect.get(descendants, "length");

    for (let index = 0; index < descendantCount && !controlsTruncated; index++) {
      consider(Reflect.get(descendants, index));
    }
  };

  considerSelector(primaryControlSelector);
  if (!controlsTruncated) considerSelector(optionSelector);
  if (!controlsTruncated) considerSelector(secondaryControlSelector);

  const identityKey = Symbol.for("@effect-agent/browser-observation");
  let identity = Reflect.get(globalThis, identityKey);

  if (identity === undefined || identity.document !== pageDocument) {
    identity = {
      document: pageDocument,
      documentId: crypto.getRandomValues(new Uint32Array(4)).join("-"),
      nodes: new WeakMap<object, string>(),
    };
    Reflect.set(globalThis, identityKey, identity);
  }

  const controls = candidates.slice(0, maximumControls).map((candidate) => {
    let nodeId = identity.nodes.get(candidate);

    if (nodeId === undefined) {
      nodeId = crypto.getRandomValues(new Uint32Array(4)).join("-");
      identity.nodes.set(candidate, nodeId);
    }
    const associated = Reflect.get(candidate, "control") ?? candidate;
    const tagName = String(Reflect.get(candidate, "tagName") ?? "").toLowerCase();
    const inputType = String(Reflect.get(associated, "type") ?? "").toLowerCase();

    const role = String(
      Reflect.apply(Reflect.get(candidate, "getAttribute"), candidate, ["role"]) ?? "",
    ).toLowerCase();

    const ariaLabel = Reflect.apply(Reflect.get(candidate, "getAttribute"), candidate, [
      "aria-label",
    ]);

    const candidateText =
      tagName === "textarea" || tagName === "input" || tagName === "select"
        ? undefined
        : Reflect.get(candidate, tagName === "option" ? "label" : "innerText");

    const associatedLabels = Reflect.get(associated, "labels");

    const associatedLabel =
      associatedLabels !== undefined &&
      associatedLabels !== null &&
      Reflect.get(associatedLabels, "length") > 0
        ? Reflect.get(Reflect.get(associatedLabels, 0), "innerText")
        : undefined;

    const label = String(
      typeof ariaLabel === "string" && ariaLabel !== ""
        ? ariaLabel
        : typeof candidateText === "string" && candidateText !== ""
          ? candidateText
          : (associatedLabel ?? ""),
    )
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 200);

    const checked = Reflect.get(associated, "checked");

    const selected =
      tagName === "select"
        ? Reflect.get(associated, "selectedIndex") >= 0
        : Reflect.get(associated, "selected");

    const disabled = Reflect.get(associated, "disabled");
    const required = Reflect.get(associated, "required");
    const validity = Reflect.get(associated, "validity");
    const form = Reflect.get(associated, "form");

    const formMatches =
      form === null || form === undefined ? undefined : Reflect.get(form, "matches");

    const ariaChecked = Reflect.apply(Reflect.get(candidate, "getAttribute"), candidate, [
      "aria-checked",
    ]);

    const ariaSelected = Reflect.apply(Reflect.get(candidate, "getAttribute"), candidate, [
      "aria-selected",
    ]);

    const ariaDisabled = Reflect.apply(Reflect.get(candidate, "getAttribute"), candidate, [
      "aria-disabled",
    ]);

    return {
      nodeId,
      ...(inputType === "" ? {} : { inputType: inputType.slice(0, 64) }),
      selector: selectorFor(candidate),
      kind: (tagName === "label"
        ? `label:${inputType || "control"}`
        : tagName === "input"
          ? `input:${inputType || "text"}`
          : role !== ""
            ? `role:${role}`
            : tagName
      ).slice(0, 128),
      ...(label === "" ? {} : { label }),
      ...(typeof checked === "boolean"
        ? { checked }
        : ariaChecked === "true" || ariaChecked === "false"
          ? { checked: ariaChecked === "true" }
          : {}),
      ...(typeof selected === "boolean"
        ? { selected }
        : ariaSelected === "true" || ariaSelected === "false"
          ? { selected: ariaSelected === "true" }
          : {}),
      ...(typeof disabled === "boolean"
        ? { disabled }
        : ariaDisabled === "true" || ariaDisabled === "false"
          ? { disabled: ariaDisabled === "true" }
          : {}),
      ...(typeof required === "boolean" ? { required } : {}),
      ...(validity !== undefined && typeof Reflect.get(validity, "valid") === "boolean"
        ? { valid: Reflect.get(validity, "valid") }
        : {}),
      ...(typeof formMatches === "function"
        ? { formValid: Reflect.apply(formMatches, form, [":valid"]) }
        : {}),
    };
  });

  if (input !== undefined) {
    const expected = input.expectedTarget;
    const control = controls.find((candidate) => candidate.nodeId === expected.nodeId);

    const fields = [
      "kind",
      "inputType",
      "label",
      "checked",
      "selected",
      "disabled",
      "required",
      "valid",
      "formValid",
    ] as const;

    if (
      target === undefined ||
      identity.documentId !== expected.documentId ||
      identity.nodes.get(target) !== expected.nodeId ||
      control === undefined ||
      (expected.state !== undefined &&
        fields.some((field) => control[field] !== expected.state?.[field])) ||
      Reflect.get(target, "isConnected") !== true
    )
      return { _tag: "MissingElement" };
    if (expected.scopeSelector !== undefined) {
      let roots;

      try {
        roots = Reflect.apply(Reflect.get(pageDocument, "querySelectorAll"), pageDocument, [
          expected.scopeSelector,
        ]);
      } catch {
        return { _tag: "MissingElement" };
      }
      if (typeof roots !== "object" || roots === null) return { _tag: "MissingElement" };
      const root = Reflect.get(roots, 0);

      if (
        Reflect.get(roots, "length") !== 1 ||
        root === undefined ||
        !Reflect.apply(Reflect.get(root, "contains"), root, [target])
      )
        return { _tag: "MissingElement" };
    }
    // Validation and dispatch share one page JS task. No coordinate lookup, selector
    // re-resolution, or asynchronous boundary can select a replacement node.
    if (input.mode === "click") {
      const prototype = Reflect.get(Reflect.get(globalThis, "HTMLElement"), "prototype");

      Reflect.apply(Reflect.get(prototype, "click"), target, []);
    } else {
      let prototype = Reflect.getPrototypeOf(target);
      let setValue: ((value: string) => void) | undefined;

      while (prototype !== null) {
        const setter = Reflect.getOwnPropertyDescriptor(prototype, "value")?.set;

        if (typeof setter === "function") {
          setValue = setter;
          break;
        }
        prototype = Reflect.getPrototypeOf(prototype);
      }
      if (setValue === undefined || input.value === undefined) return { _tag: "MissingElement" };
      Reflect.apply(setValue, target, [input.value]);
      const dispatchEvent = Reflect.get(target, "dispatchEvent");

      Reflect.apply(dispatchEvent, target, [new Event("input", { bubbles: true })]);
      Reflect.apply(dispatchEvent, target, [new Event("change", { bubbles: true })]);
    }

    return { _tag: "Text", text: "" };
  }

  // JSON is the bounded wire representation of this existing text result.
  // eslint-disable-next-line no-restricted-properties
  const text = JSON.stringify({
    documentId: identity.documentId,
    pageText,
    selectorMatchCount,
    controls,
    controlsTruncated,
  });

  const observed = new TextEncoder().encode(text).byteLength;

  return observed > maximum ? { _tag: "OverLimit", observed } : { _tag: "Text", text };
};

const runObservedPageAction = async (
  page: Page,
  selector: string,
  signal: AbortSignal,
  onDispatch: () => void,
  action: (element: NonNullable<Awaited<ReturnType<Page["$"]>>>) => Promise<void>,
  validate?: (element: NonNullable<Awaited<ReturnType<Page["$"]>>>) => Promise<boolean>,
  onComplete?: () => void,
): Promise<unknown> => {
  if (signal.aborted) throw new BrowserRunActionUndispatched(0);

  const before = Schema.decodeUnknownSync(ActionTargetState)(
    await readActionTarget(page, selector),
  );

  if (before.matchCount !== 1 || before.invalidSelector === true) {
    throw new BrowserRunActionUndispatched(before.matchCount);
  }
  const matches = await page.$$(selector);

  if (matches.length !== 1 || matches[0] === undefined) {
    await disposeActionHandles(matches);
    throw new BrowserRunActionUndispatched(Math.min(10_000, matches.length));
  }
  if (signal.aborted) {
    await disposeActionHandles(matches);
    throw new BrowserRunActionUndispatched(1);
  }

  let tracker: ReturnType<typeof makeActionRequestTracker> | undefined;
  let disposing: Promise<void> | undefined;
  const dispose = () => (disposing ??= disposeActionHandles(matches));

  const onAbort = () => {
    void dispose();
  };

  signal.addEventListener("abort", onAbort, { once: true });
  try {
    if (validate !== undefined && !(await validate(matches[0])))
      throw new BrowserRunActionUndispatched(1);
    tracker = makeActionRequestTracker(page, signal);
    // No await between the final cancellation fence and SDK dispatch. Once
    // dispatched, interruption is uncertain, even if the SDK later resolves.
    if (signal.aborted) throw new BrowserRunActionUndispatched(1);
    onDispatch();
    const inputStarted = performance.now();

    await action(matches[0]);
    const inputFinished = performance.now();

    onComplete?.();
    tracker.markActionSettled();
    const network = await tracker.wait();
    const after = signal.aborted ? undefined : await readActionTargetAfter(page, selector);

    return {
      before,
      inputMillis: Math.max(0, inputFinished - inputStarted),
      observationMillis: Math.max(0, performance.now() - inputFinished),
      ...(after === undefined ? {} : { after }),
      afterUnavailable: after === undefined,
      network,
    };
  } finally {
    tracker?.close();
    signal.removeEventListener("abort", onAbort);
    await dispose();
  }
};

const guardedPageInput = async (
  element: NonNullable<Awaited<ReturnType<Page["$"]>>>,
  expectedTarget: BrowserExpectedTarget,
  mode: "click" | "fill",
  value?: string,
): Promise<void> => {
  const observed = Schema.decodeUnknownSync(TextObservation)(
    await element.evaluate(observePage, undefined, 256 * 1024, 1, {
      mode,
      expectedTarget,
      ...(value === undefined ? {} : { value }),
    }),
  );

  if (observed._tag !== "Text") throw new BrowserRunActionUndispatched(1);
};

const makeProductionPage = (page: Page): BrowserRunInteractivePage => {
  const prepareFileSelection = makeFileSelection(page);
  const listeners = new Map<BrowserRunInteractiveRequestListener, (request: HTTPRequest) => void>();

  return {
    identity: async () => {
      const cdp = await page.createCDPSession();

      try {
        const { targetInfo } = await cdp.send("Target.getTargetInfo");

        return Schema.decodeUnknownSync(BrowserRunPageIdentity)({
          contextId: targetInfo.browserContextId,
          targetId: targetInfo.targetId,
        });
      } finally {
        await cdp.detach();
      }
    },
    close: async () => {
      if (!page.browser().isConnected()) return;
      try {
        await page.close();
      } catch (cause) {
        if (!isRemoteClosure(cause)) throw cause;
      }
    },
    setBypassServiceWorker: (enabled) => page.setBypassServiceWorker(enabled),
    setRequestInterception: (enabled) => page.setRequestInterception(enabled),
    onRequest: (listener) => {
      const sdkListener = (request: HTTPRequest) => listener(makeProductionRequest(request));

      listeners.set(listener, sdkListener);
      page.on("request", sdkListener);
    },
    offRequest: (listener) => {
      const sdkListener = listeners.get(listener);

      if (sdkListener !== undefined) {
        page.off("request", sdkListener);
        listeners.delete(listener);
      }
    },
    goto: async (url) => {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    },
    url: () => page.url(),
    readText: async (selector, maximumBytes) => {
      const observation = Schema.decodeUnknownSync(TextObservation)(
        await page.evaluate(observePage, undefined, selector, maximumBytes, MAX_OBSERVED_CONTROLS),
      );

      if (observation._tag === "Text")
        Schema.decodeSync(BrowserRunPageObservation)(observation.text);

      return observation;
    },
    fill: (selector, value, signal, onDispatch, onComplete, expectedTarget) =>
      runObservedPageAction(
        page,
        selector,
        signal,
        onDispatch,
        (element) =>
          expectedTarget !== undefined
            ? guardedPageInput(element, expectedTarget, "fill", value)
            : element.evaluate((element, nextValue) => {
                // Bypass instance setters so React can detect the change when events fire.
                let prototype = Reflect.getPrototypeOf(element);
                let setValue: ((value: string) => void) | undefined;

                while (prototype !== null) {
                  const setter = Reflect.getOwnPropertyDescriptor(prototype, "value")?.set;

                  if (typeof setter === "function") {
                    setValue = setter;
                    break;
                  }
                  prototype = Reflect.getPrototypeOf(prototype);
                }
                if (setValue === undefined) {
                  throw new Error("The selector did not resolve to a fillable field");
                }
                const focus = Reflect.get(element, "focus");

                if (typeof focus === "function") Reflect.apply(focus, element, []);
                Reflect.apply(setValue, element, [nextValue]);
                const dispatchEvent = Reflect.get(element, "dispatchEvent");

                if (typeof dispatchEvent === "function") {
                  Reflect.apply(dispatchEvent, element, [new Event("input", { bubbles: true })]);
                  Reflect.apply(dispatchEvent, element, [new Event("change", { bubbles: true })]);
                }
              }, value),
        undefined,
        onComplete,
      ),
    click: (selector, signal, onDispatch, onComplete, expectedTarget) =>
      runObservedPageAction(
        page,
        selector,
        signal,
        onDispatch,
        (element) =>
          expectedTarget === undefined
            ? element.click()
            : guardedPageInput(element, expectedTarget, "click"),
        undefined,
        onComplete,
      ),
    selectFile: async (request, signal, onDispatch, onComplete) => {
      const selection = await prepareFileSelection(request, signal);

      try {
        return await runObservedPageAction(
          page,
          request.selector,
          signal,
          onDispatch,
          selection.select,
          selection.validate,
          onComplete,
        );
      } finally {
        await selection.close();
      }
    },
    // Puppeteer materializes the complete image before returning. The adapter
    // validates the 8 MiB Schema ceiling and pass limit immediately afterward.
    screenshot: (fullPage) => page.screenshot({ type: "png", fullPage }),
    scroll: (deltaX, deltaY) =>
      page.evaluate(
        (x, y) => {
          const scrollBy = Reflect.get(globalThis, "scrollBy");

          Reflect.apply(scrollBy, globalThis, [{ left: x, top: y, behavior: "instant" }]);
        },
        deltaX,
        deltaY,
      ),
    createCdpSession: async () => makeProductionCdpSession(await page.createCDPSession()),
    setViewport: (viewport) => page.setViewport(viewport),
  };
};

const makeProductionContext = (
  context: BrowserContext,
  viewport?: BrowserRunViewport,
): BrowserRunInteractiveContext => ({
  newPage: async () => {
    const page = await context.newPage();

    if (viewport !== undefined) await page.setViewport(viewport);

    return makeProductionPage(page);
  },
  close: async () => {
    if (!context.browser().isConnected()) return;
    try {
      await context.close();
    } catch (cause) {
      if (!isRemoteClosure(cause)) throw cause;
    }
  },
});

const makeProductionBrowser = (
  browser: Browser,
  sessionId: string,
  viewport?: BrowserRunViewport,
): BrowserRunInteractiveBrowser => ({
  detach: () => browser.disconnect(),
  reattach: async (identity) => {
    const context = browser
      .browserContexts()
      .find((candidate) => candidate.id === identity.contextId);

    if (context === undefined) return undefined;
    const pages = await context.pages();

    if (pages.length > 64) throw new Error("The browser target inventory exceeds its bound");
    for (const candidate of pages) {
      const page = makeProductionPage(candidate);
      const current = await page.identity?.();

      if (current?.targetId === identity.targetId)
        return { context: makeProductionContext(context), page };
    }

    return undefined;
  },
  createContext: async () => makeProductionContext(await browser.createBrowserContext(), viewport),
  // Exact-session termination may already have closed the provider transport.
  close: async () => {
    if (!browser.isConnected()) return;
    try {
      await browser.close();
    } catch (cause) {
      if (!isRemoteClosure(cause)) throw cause;
    }
  },
  sessionId: () => sessionId,
  isConnected: () => browser.isConnected(),
  onDisconnected: (listener) => {
    browser.on("disconnected", listener);
  },
  offDisconnected: (listener) => {
    browser.off("disconnected", listener);
  },
});

const protocolError = (message: string, cause?: unknown): InteractiveBrowserProtocolError =>
  InteractiveBrowserProtocolError.make({
    implementation: browserRunInteractiveImplementation,
    message,
    ...(cause === undefined ? {} : { cause }),
  });

const failReportedProtocol = (
  operation: string,
  error: InteractiveBrowserProtocolError,
  evidence = error.evidence,
) =>
  reportBrowserCause(operation, Cause.fail(error)).pipe(
    Effect.andThen(
      Effect.fail(
        reportedBrowserError(
          InteractiveBrowserProtocolError.make({
            implementation: browserRunInteractiveImplementation,
            message: error.message,
            ...(evidence === undefined ? {} : { evidence }),
          }),
        ),
      ),
    ),
  );

const actionError = (
  operation: BrowserOperation,
  cause?: unknown,
  evidence?: InteractiveBrowserFailureEvidence,
): InteractiveBrowserActionError =>
  InteractiveBrowserActionError.make({
    implementation: browserRunInteractiveImplementation,
    operation,
    ...(evidence === undefined ? {} : { evidence }),
    message: `The interactive browser ${operation} operation failed`,
    ...(cause === undefined ? {} : { cause }),
  });

const undispatchedActionError = (operation: BrowserOperation): InteractiveBrowserActionError =>
  InteractiveBrowserActionError.make({
    implementation: browserRunInteractiveImplementation,
    operation,
    message: `The interactive browser ${operation} operation was not dispatched`,
    evidence: { stage: "preparation", dispatch: "not-dispatched", session: "attached" },
  });

/** Recognizes a local pre-dispatch refusal without exposing selector or page content. */
export const isBrowserRunUndispatchedActionError = (error: unknown): boolean =>
  Schema.is(InteractiveBrowserActionError)(error) &&
  error.implementation.identity === browserRunInteractiveImplementation.identity &&
  error.evidence?.dispatch === "not-dispatched";

const policyError = (message: string): InteractiveBrowserPolicyDeniedError =>
  InteractiveBrowserPolicyDeniedError.make({
    implementation: browserRunInteractiveImplementation,
    message,
  });

const expiredError = (session: "lost" | "disconnected" = "lost"): InteractiveBrowserExpiredError =>
  InteractiveBrowserExpiredError.make({
    implementation: browserRunInteractiveImplementation,
    message: "The remote browser is no longer usable",
    evidence: { stage: "preparation", dispatch: "not-dispatched", session },
  });

const causeText = (cause: unknown): string => {
  if (cause instanceof Error) return cause.message.slice(0, 8_000);

  return String(cause).slice(0, 8_000);
};

const isCapacityRefusal = (cause: unknown): boolean =>
  cause instanceof BrowserRunFailure && cause.status === 429;

const isRemoteClosure = (cause: unknown): boolean =>
  /target closed|browser.*closed|session.*closed|connection.*closed|not connected|websocket.*closed/i.test(
    causeText(cause),
  );

const inputMayStillRun = (cause: unknown): boolean =>
  isRemoteClosure(cause) ||
  (cause instanceof Error && cause.name === "TimeoutError") ||
  (cause instanceof BrowserRunFailure && cause.reason === "timeout");

const snapshotPolicy = Effect.fn("BrowserRunInteractive.snapshotPolicy")(function* (
  input: InteractiveBrowserPolicy,
): Effect.fn.Return<
  InteractiveBrowserPolicySnapshot,
  InteractiveBrowserPolicyDeniedError | InteractiveBrowserUnsupportedError
> {
  const decoded = yield* Schema.decodeEffect(InteractiveBrowserPolicy, {
    onExcessProperty: "error",
  })(input).pipe(Effect.mapError(() => policyError("The interactive browser policy is malformed")));

  if (decoded.network._tag === "PublicWeb") {
    return yield* InteractiveBrowserUnsupportedError.make({
      implementation: browserRunInteractiveImplementation,
      feature: "policy",
      message:
        "Cloudflare Browser Run cannot enforce the PublicWeb network policy for all session traffic",
    });
  }

  return Object.freeze({
    network:
      decoded.network._tag === "ExactHosts"
        ? Object.freeze({
            _tag: decoded.network._tag,
            allowedHosts: Object.freeze([...decoded.network.allowedHosts]),
          })
        : Object.freeze({ _tag: decoded.network._tag }),
    maxActions: decoded.maxActions,
    maxElapsedMillis: decoded.maxElapsedMillis,
    maxReturnedBytes: decoded.maxReturnedBytes,
  });
});

const hostAllowed = (policy: InteractiveBrowserPolicySnapshot, value: string): boolean => {
  if (policy.network._tag === "Unrestricted") return true;
  try {
    const url = new URL(value);

    return (
      url.protocol === "https:" &&
      url.username === "" &&
      url.password === "" &&
      policy.network.allowedHosts.some((host) => host === url.host)
    );
  } catch {
    return false;
  }
};

const keepAliveMillis = (policy: InteractiveBrowserPolicySnapshot): number =>
  Math.max(MIN_KEEP_ALIVE_MILLIS, Math.min(MAX_KEEP_ALIVE_MILLIS, policy.maxElapsedMillis));

class LateBrowserAcquisition extends Error {}

const closeLateAcquisition = async <A>(
  signal: AbortSignal,
  acquire: () => Promise<A>,
  close: (acquired: A) => Promise<void>,
): Promise<A> => {
  const acquired = await acquire();

  if (!signal.aborted) return acquired;

  // The SDK does not accept AbortSignal. If an acquisition settles after Effect
  // has interrupted it, ownership never reaches Scope, so close it here instead.
  try {
    await close(acquired);
  } catch {
    // The captured runtime reports cleanup failures before this callback absorbs rejection.
  }
  throw new LateBrowserAcquisition("The interrupted browser acquisition completed late");
};

const closeWithWarning = (close: () => Promise<void>, warning: string): Effect.Effect<void> =>
  Effect.tryPromise({
    try: close,
    catch: (cause) => protocolError(warning, cause),
  }).pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(CLEANUP_STEP_TIMEOUT_MILLIS),
      orElse: () => Effect.fail(protocolError(warning)),
    }),
    Effect.catchCause((cause) =>
      reportBrowserCause("interactive.cleanup", cause).pipe(
        Effect.andThen(Effect.logWarning(warning)),
      ),
    ),
  );

const deadlineError = Effect.fn("BrowserRunInteractive.deadlineError")(function* (
  policy: InteractiveBrowserPolicySnapshot,
  startedAt: number,
) {
  const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

  return yield* InteractiveBrowserLimitError.make({
    implementation: browserRunInteractiveImplementation,
    limit: "elapsed",
    maximum: policy.maxElapsedMillis,
    observed: Math.max(policy.maxElapsedMillis, now - startedAt),
    message: "The browser elapsed-time limit was reached",
  });
});

const withinDeadline = Effect.fn("BrowserRunInteractive.withinDeadline")(function* <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  policy: InteractiveBrowserPolicySnapshot,
  startedAt: number,
  onTimeout?: () => void,
): Effect.fn.Return<A, E | InteractiveBrowserLimitError, R> {
  const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  const elapsed = Math.max(0, now - startedAt);
  const remaining = policy.maxElapsedMillis - elapsed;

  if (remaining <= 0) return yield* deadlineError(policy, startedAt);

  return yield* effect.pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(remaining),
      orElse: () =>
        Effect.sync(() => onTimeout?.()).pipe(Effect.andThen(deadlineError(policy, startedAt))),
    }),
  );
});

interface HandleState {
  readonly closed: { value: boolean };
  readonly disconnected: { value: boolean };
  readonly uncertain: { value: boolean };
  readonly pendingInputs: Set<Promise<unknown>>;
  readonly actions: { value: number };
  readonly violation: { value: BrowserFailure | undefined };
  readonly pendingRequests: Set<Promise<void>>;
}

const trackPendingInput = <A>(state: HandleState, pending: Promise<A>): Promise<A> => {
  state.pendingInputs.add(pending);
  void pending.then(
    () => state.pendingInputs.delete(pending),
    (cause) => {
      state.pendingInputs.delete(pending);
      if (state.disconnected.value || inputMayStillRun(cause)) state.uncertain.value = true;
      if (isRemoteClosure(cause)) state.disconnected.value = true;
    },
  );

  return pending;
};

const makeHostInputObserver = Effect.fnUntraced(function* (state: HandleState, operation: string) {
  const reporterContext = yield* Effect.context<never>();

  return <A>(pending: Promise<A>, signal: AbortSignal): Promise<A> => {
    void pending.then(undefined, (cause) => {
      if (signal.aborted)
        return Effect.runPromiseWith(reporterContext)(
          reportBrowserCause(operation, Cause.fail(cause)),
        );
    });

    return trackPendingInput(state, pending);
  };
});

const stateFailure = (state: HandleState): BrowserFailure | undefined => {
  if (state.violation.value !== undefined) return state.violation.value;
  if (state.closed.value) return expiredError();
  if (state.disconnected.value) return expiredError("disconnected");

  return undefined;
};

interface CloseFailure {
  readonly error: InteractiveBrowserActionError;
  readonly warning: string;
}

interface CloseEntry {
  readonly close: Effect.Effect<void, InteractiveBrowserActionError>;
  readonly warning: string;
}

interface HandleRuntime {
  readonly handle: BrowserHandle;
  readonly run: <A>(
    effect: Effect.Effect<A, BrowserFailure>,
    preflight?: Effect.Effect<void, BrowserFailure>,
    consumeAction?: boolean,
    mutating?: boolean,
  ) => Effect.Effect<A, BrowserFailure>;
}

class BrowserRunRemoteFailure {
  readonly cause: unknown;

  constructor(cause: unknown) {
    this.cause = cause;
  }
}

const awaitPendingRequests = (state: HandleState): Effect.Effect<void> =>
  Effect.suspend(() => {
    const pending = [...state.pendingRequests];

    return pending.length === 0
      ? Effect.void
      : Effect.promise(() => Promise.all(pending)).pipe(
          Effect.asVoid,
          Effect.andThen(awaitPendingRequests(state)),
        );
  });

const decodeNavigationResult = Effect.fn("BrowserRunInteractive.decodeNavigationResult")(function* (
  page: BrowserRunInteractivePage,
  policy: InteractiveBrowserPolicySnapshot,
) {
  const url = yield* Effect.try({
    try: page.url,
    catch: (cause) => protocolError("Reading the browser navigation URL failed", cause),
  });

  const result = yield* Schema.decodeUnknownEffect(BrowserNavigationResult)({
    url,
  }).pipe(
    Effect.mapError((cause) =>
      protocolError("The browser returned a malformed navigation URL", cause),
    ),
  );

  if (!hostAllowed(policy, result.url)) {
    return yield* policyError("The browser returned an off-policy page URL");
  }

  return result;
});

const decodeActionResult = Effect.fn("BrowserRunInteractive.decodeActionResult")(function* (
  page: BrowserRunInteractivePage,
  policy: InteractiveBrowserPolicySnapshot,
) {
  const url = yield* Effect.try({
    try: page.url,
    catch: (cause) => protocolError("Reading the browser page URL failed", cause),
  });

  const result = yield* Schema.decodeUnknownEffect(BrowserActionResult)({ url }).pipe(
    Effect.mapError((cause) => protocolError("The browser returned a malformed page URL", cause)),
  );

  if (!hostAllowed(policy, result.url)) {
    return yield* policyError("The browser returned an off-policy page URL");
  }

  return result;
});

const makeRequestListener =
  (
    policy: InteractiveBrowserPolicySnapshot,
    state: HandleState,
  ): BrowserRunInteractiveRequestListener =>
  (request) => {
    let allowed = false;

    try {
      allowed = hostAllowed(policy, request.url());
    } catch {
      allowed = false;
    }
    if (!allowed) {
      state.violation.value = policyError("The browser requested an off-policy URL");
    }

    let settlement: Promise<void>;

    try {
      settlement = allowed ? request.continue() : request.abort();
    } catch (cause) {
      state.violation.value = protocolError(
        "Resolving an intercepted browser request failed",
        cause,
      );

      return;
    }

    const observed = settlement
      .catch((cause) => {
        state.violation.value = protocolError(
          "Resolving an intercepted browser request failed",
          cause,
        );
      })
      .finally(() => {
        state.pendingRequests.delete(observed);
      });

    state.pendingRequests.add(observed);
  };

const makeHandle = Effect.fn("BrowserRunInteractive.makeHandle")(function* (
  page: BrowserRunInteractivePage,
  policy: InteractiveBrowserPolicySnapshot,
  startedAt: number,
  state: HandleState,
  close: Effect.Effect<void, InteractiveBrowserError>,
): Effect.fn.Return<HandleRuntime> {
  const permits = yield* Semaphore.make(1);
  let currentOperation: BrowserOperation | undefined;
  let generation = 0;

  let evidence: InteractiveBrowserFailureEvidence = {
    stage: "preparation",
    dispatch: "not-dispatched",
    session: "attached",
  };

  const reporterContext = yield* Effect.context<never>();

  const remote = <A>(
    operation: BrowserOperation,
    evaluate: (signal: AbortSignal) => Promise<A>,
    observed = false,
  ) =>
    Effect.suspend(() => {
      currentOperation = operation;
      const mutating = operation !== "read-text" && operation !== "screenshot";
      const currentGeneration = ++generation;

      evidence = {
        stage: observed ? "preparation" : mutating ? "input" : "observation",
        dispatch: observed || !mutating ? "not-dispatched" : "running",
        session: "attached",
      };

      return Effect.tryPromise({
        try: (signal) => {
          const pending = evaluate(signal);

          if (mutating) trackPendingInput(state, pending);
          void pending.then(
            () => {
              if (mutating && generation === currentGeneration)
                evidence = { ...evidence, stage: "observation", dispatch: "completed" };
            },
            (cause) => {
              if (state.disconnected.value || isRemoteClosure(cause)) {
                state.disconnected.value = true;
                if (mutating) state.uncertain.value = true;
              }
              if (generation === currentGeneration && evidence.dispatch === "running")
                evidence = { ...evidence, dispatch: "unknown" };
              if (signal.aborted && !(cause instanceof BrowserRunActionUndispatched))
                void Effect.runPromiseWith(reporterContext)(
                  reportBrowserCause("interactive.input.late", Cause.fail(cause)),
                );
            },
          );

          return pending;
        },
        catch: (cause) => new BrowserRunRemoteFailure(cause),
      }).pipe(
        Effect.catch((failure) => {
          if (failure.cause instanceof BrowserRunActionUndispatched)
            return Effect.logInfo("Browser interactive action was not dispatched").pipe(
              Effect.annotateLogs({
                "browser.action": operation,
                "browser.selector_match_count": failure.cause.matchCount,
              }),
              Effect.andThen(Effect.fail(undispatchedActionError(operation))),
            );
          if (mutating && inputMayStillRun(failure.cause)) state.uncertain.value = true;
          if (isRemoteClosure(failure.cause)) {
            state.disconnected.value = true;
            if (mutating) state.uncertain.value = true;
          }
          evidence = {
            ...evidence,
            dispatch: evidence.dispatch === "running" ? "unknown" : evidence.dispatch,
            session: state.disconnected.value ? "disconnected" : "attached",
          };

          return reportBrowserCause(
            `interactive.${operation}.${evidence.stage}.${evidence.dispatch}`,
            Cause.fail(failure.cause),
          ).pipe(
            Effect.andThen(
              Effect.fail(reportedBrowserError(actionError(operation, undefined, evidence))),
            ),
          );
        }),
      );
    });

  const observedAction = (
    operation: "fill" | "click" | "select-file",
    evaluate: (
      signal: AbortSignal,
      onDispatch: () => void,
      onComplete: () => void,
    ) => Promise<unknown>,
  ) =>
    remote(
      operation,
      (signal) => {
        const observedGeneration = generation;

        return evaluate(
          signal,
          () => {
            if (generation === observedGeneration)
              evidence = { ...evidence, stage: "input", dispatch: "running" };
          },
          () => {
            if (generation === observedGeneration)
              evidence = { ...evidence, stage: "observation", dispatch: "completed" };
          },
        );
      },
      true,
    ).pipe(
      Effect.onInterrupt(() =>
        Effect.logWarning("Browser interactive action interrupted").pipe(
          Effect.annotateLogs({
            "browser.action": operation,
            "browser.action_dispatched": evidence.dispatch !== "not-dispatched",
            "browser.action_outcome_unknown":
              evidence.dispatch === "running" || evidence.dispatch === "unknown",
          }),
        ),
      ),
    );

  const decodeActionObservation = Effect.fn("BrowserRunInteractive.decodeActionObservation")(
    function* (raw: unknown) {
      return yield* Schema.decodeUnknownEffect(ActionObservation)(raw).pipe(
        Effect.mapError((cause) =>
          protocolError("The browser returned a malformed action observation", cause),
        ),
      );
    },
  );

  const logActionObservation = (
    operation: "fill" | "click" | "select-file",
    observation: typeof ActionObservation.Type,
  ) =>
    Effect.logInfo("Browser interactive action observed").pipe(
      Effect.annotateLogs({
        "browser.action": operation,
        "browser.input_millis": observation.inputMillis,
        "browser.observation_millis": observation.observationMillis,
        "browser.selector_match_count": observation.before.matchCount,
        ...(observation.before.kind === undefined
          ? {}
          : { "browser.target_kind": observation.before.kind }),
        ...(observation.before.checked === undefined
          ? {}
          : { "browser.target_checked_before": observation.before.checked }),
        ...(observation.after?.checked === undefined
          ? {}
          : { "browser.target_checked_after": observation.after.checked }),
        ...(observation.before.selected === undefined
          ? {}
          : { "browser.target_selected_before": observation.before.selected }),
        ...(observation.after?.selected === undefined
          ? {}
          : { "browser.target_selected_after": observation.after.selected }),
        ...(observation.before.disabled === undefined
          ? {}
          : { "browser.target_disabled_before": observation.before.disabled }),
        ...(observation.after?.disabled === undefined
          ? {}
          : { "browser.target_disabled_after": observation.after.disabled }),
        ...(observation.before.required === undefined
          ? {}
          : { "browser.target_required_before": observation.before.required }),
        ...(observation.after?.required === undefined
          ? {}
          : { "browser.target_required_after": observation.after.required }),
        ...(observation.before.valid === undefined
          ? {}
          : { "browser.target_valid_before": observation.before.valid }),
        ...(observation.after?.valid === undefined
          ? {}
          : { "browser.target_valid_after": observation.after.valid }),
        ...(observation.before.formValid === undefined
          ? {}
          : { "browser.form_valid_before": observation.before.formValid }),
        ...(observation.after?.formValid === undefined
          ? {}
          : { "browser.form_valid_after": observation.after.formValid }),
        "browser.target_after_unavailable": observation.afterUnavailable,
        "browser.fetch_xhr_total": observation.network.total,
        "browser.fetch_xhr_2xx": observation.network.status2xx,
        "browser.fetch_xhr_3xx": observation.network.status3xx,
        "browser.fetch_xhr_4xx": observation.network.status4xx,
        "browser.fetch_xhr_5xx": observation.network.status5xx,
        "browser.fetch_xhr_failed": observation.network.failed,
        "browser.fetch_xhr_pending": observation.network.pending,
        "browser.network_settle_timed_out": observation.network.settleTimedOut,
      }),
    );

  const run = <A>(
    effect: Effect.Effect<A, BrowserFailure>,
    preflight: Effect.Effect<void, BrowserFailure> = Effect.void,
    consumeAction = true,
    mutating = true,
  ) =>
    permits
      .withPermitsIfAvailable(1)(
        Effect.gen(function* () {
          const unavailable = stateFailure(state);

          if (unavailable !== undefined) {
            if (unavailable._tag === "InteractiveBrowserProtocolError") {
              yield* reportBrowserCause("interactive.policy", Cause.fail(unavailable));

              const safe = reportedBrowserError(
                InteractiveBrowserProtocolError.make({
                  implementation: browserRunInteractiveImplementation,
                  message: unavailable.message,
                  evidence: unavailable.evidence ?? {
                    stage: "preparation",
                    dispatch: "not-dispatched",
                    session: "attached",
                  },
                }),
              );

              if (state.violation.value === unavailable) state.violation.value = safe;

              return yield* safe;
            }

            return yield* unavailable;
          }
          if (mutating && (state.uncertain.value || state.pendingInputs.size > 0))
            return yield* InteractiveBrowserBusyError.make({
              implementation: browserRunInteractiveImplementation,
              message: "Earlier browser input has not been proven stopped",
            });
          currentOperation = undefined;
          generation++;
          yield* preflight;

          if (consumeAction) {
            const admitted = {
              allowed: state.actions.value < policy.maxActions,
              observed: state.actions.value + 1,
            };

            if (admitted.allowed) state.actions.value++;

            if (!admitted.allowed) {
              return yield* InteractiveBrowserLimitError.make({
                implementation: browserRunInteractiveImplementation,
                limit: "actions",
                maximum: policy.maxActions,
                observed: admitted.observed,
                message: "The browser action limit was reached",
              });
            }
          }

          const completed = effect.pipe(
            Effect.flatMap((result) =>
              awaitPendingRequests(state).pipe(
                Effect.andThen(
                  Effect.suspend(() => {
                    const failure = stateFailure(state);

                    return failure === undefined ? Effect.succeed(result) : Effect.fail(failure);
                  }),
                ),
              ),
            ),
          );

          return yield* withinDeadline(completed, policy, startedAt).pipe(
            Effect.mapError((error) =>
              currentOperation === undefined
                ? error
                : error._tag === "InteractiveBrowserLimitError"
                  ? InteractiveBrowserLimitError.make({
                      implementation: error.implementation,
                      limit: error.limit,
                      maximum: error.maximum,
                      observed: error.observed,
                      message: error.message,
                      evidence,
                    })
                  : error._tag === "InteractiveBrowserPolicyDeniedError"
                    ? InteractiveBrowserPolicyDeniedError.make({
                        implementation: error.implementation,
                        message: error.message,
                        evidence,
                      })
                    : error,
            ),
            Effect.catchIf(
              (error) =>
                currentOperation !== undefined &&
                error._tag !== "InteractiveBrowserActionError" &&
                error._tag !== "InteractiveBrowserLimitError" &&
                error._tag !== "InteractiveBrowserPolicyDeniedError" &&
                error._tag !== "InteractiveBrowserExpiredError",
              (error) => {
                const safe = reportedBrowserError(
                  error._tag === "InteractiveBrowserProtocolError"
                    ? InteractiveBrowserProtocolError.make({
                        implementation: browserRunInteractiveImplementation,
                        message: error.message,
                        evidence,
                      })
                    : actionError(currentOperation ?? "read-text", undefined, evidence),
                );

                if (state.violation.value === error) state.violation.value = safe;

                return reportBrowserCause(
                  `interactive.${currentOperation}.observation`,
                  Cause.fail(error),
                ).pipe(Effect.andThen(Effect.fail(safe)));
              },
            ),
          );
        }),
      )
      .pipe(
        Effect.flatMap(
          Effect.fromOption(() =>
            InteractiveBrowserBusyError.make({
              implementation: browserRunInteractiveImplementation,
              message: "The browser handle already has an operation in flight",
            }),
          ),
        ),
      );

  const handle: BrowserHandle = {
    navigate: (request) =>
      run(
        Effect.gen(function* () {
          yield* remote("navigate", () => page.goto(request.url));

          return yield* decodeNavigationResult(page, policy);
        }),
        Effect.suspend(() =>
          Schema.is(InteractiveBrowserTargetUrl)(request.url) && hostAllowed(policy, request.url)
            ? Effect.void
            : Effect.fail(policyError("The navigation URL is outside the browser policy")),
        ),
      ),
    readText: (request) =>
      run(
        Effect.gen(function* () {
          const raw = yield* remote("read-text", () =>
            page.readText(request.selector, policy.maxReturnedBytes),
          );

          const observation = yield* Schema.decodeUnknownEffect(TextObservation)(raw).pipe(
            Effect.mapError((cause) =>
              protocolError("The browser returned a malformed text observation", cause),
            ),
          );

          if (observation._tag === "MissingElement") {
            return yield* actionError("read-text", undefined, evidence);
          }
          if (observation._tag === "OverLimit") {
            return yield* InteractiveBrowserLimitError.make({
              implementation: browserRunInteractiveImplementation,
              limit: "returned-bytes",
              maximum: policy.maxReturnedBytes,
              observed: observation.observed,
              message: "The browser returned-text limit was reached",
            });
          }
          const observed = new TextEncoder().encode(observation.text).byteLength;

          if (observed > policy.maxReturnedBytes) {
            return yield* InteractiveBrowserLimitError.make({
              implementation: browserRunInteractiveImplementation,
              limit: "returned-bytes",
              maximum: policy.maxReturnedBytes,
              observed,
              message: "The browser returned-text limit was reached",
            });
          }

          return yield* Schema.decodeEffect(BrowserTextResult)({
            text: observation.text,
          }).pipe(
            Effect.mapError((cause) =>
              protocolError("The browser returned malformed page text", cause),
            ),
          );
        }),
        Effect.void,
        true,
        false,
      ),
    fill: (request) =>
      run(
        Effect.gen(function* () {
          const observation = yield* observedAction("fill", (signal, onDispatch, onComplete) =>
            page.fill(
              request.selector,
              request.value,
              signal,
              onDispatch,
              onComplete,
              request.expectedTarget,
            ),
          ).pipe(Effect.flatMap(decodeActionObservation));

          yield* logActionObservation("fill", observation);

          return yield* decodeActionResult(page, policy);
        }),
      ),
    click: (request) =>
      run(
        Effect.gen(function* () {
          const observation = yield* observedAction("click", (signal, onDispatch, onComplete) =>
            page.click(request.selector, signal, onDispatch, onComplete, request.expectedTarget),
          ).pipe(Effect.flatMap(decodeActionObservation));

          yield* logActionObservation("click", observation);

          return yield* decodeActionResult(page, policy);
        }),
      ),
    selectFile: (request) =>
      Schema.decodeEffect(BrowserSelectFileRequest)(request).pipe(
        Effect.mapError(() => policyError("The browser file selection request is malformed")),
        Effect.flatMap((decoded) =>
          run(
            Effect.gen(function* () {
              const observation = yield* observedAction(
                "select-file",
                (signal, onDispatch, onComplete) =>
                  page.selectFile(decoded, signal, onDispatch, onComplete),
              ).pipe(Effect.flatMap(decodeActionObservation));

              yield* logActionObservation("select-file", observation);
              const result = yield* decodeActionResult(page, policy);

              return BrowserFileSelectionResult.make({
                url: result.url,
                fileName: decoded.fileName,
                mediaType: decoded.mediaType,
                size: decoded.bytes.length,
              });
            }),
          ),
        ),
      ),
    screenshot: (request) =>
      Schema.decodeEffect(BrowserScreenshotRequest)(request).pipe(
        Effect.mapError(() => policyError("The browser screenshot request is malformed")),
        Effect.flatMap((decoded) =>
          run(
            Effect.gen(function* () {
              const raw = yield* remote("screenshot", () => page.screenshot(decoded.fullPage));

              const bytes = yield* Schema.decodeUnknownEffect(PngBytes)(raw).pipe(
                Effect.mapError(() =>
                  protocolError("The browser returned a malformed PNG screenshot"),
                ),
              );

              if (bytes.length > policy.maxReturnedBytes) {
                return yield* InteractiveBrowserLimitError.make({
                  implementation: browserRunInteractiveImplementation,
                  limit: "returned-bytes",
                  maximum: policy.maxReturnedBytes,
                  observed: bytes.length,
                  message: "The browser screenshot byte limit was reached",
                });
              }

              return yield* Schema.decodeEffect(PageScreenshotResult)({
                implementation: browserRunInteractiveImplementation,
                mediaType: "image/png",
                bytes: new Uint8Array(bytes),
              }).pipe(
                Effect.mapError(() =>
                  protocolError("The browser returned a malformed PNG screenshot"),
                ),
              );
            }),
            decodeActionResult(page, policy).pipe(Effect.asVoid),
            true,
            false,
          ),
        ),
      ),
    scroll: (request) =>
      Schema.decodeEffect(BrowserScrollRequest)(request).pipe(
        Effect.mapError(() => policyError("The browser scroll request is malformed")),
        Effect.flatMap((decoded) =>
          run(
            remote("scroll", () => page.scroll(decoded.deltaX, decoded.deltaY)).pipe(
              Effect.andThen(decodeActionResult(page, policy)),
            ),
            decodeActionResult(page, policy).pipe(Effect.asVoid),
          ),
        ),
      ),
    close,
  };

  return { handle, run };
});

const remainingMillis = Effect.fn("BrowserRunInteractive.remainingMillis")(function* (
  policy: InteractiveBrowserPolicySnapshot,
  startedAt: number,
) {
  const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

  return Math.max(0, policy.maxElapsedMillis - Math.max(0, now - startedAt));
});

const closeEntry = (close: () => Promise<void>, warning: string): CloseEntry => ({
  close: Effect.tryPromise({
    try: close,
    catch: (cause) => actionError("close", cause),
  }).pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(CLEANUP_STEP_TIMEOUT_MILLIS),
      orElse: () => Effect.fail(actionError("close")),
    }),
  ),
  warning,
});

const syncCloseEntry = (close: () => void, warning: string): CloseEntry => ({
  close: Effect.try({
    try: close,
    catch: (cause) => actionError("close", cause),
  }),
  warning,
});

const runTeardown = (
  entries: ReadonlyArray<CloseEntry>,
): Effect.Effect<ReadonlyArray<CloseFailure>> =>
  Effect.forEach([...entries].reverse(), (entry) =>
    entry.close.pipe(
      Effect.match({
        onFailure: (error): CloseFailure | undefined => ({ error, warning: entry.warning }),
        onSuccess: (): CloseFailure | undefined => undefined,
      }),
    ),
  ).pipe(Effect.map((failures) => failures.filter((failure) => failure !== undefined)));

const cdpCommand = <A>(
  page: BrowserRunInteractivePage,
  state: HandleState,
  command: BrowserRunCloudflareCommand,
  parameters: unknown,
  output: Schema.Codec<A>,
  malformedMessage: string,
): Effect.Effect<A, InteractiveBrowserError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const cleanupContext = yield* Effect.context<never>();
      const observeInput = yield* makeHostInputObserver(state, "interactive.control.late");

      const cdp = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: (signal) => {
            const pending = closeLateAcquisition(signal, page.createCdpSession, (acquired) =>
              Effect.runPromiseWith(cleanupContext)(
                Effect.tryPromise({
                  try: () => acquired.detach(),
                  catch: (cause) => actionError("close", cause),
                }).pipe(
                  Effect.tapCause((cause) => reportBrowserCause("interactive.lateDetach", cause)),
                ),
              ),
            );

            void pending.then(undefined, (cause) => {
              if (signal.aborted && !(cause instanceof LateBrowserAcquisition))
                return Effect.runPromiseWith(cleanupContext)(
                  reportBrowserCause("interactive.connect", Cause.fail(cause)),
                );
            });

            return pending;
          },
          catch: (cause) =>
            state.disconnected.value || isRemoteClosure(cause)
              ? expiredError()
              : protocolError("Creating the Cloudflare browser control session failed", cause),
        }),
        (acquired) =>
          closeWithWarning(
            acquired.detach,
            "Detaching the Cloudflare browser control session failed",
          ),
        { interruptible: true },
      );

      const raw = yield* Effect.tryPromise({
        try: (signal) => {
          const pending = cdp.send(command, parameters);

          return command === "Cloudflare.getHandoffState" ? pending : observeInput(pending, signal);
        },
        catch: (cause) =>
          state.disconnected.value || isRemoteClosure(cause)
            ? expiredError()
            : protocolError("The Cloudflare browser control command failed", cause),
      });

      return yield* Schema.decodeUnknownEffect(output)(raw).pipe(
        Effect.catch((error) =>
          reportBrowserCause("interactive.command.decode", Cause.fail(error)).pipe(
            Effect.andThen(Effect.fail(reportedBrowserError(protocolError(malformedMessage)))),
          ),
        ),
      );
    }),
  ).pipe(
    Effect.catchTag("InteractiveBrowserProtocolError", (error) =>
      failReportedProtocol("interactive.control", error),
    ),
  );

const makeHostService = (
  binding: BrowserRunInteractiveBinding["Service"],
): BrowserRunInteractiveHost["Service"] => {
  const owners = new Map<string, symbol>();

  const closeSession: BrowserRunInteractiveHost["Service"]["closeSession"] = (sessionId) =>
    binding.closeSession(sessionId).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          owners.delete(Redacted.value(sessionId));
        }),
      ),
    );

  const terminate = Effect.fn("BrowserRunInteractiveHost.terminate")(function* (
    sessionId: Redacted.Redacted<string>,
    entries: ReadonlyArray<CloseEntry>,
  ) {
    const deadline = (yield* Clock.currentTimeMillis) + 10_000;

    yield* closeSession(sessionId).pipe(
      Effect.interruptible,
      Effect.timeoutOrElse({
        duration: "10 seconds",
        orElse: () => Effect.fail(actionError("close")),
      }),
    );
    const remaining = deadline - (yield* Clock.currentTimeMillis);

    if (remaining <= 0)
      return yield* Effect.logWarning(
        "Local browser teardown skipped after confirmed termination deadline",
      );
    // Remote termination is authoritative. Local cleanup must not veto it or extend the deadline.
    yield* runTeardown(entries).pipe(
      Effect.flatMap((failures) =>
        Effect.forEach(
          failures,
          (failure) =>
            reportBrowserCause("interactive.cleanup", Cause.fail(failure.error)).pipe(
              Effect.andThen(Effect.logWarning(failure.warning)),
            ),
          {
            discard: true,
          },
        ),
      ),
      Effect.interruptible,
      Effect.timeout(`${remaining} millis`),
      Effect.catchCause((cause) =>
        reportBrowserCause("interactive.cleanup", cause).pipe(
          Effect.andThen(
            Effect.logWarning("Local browser teardown incomplete after confirmed termination"),
          ),
        ),
      ),
    );
  });

  const closeAcquired = Effect.fn("BrowserRunInteractiveHost.closeAcquired")(function* (
    browser: BrowserRunInteractiveBrowser,
  ) {
    const sessionId = yield* Effect.try({
      try: browser.sessionId,
      catch: (cause) => actionError("close", cause),
    }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(BrowserRunSessionId)),
      Effect.mapError((cause) => actionError("close", cause)),
      Effect.onError(() =>
        closeWithWarning(browser.close, "Closing an unidentified browser failed"),
      ),
    );

    yield* terminate(Redacted.make(sessionId), [
      closeEntry(browser.close, "Closing the local browser connection failed"),
    ]);
  });

  const acquire = Effect.fn("BrowserRunInteractiveHost.acquire")(function* (
    policy: InteractiveBrowserPolicy,
    resume?: BrowserRunInteractiveCheckpoint,
    pendingInput = false,
  ): Effect.fn.Return<BrowserRunInteractiveAcquisition, InteractiveBrowserError, Scope.Scope> {
    const scope = yield* Scope.Scope;
    const fixedPolicy = yield* snapshotPolicy(policy);
    const startedAt = resume?.startedAt ?? (yield* Clock.currentTimeMillis);

    // Late SDK replies retain this pass's clock and private reporting scope.
    const runCleanup = <A, E>(effect: Effect.Effect<A, E>) =>
      Effect.runPromiseWith(cleanupContext)(
        effect.pipe(
          Effect.tapCause((cause) => reportBrowserCause("interactive.lateCleanup", cause)),
        ),
      );

    const cleanupContext = yield* Effect.context<never>();

    const observeLate = <A>(signal: AbortSignal, pending: Promise<A>): Promise<A> => {
      // Observe the original Promise without creating another rejected continuation.
      void pending.then(undefined, (cause) => {
        if (signal.aborted && !(cause instanceof LateBrowserAcquisition))
          return runCleanup(reportBrowserCause("interactive.acquire", Cause.fail(cause)));
      });

      return pending;
    };

    const state: HandleState = {
      closed: { value: false },
      disconnected: { value: false },
      uncertain: { value: pendingInput || (resume !== undefined && resume.inputState !== "idle") },
      pendingInputs: new Set(),
      actions: { value: resume?.consumedActions ?? 0 },
      violation: { value: undefined },
      pendingRequests: new Set(),
    };

    const lifecycle = {
      managedTeardownInstalled: false,
      retained: resume !== undefined,
    };

    const closers: Array<CloseEntry> = [];

    const releaseBeforeManaged = (entry: CloseEntry): Effect.Effect<void> =>
      Effect.suspend(() =>
        lifecycle.managedTeardownInstalled || lifecycle.retained
          ? Effect.void
          : entry.close.pipe(
              Effect.catchCause((cause) =>
                reportBrowserCause("interactive.cleanup", cause).pipe(
                  Effect.andThen(Effect.logWarning(entry.warning)),
                ),
              ),
            ),
      );

    const sessionIdValue = yield* Effect.acquireRelease(
      withinDeadline(
        resume === undefined
          ? Effect.tryPromise({
              try: (signal) =>
                observeLate(
                  signal,
                  closeLateAcquisition(
                    signal,
                    () => binding.acquire(keepAliveMillis(fixedPolicy)),
                    (id) =>
                      runCleanup(
                        Schema.decodeUnknownEffect(BrowserRunSessionId)(id).pipe(
                          Effect.flatMap((value) => terminate(Redacted.make(value), [])),
                        ),
                      ),
                  ),
                ),
              catch: (cause) =>
                isCapacityRefusal(cause)
                  ? InteractiveBrowserCapacityError.make({
                      implementation: browserRunInteractiveImplementation,
                      message: "Browser Run has no capacity for a new browser session",
                    })
                  : protocolError("Acquiring the Browser Run session failed", cause),
            }).pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(BrowserRunSessionId)),
              Effect.catchTag("SchemaError", () =>
                protocolError("The Browser Run session identity was malformed"),
              ),
            )
          : Effect.succeed(Redacted.value(resume.sessionId)),
        fixedPolicy,
        startedAt,
      ),
      (id) =>
        Effect.suspend(() => {
          state.closed.value = true;
          state.disconnected.value = true;

          return lifecycle.managedTeardownInstalled || lifecycle.retained
            ? Effect.void
            : terminate(Redacted.make(id), []).pipe(
                Effect.catchCause((cause) =>
                  reportBrowserCause("interactive.close", cause).pipe(
                    Effect.andThen(Effect.logWarning("Whole-browser cleanup remains unconfirmed")),
                  ),
                ),
              );
        }),
      { interruptible: true },
    );

    const owner = Symbol();

    const releaseOwner = () => {
      if (owners.get(sessionIdValue) === owner) owners.delete(sessionIdValue);
    };

    const connect = yield* Effect.cached(
      Effect.gen(function* (): Effect.fn.Return<
        BrowserRunInteractiveSession,
        InteractiveBrowserError,
        Scope.Scope
      > {
        if (owners.has(sessionIdValue))
          return yield* InteractiveBrowserBusyError.make({
            implementation: browserRunInteractiveImplementation,
            message: "The previous local browser owner must detach before resuming",
          });
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            owners.set(sessionIdValue, owner);
          }),
          () =>
            Effect.sync(() => {
              if (!lifecycle.managedTeardownInstalled) releaseOwner();
            }),
        );

        const browser = yield* Effect.acquireRelease(
          withinDeadline(
            Effect.tryPromise({
              try: (signal) =>
                observeLate(
                  signal,
                  closeLateAcquisition(
                    signal,
                    () => binding.connect(sessionIdValue, signal),
                    (acquired) =>
                      runCleanup(
                        resume === undefined
                          ? closeAcquired(acquired)
                          : closeEntry(
                              acquired.detach ?? (async () => {}),
                              "Detaching the browser failed",
                            ).close,
                      ),
                  ),
                ),
              catch: (cause) =>
                resume !== undefined &&
                cause instanceof BrowserRunFailure &&
                (cause.status === 404 || cause.status === 410)
                  ? expiredError()
                  : protocolError("Connecting to the Browser Run session failed", cause),
            }).pipe(
              Effect.timeoutOrElse({
                duration: "10 seconds",
                orElse: () =>
                  Effect.fail(protocolError("Connecting to the Browser Run session timed out")),
              }),
            ),
            fixedPolicy,
            startedAt,
          ),
          (acquired) =>
            lifecycle.managedTeardownInstalled
              ? Effect.void
              : lifecycle.retained
                ? closeEntry(
                    acquired.detach ?? (async () => {}),
                    "Detaching the browser failed",
                  ).close.pipe(
                    Effect.tap(() => Effect.sync(releaseOwner)),
                    Effect.catchCause((cause) => reportBrowserCause("interactive.detach", cause)),
                  )
                : releaseBeforeManaged(
                    closeEntry(acquired.close, "Closing the local browser connection failed"),
                  ),
          { interruptible: true },
        );

        if (resume !== undefined) {
          const actualSession = yield* Effect.try({
            try: browser.sessionId,
            catch: (cause) =>
              protocolError("Reading the reattached browser identity failed", cause),
          });

          if (actualSession !== sessionIdValue)
            return yield* protocolError("The reattached browser session identity did not match");
        }
        closers.push(closeEntry(browser.close, "Closing the interactive browser failed"));

        const disconnected = () => {
          state.disconnected.value = true;
        };

        yield* Effect.acquireRelease(
          Effect.try({
            try: () => browser.onDisconnected(disconnected),
            catch: (cause) =>
              protocolError("Installing the browser disconnect listener failed", cause),
          }),
          () =>
            releaseBeforeManaged(
              syncCloseEntry(
                () => browser.offDisconnected(disconnected),
                "Removing the browser disconnect listener failed",
              ),
            ),
        );
        closers.push(
          syncCloseEntry(
            () => browser.offDisconnected(disconnected),
            "Removing the browser disconnect listener failed",
          ),
        );

        const connected = yield* Effect.try({
          try: browser.isConnected,
          catch: (cause) => protocolError("Reading the Browser Run connection state failed", cause),
        });

        if (!connected) {
          state.disconnected.value = true;

          return yield* expiredError();
        }

        const reattached =
          resume === undefined
            ? undefined
            : yield* withinDeadline(
                Effect.tryPromise({
                  try: async () => {
                    if (browser.reattach === undefined || browser.detach === undefined)
                      throw new Error("The browser binding does not support exact reattachment");

                    return await browser.reattach(resume.page);
                  },
                  catch: (cause) =>
                    protocolError("Reattaching the exact browser page failed", cause),
                }).pipe(
                  Effect.timeoutOrElse({
                    duration: "10 seconds",
                    orElse: () =>
                      Effect.fail(protocolError("Reattaching the exact browser page timed out")),
                  }),
                ),
                fixedPolicy,
                startedAt,
              );

        if (resume !== undefined && reattached === undefined) return yield* expiredError();

        const context =
          reattached?.context ??
          (yield* Effect.acquireRelease(
            withinDeadline(
              Effect.tryPromise({
                try: (signal) =>
                  observeLate(
                    signal,
                    closeLateAcquisition(signal, browser.createContext, (acquired) =>
                      runCleanup(
                        Effect.tryPromise({
                          try: () => acquired.close(),
                          catch: (cause) => actionError("close", cause),
                        }),
                      ),
                    ),
                  ),
                catch: (cause) =>
                  state.disconnected.value || isRemoteClosure(cause)
                    ? expiredError()
                    : protocolError("Creating the browser context failed", cause),
              }),
              fixedPolicy,
              startedAt,
            ),
            (acquired) =>
              releaseBeforeManaged(
                closeEntry(acquired.close, "Closing the interactive browser context failed"),
              ),
            { interruptible: true },
          ));

        closers.push(closeEntry(context.close, "Closing the interactive browser context failed"));

        const page =
          reattached?.page ??
          (yield* Effect.acquireRelease(
            withinDeadline(
              Effect.tryPromise({
                try: (signal) =>
                  observeLate(
                    signal,
                    closeLateAcquisition(signal, context.newPage, (acquired) =>
                      runCleanup(
                        Effect.tryPromise({
                          try: () => acquired.close(),
                          catch: (cause) => actionError("close", cause),
                        }),
                      ),
                    ),
                  ),
                catch: (cause) =>
                  state.disconnected.value || isRemoteClosure(cause)
                    ? expiredError()
                    : protocolError("Creating the browser page failed", cause),
              }),
              fixedPolicy,
              startedAt,
            ),
            (acquired) =>
              releaseBeforeManaged(
                closeEntry(acquired.close, "Closing the interactive browser page failed"),
              ),
            { interruptible: true },
          ));

        closers.push(closeEntry(page.close, "Closing the interactive browser page failed"));

        yield* withinDeadline(
          Effect.tryPromise({
            try: () => page.setBypassServiceWorker(true),
            catch: (cause) => protocolError("Bypassing browser service workers failed", cause),
          }),
          fixedPolicy,
          startedAt,
        );

        const requestListener = makeRequestListener(fixedPolicy, state);

        yield* Effect.acquireRelease(
          Effect.try({
            try: () => page.onRequest(requestListener),
            catch: (cause) =>
              protocolError("Installing the browser request listener failed", cause),
          }),
          () =>
            releaseBeforeManaged(
              syncCloseEntry(
                () => page.offRequest(requestListener),
                "Removing the browser request policy failed",
              ),
            ),
        );
        closers.push(
          syncCloseEntry(
            () => page.offRequest(requestListener),
            "Removing the browser request policy failed",
          ),
        );

        yield* withinDeadline(
          Effect.tryPromise({
            try: () => page.setRequestInterception(true),
            catch: (cause) => protocolError("Installing the browser request policy failed", cause),
          }),
          fixedPolicy,
          startedAt,
        );

        yield* withinDeadline(awaitPendingRequests(state), fixedPolicy, startedAt);
        const setupFailure = stateFailure(state);

        if (setupFailure !== undefined) return yield* setupFailure;

        const detach = yield* Effect.cached(
          Effect.uninterruptible(
            Effect.gen(function* () {
              if (browser.detach === undefined)
                return yield* protocolError("The browser binding does not support detachment");
              lifecycle.retained = true;
              state.closed.value = true;
              state.disconnected.value = true;
              if (state.pendingInputs.size > 0) state.uncertain.value = true;
              yield* closeEntry(async () => {
                page.offRequest(requestListener);
                browser.offDisconnected(disconnected);
                await browser.detach?.();
              }, "Detaching the browser transport failed").close;
              releaseOwner();
            }).pipe(
              Effect.catchTag("InteractiveBrowserActionError", (error) =>
                reportBrowserCause("interactive.detach", Cause.fail(error)).pipe(
                  Effect.andThen(Effect.fail(reportedBrowserError(actionError("close")))),
                ),
              ),
              Effect.catchTag("InteractiveBrowserProtocolError", (error) =>
                failReportedProtocol("interactive.detach", error),
              ),
            ),
          ),
        );

        const teardown = yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const cached = yield* Effect.cached(
              terminate(Redacted.make(sessionIdValue), closers).pipe(
                Effect.tapCause((cause) => reportBrowserCause("interactive.close", cause)),
                Effect.mapError(reportedBrowserError),
              ),
            );

            lifecycle.managedTeardownInstalled = true;
            lifecycle.retained = false;
            yield* Effect.addFinalizer(() =>
              Effect.uninterruptible(
                Effect.sync(() => {
                  state.closed.value = true;
                  state.disconnected.value = true;
                }).pipe(
                  Effect.andThen(Effect.suspend(() => (lifecycle.retained ? detach : cached))),
                  Effect.catchCause((cause) =>
                    reportBrowserCause("interactive.close", cause).pipe(
                      Effect.andThen(
                        Effect.logWarning("Whole-browser cleanup remains unconfirmed"),
                      ),
                    ),
                  ),
                ),
              ),
            );

            return cached;
          }),
        );

        const close: Effect.Effect<void, InteractiveBrowserError> = Effect.uninterruptible(
          Effect.suspend(() => {
            if (lifecycle.retained || owners.get(sessionIdValue) !== owner) return Effect.void;
            state.closed.value = true;
            state.disconnected.value = true;

            return teardown;
          }),
        );

        const runtime = yield* makeHandle(page, fixedPolicy, startedAt, state, close);
        const currentPagePreflight = decodeActionResult(page, fixedPolicy).pipe(Effect.asVoid);

        const requestFitsSession = (requestedMillis: number): Effect.Effect<void, BrowserFailure> =>
          remainingMillis(fixedPolicy, startedAt).pipe(
            Effect.flatMap((remaining) =>
              remaining > 0 && requestedMillis <= remaining
                ? Effect.void
                : Effect.fail(
                    policyError("The host browser request exceeds the remaining session time"),
                  ),
            ),
          );

        const inputState = Effect.sync((): BrowserRunInputState =>
          state.uncertain.value ? "unknown" : state.pendingInputs.size > 0 ? "running" : "idle",
        );

        const pageIdentity = yield* Effect.cached(
          Effect.tryPromise({
            try: async () => {
              if (page.identity === undefined)
                throw new Error("The browser binding does not expose page identity");

              return await page.identity();
            },
            catch: (cause) =>
              protocolError("Reading the exact browser page identity failed", cause),
          }).pipe(
            Effect.catchTag("InteractiveBrowserProtocolError", (error) =>
              failReportedProtocol("interactive.checkpoint", error),
            ),
          ),
        );

        const observeResize = yield* makeHostInputObserver(state, "interactive.resize.late");

        return {
          handle: runtime.handle,
          checkpoint: withinDeadline(
            Effect.gen(function* () {
              const identity = resume?.page ?? (yield* pageIdentity);

              return BrowserRunInteractiveCheckpoint.make({
                sessionId: Redacted.make(sessionIdValue),
                page: identity,
                policy: InteractiveBrowserPolicy.make(fixedPolicy),
                startedAt,
                consumedActions: state.actions.value,
                inputState: yield* inputState,
              });
            }),
            fixedPolicy,
            startedAt,
          ),
          detach,
          inputState,
          drainInput: Effect.suspend(() =>
            Effect.promise(() =>
              boundedBestEffort(Promise.allSettled(state.pendingInputs), 500),
            ).pipe(Effect.andThen(inputState)),
          ),
          sessionId: Redacted.make(sessionIdValue),
          resizeViewport: (viewport) =>
            decodeViewport(viewport).pipe(
              Effect.flatMap((decoded) =>
                runtime.run(
                  Effect.tryPromise({
                    try: (signal) => observeResize(page.setViewport(decoded), signal),
                    catch: (cause) => {
                      if (state.disconnected.value || isRemoteClosure(cause)) {
                        state.disconnected.value = true;

                        return expiredError();
                      }

                      return protocolError("Resizing the browser viewport failed", cause);
                    },
                  }).pipe(
                    Effect.catchTag("InteractiveBrowserProtocolError", (error) =>
                      failReportedProtocol("interactive.resize", error),
                    ),
                  ),
                  currentPagePreflight,
                  false,
                ),
              ),
            ),
          getLiveView: (request) =>
            Schema.decodeEffect(BrowserRunLiveViewRequest)(request).pipe(
              Effect.mapError(() => policyError("The Live View request is malformed")),
              Effect.flatMap((decoded) =>
                runtime.run(
                  cdpCommand(
                    page,
                    state,
                    "Cloudflare.getLiveView",
                    { mode: decoded.mode, expiresInMs: decoded.expiresInMs },
                    LiveViewObservation,
                    "Cloudflare returned a malformed Live View response",
                  ).pipe(
                    Effect.flatMap((observation) =>
                      Schema.decodeEffect(BrowserRunLiveViewResult)({
                        devtoolsFrontendUrl: Redacted.make(observation.devtoolsFrontendUrl),
                      }).pipe(
                        Effect.mapError(() =>
                          protocolError("Cloudflare returned a malformed Live View response"),
                        ),
                      ),
                    ),
                  ),
                  currentPagePreflight.pipe(
                    Effect.andThen(requestFitsSession(decoded.expiresInMs)),
                  ),
                  true,
                  true,
                ),
              ),
            ),
          handoff: (request) =>
            Schema.decodeEffect(BrowserRunHandoffRequest)(request).pipe(
              Effect.mapError(() => policyError("The browser handoff request is malformed")),
              Effect.flatMap((decoded) =>
                runtime.run(
                  cdpCommand(
                    page,
                    state,
                    "Cloudflare.handoff",
                    { instructions: decoded.instructions, timeout: decoded.timeout },
                    HandoffObservation,
                    "Cloudflare returned a malformed browser handoff response",
                  ).pipe(
                    Effect.flatMap((observation) =>
                      Schema.decodeEffect(BrowserRunHandoffResult)({
                        handoffId: Redacted.make(observation.handoffId),
                      }).pipe(
                        Effect.mapError(() =>
                          protocolError("Cloudflare returned a malformed browser handoff response"),
                        ),
                      ),
                    ),
                  ),
                  currentPagePreflight.pipe(Effect.andThen(requestFitsSession(decoded.timeout))),
                ),
              ),
            ),
          getHandoffState: runtime.run(
            cdpCommand(
              page,
              state,
              "Cloudflare.getHandoffState",
              {},
              HandoffStateObservation,
              "Cloudflare returned a malformed browser handoff state",
            ).pipe(
              Effect.flatMap((observation) =>
                Schema.decodeEffect(BrowserRunHandoffState)({
                  active: observation.active,
                  ...(observation.handoffId === undefined
                    ? {}
                    : { handoffId: Redacted.make(observation.handoffId) }),
                  ...(observation.durationMs === undefined
                    ? {}
                    : { durationMs: observation.durationMs }),
                }).pipe(
                  Effect.mapError(() =>
                    protocolError("Cloudflare returned a malformed browser handoff state"),
                  ),
                ),
              ),
            ),
            currentPagePreflight,
            true,
            false,
          ),
          close,
        };
      }).pipe(Effect.provideService(Scope.Scope, scope)),
    );

    return {
      sessionId: Redacted.make(sessionIdValue),
      connect: Effect.suspend(() => (state.closed.value ? expiredError() : connect)),
    };
  });

  return BrowserRunInteractiveHost.of({
    acquire,
    open: (policy) => acquire(policy).pipe(Effect.flatMap((acquired) => acquired.connect)),
    resume: (checkpoint, options) =>
      Schema.decodeEffect(Schema.toType(BrowserRunInteractiveCheckpoint))(checkpoint).pipe(
        Effect.mapError(() => policyError("The browser checkpoint is malformed")),
        Effect.flatMap((decoded) => acquire(decoded.policy, decoded, options.pendingInput)),
        Effect.flatMap((acquired) => acquired.connect),
        Effect.catchTag("InteractiveBrowserProtocolError", (error) =>
          failReportedProtocol("interactive.resume", error, {
            stage: "preparation",
            dispatch: "not-dispatched",
            session: "disconnected",
          }),
        ),
      ),
    closeSession,
    cleanupSemantics: "confirmed-terminal",
  });
};

/** Cloudflare host controls and private session identity for one scoped Browser Run pass. */
export const browserRunInteractiveHostLayer = (): Layer.Layer<
  BrowserRunInteractiveHost,
  never,
  BrowserRunInteractiveBinding
> =>
  Layer.effect(
    BrowserRunInteractiveHost,
    Effect.gen(function* () {
      return makeHostService(yield* BrowserRunInteractiveBinding);
    }),
  );

/** Worker-only generic adapter; Cloudflare identity and controls remain host-only. */
export const browserRunInteractiveLayer = (): Layer.Layer<
  InteractiveBrowser,
  never,
  BrowserRunInteractiveBinding
> =>
  Layer.effect(
    InteractiveBrowser,
    Effect.gen(function* () {
      const binding = yield* BrowserRunInteractiveBinding;
      const host = makeHostService(binding);

      return InteractiveBrowser.of({
        open: (policy) => host.open(policy).pipe(Effect.map((session) => session.handle)),
      });
    }),
  );

/** Resolved Worker binding, cleanup credentials, and optional initial viewport. */
export interface CloudflareInteractiveBrowserOptions extends BrowserRunLifecycleOptions {
  readonly browser: BrowserRun;
  readonly viewport?: BrowserRunViewport;
}

const interactiveBindingLayer = (options: CloudflareInteractiveBrowserOptions) =>
  BrowserRunInteractiveBinding.layer(options).pipe(
    Layer.provide(BrowserRunSessionLifecycle.layer(options)),
  );

/** Worker-only assembly; importing ordinary capture never loads Puppeteer. */
export const CloudflareInteractiveBrowser = {
  /**
   * Assemble binding, confirmed-session cleanup, and the generic browser service.
   * HttpClient and construction errors remain visible. Opening a pass still requires
   * an explicit policy and Scope; no browser is launched during Layer construction.
   */
  layer: (options: CloudflareInteractiveBrowserOptions) =>
    browserRunInteractiveLayer().pipe(Layer.provide(interactiveBindingLayer(options))),
  /** Opt into private host controls instead of exposing them through the generic service. */
  hostLayer: (options: CloudflareInteractiveBrowserOptions) =>
    browserRunInteractiveHostLayer().pipe(Layer.provide(interactiveBindingLayer(options))),
};
