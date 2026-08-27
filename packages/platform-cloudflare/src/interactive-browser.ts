/// <reference types="@cloudflare/workers-types" />

import puppeteer, {
  type Browser,
  type BrowserContext,
  type CDPSession,
  type HTTPRequest,
  type Page,
} from "@cloudflare/puppeteer";
import {
  BrowserActionResult,
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
  InteractiveBrowserUnsupportedError,
  PageScreenshotResult,
  SandboxImplementation,
  type BrowserHandle,
  type InteractiveBrowserError,
} from "@effect-agent/sandbox";
import {
  Context,
  Duration,
  Effect,
  Layer,
  Option,
  Redacted,
  Ref,
  Schema,
  Semaphore,
  type Scope,
} from "effect";

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
const CLOSE_SESSION_TIMEOUT_MILLIS = 10_000;
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
  readonly allowedHosts: ReadonlyArray<string>;
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
  readonly close: () => Promise<void>;
  readonly setBypassServiceWorker: (enabled: boolean) => Promise<void>;
  readonly setRequestInterception: (enabled: boolean) => Promise<void>;
  readonly onRequest: (listener: BrowserRunInteractiveRequestListener) => void;
  readonly offRequest: (listener: BrowserRunInteractiveRequestListener) => void;
  readonly goto: (url: string) => Promise<void>;
  readonly url: () => unknown;
  readonly readText: (selector: string | undefined, maximumBytes: number) => Promise<unknown>;
  readonly fill: (selector: string, value: string) => Promise<void>;
  readonly click: (selector: string) => Promise<void>;
  readonly screenshot: (fullPage: boolean) => Promise<unknown>;
  readonly scroll: (deltaX: number, deltaY: number) => Promise<void>;
  readonly createCdpSession: () => Promise<BrowserRunInteractiveCdpSession>;
}

export interface BrowserRunInteractiveContext {
  readonly newPage: () => Promise<BrowserRunInteractivePage>;
  readonly close: () => Promise<void>;
}

export interface BrowserRunInteractiveBrowser {
  readonly createContext: () => Promise<BrowserRunInteractiveContext>;
  readonly close: () => Promise<void>;
  readonly sessionId: () => unknown;
  readonly isConnected: () => boolean;
  readonly onDisconnected: (listener: () => void) => void;
  readonly offDisconnected: (listener: () => void) => void;
}

/** Host-supplied Browser Run binding projected into one fakeable launch operation. */
export class BrowserRunInteractiveBinding extends Context.Service<
  BrowserRunInteractiveBinding,
  {
    readonly launch: (keepAliveMillis: number) => Promise<BrowserRunInteractiveBrowser>;
    readonly connect: (sessionId: string) => Promise<BrowserRunInteractiveBrowser>;
  }
>()("@effect-agent/platform-cloudflare/BrowserRunInteractiveBinding") {
  static layer(options: {
    readonly browser: BrowserRun;
  }): Layer.Layer<BrowserRunInteractiveBinding> {
    return Layer.succeed(BrowserRunInteractiveBinding)({
      launch: async (keepAliveMillis) =>
        makeProductionBrowser(
          await puppeteer.launch(options.browser, {
            keep_alive: keepAliveMillis,
          }),
        ),
      connect: async (sessionId) =>
        makeProductionBrowser(await puppeteer.connect(options.browser, sessionId)),
    });
  }
}

export interface BrowserRunInteractiveSession {
  readonly handle: BrowserHandle;
  readonly sessionId: Redacted.Redacted<string>;
  readonly getLiveView: (
    request: BrowserRunLiveViewRequest,
  ) => Effect.Effect<BrowserRunLiveViewResult, InteractiveBrowserError>;
  readonly handoff: (
    request: BrowserRunHandoffRequest,
  ) => Effect.Effect<BrowserRunHandoffResult, InteractiveBrowserError>;
  readonly getHandoffState: Effect.Effect<BrowserRunHandoffState, InteractiveBrowserError>;
  readonly close: Effect.Effect<void, InteractiveBrowserError>;
}

/** Cloudflare host authority kept separate from the provider-neutral browser handle. */
export class BrowserRunInteractiveHost extends Context.Service<
  BrowserRunInteractiveHost,
  {
    readonly open: (
      policy: InteractiveBrowserPolicy,
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

const makeProductionPage = (page: Page): BrowserRunInteractivePage => {
  const listeners = new Map<BrowserRunInteractiveRequestListener, (request: HTTPRequest) => void>();
  return {
    close: () => page.close(),
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
      await page.goto(url, { waitUntil: "networkidle0", timeout: 0 });
    },
    url: () => page.url(),
    readText: (selector, maximumBytes) =>
      page.evaluate(
        (requestedSelector, maximum) => {
          const pageDocument = Reflect.get(globalThis, "document");
          const element =
            requestedSelector === undefined
              ? Reflect.get(pageDocument, "body")
              : Reflect.apply(Reflect.get(pageDocument, "querySelector"), pageDocument, [
                  requestedSelector,
                ]);
          if (element === null) return { _tag: "MissingElement" };
          const innerText = Reflect.get(element, "innerText");
          const textContent = Reflect.get(element, "textContent");
          const text =
            typeof innerText === "string"
              ? innerText
              : typeof textContent === "string"
                ? textContent
                : "";
          const observed = new TextEncoder().encode(text).byteLength;
          return observed > maximum ? { _tag: "OverLimit", observed } : { _tag: "Text", text };
        },
        selector,
        maximumBytes,
      ),
    fill: async (selector, value) => {
      await page.$eval(
        selector,
        (element, nextValue) => {
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
        },
        value,
      );
    },
    click: (selector) => page.click(selector),
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
  };
};

const makeProductionContext = (context: BrowserContext): BrowserRunInteractiveContext => ({
  newPage: async () => makeProductionPage(await context.newPage()),
  close: () => context.close(),
});

const makeProductionBrowser = (browser: Browser): BrowserRunInteractiveBrowser => ({
  createContext: async () => makeProductionContext(await browser.createBrowserContext()),
  close: () => browser.close(),
  sessionId: () => browser.sessionId(),
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

const actionError = (operation: BrowserOperation, cause?: unknown): InteractiveBrowserActionError =>
  InteractiveBrowserActionError.make({
    implementation: browserRunInteractiveImplementation,
    operation,
    message: `The interactive browser ${operation} operation failed`,
    ...(cause === undefined ? {} : { cause }),
  });

const policyError = (message: string): InteractiveBrowserPolicyDeniedError =>
  InteractiveBrowserPolicyDeniedError.make({
    implementation: browserRunInteractiveImplementation,
    message,
  });

const expiredError = (): InteractiveBrowserExpiredError =>
  InteractiveBrowserExpiredError.make({
    implementation: browserRunInteractiveImplementation,
    message: "The remote browser is no longer usable",
  });

const causeText = (cause: unknown): string => {
  if (cause instanceof Error) return cause.message.slice(0, 8_000);
  return String(cause).slice(0, 8_000);
};

const isCapacityRefusal = (cause: unknown): boolean =>
  /(^|\D)429(\D|$)|browser time limit|capacity|too many concurrent/i.test(causeText(cause));

const isRemoteClosure = (cause: unknown): boolean =>
  /target closed|browser.*closed|session.*closed|connection.*closed|not connected|websocket.*closed/i.test(
    causeText(cause),
  );

const snapshotPolicy = Effect.fn("BrowserRunInteractive.snapshotPolicy")(function* (
  input: InteractiveBrowserPolicy,
): Effect.fn.Return<
  InteractiveBrowserPolicySnapshot,
  InteractiveBrowserPolicyDeniedError | InteractiveBrowserUnsupportedError
> {
  const decoded = yield* Schema.decodeUnknownEffect(InteractiveBrowserPolicy)(input).pipe(
    Effect.mapError(() => policyError("The interactive browser policy is malformed")),
  );
  if (decoded.network._tag === "PublicWeb") {
    return yield* InteractiveBrowserUnsupportedError.make({
      implementation: browserRunInteractiveImplementation,
      feature: "policy",
      message:
        "Cloudflare Browser Run cannot enforce the PublicWeb network policy for all session traffic",
    });
  }
  return Object.freeze({
    allowedHosts: Object.freeze([...decoded.network.allowedHosts]),
    maxActions: decoded.maxActions,
    maxElapsedMillis: decoded.maxElapsedMillis,
    maxReturnedBytes: decoded.maxReturnedBytes,
  });
});

const hostAllowed = (policy: InteractiveBrowserPolicySnapshot, value: string): boolean => {
  try {
    const url = new URL(value);
    return (
      url.protocol === "https:" &&
      url.username === "" &&
      url.password === "" &&
      policy.allowedHosts.some((host) => host === url.host)
    );
  } catch {
    return false;
  }
};

const keepAliveMillis = (policy: InteractiveBrowserPolicySnapshot): number =>
  Math.max(MIN_KEEP_ALIVE_MILLIS, Math.min(MAX_KEEP_ALIVE_MILLIS, policy.maxElapsedMillis));

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
    // No caller remains to observe a late cleanup failure, and provider details
    // must not escape through an unhandled rejection.
  }
  throw new Error("The interrupted browser acquisition completed late");
};

const closeWithWarning = (close: () => Promise<void>, warning: string): Effect.Effect<void> =>
  Effect.tryPromise({
    try: close,
    catch: () => protocolError(warning),
  }).pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(CLEANUP_STEP_TIMEOUT_MILLIS),
      orElse: () => Effect.fail(protocolError(warning)),
    }),
    Effect.catchCause(() => Effect.logWarning(warning)),
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
  readonly violation: { value: BrowserFailure | undefined };
  readonly pendingRequests: Set<Promise<void>>;
}

const stateFailure = (state: HandleState): BrowserFailure | undefined => {
  if (state.violation.value !== undefined) return state.violation.value;
  if (state.closed.value || state.disconnected.value || state.uncertain.value) {
    return expiredError();
  }
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
  ) => Effect.Effect<A, BrowserFailure>;
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
    } catch {
      state.violation.value = protocolError("Resolving an intercepted browser request failed");
      return;
    }
    const observed = settlement
      .catch(() => {
        state.violation.value = protocolError("Resolving an intercepted browser request failed");
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
  const actions = yield* Ref.make(0);

  const remote = <A>(operation: BrowserOperation, evaluate: () => Promise<A>) =>
    Effect.tryPromise({
      try: evaluate,
      catch: (cause) => {
        if (state.disconnected.value || isRemoteClosure(cause)) {
          state.disconnected.value = true;
          return expiredError();
        }
        return actionError(operation, cause);
      },
    });

  const run = <A>(
    effect: Effect.Effect<A, BrowserFailure>,
    preflight: Effect.Effect<void, BrowserFailure> = Effect.void,
  ) =>
    permits
      .withPermitsIfAvailable(1)(
        Effect.gen(function* () {
          const unavailable = stateFailure(state);
          if (unavailable !== undefined) return yield* unavailable;
          yield* preflight;

          const admitted = yield* Ref.modify(actions, (count) =>
            count >= policy.maxActions
              ? [{ allowed: false, observed: count + 1 }, count]
              : [{ allowed: true, observed: count + 1 }, count + 1],
          );
          if (!admitted.allowed) {
            return yield* InteractiveBrowserLimitError.make({
              implementation: browserRunInteractiveImplementation,
              limit: "actions",
              maximum: policy.maxActions,
              observed: admitted.observed,
              message: "The browser action limit was reached",
            });
          }

          const completed = effect.pipe(
            Effect.catch((error) => {
              const failure = stateFailure(state);
              return Effect.fail(failure ?? error);
            }),
            Effect.flatMap((result) =>
              awaitPendingRequests(state).pipe(
                Effect.flatMap(() => {
                  const failure = stateFailure(state);
                  return failure === undefined ? Effect.succeed(result) : Effect.fail(failure);
                }),
              ),
            ),
          );
          return yield* withinDeadline(completed, policy, startedAt, () => {
            state.uncertain.value = true;
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                state.uncertain.value = true;
              }),
            ),
            Effect.catch((error) => {
              if (Schema.is(InteractiveBrowserLimitError)(error) && error.limit === "elapsed") {
                return Effect.fail(error);
              }
              const failure = stateFailure(state);
              return Effect.fail(failure ?? error);
            }),
          );
        }),
      )
      .pipe(
        Effect.flatMap((result) =>
          Option.isSome(result)
            ? Effect.succeed(result.value)
            : Effect.fail(
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
          hostAllowed(policy, request.url)
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
            return yield* actionError("read-text");
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
          return yield* Schema.decodeUnknownEffect(BrowserTextResult)({
            text: observation.text,
          }).pipe(
            Effect.mapError((cause) =>
              protocolError("The browser returned malformed page text", cause),
            ),
          );
        }),
      ),
    fill: (request) =>
      run(
        remote("fill", () => page.fill(request.selector, request.value)).pipe(
          Effect.andThen(decodeActionResult(page, policy)),
        ),
      ),
    click: (request) =>
      run(
        remote("click", () => page.click(request.selector)).pipe(
          Effect.andThen(decodeActionResult(page, policy)),
        ),
      ),
    screenshot: (request) =>
      Schema.decodeUnknownEffect(BrowserScreenshotRequest)(request).pipe(
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
              return yield* Schema.decodeUnknownEffect(PageScreenshotResult)({
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
          ),
        ),
      ),
    scroll: (request) =>
      Schema.decodeUnknownEffect(BrowserScrollRequest)(request).pipe(
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
      const cdp = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: (signal) =>
            closeLateAcquisition(signal, page.createCdpSession, (acquired) => acquired.detach()),
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
        try: () => cdp.send(command, parameters),
        catch: (cause) =>
          state.disconnected.value || isRemoteClosure(cause)
            ? expiredError()
            : protocolError("The Cloudflare browser control command failed", cause),
      });
      return yield* Schema.decodeUnknownEffect(output)(raw).pipe(
        Effect.mapError(() => protocolError(malformedMessage)),
      );
    }),
  );

const makeHostService = (
  binding: BrowserRunInteractiveBinding["Service"],
): BrowserRunInteractiveHost["Service"] => {
  const open = Effect.fn("BrowserRunInteractiveHost.open")(function* (
    policy: InteractiveBrowserPolicy,
  ): Effect.fn.Return<BrowserRunInteractiveSession, InteractiveBrowserError, Scope.Scope> {
    const fixedPolicy = yield* snapshotPolicy(policy);
    const startedAt = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const state: HandleState = {
      closed: { value: false },
      disconnected: { value: false },
      uncertain: { value: false },
      violation: { value: undefined },
      pendingRequests: new Set(),
    };
    const lifecycle = {
      managedTeardownInstalled: false,
      explicitCloseInvoked: false,
    };
    const closers: Array<CloseEntry> = [];
    const releaseBeforeManaged = (entry: CloseEntry): Effect.Effect<void> =>
      Effect.suspend(() =>
        lifecycle.managedTeardownInstalled
          ? Effect.void
          : entry.close.pipe(Effect.catchCause(() => Effect.logWarning(entry.warning))),
      );

    const browser = yield* Effect.acquireRelease(
      withinDeadline(
        Effect.tryPromise({
          try: (signal) =>
            closeLateAcquisition(
              signal,
              () => binding.launch(keepAliveMillis(fixedPolicy)),
              (acquired) => acquired.close(),
            ),
          catch: (cause) =>
            isCapacityRefusal(cause)
              ? InteractiveBrowserCapacityError.make({
                  implementation: browserRunInteractiveImplementation,
                  message: "Browser Run has no capacity for a new browser session",
                })
              : protocolError("Launching the Browser Run session failed", cause),
        }),
        fixedPolicy,
        startedAt,
      ),
      (acquired) => {
        state.disconnected.value = true;
        return releaseBeforeManaged(
          closeEntry(acquired.close, "Closing the interactive browser failed"),
        );
      },
      { interruptible: true },
    );
    closers.push(closeEntry(browser.close, "Closing the interactive browser failed"));

    const disconnected = () => {
      state.disconnected.value = true;
    };
    yield* Effect.acquireRelease(
      Effect.try({
        try: () => browser.onDisconnected(disconnected),
        catch: (cause) => protocolError("Installing the browser disconnect listener failed", cause),
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

    const sessionIdValue = yield* Effect.try({
      try: browser.sessionId,
      catch: (cause) => protocolError("Reading the Browser Run session identity failed", cause),
    }).pipe(
      Effect.flatMap((value) =>
        Schema.decodeUnknownEffect(BrowserRunSessionId)(value).pipe(
          Effect.mapError(() => protocolError("The Browser Run session identity was malformed")),
        ),
      ),
    );

    const context = yield* Effect.acquireRelease(
      withinDeadline(
        Effect.tryPromise({
          try: (signal) =>
            closeLateAcquisition(signal, browser.createContext, (acquired) => acquired.close()),
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
    );
    closers.push(closeEntry(context.close, "Closing the interactive browser context failed"));
    const page = yield* Effect.acquireRelease(
      withinDeadline(
        Effect.tryPromise({
          try: (signal) =>
            closeLateAcquisition(signal, context.newPage, (acquired) => acquired.close()),
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
    );
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
        catch: (cause) => protocolError("Installing the browser request listener failed", cause),
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

    const teardown = yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const cached = yield* Effect.cached(runTeardown(closers));
        lifecycle.managedTeardownInstalled = true;
        yield* Effect.addFinalizer(() =>
          Effect.uninterruptible(
            Effect.sync(() => {
              state.closed.value = true;
              state.disconnected.value = true;
            }).pipe(
              Effect.andThen(cached),
              Effect.flatMap((failures) =>
                lifecycle.explicitCloseInvoked
                  ? Effect.void
                  : Effect.forEach(failures, (failure) => Effect.logWarning(failure.warning)).pipe(
                      Effect.asVoid,
                    ),
              ),
            ),
          ),
        );
        return cached;
      }),
    );

    const close: Effect.Effect<void, InteractiveBrowserError> = Effect.uninterruptible(
      Effect.sync(() => {
        lifecycle.explicitCloseInvoked = true;
        state.closed.value = true;
        state.disconnected.value = true;
      }).pipe(
        Effect.andThen(teardown),
        Effect.flatMap((failures) =>
          failures[0] === undefined ? Effect.void : Effect.fail(failures[0].error),
        ),
      ),
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

    return {
      handle: runtime.handle,
      sessionId: Redacted.make(sessionIdValue),
      getLiveView: (request) =>
        Schema.decodeUnknownEffect(BrowserRunLiveViewRequest)(request).pipe(
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
                  Schema.decodeUnknownEffect(BrowserRunLiveViewResult)({
                    devtoolsFrontendUrl: Redacted.make(observation.devtoolsFrontendUrl),
                  }).pipe(
                    Effect.mapError(() =>
                      protocolError("Cloudflare returned a malformed Live View response"),
                    ),
                  ),
                ),
              ),
              currentPagePreflight.pipe(Effect.andThen(requestFitsSession(decoded.expiresInMs))),
            ),
          ),
        ),
      handoff: (request) =>
        Schema.decodeUnknownEffect(BrowserRunHandoffRequest)(request).pipe(
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
                  Schema.decodeUnknownEffect(BrowserRunHandoffResult)({
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
            Schema.decodeUnknownEffect(BrowserRunHandoffState)({
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
      ),
      close,
    };
  });

  const closeSession = Effect.fn("BrowserRunInteractiveHost.closeSession")(function* (
    sessionId: Redacted.Redacted<string>,
  ) {
    const decoded = yield* Schema.decodeUnknownEffect(Schema.Redacted(BrowserRunSessionId))(
      sessionId,
    ).pipe(
      Effect.mapError(() => policyError("The Browser Run cleanup session identity is malformed")),
    );
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const closeAttempted = { value: false };
        const browser = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: (signal) =>
              closeLateAcquisition(
                signal,
                () => binding.connect(Redacted.value(decoded)),
                (acquired) => acquired.close(),
              ),
            catch: (cause) => actionError("close", cause),
          }),
          (acquired) =>
            closeAttempted.value
              ? Effect.void
              : closeWithWarning(acquired.close, "Closing the leaked Browser Run session failed"),
          { interruptible: true },
        );
        return yield* Effect.tryPromise({
          try: () => {
            // Mark and start are synchronous so interruption cannot suppress the
            // Scope fallback before the one remote close attempt begins.
            closeAttempted.value = true;
            return browser.close();
          },
          catch: (cause) => actionError("close", cause),
        });
      }),
    ).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(CLOSE_SESSION_TIMEOUT_MILLIS),
        orElse: () => Effect.fail(actionError("close")),
      }),
    );
  });

  return BrowserRunInteractiveHost.of({ open, closeSession });
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
