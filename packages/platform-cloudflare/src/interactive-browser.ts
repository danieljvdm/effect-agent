/// <reference types="@cloudflare/workers-types" />

import puppeteer, {
  type Browser,
  type BrowserContext,
  type HTTPRequest,
  type Page,
} from "@cloudflare/puppeteer";
import {
  BrowserActionResult,
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
  SandboxImplementation,
  type BrowserHandle,
  type InteractiveBrowserError,
} from "@effect-agent/sandbox";
import { Context, Duration, Effect, Layer, Option, Ref, Schema, Semaphore } from "effect";

export const browserRunInteractiveImplementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "cloudflare-browser-run-interactive",
});

const MIN_KEEP_ALIVE_MILLIS = 10_000;
const MAX_KEEP_ALIVE_MILLIS = 600_000;
const MAX_TEXT_LENGTH = 8 * 1024 * 1024;
const BoundedRemoteText = Schema.String.check(Schema.isMaxLength(MAX_TEXT_LENGTH));
const TextObservation = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Text"), text: BoundedRemoteText }),
  Schema.Struct({ _tag: Schema.Literal("MissingElement") }),
  Schema.Struct({ _tag: Schema.Literal("OverLimit"), observed: Schema.Natural }),
]);

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
}

export interface BrowserRunInteractiveContext {
  readonly newPage: () => Promise<BrowserRunInteractivePage>;
  readonly close: () => Promise<void>;
}

export interface BrowserRunInteractiveBrowser {
  readonly createContext: () => Promise<BrowserRunInteractiveContext>;
  readonly close: () => Promise<void>;
  readonly isConnected: () => boolean;
  readonly onDisconnected: (listener: () => void) => void;
  readonly offDisconnected: (listener: () => void) => void;
}

/** Host-supplied Browser Run binding projected into one fakeable launch operation. */
export class BrowserRunInteractiveBinding extends Context.Service<
  BrowserRunInteractiveBinding,
  {
    readonly launch: (keepAliveMillis: number) => Promise<BrowserRunInteractiveBrowser>;
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
    });
  }
}

const makeProductionRequest = (request: HTTPRequest): BrowserRunInteractiveRequest => ({
  url: () => request.url(),
  abort: () => request.abort("blockedbyclient"),
  continue: () => request.continue(),
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
          if (!("value" in element)) {
            throw new Error("The selector did not resolve to a fillable field");
          }
          const focus = Reflect.get(element, "focus");
          if (typeof focus === "function") Reflect.apply(focus, element, []);
          Reflect.set(element, "value", nextValue);
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
  };
};

const makeProductionContext = (context: BrowserContext): BrowserRunInteractiveContext => ({
  newPage: async () => makeProductionPage(await context.newPage()),
  close: () => context.close(),
});

const makeProductionBrowser = (browser: Browser): BrowserRunInteractiveBrowser => ({
  createContext: async () => makeProductionContext(await browser.createBrowserContext()),
  close: () => browser.close(),
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

const snapshotPolicy = (
  input: InteractiveBrowserPolicy,
): Effect.Effect<InteractiveBrowserPolicySnapshot, InteractiveBrowserPolicyDeniedError> =>
  Schema.decodeUnknownEffect(InteractiveBrowserPolicy)(input).pipe(
    Effect.mapError(() => policyError("The interactive browser policy is malformed")),
    Effect.map((decoded) =>
      Object.freeze({
        allowedHosts: Object.freeze([...decoded.allowedHosts]),
        maxActions: decoded.maxActions,
        maxElapsedMillis: decoded.maxElapsedMillis,
        maxReturnedBytes: decoded.maxReturnedBytes,
      }),
    ),
  );

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

interface CloseableRemote {
  readonly close: () => Promise<void>;
}

const closeLateAcquisition = async <A extends CloseableRemote>(
  signal: AbortSignal,
  acquire: () => Promise<A>,
): Promise<A> => {
  const acquired = await acquire();
  if (!signal.aborted) return acquired;

  // The SDK does not accept AbortSignal. If an acquisition settles after Effect
  // has interrupted it, ownership never reaches Scope, so close it here instead.
  try {
    await acquired.close();
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
  }).pipe(Effect.catchCause(() => Effect.logWarning(warning)));

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
  readonly disconnected: { value: boolean };
  readonly uncertain: { value: boolean };
  readonly violation: { value: BrowserFailure | undefined };
  readonly pendingRequests: Set<Promise<void>>;
}

const stateFailure = (state: HandleState): BrowserFailure | undefined => {
  if (state.violation.value !== undefined) return state.violation.value;
  if (state.disconnected.value || state.uncertain.value) return expiredError();
  return undefined;
};

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
): Effect.fn.Return<BrowserHandle> {
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

  return {
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
  };
});

/** Worker-only Cloudflare Puppeteer adapter; the caller supplies the Browser Run binding Layer. */
export const browserRunInteractiveLayer = (): Layer.Layer<
  InteractiveBrowser,
  never,
  BrowserRunInteractiveBinding
> =>
  Layer.effect(
    InteractiveBrowser,
    Effect.gen(function* () {
      const binding = yield* BrowserRunInteractiveBinding;
      return InteractiveBrowser.of({
        open: (policy) =>
          Effect.gen(function* () {
            const fixedPolicy = yield* snapshotPolicy(policy);
            const startedAt = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
            const state: HandleState = {
              disconnected: { value: false },
              uncertain: { value: false },
              violation: { value: undefined },
              pendingRequests: new Set(),
            };
            const browser = yield* Effect.acquireRelease(
              withinDeadline(
                Effect.tryPromise({
                  try: (signal) =>
                    closeLateAcquisition(signal, () =>
                      binding.launch(keepAliveMillis(fixedPolicy)),
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
              (acquired) =>
                Effect.sync(() => {
                  state.disconnected.value = true;
                }).pipe(
                  Effect.andThen(
                    closeWithWarning(acquired.close, "Closing the interactive browser failed"),
                  ),
                ),
              { interruptible: true },
            );

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
                Effect.sync(() => {
                  browser.offDisconnected(disconnected);
                }).pipe(
                  Effect.catchCause(() =>
                    Effect.logWarning("Removing the browser disconnect listener failed"),
                  ),
                ),
            );
            const connected = yield* Effect.try({
              try: browser.isConnected,
              catch: (cause) =>
                protocolError("Reading the Browser Run connection state failed", cause),
            });
            if (!connected) {
              state.disconnected.value = true;
              return yield* expiredError();
            }

            const context = yield* Effect.acquireRelease(
              withinDeadline(
                Effect.tryPromise({
                  try: (signal) => closeLateAcquisition(signal, browser.createContext),
                  catch: (cause) =>
                    state.disconnected.value || isRemoteClosure(cause)
                      ? expiredError()
                      : protocolError("Creating the browser context failed", cause),
                }),
                fixedPolicy,
                startedAt,
              ),
              (acquired) =>
                closeWithWarning(acquired.close, "Closing the interactive browser context failed"),
              { interruptible: true },
            );
            const page = yield* Effect.acquireRelease(
              withinDeadline(
                Effect.tryPromise({
                  try: (signal) => closeLateAcquisition(signal, context.newPage),
                  catch: (cause) =>
                    state.disconnected.value || isRemoteClosure(cause)
                      ? expiredError()
                      : protocolError("Creating the browser page failed", cause),
                }),
                fixedPolicy,
                startedAt,
              ),
              (acquired) =>
                closeWithWarning(acquired.close, "Closing the interactive browser page failed"),
              { interruptible: true },
            );

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
                Effect.sync(() => {
                  page.offRequest(requestListener);
                }).pipe(
                  Effect.catchCause(() =>
                    Effect.logWarning("Removing the browser request policy failed"),
                  ),
                ),
            );

            yield* withinDeadline(
              Effect.tryPromise({
                try: () => page.setRequestInterception(true),
                catch: (cause) =>
                  protocolError("Installing the browser request policy failed", cause),
              }),
              fixedPolicy,
              startedAt,
            );

            yield* withinDeadline(awaitPendingRequests(state), fixedPolicy, startedAt);
            const setupFailure = stateFailure(state);
            if (setupFailure !== undefined) return yield* setupFailure;

            return yield* makeHandle(page, fixedPolicy, startedAt, state);
          }),
      });
    }),
  );
