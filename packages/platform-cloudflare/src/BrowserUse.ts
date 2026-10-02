import { Deferred, Effect, Fiber, Layer, Schema, Semaphore } from "effect";
import {
  type ActionResult,
  type NavigateRequest,
  Action,
  BrowserActions,
  BrowserControl,
  BrowserUseError,
  InspectRequest,
  Observation,
  ScrollRequest,
  WaitRequest,
  type Control,
  DialogRequest,
  PressRequest,
  TabRequest,
} from "effect-agent/browser-use";
import { InteractiveBrowserTargetUrl } from "effect-agent/interactive-browser";
import {
  ElementHandle,
  type Frame,
  type Page,
  type Dialog,
} from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import type { BrowserSession } from "./BrowserSession.ts";
import { checkDom, checkFrameDom, inspectDom, waitDom } from "./internal/browser-dom.ts";

export type Command =
  | { readonly kind: "observe" | "screenshot" }
  | { readonly kind: "inspect"; readonly request: typeof InspectRequest.Type }
  | { readonly kind: "navigate"; readonly request: typeof NavigateRequest.Type }
  | { readonly kind: "scroll"; readonly request: typeof ScrollRequest.Type }
  | { readonly kind: "wait"; readonly request: typeof WaitRequest.Type }
  | { readonly kind: "select-tab"; readonly request: typeof TabRequest.Type }
  | { readonly kind: "respond-dialog"; readonly request: typeof DialogRequest.Type }
  | {
      readonly kind: "act";
      readonly action: Action | ({ readonly kind: "press" } & typeof PressRequest.Type);
    };

export interface Options<R = never> {
  /** Rechecked under the native session lock. The host owns origins, identity and consent. */
  readonly authorize: (
    command: Command,
    context: AuthorizationContext,
  ) => Effect.Effect<void, BrowserUseError, R>;
  readonly maxActions: number;
  readonly maxReturnedBytes: number;
}

/** Host-only current SDK metadata. Opaque refs never replace origin or target authority. */
export interface AuthorizationContext {
  /** Absent only before the initial attachment read; the host authorizes that attachment. */
  readonly pageUrl?: string;
  readonly frameUrl?: string;
  /** Destination of an observed select-tab command, checked again before selection. */
  readonly tabUrl?: string;
  readonly target?: typeof Control.Type;
}

const Limits = Schema.Struct({
  maxActions: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_000 })),
  maxReturnedBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 8 * 1024 * 1024 })),
});

const invalid = (message: string) =>
  new BrowserUseError({ code: "invalid", message, dispatch: "not-dispatched" });

// URL evidence identifies pages; opaque document payloads are not page observations.
// Authorization and navigation fencing always use the complete native SDK URL.
const observedUrl = (url: string): string => {
  const payload = url.startsWith("data:") ? url.indexOf(",") : -1;

  return payload < 0
    ? url.slice(0, 8_192)
    : `${url.slice(0, Math.min(payload, 256))},[payload omitted]`;
};

/** One application-neutral controller over an existing scoped native attachment.
 * No allocation, model, provider choice, automatic retry, or arbitrary page JS is exposed.
 * References expire on inspection; outstanding native work retains the session's fencing.
 */
export const make = Effect.fnUntraced(function* <R>(
  session: Pick<BrowserSession, "run">,
  options: Options<R>,
) {
  const authorityServices = yield* Effect.context<R>();

  const authorize = (command: Command, context: AuthorizationContext) =>
    options.authorize(command, context).pipe(Effect.provideContext(authorityServices));

  const limits = yield* Schema.decodeEffect(Limits)({
    maxActions: options.maxActions,
    maxReturnedBytes: options.maxReturnedBytes,
  }).pipe(Effect.mapError(() => invalid("Invalid browser controller limits.")));

  const lock = yield* Semaphore.make(1);
  const ownerScope = yield* Effect.scope;

  type Dispatch = NonNullable<(typeof ActionResult.Type)["dispatch"]>;
  let pendingInput: { ref: string; fiber: Fiber.Fiber<Dispatch, BrowserUseError> } | undefined;
  let inputPage: Page | undefined;
  let dialogSignal: Deferred.Deferred<void> | undefined;
  let nextInput = 0;
  let lastTabs: NonNullable<(typeof Observation.Type)["tabs"]> = [];
  let latestObservation: typeof Observation.Type | null = null;

  const clearInput = () => {
    pendingInput = undefined;
    dialogSignal = undefined;
    inputPage = undefined;
  };

  const targets = new Map<string, { frame: string; control: typeof Control.Type }>();
  const frames = new Set<string>();
  const frameIds = new WeakMap<Frame, string>();
  const tabIds = new WeakMap<Page, string>();
  const tabs = new Map<string, Page>();
  const pendingDialogs = new Map<Page, { ref: string; dialog: Dialog }>();

  const listeners = new Map<
    Page,
    { dialog: (dialog: Dialog) => void; popup: (page: Page | null) => void }
  >();

  let activePage: Page | undefined;
  let lastNativePage: Page | undefined;
  let nextTab = 0;
  let nextDialog = 0;

  const tabId = (page: Page) => {
    let id = tabIds.get(page);

    if (id === undefined) {
      id = `t${scopeId}-${nextTab++}`;
      tabIds.set(page, id);
    }

    return id;
  };

  const attach = (page: Page) => {
    if (listeners.has(page) || listeners.size >= 32) return;

    const dialog = (dialog: Dialog) => {
      pendingDialogs.set(page, { ref: `dialog-${scopeId}-${nextDialog++}`, dialog });
      if (page === inputPage && dialogSignal !== undefined)
        Deferred.doneUnsafe(dialogSignal, Effect.void);
    };

    const popup = (page: Page | null) => {
      if (page !== null) attach(page);
    };

    listeners.set(page, { dialog, popup });
    page.on("dialog", dialog);
    page.on("popup", popup);
  };

  yield* Effect.addFinalizer(() =>
    session
      .run(Effect.void, async () => {
        for (const { dialog } of pendingDialogs.values()) {
          try {
            await dialog.dismiss();
          } catch {
            /* The session owner retains cleanup authority. */
          }
        }
      })
      .pipe(
        Effect.ignore,
        Effect.andThen(
          Effect.sync(() => {
            for (const [page, listener] of listeners) {
              page.off("dialog", listener.dialog);
              page.off("popup", listener.popup);
            }
            listeners.clear();
            pendingDialogs.clear();
            tabs.clear();
            activePage = undefined;
            targets.clear();
            latestObservation = null;
            frames.clear();
          }),
        ),
      ),
  );
  const scopeId = crypto.getRandomValues(new Uint32Array(2)).join("-");
  let nextFrame = 0;

  const frameId = (frame: Frame) => {
    let id = frameIds.get(frame);

    if (id === undefined) {
      id = `f${scopeId}-${nextFrame++}`;
      frameIds.set(frame, id);
    }

    return id;
  };

  const resolveFrame = (page: Page, ref: string) =>
    page.frames().find((frame) => frameIds.get(frame) === ref);

  let generation = 0;
  let consumed = 0;

  const native = <A>(
    command: Command,
    action: (page: Page, stage: (name: string) => void) => Promise<A>,
    timeoutMillis?: number,
    phase?: "prepare" | "input",
  ) =>
    Effect.gen(function* () {
      if (pendingInput !== undefined && phase !== "input" && command.kind !== "respond-dialog")
        return yield* invalid(
          "Native input is pending. Inspect and respond to its dialog before another browser operation.",
        );
      const span = yield* Effect.currentSpan.pipe(Effect.orDie);
      let authorizedUrl: string | undefined;
      let authorizedTabUrl: string | undefined;
      let authorizedFrame: { ref: string; url: string | undefined } | undefined;

      return yield* session.run(
        Effect.suspend(() => {
          const page = activePage ?? lastNativePage;

          const target =
            command.kind === "act"
              ? targets.get(command.action.ref)
              : command.kind === "scroll" && command.request.ref !== undefined
                ? targets.get(command.request.ref)
                : undefined;

          authorizedUrl = page?.url();
          authorizedTabUrl =
            command.kind === "select-tab" ? tabs.get(command.request.ref)?.url() : undefined;

          const frameRef =
            target?.frame ??
            (command.kind === "inspect" || command.kind === "wait"
              ? command.request.frame
              : undefined);

          authorizedFrame =
            frameRef === undefined
              ? undefined
              : {
                  ref: frameRef,
                  url: page === undefined ? undefined : resolveFrame(page, frameRef)?.url(),
                };

          return authorize(command, {
            ...(authorizedUrl === undefined ? {} : { pageUrl: authorizedUrl }),
            ...(authorizedTabUrl === undefined ? {} : { tabUrl: authorizedTabUrl }),
            ...(authorizedFrame === undefined ? {} : { frameUrl: authorizedFrame.url }),
            ...(target === undefined
              ? {}
              : {
                  target: target.control,
                }),
          });
        }).pipe(
          Effect.mapError(
            (error) =>
              new BrowserUseError({
                code: error.code,
                message: error.message,
                dispatch: "not-dispatched",
              }),
          ),
        ),
        async (page) => {
          attach(page);
          const selected = activePage ?? page;

          lastNativePage = page;

          if (selected.isClosed() || selected.browserContext() !== page.browserContext())
            throw new Error("Observed tab is unavailable");
          if (authorizedUrl !== undefined && selected.url() !== authorizedUrl)
            throw new Error("Page changed during authorization");
          if (
            authorizedFrame !== undefined &&
            resolveFrame(selected, authorizedFrame.ref)?.url() !== authorizedFrame.url
          )
            throw new Error("Frame changed during authorization");
          if (
            command.kind === "select-tab" &&
            tabs.get(command.request.ref)?.url() !== authorizedTabUrl
          )
            throw new Error("Tab changed during authorization");
          attach(selected);

          return action(selected, (name) => span.attribute("browser.native.stage", name));
        },
        timeoutMillis === undefined ? undefined : { timeoutMillis },
      );
    }).pipe(
      Effect.mapError((error) => {
        if (error._tag === "BrowserUseError") return error;
        const read = ["observe", "inspect", "wait", "screenshot"].includes(command.kind);

        return new BrowserUseError({
          code: "browser",
          message: read
            ? `Browser ${command.kind} failed (${error.reason}); cleanup ${error.cleanup}. No new input was dispatched.`
            : `Browser ${error.reason}; native outcome ${error.dispatch}; cleanup ${error.cleanup}. Input is never replayed.`,
          dispatch: read || error.dispatch === "not-dispatched" ? "not-dispatched" : "unknown",
        });
      }),
      Effect.withSpan(`BrowserUse.${command.kind}${phase === undefined ? "" : `.${phase}`}`),
    );

  const bounded = <A>(value: A) =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode(JSON.stringify(value)).byteLength;

      if (bytes > limits.maxReturnedBytes)
        return yield* invalid(
          "Browser result exceeds the host byte budget. Narrow inspection before acting.",
        );

      return value;
    });

  const inspect = Effect.fnUntraced(function* (
    request: typeof InspectRequest.Type,
    command: Command = { kind: "inspect", request },
  ) {
    yield* Schema.decodeEffect(InspectRequest)(request).pipe(
      Effect.mapError(() => invalid("Invalid inspection request.")),
    );
    const selectedFrame = request.frame;

    if (request.frame !== undefined && !frames.has(request.frame))
      return yield* invalid("Frame was not observed. Inspect the page first.");
    targets.clear();
    latestObservation = null;
    generation++;

    if (pendingInput?.fiber.pollUnsafe()?._tag === "Failure") {
      const failed = pendingInput.fiber;

      clearInput();
      yield* Fiber.join(failed);
    }
    const blocked = inputPage === undefined ? undefined : pendingDialogs.get(inputPage);

    if (pendingInput !== undefined && blocked !== undefined) {
      yield* authorize(command, { pageUrl: inputPage?.url() });

      const observation = yield* bounded({
        text: "Native input is awaiting this JavaScript dialog. Respond before further page operations; never replay the input.",
        controls: [],
        tabs: lastTabs,
        dialogs: [
          {
            ref: blocked.ref,
            type: blocked.dialog.type(),
            message: blocked.dialog.message().slice(0, 4096),
            defaultValue: blocked.dialog.defaultValue().slice(0, 4096),
          },
        ],
      });

      latestObservation = observation;

      return observation;
    }

    const value = yield* native(command, async (page) => {
      const currentTabs = await page.browserContext().pages();

      tabs.clear();
      currentTabs.slice(0, 32).forEach((tab) => {
        tabs.set(tabId(tab), tab);
        attach(tab);
      });

      const tabObservation = currentTabs
        .slice(0, 32)
        .map((tab) => ({ ref: tabId(tab), url: observedUrl(tab.url()), active: tab === page }));

      const pending = pendingDialogs.get(page);

      lastTabs = tabObservation;

      if (pending !== undefined)
        return {
          text: "A native JavaScript dialog blocks page interaction. Respond to its observed reference before continuing.",
          controls: [],
          tabs: tabObservation,
          truncated: currentTabs.length > 32,
          dialogs: [
            {
              ref: pending.ref,
              type: pending.dialog.type(),
              message: pending.dialog.message().slice(0, 4096),
              defaultValue: pending.dialog.defaultValue().slice(0, 4096),
            },
          ],
        };
      const currentFrames = page.frames();

      if (
        selectedFrame !== undefined &&
        command.kind !== "observe" &&
        !currentFrames.some((frame) => frameId(frame) === selectedFrame)
      )
        throw new Error("Observed frame is detached");
      frames.clear();
      currentFrames.slice(0, 32).forEach((frame) => frames.add(frameId(frame)));

      const inspected =
        selectedFrame === undefined
          ? [page.mainFrame()]
          : currentFrames.filter((frame) => frameId(frame) === selectedFrame);

      if (inspected.length === 0) inspected.push(page.mainFrame());

      let text = "";
      let readyState: (typeof Observation.Type)["readyState"];
      let truncated = currentFrames.length > 32 || currentTabs.length > 32;
      const controls: Array<typeof Control.Type> = [];

      for (const frame of inspected) {
        if (controls.length >= 256) {
          truncated = true;
          break;
        }
        const ref = frameId(frame);

        if (ref === undefined) continue;

        const read = async (): Promise<unknown> =>
          frame
            .isolatedRealm()
            .evaluate(
              inspectDom,
              request.selector,
              `r${scopeId}-${generation}-${controls.length}`,
              256 - controls.length,
              request.optionFilter,
            );

        let result = Schema.decodeUnknownSync(Observation)(await read());

        // Native click acknowledgement can precede document parsing. Settle this
        // explicit readiness condition before returning an empty loading document.
        if (result.readyState === "loading") {
          const ready = await frame
            .isolatedRealm()
            .waitForFunction(() => document.readyState !== "loading", {
              timeout: 2_000,
              polling: "raf",
            });

          await ready.dispose();
          result = Schema.decodeUnknownSync(Observation)(await read());
        }
        readyState = result.readyState;

        text += `\n[${ref}]${result.text}`;
        truncated ||= result.truncated ?? false;
        for (const control of result.controls) {
          const current = { ...control, frame: ref };

          targets.set(control.ref, { frame: ref, control: current });
          controls.push(current);
        }
      }

      return {
        text: text.slice(0, 24_000),
        controls,
        ...(readyState === undefined ? {} : { readyState }),
        truncated: truncated || text.length > 24_000,
        frames: currentFrames.slice(0, 32).map((frame) => ({
          ref: frameId(frame),
          name: frame.name().slice(0, 300),
          url: observedUrl(frame.url()),
          inspected: inspected.includes(frame),
        })),
        tabs: tabObservation,
        dialogs: [],
      };
    }).pipe(Effect.tapError(() => Effect.sync(() => targets.clear())));

    const observation = yield* bounded(value).pipe(
      Effect.tapError(() => Effect.sync(() => targets.clear())),
    );

    latestObservation = observation;

    return observation;
  });

  const charge = Effect.fnUntraced(function* () {
    if (consumed >= limits.maxActions) return yield* invalid("Browser action budget exhausted.");
    consumed++;
  });

  const checkedPoint = async (
    element: ElementHandle<Element>,
    frame: Frame,
    expected: typeof Control.Type,
    scroll: boolean,
    pointer = true,
  ) => {
    let point = await element.evaluate(checkDom, expected, scroll, pointer);

    if (point === false) return false;
    let current = frame;
    let parent = current.parentFrame();

    while (parent !== null) {
      const owner = await current.frameElement();

      if (owner === null) return false;
      try {
        point = await owner.evaluate(checkFrameDom, point, scroll);
      } finally {
        await owner.dispose();
      }
      if (point === false) return false;
      current = parent;
      parent = current.parentFrame();
    }

    return point;
  };

  const after = Effect.fnUntraced(function* (
    completed: number,
    error: string | null,
    dispatch: NonNullable<(typeof ActionResult.Type)["dispatch"]>,
    frame?: string,
  ) {
    const observation = yield* inspect(frame === undefined ? {} : { frame }, {
      kind: "observe",
    }).pipe(
      Effect.catch((failure) => {
        error ??= `Input receipt retained; observation failed. Inspect before continuing. ${failure.message}`;

        return Effect.succeed(null);
      }),
    );

    return {
      completed,
      error,
      observation,
      dispatch,
      ...(pendingInput === undefined
        ? {}
        : { pendingInput: { ref: pendingInput.ref, reason: "dialog" as const } }),
    };
  });

  const action = Effect.fnUntraced(function* (
    value: Action | ({ readonly kind: "press" } & typeof PressRequest.Type),
  ) {
    if (pendingInput !== undefined)
      return yield* invalid("Native input is pending; respond to its dialog without replaying it.");
    const target = targets.get(value.ref);

    if (target === undefined)
      return yield* invalid("Target was not observed. Inspect before acting.");
    if (value.kind === "select" && !target.control.options.includes(value.value))
      return yield* invalid("Select option was not observed. Inspect with optionFilter first.");
    yield* charge();
    const command: Command = { kind: "act", action: value };

    // Preparation has its own 2 s budget. It never waits for a missing selector;
    // an actually pending CDP request is terminated by BrowserSession, not abandoned.
    let dispatch: NonNullable<(typeof ActionResult.Type)["dispatch"]> = "not-dispatched";
    let refusal: string | undefined;

    const ready = yield* native(
      command,
      async (page, stage) => {
        const frame = resolveFrame(page, target.frame);

        if (frame === undefined) return false;

        stage("resolve-reference");

        const handle = await frame
          .isolatedRealm()
          .evaluateHandle(
            (ref) => Reflect.get(globalThis, "@effect-agent/native-browser")?.get(ref) ?? null,
            value.ref,
          );

        try {
          const element = handle.asElement();

          stage("validate-and-scroll");

          return (
            element !== null &&
            (await checkedPoint(element, frame, target.control, true, value.kind !== "press")) !==
              false
          );
        } finally {
          stage("release-reference");
          await handle.dispose();
        }
      },
      2_000,
      "prepare",
    ).pipe(
      Effect.mapError(
        (error) =>
          new BrowserUseError({
            code: error.code,
            message: error.message,
            dispatch: "not-dispatched",
          }),
      ),
    );

    if (!ready)
      return yield* invalid(
        "Observed target is stale, disabled or obstructed. No input dispatched. Inspect for a current target.",
      );

    const input = native(
      command,
      async (page, stage) => {
        inputPage = page;
        const frame = resolveFrame(page, target.frame);

        if (frame === undefined) return "not-dispatched" as const;

        const handle = await frame
          .isolatedRealm()
          .evaluateHandle(
            (ref) => Reflect.get(globalThis, "@effect-agent/native-browser")?.get(ref) ?? null,
            value.ref,
          );

        try {
          const element = handle.asElement();

          if (element === null || !(element instanceof ElementHandle))
            return "not-dispatched" as const;

          const point = await checkedPoint(
            element,
            frame,
            target.control,
            false,
            value.kind !== "press",
          );

          if (point === false) return "not-dispatched" as const;
          stage("dispatch-input");
          if (value.kind === "click") {
            dispatch = "unknown";
            await page.mouse.click(point.x, point.y);
          } else if (value.kind === "select") {
            const valid = await element.evaluate(
              (node, value) =>
                node instanceof HTMLSelectElement &&
                Array.from(node.options).some(
                  (option) =>
                    !option.disabled &&
                    !(
                      option.parentElement instanceof HTMLOptGroupElement &&
                      option.parentElement.disabled
                    ) &&
                    option.value === value,
                ),
              value.value,
            );

            if (!valid) return "not-dispatched" as const;
            dispatch = "unknown";
            await element.select(value.value);
          } else if (value.kind === "press") {
            await element.focus();

            const focused = await element.evaluate(
              (node) =>
                node.isConnected && Reflect.get(node.getRootNode(), "activeElement") === node,
            );

            if (!focused) return "not-dispatched" as const;
            dispatch = "unknown";
            await page.keyboard.press(value.key);
          } else {
            const fillable = await element.evaluate(
              (node) =>
                (node instanceof HTMLInputElement &&
                  !["password", "file", "checkbox", "radio", "hidden", "submit", "button"].includes(
                    node.type,
                  ) &&
                  !node.readOnly) ||
                (node instanceof HTMLTextAreaElement && !node.readOnly) ||
                (node instanceof HTMLElement &&
                  node.isContentEditable &&
                  node.getAttribute("aria-readonly") !== "true"),
            );

            if (!fillable) return "not-dispatched" as const;
            await element.focus();
            // Focus handlers can replace a field. Never transfer input to the new focus.
            if (
              !(await element.evaluate(
                (node) =>
                  node.isConnected &&
                  (node.getRootNode() instanceof Document ||
                    node.getRootNode() instanceof ShadowRoot) &&
                  Reflect.get(node.getRootNode(), "activeElement") === node,
              ))
            )
              return "not-dispatched" as const;

            const selected = await element.evaluate((node) => {
              let selected = false;

              if (node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement) {
                node.select();
                selected =
                  node.value.length === 0 ||
                  (node.selectionStart === 0 && node.selectionEnd === node.value.length) ||
                  globalThis.getSelection()?.toString() === node.value;
              } else {
                const selection = globalThis.getSelection();

                if (selection === null) return false;
                const range = document.createRange();

                range.selectNodeContents(node);
                selection.removeAllRanges();
                selection.addRange(range);
                selected =
                  selection.rangeCount === 1 &&
                  selection.getRangeAt(0).compareBoundaryPoints(Range.START_TO_START, range) ===
                    0 &&
                  selection.getRangeAt(0).compareBoundaryPoints(Range.END_TO_END, range) === 0;
              }

              return (
                selected &&
                node.isConnected &&
                Reflect.get(node.getRootNode(), "activeElement") === node
              );
            });

            if (!selected) {
              refusal =
                "Native selection could not prepare this field for replacement. No input dispatched.";

              return "not-dispatched" as const;
            }

            dispatch = "unknown";
            await page.keyboard.press("Backspace");
            await element.type(value.value);
          }

          dispatch = "acknowledged";

          return "acknowledged" as const;
        } finally {
          await handle.dispose();
        }
      },
      undefined,
      "input",
    ).pipe(
      Effect.mapError(
        (error) => new BrowserUseError({ code: error.code, message: error.message, dispatch }),
      ),
    );

    // The session lock and native deadline remain owned by a controller-scoped fiber.
    // A dialog suspends input without abandoning SDK work or acknowledging it early.
    const start = yield* Deferred.make<void>();
    const dialogReady = yield* Deferred.make<void>();
    const ref = `op${scopeId}-${nextInput++}`;

    const fiber = yield* Deferred.await(start).pipe(
      Effect.andThen(input),
      Effect.forkIn(ownerScope),
    );

    pendingInput = { ref, fiber };
    dialogSignal = dialogReady;
    yield* Deferred.succeed(start, undefined);

    const result = yield* Effect.raceFirst(
      Fiber.join(fiber),
      Deferred.await(dialogReady).pipe(Effect.as("unknown" as const)),
    ).pipe(
      Effect.onInterrupt(() => Fiber.interrupt(fiber)),
      Effect.result,
    );

    if (result._tag === "Failure") {
      clearInput();

      return yield* result.failure;
    }
    if (result.success !== "unknown") clearInput();
    if (result.success === "not-dispatched" && refusal !== undefined)
      return yield* invalid(refusal);

    return result.success;
  });

  const act = Effect.fnUntraced(function* (values: ReadonlyArray<Action>) {
    yield* Schema.decodeEffect(
      Schema.Array(Action).check(Schema.isMinLength(1), Schema.isMaxLength(8)),
    )(values).pipe(Effect.mapError(() => invalid("Expected 1–8 valid actions.")));
    let completed = 0;
    let error: string | null = null;
    let dispatch: NonNullable<(typeof ActionResult.Type)["dispatch"]> = "not-dispatched";
    let frame: string | undefined;

    for (const value of values) {
      frame = targets.get(value.ref)?.frame ?? frame;
      const outcome = yield* action(value).pipe(Effect.result);

      if (outcome._tag === "Failure") {
        error = outcome.failure.message;
        if (outcome.failure.dispatch === "acknowledged") completed++;
        if (outcome.failure.dispatch === "unknown") dispatch = "unknown";
        else if (completed > 0) dispatch = "acknowledged";
        break;
      }
      if (outcome.success === "not-dispatched") {
        error = "Observed target changed before input. Inspect before continuing.";
        break;
      }
      if (outcome.success === "unknown") {
        dispatch = "unknown";
        error =
          "Native input is awaiting a JavaScript dialog. Respond to it; do not replay this input.";
        break;
      }
      dispatch = "acknowledged";
      completed++;
    }

    yield* Effect.annotateCurrentSpan({
      "browser.completed": completed,
      "browser.dispatch": dispatch,
    });

    return yield* after(completed, error, dispatch, frame);
  });

  const navigate = Effect.fnUntraced(function* (request: typeof NavigateRequest.Type) {
    yield* Schema.decodeEffect(InteractiveBrowserTargetUrl)(request.url).pipe(
      Effect.mapError(() => invalid("Navigation requires a credential-free HTTP(S) URL.")),
    );
    yield* charge();
    targets.clear();

    const outcome = yield* native({ kind: "navigate", request }, (page) =>
      page.goto(request.url, { waitUntil: "domcontentloaded" }),
    ).pipe(Effect.result);

    return yield* after(
      outcome._tag === "Success" ? 1 : 0,
      outcome._tag === "Failure" ? outcome.failure.message : null,
      outcome._tag === "Success" ? "acknowledged" : (outcome.failure.dispatch ?? "unknown"),
    );
  });

  const scroll = Effect.fnUntraced(function* (request: typeof ScrollRequest.Type) {
    yield* Schema.decodeEffect(ScrollRequest)(request).pipe(
      Effect.mapError(() => invalid("Invalid scroll request.")),
    );
    yield* charge();
    const target = request.ref === undefined ? undefined : targets.get(request.ref);

    if (request.ref !== undefined && target === undefined)
      return yield* invalid("Scroll target was not observed.");

    const outcome = yield* native(
      { kind: "scroll", request },
      async (page) => {
        if (target === undefined) {
          await page.evaluate(
            (x, y) => scrollBy({ left: x, top: y, behavior: "instant" }),
            request.deltaX,
            request.deltaY,
          );

          return true;
        }

        const frame = resolveFrame(page, target.frame);

        if (frame === undefined) return false;

        const handle = await frame
          .isolatedRealm()
          .evaluateHandle(
            (ref) => Reflect.get(globalThis, "@effect-agent/native-browser")?.get(ref) ?? null,
            request.ref ?? "",
          );

        try {
          return await handle.evaluate(
            (node, x, y) => {
              if (!(node instanceof HTMLElement) || !node.isConnected) return false;
              node.scrollBy({ left: x, top: y, behavior: "instant" });

              return true;
            },
            request.deltaX,
            request.deltaY,
          );
        } finally {
          await handle.dispose();
        }
      },
      2_000,
    ).pipe(Effect.result);

    return yield* after(
      outcome._tag === "Success" && outcome.success ? 1 : 0,
      outcome._tag === "Failure"
        ? outcome.failure.message
        : outcome.success
          ? null
          : "Scroll target is stale.",
      outcome._tag === "Failure"
        ? (outcome.failure.dispatch ?? "unknown")
        : outcome.success
          ? "acknowledged"
          : "not-dispatched",
    );
  });

  const wait = Effect.fnUntraced(function* (request: typeof WaitRequest.Type) {
    yield* Schema.decodeEffect(WaitRequest)(request).pipe(
      Effect.mapError(() => invalid("Invalid condition wait.")),
    );
    const frameRef = request.frame;

    if (request.state === "text" && request.text === undefined)
      return yield* invalid("A text wait requires expected text.");

    if (frameRef !== undefined && !frames.has(frameRef))
      return yield* invalid("Frame was not observed.");
    yield* native({ kind: "wait", request }, async (page) => {
      const frame = frameRef === undefined ? page.mainFrame() : resolveFrame(page, frameRef);

      if (frame === undefined) throw new Error("Frame detached");

      const handle = await frame
        .isolatedRealm()
        .waitForFunction(
          waitDom,
          { timeout: request.timeoutMillis, polling: "raf" },
          request.selector,
          request.state,
          request.text,
        );

      await handle.dispose();
    });

    return yield* inspect(frameRef === undefined ? {} : { frame: frameRef });
  });

  const screenshot = native({ kind: "screenshot" }, async (page) => {
    return {
      mediaType: "image/png" as const,
      base64: await page.screenshot({ type: "png", encoding: "base64" }),
    };
  }).pipe(Effect.flatMap(bounded));

  const press = Effect.fnUntraced(function* (request: typeof PressRequest.Type) {
    yield* Schema.decodeEffect(PressRequest)(request).pipe(
      Effect.mapError(() => invalid("Invalid native key request.")),
    );
    const frame = targets.get(request.ref)?.frame;
    const result = yield* action({ kind: "press", ...request }).pipe(Effect.result);

    const dispatch =
      result._tag === "Success" ? result.success : (result.failure.dispatch ?? "not-dispatched");

    return yield* after(
      dispatch === "acknowledged" ? 1 : 0,
      result._tag === "Failure"
        ? result.failure.message
        : result.success === "not-dispatched"
          ? "Target changed before keyboard input."
          : null,
      dispatch,
      frame,
    );
  });

  const selectTab = Effect.fnUntraced(function* (request: typeof TabRequest.Type) {
    yield* Schema.decodeEffect(TabRequest)(request).pipe(
      Effect.mapError(() => invalid("Invalid tab request.")),
    );
    const tab = tabs.get(request.ref);

    if (tab === undefined)
      return yield* invalid("Tab was not observed. Inspect before selecting it.");
    yield* native({ kind: "select-tab", request }, async (page) => {
      if (tab.isClosed() || tab.browserContext() !== page.browserContext())
        throw new Error("Observed tab is unavailable");
      await tab.bringToFront();
      activePage = tab;
      targets.clear();
      frames.clear();
    });

    return yield* inspect({});
  });

  const respondDialog = Effect.fnUntraced(function* (request: typeof DialogRequest.Type) {
    yield* Schema.decodeEffect(DialogRequest)(request).pipe(
      Effect.mapError(() => invalid("Invalid dialog response.")),
    );
    yield* charge();
    const outstanding = pendingInput;

    if (outstanding !== undefined && inputPage !== undefined) {
      const page = inputPage;
      const pending = pendingDialogs.get(page);

      if (
        pending === undefined ||
        pending.ref !== request.ref ||
        (request.text !== undefined && pending.dialog.type() !== "prompt")
      )
        return yield* invalid("Observed dialog is no longer current.");
      if (outstanding.fiber.pollUnsafe()?._tag === "Failure") {
        clearInput();
        const settled = yield* Fiber.join(outstanding.fiber).pipe(Effect.result);

        return {
          ...(yield* after(
            0,
            "Pending native input failed. No dialog response dispatched.",
            "not-dispatched",
          )),
          settledInput: {
            ref: outstanding.ref,
            dispatch:
              settled._tag === "Success"
                ? settled.success
                : (settled.failure.dispatch ?? "unknown"),
          },
        };
      }

      const response =
        outstanding.fiber.pollUnsafe() === undefined
          ? // Suspended input owns the session lock. This is its only allowed interleaving;
            // host authority is checked immediately before responding on that exact connection.
            authorize({ kind: "respond-dialog", request }, { pageUrl: page.url() }).pipe(
              Effect.andThen(
                Effect.tryPromise({
                  try: () =>
                    request.accept ? pending.dialog.accept(request.text) : pending.dialog.dismiss(),
                  catch: () =>
                    new BrowserUseError({
                      code: "browser",
                      message: "Native dialog response failed. Do not replay it.",
                      dispatch: "unknown",
                    }),
                }),
              ),
            )
          : native({ kind: "respond-dialog", request }, async () => {
              if (request.accept) await pending.dialog.accept(request.text);
              else await pending.dialog.dismiss();
            });

      const result = yield* response.pipe(
        Effect.withSpan("BrowserUse.respond-dialog.input"),
        Effect.result,
      );

      if (result._tag === "Failure")
        return yield* after(0, result.failure.message, result.failure.dispatch ?? "not-dispatched");
      pendingDialogs.delete(page);
      const settled = yield* Fiber.join(outstanding.fiber).pipe(Effect.result);

      clearInput();

      return {
        ...(yield* after(
          1,
          settled._tag === "Failure" ? settled.failure.message : null,
          "acknowledged",
        )),
        settledInput: {
          ref: outstanding.ref,
          dispatch:
            settled._tag === "Success" ? settled.success : (settled.failure.dispatch ?? "unknown"),
        },
      };
    }

    const receipt: { dispatch: NonNullable<(typeof ActionResult.Type)["dispatch"]> } = {
      dispatch: "not-dispatched",
    };

    const result = yield* native({ kind: "respond-dialog", request }, async (page) => {
      const pending = pendingDialogs.get(page);

      if (
        pending === undefined ||
        pending.ref !== request.ref ||
        (request.text !== undefined && pending.dialog.type() !== "prompt")
      )
        return false;
      receipt.dispatch = "unknown";
      if (request.accept) await pending.dialog.accept(request.text);
      else await pending.dialog.dismiss();
      receipt.dispatch = "acknowledged";
      pendingDialogs.delete(page);

      return true;
    }).pipe(Effect.result);

    return yield* after(
      receipt.dispatch === "acknowledged" ? 1 : 0,
      result._tag === "Failure"
        ? result.failure.message
        : result.success
          ? null
          : "Observed dialog is no longer current.",
      receipt.dispatch,
    );
  });

  const actions = BrowserActions.of({
    latestObservation: Effect.sync(() => latestObservation),
    observe: lock.withPermit(inspect({}, { kind: "observe" })),
    act: (values) => lock.withPermit(act(values)).pipe(Effect.withSpan("BrowserUse.act")),
  });

  const control = BrowserControl.of({
    inspect: (request) => lock.withPermit(inspect(request)),
    navigate: (request) => lock.withPermit(navigate(request)),
    scroll: (request) => lock.withPermit(scroll(request)),
    wait: (request) => lock.withPermit(wait(request)),
    screenshot: lock.withPermit(screenshot),
    press: (request) => lock.withPermit(press(request)),
    selectTab: (request) => lock.withPermit(selectTab(request)),
    respondDialog: (request) =>
      lock.withPermit(respondDialog(request)).pipe(Effect.withSpan("BrowserUse.respond-dialog")),
  });

  return {
    actions,
    control,
    layer: Layer.merge(
      Layer.succeed(BrowserActions, actions),
      Layer.succeed(BrowserControl, control),
    ),
  };
});
