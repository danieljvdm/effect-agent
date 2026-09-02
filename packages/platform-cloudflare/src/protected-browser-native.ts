/// <reference types="@cloudflare/workers-types" />
import puppeteer, {
  type Browser,
  type Frame,
  type JSHandle,
  type Page,
} from "@cloudflare/puppeteer";
import {
  CredentialOrigin,
  CredentialTarget,
  ProtectedBrowserControl,
  ProtectedBrowserError,
  type InteractiveBrowserPolicy,
} from "@effect-agent/sandbox";
import { Clock, Context, Crypto, Effect, Layer, Redacted, Schema, type Scope } from "effect";

import { BrowserRunSessionLifecycle } from "./browser-session-lifecycle.ts";
import {
  BrowserRunProtectedTransport,
  ProtectedBrowserDispatch,
  ProtectedTransportError,
  ProtectedDiscovery,
  ProtectedPageContext,
  type ProtectedBrowserTransport,
} from "./protected-browser-policy.ts";

// The pinned SDK implements this method but strips its internal declaration from lib/types.d.ts.
// Keep the declaration narrow and refuse runtimes without it. No main-world fallback is safe.
declare module "@cloudflare/puppeteer" {
  interface Frame {
    isolatedRealm(): { evaluateHandle(source: string): Promise<JSHandle<unknown>> };
  }
}

// This fixed program runs in an isolated world. Pages cannot replace its native DOM methods.
// Handles retain actual nodes; a selector is never reconstructed from an opaque reference.
const maxAttributeLength = 2048;
const inspectFrame = `(() => {
  const doc = document;
  const forms = [];
  const elements = [...doc.querySelectorAll('input,select,button,a[href]')].slice(0, 65);
  const describe = (el) => {
    if (doc !== document || !el.isConnected || el.ownerDocument !== doc) return null;
    const form = el.form ?? null;
    let formIndex = forms.indexOf(form);
    if (formIndex < 0) { formIndex = forms.length; forms.push(form); }
    const action = form ? (el.hasAttribute('formaction') ? el.formAction : form.action || doc.URL) : doc.URL;
    const method = form ? (el.hasAttribute('formmethod') ? el.formMethod : form.method) : '';
    const enctype = form?.enctype ?? '';
    const name = el.name ?? '';
    const completion = el.getAttribute('autocomplete') ?? '';
    const inputType = el.type ?? '';
    // Reject before parsing, fingerprinting or CDP transfer. Truncation could hide a target change.
    if ([action, method, enctype, name, completion, inputType].some(value => value.length > ${maxAttributeLength})) return null;
    const type = inputType.toLowerCase();
    const autocomplete = completion.trim().toLowerCase().split(/\\s+/).at(-1);
    const cardRoles = { 'cc-name':'card-name', 'cc-number':'card-number', 'cc-exp':'card-expiry',
      'cc-exp-month':'card-expiry-month', 'cc-exp-year':'card-expiry-year', 'cc-csc':'card-security-code' };
    let role = 'unsupported';
    const nativeField = el instanceof HTMLInputElement || el instanceof HTMLSelectElement;
    if (nativeField && form && !['submit','button'].includes(type) && !el.disabled && !el.readOnly && el.getClientRects().length > 0) {
      if (el instanceof HTMLInputElement && type === 'password') role = 'password';
      else if (['text','email','tel','number','month',''].includes(type) || el instanceof HTMLSelectElement) {
        if (cardRoles[autocomplete]) role = cardRoles[autocomplete];
        else if (autocomplete === 'username' || autocomplete === 'email') role = 'username';
        else if (['text','email'].includes(type) && form && form.querySelector('input[type="password"]')) role = 'username';
      }
    } else if (el instanceof HTMLButtonElement || (el instanceof HTMLInputElement && ['submit','button'].includes(type))) {
      if (!el.disabled) role = type === 'submit' && form ? 'submit' : type === 'button' ? 'button' : 'unsupported';
    } else if (el instanceof HTMLAnchorElement) role = 'link';
    const fingerprint = JSON.stringify([role, action, method, enctype, name, completion, type]);
    return { role, formIndex, action, fingerprint,
      label: (el.labels?.[0]?.textContent ?? el.getAttribute('aria-label') ?? el.textContent ?? '').slice(0,200) };
  };
  const expose = ({role, formIndex, action, label}) => ({role, formIndex, action, label});
  const original = elements.map(describe);
  const validate = (index) => {
    const current = describe(elements[index]);
    if (!current || !original[index] || current.fingerprint !== original[index].fingerprint ||
      current.formIndex !== original[index].formIndex) return null;
    return expose(current);
  };
  return {
    doc, elements, original: original.map(current => current && expose(current)), validate,
    text: () => {
      if (doc !== document) return null;
      const clone = doc.body?.cloneNode(true);
      clone?.querySelectorAll('input,textarea,select,script,style,noscript,iframe,object,embed').forEach(el => el.remove());
      return (clone?.textContent ?? '').slice(0,65536);
    },
    fill: (index, role, value) => {
      const current = validate(index);
      if (!current || current.role !== role) return false;
      const el = elements[index];
      let prototype = Object.getPrototypeOf(el);
      let setter;
      while (prototype && !setter) { setter = Object.getOwnPropertyDescriptor(prototype,'value')?.set; prototype = Object.getPrototypeOf(prototype); }
      if (!setter) return false;
      // Validate and mutate in one isolated-world task, without focusing first and running
      // page handlers between validation and assignment. Native setters support controlled inputs.
      setter.call(el, value);
      if (el.value !== value) return 'unsupported';
      el.dispatchEvent(new Event('input', {bubbles:true}));
      el.dispatchEvent(new Event('change', {bubbles:true}));
      return true;
    },
    click: (index) => {
      const current = validate(index);
      if (!current || !['button','submit','link'].includes(current.role)) return false;
      if (current.role === 'submit' && elements[index].form && !elements[index].form.matches(':valid')) return 'needs-attention';
      elements[index].click();
      return true;
    }
  };
})()`;

const Description = Schema.Struct({
  role: ProtectedBrowserControl.fields.role,
  formIndex: Schema.Natural,
  action: Schema.String.check(Schema.isMaxLength(maxAttributeLength)),
  label: Schema.String.check(Schema.isMaxLength(200)),
});
const Descriptions = Schema.Array(Schema.NullOr(Description)).check(Schema.isMaxLength(65));
interface FrameState {
  readonly frame: Frame;
  readonly handle: JSHandle<unknown>;
  readonly ref: string;
  readonly document: string;
  readonly forms: Map<number, string>;
}
interface ControlState {
  readonly frame: FrameState;
  readonly index: number;
  readonly control: ProtectedBrowserControl;
  readonly expires: number;
}

/** One host-private acquired session. The SDK handles and exact-session cleanup have one owner. */
export class ProtectedNativeSession extends Context.Service<
  ProtectedNativeSession,
  {
    readonly browser: Browser;
    readonly page: Page;
    readonly close: Effect.Effect<"confirmed" | "unconfirmed">;
  }
>()("@effect-agent/platform-cloudflare/ProtectedNativeSession") {}

const transportError = (reason: ProtectedTransportError["reason"]) =>
  new ProtectedTransportError({ reason });
const decode = <A>(schema: Schema.Codec<A>, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => transportError("provider")));
// Attach rejection handling inside the SDK callback before workerd can report foreign diagnostics.
const remote = <A>(run: (signal: AbortSignal) => Promise<A>) =>
  Effect.tryPromise({
    try: async (signal) => {
      try {
        return { ok: true as const, value: await run(signal) };
      } catch {
        return { ok: false as const };
      }
    },
    catch: () => transportError("provider"),
  }).pipe(
    Effect.flatMap((result) =>
      result.ok ? Effect.succeed(result.value) : Effect.fail(transportError("provider")),
    ),
  );

/** Host-private scoped transport. Session capabilities are requirements; policy is per-pass data. */
export const makeProtectedNativeTransport = Effect.fn("ProtectedNativeTransport.make")(function* (
  policy: InteractiveBrowserPolicy,
): Effect.fn.Return<
  ProtectedBrowserTransport,
  never,
  ProtectedNativeSession | Crypto.Crypto | Scope.Scope
> {
  const session = yield* ProtectedNativeSession;
  const { browser, page } = session;
  const crypto = yield* Crypto.Crypto;
  const clock = yield* Clock.Clock;
  const uuid = crypto.randomUUIDv4.pipe(Effect.mapError(() => transportError("provider")));
  let closed = false;
  let violation = false;
  let documentRef: string | undefined;
  const frames = new Map<Frame, FrameState>();
  const controls = new Map<string, ControlState>();
  const origin = (value: string) =>
    Effect.try({
      try: () => {
        const url = new URL(value);
        if (url.username || url.password) throw transportError("stale-reference");
        if (!Schema.is(CredentialOrigin)(url.origin)) throw transportError("unsupported");
        if (policy.network._tag === "ExactHosts" && !policy.network.allowedHosts.includes(url.host))
          throw transportError("stale-reference");
        return url.origin;
      },
      catch: (cause) =>
        Schema.is(ProtectedTransportError)(cause) ? cause : transportError("provider"),
    });
  const check = Effect.suspend(() =>
    closed || violation || !browser.isConnected()
      ? Effect.fail(transportError("stale-reference"))
      : Effect.void,
  );
  const clear = () => {
    controls.clear();
    // Browser event callbacks only invalidate. The next Effect operation allocates the new identity.
    documentRef = undefined;
  };
  const onNavigated = () => {
    clear();
  };
  const onTarget = (target: { type(): string }) => {
    if (target.type() !== "page") return;
    // No popup or additional page may inherit an observation channel.
    violation = true;
    clear();
  };
  const invalidate = () => {
    closed = true;
    clear();
  };
  const context = Effect.gen(function* () {
    yield* check;
    const list = page.frames();
    if (list.length > 16) return yield* transportError("unsupported");
    const topOrigin = yield* origin(page.url());
    const frameOrigins = yield* Effect.forEach(list, (frame) => origin(frame.url()));
    if (documentRef === undefined) documentRef = yield* uuid;
    return yield* decode(ProtectedPageContext, { document: documentRef, topOrigin, frameOrigins });
  });
  const isCurrent = Effect.fn("ProtectedNativeTransport.isCurrent")(function* (state: FrameState) {
    if (state.frame.detached) return false;
    return yield* remote(() =>
      state.handle.evaluate((held) => {
        if (typeof held !== "object" || held === null) return false;
        return Reflect.get(held, "doc") === Reflect.get(globalThis, "document");
      }),
    );
  });
  const get = Effect.fn("ProtectedNativeTransport.get")(function* (ref: string) {
    yield* check;
    const state = controls.get(ref);
    if (
      !state ||
      state.expires <= (yield* clock.currentTimeMillis) ||
      !(yield* isCurrent(state.frame))
    )
      return yield* transportError("stale-reference");
    const current = yield* remote(() =>
      state.frame.handle.evaluate((held, index) => {
        if (typeof held !== "object" || held === null) return null;
        return Reflect.apply(Reflect.get(held, "validate"), held, [index]);
      }, state.index),
    );
    const description = yield* decode(Schema.NullOr(Description), current);
    if (description === null) return yield* transportError("stale-reference");
    const ctx = yield* context;
    if (
      ctx.topOrigin !== state.control.target.topOrigin ||
      (yield* origin(state.frame.frame.url())) !== state.control.target.frameOrigin ||
      (yield* origin(description.action)) !== state.control.target.recipientOrigin ||
      description.role !== state.control.role
    )
      return yield* transportError("stale-reference");
    return state;
  });
  const close = yield* Effect.cached(
    Effect.gen(function* () {
      invalidate();
      return yield* session.close;
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          page.off("framenavigated", onNavigated);
          page.off("framedetached", onNavigated);
          browser.off("targetcreated", onTarget);
          // Handles die with the exact session. Do not block remote termination on local disposal.
          frames.clear();
        }),
      ),
    ),
  );
  yield* Effect.acquireRelease(
    Effect.sync(() => {
      page.on("framenavigated", onNavigated);
      page.on("framedetached", onNavigated);
      browser.on("targetcreated", onTarget);
    }),
    () => close,
  );
  return {
    context,
    invalidate,
    close,
    navigate: Effect.fn("ProtectedNativeTransport.navigate")(function* (url) {
      yield* check;
      yield* origin(url);
      clear();
      yield* remote(() => page.goto(url, { waitUntil: "load" }));
      yield* context;
    }),
    target: (ref) => get(ref).pipe(Effect.map((state) => state.control)),
    discover: Effect.gen(function* () {
      const before = yield* context;
      controls.clear();
      for (const state of frames.values()) yield* remote(() => state.handle.dispose());
      frames.clear();
      const discovered: Array<ProtectedBrowserControl> = [];
      let text = "";
      let truncated = false;
      for (const frame of page.frames()) {
        if (typeof frame.isolatedRealm !== "function") return yield* transportError("unsupported");
        const handle = yield* remote(() => frame.isolatedRealm().evaluateHandle(inspectFrame));
        const state: FrameState = {
          frame,
          handle,
          ref: yield* uuid,
          document: yield* uuid,
          forms: new Map(),
        };
        frames.set(frame, state);
        const raw = yield* remote(() =>
          handle.evaluate((held) => {
            if (typeof held !== "object" || held === null) return null;
            return Reflect.get(held, "original");
          }),
        );
        const descriptions = yield* decode(Descriptions, raw);
        for (let index = 0; index < descriptions.length; index++) {
          const desc = descriptions[index];
          if (!desc) continue;
          if (discovered.length === 64) {
            truncated = true;
            break;
          }
          let form = state.forms.get(desc.formIndex);
          if (form === undefined) {
            form = yield* uuid;
            state.forms.set(desc.formIndex, form);
          }
          // Unsupported destinations/controls are not offered as credential targets.
          const recipient = yield* origin(desc.action).pipe(Effect.result);
          if (recipient._tag === "Failure") continue;
          const control = ProtectedBrowserControl.make({
            ref: yield* uuid,
            role: desc.role,
            label: desc.label,
            target: CredentialTarget.make({
              topOrigin: before.topOrigin,
              frameOrigin: yield* origin(frame.url()),
              recipientOrigin: recipient.success,
              document: state.document,
              frame: state.ref,
              form,
            }),
          });
          controls.set(control.ref, {
            frame: state,
            index,
            control,
            expires: (yield* clock.currentTimeMillis) + 60_000,
          });
          discovered.push(control);
        }
        const rawText = yield* remote(() =>
          handle.evaluate((held) => {
            if (typeof held !== "object" || held === null) return null;
            return Reflect.apply(Reflect.get(held, "text"), held, []);
          }),
        );
        text += yield* decode(Schema.String.check(Schema.isMaxLength(65536)), rawText);
        if (text.length > 65536) {
          text = text.slice(0, 65536);
          truncated = true;
        }
      }
      if ((yield* context).document !== before.document)
        return yield* transportError("stale-reference");
      return yield* decode(ProtectedDiscovery, {
        ...before,
        text,
        controls: discovered,
        truncated,
      });
    }),
    fill: Effect.fn("ProtectedNativeTransport.fill")(function* (ref, role, value) {
      const dispatch = yield* ProtectedBrowserDispatch;
      const state = yield* get(ref);
      // CDP dispatch may mutate before its reply is lost, so uncertainty starts here.
      yield* dispatch.mark;
      const filled = yield* remote(() =>
        state.frame.handle.evaluate(
          (held, index, expectedRole, secret) => {
            if (typeof held !== "object" || held === null) return false;
            return Reflect.apply(Reflect.get(held, "fill"), held, [index, expectedRole, secret]);
          },
          state.index,
          role,
          Redacted.value(value),
        ),
      );
      if (filled === "unsupported") return yield* transportError("unsupported");
      if (filled !== true) return yield* transportError("stale-reference");
    }),
    click: Effect.fn("ProtectedNativeTransport.click")(function* (ref) {
      const state = yield* get(ref);
      const clicked = yield* remote(() =>
        state.frame.handle.evaluate((held, index) => {
          if (typeof held !== "object" || held === null) return false;
          return Reflect.apply(Reflect.get(held, "click"), held, [index]);
        }, state.index),
      );
      if (clicked === "needs-attention") return yield* transportError("needs-attention");
      if (clicked !== true) return yield* transportError("stale-reference");
    }),
  };
}, Effect.withTracerEnabled(false));

/**
 * Acquires through BROWSER with recording=false explicitly on the wire. The trusted provider's
 * documented opt-in semantics are the recording guarantee; no recording-status API exists.
 * Account operators remain trusted, and must not attach external observers to private passes.
 */
export const browserRunProtectedBindingLayer = (options: {
  readonly browser: Pick<BrowserRun, "fetch">;
}) =>
  Layer.effect(BrowserRunProtectedTransport)(
    Effect.gen(function* () {
      const lifecycle = yield* BrowserRunSessionLifecycle;
      const crypto = yield* Crypto.Crypto;
      const open = Effect.fn("BrowserRunProtectedTransport.open")(function* (
        policy: InteractiveBrowserPolicy,
      ): Effect.fn.Return<ProtectedBrowserTransport, ProtectedBrowserError, Scope.Scope> {
        const failure = () =>
          new ProtectedBrowserError({
            reason: "provider",
            dispatch: "not-dispatched",
            milestone: "none",
            observation: "closed",
            cleanup: "unconfirmed",
          });
        let sessionId: Redacted.Redacted<string> | undefined;
        let browser: Browser | undefined;
        let driver: ProtectedBrowserTransport | undefined;
        let invalid = false;
        // SDK acquisition may finish after interruption. Its late-reply callback must await cleanup,
        // using this pass's clock rather than starting an Effect runtime with default services.
        const runCleanup = Effect.runPromiseWith(Context.make(Clock.Clock, yield* Clock.Clock));
        const terminate = Effect.gen(function* () {
          invalid = true;
          driver?.invalidate();
          if (sessionId === undefined) return "unconfirmed" as const;
          const cleanup = yield* lifecycle.close(sessionId).pipe(
            Effect.as("confirmed" as const),
            Effect.catchCause(() => Effect.succeed("unconfirmed" as const)),
            Effect.interruptible,
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () => Effect.succeed("unconfirmed" as const),
            }),
          );
          // Local disconnect is not remote-closure evidence. Do it even when confirmation fails.
          const connected = browser;
          if (connected !== undefined)
            yield* Effect.promise(() => connected.disconnect()).pipe(
              Effect.catchCause(() => Effect.void),
              Effect.interruptible,
              Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.void }),
            );
          return cleanup;
        });
        const close = yield* Effect.cached(terminate);
        yield* Effect.addFinalizer(() =>
          close.pipe(
            Effect.flatMap((state) =>
              state === "confirmed"
                ? Effect.void
                : Effect.logWarning("Protected browser exact-session closure unconfirmed"),
            ),
          ),
        );
        const acquired = yield* Effect.tryPromise({
          try: async (signal) => {
            let recordingDisabled = false;
            const binding = {
              fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
                const request = new Request(input, init);
                const url = new URL(request.url);
                if (url.pathname === "/v1/devtools/browser" && request.method === "POST") {
                  url.searchParams.set("recording", "false");
                  recordingDisabled = url.searchParams.get("recording") === "false";
                  return options.browser.fetch(new Request(url, request));
                }
                return options.browser.fetch(request);
              },
            };
            const acquired = await puppeteer.acquire(binding, {
              recording: false,
              keep_alive: Math.max(10_000, policy.maxElapsedMillis),
            });
            sessionId = Redacted.make(
              Schema.decodeUnknownSync(Schema.String.check(Schema.isUUID()))(acquired.sessionId),
            );
            if (!recordingDisabled || signal.aborted || invalid) {
              await runCleanup(terminate);
              throw transportError("stale-reference");
            }
            // Initial attachment only. No recovery/reconnection path exists.
            browser = await puppeteer.connect(options.browser, acquired.sessionId);
            if (signal.aborted || invalid) {
              await runCleanup(terminate);
              throw transportError("stale-reference");
            }
            const context = await browser.createBrowserContext();
            const page = await context.newPage();
            await page.setBypassServiceWorker(true);
            await page.setRequestInterception(true);
            page.on("request", (request) => {
              let allowed = policy.network._tag === "Unrestricted";
              try {
                const url = new URL(request.url());
                allowed ||=
                  policy.network._tag === "ExactHosts" &&
                  url.protocol === "https:" &&
                  !url.username &&
                  !url.password &&
                  policy.network.allowedHosts.includes(url.host);
              } catch {
                /* Refuse malformed destinations. */
              }
              void (
                allowed && !invalid ? request.continue() : request.abort("blockedbyclient")
              ).catch(() => {
                invalid = true;
                driver?.invalidate();
              });
            });
            if (signal.aborted || invalid) {
              await runCleanup(terminate);
              throw transportError("stale-reference");
            }
            return ProtectedNativeSession.of({ browser, page, close });
          },
          catch: failure,
        }).pipe(
          Effect.timeoutOrElse({
            duration: Math.min(policy.maxElapsedMillis, 30_000),
            orElse: () =>
              Effect.fail(new ProtectedBrowserError({ ...failure(), reason: "timeout" })),
          }),
          Effect.catch((error) =>
            close.pipe(
              Effect.flatMap((cleanup) =>
                Effect.fail(new ProtectedBrowserError({ ...error, cleanup })),
              ),
            ),
          ),
        );
        driver = yield* makeProtectedNativeTransport(policy).pipe(
          Effect.provideService(ProtectedNativeSession, acquired),
          Effect.provideService(Crypto.Crypto, crypto),
        );
        return driver;
      }, Effect.withTracerEnabled(false));
      return { open };
    }),
  );
