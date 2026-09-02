/// <reference types="@cloudflare/workers-types" />
import type { Browser, Frame, JSHandle, Page } from "@cloudflare/puppeteer";
import {
  CredentialOrigin,
  CredentialTarget,
  ProtectedBrowserControl,
  type InteractiveBrowserPolicy,
} from "@effect-agent/sandbox";
import { Clock, Context, Crypto, Effect, Redacted, Schema, type Scope } from "effect";

import { inspectFrame, maxAttributeLength } from "./inspect-frame.ts";
import {
  ProtectedBrowserDispatch,
  ProtectedTransportError,
  ProtectedDiscovery,
  ProtectedPageContext,
  type ProtectedBrowserTransport,
} from "./policy.ts";

// The pinned SDK implements this method but strips its internal declaration from lib/types.d.ts.
// Keep the declaration narrow and refuse runtimes without it. No main-world fallback is safe.
declare module "@cloudflare/puppeteer" {
  interface Frame {
    isolatedRealm(): { evaluateHandle(source: string): Promise<JSHandle<unknown>> };
  }
}

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
