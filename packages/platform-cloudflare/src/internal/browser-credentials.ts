import { Cause, Context, Effect, Redacted, Schema } from "effect";
import type { Frame, Page } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import {
  BrowserCredentialAccess,
  type BrowserCredentialMaterial,
  CardCredential,
  type CredentialDispatch,
  type CredentialFieldRole,
  CredentialFillError,
  CredentialFillResult,
  CredentialOrigin,
  CredentialTarget,
  FillCredentialRequest,
  LoginCredential,
  type CredentialAccessRequest,
} from "../BrowserCredentials.ts";
import type { CredentialTargetGuard } from "../BrowserSession.ts";

/** Supplied only under the attachment lock, with the remaining command/session budget. */
export class BrowserSessionPage extends Context.Service<
  BrowserSessionPage,
  {
    readonly page: Page;
    readonly commandTimeoutMillis: number;
  }
>()("@effect-agent/platform-cloudflare/internal/BrowserSessionPage") {}

const TargetReply = Schema.Union([
  Schema.Struct({ action: Schema.String.check(Schema.isMaxLength(2048)) }),
  Schema.Struct({ reason: Schema.Literals(["stale-target", "unsupported"]) }),
]);

const WriteReply = Schema.Literals([
  "filled",
  "stale-target",
  "unsupported-before-write",
  "unsupported",
]);

// One invocation-local isolated-world handle. Actual nodes and fingerprints never leave the page.
// This program neither discovers controls nor classifies roles nor observes page content.
const prepareFields = `(() => {
  const doc = document;
  let fields = [];
  let form;
  let snapshots = [];
  const describe = el => {
    if (!el || !el.isConnected || el.ownerDocument !== doc || doc !== document) return null;
    const native = el instanceof HTMLTextAreaElement ||
      (el instanceof HTMLSelectElement && !el.multiple) ||
      (el instanceof HTMLInputElement && ['text','email','password','tel','number','month','search','url','date','time','week','datetime-local'].includes(el.type));
    if (!native || el.matches(':disabled') || el.readOnly || !el.form) return null;
    const values = [el.form.action, el.form.method, el.form.enctype, el.name, el.type, el.autocomplete ?? ''];
    if (values.some(value => value.length > 2048)) return null;
    return JSON.stringify(values);
  };
  const validate = () => fields.length > 0 && fields.every((el, index) =>
    el.form === form && describe(el) !== null && describe(el) === snapshots[index]);
  return {
    prepare(selectors) {
      try {
        fields = selectors.map(selector => {
          const matches = doc.querySelectorAll(selector);
          return matches.length === 1 ? matches[0] : null;
        });
      } catch { return {reason: 'stale-target'}; }
      if (fields.some(el => !el) || new Set(fields).size !== fields.length) return {reason: 'stale-target'};
      snapshots = fields.map(describe);
      form = fields[0].form;
      if (!form || snapshots.some(value => value === null) || fields.some(el => el.form !== form)) return {reason: 'unsupported'};
      return {action: form.action};
    },
    target() { return validate() ? {action: form.action} : {reason: 'stale-target'}; },
    fill(index, value, guard) {
      if (!validate()) return 'stale-target';
      const el = fields[index];
      if (guard) {
        try { if (!guard(el, index)) return 'stale-target'; }
        catch { return 'stale-target'; }
      }
      if (el instanceof HTMLSelectElement) {
        const options = [...el.options].filter(option => option.value === value);
        if (options.length !== 1 || options[0].matches(':disabled')) return 'unsupported-before-write';
      }
      let prototype = Object.getPrototypeOf(el);
      let setter;
      while (prototype && !setter) { setter = Object.getOwnPropertyDescriptor(prototype, 'value')?.set; prototype = Object.getPrototypeOf(prototype); }
      if (!setter) return 'unsupported-before-write';
      setter.call(el, value);
      if (el.value !== value) return 'unsupported';
      el.dispatchEvent(new Event('input', {bubbles: true}));
      el.dispatchEvent(new Event('change', {bubbles: true}));
      return 'filled';
    }
  };
})()`;

const secretFor = (material: BrowserCredentialMaterial, role: typeof CredentialFieldRole.Type) => {
  if (material._tag === "LoginCredential")
    return role === "username"
      ? material.username
      : role === "password"
        ? material.password
        : undefined;
  switch (role) {
    case "card-name":
      return material.name;
    case "card-number":
      return material.number;
    case "card-expiry":
      return material.expiry;
    case "card-expiry-month":
      return material.expiryMonth;
    case "card-expiry-year":
      return material.expiryYear;
    case "card-security-code":
      return material.securityCode;
    case "username":
    case "password":
      return undefined;
  }
};

/**
 * Fill explicit native fields on an existing page without submitting. The owning browser session
 * serializes operations and closes on timeout, interruption or uncertain dispatch; this function releases
 * only its local handles. Ordinary native observations remain available, including credential echoes.
 */
export const fillCredential = Effect.fnUntraced(function* (
  input: FillCredentialRequest,
  guard?: CredentialTargetGuard,
): Effect.fn.Return<
  CredentialFillResult,
  CredentialFillError,
  BrowserCredentialAccess | BrowserSessionPage
> {
  const { page, commandTimeoutMillis } = yield* BrowserSessionPage;
  let dispatch: typeof CredentialDispatch.Type = "not-dispatched";
  let filled = 0;

  const fail = (reason: CredentialFillError["reason"]) =>
    new CredentialFillError({ reason, dispatch, filled, cleanup: "not-requested" });

  const remote = <A>(run: (signal: AbortSignal) => Promise<A>) =>
    Effect.tryPromise({
      try: async (signal) => {
        try {
          return { ok: true as const, value: await run(signal) };
        } catch {
          return { ok: false as const };
        }
      },
      catch: () => fail("provider"),
    }).pipe(
      Effect.flatMap((result) =>
        result.ok ? Effect.succeed(result.value) : Effect.fail(fail("provider")),
      ),
    );

  const release = (handles: ReadonlyArray<{ dispose(): Promise<void> }>) =>
    Effect.promise(async () => {
      await Promise.all(handles.map((handle) => handle.dispose().catch(() => {})));
    }).pipe(Effect.interruptible, Effect.timeoutOption("1 second"), Effect.asVoid);

  const acquire = <A extends { dispose(): Promise<void> }>(run: () => Promise<ReadonlyArray<A>>) =>
    Effect.acquireRelease(
      remote(async (signal) => {
        const handles = await run();

        if (signal.aborted) {
          await Promise.all(handles.map((handle) => handle.dispose().catch(() => {})));
          throw fail("provider");
        }

        return handles;
      }).pipe(Effect.interruptible),
      release,
    );

  const origin = (value: string) =>
    Effect.try({
      try: () => {
        const url = new URL(value);

        if (url.username || url.password) throw fail("denied");

        return url.origin;
      },
      catch: () => fail("denied"),
    }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(CredentialOrigin)),
      Effect.mapError(() => fail("denied")),
    );

  return yield* Effect.gen(function* () {
    const request = yield* Schema.decodeEffect(FillCredentialRequest, {
      onExcessProperty: "error",
    })(input).pipe(Effect.mapError(() => fail("denied")));

    const roles = request.fields.map((field) => field.role);

    if (
      new Set(roles).size !== roles.length ||
      roles.some(
        (role) => (request.kind === "login") !== (role === "username" || role === "password"),
      )
    )
      return yield* fail("denied");
    const access = yield* BrowserCredentialAccess;
    let frame: Frame = page.mainFrame();

    for (const selector of request.frame ?? []) {
      const matches = yield* acquire(() => frame.$$(selector));
      const element = matches[0];

      if (matches.length !== 1 || element === undefined) return yield* fail("stale-target");
      const child = yield* remote(() => element.contentFrame());

      if (child === null) return yield* fail("unsupported");
      frame = child;
    }

    const handles = yield* acquire(async () => [
      await frame.isolatedRealm().evaluateHandle(prepareFields),
    ]);

    const guarded =
      guard === undefined ? undefined : (yield* acquire(async () => [await guard(frame)]))[0];

    const held = handles[0];

    if (held === undefined) return yield* fail("provider");

    const inspect = (raw: unknown) =>
      Schema.decodeUnknownEffect(TargetReply)(raw).pipe(
        Effect.mapError(() => fail("provider")),
        Effect.flatMap((reply) =>
          "reason" in reply ? Effect.fail(fail(reply.reason)) : Effect.succeed(reply.action),
        ),
      );

    const action = yield* remote(() =>
      held.evaluate(
        (state, selectors) => {
          if (typeof state !== "object" || state === null) return null;

          return Reflect.apply(Reflect.get(state, "prepare"), state, [selectors]);
        },
        request.fields.map((field) => field.selector),
      ),
    ).pipe(Effect.flatMap(inspect));

    const target = CredentialTarget.make({
      topOrigin: yield* origin(page.url()),
      frameOrigin: yield* origin(frame.url()),
      recipientOrigin: yield* origin(action),
    });

    const authorization: CredentialAccessRequest = {
      credential: request.credential,
      kind: request.kind,
      target,
      roles,
    };

    const authorize = Effect.suspend(() => access.authorize(authorization)).pipe(
      Effect.mapError((error) => fail(error.reason)),
    );

    const validate = Effect.gen(function* () {
      if (
        frame.detached ||
        (yield* origin(page.url())) !== target.topOrigin ||
        (yield* origin(frame.url())) !== target.frameOrigin
      )
        return yield* fail("stale-target");

      const current = yield* remote(() =>
        held.evaluate((state) => {
          if (typeof state !== "object" || state === null) return null;

          return Reflect.apply(Reflect.get(state, "target"), state, []);
        }),
      ).pipe(Effect.flatMap(inspect));

      if ((yield* origin(current)) !== target.recipientOrigin) return yield* fail("stale-target");
    });

    yield* authorize;
    yield* validate;

    const raw = yield* access
      .resolve(authorization)
      .pipe(Effect.mapError((error) => fail(error.reason)));

    const material = yield* Schema.decodeEffect(
      request.kind === "login" ? LoginCredential : CardCredential,
    )(raw).pipe(Effect.mapError(() => fail("resolver")));

    for (const role of roles)
      if (secretFor(material, role) === undefined) return yield* fail("missing-credential");
    for (const [index, role] of roles.entries()) {
      yield* authorize;
      yield* validate;
      const value = secretFor(material, role);

      if (value === undefined) return yield* fail("missing-credential");
      const previous = dispatch;

      const result = yield* remote((signal) => {
        if (signal.aborted) return Promise.reject(fail("provider"));
        dispatch = "possibly-dispatched";

        return held.evaluate(
          (state, field, secret, predicate) => {
            if (typeof state !== "object" || state === null) return "stale-target";

            return Reflect.apply(Reflect.get(state, "fill"), state, [field, secret, predicate]);
          },
          index,
          Redacted.value(value),
          guarded,
        );
      }).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(WriteReply)),
        Effect.mapError(() => fail("provider")),
      );

      if (result === "stale-target" || result === "unsupported-before-write") {
        dispatch = previous;

        return yield* fail(result === "stale-target" ? "stale-target" : "unsupported");
      }
      if (result === "unsupported") return yield* fail("unsupported");
      dispatch = "dispatched";
      filled++;
    }

    return CredentialFillResult.make({ dispatch: "dispatched", filled });
  }).pipe(
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: commandTimeoutMillis,
      orElse: () => Effect.fail(fail("timeout")),
    }),
    Effect.catchCause((cause) => {
      if (Cause.hasInterrupts(cause)) return Effect.interrupt;
      if (
        cause.reasons.some(
          (reason) =>
            reason._tag === "Die" ||
            (reason._tag === "Fail" && !Schema.is(CredentialFillError)(reason.error)),
        )
      )
        return Effect.fail(fail("provider"));

      const expected = cause.reasons.find(
        (reason) => reason._tag === "Fail" && Schema.is(CredentialFillError)(reason.error),
      );

      return Effect.fail(
        fail(
          expected?._tag === "Fail" && Schema.is(CredentialFillError)(expected.error)
            ? expected.error.reason
            : "provider",
        ),
      );
    }),
    Effect.withTracerEnabled(false),
  );
});
