/// <reference types="@cloudflare/workers-types" />
import {
  BrowserCredentialAccess,
  type BrowserCredentialMaterial,
  CardCredential,
  CredentialOffer,
  CredentialOfferMetadata,
  CredentialOrigin,
  type CredentialTarget,
  CredentialUseResult,
  InteractiveBrowserPolicy,
  ListCredentialOffers,
  LoginCredential,
  ProtectedBrowser,
  ProtectedBrowserClick,
  ProtectedBrowserError,
  ProtectedBrowserNavigate,
  ProtectedBrowserObservation,
  ProtectedBrowserControl,
  UseCredential,
  type CredentialFieldRole,
  type CredentialKind,
  type CredentialUseAuthorization,
  type ProtectedBrowserHandle,
  type ProtectedCleanup,
  type ProtectedObservationState,
} from "@effect-agent/sandbox";
import {
  Clock,
  Context,
  Effect,
  Layer,
  Option,
  Redacted,
  Schema,
  Semaphore,
  type Scope,
} from "effect";

export class ProtectedTargetChanged extends Error {}
export class ProtectedNeedsAttention extends Error {}
export class ProtectedUnsupported extends Error {}
/** Internal SDK boundary. Implementations must never send page/provider diagnostics to a logger. */
export interface ProtectedBrowserTransport {
  readonly context: () => Promise<unknown>;
  readonly discover: () => Promise<unknown>;
  readonly target: (ref: string) => Promise<unknown>;
  readonly navigate: (url: string) => Promise<void>;
  readonly click: (ref: string, signal: AbortSignal) => Promise<void>;
  readonly fill: (
    ref: string,
    role: typeof CredentialFieldRole.Type,
    value: Redacted.Redacted<string>,
    signal: AbortSignal,
    dispatch: () => void,
  ) => Promise<void>;
  /** Invalidates local references synchronously before bounded exact-session cleanup. */
  readonly invalidate: () => void;
  readonly close: Effect.Effect<typeof ProtectedCleanup.Type>;
}
/** Adapter-private injection seam for deterministic tests, not a model capability. */
export class BrowserRunProtectedTransport extends Context.Service<
  BrowserRunProtectedTransport,
  {
    readonly open: (
      policy: InteractiveBrowserPolicy,
    ) => Effect.Effect<ProtectedBrowserTransport, ProtectedBrowserError, Scope.Scope>;
  }
>()("@effect-agent/platform-cloudflare/BrowserRunProtectedTransport") {}

export const ProtectedPageContext = Schema.Struct({
  document: Schema.String.check(Schema.isUUID()),
  topOrigin: CredentialOrigin,
  frameOrigins: Schema.Array(CredentialOrigin).check(Schema.isMaxLength(16)),
});
export const ProtectedDiscovery = Schema.Struct({
  ...ProtectedPageContext.fields,
  text: Schema.String.check(Schema.isMaxLength(64 * 1024)),
  controls: Schema.Array(ProtectedBrowserControl).check(Schema.isMaxLength(64)),
  truncated: Schema.Boolean,
});
const sameTarget = (a: CredentialTarget, b: CredentialTarget) =>
  a.topOrigin === b.topOrigin &&
  a.frameOrigin === b.frameOrigin &&
  a.recipientOrigin === b.recipientOrigin &&
  a.document === b.document &&
  a.frame === b.frame &&
  a.form === b.form;
const kindFor = (role: string): typeof CredentialKind.Type | undefined =>
  role === "username" || role === "password"
    ? "login"
    : role.startsWith("card-")
      ? "card"
      : undefined;
const secretFor = (material: BrowserCredentialMaterial, role: typeof CredentialFieldRole.Type) => {
  if (material._tag === "LoginCredential") {
    return role === "username"
      ? material.username
      : role === "password"
        ? material.password
        : undefined;
  }
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
 * Fresh private passes only. Account administrators and Browser Rendering token holders are
 * trusted operators. No viewer, handoff, raw JavaScript, screenshot or plaintext-fill API exists.
 * Hosts explicitly authorize post-exposure observations for recipients they trust not to echo.
 */
export const browserRunProtectedLayer = () =>
  Layer.effect(ProtectedBrowser)(
    Effect.gen(function* () {
      const transport = yield* BrowserRunProtectedTransport;
      const open = Effect.fn("ProtectedBrowser.open")(function* (
        input: InteractiveBrowserPolicy,
      ): Effect.fn.Return<
        ProtectedBrowserHandle,
        ProtectedBrowserError,
        Scope.Scope | BrowserCredentialAccess
      > {
        const initialError = (reason: ProtectedBrowserError["reason"]) =>
          new ProtectedBrowserError({
            reason,
            dispatch: "not-dispatched",
            milestone: "none",
            observation: "closed",
            cleanup: "not-requested",
          });
        const decodedPolicy = yield* Schema.decodeUnknownEffect(InteractiveBrowserPolicy)(
          input,
        ).pipe(Effect.mapError(() => initialError("denied")));
        if (decodedPolicy.network._tag === "PublicWeb") return yield* initialError("unsupported");
        const policy = InteractiveBrowserPolicy.make({
          ...decodedPolicy,
          network:
            decodedPolicy.network._tag === "ExactHosts"
              ? { _tag: "ExactHosts", allowedHosts: [...decodedPolicy.network.allowedHosts] }
              : { _tag: "Unrestricted" },
        });
        // Capture the host access service for THIS execution, not the application singleton.
        const access = yield* BrowserCredentialAccess;
        const started = yield* Clock.currentTimeMillis;
        const driver = yield* transport.open(policy);
        const lock = yield* Semaphore.make(1);
        let observation: typeof ProtectedObservationState.Type = "before-exposure";
        let cleanup: typeof ProtectedCleanup.Type = "not-requested";
        let actions = 0;
        let dispatch: ProtectedBrowserError["dispatch"] = "not-dispatched";
        let milestone: ProtectedBrowserError["milestone"] = "none";
        const exposures: Array<CredentialTarget> = [];
        const offers = new Map<
          string,
          {
            caller: Redacted.Redacted<string>;
            key: Redacted.Redacted<string>;
            target: CredentialTarget;
            targetRef: string;
            kind: typeof CredentialKind.Type;
            expires: number;
          }
        >();
        const fail = (reason: ProtectedBrowserError["reason"]) =>
          new ProtectedBrowserError({ reason, dispatch, milestone, observation, cleanup });
        const close = yield* Effect.cached(
          Effect.uninterruptible(
            Effect.gen(function* () {
              observation = "closed";
              offers.clear();
              driver.invalidate();
              cleanup = yield* driver.close.pipe(
                Effect.interruptible,
                Effect.timeoutOrElse({
                  duration: "10 seconds",
                  orElse: () => Effect.succeed("unconfirmed" as const),
                }),
                Effect.catchCause(() => Effect.succeed("unconfirmed" as const)),
              );
              return cleanup;
            }),
          ),
        );
        yield* Effect.addFinalizer(() =>
          close.pipe(
            Effect.flatMap((result) =>
              result === "unconfirmed"
                ? Effect.logWarning("Protected browser exact-session closure unconfirmed")
                : Effect.void,
            ),
          ),
        );
        const remote = <A>(f: (signal: AbortSignal) => Promise<A>) =>
          Effect.tryPromise({
            // Attach the rejection handler immediately, before Effect's scheduler resumes. In workerd
            // a foreign rejected Promise can otherwise reach unhandled-rejection diagnostics first.
            try: async (signal) => {
              try {
                return { ok: true as const, value: await f(signal) };
              } catch (cause) {
                return {
                  ok: false as const,
                  reason:
                    cause instanceof ProtectedTargetChanged
                      ? ("stale-reference" as const)
                      : cause instanceof ProtectedNeedsAttention
                        ? ("needs-attention" as const)
                        : cause instanceof ProtectedUnsupported
                          ? ("unsupported" as const)
                          : ("provider" as const),
                };
              }
            },
            catch: () => fail("provider"),
          }).pipe(
            Effect.flatMap((result) =>
              result.ok ? Effect.succeed(result.value) : Effect.fail(fail(result.reason)),
            ),
          );
        const decode = <A>(schema: Schema.Codec<A>, value: unknown) =>
          Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => fail("provider")));
        const caller = access.caller.pipe(Effect.mapError((error) => fail(error.reason)));
        const pageContext = remote(() => driver.context()).pipe(
          Effect.flatMap((raw) => decode(ProtectedPageContext, raw)),
        );
        const permitObservation = Effect.gen(function* () {
          const context = yield* pageContext;
          if (exposures.length > 0) {
            observation = "protected";
            const decision = yield* access
              .observation({ ...context, caller: yield* caller, exposures: [...exposures] })
              .pipe(Effect.mapError((error) => fail(error.reason)));
            if (decision !== "trust-recipient-no-credential-echo")
              return yield* fail("observation-blocked");
            observation = "approved-after-exposure";
          }
          return context;
        });
        const target = (ref: string) =>
          remote(() => driver.target(ref)).pipe(
            Effect.flatMap((raw) => decode(ProtectedBrowserControl, raw)),
          );
        const bounded = <A>(result: A) => {
          const bytes = new TextEncoder().encode(JSON.stringify(result)).byteLength;
          return bytes <= policy.maxReturnedBytes
            ? Effect.succeed(result)
            : Effect.fail(fail("limit"));
        };
        const run = <A>(effect: Effect.Effect<A, ProtectedBrowserError>) =>
          lock
            .withPermitsIfAvailable(1)(
              Effect.gen(function* () {
                dispatch = "not-dispatched";
                milestone = "none";
                if (observation === "closed") return yield* fail("closed");
                if (++actions > policy.maxActions) return yield* fail("limit");
                const remaining =
                  policy.maxElapsedMillis - ((yield* Clock.currentTimeMillis) - started);
                if (remaining <= 0) {
                  yield* close;
                  return yield* fail("timeout");
                }
                return yield* effect.pipe(
                  Effect.timeoutOrElse({
                    duration: remaining,
                    orElse: () => Effect.fail(fail("timeout")),
                  }),
                  Effect.catchCause((cause) =>
                    Effect.gen(function* () {
                      // No foreign exception or defect is retained. Interrupted operations still finalize.
                      const error = cause.reasons.find(
                        (reason) =>
                          reason._tag === "Fail" && Schema.is(ProtectedBrowserError)(reason.error),
                      );
                      const reason =
                        error?._tag === "Fail" && Schema.is(ProtectedBrowserError)(error.error)
                          ? error.error.reason
                          : "provider";
                      const interrupt = cause.reasons.some((reason) => reason._tag === "Interrupt");
                      if (
                        dispatch !== "not-dispatched" ||
                        interrupt ||
                        reason === "provider" ||
                        reason === "timeout"
                      )
                        yield* close;
                      if (interrupt) return yield* Effect.interrupt;
                      return yield* fail(
                        dispatch === "possibly-dispatched" && reason === "provider"
                          ? "outcome-unknown"
                          : reason,
                      );
                    }),
                  ),
                  Effect.onInterrupt(() => close),
                  Effect.withTracerEnabled(false),
                );
              }),
            )
            .pipe(
              Effect.flatMap((result) =>
                Option.isSome(result)
                  ? Effect.succeed(result.value)
                  : Effect.fail(
                      new ProtectedBrowserError({
                        reason: "busy",
                        dispatch: "not-dispatched",
                        milestone: "none",
                        observation,
                        cleanup,
                      }),
                    ),
              ),
            );

        return {
          close,
          navigate: (request) =>
            run(
              Effect.gen(function* () {
                const decoded = yield* Schema.decodeUnknownEffect(ProtectedBrowserNavigate)(
                  request,
                ).pipe(Effect.mapError(() => fail("denied")));
                let url: URL;
                try {
                  url = new URL(decoded.url);
                } catch {
                  return yield* fail("denied");
                }
                if (
                  url.protocol !== "https:" ||
                  url.username ||
                  url.password ||
                  (policy.network._tag === "ExactHosts" &&
                    !policy.network.allowedHosts.includes(url.host))
                )
                  return yield* fail("denied");
                if (exposures.length > 0) yield* permitObservation;
                offers.clear();
                dispatch = "possibly-dispatched";
                yield* remote(() => driver.navigate(decoded.url));
                dispatch = "dispatched";
                yield* permitObservation;
              }),
            ),
          observe: run(
            Effect.gen(function* () {
              const before = yield* permitObservation;
              const raw = yield* remote(() => driver.discover());
              const result = yield* decode(ProtectedDiscovery, raw);
              const after = yield* permitObservation;
              if (before.document !== after.document || result.document !== after.document)
                return yield* fail("stale-reference");
              return yield* bounded(
                ProtectedBrowserObservation.make({
                  ...result,
                  observation: exposures.length > 0 ? "approved-after-exposure" : "before-exposure",
                }),
              );
            }),
          ),
          click: (request) =>
            run(
              Effect.gen(function* () {
                const decoded = yield* Schema.decodeUnknownEffect(ProtectedBrowserClick)(
                  request,
                ).pipe(Effect.mapError(() => fail("denied")));
                yield* permitObservation;
                const control = yield* target(decoded.ref);
                // Credential submission goes through useCredential, never a generic click.
                if (control.role !== "link" && control.role !== "button")
                  return yield* fail("unsupported");
                dispatch = "possibly-dispatched";
                yield* remote((signal) => driver.click(decoded.ref, signal));
                dispatch = "dispatched";
                yield* permitObservation;
              }),
            ),
          listCredentialOffers: (request) =>
            run(
              Effect.gen(function* () {
                const decoded = yield* Schema.decodeUnknownEffect(ListCredentialOffers)(
                  request,
                ).pipe(Effect.mapError(() => fail("denied")));
                yield* permitObservation;
                const control = yield* target(decoded.target);
                if (kindFor(control.role) !== decoded.kind) return yield* fail("unsupported");
                const principal = yield* caller;
                const candidates = yield* access
                  .list({ caller: principal, kind: decoded.kind, target: control.target })
                  .pipe(Effect.mapError((error) => fail(error.reason)));
                if (candidates.length > 16 || offers.size + candidates.length > 64)
                  return yield* fail("limit");
                if (!sameTarget(control.target, (yield* target(decoded.target)).target))
                  return yield* fail("stale-reference");
                const expires = (yield* Clock.currentTimeMillis) + 60_000;
                const result: Array<CredentialOffer> = [];
                for (const candidate of candidates) {
                  const metadata = yield* decode(CredentialOfferMetadata, candidate.metadata);
                  const ref = crypto.randomUUID();
                  offers.set(ref, {
                    caller: principal,
                    key: candidate.key,
                    target: control.target,
                    targetRef: decoded.target,
                    kind: decoded.kind,
                    expires,
                  });
                  result.push(CredentialOffer.make({ ref, kind: decoded.kind, metadata }));
                }
                return yield* bounded(result);
              }),
            ),
          useCredential: (request) =>
            run(
              Effect.gen(function* () {
                const decoded = yield* Schema.decodeUnknownEffect(UseCredential)(request).pipe(
                  Effect.mapError(() => fail("denied")),
                );
                const offer = offers.get(decoded.offer);
                if (offer === undefined || offer.expires <= (yield* Clock.currentTimeMillis))
                  return yield* fail("stale-reference");
                const principal = yield* caller;
                if (Redacted.value(principal) !== Redacted.value(offer.caller))
                  return yield* fail("denied");
                if (offer.kind === "card" && decoded.submit !== undefined)
                  return yield* fail("unsupported");
                if (
                  new Set(decoded.fields.map((field) => field.ref)).size !==
                    decoded.fields.length ||
                  new Set(decoded.fields.map((field) => field.role)).size !== decoded.fields.length
                )
                  return yield* fail("denied");
                const validate = Effect.gen(function* () {
                  if (!sameTarget(offer.target, (yield* target(offer.targetRef)).target))
                    return yield* fail("stale-reference");
                  for (const field of decoded.fields) {
                    const current = yield* target(field.ref);
                    if (
                      field.role !== current.role ||
                      kindFor(field.role) !== offer.kind ||
                      !sameTarget(offer.target, current.target)
                    )
                      return yield* fail("denied");
                  }
                  if (decoded.submit !== undefined) {
                    const submit = yield* target(decoded.submit);
                    if (submit.role !== "submit" || !sameTarget(offer.target, submit.target))
                      return yield* fail("denied");
                  }
                });
                yield* validate;
                const authorization: CredentialUseAuthorization = {
                  caller: principal,
                  key: offer.key,
                  kind: offer.kind,
                  target: offer.target,
                  roles: decoded.fields.map((field) => field.role),
                  submit: decoded.submit !== undefined,
                };
                const authorize = Effect.gen(function* () {
                  if (Redacted.value(yield* caller) !== Redacted.value(principal))
                    return yield* fail("denied");
                  yield* access
                    .authorize(authorization)
                    .pipe(Effect.mapError((error) => fail(error.reason)));
                });
                yield* authorize;
                yield* validate;
                const raw = yield* access
                  .resolve(authorization)
                  .pipe(Effect.mapError((error) => fail(error.reason)));
                const material = yield* Schema.decodeUnknownEffect(
                  offer.kind === "login" ? LoginCredential : CardCredential,
                )(raw).pipe(Effect.mapError(() => fail("resolver")));
                for (const field of decoded.fields)
                  if (secretFor(material, field.role) === undefined)
                    return yield* fail("missing-credential");
                offers.delete(decoded.offer);
                for (const field of decoded.fields) {
                  yield* validate;
                  yield* authorize;
                  const value = secretFor(material, field.role);
                  if (value === undefined) return yield* fail("missing-credential");
                  observation = "protected";
                  yield* remote((signal) =>
                    driver.fill(field.ref, field.role, value, signal, () => {
                      dispatch = "possibly-dispatched";
                      if (!exposures.some((target) => sameTarget(target, offer.target)))
                        exposures.push(offer.target);
                    }),
                  );
                  dispatch = "dispatched";
                  milestone = "partial-fill";
                }
                milestone = "filled";
                if (decoded.submit !== undefined) {
                  yield* validate;
                  yield* authorize;
                  const ref = decoded.submit;
                  dispatch = "possibly-dispatched";
                  yield* remote((signal) => driver.click(ref, signal)).pipe(
                    Effect.catch((error) => {
                      if (error.reason === "needs-attention") dispatch = "dispatched";
                      return Effect.fail(error);
                    }),
                  );
                  dispatch = "dispatched";
                  milestone = "submission-dispatched";
                }
                yield* permitObservation;
                return CredentialUseResult.make({
                  dispatch,
                  milestone,
                  observation,
                  cleanup,
                  authentication: "unverified",
                });
              }),
            ),
        };
      }, Effect.withTracerEnabled(false));
      return { open };
    }),
  );
