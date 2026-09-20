import {
  Cause,
  Clock,
  Context,
  Crypto,
  Effect,
  Layer,
  Redacted,
  Schema,
  Semaphore,
  type Scope,
} from "effect";
import { InteractiveBrowserPolicy } from "effect-agent/interactive-browser";
/// <reference types="@cloudflare/workers-types" />
import {
  BrowserCredentialAccess,
  type BrowserCredentialMaterial,
  CardCredential,
  CredentialOffer,
  CredentialOfferMetadata,
  CredentialOrigin,
  CredentialObservationDecision,
  type CredentialTarget,
  CredentialUseResult,
  ListCredentialOffers,
  LoginCredential,
  ProtectedBrowser,
  ProtectedBrowserClick,
  ProtectedBrowserCheckpoint,
  ProtectedBrowserError,
  ProtectedBrowserFill,
  ProtectedBrowserNavigate,
  ProtectedBrowserObservation,
  ProtectedBrowserControl,
  UseCredential,
  type CredentialFieldRole,
  type CredentialKind,
  type CredentialUseAuthorization,
  type ProtectedBrowserHandle,
  type ProtectedBrowserAction,
  type ProtectedCleanup,
  type ProtectedObservationState,
} from "effect-agent/protected-browser";

import {
  BrowserRunFailure,
  inheritBrowserReport,
  reportBrowserCause,
  reportedBrowserError,
} from "../internal/browser-failure.ts";

export class ProtectedTransportError extends Schema.TaggedError<ProtectedTransportError>()(
  "ProtectedTransportError",
  { reason: Schema.Literals(["stale-reference", "needs-attention", "unsupported", "provider"]) },
) {}

/** Per-write evidence authority, provided by the policy immediately around a transport fill. */
export class ProtectedBrowserDispatch extends Context.Service<
  ProtectedBrowserDispatch,
  { readonly mark: Effect.Effect<void> }
>()("@effect-agent/platform-cloudflare/ProtectedBrowserDispatch") {}

/** Decoded adapter boundary. SDK exceptions and page diagnostics never cross this port. */
export interface ProtectedBrowserTransport {
  /** Select observation/target origins; undefined restores all network-permitted HTTPS frames. */
  readonly restrictObservation: (
    origins: ReadonlyArray<typeof CredentialOrigin.Type> | undefined,
  ) => Effect.Effect<void, ProtectedTransportError>;
  readonly context: Effect.Effect<typeof ProtectedPageContext.Type, ProtectedTransportError>;
  readonly discover: Effect.Effect<typeof ProtectedDiscovery.Type, ProtectedTransportError>;
  readonly target: (ref: string) => Effect.Effect<ProtectedBrowserControl, ProtectedTransportError>;
  readonly navigate: (url: string) => Effect.Effect<void, ProtectedTransportError>;
  readonly click: (ref: string) => Effect.Effect<void, ProtectedTransportError>;
  readonly fill: (
    ref: string,
    role: typeof CredentialFieldRole.Type | "text" | "select",
    value: Redacted.Redacted<string>,
  ) => Effect.Effect<void, ProtectedTransportError, ProtectedBrowserDispatch>;
  /** Invalidates local references synchronously before bounded exact-session cleanup. */
  readonly invalidate: () => void;
  /** Forget all discovered controls on a controller transition. */
  readonly resetReferences: () => void;
  /** Host attachment release, available only for transferable provider sessions. */
  readonly detach?: Effect.Effect<void, ProtectedBrowserError>;
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

/** Shared protected policy implementation for ephemeral and host-owned passes. */
export const makeProtectedBrowserPolicy = Effect.fn("ProtectedBrowser.open")(function* (
  input: InteractiveBrowserPolicy,
  checkpoint?: ProtectedBrowserCheckpoint,
) {
  const initialError = (reason: ProtectedBrowserError["reason"]) =>
    new ProtectedBrowserError({
      reason,
      dispatch: "not-dispatched",
      milestone: "none",
      observation: "closed",
      cleanup: "not-requested",
    });

  const decodedPolicy = yield* Schema.decodeEffect(InteractiveBrowserPolicy, {
    onExcessProperty: "error",
  })(input).pipe(Effect.mapError(() => initialError("denied")));

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

  const restored =
    checkpoint === undefined
      ? undefined
      : yield* Schema.decodeEffect(ProtectedBrowserCheckpoint)(checkpoint).pipe(
          Effect.mapError(() => initialError("denied")),
        );

  const now = yield* Clock.currentTimeMillis;

  if (
    restored !== undefined &&
    (restored.startedAt > now ||
      restored.actions > policy.maxActions ||
      now - restored.startedAt >= policy.maxElapsedMillis)
  )
    return yield* initialError("timeout");
  const started = restored?.startedAt ?? now;
  const driver = yield* (yield* BrowserRunProtectedTransport).open(policy);
  const crypto = yield* Crypto.Crypto;
  let suspended = restored !== undefined;
  let detached = false;
  let needsObservation = restored !== undefined;
  let humanExposure = restored?.humanExposure ?? false;
  let humanOrigins = [...(restored?.humanOrigins ?? [])];
  const lock = yield* Semaphore.make(1);
  let observation: typeof ProtectedObservationState.Type = "before-exposure";
  let cleanup: typeof ProtectedCleanup.Type = "not-requested";
  let actions = restored?.actions ?? 0;
  let dispatch: ProtectedBrowserError["dispatch"] = restored?.dispatch ?? "not-dispatched";
  let milestone: ProtectedBrowserError["milestone"] = restored?.milestone ?? "none";
  const exposures: Array<CredentialTarget> = [...(restored?.exposures ?? [])];

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
            orElse: () =>
              Effect.fail(
                new BrowserRunFailure({ operation: "protected.close", reason: "timeout" }),
              ),
          }),
          Effect.catchCause((cause) =>
            reportBrowserCause("protected.close", cause).pipe(Effect.as("unconfirmed" as const)),
          ),
        );

        return cleanup;
      }),
    ),
  );

  yield* Effect.addFinalizer(() => (detached ? Effect.void : close));

  const publicFailure = <A, E extends { readonly reason: ProtectedBrowserError["reason"] }, R>(
    effect: Effect.Effect<A, E, R>,
  ) =>
    effect.pipe(
      Effect.catchCause((cause) =>
        Effect.failCause(
          Cause.map(cause, (error) => inheritBrowserReport(error, fail(error.reason))),
        ),
      ),
    );

  const decode = <A>(schema: Schema.Codec<A>, value: unknown) =>
    Schema.decodeUnknownEffect(schema)(value).pipe(
      Effect.catch((error) =>
        reportBrowserCause("protected.policy.decode", Cause.fail(error)).pipe(
          Effect.andThen(Effect.fail(reportedBrowserError(fail("provider")))),
        ),
      ),
    );

  const caller = access.caller.pipe(publicFailure);
  const pageContext = publicFailure(driver.context);

  const permitObservation = Effect.gen(function* () {
    const context = yield* pageContext;
    let origins: ReadonlyArray<typeof CredentialOrigin.Type> | undefined;

    if (exposures.length > 0 || humanExposure) {
      observation = "protected";
      const principal = yield* caller;

      const rawDecision = yield* access
        .observation({
          ...context,
          caller: principal,
          exposures: [...exposures],
          humanExposure,
          humanOrigins,
        })
        .pipe(publicFailure);

      if (Redacted.value(yield* caller) !== Redacted.value(principal)) return yield* fail("denied");

      const decision = yield* Schema.decodeEffect(CredentialObservationDecision)(rawDecision).pipe(
        Effect.mapError(() => fail("observation-blocked")),
      );

      if (decision === "deny") return yield* fail("observation-blocked");
      if (typeof decision !== "string") {
        origins = [...decision.origins];
        if (!origins.includes(context.topOrigin)) return yield* fail("observation-blocked");
      }
      observation = "approved-after-exposure";
    }
    yield* publicFailure(driver.restrictObservation(origins));

    return {
      ...context,
      frameOrigins: context.frameOrigins.filter(
        (origin) => origins === undefined || origins.includes(origin),
      ),
    };
  });

  const target = (ref: string) => publicFailure(driver.target(ref));

  const authorizeAction = Effect.fn("ProtectedBrowser.authorizeAction")(function* (
    action: ProtectedBrowserAction,
  ) {
    if (access.authorizeAction === undefined) {
      if (action._tag === "Submit") return yield* fail("unsupported");

      return;
    }
    const principal = yield* caller;

    yield* access
      .authorizeAction({ caller: principal, action, exposures: [...exposures] })
      .pipe(publicFailure);
    if (Redacted.value(yield* caller) !== Redacted.value(principal)) return yield* fail("denied");
    if (action._tag !== "Navigate") {
      const current = yield* target(action.ref);

      if (
        !sameTarget(current.target, action.target) ||
        current.role !== (action._tag === "Submit" ? "submit" : action.role) ||
        (action._tag === "Click" && action.role === "link" && current.url !== action.url)
      )
        return yield* fail("stale-reference");
    }
  }, Effect.withTracerEnabled(false));

  const bounded = <A>(result: A) => {
    const bytes = new TextEncoder().encode(JSON.stringify(result)).byteLength;

    return bytes <= policy.maxReturnedBytes ? Effect.succeed(result) : Effect.fail(fail("limit"));
  };

  const run = <A>(effect: Effect.Effect<A, ProtectedBrowserError>, observing = false) =>
    lock
      .withPermitsIfAvailable(1)(
        Effect.gen(function* () {
          if (suspended || detached) return yield* fail("busy");
          if (needsObservation && !observing) return yield* fail("stale-reference");
          dispatch = "not-dispatched";
          milestone = "none";
          if (observation === "closed") return yield* fail("closed");
          if (++actions > policy.maxActions) return yield* fail("limit");

          const remaining = policy.maxElapsedMillis - ((yield* Clock.currentTimeMillis) - started);

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
                // Expected rejections cannot hide a concurrent defect. Preserve safe diagnostics first.
                const unexpected = cause.reasons.filter(
                  (reason) =>
                    reason._tag === "Die" ||
                    (reason._tag === "Fail" && !Schema.is(ProtectedBrowserError)(reason.error)),
                );

                if (unexpected.length > 0)
                  yield* reportBrowserCause("protected.operation", Cause.fromReasons(unexpected));

                const error = cause.reasons.find(
                  (reason) =>
                    reason._tag === "Fail" && Schema.is(ProtectedBrowserError)(reason.error),
                );

                const reason =
                  unexpected.length === 0 &&
                  error?._tag === "Fail" &&
                  Schema.is(ProtectedBrowserError)(error.error)
                    ? error.error.reason
                    : "provider";

                const interrupt = cause.reasons.some((reason) => reason._tag === "Interrupt");

                if (
                  dispatch === "possibly-dispatched" ||
                  (dispatch === "dispatched" && reason !== "busy") ||
                  interrupt ||
                  reason === "provider" ||
                  reason === "timeout"
                )
                  yield* close;
                if (interrupt) return yield* Effect.interrupt;

                const outcome = fail(
                  dispatch === "possibly-dispatched" && reason === "provider"
                    ? "outcome-unknown"
                    : reason,
                );

                return yield* unexpected.length > 0
                  ? reportedBrowserError(outcome)
                  : inheritBrowserReport(error?._tag === "Fail" ? error.error : undefined, outcome);
              }),
            ),
            Effect.onInterrupt(() => close),
            Effect.withTracerEnabled(false),
          );
        }),
      )
      .pipe(
        Effect.flatMap(
          Effect.fromOption(
            () =>
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

  const handle: ProtectedBrowserHandle = {
    close: Effect.suspend(() => (detached ? Effect.succeed("not-requested" as const) : close)),
    navigate: (request) =>
      run(
        Effect.gen(function* () {
          const decoded = yield* Schema.decodeEffect(ProtectedBrowserNavigate)(request).pipe(
            Effect.mapError(() => fail("denied")),
          );

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
          if (exposures.length > 0 || humanExposure) yield* permitObservation;
          yield* authorizeAction({ _tag: "Navigate", url: decoded.url });
          offers.clear();
          dispatch = "possibly-dispatched";
          yield* publicFailure(driver.navigate(decoded.url));
          dispatch = "dispatched";
          yield* permitObservation;
        }),
      ),
    observe: run(
      Effect.gen(function* () {
        const before = yield* permitObservation;
        const result = yield* publicFailure(driver.discover);
        const after = yield* permitObservation;

        if (
          before.document !== after.document ||
          result.document !== after.document ||
          result.topOrigin !== after.topOrigin
        )
          return yield* fail("stale-reference");
        if (result.frameOrigins.some((origin) => !after.frameOrigins.includes(origin)))
          return yield* fail("observation-blocked");

        const observation = yield* bounded(
          ProtectedBrowserObservation.make({
            ...result,
            observation:
              exposures.length > 0 || humanExposure ? "approved-after-exposure" : "before-exposure",
          }),
        );

        needsObservation = false;

        return observation;
      }),
      true,
    ),
    click: (request) =>
      run(
        Effect.gen(function* () {
          const decoded = yield* Schema.decodeEffect(ProtectedBrowserClick)(request).pipe(
            Effect.mapError(() => fail("denied")),
          );

          yield* permitObservation;
          const control = yield* target(decoded.ref);

          if (
            control.role !== "link" &&
            control.role !== "button" &&
            control.role !== "radio" &&
            control.role !== "checkbox" &&
            control.role !== "submit"
          )
            return yield* fail("unsupported");
          let action: ProtectedBrowserAction;

          if (control.role === "link") {
            if (control.url === undefined) return yield* fail("unsupported");
            action = {
              _tag: "Click",
              ref: decoded.ref,
              target: control.target,
              role: "link",
              url: control.url,
            };
          } else if (control.role === "submit") {
            action = { _tag: "Submit", ref: decoded.ref, target: control.target };
          } else {
            action = {
              _tag: "Click",
              ref: decoded.ref,
              target: control.target,
              role: control.role,
            };
          }
          yield* authorizeAction(action);
          dispatch = "possibly-dispatched";
          yield* publicFailure(driver.click(decoded.ref)).pipe(
            Effect.catch((error) => {
              if (error.reason === "needs-attention") dispatch = "not-dispatched";

              return Effect.fail(error);
            }),
          );
          dispatch = "dispatched";
          if (control.role === "submit") milestone = "submission-dispatched";
          yield* permitObservation;
        }),
      ),
    fill: (request) =>
      run(
        Effect.gen(function* () {
          const decoded = yield* Schema.decodeEffect(ProtectedBrowserFill, {
            onExcessProperty: "error",
          })(request).pipe(Effect.mapError(() => fail("denied")));

          yield* permitObservation;
          const control = yield* target(decoded.ref);

          if (control.role !== "text" && control.role !== "select")
            return yield* fail("unsupported");
          yield* authorizeAction({
            _tag: "Fill",
            ref: decoded.ref,
            target: control.target,
            role: control.role,
          });
          yield* publicFailure(
            driver.fill(decoded.ref, control.role, Redacted.make(decoded.value)),
          ).pipe(
            Effect.provideService(ProtectedBrowserDispatch, {
              mark: Effect.sync(() => {
                dispatch = "possibly-dispatched";
              }),
            }),
          );
          dispatch = "dispatched";
          milestone = "filled";
          yield* permitObservation;
        }),
      ),
    listCredentialOffers: (request) =>
      run(
        Effect.gen(function* () {
          const decoded = yield* Schema.decodeEffect(ListCredentialOffers)(request).pipe(
            Effect.mapError(() => fail("denied")),
          );

          yield* permitObservation;
          const control = yield* target(decoded.target);

          if (kindFor(control.role) !== decoded.kind) return yield* fail("unsupported");
          const principal = yield* caller;

          const candidates = yield* access
            .list({ caller: principal, kind: decoded.kind, target: control.target })
            .pipe(publicFailure);

          const now = yield* Clock.currentTimeMillis;

          for (const [ref, offer] of offers) if (offer.expires <= now) offers.delete(ref);
          if (candidates.length > 16 || offers.size + candidates.length > 64)
            return yield* fail("limit");
          if (!sameTarget(control.target, (yield* target(decoded.target)).target))
            return yield* fail("stale-reference");
          const expires = now + 60_000;
          const result: Array<CredentialOffer> = [];
          const pending: typeof offers = new Map();

          for (const candidate of candidates) {
            const metadata = yield* decode(CredentialOfferMetadata, candidate.metadata);

            const ref = yield* crypto.randomUUIDv4.pipe(
              Effect.catch((error) =>
                reportBrowserCause("protected.identity", Cause.fail(error)).pipe(
                  Effect.andThen(Effect.fail(reportedBrowserError(fail("provider")))),
                ),
              ),
            );

            pending.set(ref, {
              caller: principal,
              key: candidate.key,
              target: control.target,
              targetRef: decoded.target,
              kind: decoded.kind,
              expires,
            });
            result.push(CredentialOffer.make({ ref, kind: decoded.kind, metadata }));
          }
          yield* bounded(result);
          for (const [ref, offer] of pending) offers.set(ref, offer);

          return result;
        }),
      ),
    useCredential: (request) =>
      run(
        Effect.gen(function* () {
          const decoded = yield* Schema.decodeEffect(UseCredential)(request).pipe(
            Effect.mapError(() => fail("denied")),
          );

          const offer = offers.get(decoded.offer);

          if (offer === undefined || offer.expires <= (yield* Clock.currentTimeMillis))
            return yield* fail("stale-reference");
          const principal = yield* caller;

          if (Redacted.value(principal) !== Redacted.value(offer.caller))
            return yield* fail("denied");
          if (
            new Set(decoded.fields.map((field) => field.ref)).size !== decoded.fields.length ||
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
            yield* access.authorize(authorization).pipe(publicFailure);
            if (Redacted.value(yield* caller) !== Redacted.value(principal))
              return yield* fail("denied");
          });

          yield* authorize;
          yield* validate;

          const raw = yield* access.resolve(authorization).pipe(publicFailure);

          const material = yield* Schema.decodeEffect(
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
            yield* publicFailure(driver.fill(field.ref, field.role, value)).pipe(
              Effect.provideService(ProtectedBrowserDispatch, {
                mark: Effect.sync(() => {
                  dispatch = "possibly-dispatched";
                  if (!exposures.some((target) => sameTarget(target, offer.target)))
                    exposures.push(offer.target);
                }),
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
            yield* publicFailure(driver.click(ref)).pipe(
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

  const locked = <A>(effect: Effect.Effect<A, ProtectedBrowserError>) =>
    lock
      .withPermitsIfAvailable(1)(effect)
      .pipe(Effect.flatMap(Effect.fromOption(() => fail("busy"))));

  return {
    handle,
    suspend: (human = false) =>
      locked(
        Effect.gen(function* () {
          if (detached || observation === "closed") return yield* fail("closed");
          if (dispatch === "possibly-dispatched") return yield* fail("outcome-unknown");
          if ((yield* Clock.currentTimeMillis) - started >= policy.maxElapsedMillis)
            return yield* fail("timeout");
          suspended = true;
          if (human) {
            const context = yield* pageContext;

            const origins = yield* Schema.decodeEffect(
              ProtectedBrowserCheckpoint.fields.humanOrigins,
            )([...new Set([...humanOrigins, context.topOrigin, ...context.frameOrigins])]).pipe(
              Effect.catch(() => fail("needs-attention")),
            );

            humanExposure = true;
            humanOrigins = [...origins];
            observation = "protected";
          }
          driver.resetReferences();
          offers.clear();

          return ProtectedBrowserCheckpoint.make({
            policy,
            startedAt: started,
            actions,
            exposures: [...exposures],
            humanExposure,
            humanOrigins,
            dispatch,
            milestone,
          });
        }),
      ),
    returnControl: locked(
      Effect.gen(function* () {
        if (detached || observation === "closed") return yield* fail("closed");
        if (!suspended) return yield* fail("denied");
        yield* permitObservation;
        offers.clear();
        driver.resetReferences();
        needsObservation = true;
        suspended = false;
      }),
    ),
    detach: locked(
      Effect.gen(function* () {
        if (!suspended || observation === "closed") return yield* fail("denied");
        if (driver.detach === undefined) return yield* fail("unsupported");
        yield* driver.detach.pipe(
          Effect.onError(() => close),
          Effect.onInterrupt(() => close),
        );
        detached = true;
        offers.clear();
        driver.invalidate();
      }),
    ),
  };
}, Effect.withTracerEnabled(false));

/** Fresh private passes; scoped release confirms exact-session termination. */
export const browserRunProtectedLayer = () =>
  Layer.effect(ProtectedBrowser)(
    Effect.gen(function* () {
      const transport = yield* BrowserRunProtectedTransport;
      const crypto = yield* Crypto.Crypto;

      return {
        open: (policy: InteractiveBrowserPolicy) =>
          makeProtectedBrowserPolicy(policy).pipe(
            Effect.map((session) => session.handle),
            Effect.provideService(BrowserRunProtectedTransport, transport),
            Effect.provideService(Crypto.Crypto, crypto),
          ),
      };
    }),
  );
