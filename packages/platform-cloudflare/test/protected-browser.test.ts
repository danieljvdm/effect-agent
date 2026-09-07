import { browserRunProtectedBindingLayer } from "@effect-agent/platform-cloudflare/ProtectedBrowser";
import { InteractiveBrowserPolicy } from "@effect-agent/sandbox/InteractiveBrowser";
import {
  BrowserCredentialAccess,
  CardCredential,
  CredentialAccessError,
  CredentialOfferMetadata,
  CredentialObservationGrant,
  CredentialTarget,
  ListCredentialOffers,
  LoginCredential,
  ProtectedBrowser,
  ProtectedBrowserClick,
  ProtectedBrowserControl,
  ProtectedBrowserFill,
  ProtectedBrowserNavigate,
  ProtectedBrowserSession,
  UseCredential,
  type CredentialFieldRole,
} from "@effect-agent/sandbox/ProtectedBrowser";
import { BrowserCrypto } from "@effect/platform-browser";
import { expect, expectTypeOf, it } from "@effect/vitest";
import { type Crypto, Deferred, Effect, Fiber, Layer, Redacted, Schema, type Scope } from "effect";
import { TestClock } from "effect/testing";

import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";
import type {
  makeProtectedNativeTransport,
  ProtectedNativeSession,
} from "../src/protected-browser/native.ts";
import {
  BrowserRunProtectedTransport,
  browserRunProtectedLayer,
  ProtectedTransportError,
  ProtectedBrowserDispatch,
  type ProtectedBrowserTransport,
} from "../src/protected-browser/policy.ts";

const policy = InteractiveBrowserPolicy.make({
  network: { _tag: "ExactHosts", allowedHosts: ["shop.test", "pay.test"] },
  maxActions: 100,
  maxElapsedMillis: 120_000,
  maxReturnedBytes: 16384,
});

const password = "sentinel-password-never-record";
const cardNumber = "4111111111111111";

const material = LoginCredential.make({
  username: Redacted.make("sentinel-user"),
  password: Redacted.make(password),
});

const fixture = (kind: "login" | "card" = "login", actionAuthority = true) => {
  let principal = "alice";
  let granted = true;
  let observes = true;
  let stale = false;
  let resolved = 0;
  let closed = 0;
  let reads = 0;
  let opens = 0;
  let attention = false;
  let disposed = false;
  const filled: Array<string> = [];
  let cleanup: "confirmed" | "unconfirmed" = "confirmed";
  let listOverride: BrowserCredentialAccess["Service"]["list"] | undefined;
  let resolveOverride: BrowserCredentialAccess["Service"]["resolve"] | undefined;
  let fillOverride: ProtectedBrowserTransport["fill"] | undefined;
  let navigateOverride: ProtectedBrowserTransport["navigate"] | undefined;
  let clickOverride: ProtectedBrowserTransport["click"] | undefined;

  let authorizeActionOverride:
    | NonNullable<BrowserCredentialAccess["Service"]["authorizeAction"]>
    | undefined;

  let observationOverride: BrowserCredentialAccess["Service"]["observation"] | undefined;
  let selectedOrigins: ReadonlyArray<string> | undefined;

  const target = CredentialTarget.make({
    topOrigin: "https://shop.test",
    frameOrigin: kind === "card" ? "https://pay.test" : "https://shop.test",
    recipientOrigin: kind === "card" ? "https://pay.test" : "https://shop.test",
    document: crypto.randomUUID(),
    frame: crypto.randomUUID(),
    form: crypto.randomUUID(),
  });

  const roles: Array<typeof CredentialFieldRole.Type> =
    kind === "login"
      ? ["username", "password"]
      : ["card-name", "card-number", "card-expiry", "card-security-code"];

  const controls = [...roles, "submit" as const].map((role) =>
    ProtectedBrowserControl.make({ ref: crypto.randomUUID(), target, role, label: role }),
  );

  const getTarget = (ref: string) =>
    Effect.suspend(() => {
      const found = controls.find((control) => control.ref === ref);

      return stale ||
        disposed ||
        !found ||
        (selectedOrigins !== undefined && !selectedOrigins.includes(found.target.frameOrigin))
        ? Effect.fail(new ProtectedTransportError({ reason: "stale-reference" }))
        : Effect.succeed(found);
    });

  const context = {
    document: crypto.randomUUID(),
    topOrigin: target.topOrigin,
    frameOrigins: [target.frameOrigin],
  };

  const driver: ProtectedBrowserTransport = {
    restrictObservation: (origins) =>
      Effect.sync(() => {
        selectedOrigins = origins;
      }),
    context: Effect.succeed(context),
    discover: Effect.sync(() => {
      reads++;

      return {
        ...context,
        frameOrigins: context.frameOrigins.filter(
          (origin) => selectedOrigins === undefined || selectedOrigins.includes(origin),
        ),
        text: "account dashboard",
        controls: controls.filter(
          (control) =>
            selectedOrigins === undefined || selectedOrigins.includes(control.target.frameOrigin),
        ),
        truncated: false,
      };
    }),
    target: getTarget,
    navigate: (url) => Effect.suspend(() => navigateOverride?.(url) ?? Effect.void),
    click: (ref) =>
      Effect.suspend(() =>
        attention
          ? Effect.fail(new ProtectedTransportError({ reason: "needs-attention" }))
          : (clickOverride?.(ref) ?? Effect.void),
      ),
    fill: Effect.fn(function* (
      ref: string,
      role: Parameters<ProtectedBrowserTransport["fill"]>[1],
      value: Redacted.Redacted<string>,
    ) {
      yield* getTarget(ref);
      if (fillOverride) return yield* fillOverride(ref, role, value);
      yield* (yield* ProtectedBrowserDispatch).mark;
      filled.push(Redacted.value(value));
    }),
    invalidate: () => {
      disposed = true;
    },
    close: Effect.sync(() => {
      closed++;

      return cleanup;
    }),
  };

  const access = BrowserCredentialAccess.of({
    caller: Effect.sync(() => Redacted.make(principal)),
    list: (request) =>
      granted && Redacted.value(request.caller) === "alice"
        ? (listOverride?.(request) ??
          Effect.succeed([
            {
              key: Redacted.make("vault-private-key"),
              metadata: CredentialOfferMetadata.make({ label: "personal" }),
            },
          ]))
        : Effect.fail(new CredentialAccessError({ reason: "denied" })),
    authorize: (request) =>
      Effect.suspend(() =>
        granted &&
        Redacted.value(request.caller) === "alice" &&
        request.target.topOrigin === "https://shop.test" &&
        request.target.frameOrigin === target.frameOrigin &&
        request.target.recipientOrigin === target.recipientOrigin
          ? Effect.void
          : Effect.fail(new CredentialAccessError({ reason: "denied" })),
      ),
    resolve: (request) =>
      Effect.suspend(() => {
        resolved++;

        return resolveOverride
          ? resolveOverride(request)
          : Effect.succeed(
              kind === "login"
                ? material
                : CardCredential.make({
                    name: Redacted.make("Test Person"),
                    number: Redacted.make(cardNumber),
                    expiry: Redacted.make("12/30"),
                    expiryMonth: Redacted.make("12"),
                    expiryYear: Redacted.make("2030"),
                    securityCode: Redacted.make("123"),
                  }),
            );
      }),
    ...(actionAuthority
      ? {
          authorizeAction: (
            request: Parameters<
              NonNullable<BrowserCredentialAccess["Service"]["authorizeAction"]>
            >[0],
          ) =>
            authorizeActionOverride?.(request) ??
            (request.action._tag === "Submit"
              ? Effect.fail(new CredentialAccessError({ reason: "denied" }))
              : Effect.void),
        }
      : {}),
    observation: (request) =>
      observationOverride?.(request) ??
      Effect.sync(() => (observes ? "trust-recipient-no-credential-echo" : "deny")),
  });

  const layer = browserRunProtectedLayer().pipe(
    Layer.provide(
      Layer.succeed(BrowserRunProtectedTransport)({
        open: () =>
          Effect.sync(() => {
            opens++;

            return driver;
          }),
      }),
    ),
    Layer.provideMerge(Layer.succeed(BrowserCredentialAccess)(access)),
    Layer.provide(BrowserCrypto.layer),
  );

  const open = Effect.gen(function* () {
    return yield* (yield* ProtectedBrowser).open(policy);
  });

  const fields = controls
    .filter((control) => Schema.is(CredentialFieldRoleSchema)(control.role))
    .map((control) => ({
      ref: control.ref,
      role: Schema.decodeUnknownSync(CredentialFieldRoleSchema)(control.role),
    }));

  return {
    layer,
    open,
    controls,
    fields,
    kind,
    filled,
    stats: () => ({ resolved, closed, reads, opens }),
    setPrincipal: (value: string) => {
      principal = value;
    },
    revoke: () => {
      granted = false;
    },
    blockObservations: () => {
      observes = false;
    },
    expire: () => {
      stale = true;
    },
    setCleanup: (value: typeof cleanup) => {
      cleanup = value;
    },
    setResolve: (value: typeof resolveOverride) => {
      resolveOverride = value;
    },
    setList: (value: typeof listOverride) => {
      listOverride = value;
    },
    setFill: (value: typeof fillOverride) => {
      fillOverride = value;
    },
    setNavigate: (value: typeof navigateOverride) => {
      navigateOverride = value;
    },
    setClick: (value: typeof clickOverride) => {
      clickOverride = value;
    },
    setAuthorizeAction: (value: typeof authorizeActionOverride) => {
      authorizeActionOverride = value;
    },
    setObservation: (value: typeof observationOverride) => {
      observationOverride = value;
    },
    context,
    needAttention: () => {
      attention = true;
    },
  };
};

const CredentialFieldRoleSchema = Schema.Literals([
  "username",
  "password",
  "card-name",
  "card-number",
  "card-expiry",
  "card-security-code",
]);

const proposal = (
  f: ReturnType<typeof fixture>,
  handle: Effect.Success<ReturnType<ProtectedBrowser["Service"]["open"]>>,
) =>
  Effect.gen(function* () {
    const offers = yield* handle.listCredentialOffers(
      ListCredentialOffers.make({ kind: f.kind, target: f.controls[0]!.ref }),
    );

    return UseCredential.make({ offer: offers[0]!.ref, fields: f.fields });
  });

it("keeps session, crypto, scope, and dispatch authority in the requirement channel", () => {
  expectTypeOf<Effect.Services<ReturnType<typeof makeProtectedNativeTransport>>>().toEqualTypeOf<
    ProtectedNativeSession | Crypto.Crypto | Scope.Scope
  >();
  expectTypeOf<Layer.Services<ReturnType<typeof browserRunProtectedLayer>>>().toEqualTypeOf<
    BrowserRunProtectedTransport | Crypto.Crypto
  >();
  expectTypeOf<Layer.Services<ReturnType<typeof browserRunProtectedBindingLayer>>>().toEqualTypeOf<
    BrowserRunSessionLifecycle | Crypto.Crypto
  >();
  expectTypeOf<
    Effect.Services<ReturnType<ProtectedBrowserTransport["fill"]>>
  >().toEqualTypeOf<ProtectedBrowserDispatch>();
  expectTypeOf<
    Effect.Error<ReturnType<ProtectedBrowserTransport["target"]>>
  >().toEqualTypeOf<ProtectedTransportError>();
  expectTypeOf<
    Effect.Success<ReturnType<ProtectedBrowserTransport["target"]>>
  >().toEqualTypeOf<ProtectedBrowserControl>();
});

it.effect(
  "explicitly disables recording on acquisition and closes that exact session when attachment fails",
  () =>
    Effect.gen(function* () {
      const sessionId = crypto.randomUUID();
      const closed: Array<string> = [];
      const requests: Array<Request> = [];

      const browser = {
        fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
          const request = new Request(input, init);

          requests.push(request);

          return request.method === "POST"
            ? Response.json({ sessionId })
            : new Response("private-provider-diagnostic", { status: 503 });
        },
      };

      const error = yield* Effect.gen(function* () {
        return yield* (yield* BrowserRunProtectedTransport).open(
          InteractiveBrowserPolicy.make({ ...policy, maxElapsedMillis: 3_600_000 }),
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          browserRunProtectedBindingLayer({ browser }).pipe(
            Layer.provide(BrowserCrypto.layer),
            Layer.provide(
              Layer.succeed(BrowserRunSessionLifecycle)({
                close: (id) =>
                  Effect.sync(() => {
                    closed.push(Redacted.value(id));
                  }),
              }),
            ),
          ),
        ),
        Effect.flip,
      );

      expect(new URL(requests[0]!.url).searchParams.get("recording")).toBe("false");
      expect(new URL(requests[0]!.url).searchParams.get("keep_alive")).toBe("600000");
      expect(closed).toEqual([sessionId]);
      expect(error).toMatchObject({
        reason: "provider",
        dispatch: "not-dispatched",
        cleanup: "confirmed",
      });
      expect(JSON.stringify(error)).not.toContain("private-provider-diagnostic");
    }),
);

it.effect.each(["login", "card"] as const)(
  "fills %s privately and continues under a current host observation grant",
  (kind) => {
    const f = fixture(kind);

    return Effect.gen(function* () {
      const handle = yield* f.open;
      const request = yield* proposal(f, handle);
      const result = yield* handle.useCredential(request);
      const observed = yield* handle.observe;

      expect(result).toMatchObject({
        dispatch: "dispatched",
        milestone: "filled",
        observation: "approved-after-exposure",
        authentication: "unverified",
      });
      expect(f.filled).toContain(kind === "login" ? password : cardNumber);
      expect(JSON.stringify({ request, result, observed })).not.toContain(
        kind === "login" ? password : cardNumber,
      );
      f.blockObservations();
      const reads = f.stats().reads;

      expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({
        reason: "observation-blocked",
        observation: "protected",
      });
      expect(f.stats().reads).toBe(reads);
      expect(yield* handle.useCredential(request).pipe(Effect.flip)).toMatchObject({
        reason: "stale-reference",
        dispatch: "not-dispatched",
      });
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect.each([
  { action: "navigate", mode: "observation-denied" },
  { action: "click", mode: "observation-denied" },
  { action: "navigate", mode: "reply-lost" },
  { action: "click", mode: "reply-lost" },
] as const)("preserves $action dispatch and closes after $mode", ({ action, mode }) => {
  const f = fixture();

  const link = ProtectedBrowserControl.make({
    ...f.controls[0]!,
    ref: crypto.randomUUID(),
    role: "link",
  });

  f.controls.push(link);
  let dispatched = 0;

  const mutate = () =>
    Effect.gen(function* () {
      dispatched++;
      if (mode === "reply-lost") return yield* new ProtectedTransportError({ reason: "provider" });
      f.blockObservations();
    });

  if (action === "navigate") f.setNavigate(mutate);
  else f.setClick(mutate);

  return Effect.gen(function* () {
    const handle = yield* f.open;

    yield* handle.useCredential(yield* proposal(f, handle));

    const refused = yield* (
      action === "navigate"
        ? handle.navigate(ProtectedBrowserNavigate.make({ url: "https://evil.test" }))
        : handle.click(ProtectedBrowserClick.make({ ref: f.fields[0]!.ref }))
    ).pipe(Effect.flip);

    expect(refused).toMatchObject({
      reason: action === "navigate" ? "denied" : "unsupported",
      dispatch: "not-dispatched",
      cleanup: "not-requested",
    });
    expect(dispatched).toBe(0);

    const execute =
      action === "navigate"
        ? handle.navigate(ProtectedBrowserNavigate.make({ url: "https://shop.test/account" }))
        : handle.click(ProtectedBrowserClick.make({ ref: link.ref }));

    const error = yield* execute.pipe(Effect.flip);

    expect(error).toMatchObject({
      reason: mode === "reply-lost" ? "outcome-unknown" : "observation-blocked",
      dispatch: mode === "reply-lost" ? "possibly-dispatched" : "dispatched",
      milestone: "none",
      observation: "closed",
      cleanup: "confirmed",
    });
    expect(JSON.stringify(error)).not.toContain(password);
    expect(f.stats().closed).toBe(1);
    expect(yield* execute.pipe(Effect.flip)).toMatchObject({
      reason: "closed",
      dispatch: "not-dispatched",
    });
    expect(dispatched).toBe(1);
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect.each([
  "caller",
  "revoked",
  "stale",
  "wrong-role",
  "merchant",
  "port",
  "frame",
  "recipient",
  "expiry",
] as const)("refuses %s before resolving any material", (case_) => {
  const f = fixture();

  return Effect.gen(function* () {
    const handle = yield* f.open;
    let request = yield* proposal(f, handle);

    if (case_ === "caller") f.setPrincipal("mallory");
    if (case_ === "revoked") f.revoke();
    if (case_ === "stale") f.expire();
    if (case_ === "wrong-role")
      request = UseCredential.make({
        ...request,
        fields: [{ ref: f.fields[0]!.ref, role: "password" }],
      });
    if (["merchant", "port", "frame", "recipient"].includes(case_)) {
      const control = f.controls[0]!;

      f.controls[0] = ProtectedBrowserControl.make({
        ...control,
        target: CredentialTarget.make({
          ...control.target,
          ...(case_ === "merchant" ? { topOrigin: "https://evil.test" } : {}),
          ...(case_ === "port" ? { topOrigin: "https://shop.test:8443" } : {}),
          ...(case_ === "frame" ? { frameOrigin: "https://evil.test" } : {}),
          ...(case_ === "recipient" ? { recipientOrigin: "https://evil.test" } : {}),
        }),
      });
    }
    if (case_ === "expiry") yield* TestClock.adjust("61 seconds");
    const error = yield* handle.useCredential(request).pipe(Effect.flip);

    expect(error.dispatch).toBe("not-dispatched");
    expect(f.stats().resolved).toBe(0);
    expect(f.filled).toEqual([]);
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect("reclaims expired offers without discarding still-live offers at capacity", () => {
  const f = fixture();

  f.setList(() =>
    Effect.succeed(
      Array.from({ length: 16 }, () => ({
        key: Redacted.make("vault-private-key"),
        metadata: CredentialOfferMetadata.make({ label: "personal" }),
      })),
    ),
  );

  return Effect.gen(function* () {
    const handle = yield* f.open;

    const list = handle.listCredentialOffers(
      ListCredentialOffers.make({ kind: "login", target: f.controls[0]!.ref }),
    );

    const expired = yield* list;

    yield* TestClock.adjust("30 seconds");
    const live = yield* list;

    yield* list;
    yield* list;
    yield* TestClock.adjust("29999 millis");
    expect(yield* list.pipe(Effect.flip)).toMatchObject({ reason: "limit" });
    yield* TestClock.adjust("1 milli");
    const refreshed = yield* list;

    expect(refreshed).toHaveLength(16);
    expect(
      yield* handle
        .useCredential(UseCredential.make({ offer: expired[0]!.ref, fields: f.fields }))
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "stale-reference", dispatch: "not-dispatched" });
    for (const offer of [live[0]!, refreshed[0]!]) {
      expect(
        yield* handle.useCredential(UseCredential.make({ offer: offer.ref, fields: f.fields })),
      ).toMatchObject({ milestone: "filled" });
    }
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect("failed oversized lists do not consume credential-offer capacity", () => {
  const f = fixture();

  f.setList(() =>
    Effect.succeed(
      Array.from({ length: 16 }, () => ({
        key: Redacted.make("vault-private-key"),
        metadata: CredentialOfferMetadata.make({ label: "x".repeat(200) }),
      })),
    ),
  );

  return Effect.gen(function* () {
    const handle = yield* (yield* ProtectedBrowser).open(
      InteractiveBrowserPolicy.make({ ...policy, maxReturnedBytes: 1024 }),
    );

    const list = handle.listCredentialOffers(
      ListCredentialOffers.make({ kind: "login", target: f.controls[0]!.ref }),
    );

    for (let attempt = 0; attempt < 5; attempt++)
      expect(yield* list.pipe(Effect.flip)).toMatchObject({ reason: "limit" });
    f.setList(undefined);
    const offers = yield* list;

    expect(offers).toHaveLength(1);
    expect(
      yield* handle.useCredential(UseCredential.make({ offer: offers[0]!.ref, fields: f.fields })),
    ).toMatchObject({ milestone: "filled" });
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect.each(["caller", "grant"] as const)(
  "rechecks %s after vault resolution and before any mutation",
  (mode) => {
    const f = fixture();

    f.setResolve(() =>
      Effect.sync(() => {
        if (mode === "caller") f.setPrincipal("mallory");
        else f.revoke();

        return material;
      }),
    );

    return Effect.gen(function* () {
      const handle = yield* f.open;
      const error = yield* handle.useCredential(yield* proposal(f, handle)).pipe(Effect.flip);

      expect(error).toMatchObject({
        reason: "denied",
        dispatch: "not-dispatched",
        milestone: "none",
      });
      expect(f.stats().resolved).toBe(1);
      expect(f.filled).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect("reports filled but not submitted when native requirements need attention", () => {
  const f = fixture();

  f.needAttention();

  return Effect.gen(function* () {
    const handle = yield* f.open;
    const request = yield* proposal(f, handle);

    const error = yield* handle
      .useCredential(UseCredential.make({ ...request, submit: f.controls.at(-1)!.ref }))
      .pipe(Effect.flip);

    expect(error).toMatchObject({
      reason: "needs-attention",
      dispatch: "dispatched",
      milestone: "filled",
      observation: "closed",
      cleanup: "confirmed",
    });
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect(
  "denies metadata listing and card submission after grant revocation without resolving credentials",
  () => {
    const f = fixture("card");

    return Effect.gen(function* () {
      const handle = yield* f.open;
      const request = yield* proposal(f, handle);

      f.revoke();

      expect(
        yield* handle
          .useCredential(UseCredential.make({ ...request, submit: f.controls.at(-1)!.ref }))
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "denied", dispatch: "not-dispatched" });
      expect(
        yield* handle
          .listCredentialOffers(
            ListCredentialOffers.make({ kind: "card", target: f.controls[0]!.ref }),
          )
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "denied" });
      expect(f.stats().resolved).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect.each(["failure", "defect", "partial", "cleanup"] as const)(
  "sanitizes %s and preserves independent dispatch/cleanup evidence",
  (mode) => {
    const f = fixture();

    if (mode === "failure")
      f.setResolve(() => Effect.fail(new CredentialAccessError({ reason: "missing-credential" })));
    if (mode === "defect") f.setResolve(() => Effect.die(new Error(password)));
    if (mode === "partial" || mode === "cleanup") {
      let calls = 0;

      f.setFill(() =>
        Effect.gen(function* () {
          yield* (yield* ProtectedBrowserDispatch).mark;
          if (++calls === 2) return yield* Effect.die(new Error(password));
        }),
      );
      if (mode === "cleanup") f.setCleanup("unconfirmed");
    }

    return Effect.gen(function* () {
      const handle = yield* f.open;
      const error = yield* handle.useCredential(yield* proposal(f, handle)).pipe(Effect.flip);

      expect(JSON.stringify(error)).not.toContain(password);
      expect(error).toMatchObject(
        mode === "partial" || mode === "cleanup"
          ? {
              reason: "outcome-unknown",
              dispatch: "possibly-dispatched",
              milestone: "partial-fill",
              observation: "closed",
              cleanup: mode === "cleanup" ? "unconfirmed" : "confirmed",
            }
          : { dispatch: "not-dispatched", milestone: "none" },
      );
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect.each(["interrupt", "timeout"] as const)(
  "locks competing reads and finalizes on %s",
  (mode) => {
    const f = fixture();

    return Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();

      f.setResolve(() => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)));
      const handle = yield* f.open;
      const request = yield* proposal(f, handle);
      const fiber = yield* Effect.forkChild(handle.useCredential(request));

      yield* Deferred.await(entered);
      expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({
        reason: "busy",
        dispatch: "not-dispatched",
      });
      if (mode === "interrupt") yield* Fiber.interrupt(fiber);
      else {
        yield* TestClock.adjust("121 seconds");
        expect(yield* Fiber.join(fiber).pipe(Effect.flip)).toMatchObject({
          reason: "timeout",
          cleanup: "confirmed",
        });
      }
      expect(f.stats().closed).toBeGreaterThan(0);
      expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({ reason: "closed" });
      expect(f.filled).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect.each(["interrupt", "timeout"] as const)(
  "closes without replay after dispatch on %s",
  (mode) => {
    const f = fixture();

    return Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      let writes = 0;

      f.setFill(() =>
        Effect.gen(function* () {
          yield* (yield* ProtectedBrowserDispatch).mark;
          writes++;
          yield* Deferred.succeed(entered, undefined);

          return yield* Effect.never;
        }),
      );
      const handle = yield* f.open;
      const request = yield* proposal(f, handle);
      const fiber = yield* Effect.forkChild(handle.useCredential(request));

      yield* Deferred.await(entered);
      expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({ reason: "busy" });
      if (mode === "interrupt") yield* Fiber.interrupt(fiber);
      else {
        yield* TestClock.adjust("121 seconds");
        expect(yield* Fiber.join(fiber).pipe(Effect.flip)).toMatchObject({
          reason: "timeout",
          dispatch: "possibly-dispatched",
          milestone: "none",
          observation: "closed",
          cleanup: "confirmed",
        });
      }
      expect(yield* handle.useCredential(request).pipe(Effect.flip)).toMatchObject({
        reason: "closed",
        dispatch: "not-dispatched",
        milestone: "none",
      });
      expect(writes).toBe(1);
      expect(f.stats().closed).toBe(1);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect("one scoped session is shared by successive Tools and invalidated at Scope exit", () => {
  const f = fixture();

  return Effect.gen(function* () {
    const handle = yield* Effect.gen(function* () {
      const session = yield* ProtectedBrowserSession;
      const first = yield* session.get;

      expect(yield* session.get).toBe(first);
      yield* first.observe;

      return first;
    }).pipe(Effect.provide(ProtectedBrowserSession.layer(policy)), Effect.scoped);

    expect(f.stats().opens).toBe(1);
    expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({ reason: "closed" });
  }).pipe(Effect.provide(f.layer));
});

it.effect.each(["text", "select"] as const)(
  "fills ordinary %s without vault resolution",
  (role) => {
    const f = fixture();

    const control = ProtectedBrowserControl.make({
      ...f.controls[0]!,
      ref: crypto.randomUUID(),
      role,
    });

    f.controls.push(control);

    return Effect.gen(function* () {
      const handle = yield* f.open;

      yield* handle.fill(
        ProtectedBrowserFill.make({ ref: control.ref, value: "123 Example Street" }),
      );
      yield* handle.fill(ProtectedBrowserFill.make({ ref: control.ref, value: "" }));
      expect(f.filled).toEqual(["123 Example Street", ""]);
      expect(f.stats().resolved).toBe(0);
      expect((yield* handle.observe).observation).toBe("before-exposure");
      f.expire();
      expect(
        yield* handle
          .fill(ProtectedBrowserFill.make({ ref: control.ref, value: "stale" }))
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "stale-reference", dispatch: "not-dispatched" });
      expect(f.filled).toHaveLength(2);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect.each(["login", "card"] as const)(
  "refuses generic fill and submit for %s credentials",
  (kind) => {
    const f = fixture(kind);

    return Effect.gen(function* () {
      const handle = yield* f.open;

      for (const field of f.controls) {
        expect(
          yield* handle
            .fill(ProtectedBrowserFill.make({ ref: field.ref, value: "ordinary" }))
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "unsupported", dispatch: "not-dispatched" });
      }
      expect(
        yield* handle
          .click(ProtectedBrowserClick.make({ ref: f.controls.at(-1)!.ref }))
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "denied", dispatch: "not-dispatched" });
      expect(f.stats().resolved).toBe(0);
      expect(f.filled).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect.each(["radio", "checkbox"] as const)(
  "clicks an observed %s and reports checked state",
  (role) => {
    const f = fixture();

    const control = ProtectedBrowserControl.make({
      ...f.controls[0]!,
      ref: crypto.randomUUID(),
      role,
      checked: false,
    });

    f.controls.push(control);
    f.setClick((ref) =>
      Effect.sync(() => {
        expect(ref).toBe(control.ref);
        f.controls[f.controls.length - 1] = ProtectedBrowserControl.make({
          ...control,
          checked: true,
        });
      }),
    );

    return Effect.gen(function* () {
      const handle = yield* f.open;

      expect((yield* handle.observe).controls.at(-1)!.checked).toBe(false);
      yield* handle.click(ProtectedBrowserClick.make({ ref: control.ref }));
      expect((yield* handle.observe).controls.at(-1)!.checked).toBe(true);
      expect(f.stats().resolved).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect("discloses billing metadata only through an authorized bounded offer", () => {
  const f = fixture("card");

  const metadata = CredentialOfferMetadata.make({
    label: "Card",
    billingAddress: { line1: "123 Example Street", country: "US" },
  });

  f.setList(() => Effect.succeed([{ key: Redacted.make("private-key"), metadata }]));

  return Effect.gen(function* () {
    const handle = yield* f.open;
    const request = ListCredentialOffers.make({ kind: "card", target: f.controls[0]!.ref });
    const offers = yield* handle.listCredentialOffers(request);

    expect(offers[0]!.metadata).toEqual(metadata);
    expect(JSON.stringify(offers)).not.toContain("private-key");
    expect(f.stats().resolved).toBe(0);
    f.revoke();
    expect(yield* handle.listCredentialOffers(request).pipe(Effect.flip)).toMatchObject({
      reason: "denied",
    });
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect.each(["before", "after"] as const)(
  "requires the post-exposure grant %s ordinary fill",
  (when) => {
    const f = fixture();

    const control = ProtectedBrowserControl.make({
      ...f.controls[0]!,
      ref: crypto.randomUUID(),
      role: "text",
    });

    f.controls.push(control);

    return Effect.gen(function* () {
      const handle = yield* f.open;

      yield* handle.useCredential(yield* proposal(f, handle));
      let writes = 0;

      f.setFill(() =>
        Effect.gen(function* () {
          yield* (yield* ProtectedBrowserDispatch).mark;
          writes++;
          f.blockObservations();
        }),
      );
      if (when === "before") f.blockObservations();
      expect(
        yield* handle
          .fill(ProtectedBrowserFill.make({ ref: control.ref, value: "address" }))
          .pipe(Effect.flip),
      ).toMatchObject({
        reason: "observation-blocked",
        dispatch: when === "before" ? "not-dispatched" : "dispatched",
      });
      expect(writes).toBe(when === "before" ? 0 : 1);
      expect(f.stats().closed).toBe(when === "before" ? 0 : 1);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect.each(["failure", "defect", "timeout", "interrupt"] as const)(
  "sanitizes ordinary fill %s and closes once without replay",
  (mode) => {
    const f = fixture();

    const control = ProtectedBrowserControl.make({
      ...f.controls[0]!,
      ref: crypto.randomUUID(),
      role: "text",
    });

    f.controls.push(control);

    return Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      let writes = 0;

      f.setFill(() =>
        Effect.gen(function* () {
          yield* (yield* ProtectedBrowserDispatch).mark;
          writes++;
          yield* Deferred.succeed(entered, undefined);
          if (mode === "failure") return yield* new ProtectedTransportError({ reason: "provider" });
          if (mode === "defect") return yield* Effect.die(new Error("private-fill-diagnostic"));

          return yield* Effect.never;
        }),
      );
      const handle = yield* f.open;

      const request = ProtectedBrowserFill.make({
        ref: control.ref,
        value: "private-fill-diagnostic",
      });

      const fiber = yield* Effect.forkChild(handle.fill(request));

      yield* Deferred.await(entered);
      if (mode === "interrupt") yield* Fiber.interrupt(fiber);
      else {
        if (mode === "timeout") {
          expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({ reason: "busy" });
          yield* TestClock.adjust("121 seconds");
        }
        const error = yield* Fiber.join(fiber).pipe(Effect.flip);

        expect(error).toMatchObject({
          reason: mode === "timeout" ? "timeout" : "outcome-unknown",
          dispatch: "possibly-dispatched",
          cleanup: "confirmed",
        });
        expect(JSON.stringify(error)).not.toContain("private-fill-diagnostic");
      }
      expect(yield* handle.fill(request).pipe(Effect.flip)).toMatchObject({ reason: "closed" });
      expect(writes).toBe(1);
      expect(f.stats().closed).toBe(1);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect("enforces action and elapsed budgets during an hour-long pass", () => {
  const f = fixture();

  return Effect.gen(function* () {
    const handle = yield* (yield* ProtectedBrowser).open(
      InteractiveBrowserPolicy.make({ ...policy, maxElapsedMillis: 3_600_000, maxActions: 2 }),
    );

    yield* TestClock.adjust("11 minutes");
    yield* handle.observe;
    yield* handle.observe;
    expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({ reason: "limit" });

    const timed = yield* (yield* ProtectedBrowser).open(
      InteractiveBrowserPolicy.make({ ...policy, maxElapsedMillis: 3_600_000 }),
    );

    yield* TestClock.adjust("60 minutes");
    expect(yield* timed.observe.pipe(Effect.flip)).toMatchObject({
      reason: "timeout",
      cleanup: "confirmed",
    });
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect("allows card submission only through current credential authorization", () => {
  const f = fixture("card");
  let clicks = 0;

  f.setClick(() =>
    Effect.sync(() => {
      clicks++;
    }),
  );

  return Effect.gen(function* () {
    const handle = yield* f.open;
    const request = yield* proposal(f, handle);

    expect(
      yield* handle.useCredential(
        UseCredential.make({ ...request, submit: f.controls.at(-1)!.ref }),
      ),
    ).toMatchObject({
      dispatch: "dispatched",
      milestone: "submission-dispatched",
      authentication: "unverified",
    });
    expect(clicks).toBe(1);
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect.each([
  "success",
  "revoked",
  "caller-changed",
  "target-changed",
  "reply-lost",
  "needs-attention",
] as const)("authorizes a merchant submit after separate card-frame fills: %s", (mode) => {
  const f = fixture("card");
  let clicks = 0;

  f.setClick(() =>
    Effect.gen(function* () {
      clicks++;
      if (mode === "reply-lost") return yield* new ProtectedTransportError({ reason: "provider" });
    }),
  );

  return Effect.gen(function* () {
    const handle = yield* f.open;

    yield* handle.useCredential(yield* proposal(f, handle));

    const separate = CredentialTarget.make({
      ...f.controls[0]!.target,
      frame: crypto.randomUUID(),
      form: crypto.randomUUID(),
      document: crypto.randomUUID(),
    });

    for (let index = 0; index < f.controls.length; index++)
      f.controls[index] = ProtectedBrowserControl.make({ ...f.controls[index]!, target: separate });
    yield* handle.useCredential(yield* proposal(f, handle));

    const submit = ProtectedBrowserControl.make({
      ref: crypto.randomUUID(),
      role: "submit",
      label: "Pay",
      target: CredentialTarget.make({
        ...separate,
        frameOrigin: "https://shop.test",
        recipientOrigin: "https://shop.test",
        frame: crypto.randomUUID(),
        form: crypto.randomUUID(),
      }),
    });

    f.controls.push(submit);
    f.setAuthorizeAction((request) =>
      Effect.gen(function* () {
        expect(Redacted.value(request.caller)).toBe("alice");
        expect(request.action).toEqual({ _tag: "Submit", ref: submit.ref, target: submit.target });
        expect(request.exposures).toHaveLength(2);
        expect(request.exposures.every((target) => target.frameOrigin === "https://pay.test")).toBe(
          true,
        );
        if (mode === "revoked") return yield* new CredentialAccessError({ reason: "denied" });
        if (mode === "caller-changed") f.setPrincipal("mallory");
        if (mode === "target-changed")
          f.controls[f.controls.length - 1] = ProtectedBrowserControl.make({
            ...submit,
            target: CredentialTarget.make({
              ...submit.target,
              recipientOrigin: "https://evil.test",
            }),
          });
      }),
    );
    if (mode === "needs-attention") f.needAttention();
    const execute = handle.click(ProtectedBrowserClick.make({ ref: submit.ref }));

    if (mode === "success") {
      yield* execute;
      f.setAuthorizeAction(() => Effect.fail(new CredentialAccessError({ reason: "denied" })));
      expect(yield* execute.pipe(Effect.flip)).toMatchObject({
        reason: "denied",
        dispatch: "not-dispatched",
      });
    } else {
      expect(yield* execute.pipe(Effect.flip)).toMatchObject({
        reason:
          mode === "reply-lost"
            ? "outcome-unknown"
            : mode === "target-changed"
              ? "stale-reference"
              : mode === "needs-attention"
                ? "needs-attention"
                : "denied",
        dispatch: mode === "reply-lost" ? "possibly-dispatched" : "not-dispatched",
      });
    }
    expect(clicks).toBe(mode === "success" || mode === "reply-lost" ? 1 : 0);
    if (mode === "reply-lost") {
      expect(f.stats().closed).toBe(1);
      expect(yield* execute.pipe(Effect.flip)).toMatchObject({ reason: "closed" });
    }
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect.each(["navigate", "fill", "click"] as const)(
  "checks current continuation authority before ordinary %s",
  (action) => {
    const f = fixture();

    const field = ProtectedBrowserControl.make({
      ...f.controls[0]!,
      ref: crypto.randomUUID(),
      role: "text",
    });

    const button = ProtectedBrowserControl.make({
      ...f.controls[0]!,
      ref: crypto.randomUUID(),
      role: "button",
    });

    f.controls.push(field, button);

    return Effect.gen(function* () {
      const handle = yield* f.open;

      yield* handle.useCredential(yield* proposal(f, handle));
      let writes = 0;

      f.setFill(() =>
        Effect.sync(() => {
          writes++;
        }),
      );
      f.setClick(() =>
        Effect.sync(() => {
          writes++;
        }),
      );
      f.setNavigate(() =>
        Effect.sync(() => {
          writes++;
        }),
      );
      f.setAuthorizeAction((request) =>
        Effect.gen(function* () {
          expect(request.exposures).toHaveLength(1);

          return yield* new CredentialAccessError({ reason: "denied" });
        }),
      );

      const operation =
        action === "navigate"
          ? handle.navigate(ProtectedBrowserNavigate.make({ url: "https://shop.test/next" }))
          : action === "fill"
            ? handle.fill(ProtectedBrowserFill.make({ ref: field.ref, value: "ordinary" }))
            : handle.click(ProtectedBrowserClick.make({ ref: button.ref }));

      expect(yield* operation.pipe(Effect.flip)).toMatchObject({
        reason: "denied",
        dispatch: "not-dispatched",
      });
      expect(writes).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect("uses explicit recipient selection and rejects a grant narrowed during discovery", () => {
  const f = fixture("card");

  return Effect.gen(function* () {
    const handle = yield* f.open;

    yield* handle.useCredential(yield* proposal(f, handle));
    f.context.frameOrigins.push("https://untrusted.test");
    let calls = 0;

    f.setObservation((request) =>
      Effect.sync(() => {
        expect(request.frameOrigins).toContain("https://untrusted.test");
        calls++;

        return CredentialObservationGrant.make({
          decision: "trust-recipient-no-credential-echo",
          origins: calls >= 4 ? ["https://shop.test"] : ["https://shop.test", "https://pay.test"],
        });
      }),
    );
    expect((yield* handle.observe).controls).toHaveLength(f.controls.length);
    expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({
      reason: "observation-blocked",
      dispatch: "not-dispatched",
    });
    expect(
      yield* handle
        .listCredentialOffers(
          ListCredentialOffers.make({ kind: "card", target: f.controls[0]!.ref }),
        )
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "stale-reference" });
    f.setObservation(() =>
      Effect.succeed(
        CredentialObservationGrant.make({
          decision: "trust-recipient-no-credential-echo",
          origins: ["https://pay.test"],
        }),
      ),
    );
    const reads = f.stats().reads;

    expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({
      reason: "observation-blocked",
    });
    expect(f.stats().reads).toBe(reads);
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect.each(["timeout", "interrupt"] as const)(
  "closes an uncertain authorized submit on %s without replay",
  (mode) => {
    const f = fixture("card");

    f.setAuthorizeAction(() => Effect.void);

    return Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      let clicks = 0;

      f.setClick(() =>
        Effect.gen(function* () {
          clicks++;
          yield* Deferred.succeed(entered, undefined);

          return yield* Effect.never;
        }),
      );
      const handle = yield* f.open;

      yield* handle.useCredential(yield* proposal(f, handle));
      const execute = handle.click(ProtectedBrowserClick.make({ ref: f.controls.at(-1)!.ref }));
      const fiber = yield* Effect.forkChild(execute);

      yield* Deferred.await(entered);
      expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({ reason: "busy" });
      if (mode === "interrupt") yield* Fiber.interrupt(fiber);
      else {
        yield* TestClock.adjust("121 seconds");
        expect(yield* Fiber.join(fiber).pipe(Effect.flip)).toMatchObject({
          reason: "timeout",
          dispatch: "possibly-dispatched",
          cleanup: "confirmed",
        });
      }
      expect(f.stats().closed).toBe(1);
      expect(yield* execute.pipe(Effect.flip)).toMatchObject({ reason: "closed" });
      expect(clicks).toBe(1);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect("rejects a caller change while obtaining an observation grant before reading", () => {
  const f = fixture();

  return Effect.gen(function* () {
    const handle = yield* f.open;

    yield* handle.useCredential(yield* proposal(f, handle));
    const reads = f.stats().reads;

    f.setObservation(() =>
      Effect.sync(() => {
        f.setPrincipal("mallory");

        return CredentialObservationGrant.make({
          decision: "trust-recipient-no-credential-echo",
          origins: ["https://shop.test"],
        });
      }),
    );
    expect(yield* handle.observe.pipe(Effect.flip)).toMatchObject({
      reason: "denied",
      dispatch: "not-dispatched",
    });
    expect(f.stats().reads).toBe(reads);
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

it.effect("refuses native submission without explicit host action authority", () => {
  const f = fixture("card", false);
  let clicks = 0;

  f.setClick(() =>
    Effect.sync(() => {
      clicks++;
    }),
  );

  return Effect.gen(function* () {
    const handle = yield* f.open;
    const execute = handle.click(ProtectedBrowserClick.make({ ref: f.controls.at(-1)!.ref }));

    expect(yield* execute.pipe(Effect.flip)).toMatchObject({
      reason: "unsupported",
      dispatch: "not-dispatched",
    });
    yield* handle.useCredential(yield* proposal(f, handle));
    expect(yield* execute.pipe(Effect.flip)).toMatchObject({
      reason: "unsupported",
      dispatch: "not-dispatched",
    });
    expect(clicks).toBe(0);
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});
