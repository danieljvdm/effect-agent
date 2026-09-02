import {
  BrowserCredentialAccess,
  CardCredential,
  CredentialAccessError,
  CredentialOfferMetadata,
  CredentialTarget,
  InteractiveBrowserPolicy,
  ListCredentialOffers,
  LoginCredential,
  ProtectedBrowser,
  ProtectedBrowserClick,
  ProtectedBrowserControl,
  ProtectedBrowserNavigate,
  ProtectedBrowserSession,
  UseCredential,
  type CredentialFieldRole,
} from "@effect-agent/sandbox";
import { BrowserCrypto } from "@effect/platform-browser";
import { expect, expectTypeOf, it } from "@effect/vitest";
import { type Crypto, Deferred, Effect, Fiber, Layer, Redacted, Schema, type Scope } from "effect";
import { TestClock } from "effect/testing";

import { BrowserRunSessionLifecycle } from "../src/browser-session-lifecycle.ts";
import { browserRunProtectedBindingLayer } from "../src/protected-browser.ts";
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

const fixture = (kind: "login" | "card" = "login") => {
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

      return stale || disposed || !found
        ? Effect.fail(new ProtectedTransportError({ reason: "stale-reference" }))
        : Effect.succeed(found);
    });

  const context = {
    document: crypto.randomUUID(),
    topOrigin: target.topOrigin,
    frameOrigins: [target.frameOrigin],
  };

  const driver: ProtectedBrowserTransport = {
    context: Effect.succeed(context),
    discover: Effect.sync(() => {
      reads++;

      return { ...context, text: "account dashboard", controls, truncated: false };
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
      role: typeof CredentialFieldRole.Type,
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
    observation: () =>
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
        return yield* (yield* BrowserRunProtectedTransport).open(policy);
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

it.effect("denies metadata listing and card submission without resolving credentials", () => {
  const f = fixture("card");

  return Effect.gen(function* () {
    const handle = yield* f.open;
    const request = yield* proposal(f, handle);

    expect(
      yield* handle
        .useCredential(UseCredential.make({ ...request, submit: f.controls.at(-1)!.ref }))
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "unsupported", dispatch: "not-dispatched" });
    f.revoke();
    expect(
      yield* handle
        .listCredentialOffers(
          ListCredentialOffers.make({ kind: "card", target: f.controls[0]!.ref }),
        )
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "denied" });
    expect(f.stats().resolved).toBe(0);
  }).pipe(Effect.scoped, Effect.provide(f.layer));
});

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
          yield* Effect.never;
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
