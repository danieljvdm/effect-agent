import { Schema } from "effect";
import { AtomRegistry, AsyncResult } from "effect/unstable/reactivity";
import { expect, it, vi } from "vite-plus/test";

import {
  auth,
  completeGithub,
  consumeCallback,
  githubLogin,
  loginView,
  requestEmailCode,
  verifyEmailCode,
} from "../src/auth/client";

const session = {
  sessionId: "fixture-session",
  subjectId: "00000000-0000-0000-0000-000000000001",
  securityRevision: "revision",
  assurance: { method: "oauth", factors: ["possession"], authenticatedAt: 1_800_000_000_000 },
  issuedAt: 1_800_000_000_000,
  expiresAt: 1_900_000_000_000,
  absoluteExpiresAt: 1_900_000_000_000,
  claims: { displayName: "River Traveler" },
};

const envelope = Schema.fromJsonString(
  Schema.Struct({ payload: Schema.Record(Schema.String, Schema.Unknown) }),
);

const fixture = () => {
  const storage = new Map([["elsewhere:github", JSON.stringify({ flowId: "fixture-flow" })]]);
  const assign = vi.fn<(url: string) => void>();
  const settledBegin = vi.fn<() => void>();
  const requests: string[] = [];
  const pendingSessions: Array<() => void> = [];
  const pendingBegins: Array<() => void> = [];

  const state = {
    authenticated: false,
    holdSession: true,
    outcome: "authenticated" as "authenticated" | "registration" | "cancelled" | "rejected",
    rejectCode: false,
    holdBegin: false,
  };

  vi.stubGlobal("location", { origin: "https://fixture.test", assign });
  vi.stubGlobal("window", {
    __elsewhereCallback:
      "?code=fixture-code&state=fixture-state&iss=https%3A%2F%2Fgithub.com%2Flogin%2Foauth",
  });
  vi.stubGlobal("sessionStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => storage.set(key, value),
    removeItem: (key: string) => storage.delete(key),
  });
  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    const name = new URL(request.url).pathname.split("/").at(-1) ?? "";

    requests.push(name);
    const success = (value: unknown) => Response.json({ _tag: "Success", value });

    const rejected = (tag: string) =>
      Response.json({ _tag: "Failure", error: { _tag: tag } }, { status: 400 });

    if (name === "getSession") {
      if (state.authenticated && state.holdSession)
        return new Promise<Response>((resolve) => {
          pendingSessions.push(() => resolve(success(session)));
        });

      return success(state.authenticated ? session : null);
    }
    const { payload } = Schema.decodeUnknownSync(envelope)(await request.text());

    if (name === "signIn")
      return success({
        flowId: payload.flowId,
        authorizationUrl: "https://github.com/login/oauth/authorize?state=fixture-state",
        expiresAtMillis: 1_900_000_000_000,
      });
    if (name === "completeSignIn") {
      if (state.outcome === "rejected") return rejected("OAuthRejected");
      if (state.outcome === "cancelled") return success({ _tag: "Cancelled", returnTarget: "/" });
      if (state.outcome === "registration")
        return success({
          _tag: "RegistrationRequired",
          reference: "r".repeat(43),
          expiresAtMillis: 1_900_000_000_000,
          returnTarget: "/",
        });
    }
    if (name === "completeSignIn" || name === "completeEmailSignIn") {
      state.authenticated = true;

      return success({ completion: { _tag: "Authenticated", session }, returnTarget: "/" });
    }
    if (name === "register" || name === "completeEmailRegistration")
      return success({ _tag: "RegistrationAccepted" });
    if (name === "beginEmailRegistration" || name === "beginEmailSignIn") {
      if (state.holdBegin) await new Promise<void>((resolve) => pendingBegins.push(resolve));

      settledBegin();

      return success({ flowId: payload.flowId, expiresAtMillis: 1_900_000_000_000 });
    }
    if (name === "requestEmailCode" || name === "registerEmail")
      return success({
        requestId: payload.requestId,
        reference: {
          proofId: `fixture-${name}`,
          purpose: name === "registerEmail" ? "email-code-registration" : "email-code-sign-in",
          keyId: "v1",
        },
      });
    if (name === "verifyEmailCode" || name === "verifyEmailRegistration") {
      if (state.rejectCode) return rejected("EmailRejected");

      return success({
        continuation: {
          continuationId: "fixture-continuation",
          purpose: "email-code-sign-in",
          expiresAtMillis: 1_900_000_000_000,
        },
      });
    }
    throw new Error(`Unexpected fixture action: ${name}`);
  });

  const host = AtomRegistry.make();

  const releaseSession = () => {
    state.holdSession = false;
    for (const release of pendingSessions.splice(0)) release();
  };

  const releaseBegin = () => {
    for (const release of pendingBegins.splice(0)) release();
  };

  host.mount(auth.session);

  return {
    host,
    state,
    assign,
    settledBegin,
    requests,
    releaseSession,
    releaseBegin,
    beginPending: () => pendingBegins.length > 0,
    ready: () =>
      vi.waitFor(() => expect(AsyncResult.getOrThrow(host.get(auth.session))).toBeNull()),
    dispose: () => {
      releaseSession();
      host.dispose();
      releaseBegin();
      vi.unstubAllGlobals();
    },
  };
};

it.each(["github", "email"] as const)(
  "keeps %s sign-in pending through account replacement until the session is published",
  async (method) => {
    const f = fixture();
    const view = loginView(method === "github");
    const observedFailures: boolean[] = [];

    const unsubscribe = f.host.subscribe(
      view,
      (value) => observedFailures.push(value._tag === "Form" && value.error !== undefined),
      { immediate: true },
    );

    try {
      await f.ready();
      expect(f.host.get(view)._tag).toBe(method === "github" ? "Loading" : "Form");
      if (method === "github") {
        f.host.set(consumeCallback, undefined);
      } else {
        f.host.set(requestEmailCode, { mode: "signin", email: "fixture@example.com" });
        await vi.waitUntil(() => AsyncResult.isSuccess(f.host.get(requestEmailCode)));
        const pending = AsyncResult.getOrThrow(f.host.get(requestEmailCode));

        f.host.set(verifyEmailCode, { pending, code: "123456" });
      }
      await vi.waitFor(() => {
        const result =
          method === "github" ? f.host.get(completeGithub) : f.host.get(verifyEmailCode);

        expect(result._tag).toBe("Success");
      });
      expect(f.host.get(view)).toEqual({ _tag: "Loading", step: "callback" });
      expect(observedFailures).not.toContain(true);
      f.releaseSession();
      await vi.waitFor(() => expect(f.host.get(view)).toEqual({ _tag: "Authenticated" }));
      expect(f.requests.filter((name) => name === "completeSignIn")).toHaveLength(
        method === "github" ? 1 : 0,
      );
    } finally {
      unsubscribe();
      f.dispose();
    }
  },
);

it("keeps GitHub registration in the loading screen while redirecting to its fresh sign-in", async () => {
  const f = fixture();

  f.state.outcome = "registration";
  f.host.mount(loginView(true));
  try {
    await f.ready();
    f.host.set(consumeCallback, undefined);
    f.host.set(consumeCallback, undefined);
    await vi.waitFor(() => expect(f.assign).toHaveBeenCalledTimes(1));
    expect(f.host.get(loginView(true))).toEqual({ _tag: "Loading", step: "callback" });
    expect(f.requests.filter((name) => name === "completeSignIn")).toHaveLength(1);
    expect(f.requests.filter((name) => name === "register")).toHaveLength(1);
    expect(f.requests.filter((name) => name === "signIn")).toHaveLength(1);
  } finally {
    f.dispose();
  }
});

it("releases email input and challenge state when the login screen is left", async () => {
  const f = fixture();
  const unmount = f.host.mount(loginView(false));

  try {
    await f.ready();
    f.host.set(requestEmailCode, { mode: "signin", email: "fixture@example.com" });
    await vi.waitFor(() => expect(f.host.get(requestEmailCode)._tag).toBe("Success"));
    unmount();
    await vi.waitFor(() => expect(f.host.get(requestEmailCode)._tag).toBe("Initial"));
    expect(f.host.get(loginView(false))).toMatchObject({ _tag: "Form", pending: undefined });
  } finally {
    f.dispose();
  }
});

it("stops the email workflow on disposal while an admitted credential response settles", async () => {
  const f = fixture();

  f.state.holdBegin = true;
  f.host.mount(loginView(false));
  try {
    await f.ready();
    f.host.set(requestEmailCode, { mode: "signin", email: "fixture@example.com" });
    await vi.waitFor(() => expect(f.beginPending()).toBe(true));
    f.host.dispose();
    // Auth settles admitted Set-Cookie responses uninterruptibly. The disposed
    // application workflow must not proceed to issue a code afterward.
    f.releaseBegin();
    await vi.waitFor(() => expect(f.settledBegin).toHaveBeenCalledTimes(1));
    expect(f.requests).not.toContain("requestEmailCode");
  } finally {
    f.dispose();
  }
});

it.each(["cancelled", "rejected"] as const)(
  "offers a fresh GitHub attempt after a %s callback without repeating the exchange",
  async (outcome) => {
    const f = fixture();

    f.state.outcome = outcome;
    f.host.mount(loginView(true));
    try {
      await f.ready();
      f.host.set(consumeCallback, undefined);
      await vi.waitFor(() => expect(f.host.get(loginView(true))._tag).toBe("Form"));
      expect(f.host.get(loginView(true))).toMatchObject({
        cancelled: outcome === "cancelled",
        error: outcome === "rejected" ? "github" : undefined,
      });
      expect(f.requests.filter((name) => name === "completeSignIn")).toHaveLength(1);
      f.host.set(githubLogin, undefined);
      expect(f.host.get(loginView(true))).toEqual({ _tag: "Loading", step: "github" });
      await vi.waitFor(() => expect(f.assign).toHaveBeenCalledTimes(1));
      expect(f.host.get(loginView(true))).toEqual({ _tag: "Loading", step: "github" });
    } finally {
      f.dispose();
    }
  },
);

it("retains the new email sign-in challenge after a registration succeeds and a sign-in code fails", async () => {
  const f = fixture();

  f.host.mount(loginView(false));
  try {
    await f.ready();
    f.host.set(requestEmailCode, { mode: "register", email: "fixture@example.com" });
    await vi.waitFor(() => expect(AsyncResult.isSuccess(f.host.get(requestEmailCode))).toBe(true));
    f.host.set(verifyEmailCode, {
      pending: AsyncResult.getOrThrow(f.host.get(requestEmailCode)),
      code: "123456",
    });
    await vi.waitFor(() => expect(AsyncResult.isSuccess(f.host.get(verifyEmailCode))).toBe(true));
    const pending = AsyncResult.getOrThrow(f.host.get(verifyEmailCode));

    if (!pending) throw new Error("Expected the fresh sign-in challenge");
    expect(pending.mode).toBe("signin");
    f.state.rejectCode = true;
    f.host.set(verifyEmailCode, { pending, code: "000000" });
    await vi.waitFor(() => expect(f.host.get(verifyEmailCode)._tag).toBe("Failure"));
    expect(f.host.get(loginView(false))).toMatchObject({
      _tag: "Form",
      pending,
      registered: true,
      busy: false,
      error: "email",
    });
  } finally {
    f.dispose();
  }
});
