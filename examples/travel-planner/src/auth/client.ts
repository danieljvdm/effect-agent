import * as AuthAtom from "@yielded/auth/Atom";
import * as Client from "@yielded/auth/Client";
import type { ProofReference } from "@yielded/auth/Proofs";
import { Effect, Redacted, Schema } from "effect";
import { Atom, AsyncResult } from "effect/unstable/reactivity";

import { LoginApi } from "./contract";

export const runtime = Atom.context();

export const AppClient = Client.make(LoginApi, {
  baseUrl: typeof location === "undefined" ? "https://travel.effect-agent.com" : location.origin,
});

export const auth = AuthAtom.make(AppClient, { runtime });

export const accountLifetime = auth.runtime.atom(
  Effect.flatMap(AuthAtom.AuthAtomLifetime, (lifetime) => lifetime.get),
);

/** One host registry observes cookie changes; account runtimes own private work. */
export const sessionObservation = Atom.make((get) => {
  if (typeof window === "undefined") return;

  const refresh = () => {
    if (document.visibilityState === "visible") get.refresh(auth.session);
  };

  window.addEventListener("focus", refresh);
  document.addEventListener("visibilitychange", refresh);
  const timer = setInterval(refresh, 60_000);
  let previous: string | undefined;

  get.subscribe(
    auth.session,
    (result) => {
      if (!AsyncResult.isSuccess(result)) return;
      const next = result.value?.subjectId;

      if (previous !== undefined && previous !== next) {
        // Voice retains only admission identities in sessionStorage, scoped to this account.
        const prefix = `travel-voice:v1:${previous}:`;

        try {
          for (let i = sessionStorage.length - 1; i >= 0; i--) {
            const key = sessionStorage.key(i);

            if (key?.startsWith(prefix)) sessionStorage.removeItem(key);
          }
        } catch {
          // Browsers may deny storage. The account registry still retires independently.
        }
      }
      previous = next;
    },
    { immediate: true },
  );
  get.addFinalizer(() => {
    clearInterval(timer);
    window.removeEventListener("focus", refresh);
    document.removeEventListener("visibilitychange", refresh);
  });
});

export class BrowserFlowUnavailable extends Schema.TaggedError<BrowserFlowUnavailable>()(
  "BrowserFlowUnavailable",
  {},
) {}

const browser = <A>(run: () => A) =>
  Effect.try({ try: run, catch: () => new BrowserFlowUnavailable() });

const id = () => browser(() => crypto.randomUUID());
const PendingGithub = Schema.fromJsonString(Schema.Struct({ flowId: Schema.NonEmptyString }));

const startGithub = Effect.gen(function* () {
  const client = yield* AppClient;
  const flowId = yield* id();

  yield* browser(() =>
    sessionStorage.setItem("elsewhere:github", Schema.encodeSync(PendingGithub)({ flowId })),
  );

  const started = yield* client.auth.signIn({
    flowId,
    commandId: yield* id(),
    provider: "github",
    callbackId: "github",
    returnTarget: "/",
  });

  yield* browser(() => location.assign(Redacted.value(started.authorizationUrl)));
});

export const githubLogin = auth.runtime.fn<void>()(() => startGithub);

// The Worker captures callback credentials in memory before loading page resources.
// Only public flow correlation survives navigation.
declare global {
  interface Window {
    __elsewhereCallback?: string;
  }
}

export const callbackInput = Effect.gen(function* () {
  const captured = yield* browser(() => {
    const query = window.__elsewhereCallback;

    delete window.__elsewhereCallback;
    const pending = sessionStorage.getItem("elsewhere:github");

    sessionStorage.removeItem("elsewhere:github");

    return { query, pending };
  });

  if (!captured.query || !captured.pending) return yield* new BrowserFlowUnavailable();
  const pending = yield* Schema.decodeEffect(PendingGithub)(captured.pending);
  const query = new URLSearchParams(captured.query);

  for (const key of ["state", "code", "error", "iss"])
    if (query.getAll(key).length > 1) return yield* new BrowserFlowUnavailable();

  const state = query.get("state"),
    code = query.get("code"),
    error = query.get("error"),
    issuer = query.get("iss");

  if (!state || (code === null) === (error === null)) return yield* new BrowserFlowUnavailable();

  return {
    ...pending,
    provider: "github",
    callbackId: "github",
    response:
      code === null
        ? {
            _tag: "Error" as const,
            state,
            error: error === "access_denied" ? ("access-denied" as const) : ("rejected" as const),
            ...(issuer === null ? {} : { issuer }),
          }
        : { _tag: "Code" as const, state, code, ...(issuer === null ? {} : { issuer }) },
  };
});

export const completeGithub = auth.runtime.fn<void>()(() =>
  Effect.gen(function* () {
    const client = yield* AppClient;
    const input = yield* callbackInput;
    const result = yield* client.auth.completeSignIn(input);

    if ("_tag" in result && result._tag === "RegistrationRequired") {
      const registered = yield* client.auth.register({
        flowId: input.flowId,
        commandId: yield* id(),
        reference: result.reference,
        registration: { displayName: "GitHub traveler" },
      });

      if (registered._tag !== "RegistrationAccepted") return yield* new BrowserFlowUnavailable();
      // Accepted registration creates an account, not a session. Start a NEW authorized flow.
      yield* startGithub;
    }

    return result;
  }),
);

const callbackStarted = Atom.make(false).pipe(Atom.keepAlive);

export const consumeCallback = Atom.fnSync<void>()((_, get) => {
  if (get(callbackStarted)) return;
  get.set(callbackStarted, true);
  get.set(completeGithub, undefined);
});

export type EmailPending = {
  readonly mode: "register" | "signin";
  readonly flowId: string;
  readonly email: string;
  readonly reference: typeof ProofReference.Encoded;
};

const sendSignInCode = Effect.fn("Login.sendSignInCode")(function* (email: string) {
  const client = yield* AppClient;
  const flowId = yield* id();

  yield* client.auth.beginEmailSignIn({ flowId });

  const receipt = yield* client.auth.requestEmailCode({
    flowId,
    email,
    requestId: yield* id(),
    returnTarget: "/",
    locale: "en",
  });

  return { mode: "signin", flowId, email, reference: receipt.reference } satisfies EmailPending;
});

export const requestEmailCode = auth.runtime.fn<{
  readonly mode: "register" | "signin";
  readonly email: string;
}>()((input) =>
  Effect.gen(function* () {
    const email = input.email.trim().toLowerCase();

    if (input.mode === "signin") return yield* sendSignInCode(email);
    const client = yield* AppClient;
    const flowId = yield* id();

    yield* client.auth.beginEmailRegistration({ flowId });

    const receipt = yield* client.auth.registerEmail({
      flowId,
      email,
      registration: { displayName: "Traveler" },
      requestId: yield* id(),
      locale: "en",
    });

    return { mode: "register", flowId, email, reference: receipt.reference } satisfies EmailPending;
  }),
);

export const verifyEmailCode = auth.runtime.fn<{
  readonly pending: EmailPending;
  readonly code: string;
}>()((input) =>
  Effect.gen(function* () {
    const client = yield* AppClient;
    const { flowId, email, reference, mode } = input.pending;

    if (mode === "register") {
      const base = { flowId, email, registration: { displayName: "Traveler" } };

      const verified = yield* client.auth.verifyEmailRegistration({
        ...base,
        reference,
        secret: input.code,
      });

      const result = yield* client.auth.completeEmailRegistration({
        ...base,
        continuationId: verified.continuation.continuationId,
        commandId: yield* id(),
      });

      if (result._tag !== "RegistrationAccepted") return yield* new BrowserFlowUnavailable();

      return yield* sendSignInCode(email);
    }
    const base = { flowId, email, returnTarget: "/" };
    const verified = yield* client.auth.verifyEmailCode({ ...base, reference, secret: input.code });

    yield* client.auth.completeEmailSignIn({
      ...base,
      continuationId: verified.continuation.continuationId,
    });
    // Session rendering owns the transition: this custom workflow may retire here.
  }),
);
