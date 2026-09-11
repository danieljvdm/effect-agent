import { Effect, Schema } from "effect";
import { AsyncResult, Atom, Reactivity } from "effect/unstable/reactivity";

import { AccessError, adminEmail, type DemoAccessList, Email } from "./access-domain.ts";
import { AccessClient, sessionAtom } from "./state.ts";

const query = AccessClient.runtime
  .atom((get) =>
    Effect.gen(function* () {
      const session = get(sessionAtom);

      if (
        !AsyncResult.isSuccess(session) ||
        session.waiting ||
        !session.value.isAdmin ||
        session.value.email !== adminEmail
      )
        return yield* Effect.interrupt;
      const client = yield* AccessClient;
      const result = yield* client("GetDemoAccess", undefined);
      const current = get.once(sessionAtom);

      if (
        !AsyncResult.isSuccess(current) ||
        !current.value.isAdmin ||
        current.value.email !== adminEmail
      )
        return yield* Effect.interrupt;

      return result;
    }),
  )
  .pipe(AccessClient.runtime.factory.withReactivity(["demo-access"]));

export const demoAccessAtom = Atom.make((get) => {
  const session = get(sessionAtom);

  return AsyncResult.isSuccess(session) &&
    !session.waiting &&
    session.value.isAdmin &&
    session.value.email === adminEmail
    ? get(query)
    : AsyncResult.initial<DemoAccessList>();
});

export const demoEmailAtom = Atom.make("");

export const changeDemoAccessAtom = AccessClient.runtime.fn<
  { readonly action: "grant" } | { readonly action: "revoke"; readonly email: string }
>()(
  Effect.fnUntraced(function* (request, get) {
    const session = get(sessionAtom);

    if (
      !AsyncResult.isSuccess(session) ||
      session.waiting ||
      !session.value.isAdmin ||
      session.value.email !== adminEmail
    )
      return yield* new AccessError({
        code: "forbidden",
        message: "Sign in as the administrator to manage demo access.",
      });
    const draft = get(demoEmailAtom);

    const email = yield* Schema.decodeUnknownEffect(Email)(
      (request.action === "grant" ? draft : request.email).trim().toLowerCase(),
    ).pipe(
      Effect.mapError(
        () => new AccessError({ code: "invalid", message: "Enter a valid email address." }),
      ),
    );

    const client = yield* AccessClient;
    const current = get(sessionAtom);

    if (
      !AsyncResult.isSuccess(current) ||
      current.waiting ||
      !current.value.isAdmin ||
      current.value.email !== adminEmail
    )
      return yield* Effect.interrupt;

    yield* Reactivity.mutation(
      request.action === "grant"
        ? client("GrantDemoAccess", { email })
        : client("RevokeDemoAccess", { email }),
      ["demo-access", `openai-connection:${email}`],
    );
    if (request.action === "grant" && get(demoEmailAtom) === draft) get.set(demoEmailAtom, "");

    return request.action === "grant"
      ? `Demo access added for ${email}.`
      : `Demo access removed for ${email}.`;
  }),
);

export const refreshDemoAccessAtom = Atom.fnSync<void>()((_, get) => {
  if (get(changeDemoAccessAtom).waiting) return;
  get.set(changeDemoAccessAtom, Atom.Reset);
  get.refresh(query);
});
