import { Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";

import { AccessError, AccessRpcs, adminEmail, type AccessSession } from "../access-domain.ts";
import { AccessCommand, AccessReply } from "./access-admin.ts";
import { storageOwner } from "./tenancy.ts";

export const accessResponse = Effect.fn("accessResponse")(function* (
  request: Request,
  env: Cloudflare.Env,
  session: AccessSession,
) {
  const manage = Effect.fn("accessManage")(
    function* (command: typeof AccessCommand.Type) {
      if (session.registration === "open")
        return yield* new AccessError({
          code: "invalid",
          message:
            "Registration is open. Anyone can sign in with their email and connect their own OpenAI key.",
        });
      if (session.email !== adminEmail || !session.isAdmin)
        return yield* new AccessError({
          code: "forbidden",
          message: "Only the administrator can manage access.",
        });
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(AccessCommand))(command);

      const reply = yield* Effect.tryPromise({
        try: () => env.THREADS.getByName(storageOwner).manageInvitations(encoded),
        catch: () =>
          new AccessError({
            code: "unavailable",
            message: "Access management is unavailable. Refresh before retrying.",
          }),
      });

      const result = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(AccessReply))(reply);

      if (result._tag === "Failure") return yield* result.error;

      return result.value;
    },
    Effect.catchTag(
      "SchemaError",
      () =>
        new AccessError({ code: "unavailable", message: "The access response could not be read." }),
    ),
  );

  const handlers = AccessRpcs.toLayer({
    GetSession: () => Effect.succeed(session),
    GetMembers: () => manage({ _tag: "List" }),
    InviteMember: ({ email }) => manage({ _tag: "Invite", email }),
    RemoveMember: ({ email }) => manage({ _tag: "Remove", email }),
  });

  const web = yield* Effect.acquireRelease(
    Effect.sync(() =>
      HttpRouter.toWebHandler(
        RpcServer.layerHttp({
          group: AccessRpcs,
          path: "/api/access",
          protocol: "http",
          concurrency: 4,
        }).pipe(Layer.provide(handlers), Layer.provide(RpcSerialization.layerNdjson)),
        { disableLogger: true },
      ),
    ),
    (handler) => Effect.promise(() => handler.dispose()),
  );

  const response = yield* Effect.promise(() => web.handler(request));
  const body = yield* Effect.promise(() => response.arrayBuffer());

  return new Response(body, {
    status: response.status,
    headers: { ...Object.fromEntries(response.headers), "cache-control": "no-store" },
  });
}, Effect.scoped);
