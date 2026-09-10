import { Context, Effect, Redacted, Schema } from "effect";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

import { AccessError, AccessMembers, adminEmail, Email } from "../access-domain.ts";

export interface AccessAdminEnvironment {
  readonly ACCESS_ACCOUNT_ID?: string;
  readonly ACCESS_GROUP_ID?: string;
  readonly ACCESS_API_TOKEN?: string;
}

export const AccessCommand = Schema.Union([
  Schema.TaggedStruct("List", {}),
  Schema.TaggedStruct("Invite", { email: Email }),
  Schema.TaggedStruct("Remove", { email: Email }),
]);

export const AccessReply = Schema.Union([
  Schema.TaggedStruct("Success", { value: AccessMembers }),
  Schema.TaggedStruct("Failure", { error: AccessError }),
]);

export const AccessFailpoint = Context.Reference<{
  readonly hit: (point: "before-update" | "after-update") => Effect.Effect<void, AccessError>;
}>("travel-planner/AccessFailpoint", {
  defaultValue: () => ({
    hit: (_point: "before-update" | "after-update"): Effect.Effect<void, AccessError> =>
      Effect.void,
  }),
});

const Group = Schema.Struct({
  id: Schema.NonEmptyString,
  name: Schema.Literal("effect-agent-travel-planner-invited"),
  include: Schema.Array(
    Schema.Struct({
      email: Schema.Struct({ email: Email }).annotate({
        parseOptions: { onExcessProperty: "error" },
      }),
    }).annotate({ parseOptions: { onExcessProperty: "error" } }),
  ).check(Schema.isMinLength(1), Schema.isMaxLength(200)),
  exclude: Schema.Array(Schema.Never),
  require: Schema.Array(Schema.Never),
});

const ApiResponse = Schema.Struct({ success: Schema.Literal(true), result: Group });

const Configuration = Schema.Struct({
  ACCESS_ACCOUNT_ID: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
  ACCESS_GROUP_ID: Schema.String.check(Schema.isPattern(/^[a-f0-9-]{36}$/)),
  ACCESS_API_TOKEN: Schema.NonEmptyString,
});

const unavailable = () =>
  new AccessError({
    code: "unavailable",
    message:
      "Access management is unavailable. Refresh the list before retrying; a previous change may have succeeded.",
  });

/** Cloudflare is the membership source of truth. The owner Object serializes read/update pairs. */
export const manageAccess = Effect.fn("manageAccess")(
  function* (command: typeof AccessCommand.Type, environment: AccessAdminEnvironment) {
    const env = yield* Schema.decodeUnknownEffect(Configuration)(environment).pipe(
      Effect.mapError(
        () =>
          new AccessError({
            code: "unavailable",
            message: "Invitations are not configured for this deployment. No access was changed.",
          }),
      ),
    );

    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.mapRequest(HttpClientRequest.bearerToken(Redacted.make(env.ACCESS_API_TOKEN))),
      HttpClient.filterStatusOk,
    );

    const url = `https://api.cloudflare.com/client/v4/accounts/${env.ACCESS_ACCOUNT_ID}/access/groups/${env.ACCESS_GROUP_ID}`;
    const decode = HttpClientResponse.schemaBodyJson(ApiResponse);

    const current = (yield* client
      .get(url)
      .pipe(Effect.flatMap(decode), Effect.mapError(unavailable))).result;

    if (current.id !== env.ACCESS_GROUP_ID) return yield* unavailable();
    const emails = [...new Set(current.include.map((rule) => rule.email.email.toLowerCase()))];

    if (!emails.includes(adminEmail)) return yield* unavailable();

    if (command._tag === "List") return { emails: emails.sort(), adminEmail };
    const email = command.email.toLowerCase();

    if (command._tag === "Remove" && email === adminEmail)
      return yield* new AccessError({
        code: "forbidden",
        message: "The administrator cannot be removed.",
      });

    const next =
      command._tag === "Invite"
        ? [...new Set([...emails, email])]
        : emails.filter((item) => item !== email);

    if (next.length > 200)
      return yield* new AccessError({
        code: "invalid",
        message: "The planner allows up to 200 email addresses.",
      });
    if (next.length === emails.length) return { emails: emails.sort(), adminEmail };

    const body = yield* Schema.encodeEffect(Group)({
      ...current,
      include: next.map((item) => ({ email: { email: item } })),
    }).pipe(Effect.mapError(unavailable));

    const failpoint = yield* AccessFailpoint;

    yield* failpoint.hit("before-update");

    const saved = (yield* HttpClientRequest.put(url).pipe(
      HttpClientRequest.bodyJsonUnsafe(body),
      client.execute,
      Effect.flatMap(decode),
      Effect.mapError(unavailable),
    )).result;

    if (
      saved.id !== env.ACCESS_GROUP_ID ||
      !saved.include.some((rule) => rule.email.email.toLowerCase() === adminEmail)
    )
      return yield* unavailable();

    yield* failpoint.hit("after-update");

    return {
      emails: saved.include.map((rule) => rule.email.email.toLowerCase()).sort(),
      adminEmail,
    };
  },
  Effect.timeoutOrElse({ duration: "15 seconds", orElse: unavailable }),
  // workerd rejects "error" before I/O. Manual mode plus filterStatusOk refuses
  // redirects without forwarding the credential to another destination.
  Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
);
