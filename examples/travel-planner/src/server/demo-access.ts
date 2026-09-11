import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { Context, Effect, Layer, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { AccessError, DemoAccessList, Email } from "../access-domain.ts";
import { demoKeyConfigured } from "../credential-domain.ts";
import { plannerOwner, storageOwner } from "./tenancy.ts";

export const DemoAccessCommand = Schema.Union([
  Schema.TaggedStruct("List", {}),
  Schema.TaggedStruct("Grant", { email: Email }),
  Schema.TaggedStruct("Revoke", { email: Email }),
]);

export const DemoAccessReply = Schema.Union([
  Schema.TaggedStruct("Success", { value: DemoAccessList }),
  Schema.TaggedStruct("Failure", { error: AccessError }),
]);

const StoredAccess = Schema.Struct({
  version: Schema.Literal(1),
  emails: DemoAccessList.fields.emails.check(
    Schema.makeFilter(
      (emails) =>
        new Set(emails).size === emails.length &&
        emails.every((email) => email === email.trim().toLowerCase()),
    ),
  ),
});

const Rows = Schema.Array(Schema.Struct({ value: Schema.String }));

const Owner = Schema.String.check(
  Schema.isPattern(/^(?:travel-planner-owner-v1|member-[a-f0-9]{64})$/),
);

const unavailable = () =>
  new AccessError({
    code: "unavailable",
    message: "Demo access could not be read or updated. Refresh the list before retrying.",
  });

export const DemoAccessFailpoint = Context.Reference<{
  readonly hit: (
    point: "schema:before" | "schema:after" | "save:before" | "save:after",
  ) => Effect.Effect<void, AccessError>;
}>("travel-planner/DemoAccessFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

export class DemoAccessStore extends Context.Service<
  DemoAccessStore,
  {
    readonly manage: (
      command: typeof DemoAccessCommand.Type,
    ) => Effect.Effect<DemoAccessList, AccessError>;
    readonly allows: (owner: string) => Effect.Effect<boolean, AccessError>;
  }
>()("travel-planner/DemoAccessStore") {}

/** Mutable funding permissions live only in the administrator's existing private Object. */
export const DemoAccessStoreLive = Layer.effect(
  DemoAccessStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient;
    const identity = yield* ThreadObjectIdentity;
    const env = yield* WorkerEnvironment;
    const failpoint = yield* DemoAccessFailpoint;

    const requireAdminStore = Effect.suspend(() =>
      identity.threadId === storageOwner
        ? Effect.void
        : Effect.fail(new AccessError({ code: "forbidden", message: "Demo access is private." })),
    );

    // Initialize on first use, not every unrelated account/worker activation.
    const read = Effect.gen(function* () {
      yield* requireAdminStore;
      yield* failpoint.hit("schema:before");
      yield* sql`CREATE TABLE IF NOT EXISTS travel_demo_access (
        id INTEGER PRIMARY KEY CHECK (id = 1), value TEXT NOT NULL
      )`.pipe(Effect.mapError(unavailable));
      yield* failpoint.hit("schema:after");

      const rows = yield* sql`SELECT value FROM travel_demo_access WHERE id = 1`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
        Effect.mapError(unavailable),
      );

      if (rows[0] === undefined) return [];

      const stored = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(StoredAccess))(
        rows[0].value,
      ).pipe(Effect.mapError(unavailable));

      return stored.emails;
    });

    const manage = Effect.fn("DemoAccessStore.manage")(function* (
      command: typeof DemoAccessCommand.Type,
    ) {
      yield* requireAdminStore;
      if (command._tag === "List")
        return { emails: yield* read, configured: demoKeyConfigured(env) };

      const email = yield* Schema.decodeUnknownEffect(Email)(
        command.email.trim().toLowerCase(),
      ).pipe(
        Effect.mapError(
          () => new AccessError({ code: "invalid", message: "Enter a valid email." }),
        ),
      );

      yield* failpoint.hit("save:before");

      const emails = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const current = yield* read;

            const next = (
              command._tag === "Grant"
                ? [...new Set([...current, email])]
                : current.filter((entry) => entry !== email)
            ).sort();

            if (next.length > 200)
              return yield* new AccessError({
                code: "invalid",
                message: "Demo access supports up to 200 email addresses.",
              });

            const value = yield* Schema.encodeEffect(Schema.fromJsonString(StoredAccess))({
              version: 1,
              emails: next,
            }).pipe(Effect.mapError(unavailable));

            yield* sql`INSERT INTO travel_demo_access (id, value) VALUES (1, ${value})
            ON CONFLICT (id) DO UPDATE SET value = excluded.value`;

            return next;
          }),
        )
        .pipe(Effect.catchTag("SqlError", unavailable));

      yield* failpoint.hit("save:after");

      return { emails, configured: demoKeyConfigured(env) };
    });

    const allows = Effect.fn("DemoAccessStore.allows")(function* (owner: string) {
      if (!Schema.is(Owner)(owner)) return false;
      const emails = yield* read;
      const owners = yield* Effect.forEach(emails, plannerOwner).pipe(Effect.mapError(unavailable));

      return owners.includes(owner);
    });

    return DemoAccessStore.of({ manage, allows });
  }),
);
