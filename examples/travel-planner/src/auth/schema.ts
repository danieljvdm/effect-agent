import { DrizzleMappingError } from "@yielded/auth/Drizzle";
import { SubjectId } from "@yielded/auth/Schema";
import { type AuthenticationRequirement, SecurityRevision } from "@yielded/auth/Sessions";
import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { DateTime, Effect, Schema } from "effect";

// Local subjects are random IDs. Provider email/profile never grants account ownership.
export const subject = sqliteTable("auth_subject", {
  id: text().primaryKey(),
  active: integer({ mode: "boolean" }).notNull(),
  revision: text().notNull(),
  displayName: text().notNull(),
});

export const identifier = sqliteTable(
  "auth_identifier",
  {
    namespace: text().notNull(),
    value: text().notNull(),
    subjectId: text().notNull(),
    verifiedAt: integer().notNull(),
    revision: text().notNull(),
  },
  (t) => [uniqueIndex("auth_identifier_unique").on(t.namespace, t.value)],
);

export const credential = sqliteTable(
  "auth_credential",
  {
    subjectId: text().notNull(),
    credentialId: text().notNull(),
    revision: text().notNull(),
    active: integer({ mode: "boolean" }).notNull(),
  },
  (t) => [uniqueIndex("auth_credential_unique").on(t.subjectId, t.credentialId)],
);

export const emailCredential = sqliteTable(
  "auth_email_credential",
  {
    moduleId: text().notNull(),
    subjectId: text().notNull(),
    credentialId: text().notNull(),
    namespace: text().notNull(),
    value: text().notNull(),
    revision: text().notNull(),
    active: integer({ mode: "boolean" }).notNull(),
  },
  (t) => [
    uniqueIndex("auth_email_id_unique").on(t.moduleId, t.credentialId),
    uniqueIndex("auth_email_identifier_unique").on(t.moduleId, t.namespace, t.value),
  ],
);

export const emailRegistration = sqliteTable(
  "auth_email_registration",
  {
    moduleId: text().notNull(),
    commandId: text().notNull(),
    fingerprint: text().notNull(),
    state: text().notNull(),
    subjectId: text(),
    pendingReference: text(),
    retentionUntil: integer().notNull(),
  },
  (t) => [
    uniqueIndex("auth_email_registration_unique").on(t.moduleId, t.commandId),
    uniqueIndex("auth_email_pending_unique").on(t.pendingReference),
  ],
);

export const mappingError = () =>
  DrizzleMappingError.make({ operation: "travel-auth", cause: "Invalid authentication record" });

export const decodeMillis = (value: unknown) =>
  Schema.decodeUnknownEffect(Schema.Natural)(value).pipe(Effect.mapError(mappingError));

export const decodeUtc = (value: unknown) =>
  decodeMillis(value).pipe(Effect.map(DateTime.makeUnsafe));

export const encodeInstant = (millis: number) => millis;

export const subjectId = {
  toNative: (id: SubjectId) => Effect.succeed(id),
  toSubject: (id: string) =>
    Schema.decodeUnknownEffect(SubjectId)(id).pipe(Effect.mapError(mappingError)),
  equals: (a: string, b: string) => a === b,
};

export const requirement: AuthenticationRequirement = {
  maximumAgeMillis: 300_000,
  alternatives: [
    {
      factors: ["possession"],
      minimumCredentials: 1,
      userVerified: false,
      phishingResistant: false,
    },
  ],
};

export const subjectMapping = {
  table: subject,
  id: "id",
  status: "active",
  securityRevision: "revision",
  isActiveStatus: (value: unknown) => value === true,
  d1ActiveStatusValue: true,
  decodeRequirement: () => Effect.succeed(requirement),
  nextSecurityRevisionSync: () => SecurityRevision.make(crypto.randomUUID()),
} as const;

export const credentialMapping = {
  table: credential,
  subjectId: "subjectId",
  credentialId: "credentialId",
  revision: "revision",
  status: "active",
  isActiveStatus: (value: unknown) => value === true,
  d1ActiveStatusValue: true,
} as const;
