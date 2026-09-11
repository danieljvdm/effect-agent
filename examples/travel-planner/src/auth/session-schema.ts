import { requiredSessionConstraints, type StatefulSessionMapping } from "@yielded/auth/Drizzle";
import { TokenDigest } from "@yielded/auth/Schema";
import {
  SecurityRevision,
  SessionId,
  SessionMetadata,
  SessionAuthenticationProvenance,
  SessionCredentialVersion,
} from "@yielded/auth/Sessions";
import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { DateTime, Effect, Schema } from "effect";

import type { subject, credential } from "./schema";
import { subjectMapping, credentialMapping, subjectId, decodeUtc, mappingError } from "./schema";

export const Claims = Schema.Struct({ displayName: Schema.String });

const Record = Schema.Struct({
  ...SessionMetadata.fields,
  claims: Claims,
  digest: TokenDigest,
  version: SecurityRevision,
  provenance: SessionAuthenticationProvenance,
  credentialVersion: SessionCredentialVersion,
});

const recordJson = Schema.fromJsonString(Record);

export const session = sqliteTable(
  "auth_session",
  {
    id: text().primaryKey(),
    subjectId: text().notNull(),
    digest: text().notNull(),
    version: text().notNull(),
    revision: text().notNull(),
    issuedAt: integer().notNull(),
    expiresAt: integer().notNull(),
    absoluteExpiresAt: integer().notNull(),
    record: text().notNull(),
  },
  (t) => [uniqueIndex("auth_session_digest_unique").on(t.digest)],
);

export const sessionFlow = sqliteTable("auth_session_flow", {
  flowId: text().primaryKey(),
  subjectId: text().notNull(),
  state: text().notNull(),
  pendingDigest: text(),
  dedupUntil: integer().notNull(),
});

export const sessionsMapping: StatefulSessionMapping<
  typeof Claims.Type,
  typeof subject,
  typeof credential,
  typeof session,
  typeof sessionFlow,
  typeof sessionFlow,
  string,
  string
> = {
  subject: subjectMapping,
  credential: credentialMapping,
  subjectId,
  sessionId: {
    toNative: (id) => Effect.succeed(id),
    toSession: (id) =>
      Schema.decodeUnknownEffect(SessionId)(id).pipe(Effect.mapError(mappingError)),
    equals: (a, b) => a === b,
  },
  constraints: requiredSessionConstraints,
  isConstraintConflict: () => false,
  flow: {
    table: sessionFlow,
    flowId: "flowId",
    subjectId: "subjectId",
    state: "state",
    pendingDigest: "pendingDigest",
    dedupUntil: "dedupUntil",
    pendingStateValue: "pending",
    establishedStateValue: "established",
    encodeInstant: DateTime.toEpochMillis,
    decodeInstant: decodeUtc,
    encodePendingInsert: (input) => ({
      flowId: input.evidence.flowId,
      subjectId: input.subjectId,
      state: "pending",
      pendingDigest: input.pendingDigest,
      dedupUntil: DateTime.toEpochMillis(input.dedupUntil),
    }),
    encodeEstablishedInsert: (input) => ({
      flowId: input.evidence.flowId,
      subjectId: input.subjectId,
      state: "established",
      pendingDigest: null,
      dedupUntil: DateTime.toEpochMillis(input.dedupUntil),
    }),
  },
  session: {
    table: session,
    sessionId: "id",
    subjectId: "subjectId",
    digest: "digest",
    version: "version",
    securityRevision: "revision",
    issuedAt: "issuedAt",
    expiresAt: "expiresAt",
    absoluteExpiresAt: "absoluteExpiresAt",
    encodeInstant: DateTime.toEpochMillis,
    encodeInsert: (record, ids) => ({
      id: ids.sessionId,
      subjectId: ids.subjectId,
      digest: record.digest,
      version: record.version,
      revision: record.securityRevision,
      issuedAt: DateTime.toEpochMillis(record.issuedAt),
      expiresAt: DateTime.toEpochMillis(record.expiresAt),
      absoluteExpiresAt: DateTime.toEpochMillis(record.absoluteExpiresAt),
      record: Schema.encodeSync(recordJson)(record),
    }),
    encodeRotation: (record) => ({
      digest: record.digest,
      version: record.version,
      issuedAt: DateTime.toEpochMillis(record.issuedAt),
      expiresAt: DateTime.toEpochMillis(record.expiresAt),
      record: Schema.encodeSync(recordJson)(record),
    }),
    decode: (row) =>
      Schema.decodeEffect(recordJson)(row.record).pipe(Effect.mapError(mappingError)),
    allocateIdSync: () => crypto.randomUUID(),
    allocateVersionSync: () => SecurityRevision.make(crypto.randomUUID()),
  },
};
