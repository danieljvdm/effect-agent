import {
  DrizzleMappingError,
  requiredProofConstraints,
  type ProofPersistenceMapping,
} from "@yielded/auth/Drizzle";
import {
  ProofBinding,
  ProofPurpose,
  ProofId,
  ProofRequestId,
  ProofDeliveryId,
  ProofVersion,
  ProofRequestReceipt,
  ProofContinuationId,
} from "@yielded/auth/Proofs";
import { TokenDigest } from "@yielded/auth/Schema";
import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { Effect, Schema } from "effect";

import type { subject, credential } from "./schema";
import {
  identifier,
  subjectMapping,
  credentialMapping,
  subjectId,
  encodeInstant,
  decodeMillis as decodeInstant,
} from "./schema";

export const request = sqliteTable(
  "auth_proof_request",
  {
    moduleId: text().notNull(),
    requestId: text().notNull(),
    fingerprint: text().notNull(),
    proofId: text().notNull(),
    purpose: text().notNull(),
    keyId: text().notNull(),
    createdAt: integer().notNull(),
    retentionUntil: integer().notNull(),
    receipt: text().notNull(),
  },
  (t) => [uniqueIndex("auth_request_unique").on(t.moduleId, t.requestId)],
);

export const series = sqliteTable(
  "auth_proof_series",
  {
    moduleId: text().notNull(),
    purpose: text().notNull(),
    scopeKey: text().notNull(),
    activeProofId: text(),
    lastIssueAt: integer(),
    version: text().notNull(),
  },
  (t) => [uniqueIndex("auth_series_unique").on(t.moduleId, t.purpose, t.scopeKey)],
);

export const generation = sqliteTable(
  "auth_proof_generation",
  {
    moduleId: text().notNull(),
    purpose: text().notNull(),
    proofId: text().notNull(),
    requestId: text().notNull(),
    seriesKey: text().notNull(),
    deliveryId: text().notNull(),
    binding: text().notNull(),
    verifierKeyId: text().notNull(),
    verifierDigest: text().notNull(),
    issuedAt: integer().notNull(),
    expiresAt: integer().notNull(),
    version: text().notNull(),
    state: text().notNull(),
    sendCount: integer().notNull(),
    deliveryState: text().notNull(),
    claimVersion: text(),
    claimDeadline: integer(),
    retryAt: integer(),
    deliveryRetryMillis: integer().notNull(),
    retentionUntil: integer().notNull(),
    fingerprint: text().notNull(),
  },
  (t) => [
    uniqueIndex("auth_generation_unique").on(t.moduleId, t.proofId),
    uniqueIndex("auth_delivery_unique").on(t.moduleId, t.deliveryId),
  ],
);

export const continuation = sqliteTable(
  "auth_proof_continuation",
  {
    moduleId: text().notNull(),
    purpose: text().notNull(),
    continuationId: text().notNull(),
    digest: text().notNull(),
    proofId: text().notNull(),
    seriesKey: text().notNull(),
    binding: text().notNull(),
    expiresAt: integer().notNull(),
    consumed: integer({ mode: "boolean" }).notNull(),
    version: text().notNull(),
    retentionUntil: integer().notNull(),
  },
  (t) => [
    uniqueIndex("auth_continuation_unique").on(t.moduleId, t.continuationId),
    uniqueIndex("auth_continuation_digest").on(t.moduleId, t.digest),
  ],
);

export const rate = sqliteTable(
  "auth_proof_rate",
  {
    moduleId: text().notNull(),
    purpose: text().notNull(),
    action: text().notNull(),
    scopeKind: text().notNull(),
    scopeKey: text().notNull(),
  },
  (t) => [
    uniqueIndex("auth_rate_unique").on(t.moduleId, t.purpose, t.action, t.scopeKind, t.scopeKey),
  ],
);

export const abuse = sqliteTable(
  "auth_proof_abuse",
  {
    moduleId: text().notNull(),
    purpose: text().notNull(),
    action: text().notNull(),
    scopeKind: text().notNull(),
    scopeKey: text().notNull(),
    commandId: text().notNull(),
    occurredAt: integer().notNull(),
    retentionUntil: integer().notNull(),
  },
  (t) => [
    uniqueIndex("auth_abuse_unique").on(t.moduleId, t.action, t.scopeKind, t.scopeKey, t.commandId),
  ],
);

export const failure = sqliteTable(
  "auth_proof_failure",
  {
    moduleId: text().notNull(),
    purpose: text().notNull(),
    seriesKey: text().notNull(),
    commandId: text().notNull(),
    occurredAt: integer().notNull(),
    retentionUntil: integer().notNull(),
  },
  (t) => [uniqueIndex("auth_failure_unique").on(t.moduleId, t.seriesKey, t.commandId)],
);

export const command = sqliteTable(
  "auth_proof_command",
  {
    moduleId: text().notNull(),
    commandId: text().notNull(),
    kind: text().notNull(),
    decision: text().notNull(),
    retentionUntil: integer().notNull(),
  },
  (t) => [uniqueIndex("auth_command_unique").on(t.moduleId, t.commandId)],
);

const bindingCodec = Schema.fromJsonString(ProofBinding),
  receiptCodec = Schema.fromJsonString(ProofRequestReceipt);

const decodeBinding = (value: string) =>
  Schema.decodeEffect(bindingCodec)(value).pipe(
    Effect.mapError(() =>
      DrizzleMappingError.make({
        operation: "travel-auth-codec",
        cause: "invalid consumer value",
      }),
    ),
  );

export const proofs: ProofPersistenceMapping<
  typeof request,
  typeof series,
  typeof generation,
  typeof continuation,
  typeof rate,
  typeof abuse,
  typeof failure,
  typeof command,
  typeof subject,
  typeof identifier,
  typeof credential,
  string
> = {
  constraints: requiredProofConstraints,
  encodeInstant,
  decodeInstant,
  allocateVersionSync: () => ProofVersion.make(crypto.randomUUID()),
  isRequestConflict: () => false,
  isSeriesConflict: () => false,
  isCommandConflict: () => false,
  scopeKeys: ({ binding }) => ({
    series: Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)))([
      binding.identifier.namespace,
      binding.identifier.value,
      binding._tag === "Identifier" ? binding.identifier.value : binding.revision.subjectId,
    ]),
    identifier: Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)))([
      binding.identifier.namespace,
      binding.identifier.value,
    ]),
    subject: binding._tag === "Identifier" ? binding.identifier.value : binding.revision.subjectId,
  }),
  authority: {
    subjectId,
    subject: subjectMapping,
    credential: credentialMapping,
    identifier: {
      table: identifier,
      namespace: "namespace",
      value: "value",
      isCurrent: (input, rows) =>
        input.binding._tag === "Identifier"
          ? rows.length === 0
          : input.binding._tag === "IdentifierChange"
            ? rows.length === 0
            : rows.length === 0 || rows.every((r) => r.subjectId === input.nativeSubjectId),
    },
  },
  request: {
    table: request,
    moduleId: "moduleId",
    requestId: "requestId",
    fingerprint: "fingerprint",
    proofId: "proofId",
    purpose: "purpose",
    keyId: "keyId",
    createdAt: "createdAt",
    retentionUntil: "retentionUntil",
    encodeInsert: (input) => ({
      moduleId: input.record.moduleId,
      requestId: input.record.requestId,
      fingerprint: input.record.fingerprint,
      proofId: input.record.proofId,
      purpose: input.record.purpose,
      keyId: input.record.verifier.keyId,
      createdAt: encodeInstant(input.record.issuedAtMillis),
      retentionUntil: encodeInstant(input.retentionUntilMillis),
      receipt: Schema.encodeSync(receiptCodec)({
        requestId: input.record.requestId,
        reference: {
          proofId: input.record.proofId,
          purpose: input.record.purpose,
          keyId: input.record.verifier.keyId,
        },
      }),
    }),
    decodeReceipt: (row) =>
      Schema.decodeEffect(receiptCodec)(row.receipt).pipe(
        Effect.mapError(() =>
          DrizzleMappingError.make({
            operation: "travel-auth-codec",
            cause: "invalid consumer value",
          }),
        ),
      ),
  },
  series: {
    table: series,
    moduleId: "moduleId",
    purpose: "purpose",
    scopeKey: "scopeKey",
    activeProofId: "activeProofId",
    lastIssueAt: "lastIssueAt",
    version: "version",
    encodeInsert: (input) => ({ ...input, activeProofId: null, lastIssueAt: null }),
  },
  generation: {
    table: generation,
    moduleId: "moduleId",
    purpose: "purpose",
    proofId: "proofId",
    requestId: "requestId",
    seriesKey: "seriesKey",
    deliveryId: "deliveryId",
    binding: "binding",
    verifierKeyId: "verifierKeyId",
    verifierDigest: "verifierDigest",
    issuedAt: "issuedAt",
    expiresAt: "expiresAt",
    version: "version",
    state: "state",
    sendCount: "sendCount",
    deliveryState: "deliveryState",
    claimVersion: "claimVersion",
    claimDeadline: "claimDeadline",
    retryAt: "retryAt",
    deliveryRetryMillis: "deliveryRetryMillis",
    retentionUntil: "retentionUntil",
    encodeInsert: ({ record, seriesKey, retentionUntilMillis, state, deliveryState, policy }) => ({
      moduleId: record.moduleId,
      purpose: record.purpose,
      proofId: record.proofId,
      requestId: record.requestId,
      seriesKey,
      deliveryId: record.deliveryId,
      binding: Schema.encodeSync(bindingCodec)(record.binding),
      verifierKeyId: record.verifier.keyId,
      verifierDigest: record.verifier.digest,
      issuedAt: encodeInstant(record.issuedAtMillis),
      expiresAt: encodeInstant(record.expiresAtMillis),
      version: record.version,
      state,
      sendCount: 0,
      deliveryState,
      claimVersion: null,
      claimDeadline: null,
      retryAt: null,
      deliveryRetryMillis: policy.deliveryRetryMillis,
      retentionUntil: encodeInstant(retentionUntilMillis),
      fingerprint: record.fingerprint,
    }),
    decodeBinding: (row) => decodeBinding(row.binding),
    decodeRecord: (row) =>
      Effect.gen(function* () {
        return {
          moduleId: row.moduleId,
          purpose: ProofPurpose.make(row.purpose),
          proofId: ProofId.make(row.proofId),
          requestId: ProofRequestId.make(row.requestId),
          fingerprint: TokenDigest.make(row.fingerprint),
          deliveryId: ProofDeliveryId.make(row.deliveryId),
          binding: yield* decodeBinding(row.binding),
          verifier: { keyId: row.verifierKeyId, digest: TokenDigest.make(row.verifierDigest) },
          issuedAtMillis: yield* decodeInstant(row.issuedAt),
          expiresAtMillis: yield* decodeInstant(row.expiresAt),
          version: ProofVersion.make(row.version),
        };
      }),
  },
  continuation: {
    table: continuation,
    moduleId: "moduleId",
    purpose: "purpose",
    continuationId: "continuationId",
    digest: "digest",
    proofId: "proofId",
    seriesKey: "seriesKey",
    binding: "binding",
    expiresAt: "expiresAt",
    consumed: "consumed",
    version: "version",
    retentionUntil: "retentionUntil",
    encodeInsert: (record) => ({
      ...record,
      binding: Schema.encodeSync(bindingCodec)(record.binding),
      expiresAt: encodeInstant(record.expiresAtMillis),
      consumed: false,
      retentionUntil: encodeInstant(record.retentionUntilMillis),
    }),
    decode: (row) =>
      Effect.gen(function* () {
        return {
          moduleId: row.moduleId,
          purpose: ProofPurpose.make(row.purpose),
          continuationId: ProofContinuationId.make(row.continuationId),
          digest: TokenDigest.make(row.digest),
          proofId: ProofId.make(row.proofId),
          seriesKey: row.seriesKey,
          binding: yield* decodeBinding(row.binding),
          expiresAtMillis: yield* decodeInstant(row.expiresAt),
          version: ProofVersion.make(row.version),
        };
      }),
  },
  rateScope: {
    table: rate,
    moduleId: "moduleId",
    purpose: "purpose",
    action: "action",
    scopeKind: "scopeKind",
    scopeKey: "scopeKey",
    encodeInsert: (input) => ({ ...input }),
  },
  abuseEvent: {
    table: abuse,
    moduleId: "moduleId",
    purpose: "purpose",
    action: "action",
    scopeKind: "scopeKind",
    scopeKey: "scopeKey",
    commandId: "commandId",
    occurredAt: "occurredAt",
    retentionUntil: "retentionUntil",
    encodeInsert: (input) => ({
      ...input,
      occurredAt: encodeInstant(input.occurredAtMillis),
      retentionUntil: encodeInstant(input.retentionUntilMillis),
    }),
  },
  failureEvent: {
    table: failure,
    moduleId: "moduleId",
    purpose: "purpose",
    seriesKey: "seriesKey",
    commandId: "commandId",
    occurredAt: "occurredAt",
    retentionUntil: "retentionUntil",
    encodeInsert: (input) => ({
      ...input,
      occurredAt: encodeInstant(input.occurredAtMillis),
      retentionUntil: encodeInstant(input.retentionUntilMillis),
    }),
  },
  command: {
    table: command,
    moduleId: "moduleId",
    commandId: "commandId",
    kind: "kind",
    decision: "decision",
    retentionUntil: "retentionUntil",
    encodeInsert: (input) => ({
      ...input,
      retentionUntil: encodeInstant(input.retentionUntilMillis),
    }),
  },
};
