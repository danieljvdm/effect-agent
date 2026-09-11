import { Context, Effect, Schema } from "effect";

export class AuthStorageError extends Schema.TaggedError<AuthStorageError>()("AuthStorageError", {
  cause: Schema.Defect(),
}) {}

/** Application-owned schema initialization surrounds one atomic transaction. */
export const AuthStorageFailpoint = Context.Reference<{
  readonly hit: (point: "schema:before" | "schema:after") => Effect.Effect<void, AuthStorageError>;
}>("travel-planner/AuthStorageFailpoint", { defaultValue: () => ({ hit: () => Effect.void }) });

// This fresh-start database has one format. Unknown versions fail without writes.
const statements = [
  'create table "auth_subject" ("id" text primary key, "active" integer not null, "revision" text not null, "displayName" text not null)',
  'create table "auth_identifier" ("namespace" text not null, "value" text not null, "subjectId" text not null, "verifiedAt" integer not null, "revision" text not null, constraint "auth_identifier_unique" unique ("namespace", "value"))',
  'create table "auth_credential" ("subjectId" text not null, "credentialId" text not null, "revision" text not null, "active" integer not null, constraint "auth_credential_unique" unique ("subjectId", "credentialId"))',
  'create table "auth_email_credential" ("moduleId" text not null, "subjectId" text not null, "credentialId" text not null, "namespace" text not null, "value" text not null, "revision" text not null, "active" integer not null, constraint "auth_email_id_unique" unique ("moduleId", "credentialId"), constraint "auth_email_identifier_unique" unique ("moduleId", "namespace", "value"))',
  'create table "auth_email_registration" ("moduleId" text not null, "commandId" text not null, "fingerprint" text not null, "state" text not null, "subjectId" text, "pendingReference" text, "retentionUntil" integer not null, constraint "auth_email_registration_unique" unique ("moduleId", "commandId"), constraint "auth_email_pending_unique" unique ("pendingReference"))',
  'create table "auth_proof_request" ("moduleId" text not null, "requestId" text not null, "fingerprint" text not null, "proofId" text not null, "purpose" text not null, "keyId" text not null, "createdAt" integer not null, "retentionUntil" integer not null, "receipt" text not null, constraint "auth_request_unique" unique ("moduleId", "requestId"))',
  'create table "auth_proof_series" ("moduleId" text not null, "purpose" text not null, "scopeKey" text not null, "activeProofId" text, "lastIssueAt" integer, "version" text not null, constraint "auth_series_unique" unique ("moduleId", "purpose", "scopeKey"))',
  'create table "auth_proof_generation" ("moduleId" text not null, "purpose" text not null, "proofId" text not null, "requestId" text not null, "seriesKey" text not null, "deliveryId" text not null, "binding" text not null, "verifierKeyId" text not null, "verifierDigest" text not null, "issuedAt" integer not null, "expiresAt" integer not null, "version" text not null, "state" text not null, "sendCount" integer not null, "deliveryState" text not null, "claimVersion" text, "claimDeadline" integer, "retryAt" integer, "deliveryRetryMillis" integer not null, "retentionUntil" integer not null, "fingerprint" text not null, constraint "auth_generation_unique" unique ("moduleId", "proofId"), constraint "auth_delivery_unique" unique ("moduleId", "deliveryId"))',
  'create table "auth_proof_continuation" ("moduleId" text not null, "purpose" text not null, "continuationId" text not null, "digest" text not null, "proofId" text not null, "seriesKey" text not null, "binding" text not null, "expiresAt" integer not null, "consumed" integer not null, "version" text not null, "retentionUntil" integer not null, constraint "auth_continuation_unique" unique ("moduleId", "continuationId"), constraint "auth_continuation_digest" unique ("moduleId", "digest"))',
  'create table "auth_proof_rate" ("moduleId" text not null, "purpose" text not null, "action" text not null, "scopeKind" text not null, "scopeKey" text not null, constraint "auth_rate_unique" unique ("moduleId", "purpose", "action", "scopeKind", "scopeKey"))',
  'create table "auth_proof_abuse" ("moduleId" text not null, "purpose" text not null, "action" text not null, "scopeKind" text not null, "scopeKey" text not null, "commandId" text not null, "occurredAt" integer not null, "retentionUntil" integer not null, constraint "auth_abuse_unique" unique ("moduleId", "action", "scopeKind", "scopeKey", "commandId"))',
  'create table "auth_proof_failure" ("moduleId" text not null, "purpose" text not null, "seriesKey" text not null, "commandId" text not null, "occurredAt" integer not null, "retentionUntil" integer not null, constraint "auth_failure_unique" unique ("moduleId", "seriesKey", "commandId"))',
  'create table "auth_proof_command" ("moduleId" text not null, "commandId" text not null, "kind" text not null, "decision" text not null, "retentionUntil" integer not null, constraint "auth_command_unique" unique ("moduleId", "commandId"))',
  'create table "auth_session" ("id" text primary key, "subjectId" text not null, "digest" text not null, "version" text not null, "revision" text not null, "issuedAt" integer not null, "expiresAt" integer not null, "absoluteExpiresAt" integer not null, "record" text not null, constraint "auth_session_digest_unique" unique ("digest"))',
  'create table "auth_session_flow" ("flowId" text primary key, "subjectId" text not null, "state" text not null, "pendingDigest" text, "dedupUntil" integer not null)',
  'create table "auth_oauth_flow" ("moduleId" text, "flowId" text, "commandId" text, "purpose" text, "state" text, "version" text, "stateDigest" text, "binderVerifier" text, "snapshot" text, "claimId" text, "generation" integer, "binderExpiresAt" integer, "issuedAt" integer, "expiresAt" integer, "claimedAt" integer, "claimExpiresAt" integer, "retentionUntil" integer, constraint "auth_oauth_flow_0" unique ("moduleId", "flowId"), constraint "auth_oauth_flow_1" unique ("moduleId", "commandId"), constraint "auth_oauth_flow_2" unique ("stateDigest"))',
  'create table "auth_oauth_tuple" ("identityKey" text, "provider" text, "issuer" text, "externalSubject" text, "state" text, "version" text, "subjectId" text, "reservation" text, constraint "auth_oauth_tuple_0" unique ("identityKey"))',
  'create table "auth_oauth_credential" ("moduleId" text, "credentialId" text, "subjectId" text, "identityKey" text, "revision" text, "active" integer, constraint "auth_oauth_credential_0" unique ("credentialId"), constraint "auth_oauth_credential_1" unique ("identityKey"))',
  'create table "auth_oauth_intent" ("moduleId" text, "reference" text, "flowId" text, "claimId" text, "identityKey" text, "version" text, "state" text, "snapshot" text, "commandId" text, "fingerprint" text, "pendingReference" text, "expiresAt" integer, "retentionUntil" integer, constraint "auth_oauth_intent_0" unique ("moduleId", "reference"), constraint "auth_oauth_intent_1" unique ("moduleId", "flowId"))',
  'create table "auth_oauth_command" ("moduleId" text, "commandId" text, "reference" text, "identityKey" text, "fingerprint" text, "intentSnapshot" text, "applicationSnapshot" text, "provisioningIdentity" text, "decision" text, "retentionUntil" integer, constraint "auth_oauth_command_0" unique ("moduleId", "commandId"))',
];

export const initializeAuthStorage = (storage: DurableObjectStorage) =>
  Effect.gen(function* () {
    const failpoint = yield* AuthStorageFailpoint;

    yield* failpoint.hit("schema:before");
    yield* Effect.try({
      try: () =>
        storage.transactionSync(() => {
          const tables = storage.sql
            .exec("select name from sqlite_master where type='table' and name like 'auth_%'")
            .toArray();

          if (tables.length !== 0) {
            const version = Schema.decodeUnknownSync(
              Schema.Array(Schema.Struct({ version: Schema.Int })),
            )(storage.sql.exec("select version from auth_format").toArray());

            if (version.length === 1 && version[0]?.version === 1) return;
            throw new AuthStorageError({ cause: "Unsupported auth format" });
          }
          for (const statement of statements) storage.sql.exec(statement);
          storage.sql.exec("create table auth_format (version integer not null)");
          storage.sql.exec("insert into auth_format values (1)");
        }),
      catch: (cause) => new AuthStorageError({ cause }),
    });
    yield* failpoint.hit("schema:after");
  });
