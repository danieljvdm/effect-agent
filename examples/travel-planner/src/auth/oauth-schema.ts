import {
  requiredOAuthSignInConstraints,
  requiredOAuthTupleConstraints,
  requiredOAuthRegistrationConstraints,
  type OAuthSignInMapping,
  type OAuthRegistrationIntentMapping,
  type OAuthRegistrationMapping,
} from "@yielded/auth/Drizzle";
import { SecurityRevision } from "@yielded/auth/Sessions";
import { sql } from "drizzle-orm";
import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";
import { Effect, Schema } from "effect";

import { Registration } from "./contract";
import {
  subject,
  credential,
  subjectMapping,
  credentialMapping,
  subjectId,
  mappingError,
} from "./schema";

export const oauthFlow = sqliteTable(
  "auth_oauth_flow",
  {
    moduleId: text(),
    flowId: text(),
    commandId: text(),
    purpose: text(),
    state: text(),
    version: text(),
    stateDigest: text(),
    binderVerifier: text(),
    snapshot: text(),
    claimId: text(),
    generation: integer(),
    binderExpiresAt: integer(),
    issuedAt: integer(),
    expiresAt: integer(),
    claimedAt: integer(),
    claimExpiresAt: integer(),
    retentionUntil: integer(),
  },
  (t) => [
    uniqueIndex("auth_oauth_flow_0").on(t.moduleId, t.flowId),
    uniqueIndex("auth_oauth_flow_1").on(t.moduleId, t.commandId),
    uniqueIndex("auth_oauth_flow_2").on(t.stateDigest),
  ],
);

export const oauthTuple = sqliteTable(
  "auth_oauth_tuple",
  {
    identityKey: text(),
    provider: text(),
    issuer: text(),
    externalSubject: text(),
    state: text(),
    version: text(),
    subjectId: text(),
    reservation: text(),
  },
  (t) => [uniqueIndex("auth_oauth_tuple_0").on(t.identityKey)],
);

export const oauthCredential = sqliteTable(
  "auth_oauth_credential",
  {
    moduleId: text(),
    credentialId: text(),
    subjectId: text(),
    identityKey: text(),
    revision: text(),
    active: integer({ mode: "boolean" }),
  },
  (t) => [
    uniqueIndex("auth_oauth_credential_0").on(t.credentialId),
    uniqueIndex("auth_oauth_credential_1").on(t.identityKey),
  ],
);

export const oauthIntent = sqliteTable(
  "auth_oauth_intent",
  {
    moduleId: text(),
    reference: text(),
    flowId: text(),
    claimId: text(),
    identityKey: text(),
    version: text(),
    state: text(),
    snapshot: text(),
    commandId: text(),
    fingerprint: text(),
    pendingReference: text(),
    expiresAt: integer(),
    retentionUntil: integer(),
  },
  (t) => [
    uniqueIndex("auth_oauth_intent_0").on(t.moduleId, t.reference),
    uniqueIndex("auth_oauth_intent_1").on(t.moduleId, t.flowId),
  ],
);

export const oauthCommand = sqliteTable(
  "auth_oauth_command",
  {
    moduleId: text(),
    commandId: text(),
    reference: text(),
    identityKey: text(),
    fingerprint: text(),
    intentSnapshot: text(),
    applicationSnapshot: text(),
    provisioningIdentity: text(),
    decision: text(),
    retentionUntil: integer(),
  },
  (t) => [uniqueIndex("auth_oauth_command_0").on(t.moduleId, t.commandId)],
);

const clock = {
  encodeInstant: (value: number) => value,
  decodeInstant: Schema.decodeUnknownSync(Schema.Natural),
  engineNowMillis: sql`cast(round((julianday('now') - 2440587.5) * 86400000) as integer)`,
};

const oauthSubject = { ...subjectMapping, activeCondition: sql`${subject.active} = 1` };

const oauthAuthority = {
  ...credentialMapping,
  activeCondition: sql`${credential.active} = 1`,
  encodeInsert: (input: {
    subjectId: string;
    credentialId: string;
    revision: SecurityRevision;
  }) => ({ ...input, active: true }),
};

const oauthCredentialMapping = {
  table: oauthCredential,
  moduleId: "moduleId",
  credentialId: "credentialId",
  subjectId: "subjectId",
  identityKey: "identityKey",
  credentialRevision: "revision",
  status: "active",
  isActiveStatus: (value: unknown) => value === true,
  activeCondition: sql`${oauthCredential.active} = 1`,
  removal: "delete",
  encodeInsert: (input: {
    moduleId: string;
    subjectId: string;
    identityKey: string;
    credentialId: string;
    credentialRevision: SecurityRevision;
  }) => ({
    moduleId: input.moduleId,
    subjectId: input.subjectId,
    identityKey: input.identityKey,
    credentialId: input.credentialId,
    revision: input.credentialRevision,
    active: true,
  }),
} as const;

export const oauthSignInMapping: OAuthSignInMapping<
  typeof subject,
  typeof oauthTuple,
  typeof oauthCredential,
  typeof credential,
  typeof oauthFlow,
  string
> = {
  subject: oauthSubject,
  subjectId,
  authority: oauthAuthority,
  credential: oauthCredentialMapping,
  clock,
  constraints: requiredOAuthSignInConstraints,
  ownership: {
    table: oauthTuple,
    identityKey: "identityKey",
    provider: "provider",
    issuer: "issuer",
    externalSubject: "externalSubject",
    subjectId: "subjectId",
    ownedCondition: sql`${oauthTuple.state} = 'Owned'`,
    decodeSubjectId: (row) => Schema.decodeUnknownSync(Schema.NonEmptyString)(row.subjectId),
  },
  flow: {
    moduleId: "moduleId",
    flowId: "flowId",
    commandId: "commandId",
    purpose: "purpose",
    state: "state",
    version: "version",
    stateDigest: "stateDigest",
    binderVerifier: "binderVerifier",
    snapshot: "snapshot",
    claimId: "claimId",
    generation: "generation",
    binderExpiresAt: "binderExpiresAt",
    issuedAt: "issuedAt",
    expiresAt: "expiresAt",
    claimedAt: "claimedAt",
    claimExpiresAt: "claimExpiresAt",
    retentionUntil: "retentionUntil",
    table: oauthFlow,
    encodeInsert: (input) => ({ ...input }),
  },
};

export const oauthIntentMapping: OAuthRegistrationIntentMapping<
  typeof subject,
  typeof oauthTuple,
  typeof oauthCredential,
  typeof credential,
  typeof oauthFlow,
  typeof oauthTuple,
  typeof oauthIntent,
  string
> = {
  signIn: oauthSignInMapping,
  ownership: {
    mode: "integrated",
    tuple: {
      identityKey: "identityKey",
      provider: "provider",
      issuer: "issuer",
      externalSubject: "externalSubject",
      state: "state",
      version: "version",
      subjectId: "subjectId",
      reservation: "reservation",
      table: oauthTuple,
      encodeInsert: (input) => ({ identityKey: input.identityKey }),
    },
  },
  intent: {
    moduleId: "moduleId",
    reference: "reference",
    flowId: "flowId",
    claimId: "claimId",
    identityKey: "identityKey",
    version: "version",
    state: "state",
    snapshot: "snapshot",
    commandId: "commandId",
    fingerprint: "fingerprint",
    pendingReference: "pendingReference",
    expiresAt: "expiresAt",
    retentionUntil: "retentionUntil",
    table: oauthIntent,
    encodeInsert: () => ({}),
  },
  tupleConstraints: requiredOAuthTupleConstraints,
  registrationConstraints: requiredOAuthRegistrationConstraints,
  // Public registration is immutable application policy. No invitation or first-user admin.
  eligibility: { condition: () => sql`1` },
};

const registrationJson = Schema.fromJsonString(Registration);

export const oauthRegistrationMapping: OAuthRegistrationMapping<
  typeof Registration.Type,
  typeof subject,
  typeof oauthTuple,
  typeof oauthCredential,
  typeof credential,
  typeof oauthTuple,
  typeof oauthIntent,
  typeof oauthCommand,
  string
> = {
  mode: "atomic",
  subject: oauthSubject,
  subjectId,
  authority: oauthAuthority,
  credential: oauthCredentialMapping,
  ownership: oauthIntentMapping.ownership,
  intent: oauthIntentMapping.intent,
  clock,
  tupleConstraints: requiredOAuthTupleConstraints,
  constraints: requiredOAuthRegistrationConstraints,
  command: {
    moduleId: "moduleId",
    commandId: "commandId",
    reference: "reference",
    identityKey: "identityKey",
    fingerprint: "fingerprint",
    intentSnapshot: "intentSnapshot",
    applicationSnapshot: "applicationSnapshot",
    provisioningIdentity: "provisioningIdentity",
    decision: "decision",
    retentionUntil: "retentionUntil",
    table: oauthCommand,
    encodeInsert: () => ({}),
  },
  inspect: ({ registration }) =>
    Effect.succeed({ fingerprint: registration.displayName, eligible: true }),
  inspectSync: ({ registration }) => ({ fingerprint: registration.displayName, eligible: true }),
  snapshot: (registration) =>
    Schema.decodeEffect(registrationJson)(Schema.encodeSync(registrationJson)(registration)).pipe(
      Effect.mapError(mappingError),
    ),
  snapshotSync: (registration) =>
    Schema.decodeSync(registrationJson)(Schema.encodeSync(registrationJson)(registration)),
  application: {
    encode: Schema.encodeSync(registrationJson),
    decode: Schema.decodeSync(registrationJson),
  },
  eligibility: { admission: () => sql`1`, postcondition: () => sql`1` },
  allocateProvisioningIdentity: Effect.sync(() => crypto.randomUUID()),
  allocateSubjectId: Effect.sync(() => crypto.randomUUID()),
  allocateCredentialId: Effect.sync(() => crypto.randomUUID()),
  allocateRevision: Effect.sync(() => SecurityRevision.make(crypto.randomUUID())),
  retentionMillis: 3_600_000,
  encodeSubjectInsert: ({ registration }, ids) => ({
    id: ids.subjectId,
    revision: ids.securityRevision,
    active: true,
    displayName: registration.displayName,
  }),
};
