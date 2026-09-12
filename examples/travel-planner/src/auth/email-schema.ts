import {
  requiredEmailRegistrationConstraints,
  requiredEmailSignInConstraints,
  type EmailRegistrationMapping,
  type EmailSignInMapping,
} from "@yielded/auth/Drizzle";
import { EmailCredentialSnapshot } from "@yielded/auth/Email";
import { TokenDigest } from "@yielded/auth/Schema";
import { SecurityRevision } from "@yielded/auth/Sessions";
import { Effect, Schema } from "effect";

import { Registration } from "./contract";
import type { subject, credential } from "./schema";
import {
  identifier,
  emailCredential,
  emailRegistration,
  subjectMapping,
  subjectId,
  encodeInstant,
  decodeMillis,
  mappingError,
  credentialMapping,
} from "./schema";

const identifierMapping = {
  table: identifier,
  namespace: "namespace",
  value: "value",
  subjectId: "subjectId",
  verifiedAt: "verifiedAt",
  bindingRevision: "revision",
  isCurrent: () => true,
  encodeVerifiedInsert: (input: {
    readonly identifier: { readonly namespace: string; readonly value: string };
    readonly subjectId: string;
    readonly verifiedAtMillis: number;
    readonly bindingRevision: SecurityRevision;
  }) => ({
    ...input.identifier,
    subjectId: input.subjectId,
    verifiedAt: input.verifiedAtMillis,
    revision: input.bindingRevision,
  }),
} as const;

const emailCredentialMapping = {
  table: emailCredential,
  moduleId: "moduleId",
  subjectId: "subjectId",
  credentialId: "credentialId",
  identifierNamespace: "namespace",
  identifierValue: "value",
  credentialRevision: "revision",
  status: "active",
  isActiveStatus: (value: unknown) => value === true,
  encodeVerifiedInsert: (input: {
    readonly moduleId: string;
    readonly subjectId: string;
    readonly credentialId: string;
    readonly identifier: { readonly namespace: string; readonly value: string };
    readonly credentialRevision: SecurityRevision;
  }) => ({
    moduleId: input.moduleId,
    subjectId: input.subjectId,
    credentialId: input.credentialId,
    ...input.identifier,
    revision: input.credentialRevision,
    active: true,
  }),
} as const;

export const emailSignInMapping: EmailSignInMapping<
  typeof subject,
  typeof identifier,
  typeof emailCredential,
  string
> = {
  subject: subjectMapping,
  subjectId,
  identifier: identifierMapping,
  credential: {
    ...emailCredentialMapping,
    decode: ({ moduleId, subject: account, identifier: address, credential: factor }) =>
      Schema.decodeEffect(EmailCredentialSnapshot)({
        moduleId,
        identifier: { namespace: address.namespace, value: address.value },
        identifierRevision: address.revision,
        verifiedAtMillis: address.verifiedAt,
        credentialId: factor.credentialId,
        credentialRevision: factor.revision,
        revision: {
          subjectId: account.id,
          securityRevision: account.revision,
          credentials: [{ credentialId: factor.credentialId, revision: factor.revision }],
        },
      }).pipe(Effect.mapError(mappingError)),
  },
  constraints: requiredEmailSignInConstraints,
  decodeInstant: decodeMillis,
};

const registrationJson = Schema.fromJsonString(Registration);

const fingerprint = Schema.encodeSync(
  Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String, Registration])),
);

export const emailRegistrationMapping: EmailRegistrationMapping<
  typeof Registration.Type,
  typeof subject,
  typeof identifier,
  typeof emailCredential,
  typeof credential,
  typeof emailRegistration,
  string
> = {
  mode: "atomic",
  subject: subjectMapping,
  subjectId,
  identifier: identifierMapping,
  credential: emailCredentialMapping,
  authorityCredential: {
    ...credentialMapping,
    encodeInsert: (input) => ({ ...input, active: true }),
  },
  constraints: requiredEmailRegistrationConstraints,
  registration: {
    table: emailRegistration,
    moduleId: "moduleId",
    commandId: "commandId",
    fingerprint: "fingerprint",
    state: "state",
    subjectId: "subjectId",
    pendingReference: "pendingReference",
    retentionUntil: "retentionUntil",
    encodeInsert: (input, state) => ({
      moduleId: input.moduleId,
      commandId: input.commandId,
      fingerprint: input.fingerprint,
      state: state.state,
      subjectId: state.nativeSubjectId ?? null,
      pendingReference: state.pendingReference ?? null,
      retentionUntil: state.retentionUntilMillis,
    }),
    // A consumed registration never establishes a session, including command replay.
    decodeReplay: () => Effect.succeed({ _tag: "Rejected" }),
  },
  inspect: (input) =>
    Effect.succeed({
      fingerprint: TokenDigest.make(
        fingerprint([input.identifier.namespace, input.identifier.value, input.registration]),
      ),
      eligible: true,
    }),
  inspectSync: (input) => ({
    fingerprint: TokenDigest.make(
      fingerprint([input.identifier.namespace, input.identifier.value, input.registration]),
    ),
    eligible: true,
  }),
  snapshotRegistration: (value) =>
    Schema.decodeEffect(registrationJson)(Schema.encodeSync(registrationJson)(value)).pipe(
      Effect.mapError(mappingError),
    ),
  snapshotRegistrationSync: (value) =>
    Schema.decodeSync(registrationJson)(Schema.encodeSync(registrationJson)(value)),
  allocatePendingReferenceSync: () => crypto.randomUUID(),
  allocateCredentialIdSync: () => crypto.randomUUID(),
  allocateRevisionSync: () => SecurityRevision.make(crypto.randomUUID()),
  encodeInstant,
  retentionMillis: 3_600_000,
  isRequestConflict: () => false,
  isIdentifierConflict: () => false,
  isCredentialConflict: () => false,
  provisioning: {
    idMode: "synchronous",
    allocateSubjectIdSync: () => crypto.randomUUID(),
    encodeSubjectInsert: (input, values) => ({
      id: values.nativeSubjectId!,
      active: true,
      revision: values.securityRevision,
      displayName: input.registration.displayName,
    }),
  },
};
