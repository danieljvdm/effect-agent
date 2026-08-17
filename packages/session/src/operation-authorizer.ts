import {
  AgentId,
  ConversationId,
  DelegationId,
  RunId,
  SubmissionId,
  ToolCallId,
} from "@effect-agent/core";
import { Context, Effect, Layer, Schema } from "effect";

import { ChildReservationId, Principal } from "./ledger.ts";
import { DefinitionDigests, Digest } from "./records.ts";

/** Authenticated identity supplied explicitly at every protected durable-runtime boundary. */
export class OperationCaller extends Schema.Class<OperationCaller>(
  "@effect-agent/session/OperationCaller",
)({
  principal: Principal,
}) {}

/**
 * Administrative, observation, and lifecycle operations submitted to current-policy
 * authorization (SEC-003/SEC-011). A Receipt or durable identifier is never authority.
 */
export const AuthorizedOperation = Schema.Literals([
  "awaitSettlement",
  "observe",
  "abort",
  "explain",
  "verify",
  "retry",
  "wake",
  "scanObligations",
  "resolveUnknown",
  "resolveApproval",
]);
export type AuthorizedOperation = typeof AuthorizedOperation.Type;

/** One explicit caller-bearing authorization question. */
export class OperationAuthorizationRequest extends Schema.Class<OperationAuthorizationRequest>(
  "@effect-agent/session/OperationAuthorizationRequest",
)({
  operation: AuthorizedOperation,
  principal: Principal,
  conversationId: Schema.optionalKey(ConversationId),
  submissionId: Schema.optionalKey(SubmissionId),
}) {}

/** A typed, fail-closed authorization denial (never a defect, never silent). */
export class OperationDenied extends Schema.TaggedError<OperationDenied>()("OperationDenied", {
  operation: AuthorizedOperation,
  principal: Principal,
  reason: Schema.String.check(Schema.isMaxLength(4_096)),
  conversationId: Schema.optionalKey(ConversationId),
  submissionId: Schema.optionalKey(SubmissionId),
}) {}

export interface OperationAuthorizerService {
  readonly authorize: (
    request: OperationAuthorizationRequest,
  ) => Effect.Effect<void, OperationDenied>;
}

/** Required host authorization port. There is deliberately no ambient allow default. */
export class OperationAuthorizer extends Context.Service<
  OperationAuthorizer,
  OperationAuthorizerService
>()("@effect-agent/session/OperationAuthorizer") {}

/** Explicit service-possession policy for trusted local programs and deterministic tests. */
export const possessionOperationAuthorizer: OperationAuthorizerService = {
  authorize: () => Effect.void,
};

export const operationAuthorizerLayer = (
  service: OperationAuthorizerService,
): Layer.Layer<OperationAuthorizer> => Layer.succeed(OperationAuthorizer)(service);

export const possessionOperationAuthorizerLayer: Layer.Layer<OperationAuthorizer> =
  operationAuthorizerLayer(possessionOperationAuthorizer);

/**
 * Current-policy question asked immediately before each durable child establishment attempt. The
 * opaque grant digest remains capabilities-owned; session binds it without interpreting it.
 */
export class ChildAdmissionAuthorizationRequest extends Schema.Class<ChildAdmissionAuthorizationRequest>(
  "@effect-agent/session/ChildAdmissionAuthorizationRequest",
)({
  principal: Principal,
  parentSubmissionId: SubmissionId,
  parentConversationId: ConversationId,
  parentRunId: RunId,
  parentToolCallId: ToolCallId,
  delegationId: DelegationId,
  childConversationId: ConversationId,
  childPrincipal: Principal,
  targetAgentId: AgentId,
  targetDigests: DefinitionDigests,
  childInputDigest: Digest,
  grantDigest: Digest,
  reservationId: ChildReservationId,
  reservationDigest: Digest,
}) {}

/** Current policy denied this exact child admission/establishment attempt. */
export class ChildAdmissionDenied extends Schema.TaggedError<ChildAdmissionDenied>()(
  "ChildAdmissionDenied",
  {
    principal: Principal,
    parentSubmissionId: SubmissionId,
    parentToolCallId: ToolCallId,
    childConversationId: ConversationId,
    targetAgentId: AgentId,
    reason: Schema.String.check(Schema.isMaxLength(4_096)),
  },
) {}

export interface ChildAdmissionAuthorizerService {
  readonly authorize: (
    request: ChildAdmissionAuthorizationRequest,
  ) => Effect.Effect<void, ChildAdmissionDenied>;
}

/** Narrow required authority for durable child admission; it grants no later child action. */
export class ChildAdmissionAuthorizer extends Context.Service<
  ChildAdmissionAuthorizer,
  ChildAdmissionAuthorizerService
>()("@effect-agent/session/ChildAdmissionAuthorizer") {}

export const childAdmissionAuthorizerLayer = (
  service: ChildAdmissionAuthorizerService,
): Layer.Layer<ChildAdmissionAuthorizer> => Layer.succeed(ChildAdmissionAuthorizer)(service);

/** Explicit trusted-local/test substitute; production hosts should provide current policy. */
export const possessionChildAdmissionAuthorizer: ChildAdmissionAuthorizerService = {
  authorize: () => Effect.void,
};

export const possessionChildAdmissionAuthorizerLayer: Layer.Layer<ChildAdmissionAuthorizer> =
  childAdmissionAuthorizerLayer(possessionChildAdmissionAuthorizer);
