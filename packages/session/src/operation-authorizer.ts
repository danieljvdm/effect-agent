import { ConversationId, SubmissionId } from "@effect-agent/core";
import { Context, Effect, Layer, Schema } from "effect";

import { Principal } from "./ledger.ts";

/**
 * Administrative/observation operations submitted to authorization (P7 WP1; SEC-003/SEC-011,
 * DUR-017 "authenticated per-read authorization"). The union covers every operation the durable
 * runtime consults the authorizer for: canonical observation, the administrative surface, and
 * the two durable resolution paths.
 */
export const AuthorizedOperation = Schema.Literals([
  "observe",
  "explain",
  "verify",
  "retry",
  "wake",
  "scanObligations",
  "resolveUnknown",
  "resolveApproval",
]);
export type AuthorizedOperation = typeof AuthorizedOperation.Type;

/**
 * One authorization question. `principal` is present only when the host authenticated a caller
 * identity and threaded it through — the framework never parses bearer tokens (SEC-001); absent
 * identity under the default authorizer keeps the pre-P7 service-possession behavior.
 */
export class OperationAuthorizationRequest extends Schema.Class<OperationAuthorizationRequest>(
  "@effect-agent/session/OperationAuthorizationRequest",
)({
  operation: AuthorizedOperation,
  conversationId: Schema.optionalKey(ConversationId),
  submissionId: Schema.optionalKey(SubmissionId),
  principal: Schema.optionalKey(Principal),
}) {}

/** A typed, fail-closed authorization denial (never a defect, never silent). */
export class OperationDenied extends Schema.TaggedError<OperationDenied>()("OperationDenied", {
  operation: AuthorizedOperation,
  reason: Schema.String.check(Schema.isMaxLength(4_096)),
  conversationId: Schema.optionalKey(ConversationId),
  submissionId: Schema.optionalKey(SubmissionId),
}) {}

/**
 * The minimal authorization port consulted by `observe`, the administrative operations, and the
 * `resolveUnknown`/`resolveApproval` resolution paths. `authorize` either succeeds (allow) or
 * fails with the typed `OperationDenied` (deny) — the runtime propagates the denial fail-closed
 * and performs no reads or writes for the denied operation.
 */
export interface OperationAuthorizerService {
  readonly authorize: (
    request: OperationAuthorizationRequest,
  ) => Effect.Effect<void, OperationDenied>;
}

/**
 * The default possession-behavior authorizer: possession of the `DurableAgentRuntime` service
 * (plus each mutating command's mandatory author/reason audit fields) IS the authorization —
 * exactly the pre-P7 boundary, unchanged. Hosts that authenticate callers substitute a real
 * decision procedure through `operationAuthorizerLayer`; the framework enforces the decision
 * fail-closed without inventing an identity system (D10/DUR-017 stance, plan §3).
 */
export const possessionOperationAuthorizer: OperationAuthorizerService = {
  authorize: () => Effect.void,
};

/**
 * Context reference with the possession default, mirroring `DurableApprovalResolver`: the
 * coordinator consults it unconditionally, and only a host-supplied non-default Layer changes
 * the answer. References resolve at Layer construction, so provide the override when building
 * `DurableAgentRuntime.layer` (e.g. `Layer.provide(operationAuthorizerLayer(...))`).
 */
export const OperationAuthorizer: Context.Reference<OperationAuthorizerService> =
  Context.Reference<OperationAuthorizerService>("@effect-agent/session/OperationAuthorizer", {
    defaultValue: () => possessionOperationAuthorizer,
  });

/** Layer carrying a host-supplied authorization decision procedure. */
export const operationAuthorizerLayer = (service: OperationAuthorizerService): Layer.Layer<never> =>
  Layer.succeedContext(Context.make(OperationAuthorizer, service));
