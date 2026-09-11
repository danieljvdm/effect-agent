import * as AuthContract from "@yielded/auth/AuthContract";
import * as Email from "@yielded/auth/Email";
import { HookDenied } from "@yielded/auth/Hooks";
import { IdentityConflict } from "@yielded/auth/Identity";
import * as OAuth from "@yielded/auth/OAuth";
import { RequestBindingFlowId, RequestBindingPublic } from "@yielded/auth/Operations";
import {
  ProofContinuation,
  ProofContinuationId,
  ProofReference,
  ProofRequestId,
  ProofRequestReceipt,
} from "@yielded/auth/Proofs";
import { Email as EmailAddress } from "@yielded/auth/Schema";
import { Schema } from "effect";

// Browser-safe shared contract. No provider configuration or credentials here.
export const Registration = Schema.Struct({
  displayName: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
});

const Failure = Schema.Union([
  OAuth.OAuthRejected,
  OAuth.OAuthUnavailable,
  OAuth.OAuthMethodUnsupported,
  HookDenied,
]);

const EmailFailure = Schema.Union([
  Email.EmailRejected,
  Email.EmailUnavailable,
  Email.EmailActionRequired,
  Email.EmailMethodUnsupported,
  HookDenied,
]);

const emailBase = {
  flowId: RequestBindingFlowId,
  email: Schema.String.check(Schema.isMaxLength(320)).pipe(Schema.decodeTo(EmailAddress)),
};

const emailSignIn = { ...emailBase, returnTarget: Schema.String.check(Schema.isMaxLength(2048)) };
const emailRegistration = { ...emailBase, registration: Registration };

const request = {
  requestId: ProofRequestId,
  locale: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
};

const attempt = {
  reference: ProofReference,
  secret: Schema.RedactedFromValue(Schema.String.check(Schema.isMaxLength(4096))),
};

const verified = Schema.Struct({ continuation: ProofContinuation });

export const LoginApi = AuthContract.make("travel-planner/Auth", {
  claims: Schema.Struct({ displayName: Schema.String }),
  actions: (sessions) => ({
    beginEmailSignIn: AuthContract.action({
      payload: Schema.Struct({ flowId: RequestBindingFlowId }),
      success: RequestBindingPublic,
      error: EmailFailure,
      mode: "mutation",
      credentials: true,
      method: "beginSignIn",
      strategy: "email",
    }),
    requestEmailCode: AuthContract.action({
      payload: Schema.Struct({ ...emailSignIn, ...request }),
      success: ProofRequestReceipt,
      error: EmailFailure,
      mode: "mutation",
      credentials: true,
      method: "signIn",
      strategy: "email",
      requestFields: { requestBinding: "request-binding" },
    }),
    verifyEmailCode: AuthContract.action({
      payload: Schema.Struct({ ...emailSignIn, ...attempt }),
      success: verified,
      error: EmailFailure,
      mode: "mutation",
      credentials: true,
      method: "verifySignIn",
      strategy: "email",
      requestFields: { requestBinding: "request-binding" },
    }),
    completeEmailSignIn: AuthContract.action({
      payload: Schema.Struct({ ...emailSignIn, continuationId: ProofContinuationId }),
      success: Schema.Struct({
        completion: sessions.CompletionResult,
        returnTarget: Email.SafeReturnTarget,
      }),
      error: EmailFailure,
      mode: "mutation",
      credentials: true,
      replay: "single-use",
      method: "completeSignIn",
      strategy: "email",
      requestFields: { requestBinding: "request-binding", credential: "proof-continuation" },
      subject: {
        fromSuccess: (value) =>
          value.completion._tag === "Authenticated"
            ? value.completion.session.subjectId
            : undefined,
      },
    }),
    beginEmailRegistration: AuthContract.action({
      payload: Schema.Struct({ flowId: RequestBindingFlowId }),
      success: RequestBindingPublic,
      error: EmailFailure,
      mode: "mutation",
      credentials: true,
      method: "beginRegistration",
      strategy: "emailRegistration",
    }),
    registerEmail: AuthContract.action({
      payload: Schema.Struct({ ...emailRegistration, ...request }),
      success: ProofRequestReceipt,
      error: EmailFailure,
      mode: "mutation",
      credentials: true,
      method: "register",
      strategy: "emailRegistration",
      requestFields: { requestBinding: "request-binding" },
    }),
    verifyEmailRegistration: AuthContract.action({
      payload: Schema.Struct({ ...emailRegistration, ...attempt }),
      success: verified,
      error: EmailFailure,
      mode: "mutation",
      credentials: true,
      method: "verifyRegistration",
      strategy: "emailRegistration",
      requestFields: { requestBinding: "request-binding" },
    }),
    completeEmailRegistration: AuthContract.action({
      payload: Schema.Struct({
        ...emailRegistration,
        continuationId: ProofContinuationId,
        commandId: Email.EmailCommandId,
      }),
      success: OAuth.OAuthRegistrationResult,
      error: EmailFailure,
      mode: "mutation",
      credentials: true,
      replay: "single-use",
      method: "completeRegistration",
      strategy: "emailRegistration",
      requestFields: { requestBinding: "request-binding", credential: "proof-continuation" },
    }),
    signIn: AuthContract.action({
      payload: OAuth.OAuthSignInBegin,
      success: OAuth.OAuthSignInAuthorization,
      error: Failure,
      mode: "mutation",
      credentials: true,
    }),
    completeSignIn: AuthContract.action({
      payload: Schema.Struct({
        flowId: OAuth.OAuthSignInComplete.fields.flowId,
        provider: OAuth.OAuthSignInComplete.fields.provider,
        callbackId: OAuth.OAuthSignInComplete.fields.callbackId,
        response: OAuth.OAuthCallbackResponse,
      }),
      success: Schema.Union([
        Schema.Struct({
          completion: sessions.CompletionResult,
          returnTarget: OAuth.OAuthReturnTarget,
        }),
        Schema.TaggedStruct("Cancelled", { returnTarget: OAuth.OAuthReturnTarget }),
        OAuth.OAuthRegistrationRequired,
      ]),
      error: Failure,
      mode: "mutation",
      replay: "single-use",
      credentials: true,
      requestFields: { requestBinding: "request-binding" },
      subject: {
        fromSuccess: (value) =>
          "completion" in value && value.completion._tag === "Authenticated"
            ? value.completion.session.subjectId
            : undefined,
      },
    }),
    register: AuthContract.action({
      payload: Schema.Struct({
        reference: OAuth.OAuthRegistrationReference,
        flowId: OAuth.OAuthSignInBegin.fields.flowId,
        commandId: OAuth.OAuthCommandId,
        registration: Registration,
      }),
      success: OAuth.OAuthRegistrationResult,
      error: Schema.Union([Failure, IdentityConflict]),
      mode: "mutation",
      replay: "single-use",
      credentials: true,
      requestFields: { requestBinding: "request-binding", credential: "registration" },
    }),
  }),
});
