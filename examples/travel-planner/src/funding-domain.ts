import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/unstable/httpapi";

import { AccountId } from "./auth/account";

export const FundingKind = Schema.Literals(["email", "github", "account"]);
export const FundingTarget = Schema.NonEmptyString.check(Schema.isMaxLength(320));

export const FundingGrant = Schema.Struct({
  version: Schema.Literal(1),
  kind: FundingKind,
  target: FundingTarget,
  label: FundingTarget,
  grantedBy: AccountId,
  grantedAt: Schema.String,
});

export type FundingGrant = typeof FundingGrant.Type;

export const FundingStatus = Schema.Struct({
  admin: Schema.Boolean,
  allowed: Schema.Boolean,
  configured: Schema.Boolean,
});

export const FundingUser = Schema.Struct({
  subjectId: AccountId,
  displayName: Schema.String,
  emails: Schema.Array(Schema.String),
  githubIds: Schema.Array(Schema.String),
  allowed: Schema.Boolean,
  admin: Schema.Boolean,
});

export const FundingDirectory = Schema.Struct({
  users: Schema.Array(FundingUser),
  grants: Schema.Array(FundingGrant),
  next: Schema.NullOr(AccountId),
});

export const GrantFunding = Schema.Struct({ kind: FundingKind, value: FundingTarget });
export const RevokeFunding = Schema.Struct({ kind: FundingKind, target: FundingTarget });

export class FundingError extends Schema.TaggedError<FundingError>()("FundingError", {
  message: Schema.String,
}) {}

export const FundingApi = HttpApi.make("FundingApi").add(
  HttpApiGroup.make("funding").add(
    HttpApiEndpoint.get("status", "/api/funding/status", {
      headers: { "x-elsewhere-account": AccountId },
      success: FundingStatus,
      error: FundingError.pipe(HttpApiSchema.status(400)),
    }),
    HttpApiEndpoint.get("list", "/api/funding/users", {
      headers: { "x-elsewhere-account": AccountId },
      query: { after: Schema.optionalKey(AccountId) },
      success: FundingDirectory,
      error: FundingError.pipe(HttpApiSchema.status(400)),
    }),
    HttpApiEndpoint.post("grant", "/api/funding/grant", {
      headers: { "x-elsewhere-account": AccountId },
      payload: GrantFunding,
      success: FundingGrant,
      error: FundingError.pipe(HttpApiSchema.status(400)),
    }),
    HttpApiEndpoint.post("revoke", "/api/funding/revoke", {
      headers: { "x-elsewhere-account": AccountId },
      payload: RevokeFunding,
      success: Schema.Void,
      error: FundingError.pipe(HttpApiSchema.status(400)),
    }),
  ),
);
