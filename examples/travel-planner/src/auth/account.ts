import { Schema } from "effect";

/** Server-owned local identity. Provider email and display names never authorize storage. */
export const AccountId = Schema.String.check(
  Schema.isPattern(/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/),
);

export const AccountSession = Schema.Struct({ subjectId: AccountId, displayName: Schema.String });
export type AccountSession = typeof AccountSession.Type;

export class AccountError extends Schema.TaggedError<AccountError>()("AccountError", {
  code: Schema.Literals(["unauthorized", "unavailable"]),
  message: Schema.String,
}) {}
