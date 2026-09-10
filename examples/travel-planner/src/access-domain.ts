import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";

export const adminEmail = "danieljmerwe@gmail.com";

export const Email = Schema.String.check(
  Schema.isMaxLength(254),
  Schema.isPattern(/^[^\s@]+@[^\s@]+\.[^\s@]+$/),
);

export const AccessSession = Schema.Struct({ email: Email, isAdmin: Schema.Boolean });
export type AccessSession = typeof AccessSession.Type;
export const AccessMembers = Schema.Struct({ emails: Schema.Array(Email), adminEmail: Email });
export type AccessMembers = typeof AccessMembers.Type;

export class AccessError extends Schema.TaggedError<AccessError>()("AccessError", {
  code: Schema.Literals(["unauthorized", "forbidden", "invalid", "unavailable"]),
  message: Schema.String,
}) {}

export const AccessRpcs = RpcGroup.make(
  Rpc.make("GetSession", { success: AccessSession, error: AccessError }),
  Rpc.make("GetMembers", { success: AccessMembers, error: AccessError }),
  Rpc.make("InviteMember", {
    payload: Schema.Struct({ email: Email }),
    success: AccessMembers,
    error: AccessError,
  }),
  Rpc.make("RemoveMember", {
    payload: Schema.Struct({ email: Email }),
    success: AccessMembers,
    error: AccessError,
  }),
);
