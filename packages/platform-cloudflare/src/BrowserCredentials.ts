import { Context, type Effect, Schema } from "effect";

const Selector = Schema.NonEmptyString.check(Schema.isMaxLength(2048));

export const CredentialKind = Schema.Literals(["login", "card"]);

export const CredentialFieldRole = Schema.Literals([
  "username",
  "password",
  "card-name",
  "card-number",
  "card-expiry",
  "card-expiry-month",
  "card-expiry-year",
  "card-security-code",
]);

/** Canonical HTTPS origins, including non-default ports. Grants must match exactly. */
export const CredentialOrigin = Schema.String.check(
  Schema.isMaxLength(2048),
  Schema.makeFilter((value) => {
    try {
      const url = new URL(value);

      return url.protocol === "https:" && url.origin === value && !url.username && !url.password;
    } catch {
      return false;
    }
  }),
);

export class CredentialTarget extends Schema.Class<CredentialTarget>("CredentialTarget")({
  topOrigin: CredentialOrigin,
  frameOrigin: CredentialOrigin,
  recipientOrigin: CredentialOrigin,
}) {}

/** Selectors identify one native form. Roles explicitly select material; no field inference occurs. */
export class FillCredentialRequest extends Schema.Class<FillCredentialRequest>(
  "FillCredentialRequest",
)({
  credential: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  kind: CredentialKind,
  frame: Schema.optionalKey(Schema.Array(Selector).check(Schema.isMaxLength(8))),
  fields: Schema.Array(Schema.Struct({ selector: Selector, role: CredentialFieldRole })).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(8),
  ),
}) {}

const SecretText = Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(1024)));

/** Host-only material. Never return or encode it through a Tool, log, trace, or checkpoint. */
export const LoginCredential = Schema.TaggedStruct("LoginCredential", {
  username: SecretText,
  password: SecretText,
});

/** Security codes are transient; the host must not persist them. */
export const CardCredential = Schema.TaggedStruct("CardCredential", {
  name: SecretText,
  number: Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{12,19}$/))),
  expiry: SecretText,
  expiryMonth: Schema.Redacted(Schema.String.check(Schema.isPattern(/^(0[1-9]|1[0-2])$/))),
  expiryYear: Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{4}$/))),
  securityCode: Schema.optionalKey(
    Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{3,4}$/))),
  ),
});

export const BrowserCredentialMaterial = Schema.Union([LoginCredential, CardCredential]);
export type BrowserCredentialMaterial = typeof BrowserCredentialMaterial.Type;

export class CredentialAccessError extends Schema.TaggedError<CredentialAccessError>()(
  "CredentialAccessError",
  {
    reason: Schema.Literals([
      "denied",
      "missing-credential",
      "needs-attention",
      "busy",
      "resolver",
    ]),
  },
) {}

export interface CredentialAccessRequest {
  readonly credential: string;
  readonly kind: typeof CredentialKind.Type;
  readonly target: CredentialTarget;
  readonly roles: ReadonlyArray<typeof CredentialFieldRole.Type>;
}

/**
 * Invocation-owned authority. Derive caller/ownership from the host, not the public credential
 * identifier. Authorize the actual merchant, frame, recipient, roles, and side effects of filling.
 * Repeated checks must consult current grants. Never log or trace resolved material.
 * Bind caller identity and identifier-to-vault-item selection for the whole invocation; an alias
 * changing between resolve and authorize must never authorize different material.
 */
export class BrowserCredentialAccess extends Context.Service<
  BrowserCredentialAccess,
  {
    readonly authorize: (
      request: CredentialAccessRequest,
    ) => Effect.Effect<void, CredentialAccessError>;
    readonly resolve: (
      request: CredentialAccessRequest,
    ) => Effect.Effect<BrowserCredentialMaterial, CredentialAccessError>;
  }
>()("@effect-agent/platform-cloudflare/BrowserCredentialAccess") {}

export const CredentialDispatch = Schema.Literals([
  "not-dispatched",
  "possibly-dispatched",
  "dispatched",
]);

/** Filled counts acknowledged assignments, not authentication, submission, or website acceptance. */
export class CredentialFillResult extends Schema.Class<CredentialFillResult>(
  "CredentialFillResult",
)({
  dispatch: Schema.Literal("dispatched"),
  filled: Schema.Natural.check(Schema.isLessThanOrEqualTo(8)),
}) {}

/** Content-free evidence. A lost reply never authorizes a retry or proves that no write occurred. */
export class CredentialFillError extends Schema.TaggedError<CredentialFillError>()(
  "CredentialFillError",
  {
    reason: Schema.Literals([
      "denied",
      "missing-credential",
      "stale-target",
      "unsupported",
      "needs-attention",
      "busy",
      "provider",
      "resolver",
      "timeout",
    ]),
    dispatch: CredentialDispatch,
    filled: Schema.Natural.check(Schema.isLessThanOrEqualTo(8)),
    cleanup: Schema.Literals(["not-requested", "confirmed", "unconfirmed"]),
  },
) {}
