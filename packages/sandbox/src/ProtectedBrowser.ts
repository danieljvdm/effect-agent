import { Context, Effect, Layer, Schema, Scope, type Redacted } from "effect";

import { InteractiveBrowserHost, type InteractiveBrowserPolicy } from "./InteractiveBrowser.ts";

/** Canonical HTTPS origin, including a non-default port. Host grants match exactly. */
export const CredentialOrigin = Schema.String.check(
  Schema.makeFilter(
    (value) => value.startsWith("https://") && Schema.is(InteractiveBrowserHost)(value.slice(8)),
  ),
);

export const BrowserReference = Schema.String.check(Schema.isUUID());
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

const Label = Schema.String.check(Schema.isMaxLength(200));

export class CredentialTarget extends Schema.Class<CredentialTarget>("CredentialTarget")({
  topOrigin: CredentialOrigin,
  frameOrigin: CredentialOrigin,
  recipientOrigin: CredentialOrigin,
  document: BrowserReference,
  frame: BrowserReference,
  form: BrowserReference,
}) {}

export class ProtectedBrowserControl extends Schema.Class<ProtectedBrowserControl>(
  "ProtectedBrowserControl",
)({
  ref: BrowserReference,
  target: CredentialTarget,
  role: Schema.Union([
    CredentialFieldRole,
    Schema.Literals(["submit", "link", "button", "unsupported"]),
  ]),
  label: Label,
}) {}

export class ProtectedBrowserObservation extends Schema.Class<ProtectedBrowserObservation>(
  "ProtectedBrowserObservation",
)({
  document: BrowserReference,
  topOrigin: CredentialOrigin,
  text: Schema.String.check(Schema.isMaxLength(64 * 1024)),
  controls: Schema.Array(ProtectedBrowserControl).check(Schema.isMaxLength(64)),
  truncated: Schema.Boolean,
  observation: Schema.Literals(["before-exposure", "approved-after-exposure"]),
}) {}

export class CredentialOfferMetadata extends Schema.Class<CredentialOfferMetadata>(
  "CredentialOfferMetadata",
)({
  label: Label,
  brand: Schema.optionalKey(Label),
  lastFour: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^\d{4}$/))),
}) {}

export class CredentialOffer extends Schema.Class<CredentialOffer>("CredentialOffer")({
  ref: BrowserReference,
  kind: CredentialKind,
  metadata: CredentialOfferMetadata,
}) {}

export class ListCredentialOffers extends Schema.Class<ListCredentialOffers>(
  "ListCredentialOffers",
)({
  kind: CredentialKind,
  target: BrowserReference,
}) {}

export class UseCredential extends Schema.Class<UseCredential>("UseCredential")({
  offer: BrowserReference,
  fields: Schema.Array(Schema.Struct({ ref: BrowserReference, role: CredentialFieldRole })).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(8),
  ),
  submit: Schema.optionalKey(BrowserReference),
}) {}

export class ProtectedBrowserNavigate extends Schema.Class<ProtectedBrowserNavigate>(
  "ProtectedBrowserNavigate",
)({
  url: Schema.String.check(Schema.isMaxLength(8192)),
}) {}

export class ProtectedBrowserClick extends Schema.Class<ProtectedBrowserClick>(
  "ProtectedBrowserClick",
)({
  ref: BrowserReference,
}) {}

export const CredentialDispatch = Schema.Literals([
  "not-dispatched",
  "possibly-dispatched",
  "dispatched",
]);

export const CredentialMilestone = Schema.Literals([
  "none",
  "partial-fill",
  "filled",
  "submission-dispatched",
]);

export const ProtectedObservationState = Schema.Literals([
  "before-exposure",
  "protected",
  "approved-after-exposure",
  "closed",
]);

export const ProtectedCleanup = Schema.Literals(["not-requested", "confirmed", "unconfirmed"]);

const Evidence = {
  dispatch: CredentialDispatch,
  milestone: CredentialMilestone,
  observation: ProtectedObservationState,
  cleanup: ProtectedCleanup,
};

/** No foreign message/cause or page data may enter this failure. Closure does not undo dispatch. */
export class ProtectedBrowserError extends Schema.TaggedError<ProtectedBrowserError>()(
  "ProtectedBrowserError",
  {
    reason: Schema.Literals([
      "denied",
      "missing-credential",
      "stale-reference",
      "unsupported",
      "needs-attention",
      "busy",
      "closed",
      "limit",
      "timeout",
      "provider",
      "resolver",
      "outcome-unknown",
      "observation-blocked",
    ]),
    ...Evidence,
  },
) {}

export class CredentialUseResult extends Schema.Class<CredentialUseResult>("CredentialUseResult")({
  ...Evidence,
  authentication: Schema.Literal("unverified"),
}) {}

const SecretText = Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(1024)));

/** Host-only material. Never encode it into Tool results, checkpoints, or storage. */
export const LoginCredential = Schema.TaggedStruct("LoginCredential", {
  username: SecretText,
  password: SecretText,
});

/** Security codes are transient, never persisted. Dummy tests are not PCI compliance evidence. */
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
    reason: Schema.Literals(["denied", "missing-credential", "needs-attention", "resolver"]),
  },
) {}

export interface CredentialAccessRequest {
  readonly caller: Redacted.Redacted<string>;
  readonly kind: typeof CredentialKind.Type;
  readonly target: CredentialTarget;
}

export interface CredentialUseAuthorization extends CredentialAccessRequest {
  readonly key: Redacted.Redacted<string>;
  readonly roles: ReadonlyArray<typeof CredentialFieldRole.Type>;
  readonly submit: boolean;
}

/**
 * Host-only vault and grant port. Derive caller from the authorized invocation, never Tool input.
 * Recheck account ownership, current grants, merchant/frame/recipient pairing, and any side
 * effects caused by filling. Listing authorizes metadata only. Grant checks never widen network
 * policy. Implementations must not log material or put it in defects/traces.
 */
export class BrowserCredentialAccess extends Context.Service<
  BrowserCredentialAccess,
  {
    readonly caller: Effect.Effect<Redacted.Redacted<string>, CredentialAccessError>;
    readonly list: (request: CredentialAccessRequest) => Effect.Effect<
      ReadonlyArray<{
        readonly key: Redacted.Redacted<string>;
        readonly metadata: CredentialOfferMetadata;
      }>,
      CredentialAccessError
    >;
    readonly authorize: (
      request: CredentialUseAuthorization,
    ) => Effect.Effect<void, CredentialAccessError>;
    readonly resolve: (
      request: CredentialUseAuthorization,
    ) => Effect.Effect<BrowserCredentialMaterial, CredentialAccessError>;
    /**
     * Explicitly trust all observed destinations not to echo credentials, including transformed
     * or delayed echoes. Called for every observation and non-secret action after exposure.
     * This is a recipient trust decision, not a universal secrecy guarantee or login verifier.
     */
    readonly observation: (request: {
      readonly caller: Redacted.Redacted<string>;
      readonly topOrigin: typeof CredentialOrigin.Type;
      readonly frameOrigins: ReadonlyArray<typeof CredentialOrigin.Type>;
      readonly exposures: ReadonlyArray<CredentialTarget>;
    }) => Effect.Effect<"trust-recipient-no-credential-echo" | "deny", CredentialAccessError>;
  }
>()("@effect-agent/sandbox/BrowserCredentialAccess") {}

/** A private ephemeral pass. No raw fill, JavaScript, screenshots, viewer, or provider identity. */
export interface ProtectedBrowserHandle {
  readonly navigate: (
    request: ProtectedBrowserNavigate,
  ) => Effect.Effect<void, ProtectedBrowserError>;
  readonly observe: Effect.Effect<ProtectedBrowserObservation, ProtectedBrowserError>;
  readonly click: (request: ProtectedBrowserClick) => Effect.Effect<void, ProtectedBrowserError>;
  readonly listCredentialOffers: (
    request: ListCredentialOffers,
  ) => Effect.Effect<ReadonlyArray<CredentialOffer>, ProtectedBrowserError>;
  readonly useCredential: (
    request: UseCredential,
  ) => Effect.Effect<CredentialUseResult, ProtectedBrowserError>;
  readonly close: Effect.Effect<typeof ProtectedCleanup.Type>;
}

export class ProtectedBrowser extends Context.Service<
  ProtectedBrowser,
  {
    readonly open: (
      policy: InteractiveBrowserPolicy,
    ) => Effect.Effect<
      ProtectedBrowserHandle,
      ProtectedBrowserError,
      Scope.Scope | BrowserCredentialAccess
    >;
  }
>()("@effect-agent/sandbox/ProtectedBrowser") {}

/** Build once in an execution/Attempt Layer. Acquires lazily and shares one pass across Tools. */
export class ProtectedBrowserSession extends Context.Service<
  ProtectedBrowserSession,
  {
    readonly get: Effect.Effect<ProtectedBrowserHandle, ProtectedBrowserError>;
  }
>()("@effect-agent/sandbox/ProtectedBrowserSession") {
  static layer(policy: InteractiveBrowserPolicy) {
    return Layer.effect(this)(
      Effect.gen(function* () {
        const browser = yield* ProtectedBrowser;
        const access = yield* BrowserCredentialAccess;
        const scope = yield* Effect.scope;

        const cached = yield* Effect.cached(
          browser
            .open(policy)
            .pipe(
              Effect.provideService(BrowserCredentialAccess, access),
              Effect.provideService(Scope.Scope, scope),
            ),
        );

        return {
          get: Effect.suspend(() =>
            scope.state._tag === "Closed"
              ? Effect.fail(
                  new ProtectedBrowserError({
                    reason: "closed",
                    dispatch: "not-dispatched",
                    milestone: "none",
                    observation: "closed",
                    cleanup: "not-requested",
                  }),
                )
              : cached,
          ),
        };
      }),
    );
  }
}
